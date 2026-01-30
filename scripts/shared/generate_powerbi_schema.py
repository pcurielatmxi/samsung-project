"""
Generate JSON schema for Power BI dynamic CSV loading.

STRICT MODE (default): Only generates schema for files with registered Pydantic schemas.
Schema is derived FROM Pydantic definitions, not inferred from CSV data.
This ensures only manually approved schemas are included.

Usage:
    python -m scripts.shared.generate_powerbi_schema
    python -m scripts.shared.generate_powerbi_schema --output custom_path.json
    python -m scripts.shared.generate_powerbi_schema --strict  # Fail if missing schemas
    python -m scripts.shared.generate_powerbi_schema --list    # List registered schemas
"""

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Type, Optional

import pandas as pd
from pydantic import BaseModel

from src.config.settings import settings
from scripts.shared.pipeline_registry import (
    SOURCES,
    DIMENSION_BUILDERS,
    get_all_output_files,
)

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


# Mapping from Pydantic types to Power Query M types
PYDANTIC_TO_POWERQUERY = {
    'int': 'Int64.Type',
    'float': 'Number.Type',
    'str': 'Text.Type',
    'bool': 'Logical.Type',
    'datetime': 'DateTime.Type',
    'date': 'Date.Type',
}


def pydantic_type_to_powerquery(annotation) -> str:
    """
    Convert a Pydantic field annotation to Power Query type.

    Handles Optional types by extracting the inner type.
    """
    type_str = str(annotation)

    # Handle Optional types (Union[X, None])
    if 'Optional' in type_str or 'Union' in type_str:
        if 'int' in type_str:
            return 'Int64.Type'
        elif 'float' in type_str:
            return 'Number.Type'
        elif 'bool' in type_str:
            return 'Logical.Type'
        elif 'datetime' in type_str.lower():
            return 'DateTime.Type'
        elif 'date' in type_str.lower():
            return 'Date.Type'
        elif 'str' in type_str:
            return 'Text.Type'
        # Default for unknown Optional types
        return 'Text.Type'

    # Direct type checks
    if 'int' in type_str.lower():
        return 'Int64.Type'
    elif 'float' in type_str.lower():
        return 'Number.Type'
    elif 'bool' in type_str.lower():
        return 'Logical.Type'
    elif 'datetime' in type_str.lower():
        return 'DateTime.Type'
    elif 'date' in type_str.lower():
        return 'Date.Type'

    # Default to Text
    return 'Text.Type'


def pydantic_schema_to_powerbi(
    schema: Type[BaseModel],
    relative_path: str,
) -> dict:
    """
    Convert a Pydantic schema to Power BI schema format.

    Args:
        schema: Pydantic model class
        relative_path: Path relative to data root (e.g., "processed/tbm/work_entries.csv")

    Returns:
        Dictionary with Power BI schema definition
    """
    columns = []
    for field_name, field_info in schema.model_fields.items():
        # Handle aliases (for columns with special characters)
        col_name = field_name
        if hasattr(field_info, 'alias') and field_info.alias:
            col_name = field_info.alias

        col_type = pydantic_type_to_powerquery(field_info.annotation)
        columns.append({
            'name': col_name,
            'type': col_type
        })

    return {
        'path': relative_path,
        'columns': columns,
        'column_count': len(columns),
        'schema_source': 'pydantic',  # Mark as derived from Pydantic
    }


def validate_csv_against_pydantic(
    csv_path: Path,
    schema: Type[BaseModel],
) -> list[str]:
    """
    Validate that a CSV file matches its Pydantic schema.

    Checks:
    1. All Pydantic columns exist in CSV
    2. No extra columns in CSV (warning only)

    Returns:
        List of error messages (empty if valid)
    """
    errors = []

    if not csv_path.exists():
        return [f"File not found: {csv_path}"]

    # Read just headers
    df = pd.read_csv(csv_path, nrows=0)
    csv_columns = set(df.columns)

    # Get expected columns from Pydantic schema
    expected_columns = set()
    for field_name, field_info in schema.model_fields.items():
        col_name = field_name
        if hasattr(field_info, 'alias') and field_info.alias:
            col_name = field_info.alias
        expected_columns.add(col_name)

    # Check for missing columns
    missing = expected_columns - csv_columns
    if missing:
        errors.append(f"Missing columns in CSV: {sorted(missing)}")

    return errors


def get_registered_output_files() -> list[str]:
    """
    Get all registered output files from the pipeline registry.

    Returns:
        List of relative paths (e.g., ['tbm/work_entries.csv', ...])
    """
    files = []

    # Fact tables, data quality tables, and additional outputs
    files.extend(get_all_output_files())

    # Dimension tables
    for dim in DIMENSION_BUILDERS:
        files.append(dim['output'])

    # Also include company aliases map (useful for Power BI)
    files.append('integrated_analysis/map_company_aliases.csv')

    return sorted(set(files))


def get_pydantic_schema_for_file(filename: str) -> Optional[Type[BaseModel]]:
    """
    Get the Pydantic schema for a file by its filename.

    Args:
        filename: Just the filename (e.g., 'work_entries.csv')

    Returns:
        Pydantic model class or None if not registered
    """
    from schemas.registry import SCHEMA_REGISTRY
    return SCHEMA_REGISTRY.get(filename)


def generate_schema_from_pydantic(
    output_path: Path = None,
    strict: bool = False,
) -> tuple[Path, list[str], list[str]]:
    """
    Generate Power BI schema JSON from Pydantic schemas.

    Only includes files that have registered Pydantic schemas.
    Schema is derived FROM Pydantic definitions, not inferred from CSV.

    Args:
        output_path: Where to write the JSON. Defaults to processed/powerbi_schema.json
        strict: If True, return errors for pipeline outputs without Pydantic schemas

    Returns:
        Tuple of (output_path, errors, warnings)
    """
    if output_path is None:
        output_path = settings.PROCESSED_DATA_DIR / 'powerbi_schema.json'

    logger.info(f"Data root: {settings.DATA_DIR}")
    logger.info("Mode: Pydantic schema-driven (no inference)")
    logger.info("=" * 60)

    schemas = {}
    errors = []
    warnings = []

    registered_files = get_registered_output_files()

    for rel_file in registered_files:
        filename = Path(rel_file).name
        table_name = str(Path(rel_file).with_suffix('')).replace('\\', '/')
        # Use backslashes for Windows/Power BI compatibility
        path_from_root = f"processed\\{rel_file}".replace('/', '\\')
        csv_path = settings.PROCESSED_DATA_DIR / rel_file

        # Check for Pydantic schema
        pydantic_schema = get_pydantic_schema_for_file(filename)

        if pydantic_schema is None:
            msg = f"{rel_file}: No Pydantic schema registered"
            if strict:
                errors.append(msg)
                logger.warning(f"  ✗ {table_name}: No Pydantic schema (STRICT)")
            else:
                warnings.append(msg)
                logger.info(f"  ○ {table_name}: Skipped (no Pydantic schema)")
            continue

        # Validate CSV exists and matches schema
        if csv_path.exists():
            validation_errors = validate_csv_against_pydantic(csv_path, pydantic_schema)
            if validation_errors:
                for err in validation_errors:
                    errors.append(f"{rel_file}: {err}")
                logger.warning(f"  ✗ {table_name}: Validation failed")
                continue

        # Generate schema from Pydantic (not from CSV)
        schema = pydantic_schema_to_powerbi(pydantic_schema, path_from_root)
        schemas[table_name] = schema
        logger.info(f"  ✓ {table_name}: {schema['column_count']} columns (from Pydantic)")

    # Build the output structure
    output = {
        '_meta': {
            'description': 'Power BI schema configuration for dynamic CSV loading',
            'generated_by': 'scripts/shared/generate_powerbi_schema.py',
            'generated_at': datetime.now().isoformat(),
            'mode': 'pydantic-driven',
            'table_count': len(schemas),
        },
        'tables': schemas
    }

    # Write JSON with nice formatting
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2)

    logger.info("=" * 60)
    logger.info(f"Generated: {output_path}")
    logger.info(f"Tables: {len(schemas)}")

    return output_path, errors, warnings


def list_schema_coverage():
    """List all pipeline outputs and their Pydantic schema status."""
    from schemas.registry import SCHEMA_REGISTRY

    registered_files = get_registered_output_files()

    print("Pipeline Output Schema Coverage:")
    print("=" * 70)

    with_schema = []
    without_schema = []

    for rel_file in registered_files:
        filename = Path(rel_file).name
        has_schema = filename in SCHEMA_REGISTRY
        if has_schema:
            with_schema.append(rel_file)
            print(f"  ✓ {rel_file}")
        else:
            without_schema.append(rel_file)
            print(f"  ○ {rel_file} (no Pydantic schema)")

    print("=" * 70)
    print(f"With schema: {len(with_schema)}")
    print(f"Without schema: {len(without_schema)}")

    if without_schema:
        print("\nTo add schemas, create Pydantic models in schemas/ and register in schemas/registry.py")


def main():
    parser = argparse.ArgumentParser(
        description='Generate Power BI schema JSON from Pydantic schemas (not inferred from CSV)'
    )
    parser.add_argument(
        '--output', '-o',
        type=Path,
        help='Output path for schema JSON (default: processed/powerbi_schema.json)'
    )
    parser.add_argument(
        '--strict',
        action='store_true',
        help='Fail if any pipeline output is missing a Pydantic schema'
    )
    parser.add_argument(
        '--list',
        action='store_true',
        help='List pipeline outputs and their Pydantic schema coverage'
    )
    args = parser.parse_args()

    if args.list:
        list_schema_coverage()
        return 0

    output_path, errors, warnings = generate_schema_from_pydantic(
        args.output,
        strict=args.strict,
    )

    if warnings:
        print(f"\n{len(warnings)} files without Pydantic schemas (skipped):")
        for warn in warnings[:10]:
            print(f"  ○ {warn}")
        if len(warnings) > 10:
            print(f"  ... and {len(warnings) - 10} more")

    if errors:
        print(f"\n{len(errors)} errors:")
        for err in errors:
            print(f"  ✗ {err}")
        return 1

    print(f"\n✓ Power BI schema generated successfully")
    return 0


if __name__ == '__main__':
    exit(main())
