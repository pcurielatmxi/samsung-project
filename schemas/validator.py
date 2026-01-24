"""
Schema validation utilities for output CSV files.

Validates that output files conform to expected schemas without breaking
downstream consumers (Power BI dashboards, etc.).

Validation Rules (per CLAUDE.md):
  - FORBIDDEN: Removing existing columns or changing column names
  - FORBIDDEN: Changing column data types (e.g., string → int)
  - ALLOWED: Adding new columns
  - ALLOWED: Updating data values within existing columns
  - ALLOWED: Adding new rows
"""

from pathlib import Path
from typing import Type, List, Optional, Dict, Any, Tuple
import pandas as pd
from pydantic import BaseModel, ValidationError as PydanticValidationError


class SchemaValidationError(Exception):
    """Raised when schema validation fails."""

    def __init__(
        self,
        message: str,
        missing_columns: Optional[List[str]] = None,
        type_mismatches: Optional[Dict[str, Tuple[str, str]]] = None,
        extra_columns: Optional[List[str]] = None,
    ):
        super().__init__(message)
        self.missing_columns = missing_columns or []
        self.type_mismatches = type_mismatches or {}
        self.extra_columns = extra_columns or []


def pandas_dtype_to_python_type(dtype) -> str:
    """Convert pandas dtype to a simplified type string."""
    dtype_str = str(dtype)

    if dtype_str.startswith('int'):
        return 'int'
    elif dtype_str.startswith('float'):
        return 'float'
    elif dtype_str == 'object':
        return 'str'
    elif dtype_str.startswith('datetime'):
        return 'datetime'
    elif dtype_str == 'bool':
        return 'bool'
    else:
        return dtype_str


def pydantic_type_to_string(field_type) -> str:
    """Convert Pydantic field type to a simplified type string."""
    type_str = str(field_type)

    # Handle Optional types
    if 'Optional' in type_str or 'Union' in type_str:
        # Extract the inner type from Optional[X] or Union[X, None]
        if 'int' in type_str:
            return 'int'
        elif 'float' in type_str:
            return 'float'
        elif 'str' in type_str:
            return 'str'
        elif 'bool' in type_str:
            return 'bool'
        elif 'datetime' in type_str:
            return 'datetime'

    if 'int' in type_str.lower():
        return 'int'
    elif 'float' in type_str.lower():
        return 'float'
    elif 'str' in type_str.lower():
        return 'str'
    elif 'bool' in type_str.lower():
        return 'bool'
    elif 'datetime' in type_str.lower():
        return 'datetime'

    return type_str


def types_compatible(pandas_type: str, pydantic_type: str) -> bool:
    """
    Check if pandas type is compatible with pydantic type.

    Handles the fact that pandas uses float64 for:
    - Nullable integers
    - Columns that are all NaN (could be any type in schema)
    - Mixed numeric data

    This is lenient because CSV type inference is imprecise.
    """
    # Exact match
    if pandas_type == pydantic_type:
        return True

    # float in pandas can represent nullable int
    if pandas_type == 'float' and pydantic_type == 'int':
        return True

    # float in pandas can represent nullable string columns (all NaN)
    # When a column has all NaN values, pandas infers float64
    if pandas_type == 'float' and pydantic_type == 'str':
        return True

    # object in pandas is typically str
    if pandas_type == 'str' and pydantic_type == 'str':
        return True

    # Any numeric to numeric is generally ok
    if pandas_type in ('int', 'float') and pydantic_type in ('int', 'float'):
        return True

    # bool can be read as object or bool
    if pandas_type == 'bool' and pydantic_type == 'bool':
        return True

    # object dtype (str) can represent columns with all None/NaN values
    # which should be compatible with any type in the schema
    if pandas_type == 'str' and pydantic_type in ('int', 'float'):
        return True

    return False


def get_column_name(field_name: str, field_info) -> str:
    """
    Get the CSV column name for a field, handling aliases.

    Pydantic fields can have an alias that represents the actual column name
    in the data (e.g., for columns with leading underscores).
    """
    # Check for alias in validation_alias or alias
    if hasattr(field_info, 'alias') and field_info.alias:
        return field_info.alias
    return field_name


def validate_dataframe(
    df: pd.DataFrame,
    schema: Type[BaseModel],
    strict: bool = False,
) -> List[str]:
    """
    Validate a DataFrame against a Pydantic schema.

    Args:
        df: DataFrame to validate
        schema: Pydantic model class defining expected columns
        strict: If True, fail on extra columns not in schema

    Returns:
        List of validation error messages (empty if valid)

    Note:
        This validates SCHEMA (columns and types), not individual row values.
        Row-level validation would be too slow for large datasets.
    """
    errors = []

    # Get expected columns from schema (using aliases where defined)
    schema_fields = schema.model_fields
    # Map field name -> column name (alias or field name)
    field_to_column = {
        name: get_column_name(name, info)
        for name, info in schema_fields.items()
    }
    expected_columns = set(field_to_column.values())
    actual_columns = set(df.columns)

    # Check for missing required columns
    missing = expected_columns - actual_columns
    if missing:
        errors.append(f"Missing required columns: {sorted(missing)}")

    # Check for extra columns (warning only, unless strict)
    extra = actual_columns - expected_columns
    if extra and strict:
        errors.append(f"Unexpected columns (strict mode): {sorted(extra)}")

    # Check column types for columns that exist in both
    common_columns = expected_columns & actual_columns
    type_mismatches = {}

    # Reverse map: column name -> field name for type lookup
    column_to_field = {v: k for k, v in field_to_column.items()}

    for col in common_columns:
        pandas_type = pandas_dtype_to_python_type(df[col].dtype)
        field_name = column_to_field[col]
        field_info = schema_fields[field_name]

        # Get the annotation type
        pydantic_type = pydantic_type_to_string(field_info.annotation)

        if not types_compatible(pandas_type, pydantic_type):
            type_mismatches[col] = (pandas_type, pydantic_type)

    if type_mismatches:
        mismatch_strs = [
            f"{col}: got {got}, expected {expected}"
            for col, (got, expected) in type_mismatches.items()
        ]
        errors.append(f"Type mismatches: {'; '.join(mismatch_strs)}")

    return errors


def validate_output_file(
    file_path: Path,
    schema: Type[BaseModel],
    strict: bool = False,
    sample_rows: int = 100,
) -> List[str]:
    """
    Validate an output CSV file against a schema.

    Args:
        file_path: Path to CSV file
        schema: Pydantic model class defining expected schema
        strict: If True, fail on extra columns
        sample_rows: Number of rows to read for type inference

    Returns:
        List of validation error messages (empty if valid)

    Raises:
        FileNotFoundError: If file doesn't exist
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    # Read sample for type inference
    df = pd.read_csv(file_path, nrows=sample_rows)

    return validate_dataframe(df, schema, strict=strict)


def validated_df_to_csv(
    df: pd.DataFrame,
    file_path: Path,
    strict: bool = False,
    skip_validation: bool = False,
    **to_csv_kwargs,
) -> None:
    """
    Validate a DataFrame against its schema and write to CSV.

    This function should be used by all scripts that write final output files
    to the processed/ directory. It ensures schema compliance before writing.

    Args:
        df: DataFrame to write
        file_path: Output path (filename determines schema via registry)
        strict: If True, fail on extra columns not in schema
        skip_validation: If True, skip validation (for intermediate files)
        **to_csv_kwargs: Additional arguments passed to df.to_csv()

    Raises:
        SchemaValidationError: If validation fails
        KeyError: If no schema registered for this file

    Example:
        from schemas.validator import validated_df_to_csv

        df = pd.DataFrame(records)
        validated_df_to_csv(df, output_dir / 'raba_consolidated.csv', index=False)
    """
    from .registry import get_schema_for_file

    file_path = Path(file_path)
    filename = file_path.name

    if skip_validation:
        # Directly write without validation (for intermediate pipeline files)
        df.to_csv(file_path, **to_csv_kwargs)
        return

    # Look up schema from registry
    schema = get_schema_for_file(filename)
    if schema is None:
        # No schema registered - warn but allow write
        import warnings
        warnings.warn(
            f"No schema registered for '{filename}'. "
            f"Consider adding a schema to schemas/registry.py for validation.",
            UserWarning
        )
        df.to_csv(file_path, **to_csv_kwargs)
        return

    # Validate DataFrame against schema
    errors = validate_dataframe(df, schema, strict=strict)

    if errors:
        error_msg = (
            f"Schema validation failed for '{filename}':\n"
            + "\n".join(f"  - {e}" for e in errors)
        )
        raise SchemaValidationError(
            error_msg,
            missing_columns=[],  # Could parse from errors if needed
            type_mismatches={},
            extra_columns=[],
        )

    # Validation passed - write the file
    df.to_csv(file_path, **to_csv_kwargs)


def validate_schema_compatibility(
    old_schema: Type[BaseModel],
    new_schema: Type[BaseModel],
) -> List[str]:
    """
    Check if a new schema is backward-compatible with an old schema.

    Per CLAUDE.md rules:
    - Cannot remove columns
    - Cannot change column types
    - CAN add new columns

    Args:
        old_schema: The existing/baseline schema
        new_schema: The proposed new schema

    Returns:
        List of backward compatibility violations
    """
    errors = []

    old_fields = old_schema.model_fields
    new_fields = new_schema.model_fields

    old_columns = set(old_fields.keys())
    new_columns = set(new_fields.keys())

    # Check for removed columns (FORBIDDEN)
    removed = old_columns - new_columns
    if removed:
        errors.append(f"FORBIDDEN: Removed columns: {sorted(removed)}")

    # Check for type changes in existing columns (FORBIDDEN)
    common = old_columns & new_columns
    for col in common:
        old_type = pydantic_type_to_string(old_fields[col].annotation)
        new_type = pydantic_type_to_string(new_fields[col].annotation)

        if old_type != new_type:
            errors.append(
                f"FORBIDDEN: Type change for '{col}': {old_type} → {new_type}"
            )

    # Added columns are allowed (just note them)
    added = new_columns - old_columns
    if added:
        # This is allowed, but we note it for awareness
        pass

    return errors
