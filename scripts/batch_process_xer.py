#!/usr/bin/env python3
"""
Batch XER Processor - Process all XER files from manifest with file tracking

This script processes all XER files listed in the manifest and exports ALL tables
from each XER file to CSV format. Each record includes a file_id to track which
XER file it came from.

IMPORTANT: All ID columns are prefixed with file_id to maintain referential integrity
across multiple XER files. Format: "{file_id}_{original_id}"

For example, if file_id=48 and task_id=715090, the output will be task_id="48_715090"

This ensures:
- Primary keys are unique across all files
- Foreign key relationships are preserved within each file
- PowerBI and other tools can build proper data models

Output Tables (all with file_id and prefixed IDs):
- xer_files.csv        - Metadata about each XER file (from manifest)
- task.csv             - Tasks (activities)
- taskpred.csv         - Task predecessors/dependencies
- taskrsrc.csv         - Task resource assignments
- taskactv.csv         - Task activity code assignments
- taskmemo.csv         - Task notes/memos
- projwbs.csv          - WBS (Work Breakdown Structure)
- actvcode.csv         - Activity code values
- actvtype.csv         - Activity code types
- calendar.csv         - Calendars
- rsrc.csv             - Resources
- rsrcrate.csv         - Resource rates
- udftype.csv          - User-defined field types
- udfvalue.csv         - User-defined field values
- project.csv          - Project metadata
- ... and all other tables in the XER files

Usage:
    python scripts/batch_process_xer.py
    python scripts/batch_process_xer.py --current-only  # Only process current file
    python scripts/batch_process_xer.py --output-dir data/primavera/processed
"""

import sys
import json
from pathlib import Path
from datetime import datetime
import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.xer_parser import XERParser

# Paths
MANIFEST_PATH = project_root / "data" / "raw" / "xer" / "manifest.json"
XER_DIR = project_root / "data" / "raw" / "xer"
DEFAULT_OUTPUT_DIR = project_root / "data" / "primavera" / "processed"


def prefix_id_columns(df: pd.DataFrame, file_id: int) -> pd.DataFrame:
    """
    Prefix all ID columns with file_id to ensure uniqueness across files.

    Transforms columns ending with '_id' (except 'file_id') from:
        715090 -> "48_715090"

    This ensures primary keys are unique and foreign key relationships
    are preserved within each file.

    For non-numeric values (data issues in some XER files), the value is
    preserved as-is with the file_id prefix.

    Args:
        df: DataFrame to transform
        file_id: The file_id to use as prefix

    Returns:
        DataFrame with transformed ID columns
    """
    df = df.copy()

    def transform_id(x, file_id):
        """Transform a single ID value with file_id prefix"""
        if pd.isna(x) or str(x).strip() == '':
            return ''

        # Try to convert to int for clean numeric IDs
        try:
            return f"{file_id}_{int(float(x))}"
        except (ValueError, TypeError):
            # For non-numeric values, still prefix but keep original value
            return f"{file_id}_{x}"

    for col in df.columns:
        # Skip file_id itself - it stays as the numeric file identifier
        if col == 'file_id':
            continue

        # Transform columns ending with _id
        if col.endswith('_id'):
            df[col] = df[col].apply(lambda x: transform_id(x, file_id))

    return df


def load_manifest() -> dict:
    """Load and return the XER manifest"""
    with open(MANIFEST_PATH) as f:
        return json.load(f)


def create_files_table(manifest: dict) -> pd.DataFrame:
    """
    Create xer_files table from manifest metadata

    Returns DataFrame with columns:
        - file_id: Unique identifier (1-indexed)
        - filename: Original filename
        - date: Schedule date from manifest
        - description: Description from manifest
        - status: current/archived/superseded
        - is_current: Boolean flag for current file
    """
    rows = []
    current_file = manifest["current"]

    for idx, (filename, meta) in enumerate(manifest["files"].items(), start=1):
        rows.append({
            "file_id": idx,
            "filename": filename,
            "date": meta.get("date", ""),
            "description": meta.get("description", ""),
            "status": meta.get("status", ""),
            "is_current": filename == current_file
        })

    df = pd.DataFrame(rows)
    # Sort by date
    df = df.sort_values("date").reset_index(drop=True)
    # Reassign file_id after sorting
    df["file_id"] = range(1, len(df) + 1)

    return df


def process_single_xer(xer_path: Path, file_id: int, verbose: bool = True) -> dict[str, pd.DataFrame]:
    """
    Process a single XER file and return all tables with file_id added
    and all ID columns prefixed with file_id for uniqueness.

    Returns dict mapping table name (lowercase) to DataFrame
    """
    if verbose:
        print(f"  Parsing {xer_path.name}...")

    try:
        parser = XERParser(str(xer_path))
        tables = parser.parse()
    except Exception as e:
        if verbose:
            print(f"    ⚠️  Error parsing: {e}")
        return None

    if not tables:
        if verbose:
            print(f"    ⚠️  No tables found")
        return None

    result = {}
    table_counts = []

    for table_name, df in tables.items():
        if df is None or len(df) == 0:
            continue

        # Add file_id as first column
        df_with_id = df.copy()
        df_with_id.insert(0, 'file_id', file_id)

        # Prefix all ID columns with file_id for uniqueness across files
        df_with_id = prefix_id_columns(df_with_id, file_id)

        # Use lowercase table names for output files
        result[table_name.lower()] = df_with_id
        table_counts.append(f"{table_name}:{len(df)}")

    if verbose:
        # Show summary
        task_count = len(tables.get('TASK', pd.DataFrame()))
        print(f"    ✓ {len(result)} tables, {task_count:,} tasks")

    return result


def batch_process(
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    current_only: bool = False,
    verbose: bool = True
) -> dict[str, Path]:
    """
    Process all XER files from manifest and export all tables

    Args:
        output_dir: Directory for output CSV files
        current_only: If True, only process the current file
        verbose: Print progress messages

    Returns:
        Dict mapping table name to output file path
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load manifest
    manifest = load_manifest()

    if verbose:
        print(f"XER Batch Processor")
        print(f"=" * 60)
        print(f"Manifest: {MANIFEST_PATH}")
        print(f"Output: {output_dir}")
        print(f"Current file: {manifest['current']}")
        print(f"Total files in manifest: {len(manifest['files'])}")
        print()

    # Create files metadata table
    files_df = create_files_table(manifest)

    if current_only:
        files_to_process = files_df[files_df['is_current']]
        if verbose:
            print("Mode: Current file only")
    else:
        files_to_process = files_df
        if verbose:
            print("Mode: All files")

    print()

    # Process each file and collect all tables
    all_tables: dict[str, list[pd.DataFrame]] = {}
    processed_count = 0
    error_count = 0

    if verbose:
        print(f"Processing {len(files_to_process)} XER file(s)...")
        print("-" * 60)

    for _, row in files_to_process.iterrows():
        filename = row['filename']
        file_id = row['file_id']
        xer_path = XER_DIR / filename

        if not xer_path.exists():
            if verbose:
                print(f"  ⚠️  File not found: {filename}")
            error_count += 1
            continue

        result = process_single_xer(xer_path, file_id, verbose)

        if result is not None:
            for table_name, df in result.items():
                if table_name not in all_tables:
                    all_tables[table_name] = []
                all_tables[table_name].append(df)
            processed_count += 1
        else:
            error_count += 1

    if verbose:
        print("-" * 60)
        print(f"Processed: {processed_count}, Errors: {error_count}")
        print()

    # Combine and save all tables
    output_files = {}

    # 1. Save xer_files.csv first
    files_output = output_dir / "xer_files.csv"
    files_df.to_csv(files_output, index=False)
    output_files['xer_files'] = files_output

    if verbose:
        print(f"Saving {len(all_tables) + 1} tables...")
        print("-" * 60)
        print(f"✓ xer_files.csv ({len(files_df)} rows)")

    # 2. Save all other tables
    for table_name in sorted(all_tables.keys()):
        dfs = all_tables[table_name]
        if dfs:
            combined = pd.concat(dfs, ignore_index=True)
            output_path = output_dir / f"{table_name}.csv"
            combined.to_csv(output_path, index=False)
            output_files[table_name] = output_path

            if verbose:
                print(f"✓ {table_name}.csv ({len(combined):,} rows)")

    if verbose:
        print("-" * 60)
        print(f"\n✅ Batch processing complete!")
        print(f"\nOutput directory: {output_dir}")
        print(f"Total tables: {len(output_files)}")

    return output_files


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(
        description='Batch process all XER files from manifest - exports ALL tables',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                              # Process all files
  %(prog)s --current-only               # Only process current file
  %(prog)s --output-dir ./output        # Custom output directory
        """
    )

    parser.add_argument(
        '--output-dir', '-o',
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f'Output directory (default: {DEFAULT_OUTPUT_DIR})'
    )

    parser.add_argument(
        '--current-only', '-c',
        action='store_true',
        help='Only process the current file from manifest'
    )

    parser.add_argument(
        '--quiet', '-q',
        action='store_true',
        help='Suppress progress messages'
    )

    args = parser.parse_args()

    try:
        batch_process(
            output_dir=args.output_dir,
            current_only=args.current_only,
            verbose=not args.quiet
        )
        return 0
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
