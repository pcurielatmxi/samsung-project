#!/usr/bin/env python3
"""
Batch XER Processor - Process all XER files from manifest with file tracking

This script processes all XER files listed in the manifest and exports:
1. xer_files.csv - Metadata about each XER file (from manifest)
2. tasks.csv - All tasks from all files with file_id reference
3. Additional tables as needed (WBS, activity codes, etc.)

Each record includes a file_id to track which XER file it came from.

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
    Process a single XER file and return DataFrames with file_id added

    Returns dict with keys: tasks, wbs, activity_codes
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

    # Get base tables
    tasks = parser.get_tasks()
    if tasks is None or len(tasks) == 0:
        if verbose:
            print(f"    ⚠️  No tasks found")
        return None

    taskactv = parser.get_table('TASKACTV')
    actvcode = parser.get_table('ACTVCODE')
    actvtype = parser.get_table('ACTVTYPE')
    wbs = parser.get_table('PROJWBS')

    # Add WBS information to tasks
    if wbs is not None and len(wbs) > 0:
        tasks = tasks.merge(
            wbs[['wbs_id', 'wbs_short_name', 'wbs_name']],
            on='wbs_id',
            how='left'
        )

    # Add activity codes
    activity_code_types = {
        'Z-AREA': 'area',
        'Z-LEVEL': 'level',
        'Z-BLDG': 'building',
        'Z-SUB CONTRACTOR': 'subcontractor',
        'Z-TRADE': 'trade',
        'Z-RESPONSIBLE': 'responsible',
        'Z-ROOM': 'room',
        'Z-PHASE (High Level)': 'phase',
        'Z-BID PACKAGE': 'bid_package'
    }

    if actvtype is not None and actvcode is not None and taskactv is not None:
        for code_type, column_name in activity_code_types.items():
            type_row = actvtype[actvtype['actv_code_type'] == code_type]
            if len(type_row) > 0:
                type_id = type_row['actv_code_type_id'].values[0]
                codes_of_type = actvcode[actvcode['actv_code_type_id'] == type_id]

                task_codes = taskactv.merge(
                    codes_of_type[['actv_code_id', 'actv_code_name']],
                    on='actv_code_id',
                    how='inner'
                )

                task_codes = task_codes[['task_id', 'actv_code_name']].rename(
                    columns={'actv_code_name': column_name}
                )

                tasks = tasks.merge(task_codes, on='task_id', how='left')

    # Select and rename columns for export
    export_columns = {
        'task_id': 'task_id',
        'task_code': 'task_code',
        'task_name': 'task_name',
        'status_code': 'status',
        'wbs_id': 'wbs_id',
        'wbs_name': 'wbs_name',
        'wbs_short_name': 'wbs_short_name',
        'building': 'building',
        'area': 'area',
        'level': 'level',
        'room': 'room',
        'phase': 'phase',
        'subcontractor': 'subcontractor',
        'trade': 'trade',
        'responsible': 'responsible',
        'bid_package': 'bid_package',
        'target_start_date': 'planned_start',
        'target_end_date': 'planned_finish',
        'act_start_date': 'actual_start',
        'act_end_date': 'actual_finish',
        'early_start_date': 'early_start',
        'early_end_date': 'early_finish',
        'late_start_date': 'late_start',
        'late_end_date': 'late_finish',
        'remain_drtn_hr_cnt': 'remaining_duration_hrs',
        'target_drtn_hr_cnt': 'planned_duration_hrs',
        'phys_complete_pct': 'physical_pct_complete'
    }

    # Build export dataframe with available columns
    export_cols = []
    rename_map = {}
    for old_col, new_col in export_columns.items():
        if old_col in tasks.columns:
            export_cols.append(old_col)
            rename_map[old_col] = new_col

    tasks_export = tasks[export_cols].copy()
    tasks_export = tasks_export.rename(columns=rename_map)

    # Map status codes
    status_map = {
        'TK_Complete': 'Complete',
        'TK_Active': 'Active',
        'TK_NotStart': 'Not Started'
    }
    if 'status' in tasks_export.columns:
        tasks_export['status'] = tasks_export['status'].map(status_map).fillna(tasks_export['status'])

    # Add file_id as first column
    tasks_export.insert(0, 'file_id', file_id)

    if verbose:
        print(f"    ✓ {len(tasks_export):,} tasks")

    return {
        'tasks': tasks_export
    }


def batch_process(
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    current_only: bool = False,
    verbose: bool = True
) -> dict[str, Path]:
    """
    Process all XER files from manifest

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
        print(f"=" * 50)
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

    # Process each file
    all_tasks = []
    processed_count = 0
    error_count = 0

    if verbose:
        print(f"Processing {len(files_to_process)} XER file(s)...")
        print("-" * 50)

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
            all_tasks.append(result['tasks'])
            processed_count += 1
        else:
            error_count += 1

    if verbose:
        print("-" * 50)
        print(f"Processed: {processed_count}, Errors: {error_count}")
        print()

    # Combine all tasks
    if all_tasks:
        tasks_combined = pd.concat(all_tasks, ignore_index=True)
    else:
        tasks_combined = pd.DataFrame()

    # Save output files
    output_files = {}

    # 1. Save xer_files.csv
    files_output = output_dir / "xer_files.csv"
    files_df.to_csv(files_output, index=False)
    output_files['xer_files'] = files_output
    if verbose:
        print(f"✓ Saved {files_output.name} ({len(files_df)} files)")

    # 2. Save tasks.csv
    tasks_output = output_dir / "tasks.csv"
    tasks_combined.to_csv(tasks_output, index=False)
    output_files['tasks'] = tasks_output
    if verbose:
        print(f"✓ Saved {tasks_output.name} ({len(tasks_combined):,} tasks)")

    if verbose:
        print()
        print(f"✅ Batch processing complete!")
        print(f"\nOutput files:")
        for name, path in output_files.items():
            print(f"  - {path}")

    return output_files


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(
        description='Batch process all XER files from manifest',
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
