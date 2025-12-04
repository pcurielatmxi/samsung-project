#!/usr/bin/env python3
"""
XER to CSV Processor - Extract all tasks from Primavera P6 XER files

This script processes XER files and exports all tasks to a CSV file with full
context including WBS, activity codes (area, level, building, contractor, etc.),
and all date fields.

Usage:
    python scripts/process_xer_to_csv.py <input_xer_file> [output_csv_file]

Examples:
    # Process with auto-generated output filename
    python scripts/process_xer_to_csv.py data/raw/project.xer

    # Process with specific output filename
    python scripts/process_xer_to_csv.py data/raw/project.xer data/output/tasks.csv

    # Process all XER files in a directory
    python scripts/process_xer_to_csv.py data/raw/*.xer
"""

import sys
import os
from pathlib import Path
from datetime import datetime
import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.xer_parser import XERParser


def process_xer_file(input_path: str, output_path: str = None, verbose: bool = True) -> str:
    """
    Process a single XER file and export all tasks to CSV with full context

    Args:
        input_path: Path to the input XER file
        output_path: Path for the output CSV file (optional, auto-generated if not provided)
        verbose: Print progress messages

    Returns:
        Path to the created CSV file
    """
    input_file = Path(input_path)

    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Generate output filename if not provided
    if output_path is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_filename = f"{input_file.stem}_tasks_{timestamp}.csv"
        output_path = project_root / 'data' / 'output' / 'xer_exports' / output_filename
    else:
        output_path = Path(output_path)

    # Create output directory if needed
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if verbose:
        print(f"Processing XER file: {input_file}")
        print(f"Output CSV: {output_path}\n")

    # Parse XER file
    if verbose:
        print("Step 1: Parsing XER file...")

    parser = XERParser(str(input_file))
    tables = parser.parse()

    tasks = parser.get_tasks()
    taskactv = parser.get_table('TASKACTV')
    actvcode = parser.get_table('ACTVCODE')
    actvtype = parser.get_table('ACTVTYPE')
    wbs = parser.get_table('PROJWBS')

    if verbose:
        print(f"  Found {len(tasks)} tasks")

    # Step 1: Add WBS information
    if verbose:
        print("\nStep 2: Adding WBS context...")

    tasks_enhanced = tasks.merge(
        wbs[['wbs_id', 'wbs_short_name', 'wbs_name']],
        on='wbs_id',
        how='left'
    )

    # Step 2: Add activity codes
    if verbose:
        print("\nStep 3: Adding activity code fields...")

    activity_code_types = {
        'Z-AREA': 'Area',
        'Z-LEVEL': 'Level',
        'Z-BLDG': 'Building',
        'Z-SUB CONTRACTOR': 'Subcontractor',
        'Z-TRADE': 'Trade',
        'Z-RESPONSIBLE': 'Responsible',
        'Z-ROOM': 'Room',
        'Z-PHASE (High Level)': 'Phase',
        'Z-BID PACKAGE': 'Bid Package'
    }

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

            tasks_enhanced = tasks_enhanced.merge(task_codes, on='task_id', how='left')

            if verbose:
                print(f"  Added '{column_name}' ({len(codes_of_type)} values)")

    # Step 3: Create final export with user-friendly column names
    if verbose:
        print("\nStep 4: Creating CSV export...")

    export_df = tasks_enhanced[[
        'task_code',
        'task_name',
        'status_code',
        'wbs_name',
        'Building',
        'Area',
        'Level',
        'Room',
        'Phase',
        'Subcontractor',
        'Trade',
        'Responsible',
        'Bid Package',
        'target_start_date',
        'target_end_date',
        'act_start_date',
        'act_end_date',
        'early_start_date',
        'early_end_date',
        'late_start_date',
        'late_end_date',
        'remain_drtn_hr_cnt',
        'target_drtn_hr_cnt',
        'phys_complete_pct'
    ]].copy()

    # Rename columns for readability
    export_df.columns = [
        'Task Code',
        'Task Description',
        'Status',
        'WBS',
        'Building',
        'Area',
        'Level',
        'Room',
        'Phase',
        'Subcontractor',
        'Trade',
        'Responsible Person',
        'Bid Package',
        'Planned Start',
        'Planned Finish',
        'Actual Start',
        'Actual Finish',
        'Early Start',
        'Early Finish',
        'Late Start',
        'Late Finish',
        'Remaining Duration (hrs)',
        'Planned Duration (hrs)',
        'Physical % Complete'
    ]

    # Map status codes to readable values
    status_map = {
        'TK_Complete': 'Complete',
        'TK_Active': 'Active',
        'TK_NotStart': 'Not Started'
    }
    export_df['Status'] = export_df['Status'].map(status_map)

    # Save to CSV
    export_df.to_csv(output_path, index=False)

    if verbose:
        print(f"\n✅ Export complete!")
        print(f"\nSummary:")
        print(f"  Total tasks: {len(export_df):,}")
        print(f"  Total columns: {len(export_df.columns)}")
        print(f"\nStatus breakdown:")
        for status, count in export_df['Status'].value_counts().items():
            print(f"  {status}: {count:,}")
        print(f"\nOutput file: {output_path}")

    return str(output_path)


def main():
    """Main entry point for command-line usage"""
    import argparse

    parser = argparse.ArgumentParser(
        description='Process Primavera P6 XER files and export all tasks to CSV with full context',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s data/raw/project.xer
  %(prog)s data/raw/project.xer data/output/tasks.csv
  %(prog)s data/raw/project.xer --quiet
        """
    )

    parser.add_argument(
        'input_file',
        help='Path to the input XER file'
    )

    parser.add_argument(
        'output_file',
        nargs='?',
        default=None,
        help='Path for the output CSV file (optional, auto-generated if not provided)'
    )

    parser.add_argument(
        '-q', '--quiet',
        action='store_true',
        help='Suppress progress messages'
    )

    args = parser.parse_args()

    try:
        output_path = process_xer_file(
            args.input_file,
            args.output_file,
            verbose=not args.quiet
        )

        if args.quiet:
            print(output_path)

        return 0

    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
