#!/usr/bin/env python3
"""
Filter Tasks from Full XER Export

This script takes the full task export and creates filtered subsets based on
task description keywords, status, subcontractor, level, area, etc.

Usage:
    python scripts/filter_tasks.py <input_csv> --keyword "drywall"
    python scripts/filter_tasks.py <input_csv> --status "Active"
    python scripts/filter_tasks.py <input_csv> --subcontractor "BERG"
    python scripts/filter_tasks.py <input_csv> --level "L2"
"""

import argparse
import sys
from pathlib import Path
import pandas as pd


def filter_by_keyword(df: pd.DataFrame, keyword: str, column: str = 'Task Description') -> pd.DataFrame:
    """Filter tasks by keyword in specified column"""
    return df[df[column].str.contains(keyword, case=False, na=False)]


def filter_by_exact_match(df: pd.DataFrame, value: str, column: str) -> pd.DataFrame:
    """Filter tasks by exact match in specified column"""
    return df[df[column] == value]


def filter_by_contains(df: pd.DataFrame, value: str, column: str) -> pd.DataFrame:
    """Filter tasks by partial match in specified column"""
    return df[df[column].str.contains(value, case=False, na=False)]


def main():
    parser = argparse.ArgumentParser(
        description='Filter tasks from full XER export CSV',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Filter by keyword in task description
  %(prog)s tasks.csv --keyword "drywall" -o drywall_tasks.csv

  # Filter by status
  %(prog)s tasks.csv --status "Active" -o active_tasks.csv

  # Filter by subcontractor
  %(prog)s tasks.csv --subcontractor "BERG" -o berg_tasks.csv

  # Filter by level
  %(prog)s tasks.csv --level "L2" -o level_2_tasks.csv

  # Combine multiple filters
  %(prog)s tasks.csv --keyword "door" --status "Active" -o active_doors.csv
        """
    )

    parser.add_argument('input_file', help='Input CSV file (from process_xer_to_csv.py)')
    parser.add_argument('-o', '--output', help='Output CSV file', required=True)
    parser.add_argument('--keyword', help='Filter by keyword in task description')
    parser.add_argument('--status', help='Filter by status (Complete, Active, Not Started)')
    parser.add_argument('--subcontractor', help='Filter by subcontractor name')
    parser.add_argument('--level', help='Filter by level (e.g., L2, L3)')
    parser.add_argument('--area', help='Filter by area')
    parser.add_argument('--building', help='Filter by building')
    parser.add_argument('--trade', help='Filter by trade')

    args = parser.parse_args()

    # Read input CSV
    print(f"Reading: {args.input_file}")
    df = pd.read_csv(args.input_file)
    print(f"  Total tasks: {len(df):,}")

    # Apply filters
    filtered_df = df.copy()

    if args.keyword:
        filtered_df = filter_by_keyword(filtered_df, args.keyword)
        print(f"  After keyword filter '{args.keyword}': {len(filtered_df):,}")

    if args.status:
        filtered_df = filter_by_exact_match(filtered_df, args.status, 'Status')
        print(f"  After status filter '{args.status}': {len(filtered_df):,}")

    if args.subcontractor:
        filtered_df = filter_by_exact_match(filtered_df, args.subcontractor, 'Subcontractor')
        print(f"  After subcontractor filter '{args.subcontractor}': {len(filtered_df):,}")

    if args.level:
        filtered_df = filter_by_contains(filtered_df, args.level, 'Level')
        print(f"  After level filter '{args.level}': {len(filtered_df):,}")

    if args.area:
        filtered_df = filter_by_contains(filtered_df, args.area, 'Area')
        print(f"  After area filter '{args.area}': {len(filtered_df):,}")

    if args.building:
        filtered_df = filter_by_contains(filtered_df, args.building, 'Building')
        print(f"  After building filter '{args.building}': {len(filtered_df):,}")

    if args.trade:
        filtered_df = filter_by_contains(filtered_df, args.trade, 'Trade')
        print(f"  After trade filter '{args.trade}': {len(filtered_df):,}")

    # Save output
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    filtered_df.to_csv(output_path, index=False)

    print(f"\n✅ Filtered tasks saved to: {output_path}")
    print(f"   {len(filtered_df):,} tasks exported")

    return 0


if __name__ == '__main__':
    sys.exit(main())
