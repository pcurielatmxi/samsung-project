#!/usr/bin/env python3
"""
Classify XER files as SECAI (Owner) or YATES (GC) schedules.

Uses proj_short_name from the PROJECT table, with filename fallback.
Adds schedule_type column to xer_files.csv.

Usage:
    python scripts/classify_schedules.py
"""

import argparse
from pathlib import Path

import pandas as pd


def classify_schedule(proj_short_name: str, filename: str = '') -> str:
    """Classify schedule type from project short name and filename."""
    name = str(proj_short_name).upper() if not pd.isna(proj_short_name) else ''
    fname = str(filename).upper()

    # Check project name first
    if 'SECAI' in name or 'T1P1' in name:
        return 'SECAI'
    elif any(x in name for x in ['YATES', 'SAMSUNG-FAB', 'SAMSUNG-TFAB']):
        return 'YATES'

    # Fall back to filename
    if 'SECAI' in fname or 'T1 PROJECT' in fname:
        return 'SECAI'
    elif 'YATES' in fname:
        return 'YATES'

    return 'UNKNOWN'


def parse_date_from_filename(filename: str) -> str | None:
    """Extract date from filename."""
    import re
    patterns = [
        (r'(\d{1,2})-(\d{1,2})-(\d{2,4})', 'MDY'),   # M-DD-YY or MM-DD-YYYY
        (r'(\d{1,2})\.(\d{1,2})\.(\d{2,4})', 'MDY'),  # M.DD.YY or MM.DD.YYYY
        (r'(\d{2})(\d{2})(\d{2})(?!\d)', 'MMDDYY'),   # MMDDYY (like 060625)
    ]
    for pattern, fmt in patterns:
        match = re.search(pattern, filename)
        if match:
            g = match.groups()
            try:
                if fmt == 'MMDDYY':
                    m, d, y = int(g[0]), int(g[1]), 2000 + int(g[2])
                elif len(g[2]) == 4:
                    m, d, y = int(g[0]), int(g[1]), int(g[2])
                else:
                    m, d, y = int(g[0]), int(g[1]), 2000 + int(g[2]) if int(g[2]) < 50 else 1900 + int(g[2])
                if 1 <= m <= 12 and 1 <= d <= 31 and 2020 <= y <= 2030:
                    return f"{y:04d}-{m:02d}-{d:02d}"
            except ValueError:
                continue
    return None


def main():
    parser = argparse.ArgumentParser(description='Classify XER schedules')
    parser.add_argument('--processed-dir', type=Path, default=Path('data/primavera/processed'))
    args = parser.parse_args()

    # Load data
    projects = pd.read_csv(args.processed_dir / 'project.csv', low_memory=False)
    xer_files = pd.read_csv(args.processed_dir / 'xer_files.csv')

    # Get primary project per file and classify
    file_projects = projects.groupby('file_id').first().reset_index()
    result = file_projects[['file_id', 'proj_short_name']].merge(
        xer_files[['file_id', 'filename', 'date']], on='file_id'
    )
    result['schedule_type'] = result.apply(
        lambda r: classify_schedule(r['proj_short_name'], r['filename']), axis=1
    )

    # Summary
    print("=== Schedule Classification ===\n")
    print(result['schedule_type'].value_counts().to_string())

    print(f"\n=== Files by Type ===\n")
    for stype in ['SECAI', 'YATES', 'UNKNOWN']:
        subset = result[result['schedule_type'] == stype]
        if len(subset) > 0:
            print(f"\n{stype} ({len(subset)} files):")
            for _, row in subset.head(5).iterrows():
                print(f"  {row['filename'][:55]}")
            if len(subset) > 5:
                print(f"  ... and {len(subset) - 5} more")

    # Update xer_files.csv with schedule_type and fix missing dates
    if 'schedule_type' not in xer_files.columns:
        xer_files['schedule_type'] = None

    dates_fixed = 0
    for _, row in result.iterrows():
        mask = xer_files['file_id'] == row['file_id']
        xer_files.loc[mask, 'schedule_type'] = row['schedule_type']

        # Fix missing dates by parsing from filename
        current_date = xer_files.loc[mask, 'date'].values[0]
        if current_date in ['YYYY-MM-DD', 'UNKNOWN', None, '']:
            parsed = parse_date_from_filename(row['filename'])
            if parsed:
                xer_files.loc[mask, 'date'] = parsed
                dates_fixed += 1

    output_path = args.processed_dir / 'xer_files.csv'
    xer_files.to_csv(output_path, index=False)
    print(f"\n✓ Updated {output_path}")
    if dates_fixed > 0:
        print(f"  Fixed {dates_fixed} missing dates")


if __name__ == '__main__':
    main()
