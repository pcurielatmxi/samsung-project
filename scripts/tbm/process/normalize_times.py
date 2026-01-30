#!/usr/bin/env python3
"""
Normalize time columns in TBM work_entries.csv.

Copies raw times to data quality table and normalizes to HH:MM:SS format.

Usage:
    python -m scripts.tbm.process.normalize_times
    python -m scripts.tbm.process.normalize_times --dry-run
"""

import argparse
import pandas as pd
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
from src.config.settings import Settings


def normalize_time(value) -> str:
    """
    Normalize time values from various Excel formats to HH:MM:SS.

    Handles:
    - HH:MM:SS (pass through)
    - HH:MMAM/PM, H:MMAM/PM (12-hour format)
    - H:MM AM, HH:MM AM (with space)
    - Ham, Hpm, HHam, HHpm (short format like '7am')
    - Excel serial numbers (0.25 = 6:00 AM)
    - Semicolon typos (6;00am -> 6:00am)
    - Date+time strings (extract time portion)

    Returns:
        HH:MM:SS format string or None if unparseable
    """
    if pd.isna(value):
        return None

    val = str(value).strip()
    if not val or val == ' ':
        return None

    # Skip garbage values
    if any(x in val.lower() for x in ['no work', '+h', 'n/a']):
        return None

    # Fix semicolon typos (6;00am -> 6:00am)
    val = val.replace(';', ':')

    # Already in HH:MM:SS format
    match = re.match(r'^(\d{1,2}):(\d{2}):(\d{2})$', val)
    if match:
        h, m, s = match.groups()
        return f'{int(h):02d}:{m}:{s}'

    # HH:MM format (no seconds)
    match = re.match(r'^(\d{1,2}):(\d{2})$', val)
    if match:
        h, m = match.groups()
        return f'{int(h):02d}:{m}:00'

    # 12-hour format: HH:MM:SS AM/PM or HH:MM AM/PM
    match = re.match(r'^(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM|am|pm|a\.?m\.?|p\.?m\.?)$', val, re.IGNORECASE)
    if match:
        h, m, s, ampm = match.groups()
        h = int(h)
        s = s or '00'
        if ampm.lower().startswith('p') and h != 12:
            h += 12
        elif ampm.lower().startswith('a') and h == 12:
            h = 0
        return f'{h:02d}:{m}:{s}'

    # Short format: 7am, 7AM, 7pm, 2PM, 12am
    match = re.match(r'^(\d{1,2})\s*(AM|PM|am|pm)$', val, re.IGNORECASE)
    if match:
        h, ampm = match.groups()
        h = int(h)
        if ampm.lower() == 'pm' and h != 12:
            h += 12
        elif ampm.lower() == 'am' and h == 12:
            h = 0
        return f'{h:02d}:00:00'

    # Format with space: "6:00 am", "7:30 pm"
    match = re.match(r'^(\d{1,2}):(\d{2})\s+(AM|PM|am|pm|a\.?m\.?|p\.?m\.?)$', val, re.IGNORECASE)
    if match:
        h, m, ampm = match.groups()
        h = int(h)
        if ampm.lower().startswith('p') and h != 12:
            h += 12
        elif ampm.lower().startswith('a') and h == 12:
            h = 0
        return f'{h:02d}:{m}:00'

    # Excel serial number (fraction of day: 0.25 = 6:00 AM, 0.75 = 6:00 PM)
    match = re.match(r'^0\.(\d+)$', val)
    if match:
        try:
            fraction = float(val)
            total_minutes = int(fraction * 24 * 60)
            hours = total_minutes // 60
            minutes = total_minutes % 60
            return f'{hours:02d}:{minutes:02d}:00'
        except ValueError:
            pass

    # Date+time format: extract time portion (MM/DD/YYYY HH:MM)
    match = re.search(r'\d{1,2}/\d{1,2}/\d{4}\s+(\d{1,2}):(\d{2})', val)
    if match:
        h, m = match.groups()
        return f'{int(h):02d}:{m}:00'

    # Excel datetime that looks like "1900-01-07 00:00:00" (malformed)
    match = re.match(r'^1900-01-\d{2}\s+(\d{2}):(\d{2}):(\d{2})$', val)
    if match:
        return None

    # Could not parse
    return None


def main(dry_run: bool = False):
    """Normalize times in work_entries.csv and update data quality table."""

    work_entries_path = Settings.TBM_PROCESSED_DIR / 'work_entries.csv'
    data_quality_path = Settings.TBM_PROCESSED_DIR / 'work_entries_data_quality.csv'

    if not work_entries_path.exists():
        print(f"Error: {work_entries_path} not found")
        return 1

    if not data_quality_path.exists():
        print(f"Error: {data_quality_path} not found")
        return 1

    print(f"Loading {work_entries_path}...")
    df = pd.read_csv(work_entries_path, low_memory=False)
    print(f"  Loaded {len(df)} rows")

    print(f"Loading {data_quality_path}...")
    dq = pd.read_csv(data_quality_path, low_memory=False)
    print(f"  Loaded {len(dq)} rows")

    # Copy raw times to data quality table
    print("\nCopying raw times to data quality table...")
    dq['start_time_raw'] = df['start_time'].copy()
    dq['end_time_raw'] = df['end_time'].copy()

    # Count non-null values
    start_non_null = df['start_time'].notna().sum()
    end_non_null = df['end_time'].notna().sum()
    print(f"  start_time: {start_non_null} non-null values")
    print(f"  end_time: {end_non_null} non-null values")

    # Normalize times in main table
    print("\nNormalizing times in work_entries...")
    df['start_time'] = df['start_time'].apply(normalize_time)
    df['end_time'] = df['end_time'].apply(normalize_time)

    # Count normalized values
    start_normalized = df['start_time'].notna().sum()
    end_normalized = df['end_time'].notna().sum()
    print(f"  start_time: {start_normalized} normalized ({start_non_null - start_normalized} could not be parsed)")
    print(f"  end_time: {end_normalized} normalized ({end_non_null - end_normalized} could not be parsed)")

    # Show sample of normalized times
    print("\nSample normalized start_time values:")
    sample = df['start_time'].dropna().value_counts().head(10)
    for val, count in sample.items():
        print(f"  {val}: {count}")

    if dry_run:
        print("\nDry run - no files modified")
        return 0

    # Save updated files
    print(f"\nSaving {work_entries_path}...")
    df.to_csv(work_entries_path, index=False)

    print(f"Saving {data_quality_path}...")
    dq.to_csv(data_quality_path, index=False)

    print("\nDone!")
    return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Normalize TBM time columns')
    parser.add_argument('--dry-run', '-n', action='store_true',
                        help='Show what would be done without saving')
    args = parser.parse_args()

    sys.exit(main(dry_run=args.dry_run))
