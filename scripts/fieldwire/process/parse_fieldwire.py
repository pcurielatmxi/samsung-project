#!/usr/bin/env python3
"""
Parse Fieldwire CSV data dump and extract TBM audit records.

Input: UTF-16LE encoded CSV from Fieldwire export
Output: Normalized CSV with TBM audit data for LPI analysis
"""

import argparse
import csv
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from src.config.settings import settings


# Column indices (0-based) from Fieldwire export
COLUMNS = {
    'id': 0,
    'title': 1,
    'status': 2,
    'category': 3,
    'assignee': 4,
    'start_date': 5,
    'end_date': 6,
    'plan': 7,
    'tier_1': 10,  # Grid location (L-18, J-14)
    'tier_2': 11,
    'tier_3': 12,
    'activity_name': 15,
    'activity_id': 16,
    'wbs_code': 17,
    'building': 20,
    'level': 21,
    'company': 22,
    'location_id': 23,
    'tbm_manpower': 27,
    'direct_manpower': 28,
    'indirect_manpower': 29,
    'total_idle_hours': 30,
    'created': 35,
    'last_updated': 39,
    'tag_1': 40,
    'tag_2': 41,
    # Checklists for idle time (103-108 in 1-based, 102-107 in 0-based)
    'checklist_active': 102,
    'checklist_passive': 103,
    'checklist_obstructed': 104,
    'checklist_meeting': 105,
    'checklist_no_manpower': 106,
    'checklist_not_started': 107,
}

# Valid status types for TBM analysis
TBM_STATUSES = {'TBM'}

# Categories to exclude from TBM location analysis (but Manpower Count parsed separately)
EXCLUDE_CATEGORIES = {'Doors', 'Manpower Count'}

# Category for daily headcount totals
MANPOWER_COUNT_CATEGORY = 'Manpower Count'


def parse_checklist_value(value: str) -> tuple[bool, Optional[str], Optional[str]]:
    """
    Parse checklist field value.

    Format: "{Yes|Not set}: {Category} ({Inspector}) - {Date}"
    Example: "Yes: Active (HBA) - 2026-01-12"

    Returns: (is_set, inspector_code, date)
    """
    if not value or value.strip() == '':
        return False, None, None

    # Pattern: "Yes: ... (ABC) - YYYY-MM-DD" or "Not set: ..."
    is_set = value.startswith('Yes:')

    # Extract inspector code
    inspector_match = re.search(r'\(([A-Z]{2,3})\)', value)
    inspector = inspector_match.group(1) if inspector_match else None

    # Extract date
    date_match = re.search(r'(\d{4}-\d{2}-\d{2})', value)
    date = date_match.group(1) if date_match else None

    return is_set, inspector, date


def parse_numeric(value: str) -> Optional[float]:
    """Parse numeric value, handling empty strings and quotes."""
    if not value or value.strip() in ('', '""'):
        return None
    try:
        # Remove quotes if present
        clean = value.strip().strip('"')
        return float(clean) if clean else None
    except ValueError:
        return None


def parse_date(value: str) -> Optional[str]:
    """Parse date value, return in YYYY-MM-DD format."""
    if not value or value.strip() in ('', '""'):
        return None
    try:
        clean = value.strip().strip('"')
        # Already in YYYY-MM-DD format
        if re.match(r'\d{4}-\d{2}-\d{2}', clean):
            return clean
        return None
    except Exception:
        return None


def clean_string(value: str) -> str:
    """Clean string value, removing quotes and extra whitespace."""
    if not value:
        return ''
    return value.strip().strip('"').strip()


def read_fieldwire_csv(filepath: Path) -> list[list[str]]:
    """
    Read Fieldwire CSV file (UTF-16LE encoded).

    Returns list of rows (skipping header rows).
    """
    rows = []

    with open(filepath, 'r', encoding='utf-16-le') as f:
        # Skip BOM if present
        content = f.read()
        if content.startswith('\ufeff'):
            content = content[1:]

        # Parse as TSV
        reader = csv.reader(content.splitlines(), delimiter='\t')

        for i, row in enumerate(reader):
            # Skip first 3 header rows (Generated with..., Samsung..., Date)
            # Row 4 (index 3) is the column headers
            if i < 4:
                continue
            rows.append(row)

    return rows


def parse_tbm_record(row: list[str]) -> Optional[dict]:
    """
    Parse a single TBM record from a row.

    Returns dict with normalized fields, or None if not a valid TBM record.
    """
    # Check we have enough columns
    if len(row) < 108:
        return None

    status = clean_string(row[COLUMNS['status']])
    category = clean_string(row[COLUMNS['category']])

    # Filter to TBM records only
    if status not in TBM_STATUSES:
        return None

    # Exclude certain categories
    if category in EXCLUDE_CATEGORIES:
        return None

    # Parse checklist values for idle indicators
    active_set, active_inspector, active_date = parse_checklist_value(
        row[COLUMNS['checklist_active']] if len(row) > COLUMNS['checklist_active'] else ''
    )
    passive_set, passive_inspector, passive_date = parse_checklist_value(
        row[COLUMNS['checklist_passive']] if len(row) > COLUMNS['checklist_passive'] else ''
    )
    obstructed_set, _, _ = parse_checklist_value(
        row[COLUMNS['checklist_obstructed']] if len(row) > COLUMNS['checklist_obstructed'] else ''
    )
    meeting_set, _, _ = parse_checklist_value(
        row[COLUMNS['checklist_meeting']] if len(row) > COLUMNS['checklist_meeting'] else ''
    )
    no_manpower_set, _, _ = parse_checklist_value(
        row[COLUMNS['checklist_no_manpower']] if len(row) > COLUMNS['checklist_no_manpower'] else ''
    )
    not_started_set, _, _ = parse_checklist_value(
        row[COLUMNS['checklist_not_started']] if len(row) > COLUMNS['checklist_not_started'] else ''
    )

    # Build record
    record = {
        'id': clean_string(row[COLUMNS['id']]),
        'title': clean_string(row[COLUMNS['title']]),
        'status': status,
        'category': category,
        'start_date': parse_date(row[COLUMNS['start_date']]),
        'tier_1': clean_string(row[COLUMNS['tier_1']]),
        'tier_2': clean_string(row[COLUMNS['tier_2']]),
        'tier_3': clean_string(row[COLUMNS['tier_3']]),
        'building': clean_string(row[COLUMNS['building']]),
        'level': clean_string(row[COLUMNS['level']]),
        'company': clean_string(row[COLUMNS['company']]),
        'location_id': clean_string(row[COLUMNS['location_id']]),
        'activity_name': clean_string(row[COLUMNS['activity_name']]),
        'activity_id': clean_string(row[COLUMNS['activity_id']]),
        'wbs_code': clean_string(row[COLUMNS['wbs_code']]),
        'tbm_manpower': parse_numeric(row[COLUMNS['tbm_manpower']]),
        'direct_manpower': parse_numeric(row[COLUMNS['direct_manpower']]),
        'indirect_manpower': parse_numeric(row[COLUMNS['indirect_manpower']]),
        'total_idle_hours': parse_numeric(row[COLUMNS['total_idle_hours']]),
        'tag_1': clean_string(row[COLUMNS['tag_1']]),
        'tag_2': clean_string(row[COLUMNS['tag_2']]),
        # Idle indicators
        'is_active': active_set,
        'is_passive': passive_set,
        'is_obstructed': obstructed_set,
        'is_meeting': meeting_set,
        'is_no_manpower': no_manpower_set,
        'is_not_started': not_started_set,
        'inspector': active_inspector or passive_inspector,
        'observation_date': active_date or passive_date,
        # Timestamps
        'created': clean_string(row[COLUMNS['created']]),
        'last_updated': clean_string(row[COLUMNS['last_updated']]),
    }

    return record


def parse_manpower_count_record(row: list[str]) -> Optional[dict]:
    """
    Parse a Manpower Count record (daily START/END headcount by contractor).

    These records contain daily totals like:
      - "Berg 12.17.25 START" with TBM Manpower = 116
      - "MK Marlow 12.15.25 END" with TBM Manpower = 83

    Returns dict with normalized fields, or None if not a valid manpower count record.
    """
    if len(row) < 30:
        return None

    status = clean_string(row[COLUMNS['status']])
    category = clean_string(row[COLUMNS['category']])

    # Filter to Manpower Count category only
    if category != MANPOWER_COUNT_CATEGORY:
        return None

    title = clean_string(row[COLUMNS['title']])
    company = clean_string(row[COLUMNS['company']])
    start_date = parse_date(row[COLUMNS['start_date']])
    tbm_manpower = parse_numeric(row[COLUMNS['tbm_manpower']])
    direct_manpower = parse_numeric(row[COLUMNS['direct_manpower']])
    indirect_manpower = parse_numeric(row[COLUMNS['indirect_manpower']])

    # Parse count type from title (START or END)
    count_type = None
    if 'START' in title.upper():
        count_type = 'START'
    elif 'END' in title.upper():
        count_type = 'END'

    # Try to extract date from title if start_date is missing
    # Format examples: "12.17.25", "1.2.26", "12/15/25"
    title_date = None
    date_patterns = [
        r'(\d{1,2})\.(\d{1,2})\.(\d{2,4})',  # 12.17.25 or 1.2.26
        r'(\d{1,2})/(\d{1,2})/(\d{2,4})',     # 12/17/25
    ]
    for pattern in date_patterns:
        match = re.search(pattern, title)
        if match:
            month, day, year = match.groups()
            # Handle 2-digit year
            if len(year) == 2:
                year = '2025' if int(year) >= 22 else '2026'
            title_date = f"{year}-{int(month):02d}-{int(day):02d}"
            break

    # Use title date if start_date is missing
    effective_date = start_date or title_date

    record = {
        'id': clean_string(row[COLUMNS['id']]),
        'title': title,
        'status': status,
        'category': category,
        'date': effective_date,
        'company': company,
        'count_type': count_type,
        'tbm_manpower': tbm_manpower,
        'direct_manpower': direct_manpower,
        'indirect_manpower': indirect_manpower,
        'created': clean_string(row[COLUMNS['created']]),
        'last_updated': clean_string(row[COLUMNS['last_updated']]),
    }

    return record


def find_latest_input_file(input_dir: Path) -> Optional[Path]:
    """Find the most recent Fieldwire CSV dump file."""
    csv_files = list(input_dir.glob('Samsung_-_Progress_Tracking*.csv'))

    if not csv_files:
        return None

    # Sort by modification time, newest first
    csv_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return csv_files[0]


def main():
    parser = argparse.ArgumentParser(
        description='Parse Fieldwire CSV data dump for TBM audit analysis'
    )
    parser.add_argument(
        '--input', '-i',
        type=Path,
        help='Input CSV file (default: latest in processed/fieldwire/)'
    )
    parser.add_argument(
        '--output', '-o',
        type=Path,
        help='Output CSV file (default: processed/fieldwire/tbm_audits.csv)'
    )
    parser.add_argument(
        '--dry-run', '-n',
        action='store_true',
        help='Show what would be done without writing files'
    )
    args = parser.parse_args()

    # Determine input file
    if args.input:
        input_file = args.input
    else:
        input_dir = settings.DATA_DIR / 'processed' / 'fieldwire'
        input_file = find_latest_input_file(input_dir)
        if not input_file:
            print(f"Error: No Fieldwire CSV files found in {input_dir}")
            sys.exit(1)

    print(f"Input file: {input_file}")

    # Determine output file
    if args.output:
        output_file = args.output
    else:
        output_file = settings.DATA_DIR / 'processed' / 'fieldwire' / 'tbm_audits.csv'

    print(f"Output file: {output_file}")

    # Read input
    print("Reading Fieldwire CSV...")
    rows = read_fieldwire_csv(input_file)
    print(f"  Total rows: {len(rows)}")

    # Parse TBM location records
    print("\nParsing TBM location records...")
    records = []
    skipped = 0

    for row in rows:
        record = parse_tbm_record(row)
        if record:
            records.append(record)
        else:
            skipped += 1

    print(f"  TBM location records: {len(records)}")
    print(f"  Skipped (non-TBM or excluded): {skipped}")

    # Parse Manpower Count records (daily totals)
    print("\nParsing Manpower Count records (daily totals)...")
    manpower_records = []

    for row in rows:
        record = parse_manpower_count_record(row)
        if record:
            manpower_records.append(record)

    print(f"  Manpower Count records: {len(manpower_records)}")

    # Summary by category
    categories = {}
    for r in records:
        cat = r['category'] or 'Unknown'
        categories[cat] = categories.get(cat, 0) + 1

    print("\nRecords by category:")
    for cat, count in sorted(categories.items(), key=lambda x: -x[1]):
        print(f"  {cat}: {count}")

    # Summary by company
    companies = {}
    for r in records:
        comp = r['company'] or 'Unknown'
        companies[comp] = companies.get(comp, 0) + 1

    print("\nTBM locations by company:")
    for comp, count in sorted(companies.items(), key=lambda x: -x[1]):
        print(f"  {comp}: {count}")

    # Summary of manpower counts
    if manpower_records:
        print("\n" + "=" * 50)
        print("DAILY MANPOWER COUNTS")
        print("=" * 50)

        # By company
        mp_by_company = {}
        for r in manpower_records:
            comp = r['company'] or 'Unknown'
            if comp not in mp_by_company:
                mp_by_company[comp] = {'count': 0, 'total_mp': 0}
            mp_by_company[comp]['count'] += 1
            if r['tbm_manpower']:
                mp_by_company[comp]['total_mp'] += r['tbm_manpower']

        print("\nBy company:")
        for comp, data in sorted(mp_by_company.items(), key=lambda x: -x[1]['total_mp']):
            print(f"  {comp}: {data['count']} records, total MP: {data['total_mp']:.0f}")

        # By count type
        by_type = {}
        for r in manpower_records:
            ct = r['count_type'] or 'Unknown'
            by_type[ct] = by_type.get(ct, 0) + 1

        print("\nBy count type:")
        for ct, count in sorted(by_type.items()):
            print(f"  {ct}: {count}")

        # Date range
        dates = [r['date'] for r in manpower_records if r['date']]
        if dates:
            print(f"\nDate range: {min(dates)} to {max(dates)}")

    if args.dry_run:
        print("\n[Dry run - no files written]")
        return

    # Write TBM location records
    print(f"\nWriting {len(records)} TBM location records to {output_file}...")

    output_file.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        'id', 'title', 'status', 'category', 'start_date',
        'tier_1', 'tier_2', 'tier_3', 'building', 'level', 'company',
        'location_id', 'activity_name', 'activity_id', 'wbs_code',
        'tbm_manpower', 'direct_manpower', 'indirect_manpower', 'total_idle_hours',
        'tag_1', 'tag_2',
        'is_active', 'is_passive', 'is_obstructed', 'is_meeting',
        'is_no_manpower', 'is_not_started', 'inspector', 'observation_date',
        'created', 'last_updated'
    ]

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    # Write Manpower Count records (daily totals)
    manpower_output_file = output_file.parent / 'manpower_counts.csv'
    print(f"Writing {len(manpower_records)} manpower count records to {manpower_output_file}...")

    manpower_fieldnames = [
        'id', 'title', 'status', 'category', 'date', 'company', 'count_type',
        'tbm_manpower', 'direct_manpower', 'indirect_manpower',
        'created', 'last_updated'
    ]

    with open(manpower_output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=manpower_fieldnames)
        writer.writeheader()
        writer.writerows(manpower_records)

    print("\nDone!")


if __name__ == '__main__':
    main()
