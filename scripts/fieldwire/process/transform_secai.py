#!/usr/bin/env python3
"""
Transform SECAI Fieldwire data to match main Samsung Progress Tracking format.

SECAI uses a different structure:
- Category contains company name (Berg, MK Marlow, JKaulk) instead of work type
- Different column names: "Direct Workers" vs "Direct Manpower"
- Different Status values: "Manpower (During)" vs "TBM"
- Missing Building/Level columns

This script transforms SECAI data and prefixes IDs with "SECAI-" to avoid
collision with main file IDs when appending.

Output can be used directly with tbm_metrics_report.py or appended to main file.
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


# SECAI column indices (0-based)
SECAI_COLUMNS = {
    'id': 0,
    'title': 1,
    'status': 2,
    'category': 3,  # Contains company name in SECAI
    'assignee': 4,
    'start_date': 5,
    'end_date': 6,
    'plan': 7,
    'tier_1': 10,
    'tier_2': 11,
    'tier_3': 12,
    'tier_4': 13,
    'tier_5': 14,
    'scope': 15,
    'total_manpower': 16,
    'direct_workers': 17,  # Direct Workers in SECAI
    'indirect_workers': 18,  # Indirect Workers in SECAI
    'tbm_manpower': 19,
    'plan_folder': 20,
    'plan_link': 21,
    'created': 22,
    'completed': 23,
    'verified': 24,
    'deleted': 25,
    'last_updated': 26,
    'tag_1': 27,
    'tag_2': 28,
    'tag_3': 29,
    # Checklists for idle time (34-45)
    'checklist_1': 34,
    'checklist_2': 35,
    'checklist_3': 36,
    'checklist_4': 37,
    'checklist_5': 38,
    'checklist_6': 39,
}

# SECAI Status → Main format Status mapping
STATUS_MAPPING = {
    'Manpower (During)': 'TBM',
    'Manpower (Start)': 'TBM',  # Will be classified as Manpower Count
    'Manpower (End)': 'TBM',    # Will be classified as Manpower Count
    'Obstruction': 'TBM',
    'Verified': 'TBM',
    'Completed': 'TBM',
}

# SECAI Status values that should be classified as Manpower Count
MANPOWER_COUNT_STATUSES = {'Manpower (Start)', 'Manpower (End)'}


def clean_string(value: str) -> str:
    """Clean string value, removing quotes and extra whitespace."""
    if not value:
        return ''
    return value.strip().strip('"').strip()


def parse_numeric(value: str) -> Optional[float]:
    """Parse numeric value, handling empty strings and quotes."""
    if not value or value.strip() in ('', '""'):
        return None
    try:
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
        if re.match(r'\d{4}-\d{2}-\d{2}', clean):
            return clean
        return None
    except Exception:
        return None


def parse_checklist_value(value: str) -> tuple[bool, Optional[str], Optional[str]]:
    """
    Parse checklist field value.

    Format: "{Yes|Not set}: {Category} ({Inspector}) - {Date}"
    Example: "Yes: Active (HBA) - 2026-01-12"

    Returns: (is_set, inspector_code, date)
    """
    if not value or value.strip() == '':
        return False, None, None

    is_set = value.startswith('Yes:')

    # Extract inspector code
    inspector_match = re.search(r'\(([A-Z]{2,3})\)', value)
    inspector = inspector_match.group(1) if inspector_match else None

    # Extract date
    date_match = re.search(r'(\d{4}-\d{2}-\d{2})', value)
    date = date_match.group(1) if date_match else None

    return is_set, inspector, date


def read_secai_csv(filepath: Path) -> list[list[str]]:
    """
    Read SECAI CSV file (UTF-16LE encoded).

    Returns list of rows (skipping header rows).
    """
    rows = []

    with open(filepath, 'r', encoding='utf-16-le') as f:
        content = f.read()
        if content.startswith('\ufeff'):
            content = content[1:]

        reader = csv.reader(content.splitlines(), delimiter='\t')

        for i, row in enumerate(reader):
            # Skip first 4 rows (Generated with..., SECAI, Date, column headers)
            if i < 4:
                continue
            rows.append(row)

    return rows


def transform_secai_record(row: list[str], id_prefix: str = 'SECAI-') -> Optional[dict]:
    """
    Transform a SECAI record to match main file format.

    Returns dict with normalized fields matching main format.
    """
    if len(row) < 30:
        return None

    secai_status = clean_string(row[SECAI_COLUMNS['status']])
    company = clean_string(row[SECAI_COLUMNS['category']])  # Category = Company in SECAI

    # Skip invalid statuses
    if secai_status not in STATUS_MAPPING:
        return None

    # Skip empty companies or non-contractor values
    if not company or company in ('Manpower', 'Yates Constraint Log', ''):
        return None

    # Map status
    status = STATUS_MAPPING.get(secai_status, 'TBM')

    # Determine if this is a Manpower Count record
    is_manpower_count = secai_status in MANPOWER_COUNT_STATUSES

    # Determine count type for manpower records
    count_type = None
    if secai_status == 'Manpower (Start)':
        count_type = 'START'
    elif secai_status == 'Manpower (End)':
        count_type = 'END'

    # Parse checklist values for idle indicators
    checklist_values = []
    for i in range(1, 7):
        key = f'checklist_{i}'
        if key in SECAI_COLUMNS and len(row) > SECAI_COLUMNS[key]:
            checklist_values.append(row[SECAI_COLUMNS[key]])
        else:
            checklist_values.append('')

    # Detect obstruction from either status or checklist
    is_obstructed = secai_status == 'Obstruction'

    # Parse other checklist indicators
    active_set, active_inspector, active_date = False, None, None
    passive_set = False
    meeting_set = False
    no_manpower_set = False
    not_started_set = False

    for cv in checklist_values:
        if 'Active' in cv:
            active_set, active_inspector, active_date = parse_checklist_value(cv)
        elif 'Passive' in cv:
            passive_set, _, _ = parse_checklist_value(cv)
        elif 'Obstructed' in cv:
            is_obstructed = parse_checklist_value(cv)[0] or is_obstructed
        elif 'Meeting' in cv:
            meeting_set, _, _ = parse_checklist_value(cv)
        elif 'No Manpower' in cv:
            no_manpower_set, _, _ = parse_checklist_value(cv)
        elif 'Not Started' in cv or 'Work Has Not Started' in cv:
            not_started_set, _, _ = parse_checklist_value(cv)

    # Build record in main format
    record = {
        'id': f"{id_prefix}{clean_string(row[SECAI_COLUMNS['id']])}",
        'title': clean_string(row[SECAI_COLUMNS['title']]),
        'status': status,
        'category': 'Manpower Count' if is_manpower_count else '',  # Leave blank for work locations
        'start_date': parse_date(row[SECAI_COLUMNS['start_date']]),
        'tier_1': clean_string(row[SECAI_COLUMNS['tier_1']]),
        'tier_2': clean_string(row[SECAI_COLUMNS['tier_2']]),
        'tier_3': clean_string(row[SECAI_COLUMNS['tier_3']]),
        'building': '',  # Not available in SECAI
        'level': '',     # Not available in SECAI
        'company': company,
        'location_id': '',  # Not available in SECAI
        'activity_name': '',
        'activity_id': '',
        'wbs_code': '',
        'tbm_manpower': parse_numeric(row[SECAI_COLUMNS['tbm_manpower']]),
        'direct_manpower': parse_numeric(row[SECAI_COLUMNS['direct_workers']]),
        'indirect_manpower': parse_numeric(row[SECAI_COLUMNS['indirect_workers']]),
        'total_idle_hours': None,
        'tag_1': clean_string(row[SECAI_COLUMNS['tag_1']]),
        'tag_2': clean_string(row[SECAI_COLUMNS['tag_2']]),
        # Idle indicators
        'is_active': active_set,
        'is_passive': passive_set,
        'is_obstructed': is_obstructed,
        'is_meeting': meeting_set,
        'is_no_manpower': no_manpower_set,
        'is_not_started': not_started_set,
        'inspector': active_inspector,
        'observation_date': active_date,
        # Timestamps
        'created': clean_string(row[SECAI_COLUMNS['created']]),
        'last_updated': clean_string(row[SECAI_COLUMNS['last_updated']]),
        # Extra for manpower count records
        'count_type': count_type,
        'source': 'SECAI',  # Track source for traceability
        'original_status': secai_status,  # Preserve original status
    }

    return record


def find_latest_secai_file(input_dir: Path) -> Optional[Path]:
    """Find the most recent SECAI Fieldwire CSV dump file."""
    csv_files = list(input_dir.glob('*SECAI*.csv'))

    if not csv_files:
        return None

    # Sort by modification time, newest first
    csv_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return csv_files[0]


def main():
    parser = argparse.ArgumentParser(
        description='Transform SECAI Fieldwire data to main format'
    )
    parser.add_argument(
        '--input', '-i',
        type=Path,
        help='Input SECAI CSV file (default: latest in processed/fieldwire/)'
    )
    parser.add_argument(
        '--output', '-o',
        type=Path,
        help='Output CSV file (default: processed/fieldwire/secai_transformed.csv)'
    )
    parser.add_argument(
        '--id-prefix',
        default='SECAI-',
        help='Prefix for record IDs to avoid collision (default: SECAI-)'
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
        input_file = find_latest_secai_file(input_dir)
        if not input_file:
            print(f"Error: No SECAI CSV files found in {input_dir}")
            sys.exit(1)

    print(f"Input file: {input_file}")
    print(f"ID prefix: {args.id_prefix}")

    # Determine output file
    if args.output:
        output_file = args.output
    else:
        output_file = settings.DATA_DIR / 'processed' / 'fieldwire' / 'secai_transformed.csv'

    print(f"Output file: {output_file}")

    # Read input
    print("\nReading SECAI CSV...")
    rows = read_secai_csv(input_file)
    print(f"  Total rows: {len(rows)}")

    # Transform records
    print("\nTransforming SECAI records...")
    records = []
    manpower_count_records = []
    skipped = 0

    for row in rows:
        record = transform_secai_record(row, args.id_prefix)
        if record:
            if record['category'] == 'Manpower Count':
                manpower_count_records.append(record)
            else:
                records.append(record)
        else:
            skipped += 1

    print(f"  TBM location records: {len(records)}")
    print(f"  Manpower Count records: {len(manpower_count_records)}")
    print(f"  Skipped: {skipped}")

    # Summary by company
    companies = {}
    for r in records + manpower_count_records:
        comp = r['company'] or 'Unknown'
        companies[comp] = companies.get(comp, 0) + 1

    print("\nRecords by company:")
    for comp, count in sorted(companies.items(), key=lambda x: -x[1]):
        print(f"  {comp}: {count}")

    # Summary by original status
    statuses = {}
    for r in records + manpower_count_records:
        status = r['original_status'] or 'Unknown'
        statuses[status] = statuses.get(status, 0) + 1

    print("\nRecords by original SECAI status:")
    for status, count in sorted(statuses.items(), key=lambda x: -x[1]):
        print(f"  {status}: {count}")

    # Date range
    dates = [r['start_date'] for r in records + manpower_count_records if r['start_date']]
    if dates:
        print(f"\nDate range: {min(dates)} to {max(dates)}")

    if args.dry_run:
        print("\n[Dry run - no files written]")
        return

    # Write TBM location records (matching tbm_audits.csv format)
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
        'created', 'last_updated', 'source', 'original_status'
    ]

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(records)

    # Write Manpower Count records
    manpower_output_file = output_file.parent / 'secai_manpower_counts.csv'
    print(f"Writing {len(manpower_count_records)} manpower count records to {manpower_output_file}...")

    manpower_fieldnames = [
        'id', 'title', 'status', 'category', 'start_date', 'company', 'count_type',
        'tbm_manpower', 'direct_manpower', 'indirect_manpower',
        'created', 'last_updated', 'source', 'original_status'
    ]

    with open(manpower_output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=manpower_fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(manpower_count_records)

    print("\nDone!")
    print("\nTo append to main data, you can concatenate the output files.")
    print(f"SECAI records are prefixed with '{args.id_prefix}' to avoid ID collision.")


if __name__ == '__main__':
    main()
