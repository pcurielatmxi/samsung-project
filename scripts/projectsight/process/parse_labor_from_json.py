#!/usr/bin/env python3
"""
Parse Labor Entries from ProjectSight Daily Reports JSON

Extracts structured labor data from the history/audit trail in the individual
daily report JSON files (YYYY-MM-DD.json format).

The history tab contains audit entries like:
    Added Detailed labor (Person Name) FOR Ongoing Activities (Company: Activity)
    Name
    Person Name
    Trade
    05 - Metals
    Classification
    Journeyman
    Hours
    Old: 0.00
    New: 10.00

Output:
    - labor_entries.csv: All labor entries with full audit trail

Usage:
    python scripts/projectsight/process/parse_labor_from_json.py
    python scripts/projectsight/process/parse_labor_from_json.py --input path/to/daily_reports/
"""

import argparse
import csv
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from collections import defaultdict


def parse_trade(trade_str: str) -> Tuple[str, str]:
    """
    Parse trade string into code and name.
    Example: "05 - Metals" -> ("05", "Metals")
    """
    if not trade_str:
        return (None, None)

    match = re.match(r'(\d+)\s*-\s*(.+)', trade_str)
    if match:
        return (match.group(1), match.group(2).strip())
    return (None, trade_str)


def parse_modifier(modifier_str: str) -> Dict:
    """
    Parse modifier string into components.
    Example: "John Doe (jdoe@company.com) at COMPANY NAME"
    """
    if not modifier_str:
        return {'modifier_name': None, 'modifier_email': None, 'modifier_company': None}

    match = re.match(r'([^(]+)\(([^)]+)\)\s*at\s*(.+)', modifier_str)
    if match:
        return {
            'modifier_name': match.group(1).strip(),
            'modifier_email': match.group(2).strip(),
            'modifier_company': match.group(3).strip()
        }
    return {'modifier_name': modifier_str, 'modifier_email': None, 'modifier_company': None}


def parse_labor_entry(entry_text: str, report_date: str, modifier: str = None,
                      modify_timestamp: str = None) -> Optional[Dict]:
    """
    Parse a single labor entry from history text.

    Args:
        entry_text: Raw text of the labor entry
        report_date: The report date (MM/DD/YYYY)
        modifier: Who made this change (email/name)
        modify_timestamp: When the change was made

    Returns:
        Dictionary with parsed labor data or None if parsing fails
    """
    # Extract action type
    action_match = re.match(r'(Added|Modified|Deleted) Detailed labor', entry_text)
    if not action_match:
        return None
    action = action_match.group(1)

    # Extract person name from header
    name_match = re.search(r'Detailed labor \(([^)]+)\)', entry_text)
    if not name_match:
        return None
    person_name = name_match.group(1).strip()

    # Extract company and activity
    company_match = re.search(r'FOR Ongoing Activities \(([^:]+):\s*([^)]*)\)', entry_text)
    company = company_match.group(1).strip() if company_match else None
    activity = company_match.group(2).strip() if company_match else None

    # Extract fields using line-by-line parsing
    lines = entry_text.split('\n')
    fields = {}
    current_field = None

    for i, line in enumerate(lines):
        line = line.strip()

        # Skip empty lines and header
        if not line or 'Detailed labor' in line or 'FOR Ongoing Activities' in line:
            continue

        # Check if this is a field name (capitalized word(s) followed by value on next line)
        if line in ['Name', 'Trade', 'Classification', 'Hours', 'Start', 'End', 'Break']:
            current_field = line.lower()
            continue

        # Handle Old:/New: patterns for Hours and Break
        if current_field and line.startswith('Old:'):
            fields[f'{current_field}_old'] = line.replace('Old:', '').strip()
            continue
        if current_field and line.startswith('New:'):
            fields[f'{current_field}_new'] = line.replace('New:', '').strip()
            current_field = None
            continue

        # Regular value for current field
        if current_field and current_field not in fields:
            fields[current_field] = line
            # Don't reset current_field yet - might have Old:/New: values

    # Parse trade into code and name
    trade_code, trade_name = parse_trade(fields.get('trade'))

    # Parse modifier into components
    modifier_parts = parse_modifier(modifier)

    # Parse date for derived fields
    report_dt = None
    year, month, week_number, day_of_week = None, None, None, None
    try:
        report_dt = datetime.strptime(report_date, '%m/%d/%Y')
        year = report_dt.year
        month = report_dt.month
        week_number = report_dt.isocalendar()[1]
        day_of_week = report_dt.strftime('%A')
    except (ValueError, TypeError):
        pass

    hours_new = parse_float(fields.get('hours_new', fields.get('hours', '0')))
    hours_old = parse_float(fields.get('hours_old', '0'))

    # Build result with enhanced fields
    result = {
        # Core identifiers
        'report_date': report_date,
        'year': year,
        'month': month,
        'week_number': week_number,
        'day_of_week': day_of_week,

        # Action info
        'action': action,

        # Worker info
        'person_name': person_name,
        'company': company,
        'activity': activity,

        # Trade info (split)
        'trade_code': trade_code,
        'trade_name': trade_name,
        'trade_full': fields.get('trade'),
        'classification': fields.get('classification'),

        # Hours tracking
        'hours_old': hours_old,
        'hours_new': hours_new,
        'hours_delta': hours_new - hours_old,

        # Time tracking
        'start_time': clean_time_value(fields.get('start')),
        'end_time': clean_time_value(fields.get('end')),
        'break_hours': parse_float(fields.get('break_new', fields.get('break', '0'))),

        # Derived flags
        'is_overtime': hours_new > 8.0,

        # Audit trail
        'modifier_name': modifier_parts['modifier_name'],
        'modifier_email': modifier_parts['modifier_email'],
        'modifier_company': modifier_parts['modifier_company'],
        'modify_timestamp': modify_timestamp,
    }

    return result


def clean_time_value(time_str: str) -> Optional[str]:
    """
    Clean up potentially malformed time values.
    Handles truncated AM/PM like "2:30 P" -> "2:30 PM"
    """
    if not time_str:
        return None

    time_str = time_str.strip()

    # Fix truncated AM/PM (e.g., "2:30 P" -> "2:30 PM", "9:00 A" -> "9:00 AM")
    if re.match(r'.*\d:\d{2}\s+P$', time_str):
        time_str = time_str + 'M'
    elif re.match(r'.*\d:\d{2}\s+A$', time_str):
        time_str = time_str + 'M'

    return time_str if time_str else None


def parse_float(value: str) -> float:
    """Safely parse a float value."""
    if not value:
        return 0.0
    try:
        # Remove any non-numeric characters except . and -
        cleaned = re.sub(r'[^\d.\-]', '', str(value))
        return float(cleaned) if cleaned else 0.0
    except (ValueError, TypeError):
        return 0.0


def extract_labor_from_history(history: Dict, report_date: str) -> List[Dict]:
    """
    Extract all labor entries from a report's history section.

    Args:
        history: The history dict from a report record
        report_date: The report date

    Returns:
        List of parsed labor entry dictionaries
    """
    entries = []
    raw_content = history.get('raw_content', '')

    if not raw_content:
        return entries

    # Split by "Modified by" to get change blocks with attribution
    # Pattern: content ending with "Modified by Name (email) at Company"
    blocks = re.split(r'(?=Modified by [^@]+@[^)]+\) at [^\n]+)', raw_content)

    for block in blocks:
        # Extract modifier info from this block
        modifier_match = re.search(
            r'Modified by ([^(]+)\(([^)]+)\) at ([^\n]+)',
            block
        )
        modifier = None
        modify_timestamp = None

        if modifier_match:
            modifier_name = modifier_match.group(1).strip()
            modifier_email = modifier_match.group(2).strip()
            modifier_company = modifier_match.group(3).strip()
            modifier = f"{modifier_name} ({modifier_email}) at {modifier_company}"

        # Extract timestamp (usually at the start of a change block)
        timestamp_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}\s*(?:AM|PM)?)', block)
        if timestamp_match:
            modify_timestamp = timestamp_match.group(1)

        # Find all labor entries in this block
        labor_pattern = r'((?:Added|Modified|Deleted) Detailed labor \([^)]+\) FOR Ongoing Activities \([^)]+\).*?)(?=(?:Added|Modified|Deleted) Detailed labor|Modified by|$)'

        labor_matches = re.findall(labor_pattern, block, re.DOTALL)

        for labor_text in labor_matches:
            entry = parse_labor_entry(
                labor_text,
                report_date,
                modifier,
                modify_timestamp
            )
            if entry:
                entries.append(entry)

    return entries


def load_records_from_directory(input_path: Path) -> List[Dict]:
    """
    Load records from a directory of individual JSON files.

    Args:
        input_path: Path to directory containing individual YYYY-MM-DD.json files

    Returns:
        List of record dictionaries
    """
    print(f"Loading individual report files from {input_path}/...")
    records = []
    json_files = sorted(input_path.glob('*.json'))

    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                record = json.load(f)
                records.append(record)
        except Exception as e:
            print(f"  Warning: Could not load {json_file.name}: {e}")

    print(f"  Loaded {len(records)} report files")
    return records


def process_daily_reports(input_path: Path) -> Tuple[List[Dict], Dict]:
    """
    Process daily reports and extract all labor entries.

    Args:
        input_path: Path to directory containing individual YYYY-MM-DD.json files

    Returns:
        Tuple of (labor_entries list, stats dict)
    """
    records = load_records_from_directory(input_path)

    all_entries = []
    stats = {
        'total_records': len(records),
        'records_with_labor': 0,
        'total_entries': 0,
        'entries_by_action': defaultdict(int),
        'unique_companies': set(),
        'unique_people': set(),
        'unique_trades': set(),
        'date_range': {'min': None, 'max': None}
    }

    for record in records:
        report_date = record.get('reportDate')
        history = record.get('history', {})

        entries = extract_labor_from_history(history, report_date)

        if entries:
            stats['records_with_labor'] += 1
            all_entries.extend(entries)

            for entry in entries:
                stats['entries_by_action'][entry['action']] += 1
                if entry['company']:
                    stats['unique_companies'].add(entry['company'])
                if entry['person_name']:
                    stats['unique_people'].add(entry['person_name'])
                if entry['trade_full']:
                    stats['unique_trades'].add(entry['trade_full'])

        # Track date range
        if report_date:
            try:
                dt = datetime.strptime(report_date, '%m/%d/%Y')
                if not stats['date_range']['min'] or dt < stats['date_range']['min']:
                    stats['date_range']['min'] = dt
                if not stats['date_range']['max'] or dt > stats['date_range']['max']:
                    stats['date_range']['max'] = dt
            except ValueError:
                pass

    stats['total_entries'] = len(all_entries)

    # Convert sets to counts for stats
    stats['unique_companies_count'] = len(stats['unique_companies'])
    stats['unique_people_count'] = len(stats['unique_people'])
    stats['unique_trades_count'] = len(stats['unique_trades'])

    return all_entries, stats


def write_labor_entries_csv(entries: List[Dict], output_file: Path):
    """Write labor entries to CSV file."""
    if not entries:
        print("  No entries to write!")
        return

    fieldnames = [
        # Date fields
        'report_date',
        'year',
        'month',
        'week_number',
        'day_of_week',

        # Action
        'action',

        # Worker info
        'person_name',
        'company',
        'activity',

        # Trade info
        'trade_code',
        'trade_name',
        'trade_full',
        'classification',

        # Hours
        'hours_old',
        'hours_new',
        'hours_delta',
        'is_overtime',

        # Time tracking
        'start_time',
        'end_time',
        'break_hours',

        # Audit trail
        'modifier_name',
        'modifier_email',
        'modifier_company',
        'modify_timestamp'
    ]

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(entries)

    print(f"  Wrote {len(entries)} entries to {output_file}")


def main():
    parser = argparse.ArgumentParser(description='Parse labor entries from ProjectSight daily reports JSON')
    parser.add_argument('--input', type=str, help='Input directory containing individual YYYY-MM-DD.json files')
    parser.add_argument('--output-dir', type=str, help='Output directory for CSV files')
    args = parser.parse_args()

    # Set up paths
    project_root = Path(__file__).parent.parent.parent.parent
    sys.path.insert(0, str(project_root))

    try:
        from src.config.settings import Settings
        raw_dir = Settings.PROJECTSIGHT_RAW_DIR
        processed_dir = Settings.PROJECTSIGHT_PROCESSED_DIR
    except ImportError:
        raw_dir = project_root / 'data' / 'raw' / 'projectsight'
        processed_dir = project_root / 'data' / 'processed' / 'projectsight'

    # Determine input path (directory of individual JSON files)
    if args.input:
        input_path = Path(args.input)
    else:
        input_path = raw_dir / 'extracted' / 'daily_reports'

    output_dir = Path(args.output_dir) if args.output_dir else processed_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("ProjectSight Labor Entry Parser")
    print("=" * 70)
    print(f"Input:  {input_path}")
    print(f"Output: {output_dir}")
    print()

    if not input_path.exists() or not input_path.is_dir():
        print(f"ERROR: Input directory not found: {input_path}")
        print(f"\nExpected directory: {raw_dir / 'extracted' / 'daily_reports'}/")
        return 1

    # Process the data
    entries, stats = process_daily_reports(input_path)

    # Print stats
    print()
    print("=" * 70)
    print("EXTRACTION STATISTICS")
    print("=" * 70)
    print(f"Total daily reports:     {stats['total_records']}")
    print(f"Reports with labor data: {stats['records_with_labor']}")
    print(f"Total labor entries:     {stats['total_entries']}")
    print()
    print("Entries by action:")
    for action, count in sorted(stats['entries_by_action'].items()):
        print(f"  {action:10s}: {count:,}")
    print()
    print(f"Unique companies: {stats['unique_companies_count']}")
    print(f"Unique workers:   {stats['unique_people_count']}")
    print(f"Unique trades:    {stats['unique_trades_count']}")
    print()
    if stats['date_range']['min'] and stats['date_range']['max']:
        print(f"Date range: {stats['date_range']['min'].strftime('%Y-%m-%d')} to {stats['date_range']['max'].strftime('%Y-%m-%d')}")

    # Write output file
    print()
    print("Writing output file...")
    write_labor_entries_csv(entries, output_dir / 'labor_entries.csv')

    print()
    print("Done!")
    return 0


if __name__ == '__main__':
    sys.exit(main())
