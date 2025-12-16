#!/usr/bin/env python3
"""
Parse Daily Reports from ProjectSight JSON files.

Extracts structured data from individual daily report JSON files:
- Daily report summary (metadata, status, created by)
- Weather readings (temperature, conditions, humidity, wind)
- Notes/comments (work narratives, progress updates)

Output:
    - daily_reports.csv: One row per report with metadata
    - weather.csv: Weather readings (multiple per day possible)
    - notes.csv: Notes/comments with author and timestamp

Usage:
    python scripts/projectsight/process/parse_daily_reports.py
    python scripts/projectsight/process/parse_daily_reports.py --input path/to/daily_reports/
"""

import argparse
import csv
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def load_records_from_directory(input_path: Path) -> List[Tuple[str, Dict]]:
    """
    Load records from a directory of individual JSON files.

    Returns:
        List of (filename, record) tuples
    """
    print(f"Loading individual report files from {input_path}/...")
    records = []
    json_files = sorted(input_path.glob('*.json'))

    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                record = json.load(f)
                records.append((json_file.stem, record))
        except Exception as e:
            print(f"  Warning: Could not load {json_file.name}: {e}")

    print(f"  Loaded {len(records)} report files")
    return records


def parse_date(date_str: str) -> Optional[str]:
    """Convert MM/DD/YYYY to YYYY-MM-DD format."""
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, '%m/%d/%Y')
        return dt.strftime('%Y-%m-%d')
    except ValueError:
        return date_str


def extract_daily_report_summary(filename: str, data: Dict) -> Dict:
    """
    Extract daily report summary/metadata.
    """
    history = data.get('history', {})

    # Parse created_by to extract name, email, company
    created_by_raw = history.get('created_by', '')
    created_by_name = None
    created_by_email = None
    created_by_company = None

    if created_by_raw:
        # Pattern: "Created by Name (email) at Company"
        match = re.search(r'Created by ([^(]+)\(([^)]+)\)\s*at\s*(.+)', created_by_raw)
        if match:
            created_by_name = match.group(1).strip()
            created_by_email = match.group(2).strip()
            created_by_company = match.group(3).strip()

    # Parse record number "X of Y"
    record_num = data.get('recordNumber', '')
    record_index = None
    total_records = None
    if record_num and ' of ' in record_num:
        try:
            parts = record_num.split(' of ')
            record_index = int(parts[0])
            total_records = int(parts[1])
        except (ValueError, IndexError):
            pass

    # Get status from details
    details = data.get('dailyReport', {}).get('details', {})
    status = details.get('status', '')

    # If status is empty, try to find it in history changes
    if not status:
        changes = history.get('changes', [])
        for change in changes:
            if 'Status' in change and 'New:' in change:
                match = re.search(r'New:\s*(\w+)', change)
                if match:
                    status = match.group(1)
                    break

    return {
        'date': filename,  # YYYY-MM-DD from filename
        'report_date': data.get('reportDate', ''),
        'record_index': record_index,
        'total_records': total_records,
        'status': status,
        'created_by_name': created_by_name,
        'created_by_email': created_by_email,
        'created_by_company': created_by_company,
        'extracted_at': data.get('extractedAt', ''),
    }


def extract_weather_readings(filename: str, data: Dict) -> List[Dict]:
    """
    Extract weather readings from daily report.

    Weather data is in the raw_content with pattern:
    Time, Temperature, Conditions, Humidity, Wind, Gusts, By
    """
    readings = []
    raw_content = data.get('dailyReport', {}).get('raw_content', '')

    if not raw_content:
        return readings

    # Find weather section - look for pattern with time, temp, conditions
    # Pattern: "HH:MM AM/PM\nNN °F\nCondition\nNN %\nNN mph"
    lines = raw_content.split('\n')

    i = 0
    while i < len(lines):
        line = lines[i].strip()

        # Look for time pattern (start of weather reading)
        time_match = re.match(r'^(\d{1,2}:\d{2}\s*(?:AM|PM))$', line)
        if time_match:
            reading = {
                'date': filename,
                'time': time_match.group(1),
                'temperature_f': None,
                'conditions': None,
                'humidity_pct': None,
                'wind_mph': None,
                'gusts_mph': None,
                'source': None,
            }

            # Look ahead for temperature, conditions, humidity, wind
            j = i + 1
            while j < min(i + 8, len(lines)):
                next_line = lines[j].strip()

                # Temperature: "NN °F"
                temp_match = re.match(r'^(\d+)\s*°F$', next_line)
                if temp_match and reading['temperature_f'] is None:
                    reading['temperature_f'] = int(temp_match.group(1))
                    j += 1
                    continue

                # Humidity: "NN %"
                humidity_match = re.match(r'^(\d+)\s*%$', next_line)
                if humidity_match and reading['humidity_pct'] is None:
                    reading['humidity_pct'] = int(humidity_match.group(1))
                    j += 1
                    continue

                # Wind: "NN mph"
                wind_match = re.match(r'^(\d+)\s*mph$', next_line)
                if wind_match:
                    if reading['wind_mph'] is None:
                        reading['wind_mph'] = int(wind_match.group(1))
                    elif reading['gusts_mph'] is None:
                        reading['gusts_mph'] = int(wind_match.group(1))
                    j += 1
                    continue

                # Conditions (text like Clear, Cloudy, Rain, etc.)
                if reading['conditions'] is None and next_line in [
                    'Clear', 'Sunny', 'Cloudy', 'Partly Cloudy', 'Mostly Cloudy',
                    'Overcast', 'Rain', 'Light Rain', 'Heavy Rain', 'Thunderstorm',
                    'Haze', 'Fog', 'Mist', 'Drizzle', 'Fair', 'Snow', 'Scattered Clouds'
                ]:
                    reading['conditions'] = next_line
                    j += 1
                    continue

                # Source (Automatic, Manual)
                if next_line in ['Automatic', 'Manual']:
                    reading['source'] = next_line
                    j += 1
                    continue

                j += 1

            # Only add if we got at least temperature
            if reading['temperature_f'] is not None:
                readings.append(reading)

            i = j
        else:
            i += 1

    # Deduplicate readings (same time, temp, conditions)
    seen = set()
    unique_readings = []
    for r in readings:
        key = (r['date'], r['time'], r['temperature_f'], r['conditions'])
        if key not in seen:
            seen.add(key)
            unique_readings.append(r)

    return unique_readings


def extract_notes(filename: str, data: Dict) -> List[Dict]:
    """
    Extract notes/comments from daily report.

    Notes are found in:
    1. The raw_content Notes section
    2. History "Added Comment" entries
    """
    notes = []

    # Extract from raw_content - Notes section
    raw_content = data.get('dailyReport', {}).get('raw_content', '')

    # Pattern: "Author at Company\nTimestamp\nNote text\nMore"
    # Found between "Notes" section and structural elements
    if raw_content:
        # Find notes section
        notes_match = re.search(
            r'Notes\n\d+\n\d+\n(.*?)(?:Date\nCalendar|Status\n|$)',
            raw_content,
            re.DOTALL
        )
        if notes_match:
            notes_text = notes_match.group(1)

            # Parse individual notes - pattern: "Name at Company\nDate Time\nText"
            note_pattern = re.compile(
                r'([A-Z][a-z]+ [A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+at\s+([^\n]+)\n'
                r'([A-Z][a-z]{2}\s+\d{1,2},\s+\d{4}\s+\d{1,2}:\d{2}\s*(?:AM|PM)?)\n'
                r'(.*?)(?=\n[A-Z][a-z]+ [A-Z][a-z]+\s+at\s+|More\n|File\n|$)',
                re.DOTALL
            )

            for match in note_pattern.finditer(notes_text):
                author = match.group(1).strip()
                company = match.group(2).strip()
                timestamp = match.group(3).strip()
                text = match.group(4).strip()

                # Clean up text - remove "More" and file references
                text = re.sub(r'\nMore$', '', text)
                text = re.sub(r'\n+', ' ', text)
                text = text.strip()

                if text and len(text) > 10:
                    # Detect shift from text
                    shift = None
                    if 'Night' in text[:50] or 'night' in text[:50]:
                        shift = 'Night'
                    elif 'Day' in text[:50]:
                        shift = 'Day'

                    notes.append({
                        'date': filename,
                        'author': author,
                        'company': company,
                        'timestamp': timestamp,
                        'shift': shift,
                        'text': text[:2000],  # Limit length
                    })

    # Also extract from history "Added Comment" entries
    history = data.get('history', {})
    changes = history.get('changes', [])

    for change in changes:
        if 'Added Comment' in change:
            # Extract comment text
            comment_match = re.search(r'Comment\n(.+?)(?:\nAdded|\nInclude|\nModified|$)', change, re.DOTALL)
            if comment_match:
                text = comment_match.group(1).strip()
                text = re.sub(r'\n+', ' ', text)

                # Extract modifier info
                modifier_match = re.search(
                    r'Modified by ([^(]+)\(([^)]+)\)\s*at\s*([^\n]+)',
                    change
                )
                author = None
                company = None
                if modifier_match:
                    author = modifier_match.group(1).strip()
                    company = modifier_match.group(3).strip()

                # Extract timestamp
                ts_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}\s*(?:AM|PM)?)', change)
                timestamp = ts_match.group(1) if ts_match else None

                if text and len(text) > 10:
                    # Check if this note is already captured
                    is_duplicate = any(
                        n['text'][:100] == text[:100]
                        for n in notes
                    )

                    if not is_duplicate:
                        shift = None
                        if 'Night' in text[:50] or 'night' in text[:50]:
                            shift = 'Night'
                        elif 'Day' in text[:50]:
                            shift = 'Day'

                        notes.append({
                            'date': filename,
                            'author': author,
                            'company': company,
                            'timestamp': timestamp,
                            'shift': shift,
                            'text': text[:2000],
                        })

    return notes


def write_csv(rows: List[Dict], output_file: Path, fieldnames: List[str]):
    """Write rows to CSV file."""
    if not rows:
        print(f"  {output_file.name}: 0 rows (skipped)")
        return

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  {output_file.name}: {len(rows)} rows")


def main():
    parser = argparse.ArgumentParser(description='Parse daily reports from ProjectSight JSON files')
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

    # Determine input path
    if args.input:
        input_path = Path(args.input)
    else:
        input_path = raw_dir / 'extracted' / 'daily_reports'

    output_dir = Path(args.output_dir) if args.output_dir else processed_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("ProjectSight Daily Reports Parser")
    print("=" * 70)
    print(f"Input:  {input_path}")
    print(f"Output: {output_dir}")
    print()

    if not input_path.exists() or not input_path.is_dir():
        print(f"ERROR: Input directory not found: {input_path}")
        return 1

    # Load all records
    records = load_records_from_directory(input_path)

    # Extract data
    print("\nExtracting data...")

    daily_reports = []
    weather_readings = []
    notes = []

    for filename, data in records:
        # Daily report summary
        daily_reports.append(extract_daily_report_summary(filename, data))

        # Weather readings
        weather_readings.extend(extract_weather_readings(filename, data))

        # Notes
        notes.extend(extract_notes(filename, data))

    # Print stats
    print()
    print("=" * 70)
    print("EXTRACTION STATISTICS")
    print("=" * 70)
    print(f"Total daily reports:  {len(daily_reports)}")
    print(f"Weather readings:     {len(weather_readings)}")
    print(f"Notes/comments:       {len(notes)}")

    # Weather stats
    reports_with_weather = len(set(w['date'] for w in weather_readings))
    print(f"\nReports with weather: {reports_with_weather} ({reports_with_weather/len(daily_reports)*100:.0f}%)")

    # Notes stats
    reports_with_notes = len(set(n['date'] for n in notes))
    print(f"Reports with notes:   {reports_with_notes} ({reports_with_notes/len(daily_reports)*100:.0f}%)")

    # Write output files
    print("\nWriting output files...")

    write_csv(daily_reports, output_dir / 'daily_reports.csv', [
        'date', 'report_date', 'record_index', 'total_records', 'status',
        'created_by_name', 'created_by_email', 'created_by_company', 'extracted_at'
    ])

    write_csv(weather_readings, output_dir / 'weather.csv', [
        'date', 'time', 'temperature_f', 'conditions', 'humidity_pct',
        'wind_mph', 'gusts_mph', 'source'
    ])

    write_csv(notes, output_dir / 'notes.csv', [
        'date', 'author', 'company', 'timestamp', 'shift', 'text'
    ])

    print()
    print("Done!")
    return 0


if __name__ == '__main__':
    sys.exit(main())
