#!/usr/bin/env python3
"""
Parse Yates Daily Reports audit log from ProjectSight export.

This script extracts structured data from the audit log format:
- Labor hours by person, trade, and classification
- Report workflow status changes
- Weather data

Input: data/projectsight/extracted/Yates Daily Reports.xlsx
Output: data/projectsight/tables/*.csv
"""

import pandas as pd
import re
from pathlib import Path
from datetime import datetime


def parse_labor_entries(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract labor hour entries from the audit log.

    Pattern:
    - "Added/Modified Detailed labor FOR Ongoing Activities (COMPANY: CODE)"
    - Followed by repeating: Name, value, Trade, value, Classification, value, Hours, Old: X, New: Y
    """
    records = []
    i = 0
    n = len(df)

    current_report_num = None
    current_modifier = None
    current_timestamp = None
    current_company = None

    while i < n:
        val = str(df.iloc[i]['Done']) if pd.notna(df.iloc[i]['Done']) else ''

        # Track report number
        report_match = re.match(r'(\d+)of\d+', val)
        if report_match:
            current_report_num = int(report_match.group(1))
            i += 1
            continue

        # Track modifier and timestamp
        if val.startswith('Modified by ') or val.startswith('Created by '):
            modifier_match = re.match(r'(?:Modified|Created) by ([^(]+)', val)
            if modifier_match:
                current_modifier = modifier_match.group(1).strip()
            i += 1
            # Next line should be timestamp
            if i < n:
                ts_val = str(df.iloc[i]['Done']) if pd.notna(df.iloc[i]['Done']) else ''
                if re.match(r'\d{4}-\d{2}-\d{2}', ts_val):
                    current_timestamp = ts_val
                    i += 1
            continue

        # Detect labor entry header
        if 'Detailed labor FOR Ongoing Activities' in val:
            action = 'Added' if val.startswith('Added') else 'Modified'
            # Extract company from header
            company_match = re.search(r'\(([^:]+):', val)
            if company_match:
                current_company = company_match.group(1).strip()

            i += 1

            # Parse person entries until we hit a non-labor field
            while i < n:
                field = str(df.iloc[i]['Done']) if pd.notna(df.iloc[i]['Done']) else ''

                if field == 'Name' and i + 1 < n:
                    name = str(df.iloc[i + 1]['Done']) if pd.notna(df.iloc[i + 1]['Done']) else ''
                    trade = ''
                    classification = ''
                    old_hours = None
                    new_hours = None

                    # Look ahead for Trade, Classification, Hours
                    j = i + 2
                    while j < n and j < i + 12:  # Max lookahead
                        f = str(df.iloc[j]['Done']) if pd.notna(df.iloc[j]['Done']) else ''

                        if f == 'Trade' and j + 1 < n:
                            trade = str(df.iloc[j + 1]['Done']) if pd.notna(df.iloc[j + 1]['Done']) else ''
                            j += 2
                        elif f == 'Classification' and j + 1 < n:
                            classification = str(df.iloc[j + 1]['Done']) if pd.notna(df.iloc[j + 1]['Done']) else ''
                            j += 2
                        elif f == 'Hours':
                            j += 1
                            # Look for Old: and New: values
                            while j < n and j < i + 16:
                                hv = str(df.iloc[j]['Done']) if pd.notna(df.iloc[j]['Done']) else ''
                                if hv.startswith('Old:'):
                                    try:
                                        old_hours = float(hv.replace('Old:', '').strip())
                                    except:
                                        old_hours = 0
                                    j += 1
                                elif hv.startswith('New:'):
                                    try:
                                        new_hours = float(hv.replace('New:', '').strip())
                                    except:
                                        new_hours = 0
                                    j += 1
                                    break
                                else:
                                    j += 1
                            break
                        elif f == 'Name':
                            # Hit next person, stop looking
                            break
                        elif f in ['Modified by', 'Added'] or 'Detailed labor' in f:
                            break
                        else:
                            j += 1

                    # Record the entry
                    if name and name != 'Name':
                        records.append({
                            'report_num': current_report_num,
                            'timestamp': current_timestamp,
                            'modifier': current_modifier,
                            'action': action,
                            'company': current_company,
                            'name': name,
                            'trade': trade,
                            'classification': classification,
                            'old_hours': old_hours,
                            'new_hours': new_hours,
                            'hours_delta': (new_hours or 0) - (old_hours or 0)
                        })

                    i = j
                elif field.startswith('Modified by') or field.startswith('Added') or 'of1314' in field:
                    break
                else:
                    i += 1
        else:
            i += 1

    return pd.DataFrame(records)


def parse_workflow_status(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract report status changes.

    Pattern:
    - "Modified by NAME (email)"
    - timestamp
    - "Status"
    - "Old: X"
    - "New: Y"
    """
    records = []
    i = 0
    n = len(df)

    current_report_num = None

    while i < n:
        val = str(df.iloc[i]['Done']) if pd.notna(df.iloc[i]['Done']) else ''

        # Track report number
        report_match = re.match(r'(\d+)of\d+', val)
        if report_match:
            current_report_num = int(report_match.group(1))
            i += 1
            continue

        # Look for modifier followed by timestamp and status
        if val.startswith('Modified by '):
            modifier_match = re.match(r'Modified by ([^(]+)', val)
            modifier = modifier_match.group(1).strip() if modifier_match else ''

            # Check next entries for timestamp and status
            if i + 4 < n:
                timestamp = str(df.iloc[i + 1]['Done']) if pd.notna(df.iloc[i + 1]['Done']) else ''
                field = str(df.iloc[i + 2]['Done']) if pd.notna(df.iloc[i + 2]['Done']) else ''

                if field == 'Status' and re.match(r'\d{4}-\d{2}-\d{2}', timestamp):
                    old_status = str(df.iloc[i + 3]['Done']) if pd.notna(df.iloc[i + 3]['Done']) else ''
                    new_status = str(df.iloc[i + 4]['Done']) if pd.notna(df.iloc[i + 4]['Done']) else ''

                    if old_status.startswith('Old:') and new_status.startswith('New:'):
                        records.append({
                            'report_num': current_report_num,
                            'timestamp': timestamp,
                            'modifier': modifier,
                            'old_status': old_status.replace('Old:', '').strip(),
                            'new_status': new_status.replace('New:', '').strip()
                        })
                        i += 5
                        continue

        i += 1

    return pd.DataFrame(records)


def parse_weather(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract weather entries.

    Pattern:
    - "Added Weather (CONDITIONS TIME)"
    - Time, value
    - Temperature, value
    - Conditions, value
    - Humidity, Old/New
    - Wind, Old/New
    """
    records = []
    i = 0
    n = len(df)

    current_report_num = None
    current_timestamp = None

    while i < n:
        val = str(df.iloc[i]['Done']) if pd.notna(df.iloc[i]['Done']) else ''

        # Track report number
        report_match = re.match(r'(\d+)of\d+', val)
        if report_match:
            current_report_num = int(report_match.group(1))
            i += 1
            continue

        # Track timestamp from modifier entries
        if re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', val):
            current_timestamp = val
            i += 1
            continue

        # Detect weather entry
        if val.startswith('Added Weather'):
            weather_record = {
                'report_num': current_report_num,
                'entry_timestamp': current_timestamp,
                'time': None,
                'temperature': None,
                'conditions': None,
                'humidity': None,
                'wind': None
            }

            i += 1
            # Parse weather fields
            while i < n:
                field = str(df.iloc[i]['Done']) if pd.notna(df.iloc[i]['Done']) else ''

                if field == 'Time' and i + 1 < n:
                    weather_record['time'] = str(df.iloc[i + 1]['Done']) if pd.notna(df.iloc[i + 1]['Done']) else ''
                    i += 2
                elif field == 'Temperature' and i + 1 < n:
                    try:
                        weather_record['temperature'] = float(df.iloc[i + 1]['Done'])
                    except:
                        pass
                    i += 2
                elif field == 'Conditions' and i + 1 < n:
                    weather_record['conditions'] = str(df.iloc[i + 1]['Done']) if pd.notna(df.iloc[i + 1]['Done']) else ''
                    i += 2
                elif field == 'Humidity':
                    i += 1
                    # Look for New: value
                    while i < n:
                        hv = str(df.iloc[i]['Done']) if pd.notna(df.iloc[i]['Done']) else ''
                        if hv.startswith('New:'):
                            try:
                                weather_record['humidity'] = float(hv.replace('New:', '').strip())
                            except:
                                pass
                            i += 1
                            break
                        elif hv.startswith('Old:'):
                            i += 1
                        else:
                            break
                elif field == 'Wind':
                    i += 1
                    while i < n:
                        wv = str(df.iloc[i]['Done']) if pd.notna(df.iloc[i]['Done']) else ''
                        if wv.startswith('New:'):
                            try:
                                weather_record['wind'] = float(wv.replace('New:', '').strip())
                            except:
                                pass
                            i += 1
                            break
                        elif wv.startswith('Old:'):
                            i += 1
                        else:
                            break
                    break  # Wind is typically last
                elif field.startswith('Modified by') or field.startswith('Added') or 'of1314' in field:
                    break
                else:
                    i += 1

            records.append(weather_record)
        else:
            i += 1

    return pd.DataFrame(records)


def parse_report_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract report numbers and their associated dates.

    Pattern after report marker:
    - "Created by NAME"
    - timestamp
    - "Date"
    - report_date (YYYY-MM-DD 00:00:00)
    """
    records = []
    i = 0
    n = len(df)

    while i < n:
        val = str(df.iloc[i]['Done']) if pd.notna(df.iloc[i]['Done']) else ''

        # Find report markers
        report_match = re.match(r'(\d+)of(\d+)', val)
        if report_match:
            report_num = int(report_match.group(1))
            total_reports = int(report_match.group(2))

            # Look ahead for Created by and Date
            created_by = None
            created_at = None
            report_date = None

            j = i + 1
            while j < n and j < i + 30:
                f = str(df.iloc[j]['Done']) if pd.notna(df.iloc[j]['Done']) else ''

                if f.startswith('Created by '):
                    match = re.match(r'Created by ([^(]+)', f)
                    if match:
                        created_by = match.group(1).strip()
                    if j + 1 < n:
                        ts = str(df.iloc[j + 1]['Done']) if pd.notna(df.iloc[j + 1]['Done']) else ''
                        if re.match(r'\d{4}-\d{2}-\d{2}', ts):
                            created_at = ts
                    j += 2
                elif f == 'Date' and j + 1 < n:
                    dt = str(df.iloc[j + 1]['Done']) if pd.notna(df.iloc[j + 1]['Done']) else ''
                    if re.match(r'\d{4}-\d{2}-\d{2}', dt):
                        report_date = dt.split()[0]  # Just the date part
                        break
                    j += 2
                else:
                    j += 1

            if report_date:
                records.append({
                    'report_num': report_num,
                    'total_reports': total_reports,
                    'report_date': report_date,
                    'created_by': created_by,
                    'created_at': created_at
                })

        i += 1

    return pd.DataFrame(records)


def main():
    """Main processing function."""
    input_path = Path('data/projectsight/extracted/Yates Daily Reports.xlsx')
    output_dir = Path('data/projectsight/tables')
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Reading {input_path}...")
    xlsx = pd.ExcelFile(input_path)

    # Process all sheets
    all_labor = []
    all_workflow = []
    all_weather = []
    all_reports = []

    for sheet in xlsx.sheet_names:
        print(f"\nProcessing sheet: {sheet}")
        df = pd.read_excel(xlsx, sheet_name=sheet)

        # Skip Sheet1 (different format - Date/Detail columns)
        if 'Date' in df.columns and 'Detail' in df.columns:
            print(f"  Skipping {sheet} (Date/Detail format)")
            continue

        if 'Done' not in df.columns:
            print(f"  Skipping {sheet} (no 'Done' column)")
            continue

        print(f"  Rows: {len(df)}")

        # Parse each type
        print("  Parsing labor entries...")
        labor = parse_labor_entries(df)
        print(f"    Found {len(labor)} labor records")
        all_labor.append(labor)

        print("  Parsing workflow status...")
        workflow = parse_workflow_status(df)
        print(f"    Found {len(workflow)} status changes")
        all_workflow.append(workflow)

        print("  Parsing weather...")
        weather = parse_weather(df)
        print(f"    Found {len(weather)} weather records")
        all_weather.append(weather)

        print("  Parsing report dates...")
        reports = parse_report_dates(df)
        print(f"    Found {len(reports)} reports")
        all_reports.append(reports)

    # Combine and save
    print("\n=== Saving outputs ===")

    if all_labor:
        labor_df = pd.concat(all_labor, ignore_index=True)
        labor_df.to_csv(output_dir / 'labor_entries.csv', index=False)
        print(f"labor_entries.csv: {len(labor_df)} records")

    if all_workflow:
        workflow_df = pd.concat(all_workflow, ignore_index=True)
        workflow_df.to_csv(output_dir / 'workflow_status.csv', index=False)
        print(f"workflow_status.csv: {len(workflow_df)} records")

    if all_weather:
        weather_df = pd.concat(all_weather, ignore_index=True)
        weather_df.to_csv(output_dir / 'weather.csv', index=False)
        print(f"weather.csv: {len(weather_df)} records")

    if all_reports:
        reports_df = pd.concat(all_reports, ignore_index=True).drop_duplicates()
        reports_df.to_csv(output_dir / 'reports.csv', index=False)
        print(f"reports.csv: {len(reports_df)} records")

    # Generate summary tables
    print("\n=== Generating summary tables ===")

    if all_labor and len(labor_df) > 0:
        # Hours by trade
        hours_by_trade = labor_df.groupby('trade').agg({
            'new_hours': 'sum',
            'hours_delta': 'sum',
            'name': 'nunique'
        }).reset_index()
        hours_by_trade.columns = ['trade', 'total_hours', 'hours_delta', 'unique_people']
        hours_by_trade = hours_by_trade.sort_values('total_hours', ascending=False)
        hours_by_trade.to_csv(output_dir / 'summary_hours_by_trade.csv', index=False)
        print(f"summary_hours_by_trade.csv: {len(hours_by_trade)} trades")

        # Hours by classification (role)
        hours_by_role = labor_df.groupby('classification').agg({
            'new_hours': 'sum',
            'hours_delta': 'sum',
            'name': 'nunique'
        }).reset_index()
        hours_by_role.columns = ['classification', 'total_hours', 'hours_delta', 'unique_people']
        hours_by_role = hours_by_role.sort_values('total_hours', ascending=False)
        hours_by_role.to_csv(output_dir / 'summary_hours_by_role.csv', index=False)
        print(f"summary_hours_by_role.csv: {len(hours_by_role)} roles")

        # Personnel summary
        personnel = labor_df.groupby(['name', 'trade', 'classification']).agg({
            'new_hours': 'sum',
            'report_num': 'nunique'
        }).reset_index()
        personnel.columns = ['name', 'trade', 'classification', 'total_hours', 'reports_worked']
        personnel = personnel.sort_values('total_hours', ascending=False)
        personnel.to_csv(output_dir / 'personnel_summary.csv', index=False)
        print(f"personnel_summary.csv: {len(personnel)} people")

    print("\nDone!")


if __name__ == '__main__':
    main()
