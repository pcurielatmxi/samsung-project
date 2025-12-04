#!/usr/bin/env python3
"""
Transform Daily Reports JSON data into normalized CSV tables.

Reads:
  - data/projectsight/extracted/daily_reports_415.json (summary/grid data)
  - data/projectsight/extracted/daily-report-details-history.json (audit trail)

Outputs:
  - data/projectsight/tables/daily_reports.csv
  - data/projectsight/tables/companies.csv
  - data/projectsight/tables/contacts.csv
  - data/projectsight/tables/daily_report_contributors.csv
  - data/projectsight/tables/daily_report_history.csv
  - data/projectsight/tables/daily_report_changes.csv

Usage:
  python scripts/daily_reports_to_csv.py
"""

import json
import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional


def parse_iso_date(date_str: str) -> Optional[str]:
    """Parse ISO date string to YYYY-MM-DD format."""
    if not date_str:
        return None
    try:
        # Handle ISO format: 2022-09-12T00:00:00Z
        if 'T' in date_str:
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return dt.strftime('%Y-%m-%d')
        return date_str
    except (ValueError, TypeError):
        return date_str


def parse_us_date(date_str: str) -> Optional[str]:
    """Parse US date string (M/D/YYYY) to YYYY-MM-DD format."""
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, '%m/%d/%Y')
        return dt.strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        # Try alternate format
        try:
            dt = datetime.strptime(date_str, '%m/%d/%Y %I:%M %p')
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
            return date_str


def parse_timestamp(ts_str: str) -> Optional[str]:
    """Parse various timestamp formats to ISO format."""
    if not ts_str:
        return None
    try:
        # Handle .NET format: 2024-10-17T15:16:57.9360325-05:00
        if 'T' in ts_str and len(ts_str) > 25:
            # Truncate microseconds to 6 digits
            parts = ts_str.split('.')
            if len(parts) == 2:
                micro_and_tz = parts[1]
                # Find timezone offset
                for sep in ['+', '-']:
                    if sep in micro_and_tz[1:]:  # Skip first char (could be negative)
                        idx = micro_and_tz.rfind(sep)
                        micro = micro_and_tz[:idx][:6]
                        tz = micro_and_tz[idx:]
                        ts_str = f"{parts[0]}.{micro}{tz}"
                        break
            dt = datetime.fromisoformat(ts_str)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        elif 'T' in ts_str:
            dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        return ts_str
    except (ValueError, TypeError):
        return ts_str


def extract_companies_and_contacts(
    summary_records: List[Dict],
    history_records: List[Dict]
) -> Tuple[Dict[str, int], Dict[str, Dict]]:
    """
    Extract unique companies and contacts from all data sources.

    Returns:
        companies: {name: id}
        contacts: {email: {id, name, company_id}}
    """
    companies: Dict[str, int] = {}
    contacts: Dict[str, Dict] = {}

    company_id = 1
    contact_id = 1

    # Extract from history data (has structured creator info)
    for record in history_records:
        history = record.get('history', {})

        company_name = history.get('createdByCompany')
        if company_name and company_name not in companies:
            companies[company_name] = company_id
            company_id += 1

        email = history.get('createdByEmail')
        name = history.get('createdByName')
        if email and email not in contacts:
            contacts[email] = {
                'id': contact_id,
                'name': name,
                'company_id': companies.get(company_name)
            }
            contact_id += 1

    # Extract from summary data (contributing companies/contacts)
    for record in summary_records:
        # Contributing companies
        company_names_str = record.get('contributingCompanyNames')
        if company_names_str:
            for company_name in company_names_str.split('\n'):
                company_name = company_name.strip()
                if company_name and company_name not in companies:
                    companies[company_name] = company_id
                    company_id += 1

        # Contributing contacts (names only, no email)
        contact_names_str = record.get('contributingContactNames')
        if contact_names_str:
            for contact_name in contact_names_str.split('\n'):
                contact_name = contact_name.strip()
                if contact_name:
                    # Use name as key since we don't have email
                    pseudo_email = f"name:{contact_name}"
                    if pseudo_email not in contacts:
                        contacts[pseudo_email] = {
                            'id': contact_id,
                            'name': contact_name,
                            'company_id': None
                        }
                        contact_id += 1

        # Last modified by (email only)
        modifier_email = record.get('lastModifiedBy')
        if modifier_email and modifier_email not in contacts:
            contacts[modifier_email] = {
                'id': contact_id,
                'name': None,
                'company_id': None
            }
            contact_id += 1

    return companies, contacts


def build_report_id_mapping(
    summary_records: List[Dict],
    history_records: List[Dict]
) -> Dict[str, int]:
    """
    Build mapping from report date to dailyReportId.
    History data uses recordNumber (1-415) but we need dailyReportId.
    """
    date_to_id = {}
    for record in summary_records:
        report_date = parse_iso_date(record.get('date'))
        report_id = record.get('dailyReportId')
        if report_date and report_id:
            date_to_id[report_date] = report_id
    return date_to_id


def transform_daily_reports(summary_records: List[Dict]) -> List[Dict]:
    """Transform summary records to daily_reports table format."""
    rows = []
    for record in summary_records:
        rows.append({
            'id': record.get('dailyReportId'),
            'guid': record.get('guid'),
            'project_id': record.get('projectId'),
            'report_date': parse_iso_date(record.get('date')),
            'status': record.get('status'),
            'weather': record.get('weather'),
            'weather_temp': record.get('weatherTemp'),
            'weather_uom': record.get('weatherUom'),
            'total_rainfall': record.get('totalRainfall'),
            'total_snowfall': record.get('totalSnowfall'),
            'lost_productivity': record.get('lostProductivity'),
            'total_work_approved': record.get('totalWorkApproved'),
            'total_workers_approved': record.get('totalWorkersApproved'),
            'total_equipment_records': record.get('totalEquipmentRecords'),
            'total_equipment_qty': record.get('totalEquipmentQty'),
            'total_equipment_hours': record.get('totalEquipmentHours'),
            'total_labor_file_links': record.get('totalLaborFileLinks'),
            'total_comments': record.get('totalComments'),
            'total_links': record.get('totalLinks'),
            'total_assignments_count': record.get('totalAssignmentsCount'),
            'open_assignments_count': record.get('openAssignmentsCount'),
            'is_locked': record.get('isLocked'),
            'last_modified_by': record.get('lastModifiedBy'),
            'last_modified_at': parse_timestamp(record.get('lastModified')),
        })
    return rows


def transform_contributors(
    summary_records: List[Dict],
    contacts: Dict[str, Dict]
) -> List[Dict]:
    """Extract contributor relationships from summary data."""
    rows = []
    contributor_id = 1

    for record in summary_records:
        report_id = record.get('dailyReportId')

        # Contributing contacts
        contact_names_str = record.get('contributingContactNames')
        if contact_names_str:
            for contact_name in contact_names_str.split('\n'):
                contact_name = contact_name.strip()
                if contact_name:
                    pseudo_email = f"name:{contact_name}"
                    contact_info = contacts.get(pseudo_email)
                    if contact_info:
                        rows.append({
                            'id': contributor_id,
                            'daily_report_id': report_id,
                            'contact_id': contact_info['id'],
                            'contribution_type': 'contributor'
                        })
                        contributor_id += 1

        # Open assignment contacts
        assignment_names_str = record.get('openAssignmentsContactNames')
        if assignment_names_str:
            for contact_name in assignment_names_str.split(','):
                contact_name = contact_name.strip()
                if contact_name:
                    pseudo_email = f"name:{contact_name}"
                    contact_info = contacts.get(pseudo_email)
                    if contact_info:
                        rows.append({
                            'id': contributor_id,
                            'daily_report_id': report_id,
                            'contact_id': contact_info['id'],
                            'contribution_type': 'assignment'
                        })
                        contributor_id += 1

    return rows


def transform_history(
    history_records: List[Dict],
    date_to_id: Dict[str, int]
) -> Tuple[List[Dict], List[Dict]]:
    """
    Transform history records to history and changes tables.

    Returns:
        history_rows: List of history records
        changes_rows: List of field change records
    """
    history_rows = []
    changes_rows = []

    history_id = 1
    change_id = 1

    for record in history_records:
        # Map reportDate to dailyReportId
        report_date_raw = record.get('reportDate')
        report_date = parse_us_date(report_date_raw)
        daily_report_id = date_to_id.get(report_date)

        if not daily_report_id:
            # Try to find by matching date parts
            for db_date, db_id in date_to_id.items():
                if report_date_raw and db_date:
                    # Compare just the date part
                    if report_date == db_date:
                        daily_report_id = db_id
                        break

        history = record.get('history', {})

        # History record
        history_rows.append({
            'id': history_id,
            'daily_report_id': daily_report_id,
            'record_number': record.get('recordNumber'),
            'report_date': report_date,
            'created_by_email': history.get('createdByEmail'),
            'created_by_name': history.get('createdByName'),
            'created_by_company': history.get('createdByCompany'),
            'created_at': parse_us_date(history.get('createdAt')),
            'extracted_at': parse_timestamp(record.get('extractedAt')),
        })

        # Field changes
        changes = history.get('changes', [])
        for change in changes:
            changes_rows.append({
                'id': change_id,
                'history_id': history_id,
                'field_name': change.get('field'),
                'old_value': change.get('oldValue'),
                'new_value': change.get('newValue') or change.get('value'),
            })
            change_id += 1

        history_id += 1

    return history_rows, changes_rows


def write_csv(rows: List[Dict], filepath: Path, fieldnames: List[str] = None):
    """Write rows to CSV file."""
    if not rows:
        print(f"  Skipping {filepath.name} - no data")
        return

    if fieldnames is None:
        fieldnames = list(rows[0].keys())

    filepath.parent.mkdir(parents=True, exist_ok=True)

    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  {filepath.name}: {len(rows)} rows")


def main():
    """Main entry point."""
    print("=" * 60)
    print("Daily Reports JSON to CSV Transformation")
    print("=" * 60)

    # Paths
    base_dir = Path('/workspaces/mxi-samsung')
    summary_path = base_dir / 'data/projectsight/extracted/daily_reports_415.json'
    history_path = base_dir / 'data/projectsight/extracted/daily-report-details-history.json'
    output_dir = base_dir / 'data/projectsight/tables'

    # Load data
    print("\nLoading source data...")

    with open(summary_path, 'r', encoding='utf-8') as f:
        summary_data = json.load(f)
    summary_records = summary_data.get('records', [])
    print(f"  Summary data: {len(summary_records)} records")

    with open(history_path, 'r', encoding='utf-8') as f:
        history_data = json.load(f)
    history_records = history_data.get('records', [])
    print(f"  History data: {len(history_records)} records")

    # Build mappings
    print("\nBuilding reference data...")
    date_to_id = build_report_id_mapping(summary_records, history_records)
    print(f"  Date-to-ID mapping: {len(date_to_id)} entries")

    companies, contacts = extract_companies_and_contacts(summary_records, history_records)
    print(f"  Companies: {len(companies)}")
    print(f"  Contacts: {len(contacts)}")

    # Transform data
    print("\nTransforming data...")

    # 1. Daily Reports (main table)
    daily_reports = transform_daily_reports(summary_records)

    # 2. Companies
    companies_rows = [
        {'id': cid, 'name': name}
        for name, cid in sorted(companies.items(), key=lambda x: x[1])
    ]

    # 3. Contacts
    contacts_rows = [
        {
            'id': info['id'],
            'email': email if not email.startswith('name:') else None,
            'name': info['name'],
            'company_id': info['company_id']
        }
        for email, info in sorted(contacts.items(), key=lambda x: x[1]['id'])
    ]

    # 4. Contributors
    contributors = transform_contributors(summary_records, contacts)

    # 5. History and Changes
    history_rows, changes_rows = transform_history(history_records, date_to_id)

    # Write CSVs
    print("\nWriting CSV files...")

    write_csv(daily_reports, output_dir / 'daily_reports.csv', [
        'id', 'guid', 'project_id', 'report_date', 'status',
        'weather', 'weather_temp', 'weather_uom',
        'total_rainfall', 'total_snowfall', 'lost_productivity',
        'total_work_approved', 'total_workers_approved',
        'total_equipment_records', 'total_equipment_qty', 'total_equipment_hours',
        'total_labor_file_links', 'total_comments', 'total_links',
        'total_assignments_count', 'open_assignments_count',
        'is_locked', 'last_modified_by', 'last_modified_at'
    ])

    write_csv(companies_rows, output_dir / 'companies.csv', [
        'id', 'name'
    ])

    write_csv(contacts_rows, output_dir / 'contacts.csv', [
        'id', 'email', 'name', 'company_id'
    ])

    write_csv(contributors, output_dir / 'daily_report_contributors.csv', [
        'id', 'daily_report_id', 'contact_id', 'contribution_type'
    ])

    write_csv(history_rows, output_dir / 'daily_report_history.csv', [
        'id', 'daily_report_id', 'record_number', 'report_date',
        'created_by_email', 'created_by_name', 'created_by_company',
        'created_at', 'extracted_at'
    ])

    write_csv(changes_rows, output_dir / 'daily_report_changes.csv', [
        'id', 'history_id', 'field_name', 'old_value', 'new_value'
    ])

    # Summary
    print("\n" + "=" * 60)
    print("Transformation Complete!")
    print("=" * 60)
    print(f"\nOutput directory: {output_dir}")
    print("\nTable summary:")
    print(f"  daily_reports.csv:             {len(daily_reports)} rows")
    print(f"  companies.csv:                 {len(companies_rows)} rows")
    print(f"  contacts.csv:                  {len(contacts_rows)} rows")
    print(f"  daily_report_contributors.csv: {len(contributors)} rows")
    print(f"  daily_report_history.csv:      {len(history_rows)} rows")
    print(f"  daily_report_changes.csv:      {len(changes_rows)} rows")


if __name__ == '__main__':
    main()
