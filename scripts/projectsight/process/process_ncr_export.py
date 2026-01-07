#!/usr/bin/env python3
"""
Process ProjectSight NCR/QOR/SOR/SWN/VR Excel Export to CSV

Converts native Excel export to structured CSV with:
- Extracted company names from ID prefixes
- Parsed record types from ID structure
- Normalized multiline fields
- Discipline and location codes
- Data quality flags for missing fields

Input:  {WINDOWS_DATA_DIR}/raw/projectsight/ncr/T-PJT _ FAB1 _ Construction - NCR_QOR_SOR_SWN_VR.xls
Output: {WINDOWS_DATA_DIR}/processed/projectsight/ncr.csv

Usage:
    python scripts/projectsight/process/process_ncr_export.py

    python scripts/projectsight/process/process_ncr_export.py \
      --input /custom/path/export.xls \
      --output /custom/path/output.csv
"""

import os
import sys
import argparse
import csv
import re
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

# Add parent directories to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

load_dotenv(override=True)


def excel_date_to_string(excel_date):
    """Convert Excel serial date to YYYY-MM-DD format."""
    if not excel_date or excel_date == "":
        return ""
    try:
        float_date = float(excel_date)
        base_date = datetime(1900, 1, 1)
        result_date = base_date + timedelta(days=float_date - 1)
        return result_date.strftime("%Y-%m-%d")
    except:
        return str(excel_date)


def clean_multiline(text):
    """Replace newlines with spaces and normalize whitespace."""
    if not text:
        return ""
    text_str = str(text)
    # Replace newlines and multiple spaces with single space
    text_str = re.sub(r'\s+', ' ', text_str)
    return text_str.strip()


def extract_id_components(description):
    """
    Parse NCR ID structure: {COMPANY}[-or_]{TYPE}[-or_]{SYSTEM/LOCATION...}[-or_]{NUMBER}

    Handles multiple formats:
    - Standard: SECAI-NCR-A-G-0048
    - HP format: HP-NCR-S-COLS-RBAR-G-0015
    - Yates format: YATES-NCR-L-SITE-CURB-FAB-0047
    - Underscore: SECAI_NCR_M_O_0066
    - Embedded modifiers: SECAI-W&W-NCR-S-CONC-...-0113
    - Alternative companies: IQA-SOR-CUB-MECH-0422, ITC-SOR-FAB-0411, HVAC-SOR-CUB-0407
    - No company: NCR-A-G-0051 (plain NCR with no company)

    Returns: (company_code, record_type, full_system_code)
    """
    if not description:
        return None, None, None

    desc_str = str(description).strip()

    # Try pattern 1: COMPANY-[MODIFIER-]TYPE-SYSTEM-NUMBER (with hyphens)
    # Handles: SECAI-W&W-NCR-S-CONC-..., SECAI-NCR-A-G-0048
    pattern1 = r'^([A-Z][A-Z0-9]*?)(?:-[A-Z&]+)?-([A-Z]{2,3})-(.+?)-(\d+)(?:\s|$|-)'
    match = re.match(pattern1, desc_str)
    if match:
        company_code = match.group(1)
        record_type = match.group(2)
        system_code = match.group(3)
        return company_code, record_type, system_code

    # Try pattern 2: COMPANY_TYPE_SYSTEM_NUMBER (with underscores)
    # Handles: SECAI_NCR_M_O_0066, NMS_QOR_D_DRAN_IW_SUE_0003
    pattern2 = r'^([A-Z][A-Z0-9]*?)_([A-Z]{2,3})_(.+?)_(\d+)(?:\s|$|_)'
    match = re.match(pattern2, desc_str)
    if match:
        company_code = match.group(1)
        record_type = match.group(2)
        system_code = match.group(3)
        return company_code, record_type, system_code

    # Try pattern 3: TYPE-SYSTEM-NUMBER with no company (NCR-A-G-0051)
    # This captures pure NCR/QOR/SOR/SWN/VR records with no company prefix
    pattern3 = r'^([A-Z]{2,3})-([A-Z]|[A-Z0-9]+?)-(\d+)(?:\s|$|-)'
    match = re.match(pattern3, desc_str)
    if match:
        record_type = match.group(1)
        system_code = match.group(2)
        # Return None for company to signal it's missing
        return None, record_type, system_code

    return None, None, None


def company_code_to_name(code):
    """Map company codes to names."""
    if not code:
        return ""

    code_upper = code.upper()

    # Comprehensive mapping of all known company codes
    company_map = {
        # Primary contractors
        'SECAI': 'SECAI',
        'SEAC': 'SECAI (Variant)',
        'SECA': 'SECAI (Variant)',
        'YATES': 'Yates',
        'HP': 'HP',

        # Austin-based contractors
        'AG': 'Austin Global',
        'ABR': 'Austin Bridge',
        'AUSTIN': 'Austin Global',
        'AUSTINGLOBAL': 'Austin Global',

        # Equipment/Infrastructure specialists
        'IQA': 'IQA',
        'ITC': 'ITC',
        'HVAC': 'HVAC',
        'FS': 'FS',

        # Other contractors
        'NMS': 'NMS',
        'NORTHSTAR': 'Northstar',
        'CLAYCO': 'Clayco',
        'SAS': 'SAS',
        'NOMURA': 'Nomura',
        'EMD': 'EMD',
        'JACOBS': 'Jacobs',
        'LEHNE': 'Lehne Construction',

        # Additional identified companies
        'BRAZOS': 'Brazos',
        'JMEG': 'JMEG',
        'LINDE': 'Linde',
        'SK': 'SK',
        'IQA': 'IQA',
        'GBI': 'GBI',

        # Infrastructure/facility codes
        'WTR': 'Water Systems',
        'DW': 'DW',
        'GCS': 'GCS',
        'CSF': 'CSF',
        'FAB': 'Fab',
        'CUB': 'CUB',
        'RMF': 'RMF',
    }

    return company_map.get(code_upper, code)


def extract_type_from_code(record_type_code):
    """Map record type code (NCR, QOR, SOR, SWN, VR) to standard type."""
    if not record_type_code:
        return "NCR"

    rt_upper = str(record_type_code).upper()

    type_map = {
        'NCR': 'NCR',
        'QOR': 'QOR',
        'SOR': 'SOR',
        'SWN': 'SWN',
        'VR': 'VR',
    }

    return type_map.get(rt_upper, "NCR")


def extract_discipline_from_system(system_code):
    """
    Extract discipline from system code (first component).
    Examples: "A-G" -> "A", "S-COLS-RBAR-G" -> "S", "L-SITE-CURB-FAB" -> "L"
    """
    if not system_code:
        return ""

    # First component before first hyphen
    components = str(system_code).split('-')
    if components:
        return components[0]
    return ""


def discipline_code_to_name(code):
    """Map discipline codes to names."""
    if not code:
        return ""

    discipline_map = {
        'A': 'Architecture',
        'S': 'Structural',
        'E': 'Electrical',
        'M': 'Mechanical',
        'P': 'Plumbing/MEP',
        'L': 'Landscape/Site',
        'C': 'Civil',
        'G': 'General',
    }

    return discipline_map.get(code.upper(), code)


def has_data_quality_issues(company, date_resolved, cause, description, found_id_pattern):
    """
    Identify data quality flags.

    Args:
        company: Extracted company name
        date_resolved: Resolution date
        cause: Cause of issue text
        description: Full description
        found_id_pattern: Boolean - True if an ID pattern (NCR-X-X, etc.) was found
    """
    flags = []

    # Only flag missing_company if we found an ID pattern but no company was extracted
    # This excludes plain English descriptions that genuinely have no company data
    if (not company or company == "") and found_id_pattern:
        # If no company extracted despite finding an ID pattern, it's a data issue worth noting
        # (e.g., NCR-A-G-0051 or IQA-SOR-CUB with unextracted company code)
        flags.append("missing_company")

    if not date_resolved or date_resolved == "":
        flags.append("unresolved")

    if not cause or cause == "":
        flags.append("missing_cause")

    if not description or description == "":
        flags.append("missing_description")

    return ";".join(flags) if flags else ""


def process_excel_to_csv(excel_file, csv_output):
    """Process Excel export to CSV with extracted fields."""
    import xlrd

    print(f"Reading: {excel_file}")

    # Load workbook
    wb = xlrd.open_workbook(excel_file, formatting_info=True)
    ws = wb.sheet_by_index(0)

    # Extract records
    records = []
    row_idx = 17  # Data starts at row 17 (after headers at row 14)

    while row_idx < ws.nrows:
        try:
            number = ws.cell_value(row_idx, 0)

            # Skip invalid rows
            if not number or number == "" or "Notice to Comply" in str(number):
                row_idx += 1
                continue

            # Verify it's a numeric record
            try:
                num_int = int(float(str(number).replace(' ', '')))
            except:
                row_idx += 1
                continue

            # Extract fields from Excel
            status_type = ws.cell_value(row_idx, 1)
            status_val = ws.cell_value(row_idx, 2)
            description = ws.cell_value(row_idx, 4)
            created_on = ws.cell_value(row_idx, 6)
            cause = ws.cell_value(row_idx, 8)
            date_resolved = ws.cell_value(row_idx, 10)
            resolution = ws.cell_value(row_idx, 11)

            # Only process valid status values
            if status_val not in ['Closed', 'Open', 'Placeholder', 'Reject', 'VOID']:
                row_idx += 1
                continue

            # Extract ID components
            id_company, id_type, id_system = extract_id_components(description)

            # Determine company name
            company_name = company_code_to_name(id_company) if id_company else ""

            # Determine record type
            record_type = extract_type_from_code(id_type) if id_type else "NCR"

            # Extract discipline from system code
            discipline_code = extract_discipline_from_system(id_system)
            discipline_name = discipline_code_to_name(discipline_code)

            # Clean multiline fields
            cause_clean = clean_multiline(cause)
            resolution_clean = clean_multiline(resolution)
            description_clean = str(description).strip() if description else ""

            # Data quality flags
            # found_id_pattern is True if we extracted an ID type (meaning ID structure was found)
            found_id_pattern = id_type is not None
            quality_flags = has_data_quality_issues(
                company_name,
                date_resolved,
                cause_clean,
                description_clean,
                found_id_pattern  # True if we found an ID pattern, False for plain English
            )

            records.append({
                'number': str(number).zfill(4),
                'type': record_type,
                'status': status_val,
                'company': company_name,
                'discipline': discipline_name,
                'description': description_clean,
                'created_on': excel_date_to_string(created_on),
                'cause_of_issue': cause_clean,
                'date_resolved': excel_date_to_string(date_resolved),
                'resolution': resolution_clean,
                'data_quality_flags': quality_flags
            })

            row_idx += 1
        except Exception as e:
            row_idx += 1

    # Create output directory
    output_path = Path(csv_output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Write CSV
    print(f"Writing: {csv_output}")
    with open(csv_output, 'w', newline='', encoding='utf-8') as f:
        fieldnames = [
            'number', 'type', 'status', 'company', 'discipline',
            'description', 'created_on', 'cause_of_issue', 'date_resolved',
            'resolution', 'data_quality_flags'
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        writer.writeheader()
        for record in records:
            writer.writerow(record)

    return records


def print_summary(records):
    """Print processing summary statistics."""
    from collections import Counter

    print(f"\n=== PROCESSING SUMMARY ===\n")
    print(f"Total Records: {len(records)}")

    # Type distribution
    type_dist = Counter(r['type'] for r in records)
    print(f"\nRecord Types:")
    for rtype in sorted(type_dist.keys()):
        count = type_dist[rtype]
        pct = (count / len(records)) * 100
        print(f"  {rtype:6s}: {count:4d} ({pct:5.1f}%)")

    # Status distribution
    status_dist = Counter(r['status'] for r in records)
    print(f"\nStatus:")
    for status in sorted(status_dist.keys(), key=lambda x: -status_dist[x]):
        count = status_dist[status]
        pct = (count / len(records)) * 100
        print(f"  {status:12s}: {count:4d} ({pct:5.1f}%)")

    # Company distribution
    company_dist = Counter(r['company'] for r in records)
    companies_with_data = {k: v for k, v in company_dist.items() if k}
    print(f"\nCompanies:")
    if companies_with_data:
        for comp in sorted(companies_with_data.keys(), key=lambda x: -companies_with_data[x]):
            count = companies_with_data[comp]
            pct = (count / len(records)) * 100
            print(f"  {comp:20s}: {count:4d} ({pct:5.1f}%)")
    else:
        print("  (none extracted)")

    # Discipline distribution
    disc_dist = Counter(r['discipline'] for r in records)
    discs_with_data = {k: v for k, v in disc_dist.items() if k}
    print(f"\nDisciplines:")
    if discs_with_data:
        for disc in sorted(discs_with_data.keys(), key=lambda x: -discs_with_data[x]):
            count = discs_with_data[disc]
            print(f"  {disc:20s}: {count:4d}")
    else:
        print("  (none extracted)")

    # Data quality flags
    flag_dist = Counter()
    for r in records:
        if r['data_quality_flags']:
            for flag in r['data_quality_flags'].split(';'):
                flag_dist[flag] += 1

    print(f"\nData Quality Issues:")
    if flag_dist:
        for flag in sorted(flag_dist.keys(), key=lambda x: -flag_dist[x]):
            count = flag_dist[flag]
            pct = (count / len(records)) * 100
            print(f"  {flag:25s}: {count:4d} ({pct:5.1f}%)")
    else:
        print("  None detected")

    clean_records = len(records) - sum(flag_dist.values())
    print(f"\nRecords with NO quality issues: {clean_records} ({(clean_records/len(records)*100):.1f}%)")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Process ProjectSight NCR Excel export to CSV'
    )
    parser.add_argument(
        '--input',
        help='Input Excel file path (defaults to WINDOWS_DATA_DIR/raw/projectsight/ncr/...)'
    )
    parser.add_argument(
        '--output',
        help='Output CSV file path (defaults to WINDOWS_DATA_DIR/processed/projectsight/ncr.csv)'
    )

    args = parser.parse_args()

    # Determine input path
    if args.input:
        excel_file = args.input
    else:
        windows_data_dir = os.getenv('WINDOWS_DATA_DIR', '')
        if windows_data_dir:
            base_dir = Path(windows_data_dir.replace('\\', '/'))
            if not base_dir.exists() and windows_data_dir.startswith('C:'):
                base_dir = Path('/mnt/c' + windows_data_dir[2:].replace('\\', '/'))
        else:
            base_dir = Path(__file__).resolve().parent.parent.parent.parent / 'data'

        excel_file = base_dir / 'raw' / 'projectsight' / 'ncr' / 'T-PJT _ FAB1 _ Construction - NCR_QOR_SOR_SWN_VR.xls'

    # Determine output path
    if args.output:
        csv_output = args.output
    else:
        windows_data_dir = os.getenv('WINDOWS_DATA_DIR', '')
        if windows_data_dir:
            base_dir = Path(windows_data_dir.replace('\\', '/'))
            if not base_dir.exists() and windows_data_dir.startswith('C:'):
                base_dir = Path('/mnt/c' + windows_data_dir[2:].replace('\\', '/'))
        else:
            base_dir = Path(__file__).resolve().parent.parent.parent.parent / 'data'

        csv_output = base_dir / 'processed' / 'projectsight' / 'ncr.csv'

    # Process
    try:
        records = process_excel_to_csv(str(excel_file), str(csv_output))
        print_summary(records)
        print(f"\n✓ Processing complete")
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
