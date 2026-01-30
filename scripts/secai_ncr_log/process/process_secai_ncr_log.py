#!/usr/bin/env python3
"""
Process SECAI NCR/QOR Log Excel workbook.

Reads the 4 main data sheets (External NCR, Internal NCR, External QOR, Internal QOR)
and consolidates into a single CSV with normalized columns.

Input:  raw/secai_ncr_log/260129_Taylor FAB1_ NCR, QOR Log.xlsb
Output: processed/secai_ncr_log/secai_ncr_qor.csv

Usage:
    python -m scripts.secai_ncr_log.process.process_secai_ncr_log
"""

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

# Add project root to path
_project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import settings


# Sheet configurations: (sheet_name, record_type, source_type)
SHEET_CONFIGS = [
    ('External NCR', 'NCR', 'EXTERNAL'),
    ('Internal NCR', 'NCR', 'INTERNAL'),
    ('External QOR', 'QOR', 'EXTERNAL'),
    ('Internal QOR', 'QOR', 'INTERNAL'),
]

# Column mappings for each sheet type
# Maps raw column names (or patterns) to normalized names
EXTERNAL_NCR_COLUMNS = {
    1: 'ncr_number',           # NCR No.
    2: 'seq_number',           # NCR Seq. No.
    3: 'description',          # Nonconformity Description
    4: 'building',             # Building Name
    5: 'location',             # Detailed Location
    6: 'contractor',           # Construction Contractor
    7: 'discipline',           # Discipline
    8: 'work_type',            # Work Description
    9: 'issue_date',           # Issue Date
    10: 'receipt_date',        # Date of Receipt
    11: 'requested_close_date', # Requested Close Date
    12: 'issued_by',           # Issued by
    13: 'issuing_org',         # Issuing organization
    14: 'action_description',  # Action description
    15: 'actual_close_date',   # Actual Close Date
    16: 'status',              # Status
}

INTERNAL_NCR_COLUMNS = {
    0: 'row_number',           # No.
    1: 'ncr_number',           # NCR No.
    2: 'description',          # Nonconformity Description
    3: 'building',             # Building Name
    4: 'location',             # Detailed Location
    5: 'contractor',           # Construction Partner
    6: 'discipline',           # Discipline
    7: 'work_type',            # Work Description
    8: 'issue_date',           # Issue Date
    9: 'requested_close_date', # Action Request Date
    10: 'issued_by',           # Issued by
    11: 'issuing_org',         # Issuing organization
    12: 'action_description',  # Action description
    13: 'actual_close_date',   # Actual Close Date
    14: 'status',              # Status
}

EXTERNAL_QOR_COLUMNS = {
    1: 'ncr_number',           # QOR No.
    2: 'description',          # Nonconformity Description
    3: 'building',             # Building Name
    4: 'location',             # Detailed Location
    5: 'contractor',           # Construction Partner
    6: 'discipline',           # Work Description (discipline)
    7: 'work_type',            # Work Description
    8: 'issue_date',           # Issue Date
    9: 'receipt_date',         # Date of Receipt
    10: 'requested_close_date', # Action Request Date
    11: 'issued_by',           # Issued by
    12: 'issuing_org',         # Issuing organization
    13: 'action_description',  # Action description
    14: 'actual_close_date',   # Actual Close Date
    15: 'status',              # Status
}

INTERNAL_QOR_COLUMNS = {
    0: 'row_number',           # No.
    1: 'ncr_number',           # QOR No.
    2: 'description',          # Nonconformity Description
    3: 'building',             # Building Name
    4: 'location',             # Detailed Location
    5: 'contractor',           # Construction Partner
    6: 'discipline',           # Work Description
    7: 'work_type',            # Work Description
    8: 'issue_date',           # Issue Date
    9: 'requested_close_date', # Action Request Date
    10: 'issued_by',           # Issued by
    11: 'issuing_org',         # Issuing organization
    12: 'action_description',  # Action description
    13: 'actual_close_date',   # Actual Close Date
    14: 'status',              # Status
}


def excel_date_to_string(val) -> Optional[str]:
    """Convert Excel serial date or datetime to YYYY-MM-DD string."""
    if pd.isna(val) or val == '' or val == ' ':
        return None

    # Already a string date
    if isinstance(val, str):
        val = val.strip()
        if not val:
            return None
        # Try to parse common formats
        for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y']:
            try:
                return datetime.strptime(val, fmt).strftime('%Y-%m-%d')
            except ValueError:
                continue
        return val  # Return as-is if can't parse

    # Excel serial date (number of days since 1900-01-01)
    try:
        float_val = float(val)
        if float_val > 40000 and float_val < 50000:  # Valid Excel date range
            base_date = datetime(1899, 12, 30)  # Excel epoch
            result = base_date + timedelta(days=float_val)
            return result.strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        pass

    return str(val) if val else None


def normalize_building(building: str) -> Optional[str]:
    """Normalize building name to standard codes."""
    if pd.isna(building) or not building:
        return None

    building = str(building).strip().upper()

    # Direct mappings
    mapping = {
        'FAB': 'FAB',
        'FAB ': 'FAB',
        'FAB  ': 'FAB',
        'SUE': 'SUE',
        'SUW': 'SUW',
        'CUB': 'CUB',
        'GCS': 'GCS',
        'FIZ': 'FIZ',
        'OFFICE': 'OFFICE',
        'OFFICE ': 'OFFICE',
        'LINE B': 'SITE',
        'STORM DRAIN LINE A RCB': 'SITE',
        'STORM DRAIN LINE B RCB': 'SITE',
    }

    return mapping.get(building, building)


def process_sheet(
    xl: pd.ExcelFile,
    sheet_name: str,
    record_type: str,
    source_type: str,
    column_map: dict,
) -> pd.DataFrame:
    """
    Process a single sheet from the workbook.

    Args:
        xl: Excel file object
        sheet_name: Name of the sheet to process
        record_type: 'NCR' or 'QOR'
        source_type: 'EXTERNAL' or 'INTERNAL'
        column_map: Dict mapping column indices to normalized names

    Returns:
        DataFrame with normalized columns
    """
    # Read with header at row 4 (0-indexed), data starts at row 5
    df = pd.read_excel(xl, sheet_name=sheet_name, header=None, skiprows=5)

    # Drop completely empty rows
    df = df.dropna(how='all')

    # Filter to rows with valid NCR/QOR numbers (column 1)
    ncr_col = 1
    df = df[df.iloc[:, ncr_col].notna() & (df.iloc[:, ncr_col] != '')]

    if len(df) == 0:
        return pd.DataFrame()

    # Map columns by index
    records = []
    for _, row in df.iterrows():
        record = {
            'record_type': record_type,
            'source_type': source_type,
            'source_sheet': sheet_name,
        }

        for col_idx, col_name in column_map.items():
            if col_idx < len(row):
                val = row.iloc[col_idx]
                # Convert dates
                if 'date' in col_name:
                    val = excel_date_to_string(val)
                # Clean strings
                elif pd.notna(val) and isinstance(val, str):
                    val = val.strip()
                    if val == '':
                        val = None
                record[col_name] = val
            else:
                record[col_name] = None

        records.append(record)

    result = pd.DataFrame(records)

    # Normalize building names
    if 'building' in result.columns:
        result['building'] = result['building'].apply(normalize_building)

    return result


def process_secai_ncr_log(input_path: Path = None, output_path: Path = None) -> pd.DataFrame:
    """
    Process the SECAI NCR/QOR log workbook.

    Args:
        input_path: Path to input .xlsb file
        output_path: Path to output .csv file

    Returns:
        Combined DataFrame with all records
    """
    if input_path is None:
        input_path = settings.RAW_DATA_DIR / 'secai_ncr_log' / '260129_Taylor FAB1_ NCR, QOR Log.xlsb'

    if output_path is None:
        output_path = settings.PROCESSED_DATA_DIR / 'secai_ncr_log' / 'secai_ncr_qor.csv'

    print(f"Reading: {input_path}")
    xl = pd.ExcelFile(input_path, engine='pyxlsb')

    # Process each sheet
    all_records = []

    column_maps = {
        'External NCR': EXTERNAL_NCR_COLUMNS,
        'Internal NCR': INTERNAL_NCR_COLUMNS,
        'External QOR': EXTERNAL_QOR_COLUMNS,
        'Internal QOR': INTERNAL_QOR_COLUMNS,
    }

    for sheet_name, record_type, source_type in SHEET_CONFIGS:
        print(f"  Processing {sheet_name}...")
        df = process_sheet(
            xl=xl,
            sheet_name=sheet_name,
            record_type=record_type,
            source_type=source_type,
            column_map=column_maps[sheet_name],
        )
        print(f"    Found {len(df)} records")
        all_records.append(df)

    # Combine all records
    combined = pd.concat(all_records, ignore_index=True)

    # Create primary key
    combined['secai_ncr_id'] = combined.apply(
        lambda r: f"SECAI-{r['source_type'][:3]}-{r['record_type']}-{r['ncr_number']}"
        if pd.notna(r['ncr_number']) else None,
        axis=1
    )

    # Reorder columns
    first_cols = ['secai_ncr_id', 'record_type', 'source_type', 'source_sheet', 'ncr_number']
    other_cols = [c for c in combined.columns if c not in first_cols]
    combined = combined[first_cols + other_cols]

    # Save output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(output_path, index=False)
    print(f"\nWriting: {output_path}")
    print(f"Total records: {len(combined)}")

    # Print summary
    print("\n=== PROCESSING SUMMARY ===")
    print(f"\nRecords by type:")
    for (rt, st), count in combined.groupby(['record_type', 'source_type']).size().items():
        print(f"  {st} {rt}: {count}")

    print(f"\nBuildings:")
    for bldg, count in combined['building'].value_counts().head(10).items():
        print(f"  {bldg}: {count}")

    print(f"\nContractors:")
    for contractor, count in combined['contractor'].value_counts().head(10).items():
        print(f"  {contractor}: {count}")

    return combined


def main():
    parser = argparse.ArgumentParser(description='Process SECAI NCR/QOR log workbook')
    parser.add_argument('--input', type=Path, default=None, help='Input .xlsb file')
    parser.add_argument('--output', type=Path, default=None, help='Output .csv file')
    args = parser.parse_args()

    process_secai_ncr_log(input_path=args.input, output_path=args.output)


if __name__ == '__main__':
    main()
