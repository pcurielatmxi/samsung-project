#!/usr/bin/env python3
"""
Parse TBM (Toolbox Meeting) Daily Work Plans from subcontractors.

These are Excel files in the SECAI Daily Work Plan format containing:
- Work activities and tasks
- Crew deployment (foreman, number of employees)
- Location (building, level, row)
- Equipment usage
- High risk work indicators

Input: data/raw/tbm/*.xlsx, *.xlsm
Output: data/tbm/tables/*.csv

Incremental Mode:
    python parse_tbm_daily_plans.py --incremental

    In incremental mode, only new files (not in tbm_files.csv) are processed.
    New entries are appended to existing output files with continuing file_ids.
"""

import argparse
import pandas as pd
import re
import sys
import time
import warnings
from pathlib import Path
from datetime import datetime

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
from src.config.settings import Settings
from schemas.validator import validated_df_to_csv
from scripts.shared.sync_log import SyncLog, SyncType

# Suppress openpyxl warnings about data validation
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')


def extract_date_from_cell(value) -> str:
    """Extract date from various formats in the date cell."""
    if pd.isna(value):
        return None

    val = str(value)

    # Handle datetime objects
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d')

    # Pattern: □ Date : MM/DD/YY or similar
    match = re.search(r'(\d{1,2})[/.-](\d{1,2})[/.-](\d{2,4})', val)
    if match:
        month, day, year = match.groups()
        year = int(year)
        if year < 100:
            year += 2000
        return f'{year:04d}-{int(month):02d}-{int(day):02d}'

    # Pattern: YYYY-MM-DD (from datetime string)
    match = re.search(r'(\d{4})-(\d{2})-(\d{2})', val)
    if match:
        return match.group(0)

    return None


def extract_date_from_filename(filename: str) -> str:
    """Extract date from filename as fallback."""
    name = Path(filename).stem

    # Pattern: eml_YYYYMMDD_NN (EML archive format)
    match = re.match(r'eml_(\d{4})(\d{2})(\d{2})_\d+', name)
    if match:
        year, month, day = match.groups()
        return f'{year}-{month}-{day}'

    # Pattern: MM.DD.YY or MM-DD-YY at start
    match = re.match(r'(\d{2})[.-](\d{2})[.-](\d{2})', name)
    if match:
        month, day, year = match.groups()
        return f'20{year}-{month}-{day}'

    # Pattern: MMDDYY at start (e.g., 031725)
    match = re.match(r'(\d{2})(\d{2})(\d{2})', name)
    if match:
        month, day, year = match.groups()
        return f'20{year}-{month}-{day}'

    # Pattern: YYYY-MM-DD in name
    match = re.search(r'(\d{4})-(\d{2})-(\d{2})', name)
    if match:
        return match.group(0)

    # Pattern: MMDDYYYY in name
    match = re.search(r'(\d{2})(\d{2})(\d{4})', name)
    if match:
        month, day, year = match.groups()
        return f'{year}-{month}-{day}'

    # Pattern: (YYMMDD) in parentheses - new consolidated format
    match = re.search(r'\((\d{2})(\d{2})(\d{2})\)', name)
    if match:
        year, month, day = match.groups()
        return f'20{year}-{month}-{day}'

    return None


def extract_subcontractor_from_filename(filename: str) -> str:
    """Extract subcontractor name from filename."""
    name = Path(filename).stem

    # Remove date patterns
    name = re.sub(r'^\d{2}[.-]\d{2}[.-]\d{2}[_ ]?', '', name)
    name = re.sub(r'^\d{6}[_ ]?', '', name)
    name = re.sub(r'^\d{4}-\d{2}-\d{2}[_ ]?', '', name)

    # Remove trailing date patterns
    name = re.sub(r'[ _]\d{1,2}[.-]\d{1,2}[.-]\d{2,4}.*$', '', name)
    name = re.sub(r'[ _]\d{8}.*$', '', name)

    # Clean up common suffixes
    name = re.sub(r'[ _]?\(\d+\)$', '', name)  # Remove (0), (1), etc.
    name = re.sub(r'[ _]?-[ _]?Copy$', '', name, flags=re.IGNORECASE)

    # Map common patterns to clean names
    name_map = {
        'TBM_ALK': 'ALK',
        'Baker Daily Work Plan': 'Baker',
        'Latcon-VeltriSteel': 'Latcon-Veltri Steel',
        'Apache TBM': 'Apache',
        'Berg TBM': 'Berg',
        'Brazos Daily Work Plan': 'Brazos',
        'Cherry Daily Work Plan': 'Cherry',
        'Cherry SECAI Daily Work Plan': 'Cherry',
        'Daily TBM': 'Alpha Painting',
        'GDA TBM': 'GDA',
        'INFINITY-SECAI Daily Work Plan': 'Infinity',
        'Kovach Daily Work Plan TBM': 'Kovach',
        'Kovach Daily Work Plan': 'Kovach',
        'MK Marlow Daily Work Plan': 'MK Marlow',
        'Patriot Erectors TBM': 'Patriot Erectors',
        'Yates - SECAI Daily Work Plan': 'Yates',
        # New consolidated format - contains multiple subcontractors
        'SECAI Daily Work Plan + TBM 담당 현황': 'SECAI Consolidated',
        'TBM 담당 현황': 'SECAI Consolidated',
    }

    name = name.strip('_ -')

    # Check for exact or partial matches
    for pattern, clean_name in name_map.items():
        if pattern in name or name in pattern:
            return clean_name

    return name if name else 'Unknown'


def extract_date_from_sheet_name(sheet_name: str) -> str:
    """Extract date from sheet names like 'SECAI Daily Work Plan 02.07.25'."""
    # Pattern: MM.DD.YY at end of sheet name
    match = re.search(r'(\d{2})[.](\d{2})[.](\d{2})$', sheet_name)
    if match:
        month, day, year = match.groups()
        return f'20{year}-{month}-{day}'
    return None


def find_work_plan_sheet(excel_file: pd.ExcelFile) -> str:
    """
    Find the TBM work plan sheet (handles multiple formats).

    Accepts:
    - "SECAI Daily Work Plan" (2025 OneDrive format)
    - "Form of work list and TBM" (2023-2024 EML format)
    - Sheets containing "Daily Work Plan" or "TBM" keywords
    """
    sheet_names = excel_file.sheet_names

    # Priority 1: Exact match for known formats
    for sheet in sheet_names:
        if sheet.startswith('SECAI Daily Work Plan'):
            return sheet
        if 'Form of work list and TBM' in sheet:
            return sheet

    # Priority 2: Sheets containing "Daily Work Plan" or "TBM"
    for sheet in sheet_names:
        sheet_lower = sheet.lower()
        if 'daily work plan' in sheet_lower or 'tbm' in sheet_lower:
            return sheet

    # Priority 3: Use first sheet as fallback (many files have unnamed main sheet)
    if sheet_names:
        return sheet_names[0]

    return None


def detect_column_indices(df: pd.DataFrame) -> dict:
    """
    Detect column indices from header rows.

    Handles two formats:
    - SECAI format: Headers on row 2-3 (indices 2-3), has date on row 1
    - EML format: Headers on row 1 (index 1), has title on row 0

    Some files have NO row number column - data starts directly with Division.
    We detect this by checking if the first data rows have numeric values in col 0.

    Returns:
        dict mapping field names to column indices, plus 'has_row_num' flag
    """
    if len(df) < 4:
        return None

    # Detect header row by searching for "Division" keyword
    header_row_idx = None
    for i in range(min(5, len(df))):  # Check first 5 rows
        row_values = [str(v).lower() if pd.notna(v) else '' for v in df.iloc[i]]
        if 'division' in ' '.join(row_values):
            header_row_idx = i
            break

    if header_row_idx is None:
        return None  # No valid header found

    # Get header rows
    header_row = df.iloc[header_row_idx]
    # Sub-header is next row (for location breakdown in SECAI format)
    sub_header_row = df.iloc[header_row_idx + 1] if header_row_idx + 1 < len(df) else pd.Series()

    # Convert to lowercase strings for matching
    def normalize(val):
        if pd.isna(val):
            return ''
        return str(val).lower().replace('\n', ' ').strip()

    headers = [normalize(v) for v in header_row]
    sub_headers = [normalize(v) for v in sub_header_row]

    # Detect if column 0 has row numbers by checking first few data rows
    has_row_num = False
    for check_idx in range(4, min(10, len(df))):
        val = df.iloc[check_idx, 0]
        if pd.notna(val):
            try:
                int(val)
                has_row_num = True
                break
            except (ValueError, TypeError):
                # Not a number - likely division data
                pass

    # Find key columns by header text patterns (dynamic detection)
    cols = {
        'header_row_idx': header_row_idx,  # Store for data row calculation
        'has_row_num': has_row_num,
        'row_num': 0 if has_row_num else None,  # No row_num column in shifted files
        'division': None,
        'tier1_gc': None,
        'tier2_sc': None,
        'foreman': None,
        'contact_number': None,
        'num_employees': None,
        'work_activities': None,
        'location_building': None,
        'location_level': None,
        'location_row': None,
        'start_time': None,
        'end_time': None,
    }

    # Search for column indices
    # Note: Some files have multiple employee columns (planned, absent, on site)
    # We track the first generic match but prioritize "planned" if found
    first_employee_col = None
    for i, header in enumerate(headers):
        if 'division' in header:
            cols['division'] = i
        elif 'company' in header and 'tier 2' in sub_headers[i]:
            # SECAI format: Tier 2 (SC) - subcontractor column
            cols['tier2_sc'] = i
        elif 'company' in header and ('tier 1' in sub_headers[i] or 'gc' in sub_headers[i]):
            # SECAI format: Tier 1 (GC) - general contractor column
            cols['tier1_gc'] = i
        elif 'company name' in header:
            # EML format: Single company column (treat as tier2_sc for subcontractor)
            cols['tier2_sc'] = i
            cols['tier1_gc'] = None  # EML doesn't have separate GC column
        elif 'foreman' in header or 'supervisor' in header:
            cols['foreman'] = i
        elif 'contact' in header and ('number' in header or 'supervisor' in header):
            cols['contact_number'] = i
        elif 'employee' in header and 'no' in header:
            # Prioritize "planned" column over other employee columns (absent, on site)
            if 'planned' in header:
                cols['num_employees'] = i
            elif first_employee_col is None:
                first_employee_col = i
        elif 'activity' in header and ('task' in header or 'work' in header):
            # Matches "Activity / Task" (EML) or "Work Activities/Tasks" (SECAI)
            cols['work_activities'] = i
        elif 'work activities' in header or 'activities/tasks' in header:
            cols['work_activities'] = i
        elif 'work location' in header:
            # EML format: "Work Location" (single column)
            cols['location_building'] = i
        elif header == 'location' or ('location' in header and len(header) < 15):
            # SECAI format: "Location" header - look for sub-headers
            # Location typically spans 3 columns (building, level, row)
            cols['location_building'] = i
        elif 'start' in header and 'time' in header:
            cols['start_time'] = i
        elif 'end' in header and ('time' in header or 'finish' in header):
            cols['end_time'] = i

    # Use first employee column as fallback if no "planned" column found
    if cols['num_employees'] is None and first_employee_col is not None:
        cols['num_employees'] = first_employee_col

    # Find location sub-columns from sub-header row (only for SECAI format)
    # EML format has single "Work Location" column, no sub-columns
    has_location_subheaders = False
    for i, sub_header in enumerate(sub_headers):
        if 'lv.1' in sub_header or 'building' in sub_header:
            cols['location_building'] = i
            has_location_subheaders = True
        elif 'lv.2' in sub_header or 'level' in sub_header and 'lv' in sub_header:
            cols['location_level'] = i
            has_location_subheaders = True
        elif 'lv.3' in sub_header or 'row' in sub_header:
            cols['location_row'] = i
            has_location_subheaders = True

    # Validate we found the critical columns
    required = ['num_employees', 'work_activities', 'start_time']
    missing = [k for k in required if cols.get(k) is None]

    if missing:
        # Try fallback: if we found work_activities, guess nearby columns
        if cols['work_activities'] is not None:
            wa_idx = cols['work_activities']
            if cols['location_building'] is None:
                cols['location_building'] = wa_idx + 1
            # Don't populate location sub-columns for EML format (single column)
            # Only populate if we detected sub-headers in SECAI format
            if has_location_subheaders:
                if cols['location_level'] is None:
                    cols['location_level'] = wa_idx + 2
                if cols['location_row'] is None:
                    cols['location_row'] = wa_idx + 3
            if cols['start_time'] is None:
                # For EML: work_activities + 6 columns = start_time
                cols['start_time'] = wa_idx + 6
            if cols['end_time'] is None:
                cols['end_time'] = wa_idx + 7

    # If no row numbers in data, apply offset to ALL column positions
    # The headers still have "No." column but data is shifted left by 1
    if not has_row_num:
        for key in cols:
            if key != 'has_row_num' and cols[key] is not None:
                cols[key] = max(0, cols[key] - 1)  # Shift left by 1, min 0

    return cols


def parse_tbm_file(filepath: Path) -> tuple:
    """
    Parse a single TBM Excel file.

    Returns:
        tuple: (file_info_dict, list of work_entry_dicts)
    """
    try:
        excel_file = pd.ExcelFile(filepath)
        sheet_name = find_work_plan_sheet(excel_file)
        if not sheet_name:
            print(f"  No SECAI Daily Work Plan sheet in {filepath.name}")
            return None, []
        df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)
    except Exception as e:
        print(f"  Error reading {filepath.name}: {e}")
        return None, []

    # Extract date - try multiple sources in order of preference
    # 1. From cell (1,0) - old format has "□ Date : MM/DD/YY"
    date_cell = df.iloc[1, 0] if len(df) > 1 else None
    report_date = extract_date_from_cell(date_cell)
    # 2. From sheet name - new format has "SECAI Daily Work Plan MM.DD.YY"
    if not report_date:
        report_date = extract_date_from_sheet_name(sheet_name)
    # 3. From filename as last resort
    if not report_date:
        report_date = extract_date_from_filename(filepath.name)

    # Extract subcontractor from filename
    subcontractor = extract_subcontractor_from_filename(filepath.name)

    file_info = {
        'filename': filepath.name,
        'report_date': report_date,
        'subcontractor_file': subcontractor,
    }

    # Detect column positions dynamically from headers
    cols = detect_column_indices(df)
    if cols is None:
        print(f"  Could not detect column structure in {filepath.name}")
        return None, []

    # Helper to safely get cell value
    def get_cell(row, col_name):
        idx = cols.get(col_name)
        if idx is None or idx >= len(row):
            return None
        val = row.iloc[idx]
        if pd.isna(val):
            return None
        return val

    def get_str(row, col_name):
        val = get_cell(row, col_name)
        return str(val) if val is not None else None

    # Parse work entries starting after headers
    # EML format: header row 1, data starts row 4
    # SECAI format: header row 2-3, data starts row 4
    # Calculate data start row dynamically
    header_row_idx = cols.get('header_row_idx', 2)
    data_start_row = max(4, header_row_idx + 3)  # At least row 4, or 3 rows after header

    work_entries = []
    has_row_num = cols.get('has_row_num', True)

    for idx in range(data_start_row, len(df)):
        row = df.iloc[idx]

        # Handle files with or without row numbers
        if has_row_num:
            # Normal case: check if row number is present
            row_num = get_cell(row, 'row_num')
            if row_num is None:
                continue
            try:
                int(row_num)  # Validate it's a number, but we don't use it
            except (ValueError, TypeError):
                continue
        else:
            # No row number column - check if division has data to identify valid rows
            division = get_cell(row, 'division')
            if division is None:
                continue

        # Use absolute Excel row number (idx + 1 for 1-based) to guarantee uniqueness
        excel_row_num = idx + 1

        # Extract fields using detected column positions
        entry = {
            'report_date': report_date,
            'subcontractor_file': subcontractor,
            'row_num': excel_row_num,
            'division': get_str(row, 'division'),
            'tier1_gc': get_str(row, 'tier1_gc'),
            'tier2_sc': get_str(row, 'tier2_sc'),
            'foreman': get_str(row, 'foreman'),
            'contact_number': get_str(row, 'contact_number'),
            'num_employees': None,
            'work_activities': get_str(row, 'work_activities'),
            'location_building': get_str(row, 'location_building'),
            'location_level': get_str(row, 'location_level'),
            'location_row': get_str(row, 'location_row'),
            'start_time': get_str(row, 'start_time'),
            'end_time': get_str(row, 'end_time'),
        }

        # Parse number of employees (can be "7", "0-10", etc.)
        emp_val = get_cell(row, 'num_employees')
        if emp_val is not None:
            emp_str = str(emp_val)
            # Handle ranges like "0-10"
            if '-' in emp_str and not emp_str.startswith('-'):
                parts = emp_str.split('-')
                try:
                    entry['num_employees'] = int(parts[1])  # Use upper bound
                except (ValueError, IndexError):
                    pass
            else:
                try:
                    entry['num_employees'] = int(float(emp_val))
                except (ValueError, TypeError):
                    pass

        # Only add entries with actual work activities
        if entry['work_activities'] and entry['work_activities'].lower() not in ['nan', 'none', '']:
            work_entries.append(entry)

    return file_info, work_entries


def get_processed_files(output_dir: Path) -> tuple[set, int]:
    """
    Get set of already processed filenames and max file_id from tbm_files.csv.

    Returns:
        tuple: (set of processed filenames, max file_id or 0 if no files)
    """
    files_csv = output_dir / 'tbm_files.csv'
    if not files_csv.exists():
        return set(), 0

    try:
        df = pd.read_csv(files_csv)
        processed = set(df['filename'].tolist())
        max_id = df['file_id'].max() if len(df) > 0 else 0
        return processed, int(max_id)
    except Exception as e:
        print(f"  Warning: Could not read {files_csv}: {e}")
        return set(), 0


def main(incremental: bool = False, dry_run: bool = False):
    """Main processing function."""
    start_time = time.time()

    input_dir = Settings.TBM_RAW_DIR
    output_dir = Settings.TBM_PROCESSED_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    if dry_run:
        print("=" * 60)
        print("DRY RUN MODE - No files will be processed or saved")
        print("=" * 60)

    # Get all Excel files, excluding non-standard ones
    # MXI files are annotated copies with frozen dates that cause duplicates
    # "TBM 담당 현황" are Korean status summary files, not actual daily work plans
    excluded_patterns = ['Manpower TrendReport', 'Structural  Exteriors', 'TaylorFab', 'Labor Day', 'MXI', 'TBM 담당 현황']

    excel_files = list(input_dir.glob('*.xlsx')) + list(input_dir.glob('*.xlsm'))
    excel_files = [f for f in excel_files if not any(ex in f.name for ex in excluded_patterns)]
    excel_files = sorted(excel_files)

    # Track existing records for delta calculation
    records_before = 0
    existing_entries_path = output_dir / 'work_entries.csv'
    if existing_entries_path.exists():
        records_before = len(pd.read_csv(existing_entries_path))

    # In incremental mode, filter out already processed files
    processed_files = set()
    start_file_id = 0
    files_skipped = 0

    if incremental:
        processed_files, start_file_id = get_processed_files(output_dir)
        original_count = len(excel_files)
        files_skipped = len(processed_files)
        excel_files = [f for f in excel_files if f.name not in processed_files]
        print(f"Incremental mode: {original_count} total files, {len(processed_files)} already processed")

        if not excel_files:
            print("No new files to process.")
            # Log the no-change sync
            SyncLog.log(
                pipeline="tbm",
                sync_type=SyncType.DRY_RUN if dry_run else SyncType.INCREMENTAL,
                files_processed=0,
                files_skipped=files_skipped,
                records_before=records_before,
                records_after=records_before,
                duration_seconds=time.time() - start_time,
                status="no_change",
                message="No new files to process"
            )
            return

    # In dry-run mode, just show what would be processed
    if dry_run:
        print(f"\nWould process {len(excel_files)} TBM files:")
        for f in excel_files[:10]:
            print(f"  - {f.name}")
        if len(excel_files) > 10:
            print(f"  ... and {len(excel_files) - 10} more")

        # Log the dry run
        SyncLog.log(
            pipeline="tbm",
            sync_type=SyncType.DRY_RUN,
            files_processed=0,
            files_skipped=files_skipped,
            records_before=records_before,
            records_after=records_before,
            duration_seconds=time.time() - start_time,
            status="success",
            message=f"Dry run: {len(excel_files)} files would be processed"
        )
        print("\nDry run complete. No files were processed.")
        return

    print(f"Processing {len(excel_files)} TBM files...")

    all_entries = []
    file_infos = []
    errors = []

    file_id = start_file_id
    for i, filepath in enumerate(excel_files):
        if i % 50 == 0:
            print(f"Processing {i+1}/{len(excel_files)}...")

        file_info, entries = parse_tbm_file(filepath)

        if file_info:
            file_id += 1
            file_info['file_id'] = file_id
            file_infos.append(file_info)
            # Add file_id to each entry for traceability
            for entry in entries:
                entry['file_id'] = file_id
            all_entries.extend(entries)
        else:
            errors.append(filepath.name)

    print(f"\nProcessed {len(file_infos)} files successfully")
    print(f"Errors: {len(errors)}")
    if errors:
        print(f"  Failed files: {errors[:10]}{'...' if len(errors) > 10 else ''}")

    if not file_infos:
        print("\nNo new data to save.")
        # Log the sync with no new data (but files were attempted)
        SyncLog.log(
            pipeline="tbm",
            sync_type=SyncType.INCREMENTAL if incremental else SyncType.FULL,
            files_processed=0,
            files_skipped=files_skipped,
            files_failed=len(errors),
            records_before=records_before,
            records_after=records_before,
            duration_seconds=time.time() - start_time,
            status="partial" if errors else "no_change",
            message=f"{len(errors)} files failed to parse" if errors else "No parseable files"
        )
        return

    # Create DataFrames
    entries_df = pd.DataFrame(all_entries)
    files_df = pd.DataFrame(file_infos)

    # Reorder columns to put file_id first
    if len(entries_df) > 0:
        cols = ['file_id'] + [c for c in entries_df.columns if c != 'file_id']
        entries_df = entries_df[cols]
    if len(files_df) > 0:
        cols = ['file_id'] + [c for c in files_df.columns if c != 'file_id']
        files_df = files_df[cols]

    # In incremental mode, append to existing files
    print("\n=== Saving outputs ===")

    if incremental and processed_files:
        # Load existing data and append
        existing_entries = pd.read_csv(output_dir / 'work_entries.csv')
        existing_files = pd.read_csv(output_dir / 'tbm_files.csv')

        entries_df = pd.concat([existing_entries, entries_df], ignore_index=True)
        files_df = pd.concat([existing_files, files_df], ignore_index=True)

        print(f"  Appended {len(all_entries)} new entries to {len(existing_entries)} existing")

    # Save output files
    validated_df_to_csv(entries_df, output_dir / 'work_entries.csv', index=False)
    print(f"work_entries.csv: {len(entries_df)} records (validated)")

    validated_df_to_csv(files_df, output_dir / 'tbm_files.csv', index=False)
    print(f"tbm_files.csv: {len(files_df)} files (validated)")

    # Log the sync operation
    duration = time.time() - start_time
    records_after = len(entries_df)
    sync_type = SyncType.INCREMENTAL if incremental else SyncType.FULL

    status = "success" if not errors else "partial"
    message = f"{len(errors)} files failed" if errors else ""

    SyncLog.log(
        pipeline="tbm",
        sync_type=sync_type,
        files_processed=len(file_infos),
        files_skipped=files_skipped,
        files_failed=len(errors),
        records_before=records_before,
        records_after=records_after,
        duration_seconds=duration,
        status=status,
        message=message
    )

    print("\nDone!")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Parse TBM Daily Work Plans')
    parser.add_argument('--incremental', '-i', action='store_true',
                        help='Only process new files not in tbm_files.csv')
    parser.add_argument('--dry-run', '-n', action='store_true',
                        help='Show what would be processed without actually processing')
    args = parser.parse_args()

    main(incremental=args.incremental, dry_run=args.dry_run)
