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
"""

import pandas as pd
import re
import warnings
from pathlib import Path
from datetime import datetime

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
    }

    name = name.strip('_ -')

    # Check for exact or partial matches
    for pattern, clean_name in name_map.items():
        if pattern in name or name in pattern:
            return clean_name

    return name if name else 'Unknown'


def parse_tbm_file(filepath: Path) -> tuple:
    """
    Parse a single TBM Excel file.

    Returns:
        tuple: (file_info_dict, list of work_entry_dicts)
    """
    try:
        df = pd.read_excel(filepath, sheet_name='SECAI Daily Work Plan', header=None)
    except Exception as e:
        print(f"  Error reading {filepath.name}: {e}")
        return None, []

    # Extract date from cell (0,0) contains title, (1,0) contains date
    date_cell = df.iloc[1, 0] if len(df) > 1 else None
    report_date = extract_date_from_cell(date_cell)
    if not report_date:
        report_date = extract_date_from_filename(filepath.name)

    # Extract subcontractor from filename
    subcontractor = extract_subcontractor_from_filename(filepath.name)

    file_info = {
        'filename': filepath.name,
        'report_date': report_date,
        'subcontractor_file': subcontractor,
    }

    # Parse work entries starting from row 3 (0-indexed)
    work_entries = []

    for idx in range(3, len(df)):
        row = df.iloc[idx]

        # Skip empty rows (check if row number is present)
        row_num = row.iloc[0]
        if pd.isna(row_num):
            continue

        try:
            row_num = int(row_num)
        except (ValueError, TypeError):
            continue

        # Extract fields
        entry = {
            'report_date': report_date,
            'subcontractor_file': subcontractor,
            'row_num': row_num,
            'division': str(row.iloc[1]) if pd.notna(row.iloc[1]) else None,
            'tier1_gc': str(row.iloc[2]) if pd.notna(row.iloc[2]) else None,
            'tier2_sc': str(row.iloc[3]) if pd.notna(row.iloc[3]) else None,
            'foreman': str(row.iloc[4]) if pd.notna(row.iloc[4]) else None,
            'contact_number': str(row.iloc[5]) if pd.notna(row.iloc[5]) else None,
            'num_employees': None,
            'work_activities': str(row.iloc[7]) if pd.notna(row.iloc[7]) else None,
            'location_building': str(row.iloc[8]) if pd.notna(row.iloc[8]) else None,
            'location_level': str(row.iloc[9]) if pd.notna(row.iloc[9]) else None,
            'location_row': str(row.iloc[10]) if pd.notna(row.iloc[10]) else None,
            'start_time': None,
            'end_time': None,
        }

        # Parse number of employees (can be "7", "0-10", etc.)
        emp_val = row.iloc[6]
        if pd.notna(emp_val):
            emp_str = str(emp_val)
            # Handle ranges like "0-10"
            if '-' in emp_str:
                parts = emp_str.split('-')
                try:
                    entry['num_employees'] = int(parts[1])  # Use upper bound
                except:
                    pass
            else:
                try:
                    entry['num_employees'] = int(float(emp_val))
                except:
                    pass

        # Parse times
        if pd.notna(row.iloc[11]):
            entry['start_time'] = str(row.iloc[11])
        if pd.notna(row.iloc[12]):
            entry['end_time'] = str(row.iloc[12])

        # Only add entries with actual work activities
        if entry['work_activities'] and entry['work_activities'].lower() not in ['nan', 'none', '']:
            work_entries.append(entry)

    return file_info, work_entries


def main():
    """Main processing function."""
    input_dir = Path('data/raw/tbm')
    output_dir = Path('data/tbm/tables')
    output_dir.mkdir(parents=True, exist_ok=True)

    # Also create .gitkeep
    (output_dir / '.gitkeep').touch()

    # Get all Excel files, excluding non-standard ones
    excluded_patterns = ['Manpower TrendReport', 'Structural  Exteriors', 'TaylorFab', 'Labor Day']

    excel_files = list(input_dir.glob('*.xlsx')) + list(input_dir.glob('*.xlsm'))
    excel_files = [f for f in excel_files if not any(ex in f.name for ex in excluded_patterns)]
    excel_files = sorted(excel_files)

    print(f"Found {len(excel_files)} TBM files to process")

    all_entries = []
    file_infos = []
    errors = []

    file_id = 0
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

    # Create DataFrames and save
    print("\n=== Saving outputs ===")

    # Work entries table
    if all_entries:
        entries_df = pd.DataFrame(all_entries)
        # Reorder columns to put file_id first
        cols = ['file_id'] + [c for c in entries_df.columns if c != 'file_id']
        entries_df = entries_df[cols]
        entries_df.to_csv(output_dir / 'work_entries.csv', index=False)
        print(f"work_entries.csv: {len(entries_df)} records")

    # File info table
    if file_infos:
        files_df = pd.DataFrame(file_infos)
        # Reorder columns to put file_id first
        cols = ['file_id'] + [c for c in files_df.columns if c != 'file_id']
        files_df = files_df[cols]
        files_df.to_csv(output_dir / 'tbm_files.csv', index=False)
        print(f"tbm_files.csv: {len(files_df)} files")

    print("\nDone!")


if __name__ == '__main__':
    main()
