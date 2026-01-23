#!/usr/bin/env python3
"""
Populate Grid Bounds in location_master.csv

Reads grid mappings from Samsung_FAB_Codes_by_Gridline_3.xlsx and updates
location_master.csv with row/column bounds for each location.

Also checks PDF floor drawings to flag which rooms are visible in drawings.

Matching Strategy:
1. Room codes: Direct match (FAB110101 -> FAB110101)
2. Elevators: Number match (ELV-01 -> FAB1-EL01, ELV-22 -> FAB1-EL22)
3. Stairs: Number match (STR-01 -> FAB1-ST01, STR-22 -> FAB1-ST22)

Generates:
- Updated location_master.csv with grid bounds and in_drawings flag
- rooms_needing_gridlines.csv for gaps needing manual mapping

Usage:
    python scripts/shared/populate_grid_bounds.py
    python scripts/shared/populate_grid_bounds.py --dry-run
    python scripts/shared/populate_grid_bounds.py --rebuild-dim
"""

import argparse
import re
import sys
from pathlib import Path

import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config.settings import settings


def extract_codes_from_drawings() -> dict:
    """Extract all location codes from PDF floor drawings.

    Extracts:
    - Room codes: FAB1 + 5 digits (e.g., FAB110101)
    - Elevator codes: FAB1-EL + number (e.g., FAB1-EL01)
    - Stair codes: FAB1-ST + number (e.g., FAB1-ST01)

    Returns:
        Dict with keys 'rooms', 'elevators', 'stairs' containing sets of codes
    """
    try:
        import fitz  # PyMuPDF
    except ImportError:
        print("Warning: PyMuPDF not installed, skipping drawings check")
        return {'rooms': set(), 'elevators': set(), 'stairs': set()}

    drawings_dir = settings.RAW_DATA_DIR / 'drawings'
    if not drawings_dir.exists():
        print(f"Warning: Drawings folder not found: {drawings_dir}")
        return {'rooms': set(), 'elevators': set(), 'stairs': set()}

    pdf_files = list(drawings_dir.glob('*.pdf'))
    if not pdf_files:
        print("Warning: No PDF files found in drawings folder")
        return {'rooms': set(), 'elevators': set(), 'stairs': set()}

    all_rooms = set()
    all_elevators = set()
    all_stairs = set()

    print(f"\nExtracting location codes from {len(pdf_files)} PDF drawings...")

    for pdf_path in sorted(pdf_files):
        doc = fitz.open(pdf_path)
        text = ""
        for page in doc:
            text += page.get_text()
        doc.close()

        # Find room codes (9 characters: FAB1 + 5 digits)
        rooms = set(re.findall(r'FAB1\d{5}', text, re.IGNORECASE))
        rooms = {c.upper() for c in rooms}
        all_rooms.update(rooms)

        # Find elevator codes (FAB1-EL + number, e.g., FAB1-EL01, FAB1-EL18)
        elevators = set(re.findall(r'FAB1-EL\d+[A-Z]?', text, re.IGNORECASE))
        elevators = {c.upper() for c in elevators}
        all_elevators.update(elevators)

        # Find stair codes (FAB1-ST + number, e.g., FAB1-ST01, FAB1-ST50)
        stairs = set(re.findall(r'FAB1-ST\d+', text, re.IGNORECASE))
        stairs = {c.upper() for c in stairs}
        all_stairs.update(stairs)

        print(f"  {pdf_path.name}: {len(rooms)} rooms, {len(elevators)} elevators, {len(stairs)} stairs")

    print(f"  Total unique: {len(all_rooms)} rooms, {len(all_elevators)} elevators, {len(all_stairs)} stairs")

    return {
        'rooms': all_rooms,
        'elevators': all_elevators,
        'stairs': all_stairs
    }


def load_excel_bounds() -> pd.DataFrame:
    """Load and aggregate grid bounds from Excel file."""
    excel_path = settings.RAW_DATA_DIR / 'location_mappings' / 'Samsung_FAB_Codes_by_Gridline_3.xlsx'

    if not excel_path.exists():
        raise FileNotFoundError(f"Excel file not found: {excel_path}")

    xl = pd.read_excel(excel_path, sheet_name='All Gridlines')

    # Aggregate to get bounds per FAB Code
    bounds = xl.groupby('FAB Code').agg({
        'Row': ['min', 'max'],
        'Column': ['min', 'max'],
        'Floor': 'first',
        'Room Name': 'first'
    }).reset_index()
    bounds.columns = ['fab_code', 'row_min', 'row_max', 'col_min', 'col_max', 'floor', 'room_name']

    print(f"Loaded {len(bounds)} FAB codes with grid bounds from Excel")
    return bounds


def extract_elev_stair_number(code: str, prefix: str) -> str | None:
    """Extract number/suffix from elevator/stair code.

    Examples:
        FAB1-EL01 -> 01
        FAB1-EL01A -> 01A
        ELV-01 -> 01
        STR-22 -> 22
    """
    if 'FAB1-' in code:
        match = re.search(r'FAB1-(?:EL|ST)(\d+\w*)', code)
        if match:
            return match.group(1)
    elif code.startswith(prefix):
        suffix = code[len(prefix):]
        # Only match if it looks like a number (not letter codes like ELV-S)
        if suffix and suffix[0].isdigit():
            return suffix
    return None


def build_elev_stair_mapping(bounds: pd.DataFrame) -> tuple[dict, dict]:
    """Build mapping from elevator/stair numbers to their bounds."""
    elev_bounds = {}
    stair_bounds = {}

    for _, row in bounds.iterrows():
        code = row['fab_code']
        if '-EL' in code:
            num = extract_elev_stair_number(code, '')
            if num:
                elev_bounds[num] = row
        elif '-ST' in code:
            num = extract_elev_stair_number(code, '')
            if num:
                stair_bounds[num] = row

    return elev_bounds, stair_bounds


def populate_bounds(dry_run: bool = False) -> dict:
    """Populate grid bounds in location_master.csv.

    Returns:
        dict with statistics about the update
    """
    # Load data
    bounds = load_excel_bounds()
    master_path = settings.RAW_DATA_DIR / 'location_mappings' / 'location_master.csv'
    loc_master = pd.read_csv(master_path)

    # Extract codes from PDF drawings
    drawing_codes = extract_codes_from_drawings()

    # Build lookups
    bounds_lookup = {row['fab_code'].upper(): row for _, row in bounds.iterrows()}
    elev_bounds, stair_bounds = build_elev_stair_mapping(bounds)

    # Build elevator/stair number lookups from drawings
    # FAB1-EL01 -> 01, FAB1-ST22 -> 22
    elev_numbers_in_drawings = set()
    for code in drawing_codes['elevators']:
        match = re.search(r'FAB1-EL(\d+[A-Z]?)', code)
        if match:
            elev_numbers_in_drawings.add(match.group(1))

    stair_numbers_in_drawings = set()
    for code in drawing_codes['stairs']:
        match = re.search(r'FAB1-ST(\d+)', code)
        if match:
            stair_numbers_in_drawings.add(match.group(1))

    # Initialize in_drawings column
    loc_master['In_Drawings'] = None

    # Statistics
    stats = {
        'room_matched': 0,
        'elev_matched': 0,
        'stair_matched': 0,
        'rooms_missing': [],
        'elev_missing': [],
        'stair_missing': [],
    }

    # Update each row
    for idx, row in loc_master.iterrows():
        code = row['Code']
        code_upper = code.upper()
        loc_type = row['Location_Type']
        matched_bounds = None
        in_drawings = True  # Default True for multi-room types (GRIDLINE, LEVEL, BUILDING, AREA, SITE)

        # Determine in_drawings based on location type
        if loc_type == 'ROOM':
            in_drawings = code_upper in drawing_codes['rooms']
        elif loc_type == 'ELEVATOR':
            num = extract_elev_stair_number(code, 'ELV-')
            if num:
                in_drawings = num in elev_numbers_in_drawings
            else:
                # Letter-based codes (ELV-S, ELV-H) - not in drawings naming convention
                in_drawings = False
        elif loc_type == 'STAIR':
            num = extract_elev_stair_number(code, 'STR-')
            if num:
                in_drawings = num in stair_numbers_in_drawings
            else:
                # Letter-based codes (STR-R, STR-T) - not in drawings naming convention
                in_drawings = False

        # Try direct match (rooms)
        if code_upper in bounds_lookup:
            matched_bounds = bounds_lookup[code_upper]
            if loc_type == 'ROOM':
                stats['room_matched'] += 1

        # Try elevator number match
        elif loc_type == 'ELEVATOR':
            num = extract_elev_stair_number(code, 'ELV-')
            if num and num in elev_bounds:
                matched_bounds = elev_bounds[num]
                stats['elev_matched'] += 1
            else:
                stats['elev_missing'].append(code)

        # Try stair number match
        elif loc_type == 'STAIR':
            num = extract_elev_stair_number(code, 'STR-')
            if num and num in stair_bounds:
                matched_bounds = stair_bounds[num]
                stats['stair_matched'] += 1
            else:
                stats['stair_missing'].append(code)

        # Track missing rooms
        if loc_type == 'ROOM' and code_upper not in bounds_lookup and pd.isna(row['Row_Min']):
            stats['rooms_missing'].append({
                'Code': code,
                'Building': row['Building'],
                'Level': row['Level'],
                'Task_Count': row['Task_Count'],
                'in_drawings': in_drawings
            })

        # Set in_drawings flag
        loc_master.at[idx, 'In_Drawings'] = in_drawings

        # Apply bounds if found
        if matched_bounds is not None:
            loc_master.at[idx, 'Row_Min'] = matched_bounds['row_min']
            loc_master.at[idx, 'Row_Max'] = matched_bounds['row_max']
            loc_master.at[idx, 'Col_Min'] = matched_bounds['col_min']
            loc_master.at[idx, 'Col_Max'] = matched_bounds['col_max']
            if pd.isna(row['Room_Name']) or not row['Room_Name']:
                loc_master.at[idx, 'Room_Name'] = matched_bounds['room_name']
            loc_master.at[idx, 'Action_Status'] = 'COMPLETE'

    # Print summary
    print("\n" + "=" * 60)
    print("GRID BOUNDS POPULATION SUMMARY")
    print("=" * 60)
    print(f"\nMatched:")
    print(f"  Rooms:     {stats['room_matched']}")
    print(f"  Elevators: {stats['elev_matched']}")
    print(f"  Stairs:    {stats['stair_matched']}")
    print(f"  TOTAL:     {stats['room_matched'] + stats['elev_matched'] + stats['stair_matched']}")

    print(f"\nMissing (need manual mapping):")
    print(f"  Rooms:     {len(stats['rooms_missing'])}")
    print(f"  Elevators: {len(stats['elev_missing'])}")
    print(f"  Stairs:    {len(stats['stair_missing'])}")

    if not dry_run:
        # Save updated master
        loc_master.to_csv(master_path, index=False)
        print(f"\n✓ Updated: {master_path}")

        # Save missing rooms list
        if stats['rooms_missing']:
            missing_df = pd.DataFrame(stats['rooms_missing']).sort_values('Task_Count', ascending=False)
            missing_path = settings.RAW_DATA_DIR / 'location_mappings' / 'rooms_needing_gridlines.csv'
            missing_df.to_csv(missing_path, index=False)
            print(f"✓ Missing rooms list: {missing_path}")

            # Show top missing by building
            print(f"\nMissing rooms by building:")
            for bldg, count in missing_df.groupby('Building')['Code'].count().sort_values(ascending=False).items():
                print(f"  {bldg}: {count}")
    else:
        print("\n[DRY RUN] No files modified")

    return stats


def rebuild_dim_location():
    """Rebuild dim_location.csv from updated location_master.csv."""
    print("\n" + "=" * 60)
    print("REBUILDING DIM_LOCATION")
    print("=" * 60)

    # Import and run the build script
    from scripts.integrated_analysis.dimensions.build_dim_location import main as build_main

    # Temporarily modify sys.argv
    original_argv = sys.argv
    sys.argv = ['build_dim_location.py']
    try:
        build_main()
    finally:
        sys.argv = original_argv


def main():
    parser = argparse.ArgumentParser(
        description='Populate grid bounds in location_master.csv from Excel mapping'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview changes without writing files'
    )
    parser.add_argument(
        '--rebuild-dim',
        action='store_true',
        help='Also rebuild dim_location.csv after updating'
    )
    args = parser.parse_args()

    # Populate bounds
    stats = populate_bounds(dry_run=args.dry_run)

    # Optionally rebuild dim_location
    if args.rebuild_dim and not args.dry_run:
        rebuild_dim_location()
    elif args.rebuild_dim and args.dry_run:
        print("\n[DRY RUN] Would rebuild dim_location.csv")


if __name__ == '__main__':
    main()
