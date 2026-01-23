#!/usr/bin/env python3
"""
Populate Grid Bounds in location_master.csv

Reads grid mappings from Samsung_FAB_Codes_by_Gridline_3.xlsx and updates
location_master.csv with row/column bounds for each location.

Matching Strategy:
1. Room codes: Direct match (FAB110101 -> FAB110101)
2. Elevators: Number match (ELV-01 -> FAB1-EL01, ELV-22 -> FAB1-EL22)
3. Stairs: Number match (STR-01 -> FAB1-ST01, STR-22 -> FAB1-ST22)

Generates:
- Updated location_master.csv with grid bounds
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

    # Build lookups
    bounds_lookup = {row['fab_code'].upper(): row for _, row in bounds.iterrows()}
    elev_bounds, stair_bounds = build_elev_stair_mapping(bounds)

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
        elif loc_type == 'ROOM' and pd.isna(row['Row_Min']):
            stats['rooms_missing'].append({
                'Code': code,
                'Building': row['Building'],
                'Level': row['Level'],
                'Task_Count': row['Task_Count']
            })

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
