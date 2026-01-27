#!/usr/bin/env python3
"""
Build dim_location.csv v2 - Direct extraction from P6 taxonomy

Generates the location dimension table by extracting unique locations directly
from the P6 task taxonomy using the centralized location extraction module.

Key differences from v1:
- Extracts locations directly from P6 taxonomy (not location_master.csv)
- Uses standardized naming: STR-xx for stairs, ELV-xx for elevators
- No *-ALL aggregate codes - uses FAB1 as project-wide BUILDING
- No SITE or UNDEFINED types - everything categorizes to BUILDING minimum

Source: processed/primavera/p6_task_taxonomy.csv
Grid lookup: raw/location_mappings/location_master.csv
Output: processed/integrated_analysis/dimensions/dim_location.csv

Usage:
    python scripts/integrated_analysis/dimensions/build_dim_location_v2.py
    python scripts/integrated_analysis/dimensions/build_dim_location_v2.py --dry-run
    python scripts/integrated_analysis/dimensions/build_dim_location_v2.py --file-id 1  # Specific schedule
"""

import argparse
import re
import sys
from pathlib import Path

import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config.settings import Settings


# =============================================================================
# Grid Lookup from Location Master
# =============================================================================

def load_grid_lookup() -> dict:
    """Load grid bounds lookup from location_master.csv.

    Returns:
        Dict mapping location_code -> grid bounds dict
    """
    master_path = Settings.RAW_DATA_DIR / 'location_mappings' / 'location_master.csv'
    if not master_path.exists():
        print(f"WARNING: location_master.csv not found at {master_path}")
        return {}

    df = pd.read_csv(master_path)
    print(f"Loaded location_master.csv: {len(df)} rows for grid lookup")

    lookup = {}
    for _, row in df.iterrows():
        code = row['Code']
        if pd.notna(row['Row_Min']):
            lookup[code] = {
                'grid_row_min': row['Row_Min'],
                'grid_row_max': row['Row_Max'],
                'grid_col_min': row['Col_Min'],
                'grid_col_max': row['Col_Max'],
                'room_name': row['Room_Name'] if pd.notna(row['Room_Name']) else None,
            }

    print(f"  {len(lookup)} locations have grid bounds")
    return lookup


def infer_grid_from_sibling_rooms(grid_lookup: dict) -> dict:
    """Infer grid bounds for rooms missing them from sibling rooms on other floors.

    FAB room codes have structure: FAB1[F][AANN] where F is floor digit.
    Same room on different floors (e.g., FAB126406 and FAB136406) has same grid.

    Args:
        grid_lookup: Dict from load_grid_lookup()

    Returns:
        Dict mapping room_code -> inferred grid bounds with source
    """
    # Parse room codes into (floor_area, room_num)
    def parse_room(code):
        match = re.match(r'FAB1(\d)(\d{4})$', str(code).upper())
        if match:
            return match.group(1), match.group(2)
        return None, None

    # Build room_num -> list of rooms with grid bounds
    room_num_to_grids = {}
    for code, bounds in grid_lookup.items():
        floor_area, room_num = parse_room(code)
        if room_num:
            if room_num not in room_num_to_grids:
                room_num_to_grids[room_num] = []
            room_num_to_grids[room_num].append({
                'code': code,
                **bounds
            })

    return room_num_to_grids


# =============================================================================
# Extract Unique Locations from P6 Taxonomy
# =============================================================================

def load_p6_taxonomy(file_id: int = None) -> pd.DataFrame:
    """Load P6 task taxonomy.

    Args:
        file_id: Optional specific schedule file_id. If None, uses all schedules.

    Returns:
        DataFrame with taxonomy data
    """
    taxonomy_path = Settings.PROCESSED_DATA_DIR / 'primavera' / 'p6_task_taxonomy.csv'
    if not taxonomy_path.exists():
        raise FileNotFoundError(f"P6 taxonomy not found: {taxonomy_path}")

    df = pd.read_csv(taxonomy_path, low_memory=False)
    print(f"Loaded P6 taxonomy: {len(df)} task rows")

    # Filter to specific file_id if requested
    if file_id is not None and 'file_id' in df.columns:
        df = df[df['file_id'] == file_id]
        print(f"  Filtered to file_id={file_id}: {len(df)} rows")

    return df


def extract_unique_locations(taxonomy: pd.DataFrame) -> pd.DataFrame:
    """Extract unique locations from P6 taxonomy.

    Args:
        taxonomy: P6 task taxonomy DataFrame

    Returns:
        DataFrame with unique locations and task counts
    """
    # Get location columns
    loc_cols = ['location_type', 'location_code', 'building', 'level']

    # Filter to rows with location info
    has_loc = taxonomy['location_type'].notna() & taxonomy['location_code'].notna()
    loc_df = taxonomy[has_loc][loc_cols].copy()

    # Count tasks per location
    task_counts = loc_df.groupby(['location_type', 'location_code']).size().reset_index(name='task_count')

    # Get unique locations with their building/level (take first occurrence)
    unique_locs = loc_df.drop_duplicates(subset=['location_type', 'location_code'])

    # Merge task counts
    unique_locs = unique_locs.merge(task_counts, on=['location_type', 'location_code'])

    print(f"\nExtracted {len(unique_locs)} unique locations:")
    for lt in ['ROOM', 'STAIR', 'ELEVATOR', 'GRIDLINE', 'LEVEL', 'BUILDING', 'AREA']:
        count = len(unique_locs[unique_locs['location_type'] == lt])
        if count > 0:
            print(f"  {lt}: {count}")

    return unique_locs


# =============================================================================
# Build dim_location
# =============================================================================

def build_dim_location(
    unique_locs: pd.DataFrame,
    grid_lookup: dict,
    room_num_to_grids: dict,
) -> pd.DataFrame:
    """Build dim_location from unique locations.

    Args:
        unique_locs: DataFrame from extract_unique_locations()
        grid_lookup: Grid bounds lookup from location_master
        room_num_to_grids: Sibling room grid inference lookup

    Returns:
        DataFrame with dim_location structure
    """
    rows = []
    location_id = 1
    inferred_count = 0

    # Sort by task_count descending so most-used locations get lower IDs
    unique_locs = unique_locs.sort_values('task_count', ascending=False)

    for _, row in unique_locs.iterrows():
        loc_type = row['location_type']
        loc_code = row['location_code']
        building = row['building']
        level = row['level']
        task_count = row['task_count']

        # Look up grid bounds
        grid_bounds = grid_lookup.get(loc_code, {})
        grid_row_min = grid_bounds.get('grid_row_min')
        grid_row_max = grid_bounds.get('grid_row_max')
        grid_col_min = grid_bounds.get('grid_col_min')
        grid_col_max = grid_bounds.get('grid_col_max')
        room_name = grid_bounds.get('room_name')
        grid_inferred_from = None

        # Try to infer grid from sibling rooms if missing
        if grid_row_min is None and loc_type == 'ROOM':
            match = re.match(r'FAB1(\d)(\d{4})$', str(loc_code).upper())
            if match:
                room_num = match.group(2)
                if room_num in room_num_to_grids:
                    sibling = room_num_to_grids[room_num][0]
                    grid_row_min = sibling['grid_row_min']
                    grid_row_max = sibling['grid_row_max']
                    grid_col_min = sibling['grid_col_min']
                    grid_col_max = sibling['grid_col_max']
                    grid_inferred_from = sibling['code']
                    inferred_count += 1

        # Determine status
        if grid_row_min is not None:
            if grid_inferred_from:
                status = 'INFERRED'
            else:
                status = 'COMPLETE'
        elif loc_type in ['LEVEL', 'BUILDING', 'AREA']:
            status = 'AGGREGATE'
        else:
            status = 'NEEDS_LOOKUP'

        # Build building_level key
        if loc_type == 'LEVEL':
            building_level = level
        elif loc_type == 'BUILDING':
            building_level = building  # Just building code, no level
        elif building and level and pd.notna(level):
            building_level = f"{building}-{level}"
        elif building:
            building_level = building
        else:
            building_level = None

        entry = {
            'location_id': location_id,
            'location_code': loc_code,
            'location_type': loc_type,
            'room_name': room_name,
            'building': building,
            'level': level,
            'grid_row_min': grid_row_min,
            'grid_row_max': grid_row_max,
            'grid_col_min': grid_col_min,
            'grid_col_max': grid_col_max,
            'grid_inferred_from': grid_inferred_from,
            'status': status,
            'task_count': task_count,
            'building_level': building_level,
        }

        rows.append(entry)
        location_id += 1

    if inferred_count > 0:
        print(f"\nInferred grid bounds for {inferred_count} rooms from sibling rooms")

    # Add FAB1 as project-wide fallback (if not already present)
    existing_codes = {r['location_code'] for r in rows}
    if 'FAB1' not in existing_codes:
        print("\nAdding FAB1 as project-wide BUILDING entry...")
        rows.append({
            'location_id': location_id,
            'location_code': 'FAB1',
            'location_type': 'BUILDING',
            'room_name': 'Samsung Taylor FAB1 Project',
            'building': 'FAB1',
            'level': None,
            'grid_row_min': None,
            'grid_row_max': None,
            'grid_col_min': None,
            'grid_col_max': None,
            'grid_inferred_from': None,
            'status': 'PROJECT_WIDE',
            'task_count': 0,
            'building_level': 'FAB1',
        })
        location_id += 1

    df = pd.DataFrame(rows)

    # Ensure proper column order
    columns = [
        'location_id', 'location_code', 'location_type', 'room_name',
        'building', 'level', 'grid_row_min', 'grid_row_max',
        'grid_col_min', 'grid_col_max', 'grid_inferred_from',
        'status', 'task_count', 'building_level',
    ]
    df = df[columns]

    return df


# =============================================================================
# Summary and Output
# =============================================================================

def print_summary(df: pd.DataFrame):
    """Print summary of the generated dim_location."""
    print("\n" + "=" * 60)
    print("DIM_LOCATION V2 SUMMARY")
    print("=" * 60)

    print(f"\nTotal entries: {len(df)}")
    print(f"Unique location_codes: {df['location_code'].nunique()}")

    print("\nBy location_type:")
    for lt, count in df['location_type'].value_counts().items():
        print(f"  {lt}: {count}")

    print("\nBy status:")
    for status, count in df['status'].value_counts().items():
        print(f"  {status}: {count}")

    print("\nBy building:")
    for bldg, count in df['building'].value_counts(dropna=False).head(10).items():
        print(f"  {bldg}: {count}")

    # Grid coverage stats
    has_grid = df['grid_row_min'].notna()
    rooms = df[df['location_type'] == 'ROOM']
    rooms_with_grid = rooms[rooms['grid_row_min'].notna()]

    print(f"\nGrid coverage:")
    print(f"  Total with grid: {has_grid.sum()}/{len(df)} ({100*has_grid.sum()/len(df):.1f}%)")
    print(f"  ROOM with grid:  {len(rooms_with_grid)}/{len(rooms)} ({100*len(rooms_with_grid)/len(rooms):.1f}%)")

    # Top locations by task count
    print("\nTop 10 locations by task_count:")
    for _, row in df.nlargest(10, 'task_count').iterrows():
        print(f"  {row['location_code']:<15} {row['location_type']:<10} {row['task_count']:>6} tasks")


def main():
    parser = argparse.ArgumentParser(
        description='Build dim_location.csv v2 from P6 taxonomy'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview changes without writing file'
    )
    parser.add_argument(
        '--file-id',
        type=int,
        default=None,
        help='Specific P6 schedule file_id (default: all schedules)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default=None,
        help='Output path (default: processed/integrated_analysis/dimensions/dim_location.csv)'
    )
    args = parser.parse_args()

    print("=" * 60)
    print("BUILD DIM_LOCATION V2")
    print("=" * 60)

    # Load grid lookup from location_master
    print("\n--- Loading Grid Lookup ---")
    grid_lookup = load_grid_lookup()
    room_num_to_grids = infer_grid_from_sibling_rooms(grid_lookup)

    # Load P6 taxonomy
    print("\n--- Loading P6 Taxonomy ---")
    taxonomy = load_p6_taxonomy(file_id=args.file_id)

    # Extract unique locations
    print("\n--- Extracting Unique Locations ---")
    unique_locs = extract_unique_locations(taxonomy)

    # Build dim_location
    print("\n--- Building dim_location ---")
    dim_location = build_dim_location(unique_locs, grid_lookup, room_num_to_grids)

    # Print summary
    print_summary(dim_location)

    # Determine output path
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = Settings.PROCESSED_DATA_DIR / 'integrated_analysis' / 'dimensions' / 'dim_location.csv'

    if args.dry_run:
        print(f"\n[DRY RUN] Would write {len(dim_location)} rows to:")
        print(f"  {output_path}")
        print("\nRun without --dry-run to apply changes.")
    else:
        # Ensure directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        dim_location.to_csv(output_path, index=False)
        print(f"\nWrote {len(dim_location)} rows to:")
        print(f"  {output_path}")


if __name__ == '__main__':
    main()
