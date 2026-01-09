"""
Generate Location Report

Creates a comprehensive document of all locations from task taxonomy with:
- Current gridline data (where available)
- Level information
- Action status filter column

Action Status Categories:
- COMPLETE: Has level + gridline bounds (row and column)
- NEEDS_LOOKUP: Missing gridline bounds, needs manual lookup from drawings
- SPECIAL_CASE: Legitimate multi-level or wide-scope tasks (building/level/area-wide)
"""

import sys
from pathlib import Path

import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config.settings import Settings


def determine_action_status(row: pd.Series) -> str:
    """
    Determine the action status for a location.

    Returns:
        - COMPLETE: Has specific gridline bounds (delimited)
        - NEEDS_LOOKUP: Missing gridline data, needs manual lookup
        - SPECIAL_CASE: Legitimate wide-scope (building/level/area/gridline-wide)
    """
    loc_type = row.get('location_type', '')
    has_row_bounds = pd.notna(row.get('grid_row_min')) and pd.notna(row.get('grid_row_max'))
    has_col_bounds = pd.notna(row.get('grid_col_min')) and pd.notna(row.get('grid_col_max'))
    has_level = pd.notna(row.get('level')) and row.get('level') not in ['', 'MULTI', 'UNK']

    # BUILDING, LEVEL, AREA types are legitimate special cases
    if loc_type in ['BUILDING', 'LEVEL', 'AREA']:
        return 'SPECIAL_CASE'

    # GRIDLINE type spans full rows - legitimate special case
    if loc_type == 'GRIDLINE':
        return 'SPECIAL_CASE'

    # MULTI-level tasks are special cases
    if row.get('level') == 'MULTI':
        return 'SPECIAL_CASE'

    # ROOM, ELEVATOR, STAIR need gridline bounds
    if loc_type in ['ROOM', 'ELEVATOR', 'STAIR']:
        if has_row_bounds and has_col_bounds:
            return 'COMPLETE'
        else:
            return 'NEEDS_LOOKUP'

    # Everything else
    if has_row_bounds and has_col_bounds and has_level:
        return 'COMPLETE'
    elif not has_level:
        return 'NEEDS_LOOKUP'
    else:
        return 'NEEDS_LOOKUP'


def generate_location_report():
    """Generate comprehensive location report from task taxonomy."""

    # Load taxonomy
    taxonomy_path = Settings.DERIVED_DATA_DIR / 'primavera' / 'task_taxonomy.csv'
    print(f"Loading taxonomy from: {taxonomy_path}")
    df = pd.read_csv(taxonomy_path)

    # Filter to latest schedule only
    latest_prefix = df['task_id'].str.split('_').str[0].astype(int).max()
    df = df[df['task_id'].str.startswith(f'{latest_prefix}_')]
    print(f"Latest schedule: {latest_prefix} with {len(df)} tasks")

    # Group by location to get unique locations with their data
    location_cols = [
        'location_type', 'location_code', 'building', 'level',
        'grid_row_min', 'grid_row_max', 'grid_col_min', 'grid_col_max'
    ]

    # Filter to rows with location data
    df_with_loc = df[df['location_type'].notna() & (df['location_type'] != '')].copy()

    # Aggregate: for each unique location, get task count and sample label
    location_groups = df_with_loc.groupby(['location_type', 'location_code'], dropna=False).agg({
        'task_id': 'count',
        'label': lambda x: x.iloc[0] if len(x) > 0 else '',
        'building': 'first',
        'level': 'first',
        'grid_row_min': 'first',
        'grid_row_max': 'first',
        'grid_col_min': 'first',
        'grid_col_max': 'first',
    }).reset_index()

    location_groups.columns = [
        'location_type', 'location_code', 'task_count', 'sample_label',
        'building', 'level', 'grid_row_min', 'grid_row_max',
        'grid_col_min', 'grid_col_max'
    ]

    # Determine action status for each location
    location_groups['action_status'] = location_groups.apply(determine_action_status, axis=1)

    # Sort by action status (NEEDS_LOOKUP first), then by location type, then by task count
    status_order = {'NEEDS_LOOKUP': 0, 'SPECIAL_CASE': 1, 'COMPLETE': 2}
    location_groups['_sort_status'] = location_groups['action_status'].map(status_order)
    location_groups = location_groups.sort_values(
        ['_sort_status', 'location_type', 'task_count'],
        ascending=[True, True, False]
    ).drop(columns=['_sort_status'])

    # Reorder columns for readability
    output_cols = [
        'action_status', 'location_type', 'location_code', 'building', 'level',
        'grid_row_min', 'grid_row_max', 'grid_col_min', 'grid_col_max',
        'task_count', 'sample_label'
    ]
    location_groups = location_groups[output_cols]

    # Save report
    output_path = Settings.DERIVED_DATA_DIR / 'primavera' / 'location_report.csv'
    location_groups.to_csv(output_path, index=False)
    print(f"\nSaved location report to: {output_path}")

    # Print summary statistics
    print("\n" + "="*80)
    print("LOCATION REPORT SUMMARY")
    print("="*80)

    print(f"\nTotal unique locations: {len(location_groups)}")
    print(f"Total tasks covered: {location_groups['task_count'].sum()}")

    print("\n--- By Action Status ---")
    status_summary = location_groups.groupby('action_status').agg({
        'location_code': 'count',
        'task_count': 'sum'
    }).reset_index()
    status_summary.columns = ['Action Status', 'Locations', 'Tasks']
    for _, row in status_summary.iterrows():
        print(f"  {row['Action Status']}: {row['Locations']} locations, {row['Tasks']} tasks")

    print("\n--- By Location Type ---")
    type_summary = location_groups.groupby('location_type').agg({
        'location_code': 'count',
        'task_count': 'sum',
        'action_status': lambda x: (x == 'NEEDS_LOOKUP').sum()
    }).reset_index()
    type_summary.columns = ['Type', 'Locations', 'Tasks', 'Needs Lookup']
    for _, row in type_summary.iterrows():
        needs_pct = (row['Needs Lookup'] / row['Locations'] * 100) if row['Locations'] > 0 else 0
        print(f"  {row['Type']}: {row['Locations']} locations, {row['Tasks']} tasks, {row['Needs Lookup']} need lookup ({needs_pct:.1f}%)")

    print("\n--- NEEDS_LOOKUP Breakdown ---")
    needs_lookup = location_groups[location_groups['action_status'] == 'NEEDS_LOOKUP']
    if len(needs_lookup) > 0:
        nl_summary = needs_lookup.groupby('location_type').agg({
            'location_code': 'count',
            'task_count': 'sum'
        }).reset_index()
        nl_summary.columns = ['Type', 'Locations', 'Tasks']
        for _, row in nl_summary.iterrows():
            print(f"  {row['Type']}: {row['Locations']} locations ({row['Tasks']} tasks)")

        print(f"\n  Top 10 locations needing lookup (by task count):")
        top_needs = needs_lookup.nlargest(10, 'task_count')
        for _, row in top_needs.iterrows():
            print(f"    {row['location_type']}/{row['location_code']}: {row['task_count']} tasks")

    return location_groups


if __name__ == '__main__':
    generate_location_report()
