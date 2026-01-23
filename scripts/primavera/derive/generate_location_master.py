"""
Generate Location Master Document

Creates a comprehensive document of ALL locations for team review:
- All unique locations from task taxonomy (all schedules)
- Current gridline data from mapping file
- Action status column for filtering

Output: CSV that team can review and fill in missing gridlines.
"""

import sys
from pathlib import Path

import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config.settings import Settings


def load_existing_mapping():
    """Load existing gridline mapping from Excel file."""
    mapping_path = Settings.RAW_DATA_DIR / 'location_mappings' / 'Samsung_FAB_Codes_by_Gridline_3.xlsx'
    if not mapping_path.exists():
        print(f"Warning: Mapping file not found: {mapping_path}")
        return {}

    df = pd.read_excel(mapping_path, sheet_name='All Gridlines')

    # Build bounds table for each FAB Code
    bounds = df.groupby('FAB Code').agg({
        'Row': ['min', 'max'],
        'Column': ['min', 'max'],
        'Floor': 'first',
        'Room Name': 'first'
    }).reset_index()
    bounds.columns = ['FAB_Code', 'Row_Min', 'Row_Max', 'Col_Min', 'Col_Max', 'Floor', 'Room_Name']

    # Create lookup dictionary
    mapping = {}
    for _, row in bounds.iterrows():
        fab_code = str(row['FAB_Code']).upper()
        mapping[fab_code] = {
            'row_min': row['Row_Min'],
            'row_max': row['Row_Max'],
            'col_min': int(row['Col_Min']) if pd.notna(row['Col_Min']) else None,
            'col_max': int(row['Col_Max']) if pd.notna(row['Col_Max']) else None,
            'floor': row['Floor'],
            'room_name': row['Room_Name'],
        }
    return mapping


def normalize_code_for_lookup(location_type: str, location_code: str) -> str:
    """Convert taxonomy code to mapping lookup key."""
    if not location_code:
        return None

    code = str(location_code).upper()

    if location_type == 'ROOM':
        # Room codes are direct: FAB112345 -> FAB112345
        return code

    elif location_type == 'ELEVATOR':
        # ELV-01 -> FAB1-EL01, ELV-A -> FAB1-ELA
        import re
        match = re.match(r'ELV-(\d+|[A-Z])([A-Z])?$', code)
        if match:
            num = match.group(1)
            suffix = match.group(2) or ''
            if num.isdigit():
                num = num.zfill(2)
            return f'FAB1-EL{num}{suffix}'

    elif location_type == 'STAIR':
        # STR-01 -> FAB1-ST01
        import re
        match = re.match(r'STR-(\d+|[A-Z])$', code)
        if match:
            num = match.group(1)
            if num.isdigit():
                num = num.zfill(2)
            return f'FAB1-ST{num}'

    return None


def determine_action_status(row: pd.Series) -> str:
    """Determine the action status for a location."""
    loc_type = row.get('Location_Type', '')
    has_row_bounds = pd.notna(row.get('Row_Min')) and pd.notna(row.get('Row_Max'))
    has_col_bounds = pd.notna(row.get('Col_Min')) and pd.notna(row.get('Col_Max'))
    level = row.get('Level', '')

    # BUILDING, LEVEL, AREA, GRIDLINE types are legitimate special cases
    if loc_type in ['BUILDING', 'LEVEL', 'AREA', 'GRIDLINE']:
        return 'SPECIAL_CASE'

    # MULTI-level tasks are special cases
    if level == 'MULTI':
        return 'SPECIAL_CASE'

    # ROOM, ELEVATOR, STAIR need gridline bounds
    if loc_type in ['ROOM', 'ELEVATOR', 'STAIR']:
        if has_row_bounds and has_col_bounds:
            return 'COMPLETE'
        else:
            return 'NEEDS_LOOKUP'

    return 'NEEDS_LOOKUP'


def generate_location_master():
    """Generate comprehensive location master document."""

    # Load taxonomy (all schedules) - use processed p6_task_taxonomy.csv which has all 483K tasks
    taxonomy_path = Settings.PRIMAVERA_PROCESSED_DIR / 'p6_task_taxonomy.csv'
    print(f"Loading taxonomy from: {taxonomy_path}")
    df = pd.read_csv(taxonomy_path, low_memory=False)
    print(f"Total tasks across all schedules: {len(df)}")

    # Load existing gridline mapping
    print("Loading existing gridline mapping...")
    mapping = load_existing_mapping()
    print(f"Loaded {len(mapping)} FAB codes from mapping file")

    # Filter to rows with location data
    df_with_loc = df[df['location_type'].notna() & (df['location_type'] != '')].copy()
    print(f"Tasks with location data: {len(df_with_loc)}")

    # Get unique locations across all schedules
    location_groups = df_with_loc.groupby(
        ['location_type', 'location_code'],
        dropna=False
    ).agg({
        'task_id': 'count',
        'building': 'first',
        'level': 'first',
    }).reset_index()

    location_groups.columns = ['Location_Type', 'Code', 'Task_Count', 'Building', 'Level']

    print(f"Unique locations: {len(location_groups)}")

    # Look up gridline bounds for each location
    rows = []
    for _, loc in location_groups.iterrows():
        loc_type = loc['Location_Type']
        code = loc['Code']

        # Initialize row
        row = {
            'Location_Type': loc_type,
            'Code': code,
            'Room_Name': '',
            'Building': loc['Building'] if pd.notna(loc['Building']) else '',
            'Level': loc['Level'] if pd.notna(loc['Level']) else '',
            'Row_Min': None,
            'Row_Max': None,
            'Col_Min': None,
            'Col_Max': None,
            'Task_Count': loc['Task_Count'],
            'Notes': '',
        }

        # Try to find in mapping
        lookup_key = normalize_code_for_lookup(loc_type, code)
        if lookup_key and lookup_key in mapping:
            bounds = mapping[lookup_key]
            row['Row_Min'] = bounds['row_min']
            row['Row_Max'] = bounds['row_max']
            row['Col_Min'] = bounds['col_min']
            row['Col_Max'] = bounds['col_max']
            row['Room_Name'] = bounds['room_name'] if pd.notna(bounds['room_name']) else ''

        # Handle GRIDLINE type specially - spans full row range
        if loc_type == 'GRIDLINE':
            try:
                col = float(code)
                row['Row_Min'] = 'A'
                row['Row_Max'] = 'N'
                row['Col_Min'] = col
                row['Col_Max'] = col
            except (ValueError, TypeError):
                pass

        rows.append(row)

    result_df = pd.DataFrame(rows)

    # Determine action status
    result_df['Action_Status'] = result_df.apply(determine_action_status, axis=1)

    # Sort by action status (NEEDS_LOOKUP first), then type, then task count
    status_order = {'NEEDS_LOOKUP': 0, 'SPECIAL_CASE': 1, 'COMPLETE': 2}
    result_df['_sort'] = result_df['Action_Status'].map(status_order)
    result_df = result_df.sort_values(
        ['_sort', 'Location_Type', 'Task_Count'],
        ascending=[True, True, False]
    ).drop(columns=['_sort'])

    # Reorder columns
    output_cols = [
        'Action_Status', 'Location_Type', 'Code', 'Room_Name', 'Building', 'Level',
        'Row_Min', 'Row_Max', 'Col_Min', 'Col_Max', 'Task_Count', 'Notes'
    ]
    result_df = result_df[output_cols]

    # Save to location_mappings folder (same place as input mapping)
    output_path = Settings.RAW_DATA_DIR / 'location_mappings' / 'location_master.csv'
    result_df.to_csv(output_path, index=False)
    print(f"\nSaved location master to: {output_path}")

    # Print summary
    print("\n" + "="*80)
    print("LOCATION MASTER SUMMARY")
    print("="*80)

    print(f"\nTotal unique locations: {len(result_df)}")
    print(f"Total tasks covered: {result_df['Task_Count'].sum()}")

    print("\n--- By Action Status ---")
    for status in ['NEEDS_LOOKUP', 'SPECIAL_CASE', 'COMPLETE']:
        subset = result_df[result_df['Action_Status'] == status]
        print(f"  {status}: {len(subset)} locations, {subset['Task_Count'].sum()} tasks")

    print("\n--- By Location Type ---")
    type_summary = result_df.groupby('Location_Type').agg({
        'Code': 'count',
        'Task_Count': 'sum',
        'Action_Status': lambda x: (x == 'NEEDS_LOOKUP').sum()
    }).reset_index()
    type_summary.columns = ['Type', 'Locations', 'Tasks', 'Needs Lookup']
    for _, row in type_summary.iterrows():
        print(f"  {row['Type']}: {row['Locations']} locations, {row['Tasks']} tasks, {row['Needs Lookup']} need lookup")

    print("\n--- NEEDS_LOOKUP Detail ---")
    needs_lookup = result_df[result_df['Action_Status'] == 'NEEDS_LOOKUP']
    if len(needs_lookup) > 0:
        for loc_type in needs_lookup['Location_Type'].unique():
            subset = needs_lookup[needs_lookup['Location_Type'] == loc_type]
            print(f"\n  {loc_type} ({len(subset)} locations):")
            for _, row in subset.head(20).iterrows():
                print(f"    {row['Code']}: {row['Task_Count']} tasks, Bldg={row['Building']}, Lvl={row['Level']}")
            if len(subset) > 20:
                print(f"    ... and {len(subset) - 20} more")

    return result_df


if __name__ == '__main__':
    generate_location_master()
