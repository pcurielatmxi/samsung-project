#!/usr/bin/env python3
"""
Add Missing Building-Levels to dim_location

Analyzes P6 task taxonomy to find building-level combinations that are missing
from dim_location, and adds them to enable complete Power BI relationships.

This script should be run when generate_task_taxonomy.py reports gaps.

Usage:
    python scripts/integrated_analysis/dimensions/add_missing_building_levels.py
    python scripts/integrated_analysis/dimensions/add_missing_building_levels.py --dry-run
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config.settings import Settings


def find_missing_building_levels() -> pd.DataFrame:
    """
    Find building-level combinations in P6 taxonomy that are missing from dim_location.

    Returns:
        DataFrame with columns: building, level, count
    """
    # Load dim_location
    dim_loc_path = Settings.PROCESSED_DATA_DIR / 'integrated_analysis' / 'dimensions' / 'dim_location.csv'
    dim_location = pd.read_csv(dim_loc_path)

    # Get existing building_level values
    existing_bl = set(dim_location['building_level'].dropna().unique())

    # Load P6 taxonomy
    taxonomy_path = Settings.DERIVED_DATA_DIR / 'primavera' / 'task_taxonomy.csv'
    taxonomy = pd.read_csv(taxonomy_path, low_memory=False)

    # Find records with building+level but no mapping
    has_both = taxonomy[
        taxonomy['building'].notna() &
        taxonomy['level'].notna()
    ].copy()

    has_both['building_level'] = has_both['building'] + '-' + has_both['level']

    # Find missing
    missing = has_both[~has_both['building_level'].isin(existing_bl)]

    if len(missing) == 0:
        return pd.DataFrame(columns=['building', 'level', 'count'])

    # Aggregate
    missing_combos = missing.groupby(['building', 'level']).size().reset_index(name='count')
    missing_combos = missing_combos.sort_values('count', ascending=False)

    return missing_combos


def add_missing_entries(missing_combos: pd.DataFrame, dry_run: bool = True) -> int:
    """
    Add missing building-level entries to dim_location.

    Args:
        missing_combos: DataFrame with building, level, count columns
        dry_run: If True, don't save changes

    Returns:
        Number of entries added
    """
    if len(missing_combos) == 0:
        print("No missing building-levels to add.")
        return 0

    dim_loc_path = Settings.PROCESSED_DATA_DIR / 'integrated_analysis' / 'dimensions' / 'dim_location.csv'
    dim_location = pd.read_csv(dim_loc_path)

    print(f"\nCurrent dim_location: {len(dim_location)} rows")
    print(f"Max location_id: {dim_location['location_id'].max()}")

    next_id = dim_location['location_id'].max() + 1

    new_rows = []
    for _, row in missing_combos.iterrows():
        building = row['building']
        level = row['level']
        count = row['count']

        # Create location code and name
        location_code = f"{building}-{level}"
        room_name = f"{building} Level {level}" if level not in ('MULTI', 'ROOF') else f"{building} {level}"

        new_rows.append({
            'location_id': next_id + len(new_rows),
            'location_code': location_code,
            'location_type': 'LEVEL',
            'room_name': room_name,
            'building': building,
            'level': level,
            'grid_row_min': None,
            'grid_row_max': None,
            'grid_col_min': None,
            'grid_col_max': None,
            'status': 'ACTIVE',
            'task_count': count,
            'building_level': f"{building}-{level}",
        })

        print(f"  + {location_code} ({count:,} tasks)")

    if dry_run:
        print(f"\n[DRY RUN] Would add {len(new_rows)} entries")
        print("Run with --apply to save changes")
    else:
        new_df = pd.DataFrame(new_rows)
        dim_location_updated = pd.concat([dim_location, new_df], ignore_index=True)
        dim_location_updated.to_csv(dim_loc_path, index=False)
        print(f"\nAdded {len(new_rows)} entries to dim_location.csv")
        print(f"New total: {len(dim_location_updated)} rows")

    return len(new_rows)


def main():
    parser = argparse.ArgumentParser(
        description='Add missing building-levels to dim_location.csv'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        default=True,
        help='Preview changes without saving (default)'
    )
    parser.add_argument(
        '--apply',
        action='store_true',
        help='Apply changes to dim_location.csv'
    )
    args = parser.parse_args()

    print("Finding missing building-level combinations...")
    missing = find_missing_building_levels()

    if len(missing) == 0:
        print("All building-level combinations are already in dim_location!")
        return

    print(f"\nFound {len(missing)} missing building-level combinations:")
    total_tasks = missing['count'].sum()
    print(f"Total tasks affected: {total_tasks:,}")

    print("\nMissing combinations:")
    for _, row in missing.iterrows():
        print(f"  {row['building']}-{row['level']}: {row['count']:,} tasks")

    dry_run = not args.apply
    add_missing_entries(missing, dry_run=dry_run)


if __name__ == '__main__':
    main()
