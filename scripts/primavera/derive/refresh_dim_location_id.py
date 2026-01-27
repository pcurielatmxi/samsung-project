#!/usr/bin/env python3
"""
Refresh dim_location_id in task taxonomy.

Re-populates the dim_location_id column in p6_task_taxonomy.csv by looking up
location codes against the current dim_location table.

Use this after regenerating dim_location to fix broken FK references.

Usage:
    python scripts/primavera/derive/refresh_dim_location_id.py
    python scripts/primavera/derive/refresh_dim_location_id.py --dry-run
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config.settings import Settings
from scripts.shared.dimension_lookup import (
    get_location_id,
    get_location_id_by_code,
    reset_cache,
)


def refresh_dim_location_id(taxonomy: pd.DataFrame) -> pd.DataFrame:
    """
    Refresh dim_location_id column by looking up against current dim_location.

    Args:
        taxonomy: DataFrame with 'location_code', 'building', and 'level' columns

    Returns:
        DataFrame with updated 'dim_location_id' column
    """
    # Reset cache to ensure we pick up latest dim_location
    reset_cache()

    print("Refreshing dim_location_id column...")

    matched_by_code = 0
    matched_by_building_level = 0
    unmatched = 0

    def get_loc_id(row):
        nonlocal matched_by_code, matched_by_building_level, unmatched

        # Priority 1: Try location_code lookup (rooms, elevators, stairs)
        loc_code = row.get('location_code')
        if loc_code and pd.notna(loc_code):
            loc_id = get_location_id_by_code(str(loc_code))
            if loc_id is not None:
                matched_by_code += 1
                return loc_id

        # Priority 2: Fall back to building + level
        b = row.get('building')
        l = row.get('level')
        loc_id = get_location_id(b, l, allow_fallback=True)
        if loc_id is not None:
            matched_by_building_level += 1
            return loc_id

        unmatched += 1
        return None

    taxonomy['dim_location_id'] = taxonomy.apply(get_loc_id, axis=1)
    taxonomy['dim_location_id'] = taxonomy['dim_location_id'].astype('Int64')

    # Report coverage
    total = len(taxonomy)
    matched = taxonomy['dim_location_id'].notna().sum()
    print(f"  Total tasks: {total:,}")
    print(f"  Matched: {matched:,} ({100*matched/total:.1f}%)")
    print(f"    - by location_code: {matched_by_code:,}")
    print(f"    - by building+level: {matched_by_building_level:,}")
    print(f"  Unmatched: {unmatched:,}")

    return taxonomy


def main():
    parser = argparse.ArgumentParser(
        description='Refresh dim_location_id in task taxonomy'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview changes without writing file'
    )
    args = parser.parse_args()

    # Load taxonomy
    taxonomy_path = Settings.PROCESSED_DATA_DIR / 'primavera' / 'p6_task_taxonomy.csv'
    if not taxonomy_path.exists():
        print(f"ERROR: Taxonomy not found: {taxonomy_path}")
        return 1

    print(f"Loading taxonomy from: {taxonomy_path}")
    taxonomy = pd.read_csv(taxonomy_path, low_memory=False)
    print(f"  {len(taxonomy):,} rows")

    # Check current state
    old_null = taxonomy['dim_location_id'].isna().sum()
    print(f"  Current null dim_location_id: {old_null:,}")

    # Refresh
    taxonomy = refresh_dim_location_id(taxonomy)

    # Check new state
    new_null = taxonomy['dim_location_id'].isna().sum()
    print(f"\nAfter refresh:")
    print(f"  Null dim_location_id: {new_null:,}")

    if args.dry_run:
        print(f"\n[DRY RUN] Would write {len(taxonomy):,} rows to:")
        print(f"  {taxonomy_path}")
    else:
        taxonomy.to_csv(taxonomy_path, index=False)
        print(f"\nWrote {len(taxonomy):,} rows to:")
        print(f"  {taxonomy_path}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
