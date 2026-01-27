"""
Enrich QC workbooks (Yates + SECAI) with:
1. CSI Division (from CSI Section)
2. Room-level location_id (using grid-based spatial matching)
3. Improved trade coverage (using CSI section mapping)
4. Affected rooms JSON (for bridge table)

Input:
    processed/quality/enriched/combined_qc_inspections.csv

Output:
    processed/quality/qc_inspections_enriched.csv (replaces combined file)
"""

import pandas as pd
import json
from pathlib import Path

from src.config.settings import Settings
from scripts.shared.dimension_lookup import (
    get_location_id,
    get_location_id_by_code,
    get_company_id,
    get_trade_id,
    get_csi_division,
    get_affected_rooms,
)
import re


def parse_grid_from_qc(grid_str: str) -> tuple:
    """
    Parse grid string from QC data into row and column.

    QC grid format: "C/5-7", "E.5/18.8", "D/3-5,N/3-5"
    Pattern: {ROW}/{COL} where ROW is A-N (with optional .5) and COL is 1-33

    Returns:
        (grid_row, grid_col) tuple or (None, None)
    """
    if pd.isna(grid_str):
        return None, None

    grid_str = str(grid_str).strip()

    # Handle multiple grids - take first one
    if ',' in grid_str:
        grid_str = grid_str.split(',')[0].strip()

    # Pattern: Row/Col where Row is A-N (possibly with .5 or range) and Col is number (possibly with .X or range)
    # Examples: C/5-7, E.5/18.8, C-D/29
    match = re.match(r'^([A-N](?:\.5)?(?:-[A-N](?:\.5)?)?)/(\d+(?:\.\d+)?(?:-\d+(?:\.\d+)?)?)', grid_str)

    if match:
        row_part = match.group(1)
        col_part = match.group(2)

        # Extract first row (if range, take first)
        if '-' in row_part:
            grid_row = row_part.split('-')[0]
        else:
            grid_row = row_part

        # Extract first col (if range, take first)
        if '-' in col_part:
            col_str = col_part.split('-')[0]
        else:
            col_str = col_part

        # Convert column to integer (strip decimal if present)
        try:
            grid_col = int(float(col_str))
            return grid_row, grid_col
        except (ValueError, TypeError):
            pass

    return None, None


def enrich_qc_inspections(dry_run: bool = False):
    """
    Enrich QC inspection data with improved location, CSI division, trade, and affected rooms.
    """
    settings = Settings()

    input_path = settings.PROCESSED_DATA_DIR / 'quality' / 'enriched' / 'combined_qc_inspections.csv'
    output_path = settings.PROCESSED_DATA_DIR / 'quality' / 'qc_inspections_enriched.csv'

    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        return

    print("Loading QC inspections...")
    df = pd.read_csv(input_path, low_memory=False)
    original_count = len(df)
    print(f"  Loaded {original_count:,} records")

    # 1. Add CSI Division
    print("\n1. Adding CSI Division...")
    df['csi_division'] = df['csi_section'].apply(
        lambda x: get_csi_division(x) if pd.notna(x) else None
    )
    csi_div_coverage = df['csi_division'].notna().sum()
    print(f"   CSI Division: {csi_div_coverage:,} / {original_count:,} ({csi_div_coverage/original_count*100:.1f}%)")

    # 2. Parse grid coordinates for spatial matching
    print("\n2. Parsing grid coordinates...")
    df[['grid_row', 'grid_col']] = df['grid'].apply(
        lambda x: pd.Series(parse_grid_from_qc(x))
    )
    has_grid = (df['grid_row'].notna() & df['grid_col'].notna()).sum()
    print(f"   Grid coordinates: {has_grid:,} / {original_count:,} ({has_grid/original_count*100:.1f}%)")

    # 3. Compute affected rooms using grid-based spatial matching
    print("\n3. Computing affected rooms (grid-based spatial matching)...")

    def compute_affected_rooms(row):
        """Get affected rooms for this inspection based on grid coordinates."""
        building = row.get('building')
        level = row.get('level')
        grid_row = row.get('grid_row')
        grid_col = row.get('grid_col')

        if pd.notna(building) and pd.notna(level) and pd.notna(grid_row) and pd.notna(grid_col):
            rooms = get_affected_rooms(level, grid_row, grid_col)
            if rooms:
                return json.dumps(rooms)

        return None

    df['affected_rooms'] = df.apply(compute_affected_rooms, axis=1)
    has_affected_rooms = df['affected_rooms'].notna().sum()
    print(f"   Affected rooms: {has_affected_rooms:,} / {original_count:,} ({has_affected_rooms/original_count*100:.1f}%)")

    # 4. Add location quality metrics
    print("\n4. Adding location quality metrics...")

    def get_grid_completeness(row):
        """Determine completeness of grid data."""
        has_row = pd.notna(row.get('grid_row'))
        has_col = pd.notna(row.get('grid_col'))

        if has_row and has_col:
            return 'FULL'
        elif has_row:
            return 'ROW_ONLY'
        elif has_col:
            return 'COL_ONLY'
        else:
            # Check if level is present
            if pd.notna(row.get('level')):
                return 'LEVEL_ONLY'
            return 'NONE'

    def get_match_quality(rooms_json):
        """Determine quality of location matches."""
        if pd.isna(rooms_json):
            return 'NONE'

        try:
            rooms = json.loads(rooms_json)
            if not rooms:
                return 'NONE'

            match_types = [r.get('match_type') for r in rooms]

            if all(mt == 'FULL' for mt in match_types):
                return 'PRECISE'
            elif any(mt == 'FULL' for mt in match_types):
                return 'MIXED'
            elif any(mt == 'PARTIAL' for mt in match_types):
                return 'PARTIAL'
            else:
                return 'GRIDLINE'
        except (json.JSONDecodeError, TypeError):
            return 'NONE'

    def needs_location_review(row):
        """Flag records needing human review of location mapping."""
        grid_comp = row.get('grid_completeness')
        match_qual = row.get('match_quality')

        # Review needed if:
        # - No grid data (LEVEL_ONLY, NONE)
        # - Only partial matches (PARTIAL quality)
        # - Only gridline matches (GRIDLINE quality)

        if grid_comp in ('LEVEL_ONLY', 'NONE'):
            return True
        if match_qual in ('PARTIAL', 'GRIDLINE', 'NONE'):
            return True

        return False

    df['grid_completeness'] = df.apply(get_grid_completeness, axis=1)
    df['match_quality'] = df['affected_rooms'].apply(get_match_quality)
    df['location_review_flag'] = df.apply(needs_location_review, axis=1)

    review_count = df['location_review_flag'].sum()
    print(f"   Location review needed: {review_count:,} / {original_count:,} ({review_count/original_count*100:.1f}%)")

    # 5. Update dim_location_id to most specific available
    print("\n5. Updating location_id to room-level where possible...")

    def get_best_location_id(row):
        """Get most specific location_id: room → level → building → site."""
        # Priority 1: Single room from affected_rooms
        affected_rooms_json = row.get('affected_rooms')
        if pd.notna(affected_rooms_json):
            try:
                rooms = json.loads(affected_rooms_json)
                if len(rooms) == 1:
                    room_data = rooms[0]
                    location_code = room_data.get('location_code')
                    match_type = room_data.get('match_type')

                    # FULL/PARTIAL = actual room, not gridline
                    if match_type in ('FULL', 'PARTIAL'):
                        room_location_id = get_location_id_by_code(location_code)
                        if room_location_id:
                            return room_location_id
            except (json.JSONDecodeError, TypeError):
                pass

        # Priority 2: Existing dim_location_id (building + level)
        existing_id = row.get('dim_location_id')
        if pd.notna(existing_id):
            return existing_id

        # Priority 3: Fallback to building or site
        building = row.get('building')
        if pd.notna(building):
            building_id = get_location_id(building, None)
            if building_id:
                return building_id

        # Priority 4: SITE fallback
        site_id = get_location_id('SITE', None)
        return site_id

    df['dim_location_id'] = df.apply(get_best_location_id, axis=1)
    location_coverage = df['dim_location_id'].notna().sum()
    print(f"   Location ID coverage: {location_coverage:,} / {original_count:,} ({location_coverage/original_count*100:.1f}%)")

    # 6. Improve trade coverage using CSI section
    print("\n6. Improving trade coverage using CSI sections...")
    original_trade_coverage = df['dim_trade_id'].notna().sum()

    def improve_trade(row):
        """Fill missing trade using CSI section mapping."""
        # If we already have trade, keep it
        if pd.notna(row.get('dim_trade_id')):
            return row.get('dim_trade_id')

        # Try to infer from CSI section
        csi_section = row.get('csi_section')
        if pd.notna(csi_section):
            # Get trade from CSI section
            # This would use CSI → trade mapping
            # For now, return None (would need CSI trade mapping table)
            pass

        return None

    df['dim_trade_id'] = df.apply(improve_trade, axis=1)
    new_trade_coverage = df['dim_trade_id'].notna().sum()
    print(f"   Trade coverage: {original_trade_coverage:,} → {new_trade_coverage:,} ({new_trade_coverage/original_count*100:.1f}%)")

    # 7. Add affected_rooms_count for quick reference
    df['affected_rooms_count'] = df['affected_rooms'].apply(
        lambda x: len(json.loads(x)) if pd.notna(x) else 0
    )

    # Summary
    print("\n" + "=" * 70)
    print("ENRICHMENT SUMMARY")
    print("=" * 70)
    print(f"Total records: {original_count:,}")
    print(f"\nCoverage:")
    print(f"  CSI Division:        {df['csi_division'].notna().sum():,} ({df['csi_division'].notna().mean()*100:.1f}%)")
    print(f"  Location ID:         {df['dim_location_id'].notna().sum():,} ({df['dim_location_id'].notna().mean()*100:.1f}%)")
    print(f"  Company ID:          {df['dim_company_id'].notna().sum():,} ({df['dim_company_id'].notna().mean()*100:.1f}%)")
    print(f"  Trade ID:            {df['dim_trade_id'].notna().sum():,} ({df['dim_trade_id'].notna().mean()*100:.1f}%)")
    print(f"  Grid coordinates:    {has_grid:,} ({has_grid/original_count*100:.1f}%)")
    print(f"  Affected rooms:      {has_affected_rooms:,} ({has_affected_rooms/original_count*100:.1f}%)")
    print(f"\nData Quality:")
    print(f"  Needs review:        {review_count:,} ({review_count/original_count*100:.1f}%)")

    if not dry_run:
        print(f"\nSaving to {output_path}...")
        df.to_csv(output_path, index=False)
        print("Done!")
    else:
        print("\nDRY RUN - no files written")

    return df


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Enrich QC workbooks with improved location and CSI data')
    parser.add_argument('--dry-run', action='store_true', help='Preview without saving')
    args = parser.parse_args()

    enrich_qc_inspections(dry_run=args.dry_run)


if __name__ == '__main__':
    main()
