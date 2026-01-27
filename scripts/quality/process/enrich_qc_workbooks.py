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
    get_csi_division,
)
from scripts.integrated_analysis.location import enrich_location


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

    # 2. Centralized location enrichment
    print("\n2. Enriching location data (centralized module)...")

    def apply_location_enrichment(row):
        """Apply centralized location enrichment to a row."""
        loc = enrich_location(
            building=row.get('building'),
            level=row.get('level'),
            grid=row.get('grid'),
            source='QC_WORKBOOK'
        )
        return pd.Series({
            'dim_location_id_new': loc.dim_location_id,
            'building_level': loc.building_level,
            'grid_row': loc.grid_row_min,
            'grid_col': loc.grid_col_min,
            'grid_normalized': loc.grid_normalized,
            'affected_rooms': loc.affected_rooms,
            'affected_rooms_count': loc.affected_rooms_count,
            'grid_completeness': loc.grid_completeness,
            'match_quality': loc.match_quality,
            'location_review_flag': loc.location_review_flag,
        })

    # Apply location enrichment
    location_cols = df.apply(apply_location_enrichment, axis=1)

    # Update dim_location_id with new values (preserving original if new is None)
    df['dim_location_id'] = location_cols['dim_location_id_new'].combine_first(df.get('dim_location_id'))
    df['building_level'] = location_cols['building_level']
    df['grid_row'] = location_cols['grid_row']
    df['grid_col'] = location_cols['grid_col']
    df['affected_rooms'] = location_cols['affected_rooms']
    df['affected_rooms_count'] = location_cols['affected_rooms_count']
    df['grid_completeness'] = location_cols['grid_completeness']
    df['match_quality'] = location_cols['match_quality']
    df['location_review_flag'] = location_cols['location_review_flag']

    # Coverage stats
    has_grid = (df['grid_row'].notna() & df['grid_col'].notna()).sum()
    has_affected_rooms = df['affected_rooms'].notna().sum()
    review_count = df['location_review_flag'].sum()
    location_coverage = df['dim_location_id'].notna().sum()

    print(f"   Grid coordinates: {has_grid:,} / {original_count:,} ({has_grid/original_count*100:.1f}%)")
    print(f"   Affected rooms: {has_affected_rooms:,} / {original_count:,} ({has_affected_rooms/original_count*100:.1f}%)")
    print(f"   Location ID coverage: {location_coverage:,} / {original_count:,} ({location_coverage/original_count*100:.1f}%)")
    print(f"   Location review needed: {review_count:,} / {original_count:,} ({review_count/original_count*100:.1f}%)")

    # 3. Improve trade coverage using CSI section
    print("\n3. Improving trade coverage using CSI sections...")
    original_trade_coverage = df['dim_trade_id'].notna().sum() if 'dim_trade_id' in df.columns else 0

    def improve_trade(row):
        """Fill missing trade using CSI section mapping."""
        # If we already have trade, keep it
        if 'dim_trade_id' in row and pd.notna(row.get('dim_trade_id')):
            return row.get('dim_trade_id')

        # Try to infer from CSI section
        csi_section = row.get('csi_section')
        if pd.notna(csi_section):
            # Get trade from CSI section
            # This would use CSI → trade mapping
            # For now, return None (would need CSI trade mapping table)
            pass

        return None

    if 'dim_trade_id' in df.columns:
        df['dim_trade_id'] = df.apply(improve_trade, axis=1)
    new_trade_coverage = df['dim_trade_id'].notna().sum() if 'dim_trade_id' in df.columns else 0
    print(f"   Trade coverage: {original_trade_coverage:,} → {new_trade_coverage:,} ({new_trade_coverage/original_count*100:.1f}%)")

    # Summary
    print("\n" + "=" * 70)
    print("ENRICHMENT SUMMARY")
    print("=" * 70)
    print(f"Total records: {original_count:,}")
    print(f"\nCoverage:")
    print(f"  CSI Division:        {df['csi_division'].notna().sum():,} ({df['csi_division'].notna().mean()*100:.1f}%)")
    print(f"  Location ID:         {df['dim_location_id'].notna().sum():,} ({df['dim_location_id'].notna().mean()*100:.1f}%)")
    if 'dim_company_id' in df.columns:
        print(f"  Company ID:          {df['dim_company_id'].notna().sum():,} ({df['dim_company_id'].notna().mean()*100:.1f}%)")
    if 'dim_trade_id' in df.columns:
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
