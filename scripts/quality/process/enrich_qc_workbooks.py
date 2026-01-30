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
    processed/quality/qc_inspections_data_quality.csv (data quality columns)
"""

import pandas as pd
import json
from pathlib import Path

from src.config.settings import Settings
from scripts.shared.dimension_lookup import (
    get_csi_division,
)
from scripts.shared.pipeline_utils import get_output_path, write_fact_and_quality
from scripts.integrated_analysis.location import enrich_location


# =============================================================================
# Data quality columns - moved to separate table for Power BI cleanliness
# =============================================================================

QC_INSPECTIONS_DATA_QUALITY_COLUMNS = [
    # Grid bounds (technical)
    'grid_row_min',
    'grid_row_max',
    'grid_col_min',
    'grid_col_max',
    'grid_source',
    # Match metadata
    'match_type',
    # Source-specific columns (yates_*)
    'yates_time',
    'yates_wir_number',
    'yates_rep',
    'yates_3rd_party',
    'yates_secai_cm',
    'yates_inspection_comment',
    'yates_category',
    # Source-specific columns (secai_*)
    'secai_discipline',
    'secai_number',
    'secai_request_date',
    'secai_revision',
    'secai_building_type',
    'secai_module',
]


def enrich_qc_inspections(dry_run: bool = False, staging_dir: Path = None):
    """
    Enrich QC inspection data with improved location, CSI division, trade, and affected rooms.

    Args:
        dry_run: If True, don't write output files
        staging_dir: If provided, write outputs to staging directory
    """
    settings = Settings()

    input_path = settings.PROCESSED_DATA_DIR / 'quality' / 'enriched' / 'combined_qc_inspections.csv'

    # Output paths (staging or final)
    fact_path = get_output_path('quality/qc_inspections_enriched.csv', staging_dir)
    quality_path = get_output_path('quality/qc_inspections_data_quality.csv', staging_dir)

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
            'location_type': loc.location_type,
            'location_code': loc.location_code,
            'level_normalized': loc.level,
            'grid_row_min': loc.grid_row_min,
            'grid_row_max': loc.grid_row_max,
            'grid_col_min': loc.grid_col_min,
            'grid_col_max': loc.grid_col_max,
            'grid_source': loc.grid_source,
            'affected_rooms': loc.affected_rooms,
            'affected_rooms_count': loc.affected_rooms_count,
            'match_type': loc.match_type,
        })

    # Apply location enrichment
    location_cols = df.apply(apply_location_enrichment, axis=1)

    # Update dim_location_id with new values (preserving original if new is None)
    df['dim_location_id'] = location_cols['dim_location_id_new'].combine_first(df.get('dim_location_id'))
    df['location_type'] = location_cols['location_type']
    df['location_code'] = location_cols['location_code']
    df['level'] = location_cols['level_normalized']  # Normalized level
    df['grid_row_min'] = location_cols['grid_row_min']
    df['grid_row_max'] = location_cols['grid_row_max']
    df['grid_col_min'] = location_cols['grid_col_min']
    df['grid_col_max'] = location_cols['grid_col_max']
    df['grid_source'] = location_cols['grid_source']
    df['affected_rooms'] = location_cols['affected_rooms']
    df['affected_rooms_count'] = location_cols['affected_rooms_count']
    df['match_type'] = location_cols['match_type']

    # Coverage stats
    has_grid = (df['grid_row_min'].notna() & df['grid_col_min'].notna()).sum()
    has_affected_rooms = df['affected_rooms'].notna().sum()
    location_coverage = df['dim_location_id'].notna().sum()

    print(f"   Grid coordinates: {has_grid:,} / {original_count:,} ({has_grid/original_count*100:.1f}%)")
    print(f"   Affected rooms: {has_affected_rooms:,} / {original_count:,} ({has_affected_rooms/original_count*100:.1f}%)")
    print(f"   Location ID coverage: {location_coverage:,} / {original_count:,} ({location_coverage/original_count*100:.1f}%)")

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

    if not dry_run:
        print(f"\nWriting fact table to: {fact_path}")
        print(f"Writing data quality table to: {quality_path}")

        # Ensure inspection_id exists as primary key
        if 'inspection_id' not in df.columns:
            df['inspection_id'] = df.index.astype(str)

        fact_rows, quality_cols = write_fact_and_quality(
            df=df,
            primary_key='inspection_id',
            quality_columns=QC_INSPECTIONS_DATA_QUALITY_COLUMNS,
            fact_path=fact_path,
            quality_path=quality_path,
        )
        print(f"Wrote {fact_rows:,} rows, moved {quality_cols} columns to data quality table")
    else:
        print("\nDRY RUN - no files written")

    return df


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Enrich QC workbooks with improved location and CSI data')
    parser.add_argument('--dry-run', action='store_true', help='Preview without saving')
    parser.add_argument('--staging-dir', type=Path, default=None,
                        help='Write outputs to staging directory instead of final location')
    args = parser.parse_args()

    enrich_qc_inspections(dry_run=args.dry_run, staging_dir=args.staging_dir)


if __name__ == '__main__':
    main()
