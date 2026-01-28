#!/usr/bin/env python3
"""
Enrich Fact Tables with Unified Location Columns

Re-enriches all fact tables (RABA, PSI, TBM, QC Workbooks) with standardized
location columns using the centralized enrich_location() function.

Output columns added:
- dim_location_id: FK to dim_location
- location_type: ROOM/STAIR/ELEVATOR/GRIDLINE/LEVEL/BUILDING/UNDEFINED
- location_code: Matched code
- level: Normalized level (1F, 2F, etc.)
- grid_row_min, grid_row_max, grid_col_min, grid_col_max: Grid bounds
- affected_rooms: JSON array of rooms with grid overlap
- affected_rooms_count: Integer count
- match_type: How location was determined

Usage:
    python scripts/integrated_analysis/enrich_fact_tables_location.py
    python scripts/integrated_analysis/enrich_fact_tables_location.py --source raba
    python scripts/integrated_analysis/enrich_fact_tables_location.py --dry-run
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config.settings import Settings
from scripts.integrated_analysis.location import enrich_location
from scripts.shared.dimension_lookup import reset_cache


# Fact table configurations
FACT_TABLES = {
    'raba': {
        'input': Settings.PROCESSED_DATA_DIR / 'raba' / 'raba_consolidated.csv',
        'output': Settings.PROCESSED_DATA_DIR / 'raba' / 'raba_consolidated.csv',
        'building_col': 'building',
        'level_col': 'level',
        'grid_col': 'grid',
        'room_code_col': None,  # No direct room codes in RABA
    },
    'psi': {
        'input': Settings.PROCESSED_DATA_DIR / 'psi' / 'psi_consolidated.csv',
        'output': Settings.PROCESSED_DATA_DIR / 'psi' / 'psi_consolidated.csv',
        'building_col': 'building',
        'level_col': 'level',
        'grid_col': 'grid',
        'room_code_col': None,  # No direct room codes in PSI
    },
    'tbm': {
        'input': Settings.PROCESSED_DATA_DIR / 'tbm' / 'tbm_with_csi.csv',
        'output': Settings.PROCESSED_DATA_DIR / 'tbm' / 'tbm_with_csi.csv',
        'building_col': 'location_building',
        'level_col': 'location_level',
        'grid_col': 'grid_raw',
        'room_code_col': None,
    },
    'qc_workbooks': {
        'input': Settings.PROCESSED_DATA_DIR / 'quality' / 'qc_inspections_enriched.csv',
        'output': Settings.PROCESSED_DATA_DIR / 'quality' / 'qc_inspections_enriched.csv',
        'building_col': 'building',
        'level_col': 'level',
        'grid_col': 'grid',
        'room_code_col': None,
    },
}

# Location columns to add/update
LOCATION_COLUMNS = [
    'dim_location_id',
    'location_type',
    'location_code',
    'level',
    'grid_row_min',
    'grid_row_max',
    'grid_col_min',
    'grid_col_max',
    'grid_source',
    'affected_rooms',
    'affected_rooms_count',
    'match_type',
]

# Old columns to remove (replaced by new schema)
OLD_COLUMNS_TO_REMOVE = [
    'building_level',
    'building_normalized',
    'level_normalized',
    'grid_completeness',
    'match_quality',
    'location_review_flag',
    'location_source',
    'grid_normalized',
    'location_id',  # Replaced by dim_location_id
]


def enrich_fact_table(
    config: dict,
    source_name: str,
    dry_run: bool = False,
) -> dict:
    """
    Enrich a single fact table with location columns.

    Returns:
        Dict with statistics
    """
    input_path = config['input']
    output_path = config['output']

    print(f"\n{'='*60}")
    print(f"Enriching {source_name.upper()}")
    print(f"{'='*60}")

    if not input_path.exists():
        print(f"  ERROR: Input file not found: {input_path}")
        return {'error': 'File not found'}

    # Load data
    print(f"  Loading: {input_path}")
    df = pd.read_csv(input_path, low_memory=False)
    print(f"  Rows: {len(df):,}")

    # Reset dimension cache
    reset_cache()

    # Remove old location columns if present
    cols_to_remove = [c for c in OLD_COLUMNS_TO_REMOVE if c in df.columns]
    if cols_to_remove:
        print(f"  Removing old columns: {cols_to_remove}")
        df = df.drop(columns=cols_to_remove)

    # Enrich each row
    print(f"  Enriching locations...")
    results = []
    total = len(df)

    building_col = config['building_col']
    level_col = config['level_col']
    grid_col = config['grid_col']
    room_code_col = config.get('room_code_col')

    for idx, row in df.iterrows():
        result = enrich_location(
            building=row.get(building_col) if building_col in df.columns else None,
            level=row.get(level_col) if level_col in df.columns else None,
            grid=row.get(grid_col) if grid_col in df.columns else None,
            room_code=row.get(room_code_col) if room_code_col and room_code_col in df.columns else None,
            source=source_name.upper(),
        )
        results.append(result.to_dict())

        if (idx + 1) % 5000 == 0:
            print(f"    {idx + 1:,}/{total:,} rows...")

    # Add new columns
    results_df = pd.DataFrame(results)
    for col in LOCATION_COLUMNS:
        if col in results_df.columns:
            df[col] = results_df[col].values

    # Calculate statistics
    stats = {
        'total_rows': len(df),
        'dim_location_id_coverage': (df['dim_location_id'].notna().sum() / len(df) * 100),
        'match_type_dist': df['match_type'].value_counts().to_dict(),
        'location_type_dist': df['location_type'].value_counts().to_dict(),
        'affected_rooms_with_count': (df['affected_rooms_count'] > 0).sum(),
    }

    print(f"\n  Statistics:")
    print(f"    Total rows: {stats['total_rows']:,}")
    print(f"    dim_location_id coverage: {stats['dim_location_id_coverage']:.1f}%")
    print(f"    Rows with affected rooms: {stats['affected_rooms_with_count']:,}")
    print(f"\n    Match type distribution:")
    for mt, count in stats['match_type_dist'].items():
        print(f"      {mt}: {count:,}")
    print(f"\n    Location type distribution:")
    for lt, count in stats['location_type_dist'].items():
        print(f"      {lt}: {count:,}")

    # Save output
    if dry_run:
        print(f"\n  [DRY RUN] Would write to: {output_path}")
    else:
        df.to_csv(output_path, index=False)
        print(f"\n  Wrote {len(df):,} rows to: {output_path}")

    return stats


def main():
    parser = argparse.ArgumentParser(
        description='Enrich fact tables with unified location columns'
    )
    parser.add_argument(
        '--source',
        choices=['raba', 'psi', 'tbm', 'qc_workbooks', 'all'],
        default='all',
        help='Which source to enrich (default: all)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview changes without writing files'
    )
    args = parser.parse_args()

    print("=" * 60)
    print("FACT TABLE LOCATION ENRICHMENT")
    print("=" * 60)

    # Determine which sources to process
    if args.source == 'all':
        sources = list(FACT_TABLES.keys())
    else:
        sources = [args.source]

    all_stats = {}

    for source in sources:
        config = FACT_TABLES[source]
        stats = enrich_fact_table(config, source, dry_run=args.dry_run)
        all_stats[source] = stats

    # Print summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    for source, stats in all_stats.items():
        if 'error' in stats:
            print(f"\n{source.upper()}: ERROR - {stats['error']}")
        else:
            print(f"\n{source.upper()}:")
            print(f"  Rows: {stats['total_rows']:,}")
            print(f"  Coverage: {stats['dim_location_id_coverage']:.1f}%")


if __name__ == '__main__':
    main()
