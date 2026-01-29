#!/usr/bin/env python3
"""
Extract Data Quality Columns to Separate Tables.

Moves QC/validation columns from main fact tables into separate lookup tables
that can be hidden in Power BI while keeping the main tables clean.

Each QC table includes the primary key for joining back to the main table.

Input/Output Files:
    - processed/raba/raba_consolidated.csv → raba_data_quality.csv
    - processed/psi/psi_consolidated.csv → psi_data_quality.csv
    - processed/tbm/work_entries_enriched.csv → tbm_data_quality.csv
    - processed/quality/qc_inspections_enriched.csv → qc_inspections_data_quality.csv
    - processed/primavera/p6_task_taxonomy.csv → p6_task_taxonomy_data_quality.csv

Columns moved to data quality tables:
    - Raw/unnormalized values (*_raw)
    - Grid bounds (grid_row_min, grid_col_min, etc.)
    - Inference source tracking (*_source)
    - Affected rooms JSON
    - Validation flags

Usage:
    python -m scripts.integrated_analysis.extract_data_quality_columns
    python -m scripts.integrated_analysis.extract_data_quality_columns --dry-run
    python -m scripts.integrated_analysis.extract_data_quality_columns --keep-originals
    python -m scripts.integrated_analysis.extract_data_quality_columns --table p6_taxonomy
"""

import argparse
import sys
from pathlib import Path
from typing import NamedTuple

import pandas as pd

# Add project root to path
_project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import settings


class TableConfig(NamedTuple):
    """Configuration for extracting QC columns from a table."""
    source_path: Path
    output_main_path: Path  # Same as source (overwrite) or new path
    output_qc_path: Path
    primary_key: str
    qc_columns: list[str]
    description: str


# Define QC columns for each table
# These columns are moved to separate QC tables to keep main fact tables user-friendly
# Main tables focus on: source document data (cleaned) + dimension FKs + IDs
#
# NOTE: RABA, PSI, and TBM schemas have already been simplified.
# The QC column lists below define what WOULD be moved if those columns existed.
# Currently only QC Inspections and P6 Task Taxonomy have QC columns to extract.

RABA_PSI_QC_COLUMNS = [
    # Currently RABA/PSI consolidated files are already clean.
    # These columns would be extracted if they existed:
    'grid_row_min',
    'grid_row_max',
    'grid_col_min',
    'grid_col_max',
    'grid_source',
    'affected_rooms',
    'affected_rooms_count',
    'location_type',
    'location_code',
    'match_type',
    'csi_inference_source',
]

TBM_QC_COLUMNS = [
    # Currently TBM work_entries_enriched is already clean.
    # These columns would be extracted if they existed:
    'grid_row_min',
    'grid_row_max',
    'grid_col_min',
    'grid_col_max',
    'affected_rooms',
    'affected_rooms_count',
    'csi_inference_source',
]

QC_INSPECTIONS_QC_COLUMNS = [
    # === Raw data ===
    'location_raw',
    'status',                # Keep status_normalized
    'contractor_raw',

    # === Date parts (compute in Power BI) ===
    'year',
    'month',
    'week',
    'day_of_week',

    # === Grid bounds (technical) ===
    'grid_row',
    'grid_col',
    'grid_row_min',
    'grid_row_max',
    'grid_col_min',
    'grid_col_max',
    'grid_source',

    # === Room matching ===
    'affected_rooms',
    'affected_rooms_count',

    # === Location inference metadata ===
    'location_type',
    'location_code',
    'match_type',

    # === CSI inference metadata ===
    'csi_inference_source',
]

P6_TASK_TAXONOMY_QC_COLUMNS = [
    # === Source tracking columns (how each field was derived) ===
    'trade_source',
    'sub_trade_source',
    'building_source',
    'level_source',
    'area_source',
    'room_source',
    'sub_source',
    'phase_source',
    'work_phase_source',
    'impact_source',
    'csi_inference_source',
    'grid_source',

    # === Grid bounds (technical) ===
    'grid_row_min',
    'grid_row_max',
    'grid_col_min',
    'grid_col_max',

    # === Intermediate/redundant location fields ===
    'loc_type',              # Keep location_type
    'loc_type_desc',
    'loc_id',                # Keep dim_location_id
    'Building Code Desc',    # Keep building_desc
    'location',              # Composite field

    # === Legacy trade fields (keep CSI instead) ===
    'trade_id',
    'trade_code',
    'trade_name',

    # === Description fields (verbose, derive from codes in Power BI) ===
    'scope_desc',
    'building_desc',
    'level_desc',
    'phase_desc',
    'impact_type_desc',
    'attributed_to_desc',
    'root_cause_desc',
]

# Column renames to apply after extraction (for user-friendliness)
# Format: {table_description: {old_name: new_name}}
# NOTE: Most tables have already been simplified and don't need renames.
COLUMN_RENAMES = {
    'QC Inspections Enriched': {
        'status_normalized': 'status',
    },
}


def get_table_configs() -> list[TableConfig]:
    """Get configuration for all tables to process."""
    return [
        TableConfig(
            source_path=settings.RABA_PROCESSED_DIR / 'raba_consolidated.csv',
            output_main_path=settings.RABA_PROCESSED_DIR / 'raba_consolidated.csv',
            output_qc_path=settings.RABA_PROCESSED_DIR / 'raba_data_quality.csv',
            primary_key='inspection_id',
            qc_columns=RABA_PSI_QC_COLUMNS,
            description='RABA Consolidated',
        ),
        TableConfig(
            source_path=settings.PSI_PROCESSED_DIR / 'psi_consolidated.csv',
            output_main_path=settings.PSI_PROCESSED_DIR / 'psi_consolidated.csv',
            output_qc_path=settings.PSI_PROCESSED_DIR / 'psi_data_quality.csv',
            primary_key='inspection_id',
            qc_columns=RABA_PSI_QC_COLUMNS,
            description='PSI Consolidated',
        ),
        TableConfig(
            source_path=settings.TBM_PROCESSED_DIR / 'work_entries_enriched.csv',
            output_main_path=settings.TBM_PROCESSED_DIR / 'work_entries_enriched.csv',
            output_qc_path=settings.TBM_PROCESSED_DIR / 'tbm_data_quality.csv',
            primary_key='tbm_work_entry_id',
            qc_columns=TBM_QC_COLUMNS,
            description='TBM Work Entries',
        ),
        TableConfig(
            source_path=settings.PROCESSED_DATA_DIR / 'quality' / 'qc_inspections_enriched.csv',
            output_main_path=settings.PROCESSED_DATA_DIR / 'quality' / 'qc_inspections_enriched.csv',
            output_qc_path=settings.PROCESSED_DATA_DIR / 'quality' / 'qc_inspections_data_quality.csv',
            primary_key='inspection_id',
            qc_columns=QC_INSPECTIONS_QC_COLUMNS,
            description='QC Inspections Enriched',
        ),
        TableConfig(
            source_path=settings.PRIMAVERA_PROCESSED_DIR / 'p6_task_taxonomy.csv',
            output_main_path=settings.PRIMAVERA_PROCESSED_DIR / 'p6_task_taxonomy.csv',
            output_qc_path=settings.PRIMAVERA_PROCESSED_DIR / 'p6_task_taxonomy_data_quality.csv',
            primary_key='task_id',
            qc_columns=P6_TASK_TAXONOMY_QC_COLUMNS,
            description='P6 Task Taxonomy',
        ),
    ]


def extract_qc_columns(
    config: TableConfig,
    dry_run: bool = False,
    keep_originals: bool = False,
) -> dict:
    """
    Extract QC columns from a table into a separate file.

    If a QC file already exists, merges new columns with existing ones
    to avoid data loss from incremental extractions.

    Args:
        config: Table configuration
        dry_run: If True, don't write files
        keep_originals: If True, don't modify the source file

    Returns:
        Dict with extraction statistics
    """
    result = {
        'table': config.description,
        'source': str(config.source_path),
        'status': 'skipped',
        'rows': 0,
        'columns_extracted': 0,
        'columns_remaining': 0,
    }

    # Check if source exists
    if not config.source_path.exists():
        result['status'] = 'missing'
        print(f"  ⚠ Source not found: {config.source_path}")
        return result

    # Read source file
    print(f"  Reading {config.source_path.name}...")
    df = pd.read_csv(config.source_path, low_memory=False)
    result['rows'] = len(df)

    # Find which QC columns exist in this table
    existing_qc_cols = [col for col in config.qc_columns if col in df.columns]
    missing_qc_cols = [col for col in config.qc_columns if col not in df.columns]

    if missing_qc_cols:
        print(f"    Note: {len(missing_qc_cols)} QC columns not in source: {missing_qc_cols}")

    if not existing_qc_cols:
        # Even if no QC columns to extract, still apply renames
        renames = COLUMN_RENAMES.get(config.description, {})
        cols_to_rename = {k: v for k, v in renames.items() if k in df.columns}
        if cols_to_rename and not dry_run:
            df_renamed = df.rename(columns=cols_to_rename)
            df_renamed.to_csv(config.output_main_path, index=False)
            print(f"    Renamed {len(cols_to_rename)} columns: {cols_to_rename}")
            result['status'] = 'renamed_only'
            result['columns_remaining'] = len(df_renamed.columns)
            return result
        result['status'] = 'no_qc_columns'
        print(f"    No QC columns found to extract")
        return result

    # Check primary key exists
    if config.primary_key not in df.columns:
        result['status'] = 'missing_pk'
        print(f"  ⚠ Primary key '{config.primary_key}' not found in {config.source_path.name}")
        return result

    # Extract QC table (PK + QC columns)
    qc_columns_with_pk = [config.primary_key] + existing_qc_cols
    df_qc = df[qc_columns_with_pk].copy()

    # Check if QC file already exists - merge with existing data
    if config.output_qc_path.exists():
        print(f"    Found existing {config.output_qc_path.name}, merging...")
        df_qc_existing = pd.read_csv(config.output_qc_path, low_memory=False)

        # Get columns that exist in old file but not in new extraction
        cols_to_keep = [col for col in df_qc_existing.columns
                        if col not in df_qc.columns and col != config.primary_key]

        if cols_to_keep:
            print(f"    Preserving {len(cols_to_keep)} existing QC columns: {cols_to_keep}")
            # Merge on primary key
            df_qc = df_qc.merge(
                df_qc_existing[[config.primary_key] + cols_to_keep],
                on=config.primary_key,
                how='left'
            )
            existing_qc_cols = existing_qc_cols + cols_to_keep

    # Main table without QC columns
    main_columns = [col for col in df.columns if col not in existing_qc_cols]
    df_main = df[main_columns].copy()

    result['columns_extracted'] = len(existing_qc_cols)
    result['columns_remaining'] = len(main_columns)

    # Apply column renames for user-friendliness
    renames = COLUMN_RENAMES.get(config.description, {})
    if renames:
        cols_to_rename = {k: v for k, v in renames.items() if k in df_main.columns}
        if cols_to_rename:
            df_main = df_main.rename(columns=cols_to_rename)
            print(f"    Renamed {len(cols_to_rename)} columns: {cols_to_rename}")

    print(f"    Extracted {len(existing_qc_cols)} QC columns")
    print(f"    Main table: {len(df_main.columns)} columns remaining")

    if dry_run:
        result['status'] = 'dry_run'
        print(f"    [DRY RUN] Would write:")
        print(f"      - {config.output_qc_path}")
        if not keep_originals:
            print(f"      - {config.output_main_path} (updated)")
    else:
        # Write QC table
        df_qc.to_csv(config.output_qc_path, index=False)
        print(f"    ✓ Wrote {config.output_qc_path.name}")

        # Update main table (unless keeping originals)
        if not keep_originals:
            df_main.to_csv(config.output_main_path, index=False)
            print(f"    ✓ Updated {config.output_main_path.name}")
        else:
            print(f"    ○ Kept original {config.output_main_path.name} unchanged")

        result['status'] = 'success'

    return result


def main():
    parser = argparse.ArgumentParser(
        description='Extract data quality columns to separate tables'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without writing files'
    )
    parser.add_argument(
        '--keep-originals',
        action='store_true',
        help='Create QC tables but do not modify original files'
    )
    parser.add_argument(
        '--table',
        choices=['raba', 'psi', 'tbm', 'qc_inspections', 'p6_taxonomy', 'all'],
        default='all',
        help='Which table(s) to process (default: all)'
    )

    args = parser.parse_args()

    print("=" * 70)
    print("Extract Data Quality Columns")
    print("=" * 70)

    if args.dry_run:
        print("Mode: DRY RUN (no files will be modified)")
    elif args.keep_originals:
        print("Mode: CREATE QC TABLES ONLY (originals unchanged)")
    else:
        print("Mode: EXTRACT AND UPDATE (QC columns removed from main tables)")
    print()

    configs = get_table_configs()

    # Filter by table if specified
    if args.table != 'all':
        table_map = {
            'raba': 'RABA Consolidated',
            'psi': 'PSI Consolidated',
            'tbm': 'TBM Work Entries',
            'qc_inspections': 'QC Inspections Enriched',
            'p6_taxonomy': 'P6 Task Taxonomy',
        }
        target = table_map[args.table]
        configs = [c for c in configs if c.description == target]

    results = []
    for config in configs:
        print(f"\n[{config.description}]")
        result = extract_qc_columns(
            config,
            dry_run=args.dry_run,
            keep_originals=args.keep_originals,
        )
        results.append(result)

    # Summary
    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)

    success_count = sum(1 for r in results if r['status'] in ('success', 'renamed_only'))
    dry_run_count = sum(1 for r in results if r['status'] == 'dry_run')
    skip_count = sum(1 for r in results if r['status'] in ('missing', 'no_qc_columns', 'missing_pk'))

    print(f"\nProcessed: {len(results)} tables")
    if args.dry_run:
        print(f"  Would extract: {dry_run_count}")
    else:
        print(f"  Extracted: {success_count}")
    print(f"  Skipped: {skip_count}")

    print("\nDetails:")
    for r in results:
        status_icon = {
            'success': '✓',
            'renamed_only': '✓',
            'dry_run': '○',
            'missing': '⚠',
            'no_qc_columns': '○',
            'missing_pk': '⚠',
            'skipped': '○',
        }.get(r['status'], '?')

        print(f"  {status_icon} {r['table']}: {r['rows']:,} rows, "
              f"{r['columns_extracted']} QC cols extracted, "
              f"{r['columns_remaining']} cols remaining")

    if not args.dry_run and success_count > 0:
        print("\n✓ Data quality columns extracted successfully.")
        print("  In Power BI, you can now hide the *_data_quality.csv tables.")


if __name__ == '__main__':
    main()
