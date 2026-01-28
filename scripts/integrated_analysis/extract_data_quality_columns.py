#!/usr/bin/env python3
"""
Extract Data Quality Columns to Separate Tables.

Moves QC/validation columns from main fact tables into separate lookup tables
that can be hidden in Power BI while keeping the main tables clean.

Each QC table includes the primary key for joining back to the main table.

Input Files:
    - processed/raba/raba_consolidated.csv
    - processed/psi/psi_consolidated.csv
    - processed/tbm/work_entries_enriched.csv
    - processed/quality/qc_inspections_enriched.csv

Output Files:
    - processed/raba/raba_data_quality.csv
    - processed/psi/psi_data_quality.csv
    - processed/tbm/tbm_data_quality.csv
    - processed/quality/qc_inspections_data_quality.csv

Usage:
    python -m scripts.integrated_analysis.extract_data_quality_columns
    python -m scripts.integrated_analysis.extract_data_quality_columns --dry-run
    python -m scripts.integrated_analysis.extract_data_quality_columns --keep-originals
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
RABA_PSI_QC_COLUMNS = [
    '_validation_issues',
    'is_multi_party',
    'narrative_companies',
    'location_type',
    'location_code',
    'match_type',
]

TBM_QC_COLUMNS = [
    'grid_completeness',
    'match_quality',
    'location_review_flag',
    'location_source',
    'is_duplicate',
    'duplicate_group_id',
    'is_preferred',
    'date_mismatch',
    'room_code_extracted',
    'subcontractor_normalized',
]

QC_INSPECTIONS_QC_COLUMNS = [
    'grid_row',
    'grid_col',
    'location_type',
    'location_code',
    'match_type',
]


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
    ]


def extract_qc_columns(
    config: TableConfig,
    dry_run: bool = False,
    keep_originals: bool = False,
) -> dict:
    """
    Extract QC columns from a table into a separate file.

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

    # Main table without QC columns
    main_columns = [col for col in df.columns if col not in existing_qc_cols]
    df_main = df[main_columns].copy()

    result['columns_extracted'] = len(existing_qc_cols)
    result['columns_remaining'] = len(main_columns)

    print(f"    Extracted {len(existing_qc_cols)} QC columns: {existing_qc_cols}")
    print(f"    Main table: {len(main_columns)} columns remaining")

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
        choices=['raba', 'psi', 'tbm', 'qc_inspections', 'all'],
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

    success_count = sum(1 for r in results if r['status'] == 'success')
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
