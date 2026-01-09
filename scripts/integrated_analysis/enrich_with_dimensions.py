#!/usr/bin/env python3
"""
Enrich Data Sources with Dimension IDs

Adds dim_location_id, dim_company_id, dim_trade_id to all processed data sources.
Creates enriched copies of each file with _enriched suffix.

Usage:
    python scripts/integrated_analysis/enrich_with_dimensions.py
    python scripts/integrated_analysis/enrich_with_dimensions.py --source tbm
    python scripts/integrated_analysis/enrich_with_dimensions.py --dry-run
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, Any, Optional, Callable

import pandas as pd

# Add project root to path
_project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import Settings
from scripts.shared.dimension_lookup import (
    get_location_id,
    get_company_id,
    get_trade_id,
    get_trade_code,
    reset_cache,
)


def enrich_tbm(dry_run: bool = False) -> Dict[str, Any]:
    """Enrich TBM work_entries.csv with dimension IDs."""
    input_path = Settings.PROCESSED_DATA_DIR / 'tbm' / 'work_entries.csv'
    output_path = Settings.PROCESSED_DATA_DIR / 'tbm' / 'work_entries_enriched.csv'

    if not input_path.exists():
        return {'status': 'skipped', 'reason': 'file not found'}

    df = pd.read_csv(input_path)
    original_count = len(df)

    # Normalize building codes
    building_map = {'FAB': 'FAB', 'SUP': 'SUE', 'Fab': 'FAB', 'OFFICE': None, 'Laydown': None}
    df['building_normalized'] = df['location_building'].map(
        lambda x: building_map.get(x, x) if pd.notna(x) else None
    )

    # Normalize level codes (e.g., "1F" -> "1F", "RF" -> "ROOF")
    def normalize_level(level):
        if pd.isna(level):
            return None
        level = str(level).upper().strip()
        if level == 'RF':
            return 'ROOF'
        return level

    df['level_normalized'] = df['location_level'].apply(normalize_level)

    # Add dimension IDs
    df['dim_location_id'] = df.apply(
        lambda row: get_location_id(row['building_normalized'], row['level_normalized']),
        axis=1
    )
    df['dim_company_id'] = df['tier2_sc'].apply(get_company_id)

    # Infer trade from work activities (simplified mapping)
    def infer_trade_from_activity(activity):
        if pd.isna(activity):
            return None
        activity_lower = str(activity).lower()
        if any(x in activity_lower for x in ['concrete', 'pour', 'slab', 'form', 'strip']):
            return 'Concrete'
        if any(x in activity_lower for x in ['steel', 'erect', 'deck', 'weld']):
            return 'Structural Steel'
        if any(x in activity_lower for x in ['drywall', 'frame', 'stud', 'gyp']):
            return 'Drywall'
        if any(x in activity_lower for x in ['paint', 'coat', 'finish']):
            return 'Finishes'
        if any(x in activity_lower for x in ['fireproof', 'firestop', 'sfrm']):
            return 'Fire Protection'
        if any(x in activity_lower for x in ['insul']):
            return 'Insulation'
        if any(x in activity_lower for x in ['roof', 'membrane']):
            return 'Roofing'
        if any(x in activity_lower for x in ['panel', 'clad']):
            return 'Panels'
        if any(x in activity_lower for x in ['mep', 'hvac', 'plumb', 'elec', 'pipe']):
            return 'MEP'
        return None

    df['trade_inferred'] = df['work_activities'].apply(infer_trade_from_activity)
    df['dim_trade_id'] = df['trade_inferred'].apply(get_trade_id)
    df['dim_trade_code'] = df['dim_trade_id'].apply(get_trade_code)

    # Calculate coverage
    coverage = {
        'location': df['dim_location_id'].notna().mean() * 100,
        'company': df['dim_company_id'].notna().mean() * 100,
        'trade': df['dim_trade_id'].notna().mean() * 100,
    }

    if not dry_run:
        df.to_csv(output_path, index=False)

    return {
        'status': 'success',
        'records': original_count,
        'coverage': coverage,
        'output': str(output_path) if not dry_run else 'DRY RUN',
    }


def enrich_projectsight(dry_run: bool = False) -> Dict[str, Any]:
    """Enrich ProjectSight labor_entries.csv with dimension IDs."""
    input_path = Settings.PROCESSED_DATA_DIR / 'projectsight' / 'labor_entries.csv'
    output_path = Settings.PROCESSED_DATA_DIR / 'projectsight' / 'labor_entries_enriched.csv'

    if not input_path.exists():
        return {'status': 'skipped', 'reason': 'file not found'}

    print("  Loading data...")
    df = pd.read_csv(input_path)
    original_count = len(df)

    # ProjectSight has no location data - only company and trade
    df['dim_location_id'] = None  # No location available

    # Build lookup dictionaries for fast vectorized mapping
    print("  Building company lookup...")
    unique_companies = df['company'].dropna().unique()
    company_lookup = {c: get_company_id(c) for c in unique_companies}
    df['dim_company_id'] = df['company'].map(company_lookup)

    print("  Building trade lookup...")
    unique_trades = df['trade_name'].dropna().unique()
    trade_lookup = {t: get_trade_id(t) for t in unique_trades}
    trade_code_lookup = {t: get_trade_code(trade_lookup.get(t)) for t in unique_trades}
    df['dim_trade_id'] = df['trade_name'].map(trade_lookup)
    df['dim_trade_code'] = df['trade_name'].map(trade_code_lookup)

    # Calculate coverage
    coverage = {
        'location': 0.0,  # No location in ProjectSight
        'company': df['dim_company_id'].notna().mean() * 100,
        'trade': df['dim_trade_id'].notna().mean() * 100,
    }

    if not dry_run:
        print("  Writing output...")
        df.to_csv(output_path, index=False)

    return {
        'status': 'success',
        'records': original_count,
        'coverage': coverage,
        'output': str(output_path) if not dry_run else 'DRY RUN',
    }


def enrich_weekly_labor(dry_run: bool = False) -> Dict[str, Any]:
    """Enrich Weekly Reports labor_detail_by_company.csv with dimension IDs."""
    input_path = Settings.PROCESSED_DATA_DIR / 'weekly_reports' / 'labor_detail_by_company.csv'
    output_path = Settings.PROCESSED_DATA_DIR / 'weekly_reports' / 'labor_detail_by_company_enriched.csv'

    if not input_path.exists():
        return {'status': 'skipped', 'reason': 'file not found'}

    df = pd.read_csv(input_path)
    original_count = len(df)

    # Only has company data
    df['dim_location_id'] = None
    df['dim_company_id'] = df['company'].apply(get_company_id)
    df['dim_trade_id'] = None
    df['dim_trade_code'] = None

    # Calculate coverage
    coverage = {
        'location': 0.0,
        'company': df['dim_company_id'].notna().mean() * 100,
        'trade': 0.0,
    }

    if not dry_run:
        df.to_csv(output_path, index=False)

    return {
        'status': 'success',
        'records': original_count,
        'coverage': coverage,
        'output': str(output_path) if not dry_run else 'DRY RUN',
    }


# Define all enrichment tasks
ENRICHMENT_TASKS = {
    'tbm': ('TBM Daily Plans', enrich_tbm),
    'projectsight': ('ProjectSight Labor', enrich_projectsight),
    'weekly_labor': ('Weekly Reports Labor', enrich_weekly_labor),
}


def main():
    parser = argparse.ArgumentParser(description='Enrich data sources with dimension IDs')
    parser.add_argument('--source', choices=list(ENRICHMENT_TASKS.keys()),
                       help='Enrich only this source')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be done without writing files')
    args = parser.parse_args()

    # Reset dimension cache to ensure fresh data
    reset_cache()

    # Determine which sources to process
    if args.source:
        sources = {args.source: ENRICHMENT_TASKS[args.source]}
    else:
        sources = ENRICHMENT_TASKS

    print("=" * 70)
    print("DIMENSION ENRICHMENT")
    print("=" * 70)

    results = {}
    for key, (name, func) in sources.items():
        print(f"\nProcessing: {name}")
        print("-" * 40)

        result = func(dry_run=args.dry_run)
        results[key] = result

        if result['status'] == 'success':
            print(f"  Records: {result['records']:,}")
            print(f"  Coverage:")
            for dim, pct in result['coverage'].items():
                print(f"    {dim}: {pct:.1f}%")
            print(f"  Output: {result['output']}")
        else:
            print(f"  Status: {result['status']}")
            print(f"  Reason: {result.get('reason', 'unknown')}")

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"\n{'Source':<20} {'Records':>12} {'Location':>10} {'Company':>10} {'Trade':>10}")
    print("-" * 70)

    for key, result in results.items():
        name = ENRICHMENT_TASKS[key][0]
        if result['status'] == 'success':
            print(f"{name:<20} {result['records']:>12,} {result['coverage']['location']:>9.1f}% {result['coverage']['company']:>9.1f}% {result['coverage']['trade']:>9.1f}%")
        else:
            print(f"{name:<20} {'SKIPPED':>12}")


if __name__ == '__main__':
    main()
