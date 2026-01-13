#!/usr/bin/env python3
"""
Verify DIM table coverage across RABA, PSI, TBM, and ProjectSight.

Calculates the percentage of records that have matching dimension IDs
for location, company, and trade.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from src.config.settings import settings


def load_enriched_data():
    """Load all enriched data files."""
    processed_dir = settings.PROCESSED_DATA_DIR

    data = {}

    # RABA
    raba_path = processed_dir / 'raba' / 'raba_consolidated.csv'
    if raba_path.exists():
        data['RABA'] = pd.read_csv(raba_path)
        print(f"Loaded RABA: {len(data['RABA']):,} records")

    # PSI
    psi_path = processed_dir / 'psi' / 'psi_consolidated.csv'
    if psi_path.exists():
        data['PSI'] = pd.read_csv(psi_path)
        print(f"Loaded PSI: {len(data['PSI']):,} records")

    # TBM
    tbm_path = processed_dir / 'tbm' / 'work_entries_enriched.csv'
    if tbm_path.exists():
        data['TBM'] = pd.read_csv(tbm_path)
        print(f"Loaded TBM: {len(data['TBM']):,} records")

    # ProjectSight
    ps_path = processed_dir / 'projectsight' / 'labor_entries_enriched.csv'
    if ps_path.exists():
        data['ProjectSight'] = pd.read_csv(ps_path)
        print(f"Loaded ProjectSight: {len(data['ProjectSight']):,} records")

    return data


def calculate_coverage(df, col_name):
    """Calculate coverage percentage for a column."""
    total = len(df)
    if col_name not in df.columns:
        return None, 0, total

    # Count non-null values (dimension IDs)
    mapped = df[col_name].notna().sum()
    coverage = (mapped / total * 100) if total > 0 else 0

    return coverage, mapped, total


def analyze_unmapped(df, dim_col, source_col, source_name, top_n=10):
    """Analyze unmapped values for a dimension."""
    if dim_col not in df.columns:
        return []
    if source_col is None or source_col not in df.columns:
        return []

    unmapped = df[df[dim_col].isna()]
    if len(unmapped) == 0:
        return []

    # Get value counts of unmapped source values (excluding nulls)
    unmapped_vals = unmapped[source_col].dropna()
    if len(unmapped_vals) == 0:
        return []
    unmapped_values = unmapped_vals.value_counts().head(top_n)
    return [(val, cnt) for val, cnt in unmapped_values.items()]


def main():
    print("=" * 80)
    print("DIM TABLE COVERAGE VERIFICATION")
    print("=" * 80)
    print()

    # Load data
    data = load_enriched_data()
    print()

    if not data:
        print("ERROR: No enriched data files found!")
        return 1

    # Define column mappings for each source
    # Format: (dim_col, source_col for analysis)
    column_maps = {
        'RABA': {
            'location': ('dim_location_id', 'building'),
            'company': ('dim_company_id', 'contractor'),
            'trade': ('dim_trade_id', 'test_type'),
            'grid': ('grid_row_min', 'grid'),  # Special: grid has row/col bounds
        },
        'PSI': {
            'location': ('dim_location_id', 'building'),
            'company': ('dim_company_id', 'contractor'),
            'trade': ('dim_trade_id', 'trade'),
            'grid': ('grid_row_min', 'grid'),
        },
        'TBM': {
            'location': ('dim_location_id', 'building_normalized'),
            'company': ('dim_company_id', 'tier2_sc'),
            'trade': ('dim_trade_id', 'trade_inferred'),
        },
        'ProjectSight': {
            'location': ('dim_location_id', None),  # No location in source
            'company': ('dim_company_id', 'company'),
            'trade': ('dim_trade_id', 'trade_name'),
        },
    }

    # Calculate coverage matrix
    results = {}
    for source_name, df in data.items():
        results[source_name] = {
            'records': len(df),
            'location': None,
            'company': None,
            'trade': None,
            'grid': None,
        }

        col_map = column_maps.get(source_name, {})

        for dim_name, (dim_col, source_col) in col_map.items():
            coverage, mapped, total = calculate_coverage(df, dim_col)
            if coverage is not None:
                results[source_name][dim_name] = {
                    'coverage': coverage,
                    'mapped': mapped,
                    'total': total,
                    'dim_col': dim_col,
                    'source_col': source_col,
                }

    # Print coverage matrix
    print("=" * 80)
    print("COVERAGE MATRIX")
    print("=" * 80)
    print()
    print(f"{'Source':<15} {'Records':>10} {'Location':>12} {'Company':>12} {'Trade':>12} {'Grid':>12}")
    print("-" * 75)

    for source_name, source_data in results.items():
        loc = source_data.get('location', {})
        comp = source_data.get('company', {})
        trade = source_data.get('trade', {})
        grid = source_data.get('grid', {})

        loc_pct = f"{loc['coverage']:.1f}%" if loc else "N/A"
        comp_pct = f"{comp['coverage']:.1f}%" if comp else "N/A"
        trade_pct = f"{trade['coverage']:.1f}%" if trade else "N/A"
        grid_pct = f"{grid['coverage']:.1f}%" if grid else "N/A"

        print(f"{source_name:<15} {source_data['records']:>10,} {loc_pct:>12} {comp_pct:>12} {trade_pct:>12} {grid_pct:>12}")

    print()

    # Print detailed breakdown
    print("=" * 80)
    print("DETAILED BREAKDOWN")
    print("=" * 80)
    print()

    for source_name, source_data in results.items():
        print(f"\n{source_name}")
        print("-" * 40)

        df = data[source_name]
        col_map = column_maps.get(source_name, {})

        for dim_name in ['location', 'company', 'trade', 'grid']:
            dim_info = source_data.get(dim_name)
            if not dim_info:
                continue

            print(f"  {dim_name.capitalize():12} {dim_info['mapped']:>8,} / {dim_info['total']:>8,} ({dim_info['coverage']:.1f}%)")

            # Show top unmapped values
            if dim_info['coverage'] < 100 and dim_info['source_col']:
                dim_col = dim_info['dim_col']
                source_col = dim_info['source_col']
                unmapped = analyze_unmapped(df, dim_col, source_col, source_name, top_n=5)
                if unmapped:
                    print(f"    Top unmapped {source_col} values:")
                    for val, cnt in unmapped[:5]:
                        print(f"      - {val}: {cnt:,} records")

    print()
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print()

    # Print summary as markdown table
    print("| Source | Records | Location | Company | Trade | Grid |")
    print("|--------|---------|----------|---------|-------|------|")

    for source_name, source_data in results.items():
        loc = source_data.get('location', {})
        comp = source_data.get('company', {})
        trade = source_data.get('trade', {})
        grid = source_data.get('grid', {})

        loc_pct = f"{loc['coverage']:.1f}%" if loc else "-"
        comp_pct = f"{comp['coverage']:.1f}%" if comp else "-"
        trade_pct = f"{trade['coverage']:.1f}%" if trade else "-"
        grid_pct = f"{grid['coverage']:.1f}%" if grid else "-"

        print(f"| {source_name} | {source_data['records']:,} | {loc_pct} | {comp_pct} | {trade_pct} | {grid_pct} |")

    return 0


if __name__ == '__main__':
    sys.exit(main())
