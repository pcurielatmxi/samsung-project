#!/usr/bin/env python3
"""
Location Coverage Check

Analyzes location enrichment coverage across all fact tables.
Reports counts and percentages by location_type, match_type, and grid_source.

Usage:
    python -m scripts.integrated_analysis.location.validation.check_coverage
    python -m scripts.integrated_analysis.location.validation.check_coverage --source raba
    python -m scripts.integrated_analysis.location.validation.check_coverage --detailed
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config.settings import Settings


# Fact table configurations (same as enrich_fact_tables_location.py)
FACT_TABLES = {
    'raba': {
        'path': Settings.PROCESSED_DATA_DIR / 'raba' / 'raba_consolidated.csv',
        'description': 'RABA Quality Inspections (RKCI)',
    },
    'psi': {
        'path': Settings.PROCESSED_DATA_DIR / 'psi' / 'psi_consolidated.csv',
        'description': 'PSI Quality Inspections (Construction Hive)',
    },
    'tbm': {
        'path': Settings.PROCESSED_DATA_DIR / 'tbm' / 'tbm_with_csi.csv',
        'description': 'TBM Daily Work Plans',
    },
    'qc_workbooks': {
        'path': Settings.PROCESSED_DATA_DIR / 'quality' / 'qc_inspections_enriched.csv',
        'description': 'QC Workbooks (Yates + SECAI)',
    },
}

# Location columns to check
LOCATION_COLUMNS = [
    'dim_location_id',
    'location_type',
    'location_code',
    'level',
    'grid_row_min',
    'grid_col_min',
    'grid_source',
    'affected_rooms',
    'affected_rooms_count',
    'match_type',
]


def load_fact_table(source: str) -> pd.DataFrame:
    """Load a fact table and return as DataFrame."""
    config = FACT_TABLES.get(source)
    if not config:
        raise ValueError(f"Unknown source: {source}")

    path = config['path']
    if not path.exists():
        print(f"  WARNING: File not found: {path}")
        return pd.DataFrame()

    return pd.read_csv(path, low_memory=False)


def analyze_coverage(df: pd.DataFrame, source: str) -> dict:
    """
    Analyze location coverage for a single fact table.

    Returns dict with coverage statistics.
    """
    total = len(df)
    if total == 0:
        return {'total': 0, 'error': 'No records'}

    stats = {
        'source': source,
        'total': total,
        'columns_present': [c for c in LOCATION_COLUMNS if c in df.columns],
        'columns_missing': [c for c in LOCATION_COLUMNS if c not in df.columns],
    }

    # Basic coverage
    if 'dim_location_id' in df.columns:
        stats['dim_location_id_count'] = df['dim_location_id'].notna().sum()
        stats['dim_location_id_pct'] = df['dim_location_id'].notna().mean() * 100

    # Location type distribution
    if 'location_type' in df.columns:
        type_counts = df['location_type'].value_counts(dropna=False)
        stats['location_type_dist'] = {
            str(k) if pd.notna(k) else 'NULL': int(v)
            for k, v in type_counts.items()
        }
        stats['location_type_pct'] = {
            str(k) if pd.notna(k) else 'NULL': round(v / total * 100, 1)
            for k, v in type_counts.items()
        }

    # Match type distribution
    if 'match_type' in df.columns:
        match_counts = df['match_type'].value_counts(dropna=False)
        stats['match_type_dist'] = {
            str(k) if pd.notna(k) else 'NULL': int(v)
            for k, v in match_counts.items()
        }
        stats['match_type_pct'] = {
            str(k) if pd.notna(k) else 'NULL': round(v / total * 100, 1)
            for k, v in match_counts.items()
        }

    # Grid source distribution
    if 'grid_source' in df.columns:
        grid_counts = df['grid_source'].value_counts(dropna=False)
        stats['grid_source_dist'] = {
            str(k) if pd.notna(k) else 'NULL': int(v)
            for k, v in grid_counts.items()
        }
        stats['grid_source_pct'] = {
            str(k) if pd.notna(k) else 'NULL': round(v / total * 100, 1)
            for k, v in grid_counts.items()
        }

    # Grid coverage (has row AND column)
    if 'grid_row_min' in df.columns and 'grid_col_min' in df.columns:
        has_grid = (df['grid_row_min'].notna() & df['grid_col_min'].notna()).sum()
        stats['has_grid_bounds_count'] = has_grid
        stats['has_grid_bounds_pct'] = round(has_grid / total * 100, 1)

    # Affected rooms coverage
    if 'affected_rooms_count' in df.columns:
        has_rooms = (df['affected_rooms_count'] > 0).sum()
        stats['has_affected_rooms_count'] = has_rooms
        stats['has_affected_rooms_pct'] = round(has_rooms / total * 100, 1)

        # Average rooms per record (when present)
        rooms_when_present = df.loc[df['affected_rooms_count'] > 0, 'affected_rooms_count']
        if len(rooms_when_present) > 0:
            stats['avg_affected_rooms'] = round(rooms_when_present.mean(), 1)

    return stats


def print_summary_report(all_stats: dict):
    """Print a summary comparison table across all sources."""
    print("\n" + "=" * 90)
    print("LOCATION COVERAGE SUMMARY")
    print("=" * 90)

    # Header
    print(f"\n{'Source':<15} {'Records':>10} {'Location ID':>12} {'Has Grid':>10} {'Has Rooms':>10}")
    print("-" * 60)

    for source, stats in all_stats.items():
        if 'error' in stats:
            print(f"{source:<15} {'ERROR':>10}")
            continue

        total = stats['total']
        loc_pct = stats.get('dim_location_id_pct', 0)
        grid_pct = stats.get('has_grid_bounds_pct', 0)
        rooms_pct = stats.get('has_affected_rooms_pct', 0)

        print(f"{source:<15} {total:>10,} {loc_pct:>11.1f}% {grid_pct:>9.1f}% {rooms_pct:>9.1f}%")

    print()


def print_location_type_report(all_stats: dict):
    """Print location_type distribution comparison."""
    print("\n" + "=" * 90)
    print("LOCATION TYPE DISTRIBUTION")
    print("=" * 90)

    # Collect all location types
    all_types = set()
    for stats in all_stats.values():
        if 'location_type_dist' in stats:
            all_types.update(stats['location_type_dist'].keys())

    # Sort types in logical order
    type_order = ['ROOM', 'STAIR', 'ELEVATOR', 'GRIDLINE', 'LEVEL', 'BUILDING', 'UNDEFINED', 'NULL']
    sorted_types = [t for t in type_order if t in all_types]
    sorted_types.extend([t for t in sorted(all_types) if t not in type_order])

    # Header
    sources = list(all_stats.keys())
    header = f"{'Type':<12}" + "".join(f"{s:>15}" for s in sources)
    print(f"\n{header}")
    print("-" * (12 + 15 * len(sources)))

    for loc_type in sorted_types:
        row = f"{loc_type:<12}"
        for source in sources:
            stats = all_stats[source]
            if 'location_type_pct' in stats:
                pct = stats['location_type_pct'].get(loc_type, 0)
                count = stats['location_type_dist'].get(loc_type, 0)
                row += f"{pct:>7.1f}% ({count:>5,})"
            else:
                row += f"{'N/A':>15}"
        print(row)

    print()


def print_match_type_report(all_stats: dict):
    """Print match_type distribution comparison."""
    print("\n" + "=" * 90)
    print("MATCH TYPE DISTRIBUTION")
    print("=" * 90)

    # Collect all match types
    all_types = set()
    for stats in all_stats.values():
        if 'match_type_dist' in stats:
            all_types.update(stats['match_type_dist'].keys())

    # Sort types
    type_order = ['ROOM_DIRECT', 'ROOM_FROM_GRID', 'GRID_MULTI', 'GRIDLINE', 'LEVEL', 'BUILDING', 'UNDEFINED', 'NULL']
    sorted_types = [t for t in type_order if t in all_types]
    sorted_types.extend([t for t in sorted(all_types) if t not in type_order])

    # Header
    sources = list(all_stats.keys())
    header = f"{'Match Type':<18}" + "".join(f"{s:>15}" for s in sources)
    print(f"\n{header}")
    print("-" * (18 + 15 * len(sources)))

    for match_type in sorted_types:
        row = f"{match_type:<18}"
        for source in sources:
            stats = all_stats[source]
            if 'match_type_pct' in stats:
                pct = stats['match_type_pct'].get(match_type, 0)
                count = stats['match_type_dist'].get(match_type, 0)
                row += f"{pct:>7.1f}% ({count:>5,})"
            else:
                row += f"{'N/A':>15}"
        print(row)

    print()


def print_grid_source_report(all_stats: dict):
    """Print grid_source distribution comparison."""
    print("\n" + "=" * 90)
    print("GRID SOURCE DISTRIBUTION")
    print("=" * 90)
    print("(Where grid bounds came from: RECORD=source data, DIM_LOCATION=lookup, NONE=unavailable)")

    # Collect all grid sources
    all_sources = set()
    for stats in all_stats.values():
        if 'grid_source_dist' in stats:
            all_sources.update(stats['grid_source_dist'].keys())

    # Sort
    source_order = ['RECORD', 'DIM_LOCATION', 'NONE', 'NULL']
    sorted_sources = [s for s in source_order if s in all_sources]
    sorted_sources.extend([s for s in sorted(all_sources) if s not in source_order])

    # Header
    sources = list(all_stats.keys())
    header = f"{'Grid Source':<15}" + "".join(f"{s:>15}" for s in sources)
    print(f"\n{header}")
    print("-" * (15 + 15 * len(sources)))

    for grid_src in sorted_sources:
        row = f"{grid_src:<15}"
        for source in sources:
            stats = all_stats[source]
            if 'grid_source_pct' in stats:
                pct = stats['grid_source_pct'].get(grid_src, 0)
                count = stats['grid_source_dist'].get(grid_src, 0)
                row += f"{pct:>7.1f}% ({count:>5,})"
            else:
                row += f"{'N/A':>15}"
        print(row)

    print()


def print_detailed_report(source: str, stats: dict):
    """Print detailed report for a single source."""
    print(f"\n{'=' * 70}")
    print(f"DETAILED REPORT: {source.upper()}")
    print(f"{'=' * 70}")

    config = FACT_TABLES.get(source, {})
    print(f"\nDescription: {config.get('description', 'N/A')}")
    print(f"File: {config.get('path', 'N/A')}")
    print(f"Total Records: {stats.get('total', 0):,}")

    # Columns check
    print(f"\nLocation Columns Present: {len(stats.get('columns_present', []))}/{len(LOCATION_COLUMNS)}")
    if stats.get('columns_missing'):
        print(f"  Missing: {', '.join(stats['columns_missing'])}")

    # Coverage metrics
    print(f"\nCoverage Metrics:")
    print(f"  dim_location_id: {stats.get('dim_location_id_pct', 0):.1f}%")
    print(f"  Has grid bounds: {stats.get('has_grid_bounds_pct', 0):.1f}%")
    print(f"  Has affected rooms: {stats.get('has_affected_rooms_pct', 0):.1f}%")
    if 'avg_affected_rooms' in stats:
        print(f"  Avg rooms (when present): {stats['avg_affected_rooms']:.1f}")

    # Location type breakdown
    if 'location_type_dist' in stats:
        print(f"\nLocation Type Breakdown:")
        for loc_type, count in sorted(stats['location_type_dist'].items(), key=lambda x: -x[1]):
            pct = stats['location_type_pct'].get(loc_type, 0)
            print(f"  {loc_type:<15} {count:>8,} ({pct:>5.1f}%)")

    # Match type breakdown
    if 'match_type_dist' in stats:
        print(f"\nMatch Type Breakdown:")
        for match_type, count in sorted(stats['match_type_dist'].items(), key=lambda x: -x[1]):
            pct = stats['match_type_pct'].get(match_type, 0)
            print(f"  {match_type:<18} {count:>8,} ({pct:>5.1f}%)")

    # Grid source breakdown
    if 'grid_source_dist' in stats:
        print(f"\nGrid Source Breakdown:")
        for grid_src, count in sorted(stats['grid_source_dist'].items(), key=lambda x: -x[1]):
            pct = stats['grid_source_pct'].get(grid_src, 0)
            print(f"  {grid_src:<15} {count:>8,} ({pct:>5.1f}%)")


def main():
    parser = argparse.ArgumentParser(
        description='Check location enrichment coverage across fact tables'
    )
    parser.add_argument(
        '--source',
        choices=['raba', 'psi', 'tbm', 'qc_workbooks', 'all'],
        default='all',
        help='Which source to check (default: all)'
    )
    parser.add_argument(
        '--detailed',
        action='store_true',
        help='Show detailed breakdown for each source'
    )
    args = parser.parse_args()

    print("=" * 90)
    print("LOCATION COVERAGE CHECK")
    print("=" * 90)

    # Determine which sources to process
    if args.source == 'all':
        sources = list(FACT_TABLES.keys())
    else:
        sources = [args.source]

    # Load and analyze each source
    all_stats = {}
    for source in sources:
        print(f"\nLoading {source}...")
        df = load_fact_table(source)
        if len(df) == 0:
            all_stats[source] = {'error': 'No data'}
            continue

        stats = analyze_coverage(df, source)
        all_stats[source] = stats
        print(f"  {stats['total']:,} records loaded")

    # Print reports
    if args.detailed:
        for source, stats in all_stats.items():
            if 'error' not in stats:
                print_detailed_report(source, stats)

    # Always print summary comparison
    if len(all_stats) > 0:
        print_summary_report(all_stats)
        print_location_type_report(all_stats)
        print_match_type_report(all_stats)
        print_grid_source_report(all_stats)


if __name__ == '__main__':
    main()
