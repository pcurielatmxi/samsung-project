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
import re
import sys
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

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


def parse_tbm_grid(location_row: str) -> Dict[str, Any]:
    """
    Parse TBM location_row field into normalized grid components.

    Args:
        location_row: Raw location string from TBM (e.g., "G/H", "J11", "A-N/1-3")

    Returns:
        Dict with keys:
            grid_row_min, grid_row_max: Letter(s) A-N or None
            grid_col_min, grid_col_max: Numbers 1-33 or None
            grid_raw: Original value
            grid_type: POINT, ROW_ONLY, COL_ONLY, RANGE, AREA, NAMED, UNPARSED
    """
    result = {
        'grid_row_min': None,
        'grid_row_max': None,
        'grid_col_min': None,
        'grid_col_max': None,
        'grid_raw': location_row,
        'grid_type': 'UNPARSED',
    }

    if pd.isna(location_row):
        return result

    val = str(location_row).strip().upper()

    # Skip descriptive values (whole building/area references)
    if re.search(r'^(ALL|VARIOUS|WHOLE|WORKING|OUTSIDE|LAYDOWN|QC\b|ACM|FIZZ|SUW\s|SUE\s|FAB\s)', val):
        result['grid_type'] = 'AREA'
        return result

    # Named locations (stair, elevator, vestibule, room names)
    if re.search(r'^(STAIR|ELEVATOR|VESTIBULE|ELEV\b|ELECTRICAL|TQRLAB|BUNKER|COPING)', val):
        result['grid_type'] = 'NAMED'
        return result

    # === ROW RANGE PATTERNS ===

    # Pattern: "E - J line / Whole Floor" → row E-J
    m = re.match(r'^([A-N])\s*[-–]\s*([A-N])\s*(LINE|LINES?)?', val, re.IGNORECASE)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_type'] = 'ROW_ONLY'
        return result

    # Pattern: "D & K lines" → rows D, K (non-contiguous treated as range)
    m = re.match(r'^([A-N])\s*[&,]\s*([A-N])', val)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_type'] = 'ROW_ONLY'
        return result

    # === ROW + COLUMN PATTERNS ===

    # Pattern: "A/B 32" or "K/L 33 LINE" → rows A-B, col 32
    m = re.match(r'^([A-N])[/]([A-N])\s+(\d+)', val)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(3))
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "A 30-31" → row A, col 30-31
    m = re.match(r'^([A-N])\s+(\d+)[-–](\d+)', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(3))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "D line 1-33" → row D, col 1-33
    m = re.match(r'^([A-N])\s*LINE\s+(\d+)[-–](\d+)', val, re.IGNORECASE)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(3))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "C/11 - C/22" or "L/6 - L/11" → row C, col 11-22
    m = re.match(r'^([A-N])/(\d+)\s*[-–]\s*([A-N])?/?(\d+)', val)
    if m:
        result['grid_row_min'] = m.group(1)
        result['grid_row_max'] = m.group(3) or m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(4))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "D6/D19" → row D, col 6-19
    m = re.match(r'^([A-N])(\d+)/([A-N])?(\d+)$', val)
    if m:
        result['grid_row_min'] = m.group(1)
        result['grid_row_max'] = m.group(3) or m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(4))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "A LINE" or "C LINE" → row only
    m = re.match(r'^([A-N])\s*LINE\s*$', val, re.IGNORECASE)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        result['grid_type'] = 'ROW_ONLY'
        return result

    # === COLUMN ONLY PATTERNS ===

    # Pattern: "LINE 33" or "GL 33" → col only
    m = re.match(r'^(LINE|GL)\s*(\d+)', val, re.IGNORECASE)
    if m:
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(2))
        result['grid_type'] = 'COL_ONLY'
        return result

    # Pattern: Just "33" → col only
    m = re.match(r'^(\d+)$', val)
    if m:
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(1))
        result['grid_type'] = 'COL_ONLY'
        return result

    # Gridline: GL-33
    m = re.match(r'^GL[-]?(\d+)$', val)
    if m:
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(1))
        result['grid_type'] = 'COL_ONLY'
        return result

    # Column range only: 29-30
    m = re.match(r'^(\d+)[-–](\d+)$', val)
    if m:
        cols = sorted([float(m.group(1)), float(m.group(2))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'COL_ONLY'
        return result

    # === SIMPLE PATTERNS ===

    # Single letter: G, D, B
    m = re.match(r'^([A-N])$', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        result['grid_type'] = 'ROW_ONLY'
        return result

    # Letter/letter: G/H, D/K
    m = re.match(r'^([A-N])[/&]([A-N])$', val)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_type'] = 'ROW_ONLY'
        return result

    # Letter + Column: J11, A-19, A19, N/17
    m = re.match(r'^([A-N])[-/]?(\d+(?:\.\d+)?)$', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(2))
        result['grid_type'] = 'POINT'
        return result

    # Letter+Col range: K20-K32, J33-K33, L5-L11
    m = re.match(r'^([A-N])(\d+)[-–]([A-N])?(\d+)', val)
    if m:
        result['grid_row_min'] = m.group(1)
        result['grid_row_max'] = m.group(3) or m.group(1)
        if result['grid_row_min'] > result['grid_row_max']:
            result['grid_row_min'], result['grid_row_max'] = result['grid_row_max'], result['grid_row_min']
        cols = sorted([float(m.group(2)), float(m.group(4))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Letter range with columns: A-N/1-3
    m = re.match(r'^([A-N])[-–]([A-N])[/\s](\d+)[-–]?(\d+)?', val)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_col_min'] = float(m.group(3))
        result['grid_col_max'] = float(m.group(4) or m.group(3))
        result['grid_type'] = 'RANGE'
        return result

    return result


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

    # Infer trade from work activities (enhanced mapping)
    def infer_trade_from_activity(activity):
        if pd.isna(activity):
            return None
        activity_lower = str(activity).lower()
        # Concrete (trade_id=1)
        if any(x in activity_lower for x in ['concrete', 'pour', 'slab', 'form', 'strip', 'rebar', 'topping', 'placement', 'cure', 'finishing']):
            return 'Concrete'
        # Structural Steel (trade_id=2)
        if any(x in activity_lower for x in ['steel', 'erect', 'deck', 'weld', 'bolt', 'connection', 'joist', 'truss', 'iron']):
            return 'Structural Steel'
        # Roofing (trade_id=3)
        if any(x in activity_lower for x in ['roof', 'membrane', 'waterproof', 'eifs']):
            return 'Roofing'
        # Drywall (trade_id=4)
        if any(x in activity_lower for x in ['drywall', 'frame', 'stud', 'gyp', 'gypsum', 'framing', 'shaft', 'ceiling grid', 'metal track', 'sheathing']):
            return 'Drywall'
        # Finishes (trade_id=5)
        if any(x in activity_lower for x in ['paint', 'coat', 'finish', 'tile', 'floor', 'ceiling', 'door', 'hardware', 'casework', 'glazing', 'window']):
            return 'Finishes'
        # Fire Protection (trade_id=6)
        if any(x in activity_lower for x in ['fireproof', 'firestop', 'sfrm', 'fire caulk', 'intumescent', 'fire rating', 'fire barrier']):
            return 'Fire Protection'
        # MEP (trade_id=7)
        if any(x in activity_lower for x in ['mep', 'hvac', 'plumb', 'elec', 'pipe', 'conduit', 'duct', 'wire', 'electrical', 'mechanical', 'sprinkler']):
            return 'MEP'
        # Insulation (trade_id=8)
        if any(x in activity_lower for x in ['insul', 'thermal', 'urethane', 'wrap']):
            return 'Insulation'
        # Earthwork (trade_id=9)
        if any(x in activity_lower for x in ['excavat', 'backfill', 'grade', 'foundation', 'pier', 'pile', 'earth']):
            return 'Earthwork'
        # Precast (trade_id=10)
        if any(x in activity_lower for x in ['precast', 'tilt', 'pc panel']):
            return 'Precast'
        # Panels (trade_id=11)
        if any(x in activity_lower for x in ['panel', 'clad', 'skin', 'enclosure', 'metal wall']):
            return 'Panels'
        # Masonry (trade_id=13)
        if any(x in activity_lower for x in ['masonry', 'cmu', 'block', 'brick', 'grout']):
            return 'Masonry'
        return None

    df['trade_inferred'] = df['work_activities'].apply(infer_trade_from_activity)
    df['dim_trade_id'] = df['trade_inferred'].apply(get_trade_id)
    df['dim_trade_code'] = df['dim_trade_id'].apply(get_trade_code)

    # Parse grid from location_row
    print("  Parsing grid coordinates...")
    grid_parsed = df['location_row'].apply(parse_tbm_grid).apply(pd.Series)
    df['grid_row_min'] = grid_parsed['grid_row_min']
    df['grid_row_max'] = grid_parsed['grid_row_max']
    df['grid_col_min'] = grid_parsed['grid_col_min']
    df['grid_col_max'] = grid_parsed['grid_col_max']
    df['grid_raw'] = grid_parsed['grid_raw']
    df['grid_type'] = grid_parsed['grid_type']

    # Calculate coverage
    has_grid_row = df['grid_row_min'].notna()
    has_grid_col = df['grid_col_min'].notna()
    coverage = {
        'location': df['dim_location_id'].notna().mean() * 100,
        'company': df['dim_company_id'].notna().mean() * 100,
        'trade': df['dim_trade_id'].notna().mean() * 100,
        'grid_row': has_grid_row.mean() * 100,
        'grid_col': has_grid_col.mean() * 100,
    }

    # Grid type distribution for reporting
    grid_type_dist = df['grid_type'].value_counts().to_dict()

    if not dry_run:
        df.to_csv(output_path, index=False)

    return {
        'status': 'success',
        'records': original_count,
        'coverage': coverage,
        'grid_types': grid_type_dist,
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
            # Show grid type distribution if present (TBM only)
            if 'grid_types' in result:
                print(f"  Grid types:")
                for gtype, count in sorted(result['grid_types'].items(), key=lambda x: -x[1]):
                    pct = count / result['records'] * 100
                    print(f"    {gtype}: {count:,} ({pct:.1f}%)")
            print(f"  Output: {result['output']}")
        else:
            print(f"  Status: {result['status']}")
            print(f"  Reason: {result.get('reason', 'unknown')}")

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"\n{'Source':<20} {'Records':>12} {'Location':>10} {'Company':>10} {'Trade':>10} {'Grid Row':>10} {'Grid Col':>10}")
    print("-" * 90)

    for key, result in results.items():
        name = ENRICHMENT_TASKS[key][0]
        if result['status'] == 'success':
            grid_row = result['coverage'].get('grid_row', 0.0)
            grid_col = result['coverage'].get('grid_col', 0.0)
            print(f"{name:<20} {result['records']:>12,} {result['coverage']['location']:>9.1f}% {result['coverage']['company']:>9.1f}% {result['coverage']['trade']:>9.1f}% {grid_row:>9.1f}% {grid_col:>9.1f}%")
        else:
            print(f"{name:<20} {'SKIPPED':>12}")


if __name__ == '__main__':
    main()
