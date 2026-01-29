#!/usr/bin/env python3
"""Build company hierarchy from TBM data.

Analyzes TBM tier1_gc → tier2_sc relationships to determine which subcontractors
work under which general contractors.

Adds parent_company_id to dim_company to enable:
- Grouping subs under their GC
- Aggregating labor/quality by GC responsibility
- Understanding the contractual chain
"""

import sys
from pathlib import Path
from collections import defaultdict
import pandas as pd

_project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import Settings
from scripts.shared.dimension_lookup import get_company_id, _normalize_company_name


def load_tbm_relationships() -> pd.DataFrame:
    """Load TBM data and extract GC→Sub relationships."""
    tbm_path = Settings.TBM_PROCESSED_DIR / 'work_entries.csv'

    if not tbm_path.exists():
        print(f"TBM file not found: {tbm_path}")
        return pd.DataFrame()

    df = pd.read_csv(tbm_path, low_memory=False)

    # Normalize GC names
    df['tier1_gc_norm'] = df['tier1_gc'].str.upper().str.strip()

    # Get unique GC→Sub pairs with counts
    relationships = df.groupby(['tier1_gc_norm', 'tier2_sc']).size().reset_index(name='work_entries')

    return relationships


def analyze_relationships(relationships: pd.DataFrame) -> dict:
    """Analyze GC→Sub relationships to build hierarchy.

    Returns:
        Dict mapping subcontractor name → dict with:
        - primary_gc: Most common GC this sub works for
        - primary_gc_pct: Percentage of entries under primary GC
        - all_gcs: Dict of all GCs with entry counts
    """
    if relationships.empty:
        return {}

    # Group by subcontractor
    sub_analysis = {}

    for sub in relationships['tier2_sc'].unique():
        sub_data = relationships[relationships['tier2_sc'] == sub]

        total_entries = sub_data['work_entries'].sum()
        gc_breakdown = dict(zip(sub_data['tier1_gc_norm'], sub_data['work_entries']))

        # Find primary GC (most entries)
        primary_gc = max(gc_breakdown, key=gc_breakdown.get)
        primary_gc_entries = gc_breakdown[primary_gc]
        primary_gc_pct = primary_gc_entries / total_entries * 100

        sub_analysis[sub] = {
            'primary_gc': primary_gc,
            'primary_gc_entries': primary_gc_entries,
            'primary_gc_pct': primary_gc_pct,
            'total_entries': total_entries,
            'all_gcs': gc_breakdown,
        }

    return sub_analysis


def map_gc_to_company_id(gc_name: str) -> int:
    """Map normalized GC name to company_id."""
    gc_mapping = {
        'YATES': 2,  # W.G. Yates & Sons Construction
        'W.G. YATES': 2,
        'HENSEL PHELPS': 34,
        'PCL': 44,
        'MCCARTHY': 45,
    }

    gc_upper = gc_name.upper().strip()

    # Direct mapping
    if gc_upper in gc_mapping:
        return gc_mapping[gc_upper]

    # Try fuzzy lookup
    company_id = get_company_id(gc_name)
    if company_id:
        return company_id

    return None


def build_hierarchy(sub_analysis: dict, threshold_pct: float = 80.0) -> dict:
    """Build parent_company_id mapping.

    Args:
        sub_analysis: Output from analyze_relationships
        threshold_pct: Minimum percentage to assign a definitive parent

    Returns:
        Dict mapping subcontractor name → parent_company_id
    """
    hierarchy = {}

    for sub_name, analysis in sub_analysis.items():
        primary_gc = analysis['primary_gc']
        primary_gc_pct = analysis['primary_gc_pct']

        # Only assign parent if clear majority relationship
        if primary_gc_pct >= threshold_pct:
            gc_company_id = map_gc_to_company_id(primary_gc)
            if gc_company_id:
                hierarchy[sub_name] = {
                    'parent_company_id': gc_company_id,
                    'confidence': 'HIGH' if primary_gc_pct >= 95 else 'MEDIUM',
                    'primary_gc_pct': primary_gc_pct,
                }
        elif primary_gc_pct >= 50:
            # Lower confidence but still majority
            gc_company_id = map_gc_to_company_id(primary_gc)
            if gc_company_id:
                hierarchy[sub_name] = {
                    'parent_company_id': gc_company_id,
                    'confidence': 'LOW',
                    'primary_gc_pct': primary_gc_pct,
                }

    return hierarchy


def update_dim_company(hierarchy: dict, dry_run: bool = True) -> pd.DataFrame:
    """Update dim_company with parent_company_id.

    Args:
        hierarchy: Output from build_hierarchy
        dry_run: If True, don't write file

    Returns:
        Updated DataFrame
    """
    dim_path = Settings.PROCESSED_DATA_DIR / 'integrated_analysis' / 'dim_company.csv'

    df = pd.read_csv(dim_path)

    # Add new columns if they don't exist
    if 'parent_company_id' not in df.columns:
        df['parent_company_id'] = None
    if 'parent_confidence' not in df.columns:
        df['parent_confidence'] = None

    # First, map all sub names to company_ids and aggregate
    # This handles multiple name variations pointing to same company
    company_id_to_parent = {}

    for sub_name, parent_info in hierarchy.items():
        sub_company_id = get_company_id(sub_name)

        if sub_company_id is None:
            continue

        # Skip if company is the same as parent (can't be your own parent)
        if sub_company_id == parent_info['parent_company_id']:
            continue

        # Skip the "Unknown" placeholder (company_id=0)
        if sub_company_id == 0:
            continue

        # Keep the highest confidence assignment per company
        if sub_company_id not in company_id_to_parent:
            company_id_to_parent[sub_company_id] = parent_info
        else:
            # Keep higher confidence or higher percentage
            existing = company_id_to_parent[sub_company_id]
            confidence_order = {'HIGH': 3, 'MEDIUM': 2, 'LOW': 1}
            if (confidence_order.get(parent_info['confidence'], 0) > confidence_order.get(existing['confidence'], 0) or
                parent_info['primary_gc_pct'] > existing['primary_gc_pct']):
                company_id_to_parent[sub_company_id] = parent_info

    # Apply updates (deduplicated)
    updates = 0
    for sub_company_id, parent_info in company_id_to_parent.items():
        mask = df['company_id'] == sub_company_id
        if mask.any():
            df.loc[mask, 'parent_company_id'] = parent_info['parent_company_id']
            df.loc[mask, 'parent_confidence'] = parent_info['confidence']
            updates += 1

            company_name = df.loc[mask, 'canonical_name'].values[0]
            parent_id = parent_info['parent_company_id']
            parent_name = df.loc[df['company_id'] == parent_id, 'canonical_name'].values[0]
            print(f"  {company_name} → {parent_name} ({parent_info['confidence']}, {parent_info['primary_gc_pct']:.0f}%)")

    print(f"\nUpdated {updates} companies with parent relationships")

    if not dry_run:
        df.to_csv(dim_path, index=False)
        print(f"Saved to {dim_path}")

    return df


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Build company hierarchy from TBM data")
    parser.add_argument('--dry-run', action='store_true', default=True,
                       help='Preview changes without saving (default)')
    parser.add_argument('--apply', action='store_true',
                       help='Apply changes to dim_company.csv')
    parser.add_argument('--threshold', type=float, default=80.0,
                       help='Minimum %% to assign parent (default: 80)')

    args = parser.parse_args()

    print("Loading TBM relationships...")
    relationships = load_tbm_relationships()

    if relationships.empty:
        print("No TBM data found")
        return

    print(f"Found {len(relationships)} GC→Sub relationship records")
    print()

    print("Analyzing relationships...")
    sub_analysis = analyze_relationships(relationships)
    print(f"Found {len(sub_analysis)} unique subcontractors")
    print()

    # Show GC distribution
    print("GC Distribution (by work entries):")
    gc_totals = relationships.groupby('tier1_gc_norm')['work_entries'].sum().sort_values(ascending=False)
    for gc, count in gc_totals.head(10).items():
        print(f"  {gc}: {count:,} entries")
    print()

    print(f"Building hierarchy (threshold: {args.threshold}%)...")
    hierarchy = build_hierarchy(sub_analysis, threshold_pct=args.threshold)
    print(f"Found {len(hierarchy)} subs with clear parent relationship")
    print()

    print("Company hierarchy updates:")
    dry_run = not args.apply
    df = update_dim_company(hierarchy, dry_run=dry_run)

    if dry_run:
        print("\n(Dry run - use --apply to save changes)")


if __name__ == '__main__':
    main()
