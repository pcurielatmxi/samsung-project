#!/usr/bin/env python3
"""
Build WBS hierarchy table with tier columns.

Extends the existing projwbs.csv by adding tier_1 through tier_6 columns with
human-friendly labels, enabling easy filtering and grouping at any level of
the hierarchy. All original columns are preserved.

Output: data/primavera/processed/projwbs.csv (overwrites existing)

Usage:
    python scripts/build_wbs_hierarchy.py
    python scripts/build_wbs_hierarchy.py --schedule-type SECAI
    python scripts/build_wbs_hierarchy.py --file-id 48
    python scripts/build_wbs_hierarchy.py --all-files
"""

import argparse
from pathlib import Path

import pandas as pd


def build_hierarchy_tree(wbs_df: pd.DataFrame) -> dict:
    """
    Build lookup structures for hierarchy traversal.

    Returns:
        dict with 'id_to_row', 'id_to_parent', 'id_to_children'
    """
    id_to_row = {row['wbs_id']: row.to_dict() for _, row in wbs_df.iterrows()}
    id_to_parent = dict(zip(wbs_df['wbs_id'], wbs_df['parent_wbs_id']))

    # Build children lookup
    id_to_children = {}
    for wbs_id in id_to_row:
        id_to_children[wbs_id] = []

    for wbs_id, parent_id in id_to_parent.items():
        if pd.notna(parent_id) and parent_id in id_to_children:
            id_to_children[parent_id].append(wbs_id)

    return {
        'id_to_row': id_to_row,
        'id_to_parent': id_to_parent,
        'id_to_children': id_to_children,
    }


def get_ancestors(wbs_id: str, tree: dict, max_depth: int = 10) -> list[dict]:
    """
    Get list of ancestors from root to this node (inclusive).

    Returns:
        List of dicts with 'wbs_id', 'wbs_short_name', 'wbs_name' from root to node
    """
    ancestors = []
    current_id = wbs_id
    depth = 0

    while depth < max_depth and current_id in tree['id_to_row']:
        row = tree['id_to_row'][current_id]
        ancestors.append({
            'wbs_id': current_id,
            'wbs_short_name': row['wbs_short_name'],
            'wbs_name': row['wbs_name'],
        })

        parent_id = tree['id_to_parent'].get(current_id)
        if pd.isna(parent_id) or parent_id not in tree['id_to_row']:
            break
        current_id = parent_id
        depth += 1

    # Reverse to get root -> leaf order
    return list(reversed(ancestors))


def get_tier_label(node: dict, use_short_name: bool = False) -> str:
    """
    Get human-friendly label for a WBS node.

    Prefers wbs_name if informative, falls back to wbs_short_name.
    Truncates long names.
    """
    name = str(node.get('wbs_name', ''))
    short = str(node.get('wbs_short_name', ''))

    # Use short name if:
    # - wbs_name is missing or same as short name
    # - wbs_name starts with project code (SAMSUNG-TFAB1...)
    # - explicitly requested
    if use_short_name or not name or name == short:
        return short

    # For project root nodes, use short name
    if name.startswith('SAMSUNG') or name.startswith('Yates T FAB1'):
        return short

    # Truncate long names
    if len(name) > 50:
        name = name[:47] + '...'

    return name


def find_roots(wbs_df: pd.DataFrame, tree: dict) -> list[str]:
    """
    Find root nodes (whose parent is not in the current file).
    """
    roots = []
    for wbs_id in tree['id_to_row']:
        parent_id = tree['id_to_parent'].get(wbs_id)
        if pd.isna(parent_id) or parent_id not in tree['id_to_row']:
            roots.append(wbs_id)
    return roots


def calculate_depth(wbs_id: str, tree: dict) -> int:
    """Calculate depth from root for a node."""
    ancestors = get_ancestors(wbs_id, tree)
    return len(ancestors) - 1  # -1 because root is depth 0


def build_tier_columns(wbs_df: pd.DataFrame, num_tiers: int = 6) -> pd.DataFrame:
    """
    Build tier columns for WBS nodes.

    Args:
        wbs_df: WBS DataFrame with wbs_id, parent_wbs_id, wbs_short_name, wbs_name
        num_tiers: Number of tier columns to create (default 6)

    Returns:
        DataFrame with wbs_id, depth, and tier_1 through tier_N columns
    """
    tree = build_hierarchy_tree(wbs_df)
    roots = find_roots(wbs_df, tree)

    print(f"Building hierarchy for {len(wbs_df)} WBS nodes...")
    print(f"Found {len(roots)} root node(s)")

    results = []

    for _, row in wbs_df.iterrows():
        wbs_id = row['wbs_id']
        ancestors = get_ancestors(wbs_id, tree)
        depth = len(ancestors) - 1

        # Build result row with just wbs_id, depth, and tier columns
        result = {
            'wbs_id': wbs_id,
            'depth': depth,
        }

        # Add tier label columns only (no IDs)
        for i in range(num_tiers):
            tier_num = i + 1
            if i < len(ancestors):
                result[f'tier_{tier_num}'] = get_tier_label(ancestors[i])
            else:
                result[f'tier_{tier_num}'] = None

        results.append(result)

    return pd.DataFrame(results)


def print_hierarchy_summary(df: pd.DataFrame, num_tiers: int = 6):
    """Print summary of hierarchy structure."""
    print("\n" + "=" * 70)
    print("HIERARCHY SUMMARY")
    print("=" * 70)

    # Depth distribution
    print("\nNodes by depth:")
    for depth, count in df['depth'].value_counts().sort_index().items():
        print(f"  Depth {depth}: {count:4d} nodes")

    # Tier value distribution
    for i in range(min(3, num_tiers)):  # Show first 3 tiers
        tier_col = f'tier_{i + 1}'
        print(f"\n{tier_col.upper()} values:")
        for val, cnt in df[tier_col].value_counts().head(8).items():
            if val is not None:
                label = str(val)[:40]
                print(f"  {label:40s}: {cnt:4d}")

        remaining = df[tier_col].nunique() - 8
        if remaining > 0:
            print(f"  ... and {remaining} more unique values")


def print_sample_tree(df: pd.DataFrame, max_rows: int = 20):
    """Print sample of hierarchy as indented tree."""
    print("\n" + "=" * 70)
    print("SAMPLE HIERARCHY TREE")
    print("=" * 70 + "\n")

    # Get nodes sorted by tier path
    df_sorted = df.sort_values(['tier_1', 'tier_2', 'tier_3', 'tier_4', 'depth'])

    shown = 0
    prev_tiers = [None] * 6

    for _, row in df_sorted.iterrows():
        if shown >= max_rows:
            print(f"\n... ({len(df) - max_rows} more nodes)")
            break

        depth = row['depth']
        indent = "  " * depth

        # Get current tier label
        tier_col = f'tier_{depth + 1}'
        label = row[tier_col] if tier_col in row and pd.notna(row[tier_col]) else row['wbs_short_name']

        # Only show if this tier changed
        current_tiers = [row.get(f'tier_{i+1}') for i in range(6)]
        if current_tiers[:depth+1] != prev_tiers[:depth+1]:
            # Truncate label for display
            display_label = str(label)[:50]
            print(f"{indent}├── {display_label}")
            shown += 1
            prev_tiers = current_tiers


def main():
    parser = argparse.ArgumentParser(description='Build WBS hierarchy with tier columns')
    parser.add_argument('--processed-dir', type=Path,
                        default=Path('data/primavera/processed'))
    parser.add_argument('--num-tiers', type=int, default=6,
                        help='Number of tier columns to create (default: 6)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Print summary without saving')
    args = parser.parse_args()

    wbs_path = args.processed_dir / 'projwbs.csv'

    # Load data
    print("Loading data...")
    xer_files = pd.read_csv(args.processed_dir / 'xer_files.csv')
    wbs = pd.read_csv(wbs_path, low_memory=False)

    print(f"Loaded {len(wbs)} WBS nodes from {len(wbs['file_id'].unique())} files")

    # Check if tier columns already exist
    tier_cols = [f'tier_{i}' for i in range(1, args.num_tiers + 1)]
    existing_tier_cols = [c for c in tier_cols if c in wbs.columns]
    if existing_tier_cols:
        print(f"Removing existing tier columns: {existing_tier_cols}")
        wbs = wbs.drop(columns=existing_tier_cols + ['depth'] if 'depth' in wbs.columns else existing_tier_cols)

    # Process each file_id separately (hierarchy is file-specific)
    all_tier_dfs = []
    file_ids = wbs['file_id'].unique()

    for file_id in file_ids:
        file_wbs = wbs[wbs['file_id'] == file_id].copy()

        if len(file_wbs) == 0:
            continue

        # Build tier columns for this file
        tier_df = build_tier_columns(file_wbs, num_tiers=args.num_tiers)
        all_tier_dfs.append(tier_df)

    # Combine all tier data
    tier_data = pd.concat(all_tier_dfs, ignore_index=True)
    print(f"\nBuilt tier columns for {len(tier_data)} WBS nodes")

    # Merge tier columns back to original WBS data
    result_df = wbs.merge(tier_data, on='wbs_id', how='left')

    # Reorder columns: original columns first, then depth, then tier columns at the end
    original_cols = [c for c in wbs.columns if c not in tier_cols and c != 'depth']
    new_cols = ['depth'] + tier_cols
    result_df = result_df[original_cols + new_cols]

    # Print summary for a sample file
    sample_file_id = xer_files[xer_files['is_current'] == True]['file_id'].values
    if len(sample_file_id) > 0:
        sample_df = result_df[result_df['file_id'] == sample_file_id[0]]
    else:
        sample_df = result_df[result_df['file_id'] == file_ids[-1]]

    print_hierarchy_summary(sample_df, args.num_tiers)
    print_sample_tree(sample_df, max_rows=20)

    if args.dry_run:
        print("\n" + "=" * 70)
        print("DRY RUN - No changes saved")
        print("=" * 70)
    else:
        # Save back to original location
        result_df.to_csv(wbs_path, index=False)

        print("\n" + "=" * 70)
        print(f"Output saved to: {wbs_path}")
        print(f"Total rows: {len(result_df)}")
        print(f"New columns added: depth, {', '.join(tier_cols)}")

    # Show example usage
    print("\n" + "=" * 70)
    print("EXAMPLE USAGE")
    print("=" * 70)
    print("""
# Load and use for analysis:
import pandas as pd

wbs = pd.read_csv('data/primavera/processed/projwbs.csv', low_memory=False)
tasks = pd.read_csv('data/primavera/processed/task.csv', low_memory=False)

# Join tasks with WBS (now includes tier columns)
merged = tasks.merge(wbs[['wbs_id', 'depth', 'tier_1', 'tier_2', 'tier_3', 'tier_4']], on='wbs_id')

# Group delays by tier_2 (major divisions)
delays_by_division = merged[merged['total_float_hr_cnt'] < 0].groupby('tier_2').size()

# Filter to specific building
fab_tasks = merged[merged['tier_3'] == 'FAB BUILDING (Phase 1)']

# Roll up to any tier level
by_tier_3 = merged.groupby(['tier_1', 'tier_2', 'tier_3']).agg({
    'task_id': 'count',
    'total_float_hr_cnt': 'mean'
})
""")


if __name__ == '__main__':
    main()
