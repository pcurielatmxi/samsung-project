#!/usr/bin/env python3
"""
Generate Task Taxonomy Lookup Table for YATES Schedules

Creates a lookup table mapping task_id to taxonomy classifications.
Join with task.csv on task_id for full task details.

Data Sources (in priority order):
1. Activity codes from P6 (Z-TRADE, Z-BLDG, Z-LEVEL, Z-SUB) - highest priority
2. WBS hierarchy context - used to fill gaps when activity codes missing
3. Task name inference - fallback when neither activity code nor WBS provides info

Source Tracking:
- Each field has a corresponding _source column showing how it was derived
- Values: 'activity_code', 'wbs', 'inferred', or None

Output: derived/primavera/task_taxonomy.csv

Usage:
    python scripts/primavera/derive/generate_task_taxonomy.py [--latest-only]
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config.settings import Settings
from task_taxonomy import build_task_context, infer_all_fields


def load_yates_data(latest_only: bool = False) -> tuple:
    """
    Load YATES schedule data.

    Args:
        latest_only: If True, only load the most recent schedule

    Returns:
        Tuple of (tasks_df, wbs_df, files_df, taskactv_df, actvcode_df, actvtype_df)
    """
    data_dir = Settings.PRIMAVERA_PROCESSED_DIR

    print("Loading data files...")
    tasks = pd.read_csv(data_dir / "task.csv", low_memory=False)
    wbs = pd.read_csv(data_dir / "projwbs.csv", low_memory=False)
    files = pd.read_csv(data_dir / "xer_files.csv")
    taskactv = pd.read_csv(data_dir / "taskactv.csv", low_memory=False)
    actvcode = pd.read_csv(data_dir / "actvcode.csv", low_memory=False)
    actvtype = pd.read_csv(data_dir / "actvtype.csv", low_memory=False)

    # Filter to YATES files
    yates_files = files[files['schedule_type'] == 'YATES']
    print(f"Found {len(yates_files)} YATES schedule versions")

    if latest_only:
        current_files = yates_files[yates_files['is_current'] == True]
        if len(current_files) > 0:
            yates_files = current_files
        else:
            yates_files = yates_files.sort_values('date', ascending=False).head(1)
        print(f"Using latest: {yates_files.iloc[0]['filename']} (file_id={yates_files.iloc[0]['file_id']})")

    yates_ids = set(yates_files['file_id'].values)

    # Filter to YATES only
    tasks = tasks[tasks['file_id'].isin(yates_ids)].copy()
    wbs = wbs[wbs['file_id'].isin(yates_ids)].copy()
    taskactv = taskactv[taskactv['file_id'].isin(yates_ids)].copy()
    actvcode = actvcode[actvcode['file_id'].isin(yates_ids)].copy()
    actvtype = actvtype[actvtype['file_id'].isin(yates_ids)].copy()

    print(f"Loaded {len(tasks):,} tasks from YATES schedules")
    print(f"Loaded {len(wbs):,} WBS entries")
    print(f"Loaded {len(taskactv):,} task activity code assignments")

    return tasks, wbs, yates_files, taskactv, actvcode, actvtype


def generate_taxonomy(context: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """
    Generate taxonomy by inferring all fields for each task.

    Args:
        context: Combined task context from build_task_context()
        verbose: Print progress messages

    Returns:
        DataFrame with taxonomy columns and source tracking
    """
    if verbose:
        print(f"Generating taxonomy for {len(context):,} tasks...")

    results = []
    total = len(context)

    # Track statistics for each field's source
    stats = {
        'trade': {'activity_code': 0, 'wbs': 0, 'inferred': 0, 'none': 0},
        'building': {'activity_code': 0, 'task_code': 0, 'wbs': 0, 'inferred': 0, 'none': 0},
        'level': {'activity_code': 0, 'wbs': 0, 'inferred': 0, 'none': 0},
        'area': {'wbs': 0, 'none': 0},
        'room': {'wbs': 0, 'none': 0},
        'sub_contractor': {'activity_code': 0, 'none': 0},
        'sub_trade': {'inferred': 0, 'none': 0},
        'phase': {'inferred': 0, 'none': 0},
        'location_type': {'ROOM': 0, 'ELEVATOR': 0, 'STAIR': 0, 'GRIDLINE': 0, 'AREA': 0, 'LEVEL': 0, 'BUILDING': 0, 'MULTI': 0, 'none': 0},
        'impact': {'inferred': 0, 'none': 0},
    }

    for idx, (_, row) in enumerate(context.iterrows()):
        if verbose and idx % 10000 == 0 and idx > 0:
            print(f"  Processed {idx:,}/{total:,} ({idx/total*100:.1f}%)")

        result = infer_all_fields(row)
        results.append(result)

        # Update stats for each field
        stats['trade'][result['trade_source'] or 'none'] += 1
        stats['building'][result['building_source'] or 'none'] += 1
        stats['level'][result['level_source'] or 'none'] += 1
        stats['area'][result['area_source'] or 'none'] += 1
        stats['room'][result['room_source'] or 'none'] += 1
        stats['sub_contractor'][result['sub_source'] or 'none'] += 1
        stats['sub_trade'][result['sub_trade_source'] or 'none'] += 1
        stats['phase'][result['phase_source'] or 'none'] += 1
        stats['location_type'][result['location_type'] or 'none'] += 1
        stats['impact'][result['impact_source'] or 'none'] += 1

    if verbose:
        print(f"  Processed {total:,}/{total:,} (100%)")

    # Print statistics
    if verbose:
        print_statistics(stats, total)

    return pd.DataFrame(results)


def print_statistics(stats: dict, total: int) -> None:
    """Print source statistics for each field."""
    print("\n" + "=" * 60)
    print("SOURCE STATISTICS (Priority: activity_code > wbs > inferred)")
    print("=" * 60)

    for field, sources in stats.items():
        print(f"\n{field.upper()}:")
        for source, count in sources.items():
            if count > 0:
                pct = count / total * 100
                print(f"  {source:15s}: {count:,} ({pct:.1f}%)")


def print_summary(df: pd.DataFrame) -> None:
    """Print classification summary."""
    print("\n" + "=" * 60)
    print("CLASSIFICATION SUMMARY")
    print("=" * 60)

    # Trade distribution
    print("\n--- Trade Distribution ---")
    trade_dist = df.groupby(['trade_id', 'trade_code', 'trade_name']).size().sort_values(ascending=False)
    for (tid, code, name), count in trade_dist.items():
        if pd.notna(tid):
            pct = count / len(df) * 100
            print(f"  {int(tid):2d} {code:12s}: {count:,} ({pct:.1f}%)")
    unmapped = df['trade_id'].isna().sum()
    if unmapped > 0:
        print(f"  -- UNMAPPED: {unmapped:,} ({unmapped/len(df)*100:.1f}%)")

    # Building distribution
    print("\n--- Building Distribution ---")
    bldg_dist = df['building'].value_counts(dropna=False)
    for bldg, count in bldg_dist.items():
        pct = count / len(df) * 100
        bldg_display = bldg if pd.notna(bldg) else '(none)'
        print(f"  {bldg_display}: {count:,} ({pct:.1f}%)")

    # Level distribution
    print("\n--- Level Distribution ---")
    level_dist = df['level'].value_counts(dropna=False).sort_index()
    for lvl, count in level_dist.items():
        pct = count / len(df) * 100
        if pd.isna(lvl):
            lvl_display = '(none)'
        elif lvl in ('GEN', 'MULTI', 'UNK'):
            lvl_display = lvl
        else:
            lvl_display = f"L{lvl}"
        print(f"  {lvl_display}: {count:,} ({pct:.1f}%)")

    # Area/Room coverage
    print("\n--- Area/Room Coverage ---")
    has_area = df['area'].notna().sum()
    has_room = df['room'].notna().sum()
    print(f"  Tasks with area: {has_area:,} ({has_area/len(df)*100:.1f}%)")
    print(f"  Tasks with room: {has_room:,} ({has_room/len(df)*100:.1f}%)")

    # Subcontractor coverage
    has_sub = df['sub_contractor'].notna().sum()
    print(f"\n--- Subcontractor Coverage ---")
    print(f"  Tasks with Z-SUB: {has_sub:,} ({has_sub/len(df)*100:.1f}%)")


def add_dim_location_id(taxonomy: pd.DataFrame) -> pd.DataFrame:
    """
    Add dim_location_id column by looking up building+level in dim_location.

    This enables Power BI relationships between P6 tasks and dim_location.
    Uses fallback logic:
    1. Try building + level first (e.g., "FAB-1F")
    2. If level is missing, try building-wide (e.g., "FAB-ALL")
    3. If building is also missing, try site-wide ("SITE")

    Args:
        taxonomy: DataFrame with 'building' and 'level' columns

    Returns:
        DataFrame with 'dim_location_id' column added (Int64, nullable)
    """
    # Import here to avoid circular imports
    from scripts.shared.dimension_lookup import get_location_id, reset_cache

    # Reset cache to ensure we pick up latest dim_location
    reset_cache()

    print("\nAdding dim_location_id column...")

    def safe_get_location_id(row):
        b = row.get('building')
        l = row.get('level')
        # Pass values to get_location_id - it handles None/NaN with fallback logic
        return get_location_id(b, l, allow_fallback=True)

    taxonomy['dim_location_id'] = taxonomy.apply(safe_get_location_id, axis=1)
    taxonomy['dim_location_id'] = taxonomy['dim_location_id'].astype('Int64')

    # Report coverage
    matched = taxonomy['dim_location_id'].notna().sum()
    total = len(taxonomy)
    print(f"  dim_location_id coverage: {matched:,}/{total:,} ({100*matched/total:.1f}%)")

    # Report breakdown by fallback type
    has_both = ((taxonomy['building'].notna()) & (taxonomy['level'].notna())).sum()
    has_building_only = ((taxonomy['building'].notna()) & (taxonomy['level'].isna())).sum()
    has_neither = ((taxonomy['building'].isna()) & (taxonomy['level'].isna())).sum()
    print(f"    - building+level: {has_both:,}")
    print(f"    - building-only (→ ALL): {has_building_only:,}")
    print(f"    - neither (→ SITE): {has_neither:,}")

    return taxonomy


def main():
    parser = argparse.ArgumentParser(
        description='Generate task taxonomy lookup table for YATES schedules'
    )
    parser.add_argument(
        '--latest-only',
        action='store_true',
        help='Only process the latest YATES schedule'
    )
    parser.add_argument(
        '--output',
        type=str,
        default=None,
        help='Output CSV path (default: derived/primavera/task_taxonomy.csv)'
    )
    parser.add_argument(
        '--skip-location-id',
        action='store_true',
        help='Skip adding dim_location_id column'
    )
    args = parser.parse_args()

    # Load data
    tasks, wbs, files, taskactv, actvcode, actvtype = load_yates_data(
        latest_only=args.latest_only
    )

    # Build combined context
    context = build_task_context(
        tasks_df=tasks,
        wbs_df=wbs,
        taskactv_df=taskactv,
        actvcode_df=actvcode,
        actvtype_df=actvtype,
    )

    # Generate taxonomy
    taxonomy = generate_taxonomy(context)

    # Add dim_location_id for Power BI integration
    if not args.skip_location_id:
        taxonomy = add_dim_location_id(taxonomy)

    # Print summary
    print_summary(taxonomy)

    # Save output
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = Settings.PRIMAVERA_DERIVED_DIR / "task_taxonomy.csv"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    taxonomy.to_csv(output_path, index=False)

    print(f"\n{'='*60}")
    print(f"Saved taxonomy lookup to: {output_path}")
    print(f"Total records: {len(taxonomy):,}")
    print(f"Columns: {list(taxonomy.columns)}")


if __name__ == "__main__":
    main()
