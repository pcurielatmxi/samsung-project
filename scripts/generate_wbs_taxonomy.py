#!/usr/bin/env python3
"""
Generate Enriched WBS Taxonomy Table for YATES Schedules

This script processes all YATES schedule tasks and enriches them with:
- Phase classification (PRE/STR/ENC/INT/COM/ADM)
- Scope category (DRY/STL/MEP/etc.)
- Location type (RM/EL/ST/GL/AR/BL/BD/NA)
- Location ID (FAB146103, EL22, GL17-18, etc.)

Output: data/primavera/analysis/wbs_taxonomy_enriched.csv

Usage:
    python scripts/generate_wbs_taxonomy.py [--latest-only] [--output PATH]
"""

import argparse
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from src.classifiers.task_classifier import TaskClassifier


def load_yates_data(latest_only: bool = False) -> tuple:
    """
    Load YATES schedule data.

    Args:
        latest_only: If True, only load the most recent schedule

    Returns:
        Tuple of (tasks_df, wbs_df, files_df)
    """
    data_dir = Path("data/primavera/processed")

    print("Loading data files...")
    tasks = pd.read_csv(data_dir / "task.csv", low_memory=False)
    wbs = pd.read_csv(data_dir / "projwbs.csv", low_memory=False)
    files = pd.read_csv(data_dir / "xer_files.csv")

    # Filter to YATES files (includes SAMSUNG-TFAB1 which is also YATES)
    yates_files = files[files['filename'].str.contains('YATES|SAMSUNG-TFAB1', case=False, na=False, regex=True)]
    print(f"Found {len(yates_files)} YATES schedule versions")

    if latest_only:
        # Parse dates from filenames (more reliable than date column)
        import re
        def parse_date_from_filename(filename):
            # Pattern: MM-DD-YY or MM.DD.YY (but avoid TFAB1 prefix)
            # Look for date patterns after known prefixes
            match = re.search(r'(?:update|schedule|TFAB1)[\s\-]*(?:DD\s*)?(\d{1,2})[-.](\d{1,2})[-.](\d{2,4})', filename, re.I)
            if match:
                m, d, y = match.groups()
                y = int(y)
                if y < 100:
                    y = 2000 + y if y < 50 else 1900 + y
                try:
                    return pd.Timestamp(year=y, month=int(m), day=int(d))
                except:
                    pass
            return pd.NaT

        yates_files = yates_files.copy()
        yates_files['parsed_date'] = yates_files['filename'].apply(parse_date_from_filename)
        valid_dates = yates_files[yates_files['parsed_date'].notna()]
        if len(valid_dates) > 0:
            yates_files = valid_dates.sort_values('parsed_date', ascending=False).head(1)
        else:
            yates_files = yates_files.sort_values('file_id', ascending=False).head(1)
        print(f"Using latest: {yates_files.iloc[0]['filename']} (file_id={yates_files.iloc[0]['file_id']})")

    yates_ids = set(yates_files['file_id'].values)

    # Filter tasks and WBS to YATES only
    tasks = tasks[tasks['file_id'].isin(yates_ids)].copy()
    wbs = wbs[wbs['file_id'].isin(yates_ids)].copy()

    print(f"Loaded {len(tasks):,} tasks from YATES schedules")
    print(f"Loaded {len(wbs):,} WBS entries")

    return tasks, wbs, yates_files


def build_wbs_hierarchy(wbs_df: pd.DataFrame) -> dict:
    """
    Build a mapping of wbs_id to full WBS path.

    Returns:
        Dict mapping wbs_id to tuple of (wbs_name, parent_names...)
    """
    # Build parent lookup
    wbs_lookup = wbs_df.set_index('wbs_id')[['wbs_name', 'parent_wbs_id']].to_dict('index')

    def get_hierarchy(wbs_id, visited=None):
        if visited is None:
            visited = set()
        if wbs_id in visited or wbs_id not in wbs_lookup:
            return []
        visited.add(wbs_id)
        entry = wbs_lookup[wbs_id]
        result = [entry['wbs_name']]
        if pd.notna(entry['parent_wbs_id']):
            result.extend(get_hierarchy(entry['parent_wbs_id'], visited))
        return result

    return {wbs_id: get_hierarchy(wbs_id) for wbs_id in wbs_lookup.keys()}


def enrich_tasks(tasks_df: pd.DataFrame, wbs_df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich tasks with WBS hierarchy and taxonomy classifications.

    Args:
        tasks_df: Tasks dataframe
        wbs_df: WBS dataframe

    Returns:
        Enriched dataframe with taxonomy columns
    """
    classifier = TaskClassifier()

    # Build WBS hierarchy
    print("Building WBS hierarchy...")
    wbs_hierarchy = build_wbs_hierarchy(wbs_df)

    # Get WBS name lookup
    wbs_names = wbs_df.set_index('wbs_id')['wbs_name'].to_dict()

    # Prepare output
    results = []
    total = len(tasks_df)

    print(f"Classifying {total:,} tasks...")

    for idx, (_, row) in enumerate(tasks_df.iterrows()):
        if idx % 10000 == 0 and idx > 0:
            print(f"  Processed {idx:,}/{total:,} ({idx/total*100:.1f}%)")

        task_name = str(row.get('task_name', ''))
        wbs_id = row.get('wbs_id')
        wbs_name = wbs_names.get(wbs_id, '')

        # Get full WBS path
        wbs_path = wbs_hierarchy.get(wbs_id, [])
        wbs_path_str = ' > '.join(reversed(wbs_path)) if wbs_path else ''

        # Classify
        classification = classifier.classify_task(task_name, wbs_name)

        # Build result row
        result = {
            'file_id': row.get('file_id'),
            'task_id': row.get('task_id'),
            'task_code': row.get('task_code'),
            'task_name': task_name,
            'wbs_id': wbs_id,
            'wbs_name': wbs_name,
            'wbs_path': wbs_path_str,
            'phase': classification['phase'],
            'phase_desc': classification['phase_desc'],
            'scope': classification['scope'],
            'scope_desc': classification['scope_desc'],
            'loc_type': classification['loc_type'],
            'loc_type_desc': classification['loc_type_desc'],
            'loc_id': classification['loc_id'],
            'building': classification['building'],
            'building_desc': classification['building_desc'],
            'level': classification['level'],
            'level_desc': classification['level_desc'],
            'label': classification['label'],
            # Include schedule fields for analysis
            'target_start_date': row.get('target_start_date'),
            'target_end_date': row.get('target_end_date'),
            'act_start_date': row.get('act_start_date'),
            'act_end_date': row.get('act_end_date'),
            'status_code': row.get('status_code'),
            'total_float_hr_cnt': row.get('total_float_hr_cnt'),
        }

        results.append(result)

    print(f"  Processed {total:,}/{total:,} (100%)")

    return pd.DataFrame(results)


def generate_summary(df: pd.DataFrame) -> None:
    """Print summary statistics for the enriched data."""
    print("\n" + "=" * 60)
    print("CLASSIFICATION SUMMARY")
    print("=" * 60)

    # Phase distribution
    print("\n--- Phase Distribution ---")
    phase_dist = df['phase'].value_counts()
    for phase, count in phase_dist.items():
        desc = df[df['phase'] == phase]['phase_desc'].iloc[0]
        pct = count / len(df) * 100
        print(f"  {phase} ({desc}): {count:,} ({pct:.1f}%)")

    # Scope distribution (top 15)
    print("\n--- Top 15 Scope Categories ---")
    scope_dist = df.groupby(['phase', 'scope', 'scope_desc']).size().sort_values(ascending=False).head(15)
    for (phase, scope, desc), count in scope_dist.items():
        pct = count / len(df) * 100
        print(f"  {phase}-{scope} ({desc}): {count:,} ({pct:.1f}%)")

    # Location type distribution
    print("\n--- Location Type Distribution ---")
    loc_dist = df['loc_type'].value_counts()
    for loc_type, count in loc_dist.items():
        desc = df[df['loc_type'] == loc_type]['loc_type_desc'].iloc[0]
        pct = count / len(df) * 100
        print(f"  {loc_type} ({desc}): {count:,} ({pct:.1f}%)")

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
            lvl_display = lvl  # Don't add L prefix to special codes
        else:
            lvl_display = f"L{lvl}"
        print(f"  {lvl_display}: {count:,} ({pct:.1f}%)")

    # Tasks with room-level location
    room_tasks = df[df['loc_type'] == 'RM']
    print(f"\n--- Room-Level Tracking ---")
    print(f"  Tasks with FAB room codes: {len(room_tasks):,} ({len(room_tasks)/len(df)*100:.1f}%)")
    if len(room_tasks) > 0:
        unique_rooms = room_tasks['loc_id'].nunique()
        print(f"  Unique rooms tracked: {unique_rooms:,}")

    # Unknown classifications
    unk = df[df['phase'] == 'UNK']
    print(f"\n--- Unclassified ---")
    print(f"  Total: {len(unk)} ({len(unk)/len(df)*100:.2f}%)")
    if len(unk) > 0:
        print("  Sample unclassified tasks:")
        for task in unk['task_name'].head(5):
            print(f"    - {task[:70]}")


def main():
    parser = argparse.ArgumentParser(description='Generate enriched WBS taxonomy table')
    parser.add_argument('--latest-only', action='store_true',
                        help='Only process the latest YATES schedule')
    parser.add_argument('--output', type=str,
                        default='data/primavera/analysis/wbs_taxonomy_enriched.csv',
                        help='Output CSV path')
    args = parser.parse_args()

    # Load data
    tasks, wbs, files = load_yates_data(latest_only=args.latest_only)

    # Enrich tasks
    enriched = enrich_tasks(tasks, wbs)

    # Generate summary
    generate_summary(enriched)

    # Save output
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    enriched.to_csv(output_path, index=False)
    print(f"\n✓ Saved enriched data to: {output_path}")
    print(f"  Total records: {len(enriched):,}")

    # Also save a version grouped by file_id for version comparison
    if not args.latest_only:
        summary_path = output_path.parent / 'wbs_taxonomy_by_version.csv'
        version_summary = enriched.groupby(['file_id', 'phase']).size().unstack(fill_value=0)
        version_summary = version_summary.merge(
            files[['file_id', 'filename', 'date']],
            on='file_id'
        )
        version_summary.to_csv(summary_path, index=False)
        print(f"✓ Saved version summary to: {summary_path}")


if __name__ == "__main__":
    main()
