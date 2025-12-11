#!/usr/bin/env python3
"""
Generate WBS Taxonomy Lookup Table for YATES Schedules

Creates a lookup table mapping task_id to taxonomy classifications with descriptions.
Join with task.csv on task_id for full task details.

Output columns:
- task_id: Primary key (includes file_id prefix)
- phase/phase_desc: PRE/STR/ENC/INT/COM/ADM/UNK + description
- scope/scope_desc: Work type (DRY/STL/MEP/etc.) + description
- loc_type/loc_type_desc: Location granularity (RM/EL/ST/GL/AR/GEN) + description
- loc_id: Specific location (FAB146103, EL22, GL17-18, etc.)
- building/building_desc: FAB/SUE/SUW/FIZ/CUB/GCS/GEN/MULTI/UNK + description
- level/level_desc: 1-6/B1/GEN/MULTI/UNK + description
- label: Combined classification label
- impact_code/type/type_desc: For IMPACT tasks only
- attributed_to/attributed_to_desc: Party attribution for impacts
- root_cause/root_cause_desc: Root cause category for impacts

Output: data/primavera/analysis/wbs_taxonomy.csv

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


def enrich_tasks(tasks_df: pd.DataFrame, wbs_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate taxonomy lookup table for tasks.

    Output is a lean join table with task_id and taxonomy columns only.
    Join with task.csv on task_id for full task details.

    Args:
        tasks_df: Tasks dataframe
        wbs_df: WBS dataframe

    Returns:
        Lean dataframe with task_id + taxonomy columns only
    """
    classifier = TaskClassifier()

    # Get WBS name lookup for classification context
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

        # Classify
        classification = classifier.classify_task(task_name, wbs_name)

        # Build result row - task_id + taxonomy with descriptions
        result = {
            'task_id': row.get('task_id'),
            # Taxonomy classification with descriptions
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
            # Impact tracking (sparse - only populated for IMPACT tasks)
            'impact_code': classification.get('impact_code'),
            'impact_type': classification.get('impact_type'),
            'impact_type_desc': classification.get('impact_type_desc'),
            'attributed_to': classification.get('attributed_to'),
            'attributed_to_desc': classification.get('attributed_to_desc'),
            'root_cause': classification.get('root_cause'),
            'root_cause_desc': classification.get('root_cause_desc'),
        }

        results.append(result)

    print(f"  Processed {total:,}/{total:,} (100%)")

    return pd.DataFrame(results)


def generate_summary(df: pd.DataFrame, tasks_df: pd.DataFrame = None) -> None:
    """Print summary statistics for the taxonomy data."""
    print("\n" + "=" * 60)
    print("CLASSIFICATION SUMMARY")
    print("=" * 60)

    # Phase distribution
    print("\n--- Phase Distribution ---")
    phase_dist = df.groupby(['phase', 'phase_desc']).size().sort_values(ascending=False)
    for (phase, desc), count in phase_dist.items():
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
    loc_dist = df.groupby(['loc_type', 'loc_type_desc']).size().sort_values(ascending=False)
    for (loc_type, desc), count in loc_dist.items():
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
            lvl_display = lvl
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

    # Impact/Delay Analysis
    impact_tasks = df[df['scope'] == 'IMP']
    print(f"\n--- Impact/Delay Tracking ---")
    print(f"  Total IMPACT tasks: {len(impact_tasks):,} ({len(impact_tasks)/len(df)*100:.1f}%)")
    if len(impact_tasks) > 0:
        # Attribution breakdown
        attributed = impact_tasks[impact_tasks['attributed_to'].notna()]
        print(f"  With attribution: {len(attributed):,} ({len(attributed)/len(impact_tasks)*100:.1f}%)")
        if len(attributed) > 0:
            print("  Top attributed parties:")
            attr_dist = attributed['attributed_to'].value_counts().head(5)
            for party, count in attr_dist.items():
                pct = count / len(impact_tasks) * 100
                print(f"    {party}: {count:,} ({pct:.1f}%)")

        # Root cause breakdown
        with_cause = impact_tasks[impact_tasks['root_cause'].notna()]
        if len(with_cause) > 0:
            print("  Root cause categories:")
            cause_dist = with_cause.groupby(['root_cause', 'root_cause_desc']).size().sort_values(ascending=False).head(8)
            for (cause, desc), count in cause_dist.items():
                pct = count / len(impact_tasks) * 100
                print(f"    {cause} ({desc}): {count:,} ({pct:.1f}%)")

    # Unknown classifications
    unk_df = df[df['phase'] == 'UNK']
    print(f"\n--- Unclassified ---")
    print(f"  Total: {len(unk_df)} ({len(unk_df)/len(df)*100:.4f}%)")
    if len(unk_df) > 0 and tasks_df is not None:
        # Look up task names from original data
        unk_ids = set(unk_df['task_id'].values)
        unk_tasks = tasks_df[tasks_df['task_id'].isin(unk_ids)]
        print("  Sample unclassified tasks:")
        for task in unk_tasks['task_name'].head(5):
            print(f"    - {task[:70]}")


def main():
    parser = argparse.ArgumentParser(description='Generate WBS taxonomy lookup table')
    parser.add_argument('--latest-only', action='store_true',
                        help='Only process the latest YATES schedule')
    parser.add_argument('--output', type=str,
                        default='data/primavera/analysis/wbs_taxonomy.csv',
                        help='Output CSV path')
    args = parser.parse_args()

    # Load data
    tasks, wbs, files = load_yates_data(latest_only=args.latest_only)

    # Generate taxonomy lookup table
    taxonomy = enrich_tasks(tasks, wbs)

    # Generate summary (pass tasks for UNK sample lookup)
    generate_summary(taxonomy, tasks)

    # Save output
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    taxonomy.to_csv(output_path, index=False)
    print(f"\n✓ Saved taxonomy lookup to: {output_path}")
    print(f"  Total records: {len(taxonomy):,}")
    print(f"  Columns: {list(taxonomy.columns)}")


if __name__ == "__main__":
    main()
