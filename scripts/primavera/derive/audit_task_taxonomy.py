#!/usr/bin/env python3
"""
Audit Task Taxonomy Quality

Randomly samples tasks from the generated taxonomy and displays them alongside
original P6 data (activity codes, WBS) for quality verification and iteration.

Usage:
    python scripts/primavera/derive/audit_task_taxonomy.py <num_tasks> [--output FILE]

Examples:
    python scripts/primavera/derive/audit_task_taxonomy.py 50
    python scripts/primavera/derive/audit_task_taxonomy.py 100 --output audit_sample.csv
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config.settings import Settings


def load_data() -> tuple:
    """Load all necessary data files."""
    data_dir = Settings.PRIMAVERA_PROCESSED_DIR
    derived_dir = Settings.PRIMAVERA_DERIVED_DIR

    print("Loading data files...")

    # Load P6 source data
    tasks = pd.read_csv(data_dir / "task.csv", low_memory=False)
    wbs = pd.read_csv(data_dir / "projwbs.csv", low_memory=False)
    files = pd.read_csv(data_dir / "xer_files.csv")
    taskactv = pd.read_csv(data_dir / "taskactv.csv", low_memory=False)
    actvcode = pd.read_csv(data_dir / "actvcode.csv", low_memory=False)
    actvtype = pd.read_csv(data_dir / "actvtype.csv", low_memory=False)

    # Load generated taxonomy
    taxonomy = pd.read_csv(derived_dir / "task_taxonomy.csv")

    # Filter to YATES latest only
    yates_files = files[files['schedule_type'] == 'YATES']
    current_files = yates_files[yates_files['is_current'] == True]
    if len(current_files) > 0:
        yates_files = current_files
    else:
        yates_files = yates_files.sort_values('date', ascending=False).head(1)

    yates_ids = set(yates_files['file_id'].values)

    tasks = tasks[tasks['file_id'].isin(yates_ids)].copy()
    wbs = wbs[wbs['file_id'].isin(yates_ids)].copy()
    taskactv = taskactv[taskactv['file_id'].isin(yates_ids)].copy()
    actvcode = actvcode[actvcode['file_id'].isin(yates_ids)].copy()
    actvtype = actvtype[actvtype['file_id'].isin(yates_ids)].copy()

    print(f"Loaded {len(tasks):,} YATES tasks")
    print(f"Loaded {len(taxonomy):,} taxonomy records")

    return tasks, wbs, taskactv, actvcode, actvtype, taxonomy


def build_activity_code_lookup(taskactv_df, actvcode_df, actvtype_df) -> dict:
    """Build lookup of activity codes per task."""
    type_lookup = dict(zip(actvtype_df['actv_code_type_id'], actvtype_df['actv_code_type']))
    code_lookup = dict(zip(actvcode_df['actv_code_id'], actvcode_df['actv_code_name']))

    # Map code_id -> code_type
    code_to_type = {}
    for _, row in actvcode_df.iterrows():
        actv_code_id = row['actv_code_id']
        type_id = row['actv_code_type_id']
        if type_id in type_lookup:
            code_to_type[actv_code_id] = type_lookup[type_id]

    # Build task -> activity codes lookup
    task_actv_lookup = {}
    code_types_of_interest = {'Z-TRADE', 'Z-BLDG', 'Z-LEVEL', 'Z-SUB CONTRACTOR', 'Z-AREA'}

    for _, row in taskactv_df.iterrows():
        task_id = row['task_id']
        actv_code_id = row['actv_code_id']

        code_type = code_to_type.get(actv_code_id)
        code_value = code_lookup.get(actv_code_id)

        if code_type and code_value and code_type in code_types_of_interest:
            if task_id not in task_actv_lookup:
                task_actv_lookup[task_id] = {}
            task_actv_lookup[task_id][code_type] = code_value

    return task_actv_lookup


def build_audit_dataset(
    tasks_df, wbs_df, taxonomy_df, task_actv_lookup, num_samples
) -> pd.DataFrame:
    """Build audit dataset combining taxonomy with source data."""

    # Randomly sample tasks from taxonomy
    print(f"\nRandomly sampling {num_samples:,} tasks...")
    sampled_taxonomy = taxonomy_df.sample(n=min(num_samples, len(taxonomy_df)), random_state=42)

    # Merge with task data
    audit = sampled_taxonomy.merge(
        tasks_df[['task_id', 'task_name', 'task_code', 'wbs_id']],
        on='task_id',
        how='left'
    )

    # Merge with WBS data
    audit = audit.merge(
        wbs_df[['wbs_id', 'wbs_name', 'tier_2', 'tier_3', 'tier_4', 'tier_5', 'tier_6']],
        on='wbs_id',
        how='left'
    )

    # Add activity codes as separate columns
    audit['z_trade_actv'] = audit['task_id'].map(
        lambda x: task_actv_lookup.get(x, {}).get('Z-TRADE')
    )
    audit['z_bldg_actv'] = audit['task_id'].map(
        lambda x: task_actv_lookup.get(x, {}).get('Z-BLDG')
    )
    audit['z_level_actv'] = audit['task_id'].map(
        lambda x: task_actv_lookup.get(x, {}).get('Z-LEVEL')
    )
    audit['z_area_actv'] = audit['task_id'].map(
        lambda x: task_actv_lookup.get(x, {}).get('Z-AREA')
    )
    audit['z_sub_actv'] = audit['task_id'].map(
        lambda x: task_actv_lookup.get(x, {}).get('Z-SUB CONTRACTOR')
    )

    return audit


def print_audit_summary(audit_df) -> None:
    """Print summary statistics of audit sample."""
    print("\n" + "=" * 80)
    print("AUDIT SAMPLE SUMMARY")
    print("=" * 80)

    print(f"\nTotal sampled: {len(audit_df):,}")

    # Trade coverage
    print("\nTrade Source Distribution:")
    trade_sources = audit_df['trade_source'].value_counts(dropna=False)
    for source, count in trade_sources.items():
        source_label = source if pd.notna(source) else "None"
        print(f"  {source_label}: {count:,} ({count/len(audit_df)*100:.1f}%)")

    # Building coverage
    print("\nBuilding Source Distribution:")
    building_sources = audit_df['building_source'].value_counts(dropna=False)
    for source, count in building_sources.items():
        source_label = source if pd.notna(source) else "None"
        print(f"  {source_label}: {count:,} ({count/len(audit_df)*100:.1f}%)")

    # Location type distribution
    print("\nLocation Type Distribution:")
    loc_types = audit_df['location_type'].value_counts(dropna=False)
    for loc_type, count in loc_types.items():
        loc_label = loc_type if pd.notna(loc_type) else "None"
        print(f"  {loc_label}: {count:,} ({count/len(audit_df)*100:.1f}%)")

    # Activity code usage
    print("\nActivity Code Usage in Sample:")
    has_z_trade = audit_df['z_trade_actv'].notna().sum()
    has_z_bldg = audit_df['z_bldg_actv'].notna().sum()
    has_z_level = audit_df['z_level_actv'].notna().sum()
    has_z_area = audit_df['z_area_actv'].notna().sum()
    has_z_sub = audit_df['z_sub_actv'].notna().sum()

    print(f"  Z-TRADE: {has_z_trade:,} ({has_z_trade/len(audit_df)*100:.1f}%)")
    print(f"  Z-BLDG: {has_z_bldg:,} ({has_z_bldg/len(audit_df)*100:.1f}%)")
    print(f"  Z-LEVEL: {has_z_level:,} ({has_z_level/len(audit_df)*100:.1f}%)")
    print(f"  Z-AREA: {has_z_area:,} ({has_z_area/len(audit_df)*100:.1f}%)")
    print(f"  Z-SUB: {has_z_sub:,} ({has_z_sub/len(audit_df)*100:.1f}%)")


def print_audit_sample(audit_df, num_print: int = 10) -> None:
    """Print sample rows for visual inspection."""
    print("\n" + "=" * 80)
    print(f"SAMPLE AUDIT ROWS ({min(num_print, len(audit_df))} of {len(audit_df):,})")
    print("=" * 80)

    # Select columns for display
    display_cols = [
        'task_id', 'task_name', 'task_code',
        'z_trade_actv', 'trade_code', 'trade_source',
        'z_bldg_actv', 'building', 'building_source',
        'z_level_actv', 'level', 'level_source',
        'z_area_actv', 'location_type', 'location_code',
        'tier_3', 'tier_4', 'tier_5'
    ]

    for idx, (_, row) in enumerate(audit_df.head(num_print).iterrows()):
        print(f"\n--- Task {idx + 1} ---")
        print(f"ID: {row['task_id']}")
        print(f"Name: {row['task_name']}")
        print(f"Code: {row['task_code']}")

        print(f"\nActivity Codes (from P6):")
        print(f"  Z-TRADE: {row['z_trade_actv']}")
        print(f"  Z-BLDG: {row['z_bldg_actv']}")
        print(f"  Z-LEVEL: {row['z_level_actv']}")
        print(f"  Z-AREA: {row['z_area_actv']}")
        print(f"  Z-SUB: {row['z_sub_actv']}")

        print(f"\nWBS Context:")
        print(f"  Tier 3: {row['tier_3']}")
        print(f"  Tier 4: {row['tier_4']}")
        print(f"  Tier 5: {row['tier_5']}")

        print(f"\nInferred Taxonomy:")
        print(f"  Trade: {row['trade_code']} (source: {row['trade_source']})")
        print(f"  Building: {row['building']} (source: {row['building_source']})")
        print(f"  Level: {row['level']} (source: {row['level_source']})")
        print(f"  Location: {row['location_type']} - {row['location_code']}")
        print(f"  Area: {row['area']}")
        print(f"  Room: {row['room']}")


def main():
    parser = argparse.ArgumentParser(
        description='Audit task taxonomy quality by sampling and comparing with source data'
    )
    parser.add_argument(
        'num_tasks',
        type=int,
        help='Number of tasks to randomly sample for audit'
    )
    parser.add_argument(
        '--output',
        type=str,
        default=None,
        help='Output CSV file for audit data (optional)'
    )
    parser.add_argument(
        '--print',
        type=int,
        default=10,
        help='Number of rows to print to console (default: 10)'
    )
    args = parser.parse_args()

    # Load data
    tasks, wbs, taskactv, actvcode, actvtype, taxonomy = load_data()

    # Build activity code lookup
    print("Building activity code lookup...")
    task_actv_lookup = build_activity_code_lookup(taskactv, actvcode, actvtype)

    # Build audit dataset
    audit = build_audit_dataset(tasks, wbs, taxonomy, task_actv_lookup, args.num_tasks)

    # Print summary
    print_audit_summary(audit)

    # Print sample rows
    print_audit_sample(audit, args.print)

    # Save to CSV if requested
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Select useful columns for export
        export_cols = [
            'task_id', 'task_name', 'task_code', 'wbs_id',
            'z_trade_actv', 'trade_id', 'trade_code', 'trade_source',
            'z_bldg_actv', 'building', 'building_source',
            'z_level_actv', 'level', 'level_source',
            'z_area_actv', 'area', 'area_source',
            'z_sub_actv', 'sub_contractor', 'sub_source',
            'room', 'room_source',
            'location_type', 'location_code',
            'sub_trade', 'phase',
            'tier_2', 'tier_3', 'tier_4', 'tier_5', 'tier_6',
            'wbs_name', 'label'
        ]

        # Only keep columns that exist
        export_cols = [c for c in export_cols if c in audit.columns]
        audit[export_cols].to_csv(output_path, index=False)
        print(f"\nAudit data exported to: {output_path}")
        print(f"Columns: {export_cols}")


if __name__ == "__main__":
    main()
