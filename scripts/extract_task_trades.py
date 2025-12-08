#!/usr/bin/env python3
"""
Extract trade/craft labels for tasks from YATES schedule.

Sources (in priority order):
1. Z-TRADE activity code (parsed)
2. Z-SUB CONTRACTOR activity code (parsed)
3. Task name pattern matching (inferred)

Output: data/primavera/generated/task_trades.csv

Usage:
    python scripts/extract_task_trades.py
    python scripts/extract_task_trades.py --latest-only  # Process only latest YATES file
"""

import argparse
import re
from pathlib import Path

import pandas as pd


# Trade inference patterns from task names
# Maps pattern -> standardized trade name
TRADE_PATTERNS = {
    'STEEL_ERECTION': r'DS.?AD|STEEL\s*(ERECT|FRAM|DETAIL)|PENETRATION.*STEEL|CLADDING\s*STEEL|MISC.*STEEL',
    'CONCRETE': r'\bCONC\b|SLAB\b|TOPPING|POUR\b|FRP\b|RAT\s*SLAB|CIP\s*WALL|EPOXY.*PAD|DRILL.*EPOXY',
    'DRYWALL': r'DRYWALL|DW\s|FRAMING\s*-\s*INT|STUD\s*FRAMING|TAPE\s*&\s*FINISH|LAYOUT\s*&\s*STUD',
    'ROOFING': r'\bROOF\b|PARAPET',
    'FIREPROOFING': r'FIREPROOF|FP\s',
    'MEP': r'\bMEP\b|MECH\s*PAD|ELECTRICAL|PLUMBING',
    'WATERPROOFING': r'WATERPROOF|WP\s',
    'PAINTING': r'PAINT|COATING|PRIME.*COAT|INTERMEDIATE\s*COAT|TOP\s*COAT',
    'INSULATION': r'INSULATION|INSUL\b',
    'METAL_PANELS': r'METAL\s*PANEL|IMP\s|SKIN\s|CLADDING(?!.*STEEL)',
    'DOORS': r'\bDOOR\b|HARDWARE',
    'PRECAST': r'PRECAST|PC\s*ERECT|PC\s*FAB',
    'DECKING': r'\bDECK\b|GRATING',
    'MILESTONE': r'MILESTONE|COMPLETE\b|TURNOVER|READY\s*FOR|\bTCO\b|PRIORITY\s*\d',
    'EXPANSION_JOINTS': r'EXPANSION\s*JOINT|EXP\s*JOINT|\bEJ\b|\bVEJ\b|\bHEJ\b|EMSEAL|STEEL\s*COVER\s*INSTALL',
    'INSPECTION': r'\bINSPECT\b|INSPECTION',
    'SCAFFOLDING': r'SCAFFOLD',
    'CANOPY': r'CANOPY|AWNING',
    'STAIRS': r'\bSTAIR\b',
    'OWNER_COORDINATION': r'SECAI\s*(ROUGH|TOOK|DENIAL|APPROVE|DIRECTIVE|ISSUE|RELOCATE|RA\s*FLOOR|REVIEW)',
    'IMPACT': r'^IMPACT\s*[-:]|^IMAPCT\s*[-:]|^IMPACT\s*\[',
    'ELEVATOR': r'\bELEVATOR\b(?!.*HALL)',
    'ARCHITECTURAL': r'TOILET\s*PARTITION|FIELD\s*MEASURE|TOILET\s*ACCESS|COUNTER\s*TOP|BACKING\s*-\s*WALK',
    'RFI': r'\bRFI\b|SHOP\s*DRAW|PRODUCT\s*DATA|SBMT',
    'CONCRETE_REPAIR': r'CRACK\s*REPAIR|SIKA|AMATECH',
    'BATHROOM': r'\bBATHROOM\b|TOILET\s*ROOM',
    'CONTAINMENT': r'CONTAINMENT',
}

# Standardize Z-TRADE codes to categories
TRADE_STANDARDIZATION = {
    # CSI Division 01 - General Requirements
    '01 Towers': 'TOWER_CRANES',
    'Owner': 'OWNER',
    'MILE': 'MILESTONE',
    'BIM / VDC': 'BIM_VDC',
    'TEMP': 'TEMPORARY',
    'Yates': 'GENERAL_CONTRACTOR',

    # CSI Division 02 - Site
    '02 Site': 'SITEWORK',
    'BACKFILL': 'SITEWORK',
    'Excavate': 'SITEWORK',

    # CSI Division 03 - Concrete
    '03 CONC': 'CONCRETE',
    '03 Dril': 'DRILLED_SHAFTS',
    '03 PC Erect': 'PRECAST_ERECTION',
    '03 PC FAB': 'PRECAST_FABRICATION',
    '03 PC Fab-C': 'PRECAST_FABRICATION',
    '03 PC Fab-G': 'PRECAST_FABRICATION',
    '03 PC Fab-H': 'PRECAST_FABRICATION',
    '03 PC Fab-T': 'PRECAST_FABRICATION',
    '03 PRECAST': 'PRECAST',
    'FRP': 'CONCRETE',
    'Cure': 'CONCRETE',
    'RATSLAB': 'CONCRETE',
    'TOPPING': 'CONCRETE',
    'CIP WALL': 'CONCRETE',
    'Elevated slabs': 'CONCRETE',
    'Elevated slabs-1': 'CONCRETE',
    'CONC-AREA': 'CONCRETE',

    # CSI Division 05 - Metals
    '05 Steel Erect': 'STEEL_ERECTION',
    '05 Steel Fab': 'STEEL_FABRICATION',
    'DS/AD': 'STEEL_ERECTION',
    'Decking': 'STEEL_DECKING',
    'MISC STEEL': 'MISC_STEEL',
    'PreAssemble': 'STEEL_FABRICATION',
    'Truss Erect': 'STEEL_ERECTION',
    'Main': 'STEEL_ERECTION',
    'Inner': 'STEEL_ERECTION',
    'Intermediate': 'STEEL_ERECTION',
    'Intermed': 'STEEL_ERECTION',

    # CSI Division 07 - Thermal & Moisture
    '07 EIFS': 'EIFS',
    '07 Roofing': 'ROOFING',
    '07 WATERPROOF': 'WATERPROOFING',
    'INSULATION': 'INSULATION',
    'SKIN': 'METAL_PANELS',
    'EXP CONTROL': 'EXPANSION_CONTROL',
    'JOINT SEALANT': 'SEALANTS',
    'JOINT SEALANT-1': 'SEALANTS',

    # CSI Division 08 - Openings
    'DOORS': 'DOORS_HARDWARE',
    'DOORS-1': 'DOORS_HARDWARE',
    'DOORS-2': 'DOORS_HARDWARE',

    # CSI Division 09 - Finishes
    'DRYWALL': 'DRYWALL',
    'CFMF': 'METAL_FRAMING',
    'BERG': 'DRYWALL',
    'PAINTING': 'PAINTING',
    'CRC': 'FLOOR_COATING',
    'FLOORING': 'FLOORING',
    'ARCH': 'ARCHITECTURAL',
    'FIREPROOFING': 'FIREPROOFING',

    # CSI Division 14 - Conveying
    'STAIRS': 'STAIRS',

    # CSI Division 15/22 - Plumbing
    '15 - PLUMBING': 'PLUMBING',
    'UG FIRE-PLUMB': 'PLUMBING',

    # CSI Division 16/26 - Electrical
    '16 - U/G ELECT': 'ELECTRICAL',

    # MEP General
    'MEP': 'MEP',
    'SOMD': 'SOMD',

    # Location-based (should be filtered or handled separately)
    'A1': 'AREA_A1', 'A2': 'AREA_A2', 'A3': 'AREA_A3', 'A4': 'AREA_A4', 'A5': 'AREA_A5',
    'B1': 'AREA_B1', 'B2': 'AREA_B2', 'B3': 'AREA_B3', 'B4': 'AREA_B4', 'B5': 'AREA_B5',
    'SUE': 'AREA_SUE', 'SUW': 'AREA_SUW',
    'SUE (A&B)': 'AREA_SUE', 'SUW(N&M)': 'AREA_SUW',
    'SUE Cols A & B': 'AREA_SUE', 'SUE Inner Cols': 'AREA_SUE',
    'SUW Cols M & N': 'AREA_SUW', 'SUW Inner Cols': 'AREA_SUW',
    'FIZ': 'AREA_FIZ', 'FIZEAST': 'AREA_FIZ', 'FIZWEST': 'AREA_FIZ',

    # Miscellaneous
    'MISCELLANEOUS': 'MISCELLANEOUS',
}


def infer_trade_from_name(task_name: str) -> tuple[str | None, str]:
    """
    Infer trade from task name using pattern matching.

    Returns: (trade, source) where source is always 'inferred'
    """
    if not task_name or pd.isna(task_name):
        return None, 'manual'

    name_upper = str(task_name).upper()

    for trade, pattern in TRADE_PATTERNS.items():
        if re.search(pattern, name_upper):
            return trade, 'inferred'

    return None, 'manual'


def standardize_trade(raw_trade: str) -> str:
    """Standardize a raw trade code to a category."""
    if raw_trade in TRADE_STANDARDIZATION:
        return TRADE_STANDARDIZATION[raw_trade]
    return raw_trade.upper().replace(' ', '_').replace('-', '_')


def extract_task_trades(
    tasks: pd.DataFrame,
    taskactv: pd.DataFrame,
    actvtype: pd.DataFrame,
    actvcode: pd.DataFrame,
    file_id: int
) -> pd.DataFrame:
    """
    Extract trade labels for tasks in a single file.

    Returns DataFrame with columns:
        file_id, task_id, trade, trade_source, trade_raw,
        subcontractor, subcontractor_source
    """
    # Filter to this file
    tasks_f = tasks[tasks['file_id'] == file_id].copy()
    taskactv_f = taskactv[taskactv['file_id'] == file_id]
    actvtype_f = actvtype[actvtype['file_id'] == file_id]
    actvcode_f = actvcode[actvcode['file_id'] == file_id]

    # Get activity type IDs
    z_trade_types = actvtype_f[actvtype_f['actv_code_type'] == 'Z-TRADE']['actv_code_type_id'].values
    z_sub_types = actvtype_f[actvtype_f['actv_code_type'] == 'Z-SUB CONTRACTOR']['actv_code_type_id'].values

    # Build lookup: task_id -> trade code
    trade_lookup = {}
    if len(z_trade_types) > 0:
        trade_codes = actvcode_f[actvcode_f['actv_code_type_id'].isin(z_trade_types)]
        trade_assignments = taskactv_f[taskactv_f['actv_code_id'].isin(trade_codes['actv_code_id'])]
        trade_merged = trade_assignments.merge(
            trade_codes[['actv_code_id', 'short_name']],
            on='actv_code_id'
        )
        for _, row in trade_merged.iterrows():
            trade_lookup[row['task_id']] = row['short_name']

    # Build lookup: task_id -> subcontractor
    sub_lookup = {}
    if len(z_sub_types) > 0:
        sub_codes = actvcode_f[actvcode_f['actv_code_type_id'].isin(z_sub_types)]
        sub_assignments = taskactv_f[taskactv_f['actv_code_id'].isin(sub_codes['actv_code_id'])]
        sub_merged = sub_assignments.merge(
            sub_codes[['actv_code_id', 'short_name', 'actv_code_name']],
            on='actv_code_id'
        )
        for _, row in sub_merged.iterrows():
            sub_lookup[row['task_id']] = (row['short_name'], row['actv_code_name'])

    # Process each task
    results = []
    for _, task in tasks_f.iterrows():
        task_id = task['task_id']
        task_name = task.get('task_name', '')

        # Trade from activity code (parsed)
        trade_raw = trade_lookup.get(task_id)
        if trade_raw:
            trade = standardize_trade(trade_raw)
            trade_source = 'parsed'
        else:
            # Try to infer from task name
            trade, trade_source = infer_trade_from_name(task_name)
            trade_raw = None

        # Subcontractor from activity code (parsed)
        sub_info = sub_lookup.get(task_id)
        if sub_info:
            subcontractor = sub_info[0]
            subcontractor_name = sub_info[1]
            subcontractor_source = 'parsed'
        else:
            subcontractor = None
            subcontractor_name = None
            subcontractor_source = 'manual'

        results.append({
            'file_id': file_id,
            'task_id': task_id,
            'trade': trade,
            'trade_source': trade_source,
            'trade_raw': trade_raw,
            'subcontractor': subcontractor,
            'subcontractor_name': subcontractor_name,
            'subcontractor_source': subcontractor_source,
        })

    return pd.DataFrame(results)


def main():
    parser = argparse.ArgumentParser(description='Extract task trade labels')
    parser.add_argument('--processed-dir', type=Path, default=Path('data/primavera/processed'))
    parser.add_argument('--output-dir', type=Path, default=Path('data/primavera/generated'))
    parser.add_argument('--latest-only', action='store_true', help='Process only latest YATES file')
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Load data
    print("Loading data...")
    xer_files = pd.read_csv(args.processed_dir / 'xer_files.csv')
    tasks = pd.read_csv(args.processed_dir / 'task.csv', low_memory=False)
    taskactv = pd.read_csv(args.processed_dir / 'taskactv.csv')
    actvtype = pd.read_csv(args.processed_dir / 'actvtype.csv')
    actvcode = pd.read_csv(args.processed_dir / 'actvcode.csv')

    # Get YATES files
    yates_files = xer_files[xer_files['schedule_type'] == 'YATES'].sort_values('date')

    if args.latest_only:
        yates_files = yates_files.tail(1)

    print(f"Processing {len(yates_files)} YATES files...")

    all_results = []
    for _, xer_file in yates_files.iterrows():
        file_id = xer_file['file_id']
        filename = xer_file['filename']

        result = extract_task_trades(tasks, taskactv, actvtype, actvcode, file_id)
        all_results.append(result)

        # Stats for this file
        n_tasks = len(result)
        n_trade_parsed = (result['trade_source'] == 'parsed').sum()
        n_trade_inferred = (result['trade_source'] == 'inferred').sum()
        n_sub_parsed = (result['subcontractor_source'] == 'parsed').sum()

        print(f"  {filename[:50]}: {n_tasks} tasks, "
              f"trade={n_trade_parsed} parsed + {n_trade_inferred} inferred, "
              f"sub={n_sub_parsed} parsed")

    # Combine results
    df = pd.concat(all_results, ignore_index=True)

    # Summary
    print("\n=== Overall Summary ===")
    print(f"Total task-file combinations: {len(df)}")

    print("\nTrade Coverage:")
    trade_counts = df['trade_source'].value_counts()
    for source in ['parsed', 'inferred', 'manual']:
        count = trade_counts.get(source, 0)
        pct = 100 * count / len(df)
        print(f"  {source}: {count} ({pct:.1f}%)")

    print("\nSubcontractor Coverage:")
    sub_counts = df['subcontractor_source'].value_counts()
    for source in ['parsed', 'manual']:
        count = sub_counts.get(source, 0)
        pct = 100 * count / len(df)
        print(f"  {source}: {count} ({pct:.1f}%)")

    print("\nTop 15 Trades:")
    trade_summary = df[df['trade'].notna()].groupby('trade').size().sort_values(ascending=False)
    for trade, count in trade_summary.head(15).items():
        print(f"  {trade}: {count}")

    print("\nTop 10 Subcontractors:")
    sub_summary = df[df['subcontractor'].notna()].groupby(['subcontractor', 'subcontractor_name']).size()
    sub_summary = sub_summary.sort_values(ascending=False)
    for (sub, name), count in sub_summary.head(10).items():
        print(f"  {sub} ({name}): {count}")

    # Save output
    output_path = args.output_dir / 'task_trades.csv'
    df.to_csv(output_path, index=False)
    print(f"\n✓ Saved {len(df)} rows to {output_path}")

    # Save manual review items (latest file only, tasks without any trade label)
    latest_id = yates_files.iloc[-1]['file_id']
    manual_review = df[(df['file_id'] == latest_id) &
                       (df['trade_source'] == 'manual') &
                       (df['subcontractor_source'] == 'manual')]

    if len(manual_review) > 0:
        # Get task names for review
        task_names = tasks[tasks['task_id'].isin(manual_review['task_id'])][['task_id', 'task_name']]
        manual_review = manual_review.merge(task_names, on='task_id')

        review_path = args.output_dir / 'task_trades_manual_review.csv'
        manual_review.to_csv(review_path, index=False)
        print(f"✓ Saved {len(manual_review)} items for manual review to {review_path}")


if __name__ == '__main__':
    main()
