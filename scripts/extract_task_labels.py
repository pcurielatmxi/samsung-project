#!/usr/bin/env python3
"""
Extract location and trade labels for tasks using multiple data sources.

Data Priority (highest to lowest):
1. WBS: WBS hierarchy labels (only if WBS label is parsed/inherited - well-maintained)
2. ACTIVITY_CODE: Activity codes (Z-BLDG, Z-LEVEL, Z-ROOM, Z-TRADE, Z-SUB CONTRACTOR)
3. INFERRED: Task name pattern matching (last resort)

Note: WBS has priority over activity codes because activity codes appear less
well-maintained in the source data.

Output: data/primavera/generated/task_labels.csv

Usage:
    python scripts/extract_task_labels.py
    python scripts/extract_task_labels.py --schedule-type YATES
    python scripts/extract_task_labels.py --file-id 48
"""

import argparse
import re
from pathlib import Path

import pandas as pd


# =============================================================================
# ACTIVITY CODE MAPPINGS
# =============================================================================

# Z-BLDG activity code -> standard building code
Z_BLDG_MAPPING = {
    'SUP-E': 'SUE',
    'SUP-W': 'SUW',
    'SUP': 'SUP',       # Overall support (needs level to distinguish E/W)
    'FIZ': 'FIZ',
    'FAB': 'FAB',
    'A': 'FAB',         # South areas (Team A) = FAB
    'B': 'FAB',         # North areas (Team B) = FAB
    'PIERS': 'FAB',     # Drilled piers = FAB
    'BIM OA': 'ALL',    # BIM/VDC overall
    'MILE': 'ALL',      # Milestones
}

# Z-LEVEL activity code -> standard level code
Z_LEVEL_MAPPING = {
    'L1': 'L1',
    'L2': 'L2',
    'L3': 'L3',
    'L4': 'L4',
    'L5-s': 'L5',
    'L5-f': 'ROOF',
    'L6': 'L6',
    'L1Ext': 'L1',
    'L2 / L3': 'MULTI',
    'ROOF': 'ROOF',
    'UG': 'UG',
    'FNDN': 'FOUNDATION',
}

# Z-TRADE activity code -> standard trade code
Z_TRADE_MAPPING = {
    'SKIN': 'METAL_PANELS',
    'ARCH': 'ARCHITECTURAL',
    'DRYWALL': 'DRYWALL',
    'TOPPING': 'CONCRETE',
    'Decking': 'DECKING',
    'FIREPROOFING': 'FIREPROOFING',
    'FRP': 'CONCRETE',
    'SOMD': 'SOMD',
    'DS/AD': 'STEEL_ERECTION',
    '07 Roofing': 'ROOFING',
    'Cure': 'CONCRETE',
    '05 Steel Erect': 'STEEL_ERECTION',
    '07 WATERPROOF': 'WATERPROOFING',
    '03 PC FAB': 'PRECAST',
    '03 PC ERECT': 'PRECAST',
    'MEP': 'MEP',
    'BIM / VDC': 'BIM_VDC',
    '01 Towers': 'TOWER_CRANES',
    'TEMP': 'TEMPORARY',
    'Owner': 'OWNER',
    'MILE': 'MILESTONE',
    'Exp Joint': 'EXPANSION_JOINTS',
    '08 Exterior Doors': 'DOORS',
    '08 Interior Doors': 'DOORS',
    '08 Glass/Glazing': 'GLAZING',
    'ELEVATOR': 'ELEVATOR',
    '03 Concrete': 'CONCRETE',
    '05 MISC Steel': 'STEEL_MISC',
    '07 Insulation': 'INSULATION',
    '09 Painting': 'PAINTING',
    'SCAFFOLD': 'SCAFFOLDING',
}

# Z-SUB CONTRACTOR -> standard names (passthrough mostly)
Z_SUB_MAPPING = {
    'BERG': 'BERG',
    'BRAZOS': 'BRAZOS',
    'SECAI': 'SECAI',
    'RP': 'ROLLING_PLAINS',
    'KOVACH': 'KOVACH',
    'BAKER': 'BAKER',
    'CHERRY': 'CHERRY_COATINGS',
    'PAINT': 'FINISH_PAINT',
    'W&W': 'W_AND_W',
    'PATRIOT': 'PATRIOT',
    'ALPHA': 'ALPHA',
    'PP': 'PERRY_PERRY',
    'BCO': 'BERNCO',
    'DOORS': 'DOORS_HARDWARE',
    'LAT': 'LATCON',
    'ALK': 'ALK',
    'MK': 'MK_MARLOW',
    'INFTY': 'INFINITY',
}


# =============================================================================
# TASK NAME INFERENCE PATTERNS (Last Resort)
# =============================================================================

BUILDING_PATTERNS = {
    # Support East: SUE, SUP-E, SEA1-5, SEB1-5
    'SUE': r'\bSUE\b|\bSUP[- ]*E\b|SUPPORT.*EAST|\bSE[AB]\d?\b',
    # Support West: SUW, SUP-W, SWA1-5, SWB1-5
    'SUW': r'\bSUW\b|\bSUP[- ]*W\b|SUPPORT.*WEST|\bSW[AB]\d?\b',
    # FIZ/Data Center
    'FIZ': r'\bFIZ\b|DATA\s*CENTER',
    # FAB building
    'FAB': r'\bFAB\b(?!.*SAMSUNG)|WAFFLE|BUNKER',
}

LEVEL_PATTERNS = {
    'L1': r'\bL1\b|\b1F\b|LEVEL\s*1|FNDN|FOUNDATION',
    'L2': r'\bL2\b|\b2F\b|LEVEL\s*2|SUBFAB',
    'L3': r'\bL3\b|\b3F\b|LEVEL\s*3|WAFFLE',
    'L4': r'\bL4\b|\b4F\b|LEVEL\s*4',
    'L6': r'\bL6\b|\b6F\b|LEVEL\s*6|PENTHOUSE',
    'ROOF': r'\bROOF\b|PARAPET',
    'UG': r'\bUG\b|UNDERGROUND',
}

TRADE_PATTERNS = {
    'STEEL_ERECTION': r'DS.?AD|STEEL\s*(ERECT|FRAM)|MISC.*STEEL',
    'CONCRETE': r'\bCONC\b|\bSLAB\b|TOPPING|POUR\b|FRP\b',
    'DRYWALL': r'DRYWALL|DW\s|FRAMING.*INT|STUD\s*FRAM',
    'ROOFING': r'\bROOF\b|PARAPET',
    'FIREPROOFING': r'FIREPROOF|FP\s|FIRE\s*CAULK|FIRESTOP',
    'WATERPROOFING': r'WATERPROOF|WP\s',
    'PAINTING': r'PAINT|COATING',
    'METAL_PANELS': r'METAL\s*PANEL|IMP\s|SKIN\s',
    'DOORS': r'\bDOOR\b|HARDWARE',
    'PRECAST': r'PRECAST|PC\s*ERECT',
    'DECKING': r'\bDECK\b|GRATING',
    'EXPANSION_JOINTS': r'EXPANSION\s*JOINT|EXP\s*JOINT|\bEJ\b|CONTROL\s*JOINT',
    'ELEVATOR': r'\bELEVATOR\b',
    'MEP': r'\bMEP\b|MECH\s*PAD|ELECTRICAL|PLUMBING',
    # Note: Avoid "COMPLETE\b" as it catches "Complete Fire Caulk" work tasks
    'MILESTONE': r'\bMILESTONE\b|\bTCO\s*\d*\b|TARGET\s*DATE|COMPLETION\s*TARGET',
}


# =============================================================================
# ACTIVITY CODE EXTRACTION
# =============================================================================

def build_activity_code_lookup(
    taskactv: pd.DataFrame,
    actvcode: pd.DataFrame,
    actvtype: pd.DataFrame,
    actv_type_name: str,
    mapping: dict,
    passthrough: bool = False
) -> dict:
    """
    Build lookup: task_id -> mapped value for a given activity code type.

    Args:
        passthrough: If True, pass through unmapped values. If False, only include
                    values that are explicitly in the mapping.

    Returns dict with task_id as key and (value, raw_code) as value.
    """
    # Find the activity code type
    atype = actvtype[actvtype['actv_code_type'] == actv_type_name]
    if len(atype) == 0:
        return {}

    type_id = atype.iloc[0]['actv_code_type_id']

    # Get codes for this type
    codes = actvcode[actvcode['actv_code_type_id'] == type_id]
    code_id_to_short = dict(zip(codes['actv_code_id'], codes['short_name']))

    # Build task -> value lookup
    result = {}
    relevant_assignments = taskactv[taskactv['actv_code_id'].isin(codes['actv_code_id'])]

    for _, row in relevant_assignments.iterrows():
        task_id = row['task_id']
        code_id = row['actv_code_id']
        short_name = code_id_to_short.get(code_id, '')

        # Map to standard value
        if passthrough:
            mapped = mapping.get(short_name, short_name)  # Passthrough if not in mapping
        else:
            mapped = mapping.get(short_name)  # Only use mapped values

        if mapped and task_id not in result:
            result[task_id] = (mapped, short_name)

    return result


def infer_from_task_name(task_name: str, patterns: dict) -> str | None:
    """Infer a label from task name using regex patterns."""
    if not task_name:
        return None

    name_upper = str(task_name).upper()

    for label, pattern in patterns.items():
        if re.search(pattern, name_upper, re.IGNORECASE):
            return label

    return None


def extract_explicit_level(task_name: str) -> str | None:
    """
    Extract explicit level mention from task name.

    Looks for patterns like "(L2)", "L3 CEILING", "SUBFAB" which explicitly
    indicate the physical level of work.

    Returns standardized level (L1, L2, L3, L4, L5, L6, ROOF) or None.
    """
    if not task_name:
        return None

    name_upper = str(task_name).upper()

    # Pattern 1: Explicit (L2), (L3), etc. in parentheses - highest confidence
    match = re.search(r'\(L(\d)\)', name_upper)
    if match:
        return f'L{match.group(1)}'

    # Pattern 2: SUBFAB always means L2
    if 'SUBFAB' in name_upper:
        return 'L2'

    # Pattern 3: "L2 CEILING", "L3 FLOOR", etc. with context
    match = re.search(r'\bL(\d)\s+(CEILING|FLOOR|SLAB|BEAM)', name_upper)
    if match:
        return f'L{match.group(1)}'

    # Pattern 4: "- L2 -" or "- L3" at end with delimiters
    match = re.search(r'[-\s]L(\d)(?:\s*[-\s]|$)', name_upper)
    if match:
        return f'L{match.group(1)}'

    return None


# =============================================================================
# MAIN PROCESSING
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='Extract task labels')
    parser.add_argument('--processed-dir', type=Path,
                        default=Path('data/primavera/processed'))
    parser.add_argument('--generated-dir', type=Path,
                        default=Path('data/primavera/generated'))
    parser.add_argument('--schedule-type', choices=['YATES', 'SECAI', 'ALL'],
                        default='YATES', help='Schedule type to process')
    parser.add_argument('--file-id', type=int, help='Specific file_id to process')
    args = parser.parse_args()

    args.generated_dir.mkdir(parents=True, exist_ok=True)

    # Load data
    print("Loading data...")
    xer_files = pd.read_csv(args.processed_dir / 'xer_files.csv')
    tasks = pd.read_csv(args.processed_dir / 'task.csv', low_memory=False)
    taskactv = pd.read_csv(args.processed_dir / 'taskactv.csv', low_memory=False)
    actvcode = pd.read_csv(args.processed_dir / 'actvcode.csv', low_memory=False)
    actvtype = pd.read_csv(args.processed_dir / 'actvtype.csv', low_memory=False)

    # Load WBS labels (for fallback)
    wbs_labels_path = args.generated_dir / 'wbs_labels.csv'
    if wbs_labels_path.exists():
        wbs_labels = pd.read_csv(wbs_labels_path)
        print(f"Loaded WBS labels: {len(wbs_labels)} nodes")
    else:
        print("WARNING: wbs_labels.csv not found. Run extract_wbs_labels.py first.")
        wbs_labels = pd.DataFrame()

    # Determine which file to process
    if args.file_id:
        file_ids = [args.file_id]
    elif args.schedule_type != 'ALL':
        file_ids = xer_files[xer_files['schedule_type'] == args.schedule_type]['file_id'].tolist()
        # Use latest file only
        latest = xer_files[xer_files['file_id'].isin(file_ids)].sort_values('date').iloc[-1]
        file_ids = [latest['file_id']]
        print(f"Using latest {args.schedule_type} file: {latest['filename']}")
    else:
        file_ids = xer_files['file_id'].tolist()

    # Filter to selected file(s)
    tasks = tasks[tasks['file_id'].isin(file_ids)]
    taskactv = taskactv[taskactv['file_id'].isin(file_ids)]
    actvcode = actvcode[actvcode['file_id'].isin(file_ids)]
    actvtype = actvtype[actvtype['file_id'].isin(file_ids)]

    print(f"Processing {len(tasks):,} tasks from {len(file_ids)} file(s)")

    # Build activity code lookups
    print("\nBuilding activity code lookups...")
    zbldg_lookup = build_activity_code_lookup(taskactv, actvcode, actvtype, 'Z-BLDG', Z_BLDG_MAPPING)
    zlevel_lookup = build_activity_code_lookup(taskactv, actvcode, actvtype, 'Z-LEVEL', Z_LEVEL_MAPPING)
    zroom_lookup = build_activity_code_lookup(taskactv, actvcode, actvtype, 'Z-ROOM', {}, passthrough=True)
    ztrade_lookup = build_activity_code_lookup(taskactv, actvcode, actvtype, 'Z-TRADE', Z_TRADE_MAPPING)
    zsub_lookup = build_activity_code_lookup(taskactv, actvcode, actvtype, 'Z-SUB CONTRACTOR', Z_SUB_MAPPING, passthrough=True)

    print(f"  Z-BLDG: {len(zbldg_lookup):,} tasks")
    print(f"  Z-LEVEL: {len(zlevel_lookup):,} tasks")
    print(f"  Z-ROOM: {len(zroom_lookup):,} tasks")
    print(f"  Z-TRADE: {len(ztrade_lookup):,} tasks")
    print(f"  Z-SUB CONTRACTOR: {len(zsub_lookup):,} tasks")

    # Build WBS lookup
    wbs_lookup = {}
    if len(wbs_labels) > 0:
        for _, row in wbs_labels.iterrows():
            wbs_id = row['wbs_id']
            wbs_lookup[wbs_id] = {
                'building': row.get('building'),
                'building_source': row.get('building_source'),
                'level': row.get('level'),
                'level_source': row.get('level_source'),
                'room': row.get('room'),
                'room_source': row.get('room_source'),
            }

    # Process each task
    print("\nLabeling tasks...")
    results = []
    stats = {
        'building': {'actv_code': 0, 'wbs': 0, 'inferred': 0, 'none': 0},
        'level': {'task_name': 0, 'wbs': 0, 'actv_code': 0, 'inferred': 0, 'none': 0},
        'room': {'actv_code': 0, 'wbs': 0, 'inferred': 0, 'none': 0},
        'trade': {'actv_code': 0, 'inferred': 0, 'none': 0},
        'subcontractor': {'actv_code': 0, 'none': 0},
    }
    conflicts = []

    for _, task in tasks.iterrows():
        task_id = task['task_id']
        wbs_id = task['wbs_id']
        task_name = task.get('task_name', '')

        row = {
            'task_id': task_id,
            'wbs_id': wbs_id,
            'task_code': task.get('task_code', ''),
            'task_name': task_name,
        }

        # --- BUILDING ---
        building = None
        building_source = None
        building_raw = None

        # Priority 1: WBS hierarchy (only if parsed/inherited - well-maintained)
        if wbs_id in wbs_lookup:
            wbs_bldg = wbs_lookup[wbs_id]
            if wbs_bldg['building'] and wbs_bldg['building_source'] in ['parsed', 'inherited']:
                building = wbs_bldg['building']
                building_source = 'wbs'
                stats['building']['wbs'] += 1

        # Priority 2: Activity code (Z-BLDG)
        if building is None and task_id in zbldg_lookup:
            building, building_raw = zbldg_lookup[task_id]
            building_source = 'actv_code'
            stats['building']['actv_code'] += 1

        # Priority 3: Infer from task name
        if building is None:
            building = infer_from_task_name(task_name, BUILDING_PATTERNS)
            if building:
                building_source = 'inferred'
                stats['building']['inferred'] += 1
            else:
                stats['building']['none'] += 1

        # Check for conflicts between WBS and activity code (for reporting)
        if task_id in zbldg_lookup and wbs_id in wbs_lookup:
            actv_bldg = zbldg_lookup[task_id][0]
            wbs_bldg = wbs_lookup[wbs_id]['building']
            if actv_bldg and wbs_bldg and actv_bldg != wbs_bldg:
                # Normalize for comparison (SUP could match SUE or SUW)
                if not (actv_bldg == 'SUP' and wbs_bldg in ['SUE', 'SUW']):
                    conflicts.append({
                        'task_id': task_id,
                        'task_name': task_name[:50],
                        'dimension': 'building',
                        'actv_code_value': actv_bldg,
                        'wbs_value': wbs_bldg,
                    })

        row['building'] = building
        row['building_source'] = building_source
        row['building_raw'] = building_raw

        # --- LEVEL ---
        # Note: For level, task name is often more specific than WBS because
        # WBS organizes by work phase (e.g., "FAB FOUNDATIONS") while task names
        # specify the actual physical level (e.g., "TOPPING SLAB - SUBFAB (L2)")
        level = None
        level_source = None
        level_raw = None

        # Priority 1: Explicit level in task name (highest specificity)
        explicit_level = extract_explicit_level(task_name)
        if explicit_level:
            level = explicit_level
            level_source = 'task_name'
            stats['level']['task_name'] += 1
        # Priority 2: WBS hierarchy (if not generic like MULTI/ALL)
        elif wbs_id in wbs_lookup:
            wbs_lvl = wbs_lookup[wbs_id]
            wbs_level_val = wbs_lvl['level']
            if wbs_level_val and wbs_lvl['level_source'] in ['parsed', 'inherited']:
                # Skip generic WBS levels if task name might have more specific info
                if wbs_level_val not in ['MULTI', 'ALL', 'N/A']:
                    level = wbs_level_val
                    level_source = 'wbs'
                    stats['level']['wbs'] += 1

        # Priority 3: Activity code (Z-LEVEL)
        if level is None and task_id in zlevel_lookup:
            level, level_raw = zlevel_lookup[task_id]
            level_source = 'actv_code'
            stats['level']['actv_code'] += 1

        # Priority 4: Infer from task name patterns
        if level is None:
            level = infer_from_task_name(task_name, LEVEL_PATTERNS)
            if level:
                level_source = 'inferred'
                stats['level']['inferred'] += 1
            else:
                stats['level']['none'] += 1

        row['level'] = level
        row['level_source'] = level_source
        row['level_raw'] = level_raw

        # --- ROOM ---
        room = None
        room_source = None
        room_raw = None

        # Priority 1: WBS hierarchy (well-maintained)
        if wbs_id in wbs_lookup:
            wbs_rm = wbs_lookup[wbs_id]
            if wbs_rm['room'] and wbs_rm['room_source'] in ['parsed', 'inherited']:
                room = wbs_rm['room']
                room_source = 'wbs'
                stats['room']['wbs'] += 1

        # Priority 2: Activity code (Z-ROOM)
        if room is None and task_id in zroom_lookup:
            room, room_raw = zroom_lookup[task_id]
            room_source = 'actv_code'
            stats['room']['actv_code'] += 1

        if room is None:
            stats['room']['none'] += 1

        row['room'] = room
        row['room_source'] = room_source
        row['room_raw'] = room_raw

        # --- TRADE ---
        trade = None
        trade_source = None
        trade_raw = None

        if task_id in ztrade_lookup:
            trade, trade_raw = ztrade_lookup[task_id]
            trade_source = 'actv_code'
            stats['trade']['actv_code'] += 1
        else:
            trade = infer_from_task_name(task_name, TRADE_PATTERNS)
            if trade:
                trade_source = 'inferred'
                stats['trade']['inferred'] += 1
            else:
                stats['trade']['none'] += 1

        row['trade'] = trade
        row['trade_source'] = trade_source
        row['trade_raw'] = trade_raw

        # --- SUBCONTRACTOR ---
        subcontractor = None
        subcontractor_source = None
        subcontractor_raw = None

        if task_id in zsub_lookup:
            subcontractor, subcontractor_raw = zsub_lookup[task_id]
            subcontractor_source = 'actv_code'
            stats['subcontractor']['actv_code'] += 1
        else:
            stats['subcontractor']['none'] += 1

        row['subcontractor'] = subcontractor
        row['subcontractor_source'] = subcontractor_source
        row['subcontractor_raw'] = subcontractor_raw

        results.append(row)

    # Create output DataFrame
    df = pd.DataFrame(results)

    # Print summary
    total = len(tasks)
    print("\n" + "=" * 70)
    print("TASK LABELING SUMMARY")
    print("=" * 70)
    print(f"\nTotal tasks: {total:,}")

    for dim in ['building', 'level', 'room', 'trade', 'subcontractor']:
        print(f"\n{dim.upper()}:")
        dim_stats = stats[dim]

        if 'task_name' in dim_stats and dim_stats['task_name'] > 0:
            tn = dim_stats['task_name']
            print(f"  Task name (explicit):    {tn:,} ({tn/total*100:.1f}%)")

        if 'wbs' in dim_stats:
            wbs = dim_stats['wbs']
            print(f"  WBS hierarchy:           {wbs:,} ({wbs/total*100:.1f}%)")

        if 'actv_code' in dim_stats:
            actv = dim_stats['actv_code']
            print(f"  Activity code:           {actv:,} ({actv/total*100:.1f}%)")

        if 'inferred' in dim_stats:
            inf = dim_stats['inferred']
            print(f"  Pattern inference:       {inf:,} ({inf/total*100:.1f}%)")

        none = dim_stats['none']
        print(f"  Not labeled:             {none:,} ({none/total*100:.1f}%)")

        # Calculate traceable (task_name explicit mentions are also traceable)
        traceable = dim_stats.get('task_name', 0) + dim_stats.get('wbs', 0) + dim_stats.get('actv_code', 0)
        print(f"  => Traceable to raw data: {traceable:,} ({traceable/total*100:.1f}%)")

    # Value distributions
    print("\n" + "=" * 70)
    print("VALUE DISTRIBUTIONS")
    print("=" * 70)

    for col in ['building', 'level', 'trade']:
        print(f"\n{col.upper()}:")
        for val, cnt in df[col].value_counts().head(10).items():
            if val:
                print(f"  {str(val):20s}: {cnt:,}")

    # Conflicts
    if conflicts:
        print("\n" + "=" * 70)
        print(f"CONFLICTS: {len(conflicts)} tasks where activity code != WBS")
        print("=" * 70)
        for c in conflicts[:10]:
            print(f"  {c['task_id']}: {c['dimension']} - actv={c['actv_code_value']} vs wbs={c['wbs_value']}")
        if len(conflicts) > 10:
            print(f"  ... and {len(conflicts) - 10} more")

        # Save conflicts
        conflicts_df = pd.DataFrame(conflicts)
        conflicts_path = args.generated_dir / 'task_labels_conflicts.csv'
        conflicts_df.to_csv(conflicts_path, index=False)
        print(f"\nConflicts saved to: {conflicts_path}")

    # Save output
    output_path = args.generated_dir / 'task_labels.csv'
    df.to_csv(output_path, index=False)
    print(f"\n{'=' * 70}")
    print(f"Output saved to: {output_path}")
    print(f"Total tasks labeled: {len(df):,}")


if __name__ == '__main__':
    main()
