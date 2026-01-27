#!/usr/bin/env python3
"""
Explore and iterate on P6 task location categorization.

This is THE script for iterating on location type rules. It:
1. Loads P6 data directly (full context with task names)
2. Applies current categorization rules
3. Analyzes grid recovery potential
4. Shows samples for review and iteration

Location Types:
- ROOM: Physical rooms with grid bounds
- STAIR: Stairwells
- ELEVATOR: Elevators
- GRIDLINE: Grid coordinates/spans
- LEVEL: B1, 1F-5F, ROOF, OUTSIDE (legitimately floor-wide only)
- BUILDING: FAB1 only (project-wide)
- UNDEFINED: Everything else

Usage:
    python -m scripts.integrated_analysis.location.explore_location_types
    python -m scripts.integrated_analysis.location.explore_location_types --output results.csv
"""

import re
import argparse
import sys
from pathlib import Path
from collections import Counter

import pandas as pd

project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

taxonomy_path = project_root / 'scripts' / 'primavera' / 'derive'
sys.path.insert(0, str(taxonomy_path))

from src.config.settings import Settings
from task_taxonomy import build_task_context, infer_all_fields


# =============================================================================
# LOCATION TYPE RULES (iterate here)
# =============================================================================

VALID_LEVELS = {'B1', '1F', '2F', '3F', '4F', '5F', 'ROOF', 'OUTSIDE'}
GRANULAR_TYPES = {'ROOM', 'STAIR', 'ELEVATOR'}

# FAB grid area mapping: A1-A5, B1-B5 → gridline bounds
# A = south half, B = north half
# 1-5 = west to east zones
FAB_GRID_AREAS = {
    'A1': {'row_min': 'A', 'row_max': 'G', 'col_min': 1, 'col_max': 5},
    'A2': {'row_min': 'A', 'row_max': 'G', 'col_min': 6, 'col_max': 12},
    'A3': {'row_min': 'A', 'row_max': 'G', 'col_min': 13, 'col_max': 18},
    'A4': {'row_min': 'A', 'row_max': 'G', 'col_min': 19, 'col_max': 25},
    'A5': {'row_min': 'A', 'row_max': 'G', 'col_min': 26, 'col_max': 33},
    'B1': {'row_min': 'H', 'row_max': 'N', 'col_min': 1, 'col_max': 5},
    'B2': {'row_min': 'H', 'row_max': 'N', 'col_min': 6, 'col_max': 12},
    'B3': {'row_min': 'H', 'row_max': 'N', 'col_min': 13, 'col_max': 18},
    'B4': {'row_min': 'H', 'row_max': 'N', 'col_min': 19, 'col_max': 25},
    'B5': {'row_min': 'H', 'row_max': 'N', 'col_min': 26, 'col_max': 33},
}

# FIZ (Data Center) grid area mapping
# FIZ is east of FAB, uses rows E-J approximately
# FIZ1 = West Inner, FIZ2 = West Outer, FIZ3 = East Inner, FIZ4 = East Outer
FIZ_GRID_AREAS = {
    'FIZ1': {'row_min': 'G', 'row_max': 'J', 'col_min': 34, 'col_max': 38},  # West Inner
    'FIZ2': {'row_min': 'G', 'row_max': 'J', 'col_min': 39, 'col_max': 43},  # West Outer
    'FIZ3': {'row_min': 'E', 'row_max': 'G', 'col_min': 34, 'col_max': 38},  # East Inner
    'FIZ4': {'row_min': 'E', 'row_max': 'G', 'col_min': 39, 'col_max': 43},  # East Outer
}

# Project-wide task patterns - these legitimately apply to FAB1 (whole project)
# These are NOT "location unknown" - they are "applies to whole project"
PROJECT_WIDE_PATTERNS = [
    # Contract/Subcontract execution
    r'EXECUTE\s+SUBCONTRACT',
    r'SUBCONTRACT\s+/',
    r'LOI\s*-\s*EXECUTE',
    r'NTP\s*-',
    # Submittals and shop drawings (project-level admin)
    r'SUBCONTRACTOR\s+SUBMIT',
    r'SUBCONTRACTOR\s+SUBMITTALS',
    r'SHOP\s+DWG[S]?\s+(?:REVIEW|APPROVAL|SUBMIT)',
    r'IFC\s+PACKAGE',
    # Engineering review/approval (Jacobs, SECAI)
    r'JACOBS\s+REVIEW',
    r'SECAI\s+(?:REVIEW|APPROVAL)',
    r'SECAI/JACOBS\s+REVIEW',
    # Owner/GC milestones
    r'^OWNER\s*-',
    r'^GC\s*-',
    r'^MILE\s*-',
    # Completion milestones
    r'COMPLETE$',
    r'SUBSTANTIAL\s+COMPLETION',
    r'TCO\s+COMPLETION',
    # Fabrication/Lead time (procurement)
    r'FABRICATION\s*/\s*LEAD\s*TIME',
    r'LEAD\s*TIME',
    r'MATERIAL\s+DELIVERY',
    r'BEGIN\s+.*DELIVERIES',
    r'TRUSS\s+DELIVERY',
    r'PRECAST\s+DELIVER',
    # Mockups (project-wide)
    r'MOCKUP\s+APPROVAL',
    # Design/Engineering delays (project impact)
    r'ENGINEERING\s+DELAY',
    r'STOP\s+WORK',
    r'ON\s+HOLD',
    r'RE-?DESIGN\s*-',
    # RFI/Submittal admin
    r'RFI\s+\d+.*APPROVAL',
    r'SUBMITTAL\s+.*APPROVAL',
    r'PROD\s+DATA\s*-\s*SUBMITTAL',
    # Expected release/design issues
    r'EXPECTED\s+RELEASE',
    r'RESOLVE.*DESIGN\s+ISSUES',
    # Drawing coordination
    r'CONSOLIDATED\s+.*SET',
    r'CONSOLIDATED\s+.*DRAWING',
    r'DRAWING\s+(?:SET|PACKAGES)\s+.*APPROVAL',
    # Owner coordination
    r'OWNER\s+&\s+CITY',
    # EOR/Engineer approvals
    r'EOR\s+APPROVAL',
    # Fabrication status
    r'NOT\s+FABRICATED',
    r'SCRUB\s+/',
    r'SHOP\s+TICKETS',
    # Procurement/Bidding
    r'BID\s+PACKAGES',
    r'BIDDING\s+OUT',
    r'BID\s+LEVELING',
    r'RECOMMEND\s+TO\s+SECAI',
    # Drawing issuance
    r'IFR\s+.*DWG\s+ISSUED',
    r'IFC\s+.*DWG\s+ISSUED',
    r'SUBMITTAL.*ISSUED\s+FOR\s+APPROVAL',
    # Impact/Delay tracking
    r'^IMPACT\s*-',
]


def is_project_wide_task(row: pd.Series) -> str | None:
    """
    Check if task is legitimately project-wide (applies to FAB1 as a whole).

    Returns pattern name if matched, None otherwise.
    """
    # Check task_name and wbs_name for patterns
    text_fields = [
        row.get('task_name', ''),
        row.get('wbs_name', ''),
        row.get('tier_4', ''),
    ]

    for text in text_fields:
        if not text or pd.isna(text):
            continue
        text = str(text).upper()

        for pattern in PROJECT_WIDE_PATTERNS:
            if re.search(pattern, text):
                return pattern

    return None


def extract_grid_from_text(text: str) -> dict:
    """
    Extract grid patterns from text.

    Returns dict with pattern type and extracted value.
    """
    if not text or pd.isna(text):
        return {}

    text = str(text).upper()
    result = {}

    # Pattern 1: FAB grid areas - "A1", "A-2", "B3", "B-5"
    fab_match = re.search(r'\b([AB])[-]?([1-5])\b', text)
    if fab_match:
        area_code = f"{fab_match.group(1)}{fab_match.group(2)}"
        if area_code in FAB_GRID_AREAS:
            result['fab_area'] = area_code
            result['grid_bounds'] = FAB_GRID_AREAS[area_code]

    # Pattern 2: Support building areas - SEA-1, SWA-3, SEB-2, SWB-4
    # Also matches SUEN-5, SUES-1, SUWN-3, SUWS-2 (with cardinal directions)
    support_match = re.search(r'(SE[AB]|SW[AB]|SUE[NS]|SUW[NS])[-\s]*(\d+)', text)
    if support_match:
        # Normalize: SUEN -> SEA, SUES -> SEB, SUWN -> SWA, SUWS -> SWB
        area = support_match.group(1)
        num = support_match.group(2)
        if area.startswith('SUE'):
            area = 'SEA' if 'N' in area else 'SEB'
        elif area.startswith('SUW'):
            area = 'SWA' if 'N' in area else 'SWB'
        result['support_area'] = f"{area}-{num}"

    # Pattern 3: FIZ areas - "Area FIZ1", "FIZ1", "FIZ 2"
    fiz_area_match = re.search(r'(?:AREA\s*)?FIZ\s*([1-4])', text)
    if fiz_area_match:
        fiz_code = f"FIZ{fiz_area_match.group(1)}"
        if fiz_code in FIZ_GRID_AREAS:
            result['fiz_area'] = fiz_code
            result['grid_bounds'] = FIZ_GRID_AREAS[fiz_code]

    # Pattern 4: FIZ grid refs - "J-G", "G-E" (row ranges)
    fiz_grid_match = re.search(r'\b([E-J])-([E-J])\b', text)
    if fiz_grid_match and 'fiz_area' not in result:
        result['fiz_grid'] = f"{fiz_grid_match.group(1)}-{fiz_grid_match.group(2)}"

    # Pattern 5: Explicit gridline - "GL 5", "GRIDLINE C", or standalone gridline range "31-33", "19-16"
    gl_match = re.search(r'(?:GL|GRIDLINE)\s*[-]?\s*([A-N]|\d+)', text)
    if gl_match:
        result['gridline'] = gl_match.group(1)

    # Pattern 6: Gridline range in z_area - "31-33", "19-16"
    gridline_range = re.search(r'\b(\d{1,2})-(\d{1,2})\b', text)
    if gridline_range and 'gridline' not in result:
        result['gridline_range'] = f"{gridline_range.group(1)}-{gridline_range.group(2)}"

    # Pattern 7: Stair numbers - "Stairs #1,2,3,4" or "STAIR 21"
    stair_match = re.search(r'STAIR[S]?\s*[#]?\s*(\d+(?:\s*,\s*\d+)*)', text)
    if stair_match:
        result['stair'] = f"STR-{stair_match.group(1).split(',')[0].strip()}"

    # Pattern 8: Elevator numbers
    elev_match = re.search(r'ELEVATOR[S]?\s*[#]?\s*(\d+)', text)
    if elev_match:
        result['elevator'] = f"ELV-{elev_match.group(1).zfill(2)}"

    # Pattern 9: Grid intersection - "E10", "A21", "J17", etc. (row letter + col number)
    # This is a common way to specify column locations
    grid_intersect = re.search(r'\b([A-N])[\s/]?(\d{1,2})\b', text)
    if grid_intersect and 'gridline' not in result and 'fab_area' not in result:
        row = grid_intersect.group(1)
        col = grid_intersect.group(2)
        # Verify it's a valid grid (col 1-43)
        if 1 <= int(col) <= 43:
            result['grid_intersect'] = f"{row}/{col}"

    # Pattern 10: Column range mentions - "16 & 17", "17 &18", "17/18"
    col_range = re.search(r'\b(\d{1,2})\s*[&/]\s*(\d{1,2})\b', text)
    if col_range and 'gridline_range' not in result:
        c1, c2 = int(col_range.group(1)), int(col_range.group(2))
        # Verify valid column range
        if 1 <= c1 <= 43 and 1 <= c2 <= 43:
            result['col_range'] = f"{c1}-{c2}"

    return result


def categorize_task(row: pd.Series) -> dict:
    """
    Categorize a task using current rules.

    Returns dict with:
        - new_type: Location type
        - reason: Why this categorization
        - grid_info: Any extracted grid info
    """
    current_type = row.get('location_type')
    location_code = row.get('location_code')
    level = row.get('level')
    building = row.get('building')

    # Check ALL relevant fields for location info
    fields_to_check = [
        row.get('task_name', ''),
        row.get('task_code', ''),
        row.get('wbs_name', ''),
        row.get('tier_3', ''),
        row.get('tier_4', ''),
        row.get('tier_5', ''),
        row.get('tier_6', ''),
        row.get('z_area', ''),      # Activity code - often has gridlines!
        row.get('z_level', ''),     # Activity code - has level info
    ]

    # Try to extract grid from multiple sources
    grid_info = {}
    for text in fields_to_check:
        extracted = extract_grid_from_text(text)
        if extracted:
            # Don't overwrite existing patterns
            for key, val in extracted.items():
                if key not in grid_info:
                    grid_info[key] = val

    # 1. Granular types pass through
    if current_type in GRANULAR_TYPES:
        return {
            'new_type': current_type,
            'reason': current_type,
            'grid_info': grid_info,
        }

    # 2. Already GRIDLINE - keep it
    if current_type == 'GRIDLINE':
        return {
            'new_type': 'GRIDLINE',
            'reason': 'GRIDLINE',
            'grid_info': grid_info,
        }

    # 3. Check if we can recover STAIR or ELEVATOR from extracted patterns
    if grid_info:
        if 'stair' in grid_info:
            return {
                'new_type': 'STAIR',
                'reason': f"RECOVERED:{grid_info['stair']}",
                'grid_info': grid_info,
            }
        if 'elevator' in grid_info:
            return {
                'new_type': 'ELEVATOR',
                'reason': f"RECOVERED:{grid_info['elevator']}",
                'grid_info': grid_info,
            }

    # 4. Check if we can recover as GRIDLINE from extracted patterns
    if grid_info:
        if 'fab_area' in grid_info:
            return {
                'new_type': 'GRIDLINE',
                'reason': f"RECOVERED:FAB_{grid_info['fab_area']}",
                'grid_info': grid_info,
            }
        if 'support_area' in grid_info:
            return {
                'new_type': 'GRIDLINE',
                'reason': f"RECOVERED:{grid_info['support_area']}",
                'grid_info': grid_info,
            }
        if 'fiz_area' in grid_info:
            return {
                'new_type': 'GRIDLINE',
                'reason': f"RECOVERED:{grid_info['fiz_area']}",
                'grid_info': grid_info,
            }
        if 'fiz_grid' in grid_info:
            return {
                'new_type': 'GRIDLINE',
                'reason': f"RECOVERED:FIZ_{grid_info['fiz_grid']}",
                'grid_info': grid_info,
            }
        if 'gridline' in grid_info:
            return {
                'new_type': 'GRIDLINE',
                'reason': f"RECOVERED:GL_{grid_info['gridline']}",
                'grid_info': grid_info,
            }
        if 'gridline_range' in grid_info:
            return {
                'new_type': 'GRIDLINE',
                'reason': f"RECOVERED:GL_{grid_info['gridline_range']}",
                'grid_info': grid_info,
            }
        if 'grid_intersect' in grid_info:
            return {
                'new_type': 'GRIDLINE',
                'reason': f"RECOVERED:INTERSECT_{grid_info['grid_intersect']}",
                'grid_info': grid_info,
            }
        if 'col_range' in grid_info:
            return {
                'new_type': 'GRIDLINE',
                'reason': f"RECOVERED:COL_{grid_info['col_range']}",
                'grid_info': grid_info,
            }

    # 4. Check if this is a project-wide task (applies to FAB1 as whole)
    project_wide = is_project_wide_task(row)
    if project_wide:
        return {
            'new_type': 'BUILDING',
            'reason': f'PROJECT_WIDE:{project_wide[:20]}',
            'grid_info': grid_info,
        }

    # 5. LEVEL → UNDEFINED (unless we add floor-wide rules)
    if current_type == 'LEVEL':
        level_norm = str(level).upper() if pd.notna(level) else 'NULL'
        return {
            'new_type': 'UNDEFINED',
            'reason': f'LEVEL:{level_norm}',
            'grid_info': grid_info,
        }

    # 6. BUILDING → UNDEFINED (only FAB1 would be valid)
    if current_type == 'BUILDING':
        bldg = str(building) if pd.notna(building) else 'NULL'
        return {
            'new_type': 'UNDEFINED',
            'reason': f'BUILDING:{bldg}',
            'grid_info': grid_info,
        }

    # 7. AREA → UNDEFINED
    if current_type == 'AREA':
        return {
            'new_type': 'UNDEFINED',
            'reason': f'AREA:{location_code}',
            'grid_info': grid_info,
        }

    # 8. No type or unknown → UNDEFINED
    return {
        'new_type': 'UNDEFINED',
        'reason': 'NO_TYPE' if pd.isna(current_type) else f'UNKNOWN:{current_type}',
        'grid_info': grid_info,
    }


# =============================================================================
# DATA LOADING
# =============================================================================

def load_latest_yates(file_id: int = None):
    """Load YATES schedule data.

    Args:
        file_id: Specific file_id to load. If None, uses file_id=1 (master schedule
                 with room-level decomposition). Use --file-id to override.
    """
    data_dir = Settings.PRIMAVERA_PROCESSED_DIR

    print("Loading P6 data...")
    tasks = pd.read_csv(data_dir / "task.csv", low_memory=False)
    wbs = pd.read_csv(data_dir / "projwbs.csv", low_memory=False)
    files = pd.read_csv(data_dir / "xer_files.csv")
    taskactv = pd.read_csv(data_dir / "taskactv.csv", low_memory=False)
    actvcode = pd.read_csv(data_dir / "actvcode.csv", low_memory=False)
    actvtype = pd.read_csv(data_dir / "actvtype.csv", low_memory=False)

    # Default to file_id=1 (master schedule with room-level decomposition)
    if file_id is None:
        file_id = 1

    file_info = files[files['file_id'] == file_id]
    if len(file_info) == 0:
        raise ValueError(f"file_id {file_id} not found")

    print(f"Using file_id={file_id} ({file_info.iloc[0]['filename']})")

    tasks = tasks[tasks['file_id'] == file_id].copy()
    wbs = wbs[wbs['file_id'] == file_id].copy()
    taskactv = taskactv[taskactv['file_id'] == file_id].copy()
    actvcode = actvcode[actvcode['file_id'] == file_id].copy()
    actvtype = actvtype[actvtype['file_id'] == file_id].copy()

    print(f"Loaded {len(tasks):,} tasks")
    return tasks, wbs, taskactv, actvcode, actvtype


def build_taxonomy(context: pd.DataFrame) -> pd.DataFrame:
    """Generate taxonomy and apply categorization."""

    print(f"\nProcessing {len(context):,} tasks...")

    results = []
    for idx, (_, row) in enumerate(context.iterrows()):
        if idx % 2000 == 0 and idx > 0:
            print(f"  {idx:,}/{len(context):,}")

        taxonomy = infer_all_fields(row)
        taxonomy['task_name'] = row.get('task_name')
        taxonomy['task_code'] = row.get('task_code')
        taxonomy['wbs_name'] = row.get('wbs_name')
        taxonomy['tier_4'] = row.get('tier_4')
        results.append(taxonomy)

    df = pd.DataFrame(results)

    # Apply categorization
    print("\nApplying location rules...")
    cat_results = df.apply(categorize_task, axis=1, result_type='expand')
    df['new_type'] = cat_results['new_type']
    df['new_reason'] = cat_results['reason']
    df['grid_info'] = cat_results['grid_info']

    return df


# =============================================================================
# ANALYSIS OUTPUT
# =============================================================================

def print_analysis(df: pd.DataFrame):
    """Print categorization analysis."""

    total = len(df)

    print("\n" + "="*70)
    print("CURRENT → NEW LOCATION TYPE")
    print("="*70)

    # Current distribution
    print("\nCurrent distribution:")
    for loc_type, count in df['location_type'].value_counts(dropna=False).items():
        print(f"  {str(loc_type):15} {count:>6,} ({count/total*100:5.1f}%)")

    # New distribution
    print("\nNew distribution:")
    for loc_type, count in df['new_type'].value_counts(dropna=False).items():
        print(f"  {loc_type:15} {count:>6,} ({count/total*100:5.1f}%)")

    # Recovery stats
    recovered = df[df['new_reason'].str.startswith('RECOVERED:', na=False)]
    print(f"\nRecovered as GRIDLINE: {len(recovered):,} tasks")

    if len(recovered) > 0:
        print("\nRecovery breakdown:")
        for reason, count in recovered['new_reason'].value_counts().head(10).items():
            print(f"  {reason:30} {count:>5,}")

    # UNDEFINED breakdown
    undefined = df[df['new_type'] == 'UNDEFINED']
    print(f"\n" + "="*70)
    print(f"UNDEFINED: {len(undefined):,} tasks ({len(undefined)/total*100:.1f}%)")
    print("="*70)

    print("\nBy reason:")
    for reason, count in undefined['new_reason'].value_counts().head(15).items():
        print(f"  {reason:30} {count:>5,}")

    # Sample UNDEFINED tasks
    print("\nSample UNDEFINED tasks:")
    for reason in undefined['new_reason'].value_counts().head(5).index:
        print(f"\n--- {reason} ---")
        sample = undefined[undefined['new_reason'] == reason].head(3)
        for _, row in sample.iterrows():
            task = str(row['task_name'])[:50]
            tier4 = str(row['tier_4'])[:25]
            print(f"  {task:<50} | {tier4}")


def main():
    parser = argparse.ArgumentParser(description='Explore P6 location categorization')
    parser.add_argument('--output', '-o', type=Path, help='Save results to CSV')
    parser.add_argument('--file-id', type=int, default=1,
                        help='P6 file_id to analyze (default: 1, master with rooms)')
    args = parser.parse_args()

    tasks, wbs, taskactv, actvcode, actvtype = load_latest_yates(file_id=args.file_id)

    print("\nBuilding task context...")
    context = build_task_context(
        tasks_df=tasks,
        wbs_df=wbs,
        taskactv_df=taskactv,
        actvcode_df=actvcode,
        actvtype_df=actvtype,
    )

    df = build_taxonomy(context)
    print_analysis(df)

    if args.output:
        print(f"\nSaving to: {args.output}")
        cols = ['task_id', 'task_code', 'task_name', 'wbs_name', 'tier_4',
                'building', 'level', 'location_type', 'location_code',
                'new_type', 'new_reason']
        cols = [c for c in cols if c in df.columns]
        df[cols].to_csv(args.output, index=False)

    print("\nDone!")


if __name__ == '__main__':
    main()
