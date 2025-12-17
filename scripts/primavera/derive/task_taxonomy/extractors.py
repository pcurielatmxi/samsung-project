"""
Low-level Extraction Helpers for Task Taxonomy

Extract building, level, trade, area, and room from WBS tiers and activity codes.

WBS Hierarchy Structure:
------------------------
tier_1: Project root (e.g., "SAMSUNG-TFAB1-10-31-25- Live-3")
tier_2: Major category (CONSTRUCTION, EXECUTIVE SUMMARY, PRE CONSTRUCTION)
tier_3: Building or phase (FAB BUILDING, SUPPORT BUILDING - WEST, LEVEL 1)
tier_4: Context-dependent (see below)
tier_5: Room/area detail
tier_6: Sub-area detail

tier_4 Patterns (depends on tier_3):
------------------------------------
When tier_3 = "FAB BUILDING (Phase 1)":
    tier_4 = Trade (STRUCTURAL STEEL, ARCHITECTURAL FINISHES, FAB FOUNDATIONS)

When tier_3 = "SUPPORT BUILDING - EAST" or "SUPPORT BUILDING - WEST":
    tier_4 = Building+Trade (SUE - CONCRETE, SUW - ROOFING)
    tier_4 = Grid area (SEA-1, SEB-2, SWA-3, SWB-4)

When tier_3 = "LEVEL 1/2/3/4":
    tier_4 = Building+Level (L1 FAB, L2 SUE, L3 SUW)

When tier_3 = "Data Center Bldg":
    tier_4 = FIZ Areas (Area FIZ1, Area FIZ2, etc.)

Room Code Pattern:
------------------
Room codes like FAB112155 encode: FAB + [Level digit] + [5-digit room ID]
    - FAB1xxxxx = Level 1
    - FAB2xxxxx = Level 2
    - FAB3xxxxx = Level 3
    - FAB4xxxxx = Level 4
"""

import re
import pandas as pd
from .mappings import WBS_TRADE_PATTERNS, TASK_NAME_TRADE_PATTERNS


def safe_upper(val) -> str:
    """Convert value to uppercase string, handling None/NaN."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ''
    return str(val).upper().strip()


def extract_building_from_wbs(tier_3: str, tier_4: str) -> str | None:
    """
    Extract building code from WBS tier_3 and tier_4.

    Returns: Building code (FAB, SUE, SUW, FIZ, CUB, GCS) or None
    """
    tier_3 = safe_upper(tier_3)
    tier_4 = safe_upper(tier_4)

    # Pattern 1: tier_4 has "L1 FAB", "L2 SUE" pattern (Building+Level combined)
    tier4_bldg_level = re.match(r'^L(\d)\s+(FAB|SUE|SUW|FIZ|CUB|GCS)', tier_4)
    if tier4_bldg_level:
        return tier4_bldg_level.group(2)

    # Pattern 2: tier_4 has "SUE - CONCRETE" pattern (Building prefix + Trade)
    tier4_bldg_prefix = re.match(r'^(SUE|SUW|FAB|FIZ|CUB|GCS)\s*-', tier_4)
    if tier4_bldg_prefix:
        return tier4_bldg_prefix.group(1)

    # Pattern 3: tier_3 has building name
    tier3_building_patterns = [
        (r'FAB\s*BUILDING', 'FAB'),
        (r'SUPPORT\s*BUILDING\s*-\s*EAST', 'SUE'),
        (r'SUPPORT\s*BUILDING\s*-\s*WEST', 'SUW'),
        (r'DATA\s*CENTER', 'FIZ'),
        (r'CENTRAL\s*UTILITIES', 'CUB'),
    ]
    for pattern, bldg in tier3_building_patterns:
        if re.search(pattern, tier_3):
            return bldg

    # Pattern 4: tier_4 has grid areas that imply building
    # SEA/SEB = Support East Area A/B -> SUE
    # SWA/SWB = Support West Area A/B -> SUW
    if re.match(r'^SE[AB]\s*-\s*\d', tier_4):
        return 'SUE'
    elif re.match(r'^SW[AB]\s*-\s*\d', tier_4):
        return 'SUW'
    elif re.match(r'^AREA\s*FIZ', tier_4):
        return 'FIZ'

    return None


def extract_level_from_wbs(tier_3: str, tier_4: str, tier_5: str, wbs_name: str) -> str | None:
    """
    Extract floor level from WBS tiers.

    Returns: Level ('1'-'6', 'ROOF', 'B1') or None
    """
    tier_3 = safe_upper(tier_3)
    tier_4 = safe_upper(tier_4)
    tier_5 = safe_upper(tier_5)
    wbs_name = safe_upper(wbs_name)

    # Pattern 1: tier_4 has "L1 FAB", "L2 SUE" pattern
    tier4_bldg_level = re.match(r'^L(\d)\s+(FAB|SUE|SUW|FIZ|CUB|GCS)', tier_4)
    if tier4_bldg_level:
        return tier4_bldg_level.group(1)

    # Pattern 2: tier_3 has "LEVEL X" pattern
    tier3_level = re.search(r'LEVEL\s*(\d)', tier_3)
    if tier3_level:
        return tier3_level.group(1)

    # Pattern 3: tier_4 has standalone "L1", "L2" etc.
    tier4_level = re.match(r'^L(\d)\b', tier_4)
    if tier4_level:
        return tier4_level.group(1)

    # Pattern 4: Extract level from room codes (FAB1xxxxx = Level 1)
    for text in [tier_5, wbs_name]:
        room_match = re.search(r'FAB(\d)\d{5}', text)
        if room_match:
            return room_match.group(1)

    # Pattern 5: Check for ROOF or UNDERGROUND keywords
    all_text = f"{tier_4} {tier_5} {wbs_name}"
    if 'ROOF' in all_text:
        return 'ROOF'
    elif re.search(r'\bUNDERGROUND\b|\bUG\b', all_text):
        return 'B1'

    return None


def extract_trade_from_wbs(tier_4: str) -> int | None:
    """
    Extract trade_id from WBS tier_4.

    Returns: trade_id (1-12) or None
    """
    tier_4 = safe_upper(tier_4)

    for pattern, trade_id in WBS_TRADE_PATTERNS:
        if re.search(pattern, tier_4, re.IGNORECASE):
            return trade_id

    return None


def extract_area_from_wbs(tier_4: str) -> str | None:
    """
    Extract grid area from WBS tier_4.

    Returns: Area code (SEA-5, SWA-1, FIZ1, etc.) or None
    """
    tier_4 = safe_upper(tier_4)

    # Grid areas: SEA - 1, SEB - 2, SWA - 3, SWB - 4 (with spaces around dash)
    grid_match = re.match(r'^(SE[AB]|SW[AB])\s*-\s*(\d)', tier_4)
    if grid_match:
        return f"{grid_match.group(1)}-{grid_match.group(2)}"

    # FIZ areas: Area FIZ1 (West Inner), Area FIZ2
    fiz_match = re.match(r'^AREA\s*(FIZ\d)', tier_4)
    if fiz_match:
        return fiz_match.group(1)

    return None


def extract_room_from_wbs(tier_5: str, wbs_name: str) -> str | None:
    """
    Extract room code from WBS tier_5 or wbs_name.

    Returns: Room code (FAB112155) or None
    """
    tier_5 = safe_upper(tier_5)
    wbs_name = safe_upper(wbs_name)

    # Room code pattern: FAB + digit + 5 digits (e.g., FAB112155)
    for text in [tier_5, wbs_name]:
        room_match = re.search(r'(FAB\d{6})', text)
        if room_match:
            return room_match.group(1)

    return None


def extract_trade_from_task_name(task_name: str) -> int | None:
    """
    Extract trade_id from task_name using pattern matching.

    This is a fallback for tasks where Z-TRADE and WBS don't provide trade info,
    and the TaskClassifier scope didn't map to a trade.

    Returns: trade_id (1-12) or None
    """
    if not task_name or pd.isna(task_name):
        return None

    task_name_upper = str(task_name).upper()

    for pattern, trade_id in TASK_NAME_TRADE_PATTERNS:
        if re.search(pattern, task_name_upper, re.IGNORECASE):
            return trade_id

    return None


def extract_building_from_task_code(task_code: str) -> str | None:
    """
    Extract building code from task_code second segment.

    Task code format: PREFIX.AREA.NUMBER (e.g., CN.SWA5.1580)

    Area patterns:
    - SEA*, SEB* -> SUE (Support East)
    - SWA*, SWB* -> SUW (Support West)
    - FIZ* -> FIZ (Data Center)
    - BB* -> FAB (FAB basement)
    - FAB* -> FAB

    Returns: Building code (FAB, SUE, SUW, FIZ) or None
    """
    if not task_code or pd.isna(task_code):
        return None

    parts = str(task_code).upper().split('.')
    if len(parts) < 2:
        return None

    # Check prefix first (for FIZ.EAST, FIZ.WEST patterns)
    prefix = parts[0]
    if prefix == 'FIZ':
        return 'FIZ'

    # Check area segment
    area = parts[1]

    # Support East patterns
    if area.startswith('SEA') or area.startswith('SEB'):
        return 'SUE'

    # Support West patterns
    if area.startswith('SWA') or area.startswith('SWB'):
        return 'SUW'

    # FIZ patterns
    if area.startswith('FIZ'):
        return 'FIZ'

    # FAB basement patterns (BB = basement block)
    if area.startswith('BB'):
        return 'FAB'

    # FAB patterns
    if area.startswith('FAB'):
        return 'FAB'

    return None


def extract_building_from_z_area(z_area_value: str) -> str | None:
    """
    Extract building code from Z-AREA activity code value.

    Z-AREA patterns:
    - SUES-*, SUEN-* -> SUE (Support East South/North)
    - SUWS-*, SUWN-* -> SUW (Support West South/North)
    - SUE - *, SUW - * -> SUE/SUW
    - FIZ*, Area FIZ* -> FIZ
    - Fab A*, Fab B* -> FAB
    - PENTHOUSE -> infer from direction (NORTH EAST -> SUE, etc.)

    Returns: Building code (FAB, SUE, SUW, FIZ) or None
    """
    if not z_area_value or pd.isna(z_area_value):
        return None

    val = str(z_area_value).upper().strip()

    # Support East patterns
    if re.search(r'^SUES|^SUEN|^SUE\s*-|SUPPORT.*EAST', val):
        return 'SUE'

    # Support West patterns
    if re.search(r'^SUWS|^SUWN|^SUW\s*-|SUPPORT.*WEST', val):
        return 'SUW'

    # FIZ patterns
    if re.search(r'^FIZ|AREA\s*FIZ|DATA\s*CENTER', val):
        return 'FIZ'

    # FAB patterns
    if re.search(r'^FAB\s*[AB]|FAB\s*BUILDING|^A[1-5]\s*-|^B[1-5]\s*-', val):
        return 'FAB'

    # Penthouse patterns - infer building from direction
    if 'PENTHOUSE' in val:
        if 'EAST' in val:
            return 'SUE'
        elif 'WEST' in val:
            return 'SUW'

    return None


def extract_building_from_z_level(z_level_value: str) -> str | None:
    """
    Extract building code from Z-LEVEL activity code value based on cardinal directions.

    Z-LEVEL sometimes contains cardinal direction indicators:
    - (WEST), WEST, W -> SUW (Support West)
    - (EAST), EAST, E -> SUE (Support East)
    - (NORTH), NORTH, N -> SUE/SUW depending on context
    - (SOUTH), SOUTH, S -> SUE/SUW depending on context

    Returns: Building code (SUE, SUW) or None
    """
    if not z_level_value or pd.isna(z_level_value):
        return None

    val = str(z_level_value).upper().strip()

    # Check for cardinal directions in parentheses or as separate words
    # WEST patterns
    if re.search(r'\(WEST\)|\bWEST\b|\s+W\s*$|\s+W\)', val):
        return 'SUW'

    # EAST patterns
    if re.search(r'\(EAST\)|\bEAST\b|\s+E\s*$|\s+E\)', val):
        return 'SUE'

    # NORTH patterns (default to SUE for Support East North)
    if re.search(r'\(NORTH\)|\bNORTH\b|\s+N\s*$|\s+N\)', val):
        return 'SUE'

    # SOUTH patterns (default to SUE for Support East South)
    if re.search(r'\(SOUTH\)|\bSOUTH\b|\s+S\s*$|\s+S\)', val):
        return 'SUE'

    return None


def extract_level_from_z_level(z_level_value) -> str | None:
    """
    Extract floor level from Z-LEVEL activity code value.

    Note: Z-LEVEL is often misused (contains trade names like "DRYWALL",
    "FIREPROOFING"), so this function validates the value is actually a level.

    Returns: Level ('1'-'6', 'ROOF', 'B1', 'MULTI') or None
    """
    if not z_level_value or pd.isna(z_level_value):
        return None

    val = str(z_level_value).upper().strip()

    # Direct level matches
    level_patterns = [
        (r'^L1\b|^LEVEL\s*1\b|^L1\s*-', '1'),
        (r'^L2\b|^LEVEL\s*2\b|^L2\s*-|SUBFAB', '2'),
        (r'^L3\b|^LEVEL\s*3\b|^L3\s*-|WAFFLE', '3'),
        (r'^L4\b|^LEVEL\s*4\b|^L4\s*-', '4'),
        (r'^L5\b|^LEVEL\s*5\b|^L5\s*-', '5'),
        (r'^L6\b|^LEVEL\s*6\b|^L6\s*-|PENTHOUSE', '6'),
        (r'^LU1|UNDERGROUND', 'B1'),
        (r'ROOF\s*LEVEL|^ROOF\b', 'ROOF'),
        (r'ALL\s*LEVELS', 'MULTI'),
    ]

    for pattern, level in level_patterns:
        if re.search(pattern, val):
            return level

    return None


def extract_elevator_from_task_name(task_name: str) -> str | None:
    """
    Extract elevator code from task_name.

    Elevator code patterns:
    - ELV-A-1, ELV-A-2, ELV-A-3 (elevator letters with levels)
    - ELV_A_1, ELV A 1 (alternate formats)
    - Explicit "Elevator A", "Elevator B", etc.
    - Numeric "Elevator #01", "Elevator 1", etc.

    Returns: Elevator code (ELV-A-1, ELV-B-2, ELV-01, etc.) or None
    """
    if not task_name or pd.isna(task_name):
        return None

    task_name_upper = str(task_name).upper()

    # Pattern 1: ELV-A-1, ELV-B-2 format (most common)
    elv_match = re.search(r'ELV\s*[-_]?\s*([A-Z])\s*[-_]?\s*(\d)', task_name_upper)
    if elv_match:
        return f"ELV-{elv_match.group(1)}-{elv_match.group(2)}"

    # Pattern 2: Explicit "Elevator A", "Elevator B" with optional level
    explicit_elv = re.search(r'(?:ELEVATOR|ELEV)\s+([A-Z])\s*(?:\(L(\d)\)|-\s*(\d))?', task_name_upper)
    if explicit_elv:
        letter = explicit_elv.group(1)
        level = explicit_elv.group(2) or explicit_elv.group(3)
        if level:
            return f"ELV-{letter}-{level}"
        return f"ELV-{letter}"

    # Pattern 3: Numeric elevators (Elevator #01, Elevator 1, etc.)
    numeric_elv = re.search(r'(?:ELEVATOR|ELEV)\s+[#]?(\d+)', task_name_upper)
    if numeric_elv:
        elv_num = numeric_elv.group(1)
        # Pad single digits to 2 digits for consistency (1 -> 01)
        if len(elv_num) == 1:
            elv_num = f"0{elv_num}"
        return f"ELV-{elv_num}"

    return None


def extract_stair_from_task_name(task_name: str) -> str | None:
    """
    Extract stairwell code from task_name.

    Stair code patterns:
    - STR-A-1, STR-B-2 format
    - STAIR-A, STAIR B, STAIRWELL B format
    - Can include level: STR-A-1, STR-B-2
    - Numeric stair IDs: STAIR #01, STAIR #3, STAIRWELL 1

    Returns: Stair code (STR-A-1, STR-B-2, STR-01, STR-03, etc.) or None
    """
    if not task_name or pd.isna(task_name):
        return None

    task_name_upper = str(task_name).upper()

    # Pattern 1: STR-A-1, STR-B-2 format (most common)
    stair_match = re.search(r'STR\s*[-_]?\s*([A-Z])\s*[-_]?\s*(\d)', task_name_upper)
    if stair_match:
        return f"STR-{stair_match.group(1)}-{stair_match.group(2)}"

    # Pattern 2: STAIR/STAIRWELL A, STAIRWELL B with optional level
    explicit_stair = re.search(r'(?:STAIR|STAIRWELL|STAIRS)\s+([A-Z])\s*(?:\(L(\d)\)|-\s*(\d))?', task_name_upper)
    if explicit_stair:
        letter = explicit_stair.group(1)
        level = explicit_stair.group(2) or explicit_stair.group(3)
        if level:
            return f"STR-{letter}-{level}"
        return f"STR-{letter}"

    # Pattern 3: Numeric stairs (STAIR #01, STAIR #3, STAIRWELL 1, etc.)
    numeric_stair = re.search(r'(?:STAIR|STAIRWELL|STAIRS)\s+[#]?(\d+)', task_name_upper)
    if numeric_stair:
        stair_num = numeric_stair.group(1)
        # Pad single digits to 2 digits for consistency (3 -> 03, etc.)
        if len(stair_num) == 1:
            stair_num = f"0{stair_num}"
        return f"STR-{stair_num}"

    return None


def extract_gridline_from_task_name_and_area(task_name: str, area: str, tier_4: str) -> str | None:
    """
    Extract gridline number from task_name and area context.

    Gridline patterns:
    - Direct: "Gridline 5", "Grid 5", "GL-5"
    - From area patterns: SEA-5, SWA-3, SEB-2 (rightmost digit)
    - From tier_4 area codes: "SEA - 1", "SWB - 4"

    Returns: Gridline number (1-33) or None
    """
    if task_name:
        task_name_upper = str(task_name).upper()

        # Pattern 1: Direct gridline reference
        gridline_match = re.search(r'(?:GRIDLINE|GRID|GL)\s*[:-]?\s*(\d+)', task_name_upper)
        if gridline_match:
            return gridline_match.group(1)

    # Pattern 2: Extract from area code (SEA-5, SWA-3, etc.)
    if area and pd.notna(area):
        area_upper = str(area).upper()
        area_digit = re.search(r'[-_](\d+)$', area_upper)
        if area_digit:
            return area_digit.group(1)

    # Pattern 3: Extract from tier_4 area pattern
    if tier_4 and pd.notna(tier_4):
        tier_4_upper = str(tier_4).upper()
        tier4_area = re.search(r'(?:SE[AB]|SW[AB]|AREA)\s*[-_]?\s*(\d+)', tier_4_upper)
        if tier4_area:
            return tier4_area.group(1)

    return None
