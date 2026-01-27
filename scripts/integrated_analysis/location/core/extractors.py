"""
P6 Location Extraction Patterns

Centralized extraction patterns for extracting location information from P6 task data
(WBS tiers, activity codes, task names, task codes).

This module is the single source of truth for:
- Room code extraction (FAB116406)
- Stair/ramp code extraction (STR-10, STR-R10)
- Elevator code extraction (ELV-01, ELV-01A)
- Gridline extraction (GL-5)
- Building extraction from multiple sources
- Level extraction from multiple sources

All extraction functions return normalized codes ready for dim_location lookup.
"""

import re
from typing import Optional

import pandas as pd


# =============================================================================
# Utility Functions
# =============================================================================

def safe_upper(val) -> str:
    """Convert value to uppercase string, handling None/NaN."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ''
    return str(val).upper().strip()


# =============================================================================
# Room Extraction
# =============================================================================

def extract_room(tier_5: str, wbs_name: str) -> Optional[str]:
    """
    Extract room code from WBS tier_5 or wbs_name.

    Room code pattern: FAB + digit + 5 digits (e.g., FAB112155)
    The first digit after FAB indicates the floor level.

    Args:
        tier_5: WBS tier 5 value
        wbs_name: Full WBS name

    Returns:
        Room code (FAB112155) or None
    """
    for text in [tier_5, wbs_name]:
        if not text or pd.isna(text):
            continue
        text_upper = safe_upper(text)
        room_match = re.search(r'(FAB\d{6})', text_upper)
        if room_match:
            return room_match.group(1)
    return None


def infer_building_from_room(room: str) -> str:
    """Infer building code from room code. All FABxxxxxx rooms are in FAB."""
    return 'FAB'


def infer_level_from_room(room: str) -> Optional[str]:
    """
    Infer level from room code.

    Room code structure: FAB[F][AANN] where F is floor digit.
    Example: FAB116406 -> Floor 1 -> 1F
    """
    if not room:
        return None
    match = re.match(r'FAB(\d)\d{5}', room)
    if match:
        return f"{match.group(1)}F"
    return None


# =============================================================================
# Stair Extraction
# =============================================================================

def extract_stair(text: str) -> Optional[str]:
    """
    Extract stair/ramp code from text (task name, WBS, etc.).

    Patterns handled:
    - STAIR 21, STAIR #21, STAIRWELL 21
    - STAIR #R10 (R-prefix for roof stairs)
    - RAMP #5, RAMP #5-L
    - STAIR/RAMP #2B
    - STR-21 (explicit format)

    Args:
        text: Text to search for stair codes

    Returns:
        Normalized stair code (STR-XX) or None
    """
    if not text or pd.isna(text):
        return None

    text_upper = safe_upper(text)

    # Pattern 1: Explicit STR-XX format (STR-01, STR-50, STR 21, STR-R10)
    str_explicit = re.search(r'\bSTR\s*[-_]?\s*([R]?\d+[A-Z]?)\b', text_upper)
    if str_explicit:
        return normalize_stair_code(str_explicit.group(1))

    # Pattern 2: STAIR/RAMP with number
    # Handles: STAIR 21, STAIR #R10, STAIRWELL 27, RAMP #5, RAMP #5-L, STAIR/RAMP #2B
    stair_match = re.search(
        r'(?:STAIR|STAIRWELL|STAIRS|RAMP)[S/]*\s*[#]?\s*([R]?\d+[A-Z]?)(?:-[A-Z]+)?(?:\s|$|[,\.\-])',
        text_upper
    )
    if stair_match:
        return normalize_stair_code(stair_match.group(1))

    return None


def normalize_stair_code(raw: str) -> str:
    """
    Normalize stair code to STR-XX format.

    Normalizations:
    - Pad single digit numbers: 5 -> 05
    - Handle R-prefix: R10 -> R10 (keep as-is)
    - Strip building suffixes: 21-SUE -> 21

    Args:
        raw: Raw stair code (5, R10, 21, etc.)

    Returns:
        Normalized code in STR-XX format (STR-05, STR-R10, STR-21)
    """
    code = raw.upper().strip()

    # Remove any trailing building suffix (-SUE, -SUW, -FAB, -FIZ)
    code = re.sub(r'-(?:SUE|SUW|FAB|FIZ)$', '', code)

    # Pad single digit numbers (but preserve R prefix)
    if re.match(r'^R\d$', code):
        # R5 -> R05
        code = f"R0{code[1]}"
    elif re.match(r'^\d$', code):
        # 5 -> 05
        code = f"0{code}"

    return f"STR-{code}"


# =============================================================================
# Elevator Extraction
# =============================================================================

def extract_elevator(text: str) -> Optional[str]:
    """
    Extract elevator code from text (task name, WBS, etc.).

    Patterns handled:
    - Elevator 01, Elevator #1, Elevator 1A, ELEVATOR-5
    - ELV-01, ELV 01A

    NOTE: Does NOT match single letters like "Elevator A" as these are always
    false positives from words like "ELEVATOR ACCESS", "ELEVATOR HALL", etc.

    Args:
        text: Text to search for elevator codes

    Returns:
        Normalized elevator code (ELV-XX) or None
    """
    if not text or pd.isna(text):
        return None

    text_upper = safe_upper(text)

    # Pattern 1: Explicit ELV-XX format (ELV-01, ELV-01A, ELV 22)
    elv_explicit = re.search(r'\bELV\s*[-_]?\s*(\d+[A-Z]?)\b', text_upper)
    if elv_explicit:
        return normalize_elevator_code(elv_explicit.group(1))

    # Pattern 2: Numeric elevators (Elevator #01, Elevator 1, Elevator 1A, ELEVATOR-5)
    # Must have a number, optionally followed by a letter suffix (A, B)
    numeric_elv = re.search(
        r'(?:ELEVATOR|ELEV)[-\s]*[#]?(\d+)([A-B])?(?:\s|$|[,\.\-])',
        text_upper
    )
    if numeric_elv:
        elv_num = numeric_elv.group(1)
        suffix = numeric_elv.group(2) or ''
        return normalize_elevator_code(f"{elv_num}{suffix}")

    return None


def normalize_elevator_code(raw: str) -> str:
    """
    Normalize elevator code to ELV-XX format.

    Normalizations:
    - Pad single digit numbers: 5 -> 05, 1A -> 01A
    - Uppercase suffix

    Args:
        raw: Raw elevator code (5, 01, 1A, 22, etc.)

    Returns:
        Normalized code in ELV-XX format (ELV-05, ELV-01, ELV-01A, ELV-22)
    """
    code = raw.upper().strip()

    # Check if it's a single digit (with or without suffix)
    if len(code) == 1 and code.isdigit():
        # 5 -> 05
        code = f"0{code}"
    elif len(code) == 2 and code[0].isdigit() and code[1].isalpha():
        # 1A -> 01A
        code = f"0{code}"

    return f"ELV-{code}"


# =============================================================================
# Gridline Extraction
# =============================================================================

from dataclasses import dataclass


@dataclass
class GridlineExtraction:
    """Result of gridline extraction with optional row bounds."""

    gridline: Optional[str]  # Column number as string (e.g., "33")
    row_min: Optional[str]   # Row letter min (e.g., "D") - None means full span (A)
    row_max: Optional[str]   # Row letter max (e.g., "F") - None means full span (N)
    source: Optional[str]    # Source of extraction: task_name, area, tier_4


def extract_gridline_with_bounds(task_name: str, area: str = None, tier_4: str = None) -> GridlineExtraction:
    """
    Extract gridline number AND row bounds from task name, area, or tier_4.

    Gridline patterns with row bounds:
    - "GL 33 - D-F" -> gridline=33, row_min=D, row_max=F
    - "GL 33 - D/F" -> gridline=33, row_min=D, row_max=F
    - "D-F/33" -> gridline=33, row_min=D, row_max=F
    - "D LINE" -> gridline=None, row_min=D, row_max=D (row-only, no column)
    - "GL-5" -> gridline=5, row_min=None, row_max=None (full span)

    Row bounds are used to override the default A-N span in get_gridline_bounds().

    Args:
        task_name: Task name text
        area: Area code (SEA-5, etc.)
        tier_4: WBS tier 4 value

    Returns:
        GridlineExtraction with gridline, row_min, row_max, and source
    """
    empty = GridlineExtraction(gridline=None, row_min=None, row_max=None, source=None)

    # Pattern 1: Direct gridline reference in task name (with optional row bounds)
    if task_name and pd.notna(task_name):
        task_name_upper = safe_upper(task_name)

        # Pattern 1a: "GL 33 - D-F" or "GL 33 - D/F" (gridline + row range)
        # Also handles "GL33 - D-F", "GL-33 - D-F"
        gl_with_rows = re.search(
            r'(?:GRIDLINE|GRID|GL)\s*[:-]?\s*(\d+)\s*-\s*([A-N])\s*[-/]\s*([A-N])',
            task_name_upper
        )
        if gl_with_rows:
            return GridlineExtraction(
                gridline=gl_with_rows.group(1),
                row_min=gl_with_rows.group(2),
                row_max=gl_with_rows.group(3),
                source='task_name'
            )

        # Pattern 1b: "D-F/33" or "D/F - 33" (row range + gridline, alternate order)
        rows_then_gl = re.search(
            r'\b([A-N])\s*[-/]\s*([A-N])\s*[/-]\s*(\d+)\b',
            task_name_upper
        )
        if rows_then_gl:
            return GridlineExtraction(
                gridline=rows_then_gl.group(3),
                row_min=rows_then_gl.group(1),
                row_max=rows_then_gl.group(2),
                source='task_name'
            )

        # Pattern 1c: "[A-N]/33" or "A/33" (single row with gridline)
        single_row_gl = re.search(
            r'\b([A-N])\s*/\s*(\d+)\b',
            task_name_upper
        )
        if single_row_gl:
            row = single_row_gl.group(1)
            return GridlineExtraction(
                gridline=single_row_gl.group(2),
                row_min=row,
                row_max=row,
                source='task_name'
            )

        # Pattern 1d: Direct gridline without row bounds (full span)
        # Matches: "GL 33", "GL-33", "GRIDLINE 5"
        gridline_match = re.search(
            r'(?:GRIDLINE|GRID|GL)\s*[:-]?\s*(\d+)(?:\s|$|[,\.\-])',
            task_name_upper
        )
        if gridline_match:
            return GridlineExtraction(
                gridline=gridline_match.group(1),
                row_min=None,
                row_max=None,
                source='task_name'
            )

        # Pattern 1e: "[A-N] LINE" (row letter only, no column)
        # This indicates a row-only location, handled at GRIDLINE level
        line_match = re.search(r'\b([A-N])\s*LINE\b', task_name_upper)
        if line_match:
            row = line_match.group(1)
            return GridlineExtraction(
                gridline=None,  # No column - this is a row line
                row_min=row,
                row_max=row,
                source='task_name'
            )

    # Pattern 2: Extract from area code (SEA-5, SWA-3, etc.) - no row bounds
    if area and pd.notna(area):
        area_upper = safe_upper(area)
        area_digit = re.search(r'[-_](\d+)$', area_upper)
        if area_digit:
            return GridlineExtraction(
                gridline=area_digit.group(1),
                row_min=None,
                row_max=None,
                source='area'
            )

    # Pattern 3: Extract from tier_4 area pattern - no row bounds
    if tier_4 and pd.notna(tier_4):
        tier_4_upper = safe_upper(tier_4)
        tier4_area = re.search(r'(?:SE[AB]|SW[AB]|AREA)\s*[-_]?\s*(\d+)', tier_4_upper)
        if tier4_area:
            return GridlineExtraction(
                gridline=tier4_area.group(1),
                row_min=None,
                row_max=None,
                source='tier_4'
            )

    return empty


def extract_gridline(task_name: str, area: str, tier_4: str) -> Optional[str]:
    """
    Extract gridline number from task name, area, or tier_4.

    This is the backward-compatible wrapper around extract_gridline_with_bounds().
    For new code, prefer extract_gridline_with_bounds() to also get row bounds.

    Gridline patterns:
    - Direct: "Gridline 5", "Grid 5", "GL-5"
    - From area patterns: SEA-5, SWA-3, SEB-2 (rightmost digit)
    - From tier_4 area codes: "SEA - 1", "SWB - 4"

    Args:
        task_name: Task name text
        area: Area code (SEA-5, etc.)
        tier_4: WBS tier 4 value

    Returns:
        Gridline number as string (e.g., "5") or None
    """
    result = extract_gridline_with_bounds(task_name, area, tier_4)
    return result.gridline


# =============================================================================
# Building Extraction
# =============================================================================

def extract_building_from_wbs(tier_3: str, tier_4: str) -> Optional[str]:
    """
    Extract building code from WBS tier_3 and tier_4.

    Args:
        tier_3: WBS tier 3 value
        tier_4: WBS tier 4 value

    Returns:
        Building code (FAB, SUE, SUW, FIZ, CUB, GCS) or None
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


def extract_building_from_task_code(task_code: str) -> Optional[str]:
    """
    Extract building code from task_code second segment.

    Task code format: PREFIX.AREA.NUMBER (e.g., CN.SWA5.1580)

    Area patterns:
    - SEA*, SEB* -> SUE (Support East)
    - SWA*, SWB* -> SUW (Support West)
    - FIZ* -> FIZ (Data Center)
    - BB* -> FAB (FAB basement)
    - FAB* -> FAB

    Args:
        task_code: P6 task code

    Returns:
        Building code (FAB, SUE, SUW, FIZ) or None
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


def extract_building_from_z_area(z_area_value: str) -> Optional[str]:
    """
    Extract building code from Z-AREA activity code value.

    Z-AREA patterns:
    - SUES-*, SUEN-* -> SUE (Support East South/North)
    - SUWS-*, SUWN-* -> SUW (Support West South/North)
    - SUE - *, SUW - * -> SUE/SUW
    - FIZ*, Area FIZ* -> FIZ
    - Fab A*, Fab B* -> FAB
    - PENTHOUSE -> infer from direction (NORTH EAST -> SUE, etc.)

    Args:
        z_area_value: Z-AREA activity code value

    Returns:
        Building code (FAB, SUE, SUW, FIZ) or None
    """
    if not z_area_value or pd.isna(z_area_value):
        return None

    val = safe_upper(z_area_value)

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


def extract_building_from_z_level(z_level_value: str) -> Optional[str]:
    """
    Extract building code from Z-LEVEL activity code value based on cardinal directions.

    Z-LEVEL sometimes contains cardinal direction indicators:
    - (WEST), WEST, W -> SUW (Support West)
    - (EAST), EAST, E -> SUE (Support East)
    - (NORTH), NORTH, N -> SUE/SUW depending on context
    - (SOUTH), SOUTH, S -> SUE/SUW depending on context

    Args:
        z_level_value: Z-LEVEL activity code value

    Returns:
        Building code (SUE, SUW) or None
    """
    if not z_level_value or pd.isna(z_level_value):
        return None

    val = safe_upper(z_level_value)

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


# =============================================================================
# Level Extraction
# =============================================================================

def extract_level_from_wbs(tier_3: str, tier_4: str, tier_5: str, wbs_name: str) -> Optional[str]:
    """
    Extract floor level from WBS tiers.

    Args:
        tier_3: WBS tier 3 value
        tier_4: WBS tier 4 value
        tier_5: WBS tier 5 value
        wbs_name: Full WBS name

    Returns:
        Level ('1'-'6', 'ROOF', 'B1') or None (raw, not yet normalized)
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


def extract_level_from_z_level(z_level_value) -> Optional[str]:
    """
    Extract floor level from Z-LEVEL activity code value.

    Note: Z-LEVEL is often misused (contains trade names like "DRYWALL",
    "FIREPROOFING"), so this function validates the value is actually a level.

    Args:
        z_level_value: Z-LEVEL activity code value

    Returns:
        Level ('1'-'6', 'ROOF', 'B1', 'MULTI') or None (raw, not yet normalized)
    """
    if not z_level_value or pd.isna(z_level_value):
        return None

    val = safe_upper(z_level_value)

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
