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
from .mappings import WBS_TRADE_PATTERNS


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
