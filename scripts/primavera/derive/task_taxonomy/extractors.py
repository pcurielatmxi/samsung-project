"""
Low-level Extraction Helpers for Task Taxonomy

Extract building, level, trade, area, and room from WBS tiers and activity codes.

NOTE: Location extraction functions now route to the centralized module at:
    scripts/integrated_analysis/location/core/extractors.py

This file maintains backward compatibility for existing code that imports from here.

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

# Import centralized location extraction functions
# These are the single source of truth for location extraction
from scripts.integrated_analysis.location.core.extractors import (
    extract_room as _extract_room,
    extract_stair as _extract_stair,
    extract_elevator as _extract_elevator,
    extract_gridline as _extract_gridline,
    extract_building_from_wbs as _extract_building_from_wbs,
    extract_building_from_task_code as _extract_building_from_task_code,
    extract_building_from_z_area as _extract_building_from_z_area,
    extract_building_from_z_level as _extract_building_from_z_level,
    extract_level_from_wbs as _extract_level_from_wbs,
    extract_level_from_z_level as _extract_level_from_z_level,
    safe_upper,
)
from scripts.integrated_analysis.location.core.normalizers import (
    normalize_level as _normalize_level,
)

# Level code normalization: map raw codes to dim_location format
# Kept for backward compatibility - actual normalization uses centralized module
LEVEL_NORMALIZATION = {
    '1': '1F', '2': '2F', '3': '3F', '4': '4F', '5': '5F', '6': '6F',
    'L1': '1F', 'L2': '2F', 'L3': '3F', 'L4': '4F', 'L5': '5F', 'L6': '6F',
    'ROOF': 'ROOF', 'RF': 'ROOF', 'R': 'ROOF',
    'B1': 'B1', 'UG': 'UG', 'BSMT': 'B1', 'BASEMENT': 'B1',
    'MULTI': 'MULTI', 'ALL': 'MULTI',
}


def normalize_level(level: str | None) -> str | None:
    """
    Normalize level codes to dim_location format.

    Converts: '1' -> '1F', 'L2' -> '2F', 'ROOF' -> 'ROOF', etc.

    Routes to centralized module.
    """
    if level is None:
        return None
    # Use centralized normalizer
    return _normalize_level(str(level))


def extract_building_from_wbs(tier_3: str, tier_4: str) -> str | None:
    """
    Extract building code from WBS tier_3 and tier_4.

    Returns: Building code (FAB, SUE, SUW, FIZ, CUB, GCS) or None

    Routes to centralized module.
    """
    return _extract_building_from_wbs(tier_3, tier_4)


def extract_level_from_wbs(tier_3: str, tier_4: str, tier_5: str, wbs_name: str) -> str | None:
    """
    Extract floor level from WBS tiers.

    Returns: Level ('1'-'6', 'ROOF', 'B1') or None

    Routes to centralized module.
    """
    return _extract_level_from_wbs(tier_3, tier_4, tier_5, wbs_name)


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

    Routes to centralized module.
    """
    return _extract_room(tier_5, wbs_name)


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

    Returns: Building code (FAB, SUE, SUW, FIZ) or None

    Routes to centralized module.
    """
    return _extract_building_from_task_code(task_code)


def extract_building_from_z_area(z_area_value: str) -> str | None:
    """
    Extract building code from Z-AREA activity code value.

    Returns: Building code (FAB, SUE, SUW, FIZ) or None

    Routes to centralized module.
    """
    return _extract_building_from_z_area(z_area_value)


def extract_building_from_z_level(z_level_value: str) -> str | None:
    """
    Extract building code from Z-LEVEL activity code value based on cardinal directions.

    Returns: Building code (SUE, SUW) or None

    Routes to centralized module.
    """
    return _extract_building_from_z_level(z_level_value)


def extract_level_from_z_level(z_level_value) -> str | None:
    """
    Extract floor level from Z-LEVEL activity code value.

    Note: Z-LEVEL is often misused (contains trade names like "DRYWALL",
    "FIREPROOFING"), so this function validates the value is actually a level.

    Returns: Level ('1'-'6', 'ROOF', 'B1', 'MULTI') or None

    Routes to centralized module.
    """
    return _extract_level_from_z_level(z_level_value)


def extract_elevator_from_task_name(task_name: str) -> str | None:
    """
    Extract elevator code from task_name.

    Returns: Elevator code (ELV-01, ELV-01A, ELV-22, etc.) or None

    Routes to centralized module.
    """
    return _extract_elevator(task_name)


def extract_stair_from_task_name(task_name: str) -> str | None:
    """
    Extract stairwell code from task_name.

    Now handles enhanced patterns including:
    - STAIR #R10 (R-prefix for roof stairs)
    - RAMP #5, RAMP #5-L
    - STAIR/RAMP #2B

    Returns: Stair code (STR-01, STR-R10, STR-21, etc.) or None

    Routes to centralized module.
    """
    return _extract_stair(task_name)


def extract_gridline_from_task_name_and_area(task_name: str, area: str, tier_4: str) -> str | None:
    """
    Extract gridline number from task_name and area context.

    Returns: Gridline number (1-33) or None

    Routes to centralized module.
    """
    return _extract_gridline(task_name, area, tier_4)
