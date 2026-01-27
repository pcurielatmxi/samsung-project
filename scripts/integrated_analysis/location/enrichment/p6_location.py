"""
P6 Location Extraction

Extracts location_type and location_code from P6 task data using priority order:
ROOM > STAIR > ELEVATOR > GRIDLINE > LEVEL > BUILDING

This is the main entry point for P6 taxonomy location processing.
"""

from dataclasses import dataclass
from typing import Optional

import pandas as pd

from scripts.integrated_analysis.location.core.extractors import (
    extract_room,
    extract_stair,
    extract_elevator,
    extract_gridline,
    extract_building_from_wbs,
    extract_building_from_task_code,
    extract_building_from_z_area,
    extract_building_from_z_level,
    extract_level_from_wbs,
    extract_level_from_z_level,
    infer_building_from_room,
    infer_level_from_room,
)
from scripts.integrated_analysis.location.core.normalizers import (
    normalize_level,
    normalize_building,
)


@dataclass
class P6LocationResult:
    """Result of P6 location extraction."""

    # Location classification
    location_type: str  # ROOM, STAIR, ELEVATOR, GRIDLINE, LEVEL, BUILDING
    location_code: str  # FAB116406, STR-10, ELV-01, GL-5, FAB-2F, FAB

    # Building and level (always populated when available)
    building: Optional[str]  # FAB, SUE, SUW, FIZ
    level: Optional[str]  # 1F, 2F, ROOF, B1, MULTI

    # Area (when available from WBS)
    area: Optional[str]  # SEA-5, SWA-1, FIZ1

    # Source tracking (for debugging/audit)
    location_source: str  # wbs, task_name, activity_code, task_code, default

    def to_dict(self) -> dict:
        """Convert to dictionary for DataFrame integration."""
        return {
            'location_type': self.location_type,
            'location_code': self.location_code,
            'building': self.building,
            'level': self.level,
            'area': self.area,
            'location_source': self.location_source,
        }


def extract_p6_location(
    task_name: str,
    task_code: str,
    wbs_name: str,
    tier_3: str,
    tier_4: str,
    tier_5: str,
    z_bldg: Optional[str] = None,
    z_level: Optional[str] = None,
    z_area: Optional[str] = None,
    area: Optional[str] = None,
) -> P6LocationResult:
    """
    Extract location from P6 task data using priority order.

    Priority: ROOM > STAIR > ELEVATOR > GRIDLINE > LEVEL > BUILDING

    This function consolidates location extraction logic that was previously
    scattered across multiple inference functions in the taxonomy script.

    Args:
        task_name: P6 task name
        task_code: P6 task code (PREFIX.AREA.NUMBER format)
        wbs_name: Full WBS name
        tier_3: WBS tier 3 value
        tier_4: WBS tier 4 value
        tier_5: WBS tier 5 value
        z_bldg: Z-BLDG activity code value (optional)
        z_level: Z-LEVEL activity code value (optional)
        z_area: Z-AREA activity code value (optional)
        area: Pre-extracted area code (optional, for efficiency)

    Returns:
        P6LocationResult with location_type, location_code, and supporting fields
    """
    # First, extract building and level using existing priority logic
    # These are needed regardless of location_type
    building, building_source = _extract_building_with_source(
        tier_3=tier_3,
        tier_4=tier_4,
        task_code=task_code,
        z_bldg=z_bldg,
        z_area=z_area,
        z_level=z_level,
    )

    level, level_source = _extract_level_with_source(
        tier_3=tier_3,
        tier_4=tier_4,
        tier_5=tier_5,
        wbs_name=wbs_name,
        z_level=z_level,
    )

    # Extract area if not provided
    if area is None:
        area = _extract_area(tier_4)

    # Priority 1: ROOM - specific room code (FAB112155)
    room = extract_room(tier_5, wbs_name)
    if room:
        # Override building/level with room-derived values
        room_building = infer_building_from_room(room)
        room_level = infer_level_from_room(room)
        return P6LocationResult(
            location_type='ROOM',
            location_code=room,
            building=room_building or building,
            level=room_level or level,
            area=area,
            location_source='wbs',
        )

    # Priority 2: STAIR - specific stairwell code
    # Check task_name, tier_5, and wbs_name
    for text, source in [(task_name, 'task_name'), (tier_5, 'wbs'), (wbs_name, 'wbs')]:
        stair = extract_stair(text)
        if stair:
            return P6LocationResult(
                location_type='STAIR',
                location_code=stair,
                building=building,
                level=level,
                area=area,
                location_source=source,
            )

    # Priority 3: ELEVATOR - specific elevator code
    for text, source in [(task_name, 'task_name'), (tier_5, 'wbs'), (wbs_name, 'wbs')]:
        elevator = extract_elevator(text)
        if elevator:
            return P6LocationResult(
                location_type='ELEVATOR',
                location_code=elevator,
                building=building,
                level=level,
                area=area,
                location_source=source,
            )

    # Priority 4: GRIDLINE - specific gridline number
    gridline = extract_gridline(task_name, area, tier_4)
    if gridline:
        return P6LocationResult(
            location_type='GRIDLINE',
            location_code=f"GL-{gridline}",
            building=building,
            level=level,
            area=area,
            location_source='wbs' if area else 'task_name',
        )

    # Priority 5: LEVEL - floor level (if we have building + level)
    if building and level:
        return P6LocationResult(
            location_type='LEVEL',
            location_code=f"{building}-{level}",
            building=building,
            level=level,
            area=area,
            location_source=level_source or building_source or 'wbs',
        )

    # Priority 6: BUILDING - entire building
    if building:
        return P6LocationResult(
            location_type='BUILDING',
            location_code=building,
            building=building,
            level=level,
            area=area,
            location_source=building_source or 'wbs',
        )

    # Default: FAB1 (project-wide)
    return P6LocationResult(
        location_type='BUILDING',
        location_code='FAB1',
        building='FAB1',
        level=None,
        area=None,
        location_source='default',
    )


def _extract_building_with_source(
    tier_3: str,
    tier_4: str,
    task_code: str,
    z_bldg: Optional[str],
    z_area: Optional[str],
    z_level: Optional[str],
) -> tuple[Optional[str], Optional[str]]:
    """
    Extract building code with source tracking.

    Priority order:
    1. Z-BLDG activity code
    2. Z-AREA activity code
    3. Z-LEVEL activity code (cardinal directions)
    4. task_code segment
    5. WBS tiers

    Returns:
        Tuple of (building, source)
    """
    # Z-BLDG mapping (simplified - full mapping is in taxonomy/mappings.py)
    if z_bldg and pd.notna(z_bldg):
        z_bldg_upper = str(z_bldg).upper().strip()
        # Direct building codes
        for code in ['FAB', 'SUE', 'SUW', 'FIZ', 'CUB', 'GCS']:
            if code in z_bldg_upper:
                return (code, 'activity_code')
        # Building names
        if 'EAST' in z_bldg_upper and 'SUPPORT' in z_bldg_upper:
            return ('SUE', 'activity_code')
        if 'WEST' in z_bldg_upper and 'SUPPORT' in z_bldg_upper:
            return ('SUW', 'activity_code')
        if 'DATA CENTER' in z_bldg_upper:
            return ('FIZ', 'activity_code')

    # Z-AREA
    if z_area and pd.notna(z_area):
        building = extract_building_from_z_area(z_area)
        if building:
            return (building, 'activity_code')

    # Z-LEVEL (cardinal directions)
    if z_level and pd.notna(z_level):
        building = extract_building_from_z_level(z_level)
        if building:
            return (building, 'activity_code')

    # task_code
    if task_code and pd.notna(task_code):
        building = extract_building_from_task_code(task_code)
        if building:
            return (building, 'task_code')

    # WBS
    building = extract_building_from_wbs(tier_3, tier_4)
    if building:
        return (building, 'wbs')

    return (None, None)


def _extract_level_with_source(
    tier_3: str,
    tier_4: str,
    tier_5: str,
    wbs_name: str,
    z_level: Optional[str],
) -> tuple[Optional[str], Optional[str]]:
    """
    Extract level with source tracking and normalization.

    Priority order:
    1. Z-LEVEL activity code (when valid)
    2. WBS tiers

    Returns:
        Tuple of (level, source) where level is normalized (1F, 2F, etc.)
    """
    # Z-LEVEL (when valid level pattern)
    if z_level and pd.notna(z_level):
        level = extract_level_from_z_level(z_level)
        if level:
            return (normalize_level(level), 'activity_code')

    # WBS
    level = extract_level_from_wbs(tier_3, tier_4, tier_5, wbs_name)
    if level:
        return (normalize_level(level), 'wbs')

    return (None, None)


def _extract_area(tier_4: str) -> Optional[str]:
    """
    Extract grid area from WBS tier_4.

    Returns:
        Area code (SEA-5, SWA-1, FIZ1) or None
    """
    if not tier_4 or pd.isna(tier_4):
        return None

    tier_4_upper = str(tier_4).upper().strip()

    # Grid areas: SEA - 1, SEB - 2, SWA - 3, SWB - 4 (with spaces around dash)
    import re
    grid_match = re.match(r'^(SE[AB]|SW[AB])\s*-\s*(\d)', tier_4_upper)
    if grid_match:
        return f"{grid_match.group(1)}-{grid_match.group(2)}"

    # FIZ areas: Area FIZ1 (West Inner), Area FIZ2
    fiz_match = re.match(r'^AREA\s*(FIZ\d)', tier_4_upper)
    if fiz_match:
        return fiz_match.group(1)

    return None
