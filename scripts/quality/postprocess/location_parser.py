#!/usr/bin/env python3
"""
Location parsing module for quality inspection data.

Extracts structured location components (building, level, area, grid) from
complex location strings using regex patterns.
"""

import re
import pandas as pd
from typing import Dict, Optional


# Building code patterns (order matters - more specific first)
BUILDING_PATTERNS = [
    (r'\b(FAB)\s*1?\b', 'FAB'),      # FAB or FAB1 → FAB
    (r'\b(SUE|SUW|SUP)\b', None),    # Keep as-is
    (r'\b(CUB)\b', 'CUB'),           # CUB Building
    (r'\b(FIZ)\b', 'FIZ'),           # FIZ
    (r'\b(GCS[AB]?)\b', None),       # GCSA, GCSB, GCS
    (r'\b(OB[12])\b', None),         # OB1, OB2
]

# Level patterns (order matters - more specific first)
# NOTE: Location string is uppercased, so patterns must match uppercase
LEVEL_PATTERNS = [
    (r'\bLEVEL\s+(\d+)\b', 'level_num'),    # "LEVEL 4" → 4F
    (r'\bL(\d+)\b', 'level_num'),           # "L4" → 4F
    (r'\b(\d+)F\b', 'level_fmt'),           # "4F" → 4F
    (r'\b(ROOF|RF)\b', 'roof'),             # "ROOF" or "RF"
    (r'\b(B1|UG|B2)\b', 'basement'),        # "B1", "UG", "B2" (Basement/Underground)
]

# Area keywords
AREA_KEYWORDS = [
    'SOUTHEAST', 'SOUTHWEST', 'NORTHEAST', 'NORTHWEST',
    'NORTH', 'SOUTH', 'EAST', 'WEST'
]

# Grid pattern - matches various grid formats
# NOTE: Location string is uppercased
GRID_PATTERN = r'GRID\s+LINES?\s+([A-Z]\.?\d*[/\-]?\d+)'


def parse_location(location_str: Optional[str]) -> Dict[str, Optional[str]]:
    """
    Parse location string into structured components.

    Extracts:
    - building: Building code (FAB, SUE, SUW, CUB, etc.)
    - level: Level (1F, 2F, ROOF, B1, etc.)
    - area: Area/directional (NORTH, SOUTHEAST, SUW, etc.)
    - grid: Grid line coordinates
    - location_id: Composite key (Building-Level)

    Args:
        location_str: Raw location string from inspection report

    Returns:
        Dict with keys: building, level, area, grid, location_id (all Optional[str])
    """
    if pd.isna(location_str):
        return {
            'building': None,
            'level': None,
            'area': None,
            'grid': None,
            'location_id': None
        }

    loc = str(location_str).upper()

    # Extract building
    building = None
    for pattern, replacement in BUILDING_PATTERNS:
        match = re.search(pattern, loc)
        if match:
            if replacement:
                building = replacement
            else:
                building = match.group(1)
            break

    # Extract level
    level = None
    for pattern, pattern_type in LEVEL_PATTERNS:
        match = re.search(pattern, loc)
        if match:
            if pattern_type == 'level_num':
                level = f"{match.group(1)}F"
            elif pattern_type == 'level_fmt':
                level = match.group(1)
            elif pattern_type == 'roof':
                level = 'ROOF'
            elif pattern_type == 'basement':
                level = match.group(1)
            break

    # Extract grid
    grid = None
    grid_match = re.search(GRID_PATTERN, loc)
    if grid_match:
        grid = grid_match.group(1)

    # Extract area (simple keyword matching)
    area = None
    for keyword in AREA_KEYWORDS:
        if keyword in loc:
            area = keyword
            break

    # If no area found, check for standard area codes
    if not area:
        area_pattern = r'\b(SUW|SUE|SUP|CUP|UGL|UGR)\b'
        area_match = re.search(area_pattern, loc)
        if area_match:
            area = area_match.group(1)

    # Create location_id if we have building and level
    location_id = None
    if building and level:
        location_id = f"{building}-{level}"

    return {
        'building': building,
        'level': level,
        'area': area,
        'grid': grid,
        'location_id': location_id
    }
