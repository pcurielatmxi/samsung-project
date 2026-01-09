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

# Grid patterns - matches various grid formats
# NOTE: Location string is uppercased
# Primary pattern for explicit grid references like "GRID LINES G/10"
GRID_PATTERN_EXPLICIT = r'GRID\s+LINES?\s+([A-Z]\.?\d*[/\-]?\d+(?:\.\d+)?)'

# Pattern for column references like "COLUMN L5" or "AT COLUMN N8"
GRID_PATTERN_COLUMN = r'COLUMN\s+([A-Z]\.?\d+(?:\.\d+)?)'

# Pattern for standalone grid coordinates like "F.6/18" or "N/5" or "A26"
# Matches: letter(s), optional dot+digit, slash or dash, digits
GRID_PATTERN_STANDALONE = r'\b([A-Z](?:\.\d+)?[/\-]\d+(?:\.\d+)?)\b'

# Pattern for simple grid like "A26" or "L5" (letter followed by digits)
GRID_PATTERN_SIMPLE = r'\b([A-Z]\d+(?:\.\d+)?)\b'

# Pattern for parenthetical grid coordinates like "(A5-A6/-2)" or "(A11-A12/ -3 to -4)"
GRID_PATTERN_PAREN = r'\(([A-Z]\d+(?:\.\d+)?(?:\s*-\s*[A-Z]?\d+)?(?:\s*/\s*-?\d+(?:\s+to\s+-?\d+)?)?)\)'


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

    # Extract grid - try multiple patterns in order of specificity
    grid = None
    grids_found = []

    # 1. Try explicit "GRID LINES" pattern first (most specific)
    grid_matches = re.findall(GRID_PATTERN_EXPLICIT, loc)
    if grid_matches:
        grids_found.extend(grid_matches)

    # 2. Try column references
    col_matches = re.findall(GRID_PATTERN_COLUMN, loc)
    if col_matches:
        grids_found.extend(col_matches)

    # 3. Try parenthetical grid coordinates (e.g., "(A5-A6/-2)")
    paren_matches = re.findall(GRID_PATTERN_PAREN, loc)
    if paren_matches:
        for match in paren_matches:
            if match not in grids_found:
                grids_found.append(match)

    # 4. Try standalone grid coordinates (e.g., "F.6/18", "N/5")
    # Always try this to capture additional grids not caught by explicit pattern
    standalone_matches = re.findall(GRID_PATTERN_STANDALONE, loc)
    if standalone_matches:
        # Filter out false positives (building codes, level codes) and duplicates
        for match in standalone_matches:
            # Skip if it looks like a building or level code
            if match in ['B1', 'B2', '1F', '2F', '3F', '4F', '5F', '6F', '7F', '8F']:
                continue
            # Skip if already found via explicit pattern
            if match not in grids_found:
                grids_found.append(match)

    # Combine found grids (deduplicated, up to 3)
    if grids_found:
        unique_grids = list(dict.fromkeys(grids_found))[:3]  # Preserve order, limit to 3
        grid = ','.join(unique_grids)

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
