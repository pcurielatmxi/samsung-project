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
    (r'\bLV(\d+)\b', 'level_num'),          # "LV4" or "Lv4" → 4F (SECAI format)
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

# =============================================================================
# Grid Validation Constants
# =============================================================================
# Valid whole row letters (A-S, excluding I per standard grid convention)
VALID_GRID_ROW_LETTERS = set('ABCDEFGHJKLMNOPQRS')

# Valid fractional rows (from gridline_mapping.py reference)
VALID_FRACTIONAL_ROWS = {
    'A.5', 'B.5', 'E.3', 'E.5', 'E.8',
    'F.2', 'F.4', 'F.6', 'F.8',
    'G.3', 'G.5', 'G.8',
    'H.3', 'H.5', 'H.8',
    'L.5', 'M.5'
}

# Valid column range
VALID_GRID_COL_MIN = 1
VALID_GRID_COL_MAX = 34


def is_valid_grid(row: str, col: float) -> bool:
    """
    Check if grid coordinate is within the approved gridline system.

    Args:
        row: Row letter (A-S) or fractional row (e.g., "F.4")
        col: Column number (1-34)

    Returns:
        True if valid grid coordinate, False otherwise
    """
    row = row.upper()
    # Check fractional rows
    if '.' in row:
        return row in VALID_FRACTIONAL_ROWS
    # Check whole letter and column range
    return (
        row in VALID_GRID_ROW_LETTERS and
        VALID_GRID_COL_MIN <= col <= VALID_GRID_COL_MAX
    )


# =============================================================================
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

# NEW: Pattern for explicit "Gridline N14" or "At Gridline N 22" references
# Also matches plural "Gridlines"
GRID_PATTERN_GRIDLINE = r'(?:AT\s+)?GRIDLINES?\s+([A-S])\s*(\d+)'

# NEW: Pattern for hyphen-separated format like "B-19", "E-30"
GRID_PATTERN_HYPHEN = r'\b([A-S])-(\d+)\b'

# NEW: Pattern for no-separator format like "E39", "N14", "A26" (2+ digits required)
GRID_PATTERN_NOSEP = r'\b([A-S])(\d{2,})\b'


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

    # 5. NEW: Try explicit "Gridline N14" or "At Gridline N 22" references
    gridline_matches = re.findall(GRID_PATTERN_GRIDLINE, loc)
    for letter, num in gridline_matches:
        try:
            if is_valid_grid(letter, float(num)):
                coord = f"{letter}/{num}"
                if coord not in grids_found:
                    grids_found.append(coord)
        except ValueError:
            continue

    # 6. NEW: Try hyphen-separated format like "B-19", "E-30"
    hyphen_matches = re.findall(GRID_PATTERN_HYPHEN, loc)
    for letter, num in hyphen_matches:
        try:
            if is_valid_grid(letter, float(num)):
                coord = f"{letter}/{num}"
                if coord not in grids_found:
                    grids_found.append(coord)
        except ValueError:
            continue

    # 7. NEW: Try no-separator format like "E39", "N14" (lowest priority)
    nosep_matches = re.findall(GRID_PATTERN_NOSEP, loc)
    for letter, num in nosep_matches:
        try:
            if is_valid_grid(letter, float(num)):
                coord = f"{letter}/{num}"
                if coord not in grids_found:
                    grids_found.append(coord)
        except ValueError:
            continue

    # Combine found grids (deduplicated, up to 3)
    if grids_found:
        # Normalize: convert hyphens to slashes for consistency
        normalized = []
        for g in grids_found:
            norm = g.replace('-', '/')
            if norm not in normalized:
                normalized.append(norm)
        unique_grids = normalized[:3]  # Limit to 3
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
