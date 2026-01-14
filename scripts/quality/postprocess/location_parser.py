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

# =============================================================================
# NEW PATTERNS - Phase 2 Improvements
# =============================================================================

# Pattern for "GL 22", "GL22", "LINE 15", "GRIDLINE 22" (column-only references)
GRID_PATTERN_GL_COL = r'(?:GL|GRIDLINE|LINE)\s*#?\s*(\d+)\b'

# Pattern for fractional row with space separator: "L.5 23", "M.5 30", "F.6 18"
GRID_PATTERN_FRAC_SPACE = r'\b([A-N])\.(\d)\s+(\d+)\b'

# Pattern for grid range - same row, column range: "G/10-12", "F.5/18-22"
GRID_PATTERN_COL_RANGE = r'\b([A-N](?:\.\d)?)[/](\d+)[-–](\d+)\b'

# Pattern for grid range - row range, same column: "A-C/15", "G-J/22"
GRID_PATTERN_ROW_RANGE = r'\b([A-N])[-–]([A-N])[/](\d+)\b'

# Pattern for inverted notation (column/row): "22/G", "15/N"
GRID_PATTERN_INVERTED = r'\b(\d+)[/]([A-N])\b'

# Pattern for area prefix followed by grid: "SUBGRADE G/12", "PENTHOUSE N/5"
GRID_PATTERN_AREA_PREFIX = r'(?:SUBGRADE|PENTHOUSE|MEZZANINE|INTERSTITIAL)\s+([A-N](?:\.\d)?)[/](\d+)'

# Pattern for "at/near/by" grid references: "AT G/10", "NEAR N/5", "BY L/22"
GRID_PATTERN_PREPOSITION = r'(?:AT|NEAR|BY|@)\s+([A-N](?:\.\d)?)[/](\d+)'


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
    # IMPORTANT: Range patterns must run FIRST to prevent partial matches
    grid = None
    grids_found = []
    matched_spans = []  # Track matched positions to avoid overlap

    def add_if_new(coord, span=None):
        """Add coordinate if not already found and doesn't overlap matched spans."""
        if coord not in grids_found:
            # Check for overlap with existing spans
            if span:
                for existing_start, existing_end in matched_spans:
                    if not (span[1] <= existing_start or span[0] >= existing_end):
                        return  # Overlaps, skip
                matched_spans.append(span)
            grids_found.append(coord)

    # ==========================================================================
    # Phase 1: RANGE PATTERNS (must run first to capture full ranges)
    # ==========================================================================

    # 1a. Column range on same row: "G/10-12", "F.5/18-22"
    for match in re.finditer(GRID_PATTERN_COL_RANGE, loc):
        row, col_start, col_end = match.groups()
        try:
            start = int(col_start)
            end = int(col_end)
            if start <= end and is_valid_grid(row, float(start)):
                coord = f"{row}/{col_start}-{col_end}"
                add_if_new(coord, match.span())
        except ValueError:
            continue

    # 1b. Row range on same column: "A-C/15", "G-J/22"
    for match in re.finditer(GRID_PATTERN_ROW_RANGE, loc):
        row_start, row_end, col = match.groups()
        try:
            col_num = float(col)
            if is_valid_grid(row_start, col_num) and is_valid_grid(row_end, col_num):
                coord = f"{row_start}-{row_end}/{col}"
                add_if_new(coord, match.span())
        except ValueError:
            continue

    # ==========================================================================
    # Phase 2: EXPLICIT/SPECIFIC PATTERNS
    # ==========================================================================

    # 2. Try explicit "GRID LINES" pattern (most specific)
    for match in re.finditer(GRID_PATTERN_EXPLICIT, loc):
        add_if_new(match.group(1), match.span())

    # 3. Try column references like "COLUMN L5"
    for match in re.finditer(GRID_PATTERN_COLUMN, loc):
        add_if_new(match.group(1), match.span())

    # 4. Try parenthetical grid coordinates (e.g., "(A5-A6/-2)")
    for match in re.finditer(GRID_PATTERN_PAREN, loc):
        add_if_new(match.group(1), match.span())

    # 5. Try explicit "Gridline N14" or "At Gridline N 22" references
    for match in re.finditer(GRID_PATTERN_GRIDLINE, loc):
        letter, num = match.groups()
        try:
            if is_valid_grid(letter, float(num)):
                add_if_new(f"{letter}/{num}", match.span())
        except ValueError:
            continue

    # 6. Area prefix followed by grid: "SUBGRADE G/12", "PENTHOUSE N/5"
    for match in re.finditer(GRID_PATTERN_AREA_PREFIX, loc):
        row, col = match.groups()
        try:
            if is_valid_grid(row, float(col)):
                add_if_new(f"{row}/{col}", match.span())
        except ValueError:
            continue

    # 7. Preposition grid references: "AT G/10", "NEAR N/5"
    for match in re.finditer(GRID_PATTERN_PREPOSITION, loc):
        row, col = match.groups()
        try:
            if is_valid_grid(row, float(col)):
                add_if_new(f"{row}/{col}", match.span())
        except ValueError:
            continue

    # 8. Fractional row with space: "L.5 23", "M.5 30"
    for match in re.finditer(GRID_PATTERN_FRAC_SPACE, loc):
        letter, frac_digit, col = match.groups()
        try:
            row = f"{letter}.{frac_digit}"
            if is_valid_grid(row, float(col)):
                add_if_new(f"{row}/{col}", match.span())
        except ValueError:
            continue

    # ==========================================================================
    # Phase 3: GENERAL PATTERNS (may match substrings - run after specific)
    # ==========================================================================

    # 9. Try standalone grid coordinates (e.g., "F.6/18", "N/5")
    for match in re.finditer(GRID_PATTERN_STANDALONE, loc):
        m = match.group(1)
        # Skip building/level codes
        if m in ['B1', 'B2', '1F', '2F', '3F', '4F', '5F', '6F', '7F', '8F']:
            continue
        # Validate the coordinate - extract row and column
        parts = re.match(r'([A-Z](?:\.\d+)?)[/\-](\d+(?:\.\d+)?)', m)
        if parts:
            row, col = parts.groups()
            try:
                if is_valid_grid(row, float(col)):
                    add_if_new(m, match.span())
            except ValueError:
                continue
        else:
            add_if_new(m, match.span())

    # 10. Try hyphen-separated format like "B-19", "E-30"
    for match in re.finditer(GRID_PATTERN_HYPHEN, loc):
        letter, num = match.groups()
        try:
            if is_valid_grid(letter, float(num)):
                add_if_new(f"{letter}/{num}", match.span())
        except ValueError:
            continue

    # 11. Try no-separator format like "E39", "N14" (lowest priority)
    for match in re.finditer(GRID_PATTERN_NOSEP, loc):
        letter, num = match.groups()
        try:
            if is_valid_grid(letter, float(num)):
                add_if_new(f"{letter}/{num}", match.span())
        except ValueError:
            continue

    # 12. Inverted notation: "22/G" → "G/22"
    for match in re.finditer(GRID_PATTERN_INVERTED, loc):
        col, row = match.groups()
        try:
            if is_valid_grid(row, float(col)):
                add_if_new(f"{row}/{col}", match.span())
        except ValueError:
            continue

    # 13. GL/LINE column-only references: "GL 22", "LINE 15" (column only, row unknown)
    for match in re.finditer(GRID_PATTERN_GL_COL, loc):
        col = match.group(1)
        try:
            col_num = float(col)
            if VALID_GRID_COL_MIN <= col_num <= VALID_GRID_COL_MAX:
                add_if_new(f"?/{col}", match.span())
        except ValueError:
            continue

    # Combine found grids (deduplicated, up to 3)
    if grids_found:
        # Normalize: convert ONLY row-col separator hyphens to slashes
        # Keep range hyphens (e.g., "G/10-12" should stay as-is, not become "G/10/12")
        normalized = []
        for g in grids_found:
            # Only normalize letter-hyphen-number patterns (row-col separators)
            # e.g., "G-12" → "G/12", but "G/10-12" stays unchanged
            norm = re.sub(r'^([A-N](?:\.\d)?)-(\d)', r'\1/\2', g)
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
