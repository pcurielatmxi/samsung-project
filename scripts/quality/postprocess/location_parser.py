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

# =============================================================================
# Phase 3 Patterns - Additional coverage improvements
# =============================================================================

# Pattern for "GL-33", "GL 33", "GL33" (column-only with GL prefix and optional separator)
GRID_PATTERN_GL_HYPHEN = r'\bGL[-\s]?(\d+)\b'

# Pattern for pure column range without row: "29-30", "17-22" (must be 2 digits each)
GRID_PATTERN_COL_ONLY_RANGE = r'\b(\d{1,2})[-–](\d{1,2})\b'

# Pattern for "COL." or "COL " prefix: "COL. D24", "COL D-27", "COLS E-17"
GRID_PATTERN_COL_PREFIX = r'\bCOLS?\.?\s*([A-N])[-]?(\d+)'

# Pattern for row range with THRU/TO: "E-17 THRU J-17", "A THRU C/15", "D TO G"
GRID_PATTERN_THRU_RANGE = r'\b([A-N])[-/]?(\d+)?\s*(?:THRU|TO|THROUGH)\s*([A-N])[-/]?(\d+)?'

# Pattern for "Zone D17-E17" or "Zone Col D24-D27" (zone/area prefix)
GRID_PATTERN_ZONE = r'\bZONE\s+(?:COL\.?\s*)?([A-N])(\d+)[-–]([A-N])?(\d+)'

# Pattern for inverted with hyphen: "17-E", "22-G" (column-row format)
GRID_PATTERN_COL_ROW = r'\b(\d+)[-]([A-N])\b'

# Pattern for "Row G", "Row G-J", "Rows A thru C" (explicit row reference)
GRID_PATTERN_ROW_EXPLICIT = r'\bROWS?\s+([A-N])(?:[-–\s]+(?:THRU|TO|-)?\s*([A-N]))?'

# Pattern for room-style codes that are actually grids: "L17", "L28", "L31" (L=Line, column only)
# Only when NOT followed by F (level) and in grid-like context
GRID_PATTERN_LINE_COL = r'\bL(\d{2})\b(?!\s*F)'

# Pattern for elevator references: "ELEVATOR 22", "ELEV 18", "ELV 3"
# Capture elevator number for potential lookup via dim_location
GRID_PATTERN_ELEVATOR = r'\b(?:ELEVATOR|ELEV|ELV)\.?\s*(\d+[AB]?)\b'

# Pattern for stair references: "STAIR 19", "STR 4", "STAIRS A"
GRID_PATTERN_STAIR = r'\b(?:STAIR|STR|STAIRS?)\.?\s*(\d+|[A-Z])\b'

# Pattern for room code ranges: "A2-A5", "D24-D27" (letter+number range)
GRID_PATTERN_ROOM_RANGE = r'\b([A-N])(\d+)[-–]([A-N])?(\d+)\b'

# Pattern for "between gridlines (X)-(Y) & (A)-(B)" format
# Matches: "(C.8)-(F.1) & (0.8)-(3.2)" or "(A)-(C.5) & (5.8)-(9.1)"
# NOTE: Input is uppercased, so patterns must match uppercase
GRID_PATTERN_BETWEEN_PAREN = r'\(([A-N](?:\.\d)?)\)[-–]\(([A-N](?:\.\d)?)\)\s*[&,]\s*\((\d+(?:\.\d)?)\)[-–]\((\d+(?:\.\d)?)\)'

# Pattern for "Gridlines X - Y / A - B" format (rows then columns)
# Matches: "GRIDLINES C.9 - F / 22.1 - 25"
GRID_PATTERN_GRIDLINES_RANGE = r'GRIDLINES?\s+([A-N](?:\.\d)?)\s*[-–]\s*([A-N](?:\.\d)?)\s*/\s*(\d+(?:\.\d)?)\s*[-–]\s*(\d+(?:\.\d)?)'

# Pattern for "area XX" references: "AREA D2", "AREA E9"
# Extracts letter+number as grid hint
GRID_PATTERN_AREA_CODE = r'\bAREA\s+([A-N])(\d+)\b'

# Pattern for "between Gridlines X / A" (single point or simpler range)
GRID_PATTERN_BETWEEN_SIMPLE = r'BETWEEN\s+GRIDLINES?\s+([A-N](?:\.\d)?)\s*[-–]?\s*([A-N](?:\.\d)?)?\s*/\s*(\d+(?:\.\d)?)\s*[-–]?\s*(\d+(?:\.\d)?)?'

# Building-only fallback: "Building: FAB", "Building = CUB", "Building CUB"
# Used when no grid found but building is mentioned - returns BLDG/XXX format
GRID_PATTERN_BUILDING_ONLY = r'BUILDING[:\s=]+\s*(FAB|CUB|GCS|SUE|SUW|OB1|OB2)\b'


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

    # ==========================================================================
    # Phase 3 Patterns - Additional coverage
    # ==========================================================================

    # 14. GL-33 format (GL with hyphen separator): "GL-33", "GL-17"
    for match in re.finditer(GRID_PATTERN_GL_HYPHEN, loc):
        col = match.group(1)
        try:
            col_num = float(col)
            if VALID_GRID_COL_MIN <= col_num <= VALID_GRID_COL_MAX:
                add_if_new(f"?/{col}", match.span())
        except ValueError:
            continue

    # 15. COL/COLS prefix: "COL. D24", "COLS E-17"
    for match in re.finditer(GRID_PATTERN_COL_PREFIX, loc):
        row, col = match.groups()
        try:
            if is_valid_grid(row, float(col)):
                add_if_new(f"{row}/{col}", match.span())
        except ValueError:
            continue

    # 16. THRU/TO range: "E-17 THRU J-17", "D TO G/15"
    for match in re.finditer(GRID_PATTERN_THRU_RANGE, loc):
        row1, col1, row2, col2 = match.groups()
        try:
            # If we have both cols, it's a full range
            if col1 and col2:
                if is_valid_grid(row1, float(col1)) and is_valid_grid(row2, float(col2)):
                    add_if_new(f"{row1}/{col1}", match.span())
                    add_if_new(f"{row2}/{col2}", match.span())
            # If only one col, it's a row range at that column
            elif col1 or col2:
                col = col1 or col2
                if is_valid_grid(row1, float(col)):
                    add_if_new(f"{row1}-{row2}/{col}", match.span())
            # If no cols, it's a row-only range
            else:
                add_if_new(f"{row1}-{row2}/?", match.span())
        except (ValueError, TypeError):
            continue

    # 17. Zone prefix: "Zone D17-E17", "Zone Col D24-D27"
    for match in re.finditer(GRID_PATTERN_ZONE, loc):
        row1, col1, row2, col2 = match.groups()
        try:
            row2 = row2 or row1  # Same row if not specified
            if is_valid_grid(row1, float(col1)):
                if col2 and int(col1) != int(col2):
                    add_if_new(f"{row1}/{col1}-{col2}", match.span())
                else:
                    add_if_new(f"{row1}-{row2}/{col1}", match.span())
        except (ValueError, TypeError):
            continue

    # 18. Inverted with hyphen: "17-E", "22-G"
    for match in re.finditer(GRID_PATTERN_COL_ROW, loc):
        col, row = match.groups()
        try:
            if is_valid_grid(row, float(col)):
                add_if_new(f"{row}/{col}", match.span())
        except ValueError:
            continue

    # 19. Explicit row reference: "Row G", "Rows A-C"
    for match in re.finditer(GRID_PATTERN_ROW_EXPLICIT, loc):
        row1, row2 = match.groups()
        if row2:
            add_if_new(f"{row1}-{row2}/?", match.span())
        else:
            add_if_new(f"{row1}/?", match.span())

    # 20. Pure column range (only if no other grids found): "29-30"
    # This is low priority - only use if nothing else matched
    if not grids_found:
        for match in re.finditer(GRID_PATTERN_COL_ONLY_RANGE, loc):
            col1, col2 = match.groups()
            try:
                c1, c2 = float(col1), float(col2)
                # Only accept if both are valid columns and it looks like a range
                if (VALID_GRID_COL_MIN <= c1 <= VALID_GRID_COL_MAX and
                    VALID_GRID_COL_MIN <= c2 <= VALID_GRID_COL_MAX and
                    c1 != c2 and abs(c2 - c1) <= 10):  # Reasonable range
                    add_if_new(f"?/{col1}-{col2}", match.span())
            except ValueError:
                continue

    # ==========================================================================
    # Phase 4: Named location patterns (for dim_location lookup)
    # ==========================================================================

    # 21. Elevator references: "ELEVATOR 22" → "ELV/22" (special format for lookup)
    if not grids_found:
        for match in re.finditer(GRID_PATTERN_ELEVATOR, loc):
            elv_num = match.group(1)
            add_if_new(f"ELV/{elv_num}", match.span())

    # 22. Stair references: "STAIR 19" → "STR/19" (special format for lookup)
    if not grids_found:
        for match in re.finditer(GRID_PATTERN_STAIR, loc):
            stair_id = match.group(1)
            add_if_new(f"STR/{stair_id}", match.span())

    # 23. Room code ranges: "A2-A5", "D24-D27" → row range
    for match in re.finditer(GRID_PATTERN_ROOM_RANGE, loc):
        row1, col1, row2, col2 = match.groups()
        try:
            row2 = row2 or row1  # Same row if not specified
            c1, c2 = float(col1), float(col2)
            if is_valid_grid(row1, c1) and is_valid_grid(row2, c2):
                if row1 == row2 and c1 != c2:
                    # Same row, column range
                    add_if_new(f"{row1}/{int(c1)}-{int(c2)}", match.span())
                elif row1 != row2:
                    # Row range
                    add_if_new(f"{row1}-{row2}/{int(c1)}", match.span())
                else:
                    add_if_new(f"{row1}/{int(c1)}", match.span())
        except (ValueError, TypeError):
            continue

    # ==========================================================================
    # Phase 5: Complex gridline patterns from RABA
    # ==========================================================================

    # 24. Between gridlines with parentheses: "(C.8)-(F.1) & (0.8)-(3.2)"
    for match in re.finditer(GRID_PATTERN_BETWEEN_PAREN, loc):
        row1, row2, col1, col2 = match.groups()
        try:
            # This format has rows first, then columns
            c1, c2 = float(col1), float(col2)
            if c1 > VALID_GRID_COL_MAX or c2 > VALID_GRID_COL_MAX:
                continue  # Invalid column
            add_if_new(f"{row1}-{row2}/{col1}-{col2}", match.span())
        except (ValueError, TypeError):
            continue

    # 25. Gridlines X - Y / A - B format: "Gridlines C.9 - F / 22.1 - 25"
    for match in re.finditer(GRID_PATTERN_GRIDLINES_RANGE, loc):
        row1, row2, col1, col2 = match.groups()
        try:
            c1, c2 = float(col1), float(col2)
            if VALID_GRID_COL_MIN <= c1 <= VALID_GRID_COL_MAX:
                add_if_new(f"{row1}-{row2}/{col1}-{col2}", match.span())
        except (ValueError, TypeError):
            continue

    # 26. Between Gridlines simpler format: "between Gridlines E.9 - G / 9.1 - 11"
    for match in re.finditer(GRID_PATTERN_BETWEEN_SIMPLE, loc):
        row1, row2, col1, col2 = match.groups()
        try:
            row2 = row2 or row1
            col2 = col2 or col1
            c1 = float(col1)
            if VALID_GRID_COL_MIN <= c1 <= VALID_GRID_COL_MAX:
                if row1 != row2 or col1 != col2:
                    add_if_new(f"{row1}-{row2}/{col1}-{col2}", match.span())
                else:
                    add_if_new(f"{row1}/{col1}", match.span())
        except (ValueError, TypeError):
            continue

    # 27. Area codes: "area D2", "area E9" → treat as grid hint
    for match in re.finditer(GRID_PATTERN_AREA_CODE, loc):
        row, col = match.groups()
        try:
            c = float(col)
            if VALID_GRID_COL_MIN <= c <= VALID_GRID_COL_MAX and row in VALID_GRID_ROW_LETTERS:
                add_if_new(f"{row}/{int(c)}", match.span())
        except (ValueError, TypeError):
            continue

    # ==========================================================================
    # Phase 6: Building-only fallback (lowest priority)
    # ==========================================================================

    # 28. Building-only fallback: "Building: FAB" → "BLDG/FAB"
    # Only used if NO other grid was found
    if not grids_found:
        for match in re.finditer(GRID_PATTERN_BUILDING_ONLY, loc):
            bldg = match.group(1)
            add_if_new(f"BLDG/{bldg}", match.span())

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
