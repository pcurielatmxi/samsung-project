"""
Centralized Grid Parser for All Data Sources

This module consolidates grid parsing logic from all sources (RABA, PSI, TBM, P6)
into a single, comprehensive parser. Sources pass their raw location text and
the parser identifies grid patterns regardless of source-specific formatting.

================================================================================
GRID SYSTEM OVERVIEW
================================================================================

The Samsung Taylor FAB1 project uses a unified grid system across all buildings:
- Rows: Letters A-N (north-south axis)
- Columns: Numbers 1-33+ (east-west axis)
- Fractional rows supported: F.5, G.3, etc.

================================================================================
SUPPORTED PATTERN CATEGORIES
================================================================================

1. POINT: Single grid coordinate (row + column)
   - "G/10", "F.5/17", "J11", "N-17"

2. RANGE: Grid bounds with row and column ranges
   - "G-J/10-15", "A-N/1-3", "B-D/8-12"
   - TBM variants: "C/11 - C/22", "E13~E28", "J~G/12~18"

3. ROW_ONLY: Only row information available
   - "G", "GL-N", "G/H LINE", "K/D COLUMNS"

4. COL_ONLY: Only column information available
   - "33", "CL 5-12", "29-30", "COL 11"

5. NAMED: Named locations that may have implied grid
   - "Stair 21", "ELV-03", "CE8P" (equipment)

================================================================================
USAGE
================================================================================

from scripts.integrated_analysis.location.core.grid_parser import parse_grid

# Parse any source's grid field
result = parse_grid("G/10")
result = parse_grid("C/11 - C/22")  # TBM format
result = parse_grid("J~G/12~18")    # TBM tilde format
result = parse_grid("GL-33")         # Gridline notation

# Result contains:
# - grid_row_min, grid_row_max: Letter bounds (or None)
# - grid_col_min, grid_col_max: Numeric bounds (or None)
# - grid_type: POINT, RANGE, ROW_ONLY, COL_ONLY, NAMED, or None
# - grid_raw: Original input (cleaned)
"""

import re
from dataclasses import dataclass
from typing import Optional, Dict, Any

import pandas as pd


# Valid grid row letters (A through N, plus fractional like F.5)
VALID_GRID_ROWS = set('ABCDEFGHIJKLMN')


@dataclass
class GridParseResult:
    """Result of parsing a grid string."""
    grid_row_min: Optional[str] = None
    grid_row_max: Optional[str] = None
    grid_col_min: Optional[float] = None
    grid_col_max: Optional[float] = None
    grid_type: Optional[str] = None  # POINT, RANGE, ROW_ONLY, COL_ONLY, NAMED
    grid_raw: Optional[str] = None   # Original input (cleaned)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'grid_row_min': self.grid_row_min,
            'grid_row_max': self.grid_row_max,
            'grid_col_min': self.grid_col_min,
            'grid_col_max': self.grid_col_max,
            'grid_type': self.grid_type,
            'grid_raw': self.grid_raw,
        }

    @property
    def has_row(self) -> bool:
        """Returns True if row information is available."""
        return self.grid_row_min is not None

    @property
    def has_col(self) -> bool:
        """Returns True if column information is available."""
        return self.grid_col_min is not None

    @property
    def has_grid(self) -> bool:
        """Returns True if any grid information is available."""
        return self.has_row or self.has_col


def _clean_input(value: str) -> Optional[str]:
    """Clean and normalize input string."""
    if not value or pd.isna(value):
        return None

    val = str(value).strip().upper()

    # Skip known non-grid values
    non_grid_patterns = [
        'N/A', 'NA', 'NONE', 'ALL', 'VARIOUS', 'MULTIPLE', 'TBD',
        'SITE WIDE', 'SITE-WIDE', 'PROJECT WIDE', 'BUILDING',
    ]
    if val in non_grid_patterns:
        return None

    return val


def _sort_rows(r1: str, r2: str) -> tuple:
    """Sort two row values, handling fractional rows."""
    def row_key(r):
        base = r[0] if r else 'A'
        decimal = float(r[2:]) if len(r) > 1 and '.' in r else 0
        return (base, decimal)

    rows = sorted([r1, r2], key=row_key)
    return rows[0], rows[1]


def _sort_cols(c1: float, c2: float) -> tuple:
    """Sort two column values."""
    return (min(c1, c2), max(c1, c2))


def parse_grid(value: str) -> GridParseResult:
    """
    Parse a grid string from any source into standardized components.

    This function handles all known grid formats from RABA, PSI, TBM, and P6.
    Sources should pass their raw location/grid field value.

    Args:
        value: Raw grid string (e.g., "G/10", "C/11 - C/22", "J~K/12~18")

    Returns:
        GridParseResult with parsed grid bounds and type.
        If parsing fails, returns result with all None values.

    Examples:
        >>> parse_grid("G/10").to_dict()
        {'grid_row_min': 'G', 'grid_row_max': 'G', 'grid_col_min': 10.0, ...}

        >>> parse_grid("C/11 - C/22").to_dict()
        {'grid_row_min': 'C', 'grid_row_max': 'C', 'grid_col_min': 11.0, 'grid_col_max': 22.0, ...}
    """
    result = GridParseResult()

    val = _clean_input(value)
    if not val:
        return result

    result.grid_raw = val

    # =========================================================================
    # SKIP PATTERNS - Non-grid values that look like grid patterns
    # =========================================================================

    # Lab addresses: B-150, B-203 (letter + hyphen + 3+ digits)
    if re.match(r'^[A-Z]-\d{3,}$', val):
        return result

    # Room codes: FAB116101, FAB130311A
    if re.match(r'^FAB1?\d{5,}[A-Z]?$', val):
        result.grid_type = 'NAMED'
        return result

    # Stair/Elevator: STR-21, ELV-03, FAB1-ST21
    if re.match(r'^(STR|ELV|FAB1-ST|FAB1-EL)[-]?\d+$', val):
        result.grid_type = 'NAMED'
        return result

    # =========================================================================
    # TBM-SPECIFIC PATTERNS (more complex, check first)
    # =========================================================================

    # TBM range format: "C/11 - C/22" → row C, cols 11-22
    m = re.match(r'^([A-N])/(\d+)\s*-\s*([A-N])?/?(\d+)$', val)
    if m:
        result.grid_row_min = m.group(1)
        result.grid_row_max = m.group(3) or m.group(1)
        cols = _sort_cols(float(m.group(2)), float(m.group(4)))
        result.grid_col_min, result.grid_col_max = cols
        result.grid_type = 'RANGE'
        return result

    # Tilde row~row/col~col: J~G/12~18, G~E/9~12
    m = re.match(r'^([A-N])~([A-N])[/](\d+)~(\d+)', val)
    if m:
        rows = _sort_rows(m.group(1), m.group(2))
        result.grid_row_min, result.grid_row_max = rows
        cols = _sort_cols(float(m.group(3)), float(m.group(4)))
        result.grid_col_min, result.grid_col_max = cols
        result.grid_type = 'RANGE'
        return result

    # Tilde with comma separator: J~K,8~15 → rows J-K, cols 8-15
    m = re.match(r'^([A-N])~([A-N]),(\d+)~(\d+)$', val)
    if m:
        rows = _sort_rows(m.group(1), m.group(2))
        result.grid_row_min, result.grid_row_max = rows
        cols = _sort_cols(float(m.group(3)), float(m.group(4)))
        result.grid_col_min, result.grid_col_max = cols
        result.grid_type = 'RANGE'
        return result

    # Tilde compact range: E3~J18, E13~E28 → rows E-J, cols 3-18
    m = re.match(r'^([A-N])(\d+)~([A-N])(\d+)$', val)
    if m:
        rows = _sort_rows(m.group(1), m.group(3))
        result.grid_row_min, result.grid_row_max = rows
        cols = _sort_cols(float(m.group(2)), float(m.group(4)))
        result.grid_col_min, result.grid_col_max = cols
        result.grid_type = 'RANGE'
        return result

    # Tilde col range: C3~4 → row C, cols 3-4
    m = re.match(r'^([A-N])(\d+)~(\d+)$', val)
    if m:
        result.grid_row_min = result.grid_row_max = m.group(1)
        cols = _sort_cols(float(m.group(2)), float(m.group(3)))
        result.grid_col_min, result.grid_col_max = cols
        result.grid_type = 'RANGE'
        return result

    # Tilde col-only: E/5~E/12 or E/8~9 → row E, cols 5-12 or 8-9
    m = re.match(r'^([A-N])/(\d+)~(?:[A-N]/)?(\d+)$', val)
    if m:
        result.grid_row_min = result.grid_row_max = m.group(1)
        cols = _sort_cols(float(m.group(2)), float(m.group(3)))
        result.grid_col_min, result.grid_col_max = cols
        result.grid_type = 'RANGE'
        return result

    # Tilde fractional: A54~B55.5 → rows A-B, cols 54-55.5
    m = re.match(r'^([A-N])(\d+(?:\.\d+)?)~([A-N])(\d+(?:\.\d+)?)$', val)
    if m:
        rows = _sort_rows(m.group(1), m.group(3))
        result.grid_row_min, result.grid_row_max = rows
        cols = _sort_cols(float(m.group(2)), float(m.group(4)))
        result.grid_col_min, result.grid_col_max = cols
        result.grid_type = 'RANGE'
        return result

    # Tilde row-col range: A~C/7~A~C/29 → rows A-C, cols 7-29
    m = re.match(r'^([A-N])~([A-N])/(\d+)~(?:[A-N]~[A-N]/)?(\d+)$', val)
    if m:
        rows = _sort_rows(m.group(1), m.group(2))
        result.grid_row_min, result.grid_row_max = rows
        cols = _sort_cols(float(m.group(3)), float(m.group(4)))
        result.grid_col_min, result.grid_col_max = cols
        result.grid_type = 'RANGE'
        return result

    # GL row tilde: GL K34~44 → row K, cols 34-44
    m = re.match(r'^GL\s+([A-N])(\d+)~(\d+)$', val)
    if m:
        result.grid_row_min = result.grid_row_max = m.group(1)
        cols = _sort_cols(float(m.group(2)), float(m.group(3)))
        result.grid_col_min, result.grid_col_max = cols
        result.grid_type = 'RANGE'
        return result

    # Tilde with fractional row: G~G.3/8~32 → rows G-G.3, cols 8-32
    m = re.match(r'^([A-N])~([A-N])\.(\d+)/(\d+)~(\d+)$', val)
    if m:
        result.grid_row_min = m.group(1)
        result.grid_row_max = f"{m.group(2)}.{m.group(3)}"
        cols = _sort_cols(float(m.group(4)), float(m.group(5)))
        result.grid_col_min, result.grid_col_max = cols
        result.grid_type = 'RANGE'
        return result

    # Fractional tilde with hyphen: G.3~H.8-3~8 → rows G.3-H.8, cols 3-8
    m = re.match(r'^([A-N])\.(\d+)~([A-N])\.(\d+)[-–](\d+)~(\d+)', val)
    if m:
        result.grid_row_min = f"{m.group(1)}.{m.group(2)}"
        result.grid_row_max = f"{m.group(3)}.{m.group(4)}"
        cols = _sort_cols(float(m.group(5)), float(m.group(6)))
        result.grid_col_min, result.grid_col_max = cols
        result.grid_type = 'RANGE'
        return result

    # Fractional tilde with comma: H.8~K,6-3 → rows H.8-K, cols 3-6
    m = re.match(r'^([A-N])\.(\d+)~([A-N]),(\d+)[-–](\d+)$', val)
    if m:
        result.grid_row_min = f"{m.group(1)}.{m.group(2)}"
        result.grid_row_max = m.group(3)
        cols = _sort_cols(float(m.group(4)), float(m.group(5)))
        result.grid_col_min, result.grid_col_max = cols
        result.grid_type = 'RANGE'
        return result

    # SUW/SUE col-col/row-row: SUW 5-6/L-N, SUE 5-20/D
    m = re.match(r'^(SUW|SUE)\s+(\d+)[-–](\d+)[/]([A-N])[-–]?([A-N])?', val)
    if m:
        cols = _sort_cols(float(m.group(2)), float(m.group(3)))
        result.grid_col_min, result.grid_col_max = cols
        result.grid_row_min = m.group(4)
        result.grid_row_max = m.group(5) or m.group(4)
        result.grid_type = 'RANGE'
        return result

    # SUW col/row format: SUW 28-24/L
    m = re.match(r'^(SUW|SUE)\s+(\d+)[-–]?(\d+)?[/]([A-N])', val)
    if m:
        result.grid_col_min = float(m.group(2))
        result.grid_col_max = float(m.group(3) or m.group(2))
        if result.grid_col_min > result.grid_col_max:
            result.grid_col_min, result.grid_col_max = result.grid_col_max, result.grid_col_min
        result.grid_row_min = result.grid_row_max = m.group(4)
        result.grid_type = 'RANGE'
        return result

    # Two-letter row + cols: LM 6,4 → rows L-M, cols 4,6
    m = re.match(r'^([A-N])([A-N])\s+(\d+),(\d+)$', val)
    if m:
        rows = _sort_rows(m.group(1), m.group(2))
        result.grid_row_min, result.grid_row_max = rows
        cols = _sort_cols(float(m.group(3)), float(m.group(4)))
        result.grid_col_min, result.grid_col_max = cols
        result.grid_type = 'RANGE'
        return result

    # Tilde col-only: 3~8 → cols 3-8
    m = re.match(r'^(\d+)~(\d+)$', val)
    if m:
        cols = _sort_cols(float(m.group(1)), float(m.group(2)))
        result.grid_col_min, result.grid_col_max = cols
        result.grid_type = 'COL_ONLY'
        return result

    # =========================================================================
    # RANGE PATTERNS (row + column ranges)
    # =========================================================================

    # Letter range with columns: A-N/1-3, N-P/23-29
    m = re.match(r'^([A-Z])[-–]([A-Z])[/\s](\d+)[-–]?(\d+)?', val)
    if m:
        rows = _sort_rows(m.group(1), m.group(2))
        result.grid_row_min, result.grid_row_max = rows
        result.grid_col_min = float(m.group(3))
        result.grid_col_max = float(m.group(4) or m.group(3))
        result.grid_type = 'RANGE'
        return result

    # Letter+Col range: K20-K32, J33-K33, L5-L11
    m = re.match(r'^([A-N])(\d+)[-–]([A-N])?(\d+)', val)
    if m:
        result.grid_row_min = m.group(1)
        result.grid_row_max = m.group(3) or m.group(1)
        if result.grid_row_min > result.grid_row_max:
            result.grid_row_min, result.grid_row_max = result.grid_row_max, result.grid_row_min
        cols = _sort_cols(float(m.group(2)), float(m.group(4)))
        result.grid_col_min, result.grid_col_max = cols
        result.grid_type = 'RANGE'
        return result

    # Row + col range: N-29-23 → row N, cols 23-29
    m = re.match(r'^([A-N])-(\d+)-(\d+)(?:\s|$|;)', val)
    if m:
        result.grid_row_min = result.grid_row_max = m.group(1)
        cols = _sort_cols(float(m.group(2)), float(m.group(3)))
        result.grid_col_min, result.grid_col_max = cols
        result.grid_type = 'RANGE'
        return result

    # N/ space col: N/ 10-11
    m = re.match(r'^([A-N])/\s+(\d+)[-–](\d+)', val)
    if m:
        result.grid_row_min = result.grid_row_max = m.group(1)
        cols = _sort_cols(float(m.group(2)), float(m.group(3)))
        result.grid_col_min, result.grid_col_max = cols
        result.grid_type = 'RANGE'
        return result

    # Decimal row-col range: E-14.0 - 17.0
    m = re.match(r'^([A-N])[-–](\d+\.?\d*)\s*[-–]\s*(\d+\.?\d*)$', val)
    if m:
        result.grid_row_min = result.grid_row_max = m.group(1)
        cols = _sort_cols(float(m.group(2)), float(m.group(3)))
        result.grid_col_min, result.grid_col_max = cols
        result.grid_type = 'RANGE'
        return result

    # N-5 to N33 → row N, cols 5-33
    m = re.match(r'^([A-N])[-]?(\d+)\s+TO\s+([A-N])?[-]?(\d+)', val, re.IGNORECASE)
    if m:
        result.grid_row_min = m.group(1)
        result.grid_row_max = m.group(3) or m.group(1)
        cols = _sort_cols(float(m.group(2)), float(m.group(4)))
        result.grid_col_min, result.grid_col_max = cols
        result.grid_type = 'RANGE'
        return result

    # B-4 MEZZ → row B, col 4
    m = re.match(r'^([A-N])[-](\d+)\s+(MEZZ|MEZZANINE)', val)
    if m:
        result.grid_row_min = result.grid_row_max = m.group(1)
        result.grid_col_min = result.grid_col_max = float(m.group(2))
        result.grid_type = 'POINT'
        return result

    # =========================================================================
    # POINT PATTERNS (single grid coordinate)
    # =========================================================================

    # Standard format: G/10, F.5/17, F/18
    m = re.match(r'^([A-N])(?:\.(\d+))?/(\d+(?:\.\d+)?)$', val)
    if m:
        if m.group(2):
            result.grid_row_min = result.grid_row_max = f"{m.group(1)}.{m.group(2)}"
        else:
            result.grid_row_min = result.grid_row_max = m.group(1)
        result.grid_col_min = result.grid_col_max = float(m.group(3))
        result.grid_type = 'POINT'
        return result

    # Letter + Column: J11, A-19, A19, N/17
    m = re.match(r'^([A-N])[-/]?(\d+(?:\.\d+)?)$', val)
    if m:
        result.grid_row_min = result.grid_row_max = m.group(1)
        result.grid_col_min = result.grid_col_max = float(m.group(2))
        result.grid_type = 'POINT'
        return result

    # Letter + space + Column: "N 17", "A 32"
    m = re.match(r'^([A-N])\s+(\d+)(?:\s|$)', val)
    if m:
        result.grid_row_min = result.grid_row_max = m.group(1)
        result.grid_col_min = result.grid_col_max = float(m.group(2))
        result.grid_type = 'POINT'
        return result

    # GL-A/28, GL-N/19 format
    m = re.match(r'^GL[-]?([A-N])[/](\d+)', val)
    if m:
        result.grid_row_min = result.grid_row_max = m.group(1)
        result.grid_col_min = result.grid_col_max = float(m.group(2))
        result.grid_type = 'POINT'
        return result

    # Letter+Col with trailing text: "A32 CANOPY", "N1 PASSAGE"
    m = re.match(r'^([A-N])(\d+)\s+[A-Z]', val)
    if m:
        result.grid_row_min = result.grid_row_max = m.group(1)
        result.grid_col_min = result.grid_col_max = float(m.group(2))
        result.grid_type = 'POINT'
        return result

    # Letter+Col with comma description: "F23, Door Frames"
    m = re.match(r'^([A-N])(\d+),', val)
    if m:
        result.grid_row_min = result.grid_row_max = m.group(1)
        result.grid_col_min = result.grid_col_max = float(m.group(2))
        result.grid_type = 'POINT'
        return result

    # Letter+Col with description: "K18 (Vestibule 3)"
    m = re.match(r'^([A-N])(\d+)\s*\(', val)
    if m:
        result.grid_row_min = result.grid_row_max = m.group(1)
        result.grid_col_min = result.grid_col_max = float(m.group(2))
        result.grid_type = 'POINT'
        return result

    # K2.5 suffix: K2.5 SUW
    m = re.match(r'^([A-N])(\d+\.?\d*)\s+(SUW|SUE|FAB)', val)
    if m:
        result.grid_row_min = result.grid_row_max = m.group(1)
        result.grid_col_min = result.grid_col_max = float(m.group(2))
        result.grid_type = 'POINT'
        return result

    # =========================================================================
    # ROW-ONLY PATTERNS
    # =========================================================================

    # Single letter: G, D, B
    m = re.match(r'^([A-N])$', val)
    if m:
        result.grid_row_min = result.grid_row_max = m.group(1)
        result.grid_type = 'ROW_ONLY'
        return result

    # GL-N or GL N (gridline row letter)
    m = re.match(r'^GL[-\s]?([A-N])(?:\s|$)', val)
    if m:
        result.grid_row_min = result.grid_row_max = m.group(1)
        result.grid_type = 'ROW_ONLY'
        return result

    # Letter/letter: G/H, D/K (row range)
    m = re.match(r'^([A-N])[/&]([A-N])$', val)
    if m:
        rows = _sort_rows(m.group(1), m.group(2))
        result.grid_row_min, result.grid_row_max = rows
        result.grid_type = 'ROW_ONLY'
        return result

    # G/J-LINE → rows G-J
    m = re.match(r'^([A-N])/([A-N])[-\s]*LINE', val)
    if m:
        rows = _sort_rows(m.group(1), m.group(2))
        result.grid_row_min, result.grid_row_max = rows
        result.grid_type = 'ROW_ONLY'
        return result

    # K/D COLUMNS or K/D description
    m = re.match(r'^([A-N])[/]([A-N])\s+[A-Z]', val)
    if m:
        rows = _sort_rows(m.group(1), m.group(2))
        result.grid_row_min, result.grid_row_max = rows
        result.grid_type = 'ROW_ONLY'
        return result

    # =========================================================================
    # COLUMN-ONLY PATTERNS
    # =========================================================================

    # Just a number: "33"
    m = re.match(r'^(\d+)$', val)
    if m:
        result.grid_col_min = result.grid_col_max = float(m.group(1))
        result.grid_type = 'COL_ONLY'
        return result

    # GL-33 (gridline column)
    m = re.match(r'^GL[-]?(\d+)$', val)
    if m:
        result.grid_col_min = result.grid_col_max = float(m.group(1))
        result.grid_type = 'COL_ONLY'
        return result

    # Column range: 29-30
    m = re.match(r'^(\d+)[-–](\d+)$', val)
    if m:
        cols = _sort_cols(float(m.group(1)), float(m.group(2)))
        result.grid_col_min, result.grid_col_max = cols
        result.grid_type = 'COL_ONLY'
        return result

    # Parenthesized col range: (3-33)
    m = re.match(r'^\((\d+)[-–](\d+)\)$', val)
    if m:
        cols = _sort_cols(float(m.group(1)), float(m.group(2)))
        result.grid_col_min, result.grid_col_max = cols
        result.grid_type = 'COL_ONLY'
        return result

    # CL column range: CL 5-12
    m = re.match(r'^CL\s+(\d+)[-–](\d+)', val)
    if m:
        cols = _sort_cols(float(m.group(1)), float(m.group(2)))
        result.grid_col_min, result.grid_col_max = cols
        result.grid_type = 'COL_ONLY'
        return result

    # CL single column: CL 11
    m = re.match(r'^CL\s+(\d+)$', val)
    if m:
        result.grid_col_min = result.grid_col_max = float(m.group(1))
        result.grid_type = 'COL_ONLY'
        return result

    # COL prefix: COL 11
    m = re.match(r'^COL\s+(\d+)', val)
    if m:
        result.grid_col_min = result.grid_col_max = float(m.group(1))
        result.grid_type = 'COL_ONLY'
        return result

    # FAB with spaced dash: fab 3 - 25 → cols 3-25
    m = re.match(r'^FAB\s+(\d+)\s*[-–]\s*(\d+)', val)
    if m:
        cols = _sort_cols(float(m.group(1)), float(m.group(2)))
        result.grid_col_min, result.grid_col_max = cols
        result.grid_type = 'COL_ONLY'
        return result

    # "16-20 description" → col range with description
    m = re.match(r'^(\d+)[-–](\d+)\s+[A-Z]', val)
    if m:
        cols = _sort_cols(float(m.group(1)), float(m.group(2)))
        result.grid_col_min, result.grid_col_max = cols
        result.grid_type = 'COL_ONLY'
        return result

    # L19 CJ, L28 (Line + description)
    m = re.match(r'^L(\d+)\s+[A-Z]', val)
    if m:
        result.grid_col_min = result.grid_col_max = float(m.group(1))
        result.grid_type = 'COL_ONLY'
        return result

    # =========================================================================
    # EQUIPMENT/NAMED PATTERNS
    # =========================================================================

    # CE##P patterns: CE8P, CE22P → equipment with column hint
    m = re.match(r'^CE(\d+)P', val)
    if m:
        result.grid_col_min = result.grid_col_max = float(m.group(1))
        result.grid_type = 'COL_ONLY'  # Has useful column info
        return result

    # FE## patterns: FE15
    m = re.match(r'^FE(\d+)$', val)
    if m:
        result.grid_col_min = result.grid_col_max = float(m.group(1))
        result.grid_type = 'COL_ONLY'
        return result

    # No pattern matched
    return result


def parse_grid_to_dict(value: str) -> Dict[str, Any]:
    """
    Parse grid string and return as dictionary.

    Convenience function for use in DataFrame operations.

    Args:
        value: Raw grid string

    Returns:
        Dictionary with grid_row_min, grid_row_max, grid_col_min, grid_col_max, grid_type, grid_raw
    """
    return parse_grid(value).to_dict()
