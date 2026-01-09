"""
Gridline Mapping Module

Loads FAB code to gridline mapping from Excel and provides lookup functions
for translating room/elevator/stair codes to gridline coordinates.

Grid system:
- Rows: A-E (east side), J-M (west side)
- Columns: 2-32 (numeric)
- Building mapping: SUE → A-E, SUW → J-M, FAB → depends on location
"""

import os
import re
import sys
from functools import lru_cache
from pathlib import Path
from typing import Optional

import pandas as pd

# Add project root to path for Settings import
_project_root = Path(__file__).parent.parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.config.settings import Settings


# Default path to gridline mapping file (using Settings for WSL path conversion)
DEFAULT_MAPPING_PATH = Settings.RAW_DATA_DIR / 'location_mappings' / 'Samsung_FAB_Codes_by_Gridline_3.xlsx'

# Note: Building does NOT determine gridline rows/columns.
# Each room has specific gridline coordinates that must be looked up from drawings.
# Building-to-row inference was removed as it's misleading (a building spanning rows A-M
# doesn't mean every room inside spans A-M).

# Site gridline system reference:
# - Columns: 1 through 34 (plus 32.5)
# - Rows: A through N (plus fractional: A.5, B.5, E.3, E.5, E.8, F.2, F.4, F.6, F.8,
#         G.3, G.5, G.8, H.3, H.5, H.8, L.5, M.5)
# GRIDLINE location types span the full row range (A-N) at a specific column.
GRIDLINE_ROW_MIN = 'A'
GRIDLINE_ROW_MAX = 'N'

# Row ordering for min/max comparison
ROW_ORDER = {'A': 1, 'B': 2, 'C': 3, 'D': 4, 'E': 5, 'F': 6, 'G': 7, 'H': 8, 'J': 9, 'K': 10, 'L': 11, 'M': 12, 'N': 13}


class GridlineMapping:
    """
    Loads and manages gridline coordinate lookup from FAB code mapping.

    Provides fast lookup of gridline bounds (row_min, row_max, col_min, col_max)
    for rooms, elevators, and stairs.
    """

    def __init__(self, mapping_path: Path | str = None):
        """
        Initialize gridline mapping from Excel file.

        Args:
            mapping_path: Path to Excel mapping file. Uses default if not specified.
        """
        self.mapping_path = Path(mapping_path) if mapping_path else DEFAULT_MAPPING_PATH
        self._lookup: dict[str, dict] = {}
        self._loaded = False

    def _load(self):
        """Load and index the mapping file."""
        if self._loaded:
            return

        if not self.mapping_path.exists():
            raise FileNotFoundError(f"Gridline mapping file not found: {self.mapping_path}")

        df = pd.read_excel(self.mapping_path, sheet_name='All Gridlines')

        # Build bounds table for each FAB Code
        bounds = df.groupby('FAB Code').agg({
            'Row': ['min', 'max'],
            'Column': ['min', 'max'],
            'Floor': 'first',
            'Room Name': 'first'
        }).reset_index()
        bounds.columns = ['FAB_Code', 'Row_Min', 'Row_Max', 'Col_Min', 'Col_Max', 'Floor', 'Room_Name']

        # Create lookup dictionary keyed by FAB Code (and normalized versions)
        for _, row in bounds.iterrows():
            fab_code = str(row['FAB_Code'])
            entry = {
                'row_min': row['Row_Min'],
                'row_max': row['Row_Max'],
                'col_min': int(row['Col_Min']),
                'col_max': int(row['Col_Max']),
                'floor': row['Floor'],
                'room_name': row['Room_Name'],
            }

            # Store under original key
            self._lookup[fab_code] = entry

            # Also store under normalized key (lowercase)
            self._lookup[fab_code.lower()] = entry

        self._loaded = True

    @property
    def lookup(self) -> dict:
        """Get the lookup dictionary, loading if necessary."""
        if not self._loaded:
            self._load()
        return self._lookup

    def get_bounds(self, fab_code: str) -> Optional[dict]:
        """
        Get gridline bounds for a FAB code.

        Args:
            fab_code: FAB code like 'FAB112345', 'FAB1-EL01', 'FAB1-ST05'

        Returns:
            Dict with row_min, row_max, col_min, col_max or None if not found
        """
        if not fab_code:
            return None
        return self.lookup.get(str(fab_code)) or self.lookup.get(str(fab_code).lower())


def normalize_elevator_code(code: str) -> Optional[str]:
    """
    Normalize taxonomy elevator code to mapping format.

    Taxonomy format: ELV-01, ELV-A, ELV-18
    Mapping format: FAB1-EL01, FAB1-EL01A, FAB1-EL18

    Args:
        code: Elevator code from taxonomy (ELV-XX format)

    Returns:
        Normalized FAB code (FAB1-ELXX format) or None if can't normalize
    """
    if not code:
        return None

    code = str(code).upper()

    # Match ELV-XX pattern (numeric or letter suffix)
    match = re.match(r'ELV-(\d+|[A-Z])([A-Z])?$', code)
    if match:
        num = match.group(1)
        suffix = match.group(2) or ''
        # Pad single-digit numbers with leading zero
        if num.isdigit():
            num = num.zfill(2)
        return f'FAB1-EL{num}{suffix}'

    return None


def normalize_stair_code(code: str) -> Optional[str]:
    """
    Normalize taxonomy stair code to mapping format.

    Taxonomy format: STR-01, STR-A, STR-50
    Mapping format: FAB1-ST01, FAB1-ST50

    Note: Many stair codes in taxonomy (STR-23+) are not in mapping (only 1-5, 18-22, 50)

    Args:
        code: Stair code from taxonomy (STR-XX format)

    Returns:
        Normalized FAB code (FAB1-STXX format) or None if can't normalize
    """
    if not code:
        return None

    code = str(code).upper()

    # Match STR-XX pattern (numeric or letter suffix)
    match = re.match(r'STR-(\d+|[A-Z])$', code)
    if match:
        num = match.group(1)
        # Pad single-digit numbers with leading zero
        if num.isdigit():
            num = num.zfill(2)
        return f'FAB1-ST{num}'

    return None


def normalize_room_code(code: str) -> Optional[str]:
    """
    Normalize room code for mapping lookup.

    Taxonomy format: FAB112345 (8-digit FAB1XXXXX)
    Mapping format: FAB112345 (same format, may have variations)

    Args:
        code: Room code from taxonomy

    Returns:
        Normalized FAB code or None if invalid format
    """
    if not code:
        return None

    code = str(code).upper()

    # Match FAB1XXXXX pattern (5 digits after FAB1)
    if re.match(r'^FAB1\d{5}$', code):
        return code

    return None


def get_row_range_for_building(building: str) -> tuple[None, None]:
    """
    Building does NOT determine gridline rows.

    This function always returns (None, None) because knowing which building
    a room is in does not tell you its specific gridline coordinates.
    Each room's gridlines must be looked up from drawings.

    Args:
        building: Building code (unused)

    Returns:
        Always (None, None) - building cannot infer gridlines
    """
    return (None, None)


def get_gridline_bounds(
    location_type: str,
    location_code: str,
    building: str = None,
    mapping: GridlineMapping = None
) -> dict:
    """
    Get gridline bounds for any location type.

    Handles:
    - ROOM: Lookup FAB code directly in mapping
    - ELEVATOR: Normalize ELV-XX → FAB1-ELXX and lookup
    - STAIR: Normalize STR-XX → FAB1-STXX and lookup
    - GRIDLINE: Use column from location_code (row requires mapping lookup)
    - LEVEL/BUILDING/AREA: No gridline inference possible without mapping

    Note: Building does NOT determine gridlines. Each location's coordinates
    must come from the mapping file (which is populated from drawings).

    Args:
        location_type: Type of location (ROOM, ELEVATOR, STAIR, GRIDLINE, etc.)
        location_code: The location code/identifier
        building: Building code (not used for inference, kept for API compatibility)
        mapping: GridlineMapping instance (creates default if None)

    Returns:
        Dict with row_min, row_max, col_min, col_max (all may be None if not in mapping)
    """
    result = {
        'grid_row_min': None,
        'grid_row_max': None,
        'grid_col_min': None,
        'grid_col_max': None,
    }

    if not location_type or not location_code:
        return result

    # Use default mapping if not provided
    if mapping is None:
        mapping = get_default_mapping()

    # Handle each location type
    if location_type == 'ROOM':
        fab_code = normalize_room_code(location_code)
        if fab_code:
            bounds = mapping.get_bounds(fab_code)
            if bounds:
                result['grid_row_min'] = bounds['row_min']
                result['grid_row_max'] = bounds['row_max']
                result['grid_col_min'] = bounds['col_min']
                result['grid_col_max'] = bounds['col_max']
                return result

    elif location_type == 'ELEVATOR':
        fab_code = normalize_elevator_code(location_code)
        if fab_code:
            bounds = mapping.get_bounds(fab_code)
            if bounds:
                result['grid_row_min'] = bounds['row_min']
                result['grid_row_max'] = bounds['row_max']
                result['grid_col_min'] = bounds['col_min']
                result['grid_col_max'] = bounds['col_max']
                return result

    elif location_type == 'STAIR':
        fab_code = normalize_stair_code(location_code)
        if fab_code:
            bounds = mapping.get_bounds(fab_code)
            if bounds:
                result['grid_row_min'] = bounds['row_min']
                result['grid_row_max'] = bounds['row_max']
                result['grid_col_min'] = bounds['col_min']
                result['grid_col_max'] = bounds['col_max']
                return result

    elif location_type == 'GRIDLINE':
        # GRIDLINE tasks span the full row range (A-N) at a specific column
        # The location_code is the column number
        try:
            col = float(location_code)  # float to handle 32.5
            result['grid_col_min'] = col
            result['grid_col_max'] = col
            result['grid_row_min'] = GRIDLINE_ROW_MIN  # A
            result['grid_row_max'] = GRIDLINE_ROW_MAX  # N
        except (ValueError, TypeError):
            pass

    # No building-based inference - building doesn't determine gridlines
    return result


# Module-level cached mapping instance
_default_mapping: Optional[GridlineMapping] = None


def get_default_mapping() -> GridlineMapping:
    """Get or create the default gridline mapping instance."""
    global _default_mapping
    if _default_mapping is None:
        _default_mapping = GridlineMapping()
    return _default_mapping


def reset_default_mapping():
    """Reset the cached default mapping (useful for testing)."""
    global _default_mapping
    _default_mapping = None
