"""
Location Model - High-Level Location Lookups

Provides the core API for location-based data integration across all data sources.
Wraps gridline_mapping.py with higher-level functions for common use cases.

Key Functions:
    get_grid_bounds(location_code) - Get grid bounds for any room/elevator/stair
    get_locations_at_grid(row, col) - Find all locations containing a grid point
    location_contains_grid(location_code, row, col) - Check if location contains grid
    parse_grid_string(grid_str) - Parse grid strings like "G/10" or "B.5/22"

Usage:
    from location_model import get_grid_bounds, get_locations_at_grid

    # Forward lookup: Room -> Grid bounds
    bounds = get_grid_bounds('FAB112345')
    # {'row_min': 'B', 'row_max': 'E', 'col_min': 5, 'col_max': 12}

    # Reverse lookup: Grid coordinate -> Rooms
    locations = get_locations_at_grid('G', 10)
    # ['FAB112345', 'FAB112346', ...]
"""

import re
import sys
from pathlib import Path
from typing import Optional, List, Tuple

# Import gridline mapping from same directory
from gridline_mapping import (
    GridlineMapping,
    get_default_mapping,
    get_gridline_bounds,
    normalize_room_code,
    normalize_elevator_code,
    normalize_stair_code,
    ROW_ORDER,
    GRIDLINE_ROW_MIN,
    GRIDLINE_ROW_MAX,
)


def get_grid_bounds(
    location_code: str,
    location_type: str = None,
    mapping: GridlineMapping = None
) -> Optional[dict]:
    """
    Get grid bounds for a location code.

    Automatically detects location type from code format if not provided.

    Args:
        location_code: Room/elevator/stair code (FAB112345, ELV-01, STR-05)
        location_type: Optional type hint (ROOM, ELEVATOR, STAIR)
        mapping: Optional GridlineMapping instance

    Returns:
        Dict with row_min, row_max, col_min, col_max or None if not found
    """
    if not location_code:
        return None

    code = str(location_code).upper()

    # Auto-detect location type if not provided
    if location_type is None:
        if code.startswith('FAB1') and len(code) == 9 and code[4:].isdigit():
            location_type = 'ROOM'
        elif code.startswith('ELV-'):
            location_type = 'ELEVATOR'
        elif code.startswith('STR-'):
            location_type = 'STAIR'
        else:
            # Try as room code
            location_type = 'ROOM'

    bounds = get_gridline_bounds(location_type, location_code, mapping=mapping)

    # Return None if no bounds found
    if bounds['grid_row_min'] is None:
        return None

    return {
        'row_min': bounds['grid_row_min'],
        'row_max': bounds['grid_row_max'],
        'col_min': bounds['grid_col_min'],
        'col_max': bounds['grid_col_max'],
    }


def parse_grid_string(grid_str: str) -> Optional[Tuple[str, float]]:
    """
    Parse a grid string into (row, column) tuple.

    Handles formats:
    - "G/10" -> ('G', 10.0)
    - "B.5/22" -> ('B.5', 22.0)
    - "A/32.5" -> ('A', 32.5)
    - "G-10" -> ('G', 10.0)
    - "G10" -> ('G', 10.0)

    Args:
        grid_str: Grid string in various formats

    Returns:
        Tuple of (row_letter, column_number) or None if unparseable
    """
    if not grid_str:
        return None

    grid_str = str(grid_str).strip().upper()

    # Pattern: Row (letter with optional .digit) / or - Column (number with optional .digit)
    patterns = [
        r'^([A-N](?:\.\d+)?)[/\-](\d+(?:\.\d+)?)$',  # G/10, B.5/22, A-32.5
        r'^([A-N](?:\.\d+)?)(\d+(?:\.\d+)?)$',  # G10 (no separator)
    ]

    for pattern in patterns:
        match = re.match(pattern, grid_str)
        if match:
            row = match.group(1)
            col = float(match.group(2))
            return (row, col)

    return None


def _row_in_range(row: str, row_min: str, row_max: str) -> bool:
    """Check if row is within range (inclusive), handling fractional rows."""
    # Extract base letter for comparison
    def get_base_letter(r):
        return r[0] if r else None

    base_row = get_base_letter(row)
    base_min = get_base_letter(row_min)
    base_max = get_base_letter(row_max)

    if base_row is None or base_min is None or base_max is None:
        return False

    min_order = ROW_ORDER.get(base_min, 0)
    max_order = ROW_ORDER.get(base_max, 0)
    row_order = ROW_ORDER.get(base_row, 0)

    return min_order <= row_order <= max_order


def location_contains_grid(
    location_code: str,
    row: str,
    col: float,
    location_type: str = None,
    mapping: GridlineMapping = None
) -> bool:
    """
    Check if a location's grid bounds contain a specific grid coordinate.

    Args:
        location_code: Room/elevator/stair code
        row: Grid row letter (A-N, may include fractional like B.5)
        col: Grid column number
        location_type: Optional type hint
        mapping: Optional GridlineMapping instance

    Returns:
        True if the grid coordinate falls within the location's bounds
    """
    bounds = get_grid_bounds(location_code, location_type, mapping)
    if bounds is None:
        return False

    # Check column bounds
    if col < bounds['col_min'] or col > bounds['col_max']:
        return False

    # Check row bounds
    return _row_in_range(row, bounds['row_min'], bounds['row_max'])


def get_locations_at_grid(
    row: str,
    col: float,
    mapping: GridlineMapping = None
) -> List[str]:
    """
    Find all locations whose grid bounds contain a specific coordinate.

    This is the reverse lookup: given a grid point, find which rooms contain it.

    Args:
        row: Grid row letter (A-N)
        col: Grid column number

    Returns:
        List of location codes (FAB codes) that contain this grid point
    """
    if mapping is None:
        mapping = get_default_mapping()

    matches = []

    for fab_code, bounds in mapping.lookup.items():
        # Skip lowercase duplicates
        if fab_code != fab_code.upper():
            continue

        # Check column bounds
        if col < bounds['col_min'] or col > bounds['col_max']:
            continue

        # Check row bounds
        if _row_in_range(row, bounds['row_min'], bounds['row_max']):
            matches.append(fab_code)

    return matches


def get_location_info(location_code: str, mapping: GridlineMapping = None) -> Optional[dict]:
    """
    Get full location information including grid bounds and metadata.

    Args:
        location_code: Room/elevator/stair code

    Returns:
        Dict with code, type, grid bounds, floor, room_name
    """
    if mapping is None:
        mapping = get_default_mapping()

    code = str(location_code).upper()

    # Try to find in mapping
    entry = mapping.get_bounds(code)
    if entry is None:
        # Try normalizing
        for normalizer, loc_type in [
            (normalize_room_code, 'ROOM'),
            (normalize_elevator_code, 'ELEVATOR'),
            (normalize_stair_code, 'STAIR'),
        ]:
            normalized = normalizer(code)
            if normalized:
                entry = mapping.get_bounds(normalized)
                if entry:
                    code = normalized
                    break

    if entry is None:
        return None

    return {
        'location_code': code,
        'row_min': entry['row_min'],
        'row_max': entry['row_max'],
        'col_min': entry['col_min'],
        'col_max': entry['col_max'],
        'floor': entry.get('floor'),
        'room_name': entry.get('room_name'),
    }
