"""
Centralized Location Enrichment

This module is the SINGLE SOURCE OF TRUTH for location enrichment logic.
It consolidates duplicated code from RABA, PSI, TBM, QC Workbooks consolidation scripts.

================================================================================
APPROACH: Instance-Level Grid Bounds vs Dimension Table Grid Bounds
================================================================================

The grid coordinate system (rows A-N, columns 1-33) is used to spatially locate
work activities. Grid bounds can come from TWO sources:

1. INSTANCE LEVEL (grid_source='RECORD')
   - Grid coordinates extracted directly from the source record
   - Examples: RABA grid field "G/10", TBM grid_raw "B-D/8-12", P6 task name "GL 33 - D-F"
   - These represent the ACTUAL location where work occurred
   - May be more specific than dim_location (e.g., "GL 33 - D-F" is rows D-F only)

2. DIMENSION TABLE (grid_source='DIM_LOCATION')
   - Grid bounds looked up from dim_location based on room/stair/elevator code
   - Used when a specific location code is provided (e.g., FAB116201, STR-21)
   - Represents the FULL extent of that location in the grid system
   - Useful for joining but may be less precise than instance-level

WHY THIS MATTERS:
- A gridline in dim_location spans the FULL row range (A-N) by default
- But a P6 task like "GL 33 - D-F" only affects rows D-F on gridline 33
- The instance-level bounds enable more precise spatial matching
- The grid_source field tells analysts which source was used

PRIORITY ORDER:
1. If source record has grid coordinates → use them (grid_source='RECORD')
2. If room_code provided → look up from dim_location (grid_source='DIM_LOCATION')
3. Otherwise → no grid bounds (grid_source='NONE')

================================================================================
Usage
================================================================================

    from scripts.integrated_analysis.location import enrich_location

    result = enrich_location(
        building='FAB',
        level='2F',
        grid='G/10-12',      # Instance-level grid (takes priority)
        room_code=None,       # Or provide room code for dim_location lookup
        source='RABA'
    )

    # Use results directly
    record['dim_location_id'] = result.dim_location_id
    record['location_type'] = result.location_type
    record['affected_rooms'] = result.affected_rooms
    record['grid_source'] = result.grid_source  # 'RECORD', 'DIM_LOCATION', or 'NONE'

================================================================================
Output Schema
================================================================================

    - dim_location_id: FK to dim_location (always populated)
    - location_type: What location info we HAVE (not what we inferred):
        - ROOM/STAIR/ELEVATOR: Direct room code found (from text or room_code param)
        - GRIDLINE: Only grid coordinates available (rooms inferred via affected_rooms)
        - LEVEL: Only building + level available
        - BUILDING: Only building available
        - UNDEFINED: No usable location info
    - location_code: Matched code (e.g., FAB112345, STR-21, G/10)
    - level: Normalized level (1F, 2F, ROOF, etc.)
    - grid_row_min, grid_row_max: Grid row bounds (letters A-N)
    - grid_col_min, grid_col_max: Grid column bounds (numbers 1-33)
    - grid_source: Where grid bounds came from ('RECORD', 'DIM_LOCATION', 'NONE')
    - affected_rooms: JSON array of rooms with grid overlap
    - affected_rooms_count: Integer count (1 = single room match, use for filtering)
    - match_type: How the location was determined (see below)

Match Types:
    - ROOM_DIRECT: Room/stair/elevator code found and matched in dim_location
    - GRID_SINGLE: Grid coordinates overlap exactly one room (see affected_rooms)
    - GRID_MULTI: Grid coordinates overlap multiple rooms (see affected_rooms)
    - GRIDLINE: Grid coordinates but no room overlap found
    - LEVEL: Building + level fallback (no grid info)
    - BUILDING: Project-wide FAB1 fallback
    - UNDEFINED: No location could be determined
"""

import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, Any

import pandas as pd

# Add project root to path
_project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from scripts.integrated_analysis.location.core.normalizers import (
    normalize_level,
    normalize_building,
)
from scripts.integrated_analysis.location.core.pattern_extractor import (
    extract_location_codes_for_lookup,
)
from scripts.shared.dimension_lookup import (
    get_location_id,
    get_location_id_by_code,
    get_affected_rooms,
    get_location_by_code,
    parse_grid_field,
    reset_cache,
)


@dataclass
class LocationEnrichmentResult:
    """
    Result of enriching a record with location data.

    Simplified schema focused on:
    - Room/location matching
    - Grid bounds (4 columns)
    - Affected rooms from grid overlap
    """
    # Dimension key
    dim_location_id: Optional[int] = None

    # Matched location info
    location_type: Optional[str] = None  # ROOM, STAIR, ELEVATOR, GRIDLINE, LEVEL, BUILDING, UNDEFINED
    location_code: Optional[str] = None  # e.g., FAB112345, STR-21, ELV-01

    # Level (always populated if known)
    level: Optional[str] = None  # Normalized: 1F, 2F, ROOF, B1, etc.

    # Grid bounds (populated from grid string OR inferred from room)
    grid_row_min: Optional[str] = None
    grid_row_max: Optional[str] = None
    grid_col_min: Optional[float] = None
    grid_col_max: Optional[float] = None

    # Grid source - where the grid bounds came from
    # RECORD: From the source record's grid field or task name
    # DIM_LOCATION: Inferred from dim_location (room/stair/elevator lookup)
    # NONE: No grid bounds available
    grid_source: Optional[str] = None

    # Affected rooms (from grid overlap)
    affected_rooms: Optional[str] = None  # JSON array
    affected_rooms_count: int = 0

    # Match type - how location was determined
    match_type: Optional[str] = None  # ROOM_DIRECT, GRID_SINGLE, GRID_MULTI, GRIDLINE, LEVEL, BUILDING, UNDEFINED

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for easy DataFrame assignment."""
        return {
            'dim_location_id': self.dim_location_id,
            'location_type': self.location_type,
            'location_code': self.location_code,
            'level': self.level,
            'grid_row_min': self.grid_row_min,
            'grid_row_max': self.grid_row_max,
            'grid_col_min': self.grid_col_min,
            'grid_col_max': self.grid_col_max,
            'grid_source': self.grid_source,
            'affected_rooms': self.affected_rooms,
            'affected_rooms_count': self.affected_rooms_count,
            'match_type': self.match_type,
        }


def _get_location_with_grid(location_code: str) -> Optional[Dict]:
    """
    Get location details including grid bounds from dim_location.

    Used to infer grid bounds when a room code is provided directly.
    """
    return get_location_by_code(location_code)


def _lookup_undefined_id() -> Optional[int]:
    """Get location_id for UNDEFINED fallback."""
    return get_location_id_by_code('UNDEFINED')


def _lookup_fab1_id() -> Optional[int]:
    """Get location_id for FAB1 (project-wide building) fallback."""
    return get_location_id_by_code('FAB1')


def enrich_location(
    building: Optional[str] = None,
    level: Optional[str] = None,
    grid: Optional[str] = None,
    room_code: Optional[str] = None,
    location_text: Optional[str] = None,
    source: str = 'UNKNOWN',
) -> LocationEnrichmentResult:
    """
    Enrich a record with location information.

    This is the SINGLE FUNCTION for all location enrichment across fact tables
    (RABA, PSI, TBM, QC Workbooks).

    Grid Bounds Strategy:
    ---------------------
    Grid bounds can come from two sources, tracked via grid_source field:

    1. RECORD (instance-level): When the source record contains grid coordinates
       (e.g., RABA grid="G/10", TBM grid_raw="B-D/8-12"). These are the ACTUAL
       coordinates where work occurred and take priority.

    2. DIM_LOCATION (dimension table): When a room_code is provided, grid bounds
       are looked up from dim_location. This represents the FULL extent of that
       room/stair/elevator in the grid system.

    Why this matters: A gridline in dim_location defaults to the full A-N row span,
    but an instance like "GL 33 - D-F" only affects rows D-F. Using instance-level
    bounds enables more precise spatial matching for affected_rooms calculation.

    Priority order for location matching:
    -------------------------------------
    1. ROOM_DIRECT: Direct room/stair/elevator code found (location_type=ROOM/STAIR/ELEVATOR)
    2. GRID_SINGLE: Grid coordinates overlap one room (location_type=GRIDLINE, affected_rooms_count=1)
    3. GRID_MULTI: Grid coordinates overlap multiple rooms (location_type=GRIDLINE, affected_rooms_count>1)
    4. GRIDLINE: Grid provided but no room overlap found (location_type=GRIDLINE)
    5. LEVEL: Building + level fallback (location_type=LEVEL)
    6. BUILDING: FAB1 (project-wide) fallback (location_type=BUILDING)
    7. UNDEFINED: Final fallback (location_type=UNDEFINED)

    Args:
        building: Raw building name (e.g., "FAB", "SUE", "Main FAB")
        level: Raw level value (e.g., "1F", "2nd Floor", "Basement")
        grid: Raw grid coordinates (e.g., "G/10", "B-D/8-12", "F.5/17")
               Supports formats: single coord "G/10", ranges "B-D/8-12",
               fractional "F.5/17", TBM format "C/11 - C/22"
        room_code: Extracted room code if available (e.g., "FAB116201", "STR-21")
                   When provided and found, grid bounds come from dim_location
        location_text: Free text containing location info (e.g., location_raw, summary)
                       Pattern extraction will find room codes like "FAB116109A",
                       "Stair 21", "Elevator 22" for ROOM_DIRECT matching
        source: Source identifier for diagnostics (e.g., "RABA", "PSI", "TBM")

    Returns:
        LocationEnrichmentResult with:
        - dim_location_id: FK to dim_location (always populated)
        - location_type, location_code: What was matched
        - grid_row_min/max, grid_col_min/max: Grid bounds
        - grid_source: 'RECORD', 'DIM_LOCATION', or 'NONE'
        - affected_rooms: JSON array of overlapping rooms
        - match_type: How location was determined
    """
    result = LocationEnrichmentResult()

    # Step 1: Normalize building and level
    building_normalized = normalize_building(building)
    result.level = normalize_level(level)

    # Step 2: Try direct room/stair/elevator code lookup
    if room_code and pd.notna(room_code):
        room_code = str(room_code).strip().upper()
        loc_info = _get_location_with_grid(room_code)

        if loc_info:
            result.dim_location_id = loc_info['location_id']
            result.location_type = loc_info['location_type']
            result.location_code = loc_info['location_code']
            result.match_type = 'ROOM_DIRECT'

            # Get grid bounds from dim_location (infer from room)
            if loc_info.get('grid_row_min') and pd.notna(loc_info.get('grid_row_min')):
                result.grid_row_min = loc_info['grid_row_min']
                result.grid_row_max = loc_info['grid_row_max']
                result.grid_col_min = loc_info['grid_col_min']
                result.grid_col_max = loc_info['grid_col_max']
                result.grid_source = 'DIM_LOCATION'
            else:
                result.grid_source = 'NONE'

            # Use level from dim_location if not provided
            if not result.level and loc_info.get('level'):
                result.level = loc_info['level']

            return result

    # Step 2b: Extract room/stair/elevator codes from free text
    # This enables ROOM_DIRECT matching for RABA/PSI which have location_raw/summary
    # Returns multiple possible codes (e.g., FAB116109A, FAB116109) to try
    if location_text and pd.notna(location_text):
        extracted_codes = extract_location_codes_for_lookup(str(location_text))

        for extracted_code, extracted_type in extracted_codes:
            loc_info = _get_location_with_grid(extracted_code)

            if loc_info:
                result.dim_location_id = loc_info['location_id']
                result.location_type = loc_info['location_type']
                result.location_code = loc_info['location_code']
                result.match_type = 'ROOM_DIRECT'

                # Get grid bounds from dim_location
                if loc_info.get('grid_row_min') and pd.notna(loc_info.get('grid_row_min')):
                    result.grid_row_min = loc_info['grid_row_min']
                    result.grid_row_max = loc_info['grid_row_max']
                    result.grid_col_min = loc_info['grid_col_min']
                    result.grid_col_max = loc_info['grid_col_max']
                    result.grid_source = 'DIM_LOCATION'
                else:
                    result.grid_source = 'NONE'

                # Use level from dim_location if not provided
                if not result.level and loc_info.get('level'):
                    result.level = loc_info['level']

                return result

    # Step 3: Parse grid coordinates if provided
    grid_parsed = parse_grid_field(grid)

    if grid_parsed.get('grid_row_min') is not None:
        result.grid_row_min = grid_parsed['grid_row_min']
        result.grid_row_max = grid_parsed['grid_row_max']
    if grid_parsed.get('grid_col_min') is not None:
        result.grid_col_min = grid_parsed['grid_col_min']
        result.grid_col_max = grid_parsed['grid_col_max']

    has_row = result.grid_row_min is not None
    has_col = result.grid_col_min is not None

    # Track grid source
    if has_row or has_col:
        result.grid_source = 'RECORD'
    else:
        result.grid_source = 'NONE'

    # Step 4: Find affected rooms via grid overlap
    if result.level and (has_row or has_col):
        rooms = get_affected_rooms(
            result.level,
            result.grid_row_min if has_row else None,
            result.grid_row_max if has_row else None,
            result.grid_col_min if has_col else None,
            result.grid_col_max if has_col else None,
        )

        if rooms:
            result.affected_rooms = json.dumps(rooms)
            result.affected_rooms_count = len(rooms)

            # Use first room as the location match
            first_room = rooms[0]
            result.dim_location_id = first_room['location_id']
            result.location_code = first_room['location_code']

            # location_type reflects what info we HAVE (grid coordinates = GRIDLINE)
            # NOT what we inferred (room from grid overlap)
            # The affected_rooms array shows which rooms the grid overlaps
            result.location_type = 'GRIDLINE'

            if len(rooms) == 1:
                result.match_type = 'GRID_SINGLE'  # Single room inferred from grid
            else:
                result.match_type = 'GRID_MULTI'   # Multiple rooms inferred from grid

            return result

    # Step 5: Level fallback (building + level)
    if building_normalized and result.level:
        location_id = get_location_id(building_normalized, result.level, allow_fallback=False)
        if location_id:
            result.dim_location_id = location_id
            result.location_type = 'LEVEL'
            result.location_code = result.level
            result.match_type = 'LEVEL'
            return result

    # Step 6: Building fallback (FAB1)
    if building_normalized:
        fab1_id = _lookup_fab1_id()
        if fab1_id:
            result.dim_location_id = fab1_id
            result.location_type = 'BUILDING'
            result.location_code = 'FAB1'
            result.match_type = 'BUILDING'
            return result

    # Step 7: UNDEFINED fallback
    undefined_id = _lookup_undefined_id()
    if undefined_id:
        result.dim_location_id = undefined_id
        result.location_type = 'UNDEFINED'
        result.location_code = 'UNDEFINED'
        result.match_type = 'UNDEFINED'

    return result


def enrich_location_row(
    row: pd.Series,
    building_col: str = 'building',
    level_col: str = 'level',
    grid_col: str = 'grid',
    room_code_col: Optional[str] = None,
    source: str = 'UNKNOWN',
) -> Dict[str, Any]:
    """
    Enrich a DataFrame row with location information.

    Convenience function for use with DataFrame.apply().

    Args:
        row: DataFrame row
        building_col: Column name for building
        level_col: Column name for level
        grid_col: Column name for grid coordinates
        room_code_col: Column name for room code (optional)
        source: Source identifier

    Returns:
        Dict with all location enrichment fields

    Example:
        enriched = df.apply(
            lambda row: enrich_location_row(row, source='RABA'),
            axis=1
        ).apply(pd.Series)
        df = pd.concat([df, enriched], axis=1)
    """
    result = enrich_location(
        building=row.get(building_col),
        level=row.get(level_col),
        grid=row.get(grid_col),
        room_code=row.get(room_code_col) if room_code_col else None,
        source=source,
    )
    return result.to_dict()


def enrich_dataframe(
    df: pd.DataFrame,
    building_col: str = 'building',
    level_col: str = 'level',
    grid_col: str = 'grid',
    room_code_col: Optional[str] = None,
    source: str = 'UNKNOWN',
    show_progress: bool = True,
) -> pd.DataFrame:
    """
    Enrich entire DataFrame with location columns.

    Adds these columns:
    - dim_location_id
    - location_type
    - location_code
    - level (normalized)
    - grid_row_min, grid_row_max, grid_col_min, grid_col_max
    - affected_rooms, affected_rooms_count
    - match_type

    Args:
        df: Input DataFrame
        building_col: Column name for building
        level_col: Column name for level
        grid_col: Column name for grid
        room_code_col: Column name for room code (optional)
        source: Source identifier
        show_progress: Print progress every 10000 rows

    Returns:
        DataFrame with location columns added
    """
    # Reset dimension cache to ensure fresh data
    reset_cache()

    results = []
    total = len(df)

    for idx, (_, row) in enumerate(df.iterrows()):
        result = enrich_location(
            building=row.get(building_col) if building_col in df.columns else None,
            level=row.get(level_col) if level_col in df.columns else None,
            grid=row.get(grid_col) if grid_col in df.columns else None,
            room_code=row.get(room_code_col) if room_code_col and room_code_col in df.columns else None,
            source=source,
        )
        results.append(result.to_dict())

        if show_progress and (idx + 1) % 10000 == 0:
            print(f"  Enriched {idx + 1:,}/{total:,} rows...")

    # Convert results to DataFrame and join
    results_df = pd.DataFrame(results)

    # Add columns to original DataFrame
    for col in results_df.columns:
        df[col] = results_df[col].values

    return df
