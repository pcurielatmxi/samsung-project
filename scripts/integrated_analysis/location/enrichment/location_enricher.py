"""
Centralized Location Enrichment

This module is the SINGLE SOURCE OF TRUTH for location enrichment logic.
It consolidates ~700 lines of duplicated code from:
- scripts/raba/document_processing/consolidate.py (~80 lines)
- scripts/psi/document_processing/consolidate.py (~80 lines)
- scripts/integrated_analysis/enrich_with_dimensions.py (~500 lines)
- scripts/fieldwire/process/enrich_tbm.py (~40 lines)

Usage:
    from scripts.integrated_analysis.location import enrich_location

    result = enrich_location(
        building='FAB',
        level='2F',
        grid='G/10-12',
        source='RABA'
    )

    # Use results directly
    record['dim_location_id'] = result.dim_location_id
    record['affected_rooms'] = result.affected_rooms
    record['grid_completeness'] = result.grid_completeness
"""

import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, List, Dict, Any

import pandas as pd

# Add project root to path
_project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from scripts.integrated_analysis.location.core.normalizers import (
    normalize_level,
    normalize_building,
)
from scripts.shared.dimension_lookup import (
    get_location_id,
    get_location_id_by_code,
    get_building_level,
    get_affected_rooms,
    parse_grid_field,
    normalize_grid,
)


@dataclass
class LocationEnrichmentResult:
    """
    Result of enriching a record with location data.

    Contains all location-related fields that should be added to output records.
    """
    # Dimension keys
    dim_location_id: Optional[int] = None
    building_level: Optional[str] = None

    # Normalized values
    building_normalized: Optional[str] = None
    level_normalized: Optional[str] = None

    # Grid bounds (from parsing)
    grid_row_min: Optional[str] = None
    grid_row_max: Optional[str] = None
    grid_col_min: Optional[float] = None
    grid_col_max: Optional[float] = None
    grid_normalized: Optional[str] = None

    # Affected rooms (JSON string for CSV storage)
    affected_rooms: Optional[str] = None
    affected_rooms_count: Optional[int] = None

    # Quality diagnostics
    grid_completeness: str = 'NONE'  # FULL, ROW_ONLY, COL_ONLY, LEVEL_ONLY, NONE
    match_quality: str = 'NONE'  # PRECISE, MIXED, PARTIAL, NONE
    location_review_flag: bool = False

    # Source tracking
    location_source: Optional[str] = None  # ROOM_DIRECT, GRID_SINGLE, GRIDLINE, LEVEL, BUILDING, SITE

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for easy DataFrame assignment."""
        return {
            'dim_location_id': self.dim_location_id,
            'building_level': self.building_level,
            'building_normalized': self.building_normalized,
            'level_normalized': self.level_normalized,
            'grid_row_min': self.grid_row_min,
            'grid_row_max': self.grid_row_max,
            'grid_col_min': self.grid_col_min,
            'grid_col_max': self.grid_col_max,
            'grid_normalized': self.grid_normalized,
            'affected_rooms': self.affected_rooms,
            'affected_rooms_count': self.affected_rooms_count,
            'grid_completeness': self.grid_completeness,
            'match_quality': self.match_quality,
            'location_review_flag': self.location_review_flag,
            'location_source': self.location_source,
        }


def _compute_grid_completeness(
    level_normalized: Optional[str],
    has_row: bool,
    has_col: bool
) -> str:
    """
    Compute grid_completeness based on available grid information.

    Returns:
        FULL: Both row and column present
        ROW_ONLY: Only row present
        COL_ONLY: Only column present
        LEVEL_ONLY: Only level present (no grid)
        NONE: No location info
    """
    if has_row and has_col:
        return 'FULL'
    elif has_row:
        return 'ROW_ONLY'
    elif has_col:
        return 'COL_ONLY'
    elif level_normalized:
        return 'LEVEL_ONLY'
    else:
        return 'NONE'


def _compute_match_quality(affected_rooms_json: Optional[str]) -> str:
    """
    Compute match_quality based on affected rooms match types.

    Returns:
        PRECISE: All matches are FULL (both row and column matched)
        MIXED: Some FULL, some PARTIAL matches
        PARTIAL: All matches are PARTIAL (only row or column matched)
        NONE: No rooms matched
    """
    if not affected_rooms_json:
        return 'NONE'

    try:
        rooms = json.loads(affected_rooms_json)
        if not rooms:
            return 'NONE'

        match_types = [r.get('match_type') for r in rooms]
        full_count = sum(1 for m in match_types if m == 'FULL')
        partial_count = sum(1 for m in match_types if m == 'PARTIAL')
        gridline_count = sum(1 for m in match_types if m == 'GRIDLINE')

        # If only gridlines matched, treat as PARTIAL
        if gridline_count > 0 and full_count == 0 and partial_count == 0:
            return 'PARTIAL'

        if partial_count == 0 and gridline_count == 0:
            return 'PRECISE'
        elif full_count == 0:
            return 'PARTIAL'
        else:
            return 'MIXED'

    except (json.JSONDecodeError, TypeError):
        return 'NONE'


def _compute_location_review_flag(
    affected_rooms_count: Optional[int],
    grid_completeness: str,
    match_quality: str
) -> bool:
    """
    Determine if record needs human location review.

    Flags records where location matching may be unreliable:
    - Many rooms matched with non-precise matching
    - Only level info (no grid)
    - Many partial matches

    Returns:
        True if manual review is recommended
    """
    if not affected_rooms_count or affected_rooms_count == 0:
        return False

    # Flag if many rooms matched and not precise
    if affected_rooms_count > 10 and match_quality != 'PRECISE':
        return True

    # Flag partial matches with many rooms
    if match_quality == 'PARTIAL' and affected_rooms_count > 5:
        return True

    # Flag mixed matches with many rooms
    if match_quality == 'MIXED' and affected_rooms_count > 8:
        return True

    # Flag level-only (very coarse location)
    if grid_completeness == 'LEVEL_ONLY':
        return True

    return False


def _apply_location_hierarchy(
    building_normalized: Optional[str],
    level_normalized: Optional[str],
    room_code: Optional[str],
    affected_rooms_json: Optional[str],
) -> tuple[Optional[int], Optional[str]]:
    """
    Apply location hierarchy to determine the most appropriate location_id.

    Hierarchy (most specific to least specific):
    1. ROOM_DIRECT: Direct room code extraction
    2. GRID_SINGLE: Single room from grid-based matching
    3. GRIDLINE: Gridline location (column-specific)
    4. LEVEL: Level location (building + floor)
    5. BUILDING: Building-wide location
    6. SITE: Site-wide fallback

    Returns:
        (location_id, location_source) tuple
    """
    # Priority 1: Direct room code extraction
    if room_code and pd.notna(room_code):
        location_id = get_location_id_by_code(room_code)
        if location_id:
            return location_id, 'ROOM_DIRECT'

    # Priority 2: Single specific location from grid-based matching
    if affected_rooms_json and pd.notna(affected_rooms_json):
        try:
            rooms = json.loads(affected_rooms_json)
            if len(rooms) == 1:
                room_data = rooms[0]
                location_code = room_data.get('location_code')
                match_type = room_data.get('match_type', '')

                location_id = get_location_id_by_code(location_code)
                if location_id:
                    if match_type == 'FULL':
                        return location_id, 'GRID_SINGLE_FULL'
                    elif match_type == 'GRIDLINE':
                        return location_id, 'GRIDLINE'
                    else:
                        return location_id, 'GRID_SINGLE_PARTIAL'
        except (json.JSONDecodeError, TypeError):
            pass

    # Priority 3: Level location (building + level)
    if building_normalized and level_normalized:
        location_id = get_location_id(building_normalized, level_normalized)
        if location_id:
            return location_id, 'LEVEL'

    # Priority 4: Building-wide location
    if building_normalized:
        location_id = get_location_id(building_normalized, None)
        if location_id:
            return location_id, 'BUILDING'

    # Priority 5: Site-wide fallback
    location_id = get_location_id(None, None)
    if location_id:
        return location_id, 'SITE'

    return None, None


def enrich_location(
    building: Optional[str] = None,
    level: Optional[str] = None,
    grid: Optional[str] = None,
    room_code: Optional[str] = None,
    source: str = 'UNKNOWN',
) -> LocationEnrichmentResult:
    """
    Enrich a record with location information.

    This is the SINGLE FUNCTION that should be called for all location enrichment.
    It consolidates all location processing logic that was previously duplicated
    across RABA, PSI, TBM, and other consolidation scripts.

    Args:
        building: Raw building name (e.g., "FAB", "SUE", "Main FAB")
        level: Raw level value (e.g., "1F", "2nd Floor", "Basement")
        grid: Raw grid coordinates (e.g., "G/10", "B-D/8-12", "F.5/17")
        room_code: Extracted room code if available (e.g., "FAB116201")
        source: Source identifier for diagnostics (e.g., "RABA", "PSI", "TBM")

    Returns:
        LocationEnrichmentResult with all location fields populated
    """
    result = LocationEnrichmentResult()

    # Step 1: Normalize building and level
    result.building_normalized = normalize_building(building)
    result.level_normalized = normalize_level(level)

    # Step 2: Get building_level string (for display/filtering)
    result.building_level = get_building_level(
        result.building_normalized,
        result.level_normalized
    )

    # Step 3: Parse and normalize grid coordinates
    result.grid_normalized = normalize_grid(grid)
    grid_parsed = parse_grid_field(grid)

    result.grid_row_min = grid_parsed.get('grid_row_min')
    result.grid_row_max = grid_parsed.get('grid_row_max')
    result.grid_col_min = grid_parsed.get('grid_col_min')
    result.grid_col_max = grid_parsed.get('grid_col_max')

    # Step 4: Determine what grid info we have
    has_row = result.grid_row_min is not None
    has_col = result.grid_col_min is not None

    # Step 5: Compute affected_rooms based on grid overlap
    # NOTE: Building is ignored - FAB1 uses unified grid system across all buildings
    if result.level_normalized and (has_row or has_col):
        rooms = get_affected_rooms(
            result.level_normalized,
            result.grid_row_min if has_row else None,
            result.grid_row_max if has_row else None,
            result.grid_col_min if has_col else None,
            result.grid_col_max if has_col else None,
        )
        if rooms:
            result.affected_rooms = json.dumps(rooms)
            result.affected_rooms_count = len(rooms)

    # Step 6: Compute quality diagnostics
    result.grid_completeness = _compute_grid_completeness(
        result.level_normalized, has_row, has_col
    )
    result.match_quality = _compute_match_quality(result.affected_rooms)
    result.location_review_flag = _compute_location_review_flag(
        result.affected_rooms_count,
        result.grid_completeness,
        result.match_quality
    )

    # Step 7: Apply location hierarchy to get dim_location_id
    result.dim_location_id, result.location_source = _apply_location_hierarchy(
        result.building_normalized,
        result.level_normalized,
        room_code,
        result.affected_rooms,
    )

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
