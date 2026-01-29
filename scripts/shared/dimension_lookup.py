"""
Dimension Lookup Module

Provides functions to map raw data values to dimension table IDs.
Used by all data source consolidation scripts for consistent integration.

Dimension Tables (from processed/integrated_analysis/dimensions/):
- dim_location: location codes (rooms, elevators, stairs) with grid bounds
- dim_company: company name → company_id
- dim_csi_section: CSI MasterFormat section → csi_section_id

Mapping Tables (from processed/integrated_analysis/mappings/):
- map_company_aliases: company name variants → company_id

Location Lookup:
- get_location_id(building, level) → integer location_id from dim_location
- get_building_level(building, level) → building_level string (e.g., "FAB-1F")
- get_locations_at_grid(level, row, col) → list of rooms at grid point
- get_affected_rooms(level, row_min, row_max, col_min, col_max) → rooms in grid range

IMPORTANT: Grid Coordinate System
The FAB1 project uses a UNIFIED grid coordinate system across all buildings
(FAB, SUE, SUW, FIZ). For spatial joins, building is ignored - only level
and grid coordinates matter. See CLAUDE.md for details.

NOTE: dim_trade has been superseded by dim_csi_section. Use CSI MasterFormat
sections (52 categories) instead of the legacy 13-category trade taxonomy.
"""

import sys
from pathlib import Path
from typing import Optional, Dict, List
import re

import pandas as pd

# Project paths - dimension tables are in the external data folder
_project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import settings

# Dimension tables are stored in processed data folder (external)
_dimensions_dir = settings.PROCESSED_DATA_DIR / 'integrated_analysis' / 'dimensions'
_mappings_dir = settings.PROCESSED_DATA_DIR / 'integrated_analysis' / 'mappings'


# Cached dimension data
_dim_location: Optional[pd.DataFrame] = None
_dim_company: Optional[pd.DataFrame] = None
_map_company_aliases: Optional[pd.DataFrame] = None
_building_level_to_id: Optional[Dict[str, int]] = None
_location_code_to_id: Optional[Dict[str, int]] = None  # location_code -> location_id
_p6_alias_to_id: Optional[Dict[str, int]] = None  # p6_alias -> location_id


def _normalize_level(level: str) -> str:
    """Normalize level string: 03F -> 3F, but keep B1, ROOF, UG as-is."""
    if not level or pd.isna(level):
        return level
    level = str(level).upper().strip()
    # Remove leading zeros from floor numbers (03F -> 3F)
    match = re.match(r'^0*(\d+)F$', level)
    if match:
        return f"{match.group(1)}F"
    return level


def _load_dimensions():
    """Load dimension tables if not already loaded."""
    global _dim_location, _dim_company, _map_company_aliases
    global _building_level_to_id, _location_code_to_id, _p6_alias_to_id

    if _dim_location is None:
        _dim_location = pd.read_csv(_dimensions_dir / 'dim_location.csv')

        # Build building_level -> location_id lookup
        # Only include LEVEL and BUILDING types - these are the aggregate/fallback entries
        # ROOM/STAIR/ELEVATOR/AREA should be looked up by location_code, not building_level
        _building_level_to_id = {}
        aggregate_types = {'LEVEL', 'BUILDING'}
        for _, row in _dim_location.iterrows():
            loc_type = row.get('location_type')
            if loc_type not in aggregate_types:
                continue
            bl = row.get('building_level')
            if bl and pd.notna(bl) and bl not in _building_level_to_id:
                _building_level_to_id[bl] = int(row['location_id'])

        # Build location_code -> location_id lookup (case-insensitive)
        _location_code_to_id = {}
        for _, row in _dim_location.iterrows():
            code = row.get('location_code')
            if code and pd.notna(code):
                _location_code_to_id[str(code).upper()] = int(row['location_id'])

        # Add FAB1 as alias for project-wide building (maps to FAB building entry)
        # This handles P6 taxonomy's FAB1 location_code for tasks without specific locations
        if 'FAB' in _location_code_to_id and 'FAB1' not in _location_code_to_id:
            _location_code_to_id['FAB1'] = _location_code_to_id['FAB']

        # Build p6_alias -> location_id lookup (for STR-XX, ELV-XX codes)
        _p6_alias_to_id = {}
        for _, row in _dim_location.iterrows():
            alias = row.get('p6_alias')
            if alias and pd.notna(alias):
                _p6_alias_to_id[str(alias).upper()] = int(row['location_id'])

    if _dim_company is None:
        _dim_company = pd.read_csv(_dimensions_dir / 'dim_company.csv')

    if _map_company_aliases is None:
        _map_company_aliases = pd.read_csv(_mappings_dir / 'map_company_aliases.csv')


def get_location_id(building: str, level: str, allow_fallback: bool = True) -> Optional[int]:
    """
    Get location_id (integer FK) from building and level.

    Returns the location_id from dim_location for the given building-level.
    Prefers LEVEL type entries over ROOM/STAIR/etc for building-level aggregation.

    Fallback behavior (when allow_fallback=True):
    1. Try building + level first (e.g., "SUE-1F" for building-specific levels)
    2. Try level-only (e.g., "1F" for LEVEL entries that span all buildings)
    3. If level is missing, try FAB1 (project-wide building)
    4. If nothing matches, try UNDEFINED

    Args:
        building: Building code (FAB, SUE, SUW, FIZ, OB1, GCS)
        level: Level code (1F, 2F, B1, ROOF, etc.)
        allow_fallback: If True, fall back to project-wide or UNDEFINED entries

    Returns:
        Integer location_id or None if not found in dim_location
    """
    _load_dimensions()

    has_building = building and pd.notna(building) and str(building).strip()
    has_level = level and pd.notna(level) and str(level).strip()

    # Normalize level if provided
    normalized_level = None
    if has_level:
        normalized_level = _normalize_level(str(level).upper().strip())

    # Case 1: Have both building and level - try exact building-level match
    if has_building and has_level:
        building_str = str(building).upper().strip()
        building_level = f"{building_str}-{normalized_level}"
        loc_id = _building_level_to_id.get(building_level)
        if loc_id is not None:
            return loc_id

    # Case 2: Have level - try level-only match (LEVEL entries span all buildings)
    if allow_fallback and has_level:
        loc_id = _building_level_to_id.get(normalized_level)
        if loc_id is not None:
            return loc_id

    # Case 3: Have building but no level - try FAB1 (project-wide building)
    if allow_fallback and has_building and not has_level:
        loc_id = _location_code_to_id.get('FAB1')
        if loc_id is not None:
            return loc_id

    # Case 4: No match - try UNDEFINED
    if allow_fallback:
        loc_id = _location_code_to_id.get('UNDEFINED')
        if loc_id is not None:
            return loc_id

    return None


def get_location_id_by_code(location_code: str) -> Optional[int]:
    """
    Get location_id from location_code (room, elevator, stair, gridline codes).

    Matches against both location_code and p6_alias columns in dim_location.
    This is the preferred lookup method for tasks that have extracted location codes.

    Args:
        location_code: Location code (e.g., FAB112345, ELV-24, STR-05, FAB1-ST05)

    Returns:
        Integer location_id or None if not found
    """
    _load_dimensions()

    if not location_code or pd.isna(location_code):
        return None

    code_upper = str(location_code).upper().strip()

    # Try direct location_code match
    loc_id = _location_code_to_id.get(code_upper)
    if loc_id is not None:
        return loc_id

    # Try p6_alias match (for STR-XX, ELV-XX -> FAB1-STXX, FAB1-ELXX)
    loc_id = _p6_alias_to_id.get(code_upper)
    if loc_id is not None:
        return loc_id

    return None


def get_building_level(building: str, level: str) -> Optional[str]:
    """
    Get building_level string from building and level.

    This returns a coarse location identifier (building+level) for display/filtering.
    For integer FK joins, use get_location_id().

    Args:
        building: Building code (FAB, SUE, SUW, FIZ, OB1, GCS)
        level: Level code (1F, 2F, B1, ROOF, etc.)

    Returns:
        building_level string like "FAB-1F" or None if invalid
    """
    if not building or not level:
        return None

    building = str(building).upper().strip()
    level = _normalize_level(str(level).upper().strip())

    return f"{building}-{level}"


def _row_sort_key(r):
    """Convert row letter to sortable tuple (handles fractional rows like E.5)."""
    r = str(r).upper()
    if '.' in r:
        base = r[0]
        decimal = float(r[1:])
    else:
        base = r[0]
        decimal = 0
    return (base, decimal)


def _row_in_range(row_min, row_max, target_row):
    """Check if target_row is between row_min and row_max (inclusive)."""
    min_parsed = _row_sort_key(row_min)
    max_parsed = _row_sort_key(row_max)
    target_parsed = _row_sort_key(target_row)
    return min_parsed <= target_parsed <= max_parsed


def _rows_overlap(row_min1, row_max1, row_min2, row_max2):
    """Check if two row ranges overlap."""
    # Convert to sortable tuples
    min1 = _row_sort_key(row_min1)
    max1 = _row_sort_key(row_max1)
    min2 = _row_sort_key(row_min2)
    max2 = _row_sort_key(row_max2)
    # Ranges overlap if neither is completely before the other
    return not (max1 < min2 or max2 < min1)


def _cols_overlap(col_min1, col_max1, col_min2, col_max2):
    """Check if two column ranges overlap."""
    return not (col_max1 < col_min2 or col_max2 < col_min1)


def get_locations_at_grid(
    level: str,
    grid_row: str,
    grid_col: float
) -> List[Dict]:
    """
    Find all locations (rooms, elevators, etc.) whose grid bounds contain the given point.

    IMPORTANT: The FAB1 project uses a unified grid coordinate system across all
    buildings. Building is NOT used as a filter - only level and grid matter.

    Args:
        level: Level code (1F, 2F, B1, etc.)
        grid_row: Grid row letter (A-N, may include decimal like "E.5")
        grid_col: Grid column number (1-34, may include decimal like 17.5)

    Returns:
        List of dicts with location_id, location_code, building, location_type, room_name
    """
    if not level or not grid_row or grid_col is None:
        return []

    _load_dimensions()

    level = str(level).upper().strip()
    grid_row = str(grid_row).upper().strip()

    # Filter by level only (NOT building - unified grid system)
    candidates = _dim_location[
        (_dim_location['level'] == level) &
        (_dim_location['grid_row_min'].notna()) &
        (_dim_location['grid_col_min'].notna())
    ].copy()

    if candidates.empty:
        return []

    # Filter by grid containment
    results = []
    for _, row in candidates.iterrows():
        try:
            if (_row_in_range(row['grid_row_min'], row['grid_row_max'], grid_row) and
                row['grid_col_min'] <= grid_col <= row['grid_col_max']):
                results.append({
                    'location_id': int(row['location_id']),
                    'location_code': row['location_code'],
                    'building': row['building'],
                    'location_type': row['location_type'],
                    'room_name': row['room_name'],
                })
        except (TypeError, ValueError):
            continue

    return results


def get_location_by_code(location_code: str) -> Optional[Dict]:
    """
    Get location details by location code.

    Args:
        location_code: Location code (e.g., "FAB114402", "ELV-S", "STR-A")

    Returns:
        Dict with location details or None if not found
    """
    if not location_code:
        return None

    _load_dimensions()

    code = str(location_code).upper().strip()
    match = _dim_location[_dim_location['location_code'].str.upper() == code]

    if match.empty:
        return None

    row = match.iloc[0]
    return {
        'location_id': row['location_id'],
        'location_code': row['location_code'],
        'location_type': row['location_type'],
        'room_name': row['room_name'],
        'building': row['building'],
        'level': row['level'],
        'grid_row_min': row['grid_row_min'],
        'grid_row_max': row['grid_row_max'],
        'grid_col_min': row['grid_col_min'],
        'grid_col_max': row['grid_col_max'],
    }


def get_affected_rooms(
    level: str,
    grid_row_min: Optional[str] = None,
    grid_row_max: Optional[str] = None,
    grid_col_min: Optional[float] = None,
    grid_col_max: Optional[float] = None,
) -> List[Dict]:
    """
    Find all rooms/locations whose grid bounds overlap with the given grid range.

    IMPORTANT: The FAB1 project uses a unified grid coordinate system across all
    buildings (FAB, SUE, SUW, FIZ). Building is NOT used as a filter - only level
    and grid coordinates matter for spatial matching.

    Returns a list of affected rooms with a PARTIAL flag when grid info is incomplete.

    Args:
        level: Level code (1F, 2F, B1, etc.)
        grid_row_min: Starting row letter (A-N, may include decimal like "E.5")
        grid_row_max: Ending row letter (defaults to row_min if not provided)
        grid_col_min: Starting column number (1-34)
        grid_col_max: Ending column number (defaults to col_min if not provided)

    Returns:
        List of dicts with:
        - location_id: Integer FK to dim_location
        - location_code: Standardized room code
        - building: Building code for reference
        - room_name: Room description
        - match_type: FULL (both row+col match) or PARTIAL (only row or col)

        Returns empty list if no grid information provided.
    """
    if not level:
        return []

    # Determine what grid info we have
    has_row = grid_row_min is not None and pd.notna(grid_row_min)
    has_col = grid_col_min is not None and pd.notna(grid_col_min)

    # Need at least some grid info
    if not has_row and not has_col:
        return []

    _load_dimensions()

    level = str(level).upper().strip()

    # Normalize inputs
    if has_row:
        row_min = str(grid_row_min).upper().strip()
        row_max = str(grid_row_max).upper().strip() if grid_row_max and pd.notna(grid_row_max) else row_min
    else:
        row_min = row_max = None

    if has_col:
        col_min = float(grid_col_min)
        col_max = float(grid_col_max) if grid_col_max and pd.notna(grid_col_max) else col_min
    else:
        col_min = col_max = None

    # Filter locations by level only (NOT building - unified grid system)
    # Include ROOM, ELEVATOR, STAIR (exclude LEVEL, BUILDING, AREA, GRIDLINE)
    candidates = _dim_location[
        (_dim_location['level'] == level) &
        (_dim_location['location_type'].isin(['ROOM', 'ELEVATOR', 'STAIR'])) &
        (_dim_location['grid_row_min'].notna()) &
        (_dim_location['grid_col_min'].notna())
    ]

    if candidates.empty:
        return []

    results = []
    for _, loc in candidates.iterrows():
        try:
            loc_row_min = str(loc['grid_row_min']).upper()
            loc_row_max = str(loc['grid_row_max']).upper()
            loc_col_min = float(loc['grid_col_min'])
            loc_col_max = float(loc['grid_col_max'])

            # Check overlap based on available grid info
            if has_row and has_col:
                # Full grid - check both row and column overlap
                rows_match = _rows_overlap(row_min, row_max, loc_row_min, loc_row_max)
                cols_match = _cols_overlap(col_min, col_max, loc_col_min, loc_col_max)

                if rows_match and cols_match:
                    results.append({
                        'location_id': int(loc['location_id']),
                        'location_code': loc['location_code'],
                        'building': loc['building'],
                        'room_name': loc['room_name'],
                        'match_type': 'FULL',
                    })

            elif has_row:
                # Row only - check row overlap (PARTIAL match)
                if _rows_overlap(row_min, row_max, loc_row_min, loc_row_max):
                    results.append({
                        'location_id': int(loc['location_id']),
                        'location_code': loc['location_code'],
                        'building': loc['building'],
                        'room_name': loc['room_name'],
                        'match_type': 'PARTIAL',
                    })

            elif has_col:
                # Column only - check column overlap (PARTIAL match)
                if _cols_overlap(col_min, col_max, loc_col_min, loc_col_max):
                    results.append({
                        'location_id': int(loc['location_id']),
                        'location_code': loc['location_code'],
                        'building': loc['building'],
                        'room_name': loc['room_name'],
                        'match_type': 'PARTIAL',
                    })

        except (TypeError, ValueError):
            continue

    # If no room/elevator/stair matches, fall back to GRIDLINE locations
    # GRIDLINE entries span full row range (A-N) for a single column
    # Match at the specific level first, then MULTI level for remaining columns
    if not results and has_col:
        # Prefer level-specific gridlines, then fall back to MULTI
        gridline_candidates = _dim_location[
            (_dim_location['level'].isin([level, 'MULTI'])) &
            (_dim_location['location_type'] == 'GRIDLINE') &
            (_dim_location['grid_col_min'].notna())
        ]

        # Track which columns we've already matched to avoid duplicates
        matched_cols = set()

        for _, loc in gridline_candidates.iterrows():
            try:
                loc_col_min = float(loc['grid_col_min'])
                loc_col_max = float(loc['grid_col_max'])
                col_key = int(loc_col_min)

                # Skip if we already have a match for this column
                if col_key in matched_cols:
                    continue

                if _cols_overlap(col_min, col_max, loc_col_min, loc_col_max):
                    results.append({
                        'location_id': int(loc['location_id']),
                        'location_code': loc['location_code'],
                        'building': loc['building'],
                        'room_name': loc['room_name'],
                        'match_type': 'GRIDLINE',
                    })
                    matched_cols.add(col_key)
            except (TypeError, ValueError):
                continue

    return results


def _normalize_company_name(name: str) -> str:
    """Normalize company name for matching."""
    if not name:
        return ''
    # Remove common suffixes and normalize
    normalized = str(name).upper().strip()
    # Remove punctuation and common suffixes
    for suffix in [', INC.', ', INC', ' INC.', ' INC', ', LLC', ' LLC', ', LP', ' LP', '.']:
        normalized = normalized.replace(suffix, '')
    # Remove extra whitespace
    normalized = ' '.join(normalized.split())
    return normalized


def get_company_id(company_name: str) -> Optional[int]:
    """
    Get dim_company_id from company name.

    Tries multiple matching strategies:
    1. Exact match on canonical_name
    2. Exact match on alias
    3. Normalized match on canonical_name
    4. Normalized match on alias
    5. Partial match on key terms

    Args:
        company_name: Company name (standardized or raw)

    Returns:
        company_id integer or None if not found
    """
    if not company_name or pd.isna(company_name):
        return None

    _load_dimensions()

    name = str(company_name).strip()
    name_upper = name.upper()
    name_lower = name.lower()
    name_normalized = _normalize_company_name(name)

    # Strategy 1: Exact match on canonical_name (case-insensitive)
    for _, row in _dim_company.iterrows():
        if str(row['canonical_name']).upper() == name_upper:
            return int(row['company_id'])

    # Strategy 2: Exact match on alias (case-insensitive)
    for _, row in _map_company_aliases.iterrows():
        if str(row['alias']).upper() == name_upper:
            return int(row['company_id'])

    # Strategy 3: Normalized match on canonical_name
    for _, row in _dim_company.iterrows():
        canonical_normalized = _normalize_company_name(row['canonical_name'])
        if name_normalized == canonical_normalized:
            return int(row['company_id'])
        # Check if normalized name is substring
        if name_normalized in canonical_normalized or canonical_normalized in name_normalized:
            return int(row['company_id'])

    # Strategy 4: Normalized match on alias
    for _, row in _map_company_aliases.iterrows():
        alias_normalized = _normalize_company_name(row['alias'])
        if name_normalized == alias_normalized:
            return int(row['company_id'])
        # Check key terms (e.g., "SAMSUNG E&C" matches "SAMSUNG E&C AMERICA")
        if name_normalized in alias_normalized or alias_normalized in name_normalized:
            return int(row['company_id'])

    # Strategy 5: Partial match on short_code
    for _, row in _dim_company.iterrows():
        short_code = str(row.get('short_code', '')).upper()
        if short_code and (name_upper == short_code or short_code in name_upper):
            return int(row['company_id'])

    return None


def get_company_primary_trade_id(company_id: int) -> Optional[int]:
    """
    Get the primary trade_id for a company from dim_company.

    This is used as a fallback when trade cannot be inferred from the inspection
    type or other fields - we use the company's known primary trade.

    Args:
        company_id: Integer company_id from dim_company

    Returns:
        Integer trade_id or None if not found or company has no primary_trade_id
    """
    if company_id is None or pd.isna(company_id):
        return None

    _load_dimensions()

    match = _dim_company[_dim_company['company_id'] == int(company_id)]
    if len(match) > 0:
        primary_trade_id = match.iloc[0].get('primary_trade_id')
        if pd.notna(primary_trade_id):
            return int(primary_trade_id)
    return None


# Cached sets for performing company lookup
_yates_sub_ids: Optional[set] = None
_yates_self_ids: Optional[set] = None


def _load_yates_sets():
    """Load Yates company ID sets for performing company lookup."""
    global _yates_sub_ids, _yates_self_ids

    if _yates_sub_ids is None:
        _load_dimensions()
        _yates_sub_ids = set(
            _dim_company[_dim_company['company_type'] == 'yates_sub']['company_id'].tolist()
        )
        _yates_self_ids = set(
            _dim_company[_dim_company['company_type'] == 'yates_self']['company_id'].tolist()
        )


def get_performing_company_id(
    dim_company_id: Optional[int],
    dim_subcontractor_id: Optional[int]
) -> Optional[int]:
    """
    Determine the company that actually performed the work.

    Priority logic:
    1. Yates subcontractor (from either field) - they did the work
    2. Yates self-perform - Yates did the work themselves
    3. Subcontractor (non-Yates) - they did the work
    4. Contractor (fallback) - if nothing else, use this

    This resolves the ambiguity where 'contractor' often means who requested
    the inspection (GC level), while 'subcontractor' is who did the work.

    Args:
        dim_company_id: Company ID from contractor field
        dim_subcontractor_id: Company ID from subcontractor field

    Returns:
        Company ID of the performing company, or None if both inputs are None
    """
    _load_yates_sets()

    # Convert to int if not None/NaN
    company_id = int(dim_company_id) if dim_company_id is not None and pd.notna(dim_company_id) else None
    sub_id = int(dim_subcontractor_id) if dim_subcontractor_id is not None and pd.notna(dim_subcontractor_id) else None

    # Priority 1: Yates sub in subcontractor field
    if sub_id is not None and sub_id in _yates_sub_ids:
        return sub_id

    # Priority 2: Yates sub in contractor field
    if company_id is not None and company_id in _yates_sub_ids:
        return company_id

    # Priority 3: Yates self in subcontractor field
    if sub_id is not None and sub_id in _yates_self_ids:
        return sub_id

    # Priority 4: Yates self in contractor field
    if company_id is not None and company_id in _yates_self_ids:
        return company_id

    # Priority 5: Use subcontractor if present (non-Yates performer)
    if sub_id is not None:
        return sub_id

    # Priority 6: Fall back to contractor
    return company_id


# ============================================================================
# Company Classification Functions
# ============================================================================

def _match_company_row(name_lower: str, row: pd.Series) -> bool:
    """Check if company name matches a dim_company row."""
    # Check canonical_name
    canonical = str(row['canonical_name']).lower()
    if canonical == name_lower or canonical in name_lower or name_lower in canonical:
        return True

    # Check short_code
    short_code = str(row.get('short_code', '')).lower()
    if short_code and short_code != 'nan' and (short_code == name_lower or name_lower == short_code):
        return True

    # Check full_name
    full = str(row.get('full_name', '')).lower()
    if full and full != 'nan' and (full == name_lower or name_lower in full or full in name_lower):
        return True

    return False


def is_yates_sub(company_name: str) -> Optional[bool]:
    """
    Check if a company is a Yates subcontractor.

    Uses the is_yates_sub flag from dim_company.csv to determine classification.

    Args:
        company_name: Company name (standardized or raw)

    Returns:
        True if Yates sub, False if not, None if company not found
    """
    if not company_name or pd.isna(company_name):
        return None

    _load_dimensions()

    if _dim_company is None or _dim_company.empty:
        return None

    name_lower = str(company_name).lower().strip()

    for _, row in _dim_company.iterrows():
        if _match_company_row(name_lower, row):
            return bool(row['is_yates_sub'])

    return None


def get_company_type(company_name: str) -> Optional[str]:
    """
    Get the company type classification.

    Company types:
    - yates_self: W.G. Yates & Sons (the GC)
    - yates_sub: Subcontractors working under Yates contract
    - major_contractor: GC/major contractors with direct Samsung contracts
    - precast_supplier: Material suppliers (precast concrete)
    - other: Unclassified

    Args:
        company_name: Company name (standardized or raw)

    Returns:
        Company type string or None if not found
    """
    if not company_name or pd.isna(company_name):
        return None

    _load_dimensions()

    if _dim_company is None or _dim_company.empty:
        return None

    name_lower = str(company_name).lower().strip()

    for _, row in _dim_company.iterrows():
        if _match_company_row(name_lower, row):
            return row['company_type']

    return None


def get_company_info(company_name: str) -> Optional[Dict]:
    """
    Get full company information from dim_company.

    Args:
        company_name: Company name (standardized or raw)

    Returns:
        Dict with company_id, canonical_name, short_code, tier, full_name,
        company_type, is_yates_sub, primary_trade_id, parent_company_id,
        parent_confidence, notes - or None if not found
    """
    if not company_name or pd.isna(company_name):
        return None

    _load_dimensions()

    if _dim_company is None or _dim_company.empty:
        return None

    name_lower = str(company_name).lower().strip()

    for _, row in _dim_company.iterrows():
        if _match_company_row(name_lower, row):
            return {
                'company_id': int(row['company_id']),
                'canonical_name': row['canonical_name'],
                'short_code': row.get('short_code'),
                'tier': row.get('tier'),
                'full_name': row.get('full_name'),
                'company_type': row['company_type'],
                'is_yates_sub': bool(row['is_yates_sub']),
                'primary_trade_id': int(row['primary_trade_id']) if pd.notna(row.get('primary_trade_id')) else None,
                'parent_company_id': int(row['parent_company_id']) if pd.notna(row.get('parent_company_id')) else None,
                'parent_confidence': row.get('parent_confidence'),
                'notes': row.get('notes'),
            }

    return None


# ============================================================================
# CSI Section Lookup
# ============================================================================
# Cached CSI section data
_dim_csi_section: Optional[pd.DataFrame] = None
_dim_csi_division: Optional[pd.DataFrame] = None


def _load_csi_sections():
    """Load CSI section dimension table if not already loaded."""
    global _dim_csi_section

    if _dim_csi_section is None:
        csi_path = _dimensions_dir / 'dim_csi_section.csv'
        if csi_path.exists():
            _dim_csi_section = pd.read_csv(csi_path)
        else:
            _dim_csi_section = pd.DataFrame()


def get_csi_section_id(csi_section: str) -> Optional[int]:
    """
    Get csi_section_id from CSI section code.

    Args:
        csi_section: CSI section code like "03 30 00" or "07 84 00"

    Returns:
        csi_section_id integer or None if not found
    """
    if not csi_section or pd.isna(csi_section):
        return None

    _load_csi_sections()

    if _dim_csi_section.empty:
        return None

    code = str(csi_section).strip()

    match = _dim_csi_section[_dim_csi_section['csi_section'] == code]
    if len(match) > 0:
        return int(match.iloc[0]['csi_section_id'])
    return None


def get_csi_section_code(csi_section_id: int) -> Optional[str]:
    """Get CSI section code from csi_section_id."""
    if csi_section_id is None or pd.isna(csi_section_id):
        return None

    _load_csi_sections()

    if _dim_csi_section.empty:
        return None

    match = _dim_csi_section[_dim_csi_section['csi_section_id'] == int(csi_section_id)]
    if len(match) > 0:
        return match.iloc[0]['csi_section']
    return None


def get_csi_section_title(csi_section_id: int) -> Optional[str]:
    """Get CSI section title from csi_section_id."""
    if csi_section_id is None or pd.isna(csi_section_id):
        return None

    _load_csi_sections()

    if _dim_csi_section.empty:
        return None

    match = _dim_csi_section[_dim_csi_section['csi_section_id'] == int(csi_section_id)]
    if len(match) > 0:
        return match.iloc[0]['csi_title']
    return None


def _load_csi_divisions():
    """Load CSI division dimension table if not already loaded."""
    global _dim_csi_division
    if _dim_csi_division is None:
        csi_div_path = _dimensions_dir / 'dim_csi_division.csv'
        if csi_div_path.exists():
            _dim_csi_division = pd.read_csv(csi_div_path)
        else:
            _dim_csi_division = pd.DataFrame()


def get_csi_division(csi_section: str) -> Optional[str]:
    """
    Get CSI division code from CSI section code.

    Args:
        csi_section: CSI section code like "03 30 00" or "07 84 00"

    Returns:
        Division code like "03" or "07", or None if invalid
    """
    if not csi_section or pd.isna(csi_section):
        return None

    # Extract division from section code (first 2 digits)
    code = str(csi_section).strip()

    # Match pattern: "XX ..." where XX is the division
    import re
    match = re.match(r'^(\d{2})', code)
    if match:
        return match.group(1)

    return None


def enrich_dataframe(
    df: pd.DataFrame,
    building_col: str = 'building',
    level_col: str = 'level',
    company_col: str = None,
) -> pd.DataFrame:
    """
    Enrich a dataframe with dimension IDs.

    Adds columns:
    - dim_location_id: integer FK from building + level
    - building_level: string for display/filtering
    - dim_company_id: from company column (if specified)

    Note: For work type classification, use dim_csi_section_id via
    scripts/integrated_analysis/add_csi_to_*.py instead of dim_trade_id.

    Args:
        df: Input dataframe
        building_col: Column name for building
        level_col: Column name for level
        company_col: Column name for company (optional)

    Returns:
        Dataframe with added dimension columns
    """
    result = df.copy()

    # Add location_id (integer FK) and building_level (string for display)
    if building_col in df.columns and level_col in df.columns:
        result['dim_location_id'] = df.apply(
            lambda row: get_location_id(row.get(building_col), row.get(level_col)),
            axis=1
        )
        result['building_level'] = df.apply(
            lambda row: get_building_level(row.get(building_col), row.get(level_col)),
            axis=1
        )

    # Add company_id
    if company_col and company_col in df.columns:
        result['dim_company_id'] = df[company_col].apply(get_company_id)

    return result


def get_coverage_stats(
    df: pd.DataFrame,
    location_col: str = 'dim_location_id',
    company_col: str = 'dim_company_id',
    csi_col: str = 'dim_csi_section_id',
) -> Dict[str, Dict[str, float]]:
    """
    Calculate coverage statistics for dimension columns.

    Returns:
        Dict with coverage stats for each dimension
    """
    stats = {}

    for col_name, col in [('location', location_col), ('company', company_col), ('csi_section', csi_col)]:
        if col in df.columns:
            total = len(df)
            mapped = df[col].notna().sum()
            stats[col_name] = {
                'total': total,
                'mapped': mapped,
                'coverage': mapped / total if total > 0 else 0,
                'unmapped': total - mapped,
            }

    return stats


def reset_cache():
    """Reset cached dimension data (useful for testing)."""
    global _dim_location, _dim_company, _map_company_aliases
    global _building_level_to_id, _location_code_to_id, _p6_alias_to_id, _dim_csi_section
    global _yates_sub_ids, _yates_self_ids
    _dim_location = None
    _dim_company = None
    _map_company_aliases = None
    _building_level_to_id = None
    _location_code_to_id = None
    _p6_alias_to_id = None
    _dim_csi_section = None
    _yates_sub_ids = None
    _yates_self_ids = None


# =============================================================================
# Grid Coordinate Parsing
# =============================================================================

# Valid grid row letters
# Primary rows: A-N (no 'I' in standard grid system)
# Extended rows: O, P, Q, R, S found in some areas (CUB, external)
VALID_GRID_ROWS = set('ABCDEFGHJKLMNOPQRS')  # Expanded based on actual data

# Pattern for single grid coordinate: letter(s), optional decimal, slash, digits
# Examples: G/10, E.5/17.3, N/5, A26, O/23, R/28
GRID_COORD_PATTERN = re.compile(
    r'([A-S])(?:\.(\d+))?[/\-]?(\d+)(?:\.(\d+))?',
    re.IGNORECASE
)

# Pattern for simple grid like "A26" or "L5" (letter followed by digits, no slash)
GRID_SIMPLE_PATTERN = re.compile(r'([A-S])(\d+)(?:\.(\d+))?', re.IGNORECASE)


def parse_single_grid(coord: str) -> Optional[Dict[str, any]]:
    """
    Parse a single grid coordinate string.

    Args:
        coord: Grid coordinate like "G/10", "E.5/17.3", "N/5", "A26"

    Returns:
        Dict with 'row', 'row_decimal', 'col', 'col_decimal' or None if invalid
    """
    if not coord or pd.isna(coord):
        return None

    coord = str(coord).strip().upper()

    # Try standard row/col format first (e.g., "G/10", "E.5/17.3")
    if '/' in coord or '-' in coord:
        match = GRID_COORD_PATTERN.match(coord)
        if match:
            row_letter = match.group(1)
            row_decimal = match.group(2)  # May be None
            col_int = match.group(3)
            col_decimal = match.group(4)  # May be None

            if row_letter in VALID_GRID_ROWS and col_int:
                row = row_letter
                if row_decimal:
                    row = f"{row_letter}.{row_decimal}"
                col = float(col_int)
                if col_decimal:
                    col = float(f"{col_int}.{col_decimal}")
                return {'row': row, 'col': col}

    # Try simple format (e.g., "A26", "L5")
    match = GRID_SIMPLE_PATTERN.match(coord)
    if match:
        row_letter = match.group(1)
        col_int = match.group(2)
        col_decimal = match.group(3)

        if row_letter in VALID_GRID_ROWS and col_int:
            col = float(col_int)
            if col_decimal:
                col = float(f"{col_int}.{col_decimal}")
            return {'row': row_letter, 'col': col}

    return None


def parse_grid_field(grid_str: str) -> Dict[str, Optional[str]]:
    """
    Parse a grid field which may contain multiple coordinates.

    Args:
        grid_str: Grid string like "G/10", "F.6/18,F.8/18,E.8/18", "N/5",
                  "C/11 - C/22" (TBM range format)

    Returns:
        Dict with:
        - grid_row_min: Minimum row letter (e.g., "E")
        - grid_row_max: Maximum row letter (e.g., "G")
        - grid_col_min: Minimum column number (e.g., 17.0)
        - grid_col_max: Maximum column number (e.g., 18.0)
        - grid_rows: All unique rows as comma-separated string (e.g., "E,F,G")
        - grid_cols: All unique cols as comma-separated string (e.g., "17,18")
    """
    result = {
        'grid_row_min': None,
        'grid_row_max': None,
        'grid_col_min': None,
        'grid_col_max': None,
        'grid_rows': None,
        'grid_cols': None,
    }

    if not grid_str or pd.isna(grid_str):
        return result

    grid_str = str(grid_str).strip()

    # Handle TBM range format "C/11 - C/22" by splitting on " - " first
    # This converts "C/11 - C/22" to ["C/11", "C/22"]
    if ' - ' in grid_str:
        grid_str = grid_str.replace(' - ', ',')

    # Split by comma to handle multiple coordinates
    parts = [p.strip() for p in grid_str.split(',')]

    rows = []
    cols = []

    for part in parts:
        # Skip lab addresses like "B-150" (letter + hyphen + 3+ digits)
        # But NOT row-range patterns like "B-D/8-12" which have a letter after the hyphen
        lab_address_pattern = re.match(r'^[A-Z]-\d{3,}$', part, re.IGNORECASE)
        if lab_address_pattern:
            continue

        # Try to parse row-range patterns first (e.g., "A-N/1-3", "J-K/33")
        # Pattern: LETTER-LETTER/COL or LETTER-LETTER/COL-COL
        row_range_match = re.match(
            r'^([A-S])\s*-\s*([A-S])\s*/\s*(\d+)(?:\s*-\s*(\d+))?$',
            part,
            re.IGNORECASE
        )
        if row_range_match:
            row_min = row_range_match.group(1).upper()
            row_max = row_range_match.group(2).upper()
            col_min = float(row_range_match.group(3))
            col_max = float(row_range_match.group(4)) if row_range_match.group(4) else col_min

            # Expand rows in the range (A-N becomes A, B, C, ..., N)
            if row_min in VALID_GRID_ROWS and row_max in VALID_GRID_ROWS:
                # Get all letters between min and max
                start_idx = ord(row_min)
                end_idx = ord(row_max)
                for i in range(start_idx, end_idx + 1):
                    letter = chr(i)
                    if letter in VALID_GRID_ROWS:
                        rows.append(letter)
                # Add columns
                cols.append(col_min)
                if col_max != col_min:
                    cols.append(col_max)
                continue

        parsed = parse_single_grid(part)
        if parsed:
            rows.append(parsed['row'])
            cols.append(parsed['col'])

    if rows and cols:
        # Sort rows alphabetically (A < B < ... < N)
        # Handle fractional rows by sorting base letter first
        def row_sort_key(r):
            base = r[0]
            decimal = float(r[2:]) if len(r) > 1 and '.' in r else 0
            return (base, decimal)

        unique_rows = sorted(set(rows), key=row_sort_key)
        unique_cols = sorted(set(cols))

        result['grid_row_min'] = unique_rows[0]
        result['grid_row_max'] = unique_rows[-1]
        result['grid_col_min'] = unique_cols[0]
        result['grid_col_max'] = unique_cols[-1]
        result['grid_rows'] = ','.join(unique_rows)
        result['grid_cols'] = ','.join(str(c) for c in unique_cols)

    return result


def normalize_grid(grid_str: str) -> Optional[str]:
    """
    Normalize a grid string to standard format.

    Removes extraneous data (lab addresses, etc.) and standardizes format.

    Args:
        grid_str: Raw grid string

    Returns:
        Normalized grid string with only valid coordinates, comma-separated
    """
    if not grid_str or pd.isna(grid_str):
        return None

    grid_str = str(grid_str).strip()
    parts = [p.strip() for p in grid_str.split(',')]

    valid_coords = []
    for part in parts:
        # Skip non-grid parts
        if part.startswith('B-') and len(part) > 3:
            continue

        parsed = parse_single_grid(part)
        if parsed:
            # Reconstruct standardized format
            row = parsed['row']
            col = parsed['col']
            # Format column: use integer if whole number
            if col == int(col):
                col_str = str(int(col))
            else:
                col_str = str(col)
            valid_coords.append(f"{row}/{col_str}")

    return ','.join(valid_coords) if valid_coords else None
