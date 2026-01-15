"""
Dimension Lookup Module

Provides functions to map raw data values to dimension table IDs.
Used by all data source consolidation scripts for consistent integration.

Dimension Tables (from processed/integrated_analysis/dimensions/):
- dim_location: location codes (rooms, elevators, stairs) with grid bounds
- dim_company: company name → company_id
- dim_trade: trade/category → trade_id

Mapping Tables (from processed/integrated_analysis/mappings/):
- map_company_aliases: company name variants → company_id
- map_projectsight_trade: ProjectSight trade names → dim_trade_id

Location Lookup:
- get_location_id(building, level) → integer location_id from dim_location
- get_building_level(building, level) → building_level string (e.g., "FAB-1F")
- get_locations_at_grid(building, level, row, col) → list of location_codes
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
_dim_trade: Optional[pd.DataFrame] = None
_map_company_aliases: Optional[pd.DataFrame] = None
_building_level_to_id: Optional[Dict[str, int]] = None


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
    global _dim_location, _dim_company, _dim_trade, _map_company_aliases, _building_level_to_id

    if _dim_location is None:
        _dim_location = pd.read_csv(_dimensions_dir / 'dim_location.csv')
        # Build building_level -> location_id lookup
        # For building_levels with multiple entries, prefer LEVEL type, then lowest ID
        _building_level_to_id = {}
        for _, row in _dim_location.sort_values(['location_type', 'location_id']).iterrows():
            bl = row.get('building_level')
            if bl and pd.notna(bl) and bl not in _building_level_to_id:
                _building_level_to_id[bl] = int(row['location_id'])

    if _dim_company is None:
        _dim_company = pd.read_csv(_dimensions_dir / 'dim_company.csv')

    if _dim_trade is None:
        _dim_trade = pd.read_csv(_dimensions_dir / 'dim_trade.csv')

    if _map_company_aliases is None:
        _map_company_aliases = pd.read_csv(_mappings_dir / 'map_company_aliases.csv')


def get_location_id(building: str, level: str, allow_fallback: bool = True) -> Optional[int]:
    """
    Get location_id (integer FK) from building and level.

    Returns the location_id from dim_location for the given building-level.
    Prefers LEVEL type entries over ROOM/STAIR/etc for building-level aggregation.

    Fallback behavior (when allow_fallback=True):
    1. Try building + level first (e.g., "FAB-1F")
    2. If level is missing, try building + ALL (e.g., "FAB-ALL")
    3. If building is also missing, try "SITE"

    Args:
        building: Building code (FAB, SUE, SUW, FIZ, OB1, GCS)
        level: Level code (1F, 2F, B1, ROOF, etc.)
        allow_fallback: If True, fall back to building-wide or site-wide entries

    Returns:
        Integer location_id or None if not found in dim_location
    """
    _load_dimensions()

    has_building = building and pd.notna(building) and str(building).strip()
    has_level = level and pd.notna(level) and str(level).strip()

    # Case 1: Have both building and level - try exact match
    if has_building and has_level:
        building = str(building).upper().strip()
        level = _normalize_level(str(level).upper().strip())
        building_level = f"{building}-{level}"
        loc_id = _building_level_to_id.get(building_level)
        if loc_id is not None:
            return loc_id

    # Case 2: Have building but no level - try building-wide
    if allow_fallback and has_building and not has_level:
        building = str(building).upper().strip()
        building_wide = f"{building}-ALL"
        loc_id = _building_level_to_id.get(building_wide)
        if loc_id is not None:
            return loc_id

    # Case 3: No building - try site-wide
    if allow_fallback and not has_building:
        loc_id = _building_level_to_id.get('SITE')
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
    building: str,
    level: str,
    grid_row: str,
    grid_col: float
) -> List[Dict]:
    """
    Find all locations (rooms, elevators, etc.) whose grid bounds contain the given point.

    This enables spatial joins from quality inspection grid coordinates to room codes.

    Args:
        building: Building code (FAB, SUE, SUW)
        level: Level code (1F, 2F, B1, etc.)
        grid_row: Grid row letter (A-N, may include decimal like "E.5")
        grid_col: Grid column number (1-34, may include decimal like 17.5)

    Returns:
        List of dicts with location_id, location_code, location_type, room_name
    """
    if not building or not level or not grid_row or grid_col is None:
        return []

    _load_dimensions()

    building = str(building).upper().strip()
    level = str(level).upper().strip()
    grid_row = str(grid_row).upper().strip()

    # Filter by building and level first
    candidates = _dim_location[
        (_dim_location['building'] == building) &
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
                    'location_id': row['location_id'],
                    'location_code': row['location_code'],
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
    building: str,
    level: str,
    grid_row_min: Optional[str] = None,
    grid_row_max: Optional[str] = None,
    grid_col_min: Optional[float] = None,
    grid_col_max: Optional[float] = None,
) -> List[Dict]:
    """
    Find all rooms/locations whose grid bounds overlap with the given grid range.

    Returns a list of affected rooms with a PARTIAL flag when grid info is incomplete.

    Args:
        building: Building code (FAB, SUE, SUW)
        level: Level code (1F, 2F, B1, etc.)
        grid_row_min: Starting row letter (A-N, may include decimal like "E.5")
        grid_row_max: Ending row letter (defaults to row_min if not provided)
        grid_col_min: Starting column number (1-34)
        grid_col_max: Ending column number (defaults to col_min if not provided)

    Returns:
        List of dicts with:
        - location_code: Standardized room code
        - room_name: Room description
        - match_type: FULL (both row+col match) or PARTIAL (only row or col)

        Returns empty list if no grid information provided.
    """
    if not building or not level:
        return []

    # Determine what grid info we have
    has_row = grid_row_min is not None and pd.notna(grid_row_min)
    has_col = grid_col_min is not None and pd.notna(grid_col_min)

    # Need at least some grid info
    if not has_row and not has_col:
        return []

    _load_dimensions()

    building = str(building).upper().strip()
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

    # Filter locations by building and level, excluding non-room types
    # Include ROOM, ELEVATOR, STAIR (exclude LEVEL, BUILDING, AREA, GRIDLINE)
    candidates = _dim_location[
        (_dim_location['building'] == building) &
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
                        'location_code': loc['location_code'],
                        'room_name': loc['room_name'],
                        'match_type': 'FULL',
                    })

            elif has_row:
                # Row only - check row overlap (PARTIAL match)
                if _rows_overlap(row_min, row_max, loc_row_min, loc_row_max):
                    results.append({
                        'location_code': loc['location_code'],
                        'room_name': loc['room_name'],
                        'match_type': 'PARTIAL',
                    })

            elif has_col:
                # Column only - check column overlap (PARTIAL match)
                if _cols_overlap(col_min, col_max, loc_col_min, loc_col_max):
                    results.append({
                        'location_code': loc['location_code'],
                        'room_name': loc['room_name'],
                        'match_type': 'PARTIAL',
                    })

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


# Trade name to trade_id mapping
# Maps various names used in quality data to dim_trade trade_id
TRADE_NAME_TO_ID: Dict[str, int] = {
    # Concrete (trade_id=1)
    'concrete': 1,
    'baker concrete': 1,
    'cast-in-place': 1,
    'cip': 1,
    'topping': 1,
    'slab': 1,

    # Structural Steel (trade_id=2)
    'structural steel': 2,
    'steel': 2,
    'welding': 2,
    'steel erection': 2,
    'decking': 2,
    'misc steel': 2,

    # Roofing (trade_id=3)
    'roofing': 3,
    'roofing & waterproofing': 3,
    'waterproofing': 3,
    'membrane': 3,
    'eifs': 3,

    # Drywall (trade_id=4)
    'drywall': 4,
    'drywall & framing': 4,
    'framing': 4,
    'gypsum': 4,
    'metal stud': 4,
    'architecture / framing & drywall': 4,

    # Finishes (trade_id=5)
    'finishes': 5,
    'architectural finishes': 5,
    'architectural': 5,
    'painting': 5,
    'paint': 5,
    'coating/painting': 5,
    'coating': 5,
    'flooring': 5,
    'tile': 5,
    'ceilings': 5,
    'doors': 5,

    # Fire Protection (trade_id=6)
    'fire protection': 6,
    'fireproof': 6,
    'fireproofing': 6,
    'firestop': 6,
    'fire caulk': 6,
    'sfrm': 6,

    # MEP (trade_id=7)
    'mep': 7,
    'mep systems': 7,
    'mechanical': 7,
    'electrical': 7,
    'plumbing': 7,
    'hvac': 7,

    # Insulation (trade_id=8)
    'insulation': 8,
    'thermal insulation': 8,
    'pipe insulation': 8,

    # Earthwork (trade_id=9)
    'earthwork': 9,
    'earthwork & foundations': 9,
    'soil/earthwork': 9,
    'excavation': 9,
    'backfill': 9,
    'grading': 9,
    'drilled pier/foundation': 9,
    'deep foundations': 9,
    'reinforcing steel': 9,  # Often part of foundation work

    # Precast (trade_id=10)
    'precast': 10,
    'precast concrete': 10,

    # Panels (trade_id=11)
    'panels': 11,
    'metal panels': 11,
    'metal panels & cladding': 11,
    'cladding': 11,
    'imp': 11,
    'skin': 11,

    # General (trade_id=12)
    'general': 12,
    'general conditions': 12,
    'visual/general': 12,
    'general requirements': 12,
    'existing conditions': 12,
    'special construction': 12,
    'transportation': 12,

    # Masonry (trade_id=13)
    'masonry': 13,
    'cmu': 13,
    'block': 13,
    'brick': 13,

    # ProjectSight-specific mappings (CSI divisions)
    'metals': 2,  # CSI Div 05 - Metals = Structural Steel
    'iron': 2,  # Iron work is steel/metal work
    'thermal and moisture protection': 8,  # CSI Div 07 - Insulation
    'openings': 5,  # CSI Div 08 - Doors, windows = Finishes
    'equipment': 7,  # CSI Div 11 - Equipment = MEP
    'furnishings': 5,  # CSI Div 12 - Furnishings = Finishes
    'woods, plastics, and composites': 4,  # CSI Div 06 = Drywall/framing
    'specialties': 5,  # CSI Div 10 = Finishes

    # RABA test type patterns (quality inspections)
    'moisture-density': 9,  # Soil testing = Earthwork
    'atterberg': 9,  # Soil testing = Earthwork
    'sieve analysis': 9,  # Soil testing = Earthwork
    'drilled epoxied dowels': 1,  # Concrete anchorage
    'post installed embeds': 1,  # Concrete anchorage
    'post-installed embeds': 1,  # Concrete anchorage
    'dowel remediation': 1,  # Concrete repair
    'dowel installation': 1,  # Concrete anchorage
    'curb repair': 1,  # Concrete repair
    'column patching': 1,  # Concrete repair
    'ifrm': 6,  # Intumescent fire resistive material
    'sfrm-substrate': 6,  # SFRM substrate inspection
    'substrate condition': 6,  # Often for fireproofing substrate
    'load bearing': 1,  # Load testing typically concrete/structure
    'construction quality control': 12,  # General QC
    'laboratory testing': 12,  # General testing
    'observation': 4,  # Often drywall screws observation

    # TBM work activity patterns
    'delamination': 1,  # Concrete patching
    'patching': 1,  # Concrete repair
    'chipping': 1,  # Concrete chipping
    'elevator front clips': 2,  # Steel work
    'elevator clips': 2,  # Steel work
    'laydown yard': 12,  # General conditions
    'laydown': 12,  # General conditions

    # PSI trade patterns
    'arch / yates': 5,  # Architectural = Finishes
    'arch / dwl': 4,  # Architectural drywall = Drywall
    'arch/yates': 5,
    'arch/dwl': 4,
}


def get_trade_id(trade_name: str) -> Optional[int]:
    """
    Get dim_trade_id from trade name or category.

    Args:
        trade_name: Trade name (e.g., "Drywall", "Concrete", "Structural Steel")
                   or category (e.g., "Coating/Painting", "Firestop")

    Returns:
        trade_id integer or None if not found
    """
    if not trade_name or pd.isna(trade_name):
        return None

    _load_dimensions()

    name = str(trade_name).strip().lower()

    # Direct lookup in mapping
    if name in TRADE_NAME_TO_ID:
        return TRADE_NAME_TO_ID[name]

    # Try partial match
    for key, trade_id in TRADE_NAME_TO_ID.items():
        if key in name or name in key:
            return trade_id

    # Try matching against dim_trade directly
    for _, row in _dim_trade.iterrows():
        trade_code = str(row['trade_code']).lower()
        trade_name_dim = str(row['trade_name']).lower()
        if name == trade_code or name == trade_name_dim:
            return int(row['trade_id'])
        if trade_code in name or name in trade_code:
            return int(row['trade_id'])

    return None


def get_trade_code(trade_id: int) -> Optional[str]:
    """Get trade_code from trade_id."""
    if trade_id is None or pd.isna(trade_id):
        return None

    _load_dimensions()

    match = _dim_trade[_dim_trade['trade_id'] == trade_id]
    if len(match) > 0:
        return match.iloc[0]['trade_code']
    return None


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


def enrich_dataframe(
    df: pd.DataFrame,
    building_col: str = 'building',
    level_col: str = 'level',
    company_col: str = None,
    trade_col: str = None,
) -> pd.DataFrame:
    """
    Enrich a dataframe with dimension IDs.

    Adds columns:
    - dim_location_id: integer FK from building + level
    - building_level: string for display/filtering
    - dim_company_id: from company column (if specified)
    - dim_trade_id: from trade column (if specified)
    - dim_trade_code: trade code for readability

    Args:
        df: Input dataframe
        building_col: Column name for building
        level_col: Column name for level
        company_col: Column name for company (optional)
        trade_col: Column name for trade (optional)

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

    # Add trade_id
    if trade_col and trade_col in df.columns:
        result['dim_trade_id'] = df[trade_col].apply(get_trade_id)
        result['dim_trade_code'] = result['dim_trade_id'].apply(get_trade_code)

    return result


def get_coverage_stats(
    df: pd.DataFrame,
    location_col: str = 'dim_location_id',
    company_col: str = 'dim_company_id',
    trade_col: str = 'dim_trade_id',
) -> Dict[str, Dict[str, float]]:
    """
    Calculate coverage statistics for dimension columns.

    Returns:
        Dict with coverage stats for each dimension
    """
    stats = {}

    for col_name, col in [('location', location_col), ('company', company_col), ('trade', trade_col)]:
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
    global _dim_location, _dim_company, _dim_trade, _map_company_aliases, _building_level_to_id, _dim_csi_section
    _dim_location = None
    _dim_company = None
    _dim_trade = None
    _map_company_aliases = None
    _building_level_to_id = None
    _dim_csi_section = None


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
        grid_str: Grid string like "G/10", "F.6/18,F.8/18,E.8/18", "N/5"

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

    # Split by comma to handle multiple coordinates
    parts = [p.strip() for p in grid_str.split(',')]

    rows = []
    cols = []

    for part in parts:
        # Skip non-grid parts (e.g., "B-150" is a lab address, not a grid)
        if part.startswith('B-') and len(part) > 3:
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
