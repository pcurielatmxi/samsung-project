"""
Dimension Lookup Module

Provides functions to map raw data values to dimension table IDs.
Used by all data source consolidation scripts for consistent integration.

Dimension Tables (from scripts/integrated_analysis/dimensions/):
- dim_location: building + level → location_id
- dim_company: company name → company_id
- dim_trade: trade/category → trade_id
"""

import sys
from pathlib import Path
from typing import Optional, Dict
import re

import pandas as pd

# Project paths
_project_root = Path(__file__).parent.parent.parent
_dimensions_dir = _project_root / 'scripts' / 'integrated_analysis' / 'dimensions'
_mappings_dir = _project_root / 'scripts' / 'integrated_analysis' / 'mappings'


# Cached dimension data
_dim_location: Optional[pd.DataFrame] = None
_dim_company: Optional[pd.DataFrame] = None
_dim_trade: Optional[pd.DataFrame] = None
_map_company_aliases: Optional[pd.DataFrame] = None


def _load_dimensions():
    """Load dimension tables if not already loaded."""
    global _dim_location, _dim_company, _dim_trade, _map_company_aliases

    if _dim_location is None:
        _dim_location = pd.read_csv(_dimensions_dir / 'dim_location.csv')

    if _dim_company is None:
        _dim_company = pd.read_csv(_dimensions_dir / 'dim_company.csv')

    if _dim_trade is None:
        _dim_trade = pd.read_csv(_dimensions_dir / 'dim_trade.csv')

    if _map_company_aliases is None:
        _map_company_aliases = pd.read_csv(_mappings_dir / 'map_company_aliases.csv')


def get_location_id(building: str, level: str) -> Optional[str]:
    """
    Get dim_location_id from building and level.

    Args:
        building: Building code (FAB, SUE, SUW, FIZ)
        level: Level code (1F, 2F, B1, ROOF, etc.)

    Returns:
        location_id like "SUE-1F" or None if not found
    """
    if not building or not level:
        return None

    _load_dimensions()

    building = str(building).upper().strip()
    level = str(level).upper().strip()

    # Construct the location_id
    location_id = f"{building}-{level}"

    # Check if it exists in dim_location
    if location_id in _dim_location['location_id'].values:
        return location_id

    return None


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
    - dim_location_id: from building + level
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

    # Add location_id
    if building_col in df.columns and level_col in df.columns:
        result['dim_location_id'] = df.apply(
            lambda row: get_location_id(row.get(building_col), row.get(level_col)),
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
    global _dim_location, _dim_company, _dim_trade, _map_company_aliases
    _dim_location = None
    _dim_company = None
    _dim_trade = None
    _map_company_aliases = None
