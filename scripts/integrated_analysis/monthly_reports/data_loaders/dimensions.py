"""Dimension table lookups for resolving IDs to names.

Provides cached lookups for company, trade, and location dimensions.
Includes GC-subcontractor hierarchy support.
"""

import sys
from pathlib import Path
from typing import Dict, Optional, List
import pandas as pd

_project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import Settings

# Cache for dimension tables
_company_lookup: Optional[Dict[int, str]] = None
_company_df: Optional[pd.DataFrame] = None
_trade_lookup: Optional[Dict[int, str]] = None
_location_lookup: Optional[Dict[str, str]] = None


def _get_dimensions_dir() -> Path:
    """Get path to dimension tables."""
    return Settings.PROCESSED_DATA_DIR / 'integrated_analysis' / 'dimensions'


def _load_company_df() -> pd.DataFrame:
    """Load and cache the full company dimension DataFrame."""
    global _company_df

    if _company_df is not None:
        return _company_df

    dim_path = _get_dimensions_dir() / 'dim_company.csv'

    if not dim_path.exists():
        _company_df = pd.DataFrame()
        return _company_df

    _company_df = pd.read_csv(dim_path)
    return _company_df


def get_company_lookup() -> Dict[int, str]:
    """Load company dimension and return ID->name lookup.

    Returns:
        Dict mapping company_id (int) to canonical_name (str)
    """
    global _company_lookup

    if _company_lookup is not None:
        return _company_lookup

    df = _load_company_df()

    if df.empty:
        _company_lookup = {}
        return _company_lookup

    _company_lookup = dict(zip(df['company_id'].astype(int), df['canonical_name']))

    return _company_lookup


def get_trade_lookup() -> Dict[int, str]:
    """Load trade dimension and return ID->name lookup.

    Returns:
        Dict mapping trade_id (int) to trade_name (str)
    """
    global _trade_lookup

    if _trade_lookup is not None:
        return _trade_lookup

    dim_path = _get_dimensions_dir() / 'dim_trade.csv'

    if not dim_path.exists():
        _trade_lookup = {}
        return _trade_lookup

    df = pd.read_csv(dim_path)
    _trade_lookup = dict(zip(df['trade_id'].astype(int), df['trade_name']))

    return _trade_lookup


def resolve_company_id(company_id) -> str:
    """Resolve a company ID to its canonical name.

    Args:
        company_id: Company ID (int or float)

    Returns:
        Company name or "Unknown ({id})" if not found
    """
    if pd.isna(company_id):
        return "Unknown"

    lookup = get_company_lookup()
    company_id_int = int(company_id)

    return lookup.get(company_id_int, f"Unknown ({company_id_int})")


def resolve_trade_id(trade_id) -> str:
    """Resolve a trade ID to its name.

    Args:
        trade_id: Trade ID (int or float)

    Returns:
        Trade name or "Unknown ({id})" if not found
    """
    if pd.isna(trade_id):
        return "Unknown"

    lookup = get_trade_lookup()
    trade_id_int = int(trade_id)

    return lookup.get(trade_id_int, f"Unknown ({trade_id_int})")


def resolve_company_column(df: pd.DataFrame, id_column: str = 'dim_company_id') -> pd.Series:
    """Resolve all company IDs in a DataFrame column to names.

    Args:
        df: DataFrame with company ID column
        id_column: Name of the ID column

    Returns:
        Series of resolved company names
    """
    if id_column not in df.columns:
        return pd.Series(['Unknown'] * len(df))

    lookup = get_company_lookup()

    def resolve(x):
        if pd.isna(x):
            return "Unknown"
        return lookup.get(int(x), f"Unknown ({int(x)})")

    return df[id_column].apply(resolve)


# =============================================================================
# Company Hierarchy Functions
# =============================================================================

def get_parent_company_id(company_id) -> Optional[int]:
    """Get the parent (GC) company ID for a subcontractor.

    Args:
        company_id: Company ID (int or float)

    Returns:
        Parent company_id or None if no parent (is a GC or owner)
    """
    if pd.isna(company_id):
        return None

    df = _load_company_df()
    if df.empty or 'parent_company_id' not in df.columns:
        return None

    company_id_int = int(company_id)
    match = df[df['company_id'] == company_id_int]

    if match.empty:
        return None

    parent_id = match.iloc[0]['parent_company_id']
    if pd.isna(parent_id):
        return None

    return int(parent_id)


def get_subcontractors(gc_company_id: int) -> List[Dict]:
    """Get all subcontractors under a general contractor.

    Args:
        gc_company_id: GC company ID

    Returns:
        List of dicts with company_id, canonical_name, tier, confidence
    """
    df = _load_company_df()
    if df.empty or 'parent_company_id' not in df.columns:
        return []

    subs = df[df['parent_company_id'] == gc_company_id]

    result = []
    for _, row in subs.iterrows():
        result.append({
            'company_id': int(row['company_id']),
            'canonical_name': row['canonical_name'],
            'tier': row.get('tier', 'T1_SUB'),
            'confidence': row.get('parent_confidence', 'UNKNOWN'),
        })

    return result


def get_gc_for_company(company_id) -> Optional[Dict]:
    """Get the GC information for a company.

    If the company is itself a GC, returns its own info.
    If it's a sub, returns the parent GC info.

    Args:
        company_id: Company ID

    Returns:
        Dict with company_id, canonical_name, or None
    """
    if pd.isna(company_id):
        return None

    df = _load_company_df()
    if df.empty:
        return None

    company_id_int = int(company_id)
    match = df[df['company_id'] == company_id_int]

    if match.empty:
        return None

    row = match.iloc[0]

    # If it's a GC itself, return its own info
    if row.get('tier') == 'GC':
        return {
            'company_id': company_id_int,
            'canonical_name': row['canonical_name'],
        }

    # If it has a parent, return parent info
    if 'parent_company_id' in df.columns and pd.notna(row.get('parent_company_id')):
        parent_id = int(row['parent_company_id'])
        parent_match = df[df['company_id'] == parent_id]
        if not parent_match.empty:
            parent_row = parent_match.iloc[0]
            return {
                'company_id': parent_id,
                'canonical_name': parent_row['canonical_name'],
            }

    return None


def get_company_tier(company_id) -> Optional[str]:
    """Get the tier (OWNER, GC, T1_SUB, etc.) for a company.

    Args:
        company_id: Company ID

    Returns:
        Tier string or None
    """
    if pd.isna(company_id):
        return None

    df = _load_company_df()
    if df.empty:
        return None

    company_id_int = int(company_id)
    match = df[df['company_id'] == company_id_int]

    if match.empty:
        return None

    return match.iloc[0].get('tier')
