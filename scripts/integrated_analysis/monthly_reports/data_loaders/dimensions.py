"""Dimension table lookups for resolving IDs to names.

Provides cached lookups for company, trade, and location dimensions.
"""

import sys
from pathlib import Path
from typing import Dict, Optional
import pandas as pd

_project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import Settings

# Cache for dimension tables
_company_lookup: Optional[Dict[int, str]] = None
_trade_lookup: Optional[Dict[int, str]] = None
_location_lookup: Optional[Dict[str, str]] = None


def _get_dimensions_dir() -> Path:
    """Get path to dimension tables."""
    return Settings.DERIVED_DATA_DIR / 'integrated_analysis' / 'dimensions'


def get_company_lookup() -> Dict[int, str]:
    """Load company dimension and return ID->name lookup.

    Returns:
        Dict mapping company_id (int) to canonical_name (str)
    """
    global _company_lookup

    if _company_lookup is not None:
        return _company_lookup

    dim_path = _get_dimensions_dir() / 'dim_company.csv'

    if not dim_path.exists():
        _company_lookup = {}
        return _company_lookup

    df = pd.read_csv(dim_path)
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
