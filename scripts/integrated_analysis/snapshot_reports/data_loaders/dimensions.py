"""Dimension table lookups for resolving IDs to names in snapshot reports.

Re-exports functionality from monthly_reports dimensions module.
"""

import sys
from pathlib import Path

_project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

# Re-export from monthly_reports dimensions module
from scripts.integrated_analysis.monthly_reports.data_loaders.dimensions import (
    get_company_lookup,
    get_trade_lookup,
    resolve_company_id,
    resolve_trade_id,
    resolve_company_column,
    get_parent_company_id,
    get_subcontractors,
    get_gc_for_company,
    get_company_tier,
)

__all__ = [
    'get_company_lookup',
    'get_trade_lookup',
    'resolve_company_id',
    'resolve_trade_id',
    'resolve_company_column',
    'get_parent_company_id',
    'get_subcontractors',
    'get_gc_for_company',
    'get_company_tier',
]
