"""Data loaders for snapshot reports.

Provides snapshot period handling and data loading for all sources,
using P6 data dates as period boundaries instead of calendar months.
"""

from .base import (
    SnapshotPeriod,
    DataAvailability,
    get_all_snapshot_periods,
    get_snapshot_period,
    filter_by_period,
    get_date_range,
    list_periods,
)

from .primavera import (
    ProgressSummary,
    load_schedule_data,
)

from .quality import (
    load_quality_data,
)

from .labor import (
    load_labor_data,
)

from .narratives import (
    load_narrative_data,
)

from .dimensions import (
    get_company_lookup,
    resolve_company_id,
    resolve_trade_id,
)

__all__ = [
    # Base utilities
    'SnapshotPeriod',
    'DataAvailability',
    'get_all_snapshot_periods',
    'get_snapshot_period',
    'filter_by_period',
    'get_date_range',
    'list_periods',
    # Primavera
    'ProgressSummary',
    'load_schedule_data',
    # Quality
    'load_quality_data',
    # Labor
    'load_labor_data',
    # Narratives
    'load_narrative_data',
    # Dimensions
    'get_company_lookup',
    'resolve_company_id',
    'resolve_trade_id',
]
