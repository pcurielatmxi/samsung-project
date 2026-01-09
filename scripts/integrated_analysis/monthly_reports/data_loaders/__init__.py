"""Data loaders for monthly report consolidation.

Each loader fetches data for a specific month from one or more sources.
All loaders return DataFrames with dimension IDs for aggregation.
"""

from .base import MonthlyPeriod, get_monthly_period
from .primavera import load_schedule_data
from .labor import load_labor_data
from .quality import load_quality_data
from .narratives import load_narrative_data

__all__ = [
    'MonthlyPeriod',
    'get_monthly_period',
    'load_schedule_data',
    'load_labor_data',
    'load_quality_data',
    'load_narrative_data',
]
