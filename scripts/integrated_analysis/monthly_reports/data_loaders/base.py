"""Base utilities for monthly data loading.

Provides:
- MonthlyPeriod dataclass for consistent date handling
- Date parsing and filtering utilities
- Data availability tracking
"""

import sys
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Optional, Dict, Any, List
import pandas as pd

# Add project root to path
_project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import Settings


@dataclass
class MonthlyPeriod:
    """Represents a monthly reporting period."""
    year: int
    month: int

    @property
    def start_date(self) -> date:
        """First day of the month."""
        return date(self.year, self.month, 1)

    @property
    def end_date(self) -> date:
        """Last day of the month."""
        if self.month == 12:
            return date(self.year + 1, 1, 1) - pd.Timedelta(days=1)
        return date(self.year, self.month + 1, 1) - pd.Timedelta(days=1)

    @property
    def label(self) -> str:
        """YYYY-MM format for display."""
        return f"{self.year}-{self.month:02d}"

    def contains(self, dt: date) -> bool:
        """Check if a date falls within this period."""
        if pd.isna(dt):
            return False
        if isinstance(dt, pd.Timestamp):
            dt = dt.date()
        return self.start_date <= dt <= self.end_date

    def __str__(self) -> str:
        return self.label


def get_monthly_period(year_month: str) -> MonthlyPeriod:
    """Parse YYYY-MM string into MonthlyPeriod.

    Args:
        year_month: String in YYYY-MM format (e.g., "2024-03")

    Returns:
        MonthlyPeriod instance

    Raises:
        ValueError: If format is invalid
    """
    try:
        parts = year_month.split('-')
        if len(parts) != 2:
            raise ValueError(f"Expected YYYY-MM format, got: {year_month}")
        year = int(parts[0])
        month = int(parts[1])
        if not (1 <= month <= 12):
            raise ValueError(f"Month must be 1-12, got: {month}")
        if not (2020 <= year <= 2030):
            raise ValueError(f"Year out of expected range: {year}")
        return MonthlyPeriod(year=year, month=month)
    except (ValueError, IndexError) as e:
        raise ValueError(f"Invalid period format '{year_month}': {e}")


def filter_by_period(
    df: pd.DataFrame,
    date_column: str,
    period: MonthlyPeriod,
) -> pd.DataFrame:
    """Filter DataFrame to rows within the monthly period.

    Args:
        df: DataFrame to filter
        date_column: Name of the date column
        period: MonthlyPeriod to filter by

    Returns:
        Filtered DataFrame (copy)
    """
    if date_column not in df.columns:
        raise ValueError(f"Date column '{date_column}' not found in DataFrame")

    # Ensure datetime type
    dates = pd.to_datetime(df[date_column], errors='coerce')

    # Filter to period
    mask = (dates >= pd.Timestamp(period.start_date)) & \
           (dates <= pd.Timestamp(period.end_date))

    return df[mask].copy()


@dataclass
class DataAvailability:
    """Tracks data availability for a source in a period."""
    source: str
    period: MonthlyPeriod
    record_count: int
    date_range: Optional[tuple] = None  # (min_date, max_date)
    coverage_notes: List[str] = field(default_factory=list)

    @property
    def has_data(self) -> bool:
        return self.record_count > 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'source': self.source,
            'period': self.period.label,
            'record_count': self.record_count,
            'date_range_start': self.date_range[0] if self.date_range else None,
            'date_range_end': self.date_range[1] if self.date_range else None,
            'coverage_notes': '; '.join(self.coverage_notes) if self.coverage_notes else '',
        }


def get_date_range(df: pd.DataFrame, date_column: str) -> Optional[tuple]:
    """Get min/max dates from a DataFrame column.

    Returns:
        Tuple of (min_date, max_date) or None if no valid dates
    """
    if date_column not in df.columns or df.empty:
        return None

    dates = pd.to_datetime(df[date_column], errors='coerce').dropna()
    if dates.empty:
        return None

    return (dates.min().date(), dates.max().date())
