"""Base utilities for snapshot-based data loading.

Provides:
- SnapshotPeriod: Period bounded by P6 data dates (not calendar months)
- Period discovery from P6 project table
- Date filtering and availability tracking
"""

import sys
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
import pandas as pd

# Add project root to path
_project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import Settings


@dataclass
class SnapshotPeriod:
    """Represents a period between two P6 schedule snapshots.

    Unlike monthly periods, snapshot periods are bounded by actual P6 data dates,
    which may fall on any day of the month.

    Attributes:
        start_file_id: P6 file_id for the start of period (previous snapshot)
        end_file_id: P6 file_id for the end of period (current snapshot)
        start_data_date: Data date of the start snapshot
        end_data_date: Data date of the end snapshot
        schedule_type: YATES or SECAI
    """
    start_file_id: int
    end_file_id: int
    start_data_date: date
    end_data_date: date
    schedule_type: str = 'YATES'

    @property
    def start_date(self) -> date:
        """Start date (day after previous snapshot's data date)."""
        return self.start_data_date + pd.Timedelta(days=1)

    @property
    def end_date(self) -> date:
        """End date (current snapshot's data date)."""
        return self.end_data_date

    @property
    def duration_days(self) -> int:
        """Number of days in the period."""
        return (self.end_data_date - self.start_data_date).days

    @property
    def label(self) -> str:
        """Human-readable label for the period."""
        return f"{self.start_data_date.strftime('%Y-%m-%d')}_to_{self.end_data_date.strftime('%Y-%m-%d')}"

    @property
    def short_label(self) -> str:
        """Short label using end date."""
        return self.end_data_date.strftime('%Y-%m-%d')

    def contains(self, dt: date) -> bool:
        """Check if a date falls within this period.

        Uses half-open interval: (start_data_date, end_data_date]
        i.e., excludes start date, includes end date.
        """
        if pd.isna(dt):
            return False
        if isinstance(dt, pd.Timestamp):
            dt = dt.date()
        if isinstance(dt, datetime):
            dt = dt.date()
        return self.start_data_date < dt <= self.end_data_date

    def __str__(self) -> str:
        return f"Period({self.label}, {self.duration_days}d)"

    def __repr__(self) -> str:
        return f"SnapshotPeriod(file_id={self.start_file_id}→{self.end_file_id}, {self.label})"


@dataclass
class DataAvailability:
    """Tracks data availability for a source in a period."""
    source: str
    period: SnapshotPeriod
    record_count: int
    date_range: Optional[Tuple[date, date]] = None
    coverage_notes: List[str] = field(default_factory=list)

    @property
    def has_data(self) -> bool:
        return self.record_count > 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'source': self.source,
            'period': self.period.label,
            'record_count': self.record_count,
            'date_range_start': self.date_range[0].isoformat() if self.date_range else None,
            'date_range_end': self.date_range[1].isoformat() if self.date_range else None,
            'coverage_notes': '; '.join(self.coverage_notes) if self.coverage_notes else '',
        }


def _load_project_data() -> pd.DataFrame:
    """Load P6 project table with data dates."""
    project_path = Settings.PRIMAVERA_PROCESSED_DIR / 'project.csv'
    if not project_path.exists():
        return pd.DataFrame()

    df = pd.read_csv(project_path, low_memory=False)

    # Parse data date
    df['data_date'] = pd.to_datetime(df['last_recalc_date'], errors='coerce')

    # Filter to valid dates
    df = df[df['data_date'].notna()].copy()

    return df


def _load_xer_metadata() -> pd.DataFrame:
    """Load XER files metadata."""
    xer_path = Settings.PRIMAVERA_PROCESSED_DIR / 'xer_files.csv'
    if not xer_path.exists():
        return pd.DataFrame()

    return pd.read_csv(xer_path)


def get_all_snapshot_periods(
    schedule_type: str = 'YATES',
    min_gap_days: int = 3,
) -> List[SnapshotPeriod]:
    """Discover all valid snapshot periods from P6 data.

    Args:
        schedule_type: Filter to YATES or SECAI schedules
        min_gap_days: Minimum days between snapshots to create a period

    Returns:
        List of SnapshotPeriod objects, sorted by end date
    """
    project_df = _load_project_data()
    xer_df = _load_xer_metadata()

    if project_df.empty:
        return []

    # Merge to get schedule type if available
    if not xer_df.empty and 'schedule_type' in xer_df.columns:
        merged = project_df.merge(
            xer_df[['file_id', 'schedule_type']],
            on='file_id',
            how='left'
        )
    else:
        merged = project_df.copy()
        merged['schedule_type'] = 'YATES'  # Default

    # Filter by schedule type
    if schedule_type and 'schedule_type' in merged.columns:
        merged = merged[merged['schedule_type'] == schedule_type]

    # Sort by data date
    merged = merged.sort_values('data_date').reset_index(drop=True)

    if len(merged) < 2:
        return []

    # Build periods from consecutive snapshots
    periods = []
    for i in range(1, len(merged)):
        prev_row = merged.iloc[i - 1]
        curr_row = merged.iloc[i]

        prev_date = prev_row['data_date'].date()
        curr_date = curr_row['data_date'].date()

        gap_days = (curr_date - prev_date).days

        # Skip if gap is too small (likely same schedule run)
        if gap_days < min_gap_days:
            continue

        period = SnapshotPeriod(
            start_file_id=int(prev_row['file_id']),
            end_file_id=int(curr_row['file_id']),
            start_data_date=prev_date,
            end_data_date=curr_date,
            schedule_type=schedule_type or curr_row.get('schedule_type', 'YATES'),
        )
        periods.append(period)

    return periods


def get_snapshot_period(
    file_id: int = None,
    data_date: str = None,
) -> Optional[SnapshotPeriod]:
    """Get a specific snapshot period.

    Args:
        file_id: Get period ending at this file_id
        data_date: Get period ending at or near this date (YYYY-MM-DD)

    Returns:
        SnapshotPeriod or None if not found
    """
    all_periods = get_all_snapshot_periods()

    if file_id:
        for period in all_periods:
            if period.end_file_id == file_id:
                return period
        return None

    if data_date:
        target = pd.to_datetime(data_date).date()
        # Find closest period
        best_period = None
        best_diff = float('inf')
        for period in all_periods:
            diff = abs((period.end_data_date - target).days)
            if diff < best_diff:
                best_diff = diff
                best_period = period
        # Only return if within 7 days
        if best_diff <= 7:
            return best_period
        return None

    # Return latest if no args
    return all_periods[-1] if all_periods else None


def filter_by_period(
    df: pd.DataFrame,
    date_column: str,
    period: SnapshotPeriod,
) -> pd.DataFrame:
    """Filter DataFrame to rows within the snapshot period.

    Uses half-open interval: (start_data_date, end_data_date]

    Args:
        df: DataFrame to filter
        date_column: Name of the date column
        period: SnapshotPeriod to filter by

    Returns:
        Filtered DataFrame (copy)
    """
    if date_column not in df.columns:
        raise ValueError(f"Date column '{date_column}' not found in DataFrame")

    # Ensure datetime type
    dates = pd.to_datetime(df[date_column], errors='coerce')

    # Filter to period (half-open: excludes start, includes end)
    mask = (dates > pd.Timestamp(period.start_data_date)) & \
           (dates <= pd.Timestamp(period.end_data_date))

    return df[mask].copy()


def get_date_range(df: pd.DataFrame, date_column: str) -> Optional[Tuple[date, date]]:
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


# Convenience function for listing available periods
def list_periods(limit: int = None) -> None:
    """Print available snapshot periods."""
    periods = get_all_snapshot_periods()

    if limit:
        periods = periods[-limit:]

    print(f"Available Snapshot Periods ({len(periods)} total):")
    print("-" * 80)
    print(f"{'#':<4} {'File IDs':<12} {'Start Date':<12} {'End Date':<12} {'Days':<6} {'Label'}")
    print("-" * 80)

    for i, p in enumerate(periods, 1):
        print(f"{i:<4} {p.start_file_id:>4}→{p.end_file_id:<4}  {p.start_data_date}  {p.end_data_date}  {p.duration_days:>4}d  {p.label}")


if __name__ == '__main__':
    list_periods(20)
