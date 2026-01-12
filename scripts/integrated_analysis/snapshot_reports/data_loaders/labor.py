"""Labor hours data loader for snapshot reports.

Loads labor data from ProjectSight, TBM, and Weekly Reports filtered to snapshot periods.
"""

import sys
from pathlib import Path
from typing import Dict, Any, List
import pandas as pd

_project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import Settings
from .base import SnapshotPeriod, DataAvailability, filter_by_period, get_date_range


def _load_projectsight_data(period: SnapshotPeriod) -> tuple[pd.DataFrame, DataAvailability]:
    """Load ProjectSight labor data for period."""
    ps_path = Settings.PROJECTSIGHT_PROCESSED_DIR / 'labor_entries_enriched.csv'

    if not ps_path.exists():
        # Try non-enriched version
        ps_path = Settings.PROJECTSIGHT_PROCESSED_DIR / 'labor_entries.csv'

    if not ps_path.exists():
        return pd.DataFrame(), DataAvailability(
            source='ProjectSight',
            period=period,
            record_count=0,
            coverage_notes=['Labor entries file not found'],
        )

    df = pd.read_csv(ps_path, low_memory=False)

    # Identify date column
    date_col = None
    for col in ['report_date', 'date', 'work_date', 'entry_date']:
        if col in df.columns:
            date_col = col
            break

    if date_col is None:
        return pd.DataFrame(), DataAvailability(
            source='ProjectSight',
            period=period,
            record_count=0,
            coverage_notes=['No date column found'],
        )

    # Filter to period
    filtered = filter_by_period(df, date_col, period)

    # Ensure hours column exists
    if not filtered.empty and 'hours' not in filtered.columns:
        # Try to find hours column (ProjectSight uses hours_new or hours_old)
        for col in ['hours_new', 'hours_old', 'total_hours', 'labor_hours', 'regular_hours']:
            if col in filtered.columns:
                filtered['hours'] = filtered[col]
                break

    # Build availability
    date_range = get_date_range(filtered, date_col) if not filtered.empty else None

    coverage_notes = []
    if not filtered.empty:
        total_hours = filtered['hours'].sum() if 'hours' in filtered.columns else 0
        coverage_notes.append(f"{len(filtered):,} entries, {total_hours:,.0f} hours")
    else:
        coverage_notes.append('No data in period')

    availability = DataAvailability(
        source='ProjectSight',
        period=period,
        record_count=len(filtered),
        date_range=date_range,
        coverage_notes=coverage_notes,
    )

    return filtered, availability


def _load_tbm_data(period: SnapshotPeriod) -> tuple[pd.DataFrame, DataAvailability]:
    """Load TBM daily plan data for period."""
    tbm_path = Settings.TBM_PROCESSED_DIR / 'work_entries_enriched.csv'

    if not tbm_path.exists():
        tbm_path = Settings.TBM_PROCESSED_DIR / 'work_entries.csv'

    if not tbm_path.exists():
        return pd.DataFrame(), DataAvailability(
            source='TBM',
            period=period,
            record_count=0,
            coverage_notes=['TBM work entries file not found'],
        )

    df = pd.read_csv(tbm_path, low_memory=False)

    # Identify date column
    date_col = None
    for col in ['report_date', 'date', 'work_date', 'plan_date']:
        if col in df.columns:
            date_col = col
            break

    if date_col is None:
        return pd.DataFrame(), DataAvailability(
            source='TBM',
            period=period,
            record_count=0,
            coverage_notes=['No date column found'],
        )

    # Filter to period
    filtered = filter_by_period(df, date_col, period)

    # TBM has num_employees, estimate hours (8 hours per employee)
    if not filtered.empty:
        if 'hours' not in filtered.columns and 'num_employees' in filtered.columns:
            filtered['hours'] = filtered['num_employees'] * 8

    # Build availability
    date_range = get_date_range(filtered, date_col) if not filtered.empty else None

    coverage_notes = []
    if not filtered.empty:
        total_hours = filtered['hours'].sum() if 'hours' in filtered.columns else 0
        coverage_notes.append(f"{len(filtered):,} entries, {total_hours:,.0f} estimated hours")
    else:
        coverage_notes.append('No data in period')

    availability = DataAvailability(
        source='TBM',
        period=period,
        record_count=len(filtered),
        date_range=date_range,
        coverage_notes=coverage_notes,
    )

    return filtered, availability


def _load_weekly_reports_data(period: SnapshotPeriod) -> tuple[pd.DataFrame, DataAvailability]:
    """Load weekly report labor data for period."""
    # Use labor_detail.csv which has report_date (labor_detail_by_company is aggregated without dates)
    wr_path = Settings.WEEKLY_REPORTS_PROCESSED_DIR / 'labor_detail.csv'

    if not wr_path.exists():
        return pd.DataFrame(), DataAvailability(
            source='WeeklyReports',
            period=period,
            record_count=0,
            coverage_notes=['labor_detail.csv not found'],
        )

    df = pd.read_csv(wr_path, low_memory=False)

    # Identify date column
    date_col = None
    for col in ['report_date', 'week_ending', 'date']:
        if col in df.columns:
            date_col = col
            break

    if date_col is None:
        return pd.DataFrame(), DataAvailability(
            source='WeeklyReports',
            period=period,
            record_count=0,
            coverage_notes=['No date column found'],
        )

    # Filter to period
    filtered = filter_by_period(df, date_col, period)

    # Build availability
    date_range = get_date_range(filtered, date_col) if not filtered.empty else None

    coverage_notes = []
    if not filtered.empty:
        total_hours = filtered['hours'].sum() if 'hours' in filtered.columns else 0
        coverage_notes.append(f"{len(filtered):,} entries, {total_hours:,.0f} hours")
    else:
        coverage_notes.append('No data in period')

    availability = DataAvailability(
        source='WeeklyReports',
        period=period,
        record_count=len(filtered),
        date_range=date_range,
        coverage_notes=coverage_notes,
    )

    return filtered, availability


def load_labor_data(period: SnapshotPeriod) -> Dict[str, Any]:
    """Load labor data from all sources for a snapshot period.

    Args:
        period: SnapshotPeriod to filter by

    Returns:
        Dict with:
        - 'projectsight': DataFrame of ProjectSight labor entries
        - 'tbm': DataFrame of TBM work entries
        - 'weekly_reports': DataFrame of weekly report labor
        - 'labor': Combined DataFrame with standardized hours column
        - 'availability': List of DataAvailability for each source
    """
    ps_df, ps_avail = _load_projectsight_data(period)
    tbm_df, tbm_avail = _load_tbm_data(period)
    wr_df, wr_avail = _load_weekly_reports_data(period)

    # Combine labor data with source indicator
    combined_frames = []

    if not ps_df.empty:
        ps_copy = ps_df.copy()
        ps_copy['source'] = 'ProjectSight'
        combined_frames.append(ps_copy)

    if not tbm_df.empty:
        tbm_copy = tbm_df.copy()
        tbm_copy['source'] = 'TBM'
        combined_frames.append(tbm_copy)

    if not wr_df.empty:
        wr_copy = wr_df.copy()
        wr_copy['source'] = 'WeeklyReports'
        combined_frames.append(wr_copy)

    if combined_frames:
        # Find common columns that include hours
        common_cols = {'source', 'hours'}
        for col in ['dim_company_id', 'company', 'dim_location_id', 'building_level', 'dim_trade_id', 'trade']:
            for df in combined_frames:
                if col in df.columns:
                    common_cols.add(col)
                    break

        # Filter each frame to available common cols and combine
        filtered_frames = []
        for df in combined_frames:
            available = [c for c in common_cols if c in df.columns]
            if 'hours' in available:
                filtered_frames.append(df[available])

        if filtered_frames:
            combined = pd.concat(filtered_frames, ignore_index=True)
        else:
            combined = pd.DataFrame()
    else:
        combined = pd.DataFrame()

    return {
        'projectsight': ps_df,
        'tbm': tbm_df,
        'weekly_reports': wr_df,
        'labor': combined,
        'availability': [ps_avail, tbm_avail, wr_avail],
    }
