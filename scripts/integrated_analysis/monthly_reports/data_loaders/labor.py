"""Labor hours data loader (ProjectSight + TBM + Weekly Reports).

Loads and combines labor data from multiple sources with different date ranges.
"""

import sys
from pathlib import Path
from typing import Dict, Any
import pandas as pd

_project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import Settings
from .base import MonthlyPeriod, DataAvailability, filter_by_period, get_date_range


def _load_projectsight(period: MonthlyPeriod) -> tuple[pd.DataFrame, DataAvailability]:
    """Load ProjectSight labor entries for period.

    Note: ProjectSight data only available Jun 2022 - Mar 2023.
    """
    ps_path = Settings.PROJECTSIGHT_PROCESSED_DIR / 'labor_entries_enriched.csv'

    if not ps_path.exists():
        # Try non-enriched version
        ps_path = Settings.PROJECTSIGHT_PROCESSED_DIR / 'labor_entries.csv'

    if not ps_path.exists():
        return pd.DataFrame(), DataAvailability(
            source='ProjectSight',
            period=period,
            record_count=0,
            coverage_notes=['File not found'],
        )

    df = pd.read_csv(ps_path, low_memory=False)
    df['report_date'] = pd.to_datetime(df['report_date'], errors='coerce')

    # Filter to period
    filtered = filter_by_period(df, 'report_date', period)

    # Standardize columns
    filtered['source'] = 'ProjectSight'

    # Determine hours column (may be hours_new, hours_old, or hours_worked)
    hours_col = None
    for col in ['hours_new', 'hours_worked', 'hours_old']:
        if col in filtered.columns:
            hours_col = col
            break

    if hours_col and hours_col != 'hours':
        filtered['hours'] = filtered[hours_col]

    # Coverage notes
    notes = []
    if filtered.empty:
        notes.append("No data (ProjectSight ends Mar 2023)")
    else:
        date_range = get_date_range(filtered, 'report_date')
        if date_range:
            notes.append(f"Dates: {date_range[0]} to {date_range[1]}")
        total_hours = filtered['hours'].sum() if 'hours' in filtered.columns else 0
        notes.append(f"Total hours: {total_hours:,.0f}")

    availability = DataAvailability(
        source='ProjectSight',
        period=period,
        record_count=len(filtered),
        date_range=get_date_range(filtered, 'report_date'),
        coverage_notes=notes,
    )

    return filtered, availability


def _load_tbm(period: MonthlyPeriod) -> tuple[pd.DataFrame, DataAvailability]:
    """Load TBM daily work entries for period.

    Note: TBM data only available Mar - Dec 2025.
    """
    tbm_path = Settings.TBM_PROCESSED_DIR / 'work_entries_enriched.csv'

    if not tbm_path.exists():
        tbm_path = Settings.TBM_PROCESSED_DIR / 'work_entries.csv'

    if not tbm_path.exists():
        return pd.DataFrame(), DataAvailability(
            source='TBM',
            period=period,
            record_count=0,
            coverage_notes=['File not found'],
        )

    df = pd.read_csv(tbm_path, low_memory=False)
    df['report_date'] = pd.to_datetime(df['report_date'], errors='coerce')

    # Filter to period
    filtered = filter_by_period(df, 'report_date', period)

    # Standardize columns
    filtered['source'] = 'TBM'

    # TBM has num_employees, not hours - estimate hours (assume 8-hour day)
    if 'num_employees' in filtered.columns and 'hours' not in filtered.columns:
        filtered['hours'] = filtered['num_employees'] * 8
        filtered['hours_estimated'] = True

    # Standardize company column
    if 'tier2_sc' in filtered.columns and 'company' not in filtered.columns:
        filtered['company'] = filtered['tier2_sc']

    # Standardize location columns
    if 'location_building' in filtered.columns and 'building' not in filtered.columns:
        filtered['building'] = filtered['location_building']
    if 'location_level' in filtered.columns and 'level' not in filtered.columns:
        filtered['level'] = filtered['location_level']

    # Coverage notes
    notes = []
    if filtered.empty:
        notes.append("No data (TBM starts Mar 2025)")
    else:
        date_range = get_date_range(filtered, 'report_date')
        if date_range:
            notes.append(f"Dates: {date_range[0]} to {date_range[1]}")
        if 'num_employees' in filtered.columns:
            total_workers = filtered['num_employees'].sum()
            notes.append(f"Total worker-days: {total_workers:,.0f}")

    availability = DataAvailability(
        source='TBM',
        period=period,
        record_count=len(filtered),
        date_range=get_date_range(filtered, 'report_date'),
        coverage_notes=notes,
    )

    return filtered, availability


def _load_weekly_reports(period: MonthlyPeriod) -> tuple[pd.DataFrame, DataAvailability]:
    """Load weekly report labor data for period.

    Note: Weekly reports available Aug 2022 - Jun 2023.
    Labor is aggregated by company per week, not daily.
    """
    # Labor detail has per-company hours
    labor_path = Settings.WEEKLY_REPORTS_PROCESSED_DIR / 'labor_detail.csv'
    reports_path = Settings.WEEKLY_REPORTS_PROCESSED_DIR / 'weekly_reports.csv'

    if not labor_path.exists() or not reports_path.exists():
        # Try enriched version
        labor_path = Settings.WEEKLY_REPORTS_PROCESSED_DIR / 'labor_detail_by_company_enriched.csv'
        if not labor_path.exists():
            return pd.DataFrame(), DataAvailability(
                source='WeeklyReports',
                period=period,
                record_count=0,
                coverage_notes=['File not found'],
            )

    # Load reports for date context
    if reports_path.exists():
        reports = pd.read_csv(reports_path, low_memory=False)
        reports['report_date'] = pd.to_datetime(reports['report_date'], errors='coerce')
    else:
        reports = pd.DataFrame()

    # Load labor detail
    labor = pd.read_csv(labor_path, low_memory=False)

    # Join with reports to get dates
    if not reports.empty and 'file_id' in labor.columns and 'file_id' in reports.columns:
        labor = labor.merge(
            reports[['file_id', 'report_date']],
            on='file_id',
            how='left'
        )
    elif 'week_ending' in labor.columns:
        labor['report_date'] = pd.to_datetime(labor['week_ending'], errors='coerce')
    elif 'date' in labor.columns:
        labor['report_date'] = pd.to_datetime(labor['date'], errors='coerce')

    # Filter to period
    if 'report_date' in labor.columns:
        labor['report_date'] = pd.to_datetime(labor['report_date'], errors='coerce')
        filtered = filter_by_period(labor, 'report_date', period)
    else:
        filtered = pd.DataFrame()

    # Standardize columns
    if not filtered.empty:
        filtered['source'] = 'WeeklyReports'

        # Standardize hours column
        if 'hours' not in filtered.columns:
            for col in ['hours_worked', 'total_hours']:
                if col in filtered.columns:
                    filtered['hours'] = filtered[col]
                    break

    # Coverage notes
    notes = []
    if filtered.empty:
        notes.append("No data (Weekly Reports end Jun 2023)")
    else:
        date_range = get_date_range(filtered, 'report_date')
        if date_range:
            notes.append(f"Dates: {date_range[0]} to {date_range[1]}")
        if 'hours' in filtered.columns:
            total_hours = filtered['hours'].sum()
            notes.append(f"Total hours: {total_hours:,.0f}")

    availability = DataAvailability(
        source='WeeklyReports',
        period=period,
        record_count=len(filtered),
        date_range=get_date_range(filtered, 'report_date') if not filtered.empty else None,
        coverage_notes=notes,
    )

    return filtered, availability


def load_labor_data(period: MonthlyPeriod) -> Dict[str, Any]:
    """Load combined labor data for a monthly period.

    Combines ProjectSight, TBM, and Weekly Reports data.
    Different sources cover different time periods:
    - ProjectSight: Jun 2022 - Mar 2023
    - Weekly Reports: Aug 2022 - Jun 2023
    - TBM: Mar 2025 - Dec 2025

    Args:
        period: Monthly period to filter by

    Returns:
        Dict with:
        - 'labor': Combined DataFrame of all labor entries
        - 'projectsight': ProjectSight-only DataFrame
        - 'tbm': TBM-only DataFrame
        - 'weekly_reports': Weekly Reports-only DataFrame
        - 'availability': List of DataAvailability for each source
    """
    ps_df, ps_avail = _load_projectsight(period)
    tbm_df, tbm_avail = _load_tbm(period)
    wr_df, wr_avail = _load_weekly_reports(period)

    # Common columns for combined view
    common_cols = [
        'report_date', 'source',
        'company', 'building', 'level',
        'dim_location_id', 'dim_company_id', 'dim_trade_id', 'dim_trade_code',
        'hours', 'num_employees',
        'work_activities', 'activity',
    ]

    # Build combined DataFrame
    combined_dfs = []

    for df in [ps_df, tbm_df, wr_df]:
        if not df.empty:
            available_cols = [c for c in common_cols if c in df.columns]
            combined_dfs.append(df[available_cols])

    if combined_dfs:
        combined = pd.concat(combined_dfs, ignore_index=True)
    else:
        combined = pd.DataFrame(columns=common_cols)

    return {
        'labor': combined,
        'projectsight': ps_df,
        'tbm': tbm_df,
        'weekly_reports': wr_df,
        'availability': [ps_avail, tbm_avail, wr_avail],
    }
