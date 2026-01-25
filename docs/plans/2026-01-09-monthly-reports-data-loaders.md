# Monthly Reports Data Loaders Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build data loader modules that fetch and filter data by monthly period from all sources.

**Architecture:** Each loader returns filtered DataFrames with standardized column names. A base module provides common date filtering and validation. All loaders use existing enriched CSVs with dimension IDs already populated.

**Tech Stack:** Python 3, pandas, pathlib, src.config.settings

---

## Task 1: Create Module Structure

**Files:**
- Create: `scripts/integrated_analysis/monthly_reports/__init__.py`
- Create: `scripts/integrated_analysis/monthly_reports/data_loaders/__init__.py`

**Step 1: Create directories and init files**

```bash
mkdir -p scripts/integrated_analysis/monthly_reports/data_loaders
```

**Step 2: Create main module init**

Create `scripts/integrated_analysis/monthly_reports/__init__.py`:
```python
"""Monthly reports consolidation for MXI internal analysis."""
```

**Step 3: Create data_loaders init**

Create `scripts/integrated_analysis/monthly_reports/data_loaders/__init__.py`:
```python
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
```

**Step 4: Commit**

```bash
git add scripts/integrated_analysis/monthly_reports/
git commit -m "feat(monthly-reports): create module structure"
```

---

## Task 2: Base Module with Period Utilities

**Files:**
- Create: `scripts/integrated_analysis/monthly_reports/data_loaders/base.py`

**Step 1: Create base module**

Create `scripts/integrated_analysis/monthly_reports/data_loaders/base.py`:
```python
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
```

**Step 2: Commit**

```bash
git add scripts/integrated_analysis/monthly_reports/data_loaders/base.py
git commit -m "feat(monthly-reports): add base module with period utilities"
```

---

## Task 3: Primavera (Schedule) Data Loader

**Files:**
- Create: `scripts/integrated_analysis/monthly_reports/data_loaders/primavera.py`

**Context:**
- Source: `processed/primavera/task.csv` (470K rows) + `derived/primavera/task_taxonomy.csv` (taxonomy with location)
- Key date columns: `act_start_date`, `act_end_date`, `target_start_date`, `target_end_date`
- Need to join with taxonomy for building/level/trade
- Filter tasks that were active during the period (started before end, not completed before start)

**Step 1: Create primavera loader**

Create `scripts/integrated_analysis/monthly_reports/data_loaders/primavera.py`:
```python
"""Primavera P6 schedule data loader.

Loads task data with taxonomy enrichment for schedule progress analysis.
"""

import sys
from pathlib import Path
from typing import Dict, Any, Optional
import pandas as pd

_project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import Settings
from .base import MonthlyPeriod, DataAvailability, get_date_range


def load_schedule_data(period: MonthlyPeriod) -> Dict[str, Any]:
    """Load P6 schedule data for a monthly period.

    Returns tasks that were active during the period:
    - Started before or during the period
    - Not completed before the period started

    Args:
        period: Monthly period to filter by

    Returns:
        Dict with:
        - 'tasks': DataFrame of active tasks with taxonomy
        - 'availability': DataAvailability info
        - 'snapshots': List of P6 snapshot dates in period
    """
    # Load task data
    task_path = Settings.PRIMAVERA_PROCESSED_DIR / 'task.csv'
    if not task_path.exists():
        return {
            'tasks': pd.DataFrame(),
            'availability': DataAvailability(
                source='P6',
                period=period,
                record_count=0,
                coverage_notes=['Task file not found']
            ),
            'snapshots': [],
        }

    tasks = pd.read_csv(task_path, low_memory=False)

    # Parse date columns
    date_cols = ['act_start_date', 'act_end_date', 'target_start_date',
                 'target_end_date', 'early_start_date', 'early_end_date']
    for col in date_cols:
        if col in tasks.columns:
            tasks[col] = pd.to_datetime(tasks[col], errors='coerce')

    # Load taxonomy for building/level/trade enrichment
    taxonomy_path = Settings.PRIMAVERA_PROCESSED_DIR / 'p6_task_taxonomy.csv'
    if taxonomy_path.exists():
        taxonomy = pd.read_csv(taxonomy_path, low_memory=False)
        # Merge on task_id or task_code
        if 'task_id' in taxonomy.columns and 'task_id' in tasks.columns:
            tasks = tasks.merge(
                taxonomy[['task_id', 'building', 'level', 'trade', 'location_type',
                          'location_code', 'building_level']],
                on='task_id',
                how='left',
                suffixes=('', '_taxonomy')
            )

    # Filter to tasks active during period
    # Active = (started before period ends) AND (not completed before period starts)
    period_start = pd.Timestamp(period.start_date)
    period_end = pd.Timestamp(period.end_date)

    # Use target dates as fallback for actual dates
    start_date = tasks['act_start_date'].fillna(tasks['target_start_date'])
    end_date = tasks['act_end_date'].fillna(tasks['target_end_date'])

    # Tasks that overlap with the period
    started_before_period_ends = start_date <= period_end
    not_completed_before_period_starts = end_date.isna() | (end_date >= period_start)

    active_mask = started_before_period_ends & not_completed_before_period_starts
    active_tasks = tasks[active_mask].copy()

    # Identify tasks completed during period
    active_tasks['completed_this_period'] = (
        active_tasks['act_end_date'].notna() &
        (active_tasks['act_end_date'] >= period_start) &
        (active_tasks['act_end_date'] <= period_end)
    )

    # Identify tasks started during period
    active_tasks['started_this_period'] = (
        start_date[active_mask].notna() &
        (start_date[active_mask] >= period_start) &
        (start_date[active_mask] <= period_end)
    )

    # Calculate duration overage (actual vs target)
    active_tasks['target_duration'] = (
        active_tasks['target_end_date'] - active_tasks['target_start_date']
    ).dt.days

    active_tasks['actual_duration'] = (
        active_tasks['act_end_date'].fillna(period_end) -
        active_tasks['act_start_date']
    ).dt.days

    active_tasks['duration_overage'] = (
        active_tasks['actual_duration'] - active_tasks['target_duration']
    )

    # Get P6 snapshot info
    xer_files_path = Settings.PRIMAVERA_PROCESSED_DIR / 'xer_files.csv'
    snapshots = []
    if xer_files_path.exists():
        xer_files = pd.read_csv(xer_files_path)
        if 'date' in xer_files.columns:
            xer_files['date'] = pd.to_datetime(xer_files['date'], errors='coerce')
            period_xers = xer_files[
                (xer_files['date'] >= period_start) &
                (xer_files['date'] <= period_end)
            ]
            snapshots = period_xers['date'].dt.strftime('%Y-%m-%d').tolist()

    # Build availability info
    coverage_notes = []
    if snapshots:
        coverage_notes.append(f"{len(snapshots)} P6 snapshots")
    else:
        coverage_notes.append("No P6 snapshots in period")

    if 'building' in active_tasks.columns:
        building_coverage = active_tasks['building'].notna().mean() * 100
        coverage_notes.append(f"Building coverage: {building_coverage:.1f}%")

    availability = DataAvailability(
        source='P6',
        period=period,
        record_count=len(active_tasks),
        date_range=get_date_range(active_tasks, 'act_start_date'),
        coverage_notes=coverage_notes,
    )

    return {
        'tasks': active_tasks,
        'availability': availability,
        'snapshots': snapshots,
    }
```

**Step 2: Commit**

```bash
git add scripts/integrated_analysis/monthly_reports/data_loaders/primavera.py
git commit -m "feat(monthly-reports): add primavera schedule data loader"
```

---

## Task 4: Quality Data Loader (RABA + PSI)

**Files:**
- Create: `scripts/integrated_analysis/monthly_reports/data_loaders/quality.py`

**Context:**
- RABA: `processed/raba/raba_consolidated.csv` (9,391 rows), date column: `report_date`
- PSI: `processed/psi/psi_consolidated.csv` (6,309 rows), date column: `report_date`
- Both have dimension IDs: `dim_location_id`, `dim_company_id`, `dim_trade_id`
- Key metrics: `outcome` (PASS/FAIL), `tests_passed`, `tests_failed`, `reinspection_required`

**Step 1: Create quality loader**

Create `scripts/integrated_analysis/monthly_reports/data_loaders/quality.py`:
```python
"""Quality inspection data loader (RABA + PSI).

Loads and combines quality inspection data from both third-party QC sources.
"""

import sys
from pathlib import Path
from typing import Dict, Any, List
import pandas as pd

_project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import Settings
from .base import MonthlyPeriod, DataAvailability, filter_by_period, get_date_range


def _load_raba(period: MonthlyPeriod) -> tuple[pd.DataFrame, DataAvailability]:
    """Load RABA inspection data for period."""
    raba_path = Settings.RABA_PROCESSED_DIR / 'raba_consolidated.csv'

    if not raba_path.exists():
        return pd.DataFrame(), DataAvailability(
            source='RABA',
            period=period,
            record_count=0,
            coverage_notes=['File not found'],
        )

    df = pd.read_csv(raba_path, low_memory=False)
    df['report_date'] = pd.to_datetime(df['report_date'], errors='coerce')

    # Filter to period
    filtered = filter_by_period(df, 'report_date', period)

    # Standardize columns for combined output
    filtered['source'] = 'RABA'
    filtered['inspection_type'] = filtered.get('test_type_normalized', filtered.get('test_type', ''))
    filtered['inspection_category'] = filtered.get('test_category', '')

    # Coverage notes
    notes = []
    date_range = get_date_range(filtered, 'report_date')
    if date_range:
        notes.append(f"Dates: {date_range[0]} to {date_range[1]}")

    # Check for gaps (days without inspections)
    if not filtered.empty:
        dates = pd.to_datetime(filtered['report_date']).dt.date
        all_days = pd.date_range(period.start_date, period.end_date).date
        missing_days = set(all_days) - set(dates)
        if missing_days:
            notes.append(f"Missing {len(missing_days)} days")

    availability = DataAvailability(
        source='RABA',
        period=period,
        record_count=len(filtered),
        date_range=date_range,
        coverage_notes=notes,
    )

    return filtered, availability


def _load_psi(period: MonthlyPeriod) -> tuple[pd.DataFrame, DataAvailability]:
    """Load PSI inspection data for period."""
    psi_path = Settings.PSI_PROCESSED_DIR / 'psi_consolidated.csv'

    if not psi_path.exists():
        return pd.DataFrame(), DataAvailability(
            source='PSI',
            period=period,
            record_count=0,
            coverage_notes=['File not found'],
        )

    df = pd.read_csv(psi_path, low_memory=False)
    df['report_date'] = pd.to_datetime(df['report_date'], errors='coerce')

    # Filter to period
    filtered = filter_by_period(df, 'report_date', period)

    # Standardize columns for combined output
    filtered['source'] = 'PSI'
    filtered['inspection_type'] = filtered.get('inspection_type_normalized', filtered.get('inspection_type', ''))
    filtered['inspection_category'] = filtered.get('inspection_category', '')

    # Standardize outcome column (PSI may use different values)
    if 'outcome' not in filtered.columns and 'status' in filtered.columns:
        filtered['outcome'] = filtered['status']

    # Coverage notes
    notes = []
    date_range = get_date_range(filtered, 'report_date')
    if date_range:
        notes.append(f"Dates: {date_range[0]} to {date_range[1]}")

    availability = DataAvailability(
        source='PSI',
        period=period,
        record_count=len(filtered),
        date_range=date_range,
        coverage_notes=notes,
    )

    return filtered, availability


def load_quality_data(period: MonthlyPeriod) -> Dict[str, Any]:
    """Load combined quality inspection data for a monthly period.

    Combines RABA and PSI data with standardized columns.

    Args:
        period: Monthly period to filter by

    Returns:
        Dict with:
        - 'inspections': Combined DataFrame of all inspections
        - 'raba': RABA-only DataFrame
        - 'psi': PSI-only DataFrame
        - 'availability': List of DataAvailability for each source
    """
    raba_df, raba_avail = _load_raba(period)
    psi_df, psi_avail = _load_psi(period)

    # Common columns for combined view
    common_cols = [
        'inspection_id', 'report_date', 'source',
        'building', 'level', 'grid', 'area',
        'dim_location_id', 'dim_company_id', 'dim_trade_id', 'dim_trade_code',
        'outcome', 'inspection_type', 'inspection_category',
        'contractor', 'inspector',
        'failure_reason', 'failure_category', 'reinspection_required',
        'tests_total', 'tests_passed', 'tests_failed', 'issue_count',
    ]

    # Build combined DataFrame with available columns
    combined_dfs = []

    for df in [raba_df, psi_df]:
        if not df.empty:
            # Select columns that exist
            available_cols = [c for c in common_cols if c in df.columns]
            combined_dfs.append(df[available_cols])

    if combined_dfs:
        combined = pd.concat(combined_dfs, ignore_index=True)
    else:
        combined = pd.DataFrame(columns=common_cols)

    # Normalize outcome values
    outcome_map = {
        'PASS': 'PASS', 'Pass': 'PASS', 'pass': 'PASS', 'PASSED': 'PASS',
        'FAIL': 'FAIL', 'Fail': 'FAIL', 'fail': 'FAIL', 'FAILED': 'FAIL',
        'PARTIAL': 'PARTIAL', 'Partial': 'PARTIAL',
    }
    if 'outcome' in combined.columns:
        combined['outcome_normalized'] = combined['outcome'].map(
            lambda x: outcome_map.get(str(x).strip(), 'OTHER') if pd.notna(x) else 'UNKNOWN'
        )

    return {
        'inspections': combined,
        'raba': raba_df,
        'psi': psi_df,
        'availability': [raba_avail, psi_avail],
    }
```

**Step 2: Commit**

```bash
git add scripts/integrated_analysis/monthly_reports/data_loaders/quality.py
git commit -m "feat(monthly-reports): add quality data loader (RABA + PSI)"
```

---

## Task 5: Labor Data Loader (ProjectSight + TBM + Weekly Reports)

**Files:**
- Create: `scripts/integrated_analysis/monthly_reports/data_loaders/labor.py`

**Context:**
- ProjectSight: `processed/projectsight/labor_entries_enriched.csv` (857K rows), date: `report_date`
  - Date range: Jun 2022 - Mar 2023 only
- TBM: `processed/tbm/work_entries_enriched.csv` (13.5K rows), date: `report_date`
  - Date range: Mar - Dec 2025
- Weekly Reports: `processed/weekly_reports/labor_detail.csv` + parent `weekly_reports.csv`
  - Date range: Aug 2022 - Jun 2023
- All have dimension IDs

**Step 1: Create labor loader**

Create `scripts/integrated_analysis/monthly_reports/data_loaders/labor.py`:
```python
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
```

**Step 2: Commit**

```bash
git add scripts/integrated_analysis/monthly_reports/data_loaders/labor.py
git commit -m "feat(monthly-reports): add labor data loader (ProjectSight + TBM + Weekly)"
```

---

## Task 6: Narratives Data Loader

**Files:**
- Create: `scripts/integrated_analysis/monthly_reports/data_loaders/narratives.py`

**Context:**
- Source: `processed/narratives/narrative_statements.csv` (2,540 rows)
- Date column: `event_date` (only 1,046 non-null)
- Also need: `processed/narratives/dim_narrative_file.csv` for document metadata
- Key columns: `text`, `category`, `parties`, `locations`, `impact_days`

**Step 1: Create narratives loader**

Create `scripts/integrated_analysis/monthly_reports/data_loaders/narratives.py`:
```python
"""Narrative statements data loader.

Loads extracted statements from P6 narratives, weekly reports, and other documents.
"""

import sys
from pathlib import Path
from typing import Dict, Any
import pandas as pd

_project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import Settings
from .base import MonthlyPeriod, DataAvailability, filter_by_period, get_date_range


def load_narrative_data(period: MonthlyPeriod) -> Dict[str, Any]:
    """Load narrative statements for a monthly period.

    Filters statements by event_date field.
    Note: Many statements lack event dates (only ~41% have dates).

    Args:
        period: Monthly period to filter by

    Returns:
        Dict with:
        - 'statements': DataFrame of statements with event dates in period
        - 'statements_undated': DataFrame of statements without dates (for context)
        - 'documents': DataFrame of source document metadata
        - 'availability': DataAvailability info
    """
    # Load statements
    statements_path = Settings.NARRATIVES_PROCESSED_DIR / 'narrative_statements.csv'

    if not statements_path.exists():
        return {
            'statements': pd.DataFrame(),
            'statements_undated': pd.DataFrame(),
            'documents': pd.DataFrame(),
            'availability': DataAvailability(
                source='Narratives',
                period=period,
                record_count=0,
                coverage_notes=['File not found'],
            ),
        }

    statements = pd.read_csv(statements_path, low_memory=False)
    statements['event_date'] = pd.to_datetime(statements['event_date'], errors='coerce')

    # Split into dated and undated
    has_date = statements['event_date'].notna()
    dated_statements = statements[has_date].copy()
    undated_statements = statements[~has_date].copy()

    # Filter dated statements to period
    if not dated_statements.empty:
        filtered = filter_by_period(dated_statements, 'event_date', period)
    else:
        filtered = pd.DataFrame()

    # Load document metadata for context
    docs_path = Settings.NARRATIVES_PROCESSED_DIR / 'dim_narrative_file.csv'
    if docs_path.exists():
        documents = pd.read_csv(docs_path, low_memory=False)
        documents['document_date'] = pd.to_datetime(documents['document_date'], errors='coerce')
    else:
        documents = pd.DataFrame()

    # Enrich statements with document info
    if not filtered.empty and not documents.empty:
        if 'narrative_file_id' in filtered.columns and 'narrative_file_id' in documents.columns:
            filtered = filtered.merge(
                documents[['narrative_file_id', 'filename', 'document_type', 'document_date', 'author']],
                on='narrative_file_id',
                how='left',
                suffixes=('', '_doc')
            )

    # Parse parties and locations (stored as pipe-delimited strings)
    if not filtered.empty:
        if 'parties' in filtered.columns:
            filtered['parties_list'] = filtered['parties'].apply(
                lambda x: x.split('|') if pd.notna(x) and x else []
            )
            filtered['parties_count'] = filtered['parties_list'].apply(len)

        if 'locations' in filtered.columns:
            filtered['locations_list'] = filtered['locations'].apply(
                lambda x: x.split('|') if pd.notna(x) and x else []
            )

    # Categorize statements for delay attribution
    # Categories that indicate potentially unjustified delays
    unjustified_categories = ['QUALITY_ISSUE', 'COORDINATION', 'CONTRACTOR_PERFORMANCE',
                              'REWORK', 'DEFECT', 'FAILURE']
    # Categories that indicate potentially justified delays
    justified_categories = ['WEATHER', 'OWNER_CHANGE', 'DESIGN_CHANGE', 'FORCE_MAJEURE',
                           'PERMIT', 'SUPPLY_CHAIN']

    if not filtered.empty and 'category' in filtered.columns:
        filtered['delay_justified'] = filtered['category'].apply(
            lambda x: 'JUSTIFIED' if x in justified_categories
                      else ('UNJUSTIFIED' if x in unjustified_categories
                            else 'UNKNOWN')
        )

    # Coverage notes
    notes = []
    total_statements = len(statements)
    dated_count = len(dated_statements)
    notes.append(f"{dated_count}/{total_statements} statements have dates ({dated_count/total_statements*100:.0f}%)")

    if not filtered.empty:
        date_range = get_date_range(filtered, 'event_date')
        if date_range:
            notes.append(f"Event dates: {date_range[0]} to {date_range[1]}")

        if 'impact_days' in filtered.columns:
            with_impact = filtered['impact_days'].notna().sum()
            total_impact = filtered['impact_days'].sum()
            notes.append(f"{with_impact} statements with impact ({total_impact:.0f} total days)")

        if 'category' in filtered.columns:
            categories = filtered['category'].value_counts()
            top_cats = categories.head(3).to_dict()
            notes.append(f"Top categories: {top_cats}")

    availability = DataAvailability(
        source='Narratives',
        period=period,
        record_count=len(filtered),
        date_range=get_date_range(filtered, 'event_date') if not filtered.empty else None,
        coverage_notes=notes,
    )

    return {
        'statements': filtered,
        'statements_undated': undated_statements,
        'documents': documents,
        'availability': availability,
    }
```

**Step 2: Commit**

```bash
git add scripts/integrated_analysis/monthly_reports/data_loaders/narratives.py
git commit -m "feat(monthly-reports): add narratives data loader"
```

---

## Task 7: Update Init and Create Test Script

**Files:**
- Modify: `scripts/integrated_analysis/monthly_reports/data_loaders/__init__.py`
- Create: `scripts/integrated_analysis/monthly_reports/test_loaders.py`

**Step 1: Verify init exports are correct**

The init file from Task 1 should already have correct exports. Verify it matches.

**Step 2: Create test script**

Create `scripts/integrated_analysis/monthly_reports/test_loaders.py`:
```python
#!/usr/bin/env python3
"""Test script for monthly report data loaders.

Usage:
    python -m scripts.integrated_analysis.monthly_reports.test_loaders 2024-03
    python -m scripts.integrated_analysis.monthly_reports.test_loaders --all
"""

import argparse
import sys
from pathlib import Path
from datetime import date

_project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from scripts.integrated_analysis.monthly_reports.data_loaders import (
    get_monthly_period,
    load_schedule_data,
    load_labor_data,
    load_quality_data,
    load_narrative_data,
)


def test_period(year_month: str) -> None:
    """Test all loaders for a specific period."""
    print(f"\n{'='*60}")
    print(f"Testing period: {year_month}")
    print('='*60)

    period = get_monthly_period(year_month)
    print(f"Period: {period.start_date} to {period.end_date}")

    # Test each loader
    print("\n--- Schedule Data (P6) ---")
    schedule = load_schedule_data(period)
    print(f"Tasks: {len(schedule['tasks']):,}")
    print(f"Snapshots: {schedule['snapshots']}")
    print(f"Availability: {schedule['availability'].to_dict()}")

    if not schedule['tasks'].empty:
        df = schedule['tasks']
        print(f"  Completed this period: {df['completed_this_period'].sum():,}")
        print(f"  Started this period: {df['started_this_period'].sum():,}")
        if 'building' in df.columns:
            print(f"  Buildings: {df['building'].value_counts().head(3).to_dict()}")

    print("\n--- Quality Data (RABA + PSI) ---")
    quality = load_quality_data(period)
    print(f"Combined inspections: {len(quality['inspections']):,}")
    print(f"RABA: {len(quality['raba']):,}")
    print(f"PSI: {len(quality['psi']):,}")
    for avail in quality['availability']:
        print(f"  {avail.source}: {avail.to_dict()}")

    if not quality['inspections'].empty:
        df = quality['inspections']
        if 'outcome_normalized' in df.columns:
            print(f"  Outcomes: {df['outcome_normalized'].value_counts().to_dict()}")
        if 'dim_company_id' in df.columns:
            coverage = df['dim_company_id'].notna().mean() * 100
            print(f"  Company ID coverage: {coverage:.1f}%")

    print("\n--- Labor Data (ProjectSight + TBM + Weekly) ---")
    labor = load_labor_data(period)
    print(f"Combined entries: {len(labor['labor']):,}")
    print(f"ProjectSight: {len(labor['projectsight']):,}")
    print(f"TBM: {len(labor['tbm']):,}")
    print(f"Weekly Reports: {len(labor['weekly_reports']):,}")
    for avail in labor['availability']:
        print(f"  {avail.source}: {avail.to_dict()}")

    if not labor['labor'].empty:
        df = labor['labor']
        if 'hours' in df.columns:
            print(f"  Total hours: {df['hours'].sum():,.0f}")
        if 'source' in df.columns:
            print(f"  By source: {df.groupby('source')['hours'].sum().to_dict() if 'hours' in df.columns else df['source'].value_counts().to_dict()}")

    print("\n--- Narrative Data ---")
    narratives = load_narrative_data(period)
    print(f"Statements in period: {len(narratives['statements']):,}")
    print(f"Undated statements: {len(narratives['statements_undated']):,}")
    print(f"Source documents: {len(narratives['documents']):,}")
    print(f"Availability: {narratives['availability'].to_dict()}")

    if not narratives['statements'].empty:
        df = narratives['statements']
        if 'category' in df.columns:
            print(f"  Categories: {df['category'].value_counts().head(5).to_dict()}")
        if 'impact_days' in df.columns:
            with_impact = df['impact_days'].notna()
            print(f"  Statements with impact: {with_impact.sum()}")
            if with_impact.any():
                print(f"  Total impact days: {df.loc[with_impact, 'impact_days'].sum():.0f}")

    print("\n" + "="*60)


def find_periods_with_data() -> list:
    """Find all periods that have data in at least one source."""
    # Test a range of periods
    periods = []
    for year in range(2022, 2026):
        for month in range(1, 13):
            if year == 2022 and month < 5:
                continue  # Project starts around May 2022
            if year == 2025 and month > 12:
                continue
            periods.append(f"{year}-{month:02d}")
    return periods


def main():
    parser = argparse.ArgumentParser(description="Test monthly report data loaders")
    parser.add_argument(
        'period',
        nargs='?',
        default=None,
        help='Period in YYYY-MM format (e.g., 2024-03)'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Test all periods with potential data'
    )
    parser.add_argument(
        '--summary',
        action='store_true',
        help='Show summary only (record counts per period)'
    )

    args = parser.parse_args()

    if args.all:
        periods = find_periods_with_data()
        print(f"Testing {len(periods)} periods...")

        if args.summary:
            print("\nPeriod Summary:")
            print("-" * 80)
            print(f"{'Period':<10} {'P6':>10} {'Quality':>10} {'Labor':>10} {'Narratives':>10}")
            print("-" * 80)

            for year_month in periods:
                period = get_monthly_period(year_month)
                schedule = load_schedule_data(period)
                quality = load_quality_data(period)
                labor = load_labor_data(period)
                narratives = load_narrative_data(period)

                print(f"{year_month:<10} {len(schedule['tasks']):>10,} {len(quality['inspections']):>10,} {len(labor['labor']):>10,} {len(narratives['statements']):>10,}")
        else:
            for year_month in periods:
                test_period(year_month)
    elif args.period:
        test_period(args.period)
    else:
        # Default: test a sample period
        test_period('2024-06')


if __name__ == '__main__':
    main()
```

**Step 3: Make test script executable and run**

```bash
chmod +x scripts/integrated_analysis/monthly_reports/test_loaders.py
```

**Step 4: Commit**

```bash
git add scripts/integrated_analysis/monthly_reports/
git commit -m "feat(monthly-reports): add test script for data loaders"
```

---

## Task 8: Run Integration Test

**Step 1: Run test for a sample period**

```bash
python -m scripts.integrated_analysis.monthly_reports.test_loaders 2024-06
```

**Step 2: Run summary across all periods**

```bash
python -m scripts.integrated_analysis.monthly_reports.test_loaders --all --summary
```

**Step 3: Review output and fix any issues**

Expected output format:
```
Period Summary:
--------------------------------------------------------------------------------
Period          P6    Quality      Labor Narratives
--------------------------------------------------------------------------------
2022-05      1,234          0          0          0
2022-06      2,345        123      5,678          2
...
```

---

## Summary

After completing all tasks, you will have:

1. **Module structure** at `scripts/integrated_analysis/monthly_reports/data_loaders/`
2. **Base utilities** for period handling and date filtering
3. **Four data loaders:**
   - `primavera.py` - Schedule data with taxonomy enrichment
   - `quality.py` - Combined RABA + PSI inspections
   - `labor.py` - Combined ProjectSight + TBM + Weekly Reports
   - `narratives.py` - Extracted statements with delay categorization
4. **Test script** to verify loaders work across all periods

Each loader returns:
- Filtered DataFrames with standardized columns
- DataAvailability tracking for coverage reporting
- Source-specific DataFrames for detailed analysis
