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
    taxonomy_path = Settings.PRIMAVERA_DERIVED_DIR / 'task_taxonomy.csv'
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
