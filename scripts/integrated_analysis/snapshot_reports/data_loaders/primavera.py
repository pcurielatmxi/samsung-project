"""Primavera P6 schedule data loader for snapshot reports.

Loads schedule snapshots and provides aggregated progress summaries
for the period between two P6 data dates.
"""

import sys
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
import pandas as pd
import numpy as np

_project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import Settings
from .base import SnapshotPeriod, DataAvailability


@dataclass
class ProgressSummary:
    """Aggregated progress metrics for a grouping (floor, scope, overall)."""
    group_name: str
    group_type: str  # 'floor', 'scope', 'overall'

    # Task counts
    total_tasks: int = 0
    completed_start: int = 0
    completed_end: int = 0
    completed_this_period: int = 0

    # Percent complete (weighted by task or duration)
    pct_complete_start: float = 0.0
    pct_complete_end: float = 0.0
    pct_complete_change: float = 0.0

    # Delay indicators
    tasks_behind_schedule: int = 0
    tasks_with_negative_float: int = 0

    # Top completions (for narrative)
    top_completions: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'group_name': self.group_name,
            'group_type': self.group_type,
            'total_tasks': self.total_tasks,
            'completed_start': self.completed_start,
            'completed_end': self.completed_end,
            'completed_this_period': self.completed_this_period,
            'pct_complete_start': round(self.pct_complete_start, 1),
            'pct_complete_end': round(self.pct_complete_end, 1),
            'pct_complete_change': round(self.pct_complete_change, 1),
            'tasks_behind_schedule': self.tasks_behind_schedule,
            'tasks_with_negative_float': self.tasks_with_negative_float,
            'top_completions': self.top_completions,
        }


def _load_tasks_for_snapshot(file_id: int, taxonomy: pd.DataFrame) -> pd.DataFrame:
    """Load tasks for a specific snapshot and enrich with taxonomy."""
    task_path = Settings.PRIMAVERA_PROCESSED_DIR / 'task.csv'

    # Load only tasks for this file_id
    tasks = pd.read_csv(task_path, low_memory=False)
    tasks = tasks[tasks['file_id'] == file_id].copy()

    if tasks.empty:
        return pd.DataFrame()

    # Parse dates
    date_cols = ['act_start_date', 'act_end_date', 'target_start_date', 'target_end_date',
                 'early_start_date', 'early_end_date', 'late_start_date', 'late_end_date']
    for col in date_cols:
        if col in tasks.columns:
            tasks[col] = pd.to_datetime(tasks[col], errors='coerce')

    # Merge with taxonomy
    if not taxonomy.empty and 'task_id' in tasks.columns and 'task_id' in taxonomy.columns:
        tax_cols = ['task_id']
        for col in ['building', 'level', 'trade_code', 'trade_name', 'phase', 'phase_desc',
                    'location_type', 'location_code', 'room']:
            if col in taxonomy.columns:
                tax_cols.append(col)

        tasks = tasks.merge(taxonomy[tax_cols], on='task_id', how='left')

        # Create floor column (building-level)
        if 'building' in tasks.columns and 'level' in tasks.columns:
            tasks['floor'] = tasks['building'].fillna('') + '-' + tasks['level'].fillna('')
            tasks.loc[tasks['floor'] == '-', 'floor'] = 'UNKNOWN'

    # Determine if task is complete
    tasks['is_complete'] = (
        (tasks['status_code'] == 'TK_Complete') |
        (tasks['phys_complete_pct'] >= 100) |
        (tasks['act_end_date'].notna())
    )

    return tasks


def _calculate_progress_summary(
    start_tasks: pd.DataFrame,
    end_tasks: pd.DataFrame,
    group_col: Optional[str],
    group_name: str,
    group_type: str,
    data_date: pd.Timestamp,
) -> ProgressSummary:
    """Calculate progress summary for a group of tasks."""

    summary = ProgressSummary(group_name=group_name, group_type=group_type)

    if end_tasks.empty:
        return summary

    # Filter to group if specified
    if group_col and group_col in end_tasks.columns:
        end_group = end_tasks[end_tasks[group_col] == group_name]
        start_group = start_tasks[start_tasks[group_col] == group_name] if not start_tasks.empty else pd.DataFrame()
    else:
        end_group = end_tasks
        start_group = start_tasks

    if end_group.empty:
        return summary

    summary.total_tasks = len(end_group)
    summary.completed_end = end_group['is_complete'].sum()

    if not start_group.empty:
        summary.completed_start = start_group['is_complete'].sum()

    summary.completed_this_period = summary.completed_end - summary.completed_start

    # Percent complete (average of phys_complete_pct)
    if 'phys_complete_pct' in end_group.columns:
        summary.pct_complete_end = end_group['phys_complete_pct'].mean()
        if not start_group.empty and 'phys_complete_pct' in start_group.columns:
            summary.pct_complete_start = start_group['phys_complete_pct'].mean()

    summary.pct_complete_change = summary.pct_complete_end - summary.pct_complete_start

    # Delay indicators
    if 'total_float_hr_cnt' in end_group.columns:
        summary.tasks_with_negative_float = (end_group['total_float_hr_cnt'] < 0).sum()

    # Find tasks behind schedule (target end before data date, not complete)
    if 'target_end_date' in end_group.columns:
        overdue = (
            (end_group['target_end_date'] < data_date) &
            (~end_group['is_complete'])
        )
        summary.tasks_behind_schedule = overdue.sum()

    # Top completions this period (tasks completed in end but not in start)
    if not start_group.empty:
        start_complete_ids = set(start_group[start_group['is_complete']]['task_id'])
        end_complete = end_group[end_group['is_complete']]
        new_completions = end_complete[~end_complete['task_id'].isin(start_complete_ids)]

        # Summarize by location
        if not new_completions.empty and 'location_code' in new_completions.columns:
            location_counts = new_completions['location_code'].value_counts().head(5)
            summary.top_completions = [
                f"{loc}: {count} tasks" for loc, count in location_counts.items() if pd.notna(loc)
            ]

    return summary


def load_schedule_data(period: SnapshotPeriod) -> Dict[str, Any]:
    """Load P6 schedule data with aggregated progress summaries.

    Compares start vs end snapshots of the period to show progress.

    Args:
        period: SnapshotPeriod defining the two snapshots to compare

    Returns:
        Dict with:
        - 'overall': ProgressSummary for all tasks
        - 'by_floor': List of ProgressSummary by building-level
        - 'by_scope': List of ProgressSummary by trade
        - 'delay_tasks': DataFrame of tasks causing delays (top 20)
        - 'snapshots': Dict with start/end snapshot info
        - 'tasks_start': DataFrame of start snapshot tasks
        - 'tasks_end': DataFrame of end snapshot tasks
        - 'availability': DataAvailability info
    """
    # Load taxonomy
    taxonomy_path = Settings.PRIMAVERA_DERIVED_DIR / 'task_taxonomy.csv'
    taxonomy = pd.read_csv(taxonomy_path, low_memory=False) if taxonomy_path.exists() else pd.DataFrame()

    # Load tasks for both snapshots
    start_tasks = _load_tasks_for_snapshot(period.start_file_id, taxonomy)
    end_tasks = _load_tasks_for_snapshot(period.end_file_id, taxonomy)

    if end_tasks.empty:
        return {
            'overall': ProgressSummary(group_name='Overall', group_type='overall'),
            'by_floor': [],
            'by_scope': [],
            'delay_tasks': pd.DataFrame(),
            'snapshots': {'start': None, 'end': None},
            'tasks_start': pd.DataFrame(),
            'tasks_end': pd.DataFrame(),
            'availability': DataAvailability(
                source='P6',
                period=period,
                record_count=0,
                coverage_notes=['No tasks found for end snapshot'],
            ),
        }

    data_date = pd.Timestamp(period.end_data_date)

    # Calculate overall progress
    overall = _calculate_progress_summary(
        start_tasks, end_tasks, None, 'Overall', 'overall', data_date
    )

    # Calculate progress by floor
    by_floor = []
    if 'floor' in end_tasks.columns:
        floors = end_tasks['floor'].dropna().unique()
        for floor in sorted(floors):
            if floor and floor != 'UNKNOWN' and not floor.endswith('-'):
                summary = _calculate_progress_summary(
                    start_tasks, end_tasks, 'floor', floor, 'floor', data_date
                )
                if summary.total_tasks > 0:
                    by_floor.append(summary)

    # Calculate progress by scope (trade)
    by_scope = []
    if 'trade_name' in end_tasks.columns:
        trades = end_tasks['trade_name'].dropna().unique()
        for trade in sorted(trades):
            if trade:
                summary = _calculate_progress_summary(
                    start_tasks, end_tasks, 'trade_name', trade, 'scope', data_date
                )
                if summary.total_tasks > 0:
                    by_scope.append(summary)

    # Identify delay-causing tasks (negative float, behind schedule)
    delay_tasks = pd.DataFrame()
    if not end_tasks.empty:
        delay_mask = (
            (end_tasks['total_float_hr_cnt'] < 0) |
            (
                (end_tasks['target_end_date'] < data_date) &
                (~end_tasks['is_complete'])
            )
        )
        delay_candidates = end_tasks[delay_mask].copy()

        if not delay_candidates.empty:
            # Sort by float (most negative first)
            delay_candidates = delay_candidates.sort_values('total_float_hr_cnt', ascending=True)

            # Select key columns for output
            delay_cols = ['task_code', 'task_name', 'floor', 'trade_name', 'location_code',
                         'phys_complete_pct', 'total_float_hr_cnt', 'target_end_date', 'status_code']
            available_cols = [c for c in delay_cols if c in delay_candidates.columns]
            delay_tasks = delay_candidates[available_cols].head(20)

    # Build snapshot info
    snapshot_info = {
        'start': {
            'file_id': period.start_file_id,
            'data_date': period.start_data_date.isoformat(),
            'task_count': len(start_tasks),
        },
        'end': {
            'file_id': period.end_file_id,
            'data_date': period.end_data_date.isoformat(),
            'task_count': len(end_tasks),
        },
    }

    # Build availability
    coverage_notes = [
        f"Start: {period.start_data_date} (file_id={period.start_file_id}, {len(start_tasks):,} tasks)",
        f"End: {period.end_data_date} (file_id={period.end_file_id}, {len(end_tasks):,} tasks)",
        f"Period: {period.duration_days} days",
        f"Completed this period: {overall.completed_this_period:,}",
    ]

    availability = DataAvailability(
        source='P6',
        period=period,
        record_count=len(end_tasks),
        date_range=(period.start_data_date, period.end_data_date),
        coverage_notes=coverage_notes,
    )

    return {
        'overall': overall,
        'by_floor': by_floor,
        'by_scope': by_scope,
        'delay_tasks': delay_tasks,
        'snapshots': snapshot_info,
        'tasks_start': start_tasks,
        'tasks_end': end_tasks,
        'availability': availability,
    }
