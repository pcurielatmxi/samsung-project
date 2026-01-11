"""
Data Loader for P6 Schedule Data.

Loads task, dependency, and calendar data from P6 CSV exports
and constructs TaskNetwork objects for CPM analysis.
"""

import sys
from pathlib import Path
import pandas as pd
from typing import Optional

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config.settings import Settings
from .cpm.models import Task, Dependency
from .cpm.network import TaskNetwork
from .cpm.calendar import P6Calendar


def load_calendars(file_id: int, data_dir: Path = None) -> dict[str, P6Calendar]:
    """
    Load and parse calendars for a schedule version.

    Args:
        file_id: The schedule version file_id
        data_dir: Directory containing CSV files (default: PRIMAVERA_PROCESSED_DIR)

    Returns:
        Dict mapping clndr_id to P6Calendar objects
    """
    if data_dir is None:
        data_dir = Settings.PRIMAVERA_PROCESSED_DIR

    df = pd.read_csv(data_dir / 'calendar.csv')
    df = df[df['file_id'] == file_id]

    calendars = {}
    for _, row in df.iterrows():
        clndr_id = str(row['clndr_id'])
        clndr_data = row['clndr_data'] if pd.notna(row['clndr_data']) else ''
        day_hr_cnt = float(row['day_hr_cnt']) if pd.notna(row['day_hr_cnt']) else 8.0
        clndr_name = str(row['clndr_name']) if pd.notna(row['clndr_name']) else ''

        cal = P6Calendar.from_p6_data(
            clndr_id=clndr_id,
            clndr_data=clndr_data,
            day_hr_cnt=day_hr_cnt,
            clndr_name=clndr_name,
        )
        calendars[clndr_id] = cal

    return calendars


def load_tasks(file_id: int, data_dir: Path = None,
               include_p6_values: bool = True) -> dict[str, Task]:
    """
    Load tasks for a schedule version.

    Args:
        file_id: The schedule version file_id
        data_dir: Directory containing CSV files
        include_p6_values: If True, include P6's calculated dates for comparison

    Returns:
        Dict mapping task_id to Task objects
    """
    if data_dir is None:
        data_dir = Settings.PRIMAVERA_PROCESSED_DIR

    df = pd.read_csv(data_dir / 'task.csv')
    df = df[df['file_id'] == file_id]

    tasks = {}
    for _, row in df.iterrows():
        task_id = str(row['task_id'])

        # Parse dates
        def parse_date(val):
            if pd.isna(val) or val == '':
                return None
            try:
                return pd.to_datetime(val)
            except Exception:
                return None

        task = Task(
            task_id=task_id,
            task_code=str(row['task_code']) if pd.notna(row['task_code']) else '',
            task_name=str(row['task_name']) if pd.notna(row['task_name']) else '',
            duration_hours=float(row['target_drtn_hr_cnt']) if pd.notna(row['target_drtn_hr_cnt']) else 0.0,
            calendar_id=str(row['clndr_id']) if pd.notna(row['clndr_id']) else '',
            status=str(row['status_code']) if pd.notna(row['status_code']) else 'TK_NotStart',
            task_type=str(row['task_type']) if pd.notna(row['task_type']) else 'TT_Task',
            wbs_id=str(row['wbs_id']) if pd.notna(row['wbs_id']) else '',
            constraint_date=parse_date(row.get('cstr_date')),
            constraint_type=str(row['cstr_type']) if pd.notna(row.get('cstr_type')) else None,
            actual_start=parse_date(row.get('act_start_date')),
            actual_finish=parse_date(row.get('act_end_date')),
            remaining_duration_hours=float(row['remain_drtn_hr_cnt']) if pd.notna(row.get('remain_drtn_hr_cnt')) else 0.0,
        )

        # Include P6's calculated values for comparison
        if include_p6_values:
            task.p6_early_start = parse_date(row.get('early_start_date'))
            task.p6_early_finish = parse_date(row.get('early_end_date'))
            task.p6_late_start = parse_date(row.get('late_start_date'))
            task.p6_late_finish = parse_date(row.get('late_end_date'))
            task.p6_total_float_hours = float(row['total_float_hr_cnt']) if pd.notna(row.get('total_float_hr_cnt')) else None
            task.p6_driving_path_flag = str(row.get('driving_path_flag', '')) == 'Y'

        tasks[task_id] = task

    return tasks


def load_dependencies(file_id: int, data_dir: Path = None) -> list[Dependency]:
    """
    Load dependencies for a schedule version.

    Args:
        file_id: The schedule version file_id
        data_dir: Directory containing CSV files

    Returns:
        List of Dependency objects
    """
    if data_dir is None:
        data_dir = Settings.PRIMAVERA_PROCESSED_DIR

    df = pd.read_csv(data_dir / 'taskpred.csv')
    df = df[df['file_id'] == file_id]

    dependencies = []
    for _, row in df.iterrows():
        dep = Dependency(
            pred_task_id=str(row['pred_task_id']),
            succ_task_id=str(row['task_id']),
            pred_type=str(row['pred_type']) if pd.notna(row['pred_type']) else 'PR_FS',
            lag_hours=float(row['lag_hr_cnt']) if pd.notna(row['lag_hr_cnt']) else 0.0,
        )
        dependencies.append(dep)

    return dependencies


def load_project_info(file_id: int, data_dir: Path = None) -> dict:
    """
    Load project-level information including data date.

    Args:
        file_id: The schedule version file_id
        data_dir: Directory containing CSV files

    Returns:
        Dict with project info including data_date, target_finish, etc.
    """
    if data_dir is None:
        data_dir = Settings.PRIMAVERA_PROCESSED_DIR

    df = pd.read_csv(data_dir / 'project.csv')
    df = df[df['file_id'] == file_id]

    if len(df) == 0:
        return {}

    row = df.iloc[0]

    def parse_date(val):
        if pd.isna(val) or val == '':
            return None
        try:
            return pd.to_datetime(val)
        except Exception:
            return None

    return {
        'data_date': parse_date(row.get('last_recalc_date')),
        'plan_start_date': parse_date(row.get('plan_start_date')),
        'plan_end_date': parse_date(row.get('plan_end_date')),
        'target_finish_date': parse_date(row.get('scd_end_date')),
        'proj_id': str(row.get('proj_id', '')),
        'proj_short_name': str(row.get('proj_short_name', '')),
    }


def load_schedule(
    file_id: int,
    data_dir: Path = None,
    include_p6_values: bool = True,
    verbose: bool = False,
) -> tuple[TaskNetwork, dict[str, P6Calendar], dict]:
    """
    Load a complete schedule version.

    Args:
        file_id: The schedule version file_id
        data_dir: Directory containing CSV files
        include_p6_values: Include P6's calculated dates for comparison
        verbose: Print progress messages

    Returns:
        Tuple of (TaskNetwork, calendars dict, project_info dict)
    """
    if data_dir is None:
        data_dir = Settings.PRIMAVERA_PROCESSED_DIR

    if verbose:
        print(f"Loading schedule file_id={file_id} from {data_dir}")

    # Load project info (includes data_date)
    project_info = load_project_info(file_id, data_dir)
    if verbose and project_info.get('data_date'):
        print(f"  Data date: {project_info['data_date']}")

    # Load calendars
    calendars = load_calendars(file_id, data_dir)
    if verbose:
        print(f"  Loaded {len(calendars)} calendars")

    # Load tasks
    tasks = load_tasks(file_id, data_dir, include_p6_values)
    if verbose:
        print(f"  Loaded {len(tasks)} tasks")

    # Load dependencies
    dependencies = load_dependencies(file_id, data_dir)
    if verbose:
        print(f"  Loaded {len(dependencies)} dependencies")

    # Build network
    network = TaskNetwork()
    for task in tasks.values():
        network.add_task(task)

    skipped = 0
    for dep in dependencies:
        if not network.add_dependency_safe(dep):
            skipped += 1

    if verbose:
        if skipped > 0:
            print(f"  Skipped {skipped} dependencies (missing tasks)")
        stats = network.get_statistics()
        print(f"  Network: {stats['total_tasks']} tasks, {stats['total_dependencies']} deps")
        print(f"  Start tasks: {stats['start_tasks']}, End tasks: {stats['end_tasks']}")

    return network, calendars, project_info


def get_file_id_for_date(target_date: str, data_dir: Path = None) -> int:
    """
    Get the file_id closest to a target date.

    Args:
        target_date: Target date string (YYYY-MM-DD format)
        data_dir: Directory containing CSV files

    Returns:
        file_id of the closest schedule version
    """
    if data_dir is None:
        data_dir = Settings.PRIMAVERA_PROCESSED_DIR

    df = pd.read_csv(data_dir / 'xer_files.csv')
    df['date'] = pd.to_datetime(df['date'])
    target = pd.to_datetime(target_date)

    df['diff'] = abs(df['date'] - target)
    closest = df.loc[df['diff'].idxmin()]
    return int(closest['file_id'])


def get_latest_file_id(data_dir: Path = None) -> int:
    """
    Get the file_id of the latest (current) schedule.

    Args:
        data_dir: Directory containing CSV files

    Returns:
        file_id of the latest schedule version
    """
    if data_dir is None:
        data_dir = Settings.PRIMAVERA_PROCESSED_DIR

    df = pd.read_csv(data_dir / 'xer_files.csv')

    # Use is_current flag if available
    if 'is_current' in df.columns:
        current = df[df['is_current'] == True]
        if len(current) > 0:
            return int(current.iloc[0]['file_id'])

    # Otherwise use the highest file_id
    return int(df['file_id'].max())


def get_baseline_file_id(data_dir: Path = None) -> int:
    """
    Get the file_id of the baseline (first) schedule.

    Args:
        data_dir: Directory containing CSV files

    Returns:
        file_id of the first schedule version
    """
    if data_dir is None:
        data_dir = Settings.PRIMAVERA_PROCESSED_DIR

    df = pd.read_csv(data_dir / 'xer_files.csv')
    return int(df['file_id'].min())


def list_schedule_versions(data_dir: Path = None) -> pd.DataFrame:
    """
    List all available schedule versions.

    Args:
        data_dir: Directory containing CSV files

    Returns:
        DataFrame with file_id, date, filename, is_current columns
    """
    if data_dir is None:
        data_dir = Settings.PRIMAVERA_PROCESSED_DIR

    df = pd.read_csv(data_dir / 'xer_files.csv')
    return df[['file_id', 'date', 'filename', 'is_current']].copy()


def load_multiple_schedules(
    file_ids: list[int],
    data_dir: Path = None,
    verbose: bool = False,
) -> dict[int, tuple[TaskNetwork, dict[str, P6Calendar]]]:
    """
    Load multiple schedule versions.

    Args:
        file_ids: List of file_ids to load
        data_dir: Directory containing CSV files
        verbose: Print progress messages

    Returns:
        Dict mapping file_id to (TaskNetwork, calendars) tuple
    """
    results = {}
    for file_id in file_ids:
        if verbose:
            print(f"\nLoading schedule {file_id}...")
        network, calendars = load_schedule(file_id, data_dir, verbose=verbose)
        results[file_id] = (network, calendars)
    return results
