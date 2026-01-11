"""
Critical Path Analysis.

Identifies critical and near-critical tasks, analyzes float distribution,
and identifies schedule risk areas.
"""

from datetime import datetime
from collections import defaultdict
from typing import Optional

from ..cpm.models import Task, CriticalPathResult
from ..cpm.calendar import P6Calendar
from ..cpm.network import TaskNetwork
from ..cpm.engine import CPMEngine


def analyze_critical_path(
    network: TaskNetwork,
    calendars: dict[str, P6Calendar],
    near_critical_threshold_hours: float = 40.0,  # 5 work days
    project_start: datetime = None,
    data_date: datetime = None,
) -> CriticalPathResult:
    """
    Analyze critical path and near-critical tasks.

    Args:
        network: Task network to analyze
        calendars: Calendar lookup dict
        near_critical_threshold_hours: Float threshold for near-critical classification
        project_start: Project start date (auto-detected if None)
        data_date: Schedule status date (P6's "data date"). If provided, used as
                   data_date for CPM. If None and project_start provided, uses
                   project_start as data_date.

    Returns:
        CriticalPathResult with critical path, near-critical tasks, and statistics
    """
    # Run CPM
    engine = CPMEngine(network, calendars)
    # If data_date not specified but project_start is, use project_start as data_date
    effective_data_date = data_date if data_date is not None else project_start
    result = engine.run(project_start, data_date=effective_data_date)

    # Categorize tasks by float
    critical = []
    near_critical = []
    float_buckets = defaultdict(int)

    for task in network.tasks.values():
        if task.total_float_hours is None:
            float_buckets['unknown'] += 1
            continue

        # Categorize by float amount
        if task.total_float_hours <= 0:
            critical.append(task)
            float_buckets['0 (critical)'] += 1
        elif task.total_float_hours <= 8:
            near_critical.append(task)
            float_buckets['1-8 hrs (< 1 day)'] += 1
        elif task.total_float_hours <= 40:
            near_critical.append(task)
            float_buckets['9-40 hrs (1-5 days)'] += 1
        elif task.total_float_hours <= 80:
            float_buckets['41-80 hrs (5-10 days)'] += 1
        elif task.total_float_hours <= 160:
            float_buckets['81-160 hrs (10-20 days)'] += 1
        else:
            float_buckets['>160 hrs (> 20 days)'] += 1

    # Sort critical path by early start
    critical.sort(key=lambda t: t.early_start or datetime.max)

    # Filter near-critical to those within threshold and sort by float
    near_critical = [
        t for t in near_critical
        if t.total_float_hours is not None and t.total_float_hours <= near_critical_threshold_hours
    ]
    near_critical.sort(key=lambda t: t.total_float_hours or float('inf'))

    return CriticalPathResult(
        critical_path=critical,
        near_critical_tasks=near_critical,
        float_distribution=dict(float_buckets),
        project_finish=result.project_finish,
        near_critical_threshold_hours=near_critical_threshold_hours,
        total_tasks=len(network.tasks),
    )


def get_critical_path_by_wbs(
    network: TaskNetwork,
    calendars: dict[str, P6Calendar],
    wbs_level: int = 3,
    data_date: datetime = None,
) -> dict[str, list[Task]]:
    """
    Get critical tasks grouped by WBS.

    Args:
        network: Task network
        calendars: Calendar lookup
        wbs_level: WBS hierarchy level for grouping
        data_date: Schedule status date (P6's "data date")

    Returns:
        Dict mapping WBS name to list of critical tasks
    """
    # Run CPM
    engine = CPMEngine(network, calendars)
    engine.run(data_date=data_date)

    # Group critical tasks by WBS
    by_wbs = defaultdict(list)
    for task in network.tasks.values():
        if task.is_critical:
            wbs_key = task.wbs_id  # Could enhance to use WBS name at specific level
            by_wbs[wbs_key].append(task)

    # Sort tasks within each group
    for wbs_key in by_wbs:
        by_wbs[wbs_key].sort(key=lambda t: t.early_start or datetime.max)

    return dict(by_wbs)


def identify_risk_tasks(
    network: TaskNetwork,
    calendars: dict[str, P6Calendar],
    float_threshold_hours: float = 40.0,
    min_duration_hours: float = 40.0,
    data_date: datetime = None,
) -> list[Task]:
    """
    Identify high-risk tasks that could become critical.

    Criteria:
    - Near-critical (float <= threshold)
    - Significant duration (duration >= min_duration)
    - Not already completed

    Args:
        network: Task network
        calendars: Calendar lookup
        float_threshold_hours: Float threshold for near-critical
        min_duration_hours: Minimum duration to consider
        data_date: Schedule status date (P6's "data date")

    Returns:
        List of risk tasks sorted by (float, duration desc)
    """
    engine = CPMEngine(network, calendars)
    engine.run(data_date=data_date)

    risk_tasks = []
    for task in network.tasks.values():
        if task.is_completed():
            continue
        if task.total_float_hours is None:
            continue
        if task.total_float_hours > float_threshold_hours:
            continue
        if task.total_float_hours <= 0:
            continue  # Already critical
        if task.duration_hours < min_duration_hours:
            continue

        risk_tasks.append(task)

    # Sort by float (ascending), then by duration (descending)
    risk_tasks.sort(key=lambda t: (t.total_float_hours or 0, -t.duration_hours))

    return risk_tasks


def print_critical_path_report(result: CriticalPathResult) -> None:
    """Print a formatted critical path report."""
    print("=" * 80)
    print("CRITICAL PATH ANALYSIS REPORT")
    print("=" * 80)

    print(f"\nProject Finish: {result.project_finish}")
    print(f"Total Tasks: {result.total_tasks}")
    print(f"Critical Tasks: {len(result.critical_path)}")
    print(f"Near-Critical Tasks (< {result.near_critical_threshold_hours/8:.0f} days float): "
          f"{len(result.near_critical_tasks)}")

    print("\n--- Float Distribution ---")
    for bucket, count in sorted(result.float_distribution.items()):
        pct = count / result.total_tasks * 100
        bar = '#' * int(pct / 2)
        print(f"  {bucket:25s}: {count:5d} ({pct:5.1f}%) {bar}")

    print("\n--- Critical Path (first 20 tasks) ---")
    for i, task in enumerate(result.critical_path[:20]):
        print(f"  {i+1:3d}. {task.task_code:20s} | {task.task_name[:40]:40s} | "
              f"{task.duration_hours/8:.1f}d")

    if len(result.critical_path) > 20:
        print(f"  ... and {len(result.critical_path) - 20} more critical tasks")

    print("\n--- Near-Critical Tasks (first 10) ---")
    for i, task in enumerate(result.near_critical_tasks[:10]):
        float_days = task.total_float_hours / 8 if task.total_float_hours else 0
        print(f"  {i+1:3d}. {task.task_code:20s} | Float: {float_days:5.1f}d | "
              f"{task.task_name[:35]:35s}")

    print("\n" + "=" * 80)
