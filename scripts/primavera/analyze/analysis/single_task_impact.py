"""
Single Task Impact Analysis.

Analyze the schedule impact of changing a single task's duration.
Used for what-if scenarios and sensitivity analysis.
"""

from datetime import datetime
from typing import Optional

from ..cpm.models import Task, TaskImpactResult
from ..cpm.calendar import P6Calendar
from ..cpm.network import TaskNetwork
from ..cpm.engine import CPMEngine


def analyze_task_impact(
    network: TaskNetwork,
    calendars: dict[str, P6Calendar],
    task_id: str,
    duration_delta_hours: float,
    project_start: datetime = None,
    data_date: datetime = None,
) -> TaskImpactResult:
    """
    Calculate impact of changing one task's duration.

    Args:
        network: Task network (will not be modified)
        calendars: Calendar lookup dict
        task_id: ID of task to modify
        duration_delta_hours: Change in duration (positive = increase)
        project_start: Project start date (auto-detected if None)
        data_date: Schedule status date (P6's "data date")

    Returns:
        TaskImpactResult with original vs new finish dates and affected tasks
    """
    if task_id not in network.tasks:
        raise ValueError(f"Task {task_id} not found in network")

    task = network.tasks[task_id]

    # Run baseline CPM
    baseline_engine = CPMEngine(network, calendars)
    baseline_result = baseline_engine.run(project_start, data_date=data_date)

    # Clone network and modify task
    modified_network = network.clone()
    new_duration = task.duration_hours + duration_delta_hours
    if new_duration < 0:
        new_duration = 0
    modified_network.modify_task_duration(task_id, new_duration)

    # Run modified CPM
    modified_engine = CPMEngine(modified_network, calendars)
    modified_result = modified_engine.run(project_start, data_date=data_date)

    # Find affected tasks (tasks whose early_finish changed)
    affected = []
    for tid, modified_task in modified_network.tasks.items():
        baseline_task = network.tasks[tid]
        if modified_task.early_finish != baseline_task.early_finish:
            affected.append(tid)

    # Calculate slip in work hours
    if calendars:
        calendar = next(iter(calendars.values()))
        if modified_result.project_finish > baseline_result.project_finish:
            slip_hours = calendar.work_hours_between(
                baseline_result.project_finish,
                modified_result.project_finish
            )
        elif modified_result.project_finish < baseline_result.project_finish:
            slip_hours = -calendar.work_hours_between(
                modified_result.project_finish,
                baseline_result.project_finish
            )
        else:
            slip_hours = 0.0
    else:
        # Fallback to calendar hours
        diff = (modified_result.project_finish - baseline_result.project_finish)
        slip_hours = diff.total_seconds() / 3600

    # Check if critical path changed
    cp_changed = set(baseline_result.critical_path) != set(modified_result.critical_path)

    return TaskImpactResult(
        task_id=task_id,
        task_code=task.task_code,
        task_name=task.task_name,
        duration_delta_hours=duration_delta_hours,
        original_finish=baseline_result.project_finish,
        new_finish=modified_result.project_finish,
        slip_hours=slip_hours,
        slip_days=slip_hours / 8.0,
        affected_task_ids=affected,
        original_critical_path=baseline_result.critical_path,
        new_critical_path=modified_result.critical_path,
        critical_path_changed=cp_changed,
    )


def analyze_task_sensitivity(
    network: TaskNetwork,
    calendars: dict[str, P6Calendar],
    task_ids: list[str] = None,
    duration_delta_hours: float = 40.0,  # 5 days default
    project_start: datetime = None,
    data_date: datetime = None,
) -> list[TaskImpactResult]:
    """
    Analyze sensitivity of multiple tasks.

    Tests the impact of increasing each task's duration by the same amount.
    Useful for identifying which tasks have the most schedule risk.

    Args:
        network: Task network
        calendars: Calendar lookup dict
        task_ids: List of task IDs to analyze (default: all incomplete tasks)
        duration_delta_hours: Duration increase to test
        project_start: Project start date
        data_date: Schedule status date (P6's "data date")

    Returns:
        List of TaskImpactResult sorted by slip_hours (descending)
    """
    if task_ids is None:
        # Default: analyze incomplete tasks with non-zero duration
        task_ids = [
            t.task_id for t in network.tasks.values()
            if not t.is_completed() and t.duration_hours > 0
        ]

    results = []
    for task_id in task_ids:
        try:
            result = analyze_task_impact(
                network, calendars, task_id,
                duration_delta_hours, project_start, data_date
            )
            results.append(result)
        except Exception as e:
            # Skip tasks that fail (e.g., orphan tasks)
            continue

    # Sort by slip impact (descending)
    results.sort(key=lambda r: r.slip_hours, reverse=True)

    return results


def analyze_critical_task_sensitivity(
    network: TaskNetwork,
    calendars: dict[str, P6Calendar],
    duration_delta_hours: float = 40.0,
    project_start: datetime = None,
    data_date: datetime = None,
) -> list[TaskImpactResult]:
    """
    Analyze sensitivity of critical path tasks only.

    Args:
        network: Task network
        calendars: Calendar lookup dict
        duration_delta_hours: Duration increase to test
        project_start: Project start date
        data_date: Schedule status date (P6's "data date")

    Returns:
        List of TaskImpactResult for critical tasks
    """
    # Run CPM to identify critical tasks
    engine = CPMEngine(network, calendars)
    result = engine.run(project_start, data_date=data_date)

    critical_ids = [
        tid for tid in result.critical_path
        if not network.tasks[tid].is_completed()
    ]

    return analyze_task_sensitivity(
        network, calendars, critical_ids,
        duration_delta_hours, project_start, data_date
    )


def find_task_by_code(network: TaskNetwork, task_code: str) -> Optional[str]:
    """
    Find task_id by task_code.

    Args:
        network: Task network
        task_code: Task code to search for

    Returns:
        task_id if found, None otherwise
    """
    for task in network.tasks.values():
        if task.task_code == task_code:
            return task.task_id
    return None


def print_impact_report(result: TaskImpactResult) -> None:
    """Print a formatted impact report."""
    print("=" * 70)
    print("TASK IMPACT ANALYSIS")
    print("=" * 70)

    print(f"\nTask: {result.task_code}")
    print(f"Name: {result.task_name}")
    print(f"Duration Change: {result.duration_delta_hours:+.0f} hours "
          f"({result.duration_delta_hours/8:+.1f} days)")

    print(f"\nProject Finish:")
    print(f"  Original: {result.original_finish}")
    print(f"  New:      {result.new_finish}")
    print(f"  Slip:     {result.slip_hours:+.0f} hours ({result.slip_days:+.1f} days)")

    print(f"\nAffected Tasks: {len(result.affected_task_ids)}")
    print(f"Critical Path Changed: {'Yes' if result.critical_path_changed else 'No'}")

    if result.critical_path_changed:
        added = set(result.new_critical_path) - set(result.original_critical_path)
        removed = set(result.original_critical_path) - set(result.new_critical_path)
        if added:
            print(f"  Tasks now critical: {len(added)}")
        if removed:
            print(f"  Tasks no longer critical: {len(removed)}")

    print("=" * 70)


def print_sensitivity_report(results: list[TaskImpactResult], top_n: int = 20) -> None:
    """Print a sensitivity analysis report."""
    print("=" * 80)
    print("TASK SENSITIVITY ANALYSIS")
    print("=" * 80)

    if not results:
        print("\nNo tasks analyzed.")
        return

    delta = results[0].duration_delta_hours
    print(f"\nTested: +{delta:.0f} hours ({delta/8:.1f} days) per task")
    print(f"Tasks Analyzed: {len(results)}")

    # Count tasks with impact
    with_impact = [r for r in results if r.slip_hours > 0]
    print(f"Tasks with Schedule Impact: {len(with_impact)}")

    print(f"\n--- Top {min(top_n, len(results))} Most Sensitive Tasks ---")
    print(f"{'Rank':4s} {'Task Code':20s} {'Slip (days)':>12s} {'Task Name':40s}")
    print("-" * 80)

    for i, result in enumerate(results[:top_n]):
        print(f"{i+1:4d} {result.task_code:20s} {result.slip_days:>12.1f} "
              f"{result.task_name[:40]:40s}")

    if len(results) > top_n:
        remaining_impact = sum(r.slip_hours for r in results[top_n:] if r.slip_hours > 0)
        print(f"\n... and {len(results) - top_n} more tasks "
              f"(combined potential impact: {remaining_impact/8:.1f} days)")

    print("\n" + "=" * 80)
