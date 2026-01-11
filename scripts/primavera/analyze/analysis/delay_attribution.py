"""
Delay Attribution Analysis.

Compare schedule versions to identify which tasks and duration changes
drove schedule slippage.
"""

from datetime import datetime
from pathlib import Path
from collections import defaultdict
from typing import Optional

from ..cpm.models import Task, TaskContribution, DelayAttributionResult
from ..cpm.calendar import P6Calendar
from ..cpm.network import TaskNetwork
from ..cpm.engine import CPMEngine
from ..data_loader import load_schedule


def attribute_delays(
    baseline_file_id: int,
    current_file_id: int,
    data_dir: Path = None,
    verbose: bool = False,
) -> DelayAttributionResult:
    """
    Compare two schedule versions and attribute slip to specific tasks.

    Uses marginal analysis: identifies tasks with increased duration and
    calculates their contribution to total schedule slip.

    Args:
        baseline_file_id: Earlier schedule version file_id
        current_file_id: Later schedule version file_id
        data_dir: Directory containing P6 CSVs
        verbose: Print progress messages

    Returns:
        DelayAttributionResult with total slip and per-task contributions
    """
    if verbose:
        print(f"Loading baseline schedule (file_id={baseline_file_id})...")
    baseline_network, baseline_cals = load_schedule(baseline_file_id, data_dir, verbose=verbose)

    if verbose:
        print(f"\nLoading current schedule (file_id={current_file_id})...")
    current_network, current_cals = load_schedule(current_file_id, data_dir, verbose=verbose)

    # Run CPM on both
    if verbose:
        print("\nRunning CPM on baseline...")
    baseline_engine = CPMEngine(baseline_network, baseline_cals)
    baseline_result = baseline_engine.run()

    if verbose:
        print("Running CPM on current...")
    current_engine = CPMEngine(current_network, current_cals)
    current_result = current_engine.run()

    # Match tasks by task_code (task_id includes file_id prefix)
    baseline_by_code = {t.task_code: t for t in baseline_network.tasks.values()}
    current_by_code = {t.task_code: t for t in current_network.tasks.values()}

    # Find new and removed tasks
    baseline_codes = set(baseline_by_code.keys())
    current_codes = set(current_by_code.keys())
    new_tasks = list(current_codes - baseline_codes)
    removed_tasks = list(baseline_codes - current_codes)
    matched_codes = baseline_codes & current_codes

    if verbose:
        print(f"\nTask matching:")
        print(f"  Baseline tasks: {len(baseline_codes)}")
        print(f"  Current tasks: {len(current_codes)}")
        print(f"  Matched: {len(matched_codes)}")
        print(f"  New: {len(new_tasks)}")
        print(f"  Removed: {len(removed_tasks)}")

    # Analyze duration changes for matched tasks
    contributions = []
    for code in matched_codes:
        baseline_task = baseline_by_code[code]
        current_task = current_by_code[code]

        # Calculate duration change
        baseline_duration = baseline_task.duration_hours
        current_duration = current_task.duration_hours
        delta = current_duration - baseline_duration

        if delta > 0:
            # Task duration increased
            # Estimate contribution: if on critical path, duration increase = slip
            # More sophisticated: marginal impact analysis
            if current_task.is_critical:
                slip_contribution = delta
            else:
                # Non-critical task: only contributes if it exceeded its float
                if current_task.total_float_hours is not None:
                    excess = delta - current_task.total_float_hours
                    slip_contribution = max(0, excess)
                else:
                    slip_contribution = 0

            if slip_contribution > 0:
                contributions.append(TaskContribution(
                    task_id=current_task.task_id,
                    task_code=code,
                    task_name=current_task.task_name,
                    baseline_duration_hours=baseline_duration,
                    current_duration_hours=current_duration,
                    duration_delta_hours=delta,
                    slip_contribution_hours=slip_contribution,
                    slip_contribution_pct=0,  # Calculated below
                    is_on_critical_path=current_task.is_critical,
                ))

    # Calculate total slip
    if current_cals:
        calendar = next(iter(current_cals.values()))
        if current_result.project_finish > baseline_result.project_finish:
            total_slip = calendar.work_hours_between(
                baseline_result.project_finish,
                current_result.project_finish
            )
        else:
            total_slip = 0.0
    else:
        diff = (current_result.project_finish - baseline_result.project_finish)
        total_slip = max(0, diff.total_seconds() / 3600)

    # Calculate percentages
    total_contribution = sum(c.slip_contribution_hours for c in contributions)
    for c in contributions:
        if total_contribution > 0:
            c.slip_contribution_pct = c.slip_contribution_hours / total_contribution * 100
        else:
            c.slip_contribution_pct = 0

    # Sort by contribution (descending)
    contributions.sort(key=lambda c: c.slip_contribution_hours, reverse=True)

    return DelayAttributionResult(
        baseline_file_id=baseline_file_id,
        current_file_id=current_file_id,
        baseline_finish=baseline_result.project_finish,
        current_finish=current_result.project_finish,
        total_slip_hours=total_slip,
        total_slip_days=total_slip / 8.0,
        task_contributions=contributions,
        new_tasks=new_tasks,
        removed_tasks=removed_tasks,
        matched_tasks=len(matched_codes),
    )


def attribute_delays_marginal(
    baseline_file_id: int,
    current_file_id: int,
    data_dir: Path = None,
    top_n: int = 50,
    verbose: bool = False,
) -> DelayAttributionResult:
    """
    Attribute delays using marginal impact analysis.

    More accurate than simple attribution: tests actual schedule impact
    of each task's duration change by running CPM with/without the change.

    Warning: This is computationally expensive (runs CPM for each candidate task).

    Args:
        baseline_file_id: Earlier schedule version
        current_file_id: Later schedule version
        data_dir: Directory containing P6 CSVs
        top_n: Only test top N candidate tasks (by duration increase)
        verbose: Print progress messages

    Returns:
        DelayAttributionResult with precise marginal contributions
    """
    if verbose:
        print("Loading schedules...")

    baseline_network, baseline_cals = load_schedule(baseline_file_id, data_dir)
    current_network, current_cals = load_schedule(current_file_id, data_dir)

    # Get baseline CPM
    baseline_engine = CPMEngine(baseline_network, baseline_cals)
    baseline_result = baseline_engine.run()

    # Get current CPM
    current_engine = CPMEngine(current_network, current_cals)
    current_result = current_engine.run()

    # Match tasks
    baseline_by_code = {t.task_code: t for t in baseline_network.tasks.values()}
    current_by_code = {t.task_code: t for t in current_network.tasks.values()}

    baseline_codes = set(baseline_by_code.keys())
    current_codes = set(current_by_code.keys())
    new_tasks = list(current_codes - baseline_codes)
    removed_tasks = list(baseline_codes - current_codes)
    matched_codes = baseline_codes & current_codes

    # Find candidates: tasks with increased duration on or near critical path
    candidates = []
    for code in matched_codes:
        baseline_task = baseline_by_code[code]
        current_task = current_by_code[code]

        delta = current_task.duration_hours - baseline_task.duration_hours
        if delta > 0:
            # Prioritize: critical tasks, then by duration delta
            priority = (
                0 if current_task.is_critical else 1,
                -delta
            )
            candidates.append((code, delta, priority))

    # Sort and limit candidates
    candidates.sort(key=lambda x: x[2])
    candidates = candidates[:top_n]

    if verbose:
        print(f"Testing {len(candidates)} candidate tasks for marginal impact...")

    # Calculate marginal impact for each candidate
    contributions = []
    calendar = next(iter(current_cals.values())) if current_cals else None

    for i, (code, delta, _) in enumerate(candidates):
        if verbose and (i + 1) % 10 == 0:
            print(f"  Progress: {i+1}/{len(candidates)}")

        baseline_task = baseline_by_code[code]
        current_task = current_by_code[code]

        # Create modified network: use current network but revert this task's duration
        test_network = current_network.clone()
        test_network.modify_task_duration(current_task.task_id, baseline_task.duration_hours)

        # Run CPM on test network
        test_engine = CPMEngine(test_network, current_cals)
        test_result = test_engine.run()

        # Marginal impact = current finish - test finish
        if calendar and current_result.project_finish > test_result.project_finish:
            marginal_impact = calendar.work_hours_between(
                test_result.project_finish,
                current_result.project_finish
            )
        else:
            diff = (current_result.project_finish - test_result.project_finish)
            marginal_impact = max(0, diff.total_seconds() / 3600)

        if marginal_impact > 0:
            contributions.append(TaskContribution(
                task_id=current_task.task_id,
                task_code=code,
                task_name=current_task.task_name,
                baseline_duration_hours=baseline_task.duration_hours,
                current_duration_hours=current_task.duration_hours,
                duration_delta_hours=delta,
                slip_contribution_hours=marginal_impact,
                slip_contribution_pct=0,  # Calculated below
                is_on_critical_path=current_task.is_critical,
            ))

    # Calculate total slip
    if calendar:
        total_slip = calendar.work_hours_between(
            baseline_result.project_finish,
            current_result.project_finish
        ) if current_result.project_finish > baseline_result.project_finish else 0
    else:
        diff = (current_result.project_finish - baseline_result.project_finish)
        total_slip = max(0, diff.total_seconds() / 3600)

    # Calculate percentages
    total_contribution = sum(c.slip_contribution_hours for c in contributions)
    for c in contributions:
        if total_contribution > 0:
            c.slip_contribution_pct = c.slip_contribution_hours / total_contribution * 100

    contributions.sort(key=lambda c: c.slip_contribution_hours, reverse=True)

    return DelayAttributionResult(
        baseline_file_id=baseline_file_id,
        current_file_id=current_file_id,
        baseline_finish=baseline_result.project_finish,
        current_finish=current_result.project_finish,
        total_slip_hours=total_slip,
        total_slip_days=total_slip / 8.0,
        task_contributions=contributions,
        new_tasks=new_tasks,
        removed_tasks=removed_tasks,
        matched_tasks=len(matched_codes),
    )


def analyze_slip_by_wbs(result: DelayAttributionResult) -> dict[str, float]:
    """
    Aggregate slip attribution by WBS.

    Args:
        result: DelayAttributionResult from attribute_delays()

    Returns:
        Dict mapping WBS to total slip contribution hours
    """
    by_wbs = defaultdict(float)
    for contrib in result.task_contributions:
        # Extract WBS from task_id or use a placeholder
        # In practice, you'd look up the WBS from the task
        wbs = "Unknown"  # Would need task.wbs_id
        by_wbs[wbs] += contrib.slip_contribution_hours

    return dict(sorted(by_wbs.items(), key=lambda x: -x[1]))


def print_attribution_report(result: DelayAttributionResult, top_n: int = 20) -> None:
    """Print a formatted delay attribution report."""
    print("=" * 90)
    print("DELAY ATTRIBUTION ANALYSIS")
    print("=" * 90)

    print(f"\nSchedule Comparison:")
    print(f"  Baseline (file_id={result.baseline_file_id}): {result.baseline_finish}")
    print(f"  Current  (file_id={result.current_file_id}):  {result.current_finish}")
    print(f"  Total Slip: {result.total_slip_hours:.0f} hours ({result.total_slip_days:.1f} days)")

    print(f"\nTask Changes:")
    print(f"  Matched tasks: {result.matched_tasks}")
    print(f"  New tasks: {len(result.new_tasks)}")
    print(f"  Removed tasks: {len(result.removed_tasks)}")
    print(f"  Tasks contributing to slip: {len(result.task_contributions)}")

    print(f"\n--- Top {min(top_n, len(result.task_contributions))} Slip Contributors ---")
    print(f"{'Rank':4s} {'Task Code':20s} {'Duration +':>12s} {'Slip':>12s} {'%':>6s} {'Crit':>5s} {'Task Name':30s}")
    print("-" * 90)

    for i, contrib in enumerate(result.task_contributions[:top_n]):
        crit = "Yes" if contrib.is_on_critical_path else "No"
        print(f"{i+1:4d} {contrib.task_code:20s} "
              f"{contrib.duration_delta_hours/8:>12.1f}d "
              f"{contrib.slip_contribution_hours/8:>12.1f}d "
              f"{contrib.slip_contribution_pct:>6.1f} "
              f"{crit:>5s} "
              f"{contrib.task_name[:30]:30s}")

    if len(result.task_contributions) > top_n:
        remaining = result.task_contributions[top_n:]
        remaining_slip = sum(c.slip_contribution_hours for c in remaining)
        print(f"\n... and {len(remaining)} more contributing tasks "
              f"(combined: {remaining_slip/8:.1f} days)")

    # Summary statistics
    total_attributed = sum(c.slip_contribution_hours for c in result.task_contributions)
    unattributed = result.total_slip_hours - total_attributed

    print(f"\nAttribution Summary:")
    print(f"  Attributed to duration increases: {total_attributed/8:.1f} days ({total_attributed/result.total_slip_hours*100:.1f}%)")
    if unattributed > 0:
        print(f"  Unattributed (new tasks, logic changes): {unattributed/8:.1f} days ({unattributed/result.total_slip_hours*100:.1f}%)")

    print("\n" + "=" * 90)
