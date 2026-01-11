"""
CPM (Critical Path Method) Engine.

Implements forward and backward pass calculations with full calendar support.
"""

from datetime import datetime
from typing import Optional

from .models import Task, Dependency, CPMResult
from .calendar import P6Calendar
from .network import TaskNetwork


class CPMEngine:
    """
    CPM calculation engine.

    Performs forward pass (early dates), backward pass (late dates),
    float calculation, and critical path identification.
    """

    def __init__(self, network: TaskNetwork, calendars: dict[str, P6Calendar],
                 default_calendar_id: str = None):
        """
        Initialize CPM engine.

        Args:
            network: Task network to calculate
            calendars: Dict mapping calendar_id to P6Calendar
            default_calendar_id: Calendar to use for tasks with missing calendar
        """
        self.network = network
        self.calendars = calendars
        self._default_calendar_id = default_calendar_id

        # If no default specified, use the first calendar
        if not self._default_calendar_id and calendars:
            self._default_calendar_id = next(iter(calendars.keys()))

    def get_calendar(self, calendar_id: str) -> P6Calendar:
        """Get calendar by ID, falling back to default if needed."""
        if calendar_id and calendar_id in self.calendars:
            return self.calendars[calendar_id]
        if self._default_calendar_id and self._default_calendar_id in self.calendars:
            return self.calendars[self._default_calendar_id]
        raise ValueError(f"Calendar {calendar_id} not found and no default available")

    def forward_pass(self, project_start: datetime) -> None:
        """
        Calculate early start and early finish for all tasks.

        Processes tasks in topological order. For each task:
        - Completed tasks: use actual dates
        - In-progress tasks: early_start = actual_start, calculate early_finish
        - Not started: early_start = max(predecessor-driven dates), calculate early_finish
        """
        task_order = self.network.topological_sort()

        for task_id in task_order:
            task = self.network.tasks[task_id]
            calendar = self.get_calendar(task.calendar_id)

            # Handle completed tasks - use actuals
            if task.is_completed():
                task.early_start = task.actual_start or project_start
                task.early_finish = task.actual_finish or task.early_start
                continue

            # Calculate early start from predecessors
            early_start = project_start
            predecessors = self.network.get_predecessors(task_id)

            if predecessors:
                for dep in predecessors:
                    pred_task = self.network.tasks[dep.pred_task_id]
                    driven_date = self._get_driven_early_start(pred_task, dep, task, calendar)
                    if driven_date and driven_date > early_start:
                        early_start = driven_date

            # Apply start-no-earlier-than constraint
            if task.constraint_type in ('CS_SNET', 'CS_MSO') and task.constraint_date:
                early_start = max(early_start, task.constraint_date)

            # Handle in-progress tasks
            if task.is_in_progress() and task.actual_start:
                task.early_start = task.actual_start
                duration = task.remaining_duration_hours
            else:
                task.early_start = early_start
                duration = task.duration_hours

            # Calculate early finish
            if duration > 0:
                task.early_finish = calendar.add_work_hours(task.early_start, duration)
            else:
                # Milestone - finish equals start
                task.early_finish = task.early_start

    def _get_driven_early_start(self, pred: Task, dep: Dependency,
                                 succ: Task, calendar: P6Calendar) -> Optional[datetime]:
        """
        Calculate the early start driven by a predecessor relationship.

        Handles FS, SS, FF, SF relationship types with lag.
        """
        if pred.early_finish is None or pred.early_start is None:
            return None

        lag = dep.lag_hours

        if dep.is_finish_to_start():
            # FS: successor starts after predecessor finishes + lag
            return calendar.add_work_hours(pred.early_finish, lag)

        elif dep.is_start_to_start():
            # SS: successor starts after predecessor starts + lag
            return calendar.add_work_hours(pred.early_start, lag)

        elif dep.is_finish_to_finish():
            # FF: successor finishes after predecessor finishes + lag
            # So: successor start = (pred finish + lag) - successor duration
            target_finish = calendar.add_work_hours(pred.early_finish, lag)
            succ_duration = succ.get_effective_duration()
            if succ_duration > 0:
                return calendar.subtract_work_hours(target_finish, succ_duration)
            return target_finish

        elif dep.is_start_to_finish():
            # SF: successor finishes after predecessor starts + lag
            target_finish = calendar.add_work_hours(pred.early_start, lag)
            succ_duration = succ.get_effective_duration()
            if succ_duration > 0:
                return calendar.subtract_work_hours(target_finish, succ_duration)
            return target_finish

        return pred.early_finish

    def backward_pass(self, project_end: datetime = None) -> None:
        """
        Calculate late start and late finish for all tasks.

        Processes tasks in reverse topological order.
        """
        task_order = self.network.reverse_topological_sort()

        # Determine project end if not specified
        if project_end is None:
            project_end = self._get_project_end()

        for task_id in task_order:
            task = self.network.tasks[task_id]
            calendar = self.get_calendar(task.calendar_id)

            # Calculate late finish from successors
            successors = self.network.get_successors(task_id)

            if not successors:
                # No successors - late finish is project end
                task.late_finish = project_end
            else:
                late_finish = project_end
                for dep in successors:
                    succ_task = self.network.tasks[dep.succ_task_id]
                    driven_date = self._get_driven_late_finish(succ_task, dep, task, calendar)
                    if driven_date and driven_date < late_finish:
                        late_finish = driven_date
                task.late_finish = late_finish

            # Apply finish-no-later-than constraint
            if task.constraint_type in ('CS_FNLT', 'CS_MFO') and task.constraint_date:
                task.late_finish = min(task.late_finish, task.constraint_date)

            # Calculate late start
            duration = task.get_effective_duration()
            if duration > 0:
                task.late_start = calendar.subtract_work_hours(task.late_finish, duration)
            else:
                # Milestone
                task.late_start = task.late_finish

    def _get_driven_late_finish(self, succ: Task, dep: Dependency,
                                 pred: Task, calendar: P6Calendar) -> Optional[datetime]:
        """
        Calculate the late finish driven by a successor relationship.

        This is the reverse of _get_driven_early_start.
        """
        if succ.late_start is None or succ.late_finish is None:
            return None

        lag = dep.lag_hours

        if dep.is_finish_to_start():
            # FS: pred finishes before successor starts - lag
            return calendar.subtract_work_hours(succ.late_start, lag)

        elif dep.is_start_to_start():
            # SS: pred starts before successor starts - lag
            # So: pred finish = (succ late_start - lag) + pred duration
            target_start = calendar.subtract_work_hours(succ.late_start, lag)
            pred_duration = pred.get_effective_duration()
            if pred_duration > 0:
                return calendar.add_work_hours(target_start, pred_duration)
            return target_start

        elif dep.is_finish_to_finish():
            # FF: pred finishes before successor finishes - lag
            return calendar.subtract_work_hours(succ.late_finish, lag)

        elif dep.is_start_to_finish():
            # SF: pred starts before successor finishes - lag
            target_start = calendar.subtract_work_hours(succ.late_finish, lag)
            pred_duration = pred.get_effective_duration()
            if pred_duration > 0:
                return calendar.add_work_hours(target_start, pred_duration)
            return target_start

        return succ.late_start

    def _get_project_end(self) -> datetime:
        """Get the latest early finish as project end."""
        max_finish = None
        for task in self.network.tasks.values():
            if task.early_finish:
                if max_finish is None or task.early_finish > max_finish:
                    max_finish = task.early_finish

        if max_finish is None:
            raise ValueError("No tasks have early_finish calculated - run forward_pass first")

        return max_finish

    def calculate_float(self) -> None:
        """
        Calculate total float and free float for all tasks.

        Total Float = Late Finish - Early Finish (in work hours)
        Free Float = min(successor early start) - Early Finish - lag (for FS)
        """
        for task in self.network.tasks.values():
            if task.early_finish is None or task.late_finish is None:
                continue

            calendar = self.get_calendar(task.calendar_id)

            # Total float
            if task.late_finish >= task.early_finish:
                task.total_float_hours = calendar.work_hours_between(
                    task.early_finish, task.late_finish
                )
            else:
                # Negative float (behind schedule)
                task.total_float_hours = -calendar.work_hours_between(
                    task.late_finish, task.early_finish
                )

            # Mark as critical if float <= 0
            task.is_critical = task.total_float_hours <= 0

            # Free float (for FS relationships)
            successors = self.network.get_successors(task.task_id)
            if successors and task.early_finish:
                min_free_float = float('inf')
                for dep in successors:
                    succ = self.network.tasks[dep.succ_task_id]
                    if succ.early_start and dep.is_finish_to_start():
                        # Free float = successor early start - this early finish - lag
                        gap = calendar.work_hours_between(task.early_finish, succ.early_start)
                        free_float = gap - dep.lag_hours
                        min_free_float = min(min_free_float, free_float)

                if min_free_float != float('inf'):
                    task.free_float_hours = max(0, min_free_float)

    def get_critical_path(self) -> list[str]:
        """
        Return task IDs on the critical path in execution order.

        Critical tasks are those with total_float <= 0.
        """
        task_order = self.network.topological_sort()
        return [tid for tid in task_order if self.network.tasks[tid].is_critical]

    def get_project_start(self) -> datetime:
        """Get the earliest early start as project start."""
        min_start = None
        for task in self.network.tasks.values():
            if task.early_start:
                if min_start is None or task.early_start < min_start:
                    min_start = task.early_start
        return min_start

    def run(self, project_start: datetime = None) -> CPMResult:
        """
        Execute full CPM calculation.

        Args:
            project_start: Project start date. If None, uses earliest actual start
                          or constraint date found in tasks.

        Returns:
            CPMResult with all calculated values
        """
        # Determine project start if not specified
        if project_start is None:
            project_start = self._find_project_start()

        # Run CPM passes
        self.forward_pass(project_start)
        self.backward_pass()
        self.calculate_float()

        # Compile results
        project_finish = self._get_project_end()
        critical_path = self.get_critical_path()

        # Calculate total duration on critical path
        total_duration = sum(
            self.network.tasks[tid].get_effective_duration()
            for tid in critical_path
        )

        return CPMResult(
            tasks=self.network.tasks,
            critical_path=critical_path,
            project_start=project_start,
            project_finish=project_finish,
            total_duration_hours=total_duration,
        )

    def _find_project_start(self) -> datetime:
        """Find project start from task data."""
        candidates = []

        for task in self.network.tasks.values():
            # Use actual start for completed/in-progress tasks
            if task.actual_start:
                candidates.append(task.actual_start)

            # Use constraint date for start constraints
            if task.constraint_type in ('CS_SNET', 'CS_MSO') and task.constraint_date:
                candidates.append(task.constraint_date)

        if candidates:
            return min(candidates)

        # Fallback to now
        return datetime.now()

    def compare_with_p6(self) -> dict:
        """
        Compare calculated values with P6's stored values.

        Returns dict with comparison statistics.
        """
        comparisons = {
            'early_start_match': 0,
            'early_start_diff': 0,
            'early_finish_match': 0,
            'early_finish_diff': 0,
            'late_start_match': 0,
            'late_start_diff': 0,
            'late_finish_match': 0,
            'late_finish_diff': 0,
            'float_match': 0,
            'float_diff': 0,
            'critical_match': 0,
            'critical_diff': 0,
            'total_compared': 0,
            'differences': [],
        }

        for task in self.network.tasks.values():
            if task.p6_early_finish is None:
                continue

            comparisons['total_compared'] += 1

            # Compare early dates (allow 1 hour tolerance for rounding)
            if task.early_start and task.p6_early_start:
                diff = abs((task.early_start - task.p6_early_start).total_seconds() / 3600)
                if diff <= 1:
                    comparisons['early_start_match'] += 1
                else:
                    comparisons['early_start_diff'] += 1

            if task.early_finish and task.p6_early_finish:
                diff = abs((task.early_finish - task.p6_early_finish).total_seconds() / 3600)
                if diff <= 1:
                    comparisons['early_finish_match'] += 1
                else:
                    comparisons['early_finish_diff'] += 1
                    if len(comparisons['differences']) < 10:
                        comparisons['differences'].append({
                            'task_id': task.task_id,
                            'task_name': task.task_name[:50],
                            'field': 'early_finish',
                            'calculated': task.early_finish,
                            'p6': task.p6_early_finish,
                            'diff_hours': diff,
                        })

            # Compare late dates
            if task.late_finish and task.p6_late_finish:
                diff = abs((task.late_finish - task.p6_late_finish).total_seconds() / 3600)
                if diff <= 1:
                    comparisons['late_finish_match'] += 1
                else:
                    comparisons['late_finish_diff'] += 1

            # Compare float
            if task.total_float_hours is not None and task.p6_total_float_hours is not None:
                diff = abs(task.total_float_hours - task.p6_total_float_hours)
                if diff <= 8:  # 1 day tolerance
                    comparisons['float_match'] += 1
                else:
                    comparisons['float_diff'] += 1

            # Compare critical flag
            if task.is_critical == task.p6_driving_path_flag:
                comparisons['critical_match'] += 1
            else:
                comparisons['critical_diff'] += 1

        return comparisons
