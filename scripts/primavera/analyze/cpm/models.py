"""
Data models for CPM calculations.

Defines dataclasses for tasks, dependencies, and analysis results.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class Task:
    """Represents a schedule task/activity."""

    task_id: str
    task_code: str
    task_name: str
    duration_hours: float          # target_drtn_hr_cnt from P6
    calendar_id: str
    status: str                    # TK_Complete, TK_Active, TK_NotStart
    task_type: str                 # TT_Task, TT_FinMile, TT_Mile, TT_Rsrc
    wbs_id: str

    # Constraints (optional)
    constraint_date: Optional[datetime] = None
    constraint_type: Optional[str] = None    # CS_SNET, CS_FNLT, CS_MSO, etc.

    # Actuals (for completed/in-progress tasks)
    actual_start: Optional[datetime] = None
    actual_finish: Optional[datetime] = None
    remaining_duration_hours: float = 0.0

    # CPM Results (calculated by engine)
    early_start: Optional[datetime] = None
    early_finish: Optional[datetime] = None
    late_start: Optional[datetime] = None
    late_finish: Optional[datetime] = None
    total_float_hours: Optional[float] = None
    free_float_hours: Optional[float] = None
    is_critical: bool = False

    # P6 reference values (for validation)
    p6_early_start: Optional[datetime] = None
    p6_early_finish: Optional[datetime] = None
    p6_late_start: Optional[datetime] = None
    p6_late_finish: Optional[datetime] = None
    p6_total_float_hours: Optional[float] = None
    p6_driving_path_flag: bool = False

    def is_milestone(self) -> bool:
        """Check if task is a milestone (zero duration)."""
        return self.task_type in ('TT_FinMile', 'TT_Mile') or self.duration_hours == 0

    def is_completed(self) -> bool:
        """Check if task is completed."""
        return self.status == 'TK_Complete'

    def is_in_progress(self) -> bool:
        """Check if task is in progress."""
        return self.status == 'TK_Active'

    def is_not_started(self) -> bool:
        """Check if task has not started."""
        return self.status == 'TK_NotStart'

    def get_effective_duration(self) -> float:
        """Get duration to use for calculations (remaining if in progress)."""
        if self.is_in_progress():
            return self.remaining_duration_hours
        return self.duration_hours


@dataclass
class Dependency:
    """Represents a predecessor-successor relationship."""

    pred_task_id: str
    succ_task_id: str
    pred_type: str      # PR_FS, PR_SS, PR_FF, PR_SF
    lag_hours: float

    def is_finish_to_start(self) -> bool:
        return self.pred_type == 'PR_FS'

    def is_start_to_start(self) -> bool:
        return self.pred_type == 'PR_SS'

    def is_finish_to_finish(self) -> bool:
        return self.pred_type == 'PR_FF'

    def is_start_to_finish(self) -> bool:
        return self.pred_type == 'PR_SF'


@dataclass
class CPMResult:
    """Results from a CPM calculation."""

    tasks: dict[str, Task]
    critical_path: list[str]       # task_ids in execution order
    project_start: datetime
    project_finish: datetime
    total_duration_hours: float

    def get_critical_tasks(self) -> list[Task]:
        """Get Task objects on the critical path."""
        return [self.tasks[tid] for tid in self.critical_path if tid in self.tasks]

    def get_project_duration_days(self, hours_per_day: float = 8.0) -> float:
        """Get project duration in work days."""
        return self.total_duration_hours / hours_per_day

    def get_tasks_by_float(self, max_float_hours: float = None) -> list[Task]:
        """Get tasks sorted by float (ascending)."""
        tasks = list(self.tasks.values())
        tasks = [t for t in tasks if t.total_float_hours is not None]
        if max_float_hours is not None:
            tasks = [t for t in tasks if t.total_float_hours <= max_float_hours]
        return sorted(tasks, key=lambda t: t.total_float_hours)


@dataclass
class TaskImpactResult:
    """Results from single task what-if analysis."""

    task_id: str
    task_code: str
    task_name: str
    duration_delta_hours: float
    original_finish: datetime
    new_finish: datetime
    slip_hours: float
    slip_days: float
    affected_task_ids: list[str]
    original_critical_path: list[str]
    new_critical_path: list[str]
    critical_path_changed: bool

    def get_slip_summary(self) -> str:
        """Get human-readable slip summary."""
        if self.slip_hours <= 0:
            return "No impact on project finish"
        return f"{self.slip_days:.1f} days slip ({self.slip_hours:.0f} hours)"


@dataclass
class TaskContribution:
    """A task's contribution to schedule slip."""

    task_id: str
    task_code: str
    task_name: str
    baseline_duration_hours: float
    current_duration_hours: float
    duration_delta_hours: float
    slip_contribution_hours: float
    slip_contribution_pct: float
    is_on_critical_path: bool

    def get_summary(self) -> str:
        """Get human-readable contribution summary."""
        return (f"{self.task_code}: +{self.duration_delta_hours/8:.1f}d duration -> "
                f"{self.slip_contribution_hours/8:.1f}d slip ({self.slip_contribution_pct:.1f}%)")


@dataclass
class DelayAttributionResult:
    """Results from delay attribution analysis."""

    baseline_file_id: int
    current_file_id: int
    baseline_finish: datetime
    current_finish: datetime
    total_slip_hours: float
    total_slip_days: float
    task_contributions: list[TaskContribution]
    new_tasks: list[str]           # task_codes added since baseline
    removed_tasks: list[str]       # task_codes removed since baseline
    matched_tasks: int             # tasks present in both versions

    def get_top_contributors(self, n: int = 10) -> list[TaskContribution]:
        """Get top N contributors to slip."""
        return self.task_contributions[:n]

    def get_summary(self) -> str:
        """Get human-readable summary."""
        return (f"Schedule slip: {self.total_slip_days:.0f} days "
                f"({self.baseline_finish.date()} -> {self.current_finish.date()})")


@dataclass
class CriticalPathResult:
    """Results from critical path analysis."""

    critical_path: list[Task]
    near_critical_tasks: list[Task]
    float_distribution: dict[str, int]  # float_bucket -> count
    project_finish: datetime
    near_critical_threshold_hours: float
    total_tasks: int

    def get_critical_path_length(self) -> int:
        """Number of tasks on critical path."""
        return len(self.critical_path)

    def get_risk_summary(self) -> str:
        """Get summary of schedule risk."""
        critical = len(self.critical_path)
        near_critical = len(self.near_critical_tasks)
        return (f"{critical} critical tasks, {near_critical} near-critical "
                f"(<{self.near_critical_threshold_hours/8:.0f} days float)")
