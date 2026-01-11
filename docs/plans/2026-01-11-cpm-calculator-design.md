# CPM Calculator Design

**Date:** 2026-01-11
**Purpose:** What-if analysis to identify tasks driving schedule slippage

---

## Overview

Build a Critical Path Method (CPM) calculator that works with P6 schedule data to enable:
1. **Single task impact analysis** - "If task X takes N more days, how does the project end date change?"
2. **Delay attribution** - Compare schedule versions, identify which task changes drove slippage
3. **Critical path analysis** - Identify critical and near-critical tasks

## Architecture

```
scripts/primavera/analyze/
├── cpm/
│   ├── __init__.py
│   ├── engine.py          # Core CPM calculator
│   ├── calendar.py        # P6 calendar parser & date math
│   ├── network.py         # Task dependency graph
│   └── models.py          # Data classes (Task, Dependency, etc.)
├── analysis/
│   ├── __init__.py
│   ├── single_task_impact.py    # "What if task X slips?"
│   ├── delay_attribution.py     # Compare schedules, attribute slip
│   └── critical_path.py         # CP and near-critical analysis
├── data_loader.py         # Load P6 CSVs, build network
└── cli.py                 # Future CLI interface
```

**Data Flow:**
```
P6 CSVs (task.csv, taskpred.csv, calendar.csv)
    ↓
data_loader.py (filter by file_id, build objects)
    ↓
network.py (construct dependency graph)
    ↓
engine.py (forward/backward pass using calendar.py)
    ↓
analysis/*.py (run specific what-if scenarios)
    ↓
Results (DataFrames, dicts, or printed reports)
```

---

## Component Design

### 1. Calendar Parser (`cpm/calendar.py`)

Parse P6's `clndr_data` field which contains work hours and exceptions.

**P6 Calendar Format:**
```
(0||CalendarData()(
  (0||DaysOfWeek()(
    (0||1()())                           # Sunday - no work
    (0||2()(                             # Monday
      (0||0(s|08:00|f|12:00)())          # Work 8am-12pm
      (0||1(s|13:00|f|17:00)())))        # Work 1pm-5pm
    ...
  ))
  (0||Exceptions()(
    (0||0(d|44525)())                    # Day 44525 = no work (holiday)
    (0||1(d|44578)(                      # Day 44578 = modified hours
      (0||0(s|08:00|f|16:00)())))
  ))
))
```

**Class Design:**
```python
class P6Calendar:
    def __init__(self, clndr_id: str, clndr_data: str, day_hr_cnt: float):
        self.clndr_id = clndr_id
        self.work_week: dict[int, list[tuple[time, time]]]  # day_num (1-7) -> work periods
        self.exceptions: dict[date, list[tuple[time, time]] | None]  # date -> periods or None
        self.hours_per_day = day_hr_cnt

    def parse_clndr_data(self, data: str) -> None:
        """Parse P6's nested calendar format."""

    def is_work_day(self, dt: date) -> bool:
        """Check if date is a work day."""

    def get_work_hours(self, dt: date) -> float:
        """Get work hours available on a specific date."""

    def add_work_hours(self, start: datetime, hours: float) -> datetime:
        """Add work hours to a datetime, respecting calendar."""

    def subtract_work_hours(self, end: datetime, hours: float) -> datetime:
        """Subtract work hours from a datetime."""

    def work_hours_between(self, start: datetime, end: datetime) -> float:
        """Calculate work hours between two datetimes."""
```

**Excel Serial Date Conversion:**
- P6 uses Excel serial dates (days since 1899-12-30)
- `d|44525` = 44525 days since epoch = 2021-11-25

---

### 2. Data Models (`cpm/models.py`)

```python
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

@dataclass
class Task:
    task_id: str
    task_code: str
    task_name: str
    duration_hours: float          # target_drtn_hr_cnt
    calendar_id: str
    status: str                    # TK_Complete, TK_Active, TK_NotStart
    task_type: str                 # TT_Task, TT_FinMile, TT_Mile
    wbs_id: str

    # Constraints (optional)
    constraint_date: Optional[datetime] = None
    constraint_type: Optional[str] = None    # CS_SNET, CS_FNLT, etc.

    # Actuals (for completed/in-progress tasks)
    actual_start: Optional[datetime] = None
    actual_finish: Optional[datetime] = None
    remaining_duration_hours: float = 0.0

    # CPM Results (calculated)
    early_start: Optional[datetime] = None
    early_finish: Optional[datetime] = None
    late_start: Optional[datetime] = None
    late_finish: Optional[datetime] = None
    total_float_hours: Optional[float] = None
    free_float_hours: Optional[float] = None
    is_critical: bool = False


@dataclass
class Dependency:
    pred_task_id: str
    succ_task_id: str
    pred_type: str      # PR_FS, PR_SS, PR_FF, PR_SF
    lag_hours: float


@dataclass
class CPMResult:
    tasks: dict[str, Task]
    critical_path: list[str]       # task_ids in execution order
    project_start: datetime
    project_finish: datetime
    total_duration_hours: float

    def get_critical_tasks(self) -> list[Task]:
        return [self.tasks[tid] for tid in self.critical_path]


@dataclass
class TaskImpactResult:
    task_id: str
    duration_delta_hours: float
    original_finish: datetime
    new_finish: datetime
    slip_hours: float
    affected_task_ids: list[str]
    original_critical_path: list[str]
    new_critical_path: list[str]


@dataclass
class TaskContribution:
    task_id: str
    task_code: str
    task_name: str
    duration_delta_hours: float
    slip_contribution_hours: float
    slip_contribution_pct: float


@dataclass
class DelayAttributionResult:
    baseline_file_id: int
    current_file_id: int
    baseline_finish: datetime
    current_finish: datetime
    total_slip_hours: float
    task_contributions: list[TaskContribution]
    new_tasks: list[str]           # task_codes added
    removed_tasks: list[str]       # task_codes removed


@dataclass
class CriticalPathResult:
    critical_path: list[Task]
    near_critical_tasks: list[Task]
    float_distribution: dict[str, int]  # float_bucket -> count
    project_finish: datetime
    near_critical_threshold_hours: float
```

---

### 3. Task Network (`cpm/network.py`)

```python
class TaskNetwork:
    def __init__(self):
        self.tasks: dict[str, Task] = {}
        self.dependencies: list[Dependency] = []
        self._successors: dict[str, list[Dependency]] = {}   # task_id -> outgoing deps
        self._predecessors: dict[str, list[Dependency]] = {} # task_id -> incoming deps

    def add_task(self, task: Task) -> None:
        """Add task to network."""
        self.tasks[task.task_id] = task
        if task.task_id not in self._successors:
            self._successors[task.task_id] = []
        if task.task_id not in self._predecessors:
            self._predecessors[task.task_id] = []

    def add_dependency(self, dep: Dependency) -> None:
        """Add dependency and update adjacency lists."""
        self.dependencies.append(dep)
        self._successors[dep.pred_task_id].append(dep)
        self._predecessors[dep.succ_task_id].append(dep)

    def get_successors(self, task_id: str) -> list[Dependency]:
        """Get dependencies where task_id is the predecessor."""
        return self._successors.get(task_id, [])

    def get_predecessors(self, task_id: str) -> list[Dependency]:
        """Get dependencies where task_id is the successor."""
        return self._predecessors.get(task_id, [])

    def topological_sort(self) -> list[str]:
        """Return task_ids in dependency order (Kahn's algorithm)."""
        in_degree = {tid: len(self._predecessors.get(tid, [])) for tid in self.tasks}
        queue = [tid for tid, deg in in_degree.items() if deg == 0]
        result = []

        while queue:
            task_id = queue.pop(0)
            result.append(task_id)
            for dep in self._successors.get(task_id, []):
                in_degree[dep.succ_task_id] -= 1
                if in_degree[dep.succ_task_id] == 0:
                    queue.append(dep.succ_task_id)

        if len(result) != len(self.tasks):
            raise ValueError("Circular dependency detected")
        return result

    def get_start_tasks(self) -> list[str]:
        """Tasks with no predecessors."""
        return [tid for tid in self.tasks if not self._predecessors.get(tid)]

    def get_end_tasks(self) -> list[str]:
        """Tasks with no successors."""
        return [tid for tid in self.tasks if not self._successors.get(tid)]

    def clone(self) -> 'TaskNetwork':
        """Deep copy for what-if scenarios."""
        import copy
        new_network = TaskNetwork()
        new_network.tasks = {tid: copy.copy(task) for tid, task in self.tasks.items()}
        new_network.dependencies = [copy.copy(d) for d in self.dependencies]
        new_network._successors = {k: list(v) for k, v in self._successors.items()}
        new_network._predecessors = {k: list(v) for k, v in self._predecessors.items()}
        return new_network

    def modify_task_duration(self, task_id: str, new_duration_hours: float) -> None:
        """Modify a task's duration for what-if analysis."""
        if task_id in self.tasks:
            self.tasks[task_id].duration_hours = new_duration_hours
```

---

### 4. CPM Engine (`cpm/engine.py`)

```python
class CPMEngine:
    def __init__(self, network: TaskNetwork, calendars: dict[str, P6Calendar]):
        self.network = network
        self.calendars = calendars
        self._default_calendar_id: str | None = None

    def get_calendar(self, calendar_id: str) -> P6Calendar:
        """Get calendar, with fallback to default."""
        if calendar_id in self.calendars:
            return self.calendars[calendar_id]
        if self._default_calendar_id:
            return self.calendars[self._default_calendar_id]
        raise ValueError(f"Calendar {calendar_id} not found")

    def forward_pass(self, project_start: datetime) -> None:
        """
        Calculate early_start and early_finish for all tasks.

        Process tasks in topological order.
        For each task:
          - If completed: use actual dates
          - If in-progress: early_start = actual_start, calculate early_finish from remaining
          - If not started: early_start = max(predecessors), early_finish = start + duration
        """
        task_order = self.network.topological_sort()

        for task_id in task_order:
            task = self.network.tasks[task_id]
            calendar = self.get_calendar(task.calendar_id)

            # Handle completed tasks
            if task.status == 'TK_Complete':
                task.early_start = task.actual_start
                task.early_finish = task.actual_finish
                continue

            # Calculate early start from predecessors
            early_start = project_start
            for dep in self.network.get_predecessors(task_id):
                pred = self.network.tasks[dep.pred_task_id]
                driven_date = self._get_driven_start(pred, dep, calendar)
                if driven_date > early_start:
                    early_start = driven_date

            # Apply constraint if present
            if task.constraint_type == 'CS_SNET' and task.constraint_date:
                early_start = max(early_start, task.constraint_date)

            # Handle in-progress tasks
            if task.status == 'TK_Active' and task.actual_start:
                task.early_start = task.actual_start
                duration = task.remaining_duration_hours
            else:
                task.early_start = early_start
                duration = task.duration_hours

            # Calculate early finish
            task.early_finish = calendar.add_work_hours(task.early_start, duration)

    def _get_driven_start(self, pred: Task, dep: Dependency, calendar: P6Calendar) -> datetime:
        """Calculate the start date driven by a predecessor relationship."""
        lag = dep.lag_hours

        if dep.pred_type == 'PR_FS':  # Finish-to-Start
            return calendar.add_work_hours(pred.early_finish, lag)

        elif dep.pred_type == 'PR_SS':  # Start-to-Start
            return calendar.add_work_hours(pred.early_start, lag)

        elif dep.pred_type == 'PR_FF':  # Finish-to-Finish
            # Successor finish >= pred finish + lag
            # So successor start = pred finish + lag - successor duration
            # This is handled in a second pass after we know durations
            return calendar.add_work_hours(pred.early_finish, lag)

        elif dep.pred_type == 'PR_SF':  # Start-to-Finish (rare)
            return calendar.add_work_hours(pred.early_start, lag)

        return pred.early_finish

    def backward_pass(self, project_end: datetime = None) -> None:
        """
        Calculate late_start and late_finish for all tasks.

        Process tasks in reverse topological order.
        """
        task_order = list(reversed(self.network.topological_sort()))

        # If no project end specified, use the latest early finish
        if project_end is None:
            project_end = max(
                t.early_finish for t in self.network.tasks.values()
                if t.early_finish
            )

        for task_id in task_order:
            task = self.network.tasks[task_id]
            calendar = self.get_calendar(task.calendar_id)

            # Calculate late finish from successors
            successors = self.network.get_successors(task_id)
            if not successors:
                task.late_finish = project_end
            else:
                late_finish = project_end
                for dep in successors:
                    succ = self.network.tasks[dep.succ_task_id]
                    driven_date = self._get_driven_finish(succ, dep, calendar)
                    if driven_date < late_finish:
                        late_finish = driven_date
                task.late_finish = late_finish

            # Apply constraint if present
            if task.constraint_type == 'CS_FNLT' and task.constraint_date:
                task.late_finish = min(task.late_finish, task.constraint_date)

            # Calculate late start
            duration = task.remaining_duration_hours if task.status == 'TK_Active' else task.duration_hours
            task.late_start = calendar.subtract_work_hours(task.late_finish, duration)

    def _get_driven_finish(self, succ: Task, dep: Dependency, calendar: P6Calendar) -> datetime:
        """Calculate the finish date driven by a successor relationship."""
        lag = dep.lag_hours

        if dep.pred_type == 'PR_FS':
            return calendar.subtract_work_hours(succ.late_start, lag)

        elif dep.pred_type == 'PR_SS':
            return calendar.subtract_work_hours(succ.late_start, lag)

        elif dep.pred_type == 'PR_FF':
            return calendar.subtract_work_hours(succ.late_finish, lag)

        elif dep.pred_type == 'PR_SF':
            return calendar.subtract_work_hours(succ.late_finish, lag)

        return succ.late_start

    def calculate_float(self) -> None:
        """Calculate total float for all tasks."""
        for task in self.network.tasks.values():
            if task.early_finish and task.late_finish:
                calendar = self.get_calendar(task.calendar_id)
                task.total_float_hours = calendar.work_hours_between(
                    task.early_finish, task.late_finish
                )
                task.is_critical = task.total_float_hours <= 0

    def get_critical_path(self) -> list[str]:
        """Return task IDs on the critical path in execution order."""
        task_order = self.network.topological_sort()
        return [tid for tid in task_order if self.network.tasks[tid].is_critical]

    def run(self, project_start: datetime = None) -> CPMResult:
        """Execute full CPM calculation."""
        if project_start is None:
            # Use earliest actual start or constraint
            starts = [t.actual_start for t in self.network.tasks.values() if t.actual_start]
            project_start = min(starts) if starts else datetime.now()

        self.forward_pass(project_start)
        self.backward_pass()
        self.calculate_float()

        project_finish = max(
            t.early_finish for t in self.network.tasks.values() if t.early_finish
        )

        return CPMResult(
            tasks=self.network.tasks,
            critical_path=self.get_critical_path(),
            project_start=project_start,
            project_finish=project_finish,
            total_duration_hours=sum(
                t.duration_hours for t in self.network.tasks.values()
            )
        )
```

---

### 5. Data Loader (`data_loader.py`)

```python
import pandas as pd
from pathlib import Path
from src.config.settings import Settings
from .cpm.models import Task, Dependency
from .cpm.network import TaskNetwork
from .cpm.calendar import P6Calendar

def load_calendars(file_id: int, data_dir: Path = None) -> dict[str, P6Calendar]:
    """Load and parse calendars for a schedule version."""
    if data_dir is None:
        data_dir = Settings.PRIMAVERA_PROCESSED_DIR

    df = pd.read_csv(data_dir / 'calendar.csv')
    df = df[df['file_id'] == file_id]

    calendars = {}
    for _, row in df.iterrows():
        cal = P6Calendar(
            clndr_id=row['clndr_id'],
            clndr_data=row['clndr_data'],
            day_hr_cnt=row['day_hr_cnt']
        )
        calendars[cal.clndr_id] = cal

    return calendars


def load_tasks(file_id: int, data_dir: Path = None) -> dict[str, Task]:
    """Load tasks for a schedule version."""
    if data_dir is None:
        data_dir = Settings.PRIMAVERA_PROCESSED_DIR

    df = pd.read_csv(data_dir / 'task.csv')
    df = df[df['file_id'] == file_id]

    tasks = {}
    for _, row in df.iterrows():
        task = Task(
            task_id=row['task_id'],
            task_code=row['task_code'],
            task_name=row['task_name'],
            duration_hours=row['target_drtn_hr_cnt'] or 0,
            calendar_id=row['clndr_id'],
            status=row['status_code'],
            task_type=row['task_type'],
            wbs_id=row['wbs_id'],
            constraint_date=pd.to_datetime(row['cstr_date']) if pd.notna(row['cstr_date']) else None,
            constraint_type=row['cstr_type'] if pd.notna(row['cstr_type']) else None,
            actual_start=pd.to_datetime(row['act_start_date']) if pd.notna(row['act_start_date']) else None,
            actual_finish=pd.to_datetime(row['act_end_date']) if pd.notna(row['act_end_date']) else None,
            remaining_duration_hours=row['remain_drtn_hr_cnt'] or 0,
        )
        tasks[task.task_id] = task

    return tasks


def load_dependencies(file_id: int, data_dir: Path = None) -> list[Dependency]:
    """Load dependencies for a schedule version."""
    if data_dir is None:
        data_dir = Settings.PRIMAVERA_PROCESSED_DIR

    df = pd.read_csv(data_dir / 'taskpred.csv')
    df = df[df['file_id'] == file_id]

    dependencies = []
    for _, row in df.iterrows():
        dep = Dependency(
            pred_task_id=row['pred_task_id'],
            succ_task_id=row['task_id'],
            pred_type=row['pred_type'],
            lag_hours=row['lag_hr_cnt'] or 0,
        )
        dependencies.append(dep)

    return dependencies


def load_schedule(
    file_id: int,
    data_dir: Path = None
) -> tuple[TaskNetwork, dict[str, P6Calendar]]:
    """
    Load a complete schedule version.

    Returns:
        Tuple of (TaskNetwork, calendars dict)
    """
    if data_dir is None:
        data_dir = Settings.PRIMAVERA_PROCESSED_DIR

    calendars = load_calendars(file_id, data_dir)
    tasks = load_tasks(file_id, data_dir)
    dependencies = load_dependencies(file_id, data_dir)

    network = TaskNetwork()
    for task in tasks.values():
        network.add_task(task)

    for dep in dependencies:
        # Only add if both tasks exist
        if dep.pred_task_id in network.tasks and dep.succ_task_id in network.tasks:
            network.add_dependency(dep)

    return network, calendars


def get_file_id_for_date(target_date: str, data_dir: Path = None) -> int:
    """Get the file_id closest to a target date."""
    if data_dir is None:
        data_dir = Settings.PRIMAVERA_PROCESSED_DIR

    df = pd.read_csv(data_dir / 'xer_files.csv')
    df['date'] = pd.to_datetime(df['date'])
    target = pd.to_datetime(target_date)

    df['diff'] = abs(df['date'] - target)
    closest = df.loc[df['diff'].idxmin()]
    return int(closest['file_id'])
```

---

### 6. Analysis Modules

#### Single Task Impact (`analysis/single_task_impact.py`)

```python
def analyze_task_impact(
    network: TaskNetwork,
    calendars: dict[str, P6Calendar],
    task_id: str,
    duration_delta_hours: float,
    project_start: datetime = None
) -> TaskImpactResult:
    """
    Calculate impact of changing one task's duration.
    """
    from .cpm.engine import CPMEngine

    # Run baseline CPM
    baseline_engine = CPMEngine(network, calendars)
    baseline_result = baseline_engine.run(project_start)

    # Clone network and modify task
    modified_network = network.clone()
    task = modified_network.tasks[task_id]
    new_duration = task.duration_hours + duration_delta_hours
    modified_network.modify_task_duration(task_id, new_duration)

    # Run modified CPM
    modified_engine = CPMEngine(modified_network, calendars)
    modified_result = modified_engine.run(project_start)

    # Find affected tasks
    affected = []
    for tid, task in modified_network.tasks.items():
        baseline_task = network.tasks[tid]
        if task.early_finish != baseline_task.early_finish:
            affected.append(tid)

    # Calculate slip
    calendar = list(calendars.values())[0]  # Use any calendar for diff
    slip_hours = calendar.work_hours_between(
        baseline_result.project_finish,
        modified_result.project_finish
    )

    return TaskImpactResult(
        task_id=task_id,
        duration_delta_hours=duration_delta_hours,
        original_finish=baseline_result.project_finish,
        new_finish=modified_result.project_finish,
        slip_hours=slip_hours,
        affected_task_ids=affected,
        original_critical_path=baseline_result.critical_path,
        new_critical_path=modified_result.critical_path,
    )
```

#### Delay Attribution (`analysis/delay_attribution.py`)

```python
def attribute_delays(
    baseline_file_id: int,
    current_file_id: int,
    data_dir: Path = None
) -> DelayAttributionResult:
    """
    Compare two schedule versions and attribute slip to specific tasks.

    Uses marginal analysis: for each task with increased duration,
    calculate how much that increase contributed to total slip.
    """
    from .data_loader import load_schedule
    from .cpm.engine import CPMEngine

    # Load both schedules
    baseline_network, baseline_cals = load_schedule(baseline_file_id, data_dir)
    current_network, current_cals = load_schedule(current_file_id, data_dir)

    # Run CPM on both
    baseline_engine = CPMEngine(baseline_network, baseline_cals)
    baseline_result = baseline_engine.run()

    current_engine = CPMEngine(current_network, current_cals)
    current_result = current_engine.run()

    # Match tasks by task_code (task_id includes file_id prefix)
    baseline_by_code = {t.task_code: t for t in baseline_network.tasks.values()}
    current_by_code = {t.task_code: t for t in current_network.tasks.values()}

    # Find tasks with changed durations
    contributions = []
    for code, current_task in current_by_code.items():
        if code in baseline_by_code:
            baseline_task = baseline_by_code[code]
            delta = current_task.duration_hours - baseline_task.duration_hours

            if delta > 0 and current_task.is_critical:
                # Calculate marginal impact
                # Simplified: critical task duration increase = direct slip
                contributions.append(TaskContribution(
                    task_id=current_task.task_id,
                    task_code=code,
                    task_name=current_task.task_name,
                    duration_delta_hours=delta,
                    slip_contribution_hours=delta,  # Refined in full implementation
                    slip_contribution_pct=0,  # Calculated after totaling
                ))

    # Calculate total slip
    calendar = list(current_cals.values())[0]
    total_slip = calendar.work_hours_between(
        baseline_result.project_finish,
        current_result.project_finish
    )

    # Calculate percentages
    total_contribution = sum(c.slip_contribution_hours for c in contributions)
    for c in contributions:
        c.slip_contribution_pct = (c.slip_contribution_hours / total_contribution * 100
                                    if total_contribution > 0 else 0)

    # Sort by contribution
    contributions.sort(key=lambda c: c.slip_contribution_hours, reverse=True)

    # Find new and removed tasks
    new_tasks = [c for c in current_by_code if c not in baseline_by_code]
    removed_tasks = [c for c in baseline_by_code if c not in current_by_code]

    return DelayAttributionResult(
        baseline_file_id=baseline_file_id,
        current_file_id=current_file_id,
        baseline_finish=baseline_result.project_finish,
        current_finish=current_result.project_finish,
        total_slip_hours=total_slip,
        task_contributions=contributions,
        new_tasks=new_tasks,
        removed_tasks=removed_tasks,
    )
```

#### Critical Path Analysis (`analysis/critical_path.py`)

```python
def analyze_critical_path(
    network: TaskNetwork,
    calendars: dict[str, P6Calendar],
    near_critical_threshold_hours: float = 40,  # 5 days
    project_start: datetime = None
) -> CriticalPathResult:
    """
    Identify critical and near-critical paths.
    """
    from .cpm.engine import CPMEngine

    engine = CPMEngine(network, calendars)
    result = engine.run(project_start)

    critical = []
    near_critical = []
    float_buckets = {'0': 0, '1-8': 0, '9-40': 0, '41-80': 0, '>80': 0}

    for task in network.tasks.values():
        if task.total_float_hours is None:
            continue

        # Categorize
        if task.total_float_hours <= 0:
            critical.append(task)
            float_buckets['0'] += 1
        elif task.total_float_hours <= 8:
            near_critical.append(task)
            float_buckets['1-8'] += 1
        elif task.total_float_hours <= 40:
            near_critical.append(task)
            float_buckets['9-40'] += 1
        elif task.total_float_hours <= 80:
            float_buckets['41-80'] += 1
        else:
            float_buckets['>80'] += 1

    # Sort critical path by early start
    critical.sort(key=lambda t: t.early_start or datetime.max)
    near_critical.sort(key=lambda t: t.total_float_hours or float('inf'))

    return CriticalPathResult(
        critical_path=critical,
        near_critical_tasks=[t for t in near_critical if t.total_float_hours <= near_critical_threshold_hours],
        float_distribution=float_buckets,
        project_finish=result.project_finish,
        near_critical_threshold_hours=near_critical_threshold_hours,
    )
```

---

## Usage Examples

```python
from scripts.primavera.analyze.data_loader import load_schedule
from scripts.primavera.analyze.cpm.engine import CPMEngine
from scripts.primavera.analyze.analysis.single_task_impact import analyze_task_impact
from scripts.primavera.analyze.analysis.delay_attribution import attribute_delays
from scripts.primavera.analyze.analysis.critical_path import analyze_critical_path

# Load latest YATES schedule (file_id=66)
network, calendars = load_schedule(file_id=66)

# Run CPM
engine = CPMEngine(network, calendars)
result = engine.run()

print(f"Project finishes: {result.project_finish}")
print(f"Critical path: {len(result.critical_path)} tasks")

# What-if: What if a specific task takes 2 weeks longer?
impact = analyze_task_impact(
    network, calendars,
    task_id="66_12345678",
    duration_delta_hours=80  # 10 days * 8 hours
)
print(f"Slip impact: {impact.slip_hours} hours ({impact.slip_hours/8:.1f} days)")

# Compare schedule versions: Oct 2022 baseline vs Nov 2025 current
attribution = attribute_delays(
    baseline_file_id=1,
    current_file_id=66
)
print(f"\nTotal schedule slip: {attribution.total_slip_hours/8:.0f} days")
print(f"\nTop 10 slip contributors:")
for contrib in attribution.task_contributions[:10]:
    print(f"  {contrib.task_code}: +{contrib.duration_delta_hours/8:.0f}d -> "
          f"{contrib.slip_contribution_hours/8:.0f}d slip ({contrib.slip_contribution_pct:.1f}%)")

# Critical path analysis
cp_analysis = analyze_critical_path(network, calendars, near_critical_threshold_hours=40)
print(f"\nCritical path: {len(cp_analysis.critical_path)} tasks")
print(f"Near-critical (<5 days float): {len(cp_analysis.near_critical_tasks)} tasks")
print(f"Float distribution: {cp_analysis.float_distribution}")
```

---

## Implementation Order

1. **cpm/calendar.py** - Parse P6 calendar format, implement date math
2. **cpm/models.py** - Data classes
3. **cpm/network.py** - Task network with topological sort
4. **cpm/engine.py** - Forward/backward pass
5. **data_loader.py** - Load P6 CSVs
6. **analysis/critical_path.py** - Basic CP analysis
7. **analysis/single_task_impact.py** - What-if for single task
8. **analysis/delay_attribution.py** - Cross-version comparison

## Testing Strategy

1. **Unit tests** for calendar parsing and date math
2. **Validation** against P6's stored early/late dates for a sample schedule
3. **Integration test** running full CPM on actual schedule data
4. **Comparison test** verifying our critical path matches P6's driving_path_flag

---

## Notes

- P6's `driving_path_flag='Y'` indicates tasks on the longest path - can use for validation
- `total_float_hr_cnt` in task.csv is P6's calculated float - compare against ours
- Calendar exceptions use Excel serial dates (days since 1899-12-30)
- Some tasks may have constraints (CS_SNET, CS_FNLT) that override calculated dates
