"""
Task Network for CPM calculations.

Manages tasks and dependencies with support for topological sorting
and network traversal.
"""

from collections import defaultdict
from copy import copy
from typing import Optional

from .models import Task, Dependency


class TaskNetwork:
    """
    Task dependency network for CPM calculations.

    Maintains tasks and their predecessor/successor relationships
    with efficient lookups and topological sorting.
    """

    def __init__(self):
        self.tasks: dict[str, Task] = {}
        self.dependencies: list[Dependency] = []
        self._successors: dict[str, list[Dependency]] = defaultdict(list)
        self._predecessors: dict[str, list[Dependency]] = defaultdict(list)

    def add_task(self, task: Task) -> None:
        """Add a task to the network."""
        self.tasks[task.task_id] = task

    def add_dependency(self, dep: Dependency) -> None:
        """
        Add a dependency to the network.

        Both predecessor and successor tasks must exist in the network.
        """
        if dep.pred_task_id not in self.tasks:
            raise ValueError(f"Predecessor task {dep.pred_task_id} not in network")
        if dep.succ_task_id not in self.tasks:
            raise ValueError(f"Successor task {dep.succ_task_id} not in network")

        self.dependencies.append(dep)
        self._successors[dep.pred_task_id].append(dep)
        self._predecessors[dep.succ_task_id].append(dep)

    def add_dependency_safe(self, dep: Dependency) -> bool:
        """
        Add a dependency only if both tasks exist.

        Returns True if added, False if skipped.
        """
        if dep.pred_task_id not in self.tasks or dep.succ_task_id not in self.tasks:
            return False
        self.dependencies.append(dep)
        self._successors[dep.pred_task_id].append(dep)
        self._predecessors[dep.succ_task_id].append(dep)
        return True

    def get_task(self, task_id: str) -> Optional[Task]:
        """Get a task by ID."""
        return self.tasks.get(task_id)

    def get_successors(self, task_id: str) -> list[Dependency]:
        """Get dependencies where task_id is the predecessor."""
        return self._successors.get(task_id, [])

    def get_predecessors(self, task_id: str) -> list[Dependency]:
        """Get dependencies where task_id is the successor."""
        return self._predecessors.get(task_id, [])

    def get_successor_tasks(self, task_id: str) -> list[Task]:
        """Get successor Task objects."""
        deps = self.get_successors(task_id)
        return [self.tasks[d.succ_task_id] for d in deps if d.succ_task_id in self.tasks]

    def get_predecessor_tasks(self, task_id: str) -> list[Task]:
        """Get predecessor Task objects."""
        deps = self.get_predecessors(task_id)
        return [self.tasks[d.pred_task_id] for d in deps if d.pred_task_id in self.tasks]

    def get_start_tasks(self) -> list[str]:
        """Get task IDs with no predecessors."""
        return [tid for tid in self.tasks if not self._predecessors.get(tid)]

    def get_end_tasks(self) -> list[str]:
        """Get task IDs with no successors."""
        return [tid for tid in self.tasks if not self._successors.get(tid)]

    def topological_sort(self) -> list[str]:
        """
        Return task IDs in topological order (predecessors before successors).

        Uses Kahn's algorithm. Raises ValueError if circular dependency detected.
        """
        # Calculate in-degree for each task
        in_degree = {tid: len(self._predecessors.get(tid, [])) for tid in self.tasks}

        # Start with tasks that have no predecessors
        queue = [tid for tid, deg in in_degree.items() if deg == 0]
        result = []

        while queue:
            task_id = queue.pop(0)
            result.append(task_id)

            # Reduce in-degree for all successors
            for dep in self._successors.get(task_id, []):
                in_degree[dep.succ_task_id] -= 1
                if in_degree[dep.succ_task_id] == 0:
                    queue.append(dep.succ_task_id)

        if len(result) != len(self.tasks):
            # Find tasks involved in cycle
            remaining = set(self.tasks.keys()) - set(result)
            raise ValueError(f"Circular dependency detected involving {len(remaining)} tasks: "
                           f"{list(remaining)[:5]}...")

        return result

    def reverse_topological_sort(self) -> list[str]:
        """Return task IDs in reverse topological order (successors before predecessors)."""
        return list(reversed(self.topological_sort()))

    def get_all_predecessors(self, task_id: str, include_self: bool = False) -> set[str]:
        """Get all predecessor task IDs (transitive closure)."""
        result = set()
        if include_self:
            result.add(task_id)

        visited = set()
        queue = [task_id]

        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)

            for dep in self._predecessors.get(current, []):
                result.add(dep.pred_task_id)
                queue.append(dep.pred_task_id)

        return result

    def get_all_successors(self, task_id: str, include_self: bool = False) -> set[str]:
        """Get all successor task IDs (transitive closure)."""
        result = set()
        if include_self:
            result.add(task_id)

        visited = set()
        queue = [task_id]

        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)

            for dep in self._successors.get(current, []):
                result.add(dep.succ_task_id)
                queue.append(dep.succ_task_id)

        return result

    def clone(self) -> 'TaskNetwork':
        """
        Create a deep copy of the network for what-if analysis.

        Tasks are shallow-copied so modifications don't affect original.
        """
        new_network = TaskNetwork()

        # Copy tasks (shallow copy of each Task object)
        for tid, task in self.tasks.items():
            new_network.tasks[tid] = copy(task)

        # Copy dependencies
        new_network.dependencies = [copy(d) for d in self.dependencies]

        # Rebuild adjacency lists
        for dep in new_network.dependencies:
            new_network._successors[dep.pred_task_id].append(dep)
            new_network._predecessors[dep.succ_task_id].append(dep)

        return new_network

    def modify_task_duration(self, task_id: str, new_duration_hours: float) -> None:
        """Modify a task's duration for what-if analysis."""
        if task_id not in self.tasks:
            raise ValueError(f"Task {task_id} not in network")
        self.tasks[task_id].duration_hours = new_duration_hours

    def filter_by_status(self, statuses: list[str]) -> 'TaskNetwork':
        """
        Create a new network with only tasks having specified statuses.

        Dependencies are included only if both tasks are in the filtered set.
        """
        new_network = TaskNetwork()

        # Add tasks with matching status
        for tid, task in self.tasks.items():
            if task.status in statuses:
                new_network.tasks[tid] = copy(task)

        # Add dependencies where both tasks exist
        for dep in self.dependencies:
            if dep.pred_task_id in new_network.tasks and dep.succ_task_id in new_network.tasks:
                new_network.dependencies.append(copy(dep))
                new_network._successors[dep.pred_task_id].append(dep)
                new_network._predecessors[dep.succ_task_id].append(dep)

        return new_network

    def get_statistics(self) -> dict:
        """Get network statistics."""
        task_types = defaultdict(int)
        statuses = defaultdict(int)

        for task in self.tasks.values():
            task_types[task.task_type] += 1
            statuses[task.status] += 1

        return {
            'total_tasks': len(self.tasks),
            'total_dependencies': len(self.dependencies),
            'start_tasks': len(self.get_start_tasks()),
            'end_tasks': len(self.get_end_tasks()),
            'task_types': dict(task_types),
            'statuses': dict(statuses),
        }

    def validate(self) -> list[str]:
        """
        Validate network integrity.

        Returns list of issues found (empty if valid).
        """
        issues = []

        # Check for orphan dependencies
        for dep in self.dependencies:
            if dep.pred_task_id not in self.tasks:
                issues.append(f"Dependency references missing predecessor: {dep.pred_task_id}")
            if dep.succ_task_id not in self.tasks:
                issues.append(f"Dependency references missing successor: {dep.succ_task_id}")

        # Check for circular dependencies
        try:
            self.topological_sort()
        except ValueError as e:
            issues.append(str(e))

        # Check for tasks with missing calendars (just a warning)
        missing_calendars = set()
        for task in self.tasks.values():
            if not task.calendar_id:
                missing_calendars.add(task.task_id)
        if missing_calendars:
            issues.append(f"{len(missing_calendars)} tasks missing calendar assignment")

        return issues

    def __len__(self) -> int:
        return len(self.tasks)

    def __contains__(self, task_id: str) -> bool:
        return task_id in self.tasks

    def __repr__(self) -> str:
        return f"TaskNetwork({len(self.tasks)} tasks, {len(self.dependencies)} dependencies)"
