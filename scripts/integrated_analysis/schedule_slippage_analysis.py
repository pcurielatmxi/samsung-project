"""
Schedule Slippage Attribution Analysis
======================================

This module implements schedule-to-schedule comparison methodology to identify
which tasks contributed most to project schedule slippage between P6 snapshots.

OVERVIEW
--------
When a project schedule slips (the forecasted completion date moves later), we need
to understand WHY. This analysis compares two schedule snapshots and attributes
the slippage to specific tasks based on how their dates and float changed.

METHODOLOGY
-----------
The core insight is that when a task's finish date slips, the slip can come from
two sources:

1. INHERITED DELAY: The task's predecessors pushed back its start date
   - Measured by: start_slip = early_start[curr] - early_start[prev]
   - The task is "waiting" for predecessors that are running late

2. OWN DELAY: The task itself took longer than previously planned
   - Measured by: own_delay = finish_slip - start_slip
   - The task's duration grew, or progress was slower than expected

This decomposition allows us to trace delays to their root cause:
- If own_delay > 0: This task is CAUSING delay (investigate it)
- If inherited_delay > 0: This task is RECEIVING delay from predecessors

FORMULAS
--------
All date comparisons use P6's early_start_date and early_end_date fields, which
represent the forward-pass calculated dates from the CPM algorithm.

    finish_slip = early_end_date[curr] - early_end_date[prev]     (days)
    start_slip  = early_start_date[curr] - early_start_date[prev] (days)
    own_delay   = finish_slip - start_slip                        (days)
    inherited_delay = start_slip                                  (days)

    Relationship: finish_slip = own_delay + inherited_delay

Float (slack) is used to identify criticality:

    float_change = total_float[curr] - total_float[prev]          (days)
    is_critical  = total_float[curr] <= 0

    Note: P6 stores float in hours; we divide by 8 to convert to days.

PROJECT-LEVEL SLIPPAGE
----------------------
The project finish date is calculated as the maximum early_end_date among tasks
on the driving path (driving_path_flag = 'Y'). If no driving path tasks exist,
we fall back to the maximum early_end_date across all tasks.

    project_slippage = project_finish[curr] - project_finish[prev]

This represents how much the overall project completion forecast has moved.

TASK CATEGORIZATION
-------------------
Tasks are categorized based on their status and delay contribution:

EXISTING TASKS (present in both snapshots):
    ACTIVE_DELAYER    : status=TK_Active AND own_delay > 1 day AND finish_slip > 0
                        Task is in-progress and actively causing delay.
                        Both conditions required: took longer than planned AND
                        finish date moved later (not just consuming early start).

    COMPLETED_DELAYER : status=TK_Complete AND own_delay > 1 day AND finish_slip > 0
                        Task finished late, may have pushed successors.
                        Both conditions required: took longer than planned AND
                        finish date moved later.

    WAITING_INHERITED : status=TK_NotStart AND inherited_delay > 1 day
                        Task hasn't started, delayed by predecessors

    WAITING_SQUEEZED  : status=TK_NotStart AND float_change < -5 days
                        Task's buffer is eroding (becoming more critical)

    ACTIVE_OK         : status=TK_Active AND (own_delay <= 1 day OR finish_slip <= 0)
                        Task is in-progress but not causing delay.
                        Includes tasks that started early and took longer but
                        still finish on time (positive own_delay, non-positive finish_slip).

    COMPLETED_OK      : status=TK_Complete AND (own_delay <= 1 day OR finish_slip <= 0)
                        Task finished on time or early

    WAITING_OK        : status=TK_NotStart, no inherited delay or squeeze
                        Task is waiting but not affected by delays

NEW TASKS (only in current snapshot):
    NEW_CRITICAL      : on_driving_path=True OR is_critical=True
                        New task was added to the critical path

    NEW_NONCRITICAL   : Not on driving path and has positive float
                        New task added but not affecting project finish

CATEGORY THRESHOLDS:
    - own_delay threshold: 1 day (ignore sub-day variations as noise)
    - inherited_delay threshold: 1 day
    - float_squeeze threshold: 5 days (significant buffer erosion)

IMPACT SCORE
------------
Tasks are ranked by impact_score for prioritization:

    impact_score = |own_delay| × criticality_weight × driving_path_weight

    Where:
        criticality_weight = 2 if is_critical else 1
        driving_path_weight = 1.5 if on_driving_path else 1

This weights tasks that are on the critical/driving path higher because their
delays directly impact the project finish date.

NOTE: The impact_score uses absolute value of own_delay. This means tasks that
recovered time (negative own_delay) will also rank high. Filter by own_delay > 0
if you only want delay-causing tasks.

ASSUMPTIONS
-----------
1. task_code is stable across snapshots (same task_code = same task)
2. P6's driving_path_flag accurately identifies the driving path
3. Float is measured in hours with 8-hour workdays
4. Early dates (not late dates) reflect the current plan
5. Calendar days are used (not working days) for date differences
6. NaN float means "not calculated" (completed tasks, milestones), NOT critical
7. For NOT STARTED tasks, own_delay represents duration/scope change, not execution delay

LIMITATIONS
-----------
1. No baseline comparison: We compare snapshot-to-snapshot, not plan-to-baseline.
   If the original baseline was unrealistic, we won't detect that here.

2. Driving path calculation: P6's driving path is recalculated each snapshot.
   A task may be on the driving path in one snapshot but not another.

3. New tasks: We identify new critical tasks but cannot determine if they were
   always planned (just not in previous export) or truly new scope.

4. Resource constraints: P6 may level resources, affecting dates. This analysis
   treats all date changes equally regardless of cause.

5. Parallel paths: If multiple near-critical paths exist, focusing only on the
   driving path may miss emerging critical paths.

DATA SOURCES
------------
Input: Primavera P6 XER exports processed to CSV
    - task.csv: Task records with dates, float, status
    - xer_files.csv: Schedule snapshot metadata

Key P6 fields used:
    - task_code: Stable task identifier across snapshots
    - early_start_date, early_end_date: Forward-pass calculated dates
    - total_float_hr_cnt: Total float in hours
    - remain_drtn_hr_cnt: Remaining duration in hours
    - status_code: TK_Active, TK_Complete, TK_NotStart
    - driving_path_flag: 'Y' if task is on driving path

OUTPUT
------
The analysis produces:
1. Project-level slippage (days the project finish moved)
2. Task-level comparison DataFrame with all metrics
3. New task analysis DataFrame
4. Categorized task counts
5. Formatted report with top contributors by category

USAGE
-----
Command line:
    python -m scripts.integrated_analysis.schedule_slippage_analysis --year 2025 --month 9
    python -m scripts.integrated_analysis.schedule_slippage_analysis --prev-file 82 --curr-file 83
    python -m scripts.integrated_analysis.schedule_slippage_analysis --list-schedules

Programmatic:
    analyzer = ScheduleSlippageAnalyzer()
    result = analyzer.analyze_month(2025, 9)
    # result['tasks'] - DataFrame with task-level metrics
    # result['project_metrics'] - dict with project-level metrics
    # result['new_tasks'] - DataFrame with new task analysis
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import sys
import argparse

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.config.settings import settings


###############################################################################
# CONFIGURATION CONSTANTS
###############################################################################

# Threshold for categorizing a task as a "delayer"
# Tasks with own_delay <= this value are considered on-track (not causing delay)
# Rationale: Sub-day variations are noise from P6's date rounding
OWN_DELAY_THRESHOLD_DAYS = 1

# Threshold for categorizing inherited delay as significant
INHERITED_DELAY_THRESHOLD_DAYS = 1

# Threshold for "float squeeze" - buffer erosion that warrants attention
# Rationale: 5+ days of float loss indicates the task is becoming more critical
FLOAT_SQUEEZE_THRESHOLD_DAYS = -5

# Working hours per day for float conversion (P6 stores float in hours)
HOURS_PER_WORKDAY = 8

# Impact score weights
CRITICALITY_WEIGHT = 2      # Tasks on critical path weighted 2x
DRIVING_PATH_WEIGHT = 1.5   # Tasks on driving path weighted 1.5x


###############################################################################
# MAIN ANALYZER CLASS
###############################################################################

class ScheduleSlippageAnalyzer:
    """
    Analyze schedule slippage between consecutive P6 schedule snapshots.

    This class provides methods to:
    1. Load and query P6 schedule data
    2. Compare two schedule snapshots to calculate slippage metrics
    3. Categorize tasks by their delay contribution
    4. Generate formatted attribution reports

    Attributes:
        primavera_dir (Path): Directory containing processed P6 CSV files
        tasks_df (DataFrame): All task records across all snapshots
        files_df (DataFrame): Schedule snapshot metadata (file_id, date, etc.)

    Example:
        >>> analyzer = ScheduleSlippageAnalyzer()
        >>> result = analyzer.analyze_month(2025, 9)
        >>> print(f"Project slipped {result['project_metrics']['project_slippage_days']} days")
    """

    def __init__(self):
        """
        Initialize analyzer by loading P6 data from processed CSV files.

        Loads:
            - task.csv: ~470K task records across ~90 schedule snapshots
            - xer_files.csv: Metadata mapping file_id to snapshot dates
        """
        self.primavera_dir = settings.PRIMAVERA_PROCESSED_DIR
        self.tasks_df = None
        self.files_df = None
        self._load_data()

    def _load_data(self):
        """
        Load P6 task and file data from CSV files.

        Loads only the columns needed for slippage analysis to minimize memory:
            - Identifiers: file_id, task_id, task_code, task_name
            - Dates: early_start/end, late_start/end, target_start/end, actual_start/end
            - Criticality: total_float_hr_cnt, driving_path_flag
            - Status: status_code, remain_drtn_hr_cnt
        """
        print("Loading P6 data...")

        # Load schedule snapshot metadata (file_id -> date mapping)
        self.files_df = pd.read_csv(self.primavera_dir / 'xer_files.csv')

        # Load project data to get data_date (last_recalc_date) for each file_id
        # This is needed for gap-based metrics for active tasks
        project_path = self.primavera_dir / 'project.csv'
        if project_path.exists():
            project_df = pd.read_csv(project_path, low_memory=False)
            # Parse last_recalc_date as data_date
            project_df['data_date'] = pd.to_datetime(project_df['last_recalc_date'], errors='coerce')
            # Create file_id -> data_date mapping
            self.data_date_by_file = dict(zip(project_df['file_id'], project_df['data_date']))
            print(f"  Loaded data_date for {len(self.data_date_by_file)} schedules")
        else:
            self.data_date_by_file = {}
            print("  Warning: project.csv not found, data_date lookup not available")

        # Define columns needed for analysis
        # NOTE: We load all date fields even though we primarily use early_start/end
        # because target dates may be useful for baseline comparison in future
        # ENHANCED: Added constraint columns (cstr_type, cstr_date) for constraint change detection
        cols_needed = [
            'file_id', 'task_id', 'task_code', 'task_name', 'status_code',
            'early_start_date', 'early_end_date', 'late_start_date', 'late_end_date',
            'target_start_date', 'target_end_date', 'act_start_date', 'act_end_date',
            'total_float_hr_cnt', 'remain_drtn_hr_cnt', 'target_drtn_hr_cnt', 'driving_path_flag',
            'cstr_type', 'cstr_date'  # Constraint type and date for enhanced attribution
        ]

        # Load tasks with date parsing
        # NOTE: This is ~230MB in memory for 470K records
        self.tasks_df = pd.read_csv(
            self.primavera_dir / 'task.csv',
            usecols=cols_needed,
            parse_dates=['early_start_date', 'early_end_date',
                        'late_start_date', 'late_end_date',
                        'target_start_date', 'target_end_date',
                        'act_start_date', 'act_end_date',
                        'cstr_date']  # Parse constraint date
        )

        print(f"  Loaded {len(self.tasks_df):,} task records across {self.tasks_df['file_id'].nunique()} schedules")

    def _load_taxonomy_for_file(self, file_id):
        """
        Load task taxonomy data for a specific file_id.

        The taxonomy provides scope_desc, trade_name, and other classification
        fields that can be used to group tasks for analysis.

        Args:
            file_id: The schedule file_id to load taxonomy for

        Returns:
            DataFrame with task_code as index and taxonomy columns (scope_desc, trade_name, etc.)
            Returns empty DataFrame if taxonomy file doesn't exist.
        """
        taxonomy_path = settings.PRIMAVERA_DERIVED_DIR / 'task_taxonomy.csv'
        if not taxonomy_path.exists():
            return pd.DataFrame()

        # Load taxonomy with relevant columns
        taxonomy = pd.read_csv(
            taxonomy_path,
            usecols=['task_id', 'scope_desc', 'trade_name', 'building', 'level'],
            low_memory=False
        )

        # Filter to this file_id (task_id format is "{file_id}_{task_id}")
        file_prefix = f"{file_id}_"
        taxonomy = taxonomy[taxonomy['task_id'].str.startswith(file_prefix)].copy()

        if len(taxonomy) == 0:
            return pd.DataFrame()

        # Get task_code mapping from task.csv
        # Note: task_id in self.tasks_df already has the "{file_id}_" prefix
        task_lookup = self.tasks_df[self.tasks_df['file_id'] == file_id][['task_id', 'task_code']].copy()

        # Join taxonomy with task_code
        taxonomy = taxonomy.merge(task_lookup, on='task_id', how='inner')

        # Set task_code as index for easy lookup
        taxonomy = taxonomy.set_index('task_code')

        return taxonomy[['scope_desc', 'trade_name', 'building', 'level']]

    def _compare_relationships(self, file_id_prev: int, file_id_curr: int) -> dict:
        """
        Compare predecessor relationships between two schedule snapshots.

        This method identifies logic changes (new/removed predecessor relationships)
        which can cause inherited delays that aren't from predecessor task duration
        growth but from network restructuring.

        Args:
            file_id_prev: file_id of the earlier schedule snapshot
            file_id_curr: file_id of the later schedule snapshot

        Returns:
            dict with:
                'added_relationships': list of (pred_task_code, succ_task_code, pred_type)
                'removed_relationships': list of (pred_task_code, succ_task_code, pred_type)
                'tasks_with_new_preds': set of task_codes that gained predecessors
                'tasks_with_removed_preds': set of task_codes that lost predecessors
                'new_pred_count': dict mapping task_code -> number of new predecessors

        Note:
            P6 relationships use task_id (internal numeric IDs), not task_code.
            This method joins with task data to translate to task_codes for
            cross-snapshot comparison.
        """
        # Load relationship data (taskpred.csv)
        taskpred_path = self.primavera_dir / 'taskpred.csv'
        if not taskpred_path.exists():
            return {
                'added_relationships': [],
                'removed_relationships': [],
                'tasks_with_new_preds': set(),
                'tasks_with_removed_preds': set(),
                'new_pred_count': {}
            }

        taskpred_df = pd.read_csv(taskpred_path)

        # Get relationships for each file_id
        rels_prev = taskpred_df[taskpred_df['file_id'] == file_id_prev][
            ['task_id', 'pred_task_id', 'pred_type']
        ].copy()
        rels_curr = taskpred_df[taskpred_df['file_id'] == file_id_curr][
            ['task_id', 'pred_task_id', 'pred_type']
        ].copy()

        # Build task_id -> task_code mappings for both snapshots
        tasks_prev = self.tasks_df[self.tasks_df['file_id'] == file_id_prev][
            ['task_id', 'task_code']
        ].copy()
        tasks_curr = self.tasks_df[self.tasks_df['file_id'] == file_id_curr][
            ['task_id', 'task_code']
        ].copy()

        # Create lookup dicts
        id_to_code_prev = dict(zip(tasks_prev['task_id'].astype(str), tasks_prev['task_code']))
        id_to_code_curr = dict(zip(tasks_curr['task_id'].astype(str), tasks_curr['task_code']))

        # Convert relationships to (pred_code, succ_code, type) tuples
        def rel_to_tuple(row, id_to_code):
            pred_id = str(row['pred_task_id'])
            succ_id = str(row['task_id'])
            pred_code = id_to_code.get(pred_id)
            succ_code = id_to_code.get(succ_id)
            if pred_code and succ_code:
                return (pred_code, succ_code, row['pred_type'])
            return None

        rels_prev_set = set(
            t for t in (rel_to_tuple(row, id_to_code_prev) for _, row in rels_prev.iterrows())
            if t is not None
        )
        rels_curr_set = set(
            t for t in (rel_to_tuple(row, id_to_code_curr) for _, row in rels_curr.iterrows())
            if t is not None
        )

        # Find added and removed relationships
        added = rels_curr_set - rels_prev_set
        removed = rels_prev_set - rels_curr_set

        # Extract affected tasks
        tasks_with_new_preds = {rel[1] for rel in added}  # succ_task_code
        tasks_with_removed_preds = {rel[1] for rel in removed}

        # Count new predecessors per task
        new_pred_count = {}
        for rel in added:
            succ_code = rel[1]
            new_pred_count[succ_code] = new_pred_count.get(succ_code, 0) + 1

        return {
            'added_relationships': list(added),
            'removed_relationships': list(removed),
            'tasks_with_new_preds': tasks_with_new_preds,
            'tasks_with_removed_preds': tasks_with_removed_preds,
            'new_pred_count': new_pred_count
        }

    def trace_root_causes(self, comparison_result: dict, file_id_curr: int,
                          own_delay_threshold: float = 0.8) -> pd.DataFrame:
        """
        Trace delay propagation chains to identify root cause tasks.

        For each task with float decrease, this method determines whether the
        task is a ROOT CAUSE (originated the delay) or PROPAGATED (inherited
        the delay from upstream tasks).

        Algorithm:
            1. Build predecessor lookup from taskpred.csv
            2. For each task with float decrease:
               a. If own_delay >= threshold × float_decrease → ROOT_CAUSE
               b. Else, trace upstream through predecessors
               c. Find the first task in the chain that is a ROOT_CAUSE
            3. Calculate propagation depth from root cause

        Args:
            comparison_result: Output from compare_schedules()
            file_id_curr: Current schedule file_id (for loading relationships)
            own_delay_threshold: Fraction of float decrease that own_delay must
                                 explain to be considered a root cause (default 0.8 = 80%)

        Returns:
            DataFrame with columns:
                - task_code: The task being analyzed
                - is_root_cause: True if this task originated the delay
                - root_cause_task: task_code of the originating task (self if root cause)
                - cause_type: DURATION, CONSTRAINT, LOGIC_CHANGE, or UNKNOWN
                - propagation_depth: Distance from root cause (0 for root cause itself)
                - downstream_impact_count: Number of tasks affected by this root cause
        """
        tasks_df = comparison_result['tasks']

        if tasks_df is None or len(tasks_df) == 0:
            return pd.DataFrame()

        # Filter to tasks with significant float decrease
        float_decrease_threshold = -1  # At least 1 day of float loss
        affected_tasks = tasks_df[
            tasks_df['float_change_days'].fillna(0) < float_decrease_threshold
        ].copy()

        if len(affected_tasks) == 0:
            return pd.DataFrame()

        # Load predecessor relationships for the current schedule
        taskpred_path = self.primavera_dir / 'taskpred.csv'
        if not taskpred_path.exists():
            # No relationship data - mark all as root causes
            result = affected_tasks[['task_code']].copy()
            result['is_root_cause'] = True
            result['root_cause_task'] = result['task_code']
            result['cause_type'] = 'UNKNOWN'
            result['propagation_depth'] = 0
            result['downstream_impact_count'] = 0
            return result

        taskpred_df = pd.read_csv(taskpred_path)
        rels_curr = taskpred_df[taskpred_df['file_id'] == file_id_curr][
            ['task_id', 'pred_task_id']
        ].copy()

        # Build task_id -> task_code mapping
        tasks_curr = self.tasks_df[self.tasks_df['file_id'] == file_id_curr][
            ['task_id', 'task_code']
        ].copy()
        id_to_code = dict(zip(tasks_curr['task_id'].astype(str), tasks_curr['task_code']))

        # Build predecessor adjacency: task_code -> list of predecessor task_codes
        pred_adjacency = {}
        for _, row in rels_curr.iterrows():
            succ_code = id_to_code.get(str(row['task_id']))
            pred_code = id_to_code.get(str(row['pred_task_id']))
            if succ_code and pred_code:
                if succ_code not in pred_adjacency:
                    pred_adjacency[succ_code] = []
                pred_adjacency[succ_code].append(pred_code)

        # Create lookup dict for task metrics
        task_metrics = {}
        for _, row in tasks_df.iterrows():
            task_metrics[row['task_code']] = {
                'own_delay': row.get('own_delay_days', 0) or 0,
                'float_change': row.get('float_change_days', 0) or 0,
                'constraint_tightened': row.get('constraint_tightened', False),
                'has_new_predecessors': row.get('has_new_predecessors', False),
            }

        # Memoization for root cause tracing
        root_cause_cache = {}  # task_code -> (root_cause_task, cause_type, depth)

        def determine_cause_type(task_code: str) -> str:
            """Determine the type of cause for a root cause task."""
            metrics = task_metrics.get(task_code, {})
            if metrics.get('constraint_tightened', False):
                return 'CONSTRAINT'
            if metrics.get('has_new_predecessors', False):
                return 'LOGIC_CHANGE'
            if abs(metrics.get('own_delay', 0)) > 1:
                return 'DURATION'
            return 'UNKNOWN'

        def trace_upstream(task_code: str, visited: set = None, depth: int = 0) -> tuple:
            """
            Recursively trace upstream to find root cause.

            Returns (root_cause_task, cause_type, propagation_depth)
            """
            if visited is None:
                visited = set()

            # Prevent infinite loops in cyclic graphs
            if task_code in visited:
                return (task_code, 'UNKNOWN', depth)
            visited.add(task_code)

            # Check cache
            if task_code in root_cause_cache:
                cached = root_cause_cache[task_code]
                return (cached[0], cached[1], cached[2] + depth)

            metrics = task_metrics.get(task_code, {})
            own_delay = abs(metrics.get('own_delay', 0))
            float_decrease = abs(metrics.get('float_change', 0))

            # Task is ROOT_CAUSE if own_delay explains the float decrease
            if float_decrease > 0 and own_delay >= own_delay_threshold * float_decrease:
                cause_type = determine_cause_type(task_code)
                root_cause_cache[task_code] = (task_code, cause_type, 0)
                return (task_code, cause_type, depth)

            # Task is ROOT_CAUSE if it has constraint or logic change
            if metrics.get('constraint_tightened', False):
                root_cause_cache[task_code] = (task_code, 'CONSTRAINT', 0)
                return (task_code, 'CONSTRAINT', depth)

            if metrics.get('has_new_predecessors', False):
                root_cause_cache[task_code] = (task_code, 'LOGIC_CHANGE', 0)
                return (task_code, 'LOGIC_CHANGE', depth)

            # Otherwise, trace upstream through predecessors
            predecessors = pred_adjacency.get(task_code, [])
            if not predecessors:
                # No predecessors - this task is the root cause by default
                cause_type = determine_cause_type(task_code)
                root_cause_cache[task_code] = (task_code, cause_type, 0)
                return (task_code, cause_type, depth)

            # Find predecessor with most float decrease
            max_pred_float_decrease = 0
            max_pred = None
            for pred_code in predecessors:
                pred_metrics = task_metrics.get(pred_code, {})
                pred_float_decrease = abs(pred_metrics.get('float_change', 0))
                if pred_float_decrease > max_pred_float_decrease:
                    max_pred_float_decrease = pred_float_decrease
                    max_pred = pred_code

            # If a predecessor has significant float decrease, trace upstream
            if max_pred and max_pred_float_decrease >= float_decrease * 0.5:
                result = trace_upstream(max_pred, visited, depth + 1)
                root_cause_cache[task_code] = (result[0], result[1], result[2] - depth)
                return result

            # No predecessor with significant float decrease - this task is root cause
            cause_type = determine_cause_type(task_code)
            root_cause_cache[task_code] = (task_code, cause_type, 0)
            return (task_code, cause_type, depth)

        # Trace root causes for all affected tasks
        results = []
        for _, row in affected_tasks.iterrows():
            task_code = row['task_code']
            root_cause, cause_type, prop_depth = trace_upstream(task_code)
            results.append({
                'task_code': task_code,
                'is_root_cause': root_cause == task_code,
                'root_cause_task': root_cause,
                'cause_type': cause_type,
                'propagation_depth': prop_depth,
            })

        result_df = pd.DataFrame(results)

        # Calculate downstream impact for each root cause
        if len(result_df) > 0:
            root_cause_counts = result_df.groupby('root_cause_task').size()
            result_df['downstream_impact_count'] = result_df['root_cause_task'].map(
                lambda x: root_cause_counts.get(x, 0)
            )
        else:
            result_df['downstream_impact_count'] = 0

        return result_df

    def get_ordered_schedules(self, schedule_type='YATES'):
        """
        Get schedule snapshots ordered chronologically for comparison.

        Filters to a specific schedule type (YATES or SECAI) and returns
        snapshots in date order, with duplicates removed.

        Args:
            schedule_type: 'YATES' (main contractor, 88 snapshots) or
                          'SECAI' (subcontractor, 2 snapshots)

        Returns:
            DataFrame with columns:
                - file_id: Unique identifier for the schedule snapshot
                - filename: Original XER filename
                - snapshot_date: Date the schedule was exported/published

        Note:
            If multiple snapshots exist for the same date, only the first
            (by file_id) is kept. This handles cases where the scheduler
            may have exported multiple versions on the same day.
        """
        # Filter to requested schedule type with valid dates
        files = self.files_df[
            (self.files_df['schedule_type'] == schedule_type) &
            (self.files_df['date'].notna()) &
            (self.files_df['date'] != '')
        ].copy()

        # Parse dates (handles multiple formats via format='mixed')
        files['snapshot_date'] = pd.to_datetime(files['date'], format='mixed', errors='coerce')

        # Filter out rows where date parsing failed and sort chronologically
        files = files[files['snapshot_date'].notna()].sort_values('snapshot_date')

        # Remove duplicate dates - keep first occurrence
        # This handles cases where multiple XER files were exported on the same day
        files = files.drop_duplicates(subset='snapshot_date', keep='first')

        return files[['file_id', 'filename', 'snapshot_date']].reset_index(drop=True)

    def compare_schedules(self, file_id_prev, file_id_curr, date_prev=None, date_curr=None):
        """
        Compare two schedule snapshots and calculate slippage metrics for all tasks.

        This is the core analysis method. It:
        1. Calculates project-level slippage (overall schedule movement)
        2. Matches tasks across snapshots by task_code
        3. Computes slippage metrics for each common task
        4. Categorizes tasks by their delay contribution
        5. Analyzes newly added tasks

        Args:
            file_id_prev: file_id of the earlier schedule snapshot
            file_id_curr: file_id of the later schedule snapshot
            date_prev: Optional datetime for labeling (cosmetic only)
            date_curr: Optional datetime for labeling (cosmetic only)

        Returns:
            dict with three keys:
                'tasks': DataFrame with columns:
                    - task_code, task_name, status
                    - early_end_prev, early_end_curr, early_start_prev, early_start_curr
                    - finish_slip_days, start_slip_days
                    - own_delay_days, inherited_delay_days
                    - duration_change_days, float_change_days
                    - float_curr_days, is_critical, became_critical, on_driving_path
                    - impact_score, delay_category

                'project_metrics': dict with:
                    - project_finish_prev, project_finish_curr
                    - project_slippage_days
                    - driving_path_tasks_prev, driving_path_tasks_curr

                'new_tasks': DataFrame of tasks only in curr snapshot:
                    - task_code, task_name, status
                    - early_start, early_end
                    - total_float_days, remain_duration_days
                    - on_driving_path, is_critical
                    - delay_category, potential_impact_days

        Algorithm:
            1. Extract tasks for each file_id from the master DataFrame
            2. Calculate project finish as max(early_end_date) on driving path
            3. Merge tasks on task_code with outer join to detect added/removed
            4. For common tasks: compute finish_slip, start_slip, own_delay
            5. Categorize tasks based on status and delay metrics
            6. Analyze new tasks for critical path impact
        """
        # =========================================================================
        # STEP 1: Extract task sets for both snapshots
        # =========================================================================
        tasks_prev = self.tasks_df[self.tasks_df['file_id'] == file_id_prev].copy()
        tasks_curr = self.tasks_df[self.tasks_df['file_id'] == file_id_curr].copy()

        print(f"  Previous schedule (file_id={file_id_prev}): {len(tasks_prev):,} tasks")
        print(f"  Current schedule (file_id={file_id_curr}): {len(tasks_curr):,} tasks")

        # =========================================================================
        # STEP 1.2: Get data_date for each snapshot (needed for gap-based metrics)
        # =========================================================================
        # data_date is P6's "data date" - the date when the schedule was statused.
        # For active tasks, early_start moves with data_date, so we need this to
        # calculate the "gap" between early_start and data_date for proper attribution.
        data_date_prev = self.data_date_by_file.get(file_id_prev)
        data_date_curr = self.data_date_by_file.get(file_id_curr)

        if data_date_prev is not None and data_date_curr is not None:
            print(f"\n  DATA DATES:")
            print(f"    Previous: {data_date_prev.strftime('%Y-%m-%d') if pd.notna(data_date_prev) else 'N/A'}")
            print(f"    Current: {data_date_curr.strftime('%Y-%m-%d') if pd.notna(data_date_curr) else 'N/A'}")
            calendar_days = (data_date_curr - data_date_prev).days if pd.notna(data_date_prev) and pd.notna(data_date_curr) else None
            if calendar_days:
                print(f"    Calendar days between: {calendar_days}")

        # =========================================================================
        # STEP 1.5: ENHANCED - Compare predecessor relationships
        # =========================================================================
        # Detect logic changes (new/removed predecessor relationships)
        # This helps distinguish inherited delays from predecessor duration growth
        # vs. inherited delays from network restructuring (new predecessors added)
        relationship_changes = self._compare_relationships(file_id_prev, file_id_curr)
        tasks_with_new_preds = relationship_changes['tasks_with_new_preds']
        new_pred_count = relationship_changes['new_pred_count']

        if len(tasks_with_new_preds) > 0:
            print(f"\n  RELATIONSHIP CHANGES:")
            print(f"    Tasks with new predecessors: {len(tasks_with_new_preds):,}")
            print(f"    Total new relationships: {len(relationship_changes['added_relationships']):,}")
            print(f"    Total removed relationships: {len(relationship_changes['removed_relationships']):,}")

        # =========================================================================
        # STEP 2: Calculate PROJECT-LEVEL SLIPPAGE
        # =========================================================================
        # The project finish date is the latest early_end_date among driving path tasks.
        # If no driving path tasks exist (unusual), fall back to max of all tasks.
        #
        # NOTE: P6's driving_path_flag identifies tasks on the longest path to the
        # project finish. This is more specific than "critical path" which includes
        # all tasks with zero float.
        driving_prev = tasks_prev[tasks_prev['driving_path_flag'] == 'Y']
        driving_curr = tasks_curr[tasks_curr['driving_path_flag'] == 'Y']

        # Calculate project finish: max early_end on driving path (or all tasks if none)
        project_finish_prev = driving_prev['early_end_date'].max() if len(driving_prev) > 0 else tasks_prev['early_end_date'].max()
        project_finish_curr = driving_curr['early_end_date'].max() if len(driving_curr) > 0 else tasks_curr['early_end_date'].max()

        # Project slippage = how many days later is the current forecast vs previous
        # Positive = schedule slipped (bad), Negative = schedule improved (good)
        project_slippage_days = None
        if pd.notna(project_finish_prev) and pd.notna(project_finish_curr):
            project_slippage_days = (project_finish_curr - project_finish_prev).days

        project_metrics = {
            'project_finish_prev': project_finish_prev,
            'project_finish_curr': project_finish_curr,
            'project_slippage_days': project_slippage_days,
            'driving_path_tasks_prev': len(driving_prev),
            'driving_path_tasks_curr': len(driving_curr),
            'date_prev': date_prev,
            'date_curr': date_curr,
            'file_id_prev': file_id_prev,
            'file_id_curr': file_id_curr,
            'data_date_prev': data_date_prev,
            'data_date_curr': data_date_curr,
        }

        print(f"\n  PROJECT-LEVEL SLIPPAGE:")
        print(f"    Project finish (prev): {project_finish_prev}")
        print(f"    Project finish (curr): {project_finish_curr}")
        print(f"    Project slippage: {project_slippage_days} days")

        # =========================================================================
        # STEP 3: Match tasks across snapshots
        # =========================================================================
        # We use task_code as the stable identifier. Unlike task_id (which is a
        # database primary key that changes each export), task_code is assigned
        # by the scheduler and persists across schedule updates.
        #
        # Outer join allows us to detect:
        #   - 'both': Task exists in both snapshots (compare slippage)
        #   - 'left_only': Task removed from schedule (scope reduction or completion)
        #   - 'right_only': Task added to schedule (new scope)
        merged = pd.merge(
            tasks_prev,
            tasks_curr,
            on='task_code',
            suffixes=('_prev', '_curr'),
            how='outer',
            indicator=True
        )

        # Count task changes for summary
        n_common = (merged['_merge'] == 'both').sum()
        n_removed = (merged['_merge'] == 'left_only').sum()
        n_added = (merged['_merge'] == 'right_only').sum()
        print(f"\n  TASK CHANGES:")
        print(f"    Common tasks: {n_common:,}")
        print(f"    Tasks removed: {n_removed:,}")
        print(f"    Tasks added: {n_added:,}")

        # =========================================================================
        # STEP 4: Calculate SLIPPAGE METRICS for common tasks
        # =========================================================================
        # Focus on tasks that exist in both snapshots for date comparison
        common = merged[merged['_merge'] == 'both'].copy()

        if len(common) == 0:
            print("  No common tasks to compare!")
            return {
                'tasks': pd.DataFrame(),
                'project_metrics': project_metrics,
                'new_tasks': pd.DataFrame()
            }

        # -------------------------------------------------------------------------
        # 4a. Calculate date slippage
        # -------------------------------------------------------------------------
        # finish_slip: How many days later (or earlier) is the task's forecasted finish?
        # Positive = task finishes later now (slipped)
        # Negative = task finishes earlier now (recovered/accelerated)
        common['finish_slip_days'] = (
            common['early_end_date_curr'] - common['early_end_date_prev']
        ).dt.days

        # start_slip: How many days later (or earlier) did the task's start move?
        # This represents delay INHERITED from predecessor tasks
        common['start_slip_days'] = (
            common['early_start_date_curr'] - common['early_start_date_prev']
        ).dt.days

        # -------------------------------------------------------------------------
        # 4a.2. GAP-BASED METRICS for active tasks
        # -------------------------------------------------------------------------
        # For ACTIVE tasks, early_start moves with data_date (P6 scheduling artifact).
        # The start_slip for active tasks includes calendar time between snapshots,
        # which is NOT meaningful for delay attribution.
        #
        # GAP = early_start - data_date = how far the remaining work is pushed out
        #
        # If gap_curr > gap_prev, it means remaining work is pushed further from
        # data_date than before - this is CONSTRAINT GROWTH (inherited delay).
        #
        # Example:
        #   prev: data_date=Feb 9, early_start=Feb 9, gap=0
        #   curr: data_date=Apr 8, early_start=Apr 25, gap=17
        #   constraint_growth = 17 - 0 = 17 days of inherited delay
        if data_date_prev is not None and data_date_curr is not None:
            # Calculate gap: how far is early_start from data_date?
            common['gap_prev_days'] = (
                common['early_start_date_prev'] - data_date_prev
            ).dt.days
            common['gap_curr_days'] = (
                common['early_start_date_curr'] - data_date_curr
            ).dt.days

            # Constraint growth: how much did the gap increase?
            # Positive = remaining work pushed further out (inherited delay)
            common['constraint_growth_days'] = (
                common['gap_curr_days'].fillna(0) - common['gap_prev_days'].fillna(0)
            )

            # For completed tasks in prev, gap has different meaning - set to 0
            common.loc[common['status_code_prev'] == 'TK_Complete', 'gap_prev_days'] = 0
            common.loc[common['status_code_prev'] == 'TK_Complete', 'constraint_growth_days'] = 0
        else:
            # No data_date available - use NaN
            common['gap_prev_days'] = np.nan
            common['gap_curr_days'] = np.nan
            common['constraint_growth_days'] = np.nan

        # -------------------------------------------------------------------------
        # 4b. Decompose into OWN vs INHERITED delay
        # -------------------------------------------------------------------------
        # KEY FORMULA: finish_slip = own_delay + inherited_delay
        #
        # IMPORTANT: The formula differs by task status!
        #
        # For NOT_STARTED tasks (TK_NotStart):
        #   own_delay = finish_slip - start_slip
        #   inherited_delay = start_slip
        #   Rationale: Start can be pushed by predecessors, so start_slip = inherited
        #
        # For ACTIVE tasks (TK_Active in BOTH snapshots):
        #   own_delay = finish_slip  (ALL slip is the task's fault)
        #   inherited_delay = 0
        #   Rationale: Task already started, can't be "pushed" by predecessors.
        #   The early_start moving is just P6 recalculating from data date,
        #   not actual predecessor delay. The task is responsible for ALL its slip.
        #
        # For COMPLETED tasks or tasks that changed status:
        #   Use the standard formula as a reasonable approximation
        #
        # INTERPRETATION:
        #   own_delay > 0: Task took longer than planned (scope growth, slow progress)
        #   own_delay < 0: Task completed faster than planned (acceleration, scope cut)
        #   own_delay = 0: Task duration unchanged; any slip is from predecessors

        # Identify tasks that were ACTIVE in BOTH snapshots (already in progress)
        was_active_both = (
            (common['status_code_prev'] == 'TK_Active') &
            (common['status_code_curr'] == 'TK_Active')
        )

        # Identify tasks that were REOPENED (Complete → Active)
        # These tasks were done, then reopened with new remaining work.
        # For completed tasks, P6 sets early dates to data date. When reopened,
        # dates move to new data date + remaining. The "start_slip" is just
        # calendar time between snapshots, NOT inherited delay.
        # The REAL impact is the remaining duration that now needs completion.
        was_reopened = (
            (common['status_code_prev'] == 'TK_Complete') &
            (common['status_code_curr'] == 'TK_Active')
        )

        # For active tasks: all slip is own delay, no inherited
        # For reopened tasks: all slip is own delay (caused by reopening), no inherited
        # For other tasks: use the standard formula
        common['own_delay_days'] = np.where(
            was_active_both | was_reopened,
            common['finish_slip_days'],  # Active/Reopened: all slip is own delay
            common['finish_slip_days'] - common['start_slip_days']  # Standard formula
        )

        common['inherited_delay_days'] = np.where(
            was_active_both | was_reopened,
            0,  # Active/Reopened: no inherited delay
            common['start_slip_days']  # Standard: start_slip = inherited
        )

        # Flag reopened tasks for visibility
        common['was_reopened'] = was_reopened

        # -------------------------------------------------------------------------
        # 4b.2. FAST-TRACKING DETECTION: Active tasks with incomplete predecessors
        # -------------------------------------------------------------------------
        # An active task CAN still inherit delay if it has incomplete predecessors
        # (e.g., SS or FF relationships where the predecessor hasn't finished).
        # This is "fast-tracking" - the task started but is still constrained.
        #
        # We provide BOTH metrics:
        #   - own_delay_days / inherited_delay_days: Full attribution (active = all own)
        #   - own_delay_adj_days / inherited_delay_adj_days: Adjusted for fast-tracking
        #   - is_fast_tracked: Flag for analyst review

        # Load predecessor relationships for current snapshot
        taskpred_path = self.primavera_dir / 'taskpred.csv'
        has_incomplete_pred = pd.Series(False, index=common.index)

        if taskpred_path.exists():
            taskpred_df = pd.read_csv(taskpred_path)
            rels_curr = taskpred_df[taskpred_df['file_id'] == file_id_curr][
                ['task_id', 'pred_task_id', 'pred_type']
            ].copy()

            # Build mapping: task_id -> status in current snapshot
            # Need to get task_id for each task_code in common
            task_status_curr = self.tasks_df[self.tasks_df['file_id'] == file_id_curr][
                ['task_id', 'task_code', 'status_code']
            ].copy()
            task_id_to_status = task_status_curr.set_index('task_id')['status_code'].to_dict()
            task_code_to_id = task_status_curr.set_index('task_code')['task_id'].to_dict()

            # For each task, check if it has any incomplete predecessors
            def check_incomplete_preds(task_code):
                task_id = task_code_to_id.get(task_code)
                if task_id is None:
                    return False

                # Get predecessors for this task
                preds = rels_curr[rels_curr['task_id'] == task_id]['pred_task_id'].tolist()
                if not preds:
                    return False

                # Check if any predecessor is NOT complete
                for pred_id in preds:
                    pred_status = task_id_to_status.get(pred_id)
                    if pred_status and pred_status != 'TK_Complete':
                        return True
                return False

            # Only check for tasks that were active in both snapshots
            active_task_codes = common[was_active_both]['task_code'].tolist()
            incomplete_pred_flags = {tc: check_incomplete_preds(tc) for tc in active_task_codes}
            has_incomplete_pred = common['task_code'].map(incomplete_pred_flags).fillna(False)

        # is_fast_tracked: Active in both snapshots AND has incomplete predecessors
        common['is_fast_tracked'] = was_active_both & has_incomplete_pred

        # Adjusted metrics for fast-tracked tasks:
        # If fast-tracked, use original formula (can still inherit from predecessors)
        # If not fast-tracked (or not active), use the values we already calculated
        common['own_delay_adj_days'] = np.where(
            common['is_fast_tracked'],
            common['finish_slip_days'] - common['start_slip_days'],  # Original formula
            common['own_delay_days']  # Keep current value
        )

        common['inherited_delay_adj_days'] = np.where(
            common['is_fast_tracked'],
            common['start_slip_days'],  # Can inherit if fast-tracked
            common['inherited_delay_days']  # Keep current value (0 for active)
        )

        # -------------------------------------------------------------------------
        # 4c. Calculate duration and float changes
        # -------------------------------------------------------------------------
        # Duration change: Did the remaining work estimate grow or shrink?
        # NOTE: remain_drtn_hr_cnt decreases as work is done; an increase indicates
        # scope growth or re-estimation.
        common['duration_change_hrs'] = (
            common['remain_drtn_hr_cnt_curr'].fillna(0) -
            common['remain_drtn_hr_cnt_prev'].fillna(0)
        )
        common['duration_change_days'] = common['duration_change_hrs'] / HOURS_PER_WORKDAY

        # -------------------------------------------------------------------------
        # 4c.2. DURATION OVERRUN calculation (for active/completed tasks)
        # -------------------------------------------------------------------------
        # Duration overrun = elapsed_days - target_days
        # This is a backward-looking performance metric:
        # - For active tasks: elapsed = data_date - actual_start_date
        # - For completed tasks: elapsed = actual_end_date - actual_start_date
        #
        # duration_overrun_change = overrun_curr - overrun_prev
        # This tells us how much performance deteriorated/improved between snapshots.

        # Calculate target duration in days (from target_drtn_hr_cnt)
        common['target_duration_prev_days'] = common['target_drtn_hr_cnt_prev'].fillna(0) / HOURS_PER_WORKDAY
        common['target_duration_curr_days'] = common['target_drtn_hr_cnt_curr'].fillna(0) / HOURS_PER_WORKDAY

        # Calculate elapsed duration for previous snapshot
        # For active tasks in prev: elapsed = data_date_prev - actual_start_prev
        # For completed tasks in prev: elapsed = actual_end_prev - actual_start_prev
        # For not started: elapsed = 0
        if data_date_prev is not None:
            common['elapsed_prev_days'] = np.where(
                common['status_code_prev'] == 'TK_Active',
                (data_date_prev - common['act_start_date_prev']).dt.days,
                np.where(
                    common['status_code_prev'] == 'TK_Complete',
                    (common['act_end_date_prev'] - common['act_start_date_prev']).dt.days,
                    0
                )
            )
        else:
            common['elapsed_prev_days'] = np.where(
                common['status_code_prev'] == 'TK_Complete',
                (common['act_end_date_prev'] - common['act_start_date_prev']).dt.days,
                0
            )

        # Calculate elapsed duration for current snapshot
        if data_date_curr is not None:
            common['elapsed_curr_days'] = np.where(
                common['status_code_curr'] == 'TK_Active',
                (data_date_curr - common['act_start_date_curr']).dt.days,
                np.where(
                    common['status_code_curr'] == 'TK_Complete',
                    (common['act_end_date_curr'] - common['act_start_date_curr']).dt.days,
                    0
                )
            )
        else:
            common['elapsed_curr_days'] = np.where(
                common['status_code_curr'] == 'TK_Complete',
                (common['act_end_date_curr'] - common['act_start_date_curr']).dt.days,
                0
            )

        # Duration overrun = elapsed - target (positive = behind schedule)
        common['duration_overrun_prev_days'] = (
            common['elapsed_prev_days'] - common['target_duration_prev_days']
        )
        common['duration_overrun_curr_days'] = (
            common['elapsed_curr_days'] - common['target_duration_curr_days']
        )

        # Duration overrun change: how much did overrun grow/shrink?
        # Positive = performance deteriorated, Negative = caught up
        common['duration_overrun_change_days'] = np.where(
            (common['status_code_curr'].isin(['TK_Active', 'TK_Complete'])),
            common['duration_overrun_curr_days'].fillna(0) - common['duration_overrun_prev_days'].fillna(0),
            0  # Not meaningful for not-started tasks
        )

        # Float change: Did the task's schedule buffer increase or decrease?
        # Negative float_change = task became MORE critical (lost buffer)
        # Positive float_change = task became LESS critical (gained buffer)
        common['float_change_hrs'] = (
            common['total_float_hr_cnt_curr'].fillna(0) -
            common['total_float_hr_cnt_prev'].fillna(0)
        )
        common['float_change_days'] = common['float_change_hrs'] / HOURS_PER_WORKDAY

        # -------------------------------------------------------------------------
        # 4c.5. ENHANCED: Backward pass analysis - late date changes
        # -------------------------------------------------------------------------
        # The forward pass (early dates) shows how predecessors push dates.
        # The backward pass (late dates) shows how successors/project constraints pull dates.
        #
        # Float = Late Finish - Early Finish
        # When float decreases, it's because:
        #   1. Early Finish moved later (forward push) - predecessors or own duration
        #   2. Late Finish moved earlier (backward pull) - successors or project constraint
        #
        # This decomposition identifies whether float loss came from "in front" or "behind"
        common['late_end_change_days'] = (
            common['late_end_date_curr'] - common['late_end_date_prev']
        ).dt.days

        common['late_start_change_days'] = (
            common['late_start_date_curr'] - common['late_start_date_prev']
        ).dt.days

        # Float compression attribution:
        # - float_loss_from_front: Early dates moved later (finish_slip > 0)
        # - float_loss_from_back: Late dates moved earlier (late_end_change < 0)
        common['float_loss_from_front'] = common['finish_slip_days'].clip(lower=0)
        common['float_loss_from_back'] = (-common['late_end_change_days'].fillna(0)).clip(lower=0)

        # Determine primary float driver
        # FORWARD_PUSH: Predecessors or own duration pushed early dates
        # BACKWARD_PULL: Successors or project constraint pulled late dates
        # MIXED: Both directions contributed significantly
        common['float_driver'] = np.where(
            common['float_loss_from_front'] > common['float_loss_from_back'] + 1,  # +1 day tolerance
            'FORWARD_PUSH',
            np.where(
                common['float_loss_from_back'] > common['float_loss_from_front'] + 1,
                'BACKWARD_PULL',
                np.where(
                    (common['float_loss_from_front'] > 0) | (common['float_loss_from_back'] > 0),
                    'MIXED',
                    'NONE'
                )
            )
        )

        # -------------------------------------------------------------------------
        # 4c.6. ENHANCED: Constraint change detection
        # -------------------------------------------------------------------------
        # Detect if task constraints changed between snapshots
        # This helps identify if schedule pressure came from new/tightened constraints
        common['constraint_type_changed'] = (
            common['cstr_type_prev'].fillna('') != common['cstr_type_curr'].fillna('')
        )
        common['constraint_date_changed'] = (
            common['cstr_date_prev'].notna() & common['cstr_date_curr'].notna() &
            (common['cstr_date_prev'] != common['cstr_date_curr'])
        ) | (
            common['cstr_date_prev'].isna() != common['cstr_date_curr'].isna()
        )
        common['constraint_changed'] = (
            common['constraint_type_changed'] | common['constraint_date_changed']
        )
        # Constraint tightened = constraint changed AND (new date is earlier OR new constraint added)
        common['constraint_tightened'] = (
            common['constraint_changed'] &
            (
                (common['cstr_date_curr'].notna() & common['cstr_date_prev'].notna() &
                 (common['cstr_date_curr'] < common['cstr_date_prev'])) |
                (common['cstr_date_curr'].notna() & common['cstr_date_prev'].isna())
            )
        )

        # -------------------------------------------------------------------------
        # 4c.7. ENHANCED: Relationship change detection
        # -------------------------------------------------------------------------
        # Flag tasks that gained new predecessors (logic changes)
        # This helps distinguish inherited delay from predecessor execution issues
        # vs. inherited delay from network restructuring
        common['has_new_predecessors'] = common['task_code'].isin(tasks_with_new_preds)
        common['new_predecessor_count'] = common['task_code'].map(
            lambda x: new_pred_count.get(x, 0)
        )

        # -------------------------------------------------------------------------
        # 4d. Determine criticality status
        # -------------------------------------------------------------------------
        # is_critical: Task currently has zero or negative float
        # Zero float = on critical path (any delay directly delays project)
        # Negative float = task is ALREADY late relative to target dates
        #
        # IMPORTANT: NaN float does NOT mean critical!
        # NaN typically occurs for:
        #   - Completed tasks (float is meaningless once done)
        #   - Milestones (zero duration, no float calculation)
        #   - Tasks with missing data
        # We must exclude NaN from criticality to avoid false positives.
        common['is_critical_curr'] = (
            common['total_float_hr_cnt_curr'].notna() &
            (common['total_float_hr_cnt_curr'] <= 0)
        )
        common['was_critical_prev'] = (
            common['total_float_hr_cnt_prev'].notna() &
            (common['total_float_hr_cnt_prev'] <= 0)
        )

        # Driving path: Task is on the longest path to project completion
        # (More specific than critical path - only one driving path exists)
        common['on_driving_path'] = common['driving_path_flag_curr'] == 'Y'

        # -------------------------------------------------------------------------
        # 4e. Extract task status
        # -------------------------------------------------------------------------
        # P6 status codes:
        #   TK_NotStart: Task has not begun
        #   TK_Active: Task is in progress (has actual start, no actual finish)
        #   TK_Complete: Task is finished (has actual finish)
        common['status_curr'] = common['status_code_curr']
        common['is_in_progress'] = common['status_curr'] == 'TK_Active'
        common['is_completed'] = common['status_curr'] == 'TK_Complete'

        # -------------------------------------------------------------------------
        # 4f. Calculate impact score for ranking
        # -------------------------------------------------------------------------
        # Impact score weights delay by criticality for prioritization.
        # NOTE: Uses ABSOLUTE VALUE of own_delay, so tasks that recovered time
        # also rank high. Filter by own_delay > 0 for delay-only analysis.
        #
        # Formula: impact = |own_delay| × crit_weight × driving_weight
        common['impact_score'] = (
            common['own_delay_days'].abs() *
            np.where(common['is_critical_curr'], CRITICALITY_WEIGHT, 1) *
            np.where(common['on_driving_path'], DRIVING_PATH_WEIGHT, 1)
        )

        # =========================================================================
        # STEP 5: CATEGORIZE tasks by delay contribution type
        # =========================================================================
        # Each task is assigned to one of these categories based on its status
        # and delay metrics. Thresholds are defined as module constants.
        def categorize_task(row):
            """
            Assign a delay category to a task based on status and metrics.

            Categories:
                ACTIVE_DELAYER: In-progress task causing delay (own_delay > threshold AND finish slipped)
                COMPLETED_DELAYER: Finished task that finished late (own_delay > threshold AND finish slipped)
                WAITING_INHERITED: Not-started task with inherited delay
                WAITING_SQUEEZED: Not-started task with significant float erosion
                *_OK: Tasks not causing or experiencing significant delay

            Note on DELAYER logic:
                A task is only a "delayer" if BOTH conditions are true:
                1. own_delay > threshold (task took longer than its duration estimate)
                2. finish_slip > 0 (task's finish date actually moved later)

                This prevents false positives where a task started early, took longer than
                estimated, but still finishes on time or early. Such tasks have positive
                own_delay but negative finish_slip - they're not causing project delay.
            """
            status = row['status_curr']
            own_delay = row['own_delay_days'] if pd.notna(row['own_delay_days']) else 0
            inherited = row['inherited_delay_days'] if pd.notna(row['inherited_delay_days']) else 0
            float_change = row['float_change_days'] if pd.notna(row['float_change_days']) else 0
            finish_slip = row['finish_slip_days'] if pd.notna(row['finish_slip_days']) else 0

            if status == 'TK_Complete':
                # Completed tasks: check if they finished late
                # Must have BOTH own_delay (took longer) AND finish_slip (actually slipped)
                if own_delay > OWN_DELAY_THRESHOLD_DAYS and finish_slip > 0:
                    return 'COMPLETED_DELAYER'
                return 'COMPLETED_OK'
            elif status == 'TK_Active':
                # Active tasks: check if they're running behind
                # Must have BOTH own_delay (taking longer) AND finish_slip (finish date moved later)
                if own_delay > OWN_DELAY_THRESHOLD_DAYS and finish_slip > 0:
                    return 'ACTIVE_DELAYER'
                return 'ACTIVE_OK'
            else:  # TK_NotStart
                # Not-started tasks: check inherited delay and float squeeze
                if inherited > INHERITED_DELAY_THRESHOLD_DAYS:
                    return 'WAITING_INHERITED'
                if float_change < FLOAT_SQUEEZE_THRESHOLD_DAYS:
                    return 'WAITING_SQUEEZED'
                return 'WAITING_OK'

        common['delay_category'] = common.apply(categorize_task, axis=1)

        # -------------------------------------------------------------------------
        # STEP 5.5: ENHANCED categorization with multi-dimensional attribution
        # -------------------------------------------------------------------------
        def categorize_task_enhanced(row):
            """
            Enhanced categorization with full multi-dimensional attribution.

            This provides more granular categories than the original categorize_task()
            by considering backward pass (late date changes), constraint changes,
            and relationship changes.

            Categories:
                CAUSE_DURATION: Task took longer (own_delay > threshold, no significant inherited)
                CAUSE_CONSTRAINT: Task constraint was tightened
                CAUSE_LOGIC_CHANGE: New predecessors added that pushed dates
                INHERITED_FROM_PRED: Pushed by predecessors (start_slip > 0, own_delay ~ 0)
                INHERITED_LOGIC_CHANGE: Inherited delay from new predecessor relationship
                SQUEEZED_FROM_SUCC: Float loss from backward pull (successors/project constraint)
                CAUSE_PLUS_INHERITED: Both own delay and inherited delay
                DUAL_SQUEEZE: Float compressed from both directions
                COMPLETED_OK: Completed without delay impact
                ACTIVE_OK: Active without delay impact
                WAITING_OK: Waiting without delay impact
            """
            status = row['status_curr']
            own_delay = row['own_delay_days'] if pd.notna(row['own_delay_days']) else 0
            inherited = row['inherited_delay_days'] if pd.notna(row['inherited_delay_days']) else 0
            float_change = row['float_change_days'] if pd.notna(row['float_change_days']) else 0
            finish_slip = row['finish_slip_days'] if pd.notna(row['finish_slip_days']) else 0
            float_loss_front = row.get('float_loss_from_front', 0) or 0
            float_loss_back = row.get('float_loss_from_back', 0) or 0
            constraint_tightened = row.get('constraint_tightened', False)
            has_new_preds = row.get('has_new_predecessors', False)

            # Completed tasks - simpler logic
            if status == 'TK_Complete':
                if own_delay > OWN_DELAY_THRESHOLD_DAYS and finish_slip > 0:
                    if constraint_tightened:
                        return 'CAUSE_CONSTRAINT'
                    return 'CAUSE_DURATION'
                return 'COMPLETED_OK'

            # Active tasks - check what's causing the delay
            if status == 'TK_Active':
                if own_delay > OWN_DELAY_THRESHOLD_DAYS and finish_slip > 0:
                    if constraint_tightened:
                        return 'CAUSE_CONSTRAINT'
                    if inherited > OWN_DELAY_THRESHOLD_DAYS:
                        return 'CAUSE_PLUS_INHERITED'
                    return 'CAUSE_DURATION'
                return 'ACTIVE_OK'

            # Not-started tasks - full multi-dimensional analysis
            # Priority order: constraint > logic change > own duration > inherited > squeezed

            # Constraint tightening is a direct cause
            if constraint_tightened and float_change < FLOAT_SQUEEZE_THRESHOLD_DAYS:
                return 'CAUSE_CONSTRAINT'

            # New predecessors causing delay
            if has_new_preds and inherited > INHERITED_DELAY_THRESHOLD_DAYS:
                return 'INHERITED_LOGIC_CHANGE'

            # Check for dual squeeze (both forward and backward pressure)
            if float_loss_front > 1 and float_loss_back > 1:
                return 'DUAL_SQUEEZE'

            # Primarily backward pull (squeezed by successors/project constraint)
            if float_loss_back > float_loss_front + 1 and float_change < FLOAT_SQUEEZE_THRESHOLD_DAYS:
                return 'SQUEEZED_FROM_SUCC'

            # Standard inherited delay
            if inherited > INHERITED_DELAY_THRESHOLD_DAYS:
                return 'INHERITED_FROM_PRED'

            # Float squeeze without clear direction
            if float_change < FLOAT_SQUEEZE_THRESHOLD_DAYS:
                if float_loss_back > float_loss_front:
                    return 'SQUEEZED_FROM_SUCC'
                return 'WAITING_SQUEEZED'

            return 'WAITING_OK'

        common['delay_category_enhanced'] = common.apply(categorize_task_enhanced, axis=1)

        # =========================================================================
        # STEP 6: Build result DataFrame with all calculated metrics
        # =========================================================================
        result = pd.DataFrame({
            # Task identification
            'task_code': common['task_code'],
            'task_name': common['task_name_curr'],
            'status': common['status_curr'],

            # Dates (for verification and drill-down)
            'early_end_prev': common['early_end_date_prev'],
            'early_end_curr': common['early_end_date_curr'],
            'early_start_prev': common['early_start_date_prev'],
            'early_start_curr': common['early_start_date_curr'],

            # Core slippage metrics (the heart of the analysis)
            'finish_slip_days': common['finish_slip_days'],      # Total movement of finish date
            'start_slip_days': common['start_slip_days'],        # Movement of start date (inherited)
            'own_delay_days': common['own_delay_days'],          # Delay caused by THIS task (full attribution for active)
            'inherited_delay_days': common['inherited_delay_days'],  # Delay from predecessors (0 for active)

            # Status transition flags
            'was_reopened': common['was_reopened'],                    # Complete → Active (task was reopened)
            'is_fast_tracked': common['is_fast_tracked'],              # Active with incomplete predecessors

            # Fast-tracking adjusted metrics (for active tasks with incomplete predecessors)
            'own_delay_adj_days': common['own_delay_adj_days'],        # Adjusted own delay (considers fast-tracking)
            'inherited_delay_adj_days': common['inherited_delay_adj_days'],  # Adjusted inherited (non-zero if fast-tracked)

            # Supporting metrics
            'duration_change_days': common['duration_change_days'],  # Scope/estimate change
            'float_change_days': common['float_change_days'],        # Buffer erosion

            # Gap-based metrics (for active tasks)
            'gap_prev_days': common['gap_prev_days'],                # early_start - data_date (prev)
            'gap_curr_days': common['gap_curr_days'],                # early_start - data_date (curr)
            'constraint_growth_days': common['constraint_growth_days'],  # gap_curr - gap_prev (inherited delay for active)

            # Duration overrun metrics (backward-looking performance)
            'elapsed_curr_days': common['elapsed_curr_days'],                    # Actual elapsed time
            'target_duration_curr_days': common['target_duration_curr_days'],    # Target duration
            'duration_overrun_curr_days': common['duration_overrun_curr_days'],  # Current overrun
            'duration_overrun_change_days': common['duration_overrun_change_days'],  # Change in overrun (THIS PERIOD)

            # Remaining duration (useful for active task tracking)
            'remain_duration_curr_days': common['remain_drtn_hr_cnt_curr'] / HOURS_PER_WORKDAY,

            # ENHANCED: Backward pass metrics - late date changes
            'late_end_prev': common['late_end_date_prev'],
            'late_end_curr': common['late_end_date_curr'],
            'late_end_change_days': common['late_end_change_days'],  # Late end movement
            'float_loss_from_front': common['float_loss_from_front'],  # Float lost via forward push
            'float_loss_from_back': common['float_loss_from_back'],    # Float lost via backward pull
            'float_driver': common['float_driver'],  # FORWARD_PUSH, BACKWARD_PULL, MIXED, NONE

            # ENHANCED: Constraint change detection
            'constraint_changed': common['constraint_changed'],
            'constraint_tightened': common['constraint_tightened'],
            'cstr_type_prev': common['cstr_type_prev'],
            'cstr_type_curr': common['cstr_type_curr'],
            'cstr_date_prev': common['cstr_date_prev'],
            'cstr_date_curr': common['cstr_date_curr'],

            # ENHANCED: Relationship change detection
            'has_new_predecessors': common['has_new_predecessors'],
            'new_predecessor_count': common['new_predecessor_count'],

            # Criticality indicators
            'float_curr_days': common['total_float_hr_cnt_curr'] / HOURS_PER_WORKDAY,
            'is_critical': common['is_critical_curr'],           # Currently on critical path
            'became_critical': ~common['was_critical_prev'] & common['is_critical_curr'],  # Newly critical
            'on_driving_path': common['on_driving_path'],        # On the driving path

            # Ranking and categorization
            'impact_score': common['impact_score'],
            'delay_category': common['delay_category'],
            'delay_category_enhanced': common['delay_category_enhanced'],  # ENHANCED: multi-dimensional

            # Reference back to source snapshots
            'file_id_prev': file_id_prev,
            'file_id_curr': file_id_curr,
        })

        # Add schedule dates if provided (for reporting labels)
        if date_prev:
            result['schedule_date_prev'] = date_prev
        if date_curr:
            result['schedule_date_curr'] = date_curr

        # =========================================================================
        # STEP 7: Analyze NEW TASKS added in current snapshot
        # =========================================================================
        # New tasks (right_only in merge) may represent:
        #   - Legitimate new scope (change orders, design evolution)
        #   - Tasks that were always planned but added to the schedule
        #   - Re-coded tasks (old task removed, new code added)
        #
        # We flag new tasks that are on critical/driving path as they may be
        # contributing to schedule extension.
        new_tasks_raw = merged[merged['_merge'] == 'right_only'].copy()

        if len(new_tasks_raw) > 0:
            new_tasks = pd.DataFrame({
                'task_code': new_tasks_raw['task_code'],
                'task_name': new_tasks_raw['task_name_curr'],
                'status': new_tasks_raw['status_code_curr'],
                'early_start': new_tasks_raw['early_start_date_curr'],
                'early_end': new_tasks_raw['early_end_date_curr'],
                'total_float_days': new_tasks_raw['total_float_hr_cnt_curr'] / HOURS_PER_WORKDAY,
                'remain_duration_days': new_tasks_raw['remain_drtn_hr_cnt_curr'] / HOURS_PER_WORKDAY,
                'on_driving_path': new_tasks_raw['driving_path_flag_curr'] == 'Y',
                # NaN float does NOT mean critical - must have actual <=0 value
                'is_critical': (
                    new_tasks_raw['total_float_hr_cnt_curr'].notna() &
                    (new_tasks_raw['total_float_hr_cnt_curr'] <= 0)
                ),
            })

            # Categorize new tasks by criticality
            # NEW_CRITICAL: Added to critical/driving path (potential schedule pushers)
            # NEW_NONCRITICAL: Added but not on critical path (absorbed by float)
            new_tasks['delay_category'] = new_tasks.apply(
                lambda r: 'NEW_CRITICAL' if (r['on_driving_path'] or r['is_critical']) else 'NEW_NONCRITICAL',
                axis=1
            )

            # Calculate potential schedule impact for new critical tasks
            # If a new task is on the driving path, its duration directly extends the schedule
            # NOTE: This is an upper bound; actual impact depends on sequencing
            new_tasks['potential_impact_days'] = np.where(
                new_tasks['on_driving_path'] | new_tasks['is_critical'],
                new_tasks['remain_duration_days'],
                0
            )
        else:
            new_tasks = pd.DataFrame()

        # Print category summary
        print(f"\n  DELAY CATEGORIES (existing tasks):")
        for cat, count in result['delay_category'].value_counts().items():
            print(f"    {cat}: {count:,}")

        if len(new_tasks) > 0:
            print(f"\n  NEW TASK CATEGORIES:")
            for cat, count in new_tasks['delay_category'].value_counts().items():
                print(f"    {cat}: {count:,}")

        return {
            'tasks': result,
            'project_metrics': project_metrics,
            'new_tasks': new_tasks
        }

    def get_top_slippage_contributors(self, comparison_df, top_n=25,
                                       min_own_delay=0, critical_only=False):
        """
        Get top tasks contributing to schedule slippage, ranked by impact score.

        This method filters and ranks tasks to identify the most significant
        contributors to schedule delay. Use this for prioritizing investigation.

        Args:
            comparison_df: The 'tasks' DataFrame from compare_schedules() output
            top_n: Maximum number of tasks to return (default: 25)
            min_own_delay: Filter to tasks with |own_delay| >= this threshold (days)
            critical_only: If True, only include tasks on critical/driving path

        Returns:
            DataFrame with top contributors, sorted by impact_score descending.
            Includes all columns from the input DataFrame.

        Example:
            >>> result = analyzer.analyze_month(2025, 9)
            >>> # Get top 10 tasks causing delay on critical path
            >>> top = analyzer.get_top_slippage_contributors(
            ...     result['tasks'],
            ...     top_n=10,
            ...     min_own_delay=5,
            ...     critical_only=True
            ... )

        Note:
            impact_score uses absolute value of own_delay, so tasks that
            recovered time (negative own_delay) will rank high too.
            Filter by own_delay > 0 if you only want delay-causing tasks:
                top[top['own_delay_days'] > 0]
        """
        df = comparison_df.copy()

        # Apply filters
        if min_own_delay > 0:
            df = df[df['own_delay_days'].abs() >= min_own_delay]

        if critical_only:
            df = df[df['is_critical'] | df['on_driving_path']]

        # Sort by impact score (highest impact first)
        df = df.sort_values('impact_score', ascending=False)

        return df.head(top_n)

    def analyze_parallel_constraints(self, comparison_result, target_task_code=None):
        """
        Analyze near-critical parallel paths that could limit recovery potential.

        When you recover time on a driving path task, parallel paths with less
        float become the new critical path. This method identifies those constraints.

        IMPORTANT: A task only constrains recovery if its late_end is near the
        project finish. Tasks that finish early (even with low float) cannot
        become the project constraint because their path merges before project finish.

        Args:
            comparison_result: Output from compare_schedules() or analyze_month()
            target_task_code: Specific task to analyze (optional, analyzes all driving path if None)

        Returns:
            dict with:
                'constraints': DataFrame with near-critical tasks and their float
                'recovery_caps': dict mapping task_code -> max effective recovery
                'summary': Overall parallel path statistics
        """
        if comparison_result is None:
            return None

        tasks = comparison_result['tasks']
        project_metrics = comparison_result['project_metrics']
        project_slip = project_metrics.get('project_slippage_days', 0)
        project_finish = project_metrics.get('project_finish_curr')

        if tasks is None or len(tasks) == 0:
            return None

        # Get driving path tasks with delay
        driving_delayers = tasks[
            (tasks['on_driving_path']) &
            (tasks['own_delay_days'] > 0)
        ].copy()

        if target_task_code:
            driving_delayers = driving_delayers[driving_delayers['task_code'] == target_task_code]

        if len(driving_delayers) == 0:
            return {'constraints': pd.DataFrame(), 'recovery_caps': {}, 'summary': {}}

        max_delay = driving_delayers['own_delay_days'].max()

        # Calculate late_end for all tasks: late_end = early_end + float
        # A task can only constrain project recovery if its late_end is at or near project finish
        tasks_with_late = tasks.copy()
        tasks_with_late['late_end_curr'] = tasks_with_late['early_end_curr'] + pd.to_timedelta(
            tasks_with_late['float_curr_days'].fillna(0), unit='D'
        )

        # Calculate days between task's late_end and project finish
        if project_finish is not None:
            tasks_with_late['days_before_project_finish'] = (
                project_finish - tasks_with_late['late_end_curr']
            ).dt.days
        else:
            tasks_with_late['days_before_project_finish'] = 0

        # Find potential parallel path constraints:
        # - Not on driving path
        # - Not complete (status != TK_Complete)
        # - Has valid float (not NaN)
        # - Float is positive but less than max driving path delay
        # - CRITICAL FIX: late_end must be near project finish (within max_delay days)
        #   Otherwise the task finishes early and cannot become the new project constraint
        parallel_candidates = tasks_with_late[
            (~tasks_with_late['on_driving_path']) &
            (tasks_with_late['status'] != 'TK_Complete') &
            (tasks_with_late['float_curr_days'].notna()) &
            (tasks_with_late['float_curr_days'] > 0) &
            (tasks_with_late['float_curr_days'] < max_delay) &
            (tasks_with_late['days_before_project_finish'] <= max_delay)  # Key constraint!
        ].copy()

        # Sort by float (lowest first - these become critical soonest)
        parallel_candidates = parallel_candidates.sort_values('float_curr_days')

        # For each driving path delayer, calculate effective recovery cap
        recovery_caps = {}
        constraint_details = []

        for _, dp_row in driving_delayers.iterrows():
            task_code = dp_row['task_code']
            own_delay = dp_row['own_delay_days']

            # Find tasks that would become critical before full recovery
            limiters = parallel_candidates[parallel_candidates['float_curr_days'] < own_delay]

            if len(limiters) > 0:
                # The minimum float caps the recovery
                min_float = limiters['float_curr_days'].min()
                effective_recovery = min(min_float, own_delay, project_slip)
                limiting_count = len(limiters)

                # Get the most constraining tasks (lowest float)
                top_limiters = limiters.head(5)
                for _, lim_row in top_limiters.iterrows():
                    constraint_details.append({
                        'driving_task': task_code,
                        'driving_own_delay': own_delay,
                        'parallel_task': lim_row['task_code'],
                        'parallel_task_name': lim_row['task_name'],
                        'parallel_float_days': lim_row['float_curr_days'],
                        'becomes_critical_after': lim_row['float_curr_days'],
                    })
            else:
                # No parallel constraints found
                effective_recovery = min(own_delay, project_slip)
                limiting_count = 0

            recovery_caps[task_code] = {
                'own_delay': own_delay,
                'uncapped_recovery': min(own_delay, project_slip),
                'effective_recovery': effective_recovery if len(limiters) > 0 else min(own_delay, project_slip),
                'capped_by_parallel': len(limiters) > 0,
                'limiting_task_count': limiting_count,
                'min_parallel_float': limiters['float_curr_days'].min() if len(limiters) > 0 else None,
            }

        constraints_df = pd.DataFrame(constraint_details) if constraint_details else pd.DataFrame()

        # Summary statistics
        summary = {
            'driving_tasks_analyzed': len(driving_delayers),
            'driving_tasks_with_parallel_constraints': sum(1 for v in recovery_caps.values() if v['capped_by_parallel']),
            'total_near_critical_tasks': len(parallel_candidates),
            'min_parallel_float_overall': parallel_candidates['float_curr_days'].min() if len(parallel_candidates) > 0 else None,
        }

        return {
            'constraints': constraints_df,
            'recovery_caps': recovery_caps,
            'summary': summary,
        }
    def analyze_recovery_sequence(self, comparison_result, float_bands=None):
        """
        Analyze the sequence of bottlenecks that limit schedule recovery.

        Shows what tasks become critical at each recovery level, revealing the
        full sequence of constraints that must be addressed to recover the schedule.

        Args:
            comparison_result: Output from compare_schedules() or analyze_month()
            float_bands: List of float thresholds for grouping (default: [0,2,5,10,15,20,30,50])

        Returns:
            dict with:
                'recovery_bands': DataFrame with columns:
                    - float_min, float_max: Band range
                    - task_count: Tasks becoming critical in this band
                    - cumulative_tasks: Total tasks critical up to this band
                    - max_recovery: Max recovery if all tasks in band addressed

                'bottleneck_sequence': DataFrame of near-critical tasks ordered by float:
                    - task_code, task_name, status
                    - float_days: Current float
                    - becomes_critical_at: Recovery level where this becomes bottleneck

                'summary': dict with:
                    - max_driving_delay: The recovery target (max own_delay on driving path)
                    - total_near_critical: Count of tasks that could become bottlenecks
                    - free_recovery: Days recoverable without hitting any bottleneck

        Example:
            >>> result = analyzer.analyze_month(2025, 9)
            >>> seq = analyzer.analyze_recovery_sequence(result)
            >>> print(seq['recovery_bands'])
        """
        if comparison_result is None:
            return None

        tasks = comparison_result['tasks']
        project_metrics = comparison_result['project_metrics']
        project_slip = project_metrics.get('project_slippage_days', 0)

        if tasks is None or len(tasks) == 0:
            return None

        # Get max recovery target from driving path
        driving = tasks[tasks['on_driving_path'] & (tasks['own_delay_days'] > 0)]
        if len(driving) == 0:
            return None

        max_driving_delay = driving['own_delay_days'].max()

        # Find all near-critical tasks (potential bottlenecks)
        near_critical = tasks[
            (~tasks['on_driving_path']) &
            (tasks['status'] != 'TK_Complete') &
            (tasks['float_curr_days'].notna()) &
            (tasks['float_curr_days'] > 0) &
            (tasks['float_curr_days'] < max_driving_delay)
        ].copy()

        near_critical = near_critical.sort_values('float_curr_days')

        # Default float bands
        if float_bands is None:
            float_bands = [0, 2, 5, 10, 15, 20, 30, 50]

        # Filter bands to relevant range
        float_bands = [b for b in float_bands if b <= max_driving_delay + 10]
        if float_bands[-1] < max_driving_delay:
            float_bands.append(int(max_driving_delay) + 1)

        # Calculate recovery bands
        band_rows = []
        cumulative = 0

        for i in range(len(float_bands) - 1):
            low, high = float_bands[i], float_bands[i + 1]
            band_tasks = near_critical[
                (near_critical['float_curr_days'] > low) &
                (near_critical['float_curr_days'] <= high)
            ]
            count = len(band_tasks)
            cumulative += count
            max_recovery = min(high, max_driving_delay, project_slip)

            band_rows.append({
                'float_min': low,
                'float_max': high,
                'task_count': count,
                'cumulative_tasks': cumulative,
                'max_recovery': max_recovery,
            })

        recovery_bands = pd.DataFrame(band_rows)

        # Build bottleneck sequence
        bottleneck_rows = []
        for _, row in near_critical.iterrows():
            bottleneck_rows.append({
                'task_code': row['task_code'],
                'task_name': row['task_name'],
                'status': row['status'],
                'float_days': row['float_curr_days'],
                'becomes_critical_at': row['float_curr_days'],
                'is_critical': row['is_critical'],
            })

        bottleneck_sequence = pd.DataFrame(bottleneck_rows)

        # Calculate free recovery (first bottleneck)
        free_recovery = near_critical['float_curr_days'].min() if len(near_critical) > 0 else max_driving_delay

        summary = {
            'project_slippage_days': project_slip,
            'max_driving_delay': max_driving_delay,
            'total_near_critical': len(near_critical),
            'free_recovery': free_recovery,
        }

        return {
            'recovery_bands': recovery_bands,
            'bottleneck_sequence': bottleneck_sequence,
            'summary': summary,
        }

    def format_recovery_sequence_report(self, sequence_result, top_n=25):
        """
        Format the recovery sequence analysis as a readable report.

        Args:
            sequence_result: Output from analyze_recovery_sequence()
            top_n: Max bottleneck tasks to show (default: 25)

        Returns:
            Formatted string report
        """
        if sequence_result is None:
            return "No recovery sequence data available."

        bands = sequence_result['recovery_bands']
        bottlenecks = sequence_result['bottleneck_sequence']
        summary = sequence_result['summary']

        lines = [
            "=" * 100,
            "RECOVERY SEQUENCE ANALYSIS",
            "=" * 100,
            "",
            "Shows what tasks become bottlenecks at each recovery level.",
            "To recover X days, you must address all tasks with float < X days.",
            "",
            f"Project Slippage: {summary['project_slippage_days']} days",
            f"Max Driving Path Delay: {summary['max_driving_delay']:.0f} days",
            f"Free Recovery (before first bottleneck): {summary['free_recovery']:.1f} days",
            f"Total Near-Critical Tasks: {summary['total_near_critical']}",
            "",
            "-" * 100,
            "RECOVERY BANDS",
            "-" * 100,
            f"{'Recovery Range':<20} {'New Bottlenecks':>15} {'Cumulative':>12} {'Max Recovery':>15}",
            "-" * 100,
        ]

        for _, row in bands.iterrows():
            if row['task_count'] > 0 or row['float_min'] == 0:
                lines.append(
                    f"{row['float_min']:.0f}-{row['float_max']:.0f} days{'':<10} "
                    f"{row['task_count']:>15} "
                    f"{row['cumulative_tasks']:>12} "
                    f"{row['max_recovery']:>12.0f} days"
                )

        lines.extend([
            "",
            "-" * 100,
            "BOTTLENECK SEQUENCE (tasks in order of when they become critical)",
            "-" * 100,
            f"{'Float':>8} {'Task Code':<35} {'Task Name':<55}",
            "-" * 100,
        ])

        for _, row in bottlenecks.head(top_n).iterrows():
            name = str(row['task_name'])[:53] if pd.notna(row['task_name']) else ''
            lines.append(
                f"{row['float_days']:>8.1f} {row['task_code']:<35} {name:<55}"
            )

        if len(bottlenecks) > top_n:
            lines.append(f"  ... and {len(bottlenecks) - top_n} more tasks")

        lines.extend([
            "",
            "-" * 100,
            "INTERPRETATION",
            "-" * 100,
            f"• To recover {summary['free_recovery']:.0f} days: No additional action needed",
        ])

        # Add interpretation for each band
        prev_max = summary['free_recovery']
        for _, row in bands.iterrows():
            if row['task_count'] > 0 and row['float_max'] > prev_max:
                lines.append(
                    f"• To recover {row['max_recovery']:.0f} days: Address {row['cumulative_tasks']} tasks with float < {row['float_max']:.0f} days"
                )
                prev_max = row['max_recovery']

        lines.extend([
            "",
            "=" * 100,
        ])

        return "\n".join(lines)

    def generate_attribution_report(self, comparison_result, top_n=10):
        """
        Generate a comprehensive slippage attribution report that accounts for ALL days of slippage.

        This report is designed to be self-explanatory for analysts collecting documentation.
        It shows:
        1. Full accounting of project slippage (driving path + inherited + offsets = total)
        2. Top N drivers with individual recovery potential and parallel constraints
        3. "Others" category for remaining contributors
        4. Investigation checklist for each driver

        Args:
            comparison_result: Output from compare_schedules() or analyze_month()
            top_n: Number of top drivers to show individually

        Returns:
            Dict with 'report' (formatted string) and 'drivers' (DataFrame)
        """
        if comparison_result is None:
            return {'report': "No data available.", 'drivers': pd.DataFrame()}

        tasks = comparison_result['tasks']
        project_metrics = comparison_result['project_metrics']

        if tasks is None or len(tasks) == 0:
            return {'report': "No tasks to analyze.", 'drivers': pd.DataFrame()}

        project_slippage = project_metrics.get('project_slippage_days', 0)

        # Get driving path tasks
        driving_path = tasks[tasks['on_driving_path'] == True].copy()

        # Calculate slippage components
        # 1. Own delay from driving path tasks
        driving_own_delay = driving_path[driving_path['own_delay_days'] > 0]['own_delay_days'].sum()

        # 2. Tasks finishing early on driving path (negative own_delay = helping)
        driving_early_finish = driving_path[driving_path['own_delay_days'] < 0]['own_delay_days'].sum()

        # 3. Inherited delay at the earliest driving path task
        if len(driving_path) > 0:
            # Find the first driving path task by early_start
            driving_path_sorted = driving_path.sort_values('early_start_curr')
            first_driving_task = driving_path_sorted.iloc[0]
            inherited_at_start = first_driving_task['inherited_delay_days']
            first_task_code = first_driving_task['task_code']
            first_task_name = first_driving_task['task_name']
        else:
            inherited_at_start = 0
            first_task_code = "N/A"
            first_task_name = "N/A"

        # The theoretical sum (may not exactly match due to path complexity)
        theoretical_sum = driving_own_delay + driving_early_finish + inherited_at_start
        accounting_diff = project_slippage - theoretical_sum

        # Get parallel constraints analysis
        whatif_result = self.generate_whatif_table(comparison_result, include_non_driving=True, analyze_parallel=True)
        parallel_constraints = whatif_result.get('parallel_constraints', pd.DataFrame()) if whatif_result else pd.DataFrame()

        # Build drivers table with recovery potential
        drivers = []

        # Get tasks with positive own_delay, sorted by impact
        delay_tasks = tasks[tasks['own_delay_days'] > 0].copy()
        delay_tasks['is_driving'] = delay_tasks['on_driving_path'] == True
        delay_tasks['is_critical'] = delay_tasks['is_critical'] == True

        # Sort: driving path first, then by own_delay
        delay_tasks = delay_tasks.sort_values(
            ['is_driving', 'is_critical', 'own_delay_days'],
            ascending=[False, False, False]
        )

        for _, task in delay_tasks.head(top_n * 2).iterrows():  # Get extra to ensure we have top_n good ones
            task_code = task['task_code']

            # Find parallel constraints for this task
            task_constraints = parallel_constraints[
                parallel_constraints['driving_task'] == task_code
            ] if len(parallel_constraints) > 0 else pd.DataFrame()

            if len(task_constraints) > 0:
                # Get the most limiting constraint
                min_constraint = task_constraints.loc[task_constraints['parallel_float_days'].idxmin()]
                solo_recovery = min(task['own_delay_days'], min_constraint['parallel_float_days'])
                limiting_task = min_constraint['parallel_task']
                limiting_float = min_constraint['parallel_float_days']
            else:
                # No parallel constraint - full recovery possible
                if task['is_driving'] or task['is_critical']:
                    solo_recovery = task['own_delay_days']
                    limiting_task = None
                    limiting_float = None
                else:
                    # Non-critical: recovery limited by float
                    float_curr = task['float_curr_days'] if pd.notna(task['float_curr_days']) else 0
                    solo_recovery = max(0, task['own_delay_days'] - float_curr)
                    limiting_task = "float" if float_curr > 0 else None
                    limiting_float = float_curr

            drivers.append({
                'task_code': task_code,
                'task_name': task['task_name'],
                'own_delay_days': task['own_delay_days'],
                'inherited_delay_days': task['inherited_delay_days'],
                'solo_recovery_days': solo_recovery,
                'limiting_task': limiting_task,
                'limiting_float_days': limiting_float,
                'is_driving_path': task['is_driving'],
                'is_critical': task['is_critical'],
                'status': task['status'],
                'category': task['delay_category'],
            })

        drivers_df = pd.DataFrame(drivers)

        # Separate top N driving path vs others
        driving_drivers = drivers_df[drivers_df['is_driving_path'] == True].head(top_n)
        other_drivers = drivers_df[drivers_df['is_driving_path'] == False].head(top_n)

        # Calculate "others" aggregate
        shown_tasks = set(drivers_df.head(top_n * 2)['task_code'])
        others = delay_tasks[~delay_tasks['task_code'].isin(shown_tasks)].copy()
        others_count = len(others)
        others_own_delay = others['own_delay_days'].sum()

        # Load taxonomy to group "others" by scope_desc
        file_id_curr = tasks['file_id_curr'].iloc[0] if 'file_id_curr' in tasks.columns else None
        others_by_scope = pd.DataFrame()
        if file_id_curr is not None and others_count > 0:
            taxonomy = self._load_taxonomy_for_file(file_id_curr)
            if len(taxonomy) > 0:
                # Join others with taxonomy
                others['scope_desc'] = others['task_code'].map(taxonomy['scope_desc'])
                others['trade_name'] = others['task_code'].map(taxonomy['trade_name'])
                others['scope_desc'] = others['scope_desc'].fillna('(Unknown)')

                # Group by scope_desc
                others_by_scope = others.groupby('scope_desc').agg(
                    task_count=('task_code', 'count'),
                    own_delay_sum=('own_delay_days', 'sum'),
                    own_delay_avg=('own_delay_days', 'mean'),
                ).reset_index()
                others_by_scope = others_by_scope.sort_values('own_delay_sum', ascending=False)

        # Build report
        lines = [
            "=" * 100,
            "SLIPPAGE ATTRIBUTION REPORT - FULL ACCOUNTING",
            "=" * 100,
            "",
            f"Project Slippage: {project_slippage:.0f} days",
            f"Period: {project_metrics.get('date_prev', 'N/A')} → {project_metrics.get('date_curr', 'N/A')}",
            "",
            "-" * 100,
            "SLIPPAGE ACCOUNTING (where did the days come from?)",
            "-" * 100,
            "",
            f"  Driving Path Own Delay (tasks took longer):     {driving_own_delay:+.0f} days",
            f"  Driving Path Early Finishes (tasks helped):     {driving_early_finish:+.0f} days",
            f"  Inherited at Driving Path Start:                {inherited_at_start:+.0f} days",
            f"                                                  ─────────",
            f"  Theoretical Sum:                                {theoretical_sum:+.0f} days",
        ]

        if abs(accounting_diff) > 1:
            lines.extend([
                f"  Path Complexity Adjustment*:                   {accounting_diff:+.0f} days",
                f"                                                  ─────────",
                f"  PROJECT SLIPPAGE:                              {project_slippage:+.0f} days",
                "",
                "  *Adjustment accounts for parallel path merging, calendar differences,",
                "   and network complexity not captured in simple sum.",
            ])
        else:
            lines.extend([
                f"                                                  ═════════",
                f"  PROJECT SLIPPAGE:                              {project_slippage:+.0f} days  ✓",
            ])

        lines.extend([
            "",
            f"  Inherited delay source: {first_task_code}",
            f"    {first_task_name[:80]}",
            "",
        ])

        # Driving path drivers section
        lines.extend([
            "-" * 100,
            "DRIVING PATH DELAY DRIVERS (directly impact project finish)",
            "-" * 100,
            "",
            f"{'#':<3} {'Task Code':<25} {'Own':>6} {'Solo':>6} {'Limiting Task':<25} {'Limit':>6} {'Investigation'}",
            f"{'':3} {'':25} {'Delay':>6} {'Recov':>6} {'':25} {'Float':>6} {''}",
            "-" * 100,
        ])

        for i, (_, row) in enumerate(driving_drivers.iterrows(), 1):
            limiting = row['limiting_task'] if row['limiting_task'] else "—"
            limit_float = f"{row['limiting_float_days']:.0f}d" if pd.notna(row['limiting_float_days']) else "—"

            if row['status'] == 'TK_Active':
                investigate = "← Check progress, remaining work"
            elif row['status'] == 'TK_NotStart':
                investigate = "← Check predecessors, constraints"
            else:
                investigate = "← Review what caused duration growth"

            lines.append(
                f"{i:<3} {row['task_code'][:24]:<25} {row['own_delay_days']:>6.0f} "
                f"{row['solo_recovery_days']:>6.0f} {str(limiting)[:24]:<25} {limit_float:>6} {investigate}"
            )
            lines.append(f"    └─ {row['task_name'][:90]}")

        if len(driving_drivers) == 0:
            lines.append("    (No driving path tasks with own_delay > 0)")

        # Other significant drivers
        if len(other_drivers) > 0:
            lines.extend([
                "",
                "-" * 100,
                "OTHER SIGNIFICANT DELAY DRIVERS (may become critical if parallel paths addressed)",
                "-" * 100,
                "",
                f"{'#':<3} {'Task Code':<25} {'Own':>6} {'Float':>6} {'Project':>8} {'Status':<12} {'Category'}",
                f"{'':3} {'':25} {'Delay':>6} {'Curr':>6} {'Impact':>8} {'':12} {''}",
                "-" * 100,
            ])

            for i, (_, row) in enumerate(other_drivers.head(top_n).iterrows(), 1):
                float_curr = row['limiting_float_days'] if pd.notna(row['limiting_float_days']) else 0
                project_impact = row['solo_recovery_days']

                lines.append(
                    f"{i:<3} {row['task_code'][:24]:<25} {row['own_delay_days']:>6.0f} "
                    f"{float_curr:>6.0f} {project_impact:>8.0f} {row['status'][:12]:<12} {row['category'][:15]}"
                )

        # Others aggregate - grouped by scope_desc
        if others_count > 0:
            lines.extend([
                "",
                "-" * 100,
                f"REMAINING TASKS WITH DELAY BY SCOPE ({others_count} tasks, {others_own_delay:.0f} days total)",
                "-" * 100,
            ])

            if len(others_by_scope) > 0:
                lines.extend([
                    "",
                    f"{'Scope/Trade':<30} {'Tasks':>8} {'Own Delay':>12} {'Avg Delay':>10}",
                    "-" * 65,
                ])
                for _, row in others_by_scope.iterrows():
                    lines.append(
                        f"{row['scope_desc'][:29]:<30} {row['task_count']:>8} "
                        f"{row['own_delay_sum']:>12.0f} {row['own_delay_avg']:>10.1f}"
                    )
                lines.extend([
                    "-" * 65,
                    f"{'TOTAL':<30} {others_count:>8} {others_own_delay:>12.0f}",
                ])
            else:
                lines.extend([
                    f"  Combined own_delay: {others_own_delay:.0f} days",
                    f"  (Taxonomy data not available for scope grouping)",
                ])

            lines.extend([
                "",
                "  Note: These tasks have delay but are not in top drivers.",
                "  Most are absorbed by float or on non-critical paths.",
            ])

        # Investigation checklist
        lines.extend([
            "",
            "=" * 100,
            "INVESTIGATION CHECKLIST",
            "=" * 100,
            "",
            "For each DRIVING PATH driver above, collect:",
            "",
            "  □ Task status in P6 (% complete, remaining duration)",
            "  □ Reason for duration growth (RFI, change order, resource issue?)",
            "  □ Predecessor status (are predecessors complete?)",
            "  □ Weekly report references (was this delay discussed?)",
            "  □ Responsible party (GC, subcontractor, owner, design team?)",
            "",
            "For INHERITED DELAY, trace the chain:",
            "",
            f"  The driving path starts with inherited delay of {inherited_at_start:.0f} days.",
            f"  First task on driving path: {first_task_code}",
            "  → Identify what predecessor(s) pushed this task's start date",
            "  → Check if those predecessors have own_delay or inherited_delay",
            "",
            "For PARALLEL PATH constraints (limiting tasks):",
            "",
            "  These tasks will become critical if you accelerate the driving path.",
            "  → Verify they can maintain their current schedule",
            "  → Consider accelerating them in parallel with driving path work",
            "",
            "=" * 100,
        ])

        return {
            'report': "\n".join(lines),
            'drivers': drivers_df,
            'accounting': {
                'project_slippage': project_slippage,
                'driving_own_delay': driving_own_delay,
                'driving_early_finish': driving_early_finish,
                'inherited_at_start': inherited_at_start,
                'theoretical_sum': theoretical_sum,
                'accounting_diff': accounting_diff,
                'first_driving_task': first_task_code,
            }
        }

    def generate_category_reports(self, comparison_result, top_n=10):
        """
        Generate three separate reports by task category with simplified columns.

        This method produces three distinct analysis views:
        1. NOT STARTED: Tasks that haven't begun - track which could have started but didn't
        2. ACTIVE/COMPLETED (excluding reopened): Measure duration using first snapshot as baseline
        3. REOPENED: Tasks that were completed then reopened

        Each category uses the most relevant columns for that task type and is sorted
        by impact on schedule end date: driving_path DESC, critical DESC, finish_slip DESC.

        Args:
            comparison_result: Output from compare_schedules() or analyze_month()
            top_n: Number of tasks to show per category (default: 10)

        Returns:
            dict with:
                'not_started': DataFrame of top not-started tasks
                'active_completed': DataFrame of top active/completed tasks (excl. reopened)
                'reopened': DataFrame of top reopened tasks
                'report': Formatted text report
                'summary': Summary statistics
        """
        if comparison_result is None:
            return None

        tasks = comparison_result['tasks']
        project_metrics = comparison_result['project_metrics']

        if tasks is None or len(tasks) == 0:
            return None

        # Sort function for all categories: driving_path DESC, critical DESC, finish_slip DESC
        def sort_by_impact(df, sort_col='finish_slip_days'):
            """Sort by driving path first, then critical, then the specified column."""
            if len(df) == 0:
                return df
            df = df.copy()
            return df.sort_values(
                by=['on_driving_path', 'is_critical', sort_col],
                ascending=[False, False, False]
            )

        # =========================================================================
        # CATEGORY 1: NOT STARTED TASKS
        # =========================================================================
        # These tasks haven't begun - track which could have started but didn't
        not_started = tasks[tasks['status'] == 'TK_NotStart'].copy()
        not_started_sorted = sort_by_impact(not_started, 'finish_slip_days')

        # Simplified columns for not-started tasks
        not_started_cols = [
            'task_code', 'task_name',
            'finish_slip_days',       # Schedule impact
            'float_curr_days',        # Current buffer
            'float_change_days',      # Float loss
            'on_driving_path',        # Impact indicator
            'is_critical',            # Impact indicator
        ]
        not_started_report = not_started_sorted.head(top_n)[
            [c for c in not_started_cols if c in not_started_sorted.columns]
        ]

        # =========================================================================
        # CATEGORY 2: ACTIVE/COMPLETED (excluding reopened)
        # =========================================================================
        # Measure performance - duration overrun is the key metric
        active_completed = tasks[
            (tasks['status'].isin(['TK_Active', 'TK_Complete'])) &
            (~tasks['was_reopened'].fillna(False))
        ].copy()
        active_completed_sorted = sort_by_impact(active_completed, 'duration_overrun_change_days')

        # Simplified columns for active/completed tasks
        active_completed_cols = [
            'task_code', 'task_name',
            'duration_overrun_change_days',  # Performance this period
            'finish_slip_days',              # Schedule impact
            'float_curr_days',               # Current buffer
            'float_change_days',             # Float loss
            'on_driving_path',               # Impact indicator
            'is_critical',                   # Impact indicator
        ]
        active_completed_report = active_completed_sorted.head(top_n)[
            [c for c in active_completed_cols if c in active_completed_sorted.columns]
        ]

        # =========================================================================
        # CATEGORY 3: REOPENED TASKS
        # =========================================================================
        # Tasks that were completed then reopened - remaining duration is key
        reopened = tasks[tasks['was_reopened'].fillna(False)].copy()
        reopened_sorted = sort_by_impact(reopened, 'remain_duration_curr_days')

        # Simplified columns for reopened tasks
        reopened_cols = [
            'task_code', 'task_name',
            'remain_duration_curr_days',  # New work to complete
            'finish_slip_days',           # Schedule impact
            'float_curr_days',            # Current buffer
            'float_change_days',          # Float loss
            'on_driving_path',            # Impact indicator
            'is_critical',                # Impact indicator
        ]
        reopened_report = reopened_sorted.head(top_n)[
            [c for c in reopened_cols if c in reopened_sorted.columns]
        ]

        # =========================================================================
        # GENERATE TEXT REPORT
        # =========================================================================
        project_slip = project_metrics.get('project_slippage_days', 0)

        lines = [
            "=" * 100,
            "CATEGORY-BASED DELAY ANALYSIS",
            "=" * 100,
            "",
            f"Project Slippage: {project_slip:+d} days",
            f"Period: {project_metrics.get('date_prev', 'N/A')} → {project_metrics.get('date_curr', 'N/A')}",
            "",
        ]

        # NOT STARTED section
        lines.extend([
            "-" * 100,
            f"1. NOT STARTED TASKS (Top {min(top_n, len(not_started))} of {len(not_started):,})",
            "-" * 100,
            "   Tasks that haven't begun - which could have started but didn't?",
            "",
            f"   {'Task Code':<25} {'Finish':>8} {'Float':>8} {'Δ Float':>8} {'Drv':>5} {'Crit':>5}",
            f"   {'':25} {'Slip':>8} {'Curr':>8} {'':>8} {'':>5} {'':>5}",
            "-" * 100,
        ])
        for _, row in not_started_report.iterrows():
            dp = "Y" if row.get('on_driving_path', False) else ""
            crit = "Y" if row.get('is_critical', False) else ""
            lines.append(
                f"   {row['task_code'][:24]:<25} "
                f"{row.get('finish_slip_days', 0):>8.0f} "
                f"{row.get('float_curr_days', 0):>8.0f} "
                f"{row.get('float_change_days', 0):>8.0f} "
                f"{dp:>5} {crit:>5}"
            )
            lines.append(f"   └─ {row['task_name'][:90]}")

        if len(not_started_report) == 0:
            lines.append("   (No not-started tasks with significant slip)")

        # ACTIVE/COMPLETED section
        lines.extend([
            "",
            "-" * 100,
            f"2. ACTIVE/COMPLETED TASKS (Top {min(top_n, len(active_completed))} of {len(active_completed):,})",
            "-" * 100,
            "   Tasks in progress or finished - sorted by duration overrun change",
            "",
            f"   {'Task Code':<25} {'Overrun':>8} {'Finish':>8} {'Float':>8} {'Δ Float':>8} {'Drv':>5} {'Crit':>5}",
            f"   {'':25} {'Change':>8} {'Slip':>8} {'Curr':>8} {'':>8} {'':>5} {'':>5}",
            "-" * 100,
        ])
        for _, row in active_completed_report.iterrows():
            dp = "Y" if row.get('on_driving_path', False) else ""
            crit = "Y" if row.get('is_critical', False) else ""
            lines.append(
                f"   {row['task_code'][:24]:<25} "
                f"{row.get('duration_overrun_change_days', 0):>8.0f} "
                f"{row.get('finish_slip_days', 0):>8.0f} "
                f"{row.get('float_curr_days', 0):>8.0f} "
                f"{row.get('float_change_days', 0):>8.0f} "
                f"{dp:>5} {crit:>5}"
            )
            lines.append(f"   └─ {row['task_name'][:90]}")

        if len(active_completed_report) == 0:
            lines.append("   (No active/completed tasks with significant slip)")

        # REOPENED section
        lines.extend([
            "",
            "-" * 100,
            f"3. REOPENED TASKS (Top {min(top_n, len(reopened))} of {len(reopened):,})",
            "-" * 100,
            "   Tasks that were completed then reopened - sorted by remaining duration",
            "",
            f"   {'Task Code':<25} {'Remain':>8} {'Finish':>8} {'Float':>8} {'Δ Float':>8} {'Drv':>5} {'Crit':>5}",
            f"   {'':25} {'Dur':>8} {'Slip':>8} {'Curr':>8} {'':>8} {'':>5} {'':>5}",
            "-" * 100,
        ])
        for _, row in reopened_report.iterrows():
            dp = "Y" if row.get('on_driving_path', False) else ""
            crit = "Y" if row.get('is_critical', False) else ""
            lines.append(
                f"   {row['task_code'][:24]:<25} "
                f"{row.get('remain_duration_curr_days', 0):>8.0f} "
                f"{row.get('finish_slip_days', 0):>8.0f} "
                f"{row.get('float_curr_days', 0):>8.0f} "
                f"{row.get('float_change_days', 0):>8.0f} "
                f"{dp:>5} {crit:>5}"
            )
            lines.append(f"   └─ {row['task_name'][:90]}")

        if len(reopened_report) == 0:
            lines.append("   (No reopened tasks)")

        # Comprehensive Legend
        lines.extend([
            "",
            "=" * 100,
            "METRICS LEGEND & ANALYSIS GUIDE",
            "=" * 100,
            "",
            "SCHEDULE IMPACT METRICS (Forward-Looking)",
            "-" * 50,
            "",
            "  Finish Slip (finish_slip_days)",
            "    Formula: early_end[curr] - early_end[prev]",
            "    Meaning: How many days later the task is now forecasted to finish",
            "    Usage:   Primary measure of schedule impact. Positive = slipped, Negative = recovered",
            "",
            "  Float Curr (float_curr_days)",
            "    Formula: (late_end - early_end) in current snapshot",
            "    Meaning: Schedule buffer before this task impacts project end date",
            "    Usage:   ≤0 means task is CRITICAL (on critical path)",
            "",
            "  Δ Float (float_change_days)",
            "    Formula: float[curr] - float[prev]",
            "    Meaning: How much schedule buffer was consumed or gained",
            "    Usage:   Negative = lost buffer (becoming more critical)",
            "",
            "PERFORMANCE METRICS (Backward-Looking) - For Active/Completed Tasks",
            "-" * 50,
            "",
            "  Duration Overrun Change (duration_overrun_change_days)",
            "    Formula: (elapsed - target)[curr] - (elapsed - target)[prev]",
            "    Where:   elapsed = data_date - actual_start (time spent so far)",
            "             target = original planned duration",
            "    Meaning: How much further behind (or ahead) the task fell THIS PERIOD",
            "    Usage:   Best measure of task execution performance",
            "             +17 means task fell 17 days further behind its plan",
            "",
            "  Remain Dur (remain_duration_curr_days)",
            "    Formula: remain_drtn_hr_cnt / 8",
            "    Meaning: Work remaining to complete the task",
            "    Usage:   For reopened tasks, shows new work added back",
            "",
            "GAP-BASED METRICS - For Active Tasks",
            "-" * 50,
            "",
            "  The Problem: For ACTIVE tasks, early_start moves with data_date (P6 artifact).",
            "  Raw start_slip includes calendar time between snapshots, which is NOT delay.",
            "",
            "  Gap (gap_prev_days / gap_curr_days)",
            "    Formula: early_start - data_date",
            "    Meaning: How far out remaining work is scheduled from 'today'",
            "    Example: gap=31 means 'can't start remaining work for 31 days'",
            "",
            "  Constraint Growth (constraint_growth_days)",
            "    Formula: gap[curr] - gap[prev]",
            "    Meaning: TRUE INHERITED DELAY for active tasks",
            "    Usage:   Shows how much predecessors pushed remaining work out",
            "    Example: gap went from 3 to 31 = 28 days of inherited delay",
            "",
            "DELAY ATTRIBUTION",
            "-" * 50,
            "",
            "  For NOT STARTED tasks:",
            "    own_delay = finish_slip - start_slip (task's duration grew)",
            "    inherited = start_slip (pushed by predecessors)",
            "",
            "  For ACTIVE tasks:",
            "    own_delay ≈ duration_overrun_change (execution performance)",
            "    inherited ≈ constraint_growth (predecessor constraints)",
            "    Verify: own_delay + inherited ≈ finish_slip",
            "",
            "  For REOPENED tasks (Complete → Active):",
            "    All slip is 'own delay' - caused by reopening with new work",
            "",
            "CRITICALITY INDICATORS",
            "-" * 50,
            "",
            "  Drv (on_driving_path)",
            "    Meaning: Task is on the DRIVING PATH to project completion",
            "    Impact:  Any delay here DIRECTLY extends project end date",
            "",
            "  Crit (is_critical)",
            "    Meaning: Task has zero or negative float",
            "    Impact:  No buffer - any delay likely impacts downstream",
            "",
            "SORT PRIORITY (for all categories)",
            "-" * 50,
            "  1. Driving path tasks first (highest project impact)",
            "  2. Critical path tasks second",
            "  3. Then by key metric (finish_slip, overrun_change, or remain_dur)",
            "",
            "=" * 100,
        ])

        # Summary statistics
        summary = {
            'not_started_count': len(not_started),
            'active_completed_count': len(active_completed),
            'reopened_count': len(reopened),
            'not_started_driving': len(not_started[not_started['on_driving_path']]) if 'on_driving_path' in not_started.columns else 0,
            'active_completed_driving': len(active_completed[active_completed['on_driving_path']]) if 'on_driving_path' in active_completed.columns else 0,
            'reopened_driving': len(reopened[reopened['on_driving_path']]) if 'on_driving_path' in reopened.columns else 0,
        }

        return {
            'not_started': not_started_report,
            'active_completed': active_completed_report,
            'reopened': reopened_report,
            'report': "\n".join(lines),
            'summary': summary,
        }

    def generate_whatif_table(self, comparison_result, include_non_driving=True, analyze_parallel=True):
        """
        Generate a what-if impact table showing potential schedule recovery.

        For each task with positive own_delay, calculates:
        - How many days the project would recover if that task finished on time
        - The resulting project slip after recovery
        - Parallel path constraints that may limit actual recovery

        Args:
            comparison_result: Output from compare_schedules() or analyze_month()
            include_non_driving: If True, include tasks not on driving path (default: True)
            analyze_parallel: If True, detect parallel path constraints (default: True)

        Returns:
            dict with:
                'whatif_table': DataFrame with columns:
                    - task_code, task_name, status
                    - own_delay_days: Days this task is behind its original duration
                    - on_driving_path: Whether task is on the driving path
                    - is_critical: Whether task has zero/negative float
                    - float_days: Current total float (buffer before impacting project)
                    - uncapped_recovery: Recovery assuming no parallel constraints
                    - parallel_cap: Float of nearest parallel path (limits recovery)
                    - recovery_days: Effective recovery (capped by parallel paths)
                    - new_project_slip: Resulting project slip after recovery
                    - limiting_tasks: Count of parallel tasks that would become critical
                    - confidence: HIGH/HIGH-CAPPED/MEDIUM

                'summary': dict with project-level statistics
                'parallel_constraints': DataFrame with detailed constraint info

        Example:
            >>> result = analyzer.analyze_month(2025, 9)
            >>> whatif = analyzer.generate_whatif_table(result)
            >>> print(whatif['whatif_table'].to_string())

        Confidence Levels:
            - HIGH: Driving/critical path, no parallel constraints detected
            - HIGH-CAPPED: Driving/critical path, but parallel paths limit recovery
            - MEDIUM: Non-critical task, float-based estimate
        """
        if comparison_result is None:
            return None

        tasks = comparison_result['tasks']
        project_metrics = comparison_result['project_metrics']
        project_slip = project_metrics.get('project_slippage_days', 0)

        if tasks is None or len(tasks) == 0 or project_slip is None:
            return None

        # Filter to tasks with positive own_delay (causing delay)
        delaying = tasks[tasks['own_delay_days'] > 0].copy()

        if len(delaying) == 0:
            return {
                'whatif_table': pd.DataFrame(),
                'summary': {
                    'project_slippage_days': project_slip,
                    'max_single_recovery': 0,
                    'total_driving_path_delay': 0,
                    'driving_path_tasks_with_delay': 0
                },
                'parallel_constraints': pd.DataFrame()
            }

        # Analyze parallel constraints if requested
        parallel_analysis = None
        if analyze_parallel:
            parallel_analysis = self.analyze_parallel_constraints(comparison_result)

        # Optionally filter to driving/critical path only
        if not include_non_driving:
            delaying = delaying[delaying['on_driving_path'] | delaying['is_critical']]

        # Calculate recovery for each task
        whatif_rows = []

        for _, row in delaying.iterrows():
            task_code = row['task_code']
            own_delay = row['own_delay_days']
            on_driving = row['on_driving_path']
            is_critical = row['is_critical']
            float_days = row['float_curr_days'] if pd.notna(row['float_curr_days']) else 0

            # Default values for parallel analysis
            parallel_cap = None
            limiting_tasks = 0
            uncapped_recovery = 0

            # Calculate recovery based on path position
            if on_driving or is_critical:
                # Driving/critical path: base recovery is own_delay
                uncapped_recovery = min(own_delay, project_slip)

                # Check for parallel path constraints
                if parallel_analysis and task_code in parallel_analysis['recovery_caps']:
                    caps = parallel_analysis['recovery_caps'][task_code]
                    if caps['capped_by_parallel']:
                        parallel_cap = caps['min_parallel_float']
                        limiting_tasks = caps['limiting_task_count']
                        recovery = min(caps['effective_recovery'], project_slip)
                        confidence = 'HIGH-CAPPED'
                    else:
                        recovery = uncapped_recovery
                        confidence = 'HIGH'
                else:
                    recovery = uncapped_recovery
                    confidence = 'HIGH'
            else:
                # Non-driving path: float absorbs delay first
                uncapped_recovery = 0 if float_days >= own_delay else min(own_delay - float_days, project_slip)
                recovery = uncapped_recovery
                confidence = 'MEDIUM'

            new_slip = project_slip - recovery

            whatif_rows.append({
                'task_code': task_code,
                'task_name': row['task_name'],
                'status': row['status'],
                'own_delay_days': own_delay,
                'on_driving_path': on_driving,
                'is_critical': is_critical,
                'float_days': float_days,
                'uncapped_recovery': uncapped_recovery,
                'parallel_cap': parallel_cap,
                'recovery_days': recovery,
                'new_project_slip': new_slip,
                'limiting_tasks': limiting_tasks,
                'confidence': confidence
            })

        whatif_df = pd.DataFrame(whatif_rows)

        # Sort by recovery potential (highest first)
        whatif_df = whatif_df.sort_values('recovery_days', ascending=False)

        # Calculate summary statistics
        driving_delayers = whatif_df[whatif_df['on_driving_path']]
        capped_tasks = whatif_df[whatif_df['confidence'] == 'HIGH-CAPPED']

        summary = {
            'project_slippage_days': project_slip,
            'max_single_recovery': whatif_df['recovery_days'].max() if len(whatif_df) > 0 else 0,
            'max_uncapped_recovery': whatif_df['uncapped_recovery'].max() if len(whatif_df) > 0 else 0,
            'total_driving_path_delay': driving_delayers['own_delay_days'].sum() if len(driving_delayers) > 0 else 0,
            'driving_path_tasks_with_delay': len(driving_delayers),
            'tasks_with_parallel_constraints': len(capped_tasks),
        }

        result = {
            'whatif_table': whatif_df,
            'summary': summary
        }

        if parallel_analysis:
            result['parallel_constraints'] = parallel_analysis['constraints']

        return result

    def format_whatif_report(self, whatif_result, top_n=20):
        """
        Format the what-if table as a readable text report.

        Args:
            whatif_result: Output from generate_whatif_table()
            top_n: Maximum number of tasks to include (default: 20)

        Returns:
            Formatted string report
        """
        if whatif_result is None:
            return "No what-if data available."

        whatif_df = whatif_result['whatif_table']
        summary = whatif_result['summary']
        parallel_constraints = whatif_result.get('parallel_constraints', pd.DataFrame())

        if len(whatif_df) == 0:
            return "No tasks with positive own_delay found."

        # Check for parallel constraint info
        has_parallel_analysis = 'tasks_with_parallel_constraints' in summary
        tasks_capped = summary.get('tasks_with_parallel_constraints', 0)

        lines = [
            "=" * 100,
            "WHAT-IF IMPACT ANALYSIS",
            "=" * 100,
            "",
            "Shows potential schedule recovery if each delaying task finishes on time.",
            "",
            f"Current Project Slippage: {summary['project_slippage_days']} days",
            f"Max Single-Task Recovery: {summary['max_single_recovery']:.0f} days",
        ]

        if has_parallel_analysis and 'max_uncapped_recovery' in summary:
            if summary['max_uncapped_recovery'] != summary['max_single_recovery']:
                lines.append(f"Max Uncapped Recovery: {summary['max_uncapped_recovery']:.0f} days (before parallel path limits)")

        lines.extend([
            f"Driving Path Tasks with Delay: {summary['driving_path_tasks_with_delay']}",
            f"Total Driving Path Delay: {summary['total_driving_path_delay']:.0f} days",
        ])

        if has_parallel_analysis and tasks_capped > 0:
            lines.append(f"Tasks with Parallel Constraints: {tasks_capped}")

        # Show driving path tasks with parallel constraint info
        lines.extend([
            "",
            "-" * 100,
            "DRIVING PATH TASKS",
            "-" * 100,
        ])

        driving = whatif_df[whatif_df['on_driving_path']].head(top_n)

        # Check if any driving tasks have parallel constraints
        has_capped = 'parallel_cap' in whatif_df.columns and driving['parallel_cap'].notna().any()

        if has_capped:
            lines.append(f"{'Task Code':<35} {'Own Delay':>10} {'Uncapped':>10} {'Par.Limit':>10} {'Recovery':>10} {'New Slip':>10} {'Conf':<12}")
            lines.append("-" * 100)
            for _, row in driving.iterrows():
                par_cap = f"{row['parallel_cap']:.0f}" if pd.notna(row['parallel_cap']) else "-"
                lines.append(
                    f"{row['task_code'][:34]:<35} "
                    f"{row['own_delay_days']:>10.0f} "
                    f"{row['uncapped_recovery']:>10.0f} "
                    f"{par_cap:>10} "
                    f"{row['recovery_days']:>10.0f} "
                    f"{row['new_project_slip']:>10.0f} "
                    f"{row['confidence']:<12}"
                )
        else:
            lines.append(f"{'Task Code':<40} {'Own Delay':>10} {'Recovery':>10} {'New Slip':>10} {'Confidence':<12}")
            lines.append("-" * 100)
            for _, row in driving.iterrows():
                lines.append(
                    f"{row['task_code'][:39]:<40} "
                    f"{row['own_delay_days']:>10.0f} "
                    f"{row['recovery_days']:>10.0f} "
                    f"{row['new_project_slip']:>10.0f} "
                    f"{row['confidence']:<12}"
                )

        if len(driving) == 0:
            lines.append("  (No driving path tasks with delay)")

        # Show parallel constraint details if any tasks are capped
        if has_capped and len(parallel_constraints) > 0:
            lines.extend([
                "",
                "-" * 100,
                "PARALLEL PATH CONSTRAINTS (tasks that would become critical)",
                "-" * 100,
                f"{'Driving Task':<25} {'Parallel Task':<30} {'Par.Float':>10} {'Becomes Critical After':>25}",
                "-" * 100,
            ])
            for _, row in parallel_constraints.head(15).iterrows():
                lines.append(
                    f"{row['driving_task'][:24]:<25} "
                    f"{row['parallel_task'][:29]:<30} "
                    f"{row['parallel_float_days']:>10.0f} "
                    f"{row['becomes_critical_after']:>22.0f} days"
                )

        # Show critical (non-driving) tasks
        critical_non_driving = whatif_df[
            whatif_df['is_critical'] & ~whatif_df['on_driving_path']
        ].head(top_n)

        if len(critical_non_driving) > 0:
            lines.extend([
                "",
                "-" * 100,
                "CRITICAL PATH TASKS - NOT ON DRIVING PATH",
                "-" * 100,
                f"{'Task Code':<40} {'Own Delay':>10} {'Recovery':>10} {'New Slip':>10}",
                "-" * 100,
            ])
            for _, row in critical_non_driving.iterrows():
                lines.append(
                    f"{row['task_code'][:39]:<40} "
                    f"{row['own_delay_days']:>10.0f} "
                    f"{row['recovery_days']:>10.0f} "
                    f"{row['new_project_slip']:>10.0f}"
                )

        # Show non-critical tasks with impact
        non_critical = whatif_df[
            ~whatif_df['is_critical'] & ~whatif_df['on_driving_path'] &
            (whatif_df['recovery_days'] > 0)
        ].head(top_n)

        if len(non_critical) > 0:
            lines.extend([
                "",
                "-" * 100,
                "NON-CRITICAL TASKS WITH PROJECT IMPACT (float exceeded)",
                "-" * 100,
                f"{'Task Code':<35} {'Own Delay':>8} {'Float':>8} {'Recovery':>10} {'New Slip':>10}",
                "-" * 100,
            ])
            for _, row in non_critical.iterrows():
                lines.append(
                    f"{row['task_code'][:34]:<35} "
                    f"{row['own_delay_days']:>8.0f} "
                    f"{row['float_days']:>8.0f} "
                    f"{row['recovery_days']:>10.0f} "
                    f"{row['new_project_slip']:>10.0f}"
                )

        # Show tasks fully absorbed by float (summary only)
        absorbed = whatif_df[
            ~whatif_df['is_critical'] & ~whatif_df['on_driving_path'] &
            (whatif_df['recovery_days'] == 0)
        ]
        if len(absorbed) > 0:
            lines.extend([
                "",
                f"Note: {len(absorbed)} additional tasks have delay absorbed by float (no project impact).",
            ])

        lines.extend([
            "",
            "-" * 100,
            "CONFIDENCE LEVELS",
            "-" * 100,
            "• HIGH: Driving/critical path, no parallel constraints - recovery estimate is reliable",
            "• HIGH-CAPPED: Driving/critical path, but parallel paths limit max recovery",
            "• MEDIUM: Non-critical task, float-based estimate (less certain)",
            "",
            "LIMITATIONS",
            "-" * 90,
            "• Assumes serial driving path - parallel paths may shift after recovery",
            "• Does not account for resource constraints or calendars",
            "• For complex scenarios, re-run P6 with modified task durations",
            "",
            "=" * 90,
        ])

        return "\n".join(lines)

    def analyze_month(self, year, month):
        """
        Analyze schedule slippage for a specific calendar month.

        This is a convenience method that automatically selects appropriate
        schedule snapshots for comparison. It uses:
        - Previous snapshot: Last snapshot BEFORE the month started
        - Current snapshot: Last snapshot DURING the month (or first after if none)

        This selection ensures we capture all schedule changes that occurred
        during the month, including any that happened before the first snapshot
        of the month.

        Args:
            year: Calendar year (e.g., 2025)
            month: Calendar month (1-12, where 1=January)

        Returns:
            dict from compare_schedules() with:
                - 'tasks': DataFrame with task-level slippage metrics
                - 'project_metrics': dict with project-level slippage
                - 'new_tasks': DataFrame with new task analysis
            Returns None if no suitable schedule snapshots exist.

        Example:
            >>> result = analyzer.analyze_month(2025, 9)  # September 2025
            >>> print(f"Project slipped {result['project_metrics']['project_slippage_days']} days")

        Snapshot Selection Logic:
            Month: September 2025
            Available snapshots: [..., Aug 22, Sep 5, Sep 12, Sep 25, Oct 3, ...]

            prev = Aug 22  (last snapshot BEFORE Sep 1)
            curr = Sep 25  (last snapshot IN September)

            If no September snapshots exist:
            curr = Oct 3   (first snapshot AFTER September)
        """
        schedules = self.get_ordered_schedules()

        # Define month boundaries
        month_start = datetime(year, month, 1)
        if month == 12:
            month_end = datetime(year + 1, 1, 1) - timedelta(days=1)
        else:
            month_end = datetime(year, month + 1, 1) - timedelta(days=1)

        # Find snapshots relative to this month
        before_month = schedules[schedules['snapshot_date'] < month_start]
        during_month = schedules[
            (schedules['snapshot_date'] >= month_start) &
            (schedules['snapshot_date'] <= month_end)
        ]

        # Validate we have a "before" snapshot (baseline)
        if len(before_month) == 0:
            print(f"No schedule found before {year}-{month:02d}")
            return None

        # Select current snapshot: prefer last of month, fall back to first after
        if len(during_month) == 0:
            print(f"No schedule found during {year}-{month:02d}")
            after_month = schedules[schedules['snapshot_date'] > month_end]
            if len(after_month) == 0:
                return None
            curr_schedule = after_month.iloc[0]
        else:
            curr_schedule = during_month.iloc[-1]

        # Select previous snapshot: last one before month start
        prev_schedule = before_month.iloc[-1]

        print(f"\nAnalyzing {year}-{month:02d}:")
        print(f"  Previous: {prev_schedule['snapshot_date'].strftime('%Y-%m-%d')} (file_id={prev_schedule['file_id']})")
        print(f"  Current: {curr_schedule['snapshot_date'].strftime('%Y-%m-%d')} (file_id={curr_schedule['file_id']})")

        return self.compare_schedules(
            prev_schedule['file_id'],
            curr_schedule['file_id'],
            prev_schedule['snapshot_date'],
            curr_schedule['snapshot_date']
        )

    def generate_slippage_report(self, comparison_result, output_path=None):
        """
        Generate a formatted slippage report.

        Args:
            comparison_result: Output from compare_schedules() - dict with 'tasks', 'project_metrics', 'new_tasks'
            output_path: Optional path to save report

        Returns:
            Report as string
        """
        if comparison_result is None:
            return "No data available for slippage report."

        # Handle both old (DataFrame) and new (dict) return formats
        if isinstance(comparison_result, dict):
            df = comparison_result['tasks']
            project_metrics = comparison_result['project_metrics']
            new_tasks = comparison_result['new_tasks']
        else:
            df = comparison_result
            project_metrics = {}
            new_tasks = pd.DataFrame()

        if df is None or len(df) == 0:
            return "No data available for slippage report."

        df = df.copy()

        # Summary statistics
        total_tasks = len(df)
        tasks_with_slip = len(df[df['finish_slip_days'] != 0])
        tasks_with_own_delay = len(df[df['own_delay_days'] > 0])
        critical_tasks = len(df[df['is_critical']])
        became_critical = len(df[df['became_critical']])

        # Aggregate slippage
        total_finish_slip = df['finish_slip_days'].sum()
        total_own_delay = df['own_delay_days'].sum()
        avg_own_delay = df[df['own_delay_days'] > 0]['own_delay_days'].mean()

        # Get schedule info
        date_prev = project_metrics.get('date_prev') or (df['schedule_date_prev'].iloc[0] if 'schedule_date_prev' in df.columns else 'N/A')
        date_curr = project_metrics.get('date_curr') or (df['schedule_date_curr'].iloc[0] if 'schedule_date_curr' in df.columns else 'N/A')

        # Category counts
        category_counts = df['delay_category'].value_counts().to_dict() if 'delay_category' in df.columns else {}

        # Build report
        lines = [
            "=" * 80,
            "SCHEDULE SLIPPAGE ATTRIBUTION REPORT",
            "=" * 80,
            "",
            f"Period: {date_prev} → {date_curr}",
        ]

        # Project-level slippage
        if project_metrics:
            lines.extend([
                "",
                "PROJECT-LEVEL SLIPPAGE",
                "-" * 40,
                f"Project finish (prev): {project_metrics.get('project_finish_prev', 'N/A')}",
                f"Project finish (curr): {project_metrics.get('project_finish_curr', 'N/A')}",
                f"PROJECT SLIPPAGE: {project_metrics.get('project_slippage_days', 'N/A')} DAYS",
                f"Driving path tasks: {project_metrics.get('driving_path_tasks_curr', 'N/A'):,}",
            ])

        lines.extend([
            "",
            "TASK SUMMARY",
            "-" * 40,
            f"Total tasks compared: {total_tasks:,}",
            f"Tasks with finish slip: {tasks_with_slip:,} ({100*tasks_with_slip/total_tasks:.1f}%)",
            f"Tasks with own delay: {tasks_with_own_delay:,} ({100*tasks_with_own_delay/total_tasks:.1f}%)",
            f"Critical path tasks: {critical_tasks:,}",
            f"Became critical this period: {became_critical:,}",
        ])

        # Category breakdown
        if category_counts:
            lines.extend([
                "",
                "DELAY CATEGORIES",
                "-" * 40,
            ])
            for cat in ['ACTIVE_DELAYER', 'COMPLETED_DELAYER', 'WAITING_INHERITED', 'WAITING_SQUEEZED',
                        'ACTIVE_OK', 'COMPLETED_OK', 'WAITING_OK']:
                if cat in category_counts:
                    lines.append(f"  {cat}: {category_counts[cat]:,}")

        # New tasks summary
        if len(new_tasks) > 0:
            new_critical = new_tasks[new_tasks['delay_category'] == 'NEW_CRITICAL']
            lines.extend([
                "",
                "NEW TASKS ADDED",
                "-" * 40,
                f"Total new tasks: {len(new_tasks):,}",
                f"New tasks on critical path: {len(new_critical):,}",
            ])
            if len(new_critical) > 0:
                total_impact = new_critical['potential_impact_days'].sum()
                lines.append(f"Total duration of new critical tasks: {total_impact:,.0f} days")

        lines.extend([
            "",
            "AGGREGATE SLIPPAGE",
            "-" * 40,
            f"Total finish slip (all tasks): {total_finish_slip:,.0f} task-days",
            f"Total own delay: {total_own_delay:,.0f} task-days",
            f"Avg own delay (where positive): {avg_own_delay:.1f} days" if pd.notna(avg_own_delay) else "Avg own delay: N/A",
        ])

        # Top contributors by category - ACTIVE_DELAYER first (currently causing delays)
        active_delayers = df[df['delay_category'] == 'ACTIVE_DELAYER'].nlargest(10, 'own_delay_days') if 'delay_category' in df.columns else pd.DataFrame()
        if len(active_delayers) > 0:
            lines.extend([
                "",
                "TOP ACTIVE DELAYERS (in-progress tasks causing delay)",
                "-" * 40,
                f"{'Task Code':<25} {'Own Delay':>10} {'Inherited':>10} {'Status':>12} {'DrivPath':>8}",
                "-" * 70,
            ])
            for _, row in active_delayers.iterrows():
                dp_flag = "YES" if row['on_driving_path'] else ""
                lines.append(
                    f"{row['task_code'][:24]:<25} {row['own_delay_days']:>10.0f} "
                    f"{row['inherited_delay_days']:>10.0f} {row['status'][:12]:>12} {dp_flag:>8}"
                )

        # Completed delayers
        completed_delayers = df[df['delay_category'] == 'COMPLETED_DELAYER'].nlargest(10, 'own_delay_days') if 'delay_category' in df.columns else pd.DataFrame()
        if len(completed_delayers) > 0:
            lines.extend([
                "",
                "TOP COMPLETED DELAYERS (finished late, pushed delay downstream)",
                "-" * 40,
                f"{'Task Code':<25} {'Own Delay':>10} {'Inherited':>10} {'DrivPath':>8}",
                "-" * 60,
            ])
            for _, row in completed_delayers.iterrows():
                dp_flag = "YES" if row['on_driving_path'] else ""
                lines.append(
                    f"{row['task_code'][:24]:<25} {row['own_delay_days']:>10.0f} "
                    f"{row['inherited_delay_days']:>10.0f} {dp_flag:>8}"
                )

        # New critical tasks
        if len(new_tasks) > 0:
            new_critical = new_tasks[new_tasks['delay_category'] == 'NEW_CRITICAL'].nlargest(10, 'potential_impact_days')
            if len(new_critical) > 0:
                lines.extend([
                    "",
                    "NEW CRITICAL PATH TASKS (added this period)",
                    "-" * 40,
                    f"{'Task Code':<25} {'Duration':>10} {'Float':>10} {'Early End':>12}",
                    "-" * 60,
                ])
                for _, row in new_critical.iterrows():
                    early_end_str = row['early_end'].strftime('%Y-%m-%d') if pd.notna(row['early_end']) else 'N/A'
                    lines.append(
                        f"{row['task_code'][:24]:<25} {row['remain_duration_days']:>10.0f} "
                        f"{row['total_float_days']:>10.0f} {early_end_str:>12}"
                    )

        # Top overall by impact score (for reference)
        top_impact = df.nlargest(15, 'impact_score')[
            ['task_code', 'task_name', 'own_delay_days', 'inherited_delay_days',
             'finish_slip_days', 'is_critical', 'on_driving_path', 'delay_category']
        ]
        lines.extend([
            "",
            "TOP 15 BY IMPACT SCORE (weighted: own_delay × criticality × driving_path)",
            "-" * 40,
            f"{'Task Code':<25} {'Own':>8} {'Inherit':>8} {'Total':>8} {'Crit':>6} {'DP':>4} {'Category':<20}",
            "-" * 85,
        ])

        for _, row in top_impact.iterrows():
            crit_flag = "Y" if row['is_critical'] else ""
            dp_flag = "Y" if row['on_driving_path'] else ""
            cat = row.get('delay_category', '')[:18] if 'delay_category' in row else ''
            lines.append(
                f"{row['task_code'][:24]:<25} {row['own_delay_days']:>8.0f} "
                f"{row['inherited_delay_days']:>8.0f} {row['finish_slip_days']:>8.0f} "
                f"{crit_flag:>6} {dp_flag:>4} {cat:<20}"
            )

        lines.extend([
            "",
            "INTERPRETATION",
            "-" * 40,
            "• Own Delay = delay caused by THIS task (finish slip - start slip)",
            "• Inherited = delay pushed from predecessor tasks",
            "• Critical = task on critical path (zero or negative float)",
            "• DrivPath = task is on the driving path to project finish",
            "",
            "CATEGORY DEFINITIONS",
            "-" * 40,
            "• ACTIVE_DELAYER: In-progress task with own_delay > 1 day AND finish_slip > 0",
            "• COMPLETED_DELAYER: Finished task with own_delay > 1 day AND finish_slip > 0",
            "• WAITING_INHERITED: Not-started task with inherited delay > 1 day",
            "• WAITING_SQUEEZED: Not-started task with float erosion > 5 days",
            "• NEW_CRITICAL: New task added to critical/driving path",
            "",
            "=" * 80,
        ])

        report = "\n".join(lines)

        if output_path:
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w') as f:
                f.write(report)
            print(f"\nReport saved to: {output_path}")

        return report


def main():
    parser = argparse.ArgumentParser(description='Analyze schedule slippage between P6 snapshots')
    parser.add_argument('--year', type=int, help='Year to analyze (e.g., 2024)')
    parser.add_argument('--month', type=int, help='Month to analyze (1-12)')
    parser.add_argument('--prev-file', type=int, help='Previous schedule file_id')
    parser.add_argument('--curr-file', type=int, help='Current schedule file_id')
    parser.add_argument('--output', type=str, help='Output path for report')
    parser.add_argument('--list-schedules', action='store_true', help='List available schedules')
    parser.add_argument('--top-n', type=int, default=25, help='Number of top contributors')
    parser.add_argument('--whatif', action='store_true', help='Generate what-if impact analysis table')
    parser.add_argument('--sequence', action='store_true', help='Generate recovery sequence analysis (bottleneck cascade)')
    parser.add_argument('--attribution', action='store_true', help='Generate full slippage attribution report with investigation checklist')
    parser.add_argument('--categories', action='store_true', help='Generate category-based reports (not-started, active/completed, reopened)')

    args = parser.parse_args()

    analyzer = ScheduleSlippageAnalyzer()

    if args.list_schedules:
        schedules = analyzer.get_ordered_schedules()
        print("\nAvailable YATES schedules:")
        print("-" * 60)
        for _, row in schedules.iterrows():
            print(f"  file_id={row['file_id']:2d}  {row['snapshot_date'].strftime('%Y-%m-%d')}  {row['filename'][:40]}")
        return

    # Analyze specific month or file pair
    if args.year and args.month:
        comparison = analyzer.analyze_month(args.year, args.month)
    elif args.prev_file and args.curr_file:
        comparison = analyzer.compare_schedules(args.prev_file, args.curr_file)
    else:
        # Default: analyze most recent month
        schedules = analyzer.get_ordered_schedules()
        if len(schedules) >= 2:
            latest = schedules.iloc[-1]
            prev = schedules.iloc[-2]
            print(f"\nAnalyzing latest schedule pair:")
            print(f"  Previous: {prev['snapshot_date'].strftime('%Y-%m-%d')} (file_id={prev['file_id']})")
            print(f"  Current: {latest['snapshot_date'].strftime('%Y-%m-%d')} (file_id={latest['file_id']})")
            comparison = analyzer.compare_schedules(
                prev['file_id'],
                latest['file_id'],
                prev['snapshot_date'],
                latest['snapshot_date']
            )
        else:
            print("Not enough schedules available for comparison")
            return

    if comparison is not None:
        # Handle new dict format
        tasks_df = comparison['tasks'] if isinstance(comparison, dict) else comparison

        if tasks_df is not None and len(tasks_df) > 0:
            # Generate main slippage report
            report = analyzer.generate_slippage_report(comparison, args.output)
            print("\n" + report)

            # Generate what-if table if requested
            if args.whatif:
                whatif_result = analyzer.generate_whatif_table(comparison)
                if whatif_result:
                    whatif_report = analyzer.format_whatif_report(whatif_result, top_n=args.top_n)
                    print("\n" + whatif_report)

            # Generate recovery sequence analysis if requested
            if args.sequence:
                sequence_result = analyzer.analyze_recovery_sequence(comparison)
                if sequence_result:
                    sequence_report = analyzer.format_recovery_sequence_report(sequence_result, top_n=args.top_n)
                    print("\n" + sequence_report)

            # Generate attribution report if requested
            if args.attribution:
                attribution_result = analyzer.generate_attribution_report(comparison, top_n=args.top_n)
                if attribution_result:
                    print("\n" + attribution_result['report'])

            # Generate category-based reports if requested
            if args.categories:
                category_result = analyzer.generate_category_reports(comparison, top_n=args.top_n)
                if category_result:
                    print("\n" + category_result['report'])

            # Also show top contributors with category
            print("\nTOP SLIPPAGE CONTRIBUTORS (detailed):")
            top = analyzer.get_top_slippage_contributors(tasks_df, top_n=args.top_n)
            cols = ['task_code', 'task_name', 'own_delay_days', 'inherited_delay_days',
                    'duration_change_days', 'float_change_days', 'is_critical', 'impact_score']
            if 'delay_category' in top.columns:
                cols.append('delay_category')
            print(top[cols].to_string())


if __name__ == '__main__':
    main()
