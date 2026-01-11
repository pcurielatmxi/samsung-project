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

        # Define columns needed for analysis
        # NOTE: We load all date fields even though we primarily use early_start/end
        # because target dates may be useful for baseline comparison in future
        cols_needed = [
            'file_id', 'task_id', 'task_code', 'task_name', 'status_code',
            'early_start_date', 'early_end_date', 'late_start_date', 'late_end_date',
            'target_start_date', 'target_end_date', 'act_start_date', 'act_end_date',
            'total_float_hr_cnt', 'remain_drtn_hr_cnt', 'driving_path_flag'
        ]

        # Load tasks with date parsing
        # NOTE: This is ~230MB in memory for 470K records
        self.tasks_df = pd.read_csv(
            self.primavera_dir / 'task.csv',
            usecols=cols_needed,
            parse_dates=['early_start_date', 'early_end_date',
                        'late_start_date', 'late_end_date',
                        'target_start_date', 'target_end_date',
                        'act_start_date', 'act_end_date']
        )

        print(f"  Loaded {len(self.tasks_df):,} task records across {self.tasks_df['file_id'].nunique()} schedules")

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
        # 4b. Decompose into OWN vs INHERITED delay
        # -------------------------------------------------------------------------
        # KEY FORMULA: finish_slip = own_delay + inherited_delay
        #
        # own_delay = finish_slip - start_slip
        #   = (end_curr - end_prev) - (start_curr - start_prev)
        #   = (end_curr - start_curr) - (end_prev - start_prev)
        #   = duration_curr - duration_prev  (approximately)
        #
        # This measures how much the task ITSELF contributed to the delay,
        # independent of what its predecessors did.
        #
        # INTERPRETATION:
        #   own_delay > 0: Task took longer than planned (scope growth, slow progress)
        #   own_delay < 0: Task completed faster than planned (acceleration, scope cut)
        #   own_delay = 0: Task duration unchanged; any slip is from predecessors
        common['own_delay_days'] = common['finish_slip_days'] - common['start_slip_days']

        # inherited_delay = start_slip
        # This is the delay pushed onto this task by its predecessors.
        # It's not this task's "fault" - it's waiting for upstream work.
        common['inherited_delay_days'] = common['start_slip_days']

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

        # Float change: Did the task's schedule buffer increase or decrease?
        # Negative float_change = task became MORE critical (lost buffer)
        # Positive float_change = task became LESS critical (gained buffer)
        common['float_change_hrs'] = (
            common['total_float_hr_cnt_curr'].fillna(0) -
            common['total_float_hr_cnt_prev'].fillna(0)
        )
        common['float_change_days'] = common['float_change_hrs'] / HOURS_PER_WORKDAY

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
            'own_delay_days': common['own_delay_days'],          # Delay caused by THIS task
            'inherited_delay_days': common['inherited_delay_days'],  # Delay from predecessors

            # Supporting metrics
            'duration_change_days': common['duration_change_days'],  # Scope/estimate change
            'float_change_days': common['float_change_days'],        # Buffer erosion

            # Criticality indicators
            'float_curr_days': common['total_float_hr_cnt_curr'] / HOURS_PER_WORKDAY,
            'is_critical': common['is_critical_curr'],           # Currently on critical path
            'became_critical': ~common['was_critical_prev'] & common['is_critical_curr'],  # Newly critical
            'on_driving_path': common['on_driving_path'],        # On the driving path

            # Ranking and categorization
            'impact_score': common['impact_score'],
            'delay_category': common['delay_category'],

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

        # Find potential parallel path constraints:
        # - Not on driving path
        # - Not complete (status != TK_Complete)
        # - Has valid float (not NaN)
        # - Float is positive but less than max driving path delay
        max_delay = driving_delayers['own_delay_days'].max()

        parallel_candidates = tasks[
            (~tasks['on_driving_path']) &
            (tasks['status'] != 'TK_Complete') &
            (tasks['float_curr_days'].notna()) &
            (tasks['float_curr_days'] > 0) &
            (tasks['float_curr_days'] < max_delay)
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

    def generate_whatif_table(self, comparison_result, include_non_driving=True, analyze_parallel=True):
        """
        Generate a what-if impact table showing potential schedule recovery.

        For each task with positive own_delay, calculates:
        - How many days the project would recover if that task finished on time
        - The resulting project slip after recovery
        - Parallel path constraints that may limit actual recovery

        This model leverages P6's existing calculations and analyzes near-critical
        parallel paths to estimate realistic recovery potential.

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
