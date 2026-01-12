# Schedule Delay Attribution Methodology

## Overview

This document describes the enhanced delay attribution system implemented in `scripts/integrated_analysis/schedule_slippage_analysis.py`. The system analyzes P6 schedule snapshots to identify which tasks caused schedule delays versus which tasks inherited delays from upstream tasks.

## CPM Theory Background

### Forward Pass (Early Dates)

The forward pass calculates the earliest possible dates for each task:
- **Early Start**: Earliest a task can begin (driven by predecessors)
- **Early Finish**: Earliest a task can complete = Early Start + Duration

When a predecessor delays, it pushes the successor's early start later.

### Backward Pass (Late Dates)

The backward pass calculates the latest allowable dates:
- **Late Finish**: Latest a task can finish without delaying the project
- **Late Start**: Latest a task can start = Late Finish - Duration

When a successor or project constraint tightens, it pulls the late dates earlier.

### Float (Total Slack)

```
Total Float = Late Finish - Early Finish
            = Late Start - Early Start
```

Float represents schedule buffer. When float decreases:
- **Forward push**: Early dates moved later (predecessors delayed or own duration grew)
- **Backward pull**: Late dates moved earlier (successors constrained or project deadline tightened)

## Core Metrics

### Existing Metrics (Forward Pass Only)

| Metric | Formula | Meaning |
|--------|---------|---------|
| `finish_slip_days` | `early_end[curr] - early_end[prev]` | Total movement of task's finish date |
| `start_slip_days` | `early_start[curr] - early_start[prev]` | Movement of task's start date (inherited) |
| `own_delay_days` | `finish_slip - start_slip` | Delay caused by THIS task's duration change |
| `inherited_delay_days` | `start_slip` | Delay pushed from predecessor tasks |

**Key relationship:** `finish_slip = own_delay + inherited_delay`

### Enhanced Metrics (Backward Pass)

| Metric | Formula | Meaning |
|--------|---------|---------|
| `late_end_change_days` | `late_end[curr] - late_end[prev]` | Movement of task's late finish |
| `float_loss_from_front` | `max(0, finish_slip)` | Float consumed by forward push |
| `float_loss_from_back` | `max(0, -late_end_change)` | Float consumed by backward pull |
| `float_driver` | Computed | Primary direction: FORWARD_PUSH, BACKWARD_PULL, MIXED, NONE |

### Constraint Change Metrics

| Metric | Meaning |
|--------|---------|
| `constraint_changed` | Task's constraint type or date was modified |
| `constraint_tightened` | Constraint date moved earlier or new constraint added |

### Relationship Change Metrics

| Metric | Meaning |
|--------|---------|
| `has_new_predecessors` | New predecessor relationships added |
| `new_predecessor_count` | Number of new predecessor relationships |

## Attribution Dimensions

The enhanced system attributes float changes across multiple dimensions:

### 1. Forward Pass (Own vs Inherited)

**Own Duration Change:**
- Task's execution took longer than planned
- Causes: Scope growth, productivity issues, re-estimation

**Inherited from Predecessors:**
- Predecessors pushed this task's start date
- Not this task's "fault" - waiting for upstream work

### 2. Backward Pass (Successor/Project Pressure)

**Squeezed from Successors:**
- Successors or project end constraint pulled late dates earlier
- Float compressed without forward push
- Indicates external schedule pressure

### 3. Network Logic Changes

**New Predecessors:**
- Relationships added that created new dependencies
- Can cause inherited delay without predecessor execution issues

### 4. Constraint Changes

**Constraint Tightened:**
- Task-specific constraint (deadline) was moved earlier
- Direct cause of float compression

## Enhanced Categorization

### Original Categories (Backward Compatible)

| Category | Condition | Meaning |
|----------|-----------|---------|
| ACTIVE_DELAYER | Active, own_delay > 1d, finish_slip > 0 | In-progress task causing delay |
| COMPLETED_DELAYER | Complete, own_delay > 1d, finish_slip > 0 | Finished late |
| WAITING_INHERITED | Not started, inherited > 1d | Delayed by predecessors |
| WAITING_SQUEEZED | Not started, float_change < -5d | Float eroding |
| *_OK | Within thresholds | Not causing delay |

### Enhanced Categories

| Category | Condition | Meaning |
|----------|-----------|---------|
| **CAUSE_DURATION** | own_delay > threshold, minimal inherited | Task took longer |
| **CAUSE_CONSTRAINT** | constraint_tightened, float_change significant | Constraint was tightened |
| **CAUSE_PLUS_INHERITED** | Both own_delay and inherited significant | Mixed causality |
| **INHERITED_FROM_PRED** | inherited > threshold, own_delay minimal | Pushed by predecessors |
| **INHERITED_LOGIC_CHANGE** | has_new_predecessors, inherited > threshold | New predecessor caused delay |
| **SQUEEZED_FROM_SUCC** | float_loss_from_back > front, float decreased | Pulled by successors |
| **DUAL_SQUEEZE** | Both front and back float loss significant | Compressed both directions |
| **COMPLETED_OK** | Complete, no delay | Finished on time |
| **ACTIVE_OK** | Active, no delay | In progress on track |
| **WAITING_OK** | Not started, no impact | Waiting, unaffected |

## Root Cause Tracing Algorithm

The `trace_root_causes()` method identifies the origin of delay chains:

### Algorithm

```
For each task with float decrease:
    1. If own_delay >= 80% of float_decrease:
       → Task is ROOT_CAUSE
    2. Else if constraint_tightened:
       → Task is ROOT_CAUSE (type: CONSTRAINT)
    3. Else if has_new_predecessors:
       → Task is ROOT_CAUSE (type: LOGIC_CHANGE)
    4. Else:
       → Find predecessor with most float decrease
       → Recursively trace upstream
       → Mark task as PROPAGATED from upstream root cause
```

### Cause Types

| Type | Meaning |
|------|---------|
| DURATION | Task's own duration growth caused the delay |
| CONSTRAINT | Constraint tightening caused float loss |
| LOGIC_CHANGE | New predecessor relationship caused delay |
| UNKNOWN | Cause could not be determined |

### Output

| Column | Meaning |
|--------|---------|
| `is_root_cause` | True if this task originated the delay |
| `root_cause_task` | task_code of the originating task |
| `cause_type` | DURATION, CONSTRAINT, LOGIC_CHANGE, UNKNOWN |
| `propagation_depth` | Distance from root cause (0 = root cause itself) |
| `downstream_impact_count` | Number of tasks affected by this root cause |

## Usage

### Basic Analysis

```python
from scripts.integrated_analysis.schedule_slippage_analysis import ScheduleSlippageAnalyzer

analyzer = ScheduleSlippageAnalyzer()
result = analyzer.analyze_month(2024, 6)

# Access enhanced metrics
tasks = result['tasks']
print(tasks[['task_code', 'delay_category', 'delay_category_enhanced', 'float_driver']])
```

### Root Cause Tracing

```python
# Get root causes for tasks with float decrease
root_causes = analyzer.trace_root_causes(result, file_id_curr=result['project_metrics']['file_id_curr'])

# Find the top root causes by downstream impact
top_causes = root_causes[root_causes['is_root_cause']].nlargest(10, 'downstream_impact_count')
print(top_causes[['task_code', 'cause_type', 'downstream_impact_count']])
```

### CLI

```bash
# Analyze a month
python -m scripts.integrated_analysis.schedule_slippage_analysis --year 2024 --month 6

# Include full attribution report
python -m scripts.integrated_analysis.schedule_slippage_analysis --year 2024 --month 6 --attribution
```

## Interpretation Guide

### Float Driver Analysis

| float_driver | Interpretation |
|--------------|----------------|
| FORWARD_PUSH | Delay came from predecessors or task's own duration |
| BACKWARD_PULL | Pressure from successors or project constraint |
| MIXED | Both forward and backward compression |
| NONE | No significant float change |

### Investigation Priority

1. **CAUSE_DURATION** tasks: Investigate why task took longer
2. **CAUSE_CONSTRAINT** tasks: Verify if constraint change was justified
3. **ROOT_CAUSE tasks with high downstream_impact_count**: Most important to address
4. **SQUEEZED_FROM_SUCC** tasks: Check if project deadline is realistic

### Common Patterns

**Single Root Cause, Many Propagated:**
- One task delayed → cascaded through the network
- Focus investigation on the root cause

**Multiple Independent Root Causes:**
- Several tasks contributing to delay independently
- May indicate systemic issues (e.g., resource shortage)

**High SQUEEZED_FROM_SUCC count:**
- Project end constraint is creating pressure
- May need to evaluate overall schedule feasibility

## Limitations

1. **Snapshot Comparison:** Compares two snapshots, not continuous monitoring
2. **No Baseline:** Uses previous snapshot as reference, not original baseline
3. **Calendar Days:** Date differences use calendar days, not working days
4. **Resource Leveling:** Doesn't account for P6 resource leveling effects
5. **Parallel Paths:** Float-based analysis may miss some parallel path effects

## Data Requirements

### Input Columns Required

From `task.csv`:
- `early_start_date`, `early_end_date` - Forward pass dates
- `late_start_date`, `late_end_date` - Backward pass dates
- `total_float_hr_cnt` - Total float in hours
- `remain_drtn_hr_cnt` - Remaining duration
- `status_code` - Task status
- `driving_path_flag` - Driving path indicator
- `cstr_type`, `cstr_date` - Constraint information

From `taskpred.csv`:
- `task_id`, `pred_task_id` - Relationship data
- `pred_type` - Relationship type

## Version History

- **v2.0** (2026-01): Enhanced with backward pass analysis, relationship tracking, constraint detection, root cause tracing
- **v1.0** (Original): Forward pass only, own_delay vs inherited decomposition
