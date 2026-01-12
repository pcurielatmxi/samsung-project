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
| `start_slip_days` | `early_start[curr] - early_start[prev]` | Movement of task's start date |
| `own_delay_days` | See below | Delay caused by THIS task |
| `inherited_delay_days` | See below | Delay from predecessor tasks |

**Key relationship:** `finish_slip = own_delay + inherited_delay`

#### Status-Dependent Calculation (v2.1)

The formula for `own_delay` and `inherited_delay` differs by task status:

| Status | own_delay | inherited_delay | Rationale |
|--------|-----------|-----------------|-----------|
| **Not Started** | `finish_slip - start_slip` | `start_slip` | Start can be pushed by predecessors |
| **Active (in both snapshots)** | `finish_slip` | `0` | Already started - can't be "pushed" |
| **Completed** | `finish_slip - start_slip` | `start_slip` | Standard formula |

**Why Active tasks are different:**

For an active task, P6 sets `early_start` to approximately the data date (when remaining work can resume). The "start slip" between snapshots is just calendar time passing, NOT predecessor delay. The task is already in progress - its actual start date is fixed.

Example:
- Task A1500 started Nov 13, 2023
- At Mar 22, 2024: early_start = Mar 23 (data date)
- At Apr 8, 2024: early_start = Apr 8 (data date)
- Old method: start_slip = 15 days → "inherited" (WRONG)
- New method: Task is active, so inherited = 0, own_delay = 17 days (CORRECT)

#### Fast-Tracking Detection (v2.2)

**What is fast-tracking?**

Fast-tracking occurs when a task starts before all its predecessors are complete. This is common with:
- Start-to-Start (SS) relationships: Task can start when predecessor starts
- Finish-to-Finish (FF) relationships: Task finish is tied to predecessor finish
- Manual override: Task started despite incomplete predecessor

**Why it matters for attribution:**

For a normal active task (all predecessors complete), `inherited_delay = 0` is correct - the task is in progress and its start is fixed.

But for a **fast-tracked** task (active with incomplete predecessors), the task CAN still be affected by its predecessors. If a predecessor with an SS relationship delays, it may limit how much progress the task can make.

**Fast-tracking detection:**

| Metric | Meaning |
|--------|---------|
| `is_fast_tracked` | True if task is Active AND has at least one incomplete predecessor |

**Adjusted metrics:**

The system provides two sets of metrics for active tasks:

| Column | For Normal Active | For Fast-Tracked | Rationale |
|--------|-------------------|------------------|-----------|
| `own_delay_days` | `finish_slip` | `finish_slip` | Full attribution view |
| `inherited_delay_days` | `0` | `0` | Full attribution view |
| `own_delay_adj_days` | `finish_slip` | `finish_slip - start_slip` | Considers predecessor constraints |
| `inherited_delay_adj_days` | `0` | `start_slip` | Considers predecessor constraints |

**Interpretation:**

- **Normal active task**: Use `own_delay_days`. The task is solely responsible for its delay.
- **Fast-tracked task**: Compare both views:
  - `own_delay_days` shows the task's total slip (conservative accountability view)
  - `own_delay_adj_days` shows slip net of predecessor constraints (nuanced view)

Example:
```
Task: ELEC-FAB-123 (fast-tracked, SS relationship with ROUGH-IN-456)
  finish_slip: +20 days
  start_slip: +8 days

Full attribution:
  own_delay_days: +20 days    (task takes full responsibility)
  inherited_delay_days: 0

Adjusted attribution:
  own_delay_adj_days: +12 days    (task's own contribution)
  inherited_delay_adj_days: +8 days    (from incomplete predecessor)
```

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

## Interpretation Guide for Analysts

This section provides practical guidance for interpreting delay attribution results and drawing actionable conclusions.

### Understanding the Period Summary

When analyzing a period (comparing two schedule snapshots), the key question is: **"Why did the project end date move?"**

The answer falls into one of these patterns:

| Pattern | What It Means | Primary Investigation |
|---------|---------------|----------------------|
| **High FORWARD_PUSH %** | Tasks took longer or predecessors delayed | Look at CAUSE_DURATION tasks - these are the culprits |
| **High BACKWARD_PULL %** | Project deadline tightened or aggressive re-baselining | Check if project end constraint changed; may be schedule compression |
| **High DUAL_SQUEEZE %** | Both happening simultaneously | Complex period - investigate both directions |
| **High LOGIC_CHANGE count** | Many new predecessor relationships added | Schedule restructuring occurred - check if scope or sequencing changed |

### Float Driver Analysis

| float_driver | What Happened | Typical Causes | What to Investigate |
|--------------|---------------|----------------|---------------------|
| **FORWARD_PUSH** | Task's early finish moved later | Duration growth, predecessor delays | Why did the task take longer? Was there rework, scope change, or resource issues? |
| **BACKWARD_PULL** | Task's late finish moved earlier | Project deadline tightened, successor constraints | Was the project end date pulled in? Did a downstream milestone get accelerated? |
| **MIXED** | Both early and late dates moved unfavorably | Compressed from both directions | Task is getting squeezed - check if schedule is still feasible |
| **NONE** | Float didn't change significantly | Task is stable | No immediate concern for this task |

### Drawing Conclusions from Categories

#### CAUSE Categories (Tasks That Drove Delay)

| Category | Conclusion | Investigation Checklist |
|----------|------------|------------------------|
| **CAUSE_DURATION** | This task's actual/planned duration increased, pushing the schedule | ☐ Was there scope growth? ☐ Did productivity drop? ☐ Were there quality issues requiring rework? ☐ Was there resource shortage? |
| **CAUSE_CONSTRAINT** | A constraint on this task was tightened | ☐ Who changed the constraint and why? ☐ Was it a client directive? ☐ Is the new constraint achievable? |
| **CAUSE_PLUS_INHERITED** | Task both caused delay AND received delay from predecessors | ☐ Investigate both the task itself AND its predecessors ☐ May indicate a problem area affecting multiple tasks |

#### INHERITED Categories (Tasks Affected by Others)

| Category | Conclusion | Investigation Checklist |
|----------|------------|------------------------|
| **INHERITED_FROM_PRED** | Task is delayed because upstream work is delayed | ☐ Follow the predecessor chain to find root cause ☐ This task is a victim, not a cause |
| **INHERITED_LOGIC_CHANGE** | New predecessor relationships were added that delayed this task | ☐ Why were new predecessors added? ☐ Was there scope change or sequencing revision? ☐ Were previously parallel tasks made sequential? |
| **SQUEEZED_FROM_SUCC** | Task's float was consumed by downstream pressure | ☐ Check if project end date was moved earlier ☐ Check if successor tasks have hard constraints ☐ May indicate schedule is unrealistic |
| **DUAL_SQUEEZE** | Task compressed from both directions | ☐ High-risk task - investigate urgently ☐ May need schedule relief or additional resources |

### Identifying the "Why" - Root Cause Analysis

The root cause tracing identifies **which tasks originated the delay chain**. Use this hierarchy:

```
Priority 1: Root causes with highest downstream_impact_count
           → These tasks affected the most other tasks
           → Fixing/understanding these has largest benefit

Priority 2: Root causes on the driving path
           → These directly impact project end date
           → May need acceleration or recovery plan

Priority 3: Root causes by cause_type:
           → DURATION: Investigate execution issues
           → CONSTRAINT: Investigate constraint changes
           → LOGIC_CHANGE: Investigate scope/sequencing changes
```

### Period Summary Template

When summarizing a period's schedule movement, use this structure:

```
PERIOD: [Previous Snapshot Date] → [Current Snapshot Date]

PROJECT IMPACT:
- Project end date moved: [+X days / -X days / unchanged]
- From: [Previous date] → To: [Current date]

PRIMARY DRIVER:
- [X]% of tasks show [FORWARD_PUSH/BACKWARD_PULL/MIXED]
- Interpretation: [Schedule slipped due to task delays / Schedule compressed due to deadline pressure / Both]

TOP DELAY CONTRIBUTORS:
1. [Task Code] - [Task Name]
   - Own delay: +X days
   - Downstream impact: Y tasks affected
   - Cause type: [DURATION/CONSTRAINT/LOGIC_CHANGE]
   - Investigation needed: [What to look into]

2. [Task Code] - [Task Name]
   ...

CATEGORY BREAKDOWN:
- CAUSE_DURATION: X tasks (Y total days of own delay)
- INHERITED: X tasks (victims of upstream delays)
- DUAL_SQUEEZE: X tasks (high-risk, compressed both ways)

RECOMMENDED ACTIONS:
1. [Specific investigation or mitigation]
2. [...]
```

### Common Patterns and What They Mean

#### Pattern 1: Single Root Cause, Many Propagated
```
Root causes: 5-10 tasks
Propagated: 500+ tasks
```
**Interpretation:** A small number of tasks delayed, and the impact cascaded through the network.

**Action:** Focus investigation on the few root causes. Fixing these few tasks' issues could prevent hundreds of downstream delays.

**Example:** Foundation work delayed → all superstructure tasks pushed → all finishes pushed

#### Pattern 2: Many Independent Root Causes
```
Root causes: 100+ tasks
Propagated: 200 tasks
```
**Interpretation:** Delays are widespread and independent, not cascading from a few sources.

**Action:** Look for systemic issues: resource shortage, weather, quality problems affecting many areas simultaneously.

**Example:** Labor shortage affecting all trades → many tasks delayed independently

#### Pattern 3: High BACKWARD_PULL Percentage (>60%)
```
Float driver: 75% BACKWARD_PULL
```
**Interpretation:** The schedule is being compressed from the end, not pushed from the beginning. The project deadline moved earlier or was tightened.

**Action:**
- Check if the project end constraint changed
- Evaluate if compression is achievable
- May indicate aggressive re-baselining rather than actual execution delay

**Example:** Client moved substantial completion date earlier → all tasks' late dates pulled in → float consumed

#### Pattern 4: High LOGIC_CHANGE Count
```
INHERITED_LOGIC_CHANGE: 300+ tasks
New predecessors detected: 500+ relationships
```
**Interpretation:** Significant schedule restructuring occurred. Many tasks gained new predecessors.

**Action:**
- Check if scope changed (new work items added as predecessors)
- Check if sequencing was revised (parallel work made sequential)
- This may be legitimate schedule refinement, not delay

**Example:** MEP coordination identified new sequence requirements → tasks that were parallel now sequential

#### Pattern 5: DUAL_SQUEEZE Dominates
```
DUAL_SQUEEZE: 60% of tasks with float decrease
```
**Interpretation:** Tasks are being compressed from both directions - very stressed schedule.

**Action:**
- High-priority review needed
- Schedule may be unrealistic
- Look for tasks that need immediate attention or schedule relief

**Example:** Project delay + deadline pressure = tasks squeezed with no buffer

### Investigation Priorities

When reviewing a period, investigate in this order:

1. **Project-Level First**
   - Did the project end date change?
   - By how much? (project_slippage_days)
   - What's the dominant float_driver pattern?

2. **Root Causes Second**
   - Which tasks are marked is_root_cause = True?
   - Sort by downstream_impact_count
   - Focus on top 5-10 root causes

3. **Category Patterns Third**
   - What's the distribution of enhanced categories?
   - High CAUSE_DURATION → execution problems
   - High LOGIC_CHANGE → schedule restructuring
   - High SQUEEZED → deadline pressure

4. **Specific Tasks Last**
   - For top contributors, pull supporting documentation
   - Weekly reports from that period
   - Quality inspection records
   - Daily plans

### Connecting to Source Documents

For each significant root cause task, collect:

| Document Type | What to Find |
|---------------|--------------|
| **Weekly Reports** | Was this task discussed? Any issues mentioned? |
| **Daily Plans (TBM)** | Was the crew working on this task? Any notes? |
| **Quality Records** | Were there failed inspections requiring rework? |
| **P6 Narratives** | Did the scheduler document the reason for delay? |
| **Change Orders** | Was there scope change affecting this task? |

### Red Flags to Watch For

| Red Flag | What It Indicates |
|----------|-------------------|
| Same root cause appearing in multiple periods | Chronic issue not being addressed |
| DUAL_SQUEEZE count increasing over time | Schedule becoming unrealistic |
| LOGIC_CHANGE without scope change documentation | Undocumented schedule manipulation |
| High own_delay on completed tasks | Work took longer than re-planned (even after updates) |
| Constraint tightened without client directive | Internal pressure that may not be achievable |

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

## Quick Reference Card

### At a Glance: What Caused the Delay?

```
┌─────────────────────────────────────────────────────────────────────┐
│                    DELAY ATTRIBUTION QUICK REFERENCE                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  STEP 1: Check Project Slippage                                     │
│  ─────────────────────────────                                      │
│  project_slippage_days > 0?  → Schedule slipped                     │
│  project_slippage_days < 0?  → Schedule recovered                   │
│  project_slippage_days = 0?  → No change to end date                │
│                                                                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  STEP 2: Check Float Driver Distribution                            │
│  ──────────────────────────────────────                             │
│  Mostly FORWARD_PUSH → Tasks/predecessors delayed (execution issue) │
│  Mostly BACKWARD_PULL → Deadline tightened (schedule compression)   │
│  Mostly MIXED/DUAL → Both directions (stressed schedule)            │
│                                                                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  STEP 3: Find Root Causes                                           │
│  ────────────────────────                                           │
│  Sort by downstream_impact_count (descending)                       │
│  Top 5-10 root causes explain most of the delay                     │
│                                                                     │
│  cause_type:                                                        │
│    DURATION     → Task took longer than planned                     │
│    CONSTRAINT   → Constraint was tightened                          │
│    LOGIC_CHANGE → New predecessors added                            │
│                                                                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  STEP 4: Investigate Top Contributors                               │
│  ───────────────────────────────────                                │
│  For each top root cause:                                           │
│    □ Check weekly reports for that period                           │
│    □ Check quality records for rework                               │
│    □ Check P6 narratives for scheduler notes                        │
│    □ Check for scope changes or change orders                       │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Key Metrics Cheat Sheet

| Metric | Positive Value Means | Negative Value Means |
|--------|---------------------|---------------------|
| `project_slippage_days` | End date moved LATER | End date moved EARLIER |
| `finish_slip_days` | Task finishes LATER | Task finishes EARLIER |
| `own_delay_days` | Task duration INCREASED | Task duration DECREASED |
| `inherited_delay_days` | Pushed by predecessors | Pulled by predecessors |
| `float_change_days` | Float INCREASED (good) | Float DECREASED (bad) |
| `late_end_change_days` | Late date moved LATER | Late date moved EARLIER (squeezed) |

### Category Decision Tree

```
Is float decreasing?
├─ NO → *_OK (no concern)
└─ YES → Check own_delay vs inherited:
    ├─ own_delay > threshold, inherited small
    │   └─ CAUSE_DURATION (task took longer)
    ├─ inherited > threshold, own_delay small
    │   ├─ has_new_predecessors?
    │   │   ├─ YES → INHERITED_LOGIC_CHANGE
    │   │   └─ NO → INHERITED_FROM_PRED
    │   └─ (or check late_end_change)
    │       └─ late_end pulled earlier? → SQUEEZED_FROM_SUCC
    ├─ Both own_delay AND inherited significant
    │   └─ CAUSE_PLUS_INHERITED
    ├─ Both front AND back float loss
    │   └─ DUAL_SQUEEZE
    └─ constraint_tightened?
        └─ CAUSE_CONSTRAINT
```

## Example Analysis: November 2022 Period

This example shows how to interpret results from the Oct 21, 2022 → Nov 7, 2022 period comparison.

### Raw Results

```
PERIOD: 2022-10-21 → 2022-11-07

PROJECT METRICS:
- Project slippage: +23 days
- Previous end: 2023-08-30
- Current end:  2023-09-22
- Tasks compared: 843

FLOAT DRIVER DISTRIBUTION:
- FORWARD_PUSH:  40 tasks (5%)
- BACKWARD_PULL: 628 tasks (75%)
- MIXED:         174 tasks (21%)
- NONE:          1 task

ENHANCED CATEGORY DISTRIBUTION:
- DUAL_SQUEEZE:           442 tasks
- INHERITED_LOGIC_CHANGE: 299 tasks
- WAITING_OK:             84 tasks
- SQUEEZED_FROM_SUCC:     10 tasks
- CAUSE_DURATION:         7 tasks
- ACTIVE_OK:              1 task

ROOT CAUSE ANALYSIS:
- Root cause tasks:  107
- Propagated tasks:  608
- Cause types:
  - LOGIC_CHANGE: 299
  - DURATION:     227
  - UNKNOWN:      188
  - CONSTRAINT:   1

TOP ROOT CAUSE:
CN.SEB2.1020 - "PH1-BLACK BASE (NOW RAT SLAB) - SEB2"
- Downstream impact: 56 tasks
- Cause type: DURATION
```

### Interpretation

**Why did the schedule slip 23 days?**

1. **Dominant pattern is BACKWARD_PULL (75%)** - This is unusual. It indicates the project end date or late dates were moved earlier, compressing float from behind. This could be:
   - Project deadline was tightened
   - Aggressive schedule compression applied
   - Early project phases where targets are being established

2. **High DUAL_SQUEEZE count (442 tasks)** - Many tasks are being squeezed from both directions. Combined with the high BACKWARD_PULL, this suggests the schedule was being compressed to meet aggressive targets while also experiencing some forward push from task delays.

3. **High LOGIC_CHANGE count (299 tasks)** - Significant schedule restructuring occurred. Many new predecessor relationships were added. This is common in early project phases when the schedule logic is being refined.

4. **Top root cause is a duration issue** - CN.SEB2.1020 (Rat Slab at SEB2) caused a +DURATION delay that propagated to 56 downstream tasks. This specific task should be investigated:
   - Was there a design issue?
   - Was there a quality problem?
   - Was there a resource constraint?

**Conclusion for this period:**

This appears to be an early-project schedule refinement period. The high BACKWARD_PULL and LOGIC_CHANGE suggest the schedule was being actively managed and restructured. The 23-day slip is partially due to actual task delays (like CN.SEB2.1020) but also due to schedule logic changes. Investigation should focus on:

1. Why the Rat Slab task (CN.SEB2.1020) delayed
2. What drove the significant schedule restructuring (299 logic changes)
3. Whether the 23-day slip represents actual execution delay or schedule refinement

## Comprehensive Example Analyses

For full worked examples showing the complete analysis workflow from raw metrics to investigation checklists, see:

| Period | Slip | Primary Pattern | Document |
|--------|------|-----------------|----------|
| Feb → Apr 2024 | +108 days | FORWARD_PUSH (execution delays) | [period_analysis_2024-04.md](examples/period_analysis_2024-04.md) |

The April 2024 example demonstrates:
- Step-by-step interpretation of each metric
- Pattern recognition (IMPACT tasks, clustered delays, procurement issues)
- How to build investigation checklists from analysis results
- Accountability assessment framework
- Connecting schedule data to source documents

Use this as a template for analyzing other periods.

## Version History

- **v2.1** (2026-01): Fixed own_delay calculation for active tasks - active tasks now correctly show full finish_slip as own_delay (not split with inherited)
- **v2.0** (2026-01): Enhanced with backward pass analysis, relationship tracking, constraint detection, root cause tracing
- **v1.0** (Original): Forward pass only, own_delay vs inherited decomposition
