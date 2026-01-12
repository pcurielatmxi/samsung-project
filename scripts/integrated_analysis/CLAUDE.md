# Integrated Analysis

This folder contains scripts and specifications for Phase 2 of the Samsung Taylor FAB1 analysis project: **Cross-Dataset Integration**.

## Purpose

Create dimension tables and mapping tables that enable joining data across the 6 primary sources:
- P6 Primavera (schedules)
- ProjectSight (labor hours)
- Weekly Labor Detail (labor hours)
- TBM Daily Plans (labor hours with location)
- Quality Records - Yates WIR
- Quality Records - SECAI IR

## Key Documents

- [PLAN.md](PLAN.md) - Full specification and implementation plan

## Core Problem

**Hours data lacks location.** Weekly Labor and ProjectSight have company and hours but no building/level. The integration layer solves this by:
1. Normalizing company names across sources
2. Inferring location from company's typical work areas (derived from P6, Quality, TBM)

## Dimension Tables

| Table | Purpose | Key Fields |
|-------|---------|------------|
| `dim_company` | Master company list with aliases | company_id, canonical_name, aliases |
| `dim_location` | Building + Level + Grid bounds | location_id, building, level, grid_row_min/max, grid_col_min/max |
| `dim_trade` | Trade/work type classification | trade_id, trade_code, trade_name |
| `dim_time` | Calendar dimension | date_id, year, month, week |

### Location Model

The location dimension is powered by the **grid-based spatial model** in `scripts/shared/`:

```
┌─────────────────────────────────────────────────────────────────┐
│                       dim_location                               │
│  location_code │ building │ level │ grid_row_min/max │ grid_col_min/max │
├─────────────────────────────────────────────────────────────────┤
│  FAB112345     │ SUE      │ 1F    │ B / E            │ 5 / 12           │
│  ELV-01        │ SUE      │ MULTI │ C / C            │ 8 / 8            │
└─────────────────────────────────────────────────────────────────┘
```

**Key Lookups:**
- **Forward:** Room code → Grid bounds (for P6 tasks)
- **Reverse:** Grid coordinate → Room(s) (for quality data with grids like "G/10")

**Supporting Files:**
- `scripts/shared/location_model.py` - High-level API
- `scripts/shared/gridline_mapping.py` - Low-level grid lookup
- `raw/location_mappings/Samsung_FAB_Codes_by_Gridline_3.xlsx` - Source mapping
- `raw/location_mappings/location_master.csv` - Working location master

## Mapping Tables

| Table | Purpose |
|-------|---------|
| `map_company_aliases` | Resolve source-specific names → company_id |
| `map_company_location` | Company → typical work locations by period |
| `map_location_codes` | Source location formats → location_id |

## Folder Structure

```
scripts/integrated_analysis/
├── PLAN.md              # Detailed specification
├── CLAUDE.md            # This file
├── dimensions/          # Dimension table builders
├── mappings/            # Mapping table builders
└── validate/            # Coverage validation
```

## Output Location

```
data/derived/integrated_analysis/
├── dimensions/          # dim_*.csv files
├── mappings/            # map_*.csv files
└── validation/          # Coverage reports
```

## Usage Notes

1. **Run dimension builders first** - mappings depend on dimensions
2. **Manual review required** - company aliases need human verification
3. **Period-aware** - company→location mappings vary by time period
4. **Confidence flags** - inferred locations should be flagged for transparency

## Integration Granularity

The integration layer operates at **two levels**:

**Building + Level** (primary):
- Sufficient coverage across all sources (66-98%)
- Used for company→location inference and aggregated metrics

**Room + Grid** (when available):
- Quality data (RABA/PSI) often includes grid coordinates
- P6 tasks have room codes with grid bounds (from mapping)
- Enables spatial join: quality inspections ↔ rooms via grid containment

## Data Traceability

Per project guidelines, outputs are classified as **derived** data:
- Includes assumptions and inference
- NOT fully traceable to raw sources
- Company aliases are curated
- Location inference uses statistical distribution

---

## Schedule Slippage Analysis

**Script:** `schedule_slippage_analysis.py`

Analyzes schedule slippage between P6 snapshots to identify which tasks contributed most to project delay.

### Core Concepts

| Metric | Formula | Meaning |
|--------|---------|---------|
| **finish_slip** | `early_end[curr] - early_end[prev]` | How much the task's finish date moved |
| **start_slip** | `early_start[curr] - early_start[prev]` | How much the task's start date moved |
| **own_delay** | Status-dependent (see below) | Delay caused by THIS task |
| **inherited_delay** | Status-dependent (see below) | Delay pushed from predecessor tasks |

**Key insight:** `finish_slip = own_delay + inherited_delay`

**Status-Dependent Calculation (v2.1):**
| Status | own_delay | inherited_delay | Rationale |
|--------|-----------|-----------------|-----------|
| Not Started | `finish_slip - start_slip` | `start_slip` | Start can be pushed by predecessors |
| Active (both snapshots) | `finish_slip` | `0` | Already started - can't be "pushed" |
| Completed | `finish_slip - start_slip` | `start_slip` | Standard formula |

**Fast-Tracking Detection (v2.2):**
| Metric | Meaning |
|--------|---------|
| `is_fast_tracked` | Active task with incomplete predecessors |
| `own_delay_adj_days` | Adjusted own delay (considers predecessor constraints for fast-tracked) |
| `inherited_delay_adj_days` | Adjusted inherited (non-zero for fast-tracked tasks) |

### Task Categories

| Category | Condition | Meaning |
|----------|-----------|---------|
| ACTIVE_DELAYER | Active, own_delay > 1d, finish_slip > 0 | In-progress task causing delay |
| COMPLETED_DELAYER | Complete, own_delay > 1d, finish_slip > 0 | Finished late, pushed successors |
| WAITING_INHERITED | Not started, inherited > 1d | Delayed by predecessors |
| WAITING_SQUEEZED | Not started, float_change < -5d | Buffer eroding |
| *_OK | Within thresholds | Not causing delay |

### Enhanced Attribution (v2.0)

The enhanced attribution system adds multi-dimensional analysis:

**Backward Pass Metrics:**
| Metric | Meaning |
|--------|---------|
| `late_end_change_days` | Movement of late finish date |
| `float_loss_from_front` | Float consumed by forward push |
| `float_loss_from_back` | Float consumed by backward pull |
| `float_driver` | FORWARD_PUSH, BACKWARD_PULL, MIXED, NONE |

**Constraint & Relationship Detection:**
| Metric | Meaning |
|--------|---------|
| `constraint_changed` | Task constraint was modified |
| `constraint_tightened` | Constraint date moved earlier |
| `has_new_predecessors` | New predecessor relationships added |
| `new_predecessor_count` | Number of new predecessors |

**Enhanced Categories (`delay_category_enhanced`):**
| Category | Meaning |
|----------|---------|
| CAUSE_DURATION | Task took longer (own_delay dominant) |
| CAUSE_CONSTRAINT | Constraint was tightened |
| INHERITED_FROM_PRED | Pushed by predecessors |
| INHERITED_LOGIC_CHANGE | New predecessor caused delay |
| SQUEEZED_FROM_SUCC | Pulled by successors/project constraint |
| CAUSE_PLUS_INHERITED | Both own delay and inherited |
| DUAL_SQUEEZE | Compressed from both directions |

**Root Cause Tracing:**
```python
# Trace delay chains to identify root causes
root_causes = analyzer.trace_root_causes(result, file_id_curr)

# Output columns:
# - is_root_cause: True if task originated the delay
# - root_cause_task: task_code of origin
# - cause_type: DURATION, CONSTRAINT, LOGIC_CHANGE
# - propagation_depth: distance from root cause
# - downstream_impact_count: tasks affected
```

**Full Documentation:** See [docs/analysis/delay_attribution_methodology.md](../../docs/analysis/delay_attribution_methodology.md)

### What-If Analysis

The script includes what-if analysis to estimate schedule recovery potential:

**Simple model:**
- Driving path tasks: `recovery ≈ own_delay`
- Non-driving tasks: `recovery = max(0, own_delay - float)`

**Parallel path detection:**
- Identifies near-critical tasks that would become bottlenecks
- Caps recovery estimates based on parallel path float
- Confidence levels: HIGH, HIGH-CAPPED, MEDIUM

**Recovery sequence analysis:**
- Shows all bottlenecks in order of when they become critical
- Groups by float bands (0-2d, 2-5d, 5-10d, etc.)
- Calculates cumulative tasks to address for each recovery level

### Attribution Report

The `--attribution` flag generates a comprehensive report that accounts for ALL days of project slippage:

**Slippage Accounting:**
```
Driving Path Own Delay (tasks took longer):     +31 days
Driving Path Early Finishes (tasks helped):     -5 days
Inherited at Driving Path Start:                +14 days
                                                ─────────
Theoretical Sum:                                +40 days
Path Complexity Adjustment*:                    -4 days
                                                ─────────
PROJECT SLIPPAGE:                              +36 days
```

**Driver Table columns:**
- **Own Delay**: Days the task took longer than planned
- **Solo Recov**: How much project recovers if ONLY this task is accelerated (limited by parallel paths)
- **Limiting Task**: Parallel path task that caps recovery
- **Investigation**: What to look for in documentation

**Remaining Tasks by Scope:** Groups non-top-driver tasks by scope/trade from taxonomy:
```
Scope/Trade                       Tasks    Own Delay  Avg Delay
-----------------------------------------------------------------
Doors & Hardware                    194          384        2.0
Finishes                            142          284        2.0
Fire Protection                      47           93        2.0
...
```

**Investigation Checklist:** Guides analysts on what documents to collect for each driver.

### CLI Usage

```bash
# Basic analysis
python -m scripts.integrated_analysis.schedule_slippage_analysis --year 2025 --month 9

# With what-if analysis
python -m scripts.integrated_analysis.schedule_slippage_analysis --year 2025 --month 9 --whatif

# With recovery sequence (bottleneck cascade)
python -m scripts.integrated_analysis.schedule_slippage_analysis --year 2025 --month 9 --sequence

# With full attribution report (recommended for documentation)
python -m scripts.integrated_analysis.schedule_slippage_analysis --year 2025 --month 9 --attribution

# Full analysis (all reports)
python -m scripts.integrated_analysis.schedule_slippage_analysis --year 2025 --month 9 --whatif --sequence --attribution

# List available schedules
python -m scripts.integrated_analysis.schedule_slippage_analysis --list-schedules

# Analyze specific file pair
python -m scripts.integrated_analysis.schedule_slippage_analysis --prev-file 82 --curr-file 83
```

### Programmatic Usage

```python
from scripts.integrated_analysis.schedule_slippage_analysis import ScheduleSlippageAnalyzer

analyzer = ScheduleSlippageAnalyzer()

# Analyze a month
result = analyzer.analyze_month(2025, 9)
print(f"Project slippage: {result['project_metrics']['project_slippage_days']} days")

# Get what-if table
whatif = analyzer.generate_whatif_table(result)
print(whatif['whatif_table'][['task_code', 'own_delay_days', 'recovery_days', 'confidence']])

# Get recovery sequence
sequence = analyzer.analyze_recovery_sequence(result)
print(sequence['recovery_bands'])  # Shows tasks per float band
print(sequence['bottleneck_sequence'].head(10))  # First 10 bottlenecks

# Get full attribution report (recommended for documentation)
attribution = analyzer.generate_attribution_report(result, top_n=10)
print(attribution['report'])  # Formatted report with investigation checklist
print(attribution['accounting'])  # Dict with slippage breakdown
print(attribution['drivers'])  # DataFrame of top delay drivers
```

### Key Methods

| Method | Purpose |
|--------|---------|
| `analyze_month(year, month)` | Compare snapshots for a calendar month |
| `compare_schedules(prev_id, curr_id)` | Compare two specific snapshots |
| `trace_root_causes(result, file_id)` | **NEW:** Identify root cause tasks in delay chains |
| `generate_whatif_table(result)` | Calculate recovery potential per task |
| `analyze_parallel_constraints(result)` | Detect parallel path bottlenecks |
| `analyze_recovery_sequence(result)` | Full bottleneck cascade analysis |
| `generate_attribution_report(result)` | Full slippage accounting with investigation checklist |
| `generate_slippage_report(result)` | Basic formatted text report |

### Output Structure

```python
result = analyzer.analyze_month(2025, 9)

# result['tasks'] - DataFrame with all task-level metrics
# result['project_metrics'] - dict with project slippage, driving path info
# result['new_tasks'] - DataFrame of tasks added in current snapshot

whatif = analyzer.generate_whatif_table(result)
# whatif['whatif_table'] - DataFrame with recovery estimates
# whatif['summary'] - Project-level recovery stats
# whatif['parallel_constraints'] - DataFrame of limiting parallel paths

sequence = analyzer.analyze_recovery_sequence(result)
# sequence['recovery_bands'] - DataFrame of float bands with task counts
# sequence['bottleneck_sequence'] - DataFrame of all near-critical tasks
# sequence['summary'] - Free recovery, total bottlenecks, etc.

attribution = analyzer.generate_attribution_report(result)
# attribution['report'] - Formatted text report
# attribution['drivers'] - DataFrame of top delay drivers with solo recovery potential
# attribution['accounting'] - Dict with full slippage breakdown:
#   - project_slippage: total days of project slip
#   - driving_own_delay: sum of own_delay from driving path tasks
#   - driving_early_finish: sum of negative own_delay (tasks helping)
#   - inherited_at_start: inherited delay at first driving path task
#   - first_driving_task: task code where inherited delay enters
```

### Limitations

1. **No baseline comparison** - Compares snapshot-to-snapshot, not plan-to-baseline
2. **Driving path changes** - P6's driving path is recalculated each snapshot
3. **Parallel path approximation** - Uses float-based detection, not full network analysis
4. **No resource constraints** - Doesn't account for resource leveling effects
5. **Calendar days** - Date differences use calendar days, not working days
