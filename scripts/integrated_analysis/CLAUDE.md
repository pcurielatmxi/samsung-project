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

The location dimension is powered by the **grid-based spatial model**:

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
- `raw/location_mappings/Samsung_FAB_Codes_by_Gridline_3.xlsx` - Source mapping
- `raw/location_mappings/location_master.csv` - Working location master

### Location as Primary Integration Key

**Location is the primary link between datasets.** While company and trade provide useful filters, location is what enables the core analysis question: *"What happened at this place?"*

| Source | Location Data | Integration Path |
|--------|---------------|------------------|
| P6 Schedule | Room codes in task names | Room → Grid bounds |
| RABA/PSI Quality | Grid coordinates (G/10) | Grid → Affected rooms |
| TBM Daily Plans | Building + Level + Grid | Grid → Affected rooms |
| QC Workbooks | Grid coordinates | Grid → Affected rooms |
| ProjectSight | None (inferred from company) | Company → Typical locations |

**CRITICAL: All location processing MUST use the centralized module.**

```python
# CORRECT - use centralized module
from scripts.integrated_analysis.location import enrich_location

loc = enrich_location(building='FAB', level='2F', grid='G/10', source='RABA')

# WRONG - do not duplicate location logic in source scripts
grid_parsed = parse_grid_field(grid_raw)  # Don't do this in source scripts
affected_rooms = get_affected_rooms(...)   # Use enrich_location() instead
```

See `scripts/integrated_analysis/location/CLAUDE.md` for full documentation.

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
├── location/            # ** CENTRALIZED LOCATION PROCESSING **
│   ├── CLAUDE.md        # Location module documentation
│   ├── core/            # Normalizers, grid parsing
│   ├── enrichment/      # enrich_location() function
│   ├── dimension/       # dim_location builders
│   └── validation/      # Coverage checks
├── context/             # Free-form context documents
│   ├── README.md        # Organization and traceability guidelines
│   ├── claims/          # CO and EOT claims
│   │   ├── MASTER_SUMMARY.md
│   │   ├── change_orders/
│   │   └── eot_claims/
│   ├── contracts/       # (Future) Contract documents
│   ├── correspondence/  # (Future) Letters, notices
│   └── experts/         # (Future) Expert reports
├── dimensions/          # Dimension table builders
├── mappings/            # Mapping table builders
└── validate/            # Coverage validation
```

## Context Documents

The `context/` folder contains free-form documentation providing contractual, legal, and procedural context for interpreting quantitative analysis. Documents are organized by category.

See [context/README.md](context/README.md) for:
- **Data traceability guidelines** - How to cite sources vs mark MXI analysis
- **Document quotation guidelines** - How to reference source documents
- **Templates** - Standard formats for different document types

**Current categories:**
- `context/claims/` - Change Orders (CO) and Extension of Time (EOT) claims
  - [MASTER_SUMMARY.md](context/claims/MASTER_SUMMARY.md) - Consolidated view
  - `change_orders/CO-XX_*.md` - Individual CO documentation
  - `eot_claims/EOT-XX_*.md` - Individual EOT documentation

**Finding source materials:**
```bash
python -m scripts.narratives.embeddings search "extension of time" --type eot_claim
python -m scripts.narratives.embeddings search "change order" --author Yates
```

## Output Location

```
data/processed/integrated_analysis/
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

**Status-Dependent Calculation (v2.1, v2.3):**
| Status Transition | own_delay | inherited_delay | Rationale |
|-------------------|-----------|-----------------|-----------|
| Not Started → * | `finish_slip - start_slip` | `start_slip` | Start can be pushed by predecessors |
| Active → Active | `finish_slip` | `0` | Already started - can't be "pushed" |
| Complete → Active | `finish_slip` | `0` | Reopened - all slip from reopening |
| Completed | `finish_slip - start_slip` | `start_slip` | Standard formula |

**Reopened Task Detection (v2.3):**
| Metric | Meaning |
|--------|---------|
| `was_reopened` | Task was Complete in prev, Active in current (rework) |

**Fast-Tracking Detection (v2.2):**
| Metric | Meaning |
|--------|---------|
| `is_fast_tracked` | Active task with incomplete predecessors |
| `own_delay_adj_days` | Adjusted own delay (considers predecessor constraints for fast-tracked) |
| `inherited_delay_adj_days` | Adjusted inherited (non-zero for fast-tracked tasks) |

**Gap-Based Metrics (v2.4):**

For ACTIVE tasks, `early_start` moves with `data_date` (P6 scheduling artifact). The gap-based metrics provide proper attribution by measuring the gap between early_start and data_date.

| Metric | Formula | Meaning |
|--------|---------|---------|
| `gap_prev_days` | `early_start_prev - data_date_prev` | How far remaining work was pushed out (prev) |
| `gap_curr_days` | `early_start_curr - data_date_curr` | How far remaining work is pushed out (curr) |
| `constraint_growth_days` | `gap_curr - gap_prev` | Inherited delay for active tasks |

**Duration Overrun Metrics (v2.4):**

Backward-looking performance metrics for active and completed tasks.

| Metric | Formula | Meaning |
|--------|---------|---------|
| `elapsed_curr_days` | `data_date - actual_start` (active) or `actual_end - actual_start` (complete) | Time spent on task |
| `target_duration_curr_days` | `target_drtn_hr_cnt / 8` | Planned duration |
| `duration_overrun_curr_days` | `elapsed - target` | Current performance (positive = behind schedule) |
| `duration_overrun_change_days` | `overrun_curr - overrun_prev` | Performance change THIS PERIOD |

**Delay Attribution by Task Status:**

| Task Status | own_delay | inherited_delay |
|-------------|-----------|-----------------|
| Not Started | `finish_slip - start_slip` | `start_slip` |
| Active | `≈ duration_overrun_change` | `≈ constraint_growth` |
| Reopened | All slip (caused by reopening) | 0 |

**Note:** The `--categories` report includes a comprehensive "METRICS LEGEND & ANALYSIS GUIDE" that explains all metrics in detail.

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

# With category-based reports (not-started, active/completed, reopened)
python -m scripts.integrated_analysis.schedule_slippage_analysis --year 2025 --month 9 --categories

# With tier-based priority report (simplified ranking by float status)
python -m scripts.integrated_analysis.schedule_slippage_analysis --year 2025 --month 9 --tiers

# Full analysis (all reports)
python -m scripts.integrated_analysis.schedule_slippage_analysis --year 2025 --month 9 --whatif --sequence --attribution --categories --tiers

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

# Get category-based reports (separates by task status)
categories = analyzer.generate_category_reports(result, top_n=10)
print(categories['report'])  # Formatted report
print(categories['not_started'])  # DataFrame of top not-started tasks
print(categories['active_completed'])  # DataFrame of top active/completed tasks
print(categories['reopened'])  # DataFrame of reopened tasks

# Get tier-based priority report (simplified ranking)
tiers = analyzer.generate_tier_report(result, top_n=15)
print(tiers['report'])  # Formatted report
print(tiers['driving'])  # DataFrame of DRIVING tier tasks (direct project impact)
print(tiers['critical'])  # DataFrame of CRITICAL tier tasks (parallel paths)
print(tiers['near_critical'])  # DataFrame of NEAR_CRITICAL tier tasks (at risk)
print(tiers['eroding'])  # DataFrame of ERODING tier tasks (losing float)
print(tiers['buffered_summary'])  # Summary dict for BUFFERED tier
print(tiers['summary'])  # Tier counts and totals
```

### Key Methods

| Method | Purpose |
|--------|---------|
| `analyze_month(year, month)` | Compare snapshots for a calendar month |
| `compare_schedules(prev_id, curr_id)` | Compare two specific snapshots |
| `trace_root_causes(result, file_id)` | Identify root cause tasks in delay chains |
| `generate_whatif_table(result)` | Calculate recovery potential per task |
| `analyze_parallel_constraints(result)` | Detect parallel path bottlenecks |
| `analyze_recovery_sequence(result)` | Full bottleneck cascade analysis |
| `generate_attribution_report(result)` | Full slippage accounting with investigation checklist |
| `generate_category_reports(result)` | **NEW (v2.4):** Category-based analysis (not-started, active/completed, reopened) |
| `generate_tier_report(result)` | **NEW (v2.5):** Tier-based priority ranking (DRIVING, CRITICAL, NEAR_CRITICAL, ERODING, BUFFERED) |
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

categories = analyzer.generate_category_reports(result)
# categories['report'] - Formatted text report
# categories['not_started'] - DataFrame: task_code, task_name, finish_slip, float_curr, float_change, driving_path, critical
# categories['active_completed'] - DataFrame: task_code, task_name, duration_overrun_change, finish_slip, float_curr, float_change, driving_path, critical
# categories['reopened'] - DataFrame: task_code, task_name, remain_duration_curr, finish_slip, float_curr, float_change, driving_path, critical
# categories['summary'] - Dict with counts by category

# NEW result['tasks'] columns (v2.4):
#   - gap_prev_days, gap_curr_days, constraint_growth_days: Gap-based metrics
#   - elapsed_curr_days, target_duration_curr_days: Duration calculation inputs
#   - duration_overrun_curr_days, duration_overrun_change_days: Duration overrun metrics
#   - remain_duration_curr_days: Remaining duration (useful for active tasks)

# NEW result['tasks'] columns (v2.5) - Tier-based priority:
#   - priority_tier: DRIVING, CRITICAL, NEAR_CRITICAL, ERODING, or BUFFERED
#   - priority_score: own_delay × tier_weight (for unified ranking)
#   - float_prev_days: Float at previous snapshot (for trend analysis)

# Tier weights: DRIVING=1000, CRITICAL=100, NEAR_CRITICAL=10, ERODING=5, BUFFERED=1
# Example: 10-day DRIVING delay (score=10000) > 100-day BUFFERED delay (score=100)
```

### Limitations

1. **No baseline comparison** - Compares snapshot-to-snapshot, not plan-to-baseline
2. **Driving path changes** - P6's driving path is recalculated each snapshot
3. **Parallel path approximation** - Uses float-based detection, not full network analysis
4. **No resource constraints** - Doesn't account for resource leveling effects
5. **Calendar days** - Date differences use calendar days, not working days

---

## Analysis Tools

### Enhanced Room Timeline with Narrative Search

**Location:** `room_timeline_enhanced.py`

Timeline analysis tool that combines quality inspections, TBM work entries, AND narrative documents for comprehensive room-level investigation.

**Key Features:**
- 🔍 Searches narrative chunks using enriched location/CSI/date metadata
- 📄 Shows relevant schedule narratives, weekly reports, and claims
- ✅ Combines with RABA, PSI, and TBM data
- 📅 Chronological timeline with all evidence

**Usage:**
```bash
# Basic timeline
python -m scripts.integrated_analysis.room_timeline_enhanced FAB116406 \
  --start 2024-01-01 --end 2024-03-31

# With full text context
python -m scripts.integrated_analysis.room_timeline_enhanced FAB116406 \
  --start 2024-01-01 --end 2024-03-31 --with-context

# Export to CSV
python -m scripts.integrated_analysis.room_timeline_enhanced FAB116406 \
  --start 2024-01-01 --end 2024-03-31 --output timeline.csv
```

**Search Strategy:**
1. Exact room code match in narrative metadata
2. Building + level match for broader context  
3. Date range filtering
4. Combined with quality/TBM data

**Example Results:**
- Room FAB116101 in 2023: 23 narrative chunks + 65 quality/TBM entries
- Narratives filtered by: FAB building, 1F level, date range
- Quality/TBM filtered by: affected_rooms JSON containing room code

**Use Cases:**
- Investigate specific room delays or quality issues
- Correlate schedule narratives with inspection failures
- Find documentary evidence for forensic analysis
- Cross-reference claims with actual work events
