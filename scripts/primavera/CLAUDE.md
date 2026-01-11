# Primavera Scripts

**Last Updated:** 2025-12-18

## Purpose

Parse and analyze YATES Primavera P6 schedule exports (XER files).

## Structure

```
primavera/
├── process/    # XER parsing -> processed/primavera/
├── derive/     # Task taxonomy, WBS enrichment -> derived/primavera/
│   └── task_taxonomy/  # Modular taxonomy inference system
├── analyze/    # CPM engine and critical path analysis
└── docs/       # Analysis documentation
```

**Note:** Schedule slippage analysis (comparing P6 snapshots) is in [`scripts/integrated_analysis/`](../integrated_analysis/) since it integrates with other data sources for cross-reference analysis.

## Planning Documents

| Document | Description | Status |
|----------|-------------|--------|
| [Schedule Slippage Analysis](docs/schedule_slippage_analysis.md) | Original plan for milestone and task-level tracking | ✅ Implemented |

**Implemented:** Schedule slippage analysis with own_delay/inherited_delay decomposition, what-if impact analysis, and recovery sequence analysis. See [`scripts/integrated_analysis/schedule_slippage_analysis.py`](../integrated_analysis/schedule_slippage_analysis.py).

## Key Scripts

| Script | Output | Description |
|--------|--------|-------------|
| `process/batch_process_xer.py` | task.csv, wbs.csv, etc. | Parse all 66 XER files to CSV tables |
| `derive/generate_task_taxonomy.py` | task_taxonomy.csv | Generate unified task classification taxonomy |
| `derive/audit_task_taxonomy.py` | (console + optional CSV) | Quality audit by sampling and comparing with P6 source data |

## Commands

```bash
# Parse all XER files
python scripts/primavera/process/batch_process_xer.py

# Generate task taxonomy (requires processed data)
python scripts/primavera/derive/generate_task_taxonomy.py --latest-only

# Audit taxonomy quality (sample N tasks and display with source data)
python scripts/primavera/derive/audit_task_taxonomy.py 50 --print 10
python scripts/primavera/derive/audit_task_taxonomy.py 100 --output audit_sample.csv
```

## Task Taxonomy System

### Overview

The task taxonomy system infers business classifications (trade, building, level, location) for each task using a hierarchical precedence system:

1. **Activity Codes** (highest priority) - Direct P6 assignments
2. **WBS Context** - Inferred from hierarchy tiers
3. **Task Code Structure** - Extracted from task_code field
4. **Task Name Patterns** - Regex-based fallback

### Output Columns

**Classification Fields:**
- `trade_id`, `trade_code`, `trade_name` - 12 trade categories (Concrete, Steel, Finishes, etc.)
- `sub_trade`, `sub_trade_desc` - Detailed scope (CIP, CTG, DRYWALL, etc.)
- `building` - FAB, SUE, SUW, FIZ
- `level` - 1-6, ROOF, B1, MULTI
- `area` - Grid area (SEA-5, SWA-1, FIZ1)
- `room` - Room code (FAB112155)
- `sub_contractor` - From Z-SUB activity code
- `phase` - Project stage (PRE, STR, INT, COM)
- `location_type`, `location_code` - Unified location classification (ROOM, ELEVATOR, STAIR, GRIDLINE, AREA, LEVEL, BUILDING, MULTI)

**Source Tracking:**
- `*_source` columns show derivation: activity_code | wbs | task_code | inferred | None

**Impact Fields** (sparse, for IMPACT tasks only):
- `impact_code`, `impact_type`, `attributed_to`, `root_cause`

### Coverage Statistics

From latest YATES schedule (12,230 tasks):

**Trade:** 96.8% coverage
- 48.5% from activity codes (Z-TRADE)
- 48.3% from task name inference
- 3.2% unmapped

**Building:** 97.3% coverage
- 55.3% from activity codes (Z-BLDG)
- 38.0% from WBS hierarchy
- 2.7% missing

**Location Type:** 98.2% coverage
- GRIDLINE: 47.4% (area-scoped work)
- ROOM: 33.3% (specific room)
- LEVEL: 12.9% (level-wide work)
- BUILDING: 2.8% (building-wide)
- ELEVATOR: 0.8%, STAIR: 0.6%, AREA: 0.4%
- Unscoped: 1.8%

### Audit Script

The `audit_task_taxonomy.py` script provides quality verification by:

1. Randomly sampling N tasks from the taxonomy
2. Joining with original P6 data (activity codes, WBS)
3. Displaying side-by-side comparison for each task
4. Printing summary statistics (source distribution, coverage)
5. Optionally exporting to CSV for analysis

**Example Output per Task:**
```
--- Task 1 ---
ID: 64_100234487
Name: PAINT & PROTECT DOORS & FRAMES - FAB1-ST21D

Activity Codes (from P6):
  Z-TRADE: None
  Z-BLDG: East Support Building
  Z-LEVEL: L3 - LEVEL 3
  Z-AREA: SEA-1

WBS Context:
  Tier 3: LEVEL 3
  Tier 4: L3 SUE
  Tier 5: Stair 21 - 3F-SUE-FAB1-ST21

Inferred Taxonomy:
  Trade: FINISHES (source: inferred)
  Building: SUE (source: wbs)
  Level: 3 (source: wbs)
  Location: LEVEL - 3
```

## Key Fields

- `target_end_date` = current scheduled finish
- `total_float` = late_end - early_end (negative = behind)
- No baseline in exports - compare across versions

## Documentation

See [docs/SOURCES.md](../../docs/SOURCES.md) for XER field mapping.

---

## CPM Analysis Engine

**Location:** `primavera/analyze/`

A Python implementation of Critical Path Method (CPM) calculations for P6 schedule analysis.

### Structure

```
analyze/
├── cpm/                     # Core CPM implementation
│   ├── models.py            # Task, Dependency, CriticalPathResult dataclasses
│   ├── network.py           # TaskNetwork graph structure
│   ├── engine.py            # CPMEngine - forward/backward pass calculations
│   └── calendar.py          # P6Calendar - work day/hour calculations
├── analysis/                # Analysis modules
│   ├── critical_path.py     # Critical path identification and float analysis
│   ├── single_task_impact.py # What-if for single task duration changes
│   └── delay_attribution.py  # Delay source attribution
├── data_loader.py           # Load P6 CSV exports into TaskNetwork
└── test_cpm.py              # Validation tests
```

### Key Functions

| Module | Function | Purpose |
|--------|----------|---------|
| `data_loader` | `load_schedule(file_id)` | Load complete schedule → (TaskNetwork, calendars, project_info) |
| `data_loader` | `list_schedule_versions()` | List all available P6 snapshots |
| `critical_path` | `analyze_critical_path(network, calendars)` | Identify critical/near-critical tasks |
| `single_task_impact` | `analyze_task_impact(network, task_code, delta)` | What-if duration change |
| `delay_attribution` | `attribute_delays(network)` | Identify delay sources |

### Usage

```python
from scripts.primavera.analyze.data_loader import load_schedule, get_latest_file_id
from scripts.primavera.analyze.analysis.critical_path import analyze_critical_path

# Load latest schedule
file_id = get_latest_file_id()
network, calendars, project_info = load_schedule(file_id)

# Analyze critical path
result = analyze_critical_path(network, calendars, data_date=project_info['data_date'])
print(f"Critical tasks: {len(result.critical_path)}")
print(f"Project finish: {result.project_finish}")
```

### CPM Accuracy vs P6

The CPM engine achieves **96-98% accuracy** compared to P6's calculated values (tested on 2,176 non-completed tasks):

| Metric | Match Rate |
|--------|------------|
| Early Start | 96.5% |
| Early Finish | 96.3% |
| Late Start | 98.2% |
| Late Finish | 97.2% |
| Total Float (±1 day) | 96.8% |

**Key Implementation Details:**

1. **Data Date Handling**: Completed tasks get early dates set to `data_date`, ensuring successors are driven from data_date forward (not historical actual dates).

2. **Lag Rules by Relationship Type**:
   - FS/SS from completed predecessor: lag=0 (start constraint satisfied)
   - FF/SF from completed predecessor: lag applies (finish constraint still relevant)
   - In-progress predecessor + in-progress successor: lag=0 for FS/SS (both already running)

3. **Milestone Handling**: Zero-duration milestones finish at exact driven time (including end-of-day), without advancing to next work period.

4. **Work Period Boundaries**: Late finish for FS relationships retreats to end of previous work period (P6 convention: tasks finish at 17:00, not 07:00 next day).

**Known Limitations (~3% mismatch):**

- **Boundary representation** (~32 tasks): Same work time, different clock representation (e.g., Friday 17:00 vs Monday 07:00 are equivalent in work hours)
- **Complex dependency chains** (~49 tasks): Some edge cases in chains with mixed completed/in-progress/not-started predecessors with multiple relationship types
- **P6-specific optimizations**: P6 may use proprietary scheduling algorithms for edge cases

For analysis purposes, this accuracy is sufficient. The remaining mismatches do not significantly impact critical path identification or float analysis.

### Note on Schedule Slippage

For snapshot-to-snapshot comparison (schedule slippage analysis), use [`scripts/integrated_analysis/schedule_slippage_analysis.py`](../integrated_analysis/schedule_slippage_analysis.py) which:
- Compares two P6 snapshots to identify what changed
- Decomposes delay into own_delay (task-caused) vs inherited_delay (predecessor-pushed)
- Provides what-if recovery estimates with parallel path constraints

---

## Schedule Slippage Analysis

**Implementation:** See [`scripts/integrated_analysis/schedule_slippage_analysis.py`](../integrated_analysis/schedule_slippage_analysis.py) for the production script with CLI and programmatic API. Full documentation is in [`scripts/integrated_analysis/CLAUDE.md`](../integrated_analysis/CLAUDE.md).

### Context

The YATES schedule lacks a true baseline. Schedule slippage must be assessed by comparing across 72 schedule versions (Oct 2022 - Nov 2025).

### Key Data Characteristics

| Metric | Value |
|--------|-------|
| Schedule versions | 72 |
| Task growth | 843 → 12,433 (14.7x) |
| Persistent tasks | 759 (90% of original) |
| Milestones (original) | 47 |
| Milestones (latest) | 137 (39 persistent) |
| Project completion slip | ~32 months |

### Recommended Approach

1. **Milestone-level** (Phase 1): Track 39 persistent milestones - stable, meaningful completion points
2. **Critical path** (Phase 2): Track tasks with float <= 0 version-by-version
3. **Task-level** (Phase 3): Track 759 persistent tasks for detailed attribution
4. **Attribution** (Phase 4): Correlate with weekly reports for root cause analysis

### Key Findings

- Task-level analysis adds value: within-WBS variance (4.7d) is 5x larger than between-WBS variance (1.0d)
- 88% of added milestones are decomposition/coordination, not new scope
- Critical path persistence is 73% between consecutive versions
- Major slippage periods: Nov 2022, Oct-Nov 2023, Jan-Feb 2024, Jun 2024, Sep 2025

See [Schedule Slippage Analysis Plan](docs/schedule_slippage_analysis.md) for full details.
