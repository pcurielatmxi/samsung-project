# Primavera Scripts

**Last Updated:** 2026-01-29

## Purpose

Parse and analyze YATES Primavera P6 schedule exports (XER files).

## Structure

```
primavera/
├── process/    # XER parsing -> processed/primavera/
├── derive/     # Task taxonomy, WBS enrichment
│   └── task_taxonomy/  # Modular taxonomy inference
├── analyze/    # CPM engine and critical path analysis
└── docs/       # Analysis documentation
```

**Note:** Schedule slippage analysis is in [`scripts/integrated_analysis/`](../integrated_analysis/) for cross-source integration.

## Key Scripts

| Script | Output | Description |
|--------|--------|-------------|
| `process/batch_process_xer.py` | task.csv, wbs.csv, etc. | Parse all 66 XER files to CSV |
| `derive/generate_task_taxonomy.py` | task_taxonomy.csv | Generate unified task classification |
| `derive/audit_task_taxonomy.py` | Console + CSV | Quality audit via sampling |

## Commands

```bash
# Parse all XER files
python scripts/primavera/process/batch_process_xer.py

# Generate task taxonomy (requires processed data)
python scripts/primavera/derive/generate_task_taxonomy.py --latest-only

# Audit taxonomy quality
python scripts/primavera/derive/audit_task_taxonomy.py 50 --print 10
python scripts/primavera/derive/audit_task_taxonomy.py 100 --output audit_sample.csv
```

## Task Taxonomy System

### Inference Hierarchy (Precedence Order)

1. **Activity Codes** - Direct P6 assignments (highest priority)
2. **WBS Context** - Inferred from hierarchy tiers
3. **Task Code Structure** - Extracted from task_code field
4. **Task Name Patterns** - Regex-based fallback

### Output Columns

**Classification:**
- `dim_csi_section_id`, `csi_section`, `csi_title` - 52 CSI MasterFormat sections
- `sub_trade`, `sub_trade_desc` - Detailed scope codes
- `building` - FAB, SUE, SUW, FIZ
- `level` - 1-6, ROOF, B1, MULTI
- `area`, `room` - Grid area and room codes
- `location_type`, `location_code`, `dim_location_id` - Unified location

**Source Tracking:** `*_source` columns show derivation origin

### Coverage (Latest Schedule - 12,230 tasks)

- **Building:** 97.3% (55% activity codes, 38% WBS)
- **Location Type:** 98.2% (47% gridline, 33% room, 13% level)

## CPM Analysis Engine

**Location:** `primavera/analyze/`

### Structure

```
analyze/
├── cpm/                     # Core CPM implementation
│   ├── models.py            # Task, Dependency, CriticalPathResult
│   ├── network.py           # TaskNetwork graph
│   ├── engine.py            # Forward/backward pass
│   └── calendar.py          # Work day/hour calculations
├── analysis/
│   ├── critical_path.py     # Critical path identification
│   ├── single_task_impact.py # What-if analysis
│   └── delay_attribution.py  # Delay source attribution
├── data_loader.py           # Load P6 CSV → TaskNetwork
└── validate_cpm.py          # P6 comparison validator
```

### Usage

```python
from scripts.primavera.analyze.data_loader import load_schedule, get_latest_file_id
from scripts.primavera.analyze.analysis.critical_path import analyze_critical_path

file_id = get_latest_file_id()
network, calendars, project_info = load_schedule(file_id)

result = analyze_critical_path(network, calendars, data_date=project_info['data_date'])
print(f"Critical tasks: {len(result.critical_path)}")
```

### Key Functions

| Module | Function | Purpose |
|--------|----------|---------|
| `data_loader` | `load_schedule(file_id)` | Load schedule → (TaskNetwork, calendars, project_info) |
| `data_loader` | `list_schedule_versions()` | List available P6 snapshots |
| `critical_path` | `analyze_critical_path()` | Identify critical/near-critical tasks |
| `single_task_impact` | `analyze_task_impact()` | What-if duration change |

### CPM Accuracy

**96-98% match** vs P6 calculated values (tested on 2,176 non-completed tasks):
- Early/Late dates: 96-98% match
- Total Float (±1 day): 96.8% match

**Validated for schedules Oct 2023+** (schedules 39-88).

### Validation

```bash
python scripts/primavera/analyze/validate_cpm.py              # Latest schedule
python scripts/primavera/analyze/validate_cpm.py --file-id 88 # Specific schedule
python scripts/primavera/analyze/validate_cpm.py --list-schedules
```

## Key Fields Reference

- `target_end_date` = current scheduled finish
- `total_float` = late_end - early_end (negative = behind)
- No baseline in exports - compare across versions

## Schedule Slippage Context

72 schedule versions (Oct 2022 - Nov 2025):
- Task growth: 843 → 12,433 (14.7x)
- 759 persistent tasks (90% of original)
- Project completion slip: ~32 months

**Analysis approach:** See [`scripts/integrated_analysis/schedule_slippage_analysis.py`](../integrated_analysis/schedule_slippage_analysis.py)

## Documentation

- [docs/SOURCES.md](../../docs/SOURCES.md) - XER field mapping
- [docs/schedule_slippage_analysis.md](docs/schedule_slippage_analysis.md) - Slippage analysis plan
