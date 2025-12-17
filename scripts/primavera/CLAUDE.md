# Primavera Scripts

**Last Updated:** 2025-12-17

## Purpose

Parse and analyze YATES Primavera P6 schedule exports (XER files).

## Structure

```
primavera/
├── process/    # XER parsing -> processed/primavera/
└── derive/     # Task taxonomy, WBS enrichment -> derived/primavera/
    └── task_taxonomy/  # Modular taxonomy inference system
```

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
