# Data Directory - Context & Documentation

**Last Updated:** 2025-12-08
**Status:** Living Document - Must be kept updated as data changes

> **IMPORTANT:** This is a living document. Update this file whenever:
> - New data sources are added
> - Data structure changes
> - New analysis findings emerge
> - Processing pipelines are modified

## Project Context

**Project:** Samsung Austin Semiconductor Chip Manufacturing Facility (Taylor, TX)
- **FAB1** - Fabrication building (primary focus)
- **Owner:** Samsung
- **Owner's Engineering Arm:** SECAI (Samsung Engineering Construction America Inc.)
- **General Contractor:** Yates Construction

## Overview

This directory contains all project data from multiple source systems for the Samsung Taylor FAB1 construction project. Data is organized by source system and processing stage.

## Directory Structure

```
data/
├── pending_organization/       # Files awaiting classification (see below)
├── raw/                        # Raw source files (gitignored except manifest)
│   └── xer/                    # Primavera P6 XER exports
│       ├── manifest.json       # Tracked file registry (113 files)
│       └── *.xer               # XER files (gitignored)
├── primavera/                  # Processed Primavera data
│   ├── processed/              # Batch-processed CSVs (ALL tables)
│   │   ├── xer_files.csv       # File metadata table
│   │   ├── task.csv            # All tasks with file_id
│   │   ├── taskpred.csv        # Task predecessors/dependencies
│   │   ├── taskrsrc.csv        # Task resource assignments
│   │   ├── projwbs.csv         # WBS structure
│   │   └── *.csv               # All other XER tables (~48 tables total)
│   └── analysis/               # Analysis reports
├── projectsight/               # ProjectSight extractions
│   ├── extracted/              # Raw JSON from browser
│   └── tables/                 # Normalized CSV tables
└── CLAUDE.md                   # This file
```

## Pending Organization

**Location:** `pending_organization/`

Staging area for unclassified files. Drop files here, then ask "organize pending files" to classify and move them to the correct location.

See [pending_organization/CLAUDE.md](pending_organization/CLAUDE.md) for detailed workflow.

## Data Sources

### 1. Primavera P6 (XER Files)

**Location:** `raw/xer/` and `primavera/`

**Current Status:** 113 XER files spanning Oct 2022 - Nov 2025

#### Schedule Grouping

Files represent **two distinct schedule perspectives**, identified by `schedule_type` in `xer_files.csv`:

| Schedule | Files | Date Range | Task Range | Maintained By |
|----------|-------|------------|------------|---------------|
| **SECAI** | 47 | 2022-10-17 → 2025-06-27 | 6K-32K | Samsung/SECAI (Owner) |
| **YATES** | 66 | 2022-10-10 → 2025-11-20 | 840-13K | Yates Construction (GC) |

**Cross-schedule overlap is only ~4%** - these are genuinely different schedule perspectives with different task coding conventions, not duplicates.

#### Classification Method

Classification uses `proj_short_name` from the PROJECT table with filename fallback:
- **SECAI**: proj_short_name contains "SECAI" or "T1P1"
- **YATES**: proj_short_name contains "SAMSUNG-FAB", "SAMSUNG-TFAB", or "YATES"

Run `python scripts/classify_schedules.py` to reclassify and update `xer_files.csv`.

#### Sequencing

Files are sequenced by date (parsed from filename). The `date` field in `xer_files.csv` enables chronological ordering within each schedule type for version comparison analysis.

**Processed Output (ALL Tables):**

The batch processor exports ALL tables from XER files, not just tasks. Key tables include:

| File | Records | Description |
|------|---------|-------------|
| `xer_files.csv` | 113 | File metadata (file_id, date, schedule_type) |
| `task.csv` | 964,002 | All tasks from all files |
| `taskpred.csv` | 2,002,060 | Task predecessors/dependencies |
| `taskactv.csv` | 4,138,671 | Task activity code assignments |
| `taskrsrc.csv` | 266,298 | Task resource assignments |
| `projwbs.csv` | 270,890 | WBS (Work Breakdown Structure) |
| `actvcode.csv` | - | Activity code values |
| `actvtype.csv` | - | Activity code types |
| `calendar.csv` | - | Calendars |
| `rsrc.csv` | - | Resources |
| `udftype.csv` | - | User-defined field types |
| `udfvalue.csv` | - | User-defined field values |
| `project.csv` | - | Project metadata |
| ... | ... | ~48 tables total |

**Schema:**
- **Every table has `file_id` as first column** linking to `xer_files.csv`
- **All ID columns are prefixed with file_id** for uniqueness: `{file_id}_{original_id}`
  - Example: `task_id=715090` in file 48 becomes `task_id="48_715090"`
  - This applies to ALL `*_id` columns (primary keys and foreign keys)
  - Ensures referential integrity when combining multiple XER files
  - Enables proper data modeling in PowerBI and other tools
- Use `is_current` flag in `xer_files.csv` to identify the active schedule
- Tables are lowercase versions of XER table names (e.g., TASK → task.csv)

### 2. ProjectSight (Daily Reports) - Manual Export

**Location:** `projectsight/`

**Current Status:** 415 daily reports (manually exported, static dataset)

**Extraction Method:** Data was manually exported from ProjectSight as JSON, then transformed to CSV via `scripts/daily_reports_to_csv.py`. No automated browser scraping - ProjectSight's JavaScript-heavy SPA made automation impractical.

**Available Tables:**
| File | Records | Description |
|------|---------|-------------|
| `daily_reports.csv` | 415 | Report summaries (date, status, weather, workforce) |
| `companies.csv` | - | Companies referenced in reports |
| `contacts.csv` | - | Contact information |
| `daily_report_history.csv` | - | Report revision history |
| `daily_report_changes.csv` | - | Field-level changes |

**Raw Data:** `extracted/daily_reports_415.json` and `extracted/daily-report-details-history.json`

### 3. Fieldwire (Future)

**Location:** TBD

**Status:** Not yet implemented

## Data Governance

### What's Tracked in Git

| Path | Tracked | Reason |
|------|---------|--------|
| `raw/xer/manifest.json` | ✅ Yes | File registry |
| `raw/xer/*.xer` | ❌ No | Large binary files |
| `primavera/processed/*.csv` | ❌ No | Large generated output |
| `primavera/analysis/*.md` | ✅ Yes | Analysis documentation |
| `projectsight/tables/*.csv` | ❌ No | Generated output |

### Regenerating Data

```bash
# Regenerate Primavera processed data
python scripts/batch_process_xer.py

# Classify schedules and fix dates (updates xer_files.csv)
python scripts/classify_schedules.py

# Regenerate ProjectSight tables
python scripts/daily_reports_to_csv.py
```

## Key Insights

### xer_files.csv Schema

Key columns for analysis:
- `file_id`: Unique identifier, used as prefix for all IDs in other tables
- `date`: Schedule date (YYYY-MM-DD), parsed from filename
- `schedule_type`: SECAI or YATES
- `is_current`: Boolean flag for active schedule

### Querying by Schedule Type

```python
import pandas as pd

xer_files = pd.read_csv('data/primavera/processed/xer_files.csv')
tasks = pd.read_csv('data/primavera/processed/task.csv')

# Get SECAI files in chronological order
secai_files = xer_files[xer_files['schedule_type'] == 'SECAI'].sort_values('date')

# Get tasks for a specific schedule version
file_id = secai_files.iloc[-1]['file_id']  # Latest SECAI
latest_tasks = tasks[tasks['file_id'] == file_id]
```

## Maintenance Checklist

When updating this document:

- [ ] Update "Last Updated" date at top
- [ ] Add new data sources to Directory Structure
- [ ] Update record counts after re-processing
- [ ] Document any schema changes
- [ ] Add new key insights from analysis
- [ ] Update regeneration commands if changed
