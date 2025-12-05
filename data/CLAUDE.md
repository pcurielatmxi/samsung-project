# Data Directory - Context & Documentation

**Last Updated:** 2025-12-05
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
├── raw/                        # Raw source files (gitignored except manifest)
│   └── xer/                    # Primavera P6 XER exports
│       ├── manifest.json       # Tracked file registry (48 files)
│       └── *.xer               # XER files (gitignored)
├── primavera/                  # Processed Primavera data
│   ├── processed/              # Batch-processed CSVs
│   │   ├── xer_files.csv       # File metadata table
│   │   └── tasks.csv           # All tasks with file_id
│   ├── raw_tables/             # Direct XER table exports
│   └── analysis/               # Analysis reports
├── projectsight/               # ProjectSight extractions
│   ├── extracted/              # Raw JSON from browser
│   └── tables/                 # Normalized CSV tables
└── CLAUDE.md                   # This file
```

## Data Sources

### 1. Primavera P6 (XER Files)

**Location:** `raw/xer/` and `primavera/`

**Current Status:** 48 XER files spanning Oct 2022 - Nov 2025

**Key Finding:** The files represent **two distinct schedule perspectives**:
- **SECAI Schedule** (47 files): Owner's schedule maintained by Samsung's engineering arm. Evolving versions with 95-100% task code overlap between consecutive updates. Task count grew from ~7K to ~32K over time.
- **SAMSUNG-TFAB1 Schedule** (1 file, current): GC (Yates) version of the schedule. Only 0.1% task code overlap with SECAI files due to different task coding conventions.

**Understanding the Difference:**
| Aspect | SECAI (Owner) | SAMSUNG-TFAB1 (GC) |
|--------|---------------|---------------------|
| Maintained by | Samsung/SECAI | Yates Construction |
| Task prefix style | TE0, TM, TA0 | CN, FAB, ZX |
| Task count | ~32K (detailed) | ~12K (summarized?) |
| Files | 47 historical versions | 1 current file |

See [primavera/analysis/xer_file_overlap_analysis.md](primavera/analysis/xer_file_overlap_analysis.md) for detailed analysis.

**Processed Output:**
| File | Records | Description |
|------|---------|-------------|
| `xer_files.csv` | 48 | File metadata with file_id |
| `tasks.csv` | 964,002 | All tasks from all files |

**Schema:**
- Every task record has `file_id` linking to `xer_files.csv`
- Use `is_current` flag to identify the active schedule

### 2. ProjectSight (Daily Reports)

**Location:** `projectsight/`

**Current Status:** 415 daily reports extracted

**Extracted Tables:**
| File | Records | Description |
|------|---------|-------------|
| `daily_reports.csv` | 415 | Report summaries |
| `companies.csv` | - | Companies referenced |
| `contacts.csv` | - | Contact information |
| `history.csv` | - | Report revision history |
| `changes.csv` | - | Field-level changes |

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

# Regenerate ProjectSight tables
python scripts/daily_reports_to_csv.py
```

## Key Insights

### Schedule Version Tracking

The SECAI schedules show clear version evolution:
- **Oct 2022 - Dec 2023:** Early project (7K-11K tasks)
- **Jul-Sep 2023:** Large interim export anomaly (66K tasks)
- **Nov 2023 - Jun 2024:** Growth phase (24K-31K tasks)
- **May 2024 - Jun 2025:** Mature phase (29K-32K tasks)

### Two Schedule Perspectives

The data contains two parallel schedule perspectives:

1. **SECAI (Owner) Schedule** - 47 files
   - Maintained by Samsung's engineering arm
   - More detailed (~32K tasks)
   - Historical versions from Oct 2022 - Jun 2025
   - Task codes: TE0, TM, TA0 prefixes

2. **SAMSUNG-TFAB1 (GC) Schedule** - 1 file (current)
   - Maintained by Yates (General Contractor)
   - More summarized (~12K tasks)
   - Current as of Nov 2025
   - Task codes: CN, FAB, ZX prefixes

**Note:** These are the same project viewed from different organizational perspectives, not different projects. The low overlap (0.1%) is due to different task coding conventions, not different scope.

## Maintenance Checklist

When updating this document:

- [ ] Update "Last Updated" date at top
- [ ] Add new data sources to Directory Structure
- [ ] Update record counts after re-processing
- [ ] Document any schema changes
- [ ] Add new key insights from analysis
- [ ] Update regeneration commands if changed
