# Data Sources Documentation

**Last Updated:** 2025-12-12

## Overview

This project integrates data from multiple construction management systems. All data follows the traceability structure defined in the main [CLAUDE.md](../CLAUDE.md).

## Data Locations

| Type | Location | Description |
|------|----------|-------------|
| Raw files | `{WINDOWS_DATA_DIR}/raw/{source}/` | Source files as received |
| Processed | `{WINDOWS_DATA_DIR}/processed/{source}/` | Parsed and enriched CSV tables |
| Analysis | `data/analysis/{source}/` | Findings (tracked in git) |

---

## 1. Primavera P6 (XER Files)

**Raw:** `raw/primavera/*.xer`
**Processed:** `processed/primavera/*.csv`

### Schedule Grouping

Files represent **two distinct schedule perspectives**:

| Schedule | Files | Date Range | Tasks/File | Maintained By |
|----------|-------|------------|------------|---------------|
| **SECAI** | 47 | Oct 2022 - Jun 2025 | 6K-32K | Samsung/SECAI (Owner) |
| **YATES** | 66 | Oct 2022 - Nov 2025 | 840-13K | Yates Construction (GC) |

**Cross-schedule overlap is only ~4%** - these are different schedule perspectives, not duplicates.

### Processed Tables

| File | Records | Description |
|------|---------|-------------|
| `xer_files.csv` | 113 | File metadata (file_id, date, schedule_type) |
| `task.csv` | 964,002 | All tasks from all files |
| `taskpred.csv` | 2,002,060 | Task predecessors/dependencies |
| `taskactv.csv` | 4,138,671 | Task activity code assignments |
| `taskrsrc.csv` | 266,298 | Task resource assignments |
| `projwbs.csv` | 270,890 | WBS (Work Breakdown Structure) |
| `actvcode.csv` | - | Activity code values |
| `calendar.csv` | - | Calendars |
| `rsrc.csv` | - | Resources |
| `udfvalue.csv` | - | User-defined field values |

### Schema Notes

- **Every table has `file_id`** as first column linking to `xer_files.csv`
- **All ID columns prefixed with file_id**: `{file_id}_{original_id}`
  - Example: `task_id=715090` in file 48 becomes `task_id="48_715090"`
- Key fields: `target_end_date` (scheduled finish), `total_float` (late_end - early_end)

### Commands

```bash
python scripts/primavera/process/batch_process_xer.py
python scripts/primavera/derive/generate_wbs_taxonomy.py
```

---

## 2. Weekly Reports (PDF)

**Raw:** `raw/weekly_reports/*.pdf`
**Processed:** `processed/weekly_reports/*.csv`

### Source

37 PDF files (Aug 2022 - Jun 2023) - Weekly progress reports from Yates containing executive summaries, issues, and procurement status.

### Processed Tables

| File | Records | Description |
|------|---------|-------------|
| `weekly_reports.csv` | 37 | Report metadata (date, author, page count) |
| `key_issues.csv` | 1,108 | Open issues and blockers |
| `work_progressing.csv` | 1,039 | Work progress items by discipline |
| `procurement.csv` | 374 | Procurement/buyout status items |

### Key Fields

- `file_id`: Links to weekly_reports.csv
- `report_date`: Week of report
- `content`: Issue/item description

### Commands

```bash
python scripts/weekly_reports/process/parse_weekly_reports.py
```

---

## 3. TBM Daily Work Plans (Excel)

**Raw:** `raw/tbm/*.xlsx, *.xlsm`
**Processed:** `processed/tbm/*.csv`

### Source

421 Excel files (SECAI Daily Work Plan format), Mar - Dec 2025. Daily work plans submitted by subcontractors to SECAI.

### Processed Tables

| File | Records | Description |
|------|---------|-------------|
| `work_entries.csv` | 13,539 | Individual work activities with crew info |
| `tbm_files.csv` | 421 | File metadata (date, subcontractor) |

### Key Fields

- `report_date`: Date of work plan
- `tier1_gc`, `tier2_sc`: GC (Yates) and subcontractor
- `foreman`, `num_employees`: Crew info
- `work_activities`: Task description
- `location_building`, `location_level`: FAB, SUP, etc.

### Top Subcontractors

Berg (25.6K employees), MK Marlow (9K), Alert Lock & Key (5K), Cherry Coatings (4.6K)

### Commands

```bash
python scripts/tbm/process/parse_tbm_daily_plans.py
```

---

## 4. ProjectSight Daily Reports

**Raw:** `raw/projectsight/` (scraped JSON)
**Processed:** `processed/projectsight/*.csv`

### Source

415 daily reports from Trimble ProjectSight (Jun 2022 - Mar 2023). Contains weather, labor hours, equipment usage.

### Processed Tables

| File | Records | Description |
|------|---------|-------------|
| `daily_reports.csv` | 415 | Report summaries |
| `labor_entries.csv` | 1,589 | Individual labor hour entries |
| `weather.csv` | 8,963 | Hourly weather readings |

### Commands

```bash
python scripts/projectsight/process/scrape_projectsight_daily_reports.py --limit 10
python scripts/projectsight/process/daily_reports_to_csv.py
```

---

## 5. Fieldwire (API)

**Raw:** `raw/fieldwire/*.csv`
**Processed:** `processed/fieldwire/*.csv`

### Configuration

```env
FIELDWIRE_BASE_URL=https://api.fieldwire.com
FIELDWIRE_API_KEY=your_api_key_here
```

### Available Data

Field task management: punch lists, inspections, task assignments.

---

## 6. Quality / Inspection Records

**Raw:** `raw/quality/*.xlsx`

### Source

Two Excel files tracking QA/QC inspections (Mar 2024 - Jun 2025):

| File | Source | Records |
|------|--------|---------|
| Inspection and Test Log | Samsung/SECAI | 17,294 |
| Work Inspection Request Log | Yates | 19,890 |

### Key Metrics

- **SECAI inspections:** 2.9% failure rate (ARCH, MECH, ELEC)
- **Yates internal QC:** 7.8% failure rate
- **Yates official:** 3.1% failure rate

---

## Field Mapping (Cross-Source)

| Primavera | ProjectSight | Fieldwire | Standardized |
|-----------|--------------|-----------|--------------|
| task_id | - | id | source_id |
| task_name | - | name | name |
| status | status | status | status |
| target_start | report_date | created_at | start_date |
