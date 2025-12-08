# Data Source Processing Notes

Learnings and gotchas from parsing each data source.

## TBM (Daily Work Plans)

**Script:** `scripts/parse_tbm_daily_plans.py`

### File Format
- All files use standardized "SECAI Daily Work Plan" Excel template
- Sheet name: `SECAI Daily Work Plan`
- Header row 0, date in row 1, data starts row 3
- Columns: No, Division, Tier1(GC), Tier2(SC), Foreman, Contact, Employees, Activities, Building, Level, Row, Start, End

### Parsing Notes
- **Date extraction**: First tries cell (1,0), falls back to filename patterns (MM.DD.YY, MMDDYY, YYYY-MM-DD)
- **Employee counts**: Some entries use ranges like "0-10" - parser takes upper bound
- **Excluded files**: Manpower TrendReport, TaylorFab, Labor Day, Structural (different formats)

### Data Quality
- 99.9% of records have dates (17 missing from MK Marlow files)
- Subcontractor names inconsistent (e.g., "MK Marlow", "MK Marlow ", "Mk Marlow") - not normalized
- `location_row` is optional (11.6% null) - most entries don't specify row detail

### Traceability
- `file_id` links each record to source file in `tbm_files.csv`
- `file_id` is first column (consistent with Primavera schema)

---

## ProjectSight / Yates Daily Reports

**Script:** `scripts/parse_yates_daily_reports.py`

### File Format
- **Source is audit log**, not actual daily reports
- Vertical key-value format with report markers like "52of1314"
- 3 sheets in Excel: Sheet1 (sample), 282 (reports 52-282), Sheet3 (reports 283-386)

### What's Available vs Missing
| Available | Missing |
|-----------|---------|
| Labor hour changes | Daily narratives |
| Workflow status changes | Work descriptions |
| Weather readings | Progress photos |
| Personnel names | Equipment usage |

### Parsing Notes
- Report numbers parsed from "NNNof1314" pattern
- Timestamps preserved from audit log
- Weather data is hourly readings, not daily summaries

### Limitations
- Only reports 52-386 of 1314 total (29%)
- Audit log shows *changes*, not complete state
- Need separate export for actual report content

---

## Primavera P6 (XER Files)

**Script:** `scripts/batch_process_xer.py`

### Schedule Types
Two distinct schedule perspectives with ~4% task overlap:
- **SECAI** (47 files): Owner's schedule, 6K-32K tasks, codes like TE0, TM, TA0
- **YATES** (66 files): GC schedule, 840-13K tasks, codes like CN, FAB, ZX

### ID Prefixing
All IDs prefixed with `file_id` for uniqueness across versions:
```
task_id=715090 in file 48 → task_id="48_715090"
```

### Classification
Uses `proj_short_name` from PROJECT table:
- SECAI: contains "SECAI" or "T1P1"
- YATES: contains "SAMSUNG-FAB", "SAMSUNG-TFAB", or "YATES"

---

## Weekly Reports (PDF)

**Script:** `scripts/parse_weekly_reports.py`

### File Format
- Large PDFs (80-270 pages), most pages are data dumps
- Valuable content in first ~25 pages (narrative section)
- Later pages: Primavera printouts, ProjectSight exports, RFIs

### Document Structure
| Pages | Content |
|-------|---------|
| 1 | Title page with date |
| 2 | Index/Table of contents |
| 3-5 | **Executive Summary** (Work Progressing, Key Issues, Procurement) |
| 5-10 | Production charts, rolling schedules |
| 10-25 | Bid packages, safety, other narratives |
| 25+ | Data dumps (skip these) |

### Section Name Variations
Format changed over time, parser handles multiple patterns:
- "Work Progressing" vs "Last Week Events"
- "Key Open Issues" vs "Key Issues" vs "OPEN ISSUES"
- "Procurement" vs "Buyout"

### Parsing Notes
- Stops extraction when it hits ProjectSight/Primavera printouts
- Author extraction: "Written by NAME (ROLE)" pattern
- Date from filename (YYYYMMDD) or first page content

### Limitations
- PDF structure varies significantly between reports
- Some reports have 0 extracted items due to non-standard formatting
- Images/charts not extracted (only text)

---

## Common Patterns

### Traceability Standard
All parsed data includes `file_id` as first column linking to source file metadata table.

### Date Extraction Priority
1. Parse from data content (cell value)
2. Fall back to filename pattern
3. Mark as null if both fail

### Summary Tables
Each parser generates summary aggregations for quick analysis without loading full datasets.
