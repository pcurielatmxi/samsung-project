# Weekly Reports Scripts

**Last Updated:** 2026-01-29

## Purpose

Parse weekly progress reports (PDF) to extract narratives, issues, RFI/submittal logs, and labor details.

## Data Flow

| Stage | Script | Output |
|-------|--------|--------|
| 1 | `parse_weekly_reports.py` | weekly_reports.csv, key_issues.csv, work_progressing.csv |
| 2 | `parse_weekly_report_addendums_fast.py` | addendum_rfi_log.csv, addendum_submittal_log.csv, addendum_manpower.csv |
| 3 | `parse_labor_detail.py` | labor_detail.csv, labor_detail_by_company.csv (with dim_company_id) |

## Structure

```
weekly_reports/
└── process/
    ├── run.sh                              # Pipeline orchestrator
    ├── process_weekly_reports.py           # Master entry point
    ├── parse_weekly_reports.py             # Stage 1: Narratives
    ├── parse_weekly_report_addendums_fast.py  # Stage 2: Addendums
    └── parse_labor_detail.py               # Stage 3: Labor tables (with company enrichment)
```

## Usage

```bash
cd scripts/weekly_reports/process
./run.sh all           # Full pipeline: parse all stages
./run.sh status        # Check outputs
```

## Key Data

- **37 weekly reports** (Aug 2022 - Jun 2023)
- **1,108 documented issues** (Key Issues section)
- **13K+ labor entries** from detailed labor tables
- Labor data has **company only** (no location/building/level)

## Dimension Coverage

| Dimension | Coverage | Notes |
|-----------|----------|-------|
| Company | ~95%+ | Matched via `get_company_id()` |
| Location | - | Not available in weekly labor data |
| CSI | - | Not available in weekly labor data |

## Output Location

- **Raw extracts:** `processed/weekly_reports/` (CSV files)
- **Labor summary:** `processed/weekly_reports/labor_detail_by_company.csv` (with dim_company_id)
