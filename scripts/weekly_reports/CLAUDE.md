# Weekly Reports Scripts

**Last Updated:** 2025-12-12

## Purpose

Parse weekly progress reports (PDF) containing documented issues, labor details, and project status.

## Structure

```
weekly_reports/
├── process/    # PDF parsing -> processed/weekly_reports/
└── derive/     # Issue correlation -> derived/weekly_reports/
```

## Key Scripts

| Script | Output | Description |
|--------|--------|-------------|
| `process/parse_weekly_reports.py` | key_issues.csv | Extract issues from 37 weekly PDFs |
| `process/parse_labor_detail.py` | labor_detail.csv | Extract labor hours by trade |

## Commands

```bash
# Parse weekly report PDFs
python scripts/weekly_reports/process/parse_weekly_reports.py
```

## Key Data

- **1,108 documented issues** covering safety, quality, coordination
- **Coverage:** Aug 2022 - Jun 2023

## Documentation

See [data/analysis/weekly_reports/](../../data/analysis/weekly_reports/) for analysis findings.
