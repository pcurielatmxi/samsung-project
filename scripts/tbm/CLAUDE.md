# TBM Scripts

**Last Updated:** 2025-12-12

## Purpose

Parse Toolbox Meeting (TBM) daily work plans from subcontractors (Excel files).

## Structure

```
tbm/
├── process/    # Excel parsing -> processed/tbm/
└── derive/     # (future analysis)
```

## Key Scripts

| Script | Output | Description |
|--------|--------|-------------|
| `process/parse_tbm_daily_plans.py` | work_entries.csv, tbm_files.csv | Parse SECAI Daily Work Plan Excel files |

## Commands

```bash
python scripts/tbm/process/parse_tbm_daily_plans.py
```

## Key Data

- **13,539 daily work activities**
- Crew deployment (foreman, headcount)
- Location (building, level, row)
- Work descriptions

## Documentation

See [docs/SOURCES.md](../../docs/SOURCES.md) for TBM field mapping.
