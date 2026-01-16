# TBM Scripts

**Last Updated:** 2026-01-16

## Purpose

Parse and enrich Toolbox Meeting (TBM) daily work plans from subcontractors (Excel files).

## Data Flow

```
raw/tbm/*.xlsx (421 files)
    ↓ [Stage 1: Parse]
work_entries.csv + tbm_files.csv
    ↓ [Stage 2: Enrich]
work_entries_enriched.csv (with dim_location_id, dim_company_id, dim_trade_id)
    ↓ [Stage 3: CSI]
work_entries_enriched.csv (with csi_section)
```

## Structure

```
tbm/
└── process/
    ├── run.sh                     # Pipeline orchestrator
    └── parse_tbm_daily_plans.py   # Excel parser
```

## Usage

```bash
cd scripts/tbm/process
./run.sh parse      # Stage 1: Extract Excel data
./run.sh enrich     # Stage 2: Add dimension IDs
./run.sh csi        # Stage 3: Add CSI codes
./run.sh all        # Run all stages
./run.sh status     # Show file counts
```

## Key Data

- **13,539 daily work activities** across 421 files
- Crew info: foreman, headcount, contact
- Location: building, level, row codes
- Date range: Mar 2025 - Dec 2025

## Dimension Coverage

| Dimension | Coverage | Notes |
|-----------|----------|-------|
| Location | 91.5% | Building + level |
| Company | 98.2% | After subcontractor name standardization |
| Trade | 37.7% | Inferred from activity descriptions |
| Grid | 0% | Not available in TBM |

## Output Location

- `processed/tbm/work_entries.csv` (raw)
- `processed/tbm/work_entries_enriched.csv` (with dimensions + CSI)
