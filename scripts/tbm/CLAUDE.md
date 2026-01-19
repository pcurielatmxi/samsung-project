# TBM Scripts

**Last Updated:** 2026-01-19

## Purpose

Parse and enrich Toolbox Meeting (TBM) daily work plans from subcontractors (Excel files).

## Data Flow

```
Field TBM (OneDrive)     raw/tbm/*.xlsx (512+ files)
        ↓ [sync]                ↓ [Stage 1: Parse]
        └──────────────► work_entries.csv + tbm_files.csv
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
    ├── sync_field_tbm.py          # Sync from field team's OneDrive
    └── parse_tbm_daily_plans.py   # Excel parser
```

## Usage

```bash
cd scripts/tbm/process
./run.sh sync --dry-run  # Preview sync from field folder
./run.sh sync            # Sync new files from field team
./run.sh parse           # Stage 1: Extract Excel data
./run.sh enrich          # Stage 2: Add dimension IDs
./run.sh csi             # Stage 3: Add CSI codes
./run.sh all             # Run all stages
./run.sh status          # Show file counts
```

## Field Sync

The field team maintains TBM files in OneDrive (FIELD_TBM_FILES env var):
- `Axios/` - Daily Work Plans by date
- `Berg & MK Marlow/` - Daily Work Plans by date

The sync script:
1. Recursively finds "Daily Work Plan" files
2. Copies new files to raw/tbm/ (flattens folder structure)
3. Tracks synced files in manifest to avoid duplicates

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
