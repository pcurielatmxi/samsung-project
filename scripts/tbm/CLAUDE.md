# TBM Scripts

**Last Updated:** 2026-01-29

## Purpose

Parse and enrich Toolbox Meeting (TBM) daily work plans from subcontractors (Excel files).

## Data Flow

```
EML Archive              raw/tbm/TBM EML Files/*.eml (740 files, Jul 2023 - Feb 2025)
        ↓ [extract-eml]
Field TBM (OneDrive)     raw/tbm/*.xlsx (5,983 total: 5,377 from EML + 606 from OneDrive)
        ↓ [sync]                ↓ [Stage 1: Parse]
        └──────────────► work_entries.csv + tbm_files.csv
                                ↓ [Stage 2: Consolidate]
                        work_entries.csv (with dim_location_id, dim_company_id, csi_section)
                                ↓ [Stage 3: Dedup]
                        work_entries.csv (with is_duplicate, is_preferred, date_mismatch)
```

## Structure

```
tbm/
└── process/
    ├── run.sh                        # Pipeline orchestrator
    ├── extract_eml_attachments.py    # Extract Excel from EML archive (one-time)
    ├── sync_field_tbm.py             # Sync from field team's OneDrive
    ├── parse_tbm_daily_plans.py      # Excel parser
    ├── consolidate_tbm.py            # Dimension enrichment + CSI inference
    └── deduplicate_tbm.py            # Duplicate detection & data quality flags
```

## Usage

```bash
cd scripts/tbm/process
./run.sh extract-eml --dry-run  # Preview EML extraction (one-time)
./run.sh extract-eml            # Extract Excel from EML archive
./run.sh sync --dry-run         # Preview sync from field folder
./run.sh sync                   # Sync new files from field team
./run.sh parse                  # Stage 1: Extract Excel data
./run.sh consolidate            # Stage 2: Add dimensions + CSI
./run.sh dedup                  # Stage 3: Flag duplicates & quality issues
./run.sh all                    # Run all stages
./run.sh status                 # Show file counts
```

## Data Sources

### EML Archive (Historical)

- **Location:** `raw/tbm/TBM EML Files/` (740 EML files)
- **Date Range:** July 2023 - February 2025
- **Extracted:** 5,377 Excel files named `eml_YYYYMMDD_NN.xlsx`
- **Manifest:** `raw/tbm/eml_extraction_manifest.json`

### Field OneDrive Sync (Current)

- **Location:** `FIELD_TBM_FILES` env var
- **Folders:** `Axios/`, `Berg & MK Marlow/`
- **Current Files:** 606 Excel files from ongoing operations

## Key Data

- **5,983 Excel files** (5,377 EML + 606 OneDrive)
- **Date range:** July 2023 - December 2025 (~30 months)
- **Content:** Crew info (foreman, headcount), location (building, level, row codes)

## Dimension Coverage

| Dimension | Coverage | Notes |
|-----------|----------|-------|
| Location | 91.5% | Building + level |
| Company | 98.2% | After subcontractor name standardization |
| CSI | 95%+ | Inferred from activity descriptions |
| Grid | 0% | Not available in TBM |

## Output

`processed/tbm/work_entries.csv` - Work entries with dimensions, CSI, and dedup flags

## Data Quality Handling

### Duplicate Files

**Problem:** Multiple files for same company+date cause double-counting.

**Root Causes:**
- Folder duplicates (same file in multiple dated folders)
- Content duplicates (files in wrong folders)
- Copy files (`Copy of`, `-2-` suffix)

**Solution:** Groups by `subcontractor + date`, scores files, marks best as `is_preferred=True`

**Columns Added:**
| Column | Description |
|--------|-------------|
| `is_duplicate` | True if file shares company+date with another |
| `duplicate_group_id` | Groups duplicate files |
| `is_preferred` | True for best file in duplicate group |
| `subcontractor_normalized` | Normalized from workbook data |

**Power BI:** Filter by `is_preferred = True` to exclude duplicates.

### Date Mismatch

**Problem:** Internal `report_date` differs from filename date.

**Solution:** Compares filename date vs internal date, adds `date_mismatch` flag.

### Parser Fixes (Embedded)

| Fix | Description |
|-----|-------------|
| Dynamic columns | Scans for known headers, adjusts indices if no row number column |
| MXI exclusion | Excludes template files with frozen internal dates |
| Employee priority | Prioritizes "planned" employee columns over other variants |
