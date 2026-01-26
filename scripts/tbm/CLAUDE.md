# TBM Scripts

**Last Updated:** 2026-01-22

## Purpose

Parse and enrich Toolbox Meeting (TBM) daily work plans from subcontractors (Excel files).

## Data Flow

```
EML Archive              raw/tbm/TBM EML Files/*.eml (740 files, Jul 2023 - Feb 2025)
        ↓ [extract-eml]
Field TBM (OneDrive)     raw/tbm/*.xlsx (5,983 total: 5,377 from EML + 606 from OneDrive)
        ↓ [sync]                ↓ [Stage 1: Parse]
        └──────────────► work_entries.csv + tbm_files.csv
                                ↓ [Stage 2: Enrich]
                        work_entries_enriched.csv (with dim_location_id, dim_company_id, dim_trade_id)
                                ↓ [Stage 3: CSI]
                        work_entries_enriched.csv (with csi_section)
                                ↓ [Stage 4: Dedup]
                        work_entries_enriched.csv (with is_duplicate, is_preferred, date_mismatch)
```

## Structure

```
tbm/
└── process/
    ├── run.sh                        # Pipeline orchestrator
    ├── extract_eml_attachments.py    # Extract Excel from EML archive (one-time)
    ├── sync_field_tbm.py             # Sync from field team's OneDrive
    ├── parse_tbm_daily_plans.py      # Excel parser
    └── deduplicate_tbm.py            # Duplicate detection & data quality flags
```

## Usage

```bash
cd scripts/tbm/process
./run.sh extract-eml --dry-run  # Preview EML extraction (one-time)
./run.sh extract-eml            # Extract Excel from EML archive (one-time)
./run.sh sync --dry-run         # Preview sync from field folder
./run.sh sync                   # Sync new files from field team
./run.sh parse                  # Stage 1: Extract Excel data
./run.sh enrich                 # Stage 2: Add dimension IDs
./run.sh csi                    # Stage 3: Add CSI codes
./run.sh dedup                  # Stage 4: Flag duplicates & quality issues
./run.sh all                    # Run all stages (parse -> enrich -> csi -> dedup)
./run.sh status                 # Show file counts
```

## Data Sources

### 1. EML Archive (Historical - One-Time Extraction)

**Location:** `raw/tbm/TBM EML Files/` (740 EML files)
**Date Range:** July 2023 - February 2025 (~20 months)
**Content:** Email messages containing TBM Excel attachments
**Extraction:** `./run.sh extract-eml`

The EML archive contains historical TBM data delivered via email before the OneDrive sync process was established. Each EML file contains 1-15 Excel attachments.

**Extracted Files:** 5,377 Excel files named `eml_YYYYMMDD_NN.xlsx`
**Manifest:** `raw/tbm/eml_extraction_manifest.json` tracks extraction status

### 2. Field OneDrive Sync (Current Operations)

**Location:** FIELD_TBM_FILES env var
**Folders:**
- `Axios/` - Daily Work Plans by date
- `Berg & MK Marlow/` - Daily Work Plans by date

**Sync Process:**
1. Recursively finds "Daily Work Plan" files
2. Copies new files to raw/tbm/ (flattens folder structure)
3. Tracks synced files in manifest to avoid duplicates

**Current Files:** 606 Excel files from ongoing field operations

## Key Data

- **5,983 Excel files total** (5,377 EML archive + 606 OneDrive sync)
- **Date range:** July 2023 - December 2025 (~30 months)
- **Previous coverage:** 13,539 work activities (Mar-Dec 2025, 421 files)
- **Expected after reprocessing:** ~30,000+ work activities (estimated 2-3x increase)
- Crew info: foreman, headcount, contact
- Location: building, level, row codes

## Dimension Coverage

| Dimension | Coverage | Notes |
|-----------|----------|-------|
| Location | 91.5% | Building + level |
| Company | 98.2% | After subcontractor name standardization |
| Trade | 37.7% | Inferred from activity descriptions |
| Grid | 0% | Not available in TBM |

## Output Location

- `processed/tbm/work_entries.csv` (raw)
- `processed/tbm/work_entries_enriched.csv` (with dimensions + CSI + dedup flags)

## Data Quality Issues

### Duplicate Files (Pipeline - Active)

**Script:** `process/deduplicate_tbm.py`

**Problem:** Multiple files for same company+date cause double-counting of manpower.

**Root Causes:**
1. **Folder duplicates:** Same file in multiple dated folders (e.g., Axios Jan 15 in both `1-15-26` and `1-16-26` folders)
2. **Content duplicates:** Files in wrong folders (e.g., Axios data in Berg folder)
3. **Copy files:** Explicit copies (`Copy of`, `-2-` suffix)

**Fix Logic:**
- Groups files by `subcontractor + date`
- Scores files: `DATE_MATCH (100) + RECORD_COUNT (up to 99) + NOT_COPY (1)`
- Marks best file as `is_preferred=True`, others as `is_duplicate=True`
- Content deduplication: Compares `tier2_sc` (actual workbook subcontractor) vs filename

**Columns Added:**
| Column | Type | Description |
|--------|------|-------------|
| `is_duplicate` | bool | True if file shares company+date with another |
| `duplicate_group_id` | int | Groups duplicate files |
| `is_preferred` | bool | True for best file in duplicate group |
| `subcontractor_normalized` | str | Normalized from `tier2_sc` (workbook data) |

**Power BI Usage:** Filter by `is_preferred = True` to exclude duplicates.

### Date Mismatch (Pipeline - Active)

**Script:** `process/deduplicate_tbm.py`

**Problem:** Some files have internal `report_date` that differs from filename date. Causes incorrect date assignment if filename is trusted.

**Example:** `Axios 12.04.25 Daily Work Plan.xlsx` contains data for `2025-12-03`

**Fix Logic:** Compares extracted filename date vs internal `report_date`

**Columns Added:**
| Column | Type | Description |
|--------|------|-------------|
| `date_mismatch` | bool | True if filename date ≠ internal date |

**Impact:** 12 records flagged. Power BI can filter or highlight these for review.

### Dynamic Column Detection (Embedded Fix)

**Script:** `process/parse_tbm_daily_plans.py`

**Problem:** Different TBM files have varying column layouts. Some files lack row number columns, causing data to shift left.

**Fix Applied:** Dynamic header detection that:
- Scans for known column headers ("ACTIVITY", "NO OF EMPLOYEES", etc.)
- Adjusts column indices if no row number column present
- Recovers 534 records from 14 previously unparseable files

### MXI File Exclusion (Embedded Fix)

**Script:** `process/parse_tbm_daily_plans.py`

**Problem:** MXI-annotated files have frozen internal dates from template creation, causing duplicate grouping issues.

**Fix Applied:** Excludes files matching patterns: `MXI`, `Manpower TrendReport`, `TaylorFab`, etc.

### Employee Column Priority (Embedded Fix)

**Script:** `process/parse_tbm_daily_plans.py`

**Problem:** Some files have multiple employee columns (planned, absent, on site). Parser selected last match, which sometimes had NULL values.

**Fix Applied:** Prioritizes columns containing "planned" over other variants.
