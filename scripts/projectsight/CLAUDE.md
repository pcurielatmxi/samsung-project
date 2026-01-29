# ProjectSight Scripts

**Last Updated:** 2026-01-29

## Purpose

Scrape and process data from Trimble ProjectSight (daily reports, NCR records, library files).

## Data Pipelines

### 1. Labor Pipeline (daily reports → labor hours)

```
[Scrape] scrape_projectsight_daily_reports.py → raw/projectsight/*.json
    ↓
[Parse] parse_labor_from_json.py → labor_entries.csv (raw)
    ↓
[Consolidate] consolidate_labor.py → labor_entries.csv (with dims + CSI)
```

**Output:** 857K+ labor records with dim_company_id, dim_csi_section_id

### 2. NCR Pipeline (quality records)

```
[Parse] process_ncr_export.py → ncr.csv
    ↓
[Consolidate] consolidate_ncr.py → ncr_consolidated.csv
```

**Output:** 1,985 NCR/QOR/SOR/SWN/VR records with dimensions

### 3. Library Structure

```
[Scrape] scrape_projectsight_library.py → library_structure_{project}.json
    ↓
[Process] process_library_files.py → library_files.csv
```

## Structure

```
projectsight/
├── process/
│   ├── run.sh                              # Pipeline orchestrator
│   ├── scrape_projectsight_daily_reports.py  # Playwright scraper
│   ├── scrape_projectsight_library.py      # Library scraper
│   ├── parse_labor_from_json.py            # Labor extraction
│   ├── consolidate_labor.py                # Labor enrichment (dims + CSI)
│   ├── process_ncr_export.py               # NCR parsing
│   ├── consolidate_ncr.py                  # NCR enrichment
│   └── process_library_files.py            # Library CSV
└── utils/
    └── projectsight_session.py             # Shared session management
```

## Usage

```bash
cd scripts/projectsight/process
./run.sh labor          # Run labor pipeline (parse + consolidate)
./run.sh ncr            # Run NCR pipeline
./run.sh all            # Run both pipelines
./run.sh status         # Show file counts
```

## Dimension Coverage

| Dimension | Coverage | Notes |
|-----------|----------|-------|
| Company | 99.9% | After name standardization |
| CSI | 80%+ | Inferred from activity/trade/division |
| Location | - | Not available in labor data |

## Environment

Requires `.env` with `PROJECTSIGHT_USERNAME`, `PROJECTSIGHT_PASSWORD`

## Notes

- Session cookies saved to `~/.projectsight_sessions/`
- Scrapers are idempotent (skip existing data)
- Trimble SSO requires two-step login handling
