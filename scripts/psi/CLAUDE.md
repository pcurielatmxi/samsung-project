# PSI Quality Inspections

**Last Updated:** 2026-01-16

## Purpose

Scrape and process quality inspection reports from Construction Hive (third-party QC firm).

## Data Flow

```
raw/psi/individual/*.pdf (6,309 reports)
    ↓ [Stage 1: Gemini Extract]
1.extract/*.extract.json
    ↓ [Stage 2: Gemini Format]
2.format/*.format.json
    ↓ [Stage 3: Python Clean]
3.clean/*.clean.json
    ↓ [Stage 4: Consolidate]
psi_consolidated.csv + validation report
```

## Structure

```
psi/
├── process/
│   └── scrape_psi_reports.py       # Playwright scraper
└── document_processing/
    ├── config.json                 # 3-stage pipeline config
    ├── run.sh                      # CLI wrapper
    ├── postprocess.py              # Stage 3: normalization
    └── consolidate.py              # Stage 4: CSV export
```

## Usage

```bash
# Download reports
python scripts/psi/process/scrape_psi_reports.py --limit 100

# Run pipeline
cd scripts/psi/document_processing
./run.sh extract --limit 100
./run.sh format
./run.sh clean
./run.sh consolidate
./run.sh status
```

## Key Data

- **6,309 inspection reports** (May 2022 - Dec 2025)
- **Coverage:** Location 98%, Company 99%, Trade 97%, Grid 73%
- Uses unified schema compatible with RABA for Power BI

## Environment

Requires `.env` with `PSI_USERNAME`, `PSI_PASSWORD`
