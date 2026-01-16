# RABA Quality Inspections

**Last Updated:** 2026-01-16

## Purpose

Scrape and process quality inspection reports from RKCI Celvis system (third-party QC firm).

## Data Flow

```
raw/raba/individual/*.pdf (9,397 reports)
    ↓ [Stage 1: Gemini Extract]
1.extract/*.extract.json
    ↓ [Stage 2: Gemini Format]
2.format/*.format.json
    ↓ [Stage 3: Python Clean]
3.clean/*.clean.json
    ↓ [Stage 4: Consolidate]
raba_consolidated.csv + validation report
```

## Structure

```
raba/
├── process/
│   └── scrape_raba_individual.py   # Playwright scraper (recommended)
├── document_processing/
│   ├── config.json                 # 3-stage pipeline config
│   ├── run.sh                      # CLI wrapper
│   ├── postprocess.py              # Stage 3: normalization
│   └── consolidate.py              # Stage 4: CSV export
└── archive/
    └── scrape_raba_reports.py      # Legacy batch scraper
```

## Usage

```bash
# Download reports
python scripts/raba/process/scrape_raba_individual.py --limit 100

# Run pipeline
cd scripts/raba/document_processing
./run.sh run              # Extract + Format + Clean
./run.sh consolidate      # Generate CSV
./run.sh status           # Check progress
```

## Key Data

- **9,397 inspection reports** (May 2022 - Dec 2025)
- **Coverage:** Location 99%, Company 99%, Trade 62%, CSI 95%
- Uses unified schema compatible with PSI for Power BI

## Environment

Requires `.env` with `RABA_USERNAME`, `RABA_PASSWORD`
