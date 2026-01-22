# RABA Quality Inspections

**Last Updated:** 2026-01-21

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
│   ├── consolidate.py              # Stage 4: CSV export
│   └── fix_raba_outcomes.py        # Programmatic outcome correction
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

## Open Issues

### Outcome Misclassification (2026-01-21)

**Status:** Prompts updated, awaiting re-extraction

**Problem:** ~40% of records have incorrect outcome classifications:
- **FAIL → CANCELLED**: Trip charge reports where inspection was cancelled (~50+ records)
- **PARTIAL → MEASUREMENT**: Observation/pickup reports without pass/fail criteria (~100+ records)
- **PARTIAL → PASS**: Reports with no deficiencies noted (~50+ records)

**Root Cause:** Extract prompt and format schema only allowed PASS/FAIL/PARTIAL. Trip charges and observation reports were forced into wrong categories.

**Fix Applied:**
- `extract_prompt.txt`: Added CANCELLED and N/A as outcome options
- `schema.json`: Added CANCELLED and MEASUREMENT to outcome enum
- `format_prompt.txt`: Added guidance for CANCELLED and MEASUREMENT cases

**Pending:** Full re-extraction required to apply fixes.

**Interim Fix Script:**
```bash
# Dry run - see what would change
python -m scripts.raba.document_processing.fix_raba_outcomes --dry-run

# Apply pattern-based fixes (creates backup)
python -m scripts.raba.document_processing.fix_raba_outcomes --apply

# Use embeddings for additional detection
python -m scripts.raba.document_processing.fix_raba_outcomes --dry-run --use-embeddings
```

**Spot-check tool:** `python -m scripts.shared.spotcheck_quality_data raba --samples 5`
