# RABA Quality Inspections

**Last Updated:** 2026-01-29

## Purpose

Scrape and process quality inspection reports from RKCI Celvis system (third-party QC firm).

## Output

**Combined RABA + PSI output:** `processed/raba/raba_psi_consolidated.csv`

The consolidation script in this folder combines both RABA and PSI clean records
into a single file for Power BI.

## Data Flow

```
raw/raba/individual/*.pdf (9,397 reports)
    ↓ [Stage 1: Gemini Extract]
1.extract/*.extract.json
    ↓ [Stage 2: Gemini Format]
2.format/*.format.json
    ↓ [Stage 3: Python Clean]
3.clean/*.clean.json
    ↓ [Stage 4: Consolidate - includes PSI]
raba_psi_consolidated.csv + validation report
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

**Status:** ✅ Fixed and integrated into pipeline (2026-01-28)

**Problem:** ~6% of records (601/9391) had incorrect outcome classifications:
- **FAIL → CANCELLED**: 57 trip charge reports where inspection was cancelled
- **PARTIAL → CANCELLED**: 214 inspections that were actually cancelled
- **PARTIAL → MEASUREMENT**: 191 observation/pickup reports without pass/fail criteria
- **PARTIAL → PASS**: 139 reports with "no deficiencies noted"

**Root Cause:** Extract prompt and format schema only allowed PASS/FAIL/PARTIAL. Trip charges and observation reports were forced into wrong categories.

**Fix Applied:**
- `extract_prompt.txt`: Added CANCELLED and N/A as outcome options
- `schema.json`: Added CANCELLED and MEASUREMENT to outcome enum
- `consolidate.py`: **Now auto-corrects outcomes during consolidation** using detection functions from `fix_raba_outcomes.py`
- Pattern-based fix applied to existing data (2026-01-28)

**Outcome Distribution (after fix):**
- PASS: 7,425 (79%)
- PARTIAL: 1,028 (11%)
- FAIL: 390 (4%)
- MEASUREMENT: 277 (3%)
- CANCELLED: 271 (3%)

**Manual Fix Script (for one-time corrections):**
```bash
# Dry run - see what would change
python -m scripts.raba.document_processing.fix_raba_outcomes --dry-run

# Apply pattern-based fixes (creates backup)
python -m scripts.raba.document_processing.fix_raba_outcomes --apply
```

### Party Parsing Issues (2026-01-25)

**Status:** ✅ Fixed in postprocess.py

**Problem:** ~9% of records had persons incorrectly assigned company roles:
- "Kevin Jeong" (Samsung PM) as "contractor" (571 instances)
- "Mark Hammond", "Saad Ekab", etc. as "contractor" instead of representative roles

**Fix Applied:** `postprocess.py` now calls `normalize_parties()` from `scripts.shared.shared_normalization` which:
- Maps known persons to correct roles (Kevin Jeong → client_contact, others → contractor_rep)
- Adds `entity_type` field (person/company)
- Uses company name indicators for entity classification

**To apply:** Re-run clean stage: `./run.sh clean --force`
