# PSI Quality Inspections

**Last Updated:** 2026-01-29

## Purpose

Scrape and process quality inspection reports from Construction Hive (third-party QC firm).

## Output Location

**Combined output with RABA:** `processed/raba/raba_psi_consolidated.csv`

See `scripts/psi/document_processing/README.md` for details.

## Data Flow

```
raw/psi/individual/*.pdf (6,309 reports)
    ↓ [Stage 1: Gemini Extract]
1.extract/*.extract.json
    ↓ [Stage 2: Gemini Format]
2.format/*.format.json
    ↓ [Stage 3: Python Clean]
3.clean/*.clean.json
    ↓ [Stage 4: Consolidate - combined with RABA]
processed/raba/raba_psi_consolidated.csv
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
    ├── consolidate.py              # Stage 4: CSV export
    └── fix_psi_outcomes.py         # Programmatic outcome correction
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

## Open Issues

### Outcome Misclassification (2026-01-21)

**Status:** ✅ Fixed and integrated into pipeline (2026-01-28)

**Problem:** ~2% of records (147/6309) had incorrect outcome classifications:
- **FAIL → CANCELLED**: 97 records where work was "not ready" marked as FAIL
- **PARTIAL → CANCELLED**: 20 inspections that were actually cancelled
- **PARTIAL → PASS**: 30 records with "accepted" or "no deficiencies"

**Root Cause:** Extract prompt didn't clearly distinguish CANCELLED from FAIL.

**Fix Applied:**
- `extract_prompt.txt`: Added clear CANCELLED definition
- `consolidate.py`: **Now auto-corrects outcomes during consolidation** using detection functions from `fix_psi_outcomes.py`
- Pattern-based fix applied to existing data (2026-01-28)

**Outcome Distribution (after fix):**
- PASS: 5,298 (84%)
- FAIL: 493 (8%)
- CANCELLED: 457 (7%)
- PARTIAL: 61 (1%)

**Manual Fix Script (for one-time corrections):**
```bash
# Dry run - see what would change
python -m scripts.psi.document_processing.fix_psi_outcomes --dry-run

# Apply pattern-based fixes (creates backup)
python -m scripts.psi.document_processing.fix_psi_outcomes --apply
```

### Party Parsing Issues (2026-01-25)

**Status:** ✅ Fixed in postprocess.py

**Problem:** ~62% of records had incorrect `parties_involved` entries:
- **Trade as party**: "Arch", "Architectural", "Drywall" extracted with role "trade" (3,931 instances)
- **Company as inspector**: "Intertek PSI" with role "inspector" instead of "inspection_company" (879 instances)
- **Mixed names**: "Chris Plassmann/YATES" combined person and company (1,144 instances)

**Fix Applied:** `postprocess.py` now calls `normalize_parties()` from `scripts.shared.shared_normalization` which:
- Filters out trade/discipline entries
- Fixes company-as-inspector roles
- Splits mixed person/company names into separate entries
- Adds `entity_type` field (person/company)

**To apply:** Re-run clean stage: `./run.sh clean --force`
