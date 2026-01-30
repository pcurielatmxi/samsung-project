# SECAI NCR Log Scripts

**Last Updated:** 2026-01-30

## Purpose

Process SECAI's internal NCR/QOR quality log workbook. This is separate from ProjectSight NCR - it tracks quality issues managed by SECAI (Samsung E&C America) directly.

## Data Pipeline

```
[Parse] process_secai_ncr_log.py → secai_ncr_qor.csv
    ↓
[Consolidate] consolidate_secai_ncr.py → secai_ncr_consolidated.csv
                                       → secai_ncr_data_quality.csv
```

## Input File

**Location:** `raw/secai_ncr_log/260129_Taylor FAB1_ NCR, QOR Log.xlsb`

The workbook contains 4 sheets:
- **External NCR** - Non-conformance reports from external audits
- **Internal NCR** - Non-conformance reports from internal QC
- **External QOR** - Quality observation reports from external audits
- **Internal QOR** - Quality observation reports from internal QC

## Output Files

| File | Records | Description |
|------|---------|-------------|
| `secai_ncr_qor.csv` | ~1,400 | Raw parsed records |
| `secai_ncr_consolidated.csv` | ~1,400 | Enriched with dimension IDs |
| `secai_ncr_data_quality.csv` | ~1,400 | CSI inference metadata |

## Key Fields

| Field | Description |
|-------|-------------|
| `secai_ncr_id` | Primary key: `SECAI-{source_type[:3]}-{record_type}-{ncr_number}` |
| `record_type` | NCR or QOR |
| `source_type` | EXTERNAL or INTERNAL |
| `ncr_number` | Original NCR/QOR number (e.g., E-NCR-A-G-0001) |
| `building` | Building code (FAB, GCS, CUB, SUE, etc.) |
| `contractor` | Construction contractor name |
| `discipline` | Work discipline (CSA, Structural, etc.) |
| `work_type` | Work description (Cast in place, Welding, etc.) |
| `status` | Current status |

## Dimension Coverage

| Dimension | Field | Notes |
|-----------|-------|-------|
| Company | contractor | Maps to dim_company_id |
| Location | building | Building-level only (no floor/grid) |
| CSI | discipline, work_type | Inferred from discipline → work_type → description |

## Usage

```bash
# Parse Excel workbook
python -m scripts.secai_ncr_log.process.process_secai_ncr_log

# Consolidate with dimensions
python -m scripts.secai_ncr_log.process.consolidate_secai_ncr

# Preview without writing
python -m scripts.secai_ncr_log.process.consolidate_secai_ncr --dry-run
```

## Structure

```
secai_ncr_log/
├── CLAUDE.md                    # This file
└── process/
    ├── process_secai_ncr_log.py # Excel parser
    └── consolidate_secai_ncr.py # Dimension enrichment
```

## Notes

- NCR numbers include prefix like `E-NCR-A-G-0001` (E=External, A=Area code, G=?)
- Excel dates are stored as serial numbers and converted during parsing
- Building names are normalized (e.g., "FAB " → "FAB")
- Some columns are bilingual (Korean + English)
