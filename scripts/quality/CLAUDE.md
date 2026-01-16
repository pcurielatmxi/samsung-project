# Quality Inspection Scripts

**Last Updated:** 2026-01-16

## Purpose

Process QC inspection data from Yates and SECAI Excel exports with dimension ID enrichment.

## Data Flow

| Stage | Script | Output |
|-------|--------|--------|
| 1 | `process_quality_inspections.py` | yates_all_inspections.csv, secai_inspection_log.csv |
| 2 | `derive_quality_taxonomy.py` | yates_taxonomy.csv, secai_taxonomy.csv (optional) |
| 3 | `consolidate.py` | combined_qc_inspections.csv (with dimension IDs) |

## Structure

```
quality/
├── process/           # Excel parsing
│   └── process_quality_inspections.py
├── derive/            # Taxonomy extraction (optional)
│   └── derive_quality_taxonomy.py
└── document_processing/
    ├── consolidate.py # Dimension enrichment
    └── run.sh
```

## Usage

```bash
cd scripts/quality/document_processing
./run.sh              # Run consolidation
./run.sh status       # Show dimension coverage
```

## Key Data

- **Yates:** ~4,000 inspections (WIR - Work Inspection Request)
- **SECAI:** ~2,000 inspections (IR - Inspection Request)
- **Coverage:** Location 95%+, Company 99%+, Trade 90%+

## Output Location

- **Raw extracts:** `processed/quality/`
- **Enriched:** `processed/quality/enriched/combined_qc_inspections.csv`

## Notes

- Shared utilities (location_parser, company_matcher) moved to `scripts/shared/`
- Quality data complements RABA/PSI third-party inspection reports
