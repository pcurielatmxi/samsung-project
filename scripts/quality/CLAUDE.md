# Quality Inspection Scripts

**Last Updated:** 2026-01-29

## Purpose

Process QC inspection data from Yates and SECAI Excel exports with dimension ID enrichment.

## Pipeline (3 Stages)

| Stage | Script | Input | Output |
|-------|--------|-------|--------|
| 1 | `process/process_quality_inspections.py` | Excel workbooks | `processed/quality/{yates,secai}_*.csv` |
| 2 | `document_processing/consolidate.py` | Stage 1 CSVs | `processed/quality/enriched/combined_qc_inspections.csv` |
| 3 | `process/enrich_qc_workbooks.py` | Stage 2 output | `processed/quality/qc_inspections_enriched.csv` |

## Usage

```bash
# Stage 1: Extract from Excel (manual - run when source files update)
python -m scripts.quality.process.process_quality_inspections

# Stage 2: Consolidate with dimension IDs
cd scripts/quality/document_processing
./run.sh

# Stage 3: Enrich with grid/affected_rooms
python -m scripts.quality.process.enrich_qc_workbooks
```

## Structure

```
quality/
├── process/
│   ├── process_quality_inspections.py  # Stage 1: Excel → CSV
│   └── enrich_qc_workbooks.py          # Stage 3: Location enrichment
└── document_processing/
    ├── consolidate.py                   # Stage 2: Dimension IDs + CSI
    └── run.sh                           # Convenience wrapper
```

## Data Sources

- **Yates:** WIR (Work Inspection Request) log - ~4,000 inspections
- **SECAI:** IR (Inspection Request) log - ~2,000 inspections

## Output Location

- **Stage 1:** `processed/quality/{yates_all_inspections,secai_inspection_log}.csv`
- **Stage 2:** `processed/quality/enriched/combined_qc_inspections.csv`
- **Stage 3:** `processed/quality/qc_inspections_enriched.csv` (Power BI source)

## Notes

- CSI inference uses keyword mappings from `scripts/integrated_analysis/add_csi_to_quality_workbook.py`
- Location enrichment uses centralized `scripts/integrated_analysis/location/` module
- NOT in daily_refresh.py (source Excel files update infrequently)
