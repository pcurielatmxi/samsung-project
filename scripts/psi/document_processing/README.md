# PSI Consolidation Output

**Note:** PSI data is now consolidated together with RABA into a single file.

## Output Location

```
processed/raba/raba_psi_consolidated.csv
```

## Why Combined?

Both RABA and PSI are third-party QC inspection sources with identical schemas.
Combining them:
- Simplifies Power BI data model (single table instead of two)
- Ensures consistent column order via unified schema
- Reduces data folder clutter

## To Run Consolidation

```bash
python -m scripts.raba.document_processing.consolidate
```

This reads from both:
- `processed/raba/3.clean/*.clean.json`
- `processed/psi/3.clean/*.clean.json`

And outputs:
- `processed/raba/raba_psi_consolidated.csv`
- `processed/raba/raba_psi_validation_report.json`
