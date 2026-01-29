# TBM Analysis Report Generation

**Last Updated:** 2026-01-29

## Purpose

Generate TBM (Toolbox Meeting) Analysis Reports comparing planned workforce against field-verified observations. These reports analyze contractor workforce planning accuracy.

## Workflow

```
Fieldwire Data Dump → Narrative Generation → DOCX Report
```

1. **Input:** Fieldwire CSV exports with field observations
2. **Process:** Analyze observations, calculate KPIs, generate narratives
3. **Output:** MXI-branded DOCX reports

## Structure

```
tbm_analysis/
├── CLAUDE.md              # This file
├── PROCESS.md             # Detailed workflow documentation
├── tbm_data_processor.py  # Parse Fieldwire CSV exports
├── narrative_generator.py # Generate analysis narratives
├── docx_generator.py      # Create branded DOCX reports
└── prompts/               # LLM prompt templates
```

## Key Scripts

| Script | Purpose |
|--------|---------|
| `tbm_data_processor.py` | Parse Fieldwire data dumps, filter by date/contractor |
| `narrative_generator.py` | Generate category-specific narratives using LLM |
| `docx_generator.py` | Create MXI-branded Word documents |

## Data Source

**Fieldwire Data Dump:**
- Format: UTF-16 CSV with tab separator
- Key fields: DirectManpower, IndirectManpower, TBMManpower, Category, Company
- Observations in Message columns (1-62)

## Notes

- See `PROCESS.md` for complete workflow documentation
- This is separate from `scripts/tbm/` which processes raw TBM Excel files
- Reports are for MXI internal analysis, not Power BI
