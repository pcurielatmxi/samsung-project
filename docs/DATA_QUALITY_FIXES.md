# Data Quality Fixes Registry

Central registry of all data quality fixes across sources. Each fix is documented in its source's `CLAUDE.md` file with full details.

**Last Updated:** 2026-01-22

---

## Active Fixes

| Source | Issue | Script | Type | Status | Details |
|--------|-------|--------|------|--------|---------|
| RABA | Outcome misclassification | `raba/document_processing/fix_raba_outcomes.py` | One-time | Prompts updated | [RABA CLAUDE.md](../scripts/raba/CLAUDE.md#outcome-misclassification-2026-01-21) |
| PSI | Outcome misclassification | `psi/document_processing/fix_psi_outcomes.py` | One-time | Prompts updated | [PSI CLAUDE.md](../scripts/psi/CLAUDE.md#outcome-misclassification-2026-01-21) |
| TBM | Duplicate files | `tbm/process/deduplicate_tbm.py` | Pipeline | Active | [TBM CLAUDE.md](../scripts/tbm/CLAUDE.md#duplicate-files-pipeline---active) |
| TBM | Date mismatch | `tbm/process/deduplicate_tbm.py` | Pipeline | Active | [TBM CLAUDE.md](../scripts/tbm/CLAUDE.md#date-mismatch-pipeline---active) |
| Mappings | Alias duplicates | `integrated_analysis/dimensions/build_company_dimension.py` | Validation | Active | Merged into generation |

## Deprecated Fixes

Fixes that have been merged into main scripts or are no longer needed:

| Source | Issue | Original Script | Deprecated | Replacement |
|--------|-------|-----------------|------------|-------------|
| Mappings | Alias duplicates | `integrated_analysis/mappings/fix_alias_duplicates.py` | 2026-01-22 | Validation in `build_company_dimension.py` |

## Embedded Fixes (In Main Scripts)

These fixes are built into main processing scripts rather than standalone fix scripts:

| Source | Issue | Script | Description |
|--------|-------|--------|-------------|
| TBM | Dynamic column detection | `tbm/process/parse_tbm_daily_plans.py` | Handles varying Excel column layouts |
| TBM | MXI file exclusion | `tbm/process/parse_tbm_daily_plans.py` | Excludes MXI-annotated template files |
| TBM | Employee column priority | `tbm/process/parse_tbm_daily_plans.py` | Prioritizes "planned" employee columns |
| RABA | Measurement detection | `raba/document_processing/consolidate.py` | Detects measurement-only records inline |

---

## Fix Classification

### By Type

| Type | Description | When to Run | Example |
|------|-------------|-------------|---------|
| **One-time** | Corrects historical data issues | Run once after identifying issue | `fix_raba_outcomes.py` |
| **Pipeline** | Part of regular processing | Run with every data refresh | `deduplicate_tbm.py` |
| **Validation** | Prevents issues at generation time | Automatic during build | Alias duplicate check |
| **Schema fix** | Changes extraction prompts/schemas | Requires full re-extraction | Updated RABA/PSI prompts |
| **Embedded** | Built into main processing scripts | Automatic with main script | Dynamic column detection |

### By Status

| Status | Meaning |
|--------|---------|
| **Active** | Fix is in use and working |
| **Prompts updated** | Schema/prompt fix applied, awaiting re-extraction |
| **Applied** | One-time fix has been run successfully |
| **Deprecated** | Fix is no longer needed (issue resolved at source) |

---

## Fix Script Standard

All standalone fix scripts should follow this pattern:

### Required Features

1. **Dry-run mode**: `--dry-run` to preview changes without applying
2. **Apply mode**: `--apply` to actually modify data
3. **Backup creation**: Automatic `.csv.bak` before modification
4. **Reporting**: Show what changed (before/after counts)

### Docstring Template

```python
#!/usr/bin/env python3
"""
Fix: {Issue Name}
Source: {source}
Type: {One-time|Pipeline|Schema fix}
Status: {Active|Applied|Deprecated}
Date Created: {YYYY-MM-DD}
Last Applied: {YYYY-MM-DD}

Issue:
    {Description of the data quality issue}

Root Cause:
    {Why the issue occurred}

Fix Logic:
    {How the fix works}

Usage:
    python -m scripts.{source}.{path}.fix_{name} --dry-run
    python -m scripts.{source}.{path}.fix_{name} --apply

Output Columns Added/Modified:
    - {column_name}: {description}
"""
```

---

## Adding New Fixes

When creating a new data quality fix:

1. **Create the fix script** in the appropriate source folder
2. **Document in source CLAUDE.md** under "Data Quality Issues" section
3. **Add entry to this registry** in the Active Fixes table
4. **Follow the standard template** for docstrings and CLI flags

### Location Guidelines

| Fix Type | Recommended Location |
|----------|---------------------|
| Standalone fix script | `scripts/{source}/document_processing/fix_{issue}.py` or `scripts/{source}/process/fix_{issue}.py` |
| Pipeline fix | Integrate into existing pipeline script (e.g., `deduplicate_tbm.py`) |
| Cross-source fix | `scripts/integrated_analysis/mappings/fix_{issue}.py` |

---

## Running Fixes

### One-time Fixes

```bash
# Always dry-run first
python -m scripts.raba.document_processing.fix_raba_outcomes --dry-run
python -m scripts.psi.document_processing.fix_psi_outcomes --dry-run
python scripts/integrated_analysis/mappings/fix_alias_duplicates.py --dry-run

# Apply when satisfied
python -m scripts.raba.document_processing.fix_raba_outcomes --apply
python -m scripts.psi.document_processing.fix_psi_outcomes --apply
python scripts/integrated_analysis/mappings/fix_alias_duplicates.py --apply
```

### Pipeline Fixes

```bash
# TBM deduplication (part of pipeline)
cd scripts/tbm/process
./run.sh dedup

# Or full pipeline
./run.sh all
```

---

## Verification

### Spot-check Tools

```bash
# Quality data spot-check
python -m scripts.shared.spotcheck_quality_data raba --samples 5
python -m scripts.shared.spotcheck_quality_data psi --samples 5
```

### Schema Validation

```bash
# Validate output file schemas
pytest tests/integration/test_output_schemas.py -v
```
