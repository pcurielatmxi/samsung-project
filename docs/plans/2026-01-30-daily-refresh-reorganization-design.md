# Daily Refresh Pipeline Reorganization

**Date:** 2026-01-30
**Status:** Approved

## Problem Statement

The current `daily_refresh.py` has several issues:
1. Output tables missing columns because new scripts aren't included in refresh
2. No pre-flight checks for required dimension tables
3. Files overwritten in-place during processing (no atomic commits)
4. Fact tables mixed with metadata columns (inference quality, raw values)
5. Schema validation happens but doesn't prevent partial writes

## Design Goals

1. **Dimension tables must exist** before source processing begins
2. **Idempotent source processing** - each source runs independently
3. **Staged writes** - all outputs go to staging, validated, then committed
4. **Split fact/metadata** - each consolidation script writes both tables
5. **Partial commits** - failed sources don't block successful ones

## Pipeline Phases

```
┌─────────────┐   ┌─────────┐   ┌─────────┐   ┌─────────────┐   ┌──────────┐   ┌────────┐
│  PREFLIGHT  │ → │  PARSE  │ → │ SCRAPE  │ → │ CONSOLIDATE │ → │ VALIDATE │ → │ COMMIT │
└─────────────┘   └─────────┘   └─────────┘   └─────────────┘   └──────────┘   └────────┘
```

### PREFLIGHT
- Verify dimension tables exist: `dim_location.csv`, `dim_company.csv`, `dim_csi_section.csv`
- If missing and no `--rebuild-dimensions` flag: **hard error, abort**
- If `--rebuild-dimensions`: run dimension builders first

### PARSE
- File-based incremental parsers (TBM, Primavera, ProjectSight)
- Each reads from `raw/`, writes intermediate files to `processed/.staging/{source}/`

### SCRAPE
- Web-based manifest-tracked scrapers (RABA, PSI)
- Skipped with `--skip-scrapers`

### CONSOLIDATE
- Enrich with dimensions, split fact/metadata
- Each source writes TWO files to staging:
  - `{source}_fact.csv` - clean columns for Power BI
  - `{source}_data_quality.csv` - inference metadata, raw values

### VALIDATE
- Schema validation against Pydantic models
- All staging files validated before any commit

### COMMIT
- Move successful sources from staging to final location
- Per-source: only commit if that source's validation passed
- Failed sources logged but don't block others

## Source Registry

Each data source registers its scripts for each phase:

```python
@dataclass
class SourceConfig:
    name: str                          # e.g., "tbm", "raba"
    parse_module: str | None           # e.g., "scripts.tbm.process.parse_tbm_daily_plans"
    parse_args: list[str]              # e.g., ["--incremental"]
    scrape_module: str | None          # e.g., "scripts.raba.process.scrape_raba_individual"
    scrape_args: list[str]
    consolidate_module: str            # Required - every source must consolidate
    consolidate_args: list[str]
    fact_table: str                    # Output filename, e.g., "work_entries.csv"
    data_quality_table: str            # e.g., "work_entries_data_quality.csv"
    schema: str                        # Pydantic schema name for validation

SOURCES = [
    SourceConfig(
        name="tbm",
        parse_module="scripts.tbm.process.parse_tbm_daily_plans",
        parse_args=["--incremental"],
        scrape_module=None,
        scrape_args=[],
        consolidate_module="scripts.tbm.process.consolidate_tbm",
        consolidate_args=[],
        fact_table="tbm/work_entries.csv",
        data_quality_table="tbm/work_entries_data_quality.csv",
        schema="TBMWorkEntry",
    ),
    # ... raba, psi, projectsight, primavera, fieldwire
]
```

## Staging Directory Structure

```
processed/
├── .staging/                    # Temporary - cleared at start of refresh
│   ├── tbm/
│   │   ├── work_entries.csv
│   │   └── work_entries_data_quality.csv
│   ├── raba/
│   │   ├── raba_psi_consolidated.csv
│   │   └── raba_psi_data_quality.csv
│   ├── projectsight/
│   │   ├── labor_entries.csv
│   │   └── labor_entries_data_quality.csv
│   └── integrated_analysis/
│       ├── dim_location.csv     # If rebuilt
│       └── affected_rooms_bridge.csv
│
├── tbm/                         # Final location (committed files)
├── raba/
└── integrated_analysis/
```

**Cleanup rules:**
- `.staging/` cleared at start of each refresh
- On partial failure: successful sources committed, failed sources' staging deleted
- No leftover staging files after refresh completes

## Consolidation Script Pattern

Each consolidation script writes both fact and data quality tables directly:

```python
from scripts.shared.pipeline_utils import get_output_path, write_fact_and_quality

DATA_QUALITY_COLUMNS = [
    'grid_row_min', 'grid_row_max', 'grid_col_min', 'grid_col_max',
    'affected_rooms', 'affected_rooms_count', 'csi_inference_source',
]

def consolidate(staging_dir: Path | None = None):
    # ... existing enrichment logic ...

    write_fact_and_quality(
        df=df_enriched,
        primary_key='tbm_work_entry_id',
        quality_columns=DATA_QUALITY_COLUMNS,
        fact_path=get_output_path('tbm/work_entries.csv', staging_dir),
        quality_path=get_output_path('tbm/work_entries_data_quality.csv', staging_dir),
    )
```

## Shared Utilities

```python
# scripts/shared/pipeline_utils.py

def write_fact_and_quality(df, primary_key, quality_columns, fact_path, quality_path):
    """Split DataFrame and write both tables atomically."""
    existing_quality_cols = [c for c in quality_columns if c in df.columns]

    # Quality table: PK + quality columns
    df_quality = df[[primary_key] + existing_quality_cols]

    # Fact table: everything except quality columns
    fact_cols = [c for c in df.columns if c not in existing_quality_cols]
    df_fact = df[fact_cols]

    df_fact.to_csv(fact_path, index=False)
    df_quality.to_csv(quality_path, index=False)
```

## CLI Interface

```bash
# Standard refresh (dimensions must exist)
python -m scripts.shared.daily_refresh

# Preview what would run
python -m scripts.shared.daily_refresh --dry-run

# Force rebuild dimension tables before processing
python -m scripts.shared.daily_refresh --rebuild-dimensions

# Skip web scrapers (fast local refresh)
python -m scripts.shared.daily_refresh --skip-scrapers

# Run specific phase only (for debugging)
python -m scripts.shared.daily_refresh --phase consolidate

# Run specific source only
python -m scripts.shared.daily_refresh --source tbm

# Verbose output
python -m scripts.shared.daily_refresh --verbose
```

## Files to Create/Modify

### New Files

| File | Purpose |
|------|---------|
| `scripts/shared/pipeline_registry.py` | Source configs, phase definitions |
| `scripts/shared/pipeline_utils.py` | `write_fact_and_quality()`, `get_output_path()` |

### Modified Files

| File | Changes |
|------|---------|
| `scripts/shared/daily_refresh.py` | Rewrite with phase-based orchestration |
| `scripts/tbm/process/consolidate_tbm.py` | Add `--staging-dir`, write both tables |
| `scripts/raba/document_processing/consolidate.py` | Add `--staging-dir`, write both tables |
| `scripts/projectsight/process/consolidate_labor.py` | Add `--staging-dir`, write both tables |
| `scripts/projectsight/process/consolidate_ncr.py` | Add `--staging-dir`, write both tables |
| `scripts/primavera/derive/generate_task_taxonomy.py` | Add `--staging-dir`, write both tables |
| `scripts/fieldwire/process/generate_powerbi_tables.py` | Add `--staging-dir`, write both tables |

### Files to Remove After Migration

| File | Reason |
|------|---------|
| `scripts/integrated_analysis/extract_data_quality_columns.py` | Logic moved into consolidation scripts |

## Implementation Order

1. Create `pipeline_registry.py` and `pipeline_utils.py`
2. Update one consolidation script (tbm) as template
3. Update remaining consolidation scripts
4. Rewrite `daily_refresh.py` with new orchestration
5. Test end-to-end
6. Remove `extract_data_quality_columns.py`
