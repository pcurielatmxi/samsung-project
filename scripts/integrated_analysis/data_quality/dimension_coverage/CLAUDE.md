# Dimension Coverage Quality Check

**Last Updated:** 2026-01-29

## Purpose

Validates dimension table coverage across all fact tables, providing:
1. **Coverage Matrix** - % of records linked to each dimension
2. **Location Granularity** - Distribution of location specificity
3. **CSI Section Analysis** - Joinability gaps between schedule and quality data
4. **Company Coverage** - Unresolved company names

## Quick Start

```bash
# Run the report
python -m scripts.integrated_analysis.data_quality.dimension_coverage

# Save markdown report
python -m scripts.integrated_analysis.data_quality.dimension_coverage -o coverage.md
```

## Module Structure

```
dimension_coverage/
├── __init__.py      # Public API and main entry point
├── __main__.py      # CLI entry point
├── models.py        # Data classes (DimensionStats, SourceCoverage)
├── config.py        # Source configurations and thresholds
├── loaders.py       # Dimension table and source data loaders
├── analyzers.py     # Coverage calculation logic
├── formatters.py    # Report formatting (console and markdown)
└── CLAUDE.md        # This file
```

## Key Concepts

### Location Granularity

Location data has varying specificity:

| Level | Types | Description |
|-------|-------|-------------|
| Room-level | ROOM, STAIR, ELEVATOR | Most specific - exact room identified |
| Grid-level | GRIDLINE | Grid coordinates only, room inferred via spatial join |
| Coarse | LEVEL, BUILDING, AREA, SITE, UNDEFINED | Least specific |

The granularity summary shows what % of each source's data falls into each level.

### Coverage Thresholds

| Level | Threshold | Indicator |
|-------|-----------|-----------|
| Good | ≥95% | ✅ |
| Warning | ≥80% | ⚠️ |
| Poor | <80% | ❌ |

### Source Configurations

Each fact table has a config in `config.py` specifying:
- `path`: File location relative to PROCESSED_DATA_DIR
- `location_id_col`: Column with dim_location_id FK
- `location_type_col`: Column with location_type (if present)
- `company_id_col`: Column with dim_company_id FK
- `company_name_col`: Raw company name for unresolved tracking
- `csi_col`: Column with CSI section code

## Programmatic Usage

```python
from scripts.integrated_analysis.data_quality.dimension_coverage import (
    check_dimension_coverage,
    load_and_analyze_sources,
    SourceCoverage,
)

# Full report
results = check_dimension_coverage()

# Access coverage metrics
for name, cov in results['coverage'].items():
    print(f"{name}: {cov.location_id_pct:.1f}% location")

    # Get granularity breakdown
    summary = cov.get_granularity_summary()
    print(f"  Room-level: {summary['room_level']:.1f}%")

# Just load and analyze without printing
coverage = load_and_analyze_sources()
```

## Output Interpretation

### Section 1: Coverage Matrix

Shows what % of each fact table has dimension IDs populated.

```
Source         Records  │  Location   Company  CSI Section
P6             483,057  │ ✅ 100.0%    N/A     ✅  99.5%
RABA             9,391  │ ✅ 100.0%  ⚠️ 89.0%  ✅  99.9%
```

### Section 2: Location Granularity

Shows distribution of location types. Higher room-level % = more specific data.

```
Source       ROOM │ GRIDLINE │ BUILDING
P6       44,637 (9%) │ 278,616 (58%) │ 51,623 (11%)
PSI       3,009 (48%) │    718 (11%) │  1,272 (20%)
```

**Interpretation:**
- PSI has best granularity (48% room-level)
- P6 is mostly gridlines (58%)
- TBM is coarsest (72% building/area level)

### Section 3: CSI Coverage

Identifies CSI sections that can't be joined between P6 and quality data.

### Section 4: Company Coverage

Shows unresolved company names that need to be added to `map_company_aliases.csv`.

## Adding New Sources

1. Add config to `SOURCE_CONFIGS` in `config.py`
2. Ensure the source file exists at the configured path
3. Run the report to verify

## Dependencies

- pandas
- src.config.settings (for path configuration)

## Related Files

- `scripts/integrated_analysis/location/` - Centralized location enrichment
- `scripts/integrated_analysis/dimensions/` - Dimension table builders
- `data/processed/integrated_analysis/dimensions/` - Dimension CSV files
