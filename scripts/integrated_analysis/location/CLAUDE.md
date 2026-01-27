# Location Processing Module

**Last Updated:** 2026-01-27

## Purpose

Centralized location processing for all data sources. This module is the **single source of truth** for:
- Building/level normalization
- Grid coordinate parsing
- Spatial matching (affected rooms)
- Location quality diagnostics
- Location hierarchy resolution

## Why This Module Exists

Previously, location processing logic was duplicated across multiple files (~700 lines total):
- `raba/document_processing/consolidate.py` (~80 lines)
- `psi/document_processing/consolidate.py` (~80 lines)
- `integrated_analysis/enrich_with_dimensions.py` (~500 lines)
- `fieldwire/process/enrich_tbm.py` (~40 lines)

This caused:
- Inconsistent normalization across sources
- Bug fixes needing to be applied in multiple places
- Difficulty maintaining location logic

## Usage

### Basic Usage

```python
from scripts.integrated_analysis.location import enrich_location

result = enrich_location(
    building='FAB',
    level='2F',
    grid='G/10-12',
    source='RABA'
)

# Access results
result.dim_location_id       # Integer FK to dim_location
result.building_level        # "FAB-2F"
result.affected_rooms        # JSON string: [{"location_id": 47, ...}, ...]
result.affected_rooms_count  # 3
result.grid_completeness     # "FULL"
result.match_quality         # "PRECISE"
result.location_review_flag  # False
result.location_source       # "GRID_SINGLE_FULL"
```

### DataFrame Usage

```python
from scripts.integrated_analysis.location import enrich_location

# Option 1: Apply to each row
def enrich_row(row):
    result = enrich_location(
        building=row['building'],
        level=row['level'],
        grid=row['grid'],
        source='RABA'
    )
    return result.to_dict()

enriched = df.apply(enrich_row, axis=1).apply(pd.Series)
df = pd.concat([df, enriched], axis=1)

# Option 2: Use helper function
from scripts.integrated_analysis.location.enrichment import enrich_location_row

enriched = df.apply(
    lambda row: enrich_location_row(row, source='RABA'),
    axis=1
).apply(pd.Series)
```

## Module Structure

```
location/
├── __init__.py                    # Public API
├── CLAUDE.md                      # This file
├── core/
│   ├── __init__.py
│   └── normalizers.py             # Building/level normalization
├── dimension/
│   └── __init__.py                # (Future) Dimension table builders
├── enrichment/
│   ├── __init__.py
│   └── location_enricher.py       # Main enrichment function
└── validation/
    └── __init__.py                # (Future) Coverage validation
```

## Key Concepts

### Grid Completeness

Describes what location information was available in the source data:

| Value | Meaning |
|-------|---------|
| `FULL` | Both grid row and column present |
| `ROW_ONLY` | Only grid row (e.g., "Row G") |
| `COL_ONLY` | Only grid column (e.g., "Line 10") |
| `LEVEL_ONLY` | Only building/level, no grid |
| `NONE` | No usable location info |

### Match Quality

Describes how confidently we matched to specific rooms:

| Value | Meaning |
|-------|---------|
| `PRECISE` | All matched rooms have FULL grid overlap |
| `MIXED` | Some FULL matches, some PARTIAL |
| `PARTIAL` | All matches are partial (row-only or col-only) |
| `NONE` | No rooms matched |

### Location Hierarchy

When determining `dim_location_id`, we use this priority order:

1. **ROOM_DIRECT**: Extracted room code (e.g., from P6 task name)
2. **GRID_SINGLE_FULL**: Single room with full grid match
3. **GRID_SINGLE_PARTIAL**: Single room with partial grid match
4. **GRIDLINE**: Gridline location (when no rooms match)
5. **LEVEL**: Building + level fallback
6. **BUILDING**: Building-wide fallback
7. **SITE**: Site-wide fallback

### Location Review Flag

`location_review_flag=True` suggests human review when:
- Many rooms matched (>10) with non-precise matching
- Many partial matches (>5 rooms)
- Only level info available (very coarse)

## Dependencies

This module uses functions from:
- `scripts.shared.dimension_lookup` - Grid parsing, affected rooms
- Internal normalizers

It does NOT depend on source-specific code (RABA, PSI, TBM).

## Migration Guide

### Before (in consolidate.py):

```python
# 80+ lines of location processing
from scripts.shared.dimension_lookup import (
    get_location_id, get_building_level, get_affected_rooms,
    parse_grid_field, normalize_grid,
)

grid_raw = content.get('grid')
grid_normalized = normalize_grid(grid_raw)
grid_parsed = parse_grid_field(grid_raw)

affected_rooms = None
if level_std:
    has_row = grid_parsed['grid_row_min'] is not None
    has_col = grid_parsed['grid_col_min'] is not None
    if has_row or has_col:
        rooms = get_affected_rooms(...)
        if rooms:
            affected_rooms = json.dumps(rooms)
affected_rooms_count = len(json.loads(affected_rooms)) if affected_rooms else None

# ... 50 more lines for completeness, quality, review flag ...
```

### After:

```python
from scripts.integrated_analysis.location import enrich_location

loc = enrich_location(
    building=content.get('building'),
    level=level_std,
    grid=content.get('grid'),
    source='RABA'
)

# Use results directly
record['dim_location_id'] = loc.dim_location_id
record['affected_rooms'] = loc.affected_rooms
record['grid_completeness'] = loc.grid_completeness
# ... etc
```

## Future Work

1. **Move grid parsing** from `dimension_lookup.py` to `location/core/grid_parser.py`
2. **Move TBM grid parsing** from `enrich_with_dimensions.py` (400+ lines) to this module
3. **Add grid coverage bridge** generator for Power BI heatmaps
4. **Add validation scripts** for location coverage reporting
