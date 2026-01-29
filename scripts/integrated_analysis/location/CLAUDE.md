# Location Processing Module

**Last Updated:** 2026-01-29

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
- `tbm/process/consolidate_tbm.py` (~500 lines)
- `fieldwire/process/enrich_tbm.py` (~40 lines)

This caused:
- Inconsistent normalization across sources
- Bug fixes needing to be applied in multiple places
- Difficulty maintaining location logic

## Usage

### Basic Usage (RABA/PSI/QC)

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
result.location_type         # ROOM, STAIR, ELEVATOR, GRIDLINE, LEVEL, BUILDING, UNDEFINED
result.affected_rooms        # JSON string: [{"location_id": 47, ...}, ...]
result.affected_rooms_count  # 3
result.match_type            # ROOM_DIRECT, GRID_SINGLE, GRID_MULTI, GRIDLINE, LEVEL, etc.
```

### TBM Usage (with pre-parsed grid bounds)

TBM has complex grid patterns (103 regex patterns) that are parsed by `parse_tbm_grid()`.
Pass pre-parsed bounds to `enrich_location()` for affected rooms and hierarchy:

```python
from scripts.integrated_analysis.location import enrich_location, parse_tbm_grid

# Parse grid using TBM-specific parser
grid_parsed = parse_tbm_grid(location_row)

# Use centralized enrichment with pre-parsed bounds
result = enrich_location(
    building='FAB',
    level='2F',
    location_text=location_row,  # For room/stair/elevator extraction
    source='TBM',
    # Pre-parsed grid bounds
    grid_row_min=grid_parsed['grid_row_min'],
    grid_row_max=grid_parsed['grid_row_max'],
    grid_col_min=grid_parsed['grid_col_min'],
    grid_col_max=grid_parsed['grid_col_max'],
    grid_type=grid_parsed['grid_type'],
)
```

### DataFrame Usage

```python
from scripts.integrated_analysis.location import enrich_location

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
```

## Module Structure

```
location/
├── __init__.py                    # Public API
├── CLAUDE.md                      # This file
├── core/
│   ├── __init__.py
│   ├── normalizers.py             # Building/level normalization
│   ├── extractors.py              # P6 extraction patterns
│   ├── pattern_extractor.py       # Room/stair/elevator from text
│   ├── grid_parser.py             # Centralized grid parsing (simple patterns)
│   └── tbm_grid_parser.py         # TBM-specific grid parsing (103+ patterns)
├── dimension/
│   └── __init__.py                # (Future) Dimension table builders
├── enrichment/
│   ├── __init__.py
│   ├── location_enricher.py       # Main enrichment function
│   └── p6_location.py             # P6 schedule location extraction
└── validation/
    └── __init__.py                # Coverage validation
```

## Grid Parser

The `parse_grid()` function in `core/grid_parser.py` handles common grid formats:

```python
from scripts.integrated_analysis.location import parse_grid

result = parse_grid("G/10")           # POINT
result = parse_grid("A-N/1-3")        # RANGE
result = parse_grid("C/11 - C/22")    # TBM RANGE format
result = parse_grid("J~K/8~15")       # TBM tilde format

# Result contains:
result.grid_row_min  # 'G'
result.grid_row_max  # 'G'
result.grid_col_min  # 10.0
result.grid_col_max  # 10.0
result.grid_type     # 'POINT', 'RANGE', 'ROW_ONLY', 'COL_ONLY', 'NAMED'
```

For TBM's 103+ complex patterns, use `parse_tbm_grid()` from this module.

## Key Concepts

### Location Type

Describes what location information the record represents:

| Value | Meaning |
|-------|---------|
| `ROOM` | Direct room code (FAB116101) |
| `STAIR` | Stair location (STR-21) |
| `ELEVATOR` | Elevator location (ELV-03) |
| `GRIDLINE` | Grid coordinates (rooms inferred via affected_rooms) |
| `LEVEL` | Building + level only |
| `BUILDING` | Building-wide |
| `UNDEFINED` | No usable location info |

### Match Type

How the `dim_location_id` was determined:

| Value | Meaning |
|-------|---------|
| `ROOM_DIRECT` | Room/stair/elevator code found in text |
| `GRID_SINGLE` | Grid overlaps exactly one room |
| `GRID_MULTI` | Grid overlaps multiple rooms (see affected_rooms) |
| `GRIDLINE` | Grid but no room overlap found |
| `LEVEL` | Building + level fallback |
| `BUILDING` | FAB1 (project-wide) fallback |
| `UNDEFINED` | Final fallback |

### Location Hierarchy

Priority order for `dim_location_id`:

1. **ROOM_DIRECT**: Room/stair/elevator code found
2. **GRID_SINGLE**: Single room from grid overlap
3. **GRID_MULTI**: First room from grid overlap (use affected_rooms for all)
4. **GRIDLINE**: Gridline location when no rooms match
5. **LEVEL**: Building + level fallback
6. **BUILDING**: FAB1 fallback
7. **UNDEFINED**: Final fallback

## Dependencies

This module uses functions from:
- `scripts.shared.dimension_lookup` - Affected rooms, location lookups
- Internal normalizers and pattern extractors

It does NOT depend on source-specific code (RABA, PSI, TBM).

## Migration Status

| Source | Centralized? | Notes |
|--------|--------------|-------|
| RABA | ✅ | Uses `enrich_location()` |
| PSI | ✅ | Uses `enrich_location()` |
| TBM | ✅ | Uses `enrich_location()` with pre-parsed bounds |
| P6 | ✅ | Uses `extract_p6_location()` |
| ProjectSight | ❌ | No location data in source |
| NCR | ❌ | No location data in source |

## Future Work

1. **Add grid coverage bridge** generator for Power BI heatmaps
2. **Add validation scripts** for location coverage reporting
