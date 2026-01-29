# Location Processing Module

**Last Updated:** 2026-01-29

## Purpose

Centralized location processing for all data sources. **Single source of truth** for:
- Building/level normalization
- Grid coordinate parsing
- Spatial matching (affected rooms)
- Location hierarchy resolution

**Why:** Consolidates ~700 lines of duplicated logic from RABA, PSI, TBM, and Fieldwire scripts.

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

TBM has 103+ complex grid patterns parsed by `parse_tbm_grid()`:

```python
from scripts.integrated_analysis.location import enrich_location, parse_tbm_grid

grid_parsed = parse_tbm_grid(location_row)

result = enrich_location(
    building='FAB',
    level='2F',
    location_text=location_row,
    source='TBM',
    grid_row_min=grid_parsed['grid_row_min'],
    grid_row_max=grid_parsed['grid_row_max'],
    grid_col_min=grid_parsed['grid_col_min'],
    grid_col_max=grid_parsed['grid_col_max'],
    grid_type=grid_parsed['grid_type'],
)
```

### DataFrame Usage

```python
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
│   ├── normalizers.py             # Building/level normalization
│   ├── extractors.py              # P6 extraction patterns
│   ├── pattern_extractor.py       # Room/stair/elevator from text
│   ├── grid_parser.py             # Simple grid parsing
│   └── tbm_grid_parser.py         # TBM-specific (103+ patterns)
├── enrichment/
│   ├── location_enricher.py       # Main enrich_location()
│   └── p6_location.py             # P6 schedule extraction
└── validation/                    # Coverage checks
```

## Grid Parser

```python
from scripts.integrated_analysis.location import parse_grid

result = parse_grid("G/10")           # POINT
result = parse_grid("A-N/1-3")        # RANGE
result = parse_grid("C/11 - C/22")    # TBM RANGE format

# Result: grid_row_min, grid_row_max, grid_col_min, grid_col_max, grid_type
```

## Key Concepts

### Location Type

| Value | Meaning |
|-------|---------|
| `ROOM` | Direct room code (FAB116101) |
| `STAIR` | Stair location (STR-21) |
| `ELEVATOR` | Elevator location (ELV-03) |
| `GRIDLINE` | Grid coordinates only |
| `LEVEL` | Building + level only |
| `BUILDING` | Building-wide |
| `UNDEFINED` | No usable location info |

### Match Type (how dim_location_id was determined)

| Value | Meaning |
|-------|---------|
| `ROOM_DIRECT` | Room/stair/elevator code found |
| `GRID_SINGLE` | Grid overlaps exactly one room |
| `GRID_MULTI` | Grid overlaps multiple rooms |
| `GRIDLINE` | Grid but no room overlap |
| `LEVEL` | Building + level fallback |
| `BUILDING` | FAB1 fallback |
| `UNDEFINED` | Final fallback |

### Location Hierarchy (priority order)

1. **ROOM_DIRECT** → 2. **GRID_SINGLE** → 3. **GRID_MULTI** → 4. **GRIDLINE** → 5. **LEVEL** → 6. **BUILDING** → 7. **UNDEFINED**

## Dependencies

Uses: `scripts.shared.dimension_lookup`

Does NOT depend on source-specific code (RABA, PSI, TBM).

## Migration Status

| Source | Centralized | Notes |
|--------|-------------|-------|
| RABA | ✅ | Uses `enrich_location()` |
| PSI | ✅ | Uses `enrich_location()` |
| TBM | ✅ | Uses `enrich_location()` with pre-parsed bounds |
| P6 | ✅ | Uses `extract_p6_location()` |
| ProjectSight | N/A | No location data |
| NCR | N/A | No location data |
