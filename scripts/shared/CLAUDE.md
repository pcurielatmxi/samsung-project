# Shared Utilities

Cross-source utility modules used by multiple data pipelines.

## Purpose

Centralize reusable logic that applies across data sources:
- Entity standardization (companies, trades, inspectors)
- Location model (grid lookups, room/elevator/stair mapping)
- Common transformations (dates, levels, categories)

## Modules

### `company_standardization.py`

Entity normalization for consistent cross-source analysis.

**Functions:**
| Function | Purpose |
|----------|---------|
| `standardize_company(name)` | Canonical company name |
| `standardize_inspector(name)` | Inspector name normalization |
| `standardize_trade(name)` | Trade name standardization |
| `standardize_level(level)` | Level format (1F, 2F, B1, ROOF) |
| `categorize_inspection_type(type)` | Map to ~15 categories |
| `categorize_failure_reason(reason)` | Failure categorization |
| `infer_trade_from_inspection_type(type)` | Trade from inspection context |
| `infer_level_from_location(location)` | Extract level from location text |

**Used by:** PSI consolidation, RABA postprocess, Quality records

### `gridline_mapping.py`

Low-level gridline coordinate lookup from FAB code mapping file.

**Key Classes:**
- `GridlineMapping` - Loads and caches mapping from Excel file

**Key Functions:**
| Function | Purpose |
|----------|---------|
| `get_gridline_bounds(type, code)` | Get grid bounds for location |
| `get_default_mapping()` | Get cached mapping instance |
| `normalize_room_code(code)` | FAB1XXXXX normalization |
| `normalize_elevator_code(code)` | ELV-XX → FAB1-ELXX |
| `normalize_stair_code(code)` | STR-XX → FAB1-STXX |

**Data Source:** `raw/location_mappings/Samsung_FAB_Codes_by_Gridline_3.xlsx`

**Used by:** P6 task taxonomy, location_model.py

### `location_model.py`

High-level location API for cross-source integration.

**Key Functions:**
| Function | Purpose |
|----------|---------|
| `get_grid_bounds(location_code)` | Forward lookup: location → grid bounds |
| `get_locations_at_grid(row, col)` | Reverse lookup: grid → locations |
| `location_contains_grid(code, row, col)` | Check containment |
| `parse_grid_string(grid_str)` | Parse "G/10" → ('G', 10.0) |
| `get_location_info(code)` | Full info with metadata |

**Usage:**
```python
from location_model import get_grid_bounds, get_locations_at_grid, parse_grid_string

# Forward lookup: Room → Grid bounds
bounds = get_grid_bounds('FAB112345')
# {'row_min': 'B', 'row_max': 'E', 'col_min': 5, 'col_max': 12}

# Reverse lookup: Grid → Rooms
row, col = parse_grid_string('G/10')
locations = get_locations_at_grid(row, col)
# ['FAB112345', 'FAB112346', ...]

# Check if quality inspection at G/10 is within FAB112345
if location_contains_grid('FAB112345', 'G', 10):
    print("Match!")
```

## Architecture

```
Quality Data (RABA/PSI)          P6 Tasks
       │                              │
       │ grid: "G/10"                 │ location_code: FAB112345
       │                              │
       ▼                              ▼
┌─────────────────────────────────────────────────────┐
│              scripts/shared/                         │
│                                                      │
│  location_model.py ─────► gridline_mapping.py       │
│       │                          │                   │
│       │                          ▼                   │
│       │               FAB_Codes_by_Gridline.xlsx    │
│       │                                              │
│       ▼                                              │
│  get_locations_at_grid('G', 10)                     │
│  get_grid_bounds('FAB112345')                       │
│                                                      │
└─────────────────────────────────────────────────────┘
                      │
                      ▼
              Spatial Join
        (Quality ↔ Room via grid bounds)
```

## Grid System Reference

- **Rows:** A through N (plus fractional: A.5, B.5, E.3, etc.)
- **Columns:** 1 through 34 (plus 32.5)
- **Buildings:** SUE spans east rows, SUW spans west rows, FAB varies by location

## Adding New Modules

When adding shared utilities:
1. Place in `scripts/shared/`
2. Add imports to consuming scripts
3. Document in this file
4. Keep modules focused (one responsibility)
