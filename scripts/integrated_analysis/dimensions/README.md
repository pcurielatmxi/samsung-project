# Dimension Tables

Dimension tables are stored in the external data folder (not in this repo):

```
{WINDOWS_DATA_DIR}/processed/integrated_analysis/dimensions/
```

## Pipeline

```bash
# Run full location dimension pipeline
./run.sh location

# Run all dimensions
./run.sh all

# Check status
./run.sh status
```

## Location Dimension Pipeline

The `dim_location` table is built through a 4-stage pipeline:

```
Stage 1: generate_location_master.py
         P6 taxonomy → location_master.csv (497 unique locations)
              ↓
Stage 2: populate_grid_bounds.py
         Excel mapping → grid bounds for rooms (291 direct)
              ↓
Stage 3: extract_location_grids.py
         P6 task names → grid coordinates for stairs/elevators
              ↓
Stage 4: build_dim_location.py
         location_master → dim_location.csv (505 entries)
         Features:
         - Grid inference from sibling rooms (+24 rooms)
         - Drawing code extraction from PDFs
         - P6 code → drawing code conversion (STR-01 → FAB1-ST01)
         - in_drawings flag computation
```

Run individual stages:
```bash
./run.sh location-master   # Stage 1
./run.sh grid-bounds       # Stage 2
./run.sh extract-grids     # Stage 3
./run.sh dim-location      # Stage 4
```

## Files

| File | Records | Description |
|------|---------|-------------|
| `dim_location.csv` | 505 | Location codes (rooms, elevators, stairs) with grid bounds |
| `dim_company.csv` | ~80 | Master company list with canonical names |
| `dim_trade.csv` | 13 | Trade/work type classification |
| `dim_csi_section.csv` | 52 | CSI MasterFormat sections |

## dim_location Key Columns

| Column | Description |
|--------|-------------|
| `location_code` | Primary identifier (FAB112345, FAB1-ST05) |
| `p6_alias` | Original P6 code if converted (STR-05) |
| `location_type` | ROOM, STAIR, ELEVATOR, GRIDLINE, LEVEL, BUILDING, AREA, SITE |
| `grid_row_min/max` | Grid row bounds (A-Z) |
| `grid_col_min/max` | Grid column bounds (1-34) |
| `grid_inferred_from` | Source code if grid was inferred from sibling room |
| `in_drawings` | Whether found in PDF floor drawings |

## Programmatic Access

Use `scripts/shared/dimension_lookup.py`:

```python
from scripts.shared.dimension_lookup import get_location_id, get_company_id, get_trade_id

# Location lookup (building+level)
loc_id = get_location_id('FAB', '1F')  # -> 'FAB-1F'

# Company lookup (fuzzy matching)
company_id = get_company_id('Samsung E&C')  # -> 1

# Trade lookup
trade_id = get_trade_id('Drywall')  # -> 4
```

## Why External?

Dimension tables are stored externally because:
1. May be updated more frequently than code
2. Should be versioned separately from analysis code
3. Used by downstream tools (Power BI dashboards, reports)
