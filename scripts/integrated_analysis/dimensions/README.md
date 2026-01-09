# Dimension Tables

Dimension tables are stored in the external data folder (not in this repo):

```
{WINDOWS_DATA_DIR}/derived/integrated_analysis/dimensions/
```

## Files

| File | Description |
|------|-------------|
| `dim_location.csv` | 523 location codes (rooms, elevators, stairs) with grid bounds |
| `dim_company.csv` | Master company list with canonical names |
| `dim_trade.csv` | Trade/work type classification |

## Access

Use `scripts/shared/dimension_lookup.py` for programmatic access:

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

Dimension tables contain derived data that:
1. May be updated more frequently than code
2. Include assumptions that need documentation
3. Should be versioned separately from analysis code
