# Mapping Tables

Mapping tables are stored in the external data folder (not in this repo):

```
{WINDOWS_DATA_DIR}/processed/integrated_analysis/mappings/
```

## Files

| File | Description |
|------|-------------|
| `map_company_aliases.csv` | Company name aliases -> company_id |
| `map_company_location.csv` | Company work areas by period |
| `map_location_codes.csv` | Location code normalization mappings |

## Access

These are loaded automatically by `scripts/shared/dimension_lookup.py`.

## Why External?

Mapping tables contain processed data that:
1. May be updated as new aliases are discovered
2. Include manual corrections and overrides
3. Should be versioned separately from analysis code
