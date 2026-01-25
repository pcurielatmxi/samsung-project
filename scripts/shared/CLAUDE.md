# Shared Utilities

**Last Updated:** 2026-01-25

## Purpose

Cross-source utility modules for entity standardization, location mapping, and dimension lookups.

## Modules

| Module | Purpose | Used By |
|--------|---------|---------|
| `company_standardization.py` | Canonical names for companies, trades, inspectors, levels | All sources |
| `dimension_lookup.py` | Map raw values → dimension IDs (location, company, trade) | RABA, PSI |
| `location_model.py` | High-level location API: room ↔ grid conversions | P6, Quality |
| `gridline_mapping.py` | Low-level grid coordinate lookup from Excel mapping | location_model |
| `qc_inspection_schema.py` | Unified column schema for RABA + PSI Power BI output | RABA, PSI |
| `location_parser.py` | Extract building/level/grid from location strings | RABA, PSI |
| `company_matcher.py` | Fuzzy match company names to canonical names | RABA, PSI |
| `shared_normalization.py` | Date, role, inspection type normalization | RABA, PSI |

## Key Functions

```python
# Dimension lookups
from scripts.shared.dimension_lookup import get_location_id, get_company_id, get_trade_id
loc_id = get_location_id('SUE', '1F')  # → 'SUE-1F'
company_id = get_company_id('Berg')    # → 4 (fuzzy match)

# Location model
from scripts.shared.location_model import get_grid_bounds, get_locations_at_grid
bounds = get_grid_bounds('FAB112345')  # → row/col min/max
rooms = get_locations_at_grid('G', 10) # → ['FAB112345', ...]

# Location parsing
from scripts.shared.location_parser import parse_location
loc = parse_location('FAB 1F Grid G/10')  # → {building, level, grid, ...}

# Company matching
from scripts.shared.company_matcher import CompanyMatcher
matcher = CompanyMatcher(threshold=0.85)
canonical, score = matcher.match('Berg Electric')  # → ('Berg', 0.95)

# Party normalization
from scripts.shared.shared_normalization import normalize_parties
parties = normalize_parties(raw_parties, source='psi')
```

## Party Normalization

**Module:** `shared_normalization.py` → `normalize_parties()`

Fixes common LLM extraction errors in `parties_involved` arrays during postprocessing.

**Issues Fixed:**

| Issue | Example | Fix |
|-------|---------|-----|
| Trade as party | `{"name": "Arch", "role": "trade"}` | Filtered out |
| Company as inspector | `{"name": "Intertek PSI", "role": "inspector"}` | Role → `inspection_company` |
| Mixed person/company | `{"name": "Chris Plassmann/YATES"}` | Split into 2 entries |
| Person as contractor | `{"name": "Kevin Jeong", "role": "contractor"}` | Role → `client_contact` |

**Output fields:** `name`, `role`, `company`, `entity_type` (person/company)

**Known persons list:** Kevin Jeong (client_contact), Mark Hammond, Saad Ekab, Chris Plassmann, etc. → contractor_rep

**Applied in:** `scripts/psi/document_processing/postprocess.py`, `scripts/raba/document_processing/postprocess.py`

## Data Dependencies

- `raw/location_mappings/Samsung_FAB_Codes_by_Gridline_3.xlsx` - Grid coordinate source
- `processed/integrated_analysis/dimensions/` - Dimension tables (dim_location, dim_company, dim_trade)

## Quality Data Audit Tool

**Script:** `spotcheck_quality_data.py`

Spot-check RABA/PSI quality data against source PDFs using Gemini to audit classifications.

```bash
# Check 5 samples per outcome
python -m scripts.shared.spotcheck_quality_data raba --samples 5
python -m scripts.shared.spotcheck_quality_data psi --samples 5

# Check specific outcome with verbose output
python -m scripts.shared.spotcheck_quality_data raba --outcome FAIL --samples 10 --verbose

# Save full report
python -m scripts.shared.spotcheck_quality_data raba --samples 10 --output report.json
```

**Features:**
- Samples N records per outcome category
- Uses Gemini to re-evaluate source PDFs
- Compares against current classification
- Flags mismatches with reasoning
- Supports `--use-embeddings` for additional detection
