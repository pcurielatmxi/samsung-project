# Integrated Analysis

This folder contains scripts and specifications for Phase 2 of the Samsung Taylor FAB1 analysis project: **Cross-Dataset Integration**.

## Purpose

Create dimension tables and mapping tables that enable joining data across the 6 primary sources:
- P6 Primavera (schedules)
- ProjectSight (labor hours)
- Weekly Labor Detail (labor hours)
- TBM Daily Plans (labor hours with location)
- Quality Records - Yates WIR
- Quality Records - SECAI IR

## Key Documents

- [PLAN.md](PLAN.md) - Full specification and implementation plan

## Core Problem

**Hours data lacks location.** Weekly Labor and ProjectSight have company and hours but no building/level. The integration layer solves this by:
1. Normalizing company names across sources
2. Inferring location from company's typical work areas (derived from P6, Quality, TBM)

## Dimension Tables

| Table | Purpose | Key Fields |
|-------|---------|------------|
| `dim_company` | Master company list with aliases | company_id, canonical_name, aliases |
| `dim_location` | Building + Level + Grid bounds | location_id, building, level, grid_row_min/max, grid_col_min/max |
| `dim_trade` | Trade/work type classification | trade_id, trade_code, trade_name |
| `dim_time` | Calendar dimension | date_id, year, month, week |

### Location Model

The location dimension is powered by the **grid-based spatial model** in `scripts/shared/`:

```
┌─────────────────────────────────────────────────────────────────┐
│                       dim_location                               │
│  location_code │ building │ level │ grid_row_min/max │ grid_col_min/max │
├─────────────────────────────────────────────────────────────────┤
│  FAB112345     │ SUE      │ 1F    │ B / E            │ 5 / 12           │
│  ELV-01        │ SUE      │ MULTI │ C / C            │ 8 / 8            │
└─────────────────────────────────────────────────────────────────┘
```

**Key Lookups:**
- **Forward:** Room code → Grid bounds (for P6 tasks)
- **Reverse:** Grid coordinate → Room(s) (for quality data with grids like "G/10")

**Supporting Files:**
- `scripts/shared/location_model.py` - High-level API
- `scripts/shared/gridline_mapping.py` - Low-level grid lookup
- `raw/location_mappings/Samsung_FAB_Codes_by_Gridline_3.xlsx` - Source mapping
- `raw/location_mappings/location_master.csv` - Working location master

## Mapping Tables

| Table | Purpose |
|-------|---------|
| `map_company_aliases` | Resolve source-specific names → company_id |
| `map_company_location` | Company → typical work locations by period |
| `map_location_codes` | Source location formats → location_id |

## Folder Structure

```
scripts/integrated_analysis/
├── PLAN.md              # Detailed specification
├── CLAUDE.md            # This file
├── dimensions/          # Dimension table builders
├── mappings/            # Mapping table builders
└── validate/            # Coverage validation
```

## Output Location

```
data/derived/integrated_analysis/
├── dimensions/          # dim_*.csv files
├── mappings/            # map_*.csv files
└── validation/          # Coverage reports
```

## Usage Notes

1. **Run dimension builders first** - mappings depend on dimensions
2. **Manual review required** - company aliases need human verification
3. **Period-aware** - company→location mappings vary by time period
4. **Confidence flags** - inferred locations should be flagged for transparency

## Integration Granularity

The integration layer operates at **two levels**:

**Building + Level** (primary):
- Sufficient coverage across all sources (66-98%)
- Used for company→location inference and aggregated metrics

**Room + Grid** (when available):
- Quality data (RABA/PSI) often includes grid coordinates
- P6 tasks have room codes with grid bounds (from mapping)
- Enables spatial join: quality inspections ↔ rooms via grid containment

## Data Traceability

Per project guidelines, outputs are classified as **derived** data:
- Includes assumptions and inference
- NOT fully traceable to raw sources
- Company aliases are curated
- Location inference uses statistical distribution
