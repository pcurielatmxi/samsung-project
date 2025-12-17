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
| `dim_location` | Building + Level standardization | location_id, building, level |
| `dim_trade` | Trade/work type classification | trade_id, trade_code, trade_name |
| `dim_time` | Calendar dimension | date_id, year, month, week |

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

The integration layer operates at **Building + Level** granularity:
- Sufficient coverage across all sources (66-98%)
- Enables meaningful spatial analysis
- Finer granularity (gridline, room) has inconsistent coverage

## Data Traceability

Per project guidelines, outputs are classified as **derived** data:
- Includes assumptions and inference
- NOT fully traceable to raw sources
- Company aliases are curated
- Location inference uses statistical distribution
