# Integrated Analysis - Phase 2 Plan

**Created:** 2025-12-17
**Status:** In Progress
**Purpose:** Define dimension tables and mapping tables to enable cross-dataset analysis
**Updated:** 2025-12-17 - Implemented dim_trade, dim_company, map_company_aliases

---

## Executive Summary

This document formalizes the plan for Phase 2 of the Samsung Taylor FAB1 data analysis project. Phase 1 established data collection and initial assessment across 6 data sources. Phase 2 creates an integration layer that enables cross-source analysis by:

1. Standardizing entity references (companies, locations) across sources
2. Building dimension tables with consistent granularity
3. Creating mapping tables to infer missing attributes (e.g., location for hours data)

The primary goal is to **tie together Quality records and Hours data to Locations and Companies/Trades** for delay attribution and resource consumption analysis.

---

## Data Source Inventory

### Source Timeline

| Source | Date Range | Records | Has Location | Has Company | Has Hours |
|--------|------------|---------|--------------|-------------|-----------|
| P6 Primavera | 2020-01 to 2025-10 | 66 files | ✓ | ✓ | ✗ |
| ProjectSight | 2022-06 to 2025-12 | 857,516 | ✗ | ✓ | ✓ |
| Weekly Labor | 2022-08 to 2023-06 | 13,205 | ✗ | ✓ | ✓ |
| Quality-Yates | 2020-07 to 2027-08 | 19,876 | ✓ | ✓ | ✗ |
| Quality-SECAI | 2023-11 to 2025-10 | 17,146 | ✓ | ✓ | ✗ |
| TBM Daily | 2025-03 to 2025-12 | 13,539 | ✓ | ✓ | ✓ |

### Key Finding: Location Gap in Hours Data

Two major hours sources lack location data:
- **Weekly Labor Detail** (13,205 records) - No location fields
- **ProjectSight Labor** (857,516 records) - No location fields

Only **TBM Daily Plans** has direct location for hours, but covers only Mar-Dec 2025.

---

## Location Granularity Analysis

### Coverage at Building + Level

| Source | Building Coverage | Level Coverage | Building+Level |
|--------|-------------------|----------------|----------------|
| Quality-Yates | 83.6% | 98.9% | 83.1% |
| Quality-SECAI | 99.6% | 66.3% | 66.1% |
| TBM Daily | 99.6% | 98.5% | 98.2% |
| P6 (Z-BLDG) | 37% of tasks | 59% (Z-LEVEL) | ~28% overlap |

### Recommended Granularity

**Building + Level** is the highest granularity with sufficient coverage across sources:
- Quality: 66-83% coverage
- TBM: 98% coverage
- P6: Available via activity codes and derived taxonomy

Finer granularity (gridline, room) has inconsistent coverage and format differences across sources.

---

## Company Name Normalization

### Problem Statement

Company names vary significantly across sources:

| Canonical | TBM | Weekly Labor | ProjectSight | Quality | P6 |
|-----------|-----|--------------|--------------|---------|-----|
| Baker | `Baker` | `BAKER CONCRETE CONSTRUCTION INC` | `BAKER CONCRETE CONSTRUCTION INC` | `BAKER` | `BAKER` |
| Berg | `BERG` | - | `BERG DRYWALL LLC` | `BERG` | `BERG` |
| Brazos | `Brazos Urethane` | `BRAZOS URETHANE INC` | `BRAZOS URETHANE INC` | `BRAZOS` | `BRAZOS` |
| Cherry | `CHERRY COATINGS` | - | `CHERRY PAINTING COMPANY...` | `CHERRY`, `CHERRY COATINGS` | `CHERRY` |
| Patriot | `Patriot Erectors, LLC` | `PATRIOT ERECTORS LLC` | `PATRIOT ERECTORS LLC` | `PATRIOT` | `PATRIOT` |

### Solution: `dim_company` with Alias Mapping

Create a master company table with:
- Canonical name (standardized)
- Array of known aliases from each source
- Primary trade association
- Tier classification (GC, T1 Sub, T2 Sub)

---

## Company → Location Inference Strategy

### Problem Statement

Hours data from Weekly Labor and ProjectSight cannot be directly tied to locations. However, companies tend to work in consistent areas during project phases.

### Available Sources for Inference

| Period | Primary Source | Data Available | Coverage |
|--------|---------------|----------------|----------|
| 2022-06 to 2025-02 | P6 Activity Codes | Z-SUB + Z-BLDG on same task | 56,764 tasks |
| 2024-03 to 2025-06 | Quality Records | Contractor + Building/Level | 37K records |
| 2025-03 to 2025-12 | TBM Daily Plans | Subcontractor + Building/Level | 13.5K records |

### P6 Activity Code Analysis

```
Z-SUB CONTRACTOR assignments: 92,852 tasks
Z-BLDG assignments:          219,546 tasks
Tasks with BOTH:              56,764 tasks (usable for mapping)
```

P6 subcontractors with building data:
- BERG: 19,384 tasks
- BRAZOS: 10,506 tasks
- BAKER: 6,932 tasks
- CHERRY: 6,483 tasks
- KOVACH: 5,801 tasks
- PATRIOT: 5,357 tasks
- (and 17 more)

### Inference Approach

For each company, calculate their work distribution by building+level from available sources:

```python
# Pseudocode
for company in companies:
    # Get location distribution from P6 (2022-2025)
    p6_distribution = get_p6_company_locations(company, period)

    # Supplement with Quality data (2024-2025)
    quality_distribution = get_quality_company_locations(company, period)

    # Use TBM for validation (2025)
    tbm_distribution = get_tbm_company_locations(company, period)

    # Merge with confidence weighting
    final_distribution = merge_distributions(p6, quality, tbm)
```

### Location Consistency Validation (from TBM)

Companies work in concentrated areas (Top 3 locations account for 77-99% of work):

| Company | Top 3 Locations | Concentration |
|---------|-----------------|---------------|
| KOVACH | FAB-1F, FAB-2F, FAB-3F | 98.6% |
| MK Marlow | FAB-2F, FAB-1F, FAB-3F | 98.1% |
| LATCON | SUP-3F, SUP-1F, SUP-2F | 95.1% |
| Rolling Plains | FAB-3F, FAB-1F, FAB-4F | 90.1% |
| BERG | FAB-3F, FAB-2F, FAB-1F | 79.5% |

This validates that company→location inference is reasonable.

---

## Dimension Table Specifications

### 1. `dim_company`

Master table of companies with alias resolution.

| Column | Type | Description |
|--------|------|-------------|
| company_id | INT | Primary key |
| canonical_name | STRING | Standardized company name |
| short_code | STRING | Abbreviation (e.g., "BERG", "BAKER") |
| tier | ENUM | 'OWNER', 'GC', 'T1_SUB', 'T2_SUB' |
| primary_trade_id | INT | FK to dim_trade (single primary trade per company) |
| notes | STRING | Additional context |

**Design Decision:** Each company has ONE primary trade. Analysis showed most subcontractors are concentrated in a single trade when using broad categories. Multi-trade appearances (e.g., Berg with concrete/drywall) occur when GC (Yates) assigns work - excluded from analysis since Yates is the GC.

**Status:** ✅ Implemented - see `dimensions/dim_company.csv` (30 companies)

### 2. `dim_location`

Standardized location hierarchy at Building + Level granularity.

| Column | Type | Description |
|--------|------|-------------|
| location_id | STRING | Primary key (e.g., "FAB-L1") |
| building | STRING | Building code (FAB, SUE, SUW, FIZ, CUB, GCS) |
| level | STRING | Level code (L1-L6, ROOF, B1, UG) |
| building_full | STRING | Full building name |
| level_full | STRING | Full level name |
| sort_order | INT | For display ordering |

**Building codes:**
- FAB: Main FAB Building
- SUE: Support Building East
- SUW: Support Building West
- FIZ: FAB Integration Zone
- CUB: Central Utilities Building
- GCS: Gas/Chemical Supply
- GCSA/GCSB: GCS Buildings A & B
- OB1/OB2: Office Buildings

**Status:** ✅ Implemented - see `dimensions/dim_location.csv` (57 locations)

### 3. `dim_trade`

Trade/work type classification using **broad categories** that facilitate 1:many company-trade relationships.

| Column | Type | Description |
|--------|------|-------------|
| trade_id | INT | Primary key |
| trade_code | STRING | Standard code |
| trade_name | STRING | Display name |
| phase | STRING | Typical project phase (STR, ENC, INT, ADM) |
| csi_division | STRING | Related CSI division(s) |
| description | STRING | Work types included |

**Trade Categories (12):**

| ID | Code | Name | Phase | CSI | Work Types |
|----|------|------|-------|-----|------------|
| 1 | CONCRETE | Concrete | STR | 03 | CIP, topping, SOG, elevated slabs |
| 2 | STEEL | Structural Steel | STR | 05 | Erection, decking, misc steel |
| 3 | ROOFING | Roofing & Waterproofing | ENC | 07 | Roofing, waterproofing, EIFS |
| 4 | DRYWALL | Drywall & Framing | INT | 09 | Metal stud, gypsum board |
| 5 | FINISHES | Architectural Finishes | INT | 09 | Paint, flooring, tile, doors |
| 6 | FIREPROOF | Fire Protection | INT | 07 | SFRM, firestop, fire caulk |
| 7 | MEP | MEP Systems | INT | 22-26 | Mechanical, electrical, plumbing |
| 8 | INSULATION | Insulation | INT | 07 | Thermal, pipe insulation |
| 9 | EARTHWORK | Earthwork & Foundations | STR | 31 | Excavation, backfill, piers |
| 10 | PRECAST | Precast Concrete | STR | 03 | Precast panels, erection |
| 11 | PANELS | Metal Panels & Cladding | ENC | 07 | Wall panels, IMP, skin |
| 12 | GENERAL | General Conditions | ADM | 01 | GC, owner activities |

**Status:** ✅ Implemented - see `dimensions/dim_trade.csv` and `dimensions/map_trade_codes.csv`

### 4. `dim_time` - NOT IMPLEMENTED

> **Note:** Time dimension deferred. Standard date fields in source data are sufficient for current analysis needs. Can be added later if calendar-based aggregations require special handling (fiscal periods, project phases, etc.).

---

## Mapping Table Specifications

### 1. `map_company_aliases`

Resolves source-specific company names to canonical IDs.

| Column | Type | Description |
|--------|------|-------------|
| source | STRING | Source system (TBM, LABOR, PS, QUALITY, P6) |
| alias | STRING | Company name as it appears in source |
| company_id | INT | FK to dim_company |
| confidence | FLOAT | Match confidence (1.0 = exact, 0.8 = fuzzy) |
| notes | STRING | Additional context |

**Status:** ✅ Implemented - see `mappings/map_company_aliases.csv` (~90 aliases)

### 2. `map_company_location`

Company work distribution by location and period.

| Column | Type | Description |
|--------|------|-------------|
| company_id | INT | FK to dim_company |
| location_id | STRING | FK to dim_location |
| period_start | DATE | Start of period |
| period_end | DATE | End of period |
| pct_of_work | FLOAT | Percentage of company's work at this location |
| record_count | INT | Number of source records |
| source | STRING | Primary data source for this mapping |

**Estimated rows:** ~500-800 mappings

### 3. `map_location_codes`

Resolves source-specific location references to standardized IDs.

| Column | Type | Description |
|--------|------|-------------|
| source_code | STRING | Location as it appears in source |
| source | STRING | Source system |
| location_id | STRING | FK to dim_location |
| extraction_rule | STRING | How this mapping was derived |

**Status:** ✅ Implemented - see `mappings/map_location_codes.csv` (~65 mappings)

---

## Implementation Plan

### Phase 2.1: Foundation

1. **Create folder structure** ✅ DONE
   ```
   scripts/integrated_analysis/
   ├── PLAN.md (this document)
   ├── CLAUDE.md (usage instructions)
   ├── dimensions/
   │   ├── dim_company.csv         ✅
   │   ├── dim_location.csv        ✅
   │   ├── dim_trade.csv           ✅
   │   └── map_trade_codes.csv     ✅
   ├── mappings/
   │   ├── map_company_aliases.csv ✅
   │   ├── map_company_location.csv ⏳
   │   └── map_location_codes.csv  ✅
   └── validate/
       └── validate_coverage.py    ⏳
   ```

2. **Build `dim_company`** ✅ DONE
   - Extract unique company names from all sources
   - Manual curation of canonical names and aliases
   - Associate primary trades (using 12 broad categories)

3. **Build `dim_location`** ✅ DONE
   - 57 building + level combinations
   - Buildings: FAB, SUE, SUW, SUP, FIZ, CUB, GCS, GCSA, GCSB, OB1, OB2, SITE
   - Levels: UG, B1, 1F-6F, ROOF

### Phase 2.2: Mapping Tables

4. **Build `map_company_aliases`** ✅ DONE
   - Source-specific alias registration
   - ~90 aliases across P6, LABOR, PS, QUALITY, TBM

5. **Build `map_company_location`** ⏳ NEXT
   - Extract from P6 (Z-SUB + Z-BLDG)
   - Extract from Quality (Contractor + Location)
   - Extract from TBM (direct)
   - Merge with period awareness

6. **Build `map_location_codes`** ✅ DONE
   - ~65 mappings for TBM, QUALITY, P6 source formats
   - Normalization rules for level naming (L1→1F, RF→ROOF)

### Phase 2.3: Validation & Integration (Week 3)

7. **Validate coverage**
   - Measure company resolution rate per source
   - Measure location inference accuracy
   - Document gaps and limitations

8. **Create integrated views**
   - Hours by Company + Location + Date
   - Quality by Company + Location + Date
   - Cross-source correlation tables

---

## Output Files

Dimension and mapping files are stored in the repository under `scripts/integrated_analysis/`:

```
scripts/integrated_analysis/
├── dimensions/
│   ├── dim_company.csv        ✅ Implemented (30 companies)
│   ├── dim_location.csv       ✅ Implemented (57 locations)
│   ├── dim_trade.csv          ✅ Implemented (12 trades)
│   └── map_trade_codes.csv    ✅ Implemented (~70 mappings)
├── mappings/
│   ├── map_company_aliases.csv    ✅ Implemented (~90 aliases)
│   ├── map_company_location.csv   ⏳ Pending
│   └── map_location_codes.csv     ✅ Implemented (~65 mappings)
└── validation/
    ├── coverage_report.csv    ⏳ Pending
    └── unmapped_entities.csv  ⏳ Pending
```

Note: These are **derived** data files (include assumptions/curation). Per project data traceability guidelines, outputs to external data directory would go to `data/derived/integrated_analysis/`.

---

## Success Criteria

1. **Company Resolution:** ≥95% of hours records can be mapped to a canonical company
2. **Location Inference:** ≥80% of hours records can be assigned a location (Building+Level)
3. **Quality Linkage:** ≥90% of quality records have both company and location
4. **Cross-Source Join:** Ability to aggregate Hours + Quality by Company + Location + Week

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Company names too varied | Low resolution rate | Manual curation + fuzzy matching |
| Location inference inaccurate | Misleading analysis | Validate against TBM; flag low-confidence |
| Time period gaps | Incomplete picture | Use P6 for historical, document coverage |
| Building code inconsistency (SUP vs SUE/SUW) | Join failures | Explicit mapping rules |

---

## Questions for Review

1. **Building granularity:** Should SUE and SUW be kept separate or combined as SUP for some analyses?

2. **Time periods:** Should `map_company_location` use monthly, quarterly, or custom periods aligned to project phases?

3. **Confidence thresholds:** What minimum confidence should be required for location inference to be used in analysis?

4. ~~**Trade association:** Should companies be associated with a single primary trade or multiple trades with percentages?~~
   - **RESOLVED:** Single primary trade per company. Broad trade categories (12 total) allow clean 1:many relationships.

---

## Appendix A: Building Code Reference

| Code | Full Name | Notes |
|------|-----------|-------|
| FAB | Main FAB Building | Primary manufacturing facility |
| SUE | Support Building East | SEA/SEB areas |
| SUW | Support Building West | SWA/SWB areas |
| SUP | Support Building (Generic) | Used when E/W not distinguished |
| FIZ | FAB Integration Zone | Data center area |
| CUB | Central Utilities Building | |
| GCS | Gas/Chemical Supply | |
| GCSA | GCS Building A | |
| GCSB | GCS Building B | |
| OB1 | Office Building 1 | |
| OB2 | Office Building 2 | |

## Appendix B: P6 Activity Code Reference

| Code Type | Purpose | Coverage |
|-----------|---------|----------|
| Z-BLDG | Building assignment | 37% of tasks |
| Z-LEVEL | Level assignment | 59% of tasks |
| Z-AREA | Area/zone assignment | 61% of tasks |
| Z-ROOM | Room assignment | 21% of tasks |
| Z-TRADE | Trade/work type | 48% of tasks |
| Z-SUB CONTRACTOR | Subcontractor | 28% of tasks |

## Appendix C: Company List (Preliminary)

Based on analysis, the following companies appear across sources:

1. Baker (Concrete)
2. Berg (Drywall)
3. Brazos (Urethane/Coatings)
4. Cherry Coatings (Painting)
5. Patriot (Steel Erection)
6. Kovach (Enclosures)
7. MK Marlow (MEP)
8. Rolling Plains (Roofing)
9. W&W Steel (Structural Steel)
10. FD Thomas (Fireproofing)
11. Alpha (Painting)
12. Apache (Insulation)
13. Infinity (Insulation)
14. LATCON (Steel)
15. GDA (General)
16. Grout Tech (Grouting)
17. Yates (GC)
18. SECAI (Owner)
19. AH Beck (Foundations)
20. Cobb Mechanical (MEP)
21. Perry & Perry (Specialties)
22. Alert Lock & Key (Hardware)
23. Spectra (Specialties)

---

*Document Version: 1.2*
*Last Updated: 2025-12-17*
*Changes: Added dim_location (57 locations) and map_location_codes (~65 mappings). Only map_company_location remains pending.*
