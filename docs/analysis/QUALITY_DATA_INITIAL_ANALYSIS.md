# Quality Inspection Data - Initial Analysis

**Date:** 2026-01-08
**Data Sources:** PSI (Intertek/Construction Hive), RABA (Raba Kistner/Celvis)
**Analyst:** MXI

---

## Executive Summary

This document presents the initial analysis of quality inspection data from the Samsung Taylor FAB1 project. Two third-party inspection firms provide complementary coverage:

- **PSI (Intertek)**: 6,309 architectural inspections (drywall, framing) - Apr 2024 to Dec 2025
- **RABA (Raba Kistner)**: 9,391 structural/MEP inspections (concrete, welding, firestop) - Jun 2022 to Dec 2025

**Key Findings:**
1. Overall pass rates are healthy (PSI: 83.5%, RABA: 77.6%)
2. Two contractors show elevated failure rates requiring attention (AMTS: 25.4%, JP Hi-Tech: 20.0%)
3. Data quality is good - apparent "missing level" in RABA is legitimate foundation work
4. Complementary inspection scopes provide comprehensive quality coverage

---

## 1. Data Overview

### 1.1 Record Counts and Date Ranges

| Dataset | Records | Date Range | Duration |
|---------|---------|------------|----------|
| PSI | 6,309 | 2024-04-08 to 2025-12-22 | 20 months |
| RABA | 9,391 | 2022-06-04 to 2025-12-23 | 42 months |
| **Combined** | **15,700** | 2022-06-04 to 2025-12-23 | 42 months |

### 1.2 Inspection Scope Differentiation

**PSI (Intertek) - Architectural Finishes:**
- Framing inspections
- 1st/2nd/3rd layer drywall
- Screw inspections
- Bottom plate inspections
- Cleanroom gypsum panels

**RABA (Raba Kistner) - Structural/MEP:**
- Compressive strength testing (concrete)
- Drilled pier inspections
- Welding inspections (visual)
- Firestop and fire resistive joint systems
- Pre-placement inspections (reinforcing steel)
- Nuclear density testing (soil/base)

---

## 2. Outcome Analysis

### 2.1 Overall Outcomes

**PSI Outcomes:**
| Outcome | Count | Percentage |
|---------|-------|------------|
| PASS | 5,268 | 83.5% |
| FAIL | 591 | 9.4% |
| CANCELLED | 340 | 5.4% |
| PARTIAL | 110 | 1.7% |

**RABA Outcomes:**
| Outcome | Count | Percentage |
|---------|-------|------------|
| PASS | 7,286 | 77.6% |
| PARTIAL | 1,658 | 17.7% |
| FAIL | 447 | 4.8% |

### 2.2 Failure Rates

| Dataset | Failures | Pass+Fail Total | Failure Rate |
|---------|----------|-----------------|--------------|
| PSI | 591 | 5,859 | **10.1%** |
| RABA | 447 | 7,733 | **5.8%** |

**Note:** RABA has higher PARTIAL rate (17.7%) which may represent inspections requiring follow-up but not outright failures.

---

## 3. Temporal Distribution

### 3.1 Monthly Inspection Volume

**PSI Activity (2024-2025):**
```
2024-04:   29
2024-05:   32
2024-06:  167  ███
2024-07:  277  █████
2024-08:  492  █████████
2024-09:  457  █████████
2024-10:  467  █████████
2024-11:  436  ████████
2024-12:  218  ████
2025-01:  111  ██
2025-02:  298  █████
2025-03:  373  ███████
2025-04:  160  ███
2025-05:  240  ████
2025-06:  485  █████████
2025-07:  341  ██████
2025-08:  296  █████
2025-09:  365  ███████
2025-10:  626  ████████████  ← Peak month
2025-11:   99  █
2025-12:  340  ██████
```

**RABA Activity (2022-2025):**
```
2022-06:   62  █       ← Project start
2022-07:  125  ██
2022-08:  211  ████
...
2024-03:   83  █       ← Low point
...
2025-05:  729  ██████████████  ← Major spike (3x normal)
2025-06:  414  ████████
2025-10:  529  ██████████
```

### 3.2 Temporal Insights

1. **Phase Alignment**: RABA started 2 years before PSI, reflecting construction sequence (foundations → finishes)
2. **May 2025 Spike**: RABA shows 729 inspections (3x normal) - likely push for structural completion
3. **Oct 2025 Surge**: Both datasets show increased activity - final project push
4. **Seasonal Dips**: Lower activity in Dec-Jan periods (holidays)

---

## 4. Location Analysis

### 4.1 Building Distribution

| Building | PSI | RABA |
|----------|-----|------|
| FAB | 6,270 (99.4%) | 9,377 (99.9%) |
| SUW | 17 | - |
| SUE | 14 | - |
| OB1 | - | 7 |
| Other | 8 | 7 |

**Observation:** Nearly all inspections are in the main FAB building.

### 4.2 Level Distribution

**PSI Levels:**
| Level | Count | % |
|-------|-------|---|
| 3F | 1,775 | 28.1% |
| 1F | 1,489 | 23.6% |
| 2F | 1,164 | 18.5% |
| 4F | 836 | 13.3% |
| Unknown | 442 | 7.0% |
| Other | 603 | 9.6% |

**RABA Levels:**
| Level | Count | % |
|-------|-------|---|
| Unknown | 3,541 | 37.7% |
| 1F | 2,127 | 22.6% |
| 3F | 1,515 | 16.1% |
| 2F | 921 | 9.8% |
| 4F | 738 | 7.9% |
| Other | 549 | 5.8% |

### 4.3 RABA "Missing Level" Analysis

The 37.7% of RABA records without level is **NOT a data quality issue**. Analysis of location_raw values shows:

| Pattern | Count | % of Missing |
|---------|-------|--------------|
| Grid references only (pier/foundation) | 2,786 | 78.7% |
| Other legitimate (utilities, sitewide) | 625 | 17.7% |
| Foundation/pier work | 121 | 3.4% |
| Actually missing (extraction issue) | 8 | 0.2% |

**Conclusion:** RABA covers early construction phases (foundations, piers, utilities) that don't have building levels. This is expected and correct.

---

## 5. Contractor Performance Analysis

### 5.1 PSI Contractor Failure Rates

| Contractor | Total | Pass | Fail | Fail Rate | Assessment |
|------------|-------|------|------|-----------|------------|
| AMTS | 410 | 257 | 104 | **25.4%** | HIGH CONCERN |
| JP Hi-Tech | 55 | 27 | 11 | **20.0%** | HIGH CONCERN |
| Axios | 439 | 356 | 49 | 11.2% | Elevated |
| Berg | 1,281 | 1,063 | 137 | 10.7% | Elevated |
| Baker Triangle | 203 | 172 | 22 | 10.8% | Elevated |
| MK Marlow | 349 | 311 | 22 | 6.3% | Acceptable |
| Yates | 2,637 | 2,323 | 148 | 5.6% | Good |
| Samsung E&C | 63 | 55 | 4 | 6.3% | Acceptable |
| Austin Bridge | 180 | 172 | 6 | 3.3% | Excellent |

### 5.2 RABA Contractor Failure Rates

| Contractor | Total | Pass | Fail | Fail Rate | Assessment |
|------------|-------|------|------|-----------|------------|
| Berg | 69 | 36 | 5 | 7.2% | Acceptable |
| Baker Concrete | 86 | 79 | 4 | 4.7% | Good |
| Samsung E&C | 6,349 | 5,053 | 295 | 4.6% | Good |
| Yates | 1,090 | 765 | 28 | 2.6% | Excellent |
| Hensel Phelps | 351 | 287 | 2 | **0.6%** | Excellent |
| Austin Bridge | 60 | 47 | 0 | **0.0%** | Excellent |

### 5.3 Key Findings

**Contractors Requiring Attention:**
1. **AMTS** - 25.4% failure rate on 410 inspections (PSI architectural work)
2. **JP Hi-Tech** - 20.0% failure rate on 55 inspections (PSI architectural work)

**Top Performing Contractors:**
1. **Hensel Phelps** - 0.6% failure rate (RABA structural)
2. **Austin Bridge** - 0.0-3.3% failure rate (both datasets)
3. **Yates** - 2.6-5.6% failure rate (both datasets, high volume)

---

## 6. Failure Analysis

### 6.1 PSI Common Failure Reasons

| Reason | Count |
|--------|-------|
| Missing screws | 17+ |
| Not compliant with construction documents | 7+ |
| Screws in solid part of sliptrack | 3+ |
| Didn't pass internal inspection | 3+ |
| Framing deformation/misalignment | 2+ |

### 6.2 RABA Common Failure Reasons

| Reason | Count |
|--------|-------|
| 28-day compressive strength not achieved | 37 |
| Not in accordance with project documents | 20 |
| Cancelled - didn't pass Yates internal inspection | 19 |
| Not in accordance with submitted documents | 13 |
| Uncured coating / contamination | 3+ |

### 6.3 Failure Pattern Insights

1. **PSI failures** are often workmanship issues (screws, alignment)
2. **RABA failures** include material performance issues (concrete strength)
3. **Internal inspections** catch issues before third-party inspection
4. Some "failures" are actually cancellations/rescheduling

---

## 7. Data Quality Assessment

### 7.1 Field Completeness

**PSI:**
| Field | Completeness | Status |
|-------|--------------|--------|
| inspection_id | 100.0% | ✓ |
| report_date | 100.0% | ✓ |
| outcome | 100.0% | ✓ |
| building | 99.9% | ✓ |
| level | 93.0% | ✓ |
| contractor | 91.1% | ✓ |
| inspector | 99.0% | ✓ |
| trade | 38.1% | ⚠ (many records don't specify) |
| summary | 100.0% | ✓ |

**RABA:**
| Field | Completeness | Status |
|-------|--------------|--------|
| inspection_id | 100.0% | ✓ |
| report_date | 100.0% | ✓ |
| outcome | 100.0% | ✓ |
| building | 99.9% | ✓ |
| level | 62.3% | ✓ (legitimate - see Section 4.3) |
| contractor | 89.2% | ✓ |
| inspector | 95.8% | ✓ |
| test_type | 100.0% | ✓ |
| summary | 100.0% | ✓ |

### 7.2 Pipeline Quality Assessment

**Status: GOOD**

The document processing pipeline is performing well:
- Core fields (ID, date, outcome, building) have 99-100% completeness
- Company name standardization reduced unique values by 60-67%
- Trade standardization reduced 173 variations to 16 categories
- "Missing" level data in RABA is legitimate (foundation work)

---

## 8. Recommendations

### 8.1 Pipeline Improvements (Implemented)

- [x] Company name standardization (67% reduction in PSI, 60% in RABA)
- [x] Trade standardization (91% reduction in PSI)
- [x] Cross-dataset company alignment (15 companies matched)

### 8.2 Additional Improvements (Recommended)

1. **Inspection Type Categorization**
   - Group 2,000+ inspection types into ~15 standard categories
   - Enable trend analysis by inspection category

2. **Level Value Enhancement**
   - Add "UG" (underground) and "FOUNDATION" as valid level values
   - Better represents foundation/pier work in RABA

3. **Failure Reason Categorization**
   - Extract root cause codes from failure reasons
   - Categories: Workmanship, Materials, Documentation, Scheduling

### 8.3 Analysis Recommendations

1. **Contractor Performance Review**
   - Investigate AMTS 25.4% and JP Hi-Tech 20.0% failure rates
   - Correlate with schedule delays and rework costs

2. **Temporal Correlation**
   - Analyze May 2025 RABA spike - what drove 3x inspection volume?
   - Correlate inspection failures with schedule slippage

3. **Cross-Source Integration**
   - Link quality failures to P6 schedule activities
   - Correlate contractor performance with labor hours

---

## Appendix A: Data Sources

| Source | System | Records | Coverage |
|--------|--------|---------|----------|
| PSI | Construction Hive | 6,309 | Architectural QC |
| RABA | Celvis | 9,391 | Structural/MEP QC |

**Output Files:**
- `processed/psi/psi_consolidated.csv`
- `processed/raba/raba_consolidated.csv`
- `processed/psi/psi_validation_report.json`
- `processed/raba/raba_validation_report.json`

## Appendix B: Company Standardization

15 companies consistently named across both datasets:
- Yates, Samsung E&C, Berg, Axios, Austin Bridge
- Hensel Phelps, MK Marlow, Baker Triangle, Alpha
- Apache, Cherry Coatings, Chaparral, McDean, Patriot, Baker

Standardization module: `scripts/shared/company_standardization.py`
