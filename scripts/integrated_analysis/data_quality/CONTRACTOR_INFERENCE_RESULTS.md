# Contractor Inference Results

**Date:** 2026-01-26
**Updated:** 2026-01-26 (v2 - Added subcontractor field priority)
**Purpose:** Assign actual contractor using document data and scope inference

---

## Problem

Samsung E&C appeared as the contractor on 67.6% of RABA inspections (6,349 records), but Samsung is the project owner, not performing most construction work. Additionally, many records had blank contractor fields, but the actual subcontractor was listed in the source PDF documents.

## Root Cause

When contractor field is blank or generic in inspection forms, it defaults to "Samsung E&C America, Inc." as the project owner. However, **RABA inspection reports often include a "subcontractor" field on page 2** that identifies the actual company performing the work (Cherry Coatings, Rolling Plains, Baker Concrete, etc.).

## Solution

Created contractor inference system with prioritized logic:

**Inference Priority:**
1. **Subcontractor field** (from source document - most reliable)
2. **Specific contractor** listed (not Samsung/blank)
3. **CSI-based inference** (Samsung/blank + CSI in P6 scope → Yates)
4. **Keep original** (otherwise)

**New Columns:**
- `inferred_contractor_id`: Actual contractor based on best available data
- `contractor_inference_source`: How the contractor was determined

**Inference Sources:**
- `subcontractor_field` - From source document subcontractor field
- `original` - Non-Samsung/blank contractor kept as-is
- `csi_scope` - Samsung/blank → Yates based on CSI in P6 scope
- `no_csi` - Samsung/blank without CSI data
- `owner_work` - Samsung with CSI NOT in P6 (owner self-perform)

---

## Results (v2 - With Subcontractor Priority)

### RABA (9,391 total inspections)

| Contractor | Original | Inferred | Change |
|------------|----------|----------|--------|
| **Yates** | 1,092 (11.6%) | **5,936 (63.2%)** | +4,844 |
| **Cherry Coatings** | 0 (0.0%) | **722 (7.7%)** | +722 |
| **Rolling Plains** | 0 (0.0%) | **429 (4.6%)** | +429 |
| **Baker Concrete** | 102 (1.1%) | **426 (4.5%)** | +324 |
| **Samsung E&C** | 6,349 (67.6%) | **375 (4.0%)** | -5,974 |
| Other contractors | 1,848 (19.7%) | 1,503 (16.0%) | -345 |

**Inference Source Breakdown:**
- `csi_scope`: 4,312 (45.9%) - Samsung/blank → Yates based on CSI in P6
- **`subcontractor_field`: 3,060 (32.6%)** - From source document ✓
- `original`: 2,012 (21.4%) - Non-Samsung contractors kept as-is
- `no_csi`: 7 (0.1%) - Samsung without CSI, kept as Samsung

**Key Improvement:** 32.6% of RABA inspections (3,060 records) now use actual subcontractor from source documents instead of blanket CSI-based inference.

### PSI (6,309 total inspections)

| Contractor | Original | Inferred | Change |
|------------|----------|----------|--------|
| **Yates** | 2,719 (43.1%) | **2,682 (42.5%)** | -37 |
| **Berg** | 1,568 (24.9%) | **1,568 (24.9%)** | 0 |
| **AMTS** | 476 (7.5%) | **476 (7.5%)** | 0 |
| **Samsung E&C** | 65 (1.0%) | **11 (0.2%)** | -54 |
| Other contractors | 1,481 (23.5%) | 1,572 (24.9%) | +91 |

**Inference Source Breakdown:**
- `original`: 4,947 (78.4%) - Non-Samsung contractors kept as-is
- **`subcontractor_field`: 1,267 (20.1%)** - From source document ✓
- `csi_scope`: 87 (1.4%) - Samsung/blank → Yates based on CSI in P6
- `no_csi`: 4 (0.1%) - Samsung without CSI, kept as Samsung
- `owner_work`: 4 (0.1%) - Samsung with CSI NOT in P6, kept as Samsung

**Key Improvement:** 20.1% of PSI inspections (1,267 records) now use actual subcontractor from source documents.

---

## Example: Document-Based Attribution

### A25-024896 - Chemical-Resistant Coatings
**Source PDF:**
- Page 1: TO: Samsung E&C (recipient, not contractor)
- Page 2: Subcontractor: Cherry

**Result:**
- `contractor_raw`: blank
- `subcontractor_raw`: Cherry
- `subcontractor`: Cherry Coatings (standardized)
- `inferred_contractor_id`: 6 (Cherry Coatings)
- `contractor_inference_source`: **subcontractor_field** ✓

### A25-018313 - Applied Fireproofing
**Source PDF:**
- Page 1: TO: Samsung E&C (recipient)
- Page 2: Subcontractor: Rolling Planes

**Result:**
- `contractor_raw`: blank
- `subcontractor_raw`: Rolling Planes
- `subcontractor`: Rolling Plains (standardized)
- `inferred_contractor_id`: 10 (Rolling Plains)
- `contractor_inference_source`: **subcontractor_field** ✓

### A23-020802 - Cast-in-Place Concrete
**Source PDF:**
- Page 1: TO: Samsung E&C (recipient)
- Page 2: No subcontractor field listed

**Result:**
- `contractor_raw`: blank
- `subcontractor_raw`: blank
- `inferred_contractor_id`: 2 (Yates)
- `contractor_inference_source`: **csi_scope** (CSI-based fallback)

---

## Validation

**Top Subcontractors Identified (RABA):**
- Cherry Coatings: 722 inspections (Chemical-Resistant Coatings)
- Rolling Plains: 429 inspections (Applied Fireproofing)
- Baker Concrete: 324 inspections (Structural Concrete)
- Austin Global: 187 inspections (Structural Steel)
- Fabco: 164 inspections (Structural Steel)

All of these are legitimate Yates subcontractors performing specialized work.

**CSI Sections with Most Subcontractor Attribution:**
- 09 06 65 Chemical-Resistant Coatings: 722 inspections → Cherry Coatings
- 07 81 00 Applied Fireproofing: 429 inspections → Rolling Plains
- 03 30 00 Cast-in-Place Concrete: 324+ inspections → Baker Concrete
- 05 12 00 Structural Steel: 351 inspections → Austin Global, Fabco

---

## Usage for Analysis

**For Yates performance analysis, use `inferred_contractor_id`:**

```python
# Load quality data
raba = pd.read_csv("raba_consolidated.csv")

# Filter to Yates scope only
yates_raba = raba[raba['inferred_contractor_id'] == 2]  # Yates company_id = 2

# Or filter to Yates + Yates subs
yates_subs = [2, 6, 10, 30]  # Yates, Cherry, Rolling Plains, Baker Concrete
yates_scope = raba[raba['inferred_contractor_id'].isin(yates_subs)]
```

**For multi-contractor analysis, use `inferred_contractor_id` with groupby:**

```python
# Compare performance by contractor
contractor_stats = raba.merge(
    dim_company[['company_id', 'canonical_name']],
    left_on='inferred_contractor_id',
    right_on='company_id'
).groupby('canonical_name').agg({
    'outcome': lambda x: (x == 'PASS').mean(),
    'report_date': 'count'
})
```

**Filter by inference source for data quality checks:**

```python
# Show only document-based attributions (highest confidence)
document_based = raba[raba['contractor_inference_source'] == 'subcontractor_field']

# Show CSI-based inferences (may need review)
csi_based = raba[raba['contractor_inference_source'] == 'csi_scope']
```

---

## Implementation

**Scripts:**
- `scripts/integrated_analysis/add_contractor_inference.py` - Prioritized inference logic
- `scripts/raba/document_processing/consolidate.py` - Extract subcontractor from parties_involved

**Modified Files:**
- `processed/raba/raba_consolidated.csv` - Added `inferred_contractor_id`, `contractor_inference_source`, `subcontractor`, `subcontractor_raw`
- `processed/psi/psi_consolidated.csv` - Added `inferred_contractor_id`, `contractor_inference_source`, `subcontractor`, `subcontractor_raw`

**Key Changes (v2):**
1. Updated consolidation script to extract `subcontractor` from `parties_involved` JSON
2. Added prioritized inference logic: subcontractor → contractor → CSI-based → original
3. 52.7% of quality data (4,327 records) now uses document-based attribution instead of CSI inference

---

## Recommendation

**All future Yates performance analysis should use `inferred_contractor_id` instead of `dim_company_id`.**

The original `dim_company_id` represents what was written in the inspection form "contractor" field (often blank or Samsung E&C). The `inferred_contractor_id` represents who actually performed the work based on:
1. **Best:** Subcontractor field from source document
2. **Good:** CSI scope patterns from P6 schedule
3. **Fallback:** Original contractor value

**Data Quality:** 52.7% of records (4,327 out of 9,391 RABA + 6,309 PSI) now use document-based subcontractor instead of scope inference, significantly improving attribution accuracy.
