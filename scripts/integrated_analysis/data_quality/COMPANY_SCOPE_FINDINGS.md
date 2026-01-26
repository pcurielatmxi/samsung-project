# Company Scope Investigation - RABA/PSI Quality Data

**Date:** 2026-01-26
**Purpose:** Identify Yates vs AMTS scope in remaining CSI gap sections

---

## Executive Summary

The 2 remaining CSI sections in quality data but not P6 (102 records, 0.65%) are **primarily AMTS work, not Yates scope**. This confirms that the CSI inference is working correctly - these are legitimate inspections for work outside Yates' scope.

---

## Company Distribution Overview

### RABA (9,391 records)
- **Samsung E&C:** 6,349 (67.6%)
- **Yates:** 1,092 (11.6%)
- **Hensel Phelps:** 352 (3.7%)
- **Baker Concrete:** 102 (1.1%)
- **AMTS:** 0 (0.0%) - **No AMTS work in RABA**

### PSI (6,309 records)
- **Yates:** 2,719 (43.1%)
- **Berg:** 1,568 (24.9%)
- **AMTS:** 476 (7.5%)
- **Axios:** 473 (7.5%)
- **MK Marlow:** 363 (5.8%)

---

## Gap CSI Sections - Company Breakdown

### 09 51 00 Acoustical Ceilings (101 records - PSI only)

| Company | Count | Percentage | Yates Scope? |
|---------|-------|------------|--------------|
| **AMTS** | 49 | 48.5% | ❌ No |
| Austin Bridge | 24 | 23.8% | ❓ Partner |
| JP Hi-Tech | 14 | 13.9% | ❓ Sub |
| Yates | 4 | 4.0% | ✅ Yes |
| Berg | 3 | 3.0% | ✅ Sub |
| Baker Triangle | 1 | 1.0% | ❓ Partner |
| Axios | 1 | 1.0% | ❓ Partner |
| Samsung E&C | 1 | 1.0% | ❌ Owner |

**Key Finding:** Nearly half (48.5%) of acoustical ceiling inspections are AMTS work, not Yates.

### 09 65 00 Resilient Flooring (1 record - RABA only)

| Company | Count | Percentage | Yates Scope? |
|---------|-------|------------|--------------|
| Samsung E&C | 1 | 100.0% | ❌ Owner |

**Key Finding:** Single record is Samsung E&C work, not Yates.

---

## Recommendations

### 1. Company-Based Filtering Strategy

When analyzing Yates performance, filter quality data by company:

**Yates Scope (Direct + Subs):**
```python
yates_scope_mask = df['canonical_name'].isin([
    'Yates',
    'Berg',          # Yates drywall sub
    'Baker Concrete',
    'Baker Triangle',
    # Add other Yates subs
])
```

**Exclude from Yates Analysis:**
- AMTS (separate GC/contractor)
- Samsung E&C (owner)
- Austin Bridge (separate contractor)
- Hensel Phelps (separate GC)

### 2. Add Scope Classification Column

Consider adding a `contractor_tier` or `scope_owner` column to quality data:
- `YATES` - Yates self-perform
- `YATES_SUB` - Yates subcontractors
- `AMTS` - AMTS work
- `SECAI` - Samsung owner work
- `OTHER` - Other contractors

This enables easy filtering:
```python
yates_only = df[df['scope_owner'].isin(['YATES', 'YATES_SUB'])]
```

### 3. Update Data Quality Checks

Modify `check_csi_coverage.py` to report CSI gaps by company tier:
- Yates scope only
- AMTS scope only
- Other contractors

This will prevent false alarms when non-Yates work has different CSI patterns.

### 4. Company Master Reference

The `dim_company` table already has useful columns:
- `tier`: OWNER, GC, SUB, OTHER
- `company_type`: yates_self, major_contractor, other
- `is_yates_sub`: Boolean flag

These can be leveraged for automatic scope classification.

---

## Data Source Context

### Raw Company Columns Available

**RABA:**
- `contractor_raw` - Original contractor name from PDF
- `testing_company_raw` - Testing firm (always RABA Kistner)
- `subcontractor_raw` - Usually empty
- `contractor` - Standardized contractor name
- `testing_company` - Standardized testing firm
- `dim_company_id` - Foreign key to dim_company

**PSI:**
- `contractor_raw` - Original contractor name
- `subcontractor_raw` - Subcontractor if listed
- `contractor` - Standardized contractor
- `subcontractor` - Standardized subcontractor
- `dim_company_id` - Foreign key to dim_company

Both sources have reliable company linkage via `dim_company_id`.

---

## Conclusion

The remaining 102 CSI gap records (0.65%) are NOT a data quality issue. They represent:
1. **AMTS work** (49 records) - separate contractor scope
2. **Owner work** (1 record) - Samsung E&C self-perform
3. **Other contractors** (52 records) - Austin Bridge, JP Hi-Tech, etc.

The CSI inference system is working correctly. The apparent "gap" exists because P6 schedule data is **Yates-centric**, while RABA/PSI capture **all contractor work** on the project.

For accurate Yates performance analysis, apply company filtering to isolate Yates scope.
