# Data Quality Check Framework

**Purpose:** Validate dimension coverage and consistency across all integrated data sources.

## Quick Start

```bash
# Run all checks
python -m scripts.integrated_analysis.data_quality

# Run specific check
python -m scripts.integrated_analysis.data_quality.check_csi_coverage
python -m scripts.integrated_analysis.data_quality.check_location_coverage
python -m scripts.integrated_analysis.data_quality.check_company_coverage

# Verbose output
python -m scripts.integrated_analysis.data_quality --verbose

# Save markdown report
python -m scripts.integrated_analysis.data_quality --output report.md
```

## Available Checks

### 1. CSI Coverage (`check_csi_coverage.py`)

Validates CSI MasterFormat section assignments:
- Coverage percentage per source
- CSI sections in quality data but not P6 (can't join)
- CSI sections in P6 but not quality data
- P6 misclassifications (e.g., FIR code used for non-fireproofing)

**Known Issue:** P6 uses `FIR` sub_trade for ALL fire-related work (sprinklers, alarms, firestopping, fireproofing). This maps to 07 81 00 (Fireproofing) but many tasks are actually:
- Firestopping (07 84 00) - 1,359 RABA records
- Fire Suppression (21 10 00)
- Fire Alarms (26 XX XX)

### 2. Location Coverage (`check_location_coverage.py`)

Validates dim_location_id assignments:
- Location dimension statistics (505 codes, 84.6% with grid)
- Coverage per source (P6 100%, RABA 33.6%, PSI 46.9%)
- Grid coordinate coverage for spatial joins
- Invalid location codes not in dim_location

### 3. Company Coverage (`check_company_coverage.py`)

Validates dim_company_id assignments:
- Coverage per source
- Unresolved company names
- Trade ID assignment coverage

## Data Sources Checked

| Source | Records | CSI | Location | Company |
|--------|---------|-----|----------|---------|
| P6 | 483K | ✓ 100% | ✓ 100% | N/A |
| RABA | 9.4K | ✓ 99.9% | ⚠️ 33.6% | ✓ |
| PSI | 6.3K | ✓ 99.2% | ⚠️ 46.9% | ✓ |
| ProjectSight | 857K | ⚠️ 75.2% | ❌ 0% | ✓ |
| TBM | 18K | ⚠️ 68.6% | ⚠️ 57.7% | ✓ |
| Yates_QC | 20K | ✓ 89.7% | N/A | ✓ |
| SECAI_QC | 17K | ✓ 88.0% | N/A | ✓ |
| NCR | 2K | ✓ 91.3% | N/A | ✓ |

## Structure

```
data_quality/
├── __init__.py                      # Package exports
├── __main__.py                      # Entry point for python -m
├── run_all_checks.py                # Run all checks, generate report
├── check_csi_coverage.py            # CSI section coverage check
├── check_location_coverage.py       # Location dimension coverage
├── check_company_coverage.py        # Company dimension coverage
├── investigate_company_scope.py     # Samsung contractor investigation
├── analyze_samsung_scope.py         # Samsung CSI scope analysis
├── CLAUDE.md                        # This file
├── COMPANY_SCOPE_FINDINGS.md        # Company scope investigation
└── CONTRACTOR_INFERENCE_RESULTS.md  # Contractor inference solution
```

## Key Findings (as of 2026-01-25)

### CSI Gaps

13 CSI sections in RABA/PSI not in P6:
- **07 84 00 Firestopping** - 1,359 records (most critical)
- 03 60 00 Grouting - 241 records
- 04 20 00 Unit Masonry - 125 records
- 09 51 00 Acoustical Ceilings - 101 records
- Plus 9 more minor sections

### Recommendations

1. ✅ **COMPLETED:** Added keyword-based CSI detection to P6 (commit c2cbfc3)
2. ✅ **COMPLETED:** Fixed RABA/PSI CSI misclassifications (commit 25ea41e)
3. **Company Filtering:** Apply company-based scope filtering when analyzing Yates performance
4. Improve location coverage for RABA (33.6%) and PSI (46.9%)
5. Add location linkage to ProjectSight labor entries

### Company Scope Findings (2026-01-26)

The 2 remaining CSI gaps (102 records, 0.65%) are **not Yates scope**:
- **09 51 00 Acoustical Ceilings** (101 PSI): 48.5% AMTS, 23.8% Austin Bridge, 4.0% Yates
- **09 65 00 Resilient Flooring** (1 RABA): 100% Samsung E&C

**Key Insight:** P6 schedule is Yates-centric, while RABA/PSI capture all contractors.

**✅ SOLUTION IMPLEMENTED (v2):** Contractor inference with subcontractor priority:

**Inference Priority:**
1. **Subcontractor field** from source document (most reliable) - 32.6% of RABA, 20.1% of PSI
2. **Specific contractor** listed (not Samsung/blank)
3. **CSI-based inference** (Samsung/blank + CSI in P6 → Yates) - 45.9% of RABA, 1.4% of PSI
4. **Keep original** (otherwise)

**Key Results:**
- **RABA:** 5,936 inspections (63.2%) now attributed to Yates or Yates subs
  - 3,060 from subcontractor field (Cherry Coatings, Rolling Plains, Baker Concrete, etc.)
  - 4,312 from CSI-based inference
- **PSI:** 2,682 inspections (42.5%) attributed to Yates
  - 1,267 from subcontractor field
  - 87 from CSI-based inference

**Data Quality Improvement:** 52.7% of quality data (4,327 records) now uses document-based subcontractor attribution instead of CSI inference, significantly improving accuracy.

**Usage:** Use `inferred_contractor_id` for all Yates performance analysis instead of `dim_company_id`.

See:
- [COMPANY_SCOPE_FINDINGS.md](COMPANY_SCOPE_FINDINGS.md) - Original investigation
- [CONTRACTOR_INFERENCE_RESULTS.md](CONTRACTOR_INFERENCE_RESULTS.md) - Solution implementation (v2)
