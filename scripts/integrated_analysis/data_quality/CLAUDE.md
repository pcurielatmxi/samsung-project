# Data Quality Check Framework

**Purpose:** Validate dimension coverage and consistency across all integrated data sources.

## Quick Start

```bash
# Run comprehensive dimension coverage report
python -m scripts.integrated_analysis.data_quality.dimension_coverage

# Or equivalently:
python -m scripts.integrated_analysis.data_quality

# Save markdown report
python -m scripts.integrated_analysis.data_quality.dimension_coverage -o coverage.md
```

## Dimension Coverage Report

The `dimension_coverage` module provides a comprehensive unified report with:

1. **Coverage Matrix** - % of records linked to each dimension (location, company, CSI)
2. **Location Granularity** - Distribution of location specificity per source (ROOM vs GRIDLINE vs BUILDING)
3. **CSI Section Analysis** - Joinability gaps between schedule and quality data
4. **Company Coverage** - Unresolved company names
5. **Actionable Recommendations** - File paths for each issue

See `dimension_coverage/CLAUDE.md` for detailed documentation.

## Data Sources Checked

| Source | Records | Location | Company | CSI |
|--------|---------|----------|---------|-----|
| P6 | 483K | ✓ 100% | N/A | ✓ 99.5% |
| RABA | 9.4K | ✓ 100% | ⚠️ 89% | ✓ 99.9% |
| PSI | 6.3K | ✓ 100% | ✓ 99.2% | ✓ 99.4% |
| ProjectSight | 863K | — | ✓ 100% | ✓ 100% |
| TBM | 76K | — | — | — |
| NCR | 2K | — | ❌ 66% | ⚠️ 91% |

## Structure

```
data_quality/
├── __init__.py                      # Package exports
├── __main__.py                      # Entry point for python -m
├── CLAUDE.md                        # This file
│
├── dimension_coverage/              # Comprehensive coverage report
│   ├── __init__.py                  # Public API and main entry point
│   ├── __main__.py                  # CLI entry point
│   ├── models.py                    # Data classes (DimensionStats, SourceCoverage)
│   ├── config.py                    # Source configurations and thresholds
│   ├── loaders.py                   # Dimension table and source data loaders
│   ├── analyzers.py                 # Coverage calculation logic
│   ├── formatters.py                # Report formatting (console and markdown)
│   └── CLAUDE.md                    # Module documentation
│
├── investigate_company_scope.py     # Samsung contractor investigation
├── analyze_samsung_scope.py         # Samsung CSI scope analysis
├── COMPANY_SCOPE_FINDINGS.md        # Company scope investigation
└── CONTRACTOR_INFERENCE_RESULTS.md  # Contractor inference solution
```

## Key Findings

### CSI Gaps (Resolved)

Previously 13 CSI sections in RABA/PSI were not in P6. Now resolved:
- ✅ Added keyword-based CSI detection to P6
- ✅ Fixed RABA/PSI CSI misclassifications

Remaining 2 gaps (102 records, 0.65%) are **not Yates scope**:
- **09 51 00 Acoustical Ceilings** (101 PSI): 48.5% AMTS, 23.8% Austin Bridge
- **09 65 00 Resilient Flooring** (1 RABA): 100% Samsung E&C

### Company Scope

**Key Insight:** P6 schedule is Yates-centric, while RABA/PSI capture all contractors.

**Solution:** Contractor inference with subcontractor priority:
1. **Subcontractor field** from source document (most reliable)
2. **Specific contractor** listed (not Samsung/blank)
3. **CSI-based inference** (Samsung/blank + CSI in P6 → Yates)
4. **Keep original** (otherwise)

**Usage:** Use `inferred_contractor_id` for Yates performance analysis instead of `dim_company_id`.

See:
- [COMPANY_SCOPE_FINDINGS.md](COMPANY_SCOPE_FINDINGS.md) - Investigation details
- [CONTRACTOR_INFERENCE_RESULTS.md](CONTRACTOR_INFERENCE_RESULTS.md) - Solution implementation
