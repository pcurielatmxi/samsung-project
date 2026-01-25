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
├── __init__.py              # Package exports
├── __main__.py              # Entry point for python -m
├── run_all_checks.py        # Run all checks, generate report
├── check_csi_coverage.py    # CSI section coverage check
├── check_location_coverage.py # Location dimension coverage
├── check_company_coverage.py  # Company dimension coverage
└── CLAUDE.md                # This file
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

1. **CRITICAL:** Add keyword-based Firestopping detection to P6 CSI inference
2. Update `add_csi_to_p6_tasks.py` to check task_name keywords (like RABA/PSI scripts)
3. Improve location coverage for RABA (33.6%) and PSI (46.9%)
4. Add location linkage to ProjectSight labor entries
