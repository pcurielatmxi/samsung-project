# Monthly Reports Consolidation Design

**Date:** 2026-01-09
**Status:** Approved
**Author:** MXI Analysis Team

---

## Overview

Build a monthly report consolidation system that aggregates data from all sources for MXI internal analysis. The system produces structured markdown files that an LLM can validate for data quality and use to generate executive summaries.

## Goals

1. Consolidate all data sources by month into a single structured document
2. Enable LLM validation of data quality and statement veracity
3. Support delay attribution analysis (justified vs unjustified)
4. Connect quality issues to labor consumption and schedule impact
5. Maintain full traceability to source data

## Workflow

```
┌─────────────────────────────────────────────────────────────────┐
│  1. CONSOLIDATION (Script)                                      │
│     python -m scripts.integrated_analysis.monthly_reports       │
│            .consolidate_month 2024-03                           │
│     → Reads all source CSVs, filters by month                   │
│     → Aggregates metrics by building/company/trade              │
│     → Outputs consolidated_data.md with source citations        │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│  2. VALIDATION (LLM)                                            │
│     Reviews consolidated_data.md                                │
│     → Spot-checks statement context and ownership               │
│     → Flags data anomalies (spikes, gaps, inconsistencies)      │
│     → Cross-references statements against quality/labor data    │
│     → Outputs data_quality_notes.md with specific issues        │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│  3. SUMMARY (LLM)                                               │
│     Writes executive_summary.md                                 │
│     → Factual narrative based on validated data                 │
│     → Key findings, trends, concerns for the month              │
│     → Delay attribution with justification categorization       │
└─────────────────────────────────────────────────────────────────┘
```

## Output Structure

```
data/analysis/monthly_reports/
├── 2022-10/
│   ├── consolidated_data.md      # Script output
│   ├── data_quality_notes.md     # LLM validation output
│   └── executive_summary.md      # LLM summary output
├── 2022-11/
│   └── ...
└── index.md                      # Summary table linking all months
```

## Report Sections

### 1. Schedule Progress & Delays

- **Progress by Building + Level**: Tasks total, completed, in progress, behind schedule, avg days behind
- **Progress by Trade**: Same metrics plus float consumed
- **Duration Overages**: Tasks exceeding original duration with location, trade, overage amount
- **Critical Path Delays**: Delayed tasks with successor impact and cause category

### 2. Labor Hours & Consumption

- **Labor by Building + Level**: Hours, cost, top company, top trade
- **Labor by Trade**: Hours, % of total, companies, avg hourly rate
- **Labor by Company**: Hours, primary trade, primary location, trend vs prior month
- **Labor Anomalies**: Unusual spikes with possible causes (linked to quality issues)

### 3. Quality Metrics & Issues

- **Inspection Summary**: Pass/fail/partial by source (RABA, PSI)
- **Failures by Location**: Building/level breakdown with top failure type and company
- **Failures by Trade**: Trade-level pass rates and top issues
- **Failures by Company**: Company performance with repeat issue tracking
- **Quality Issue Details**: Individual failure records with re-inspection status

### 4. Narrative Statements

- **Summary by Category**: DELAY, QUALITY_ISSUE, WEATHER, OWNER_CHANGE, COORDINATION
- **Delay Statements (Potentially Unjustified)**: Quality issues, contractor performance, coordination
- **Delay Statements (Potentially Justified)**: Weather, owner changes, design changes
- **Statements Requiring Validation**: Questions for LLM to verify against data

### 5. Cross-Reference Analysis

- **Quality Issues → Labor Impact**: Link quality failures to labor hour increases
- **Quality Issues → Schedule Impact**: Link quality failures to task delays
- **Delay Attribution Summary**: Days by category with justified/unjustified classification

### 6. Data Availability & Gaps

- Coverage notes for each source for the month
- Missing date ranges flagged

## Script Architecture

**Location:** `scripts/integrated_analysis/monthly_reports/`

```
monthly_reports/
├── consolidate_month.py      # Main entry point
├── data_loaders/
│   ├── __init__.py
│   ├── primavera.py          # P6 schedule data
│   ├── labor.py              # ProjectSight, Weekly Reports, TBM
│   ├── quality.py            # RABA + PSI consolidated
│   └── narratives.py         # Narrative statements
├── analyzers/
│   ├── __init__.py
│   ├── schedule_progress.py  # Section 1: Progress & delays
│   ├── labor_consumption.py  # Section 2: Labor hours
│   ├── quality_metrics.py    # Section 3: Quality issues
│   ├── statements.py         # Section 4: Narrative statements
│   └── cross_reference.py    # Section 5: Impact correlations
├── output/
│   ├── __init__.py
│   └── markdown_writer.py    # Renders consolidated_data.md
└── README.md                 # Usage documentation
```

## Data Sources

| Source | Date Range | Records | Key Fields |
|--------|-----------|---------|------------|
| P6 Primavera | Oct 2022 - Nov 2025 | 964K tasks | task completion, float, duration |
| RABA Quality | May 2022 - Dec 2025 | 9,391 | pass/fail, location, company, trade |
| PSI Quality | 2023 - 2025 | 6,309 | pass/fail, location, company, trade |
| ProjectSight Labor | Jun 2022 - Mar 2023 | 857K | hours, company, trade |
| Weekly Reports | Aug 2022 - Jun 2023 | 13K hours | hours, company, issues |
| TBM Daily Plans | Mar - Dec 2025 | 13,539 | hours, location, company |
| Narratives | ~200 documents | varies | statements, categories, impact |

## Dimension Integration

All data sources are joined via dimension IDs:

- `dim_location_id` - Building + Level (e.g., "FAB-2F")
- `dim_company_id` - Standardized company name
- `dim_trade_id` - Standardized trade code (CON, DRY, MEP, etc.)

## Usage

```bash
# Generate single month
python -m scripts.integrated_analysis.monthly_reports.consolidate_month 2024-03

# Generate all months with data
python -m scripts.integrated_analysis.monthly_reports.consolidate_month --all

# Generate date range
python -m scripts.integrated_analysis.monthly_reports.consolidate_month --start 2023-01 --end 2023-12

# Dry run
python -m scripts.integrated_analysis.monthly_reports.consolidate_month 2024-03 --dry-run
```

## Data Quality Feedback Loop

The LLM validation step may identify issues that require updates to source data:

1. **Statement ownership errors** → Update narrative extraction prompts
2. **Company name mismatches** → Update `map_company_aliases.csv`
3. **Location mapping gaps** → Update `dim_location.csv` grid bounds
4. **Trade inference errors** → Update trade classification rules

Issues are tracked in `data_quality_notes.md` and fed back to improve data pipelines.

## Success Criteria

1. Script generates complete `consolidated_data.md` for any month with available data
2. All metrics include source citations for traceability
3. Cross-reference analysis correctly links quality issues to labor/schedule impact
4. LLM can validate statements against quality/labor data
5. Delay attribution distinguishes justified from unjustified delays
