# Samsung Taylor FAB1 - Construction Delay Analysis

**Last Updated:** 2025-12-11

## Project Purpose

Data analysis project for the Samsung Austin semiconductor chip manufacturing facility (Taylor, TX). MXI is conducting an independent, data-driven assessment of schedule delays and labor consumption to support Samsung's decision-making.

## Analysis Objectives

**See [docs/EXECUTIVE_SUMMARY.md](docs/EXECUTIVE_SUMMARY.md) for full analysis plan and status.**

### Central Question

**What is the impact of quality issues and inefficiencies on the project's schedule and labor consumption?**

| Objective | Question | Approach |
|-----------|----------|----------|
| **Scope Evolution** | How much scope was added due to rework or coordination issues? | Compare 66 YATES schedule snapshots to track task growth. Cross-reference with weekly reports to categorize additions. Reconcile against change orders when available. |
| **Delay Attribution** | How much schedule impact resulted from quality and performance issues? | Track completion date movement across snapshots. Correlate with 1,108 documented issues. Identify which activities and trades were involved when delays occurred. |
| **Resource Consumption** | How much labor was consumed and what factors drove consumption? | Track labor hours by contractor, trade, and date. Correlate with documented issues and rework tasks. Quantify labor during problem periods. |
| **Quality Impact** | What quality issues occurred and what rework did they drive? | Compile NCR and inspection reports by trade and location. Link to rework tasks in schedule. Quantify rework duration and labor. |

**Status Tracking:** [docs/analysis_status.csv](docs/analysis_status.csv) | [docs/analysis_objectives.csv](docs/analysis_objectives.csv)

## Stakeholders

| Role | Entity | Description |
|------|--------|-------------|
| Owner | Samsung | Project owner |
| Owner's Engineering | SECAI | Maintains owner's schedule |
| General Contractor | Yates Construction | Maintains GC schedule |
| Analyst | MXI | Performing delay analysis |

## Data Sources

| Source | Records | Coverage | Status |
|--------|---------|----------|--------|
| YATES Schedules | 66 versions | Oct 2022 - Nov 2025 | ✅ Processed |
| Weekly Reports | 37 reports | Aug 2022 - Jun 2023 | ✅ Processed |
| RFI/Submittal Logs | 10,789 entries | 2022-2023 | ✅ Extracted |
| Labor Records | 13,205 entries | 2022-2023 | ✅ Extracted |
| TBM Daily Plans | 13,539 activities | Mar-Dec 2025 | ✅ Processed |
| NCR Reports | TBD | TBD | 🔄 In Progress |
| Inspection Reports | TBD | TBD | 🔄 In Progress |
| Bid Documents / RFP | TBD | TBD | ⬜ Pending |
| Change Orders | TBD | TBD | ⬜ Pending |

## Directory Structure

```
mxi-samsung/
├── data/                       # All project data
│   ├── raw/xer/                # Primavera XER source files
│   ├── primavera/processed/    # Parsed schedule tables
│   ├── primavera/analysis/     # Analysis outputs
│   ├── weekly_reports/         # Weekly report data
│   └── tbm/                    # TBM daily plans
├── docs/                       # Documentation
│   ├── EXECUTIVE_SUMMARY.md    # Analysis objectives & status
│   ├── analysis_status.csv     # Progress tracking (Power BI)
│   └── analysis_objectives.csv # Questions & approach (Power BI)
├── scripts/                    # Processing scripts
├── src/                        # Python modules
│   └── classifiers/            # WBS taxonomy classifier
└── notebooks/                  # Jupyter exploration
```

## Quick Reference

### Key Commands

```bash
# Process YATES schedules
python scripts/batch_process_xer.py

# Generate WBS taxonomy
python scripts/generate_wbs_taxonomy.py

# Validate XER manifest
python scripts/validate_xer_manifest.py
```

### Key Data Files

| File | Description |
|------|-------------|
| `data/primavera/processed/task.csv` | All tasks across schedule versions |
| `data/primavera/processed/xer_files.csv` | Schedule version metadata |
| `data/weekly_reports/tables/key_issues.csv` | 1,108 documented issues |
| `data/primavera/analysis/wbs_taxonomy.csv` | 333K tasks with taxonomy labels |

### WBS Taxonomy

Standardized classification for YATES tasks (99.95% coverage):

**Format:** `{PHASE}-{SCOPE}|{LOC_TYPE}:{LOC_ID}`

**Example:** `INT-DRY|RM:FAB146103` = Interior Drywall in Room FAB146103

**Phases:** STR (41%), INT (33%), ENC (12%), PRE (11%), ADM (2%), COM (<1%)

## Technical Reference

For detailed technical documentation:

| Document | Purpose |
|----------|---------|
| [data/CLAUDE.md](data/CLAUDE.md) | Data directory details |
| [docs/SOURCES.md](docs/SOURCES.md) | Data source APIs & field mapping |
| [data/primavera/analysis/](data/primavera/analysis/) | Analysis findings |

### Primavera Key Fields

- `target_end_date` = current scheduled finish (recalculated each update)
- `total_float` = late_end - early_end (negative = behind schedule)
- No baseline in XER exports — compare across schedule versions

### ID Convention

All IDs prefixed with file_id: `{file_id}_{original_id}`

---

*Analysis repository for Python exploration. Power BI dashboards built separately for client presentation.*
