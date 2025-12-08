# Samsung Taylor FAB1 - Construction Delay Analysis

**Last Updated:** 2025-12-08

## Project Purpose

Data analysis project for the Samsung Austin semiconductor chip manufacturing facility (Taylor, TX). The contract is behind schedule, and we are analyzing contractor performance to identify sources of delays.

**Key Question:** What are the root causes of schedule delays, and which contractors are responsible?

## Stakeholders

| Role | Entity | Description |
|------|--------|-------------|
| Owner | Samsung | Project owner |
| Owner's Engineering | SECAI (Samsung Engineering Construction America Inc.) | Maintains owner's schedule |
| General Contractor | Yates Construction | Maintains GC schedule |
| Analyst | MXI | Performing delay analysis |

## Analysis Workflow

```
Data Sources → Python Exploration → Insights → Power BI Presentation
     ↓               ↓                 ↓              ↓
  Raw Files    Jupyter/Scripts    Findings      Client Deliverable
```

- **Python**: Data exploration, cleaning, analysis, hypothesis testing
- **Power BI**: Final dashboards and presentation to customer (handled separately)
- **Airflow**: Optional ETL automation (supporting tool, not primary focus)

## Data Sources Overview

### Currently Available

| Source | Type | Status | Location | Analysis Value |
|--------|------|--------|----------|----------------|
| Primavera P6 (SECAI) | XER files | 47 versions | `data/raw/xer/` | Schedule evolution, baseline comparisons, delay trends |
| Primavera P6 (Yates) | XER files | 1 version | `data/raw/xer/` | GC perspective, compare with owner schedule |
| ProjectSight Daily Reports | JSON/CSV | 415 reports | `data/projectsight/` | Daily activities, man hours, location tracking |

### Planned/In Progress

| Source | Type | Status | Analysis Value |
|--------|------|--------|----------------|
| PDF Weekly Reports | Unstructured PDF | Not started | Progress summaries, narrative context, issues |
| Inspection Reports | PDF/Structured | Not started | Quality issues, rework indicators |
| NCR Reports | PDF/Structured | Not started | Non-conformances, contractor quality issues |
| Daily Toolbox Meetings | PDF/Structured | Not started | Safety, crew deployment |
| TCO Reports | TBD | Not started | Turnover status, completion tracking |

## Directory Structure

```
mxi-samsung/
├── data/                       # All project data
│   ├── raw/xer/                # Primavera XER source files (48 files)
│   ├── primavera/processed/    # Parsed XER tables (~48 CSV tables)
│   ├── primavera/analysis/     # Analysis reports and findings
│   └── projectsight/           # Daily reports (JSON + CSV)
├── notebooks/                  # Jupyter notebooks for exploration
├── scripts/                    # Processing and analysis scripts
├── src/                        # Reusable Python modules
│   ├── utils/                  # Utilities (xer_parser.py, etc.)
│   ├── extractors/             # Data extraction logic
│   ├── connectors/             # API/web scraping connectors
│   └── transformers/           # Data normalization
├── dags/                       # Airflow DAGs (optional automation)
└── docs/                       # Documentation
```

## Key Analysis Areas

### 1. Schedule Analysis (Primavera)

**Goal:** Identify when delays occurred, which activities slipped, and who was responsible.

**Two Schedule Perspectives:**
- **SECAI Schedule** (47 versions): Owner's detailed view (~32K tasks)
- **Yates Schedule** (1 version): GC's view (~12K tasks)

**Key Analyses:**
- Baseline vs actual comparisons
- Schedule version evolution (task growth: 7K → 32K over time)
- Critical path analysis
- Float erosion tracking
- Delay attribution by contractor/area/trade

**Processed Data:** `data/primavera/processed/`
- `task.csv` - 964K tasks across all versions
- `taskpred.csv` - 2M predecessor relationships
- `taskrsrc.csv` - 266K resource assignments
- All tables have `file_id` linking to `xer_files.csv`

### 2. Daily Activity Analysis (ProjectSight)

**Goal:** Track daily progress, man hours, and identify productivity patterns.

**Available Data:** 415 daily reports with:
- Workforce counts by contractor
- Work locations
- Activities performed
- Weather impacts

### 3. Document Analysis (Planned)

**Goal:** Extract structured data from unstructured PDF reports.

**Documents to Process:**
- Weekly progress reports (narrative + tables)
- Inspection reports (quality issues)
- NCRs (non-conformance tracking)
- Toolbox meeting records

## Quick Start

### Process Primavera Data

```bash
# Process all XER files from manifest
python scripts/batch_process_xer.py

# Process only current schedule
python scripts/batch_process_xer.py --current-only

# Filter tasks by keyword
python scripts/filter_tasks.py data/primavera/processed/task.csv --keyword "drywall"
```

### Explore Data (Python)

```python
import pandas as pd

# Load schedule metadata
files = pd.read_csv('data/primavera/processed/xer_files.csv')
tasks = pd.read_csv('data/primavera/processed/task.csv')

# Get current schedule tasks
current_id = files[files['is_current']]['file_id'].values[0]
current_tasks = tasks[tasks['file_id'] == current_id]

# Compare task counts across versions
version_summary = tasks.groupby('file_id').size().reset_index(name='task_count')
version_summary = version_summary.merge(files[['file_id', 'filename', 'date']])
```

### XER File Management

XER files are gitignored, but tracked via `data/raw/xer/manifest.json`.

```bash
# Validate manifest
python scripts/validate_xer_manifest.py

# Add missing files to manifest
python scripts/validate_xer_manifest.py --fix
```

## Primavera Data Schema

All processed CSVs have `file_id` as first column for version tracking.

**Key Tables:**

| Table | Description | Key Fields |
|-------|-------------|------------|
| `xer_files.csv` | File metadata | file_id, filename, date, is_current |
| `task.csv` | All tasks | task_id, task_code, task_name, target_start/end |
| `taskpred.csv` | Dependencies | pred_task_id, task_id, pred_type |
| `taskrsrc.csv` | Resource assignments | task_id, rsrc_id, target_qty |
| `projwbs.csv` | WBS structure | wbs_id, wbs_name, parent_wbs_id |
| `actvcode.csv` | Activity codes | actv_code_id, short_name (contractor, area, etc.) |

**ID Convention:** All IDs are prefixed with file_id for uniqueness: `{file_id}_{original_id}`

## Data Source Documentation

### Subfolder CLAUDE.md Files

Each data subsystem has its own documentation:

| Location | Purpose |
|----------|---------|
| [data/CLAUDE.md](data/CLAUDE.md) | Data directory context, schedule perspectives |
| [src/extractors/system_specific/CLAUDE.md](src/extractors/system_specific/CLAUDE.md) | ProjectSight extraction details |

**Keep these updated** as analysis progresses and new data sources are added.

### Detailed Guides

| Document | Purpose |
|----------|---------|
| [docs/SOURCES.md](docs/SOURCES.md) | Data source APIs & field mapping |
| [docs/ETL_DESIGN.md](docs/ETL_DESIGN.md) | Architecture patterns |
| [data/primavera/analysis/xer_file_overlap_analysis.md](data/primavera/analysis/xer_file_overlap_analysis.md) | Schedule version analysis |

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Data Processing | Pandas, NumPy | Analysis and transformation |
| PDF Extraction | TBD (pdfplumber, camelot) | Weekly reports, NCRs |
| Visualization | Matplotlib, Seaborn | Python exploration |
| Presentation | Power BI | Client deliverables |
| Web Scraping | Playwright | ProjectSight extraction |
| Orchestration | Apache Airflow | Optional ETL automation |
| Database | PostgreSQL | Optional structured storage |

## Configuration

Environment variables in `.env`:

```env
# ProjectSight credentials
PROJECTSIGHT_BASE_URL=https://...
PROJECTSIGHT_USERNAME=...
PROJECTSIGHT_PASSWORD=...

# Database (optional)
DB_HOST=localhost
DB_PORT=5432
DB_NAME=etl_db
```

## Analysis Status

### Completed
- XER file parsing and batch processing
- Schedule version tracking with file_id
- ProjectSight daily report extraction
- Basic schedule metrics

### In Progress
- Schedule comparison analysis
- Delay attribution methodology

### TODO
- PDF weekly report extraction
- Inspection/NCR report processing
- Integrated delay analysis across sources
- Power BI data model design

## Adding New Data Sources

When new data becomes available:

1. Create folder under `data/` (e.g., `data/weekly_reports/`)
2. Add processing script under `scripts/`
3. Document in `data/CLAUDE.md`
4. Update this file's Data Sources table
5. Create analysis notebook under `notebooks/`

## Key Insights (Living Section)

### Schedule Perspectives

The SECAI and Yates schedules show only 0.1% task code overlap due to different coding conventions:
- **SECAI**: Task codes like TE0, TM, TA0
- **Yates**: Task codes like CN, FAB, ZX

This is the same project viewed from different organizational perspectives. See [data/primavera/analysis/xer_file_overlap_analysis.md](data/primavera/analysis/xer_file_overlap_analysis.md).

### Schedule Evolution

SECAI schedule growth over time:
- Oct 2022 - Dec 2022: 7K-11K tasks (early project)
- 2023: Growth to 24K-31K tasks
- 2024-2025: Mature at 29K-32K tasks

---

*This is a data analysis repository. Python exploration happens here; Power BI dashboards are built separately for client presentation.*
