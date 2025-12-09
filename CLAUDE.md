# Samsung Taylor FAB1 - Construction Delay Analysis

**Last Updated:** 2025-12-09

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
| Primavera P6 (Yates) | XER files | 65 versions | `data/raw/xer/` | GC perspective, schedule growth analysis |
| ProjectSight Daily Reports | JSON/CSV | 415 reports (manual export) | `data/projectsight/` | Daily activities, man hours, location tracking |
| Weekly Reports | PDF → CSV | 37 reports (Aug 2022 - Jun 2023) | `data/weekly_reports/` | Progress narrative, issues, scope change context |

### Planned/In Progress

| Source | Type | Status | Analysis Value |
|--------|------|--------|----------------|
| Inspection Reports | PDF/Structured | Not started | Quality issues, rework indicators |
| NCR Reports | PDF/Structured | Not started | Non-conformances, contractor quality issues |
| Daily Toolbox Meetings | PDF/Structured | Not started | Safety, crew deployment |
| TCO Reports | TBD | Not started | Turnover status, completion tracking |

## Directory Structure

```
mxi-samsung/
├── data/                       # All project data
│   ├── raw/xer/                # Primavera XER source files (113 files)
│   ├── raw/weekly_reports/     # Weekly report PDFs (37 files)
│   ├── primavera/processed/    # Parsed XER tables (~48 CSV tables)
│   ├── primavera/analysis/     # Analysis reports and findings
│   ├── weekly_reports/tables/  # Parsed weekly report CSVs
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

### 3. Weekly Reports Analysis

**Goal:** Extract narrative context, issues, and progress from weekly PDF reports.

**Processed Data:** `data/weekly_reports/tables/`

| Table | Records | Description |
|-------|---------|-------------|
| `weekly_reports.csv` | 37 | Report metadata (file_id, filename, report_date, author) |
| `key_issues.csv` | 1,108 | Open issues and blockers extracted from narrative |
| `work_progressing.csv` | 1,039 | Work progress items by discipline/trade |
| `procurement.csv` | 374 | Procurement and buyout status items |
| `addendum_rfi_log.csv` | 3,849 | RFI entries from ProjectSight export |
| `addendum_submittal_log.csv` | 6,940 | Submittal status tracking |
| `addendum_manpower.csv` | 613 | Daily labor hours by company |
| `labor_detail.csv` | 13,205 | Individual worker entries (name, classification, hours) |

**Coverage:** August 21, 2022 → June 12, 2023 (10 months, 37 reports)

**Analysis Documents:**

| Document | Location | Description |
|----------|----------|-------------|
| Scope Growth Analysis | [scope_growth_analysis.md](data/primavera/analysis/scope_growth_analysis.md) | Correlation of weekly report events with schedule scope growth |
| Weekly Report Highlights | [weekly_report_highlights.md](data/weekly_reports/weekly_report_highlights.md) | 177+ curated highlights with file, page, importance |
| Key Events Timeline | [key_events_timeline.md](data/weekly_reports/key_events_timeline.md) | Critical milestones and scope change events |
| Executive Summaries | [weekly_report_summaries.md](data/weekly_reports/weekly_report_summaries.md) | Detailed summaries for all 37 reports |

**Narrative Extracts:** `data/weekly_reports/narrative_extracts/` contains raw text extracted from first 20 pages of each PDF (narrative sections only, excludes data dumps).

### 4. Document Analysis (Planned)

**Goal:** Extract structured data from remaining unstructured PDF reports.

**Documents to Process:**
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
| PDF Extraction | PyMuPDF (fitz) | Weekly reports text extraction |
| Visualization | Matplotlib, Seaborn | Python exploration |
| Presentation | Power BI | Client deliverables |
| Orchestration | Apache Airflow | Optional ETL automation |
| Database | PostgreSQL | Optional structured storage |

## Configuration

Environment variables in `.env` (optional, for API integrations and database):

```env
# Fieldwire API (if using)
FIELDWIRE_API_KEY=your_api_key

# Database (optional)
DB_HOST=localhost
DB_PORT=5432
DB_NAME=etl_db
```

## Analysis Status

### Completed
- XER file parsing and batch processing (113 files, 48 tables)
- Schedule version tracking with file_id
- ProjectSight daily report transformation (from manual export)
- Basic schedule metrics
- Weekly report PDF extraction (37 reports → 27K records)
- Scope growth analysis with weekly report correlation
- Weekly report highlights analysis (177+ curated items with page citations)

### In Progress
- Schedule comparison analysis
- Delay attribution methodology

### TODO
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

### YATES Schedule Scope Growth

Key growth events identified with weekly report correlation:
- **Nov 2022**: +25% (Support Buildings scope appearing)
- **Jan 2023**: +35% (HEI engagement, accelerated schedule study)
- **Mar 2023**: +27% (FIZ retrofit decision, SUP design changes)

Root causes: Progressive elaboration from summary-level baseline, design finalization, production acceleration, quality/rework tracking. See [scope_growth_analysis.md](data/primavera/analysis/scope_growth_analysis.md).

---

## Data Processing Lessons Learned

### PDF Weekly Report Extraction

**What Worked:**
- **PyMuPDF (fitz)** was fast and reliable (~23 seconds for 37 files)
- Extracting first ~25 pages only (narrative section) avoids data dump noise
- Bounding box approach for table reconstruction (group text by Y coordinate)
- Content truncation at 500 characters prevents bloated CSVs

**Challenges:**
- Inconsistent PDF structure across reports (page layouts vary)
- Data dump sections (RFI logs, submittals) are cumulative/repeating across reports
- Author identification missing in 4 older reports
- No page numbers tracked in parsed output (limits citation precision)

**Recommendations for Future PDF Processing:**
1. **Extract page numbers** alongside content for proper citations
2. **Deduplicate addendum data** — RFI/submittal logs repeat across reports
3. **Use section headers** to categorize content (e.g., "OPEN ISSUES", "WORK PROGRESSING")
4. **Handle date parsing carefully** — filename dates vary in format (YYYYMMDD vs MMDD)

### Primavera Schedule Analysis

**Key Learnings:**
- `target_end_date` = current scheduled finish (NOT baseline) — gets recalculated with each update
- `early_end_date` = CPM forward pass result; equals target_end for non-started tasks
- `late_end_date` = CPM backward pass from project constraint (deadline)
- `total_float` = late_end - early_end; negative = behind schedule
- **No baseline stored** in XER exports — must compare across schedule versions

**Duration vs Labor Hours:**
- `target_drtn_hr_cnt` = task duration (calendar hours), NOT labor effort
- `taskrsrc.target_qty` = labor hours, but only ~10% of tasks have resource assignments
- Use duration-based analysis when labor data is incomplete

**Activity Code Analysis:**
- `Z-TRADE`, `Z-AREA`, `Z-BLDG` activity codes provide scope categorization
- Join `taskactv` → `actvcode` → `actvtype` to decode task attributes
- Trade category analysis reveals Interior Finishes grew from 6% to 44% of schedule

### Cross-Source Correlation

**Best Practice:**
- Identify overlap periods where multiple data sources exist
- Weekly reports (Aug 2022 - Jun 2023) overlap with early YATES schedules
- Key events in weekly reports correlate with schedule growth inflection points
- Use quotations with file references for narrative analysis

---

*This is a data analysis repository. Python exploration happens here; Power BI dashboards are built separately for client presentation.*
