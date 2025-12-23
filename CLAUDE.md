# Samsung Taylor FAB1 Performance Analysis

**Project:** Data-driven analysis of schedule delays and labor consumption
**Client:** Samsung
**Analyst:** MXI

---

## Project Phases

### Phase 1: Data Collection & Initial Assessment ✅

**Status:** Complete

Established data pipelines for 9 primary sources:

| Source | Purpose | Records | Status |
|--------|---------|---------|--------|
| Primavera P6 | Schedule snapshots (YATES + SECAI) | 66 files, 964K tasks | ✅ Processed |
| Weekly Reports | Issues, progress, manpower | 37 reports, 1,108 issues | ✅ Processed |
| TBM Daily Plans | Daily work activities by crew | 13.5K entries | ✅ Processed |
| ProjectSight | Daily reports, labor hours | 857K labor entries | ✅ Processed |
| Quality Records | Inspections (Yates WIR + SECAI IR) | 37K inspections | ✅ Processed |
| RABA | Quality inspections (RKCI Celvis) | 995+ daily batches | ✅ Scraped |
| PSI | Quality inspections (Construction Hive) | 6,309 reports | ✅ Scraped |
| QC Logs | Inspection request tracking (CPMS exports) | 61K+ records, 141 files | 📁 Raw |
| Fieldwire | Punch lists, field tasks | TBD | 🔄 In Progress |

**Key Deliverables:**
- Parsed CSV tables for all sources in `data/processed/`
- WBS taxonomy classifier (`src/classifiers/task_classifier.py`)
- Quality taxonomy extraction (`scripts/quality/derive/`)
- Data source documentation (`docs/SOURCES.md`, `docs/DATA_SOURCE_NOTES.md`)

### Phase 2: Integrated Analysis 🔄

**Status:** Planning Complete - Implementation Pending

**Goal:** Create dimension tables and mapping tables to enable cross-dataset analysis.

**Primary Objective:** Tie together Quality records and Hours data to Locations and Companies/Trades.

**Key Challenge:** Hours data (Weekly Labor, ProjectSight) lacks location fields. Solution uses company→location inference from P6 activity codes and TBM.

**Documentation:** [scripts/integrated_analysis/PLAN.md](scripts/integrated_analysis/PLAN.md)

**Deliverables:**
- `dim_company` - Master company list with alias resolution
- `dim_location` - Building + Level standardization
- `dim_trade` - Trade/work type classification
- `map_company_location` - Company work areas by period
- Cross-source integration views

### Phase 3: Analysis & Conclusions (Planned)

**Status:** Not Started

Four analysis tracks per Executive Summary:
1. Scope Evolution - Task growth attribution
2. Delay Attribution - Critical path impact analysis
3. Resource Consumption - Labor hours correlation
4. Quality Impact - Rework quantification

---

## Repository Structure

```
samsung-project/
├── CLAUDE.md                    # This file - project overview
├── docs/
│   ├── EXECUTIVE_SUMMARY.md     # Analysis goals and status
│   ├── SOURCES.md               # Data source inventory
│   ├── DATA_SOURCE_NOTES.md     # Technical parsing notes
│   └── analysis/                # Analysis documentation
├── scripts/
│   ├── primavera/               # P6 XER parsing and analysis
│   ├── weekly_reports/          # PDF report parsing
│   ├── tbm/                     # TBM Excel parsing
│   ├── projectsight/            # ProjectSight export processing
│   ├── quality/                 # Quality record processing
│   ├── raba/                    # RABA quality inspection scraper
│   ├── integrated_analysis/     # Phase 2 - cross-source integration
│   └── document_processor/      # Batch document-to-JSON extraction tool
├── src/
│   ├── config/settings.py       # Path configuration
│   └── classifiers/             # WBS taxonomy classifier
└── data/                        # Git-tracked analysis outputs
```

## Data Directory Structure

External data (not in repo) follows traceability classification:

```
{WINDOWS_DATA_DIR}/
├── raw/{source}/           # Source files exactly as received
│                           # 100% traceable to external source
├── processed/{source}/     # Parsed/transformed data
│                           # 100% traceable to raw/
└── derived/{source}/       # Enhanced data with inference
                            # Includes assumptions - NOT fully traceable
```

## Key Configuration

- **Settings:** `src/config/settings.py` - All path constants
- **Environment:** `.env` file with `WINDOWS_DATA_DIR` path
- **Python Environment:** Use the existing `.venv` virtual environment for all Python scripts and installations. Activate with `source .venv/bin/activate`

## Analysis Objectives

1. **Scope Evolution** - How much scope was added due to rework or coordination issues?
2. **Delay Attribution** - How much schedule impact resulted from quality and performance issues?
3. **Resource Consumption** - How much labor was consumed and what factors drove consumption?
4. **Quality Impact** - What quality issues occurred and what rework did they drive?

## Data Traceability

All analysis must maintain traceability to source documents:
- `raw/` data is untouched source files
- `processed/` data is direct transformation (fully traceable)
- `derived/` data includes assumptions (document methodology)

See [.claude/skills/mxi-powerpoint/SKILL.md](.claude/skills/mxi-powerpoint/SKILL.md) for presentation data traceability requirements.

---

## Tools

### Document Processor (Batch Unstructured → Structured)

**Location:** [scripts/document_processor/](scripts/document_processor/)

A CLI tool for batch processing unstructured documents (PDF, Word, text) into structured JSON using Claude Code in non-interactive mode.

**Features:**
- Multi-format parsing (PDF via PyMuPDF, DOCX via python-docx, TXT/MD)
- Concurrent processing with configurable parallelism
- Idempotency (`--skip-existing` flag)
- Automatic skip for documents exceeding token limit (default: 100K)
- Rate limit handling with exponential backoff
- Error rate monitoring (aborts if >50% failure after 10 files)
- Subdirectory preservation in output structure
- Full source filepath in output metadata

**Usage:**
```bash
python scripts/document_processor/process_documents.py \
  -i /path/to/documents \
  -o /path/to/output \
  -p "Your extraction prompt" \
  --schema schema.json \      # Optional JSON schema
  --model sonnet \            # sonnet, opus, or haiku
  --concurrency 5 \           # Parallel limit
  --skip-existing             # Idempotency
```

**Output:** Each document produces `{filename}.json` with metadata and structured content.

### RABA Quality Reports Scraper

**Location:** [scripts/raba/process/scrape_raba_reports.py](scripts/raba/process/scrape_raba_reports.py)

A Playwright-based automation tool for downloading quality inspection reports from the RABA (RKCI Celvis) system. Downloads daily batch PDFs containing all inspection reports for each date.

**Features:**
- Automated login with credentials from `.env`
- Daily batch downloads - selects all reports for a date and downloads as single PDF
- Idempotent operation via `manifest.json` tracking
- Empty date tracking (dates with no reports marked in manifest to avoid retries)
- Resume capability - skips already downloaded dates
- `--force` flag to re-download specific dates

**Output Structure:**
```
{WINDOWS_DATA_DIR}/raw/raba/
├── daily/
│   ├── 2023-06-01.pdf    # All reports for June 1, 2023
│   ├── 2023-06-02.pdf    # All reports for June 2, 2023
│   └── ...
└── manifest.json          # Download tracking
```

**Usage:**
```bash
# Download date range
python scripts/raba/process/scrape_raba_reports.py --start 2023-06-01 --end 2023-06-30

# Force re-download
python scripts/raba/process/scrape_raba_reports.py --start 2023-06-01 --end 2023-06-01 --force

# Non-headless mode for debugging
python scripts/raba/process/scrape_raba_reports.py --start 2023-06-01 --end 2023-06-01 --no-headless
```

**Environment Variables (`.env`):**
- `RABA_BASE_URL` - Login URL
- `RABA_USERNAME` - Login username
- `RABA_PASSWORD` - Login password

### PSI Quality Reports Scraper

**Location:** [scripts/psi/process/scrape_psi_reports.py](scripts/psi/process/scrape_psi_reports.py)

A Playwright-based automation tool for downloading quality inspection reports from the PSI (Construction Hive) system. Downloads individual PDF reports with metadata tracking.

**Features:**
- Automated login with credentials from `.env`
- Pagination handling (10 documents per page)
- Individual PDF downloads with metadata extraction
- Idempotent operation via `manifest.json` tracking by document UUID
- Resume capability via `--start-offset` parameter
- `--force` flag to re-download existing documents
- `--dry-run` mode to preview what would be downloaded
- `--headless` mode for background operation

**Output Structure:**
```
{WINDOWS_DATA_DIR}/raw/psi/
├── reports/
│   ├── DFR_0306103-9671-O1.pdf    # Individual report PDFs
│   ├── DFR_0306103-9670-O1.pdf
│   └── ...
├── manifest.json                   # Download tracking with metadata
└── scraper.log                     # Execution log
```

**Usage:**
```bash
# Download all documents (6309 total)
python scripts/psi/process/scrape_psi_reports.py

# Download with limit
python scripts/psi/process/scrape_psi_reports.py --limit 100

# Resume from specific offset
python scripts/psi/process/scrape_psi_reports.py --start-offset 500

# Run in headless mode
python scripts/psi/process/scrape_psi_reports.py --headless

# Force re-download existing files
python scripts/psi/process/scrape_psi_reports.py --force

# Dry run (show what would be downloaded)
python scripts/psi/process/scrape_psi_reports.py --dry-run
```

**Environment Variables (`.env`):**
- `PSI_BASE_URL` - Base URL (default: https://www.constructionhive.com/)
- `PSI_USERNAME` - Login email
- `PSI_PASSWORD` - Login password

### QC Logs (CPMS Inspection Tracking)

**Location:** `{WINDOWS_DATA_DIR}/raw/qc_logs/`

Excel exports from CPMS (Construction Project Management System) tracking all inspection requests across contractors. This is a consolidated view of inspection activity that complements the individual inspection reports from RABA and PSI.

**Data Structure:**
```
qc_logs/
├── DAILY INSPECTION REQUESTS/     # 141 daily snapshot files
│   ├── QA_QC Inspections 12-23-25 Official.xlsx
│   └── ...
└── MASTER LIST/                   # Cumulative logs by discipline
    ├── 12172025_USA T1 Project_Inspection and Test Log.xlsx
    └── 06112024_USA T1 Project_Inspection and Test Log.xlsm
```

**Daily Inspection Requests (61K+ records):**
| Column | Description |
|--------|-------------|
| Date | Inspection date |
| Time | Inspection time |
| Number | Sequential ID |
| IR Number | Inspection Request ID (prefix indicates contractor: YT=Yates, AG=Austin Bridge, SECAI, ABR) |
| Status | Accepted, Failure, Open, VOID, Re-Inspection Required, etc. |
| Template | Inspection type (759 unique types) |
| System / Equip/ Location | Location description |
| Inspector | Inspector name |

**Master List (by discipline: ARCH, MECH, ELEC):**
Additional fields include: Author Company, Module, Reasons for failure, Week, Year, ITP

**Key Metrics:**
- Status distribution: ~88% Accepted, ~3% Failure, ~3% Open, ~3% VOID
- Date range: 2023-02 through 2025-12
- Contractors: Yates (YT), Austin Bridge (AG), SECAI, ABR

**Use Cases:**
- Inspection volume trends over time
- Failure rate analysis by template/discipline
- Contractor performance comparison
- Re-inspection tracking and rework quantification
