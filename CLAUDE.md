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
| ProjectSight NCR | Quality non-conformance records (Notice to Comply) | 1,985 records (NCR/QOR/SOR/SWN/VR) | ✅ Processed |
| Quality Records | Inspections (Yates WIR + SECAI IR) | 37K inspections | ✅ Processed |
| RABA | Quality inspections (RKCI Celvis) | 995+ daily batches | ✅ Scraped |
| PSI | Quality inspections (Construction Hive) | 6,309 reports | ✅ Scraped |
| QC Logs | Inspection request tracking (CPMS exports) | 61K+ records, 141 files | 📁 Raw |
| Fieldwire | Punch lists, field tasks | TBD | 🔄 In Progress |
| Narratives | P6 narratives, weekly report narratives, milestone variance | ~80 documents | 🔄 Processing |

**Key Deliverables:**
- Parsed CSV tables for all sources in `data/processed/`
- WBS taxonomy classifier (`src/classifiers/task_classifier.py`)
- Quality taxonomy extraction (`scripts/quality/derive/`)
- Data source documentation (`docs/SOURCES.md`, `docs/DATA_SOURCE_NOTES.md`)

### Phase 2: Integrated Analysis 🔄

**Status:** In Progress

**Goal:** Link all data sources through a unified location model to enable cross-dataset analysis.

**Primary Objective:** Answer "What quality issues occurred WHERE, by WHOM, and how much rework did they cause?"

#### The Integration Challenge

Each data source has different location granularity:

| Source | Location Data | Linkage Key |
|--------|---------------|-------------|
| P6 Tasks | Room codes (FAB112345), Building, Level | `location_code` |
| RABA/PSI | Building, Level, Grid (e.g., G/10) | Grid coordinates |
| Labor Hours | Company only | Company → Trade → Location inference |

#### Solution: `dim_location` with Grid Bounds

The centerpiece is a location dimension table where every room/elevator/stair has **grid bounds** (row_min/max, col_min/max). This enables:

1. **Room → Grid**: Look up grid bounds for any room code
2. **Grid → Room(s)**: Reverse lookup - find which rooms contain a grid coordinate
3. **Company → Location**: Infer from quality inspection patterns (e.g., "Berg works drywall on SUE levels 2-4")

```
┌─────────────────────────────────────────────────────────────────┐
│                       dim_location                               │
│  location_code │ building │ level │ grid_row_min/max │ grid_col_min/max │
├─────────────────────────────────────────────────────────────────┤
│  FAB112345     │ SUE      │ 1F    │ B / E            │ 5 / 12           │
│  ELV-S         │ SUW      │ 2F    │ L / M            │ 17 / 17          │
│  ...           │          │       │                  │                  │
└─────────────────────────────────────────────────────────────────┘
         │                              │
         ▼                              ▼
┌─────────────────┐           ┌─────────────────┐
│    P6 Tasks     │           │  Quality Data   │
│ JOIN ON         │           │ SPATIAL JOIN    │
│ location_code   │           │ WHERE grid IN   │
│                 │           │ (row/col bounds)│
└─────────────────┘           └─────────────────┘
```

#### Location Master Status

| Location Type | Total | With Grid Bounds | Status |
|---------------|-------|------------------|--------|
| ROOM | 360 | ~60 | Needs manual lookup from drawings |
| ELEVATOR | 13 | 13 | Complete |
| STAIR | 25 | ~10 | Partial |
| GRIDLINE | 35 | 35 | Auto-generated (full row span) |
| LEVEL/AREA | 90 | N/A | Special cases |

**Working File:** `raw/location_mappings/location_master.csv`
**Grid Source:** `raw/location_mappings/Samsung_FAB_Codes_by_Gridline_3.xlsx`

#### Key Scripts

| Script | Purpose |
|--------|---------|
| `scripts/primavera/derive/generate_location_master.py` | Generate location master from P6 taxonomy |
| `scripts/shared/gridline_mapping.py` | Low-level grid coordinate lookup |
| `scripts/shared/location_model.py` | High-level location API (forward/reverse lookups) |
| `scripts/shared/company_standardization.py` | Company/trade/category normalization |

#### Deliverables

- `dim_location` - All locations with grid bounds (in progress)
- `dim_company` - Master company list with alias resolution
- `dim_trade` - Trade/work type classification
- `map_company_location` - Company work areas by period (derived from quality data)

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
│   ├── shared/                  # Cross-source utilities (location model, standardization)
│   ├── primavera/               # P6 XER parsing and analysis
│   ├── weekly_reports/          # PDF report parsing
│   ├── tbm/                     # TBM Excel parsing
│   ├── projectsight/            # ProjectSight export processing
│   ├── quality/                 # Quality record processing
│   ├── raba/                    # RABA scraper + document_processing config
│   ├── psi/                     # PSI scraper + document_processing config
│   ├── narratives/              # Narratives document_processing config
│   └── integrated_analysis/     # Phase 2 - cross-source integration
├── src/
│   ├── config/settings.py       # Path configuration
│   ├── classifiers/             # WBS taxonomy classifier
│   └── document_processor/      # Centralized N-stage document processing
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

## Quality Data Architecture

Quality inspection data is central to the project's rework and delay analysis. Three complementary data sources provide different views of the same inspection events:

```
┌─────────────────────────────────────────────────────────────────────┐
│                      QUALITY DATA SOURCES                           │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  QC Logs (CPMS)              RABA (RKCI Celvis)    PSI (Const Hive) │
│  ════════════════            ═════════════════     ════════════════ │
│  Master tracking list        3rd-party QC firm    3rd-party QC firm│
│  61K+ inspection requests    9K+ inspection recs  6K+ field reports│
│                                                                     │
│  ┌─────────────────┐         ┌──────────────┐     ┌──────────────┐ │
│  │ • IR Number     │         │ • Full PDF   │     │ • Full PDF   │ │
│  │ • Date/Time     │  ───►   │ • Photos     │     │ • Photos     │ │
│  │ • Status        │  Detail │ • Signatures │     │ • Checklists │ │
│  │ • Template      │  View   │ • Findings   │     │ • Findings   │ │
│  │ • Location      │         │ • Defects    │     │ • Defects    │ │
│  │ • Failure reason│         └──────────────┘     └──────────────┘ │
│  └─────────────────┘                                                │
│        List View                    Detail Views                    │
│                                                                     │
├─────────────────────────────────────────────────────────────────────┤
│  RELATIONSHIP: QC Logs tracks ALL inspection requests from ALL     │
│  contractors. RABA and PSI contain the actual inspection reports   │
│  with photos, signatures, and detailed findings. Some QC Log       │
│  entries reference PSI via "Duplicate of ProjectSight#XXXX".       │
├─────────────────────────────────────────────────────────────────────┤
│  CONTRACTOR COVERAGE (from QC Logs IR Number prefixes):            │
│  • YT (Yates): 15K records        • SECAI: 2K records              │
│  • AG/ABR (Austin Bridge): 4K     • 50+ subcontractors: 40K        │
├─────────────────────────────────────────────────────────────────────┤
│  USE CASES:                                                         │
│  • QC Logs → Volume trends, pass/fail rates, contractor metrics    │
│  • RABA/PSI → Defect details, root cause analysis, photo evidence  │
└─────────────────────────────────────────────────────────────────────┘
```

**Key Insight:** QC Logs provides the "what happened" summary (pass/fail, counts, dates), while RABA/PSI provide the "why it happened" evidence (detailed PDF reports with photos and findings).

---

## Tools

### Document Processor (Centralized N-Stage Pipeline)

**Location:** [src/document_processor/](src/document_processor/)

Centralized document processing pipeline with flexible N-stage support, implicit stage chaining, and LLM-based quality checking.

**Architecture:**
```
src/document_processor/
├── pipeline.py           # N-stage runner with QC integration
├── config.py             # N-stage config loader
├── cli.py                # Unified CLI entry point
├── quality_check.py      # QC sampling and halt logic
├── stages/               # Stage implementations
│   ├── llm_stage.py      # Gemini PDF/text processing
│   └── script_stage.py   # Python postprocessing
├── clients/
│   └── gemini_client.py  # Gemini API wrapper
└── utils/
    ├── file_utils.py     # Atomic writes, error files
    └── status.py         # Pipeline status analysis
```

**Stage Types:**
- `llm`: Process documents with Gemini (PDF native upload, DOCX/XLSX text extraction)
- `script`: Custom Python postprocessing (location parsing, date normalization, etc.)

**Features:**
- Flexible N stages (not fixed to 2) - configure as many as needed
- Implicit chaining: Stage 1 uses input files, Stage N uses Stage N-1 output
- Per-stage QC prompts with automatic halt on >10% failure rate
- Idempotency via `.error.json` files
- Sequential numbered output folders (1.extract/, 2.format/, 3.clean/)
- Source-specific configs in `scripts/{source}/document_processing/`

**Config Schema (config.json):**
```json
{
  "input_dir": "${WINDOWS_DATA_DIR}/raw/raba/individual",
  "output_dir": "${WINDOWS_DATA_DIR}/processed/raba",
  "file_extensions": [".pdf"],
  "concurrency": 5,
  "qc_batch_size": 50,
  "qc_failure_threshold": 0.10,
  "stages": [
    {"name": "extract", "type": "llm", "model": "gemini-3-flash-preview", "prompt_file": "extract_prompt.txt"},
    {"name": "format", "type": "llm", "model": "gemini-3-flash-preview", "prompt_file": "format_prompt.txt", "schema_file": "schema.json"},
    {"name": "clean", "type": "script", "script": "postprocess.py", "function": "process_record"}
  ]
}
```

**Usage:**
```bash
# Run all stages
python -m src.document_processor scripts/raba/document_processing/

# Run specific stage
python -m src.document_processor scripts/raba/document_processing/ --stage extract

# Show status
python -m src.document_processor scripts/raba/document_processing/ --status

# Common options
--force              # Reprocess completed files
--retry-errors       # Retry failed files only
--limit N            # Process N files max
--dry-run            # Preview without processing
--bypass-qc-halt     # Continue despite QC halt file
--disable-qc         # Skip quality checks
```

**Quality Check System:**
- Samples 1 file per `qc_batch_size` files processed
- QC prompt verifies input→output quality with LLM
- Tracks failure rate; halts if >10% after min samples
- Creates `.qc_halt.json` requiring prompt fix or `--bypass-qc-halt`

**Output Structure:**
```
processed/raba/
├── 1.extract/
│   ├── {file}.extract.json
│   └── {file}.extract.error.json
├── 2.format/
│   └── {file}.format.json
├── 3.clean/
│   └── {file}.clean.json
└── .qc_halt.json  # If QC failure rate exceeded
```

### Source-Specific Pipelines

Each data source has its own config in `scripts/{source}/document_processing/`:

| Source | Stages | Config Location |
|--------|--------|-----------------|
| RABA | extract → format → clean | `scripts/raba/document_processing/` |
| PSI | extract → format → clean | `scripts/psi/document_processing/` |
| Narratives | extract (only) | `scripts/narratives/document_processing/` |

**RABA/PSI Pipeline:**
```bash
cd scripts/raba/document_processing
./run.sh status              # Check progress
./run.sh test 10             # Dry run 10 files
./run.sh extract --limit 50  # Extract 50 files
./run.sh format              # Format all extracted
./run.sh clean               # Normalize all formatted
./run.sh retry               # Retry failures
```

**Narratives Pipeline (single stage):**
```bash
cd scripts/narratives/document_processing
./run.sh extract --limit 10  # Extract narrative documents
./run.sh status              # Check progress
```

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

### RABA Individual Reports Scraper (Recommended)

**Location:** [scripts/raba/process/scrape_raba_individual.py](scripts/raba/process/scrape_raba_individual.py)

An improved Playwright-based automation tool that downloads RABA reports as individual PDFs (one per inspection assignment). This approach is preferred over the batch scraper because:
- No need to split multi-report batch PDFs afterward
- Works for days with only 1 report (batch button unavailable in RABA UI)
- Each PDF is named by assignment number for direct processing
- Handles pagination for months with many reports

**Features:**
- Automated login with credentials from `.env`
- Month-by-month processing with pagination support
- Individual PDF downloads named by assignment number (e.g., `A22-016871.pdf`)
- Idempotent operation via `manifest.json` tracking each report
- Resume capability - skips already downloaded reports
- `--force` flag to re-download existing files
- `--limit` flag for testing (limits total downloads)

**Output Structure:**
```
{WINDOWS_DATA_DIR}/raw/raba/
├── individual/
│   ├── A22-016104.pdf    # Individual inspection report
│   ├── A22-016105.pdf
│   ├── A22-016871.pdf
│   └── ...
└── individual_manifest.json   # Download tracking with metadata per report
```

**Usage:**
```bash
# Download all reports from project start (May 2022) to now
python scripts/raba/process/scrape_raba_individual.py

# Download specific date range
python scripts/raba/process/scrape_raba_individual.py --start-date 2022-06-01 --end-date 2022-06-30

# Test with limit
python scripts/raba/process/scrape_raba_individual.py --limit 10

# Force re-download
python scripts/raba/process/scrape_raba_individual.py --start-date 2022-06-01 --end-date 2022-06-30 --force

# Run headless (for background operation)
python scripts/raba/process/scrape_raba_individual.py --headless
```

**Environment Variables (`.env`):**
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
