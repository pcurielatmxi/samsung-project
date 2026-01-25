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
| Narratives | P6 narratives, weekly report narratives, milestone variance | 108 documents, 2.5K statements | ✅ Processed |

**Key Deliverables:**
- Parsed CSV tables for all sources in `data/processed/`
- WBS taxonomy classifier (`src/classifiers/task_classifier.py`)
- Quality taxonomy extraction (`scripts/quality/derive/`)
- Data source documentation (`docs/SOURCES.md`, `docs/DATA_SOURCE_NOTES.md`)
- Data quality fixes registry (`docs/DATA_QUALITY_FIXES.md`)

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

#### Unified Grid Coordinate System

**IMPORTANT:** The entire FAB1 project uses a **single unified grid coordinate system** across all buildings:
- Grid rows: A through N (north-south)
- Grid columns: 1 through 33+ (east-west)

While P6 data references different "buildings" (FAB, SUE, SUW, FIZ), these are conceptually different areas of the same facility sharing one continuous grid. For spatial joins:
- **Ignore building** - it's not a reliable discriminator
- **Use LEVEL + GRID only** - a grid coordinate like G/10 on 1F identifies a specific location regardless of building label
- Grid coordinates are the primary means to calculate affected rooms

This means `get_affected_rooms(level, grid_row, grid_col)` should match rooms across ALL buildings that contain those grid coordinates on that level.

#### Location Master Status

| Location Type | Total | With Grid Bounds | In Drawings | Status |
|---------------|-------|------------------|-------------|--------|
| ROOM | 367 | 315 (291 direct + 24 inferred) | 328 True / 39 False | 52 rooms need manual lookup |
| ELEVATOR | 11 | 10 | 10 True / 1 False | 90.9% grid coverage |
| STAIR | 66 | 66 | 11 True / 55 False | 100% grid coverage (extracted from task names) |
| GRIDLINE | 35 | 35 | Always True | Auto-generated (full row span) |
| LEVEL/AREA/BUILDING | 26 | N/A | Always True | Multi-room aggregates |

**Grid Inference:**
Rooms missing grid bounds can inherit coordinates from sibling rooms on other floors. FAB codes have structure `FAB1[FLOOR_AREA][ROOM_NUM]` where the same `ROOM_NUM` on different floors represents the same room type at the same grid location. The `grid_inferred_from` column tracks the source room when grids are inferred.

**Working Files:**
- `raw/location_mappings/location_master.csv` - Master location list with grid bounds
- `raw/location_mappings/Samsung_FAB_Codes_by_Gridline_3.xlsx` - Grid coordinate source (340 codes)
- `raw/location_mappings/rooms_needing_gridlines.csv` - 76 rooms missing grid bounds
- `raw/drawings/*.pdf` - Floor plan PDFs (1st-5th floor)

**In_Drawings Flag:**
The `in_drawings` column indicates whether a location code was found in the PDF floor drawings:
- **Rooms**: FAB1XXXXX codes checked against drawing text
- **Elevators/Stairs**: Numeric codes (ELV-01, STR-23) matched to FAB1-ELXX, FAB1-STXX in drawings
- **Multi-room types**: Always True (GRIDLINE, LEVEL, BUILDING, AREA, SITE)

**Known Gap - 74 rooms not in drawings (investigated 2026-01-23):**

The floor drawings (1st-5th-Floor.pdf) only cover SUE, SUW, and FIZ buildings:
| Building | In Drawings | Not In Drawings | Reason |
|----------|-------------|-----------------|--------|
| FAB | 0 | 29 | **No FAB building drawings available** |
| SUW | 119 | 24 | Equipment/utility rooms not on architectural drawings |
| SUE | 109 | 18 | Equipment/utility rooms not on architectural drawings |
| FIZ | 40 | 3 | Rooms added after drawings created |

These are NOT data quality issues - they are legitimate P6 rooms that don't appear in the available drawings. The FAB building (main fab) needs separate drawings to be added to `raw/drawings/`.

#### Key Scripts

| Script | Purpose |
|--------|---------|
| `scripts/primavera/derive/generate_location_master.py` | Generate location master from P6 taxonomy |
| `scripts/integrated_analysis/dimensions/build_dim_location.py` | Build dim_location with grid inference + drawing extraction |
| `scripts/shared/extract_location_grids.py` | Extract grid coordinates from P6 task names |
| `scripts/shared/populate_grid_bounds.py` | Sync grid bounds from Excel + check drawings |
| `scripts/shared/gridline_mapping.py` | Low-level grid coordinate lookup |
| `scripts/shared/location_model.py` | High-level location API (forward/reverse lookups) |
| `scripts/shared/company_standardization.py` | Company/trade/category normalization |
| `scripts/shared/dimension_lookup.py` | Dimension ID lookups for integration |

#### Dimension Mapping Coverage

Coverage of dimension IDs (`dim_location_id`, `dim_company_id`, `dim_trade_id`) by data source:

| Source | Records | Location | Company | Trade | Grid | Notes |
|--------|---------|----------|---------|-------|------|-------|
| **RABA** | 9,391 | 99.9% | 88.8% | 99.4% | 62.1% | Quality inspections (RKCI) |
| **PSI** | 6,309 | 98.2% | 99.0% | 96.7% | 73.3% | Quality inspections (Const Hive) |
| **TBM** | 13,539 | 91.5% | 98.2% | 37.7% | - | Daily plans - trade inferred |
| **ProjectSight** | 857,516 | - | 99.9% | 35.2% | - | Labor hours - no location |
| **Weekly Reports** | 10 | - | 100% | - | - | Aggregated labor by company |
| P6 Tasks | 470K | 97.6%* | N/A | 96.8% | - | *building only |

**Legend:** - not applicable/available in source

**Notes:**
- **Location** = building+level (coarse), **Grid** = grid coordinates (fine-grained)
- Trade coverage is lower for labor sources because trade is inferred from activity descriptions
- Grid coordinates in RABA/PSI enable spatial joins to room codes via `get_locations_at_grid()`

**Enriched Output Files:**
- `processed/raba/raba_consolidated.csv` - RABA with dimension IDs + grid bounds
- `processed/psi/psi_consolidated.csv` - PSI with dimension IDs + grid bounds
- `processed/tbm/work_entries_enriched.csv` - TBM with dimension IDs
- `processed/projectsight/labor_entries_enriched.csv` - ProjectSight with dimension IDs
- `processed/weekly_reports/labor_detail_by_company_enriched.csv` - Weekly Reports with dimension IDs

#### dim_location Structure

The location dimension (`processed/integrated_analysis/dimensions/dim_location.csv`) contains 505 location codes:

| Location Type | Count | Grid Coverage | In Drawings | Description |
|---------------|-------|---------------|-------------|-------------|
| ROOM | 367 | 86% (315) | 328 True | Room codes (FAB114402, FAB136302) |
| STAIR | 66 | 100% (66) | 11 True | Stair codes - FAB1-STXX format (drawing codes) |
| GRIDLINE | 35 | 100% (35) | Always True | Column-based locations (full row span) |
| ELEVATOR | 11 | 91% (10) | 10 True | Elevator codes - FAB1-ELXX format (drawing codes) |
| LEVEL | 9 | 0% | Always True | Level-only references |
| BUILDING | 12 | 0% | Always True | Building refs + building-wide (SUP-ALL) |
| AREA | 4 | 0% | Always True | Area references |
| SITE | 1 | 0% | Always True | Site-wide fallback |

**Key columns:** `location_code`, `location_type`, `p6_alias`, `building`, `level`, `grid_row_min/max`, `grid_col_min/max`, `grid_inferred_from`, `building_level`, `in_drawings`

**Spatial Join Workflow:**
```
Quality inspection at FAB-1F, grid G/10
    ↓
get_locations_at_grid('FAB', '1F', 'G', 10)
    ↓
Returns: rooms whose grid bounds contain G/10
```

#### Deliverables

- `dim_location` - 505 location codes with grid bounds, grid inference tracking, and in_drawings flag
- `dim_company` - Master company list with alias resolution
- `dim_trade` - Trade/work type classification
- `map_company_location` - Company work areas by period (derived from quality data)

#### Monthly Reports Consolidation 🔄

**Status:** In Development
**Design:** [docs/plans/2026-01-09-monthly-reports-design.md](docs/plans/2026-01-09-monthly-reports-design.md)

Monthly report system that consolidates all data sources for MXI internal analysis with LLM validation.

**Workflow:**
```
1. Script consolidates data → consolidated_data.md (metrics, tables, source citations)
2. LLM validates data quality → data_quality_notes.md (anomalies, verification)
3. LLM writes summary → executive_summary.md (factual narrative, findings)
```

**Report Sections:**
1. Schedule Progress & Delays - by location, trade; duration overages; critical path
2. Labor Hours & Consumption - by location, trade, company; anomaly detection
3. Quality Metrics & Issues - inspection pass/fail rates; failures by location/trade/company
4. Narrative Statements - categorized (justified vs unjustified delays); validation questions
5. Cross-Reference Analysis - quality→labor impact; quality→schedule impact; delay attribution

**Output Location:** `data/analysis/monthly_reports/{YYYY-MM}/`

**Scripts:** `scripts/integrated_analysis/monthly_reports/`

| Script | Purpose |
|--------|---------|
| `consolidate_month.py` | Main entry point - generates consolidated_data.md |
| `data_loaders/` | Source-specific data loading (P6, labor, quality, narratives) |
| `analyzers/` | Section generators (schedule, labor, quality, statements, cross-ref) |
| `output/markdown_writer.py` | Renders final markdown output |

**Usage:**
```bash
# Single month
python -m scripts.integrated_analysis.monthly_reports.consolidate_month 2024-03

# All months
python -m scripts.integrated_analysis.monthly_reports.consolidate_month --all

# Date range
python -m scripts.integrated_analysis.monthly_reports.consolidate_month --start 2023-01 --end 2023-12
```

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
│   ├── analysis/                # Analysis documentation
│   └── plans/                   # Design documents for new features
├── scripts/
│   ├── shared/                  # Cross-source utilities (location model, standardization)
│   ├── primavera/               # P6 XER parsing and analysis
│   ├── weekly_reports/          # PDF report parsing
│   ├── tbm/                     # TBM Excel parsing
│   ├── projectsight/            # ProjectSight export processing
│   ├── quality/                 # Quality record processing
│   ├── raba/                    # RABA scraper + document_processing config
│   ├── psi/                     # PSI scraper + document_processing config
│   ├── narratives/              # Narratives document_processing + embeddings search
│   └── integrated_analysis/     # Phase 2 - cross-source integration
│       └── monthly_reports/     # Monthly report consolidation scripts
├── src/
│   ├── config/settings.py       # Path configuration
│   ├── classifiers/             # WBS taxonomy classifier
│   └── document_processor/      # Centralized N-stage document processing
└── data/                        # Git-tracked analysis outputs
```

## Subfolder Documentation

Each `scripts/{source}/` folder has a `CLAUDE.md` file documenting that data source's pipeline.

**Requirements:**
- **Keep updated:** Update `CLAUDE.md` when modifying scripts in that folder
- **Keep concise:** Maximum 150 lines per file
- **Standard sections:** Purpose, Data Flow, Structure, Usage, Key Data

**Current documentation:**
- `scripts/shared/CLAUDE.md` - Cross-source utilities
- `scripts/primavera/CLAUDE.md` - P6 schedule processing
- `scripts/weekly_reports/CLAUDE.md` - PDF report parsing
- `scripts/tbm/CLAUDE.md` - TBM Excel parsing
- `scripts/projectsight/CLAUDE.md` - Labor and NCR processing
- `scripts/quality/CLAUDE.md` - Quality record processing
- `scripts/raba/CLAUDE.md` - RABA inspection pipeline
- `scripts/psi/CLAUDE.md` - PSI inspection pipeline
- `scripts/narratives/CLAUDE.md` - Document processing and embeddings

## Data Directory Structure

External data (not in repo) follows traceability classification:

```
{WINDOWS_DATA_DIR}/
├── raw/{source}/           # Source files exactly as received
│                           # 100% traceable to external source
├── processed/{source}/     # Parsed/transformed data (including AI-enriched)
│                           # 100% traceable to raw/
└── derived/{source}/       # ⛔ DEPRECATED - DO NOT USE
```

**⛔ DEPRECATED:** The `derived/` folder is deprecated and should NOT be used:
- **Data files:** All outputs go to `processed/`, not `derived/`
- **Scripts:** Use `scripts/shared/` or `scripts/{source}/process/`, not `scripts/{source}/derive/`
- **Existing files:** Being migrated to `processed/` over time

## Key Configuration

- **Settings:** `src/config/settings.py` - All path constants
- **Environment:** `.env` file with `WINDOWS_DATA_DIR` path
- **Python Environment:** Use the existing `.venv` virtual environment for all Python scripts and installations. Activate with `source .venv/bin/activate`
- **Gemini Model:** Use `gemini-3-flash-preview` for all AI enrichment and document processing. Older models have consistency issues.

## Analysis Objectives

1. **Scope Evolution** - How much scope was added due to rework or coordination issues?
2. **Delay Attribution** - How much schedule impact resulted from quality and performance issues?
3. **Resource Consumption** - How much labor was consumed and what factors drove consumption?
4. **Quality Impact** - What quality issues occurred and what rework did they drive?

## Data Traceability

All analysis must maintain traceability to source documents:
- `raw/` data is untouched source files
- `processed/` data is direct transformation (fully traceable), including AI-enriched outputs
- `derived/` folder is **⛔ DEPRECATED** - see Data Directory Structure above

See [.claude/skills/mxi-powerpoint/SKILL.md](.claude/skills/mxi-powerpoint/SKILL.md) for presentation data traceability requirements.

## Data Artifact Schema Stability

**IMPORTANT:** Generated data artifacts (CSV files in `processed/`) are production outputs consumed by Power BI dashboards and downstream analysis.

**Schema Rules:**
- ❌ **FORBIDDEN:** Removing existing columns or changing column names without approval
- ❌ **FORBIDDEN:** Changing column data types (e.g., string → int) without approval
- ✅ **ALLOWED:** Adding new columns
- ✅ **ALLOWED:** Updating data values within existing columns
- ✅ **ALLOWED:** Adding new rows

Breaking schema changes require explicit user approval as they may break Power BI data models and existing reports.

### Schema Validation System

**Location:** [`schemas/`](schemas/)

Pydantic-based schema definitions and validation tests ensure output files maintain stable schemas.

**Structure:**
```
schemas/
├── __init__.py          # Package exports
├── validator.py         # Core validation logic
├── registry.py          # File-to-schema mapping
├── dimensions.py        # DimLocation, DimCompany, DimTrade, DimCSISection
├── mappings.py          # MapCompanyAliases, MapCompanyLocation
├── quality.py           # QCInspectionConsolidated (RABA/PSI)
├── tbm.py               # TbmFiles, TbmWorkEntries, TbmWorkEntriesEnriched
├── ncr.py               # NcrConsolidated
└── weekly_reports.py    # WeeklyReports, KeyIssues, LaborDetail, etc.
```

**Registered Output Files:**
| Source | Files | Schema |
|--------|-------|--------|
| Dimensions | `dim_location.csv`, `dim_company.csv`, `dim_trade.csv`, `dim_csi_section.csv` | `DimLocation`, `DimCompany`, `DimTrade`, `DimCSISection` |
| Mappings | `map_company_aliases.csv`, `map_company_location.csv` | `MapCompanyAliases`, `MapCompanyLocation` |
| RABA | `raba_consolidated.csv` | `RabaConsolidated` |
| PSI | `psi_consolidated.csv` | `PsiConsolidated` |
| TBM | `work_entries.csv`, `work_entries_enriched.csv`, `tbm_files.csv` | `TbmWorkEntries`, `TbmWorkEntriesEnriched`, `TbmFiles` |
| NCR | `ncr_consolidated.csv` | `NcrConsolidated` |
| Weekly Reports | `weekly_reports.csv`, `key_issues.csv`, `labor_detail.csv`, etc. | Various |

**Usage:**
```python
from schemas import validate_output_file, get_schema_for_file

# Validate a file
schema = get_schema_for_file('dim_location.csv')
errors = validate_output_file('/path/to/dim_location.csv', schema)
if errors:
    print(f"Validation failed: {errors}")
```

**Running Tests:**
```bash
# Unit tests (schema definitions)
pytest tests/unit/test_schemas.py -v

# Integration tests (validate actual output files)
pytest tests/integration/test_output_schemas.py -v

# All schema tests
pytest tests/unit/test_schemas.py tests/integration/test_output_schemas.py -v
```

**Test Coverage:**
- 38 unit tests: Schema definitions, registry, type mapping, compatibility checking
- 28 integration tests: Validate actual output files against schemas

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

### Room Timeline Analysis Tool

**Location:** [scripts/integrated_analysis/room_timeline.py](scripts/integrated_analysis/room_timeline.py)

Query tool that retrieves all work entries and inspections for a specific room within a time window, with optional P6 schedule cross-reference.

**Purpose:** Answer "What happened at this room during this time period?" by consolidating RABA, PSI, and TBM data with schedule context.

**Features:**
- Searches RABA, PSI, and TBM for entries matching a room via `affected_rooms` JSON
- Separates **single room matches** (precise) from **multi-room matches** (shared)
- Optional P6 schedule lookup showing what was planned at the time of each event
- CSV export for further analysis

**Usage:**
```bash
# Basic usage
python -m scripts.integrated_analysis.room_timeline FAB116101 --start 2023-01-01 --end 2023-06-30

# Skip P6 schedule lookup (faster)
python -m scripts.integrated_analysis.room_timeline FAB116101 --start 2023-01-01 --end 2023-06-30 --no-schedule

# Export to CSV
python -m scripts.integrated_analysis.room_timeline FAB116101 --start 2023-01-01 --end 2023-06-30 --output timeline.csv
```

**Output Sections:**
1. **Single room matches** - Entries where ONLY this room was affected (~22% of RABA/PSI)
2. **Multi-room matches** - Entries where this room was one of several affected

**Schedule Cross-Reference Notes:**
- P6 room-level tasks only exist for interior work (2025+)
- Foundation work (2023) is not tracked at room level in P6
- Schedule context shows tasks that were in-progress, starting soon, or recently completed

### Document Embeddings Search

**Location:** [scripts/narratives/embeddings/](scripts/narratives/embeddings/)

Semantic search tool for project documents using Gemini embeddings and ChromaDB. Enables quick investigation of documentary evidence by searching across raw document chunks with rich metadata filtering.

**Index Contents:**
- 13,687 chunks from 304 narrative documents
- Source types: `narratives` (schedule narratives, weekly reports, expert reports, exhibits, meeting notes)

**Features:**
- Semantic search using `gemini-embedding-001` (768 dimensions)
- Source-based indexing with `--source` flag
- Rich metadata filtering (source, document type, author, date range, subfolder)
- Context navigation (show adjacent chunks)
- Idempotent incremental builds with deduplication
- Cross-computer sync via OneDrive backup

**Storage Architecture:**
```
~/.local/share/samsung-embeddings/documents/   # WSL local (fast queries)
    ├── chroma.sqlite3                         # SQLite metadata
    └── {uuid}/                                # HNSW vector index

{WINDOWS_DATA_DIR}/backup/embeddings/documents/  # OneDrive backup (sync)
```

- **Primary:** WSL local path for fast queries
- **Backup:** OneDrive for cross-computer sync
- **Auto-restore:** If WSL folder is empty, automatically copies from OneDrive on first use

**Usage:**
```bash
# Build index (--source required)
python -m scripts.narratives.embeddings build --source narratives
python -m scripts.narratives.embeddings build --source narratives --force   # Rebuild all
python -m scripts.narratives.embeddings build --source narratives --sync    # Sync to OneDrive after

# Search all sources
python -m scripts.narratives.embeddings search "HVAC delays"
python -m scripts.narratives.embeddings search "scope changes" --limit 5

# Search with filters
python -m scripts.narratives.embeddings search "delay" --source narratives
python -m scripts.narratives.embeddings search "delay" --author Yates
python -m scripts.narratives.embeddings search "delay" --type schedule_narrative
python -m scripts.narratives.embeddings search "meeting" --subfolder meeting_notes
python -m scripts.narratives.embeddings search "delay" --after 2024-01-01 --before 2024-06-30

# Show context (adjacent chunks)
python -m scripts.narratives.embeddings search "delay" --context 2 --limit 5

# Check index status
python -m scripts.narratives.embeddings status

# Manual sync to OneDrive
python -m scripts.narratives.embeddings sync
```

**Python API:**
```python
from scripts.narratives.embeddings import search_chunks

# Search with filters
results = search_chunks(
    query="HVAC delay",
    source_type="narratives",
    author="Yates",
    limit=10
)

for r in results:
    print(f"{r.score}: {r.text[:100]}...")
    print(f"  Source: {r.source_file} | Date: {r.file_date}")
```

**Output Format:**
```
[1] Score: 0.72
    Text: "However, there is still 8 missing spools of CSF pipe..."
    Source: narratives | Type: eot_claim | Date: 2025-04-30 | Author: Yates
    Source: 2025.04.30 - OFFICIAL NOTICE - Yates Response to LT-0793.pdf (page 1) [chunk 7/30]
```

**Metadata Fields:**
- `source_type`: narratives (future: raba, psi)
- `document_type`: schedule_narrative, weekly_report, expert_report, meeting_notes, eot_claim, etc.
- `author`: Yates, SECAI, BRG, Samsung
- `file_date`: Extracted from filename (YYYY-MM-DD)
- `subfolder`: Relative path within source directory

**Robustness:**
- Content-based change detection (SHA-256, not mtime)
- Manifest tracks indexed files (`~/.local/share/samsung-embeddings/manifest.json`)
- Automatic backups before destructive operations
- `--cleanup-deleted` flag required to remove stale chunks
- `verify` command checks manifest/DB consistency

**Dependencies:** `chromadb`, `google-genai` (in requirements.txt)
