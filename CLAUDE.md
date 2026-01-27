# Samsung Taylor FAB1 Performance Analysis

**Project:** Data-driven analysis of schedule delays and labor consumption
**Client:** Samsung | **Analyst:** MXI

---

## Project Phases

### Phase 1: Data Collection & Initial Assessment ✅

Established data pipelines for 9 primary sources:

| Source | Purpose | Records | Status |
|--------|---------|---------|--------|
| Primavera P6 | Schedule snapshots | 66 files, 964K tasks | ✅ |
| Weekly Reports | Issues, progress, manpower | 37 reports, 1,108 issues | ✅ |
| TBM Daily Plans | Daily work activities | 5,983 files | 🔄 Needs Reprocessing |
| ProjectSight | Labor hours | 857K entries | ✅ |
| ProjectSight NCR | Non-conformance records | 1,985 records | ✅ |
| Quality Records | WIR + IR inspections | 37K inspections | ✅ |
| RABA | Quality inspections (RKCI) | 9,391 reports | ✅ |
| PSI | Quality inspections (Const Hive) | 6,309 reports | ✅ |
| QC Logs | Inspection tracking (CPMS) | 61K+ records | 📁 Raw |
| Narratives | P6/weekly narratives | 108 docs, 2.5K statements | ✅ |

### Phase 2: Integrated Analysis 🔄

**Goal:** Link all data sources through unified location model to answer: "What quality issues occurred WHERE, by WHOM, and how much rework did they cause?"

#### Solution: `dim_location` with Grid Bounds

Location dimension table with **grid bounds** enables spatial joins:
- **Room → Grid**: Look up grid bounds for any room code
- **Grid → Room(s)**: Find which rooms contain a grid coordinate
- **Company → Location**: Infer from quality inspection patterns

#### Unified Grid System

**IMPORTANT:** Single unified grid across all buildings (FAB, SUE, SUW, FIZ):
- Grid rows: A-N (north-south), columns: 1-33+ (east-west)
- For spatial joins: **Ignore building, use LEVEL + GRID only**
- `get_affected_rooms(level, grid_row, grid_col)` matches rooms across ALL buildings

#### Location Master Status

| Type | Total | Grid Coverage | Notes |
|------|-------|---------------|-------|
| ROOM | 367 | 86% (315/367) | 52 need manual lookup |
| STAIR | 66 | 100% | Extracted from task names |
| GRIDLINE | 35 | 100% | Auto-generated |
| ELEVATOR | 11 | 91% (10/11) | - |
| LEVEL/AREA/BUILDING | 26 | N/A | Multi-room aggregates |

**Grid Inference:** Rooms inherit coordinates from siblings on other floors via `FAB1[FLOOR_AREA][ROOM_NUM]` structure.

**Working Files:**
- `raw/location_mappings/location_master.csv` - Master with grid bounds
- `raw/location_mappings/Samsung_FAB_Codes_by_Gridline_3.xlsx` - Grid source
- `raw/drawings/*.pdf` - Floor plans (1st-5th floor, SUE/SUW/FIZ only)

#### Dimension Coverage

| Source | Records | Location | Company | Trade | Grid |
|--------|---------|----------|---------|-------|------|
| RABA | 9,391 | 99.9% | 88.8% | 99.4% | 62.1% |
| PSI | 6,309 | 98.2% | 99.0% | 96.7% | 73.3% |
| TBM | 13,539 | 91.5% | 98.2% | 37.7% | - |
| ProjectSight | 857K | - | 99.9% | 35.2% | - |

**Key Scripts:**
- `scripts/integrated_analysis/dimensions/build_dim_location.py` - Build dim_location
- `scripts/shared/location_model.py` - High-level location API
- `scripts/shared/dimension_lookup.py` - Dimension ID lookups

**⚠️ CRITICAL: Centralized Location Processing**
All location enrichment (grid parsing, room matching, dim_location_id lookup) MUST use `scripts/integrated_analysis/location/`. Do NOT duplicate location logic in source scripts. Import `enrich_location()` from that module. See `scripts/integrated_analysis/location/CLAUDE.md` for details.

#### Monthly Reports (In Development)

Consolidates all sources monthly for MXI analysis. Sections: Schedule Progress, Labor Hours, Quality Metrics, Narratives, Cross-Reference Analysis.

**Usage:** `python -m scripts.integrated_analysis.monthly_reports.consolidate_month 2024-03`

### Phase 3: Analysis & Conclusions (Planned)

1. Scope Evolution - Task growth attribution
2. Delay Attribution - Critical path impact
3. Resource Consumption - Labor correlation
4. Quality Impact - Rework quantification

---

## Repository Structure

```
samsung-project/
├── CLAUDE.md                    # This file
├── docs/                        # Documentation (EXECUTIVE_SUMMARY, SOURCES, plans/)
├── scripts/
│   ├── shared/                  # Cross-source utilities
│   ├── primavera/               # P6 XER parsing
│   ├── {source}/                # Source-specific processing (weekly_reports, tbm, etc.)
│   └── integrated_analysis/     # Cross-source integration
├── src/
│   ├── config/settings.py       # Path configuration
│   ├── classifiers/             # WBS taxonomy
│   └── document_processor/      # N-stage document pipeline
└── data/                        # Git-tracked outputs
```

**Subfolder Documentation:** Each `scripts/{source}/` has `CLAUDE.md` (≤150 lines) documenting that pipeline.

## Data Directory Structure

```
{WINDOWS_DATA_DIR}/
├── raw/{source}/           # Source files as received (100% traceable)
├── processed/{source}/     # Parsed/transformed (traceable to raw/)
└── derived/{source}/       # ⛔ DEPRECATED - DO NOT USE
```

## Configuration

- **Settings:** `src/config/settings.py` - All path constants
- **Environment:** `.env` with `WINDOWS_DATA_DIR` path
- **Python:** Use existing `.venv` virtual environment
- **AI Model:** Use `gemini-3-flash-preview` for all processing

## Analysis Objectives

1. **Scope Evolution** - Rework/coordination-driven scope additions
2. **Delay Attribution** - Schedule impact from quality/performance issues
3. **Resource Consumption** - Labor consumption drivers
4. **Quality Impact** - Quality issues and resulting rework

## Data Artifact Schema Stability

**CRITICAL:** CSV files in `processed/` are production outputs consumed by Power BI.

**Schema Rules:**
- ❌ **FORBIDDEN:** Remove columns, rename columns, change data types (without approval)
- ✅ **ALLOWED:** Add columns, update values, add rows

**Schema Validation:** `schemas/` contains Pydantic definitions for all output files.

**Run Tests:** `pytest tests/unit/test_schemas.py tests/integration/test_output_schemas.py -v`

**Registered Files:** dim_location, dim_company, dim_trade, dim_csi_section, map_company_aliases, map_company_location, raba_consolidated, psi_consolidated, tbm_*, ncr_consolidated, weekly_reports_*

## Quality Data Architecture

Three complementary sources provide different views of inspection events:

**QC Logs (CPMS)** - Master tracking list (61K+ requests)
- What: IR Number, Date, Status, Template, Location, Inspector
- Use: Volume trends, pass/fail rates, contractor metrics

**RABA (RKCI)** + **PSI (Const Hive)** - Detailed inspection reports (15K+ PDFs)
- What: Full PDFs with photos, signatures, findings, defects
- Use: Root cause analysis, photo evidence

**Relationship:** QC Logs = "what happened" summary; RABA/PSI = "why it happened" evidence

---

## Tools

### Document Processor (N-Stage Pipeline)

**Location:** `src/document_processor/`

Centralized pipeline with flexible N stages, implicit chaining, LLM-based QC.

**Stage Types:**
- `llm`: Gemini processing (PDF native, DOCX/XLSX text extraction)
- `script`: Python postprocessing

**Features:** N stages (not fixed), per-stage QC with auto-halt on >10% failure, idempotent via `.error.json`, source-specific configs in `scripts/{source}/document_processing/`

**Usage:**
```bash
python -m src.document_processor scripts/raba/document_processing/
python -m src.document_processor scripts/raba/document_processing/ --stage extract
python -m src.document_processor scripts/raba/document_processing/ --status
```

**Options:** `--force`, `--retry-errors`, `--limit N`, `--dry-run`, `--bypass-qc-halt`, `--disable-qc`

**Current Pipelines:**
- RABA/PSI: extract → format → clean
- Narratives: extract only

**Quick Commands:**
```bash
cd scripts/raba/document_processing
./run.sh status              # Check progress
./run.sh extract --limit 50  # Extract 50 files
./run.sh retry               # Retry failures
```

### RABA Individual Reports Scraper (Recommended)

**Location:** `scripts/raba/process/scrape_raba_individual.py`

Playwright-based scraper downloading individual PDFs (one per inspection). Preferred over batch scraper (works with single-report days, no post-splitting needed).

**Features:** Month-by-month with pagination, individual PDFs named by assignment number, idempotent via manifest, resume capability

**Usage:**
```bash
python scripts/raba/process/scrape_raba_individual.py  # All from May 2022
python scripts/raba/process/scrape_raba_individual.py --start-date 2022-06-01 --end-date 2022-06-30
python scripts/raba/process/scrape_raba_individual.py --limit 10  # Test
```

**Environment:** `RABA_USERNAME`, `RABA_PASSWORD` in `.env`

### PSI Quality Reports Scraper

**Location:** `scripts/psi/process/scrape_psi_reports.py`

Similar Playwright scraper for PSI (Construction Hive). Downloads individual PDFs with metadata.

**Usage:**
```bash
python scripts/psi/process/scrape_psi_reports.py  # All 6309 docs
python scripts/psi/process/scrape_psi_reports.py --limit 100 --headless
```

**Environment:** `PSI_BASE_URL`, `PSI_USERNAME`, `PSI_PASSWORD` in `.env`

### QC Logs (CPMS Inspection Tracking)

**Location:** `{WINDOWS_DATA_DIR}/raw/qc_logs/`

Excel exports tracking all inspections (61K+ records).

**Structure:**
- `DAILY INSPECTION REQUESTS/` - 141 daily snapshots
- `MASTER LIST/` - Cumulative by discipline (ARCH, MECH, ELEC)

**Key Fields:** Date, Time, IR Number, Status, Template, Location, Inspector, Failure reasons

**Metrics:** ~88% Accepted, ~3% Failure, date range 2023-02 to 2025-12

### Room Timeline Analysis

**Location:** `scripts/integrated_analysis/room_timeline.py`

Query tool: "What happened at this room during this period?" Consolidates RABA, PSI, TBM with optional P6 schedule cross-reference.

**Usage:**
```bash
python -m scripts.integrated_analysis.room_timeline FAB116101 --start 2023-01-01 --end 2023-06-30
python -m scripts.integrated_analysis.room_timeline FAB116101 --start 2023-01-01 --end 2023-06-30 --no-schedule
python -m scripts.integrated_analysis.room_timeline FAB116101 --start 2023-01-01 --end 2023-06-30 --output timeline.csv
```

**Output:** Single room matches (precise) + multi-room matches (shared)

### Document Embeddings Search

**Location:** `scripts/narratives/embeddings/`

Semantic search using Gemini embeddings + ChromaDB. 13,687 chunks from 304 narrative documents.

**Features:** Semantic search, rich metadata filtering (source, type, author, date, subfolder), context navigation, cross-computer sync via OneDrive

**Storage:**
- Primary: `~/.local/share/samsung-embeddings/documents/` (WSL, fast queries)
- Backup: `{WINDOWS_DATA_DIR}/backup/embeddings/documents/` (OneDrive, auto-synced)

**Usage:**
```bash
# Build (auto-syncs to OneDrive)
python -m scripts.narratives.embeddings build --source narratives

# Search
python -m scripts.narratives.embeddings search "HVAC delays"
python -m scripts.narratives.embeddings search "delay" --source narratives --author Yates --limit 5
python -m scripts.narratives.embeddings search "delay" --after 2024-01-01 --before 2024-06-30
python -m scripts.narratives.embeddings search "delay" --context 2  # Show adjacent chunks

# Status & sync
python -m scripts.narratives.embeddings status
python -m scripts.narratives.embeddings sync
```

**Enrichment (for cross-dataset integration):**
```bash
python -m scripts.narratives.embeddings enrich --source narratives
```

Extracts locations, CSI codes, companies to link narrative chunks with P6 tasks and quality inspections.

**Metadata Fields:**
- Standard: `source_type`, `document_type`, `author`, `file_date`, `subfolder`
- Enriched: `doc_locations`, `chunk_locations`, `doc_buildings`, `chunk_buildings`, `doc_levels`, `chunk_levels`, `doc_csi_sections`, `chunk_csi_sections`, `doc_company_ids`, `chunk_company_ids`

**Python API:**
```python
from scripts.narratives.embeddings import search_chunks

results = search_chunks(query="HVAC delay", source_type="narratives", author="Yates", limit=10)
for r in results:
    print(f"{r.score}: {r.text[:100]}... | {r.source_file} | {r.file_date}")
```

**Dependencies:** `chromadb`, `google-genai` (in requirements.txt)
