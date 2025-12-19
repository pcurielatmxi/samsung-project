# Samsung Taylor FAB1 Performance Analysis

**Project:** Data-driven analysis of schedule delays and labor consumption
**Client:** Samsung
**Analyst:** MXI

---

## Project Phases

### Phase 1: Data Collection & Initial Assessment ✅

**Status:** Complete

Established data pipelines for 6 primary sources:

| Source | Purpose | Records | Status |
|--------|---------|---------|--------|
| Primavera P6 | Schedule snapshots (YATES + SECAI) | 66 files, 964K tasks | ✅ Processed |
| Weekly Reports | Issues, progress, manpower | 37 reports, 1,108 issues | ✅ Processed |
| TBM Daily Plans | Daily work activities by crew | 13.5K entries | ✅ Processed |
| ProjectSight | Daily reports, labor hours | 857K labor entries | ✅ Processed |
| Quality Records | Inspections (Yates WIR + SECAI IR) | 37K inspections | ✅ Processed |
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
