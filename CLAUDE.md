# MXI Samsung ETL Pipeline - Quick Reference

## Project Overview

ETL pipeline for integrating construction project data from multiple sources into PostgreSQL for analysis. Orchestrated with Apache Airflow 2.7.0.

**Data Sources:**
- **ProjectSight (Trimble)** - Web scraping via Playwright
- **Fieldwire** - REST API integration
- **Primavera P6 (XER files)** - Schedule data extraction

## Architecture

```
External Systems → Extract → Transform → Load → PostgreSQL
                      ↓          ↓         ↓
                   Connectors  Utilities Database
```

Each layer is independently testable, validated, and logged.

## Project Structure

- `src/connectors/` - System connectors (API, web scraper)
- `src/extractors/` - Data extraction logic (ProjectSight, Fieldwire)
- `src/transformers/` - Data normalization and standardization
- `src/loaders/` - Load to PostgreSQL or files
- `src/utils/` - Logging, validation, helpers
- `src/config/` - Environment-based settings
- `dags/` - Airflow DAG definitions
- `plugins/` - Custom Airflow operators/hooks/sensors
- `tests/` - Unit and integration tests
- `docs/` - Comprehensive guides

### Data Directory Structure

```
data/
├── raw/xer/                    # Primavera XER source files
├── projectsight/
│   ├── extracted/              # Raw JSON from browser extraction
│   └── tables/                 # Normalized CSV tables
└── primavera/
    ├── raw_tables/             # Direct XER table exports
    └── processed/              # Filtered/enhanced task CSVs
```

See [PROJECT_OVERVIEW.md](PROJECT_OVERVIEW.md) for detailed structure.

## Subfolder Documentation (CLAUDE.md Files)

**Living Documents:** Each major subsystem has its own `CLAUDE.md` file providing focused, high-level context.

### Purpose
- Provide quick reference for specific subsystems
- Document current implementation status
- Track key decisions and architecture
- Must be updated as work progresses

### Guidelines
- **Concise:** High-level overview, not detailed API docs
- **Current:** Update after significant changes
- **Actionable:** Include "Next Steps" and "TODO" sections
- **Dated:** Mark last update date at top

### Existing Subfolder Docs
- [src/extractors/system_specific/CLAUDE.md](src/extractors/system_specific/CLAUDE.md) - ProjectSight extractor status

### When to Update
- After implementing new features
- After fixing critical bugs
- After major architectural changes
- When status changes (✅ Complete, ⏳ In Progress, ❌ Blocked)

### Template Structure
```markdown
# [Subsystem Name] - Quick Reference

**Last Updated:** [Date]
**Status:** [Current implementation status]

## Purpose
[What this subsystem does]

## Current Implementation Status
- ✅ Completed items
- ⏳ TODO items

## Key Files
[Main files with line references]

## Quick Start
[How to use this subsystem]

## Next Steps
[What needs to be done next]
```

## Web Scraping (ProjectSight)

ProjectSight uses modal-based UI with client-side routing. Playwright is required.

**Key Discovery:** ProjectSight uses **Infragistics igGrid** for data grids. Data is accessible via jQuery API, bypassing virtualized scrolling:
```javascript
$('#ugDataView').igGrid('option', 'dataSource')  // Returns all records
```

**Key Points:**
- Modal-aware extraction: click → extract → close pattern
- Chromium auto-installed during container build
- Use `debug=True` to capture actual HTML structure
- For grids: Access data source directly via igGrid API (don't scrape DOM)
- See [docs/PROJECTSIGHT_DAILY_REPORTS_EXTRACTION.md](docs/PROJECTSIGHT_DAILY_REPORTS_EXTRACTION.md) for grid extraction

**Extracted Data:**
- Raw JSON: `data/projectsight/extracted/daily_reports_415.json` (415 records)
- Raw JSON: `data/projectsight/extracted/daily-report-details-history.json` (414 records)
- CSV Tables: `data/projectsight/tables/` (daily_reports, companies, contacts, history, changes)

**Core Files:**
- [src/connectors/web_scraper.py](src/connectors/web_scraper.py) - Playwright wrapper
- [src/extractors/system_specific/projectsight_extractor.py](src/extractors/system_specific/projectsight_extractor.py) - Modal extraction
- [scripts/extract_daily_report_details.py](scripts/extract_daily_report_details.py) - Detail/history extraction script

**Extraction Guides:**
- [docs/PROJECTSIGHT_DAILY_REPORTS_EXTRACTION.md](docs/PROJECTSIGHT_DAILY_REPORTS_EXTRACTION.md) - Grid/summary extraction
- [docs/DAILY_REPORTS_DETAIL_EXTRACTION.md](docs/DAILY_REPORTS_DETAIL_EXTRACTION.md) - Detail/history extraction (repeatable process)

## XER File Processing (Primavera P6)

Process Primavera P6 schedule exports to CSV with full context (area, level, building, contractor, dates).

### XER File Management

XER files are stored in `data/raw/xer/` but gitignored. A tracked `manifest.json` documents all files and identifies the current version.

**Manifest Location:** [data/raw/xer/manifest.json](data/raw/xer/manifest.json)

**Manifest Schema:**
```json
{
  "current": "filename.xer",
  "files": {
    "filename.xer": {
      "date": "YYYY-MM-DD",
      "description": "Human-readable description",
      "status": "current|archived|superseded"
    }
  }
}
```

**Rules:**
- `current` must reference a key in `files`
- Exactly one file must have `status: "current"`
- The file referenced by `current` must have `status: "current"`
- Valid statuses: `current`, `archived`, `superseded`

**Validation:**
```bash
# Validate manifest structure
python scripts/validate_xer_manifest.py

# Auto-add missing XER files to manifest (sets status=archived, requires manual metadata update)
python scripts/validate_xer_manifest.py --fix
```

**When Adding New XER Files:**
1. Copy `.xer` file(s) to `data/raw/xer/`
2. Update `manifest.json`:
   - Add entry for new file with date, description, status
   - If this is the new current version: update `current` field and set old file's status to `archived`
3. Run `python scripts/validate_xer_manifest.py` to verify

### Processing Commands

- Process XER files to CSV: `python scripts/process_xer_to_csv.py data/raw/xer/schedule.xer`
- Exports all 12,000+ tasks with 24 columns including WBS, activity codes, and all date fields
- Filter by keyword, status, subcontractor, level: `python scripts/filter_tasks.py input.csv --keyword "drywall" -o output.csv`
- Batch process multiple files: `./scripts/batch_process_xer.sh`
- Output location: `data/primavera/processed/`
- See [QUICKSTART_XER_PROCESSING.md](QUICKSTART_XER_PROCESSING.md) for detailed usage

**Core Files:**
- [src/utils/xer_parser.py](src/utils/xer_parser.py) - XER file parser
- [scripts/process_xer_to_csv.py](scripts/process_xer_to_csv.py) - Main processing script
- [scripts/validate_xer_manifest.py](scripts/validate_xer_manifest.py) - Manifest validation
- [notebooks/xer_to_csv_converter.ipynb](notebooks/xer_to_csv_converter.ipynb) - Interactive exploration

## Quick Start

```bash
# Install & configure
pip install -r requirements.txt
cp .env.example .env
# Edit .env with credentials

# Run with Docker
docker-compose up -d

# Access Airflow
# http://localhost:8080 (airflow/airflow)
```

## Key Technology Stack

| Component | Technology |
|-----------|-----------|
| Orchestration | Apache Airflow 2.7.0 |
| Web Scraping | Playwright 1.40.0 |
| API Client | Requests 2.31.0 |
| Data Processing | Pandas 2.1.1 |
| Database | PostgreSQL 15 |
| Testing | Pytest 7.4.3 |

## Configuration

All settings via environment variables in `.env`:

```env
# ProjectSight credentials
PROJECTSIGHT_BASE_URL=https://...
PROJECTSIGHT_USERNAME=...
PROJECTSIGHT_PASSWORD=...

# Fieldwire API
FIELDWIRE_API_KEY=...

# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=etl_db
DB_USER=airflow
DB_PASSWORD=airflow
```

See [.env.example](.env.example) for full list.

## Documentation

### Project-Level Docs

| Document | Purpose |
|----------|---------|
| [PROJECT_OVERVIEW.md](PROJECT_OVERVIEW.md) | Complete project structure and status |
| [PLAYWRIGHT_MIGRATION.md](PLAYWRIGHT_MIGRATION.md) | Selenium → Playwright migration |
| [docs/PLAYWRIGHT_DEBUGGING.md](docs/PLAYWRIGHT_DEBUGGING.md) | Selector discovery & debugging |
| [docs/ETL_DESIGN.md](docs/ETL_DESIGN.md) | Architecture & design patterns |
| [docs/SOURCES.md](docs/SOURCES.md) | Data source APIs & field mapping |
| [IMPLEMENTATION_GUIDE.md](IMPLEMENTATION_GUIDE.md) | Step-by-step implementation |

### Subsystem Docs (Living Documents)

| Location | Subsystem | Status |
|----------|-----------|--------|
| [src/extractors/system_specific/CLAUDE.md](src/extractors/system_specific/CLAUDE.md) | ProjectSight Extractor | ✅ Login / ⏳ Extraction |

**Note:** Subfolder CLAUDE.md files must be kept current. See [Subfolder Documentation](#subfolder-documentation-claudemd-files) section.

## Development

```bash
# Run tests
pytest                    # All tests
pytest --cov=src tests/   # With coverage
pytest tests/unit/        # Unit only

# Code quality
black src/ tests/         # Format
flake8 src/ tests/        # Lint
pylint src/ tests/        # Analysis
```

## Implementation Status

- ✅ Project structure and base classes
- ✅ Connectors (API & web scraping)
- ✅ Extractors and transformers
- ✅ Loaders (database & files)
- ✅ Testing infrastructure
- ✅ Comprehensive documentation
- ⏳ **TODO:** Create ETL DAGs
- ⏳ **TODO:** Configure database schema
- ⏳ **TODO:** Test with actual data
