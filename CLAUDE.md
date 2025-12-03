# MXI Samsung ETL Pipeline - Quick Reference

## Project Overview

ETL pipeline for integrating construction project data from multiple sources into PostgreSQL for analysis. Orchestrated with Apache Airflow 2.7.0.

**Data Sources:**
- **ProjectSight (Trimble)** - Web scraping via Playwright
- **Fieldwire** - REST API integration

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

See [PROJECT_OVERVIEW.md](PROJECT_OVERVIEW.md) for detailed structure.

## Web Scraping (ProjectSight)

ProjectSight uses modal-based UI with client-side routing. Playwright is required.

**Key Points:**
- Modal-aware extraction: click → extract → close pattern
- Chromium auto-installed during container build
- Use `debug=True` to capture actual HTML structure
- See [CLAUDE.md#web-scraping](CLAUDE.md) and [docs/PLAYWRIGHT_DEBUGGING.md](docs/PLAYWRIGHT_DEBUGGING.md) for selectors

**Core Files:**
- [src/connectors/web_scraper.py](src/connectors/web_scraper.py) - Playwright wrapper
- [src/extractors/system_specific/projectsight_extractor.py](src/extractors/system_specific/projectsight_extractor.py) - Modal extraction

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

| Document | Purpose |
|----------|---------|
| [PROJECT_OVERVIEW.md](PROJECT_OVERVIEW.md) | Complete project structure and status |
| [PLAYWRIGHT_MIGRATION.md](PLAYWRIGHT_MIGRATION.md) | Selenium → Playwright migration |
| [docs/PLAYWRIGHT_DEBUGGING.md](docs/PLAYWRIGHT_DEBUGGING.md) | Selector discovery & debugging |
| [docs/ETL_DESIGN.md](docs/ETL_DESIGN.md) | Architecture & design patterns |
| [docs/SOURCES.md](docs/SOURCES.md) | Data source APIs & field mapping |
| [IMPLEMENTATION_GUIDE.md](IMPLEMENTATION_GUIDE.md) | Step-by-step implementation |

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
