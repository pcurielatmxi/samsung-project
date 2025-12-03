# MXI Samsung ETL Pipeline

## Project Purpose

ETL (Extract-Transform-Load) pipeline for integrating construction project data from multiple sources into PostgreSQL for analysis. Uses Apache Airflow for orchestration.

**Data Sources:**
- **ProjectSight** (Trimble) - Web scraping via Playwright
- **Fieldwire** - REST API integration

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt
playwright install chromium

# Configure environment
cp .env.example .env
# Edit .env with ProjectSight and Fieldwire credentials

# Start Airflow
docker-compose up -d

# Access UI: http://localhost:8080 (airflow/airflow)
```

## Folder Structure

```
mxi-samsung/
├── dags/                      # Airflow DAG definitions
│   ├── config.py             # Shared DAG configuration
│   └── utils/                # DAG helper functions
│
├── plugins/                  # Custom Airflow operators/hooks/sensors
│   ├── operators/
│   ├── hooks/
│   └── sensors/
│
├── src/                      # Business logic (ETL code)
│   ├── config/               # Settings and environment variables
│   ├── connectors/           # External system connectors
│   │   ├── api_connector.py         # REST API (Fieldwire)
│   │   └── web_scraper.py          # Web scraping (ProjectSight)
│   ├── extractors/           # Data extraction from sources
│   │   └── system_specific/
│   │       ├── fieldwire_extractor.py
│   │       └── projectsight_extractor.py
│   ├── transformers/         # Data standardization
│   │   └── system_specific/
│   │       ├── fieldwire_transformer.py
│   │       └── projectsight_transformer.py
│   ├── loaders/              # Data loading to destinations
│   │   ├── db_loader.py           # PostgreSQL
│   │   └── file_loader.py         # CSV/Parquet/JSON
│   └── utils/                # Utilities (logging, validation, helpers)
│
├── tests/                    # Test suite
│   ├── unit/                 # Unit tests
│   ├── integration/          # Integration tests
│   └── conftest.py           # Pytest fixtures
│
├── data/                     # Data directories
│   ├── raw/                  # Raw extracted data
│   ├── processed/            # Processed data
│   └── output/               # Final output
│
├── docs/                     # Documentation
│   ├── README.md                    # Project overview
│   ├── SETUP.md                     # Installation & troubleshooting
│   ├── ETL_DESIGN.md                # Architecture & patterns
│   ├── SOURCES.md                   # Data source documentation
│   └── PLAYWRIGHT_DEBUGGING.md      # Debugging guide
│
├── scripts/                  # Helper scripts
├── config/                   # Configuration files
├── .env.example              # Environment template
├── requirements.txt          # Python dependencies
├── docker-compose.yml        # Airflow Docker setup
├── pytest.ini                # Pytest configuration
│
├── PLAYWRIGHT_MIGRATION.md   # Migration from Selenium to Playwright
├── IMPLEMENTATION_GUIDE.md   # Step-by-step implementation
└── PROJECT_OVERVIEW.md       # This file
```

## Key Components

### Connectors
- **APIConnector** - REST API calls with retry logic and authentication
- **WebScraperConnector** - Playwright-based web scraping with modal support

### Extractors
- **ProjectSightExtractor** - Scrapes Trimble ProjectSight (list view + modals)
- **FieldwireExtractor** - Fetches data from Fieldwire REST API

### Transformers
- **ProjectSightTransformer** - Normalizes ProjectSight data
- **FieldwireTransformer** - Standardizes Fieldwire API responses

### Loaders
- **DatabaseLoader** - Loads to PostgreSQL with UPSERT
- **FileLoader** - Exports to CSV, Parquet, or JSON

## Architecture

```
External Systems
     ↓
[Extract] → [Transform] → [Load] → PostgreSQL
     ↑            ↑           ↑
  Connector    Utilities   Database
```

Each step is:
- ✅ Independently testable
- ✅ Reusable across sources
- ✅ Validated at each stage
- ✅ Fully logged

## Configuration

All settings via environment variables (`.env`):

```env
# ProjectSight (Trimble) - Web Scraping
PROJECTSIGHT_BASE_URL=https://...
PROJECTSIGHT_USERNAME=...
PROJECTSIGHT_PASSWORD=...

# Fieldwire - REST API
FIELDWIRE_API_KEY=...

# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=etl_db
DB_USER=airflow
DB_PASSWORD=airflow

# Logging
LOG_LEVEL=INFO
```

## Technology Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Orchestration | Apache Airflow | 2.7.0 |
| Web Scraping | Playwright | 1.40.0 |
| API Client | Requests | 2.31.0 |
| Data Processing | Pandas/NumPy | 2.1.1/1.24.3 |
| Database | PostgreSQL | 15 |
| Testing | Pytest | 7.4.3 |
| Code Quality | Black/Flake8/Pylint | Latest |

## Development

### Running Tests
```bash
pytest                           # All tests
pytest --cov=src tests/          # With coverage
pytest tests/unit/               # Unit tests only
```

### Code Quality
```bash
black src/ tests/                # Format
flake8 src/ tests/               # Check style
pylint src/ tests/               # Lint
```

## Documentation

| Document | Purpose |
|----------|---------|
| **README.md** | Project overview & quick start |
| **SETUP.md** | Installation & troubleshooting |
| **ETL_DESIGN.md** | Architecture & design patterns |
| **SOURCES.md** | Data source APIs & field mapping |
| **PLAYWRIGHT_DEBUGGING.md** | Debugging & selector discovery |
| **IMPLEMENTATION_GUIDE.md** | Step-by-step implementation |
| **PLAYWRIGHT_MIGRATION.md** | Selenium → Playwright migration |

## Implementation Status

- ✅ Project structure created
- ✅ Base classes implemented
- ✅ Connectors (API & web scraping)
- ✅ Extractors (ProjectSight & Fieldwire)
- ✅ Transformers (data standardization)
- ✅ Loaders (database & files)
- ✅ Utilities (logging, validation, helpers)
- ✅ Testing infrastructure
- ✅ Comprehensive documentation
- ⏳ **TODO:** Create ETL DAGs
- ⏳ **TODO:** Configure database schema
- ⏳ **TODO:** Test with actual data

## Next Steps

1. **Review** [docs/PLAYWRIGHT_DEBUGGING.md](docs/PLAYWRIGHT_DEBUGGING.md) to understand selector discovery
2. **Run** extraction with `debug=True` to capture selectors
3. **Update** ProjectSight extractor with correct selectors
4. **Create** ETL DAGs in `dags/`
5. **Set up** database schema in PostgreSQL
6. **Test** extraction → transformation → loading pipeline

## Support

- 📄 **Architecture questions?** → See [docs/ETL_DESIGN.md](docs/ETL_DESIGN.md)
- 🐛 **Debugging selectors?** → See [docs/PLAYWRIGHT_DEBUGGING.md](docs/PLAYWRIGHT_DEBUGGING.md)
- 🛠️ **Implementation details?** → See [IMPLEMENTATION_GUIDE.md](IMPLEMENTATION_GUIDE.md)
- 🔌 **API documentation?** → See [docs/SOURCES.md](docs/SOURCES.md)

---

**Last Updated:** 2025-12-03
**Status:** Ready for implementation
