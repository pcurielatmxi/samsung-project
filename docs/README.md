# MXI Samsung ETL Pipeline

An Apache Airflow-based ETL (Extract-Transform-Load) pipeline for integrating data from multiple project management sources (ProjectSight and Fieldwire) for data analysis.

## Overview

This project sets up a scalable ETL pipeline that:
- **Extracts** data from ProjectSight (web scraping) and Fieldwire (REST API)
- **Transforms** data into a standardized format
- **Loads** processed data into PostgreSQL for analysis

## Project Structure

```
mxi-samsung/
├── dags/                 # Airflow DAG definitions
├── plugins/              # Custom Airflow operators and hooks
├── src/                  # Business logic (extractors, transformers, loaders)
├── tests/                # Test suite (unit and integration)
├── data/                 # Data directories (raw, processed, output)
├── config/               # Configuration files
├── docs/                 # Documentation
├── scripts/              # Helper scripts
└── docker-compose.yml    # Airflow Docker setup
```

## Quick Start

### Prerequisites
- Docker and Docker Compose
- Python 3.11+
- PostgreSQL (or use Docker Compose to run it)

### Setup

1. **Clone and configure environment:**
   ```bash
   cp .env.example .env
   # Edit .env with your credentials and configuration
   ```

2. **Start Airflow with Docker Compose:**
   ```bash
   docker-compose up -d
   ```

3. **Access Airflow UI:**
   - Open http://localhost:8080
   - Default credentials: `airflow` / `airflow`

4. **Configure Connections in Airflow UI:**
   - ProjectSight (credentials for web scraping)
   - Fieldwire (API key)
   - PostgreSQL (target database)

## Data Sources

### 1. ProjectSight (Trimble)
- **Type:** Web Application (requires web scraping)
- **Data:** Projects, resources, schedules
- **Extractor:** [src/extractors/system_specific/projectsight_extractor.py](../src/extractors/system_specific/projectsight_extractor.py)

### 2. Fieldwire
- **Type:** REST API
- **Data:** Projects, tasks, workers, checklists
- **Extractor:** [src/extractors/system_specific/fieldwire_extractor.py](../src/extractors/system_specific/fieldwire_extractor.py)

## Core Components

### Extractors
Extract data from source systems. Each extractor:
- Authenticates with the source system
- Extracts and validates data
- Returns standardized list of dictionaries

### Transformers
Transform raw extracted data into a standard format:
- Normalize field names
- Convert data types
- Handle missing values
- Add metadata (extraction timestamp, etc.)

### Loaders
Load processed data to destinations:
- **DatabaseLoader:** PostgreSQL with UPSERT support
- **FileLoader:** CSV, Parquet, JSON formats

## Development

### Running Tests
```bash
# Run all tests
pytest

# Run specific test file
pytest tests/unit/test_extractors.py

# Run with coverage
pytest --cov=src tests/
```

### Code Quality
```bash
# Format code
black src/ tests/

# Check style
flake8 src/ tests/

# Lint
pylint src/ tests/
```

### Creating a New Data Source

1. Create extractor: `src/extractors/system_specific/{source}_extractor.py`
2. Implement transformer: `src/transformers/system_specific/{source}_transformer.py`
3. Create DAG: `dags/etl_{source}_dag.py`
4. Add tests: `tests/unit/test_{source}.py`

## Configuration

See [.env.example](.env.example) for all available configuration options.

Key settings:
- **PROJECTSIGHT_BASE_URL:** ProjectSight application URL
- **PROJECTSIGHT_USERNAME/PASSWORD:** Login credentials
- **FIELDWIRE_API_KEY:** API authentication key
- **DB_HOST/DB_NAME/DB_USER:** Target database connection

## Logging

Logs are stored in:
- Airflow logs: `./logs/`
- ETL-specific logs: `./logs/etl/`

Configure log level with `LOG_LEVEL` environment variable.

## Documentation

- [Setup Instructions](./SETUP.md)
- [ETL Architecture](./ETL_DESIGN.md)
- [Data Sources](./SOURCES.md)

## Support

For issues or questions, check the logs and ensure:
1. All required environment variables are configured
2. External system credentials are valid
3. Network connectivity to source systems
4. Database connectivity

## License

Internal use only
