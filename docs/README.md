# Samsung Taylor FAB1 - Data Analysis

Data analysis tools and pipeline for construction delay analysis on the Samsung Austin semiconductor manufacturing facility project.

## Overview

This project provides tools to:
- **Process** Primavera P6 schedule exports (XER files)
- **Transform** ProjectSight daily report exports
- **Analyze** construction data to identify delay patterns
- **Load** data to PostgreSQL (optional)

## Project Structure

```
mxi-samsung/
├── data/                 # Project data (raw and processed)
│   ├── raw/xer/          # Primavera XER files
│   ├── primavera/        # Processed schedule data
│   └── projectsight/     # Daily reports data
├── scripts/              # Processing scripts
├── notebooks/            # Jupyter notebooks for analysis
├── src/                  # Reusable Python modules
├── tests/                # Test suite
└── docs/                 # Documentation
```

## Quick Start

### Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Configure environment (optional)
cp .env.example .env
```

### Process Schedule Data

```bash
# Process all Primavera XER files
python scripts/primavera/process/batch_process_xer.py

# Output: data/primavera/processed/*.csv
```

### Process Daily Reports

```bash
# Transform ProjectSight JSON to CSV
python scripts/daily_reports_to_csv.py

# Output: data/projectsight/tables/*.csv
```

## Data Sources

### 1. Primavera P6 (Schedules)
- **48 XER files** spanning Oct 2022 - Nov 2025
- Two perspectives: SECAI (owner) and Yates (GC) schedules
- Processed to ~48 CSV tables with version tracking

### 2. ProjectSight (Daily Reports)
- **415 daily reports** with workforce, weather, activities
- Manually exported, transformed via script
- CSV tables for analysis

### 3. Fieldwire (API - optional)
- REST API integration available
- Requires API key configuration

## Development

### Running Tests
```bash
pytest                    # All tests
pytest --cov=src tests/   # With coverage
```

### Code Quality
```bash
black src/ tests/         # Format
flake8 src/ tests/        # Lint
```

## Documentation

- [Setup Instructions](./SETUP.md)
- [Data Sources](./SOURCES.md)
- [ETL Architecture](./ETL_DESIGN.md)
- [Main Project Context](../CLAUDE.md)
