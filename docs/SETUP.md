# Setup Instructions

## Prerequisites

- Python 3.11+
- Git

## Installation Steps

### 1. Environment Configuration

```bash
# Copy example configuration
cp .env.example .env

# Edit with your credentials (if using APIs)
# nano .env
```

### 2. Python Environment

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install dependencies
pip install -r requirements.txt
```

### 3. Verify Installation

```bash
# Test imports
python -c "from src.config.settings import settings; print('Settings loaded')"

# Run tests
pytest
```

## Data Processing

### Process Primavera XER Files

```bash
# Process all XER files (auto-discovers from input directory)
python scripts/primavera/process/batch_process_xer.py

# Or use the run script
./scripts/primavera/process/run.sh
```

### Transform ProjectSight Data

```bash
# Transform already-extracted JSON to CSV
python scripts/daily_reports_to_csv.py
```

## Development Setup

### IDE Setup

**VS Code:**
1. Install Python extension
2. Select interpreter: `.venv/bin/python`

**PyCharm:**
1. File > Settings > Project > Python Interpreter
2. Choose existing environment: `.venv`

## Troubleshooting

### Python import errors
```bash
# Ensure project root is in PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# Or install in editable mode
pip install -e .
```

### XER Processing Issues

```bash
# Check XER files in input directory
ls -la $WINDOWS_DATA_DIR/raw/primavera/*.xer | wc -l

# Process with verbose output
python scripts/primavera/process/batch_process_xer.py
```

## Next Steps

1. Review [CLAUDE.md](../CLAUDE.md) for project overview
2. Explore data in `data/primavera/processed/`
3. Run analysis notebooks in `notebooks/`
4. See [SOURCES.md](./SOURCES.md) for data source details

## Useful Commands

```bash
# Process XER files
python scripts/primavera/process/batch_process_xer.py

# Filter tasks
python scripts/filter_tasks.py data/primavera/processed/task.csv --keyword "drywall"

# Run tests
pytest tests/unit/
```
