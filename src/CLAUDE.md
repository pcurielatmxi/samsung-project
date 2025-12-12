# Source Modules

**Last Updated:** 2025-12-12

## Purpose

Shared Python modules for configuration, parsing, and classification.

## Structure

```
src/
├── config/
│   └── settings.py     # Central path configuration
└── classifiers/        # WBS taxonomy classifier
```

## Key Modules

| Module | Description |
|--------|-------------|
| `config/settings.py` | All path constants, Windows/WSL conversion |
| `classifiers/` | WBS task classification logic |

## Usage

```python
from src.config.settings import settings

# Access paths
input_path = settings.PRIMAVERA_RAW_DIR
output_path = settings.PRIMAVERA_PROCESSED_DIR
```

## Path Constants

Each source has four paths:
- `{SOURCE}_RAW_DIR` - raw input files
- `{SOURCE}_PROCESSED_DIR` - parsed output (traceable)
- `{SOURCE}_DERIVED_DIR` - enhanced output (includes assumptions)
- `{SOURCE}_ANALYSIS_DIR` - analysis in repo (git tracked)
