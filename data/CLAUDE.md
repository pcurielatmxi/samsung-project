# Data Directory

**Last Updated:** 2025-12-12

## Structure

This directory contains **analysis outputs** tracked in git. Raw data and processed files live in the external Windows data directory.

```
data/
└── analysis/           # Analysis findings (tracked in git)
    ├── primavera/      # Schedule analysis
    ├── weekly_reports/ # Issue analysis
    ├── tbm/
    ├── fieldwire/
    └── projectsight/
```

## External Data

Raw and processed data are stored externally (configured via `WINDOWS_DATA_DIR` in `.env`):

```
{WINDOWS_DATA_DIR}/
├── raw/{source}/       # Source files (100% traceable)
├── processed/{source}/ # Parsed CSV tables (100% traceable)
└── derived/{source}/   # Enhanced data (includes assumptions)
```

## Documentation

- **Main project context:** [../CLAUDE.md](../CLAUDE.md)
- **Data source details:** [../docs/SOURCES.md](../docs/SOURCES.md)
- **Path configuration:** [../src/config/settings.py](../src/config/settings.py)
