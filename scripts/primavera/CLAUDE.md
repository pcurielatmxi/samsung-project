# Primavera Scripts

**Last Updated:** 2025-12-12

## Purpose

Parse and analyze YATES Primavera P6 schedule exports (XER files).

## Structure

```
primavera/
├── process/    # XER parsing -> processed/primavera/
└── derive/     # WBS taxonomy, enrichment -> derived/primavera/
```

## Key Scripts

| Script | Output | Description |
|--------|--------|-------------|
| `process/batch_process_xer.py` | task.csv, wbs.csv, etc. | Parse all 66 XER files to CSV tables |
| `derive/generate_wbs_taxonomy.py` | wbs_taxonomy.csv | Classify tasks by phase/scope/location |

## Commands

```bash
# Parse all XER files
python scripts/primavera/process/batch_process_xer.py

# Generate WBS taxonomy (requires processed data)
python scripts/primavera/derive/generate_wbs_taxonomy.py
```

## Key Fields

- `target_end_date` = current scheduled finish
- `total_float` = late_end - early_end (negative = behind)
- No baseline in exports - compare across versions

## Documentation

See [docs/SOURCES.md](../../docs/SOURCES.md) for XER field mapping.
