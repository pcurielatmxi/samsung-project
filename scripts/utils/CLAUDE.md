# Utility Scripts

**Last Updated:** 2025-12-12

## Purpose

Helper scripts for path validation, data migration, and file conversion.

## Key Scripts

| Script | Description |
|--------|-------------|
| `check_paths.py` | Verify WINDOWS_DATA_DIR configuration |
| `migrate_data_structure.py` | Migrate files to raw/processed/derived structure |
| `validate_xer_manifest.py` | Validate XER file manifest |
| `convert_drawings_to_png.py` | Convert CAD drawings to PNG |

## Commands

```bash
# Check path configuration
python scripts/utils/check_paths.py

# Migrate data (dry run first)
python scripts/utils/migrate_data_structure.py --dry-run
python scripts/utils/migrate_data_structure.py
```
