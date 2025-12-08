# Pending Organization - Staging Area

**Purpose:** Drop files here for later classification and organization.

## Workflow

When asked to "organize pending files", follow these steps:

1. **Identify** - Examine each file (type, content, source system)
2. **Classify** - Determine which data source it belongs to
3. **Move** - Place in appropriate folder (see destinations below)
4. **Document** - Update relevant CLAUDE.md files

## Destination Mapping

| File Type | Destination | Additional Steps |
|-----------|-------------|------------------|
| XER files (*.xer) | `raw/xer/` | Run `validate_xer_manifest.py --fix` |
| ProjectSight JSON | `projectsight/extracted/` | - |
| PDF reports | `raw/{report_type}/` | Create folder if needed |
| Unknown | Ask user | - |

## Naming Conventions

- Use lowercase with underscores: `weekly_report_2025_01_15.pdf`
- Include dates where applicable: `YYYY_MM_DD` or `YYYY-MM-DD`
- Preserve original names when they contain useful metadata

## Creating New Data Source Folders

When a new data type requires a new folder structure:

```
data/
├── raw/{source}/           # Raw files (add to .gitignore if large/binary)
├── {source}/
│   ├── extracted/          # Initial extraction output
│   ├── processed/          # Cleaned/normalized data
│   └── analysis/           # Findings and reports
```

After creating:
1. Add `.gitkeep` files to empty folders
2. Update `.gitignore` if needed
3. Update `data/CLAUDE.md` directory structure
4. Create `{source}/CLAUDE.md` for source-specific documentation
