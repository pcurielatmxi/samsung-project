# Data Sources Documentation

## Overview

This project integrates data from multiple sources for construction delay analysis. Data is either extracted via API, manually exported, or processed from file exports.

## Primavera P6 (XER Files)

### Overview
- **Type:** File Export (XER format)
- **Extraction Method:** Batch processing script
- **Data:** Schedule tasks, predecessors, resources, WBS, activity codes

### Configuration

XER files are stored in `data/raw/xer/` and tracked via `manifest.json`.

```bash
# Process all XER files from manifest
python scripts/batch_process_xer.py

# Process only current schedule
python scripts/batch_process_xer.py --current-only
```

### Available Data

| Table | Description |
|-------|-------------|
| task.csv | All tasks with dates, status, codes |
| taskpred.csv | Task predecessors/dependencies |
| taskrsrc.csv | Task resource assignments |
| projwbs.csv | Work Breakdown Structure |
| actvcode.csv | Activity code values |
| calendar.csv | Calendar definitions |
| rsrc.csv | Resources |
| udfvalue.csv | User-defined field values |

### Schema

All tables have `file_id` as first column linking to `xer_files.csv` for version tracking.

## ProjectSight (Trimble) - Manual Export

### Overview
- **Type:** Web Application (manual export)
- **Extraction Method:** Manual JSON export, then transformation script
- **Data:** Daily reports, companies, contacts, history

### Current Data

Already extracted and available in `data/projectsight/`:
- **415 daily reports** with workforce counts, weather, activities
- Companies and contacts referenced in reports
- Report revision history

### Transformation

```bash
# Transform JSON exports to CSV tables
python scripts/daily_reports_to_csv.py
```

### Available Tables

| Table | Records | Description |
|-------|---------|-------------|
| daily_reports.csv | 415 | Report summaries |
| companies.csv | - | Companies referenced |
| contacts.csv | - | Contact information |
| daily_report_history.csv | - | Report revision history |
| daily_report_changes.csv | - | Field-level changes |

## Fieldwire (API)

### Overview
- **Type:** REST API
- **Extraction Method:** HTTP API Calls
- **Authentication:** API Key (Bearer Token)
- **Documentation:** https://apidocs.fieldwire.com

### Configuration

```env
FIELDWIRE_BASE_URL=https://api.fieldwire.com
FIELDWIRE_API_KEY=your_api_key_here
FIELDWIRE_TIMEOUT=30
FIELDWIRE_RETRY_ATTEMPTS=3
```

### Available Endpoints

#### Projects
**GET /v2/projects**

#### Tasks
**GET /v2/projects/{project_id}/tasks**

#### Workers
**GET /v2/projects/{project_id}/workers**

### API Features

- **Pagination:** `?page=1&per_page=100`
- **Rate Limits:** 1000 requests per hour
- **Authentication:** `Authorization: Bearer {API_KEY}`

## Planned Data Sources

| Source | Type | Status | Analysis Value |
|--------|------|--------|----------------|
| PDF Weekly Reports | Unstructured PDF | Not started | Progress summaries, narrative context |
| Inspection Reports | PDF/Structured | Not started | Quality issues, rework indicators |
| NCR Reports | PDF/Structured | Not started | Non-conformances, contractor issues |
| Daily Toolbox Meetings | PDF/Structured | Not started | Safety, crew deployment |
| TCO Reports | TBD | Not started | Turnover status |

## Data Integration Considerations

### Field Mapping

| Primavera | ProjectSight | Fieldwire | Standardized |
|-----------|--------------|-----------|--------------|
| task_id | - | id | source_id |
| task_name | - | name | name |
| status | status | status | status |
| target_start | report_date | created_at | start_date |

### Storage Strategy

1. **Raw Data:** Store as-is in source-specific directories
2. **Processed:** Transform to CSV tables with consistent schema
3. **Analysis:** Combine sources for delay analysis
