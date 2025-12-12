# ProjectSight Scripts

**Last Updated:** 2025-12-12

## Purpose

Scrape and process daily reports from Trimble ProjectSight.

## Structure

```
projectsight/
├── process/    # Web scraping -> raw/projectsight/, processed/projectsight/
└── derive/     # (future analysis)
```

## Key Scripts

| Script | Output | Description |
|--------|--------|-------------|
| `process/scrape_projectsight_daily_reports.py` | daily_reports.json | Scrape daily reports via Playwright |
| `process/daily_reports_to_csv.py` | daily_reports.csv | Convert JSON to CSV |

## Commands

```bash
# Scrape (requires .env credentials)
python scripts/projectsight/process/scrape_projectsight_daily_reports.py --limit 10

# Convert to CSV
python scripts/projectsight/process/daily_reports_to_csv.py
```

## Configuration

Requires `.env` with:
- `PROJECTSIGHT_USERNAME`
- `PROJECTSIGHT_PASSWORD`
- `PROJECTSIGHT_HEADLESS` (false recommended)

## Key Data

- Weather conditions
- Labor hours by trade
- Equipment usage
