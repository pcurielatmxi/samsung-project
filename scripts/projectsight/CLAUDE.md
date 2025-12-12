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
# Extract 10 reports (test)
DISPLAY=:0 python scripts/projectsight/process/scrape_projectsight_daily_reports.py --limit 10

# Idempotent extraction - resumes from where it left off
DISPLAY=:0 python scripts/projectsight/process/scrape_projectsight_daily_reports.py --skip-existing --limit 50

# Full extraction (all 1320 reports)
DISPLAY=:0 python scripts/projectsight/process/scrape_projectsight_daily_reports.py --skip-existing --limit 0
```

## Configuration

Requires `.env` with:
- `PROJECTSIGHT_USERNAME`
- `PROJECTSIGHT_PASSWORD`
- `PROJECTSIGHT_HEADLESS` (false recommended)

## Lessons Learned

- **dotenv override**: Use `load_dotenv(override=True)` - shell env vars override .env by default
- **Session persistence**: Cookies saved to `~/.projectsight_sessions/`. Delete to force re-login
- **Trimble SSO**: Two-step login (username → Next → password). Must wait for buttons to be enabled
- **Iframe structure**: List in `fraMenuContent`, detail view in `fraDef`
- **Tabs**: Extract from 3 tabs: Daily report, Additional Info, History
- **Grid navigation**: Use `click_report_at_position(n)` for direct row access, `click_next_record()` for sequential
