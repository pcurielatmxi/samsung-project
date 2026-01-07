# ProjectSight Scripts

**Last Updated:** 2026-01-06

## Purpose

Process daily reports and library structure from Trimble ProjectSight.

## Structure

```
projectsight/
├── process/    # Web scraping -> raw/projectsight/, processed/projectsight/
├── utils/      # Shared session management
└── derive/     # (future analysis)
```

## Key Scripts

| Script | Output | Description |
|--------|--------|-------------|
| `process/scrape_projectsight_daily_reports.py` | daily_reports.json | Scrape daily reports via Playwright |
| `process/scrape_projectsight_library.py` | library_structure_{project}.json | Scrape library folder/file structure |
| `process/daily_reports_to_csv.py` | daily_reports.csv | Convert JSON to CSV |

## Commands

### Daily Reports
```bash
# Idempotent extraction - resumes from where it left off
DISPLAY=:0 python scripts/projectsight/process/scrape_projectsight_daily_reports.py --skip-existing --limit 50
```

### Library Scraper
```bash
# Recursive extraction with idempotency (skip folders scanned in last 7 days)
DISPLAY=:0 python scripts/projectsight/process/scrape_projectsight_library.py \
  --headless --recursive --project taylor_fab1 --skip-scanned-days 7

# Verbose mode (DEBUG on console)
DISPLAY=:0 python scripts/projectsight/process/scrape_projectsight_library.py \
  --headless --recursive -v --project tpjt_fab1

# Available projects: taylor_fab1, tpjt_fab1
```

## Configuration

Requires `.env` with:
- `PROJECTSIGHT_USERNAME`
- `PROJECTSIGHT_PASSWORD`
- `PROJECTSIGHT_HEADLESS` (false recommended)

## Troubleshooting Library Scraper

Log file: `data/projectsight/extracted/library_scraper_{project}.log`

```bash
# View full log
cat data/projectsight/extracted/library_scraper_taylor_fab1.log

# Find navigation failures
grep "Navigation failed" data/projectsight/extracted/library_scraper_*.log

# Find extraction errors
grep "ERROR" data/projectsight/extracted/library_scraper_*.log

# Check summary section
tail -30 data/projectsight/extracted/library_scraper_taylor_fab1.log
```

**Common issues:**
- `navigationFailed: True` in JSON = folder couldn't be opened, will retry on next run
- Many navigation failures = virtualized grid not scrolling properly
- Extraction failures = iframe didn't load, check login session

**Output JSON includes `scrapeStats`:**
```json
"scrapeStats": {
  "duration_seconds": 123.4,
  "folders_processed": 50,
  "navigation_failures": [...]
}
```

## Lessons Learned

- **dotenv override**: Use `load_dotenv(override=True)` - shell env vars override .env by default
- **Session persistence**: Cookies saved to `~/.projectsight_sessions/`. Delete to force re-login
- **Trimble SSO**: Two-step login (username → Next → password). Must wait for buttons to be enabled
- **Iframe structure**: List in `fraMenuContent`, detail view in `fraDef`. Library uses nested Trimble Connect iframe
- **Virtualized grid**: Only renders visible rows - must scroll to find folder links
- **Login detection**: Can appear via redirect OR embedded iframe - must check both
