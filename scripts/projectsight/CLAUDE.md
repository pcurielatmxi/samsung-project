# ProjectSight Scripts

**Last Updated:** 2026-01-03

## Purpose

Scrape daily reports, library structure, and quality records (NCR/QOR/SOR/SWN/VR) from Trimble ProjectSight.

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
| `process/scrape_projectsight_ncr.py` | ncr/records/{TYPE}-{NUM}.json | Scrape NCR/QOR/SOR/SWN/VR records |
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

### NCR/QOR/SOR/SWN/VR Scraper
```bash
# Full extraction (all 275 records from T-PJT FAB1)
DISPLAY=:0 python scripts/projectsight/process/scrape_projectsight_ncr.py

# Process specific date range (by Required Correction Date)
DISPLAY=:0 python scripts/projectsight/process/scrape_projectsight_ncr.py \
  --start-date 2024-01-01 --end-date 2024-12-31

# Idempotent mode - skip already processed records
DISPLAY=:0 python scripts/projectsight/process/scrape_projectsight_ncr.py --skip-existing

# Force re-process
DISPLAY=:0 python scripts/projectsight/process/scrape_projectsight_ncr.py --force

# Limit for testing
DISPLAY=:0 python scripts/projectsight/process/scrape_projectsight_ncr.py --limit 10

# Headless mode
python scripts/projectsight/process/scrape_projectsight_ncr.py --headless --limit 10
```

**Output structure:**
```
{WINDOWS_DATA_DIR}/raw/projectsight/ncr/
├── records/          # Individual JSON files per record
│   ├── NCR-0828.json
│   ├── QOR-0123.json
│   └── ...
├── attachments/      # Downloaded files per record
│   └── NCR-0828/
└── manifest.json     # Tracking processed records
```

**Record types:** NCR (72), QOR (23), SOR (174), SWN (5), Variance Request (1) = 275 total

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
