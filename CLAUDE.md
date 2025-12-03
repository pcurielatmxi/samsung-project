# Web Scraping with Playwright

## Key Architecture

ProjectSight uses **modal-based UI** with client-side routing—detail views don't have unique URLs, they open modals on the list page. Playwright handles this better than Selenium.

## Core Files

| File | Purpose |
|------|---------|
| [src/connectors/web_scraper.py](src/connectors/web_scraper.py) | Playwright wrapper with modal support |
| [src/extractors/system_specific/projectsight_extractor.py](src/extractors/system_specific/projectsight_extractor.py) | Modal-aware extraction logic |
| [docs/PLAYWRIGHT_DEBUGGING.md](docs/PLAYWRIGHT_DEBUGGING.md) | Selector discovery guide |

## Extraction Workflow

```python
# 1. Authenticate & navigate
connector.authenticate()
connector.navigate_to('/projects')

# 2. Find all project rows
project_rows = connector.find_elements('table tbody tr')

# 3. For each project: click → extract → close modal
for row in project_rows:
    connector.click_element(element=row)
    data = connector.extract_text(selector='.modal-body .description')
    connector.close_modal()
```

## Important Methods

**Modal handling:**
- `wait_for_modal()` - Wait for modal to appear
- `close_modal()` - Close modal and wait for disappearance

**Element interaction:**
- `find_element(selector)` / `find_elements(selector)` - Query page
- `extract_text(selector)` - Get element text
- `click_element(selector)` - Click element

**Debugging:**
- `take_screenshot(filename)` - Capture page state
- `get_page_content()` - Get HTML for inspection
- `evaluate_javascript(script)` - Run JS on page

## Setup

Chromium is automatically installed during container build:
```bash
playwright install chromium        # Binary
playwright install-deps chromium   # System dependencies
```

## Debugging Selectors

When extraction fails, use debug mode to capture the actual HTML structure:

```python
from src.extractors.system_specific.projectsight_extractor import ProjectSightExtractor

extractor = ProjectSightExtractor()
projects = extractor.extract(debug=True)
# Screenshots saved to /tmp/projectsight_projects_page.png, /tmp/projectsight_modal_*.png
```

Then inspect the screenshots and check [docs/PLAYWRIGHT_DEBUGGING.md](docs/PLAYWRIGHT_DEBUGGING.md) for selector discovery techniques.

## Common Selectors

```python
# Table rows
'table tbody tr'
'[role="row"][data-project-id]'

# Modal dialog
'.modal'
'[role="dialog"]'

# Close button
'.close'
'[aria-label="Close"]'
```

## Next Steps

1. Run extraction with `debug=True` to find actual selectors
2. Update selector constants in extractor
3. Test extraction with real ProjectSight data
4. Create DAGs for Airflow orchestration
