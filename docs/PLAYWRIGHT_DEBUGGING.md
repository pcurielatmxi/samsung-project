# Playwright Debugging Guide for ProjectSight

Since ProjectSight uses modals instead of unique URLs, you'll need to inspect the actual HTML structure to get the correct selectors. This guide walks you through that process.

## Step 1: Enable Debug Mode with Screenshots

Create a test script to capture what's happening:

```python
from src.extractors.system_specific.projectsight_extractor import ProjectSightExtractor

# Run extraction with debug=True
extractor = ProjectSightExtractor()
try:
    projects = extractor.extract(debug=True)
except Exception as e:
    print(f"Error: {e}")
```

This will save screenshots to `/tmp/` showing:
- `projectsight_projects_page.png` - Projects list view
- `projectsight_modal_0.png` - First project modal
- `projectsight_modal_1.png` - Second project modal, etc.

These show what the browser sees during extraction.

## Step 2: Inspect HTML Structure

Get the HTML content to identify selectors:

```python
from src.connectors.web_scraper import WebScraperConnector
from src.config.settings import settings

connector = WebScraperConnector(
    name='ProjectSight',
    base_url=settings.PROJECTSIGHT_BASE_URL,
    username=settings.PROJECTSIGHT_USERNAME,
    password=settings.PROJECTSIGHT_PASSWORD,
    headless=False  # Don't use headless so you can see what's happening
)

# Authenticate and navigate
connector.authenticate()
connector.navigate_to('/projects')

# Get page HTML
html = connector.get_page_content()

# Save for inspection
with open('/tmp/projectsight_projects.html', 'w') as f:
    f.write(html)

print("HTML saved to /tmp/projectsight_projects.html")
connector.close()
```

Open the HTML file in a browser and use DevTools to inspect elements.

## Step 3: Use Browser DevTools

Run Playwright in non-headless mode to use browser DevTools:

```python
connector = WebScraperConnector(
    ...,
    headless=False  # This opens a visible browser window
)

# Now you can:
# 1. Inspect elements (right-click > Inspect)
# 2. Copy CSS selectors
# 3. Test selectors in console
```

## Step 4: Test Selectors in Playwright Inspector

Playwright has a built-in inspector for testing selectors:

```bash
# Install Playwright inspector (one-time)
npx playwright install

# Run with inspector (opens interactive UI)
PWDEBUG=1 python your_script.py
```

This opens the Playwright Inspector where you can:
- Step through code
- Test selectors in real-time
- View the DOM
- Take screenshots

## Common ProjectSight Selectors

Based on typical Trimble UI patterns, look for these elements:

### Projects List Table

```html
<!-- Projects list table structure -->
<table id="projectsTable" class="table">
  <thead>
    <tr>
      <th>Project ID</th>
      <th>Name</th>
      <th>Status</th>
      <th>Manager</th>
      <th>Start Date</th>
    </tr>
  </thead>
  <tbody>
    <tr class="project-row" data-project-id="P001">
      <td>P001</td>
      <td>Downtown Office</td>
      <td>Active</td>
      <td>John Doe</td>
      <td>2025-01-01</td>
    </tr>
  </tbody>
</table>
```

**Selectors to try:**
```python
# Table rows
'table tbody tr'
'tr[data-project-id]'
'.project-row'

# Individual cells
'td'  # in context of row
'[role="cell"]'
```

### Modal Dialog

```html
<!-- Modal structure -->
<div class="modal fade show" id="projectModal" role="dialog">
  <div class="modal-dialog">
    <div class="modal-content">
      <div class="modal-header">
        <h5>Project Name</h5>
        <button class="close" data-dismiss="modal">&times;</button>
      </div>
      <div class="modal-body">
        <div class="description">Project description here</div>
        <div class="end-date">2025-12-31</div>
        <div class="budget">$100,000</div>
        <div class="location">Downtown</div>
      </div>
    </div>
  </div>
</div>
```

**Selectors to try:**
```python
# Modal container
'.modal'
'[role="dialog"]'
'.modal-body'

# Close button
'.close'
'[data-dismiss="modal"]'
'[aria-label="Close"]'

# Modal fields
'.description'
'.end-date'
'.budget'
'.location'
```

## Step 5: Update Selectors in Code

Once you've identified the correct selectors, update them in:

1. **[src/extractors/system_specific/projectsight_extractor.py](../src/extractors/system_specific/projectsight_extractor.py)**
   - `_extract_project_list()` - Update table/row selector
   - `_extract_row_data()` - Update cell selectors
   - `_extract_modal_data()` - Update modal field selectors

2. **[src/connectors/web_scraper.py](../src/connectors/web_scraper.py)**
   - `wait_for_modal()` - Update modal selector
   - `close_modal()` - Update close button selector

Example update:

```python
# In _extract_project_list()
project_rows = self.connector.find_elements(
    'table#projectsTable tbody tr'  # Updated with correct table ID
)

# In _extract_modal_data()
data = {
    'description': self.connector.extract_text(selector='.modal-body .project-description'),
    'end_date': self.connector.extract_text(selector='.modal-body [data-field="endDate"]'),
    # ... etc
}
```

## Debugging Tips

### Check Logs

```bash
# View detailed extraction logs
LOG_LEVEL=DEBUG python -c "
from src.extractors.system_specific.projectsight_extractor import ProjectSightExtractor
extractor = ProjectSightExtractor()
try:
    projects = extractor.extract(debug=True)
except Exception as e:
    print(f'Error: {e}')
"
```

### Wait Times

If elements aren't being found, the page might not be fully loaded:

```python
# Add explicit waits
connector.wait_for_selector('table tbody tr', state='visible')  # Wait for table to render
time.sleep(2)  # Additional safety delay
```

### Element Not Found

If you get "element not found" errors:

```python
# 1. Take a screenshot at that point
connector.take_screenshot('/tmp/debug_point.png')

# 2. Get the HTML
html = connector.get_page_content()

# 3. Check if element exists with JavaScript
result = connector.evaluate_javascript('''
    () => {
        const rows = document.querySelectorAll('table tbody tr');
        return {
            found: rows.length > 0,
            count: rows.length,
            firstRow: rows[0]?.outerHTML
        };
    }
''')
print(result)
```

### Modal Won't Close

If close button isn't working:

```python
# Try alternative close methods
# 1. Press Escape key
connector.page.press('Escape')

# 2. Click overlay to dismiss
connector.click_element(selector='.modal-backdrop')

# 3. Direct modal hide
connector.evaluate_javascript('() => { $(".modal").modal("hide"); }')
```

## Example Debugging Session

```python
from src.connectors.web_scraper import WebScraperConnector
from src.config.settings import settings
import json

# Create connector
connector = WebScraperConnector(
    name='ProjectSight',
    base_url=settings.PROJECTSIGHT_BASE_URL,
    username=settings.PROJECTSIGHT_USERNAME,
    password=settings.PROJECTSIGHT_PASSWORD,
    headless=False  # Visual debugging
)

# Authenticate
if not connector.authenticate():
    print("Auth failed")
    exit(1)

# Navigate
connector.navigate_to('/projects')

# Check table structure
result = connector.evaluate_javascript('''
    () => {
        const rows = document.querySelectorAll('table tbody tr');
        if (rows.length === 0) return "NO ROWS FOUND";

        const firstRow = rows[0];
        const cells = firstRow.querySelectorAll('td');

        return {
            totalRows: rows.length,
            columnsInFirstRow: cells.length,
            firstRowData: Array.from(cells).map(c => c.textContent.trim()),
            firstRowHTML: firstRow.outerHTML
        };
    }
''')

print(json.dumps(result, indent=2))

# Try clicking first project
first_row = connector.find_element('table tbody tr')
if first_row:
    connector.click_element(element=first_row)

    # Wait and inspect modal
    connector.take_screenshot('/tmp/modal_opened.png')

    modal_html = connector.get_page_content()
    with open('/tmp/modal.html', 'w') as f:
        f.write(modal_html)

    print("Modal HTML saved")

connector.close()
```

## Next Steps

1. **Run debug script** to capture screenshots and HTML
2. **Inspect captured files** in browser DevTools or text editor
3. **Identify correct selectors** for your specific ProjectSight instance
4. **Update extractor code** with correct selectors
5. **Test extraction** with small dataset first
6. **Validate data** before running full extraction

## Common Issues & Solutions

| Issue | Solution |
|-------|----------|
| Timeout waiting for modal | Modal selector wrong, or page structure different |
| Elements not found after click | Wait longer before checking, or page not responding to click |
| Modal won't close | Try `page.press('Escape')` or click backdrop |
| Data extraction empty | Check selector matches actual HTML structure |
| Authentication fails | Verify username/password format in `.env` |
| Page load timeout | Increase timeout or check network connectivity |

## Playwright Documentation

For more advanced techniques:
- [Playwright Selectors](https://playwright.dev/python/docs/locators)
- [Element Inspection](https://playwright.dev/python/docs/inspector)
- [Debugging Guide](https://playwright.dev/python/docs/debug)
