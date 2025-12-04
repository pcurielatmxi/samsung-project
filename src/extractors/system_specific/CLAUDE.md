# ProjectSight Extractor - Quick Reference

**Last Updated:** December 4, 2025
**Status:** ✅ Login Working | ⏳ Data Extraction Pending

## Purpose

Web scraper for Trimble ProjectSight construction management platform. Extracts project metrics, RFIs, submittals, and dashboard data via Playwright automation.

## Current Implementation Status

### ✅ Completed
- **Authentication System** - Trimble Identity SSO login (projectsight_extractor.py:37-77)
- **Session Persistence** - 7-day cookie caching (web_scraper.py:496-594)
- **MFA Detection** - Automatic verification code handling (projectsight_extractor.py:142-216)
- **URL-Based Verification** - Crash-resistant success detection (projectsight_extractor.py:288-350)

### ⏳ TODO
- **Project List Extraction** - Iterate through available projects
- **Project Detail Scraping** - Extract metrics from dashboard
- **Iframe Handling** - Switch context for widget data
- **Data Transformation** - Map to standardized schema
- **Database Loading** - Store in PostgreSQL

## Key Files

```
src/extractors/system_specific/
├── projectsight_extractor.py    # Main extractor (login only)
├── CLAUDE.md                     # This file
└── __init__.py

src/connectors/
└── web_scraper.py                # Playwright wrapper with session management

docs/
├── PROJECTSIGHT_SELECTORS.md     # Complete selector reference
├── PROJECTSIGHT_DISCOVERY_SUMMARY.md  # UI structure documentation
└── PLAYWRIGHT_DEBUGGING.md        # Debugging guide

test_*.py (root)                  # Test scripts for login
```

## Quick Start

```python
from src.extractors.system_specific.projectsight_extractor import ProjectSightExtractor

# Initialize extractor
extractor = ProjectSightExtractor()

# Authenticate (uses cached session if valid)
if extractor.connector.authenticate():
    print("✅ Logged in successfully")

    # TODO: Implement extraction
    # projects = extractor.extract()
```

## Authentication Flow

1. **Check Session** - Load cookies from `.sessions/` if valid
2. **Navigate to ProjectSight** - Redirects to Trimble Identity
3. **Two-Step Login**:
   - Enter username → Press Enter
   - Enter password → Press Enter
4. **MFA Detection** - Auto-detect verification prompts (if required)
5. **Verify Success** - Check URL contains `/web/app/Projects`
6. **Save Session** - Store cookies for 7 days

## Configuration

Required environment variables in `.env`:

```bash
PROJECTSIGHT_BASE_URL=https://prod.projectsightapp.trimble.com/
PROJECTSIGHT_USERNAME=your_email@domain.com
PROJECTSIGHT_PASSWORD=your_password
PROJECTSIGHT_SESSION_VALIDITY_DAYS=7
PROJECTSIGHT_HEADLESS=true
```

## Architecture Decisions

### Why URL Verification Instead of Page Content?
ProjectSight loads heavy JavaScript that crashes headless browsers when reading page content. URL checks are sufficient and more reliable.

### Why Session Persistence?
Reduces login frequency from every run to once per week. Minimizes verification code requests.

### Why Playwright Over Selenium?
- Better async support
- More reliable selectors (`getByRole`, `getByText`)
- Built-in waiting mechanisms
- Modern API design

## Known Issues & Solutions

### Issue: Browser Crash After Login
**Status:** ✅ FIXED
**Solution:** Skip page content reading, use URL verification only

### Issue: Verification Code Required
**Frequency:** Only on first login or after 7+ days
**Handling:** Automatic detection + 5-minute wait for manual entry

### Issue: Session Expiry
**Solution:** Automatically detected and triggers fresh login

## Testing

```bash
# Test fresh login
PROJECTSIGHT_HEADLESS=true python test_login_debug.py

# Test session reuse
python test_session_reuse.py

# Clear session to force re-login
rm .sessions/projectsight_*
```

## Next Implementation Steps

### 1. Project List Extraction (Priority: High)
**Location:** `projectsight_extractor.py:extract()`

```python
def extract(self, **kwargs):
    # Navigate to projects page
    # Find all project cards
    # Extract: name, hierarchy, ID
    # Return list of projects
```

### 2. Project Detail Extraction (Priority: High)
**Navigate to:** `/web/app/Project?orgid={id}&projid={id}`

**Extract From Dashboard:**
- Work progress percentage
- NCR/QOR/SOR counts (Open, Closed, Placeholder)
- Overdue items (count/total)
- RFI statistics by workflow state
- Submittal statistics by workflow state

### 3. Iframe Data Extraction (Priority: Medium)
Dashboard widgets are in iframes - need to switch context:

```python
frame = page.frame_locator('iframe').first
data = frame.locator('div:has-text("Work progress")').text_content()
```

### 4. Error Handling (Priority: Medium)
- Retry logic for navigation failures
- Graceful degradation for missing widgets
- Screenshot capture on errors

## Performance Notes

- **Login Time:** ~10-15 seconds (first time)
- **Session Reuse:** <1 second
- **Expected Extraction Time:** ~5-10 seconds per project
- **Recommended Frequency:** Daily or weekly (depends on update frequency)

## Security Considerations

- ✅ Session files restricted to owner (chmod 600)
- ✅ Passwords not stored in session
- ✅ `.sessions/` directory in `.gitignore`
- ⚠️ Run in headless mode in production
- ⚠️ Rotate credentials periodically

## Debug Tips

1. **Enable Debug Logging:**
   ```python
   import logging
   logging.basicConfig(level=logging.DEBUG)
   ```

2. **Disable Headless Mode:**
   ```bash
   PROJECTSIGHT_HEADLESS=false python test_login_debug.py
   ```

3. **Capture Screenshots:**
   ```python
   extractor.connector.take_screenshot('/tmp/debug.png')
   ```

4. **Inspect Page HTML:**
   ```python
   html = extractor.connector.page.content()
   with open('/tmp/page.html', 'w') as f:
       f.write(html)
   ```

## External References

- [Trimble ProjectSight](https://prod.projectsightapp.trimble.com/)
- [Playwright Python Docs](https://playwright.dev/python/)
- [Trimble Identity](https://id.trimble.com/)
- Main project docs: `docs/PROJECTSIGHT_*.md`

---

**Remember:** This is a living document. Update as implementation progresses.
