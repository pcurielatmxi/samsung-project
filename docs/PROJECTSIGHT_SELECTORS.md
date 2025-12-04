# ProjectSight Web Scraping Selectors and Structure

**Generated:** 2025-12-04
**Base URL:** https://prod.projectsightapp.trimble.com/

## Overview

ProjectSight uses Trimble Identity for SSO authentication and a page-based navigation system (not modals). The application is heavily JavaScript-based with delayed rendering.

## Authentication Flow

### 1. Login Page (Trimble Identity)

**Initial URL:** `https://id.trimble.com/ui/sign_in.html`
**After region detection:** `https://us.id.trimble.com/ui/sign_in.html`

#### Selectors

```python
# Cookie consent (appears first)
COOKIE_ACCEPT_BUTTON = 'button:has-text("Accept All")'

# Username page
USERNAME_FIELD = 'input[type="text"][placeholder*="Email"], textbox[name="Email or username"]'
USERNAME_SUBMIT = 'button:has-text("Next")'
# Alternative: Press Enter after filling username

# Password page (after username submission)
PASSWORD_FIELD = 'input[type="password"], textbox[name="Password"]'
PASSWORD_SUBMIT = 'button:has-text("Sign in")'
# Alternative: Press Enter after filling password

# Remember me checkbox (optional)
REMEMBER_ME_CHECKBOX = 'input[type="checkbox"][name="remember_me"]'
```

#### Using Playwright getByRole (Recommended)

```python
# Username
page.get_by_role("textbox", name="Email or username").fill(username)
page.keyboard.press("Enter")

# Password
page.get_by_role("textbox", name="Password").fill(password)
page.keyboard.press("Enter")
```

### 2. MFA Handling

**Status:** Not required for this account (pcuriel@mxi.pro)
**If required:** Manual entry by user - wait for home screen to appear

```python
# MFA detection (if implemented in future)
MFA_INPUT_FIELD = 'input[type="text"][placeholder*="code"], input[name="otp"]'
HOME_SCREEN_INDICATOR = 'a[href*="/web/app/Projects"], .projectsight-logo'
```

### 3. Login Success Detection

After successful login, the page redirects to:
- **URL:** `https://prod.projectsightapp.trimble.com/web/app/Projects`
- **Page Title:** "Trimble ProjectSight Projects"

```python
# Wait for navigation to complete
page.wait_for_url("**/web/app/Projects*", timeout=30000)

# Alternative: Wait for projects page element
page.wait_for_selector('div:has-text("Projects ("), timeout=30000)
```

## Projects List Page Structure

**URL:** `https://prod.projectsightapp.trimble.com/web/app/Projects`

### Key Elements

```python
# Projects heading in sidebar
PROJECTS_HEADING = 'div:has-text("Projects (")'  # Shows count: "Projects (1)"

# Portfolios section
PORTFOLIOS_HEADING = 'div:has-text("Portfolios (")'  # Shows count: "Portfolios (1)"

# Search box
SEARCH_BOX = 'input[placeholder="Search..."]'

# View selector dropdown
VIEW_SELECTOR = 'div:has-text("Default view")'

# Owner filter dropdown
OWNER_FILTER = 'div:has-text("Owned by anyone")'

# Project cards
PROJECT_CARDS = 'div:has-text("Samsung Taylor Project")'  # Will vary by project
PROJECT_HIERARCHY = 'div:has-text("T-PJT > FAB1 > Construction")'  # Breadcrumb path

# Walkthrough link
WALKTHROUGH_LINK = 'div:has-text("Walkthrough")'
```

### Project Card Structure

Each project is displayed as a card with:
- **Project Image:** Circular thumbnail
- **Hierarchy Path:** `T-PJT > FAB1 > Construction`
- **Project Name:** `Samsung Taylor Project`
- **Walkthrough Link:** Bottom of page

**Note:** Clicking a project card navigates to a new page, NOT a modal.

## Project Details Page Structure

**URL Pattern:** `https://prod.projectsightapp.trimble.com/web/app/Project?orgid={org_id}&projid={proj_id}`
**Example:** `.../Project?orgid=4540f425-f7b5-4ad8-837d-c270d5d09490&projid=3`

### Navigation Menu (Left Sidebar)

```python
# Project menu sections
HOME_LINK = 'a:has-text("Home")'
DRAWINGS_LINK = 'a:has-text("Drawings")'
SPECIFICATIONS_LINK = 'a:has-text("Specifications")'
PHOTOS_LINK = 'a:has-text("Photos")'

# Records section (expandable)
RECORDS_SECTION = 'div:has-text("Records")'
SUBMITTALS_LINK = 'a:has-text("Submittals")'
EXCAVATION_LINK = 'a:has-text("Excavation Permit & Punch Item")'
RFIS_LINK = 'a:has-text("RFIs")'
DAILY_REPORTS_LINK = 'a:has-text("Daily reports")'
QA_QC_LINK = 'a:has-text("QA/QC Inspections")'

# Files section
LIBRARY_LINK = 'a:has-text("Library")'
MODELS_LINK = 'a:has-text("Models")'

# Return to projects list
MY_PROJECTS_LINK = 'a:has-text("My Projects"), div:has-text("My Projects")'
```

### Dashboard Widgets (Main Content Area)

The project home page contains multiple widgets in an iframe:

```python
# Main iframe selector
DASHBOARD_IFRAME = 'iframe'  # May need more specific selector

# Within iframe - Work Progress Widget
WORK_PROGRESS_WIDGET = 'div:has-text("Work progress")'
WORK_PROGRESS_VALUE = 'div:has-text("N/A")'  # Or actual percentage

# NCR/QOR/SOR/SWN/VR Widget
NCR_WIDGET = 'a:has-text("NCR/QOR/SOR/SWN/VR")'
NCR_OPEN_COUNT = 'a:has-text("Open") + a'  # Value: "522"
NCR_CLOSED_COUNT = 'a:has-text("Closed") + a'  # Value: "1447"
NCR_PLACEHOLDER_COUNT = 'a:has-text("Placeholder") + a'  # Value: "27"

# Overdue Items Widget
OVERDUE_WIDGET = 'div:has-text("Overdue items")'
OVERDUE_TOP3 = 'div:has-text("Total:") + div'  # Format: "5110 / 5956"

# Due Today Widget
DUE_TODAY_WIDGET = 'div:has-text("Due today items")'
DUE_TODAY_COUNT = 'div:has-text("Total:") + div'  # Format: "33 / 46"

# Due in Next 7 Days Widget
DUE_7DAYS_WIDGET = 'div:has-text("Due in next 7 days")'
DUE_7DAYS_COUNT = 'div:has-text("Total:") + div'  # Format: "94 / 144"

# RFIs Widget
RFIS_WIDGET = 'a:has-text("RFIs")'
RFI_CM_REVIEW = 'a:has-text("CM Review") + a'  # Value: "68"
RFI_AE_REVIEW = 'a:has-text("A/E Review") + a'  # Value: "92"
RFI_AE_CLOSED = 'a:has-text("A/E Closed") + a'  # Value: "9630"
RFI_OWNER_REVIEW = 'a:has-text("Owner Review") + a'  # Value: "20"
RFI_AVG_TURNAROUND = 'div:has-text("Average turnaround:")'  # Value: "13 Day(s)"

# Submittals Widget
SUBMITTALS_WIDGET = 'a:has-text("Submittals")'
SUBMITTALS_OPEN = 'a:has-text("Open") + a'  # Value: "146"
SUBMITTALS_REVISE = 'a:has-text("Not Accepted - Revise and Resubmit") + a'  # Value: "1061"
SUBMITTALS_APPROVED = 'a:has-text("Approved") + a'  # Value: "1743"
SUBMITTALS_APPROVED_COMMENTS = 'a:has-text("Approved with Comments") + a'  # Value: "4654"
SUBMITTALS_INFO_ONLY = 'a:has-text("Received for Information") + a'  # Value: "803"
SUBMITTALS_AVG_TURNAROUND = 'div:has-text("Average turnaround:")'  # Value: "16 Day(s)"
```

### Assignments Panel (Right Side)

```python
# Assignments iframe
ASSIGNMENTS_IFRAME = 'iframe:has-text("Assignments")'

# Within assignments iframe
ASSIGNED_TO_ME = 'div:has-text("Assigned to: Me")'
COMPANY_FILTER = 'div:has-text("Company: My company")'
RECORD_TYPE_FILTER = 'div:has-text("Record type: All")'
STATUS_FILTER = 'div:has-text("Status: All")'
DUE_DATE_FILTER = 'div:has-text("Due date: All")'

# No assignments message
NO_ASSIGNMENTS = 'div:has-text("No assignments")'
```

## Data Extraction Strategy

### Important Notes

1. **No Modal Structure:** ProjectSight uses page navigation instead of modals
2. **JavaScript Rendering:** Pages require 3-5 seconds to fully load
3. **Multiple Iframes:** Dashboard widgets and assignments are in separate iframes
4. **Dynamic Content:** Counts and statistics update based on project data

### Recommended Scraping Flow

```python
# 1. Login
page.goto("https://prod.projectsightapp.trimble.com/")
# Fill credentials and submit (see authentication flow above)
page.wait_for_url("**/web/app/Projects*")

# 2. Get project list
page.wait_for_timeout(3000)  # Wait for JS to render
projects = []

# Extract project cards
project_elements = page.query_selector_all('div:has-text("Samsung")')  # Adjust selector
for project in project_elements:
    hierarchy = project.query_selector('div').text_content()  # e.g., "T-PJT > FAB1 > Construction"
    name = project.query_selector_all('div')[1].text_content()  # e.g., "Samsung Taylor Project"

    # Click to navigate to project details
    project.click()
    page.wait_for_url("**/web/app/Project?*")
    page.wait_for_timeout(5000)  # Wait for dashboard to load

    # Extract dashboard data (see iframe handling below)
    # ...

    # Return to projects list
    page.click('a:has-text("My Projects")')
    page.wait_for_url("**/web/app/Projects*")
    page.wait_for_timeout(2000)

# 3. Extract data from iframe
frame = page.frame_locator('iframe').first
work_progress = frame.locator('div:has-text("Work progress")').text_content()
# ... extract other widgets

# 4. Handle nested iframes
assignments_frame = page.frame_locator('iframe').nth(1)  # Second iframe
assignments = assignments_frame.locator('div:has-text("No assignments")').is_visible()
```

## Environment Variables for .env

```bash
# URLs
PROJECTSIGHT_BASE_URL=https://prod.projectsightapp.trimble.com/
PROJECTSIGHT_LOGIN_URL=https://id.trimble.com/ui/sign_in.html

# Selectors - Username page
PROJECTSIGHT_SELECTOR_USERNAME=textbox[name="Email or username"]
PROJECTSIGHT_SELECTOR_PASSWORD=textbox[name="Password"]
PROJECTSIGHT_SELECTOR_SUBMIT=button:has-text("Sign in")

# Selectors - MFA (if needed)
PROJECTSIGHT_SELECTOR_MFA_INPUT=input[name="otp"]

# Selectors - Home screen verification
PROJECTSIGHT_SELECTOR_HOME_INDICATOR=div:has-text("Projects (")

# Selectors - Projects list
PROJECTSIGHT_SELECTOR_PROJECT_CARDS=div[class*="project-card"]
PROJECTSIGHT_SELECTOR_MY_PROJECTS=a:has-text("My Projects")

# Timeouts (in seconds)
PROJECTSIGHT_TIMEOUT=30
PROJECTSIGHT_PAGE_LOAD_WAIT=5
PROJECTSIGHT_IFRAME_WAIT=3

# Session management
PROJECTSIGHT_HEADLESS=false  # Set to true for production
PROJECTSIGHT_SESSION_VALIDITY_DAYS=7
```

## Screenshots Captured

All screenshots saved to `.playwright-mcp/`:

1. `projectsight_01_login_page.png` - Initial login page (username entry)
2. `projectsight_02_password_page.png` - Password entry page
3. `projectsight_03_projects_home.png` - Projects list page after login
4. `projectsight_04_project_details.png` - Project detail dashboard
5. `projectsight_05_back_to_projects.png` - Return to projects list

## Next Steps

1. Update `src/extractors/system_specific/projectsight_extractor.py`:
   - Remove modal-based extraction logic
   - Implement page navigation approach
   - Add iframe handling for dashboard widgets

2. Update `src/config/settings.py`:
   - Add new selector environment variables
   - Add page load timeout settings

3. Test extraction with actual data:
   - Run extractor with `debug=True`
   - Verify all dashboard widgets are captured
   - Handle pagination if multiple projects exist

4. Implement robust error handling:
   - Retry logic for failed page loads
   - Graceful degradation if widgets are missing
   - Screenshot capture on errors for debugging
