# Daily Reports Detail/History Extraction Guide

**Last Updated:** 2024-12-04

## Overview

This document describes how to extract Daily Report **detail view data** from ProjectSight, specifically the **History tab** which contains audit trail information (who created each report, when, and what changes were made).

This process complements the grid extraction documented in [PROJECTSIGHT_DAILY_REPORTS_EXTRACTION.md](PROJECTSIGHT_DAILY_REPORTS_EXTRACTION.md), which extracts summary data from the list view.

## Data Files

| File | Records | Description |
|------|---------|-------------|
| `data/extracted/daily_reports_415.json` | 415 | Summary grid data (date, status, weather, counts) |
| `data/extracted/daily-report-details-history.json` | 414 | History/audit trail from detail view |

## What the History Tab Contains

Each Daily Report detail view has two tabs:
1. **Daily report** - Form fields (date, status, rainfall, snowfall, etc.)
2. **History** - Audit trail showing:
   - Created by (name, email, company)
   - Created at (timestamp)
   - Field changes (with old/new values)

### Sample Extracted Record

```json
{
  "recordNumber": 100,
  "totalRecords": 415,
  "reportDate": "4/19/2023",
  "history": {
    "createdByFull": "Herman Villalobos (hvillalobos@wgyates.com)",
    "createdByCompany": "W.G. Yates & Sons Construction Company",
    "createdByEmail": "hvillalobos@wgyates.com",
    "createdByName": "Herman Villalobos",
    "createdAt": "4/19/2023 9:42 AM",
    "changes": [
      { "field": "Date", "value": "4/19/2023" },
      { "field": "Lost productivity UOM", "oldValue": "None", "newValue": "Hours" },
      { "field": "Status", "value": "Pending" }
    ]
  },
  "extractedAt": "2025-12-04T17:51:03.338Z"
}
```

## How to Re-run the Extraction

### Prerequisites

1. MCP Playwright browser connected to ProjectSight
2. Logged into ProjectSight with appropriate permissions
3. Navigated to Daily Reports section

### Step-by-Step Process

#### 1. Navigate to Daily Reports

```
Project Menu > Records > Daily reports
```

URL pattern: `https://prod.projectsightapp.trimble.com/web/app/Project?listid=-4038&orgid=...&projid=3`

#### 2. Open the First Daily Report Detail View

Double-click on the first row in the grid to open the detail modal.

#### 3. Initialize Storage in Browser

Run this JavaScript via `browser_evaluate`:

```javascript
window.__dailyReportDetails = [];
```

#### 4. Extract Records in Batches

The extraction loops through all records using the "Next record" navigation button. Run this JavaScript to extract a batch:

```javascript
async function extractBatch(batchSize = 50) {
  const results = [];

  for (let i = 0; i < batchSize; i++) {
    // Click History tab
    const detailIframe = document.querySelector('iframe[name="fraDef"]');
    const iframeDoc = detailIframe.contentDocument || detailIframe.contentWindow.document;

    const historyTab = Array.from(iframeDoc.querySelectorAll('div, span'))
      .find(el => el.textContent === 'History');
    if (historyTab) historyTab.click();

    await new Promise(r => setTimeout(r, 300));

    // Extract data
    const textContent = iframeDoc.body.innerText;

    const positionMatch = textContent.match(/(\d+)\s*of\s*(\d+)/);
    const currentRecord = positionMatch ? parseInt(positionMatch[1]) : null;
    const totalRecords = positionMatch ? parseInt(positionMatch[2]) : null;

    const headerMatch = textContent.match(/Daily report - (\d{1,2}\/\d{1,2}\/\d{4})/);
    const reportDate = headerMatch ? headerMatch[1] : null;

    // Parse history
    const history = {};
    const createdByMatch = textContent.match(/Created by\s+([^\n]+?)\s+at\s+([^\n]+?)(?:\n|$)/);
    if (createdByMatch) {
      history.createdByFull = createdByMatch[1].trim();
      history.createdByCompany = createdByMatch[2].trim();

      const emailMatch = history.createdByFull.match(/\(([^)]+@[^)]+)\)/);
      if (emailMatch) {
        history.createdByEmail = emailMatch[1];
        history.createdByName = history.createdByFull.replace(/\s*\([^)]+@[^)]+\)/, '').trim();
      }
    }

    const createdAtMatch = textContent.match(/Created by[\s\S]*?(\d{1,2}\/\d{1,2}\/\d{4}\s+\d{1,2}:\d{2}\s+[AP]M)/);
    if (createdAtMatch) {
      history.createdAt = createdAtMatch[1];
    }

    // Parse changes
    const changes = [];
    const historyStart = textContent.indexOf('Created by');
    if (historyStart > -1) {
      const historyText = textContent.substring(historyStart);

      const oldNewPattern = /([A-Za-z ]+)\s+Old:\s*([^\n]+)\s+New:\s*([^\n]+)/g;
      let match;
      while ((match = oldNewPattern.exec(historyText)) !== null) {
        changes.push({
          field: match[1].trim(),
          oldValue: match[2].trim(),
          newValue: match[3].trim()
        });
      }

      const dateMatch = historyText.match(/\nDate\n(\d{1,2}\/\d{1,2}\/\d{4})/);
      if (dateMatch) changes.push({ field: 'Date', value: dateMatch[1] });

      const statusMatch = historyText.match(/\nStatus\n(Pending|Ready For Review|Approved|Rejected)/);
      if (statusMatch) changes.push({ field: 'Status', value: statusMatch[1] });
    }
    history.changes = changes;

    const record = {
      recordNumber: currentRecord,
      totalRecords,
      reportDate,
      history,
      extractedAt: new Date().toISOString()
    };

    // Store if not duplicate
    if (!window.__dailyReportDetails.find(r => r.recordNumber === currentRecord)) {
      window.__dailyReportDetails.push(record);
      results.push(record);
    }

    console.log(`Extracted ${currentRecord} of ${totalRecords}: ${reportDate}`);

    if (currentRecord >= totalRecords) {
      console.log('Reached last record');
      break;
    }

    // Click Next
    const nextBtn = iframeDoc.querySelector('[title="Next record"] a');
    if (nextBtn) {
      nextBtn.click();
      await new Promise(r => setTimeout(r, 1000));
    } else {
      console.log('Next button not found');
      break;
    }
  }

  return {
    batchExtracted: results.length,
    totalStored: window.__dailyReportDetails.length,
    lastRecord: results[results.length - 1]
  };
}

extractBatch(50);
```

Repeat the batch extraction until all records are processed.

#### 5. Save the Extracted Data

Once extraction is complete, trigger a download:

```javascript
(() => {
  const data = window.__dailyReportDetails;

  const exportData = {
    extractedAt: new Date().toISOString(),
    source: 'ProjectSight Daily Report Details - History Tab',
    description: 'Audit trail data including createdBy, createdAt, and field changes',
    totalRecords: data.length,
    dateRange: {
      first: data[0]?.reportDate,
      last: data[data.length - 1]?.reportDate
    },
    records: data
  };

  const jsonStr = JSON.stringify(exportData, null, 2);
  const blob = new Blob([jsonStr], { type: 'application/json' });
  const url = URL.createObjectURL(blob);

  const a = document.createElement('a');
  a.href = url;
  a.download = 'daily_report_details_history.json';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);

  return { success: true, recordCount: data.length };
})();
```

#### 6. Copy to Data Directory

```bash
cp /workspaces/mxi-samsung/.playwright-mcp/daily-report-details-history.json \
   /workspaces/mxi-samsung/data/extracted/
```

## Technical Details

### Page Structure

ProjectSight uses nested iframes:
- Main page contains `iframe[name="fraMenuContent"]` for the grid
- Detail modal loads in `iframe[name="fraDef"]`

### Navigation

- Records are navigated using "Previous record" / "Next record" buttons
- Position indicator shows "X of Y" format (e.g., "100 of 415")
- Must use JavaScript click via `page.evaluate()` due to iframe context

### Key Selectors

| Element | Selector |
|---------|----------|
| Detail iframe | `iframe[name="fraDef"]` |
| History tab | `div` or `span` with text "History" |
| Next button | `[title="Next record"] a` |
| Position | Text matching `/(\d+)\s*of\s*(\d+)/` |

## Incremental Updates

To fetch only new records:

1. Check the last extracted record date
2. Navigate to Daily Reports grid
3. Sort by date descending (newest first)
4. Open the first (newest) record
5. Extract until you reach a record that was previously extracted
6. Merge new records with existing data

## Troubleshooting

### "Detail iframe not found"
- Ensure a detail modal is open (double-click a grid row)
- Wait for the modal to fully load

### "Next button not found"
- You may have reached the last record
- Check the position indicator (X of Y)

### Duplicate records
- The extraction script checks for duplicates by `recordNumber`
- Re-running will skip already-extracted records in the same session

### Missing history data
- Some records may have empty history (no audit trail)
- This is normal for records that were never modified after creation

## Python Script

A Python script is also available for automated extraction:

```bash
python scripts/extract_daily_report_details.py
```

This requires Chrome running with remote debugging:
```bash
google-chrome --remote-debugging-port=9222
```

## Related Documentation

- [PROJECTSIGHT_DAILY_REPORTS_EXTRACTION.md](PROJECTSIGHT_DAILY_REPORTS_EXTRACTION.md) - Grid/summary extraction
- [BROWSER_EXTRACTION_GUIDE.md](BROWSER_EXTRACTION_GUIDE.md) - General browser extraction patterns
