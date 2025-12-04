# ProjectSight Daily Reports Extraction Guide

**Last Updated:** 2024-12-04

## Overview

This document describes how to extract Daily Reports data from ProjectSight using MCP Playwright. The key discovery is that ProjectSight uses **Infragistics igGrid** for data grids, which allows direct access to all data via the jQuery API, bypassing virtualized scrolling limitations.

## The Challenge

ProjectSight's Daily Reports grid uses virtualized scrolling (Infragistics igGrid), which means:
- Only ~30 visible rows are rendered in the DOM at any time
- Standard DOM scraping (`querySelectorAll('tr')`) only captures visible rows
- Scrolling loads new rows but unloads previous ones
- The grid has 415 total records but scroll-based extraction yielded incomplete data

## The Solution

Infragistics igGrid stores all data client-side and exposes it via jQuery:

```javascript
// Access the grid's underlying data source
const dataSource = $('#ugDataView').igGrid('option', 'dataSource');
```

This returns an array of all 415 records with full field data, regardless of which rows are currently visible.

## Step-by-Step Extraction Process

### 1. Navigate to Daily Reports

The Daily Reports section is accessed via ProjectSight's navigation:
- URL pattern: `https://prod.projectsightapp.trimble.com/web/app/Project?listid=-4038&orgid=...&projid=3`
- The content loads inside an iframe named `fraMenuContent`

### 2. Access the Grid Data

The grid is inside an iframe, so we need to access it through the iframe's window:

```javascript
// Get the iframe
const iframe = document.querySelector('iframe[name="fraMenuContent"]');
const iframeWindow = iframe.contentWindow;

// Access jQuery and the grid
const $grid = iframeWindow.$('#ugDataView');

// Get all data from the grid's data source
const dataSource = $grid.igGrid('option', 'dataSource');
```

### 3. Extract and Transform Data

```javascript
const records = dataSource.map(record => ({
  dailyReportId: record.dailyreportid,
  date: record.date,
  status: record.workflowstatename,
  weather: record.currentweatherconditions,
  weatherTemp: record.currentweathertemp,
  totalWorkApproved: record.totalworkapproved,
  totalComments: record.totalcomments,
  totalLinks: record.totallinks,
  totalSnowfall: record.totalsnowfall,
  totalRainfall: record.totalrainfall,
  lostProductivity: record.lostproductivity,
  lastModifiedBy: record.lastmodifiedby,
  lastModified: record.lastmodified,
  isLocked: record.islocked,
  guid: record.guid,
  projectId: record.projectid,
  totalAssignmentsCount: record.totalassignmentscount,
  openAssignmentsCount: record.openassignmentscount,
  openAssignmentsContactNames: record.openassignmentscontactnames,
  contributingContactNames: record.contributingcontactnames,
  contributingCompanyNames: record.contributingcompanynames
}));
```

### 4. Save to File

Using MCP Playwright's browser_evaluate, trigger a download:

```javascript
const exportData = {
  extractedAt: new Date().toISOString(),
  source: 'ProjectSight Daily Reports',
  totalRecords: records.length,
  records: records
};

const jsonStr = JSON.stringify(exportData, null, 2);
const blob = new Blob([jsonStr], { type: 'application/json' });
const url = URL.createObjectURL(blob);

const a = document.createElement('a');
a.href = url;
a.download = 'daily_reports_415.json';
document.body.appendChild(a);
a.click();
```

## Complete Extraction Script

```javascript
// Run this in browser_evaluate after navigating to Daily Reports
() => {
  const iframe = document.querySelector('iframe[name="fraMenuContent"]');
  if (!iframe) return { error: 'Iframe not found' };

  const iframeWindow = iframe.contentWindow;
  const $grid = iframeWindow.$('#ugDataView');

  if (!$grid.length) return { error: 'Grid not found' };

  const dataSource = $grid.igGrid('option', 'dataSource');

  const records = dataSource.map(record => ({
    dailyReportId: record.dailyreportid,
    date: record.date,
    status: record.workflowstatename,
    weather: record.currentweatherconditions,
    weatherTemp: record.currentweathertemp,
    weatherUom: record.currentweatheruom,
    totalWorkApproved: record.totalworkapproved,
    totalComments: record.totalcomments,
    totalLinks: record.totallinks,
    totalSnowfall: record.totalsnowfall,
    totalRainfall: record.totalrainfall,
    lostProductivity: record.lostproductivity,
    totalLaborFileLinks: record.totallaborfilelinks,
    totalWorkersApproved: record.totalworkersapproved,
    totalEquipmentRecords: record.totalequipmentrecords,
    totalEquipmentQty: record.totalequipmentqty,
    totalEquipmentHours: record.totalequipmenthours,
    lastModifiedBy: record.lastmodifiedby,
    lastModified: record.lastmodified,
    isLocked: record.islocked,
    guid: record.guid,
    projectId: record.projectid,
    totalAssignmentsCount: record.totalassignmentscount,
    openAssignmentsCount: record.openassignmentscount,
    openAssignmentsContactNames: record.openassignmentscontactnames,
    contributingContactNames: record.contributingcontactnames,
    contributingCompanyNames: record.contributingcompanynames
  }));

  // Store in window for retrieval
  window.__dailyReportsData = records;

  return {
    success: true,
    recordCount: records.length,
    sample: records.slice(0, 2)
  };
}
```

## Data Schema

Each Daily Report record contains:

| Field | Type | Description |
|-------|------|-------------|
| `dailyReportId` | number | Unique report ID |
| `date` | string (ISO) | Report date |
| `status` | string | Workflow status (e.g., "Pending", "Ready For Review") |
| `weather` | string/null | Weather conditions |
| `weatherTemp` | number/null | Temperature |
| `weatherUom` | string/null | Temperature unit of measure |
| `totalWorkApproved` | number | Approved work count |
| `totalComments` | number | Number of comments |
| `totalLinks` | number | Number of links |
| `totalSnowfall` | number | Snowfall amount |
| `totalRainfall` | number | Rainfall amount |
| `lostProductivity` | number | Lost productivity hours |
| `totalLaborFileLinks` | number | Labor file links count |
| `totalWorkersApproved` | number | Approved workers count |
| `totalEquipmentRecords` | number | Equipment records count |
| `totalEquipmentQty` | number | Equipment quantity |
| `totalEquipmentHours` | number | Equipment hours |
| `lastModifiedBy` | string | Email of last modifier |
| `lastModified` | string (ISO) | Last modification timestamp |
| `isLocked` | boolean | Whether report is locked |
| `guid` | string (UUID) | Unique identifier for API access |
| `projectId` | number | Project ID |
| `totalAssignmentsCount` | number | Total assignments |
| `openAssignmentsCount` | number | Open assignments |
| `openAssignmentsContactNames` | string/null | Comma-separated contact names |
| `contributingContactNames` | string/null | Newline-separated contributor names |
| `contributingCompanyNames` | string/null | Newline-separated company names |

## Output

- **File:** `data/projectsight/extracted/daily_reports_415.json`
- **Size:** ~368 KB
- **Records:** 415 Daily Reports
- **Date Range:** 2022-09-12 to present

## Applying to Other Grids

This technique works for any Infragistics igGrid in ProjectSight:

1. Identify the grid element ID (e.g., `#ugDataView`)
2. Access via iframe if needed: `iframe.contentWindow.$('#gridId')`
3. Get data source: `.igGrid('option', 'dataSource')`
4. Map fields as needed (field names are lowercase in the raw data)

## Potential Grids to Extract

- RFIs: Similar grid structure
- Submittals: Similar grid structure
- Issues: Similar grid structure
- Change Orders: Similar grid structure

## Troubleshooting

### "Grid not found"
- Ensure the page has fully loaded (wait for grid to render)
- Check if content is inside an iframe
- Verify the grid element ID in browser DevTools

### "$ is not defined"
- Access jQuery through the iframe's window object
- ProjectSight uses jQuery internally

### Incomplete data
- Don't use DOM-based extraction for virtualized grids
- Always use the `.igGrid('option', 'dataSource')` method
