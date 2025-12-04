# Browser-Based ProjectSight Extraction Guide

This guide covers how to extract data from ProjectSight using your existing browser session, bypassing the headless browser and login challenges.

## Quick Start

### Option 1: CDP Connection (Recommended)

Connect Playwright to your already-logged-in browser:

```bash
# 1. Start Chrome with remote debugging
google-chrome --remote-debugging-port=9222

# 2. Log into ProjectSight manually in that browser

# 3. Navigate to the page you want to extract (RFIs, Submittals, etc.)

# 4. Run the interactive extractor
python scripts/projectsight_interactive_extractor.py -i
```

### Option 2: Local Data Receiver

Push data from browser console to a local server:

```bash
# Terminal 1: Start the data receiver
python scripts/data_receiver_server.py

# Terminal 2: Open ProjectSight in any browser, then paste this in console:
# (See Browser Console Scripts section below)
```

---

## Detailed Setup

### Starting Chrome with Remote Debugging

Choose your platform:

```bash
# Linux
google-chrome --remote-debugging-port=9222

# macOS
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --remote-debugging-port=9222

# Windows (PowerShell)
& "C:\Program Files\Google\Chrome\Application\chrome.exe" --remote-debugging-port=9222

# Windows (CMD)
"C:\Program Files\Google\Chrome\Application\chrome.exe" --remote-debugging-port=9222
```

**Important:**
- Close all Chrome windows first, then run the command
- Or use `--user-data-dir=<temp-folder>` to run a separate instance

### Scripts Overview

| Script | Purpose |
|--------|---------|
| `scripts/browser_cdp_extractor.py` | Basic CDP extraction with multiple modes |
| `scripts/projectsight_interactive_extractor.py` | Interactive menu-driven extraction |
| `scripts/data_receiver_server.py` | Local server to receive data from browser |

---

## Usage Examples

### Interactive Mode

```bash
python scripts/projectsight_interactive_extractor.py -i
```

Menu options:
1. **discover** - Analyze page structure (tables, grids, rows)
2. **extract** - Extract visible table/list data
3. **modals** - Click each row, extract from modal, close, repeat
4. **pages** - Extract across all pages (pagination)
5. **save** - Save extracted data to JSON

### Command Line Mode

```bash
# Discover page structure
python scripts/projectsight_interactive_extractor.py --discover

# Extract current table view
python scripts/projectsight_interactive_extractor.py --extract -o rfis.json

# Extract by clicking each row (modal loop)
python scripts/projectsight_interactive_extractor.py --modal-loop "table tbody tr" -o data.json
```

### Basic CDP Extractor

```bash
# Extract table from current page
python scripts/browser_cdp_extractor.py --mode table

# Extract with pagination
python scripts/browser_cdp_extractor.py --mode paginated --max-pages 10

# Capture API calls (navigate around while running)
python scripts/browser_cdp_extractor.py --mode api --duration 60

# Navigate to section first
python scripts/browser_cdp_extractor.py --section rfis --mode paginated
```

---

## Browser Console Scripts

Paste these directly in the browser console while on ProjectSight:

### Extract Table Data

```javascript
// Extract all visible table rows
const data = [...document.querySelectorAll('table tbody tr')].map((row, i) => {
    const cells = [...row.querySelectorAll('td')];
    return {
        index: i,
        values: cells.map(c => c.textContent.trim())
    };
});
console.table(data);
copy(JSON.stringify(data, null, 2));  // Copies to clipboard
```

### Send to Local Server

```javascript
// First start: python scripts/data_receiver_server.py

async function extractAndSend() {
    const rows = [...document.querySelectorAll('table tbody tr')].map((row, i) => ({
        index: i,
        cells: [...row.querySelectorAll('td')].map(c => c.textContent.trim())
    }));

    const resp = await fetch('http://localhost:5050/ingest', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(rows)
    });

    const result = await resp.json();
    console.log(`Sent ${rows.length} rows. Total: ${result.total}`);
}

extractAndSend();
```

### Extract with Pagination (Manual Loop)

```javascript
async function extractAllPages() {
    const allData = [];
    let pageNum = 1;

    while (true) {
        console.log(`Extracting page ${pageNum}...`);

        // Extract current page
        const rows = [...document.querySelectorAll('table tbody tr')].map(row => ({
            page: pageNum,
            cells: [...row.querySelectorAll('td')].map(c => c.textContent.trim())
        }));

        allData.push(...rows);
        console.log(`Total: ${allData.length} rows`);

        // Find and click next button
        const nextBtn = document.querySelector('button[aria-label="Next"], .pagination-next');
        if (!nextBtn || nextBtn.disabled) {
            console.log('No more pages');
            break;
        }

        nextBtn.click();
        await new Promise(r => setTimeout(r, 2000));  // Wait for page load
        pageNum++;
    }

    // Send to server
    await fetch('http://localhost:5050/ingest', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(allData)
    });

    console.log(`Done! Extracted ${allData.length} rows from ${pageNum} pages`);
}

extractAllPages();
```

### Click-and-Extract Modal Loop

```javascript
async function extractWithModals(rowSelector = 'table tbody tr') {
    const results = [];
    const rows = document.querySelectorAll(rowSelector);

    for (let i = 0; i < rows.length; i++) {
        console.log(`Processing ${i + 1}/${rows.length}...`);

        // Re-query (DOM may change after modal closes)
        const currentRows = document.querySelectorAll(rowSelector);
        if (i >= currentRows.length) break;

        // Click to open modal
        currentRows[i].click();
        await new Promise(r => setTimeout(r, 1500));

        // Extract from modal
        const modal = document.querySelector('[role="dialog"], .modal');
        if (modal) {
            const data = {};

            // Get all label-value pairs
            modal.querySelectorAll('label, .label').forEach(label => {
                const key = label.textContent.trim().replace(':', '');
                const value = label.nextElementSibling?.textContent?.trim() ||
                              label.parentElement?.querySelector('input, .value')?.value;
                if (key && value) data[key] = value;
            });

            results.push(data);

            // Close modal
            const closeBtn = modal.querySelector('button[aria-label="Close"], .close-button');
            if (closeBtn) closeBtn.click();
            else document.querySelector('.modal-backdrop')?.click();

            await new Promise(r => setTimeout(r, 500));
        }
    }

    // Send results
    await fetch('http://localhost:5050/ingest', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(results)
    });

    console.log(`Done! Extracted ${results.length} items`);
}

extractWithModals();
```

---

## Capturing API Calls

ProjectSight's SPA makes API calls for data. Capture them:

### In Browser Console

```javascript
// Intercept fetch calls
const originalFetch = window.fetch;
window.capturedResponses = [];

window.fetch = async (...args) => {
    const response = await originalFetch(...args);
    const url = args[0];

    if (url.includes('/api/') || url.includes('/v1/')) {
        try {
            const clone = response.clone();
            const data = await clone.json();
            window.capturedResponses.push({ url, data });
            console.log('Captured:', url);
        } catch (e) {}
    }

    return response;
};

// Navigate around, then:
console.log(window.capturedResponses);
copy(JSON.stringify(window.capturedResponses, null, 2));
```

### Using CDP Extractor

```bash
python scripts/browser_cdp_extractor.py --mode api --duration 60
# Navigate around ProjectSight while this runs
```

---

## Tampermonkey Userscript

For persistent extraction across sessions, install as a userscript:

```javascript
// ==UserScript==
// @name         ProjectSight Extractor
// @namespace    http://tampermonkey.net/
// @version      1.0
// @description  Extract data from ProjectSight
// @match        https://*.projectsight*.trimble.com/*
// @grant        GM_xmlhttpRequest
// ==/UserScript==

(function() {
    'use strict';

    // Add floating button
    const btn = document.createElement('button');
    btn.innerHTML = '📥 Extract';
    btn.style.cssText = `
        position: fixed;
        bottom: 20px;
        right: 20px;
        z-index: 99999;
        padding: 10px 20px;
        background: #4CAF50;
        color: white;
        border: none;
        border-radius: 5px;
        cursor: pointer;
        font-size: 14px;
    `;
    document.body.appendChild(btn);

    btn.onclick = async () => {
        btn.innerHTML = '⏳ Extracting...';

        const rows = [...document.querySelectorAll('table tbody tr')].map((row, i) => ({
            index: i,
            cells: [...row.querySelectorAll('td')].map(c => c.textContent.trim())
        }));

        try {
            const resp = await fetch('http://localhost:5050/ingest', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(rows)
            });
            const result = await resp.json();
            btn.innerHTML = `✅ Sent ${rows.length}`;
        } catch (e) {
            btn.innerHTML = '❌ Error';
            console.error(e);
        }

        setTimeout(() => btn.innerHTML = '📥 Extract', 2000);
    };
})();
```

---

## Data Output

All extracted data is saved to `data/extracted/`:

```
data/extracted/
├── projectsight_20241201_143022.json
├── projectsight_20241201_152145.json
└── ...
```

JSON format:
```json
[
    {
        "col_0": "RFI-001",
        "col_1": "Foundation Question",
        "col_2": "Open",
        "_index": 0,
        "_extracted_at": "2024-12-01T14:30:22"
    },
    ...
]
```

---

## Troubleshooting

### "Cannot connect to browser"

- Make sure Chrome is running with `--remote-debugging-port=9222`
- Close all other Chrome windows first
- Try: `curl http://localhost:9222/json/version`

### "No tables found"

- ProjectSight uses dynamic rendering; wait for content to load
- Run `discover` command to see actual page structure
- Check if data is in a grid/list instead of table

### "Modal won't close"

- Try pressing Escape key
- Update `close_button_selector` in config
- Check for multiple modal layers

### "CORS error" (data receiver)

- The server has CORS enabled by default
- Make sure you're using `http://localhost:5050`, not `127.0.0.1`

---

## Next Steps

1. **Run discovery** to understand ProjectSight's page structure
2. **Test extraction** on a single page
3. **Scale up** with pagination or modal loops
4. **Integrate** with your ETL pipeline

For questions, check the main [CLAUDE.md](../CLAUDE.md) or [PROJECT_OVERVIEW.md](../PROJECT_OVERVIEW.md).
