#!/usr/bin/env python3
"""
Extract Daily Report detail and history data from ProjectSight.

This script navigates through all Daily Reports in the detail view,
extracting both the main report data and the History tab audit trail.

USAGE:
1. Start the MCP Playwright browser and navigate to Daily Reports
2. Open the first daily report detail view (double-click a row)
3. Run this script to extract all 415 reports

The script uses the "Next record" navigation to iterate through all reports.
"""

import json
import asyncio
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright


# JavaScript extraction function to run in the browser
EXTRACT_DETAIL_JS = """
() => {
  // Access the detail iframe (fraDef)
  const detailIframe = document.querySelector('iframe[name="fraDef"]');
  if (!detailIframe) return { error: 'Detail iframe not found' };

  const iframeDoc = detailIframe.contentDocument || detailIframe.contentWindow.document;
  if (!iframeDoc) return { error: 'Cannot access iframe document' };

  const textContent = iframeDoc.body.innerText;

  // Get the current record position
  const positionMatch = textContent.match(/(\\d+)of(\\d+)/);
  const currentRecord = positionMatch ? parseInt(positionMatch[1]) : null;
  const totalRecords = positionMatch ? parseInt(positionMatch[2]) : null;

  // Get report date from header
  const headerMatch = textContent.match(/Daily report - (\\d{1,2}\\/\\d{1,2}\\/\\d{4})/);
  const reportDate = headerMatch ? headerMatch[1] : null;

  // Extract History tab data
  const history = {};

  // Created by pattern: "Created by Name (email) at Company"
  const createdByMatch = textContent.match(/Created by\\s+([^\\n]+?)\\s+at\\s+([^\\n]+?)(?:\\n|$)/);
  if (createdByMatch) {
    history.createdByName = createdByMatch[1].trim();
    history.createdByCompany = createdByMatch[2].trim();

    // Extract email from name
    const emailMatch = history.createdByName.match(/\\(([^)]+@[^)]+)\\)/);
    if (emailMatch) {
      history.createdByEmail = emailMatch[1];
      history.createdByName = history.createdByName.replace(/\\s*\\([^)]+@[^)]+\\)/, '').trim();
    }
  }

  // Created at timestamp - first datetime pattern after "Created by"
  const createdAtMatch = textContent.match(/Created by[\\s\\S]*?(\\d{1,2}\\/\\d{1,2}\\/\\d{4}\\s+\\d{1,2}:\\d{2}\\s+[AP]M)/);
  if (createdAtMatch) {
    history.createdAt = createdAtMatch[1];
  }

  // Parse all field changes from History section
  // The pattern is: Field Name followed by values
  const changes = [];

  // Look for the history section (after the created by line)
  const historyStart = textContent.indexOf('Created by');
  if (historyStart > -1) {
    const historyText = textContent.substring(historyStart);

    // Extract changes with old/new values
    const oldNewPattern = /([A-Za-z ]+)\\s+Old:\\s*([^\\n]+)\\s+New:\\s*([^\\n]+)/g;
    let match;
    while ((match = oldNewPattern.exec(historyText)) !== null) {
      changes.push({
        field: match[1].trim(),
        oldValue: match[2].trim(),
        newValue: match[3].trim()
      });
    }

    // Extract date change (standalone)
    const dateMatch = historyText.match(/\\nDate\\n(\\d{1,2}\\/\\d{1,2}\\/\\d{4})/);
    if (dateMatch) {
      changes.push({ field: 'Date', value: dateMatch[1] });
    }

    // Extract status (standalone)
    const statusMatch = historyText.match(/\\nStatus\\n(Pending|Ready For Review|Approved|Rejected)/);
    if (statusMatch) {
      changes.push({ field: 'Status', value: statusMatch[1] });
    }
  }

  history.changes = changes;

  // Extract side panel data (visible in Daily report tab)
  // These are always visible even when History tab is selected
  const sidePanel = {};

  // Status from combobox
  const statusCombo = iframeDoc.querySelector('[role="combobox"]');
  if (statusCombo) {
    const statusText = statusCombo.textContent || statusCombo.innerText;
    if (statusText && (statusText.includes('Pending') || statusText.includes('Review') || statusText.includes('Approved') || statusText.includes('Rejected'))) {
      sidePanel.status = statusText.trim();
    }
  }

  // Get values from input fields
  const inputs = iframeDoc.querySelectorAll('input[type="text"], input:not([type])');
  inputs.forEach(input => {
    const value = input.value;
    const label = input.closest('div')?.querySelector('[class*="label"]')?.textContent || '';

    if (label.includes('rainfall') || input.id?.includes('rainfall')) {
      sidePanel.totalRainfall = value;
    }
    if (label.includes('snowfall') || input.id?.includes('snowfall')) {
      sidePanel.totalSnowfall = value;
    }
    if (label.includes('productivity') || input.id?.includes('productivity')) {
      sidePanel.lostProductivity = value;
    }
  });

  // Date from combobox
  const dateCombo = iframeDoc.querySelector('input[disabled][value*="/"]');
  if (dateCombo) {
    sidePanel.date = dateCombo.value;
  }

  return {
    currentRecord,
    totalRecords,
    reportDate,
    history,
    sidePanel,
    extractedAt: new Date().toISOString()
  };
}
"""

# JavaScript to click "Next record" button
CLICK_NEXT_JS = """
() => {
  const detailIframe = document.querySelector('iframe[name="fraDef"]');
  if (!detailIframe) return { error: 'Detail iframe not found' };

  const iframeDoc = detailIframe.contentDocument || detailIframe.contentWindow.document;

  // Find the "Next record" button by title (most reliable)
  const nextBtn = iframeDoc.querySelector('[title="Next record"] a');
  if (nextBtn) {
    nextBtn.click();
    return { success: true, method: 'title selector' };
  }

  // Try by class as fallback
  const nextBtnByClass = iframeDoc.querySelector('a.fa-caret-right');
  if (nextBtnByClass) {
    nextBtnByClass.click();
    return { success: true, method: 'class selector' };
  }

  return { error: 'Next button not found' };
}
"""

# JavaScript to click "History" tab
CLICK_HISTORY_TAB_JS = """
() => {
  const detailIframe = document.querySelector('iframe[name="fraDef"]');
  if (!detailIframe) return { error: 'Detail iframe not found' };

  const iframeDoc = detailIframe.contentDocument || detailIframe.contentWindow.document;

  // Find the History tab
  const historyTab = Array.from(iframeDoc.querySelectorAll('div, span')).find(el => el.textContent === 'History');
  if (historyTab) {
    historyTab.click();
    return { success: true };
  }

  return { error: 'History tab not found' };
}
"""


async def extract_daily_report_details():
    """Connect to browser and extract all daily report details."""
    output_dir = Path('/workspaces/mxi-samsung/data/extracted')
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path = output_dir / f"daily_report_details_{timestamp}.json"

    async with async_playwright() as p:
        # Connect to existing browser via CDP
        try:
            browser = await p.chromium.connect_over_cdp('http://localhost:9222')
            print(f"Connected to browser")
        except Exception as e:
            print(f"Error connecting to browser: {e}")
            print("Make sure Chrome is running with --remote-debugging-port=9222")
            print("Or use the MCP Playwright browser.")
            return

        # Get the first context and page
        contexts = browser.contexts
        if not contexts:
            print("No browser contexts found")
            return

        pages = contexts[0].pages
        if not pages:
            print("No pages found")
            return

        page = pages[0]
        print(f"Connected to page: {page.url}")

        all_records = []
        max_records = 415  # Safety limit
        current = 0
        errors = []

        while current < max_records:
            try:
                # Click History tab first to ensure history data is visible
                await page.evaluate(CLICK_HISTORY_TAB_JS)
                await asyncio.sleep(0.3)  # Brief pause for tab switch

                # Extract current record
                data = await page.evaluate(EXTRACT_DETAIL_JS)

                if 'error' in data:
                    print(f"Error extracting record {current + 1}: {data['error']}")
                    errors.append({'record': current + 1, 'error': data['error']})
                    break

                all_records.append(data)
                current = data.get('currentRecord', current + 1)
                total = data.get('totalRecords', max_records)

                print(f"Extracted record {current} of {total}: {data.get('reportDate', 'unknown date')}")

                if current >= total:
                    print("Reached last record")
                    break

                # Click next
                await asyncio.sleep(0.3)  # Brief pause
                next_result = await page.evaluate(CLICK_NEXT_JS)

                if 'error' in next_result:
                    print(f"Error clicking next: {next_result['error']}")
                    errors.append({'record': current, 'error': next_result['error']})
                    break

                # Wait for page to load
                await asyncio.sleep(1.0)

            except Exception as e:
                print(f"Exception at record {current + 1}: {e}")
                errors.append({'record': current + 1, 'error': str(e)})
                break

        # Save results
        export_data = {
            'extractedAt': datetime.now().isoformat(),
            'source': 'ProjectSight Daily Report Details',
            'totalRecords': len(all_records),
            'errors': errors,
            'records': all_records
        }

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)

        print(f"\nExtraction complete!")
        print(f"Saved {len(all_records)} records to: {output_path}")
        print(f"File size: {output_path.stat().st_size:,} bytes")
        if errors:
            print(f"Errors encountered: {len(errors)}")


if __name__ == '__main__':
    asyncio.run(extract_daily_report_details())
