#!/usr/bin/env python3
"""
Save Daily Reports data from browser memory to JSON file.
This script retrieves data from window.__dailyReportsData in the browser.
"""

import json
import asyncio
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright


async def save_daily_reports():
    """Connect to browser and save the daily reports data."""
    output_dir = Path('/workspaces/mxi-samsung/data/extracted')
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path = output_dir / f"daily_reports_{timestamp}.json"

    async with async_playwright() as p:
        # Connect to existing browser via CDP
        try:
            browser = await p.chromium.connect_over_cdp('http://localhost:9222')
            print(f"Connected to browser")
        except Exception as e:
            print(f"Error connecting to browser: {e}")
            print("Make sure Chrome is running with --remote-debugging-port=9222")
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

        # Get the data from window.__dailyReportsData
        data = await page.evaluate('''
            () => {
                if (window.__dailyReportsData) {
                    return window.__dailyReportsData;
                }
                return null;
            }
        ''')

        if not data:
            print("No data found in window.__dailyReportsData")
            print("Make sure you ran the extraction first")
            return

        print(f"Retrieved {len(data)} daily reports")

        # Add extraction metadata
        export_data = {
            'extractedAt': datetime.now().isoformat(),
            'source': 'ProjectSight Daily Reports',
            'totalRecords': len(data),
            'records': data
        }

        # Save to file
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)

        print(f"Saved to: {output_path}")
        print(f"File size: {output_path.stat().st_size:,} bytes")


if __name__ == '__main__':
    asyncio.run(save_daily_reports())
