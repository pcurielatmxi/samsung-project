#!/usr/bin/env python3
"""Debug script to check login page structure."""
import os
import time
from pathlib import Path
import sys

# Add project root to path (scripts/projectsight/process -> project root)
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / '.env')

from playwright.sync_api import sync_playwright

daily_reports_url = (
    "https://prod.projectsightapp.trimble.com/web/app/Project"
    "?listid=-4038&orgid=4540f425-f7b5-4ad8-837d-c270d5d09490&projid=3"
)

print("Starting browser...")
with sync_playwright() as p:
    browser = p.chromium.launch(headless=False, slow_mo=100)
    context = browser.new_context(
        viewport={'width': 1920, 'height': 1080},
        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    )
    page = context.new_page()
    page.set_default_timeout(30000)

    print(f"Navigating to: {daily_reports_url}")
    page.goto(daily_reports_url, wait_until='networkidle')
    time.sleep(3)

    print(f"Current URL: {page.url}")

    # Take screenshot
    screenshot_path = Path("/tmp/login_page_debug.png")
    page.screenshot(path=str(screenshot_path))
    print(f"Screenshot saved to: {screenshot_path}")

    # Print page title
    print(f"Page title: {page.title()}")

    # Check for various input elements
    print("\nLooking for input elements...")
    inputs = page.locator('input').all()
    print(f"Found {len(inputs)} input elements")

    for i, inp in enumerate(inputs[:10]):  # First 10 inputs
        try:
            inp_type = inp.get_attribute('type')
            inp_name = inp.get_attribute('name')
            inp_id = inp.get_attribute('id')
            inp_placeholder = inp.get_attribute('placeholder')
            is_visible = inp.is_visible()
            print(f"  Input {i}: type={inp_type}, name={inp_name}, id={inp_id}, placeholder={inp_placeholder}, visible={is_visible}")
        except Exception as e:
            print(f"  Input {i}: error - {e}")

    # Check for buttons
    print("\nLooking for buttons...")
    buttons = page.locator('button').all()
    print(f"Found {len(buttons)} buttons")

    for i, btn in enumerate(buttons[:10]):
        try:
            btn_type = btn.get_attribute('type')
            btn_id = btn.get_attribute('id')
            btn_class = btn.get_attribute('class')
            btn_text = btn.text_content()[:50] if btn.text_content() else None
            is_visible = btn.is_visible()
            is_enabled = btn.is_enabled()
            print(f"  Button {i}: type={btn_type}, id={btn_id}, text='{btn_text}', visible={is_visible}, enabled={is_enabled}")
        except Exception as e:
            print(f"  Button {i}: error - {e}")

    # Check for iframes
    print("\nLooking for iframes...")
    iframes = page.locator('iframe').all()
    print(f"Found {len(iframes)} iframes")
    for i, iframe in enumerate(iframes[:5]):
        try:
            src = iframe.get_attribute('src')
            name = iframe.get_attribute('name')
            print(f"  Iframe {i}: name={name}, src={src[:80] if src else None}...")
        except Exception as e:
            print(f"  Iframe {i}: error - {e}")

    # Wait for user to see
    print("\nWaiting 10 seconds for visual inspection...")
    time.sleep(10)

    browser.close()
    print("\nDone!")
