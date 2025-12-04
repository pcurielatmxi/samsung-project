#!/usr/bin/env python3
"""Basic browser test to diagnose Playwright issues."""
from playwright.sync_api import sync_playwright
import sys

def test_basic_browser():
    """Test if browser can launch and navigate."""
    print("Testing basic Playwright browser launch...")

    playwright = None
    browser = None

    try:
        playwright = sync_playwright().start()
        print("✅ Playwright started")

        # Launch with container-friendly args
        browser = playwright.chromium.launch(
            headless=True,
            args=[
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-gpu',
                '--disable-software-rasterizer',
                '--disable-dev-tools',
            ]
        )
        print("✅ Browser launched")

        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            ignore_https_errors=True,
        )
        print("✅ Browser context created")

        page = context.new_page()
        print("✅ Page created")

        # Try to navigate to a simple page
        print("\nNavigating to example.com...")
        page.goto("https://example.com", timeout=30000)
        print(f"✅ Navigation successful: {page.url}")

        title = page.title()
        print(f"✅ Page title: {title}")

        # Try to navigate to ProjectSight
        print("\nNavigating to ProjectSight...")
        page.goto("https://prod.projectsightapp.trimble.com/", timeout=30000)
        print(f"✅ Navigation successful: {page.url}")

        title = page.title()
        print(f"✅ Page title: {title}")

        print("\n" + "="*60)
        print("✅ BROWSER TEST PASSED - Browser is working correctly!")
        print("="*60)
        return True

    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        if browser:
            browser.close()
            print("\n🔒 Browser closed")
        if playwright:
            playwright.stop()
            print("🔒 Playwright stopped")

if __name__ == '__main__':
    success = test_basic_browser()
    sys.exit(0 if success else 1)
