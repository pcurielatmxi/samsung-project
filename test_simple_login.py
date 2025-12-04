#!/usr/bin/env python3
"""
Simplified login test to debug the issue.
"""
import sys
import time
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from playwright.sync_api import sync_playwright
from src.config.settings import settings

def test_manual_login():
    """Test login with detailed logging."""
    print("=" * 60)
    print("Manual Login Test with Detailed Logging")
    print("=" * 60)

    playwright = None
    browser = None

    try:
        # Start Playwright
        print("\n1. Starting Playwright...")
        playwright = sync_playwright().start()

        # Launch browser with stability flags
        print("2. Launching browser...")
        browser = playwright.chromium.launch(
            headless=True,
            args=[
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-gpu',
            ]
        )

        # Create context and page
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            ignore_https_errors=True,
        )
        page = context.new_page()
        page.set_default_timeout(30000)

        # Navigate to ProjectSight
        print(f"\n3. Navigating to ProjectSight: {settings.PROJECTSIGHT_BASE_URL}")
        page.goto(settings.PROJECTSIGHT_BASE_URL)
        time.sleep(3)

        print(f"   Current URL after navigation: {page.url}")

        # Fill username
        print(f"\n4. Filling username: {settings.PROJECTSIGHT_USERNAME}")
        page.get_by_role('textbox', name='Email or username').fill(settings.PROJECTSIGHT_USERNAME)
        print("   Username filled successfully")

        # Submit username
        print("5. Submitting username (pressing Enter)...")
        page.keyboard.press('Enter')
        time.sleep(3)

        print(f"   Current URL after username: {page.url}")

        # Fill password
        print("\n6. Filling password...")
        page.get_by_role('textbox', name='Password').fill(settings.PROJECTSIGHT_PASSWORD)
        print("   Password filled successfully")

        # Take screenshot before submit
        page.screenshot(path='/tmp/before_password_submit.png')
        print("   Screenshot saved: /tmp/before_password_submit.png")

        # Submit password
        print("7. Submitting password (pressing Enter)...")
        page.keyboard.press('Enter')
        time.sleep(5)

        print(f"   Current URL after password: {page.url}")

        # Check if we're on Projects page
        if '/web/app/Projects' in page.url:
            print("\n✅ SUCCESS! Logged in to ProjectSight")
            page.screenshot(path='/tmp/login_success.png')
            print("   Screenshot saved: /tmp/login_success.png")
            return True
        else:
            print(f"\n❌ FAILED! Still on login page: {page.url}")
            page.screenshot(path='/tmp/login_failed.png')
            print("   Screenshot saved: /tmp/login_failed.png")

            # Try to get any error messages
            try:
                error_text = page.text_content('body')
                if 'error' in error_text.lower() or 'invalid' in error_text.lower():
                    print(f"\n   Possible error on page")
            except:
                pass

            return False

    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        # Cleanup
        try:
            if browser:
                browser.close()
            if playwright:
                playwright.stop()
            print("\n8. Browser closed")
        except:
            pass

if __name__ == '__main__':
    success = test_manual_login()
    print("\n" + "=" * 60)
    print("✅ TEST PASSED" if success else "❌ TEST FAILED")
    print("=" * 60)
    sys.exit(0 if success else 1)
