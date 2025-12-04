#!/usr/bin/env python3
"""Debug login test with detailed logging."""
import sys
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Set up detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

from src.extractors.system_specific.projectsight_extractor import ProjectSightExtractor

def test_login_debug():
    """Test login with detailed debugging."""
    print("=" * 70)
    print("ProjectSight Login Debug Test")
    print("=" * 70)

    extractor = None
    try:
        print("\n[1] Initializing extractor...")
        extractor = ProjectSightExtractor()
        print("    ✅ Extractor initialized")

        print("\n[2] Starting authentication...")
        result = extractor.connector.authenticate()
        print(f"    Authentication result: {result}")

        if result:
            print("\n[3] ✅ LOGIN SUCCESSFUL!")

            # Check current state
            if extractor.connector.page:
                try:
                    print(f"    Current URL: {extractor.connector.page.url}")
                except:
                    print("    Could not get URL (page may have crashed after login)")

                try:
                    print(f"    Page title: {extractor.connector.page.title()}")
                except:
                    print("    Could not get page title (page may have crashed after login)")

                try:
                    # Take screenshot
                    screenshot_path = '/tmp/login_success.png'
                    extractor.connector.take_screenshot(screenshot_path)
                    print(f"    Screenshot saved: {screenshot_path}")
                except:
                    print("    Could not save screenshot (page may have crashed after login)")

            return True
        else:
            print("\n[3] ❌ LOGIN FAILED")

            # Try to get current state
            if extractor.connector.page:
                try:
                    print(f"    Current URL: {extractor.connector.page.url}")
                    print(f"    Page title: {extractor.connector.page.title()}")

                    # Take screenshot for debugging
                    screenshot_path = '/tmp/login_failed.png'
                    extractor.connector.take_screenshot(screenshot_path)
                    print(f"    Screenshot saved: {screenshot_path}")
                except Exception as e:
                    print(f"    Could not get page state: {e}")

            return False

    except Exception as e:
        print(f"\n❌ EXCEPTION: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        if extractor and extractor.connector:
            try:
                print("\n[4] Closing browser...")
                extractor.connector.close()
                print("    ✅ Browser closed")
            except Exception as e:
                print(f"    ⚠️  Error closing browser: {e}")

if __name__ == '__main__':
    success = test_login_debug()

    print("\n" + "=" * 70)
    if success:
        print("✅ TEST PASSED")
    else:
        print("❌ TEST FAILED")
    print("=" * 70)

    sys.exit(0 if success else 1)
