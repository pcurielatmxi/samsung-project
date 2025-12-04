#!/usr/bin/env python3
"""
Test ProjectSight login with verification code support.
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.extractors.system_specific.projectsight_extractor import ProjectSightExtractor

def test_login_with_verification():
    """Test login flow with verification code handling."""
    print("=" * 70)
    print("Testing ProjectSight Login with Verification Code Support")
    print("=" * 70)
    print()
    print("NOTE: A browser window will open.")
    print("      If a verification code is required, you'll need to:")
    print("      1. Check your email/phone for the code")
    print("      2. Enter it in the browser window")
    print("      3. The script will wait up to 5 minutes")
    print()
    print("=" * 70)

    try:
        # Initialize extractor
        print("\n[1/4] Initializing ProjectSight extractor...")
        extractor = ProjectSightExtractor()

        # Test authentication (this will handle verification code if needed)
        print("\n[2/4] Attempting to authenticate...")
        print("      (Browser window will open)")

        if extractor.connector.authenticate():
            print("\n✅ Authentication successful!")

            # Check current URL
            current_url = extractor.connector.page.url
            print(f"\n[3/4] Current URL: {current_url}")

            # Verify we're on the right page
            if '/web/app/Projects' in current_url:
                print("✅ Successfully logged into ProjectSight Projects page!")

                # Take screenshot
                screenshot_path = '/tmp/projectsight_logged_in.png'
                extractor.connector.take_screenshot(screenshot_path)
                print(f"\n[4/4] Screenshot saved: {screenshot_path}")

                print("\n" + "=" * 70)
                print("✅ LOGIN TEST PASSED")
                print("=" * 70)
                print("\n✅ You can now proceed with data extraction!")
                return True
            else:
                print(f"⚠️  Logged in but not on Projects page: {current_url}")
                return False
        else:
            print("\n❌ Authentication failed!")
            return False

    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
        return False
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Cleanup
        try:
            print("\nClosing browser...")
            extractor.connector.close()
        except:
            pass

if __name__ == '__main__':
    success = test_login_with_verification()

    if success:
        print("\n🎉 Login system is ready!")
        print("\nNext steps:")
        print("  1. ✅ Login works (with verification code support)")
        print("  2. → Implement project list extraction")
        print("  3. → Implement project detail extraction")
        print("  4. → Implement dashboard metrics extraction")
    else:
        print("\n❌ Login test failed - check errors above")

    sys.exit(0 if success else 1)
