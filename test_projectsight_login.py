#!/usr/bin/env python3
"""
Quick test script to verify ProjectSight login functionality.
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.extractors.system_specific.projectsight_extractor import ProjectSightExtractor

def test_login():
    """Test the login flow."""
    print("=" * 60)
    print("Testing ProjectSight Login Flow")
    print("=" * 60)

    try:
        # Initialize extractor
        print("\n1. Initializing ProjectSight extractor...")
        extractor = ProjectSightExtractor()

        # Test authentication
        print("\n2. Testing authentication...")
        if extractor.connector.authenticate():
            print("   ✅ Authentication successful!")

            # Check current URL
            current_url = extractor.connector.page.url
            print(f"   Current URL: {current_url}")

            # Take screenshot
            screenshot_path = '/tmp/test_login_success.png'
            extractor.connector.take_screenshot(screenshot_path)
            print(f"   Screenshot saved: {screenshot_path}")

            # Try to extract projects (basic test)
            print("\n3. Testing basic extraction...")
            projects = extractor.extract(debug=True)
            print(f"   ✅ Found {len(projects)} projects")

            if projects:
                print("\n   Project data:")
                for i, project in enumerate(projects, 1):
                    print(f"   {i}. {project}")

            return True
        else:
            print("   ❌ Authentication failed!")
            return False

    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Cleanup
        try:
            extractor.connector.close()
            print("\n4. Browser closed")
        except:
            pass

if __name__ == '__main__':
    success = test_login()
    print("\n" + "=" * 60)
    if success:
        print("✅ LOGIN TEST PASSED")
        print("=" * 60)
        print("\nNext steps:")
        print("1. Implement list view iteration")
        print("2. Implement detail page data extraction")
        print("3. Implement iframe data extraction")
    else:
        print("❌ LOGIN TEST FAILED")
        print("=" * 60)
        print("\nCheck the error messages above for details")

    sys.exit(0 if success else 1)
