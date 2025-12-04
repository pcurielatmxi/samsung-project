"""Test ProjectSight login flow with MFA and session persistence."""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.extractors.system_specific.projectsight_extractor import ProjectSightExtractor

def main():
    print("=" * 60)
    print("ProjectSight Login Test")
    print("=" * 60)

    extractor = ProjectSightExtractor()

    # Test authentication
    print("\n[1/2] Testing authentication...")
    success = extractor.connector.authenticate()

    if success:
        print("✅ Authentication successful!")
        print(f"📍 Current URL: {extractor.connector.page.url}")

        # Take screenshot of home screen
        screenshot_path = Path(__file__).parent.parent / 'data' / 'output' / 'home_screen.png'
        screenshot_path.parent.mkdir(parents=True, exist_ok=True)
        extractor.connector.take_screenshot(str(screenshot_path))
        print(f"📸 Screenshot saved to: {screenshot_path}")
    else:
        print("❌ Authentication failed!")
        return 1

    print("\n[2/2] Cleaning up...")
    extractor.connector.close()
    print("✅ Browser closed")

    print("\n" + "=" * 60)
    print("Test completed successfully!")
    print("=" * 60)
    return 0

if __name__ == '__main__':
    sys.exit(main())
