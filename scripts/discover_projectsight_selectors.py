"""Interactive selector discovery for ProjectSight."""
import sys
from pathlib import Path
import time

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config.settings import settings
from src.connectors.web_scraper import WebScraperConnector

def main():
    print("=" * 60)
    print("ProjectSight Selector Discovery Tool")
    print("=" * 60)
    print("\nThis tool will help you find the correct CSS selectors")
    print("for ProjectSight's login form and home screen.")
    print("\n⚠️  Make sure PROJECTSIGHT_HEADLESS=false in your .env file!")
    print("\nPress Enter to start...")
    input()

    # Initialize connector (non-headless for inspection)
    connector = WebScraperConnector(
        name='ProjectSight',
        base_url=settings.PROJECTSIGHT_BASE_URL,
        username=settings.PROJECTSIGHT_USERNAME,
        password=settings.PROJECTSIGHT_PASSWORD,
        timeout=settings.PROJECTSIGHT_TIMEOUT,
        headless=False,  # Force non-headless
    )

    # Initialize browser
    print("\nInitializing browser...")
    if not connector.authenticate():
        print("❌ Failed to initialize browser")
        return 1

    # Navigate to login page
    login_url = input("\nEnter the ProjectSight login URL (or press Enter for default): ").strip()
    if not login_url:
        login_url = settings.PROJECTSIGHT_BASE_URL

    print(f"\nNavigating to {login_url}...")
    connector.navigate_to(login_url)
    time.sleep(2)

    print("\n" + "=" * 60)
    print("STEP 1: Username Field")
    print("=" * 60)
    print("Right-click the username field → Inspect")
    print("Find a unique selector (id, name, or CSS class)")
    print("\nExamples:")
    print("  - input#username")
    print("  - input[name='username']")
    print("  - input.login-username")
    username_selector = input("\nEnter username field selector: ").strip()

    print("\n" + "=" * 60)
    print("STEP 2: Password Field")
    print("=" * 60)
    password_selector = input("Enter password field selector: ").strip()

    print("\n" + "=" * 60)
    print("STEP 3: Submit Button")
    print("=" * 60)
    submit_selector = input("Enter submit button selector: ").strip()

    print("\n" + "=" * 60)
    print("STEP 4: Test Login")
    print("=" * 60)
    print("Testing selectors by filling the form...")

    try:
        connector.send_keys(selector=username_selector, text=settings.PROJECTSIGHT_USERNAME)
        print(f"✅ Username field filled")

        connector.send_keys(selector=password_selector, text=settings.PROJECTSIGHT_PASSWORD)
        print(f"✅ Password field filled")

        print("\nPress Enter to submit the form...")
        input()

        connector.click_element(submit_selector)
        print(f"✅ Form submitted")

        time.sleep(5)  # Wait for redirect

    except Exception as e:
        print(f"❌ Error: {e}")
        connector.close()
        return 1

    print("\n" + "=" * 60)
    print("STEP 5: MFA Input (if applicable)")
    print("=" * 60)
    print("If MFA prompt appeared, find the MFA code input field")
    print("Otherwise, press Enter to skip")
    mfa_selector = input("Enter MFA input selector (or press Enter): ").strip()
    if not mfa_selector:
        mfa_selector = "input[name='mfa_code']"  # Default

    print("\n" + "=" * 60)
    print("STEP 6: Home Screen Indicator")
    print("=" * 60)
    print("Find a unique element that only appears after successful login")
    print("Examples: .dashboard, #main-nav, .project-list")
    home_selector = input("Enter home screen indicator selector: ").strip()

    # Test home screen selector
    try:
        print(f"\nTesting home screen selector...")
        if connector.wait_for_selector(home_selector, timeout=5):
            print(f"✅ Home screen indicator found!")
        else:
            print(f"⚠️  Home screen indicator not found (might still work)")
    except:
        print(f"⚠️  Could not verify home screen indicator")

    # Generate .env configuration
    print("\n" + "=" * 60)
    print("DISCOVERED SELECTORS")
    print("=" * 60)
    print("\nAdd these to your .env file:\n")
    print(f"PROJECTSIGHT_LOGIN_URL={login_url}")
    print(f"PROJECTSIGHT_SELECTOR_USERNAME={username_selector}")
    print(f"PROJECTSIGHT_SELECTOR_PASSWORD={password_selector}")
    print(f"PROJECTSIGHT_SELECTOR_SUBMIT={submit_selector}")
    print(f"PROJECTSIGHT_SELECTOR_MFA_INPUT={mfa_selector}")
    print(f"PROJECTSIGHT_SELECTOR_HOME_INDICATOR={home_selector}")

    print("\n" + "=" * 60)

    connector.close()
    return 0

if __name__ == '__main__':
    sys.exit(main())
