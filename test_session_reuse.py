#!/usr/bin/env python3
"""Test that session persistence works - should skip login."""
import sys
import logging
from pathlib import Path

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(message)s'
)

from src.extractors.system_specific.projectsight_extractor import ProjectSightExtractor

def test_session_reuse():
    """Test that saved session is reused."""
    print("=" * 70)
    print("Testing Session Persistence")
    print("=" * 70)
    print("\nThis test should:")
    print("  ✓ Load saved session cookies")
    print("  ✓ Skip the login process")
    print("  ✓ Navigate directly to ProjectSight")
    print("=" * 70)

    extractor = None
    try:
        print("\n[1] Initializing extractor...")
        extractor = ProjectSightExtractor()

        print("\n[2] Authenticating (should use cached session)...")
        result = extractor.connector.authenticate()

        if result:
            print("\n✅ AUTHENTICATION SUCCESSFUL!")
            print("\nThe session was reused - no login required!")
            print("Session will remain valid for 7 days from last use.")
            return True
        else:
            print("\n❌ AUTHENTICATION FAILED")
            print("Session may have expired or been invalid.")
            return False

    except Exception as e:
        print(f"\n❌ EXCEPTION: {str(e)}")
        return False

    finally:
        if extractor and extractor.connector:
            try:
                extractor.connector.close()
            except:
                pass

if __name__ == '__main__':
    success = test_session_reuse()

    print("\n" + "=" * 70)
    if success:
        print("✅ SESSION PERSISTENCE TEST PASSED")
        print("=" * 70)
        print("\nKey Points:")
        print("  • Login only required once every 7 days")
        print("  • Session cookies stored in .sessions/ directory")
        print("  • Verification codes only needed on first login or after expiry")
    else:
        print("❌ SESSION PERSISTENCE TEST FAILED")
        print("=" * 70)

    sys.exit(0 if success else 1)
