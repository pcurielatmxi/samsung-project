#!/usr/bin/env python3
"""
Verify data path configuration and optionally initialize directories.

Usage:
    python scripts/check_paths.py           # Check configuration
    python scripts/check_paths.py --init    # Create directories if missing
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.config.settings import settings


def main():
    """Check path configuration and optionally initialize directories."""
    import argparse

    parser = argparse.ArgumentParser(description="Check data path configuration")
    parser.add_argument(
        "--init",
        action="store_true",
        help="Create directories if they don't exist"
    )
    args = parser.parse_args()

    # Print current configuration
    settings.print_path_config()

    if args.init:
        print("\nInitializing directories...")
        settings.ensure_directories()
        print("\nDone!")
    else:
        print("\nRun with --init to create missing directories.")


if __name__ == "__main__":
    main()
