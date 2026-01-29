#!/usr/bin/env python3
"""
PSI consolidation is now combined with RABA.

Output: processed/raba/raba_psi_consolidated.csv

This script redirects to the combined consolidation script.
"""

import sys
from pathlib import Path

# Add project root to path for imports
_project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(_project_root))


def main():
    print("=" * 60)
    print("PSI CONSOLIDATION MOVED")
    print("=" * 60)
    print()
    print("PSI data is now consolidated together with RABA.")
    print()
    print("Running combined consolidation...")
    print()

    # Import and run the combined consolidation
    from scripts.raba.document_processing.consolidate import consolidate
    consolidate()


if __name__ == "__main__":
    main()
