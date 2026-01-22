#!/usr/bin/env python3
"""
Fix: Company Alias Duplicates
Source: integrated_analysis/mappings
Type: One-time
Status: DEPRECATED (2026-01-22)
Date Created: 2026-01-20
Last Applied: 2026-01-20

DEPRECATION NOTICE:
    This script is DEPRECATED. Duplicate alias prevention is now built into the
    generation script (build_company_dimension.py).

    To fix duplicate aliases:
    1. Edit COMPANIES list in scripts/integrated_analysis/dimensions/build_company_dimension.py
    2. Remove the duplicate alias from the wrong company's aliases list
    3. Re-run: python -m scripts.integrated_analysis.dimensions.build_company_dimension

    The generation script will fail if duplicates are detected, forcing you to
    fix them at the source rather than patching the output CSV.

Original Issue:
    Some aliases in map_company_aliases.csv map to multiple companies, causing
    Power BI relationship errors (many-to-many not allowed in some contexts).

Original Fix Logic:
    Removed specific known duplicate mappings based on manual review:
    - ABR: Remove mapping to company_id=0 (Unknown), keep mapping to 35 (Austin Bridge & Road)
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config.settings import Settings


# Known duplicates and their resolutions
# Format: (alias, company_id_to_remove, reason)
DUPLICATE_FIXES = [
    ('ABR', 0, 'ABR is abbreviation for Austin Bridge & Road (company_id=35), not Unknown'),
]


def find_duplicates() -> pd.DataFrame:
    """
    Find aliases that map to multiple companies.

    Returns:
        DataFrame with duplicate aliases and their mappings
    """
    aliases_path = Settings.PROCESSED_DATA_DIR / 'integrated_analysis' / 'mappings' / 'map_company_aliases.csv'
    aliases = pd.read_csv(aliases_path)

    # Find aliases mapping to multiple companies
    alias_counts = aliases.groupby('alias')['company_id'].nunique()
    duplicates = alias_counts[alias_counts > 1].index.tolist()

    if not duplicates:
        return pd.DataFrame()

    return aliases[aliases['alias'].isin(duplicates)]


def fix_duplicates(dry_run: bool = True) -> int:
    """
    Apply known duplicate fixes.

    Args:
        dry_run: If True, don't save changes

    Returns:
        Number of rows removed
    """
    aliases_path = Settings.PROCESSED_DATA_DIR / 'integrated_analysis' / 'mappings' / 'map_company_aliases.csv'
    aliases = pd.read_csv(aliases_path)

    original_count = len(aliases)
    rows_removed = 0

    for alias, company_id_to_remove, reason in DUPLICATE_FIXES:
        # Check if this duplicate exists
        mask = (aliases['alias'] == alias) & (aliases['company_id'] == company_id_to_remove)
        matches = aliases[mask]

        if len(matches) == 0:
            print(f"  [SKIP] {alias} → {company_id_to_remove}: not found (already fixed)")
            continue

        print(f"  [FIX] Removing: {alias} → company_id={company_id_to_remove}")
        print(f"        Reason: {reason}")

        if not dry_run:
            aliases = aliases[~mask]
            rows_removed += len(matches)

    if dry_run:
        print(f"\n[DRY RUN] Would remove {len([f for f in DUPLICATE_FIXES if (aliases['alias'] == f[0]).any()])} duplicate mappings")
        print("Run with --apply to save changes")
    else:
        aliases.to_csv(aliases_path, index=False)
        print(f"\nRemoved {rows_removed} duplicate mappings")
        print(f"Updated: {aliases_path}")
        print(f"Rows: {original_count} → {len(aliases)}")

    return rows_removed


def main():
    parser = argparse.ArgumentParser(
        description='Fix duplicate aliases in map_company_aliases.csv'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        default=True,
        help='Preview changes without saving (default)'
    )
    parser.add_argument(
        '--apply',
        action='store_true',
        help='Apply fixes to map_company_aliases.csv'
    )
    args = parser.parse_args()

    # Deprecation warning
    print("=" * 70)
    print("⚠️  DEPRECATION WARNING")
    print("=" * 70)
    print("This script is DEPRECATED as of 2026-01-22.")
    print()
    print("Duplicate alias prevention is now built into the generation script.")
    print("To fix duplicates:")
    print("  1. Edit COMPANIES in scripts/integrated_analysis/dimensions/build_company_dimension.py")
    print("  2. Re-run: python -m scripts.integrated_analysis.dimensions.build_company_dimension")
    print("=" * 70)
    print()

    print("Checking for duplicate aliases...")
    duplicates = find_duplicates()

    if len(duplicates) == 0:
        print("No duplicate aliases found!")
        return

    print(f"\nFound {len(duplicates)} rows with duplicate alias mappings:")
    for alias in duplicates['alias'].unique():
        rows = duplicates[duplicates['alias'] == alias]
        print(f"\n  '{alias}' maps to {len(rows)} companies:")
        for _, row in rows.iterrows():
            print(f"    [{row['source']}] company_id={row['company_id']}")

    print("\nApplying known fixes...")
    dry_run = not args.apply
    fix_duplicates(dry_run=dry_run)


if __name__ == '__main__':
    main()
