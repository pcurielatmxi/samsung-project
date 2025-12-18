#!/usr/bin/env python3
"""
Timeline Analysis Status

Shows progress of narrative analysis and findings statistics.

Usage:
    python timeline_status.py              # Summary
    python timeline_status.py --detailed   # Detailed breakdown
"""

import argparse
import csv
from collections import defaultdict
from pathlib import Path
import sys

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from src.config.settings import Settings

# Paths
MAPPING_CSV = Settings.PROCESSED_DATA_DIR / "primavera_narratives" / "narrative_xer_mapping.csv"
FINDINGS_CSV = Settings.DERIVED_DATA_DIR / "primavera_narratives" / "narrative_findings.csv"


def load_mappings() -> list[dict]:
    """Load narrative mappings."""
    with open(MAPPING_CSV, encoding='utf-8') as f:
        return list(csv.DictReader(f))


def load_findings() -> list[dict]:
    """Load findings."""
    if not FINDINGS_CSV.exists():
        return []
    with open(FINDINGS_CSV, encoding='utf-8') as f:
        return list(csv.DictReader(f))


def print_summary(mappings: list[dict], findings: list[dict]) -> None:
    """Print analysis progress summary."""
    # Files analyzed
    analyzed_files = set(f['source_file'] for f in findings if f.get('source_file'))
    total_files = len(mappings)
    dated_files = len([m for m in mappings if m['narrative_date']])

    print("=" * 60)
    print("NARRATIVE TIMELINE ANALYSIS STATUS")
    print("=" * 60)
    print()

    # Progress
    print("FILE PROGRESS")
    print("-" * 40)
    print(f"Total narrative files:     {total_files:4}")
    print(f"Files with dates:          {dated_files:4}")
    print(f"Files analyzed:            {len(analyzed_files):4}")
    print(f"Remaining (dated):         {dated_files - len(analyzed_files):4}")
    if dated_files > 0:
        pct = len(analyzed_files) / dated_files * 100
        print(f"Progress:                  {pct:5.1f}%")
    print()

    # Findings
    print("FINDINGS SUMMARY")
    print("-" * 40)
    print(f"Total findings:            {len(findings):4}")
    if len(analyzed_files) > 0:
        avg = len(findings) / len(analyzed_files)
        print(f"Avg findings per file:     {avg:5.1f}")
    print()

    if findings:
        # By category
        by_category = defaultdict(int)
        for f in findings:
            by_category[f.get('category', 'UNKNOWN')] += 1

        print("BY CATEGORY")
        print("-" * 40)
        for cat in sorted(by_category.keys()):
            print(f"  {cat:20}: {by_category[cat]:4}")
        print()

        # By responsible party
        by_party = defaultdict(int)
        for f in findings:
            by_party[f.get('responsible_party', 'UNKNOWN')] += 1

        print("BY RESPONSIBLE PARTY")
        print("-" * 40)
        for party in sorted(by_party.keys()):
            print(f"  {party:20}: {by_party[party]:4}")
        print()

        # By impact type
        by_impact = defaultdict(int)
        for f in findings:
            by_impact[f.get('impact_type', 'UNKNOWN')] += 1

        print("BY IMPACT TYPE")
        print("-" * 40)
        for impact in sorted(by_impact.keys()):
            print(f"  {impact:20}: {by_impact[impact]:4}")

    print()
    print("=" * 60)


def print_detailed(mappings: list[dict], findings: list[dict]) -> None:
    """Print detailed breakdown by folder and date range."""
    analyzed_files = set(f['source_file'] for f in findings if f.get('source_file'))

    # By subfolder
    by_folder = defaultdict(lambda: {'total': 0, 'analyzed': 0, 'findings': 0})

    for m in mappings:
        folder = m.get('subfolder', '') or '(root)'
        by_folder[folder]['total'] += 1
        if m['narrative_file'] in analyzed_files:
            by_folder[folder]['analyzed'] += 1

    for f in findings:
        folder = f.get('subfolder', '') or '(root)'
        by_folder[folder]['findings'] += 1

    print("\nBY FOLDER")
    print("-" * 70)
    print(f"{'Folder':<40} {'Total':>6} {'Done':>6} {'Findings':>8}")
    print("-" * 70)
    for folder in sorted(by_folder.keys()):
        stats = by_folder[folder]
        print(f"{folder[:40]:<40} {stats['total']:>6} {stats['analyzed']:>6} {stats['findings']:>8}")

    # Date range coverage
    dated = [m for m in mappings if m['narrative_date']]
    if dated:
        dates = sorted(m['narrative_date'] for m in dated)
        analyzed_dates = sorted(m['narrative_date'] for m in dated if m['narrative_file'] in analyzed_files)

        print("\nDATE RANGE")
        print("-" * 40)
        print(f"Earliest file:   {dates[0]}")
        print(f"Latest file:     {dates[-1]}")
        if analyzed_dates:
            print(f"Analyzed from:   {analyzed_dates[0]}")
            print(f"Analyzed to:     {analyzed_dates[-1]}")

    # Recent findings
    if findings:
        print("\nRECENT FINDINGS (last 10)")
        print("-" * 70)
        for f in findings[-10:]:
            date = f.get('source_date', '?')[:10]
            cat = f.get('category', '?')[:15]
            desc = f.get('description', '')[:40]
            print(f"{date}  {cat:<15}  {desc}")


def main():
    parser = argparse.ArgumentParser(description='Show narrative analysis status')
    parser.add_argument('--detailed', '-d', action='store_true', help='Show detailed breakdown')
    args = parser.parse_args()

    mappings = load_mappings()
    findings = load_findings()

    print_summary(mappings, findings)

    if args.detailed:
        print_detailed(mappings, findings)


if __name__ == '__main__':
    main()
