#!/usr/bin/env python3
"""
Fix: PSI Outcome Misclassification
Source: psi
Type: One-time
Status: Prompts updated, awaiting re-extraction
Date Created: 2026-01-21
Last Applied: 2026-01-21

Issue:
    ~30% of PSI records have incorrect outcome classifications. Records where work
    was "not ready" were marked as FAIL instead of CANCELLED.

Root Cause:
    Extract prompt didn't clearly distinguish CANCELLED from FAIL.

Fix Logic:
    Uses regex patterns to detect misclassified records:
    - FAIL → CANCELLED: 11 patterns (not ready, cancelled, no access, etc.)
    - PARTIAL → PASS: 10 patterns (accepted, no deficiencies, in compliance, etc.)
    Checks if issues were resolved before reclassifying PARTIAL → PASS.

Usage:
    # Dry run - show what would be changed
    python -m scripts.psi.document_processing.fix_psi_outcomes --dry-run

    # Apply fixes (creates .csv.bak backup)
    python -m scripts.psi.document_processing.fix_psi_outcomes --apply

Output Columns Modified:
    - outcome: Changed from FAIL/PARTIAL to CANCELLED/PASS where detected
"""

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

# Add project root to path
_project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import Settings


# Patterns for detecting CANCELLED (from FAIL)
CANCELLED_PATTERNS = [
    r"not\s+ready",
    r"work\s+was\s+not\s+ready",
    r"site\s+was\s+not\s+ready",
    r"area\s+was\s+not\s+ready",
    r"did\s+not\s+pass\s+internal",
    r"not\s+pass\s+internal",
    r"failed\s+internal\s+inspection",
    r"inspection\s+was\s+cancell?ed",
    r"cancell?ed\s+due\s+to",
    r"work\s+has\s+been\s+cancell?ed",
    r"could\s+not\s+be\s+inspected",
    r"inspection\s+did\s+not\s+occur",
    r"no\s+access",
    r"unable\s+to\s+access",
]

# Patterns suggesting PASS (from PARTIAL)
PASS_INDICATORS = [
    r"accepted",
    r"passed",
    r"no\s+deficienc",
    r"no\s+issues?\s+noted",
    r"no\s+issues?\s+found",
    r"in\s+compliance",
    r"meets?\s+requirements",
    r"items?\s+.*were\s+accepted",
    r"reinspection\s+.*passed",
    r"re-inspection\s+.*passed",
]


def load_extract_content(inspection_id: str) -> str:
    """Load the extract stage content for a record."""
    # PSI extract files use the DFR ID
    extract_file = Settings.PSI_PROCESSED_DIR / "1.extract" / f"{inspection_id}.extract.json"
    if extract_file.exists():
        with open(extract_file) as f:
            data = json.load(f)
        return data.get("content", "")
    return ""


def check_patterns(text: str, patterns: List[str]) -> List[str]:
    """Check which patterns match in the text."""
    text_lower = text.lower()
    matches = []
    for pattern in patterns:
        if re.search(pattern, text_lower):
            matches.append(pattern)
    return matches


def detect_cancelled(row: pd.Series, extract_content: str = "") -> Tuple[bool, List[str]]:
    """
    Detect if a FAIL record should be CANCELLED.
    """
    if row["outcome"] != "FAIL":
        return False, []

    # Check summary
    summary = str(row.get("summary", ""))
    matches = check_patterns(summary, CANCELLED_PATTERNS)

    # Check failure reason
    failure_reason = str(row.get("failure_reason", ""))
    matches.extend(check_patterns(failure_reason, CANCELLED_PATTERNS))

    # Check extract content
    if extract_content:
        matches.extend(check_patterns(extract_content, CANCELLED_PATTERNS))

    return len(matches) > 0, list(set(matches))


def detect_implied_pass(row: pd.Series, extract_content: str = "") -> Tuple[bool, List[str]]:
    """
    Detect if a PARTIAL record should be PASS.
    """
    if row["outcome"] != "PARTIAL":
        return False, []

    # Don't reclassify if there are unresolved issues
    if row.get("issue_count", 0) > 0:
        # Check if issues were resolved
        summary = str(row.get("summary", ""))
        if not any(re.search(p, summary.lower()) for p in [r"resolved", r"corrected", r"fixed", r"accepted"]):
            return False, []

    summary = str(row.get("summary", ""))
    matches = check_patterns(summary, PASS_INDICATORS)

    if extract_content:
        matches.extend(check_patterns(extract_content, PASS_INDICATORS))

    return len(matches) > 0, list(set(matches))


def analyze_records(df: pd.DataFrame, use_extracts: bool = True) -> Dict[str, List[Dict]]:
    """Analyze all records and identify misclassifications."""
    fixes = {
        "FAIL_to_CANCELLED": [],
        "PARTIAL_to_PASS": [],
    }

    for idx, row in df.iterrows():
        inspection_id = row["inspection_id"]

        extract_content = ""
        if use_extracts:
            extract_content = load_extract_content(inspection_id)

        # Check for FAIL → CANCELLED
        should_cancel, cancel_reasons = detect_cancelled(row, extract_content)
        if should_cancel:
            fixes["FAIL_to_CANCELLED"].append({
                "inspection_id": inspection_id,
                "current_outcome": "FAIL",
                "new_outcome": "CANCELLED",
                "reasons": cancel_reasons,
                "inspection_type": row.get("inspection_type"),
                "summary": str(row.get("summary", ""))[:200],
            })
            continue

        # Check for PARTIAL → PASS
        should_pass, pass_reasons = detect_implied_pass(row, extract_content)
        if should_pass:
            fixes["PARTIAL_to_PASS"].append({
                "inspection_id": inspection_id,
                "current_outcome": "PARTIAL",
                "new_outcome": "PASS",
                "reasons": pass_reasons,
                "inspection_type": row.get("inspection_type"),
                "summary": str(row.get("summary", ""))[:200],
            })

    return fixes


def apply_fixes(df: pd.DataFrame, fixes: Dict[str, List[Dict]]) -> pd.DataFrame:
    """Apply the identified fixes to the dataframe."""
    df = df.copy()

    changes = {}
    for fix_type, records in fixes.items():
        for record in records:
            changes[record["inspection_id"]] = record["new_outcome"]

    for idx, row in df.iterrows():
        if row["inspection_id"] in changes:
            df.at[idx, "outcome"] = changes[row["inspection_id"]]

    return df


def print_summary(fixes: Dict[str, List[Dict]]):
    """Print a summary of identified fixes."""
    print("\n" + "=" * 60)
    print("OUTCOME FIX ANALYSIS")
    print("=" * 60)

    total = sum(len(v) for v in fixes.values())
    print(f"\nTotal records to fix: {total}")

    for fix_type, records in fixes.items():
        if records:
            print(f"\n{fix_type}: {len(records)} records")
            print("-" * 40)
            for r in records[:5]:
                print(f"  {r['inspection_id']}: {r['inspection_type'][:50] if r['inspection_type'] else 'N/A'}...")
                print(f"    Reasons: {', '.join(r['reasons'][:3])}")
            if len(records) > 5:
                print(f"  ... and {len(records) - 5} more")


def main():
    parser = argparse.ArgumentParser(
        description="Fix PSI outcome misclassifications"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be changed without applying"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the fixes to the consolidated CSV"
    )
    parser.add_argument(
        "--no-extracts",
        action="store_true",
        help="Only use summary/failure_reason fields"
    )
    parser.add_argument(
        "--output",
        help="Output file for fix report (JSON)"
    )

    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        print("Specify --dry-run or --apply")
        sys.exit(1)

    use_extracts = not args.no_extracts

    # Load data
    csv_path = Settings.PSI_PROCESSED_DIR / "psi_consolidated.csv"
    print(f"Loading: {csv_path}")
    df = pd.read_csv(csv_path)
    print(f"Total records: {len(df)}")

    # Analyze
    print(f"\nAnalyzing records (use_extracts={use_extracts})...")
    fixes = analyze_records(df, use_extracts=use_extracts)

    # Print summary
    print_summary(fixes)

    # Save report if requested
    if args.output:
        with open(args.output, "w") as f:
            json.dump(fixes, f, indent=2)
        print(f"\nFix report saved to: {args.output}")

    # Apply fixes if requested
    if args.apply:
        total = sum(len(v) for v in fixes.values())
        if total == 0:
            print("\nNo fixes to apply.")
            return

        print(f"\nApplying {total} fixes...")
        df_fixed = apply_fixes(df, fixes)

        # Backup original
        backup_path = csv_path.with_suffix(".csv.bak")
        df.to_csv(backup_path, index=False)
        print(f"Backup saved to: {backup_path}")

        # Write fixed
        df_fixed.to_csv(csv_path, index=False)
        print(f"Fixed CSV saved to: {csv_path}")

        # Show outcome distribution change
        print("\nOutcome distribution (before → after):")
        before = df["outcome"].value_counts()
        after = df_fixed["outcome"].value_counts()
        all_outcomes = set(before.index) | set(after.index)
        for outcome in sorted(all_outcomes):
            b = before.get(outcome, 0)
            a = after.get(outcome, 0)
            diff = a - b
            sign = "+" if diff > 0 else ""
            print(f"  {outcome}: {b} → {a} ({sign}{diff})")


if __name__ == "__main__":
    main()
