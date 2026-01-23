#!/usr/bin/env python3
"""
Fix: RABA Outcome Misclassification
Source: raba
Type: One-time
Status: Prompts updated, awaiting re-extraction
Date Created: 2026-01-21
Last Applied: 2026-01-21

Issue:
    ~40% of RABA records have incorrect outcome classifications due to the original
    extract/format prompts only allowing PASS/FAIL/PARTIAL options.

Root Cause:
    Trip charges and observation reports were forced into wrong categories because
    CANCELLED and MEASUREMENT were not available as outcome options.

Fix Logic:
    Uses regex patterns to detect misclassified records:
    - FAIL → CANCELLED: 14 patterns (trip charge, work not ready, etc.)
    - PARTIAL → CANCELLED: Patterns in issues field (cancelled by contractor, not ready, etc.)
    - PARTIAL → MEASUREMENT: 14 patterns (pickup report, observation only, etc.)
    - PARTIAL → PASS: 9 patterns (no deficiencies, acceptable, etc.)
    Optional embeddings-based semantic search for additional detection.

Usage:
    # Dry run - show what would be changed
    python -m scripts.raba.document_processing.fix_raba_outcomes --dry-run

    # Apply fixes (creates .csv.bak backup)
    python -m scripts.raba.document_processing.fix_raba_outcomes --apply

    # Use embeddings for additional detection (slower but more thorough)
    python -m scripts.raba.document_processing.fix_raba_outcomes --apply --use-embeddings

Output Columns Modified:
    - outcome: Changed from FAIL/PARTIAL to CANCELLED/MEASUREMENT/PASS where detected
"""

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

# Add project root to path
_project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import Settings


# Patterns for detecting CANCELLED (from FAIL)
CANCELLED_PATTERNS = [
    r"trip\s*charge",
    r"inspection\s+was\s+cancell?ed",
    r"cancell?ed\s+due\s+to",
    r"did\s+not\s+pass\s+.*internal\s+inspection",
    r"not\s+pass\s+yates\s+internal",
    r"work\s+was\s+not\s+ready",
    r"site\s+was\s+not\s+ready",
    r"area\s+was\s+not\s+ready",
    r"inspection\s+did\s+not\s+occur",
    r"inspection\s+not\s+performed",
    r"could\s+not\s+be\s+inspected",
]

# Patterns for detecting CANCELLED in issues field (from PARTIAL)
# These patterns indicate inspections that were cancelled but document was marked PARTIAL
# because other inspections in the same report passed
PARTIAL_CANCELLED_PATTERNS = [
    r"cancell?ed\s+by\s+(yates|contractor|the\s+contractor)",
    r"was\s+cancell?ed",
    r"work\s+to\s+be\s+cancell?ed",
    r"not\s+ready\s+for\s+inspection",
    r"was\s+not\s+ready",
    r"wasn't\s+ready",
    r"weren't\s+ready",
    r"no\s+access",
    r"removed\s+from\s+schedule",
    r"cancel-?\s*contractor\s+not\s+ready",
    r"rescheduled",
    r"will\s+be\s+re-?scheduled",
]

# Patterns for detecting MEASUREMENT (from PARTIAL)
MEASUREMENT_PATTERNS = [
    r"pick-?up\s+report",
    r"pickup\s+report",
    r"concrete\s+pick-?up",
    r"cylinder\s+pick-?up",
    r"observation\s+report",
    r"observation\s+only",
    r"no\s+pass/?fail\s+criteria",
    r"does\s+not\s+.*pass/?fail",
    r"no\s+explicit\s+pass/?fail",
    r"without\s+pass/?fail",
    r"characterization\s+report",
    r"material\s+characterization",
    r"data\s+collected\s+but",
    r"provides\s+data\s+but\s+does\s+not",
]

# Inspection types that are inherently measurement-only
MEASUREMENT_INSPECTION_TYPES = [
    "pick-up",
    "pickup",
    "length change",
    "shrinkage",
    "observation of drilled",
    "observations of drilled",
]

# Patterns suggesting PASS (from PARTIAL) - no deficiencies noted
PASS_INDICATORS = [
    r"no\s+deficienc",
    r"no\s+issues?\s+noted",
    r"no\s+issues?\s+found",
    r"in\s+accordance\s+with",
    r"items?\s+inspected\s+.*were\s+in\s+accordance",
    r"met\s+requirements",
    r"meets?\s+specification",
    r"acceptable",
    r"approved",
]


def load_extract_content(inspection_id: str) -> Optional[str]:
    """Load the extract stage content for a record."""
    extract_file = Settings.RABA_PROCESSED_DIR / "1.extract" / f"{inspection_id}.extract.json"
    if extract_file.exists():
        with open(extract_file) as f:
            data = json.load(f)
        return data.get("content", "")
    return None


def check_patterns(text: str, patterns: List[str]) -> List[str]:
    """Check which patterns match in the text."""
    text_lower = text.lower()
    matches = []
    for pattern in patterns:
        if re.search(pattern, text_lower):
            matches.append(pattern)
    return matches


def detect_cancelled(row: pd.Series, extract_content: Optional[str] = None) -> Tuple[bool, List[str]]:
    """
    Detect if a FAIL record should be CANCELLED.

    Returns (should_change, matching_patterns)
    """
    if row["outcome"] != "FAIL":
        return False, []

    # Check summary
    summary = str(row.get("summary", ""))
    matches = check_patterns(summary, CANCELLED_PATTERNS)

    # Check failure reason
    failure_reason = str(row.get("failure_reason", ""))
    matches.extend(check_patterns(failure_reason, CANCELLED_PATTERNS))

    # Check extract content if available
    if extract_content:
        matches.extend(check_patterns(extract_content, CANCELLED_PATTERNS))

    return len(matches) > 0, list(set(matches))


def detect_partial_cancelled(row: pd.Series, extract_content: Optional[str] = None) -> Tuple[bool, List[str]]:
    """
    Detect if a PARTIAL record should be CANCELLED.

    This catches cases where a multi-inspection report has some cancelled inspections
    but was marked PARTIAL because other inspections passed. The cancellation info
    is typically in the 'issues' field.

    Returns (should_change, matching_patterns)
    """
    if row["outcome"] != "PARTIAL":
        return False, []

    matches = []

    # Check issues field (primary source for partial cancellations)
    issues = str(row.get("issues", ""))
    matches.extend(check_patterns(issues, PARTIAL_CANCELLED_PATTERNS))

    # Check summary
    summary = str(row.get("summary", ""))
    matches.extend(check_patterns(summary, PARTIAL_CANCELLED_PATTERNS))

    # Check failure reason
    failure_reason = str(row.get("failure_reason", ""))
    matches.extend(check_patterns(failure_reason, PARTIAL_CANCELLED_PATTERNS))

    # Check extract content if available
    if extract_content:
        matches.extend(check_patterns(extract_content, PARTIAL_CANCELLED_PATTERNS))

    return len(matches) > 0, list(set(matches))


def detect_measurement(row: pd.Series, extract_content: Optional[str] = None) -> Tuple[bool, List[str]]:
    """
    Detect if a PARTIAL record should be MEASUREMENT.

    Returns (should_change, matching_patterns)
    """
    if row["outcome"] != "PARTIAL":
        return False, []

    reasons = []

    # Check inspection type
    inspection_type = str(row.get("inspection_type", "")).lower()
    for mtype in MEASUREMENT_INSPECTION_TYPES:
        if mtype in inspection_type:
            reasons.append(f"inspection_type:{mtype}")

    # Check summary
    summary = str(row.get("summary", ""))
    matches = check_patterns(summary, MEASUREMENT_PATTERNS)
    reasons.extend(matches)

    # Check extract content if available
    if extract_content:
        matches = check_patterns(extract_content, MEASUREMENT_PATTERNS)
        reasons.extend(matches)

    return len(reasons) > 0, list(set(reasons))


def detect_implied_pass(row: pd.Series, extract_content: Optional[str] = None) -> Tuple[bool, List[str]]:
    """
    Detect if a PARTIAL record should be PASS (no deficiencies noted).

    This is more conservative - only suggest PASS if there are clear indicators.
    Returns (should_change, matching_patterns)
    """
    if row["outcome"] != "PARTIAL":
        return False, []

    # Don't reclassify if there are issues
    if row.get("issue_count", 0) > 0:
        return False, []

    summary = str(row.get("summary", ""))
    matches = check_patterns(summary, PASS_INDICATORS)

    if extract_content:
        matches.extend(check_patterns(extract_content, PASS_INDICATORS))

    # Require at least one strong indicator
    return len(matches) > 0, list(set(matches))


def analyze_records(
    df: pd.DataFrame,
    use_extracts: bool = True,
    verbose: bool = False
) -> Dict[str, List[Dict]]:
    """
    Analyze all records and identify misclassifications.

    Returns dict with lists of records to fix for each correction type.
    """
    fixes = {
        "FAIL_to_CANCELLED": [],
        "PARTIAL_to_CANCELLED": [],
        "PARTIAL_to_MEASUREMENT": [],
        "PARTIAL_to_PASS": [],
    }

    for idx, row in df.iterrows():
        inspection_id = row["inspection_id"]

        # Load extract content if requested
        extract_content = None
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
            continue  # Don't check other patterns

        # Check for PARTIAL → CANCELLED (multi-inspection reports with some cancelled)
        should_partial_cancel, partial_cancel_reasons = detect_partial_cancelled(row, extract_content)
        if should_partial_cancel:
            fixes["PARTIAL_to_CANCELLED"].append({
                "inspection_id": inspection_id,
                "current_outcome": "PARTIAL",
                "new_outcome": "CANCELLED",
                "reasons": partial_cancel_reasons,
                "inspection_type": row.get("inspection_type"),
                "summary": str(row.get("summary", ""))[:200],
                "issues": str(row.get("issues", ""))[:200],
            })
            continue  # Don't check other patterns

        # Check for PARTIAL → MEASUREMENT
        should_measure, measure_reasons = detect_measurement(row, extract_content)
        if should_measure:
            fixes["PARTIAL_to_MEASUREMENT"].append({
                "inspection_id": inspection_id,
                "current_outcome": "PARTIAL",
                "new_outcome": "MEASUREMENT",
                "reasons": measure_reasons,
                "inspection_type": row.get("inspection_type"),
                "summary": str(row.get("summary", ""))[:200],
            })
            continue

        # Check for PARTIAL → PASS (more conservative)
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

    # Build lookup of inspection_id -> new_outcome
    changes = {}
    for fix_type, records in fixes.items():
        for record in records:
            changes[record["inspection_id"]] = record["new_outcome"]

    # Apply changes
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
            for r in records[:5]:  # Show first 5
                print(f"  {r['inspection_id']}: {r['inspection_type'][:50]}...")
                print(f"    Reasons: {', '.join(r['reasons'][:3])}")
            if len(records) > 5:
                print(f"  ... and {len(records) - 5} more")


def search_with_embeddings(
    df: pd.DataFrame,
    query: str,
    outcome_filter: str,
    limit: int = 50
) -> List[Dict]:
    """
    Use embeddings to find records semantically similar to query.

    Requires chromadb and google-genai to be installed.
    """
    try:
        import chromadb
        import google.genai as genai
        import os
    except ImportError:
        print("Warning: chromadb or google-genai not installed, skipping embeddings search")
        return []

    # Get API key
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        env_path = _project_root / ".env"
        if env_path.exists():
            with open(env_path) as f:
                for line in f:
                    if line.startswith("GEMINI_API_KEY"):
                        api_key = line.split("=")[1].strip().strip('"\'')
                        break

    if not api_key:
        print("Warning: GEMINI_API_KEY not found, skipping embeddings search")
        return []

    # Create in-memory collection for this search
    client = chromadb.Client()
    collection = client.create_collection(
        name="raba_summaries",
        metadata={"hnsw:space": "cosine"}
    )

    # Filter to relevant outcome
    filtered = df[df["outcome"] == outcome_filter]

    # Add documents (summaries) to collection
    summaries = filtered["summary"].fillna("").tolist()
    ids = filtered["inspection_id"].tolist()

    # Get embeddings using Gemini
    genai_client = genai.Client(api_key=api_key)

    # Batch embed summaries (Gemini has limits)
    batch_size = 100
    all_embeddings = []
    for i in range(0, len(summaries), batch_size):
        batch = summaries[i:i+batch_size]
        response = genai_client.models.embed_content(
            model="text-embedding-004",
            contents=batch
        )
        all_embeddings.extend([e.values for e in response.embeddings])

    # Add to collection
    collection.add(
        documents=summaries,
        embeddings=all_embeddings,
        ids=ids
    )

    # Query
    query_response = genai_client.models.embed_content(
        model="text-embedding-004",
        contents=[query]
    )
    query_embedding = query_response.embeddings[0].values

    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=min(limit, len(ids))
    )

    # Format results
    matches = []
    for idx, (doc_id, distance) in enumerate(zip(results["ids"][0], results["distances"][0])):
        # Higher distance = less similar for cosine
        similarity = 1 - distance
        if similarity > 0.5:  # Only include if reasonably similar
            row = df[df["inspection_id"] == doc_id].iloc[0]
            matches.append({
                "inspection_id": doc_id,
                "similarity": similarity,
                "inspection_type": row.get("inspection_type"),
                "summary": str(row.get("summary", ""))[:200],
            })

    return matches


def main():
    parser = argparse.ArgumentParser(
        description="Fix RABA outcome misclassifications"
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
        "--use-extracts",
        action="store_true",
        default=True,
        help="Also check extract stage content (default: True)"
    )
    parser.add_argument(
        "--no-extracts",
        action="store_true",
        help="Only use summary/failure_reason fields"
    )
    parser.add_argument(
        "--use-embeddings",
        action="store_true",
        help="Also use embeddings search for additional detection (slower)"
    )
    parser.add_argument(
        "--output",
        help="Output file for fix report (JSON)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show detailed output"
    )

    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        print("Specify --dry-run or --apply")
        sys.exit(1)

    use_extracts = not args.no_extracts

    # Load data
    csv_path = Settings.RABA_PROCESSED_DIR / "raba_consolidated.csv"
    print(f"Loading: {csv_path}")
    df = pd.read_csv(csv_path)
    print(f"Total records: {len(df)}")

    # Analyze
    print(f"\nAnalyzing records (use_extracts={use_extracts})...")
    fixes = analyze_records(df, use_extracts=use_extracts, verbose=args.verbose)

    # Print summary
    print_summary(fixes)

    # Embeddings-based additional detection
    if args.use_embeddings:
        print("\n" + "-" * 60)
        print("EMBEDDINGS-BASED SEARCH (for additional review)")
        print("-" * 60)

        # Search for CANCELLED-like records in remaining FAIL
        already_fixed = {r["inspection_id"] for r in fixes["FAIL_to_CANCELLED"]}
        remaining_fail = df[(df["outcome"] == "FAIL") & (~df["inspection_id"].isin(already_fixed))]

        if len(remaining_fail) > 0:
            print(f"\nSearching {len(remaining_fail)} remaining FAIL records...")
            cancelled_candidates = search_with_embeddings(
                remaining_fail,
                "inspection cancelled trip charge work not ready site not ready did not pass internal inspection",
                "FAIL",
                limit=30
            )
            if cancelled_candidates:
                print(f"\nPotential additional CANCELLED (review manually):")
                for c in cancelled_candidates[:10]:
                    print(f"  {c['inspection_id']} (sim={c['similarity']:.2f}): {c['inspection_type'][:50]}...")

        # Search for MEASUREMENT-like records in remaining PARTIAL
        already_fixed_partial = {r["inspection_id"] for r in fixes["PARTIAL_to_MEASUREMENT"]}
        already_fixed_partial.update({r["inspection_id"] for r in fixes["PARTIAL_to_PASS"]})
        remaining_partial = df[(df["outcome"] == "PARTIAL") & (~df["inspection_id"].isin(already_fixed_partial))]

        if len(remaining_partial) > 0:
            print(f"\nSearching {len(remaining_partial)} remaining PARTIAL records...")
            measurement_candidates = search_with_embeddings(
                remaining_partial,
                "observation report pickup receipt no pass fail criteria characterization data collected",
                "PARTIAL",
                limit=30
            )
            if measurement_candidates:
                print(f"\nPotential additional MEASUREMENT (review manually):")
                for c in measurement_candidates[:10]:
                    print(f"  {c['inspection_id']} (sim={c['similarity']:.2f}): {c['inspection_type'][:50]}...")

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
