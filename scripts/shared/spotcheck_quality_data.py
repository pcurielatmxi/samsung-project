#!/usr/bin/env python3
"""
Spot-check quality data (RABA/PSI) against source documents using Gemini.

This script samples records from the consolidated quality data and uses Gemini
to audit the classification by reading the original PDF and comparing outcomes.

Usage:
    # Check 5 samples per outcome for RABA
    python -m scripts.shared.spotcheck_quality_data raba --samples 5

    # Check specific outcome for PSI
    python -m scripts.shared.spotcheck_quality_data psi --outcome FAIL --samples 10

    # Check all outcomes with detailed output
    python -m scripts.shared.spotcheck_quality_data raba --samples 3 --verbose
"""

import argparse
import json
import os
import random
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

import pandas as pd
import google.genai as genai

# Add project root to path
_project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import Settings


# Valid outcomes by source
RABA_OUTCOMES = ["PASS", "FAIL", "PARTIAL", "MEASUREMENT"]
PSI_OUTCOMES = ["PASS", "FAIL", "PARTIAL", "CANCELLED"]


AUDIT_PROMPT = """You are auditing quality inspection data extraction. Read this {source} quality report and determine the CORRECT classification.

IMPORTANT: Your job is to determine what the outcome SHOULD be, not what the document might say in error.

Document types and their outcomes:
- **Trip Charge Report**: Inspector arrived but inspection was cancelled → CANCELLED
- **Inspection Report with PASS/FAIL**: Actual inspection occurred → PASS, FAIL, or PARTIAL based on results
- **Lab Test Report with pass/fail criteria**: Tests were run with specs → PASS, FAIL, or PARTIAL
- **Material Pickup/Delivery Receipt**: Just a record of pickup, no testing → MEASUREMENT
- **Observation/Characterization Report**: Data collected without pass/fail specs → MEASUREMENT
- **Work not ready / Site not ready**: No inspection could occur → CANCELLED

Key distinction:
- FAIL = Inspection/test occurred but work/material did not meet requirements
- CANCELLED = Inspection could not occur (not ready, cancelled, trip charge)
- MEASUREMENT = Data collected but no pass/fail criteria exist in the document

Analyze the document and respond in this exact JSON format:
{{
    "document_type": "<type of document>",
    "inspection_occurred": <true/false>,
    "has_pass_fail_criteria": <true/false>,
    "correct_outcome": "<PASS|FAIL|PARTIAL|CANCELLED|MEASUREMENT>",
    "reasoning": "<brief explanation>",
    "key_quotes": ["<relevant quote from document>"]
}}
"""


def load_consolidated_data(source: str) -> pd.DataFrame:
    """Load the consolidated CSV for the specified source."""
    if source == "raba":
        path = Settings.RABA_PROCESSED_DIR / "raba_consolidated.csv"
    else:
        path = Settings.PSI_PROCESSED_DIR / "psi_consolidated.csv"

    return pd.read_csv(path)


def get_pdf_path(source: str, inspection_id: str, source_file: str) -> Path:
    """Get the path to the source PDF."""
    if source == "raba":
        # RABA files are named by inspection_id
        return Settings.RABA_RAW_DIR / "individual" / f"{inspection_id}.pdf"
    else:
        # PSI files use the source_file name (with .pdf extension)
        filename = source_file.replace('.clean.json', '').replace('.format.json', '')
        if not filename.endswith('.pdf'):
            filename = f"{filename}.pdf"
        return Settings.PSI_RAW_DIR / "reports" / filename


def sample_records(
    df: pd.DataFrame,
    outcomes: List[str],
    samples_per_outcome: int,
    specific_outcome: Optional[str] = None,
    seed: int = 42
) -> pd.DataFrame:
    """Sample records from each outcome category."""
    random.seed(seed)

    if specific_outcome:
        outcomes = [specific_outcome]

    samples = []
    for outcome in outcomes:
        outcome_df = df[df['outcome'] == outcome]
        n = min(samples_per_outcome, len(outcome_df))
        if n > 0:
            sampled = outcome_df.sample(n, random_state=seed)
            samples.append(sampled)

    if samples:
        return pd.concat(samples, ignore_index=True)
    return pd.DataFrame()


def audit_record(
    client: genai.Client,
    source: str,
    pdf_path: Path,
    current_outcome: str,
    model: str = "gemini-2.0-flash"
) -> Dict[str, Any]:
    """
    Audit a single record by sending the PDF to Gemini.

    Returns dict with audit results and comparison.
    """
    result = {
        "pdf_path": str(pdf_path),
        "pdf_exists": pdf_path.exists(),
        "current_outcome": current_outcome,
        "audit_success": False,
        "error": None,
    }

    if not pdf_path.exists():
        result["error"] = f"PDF not found: {pdf_path}"
        return result

    try:
        # Upload and analyze the PDF
        upload = client.files.upload(file=pdf_path)

        prompt = AUDIT_PROMPT.format(source=source.upper())

        response = client.models.generate_content(
            model=model,
            contents=[upload, prompt]
        )

        # Parse the JSON response
        response_text = response.text.strip()
        # Handle markdown code blocks
        if response_text.startswith("```"):
            lines = response_text.split("\n")
            response_text = "\n".join(lines[1:-1])

        audit_data = json.loads(response_text)

        result["audit_success"] = True
        result["audit_data"] = audit_data
        result["gemini_outcome"] = audit_data.get("correct_outcome")
        result["match"] = (
            audit_data.get("correct_outcome", "").upper() ==
            current_outcome.upper()
        )
        result["reasoning"] = audit_data.get("reasoning")

    except json.JSONDecodeError as e:
        result["error"] = f"JSON parse error: {e}"
        result["raw_response"] = response.text if 'response' in dir() else None
    except Exception as e:
        result["error"] = str(e)

    return result


def run_spotcheck(
    source: str,
    samples_per_outcome: int = 5,
    specific_outcome: Optional[str] = None,
    verbose: bool = False,
    seed: int = 42,
    model: str = "gemini-2.0-flash"
) -> Dict[str, Any]:
    """
    Run the spot-check audit.

    Returns a report dict with results and statistics.
    """
    # Initialize Gemini client
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        # Try loading from .env file in project root
        env_path = _project_root / ".env"
        if env_path.exists():
            with open(env_path) as f:
                for line in f:
                    if line.startswith("GEMINI_API_KEY"):
                        api_key = line.split("=")[1].strip().strip('"\'')
                        break

    if not api_key:
        raise ValueError("GEMINI_API_KEY not found in environment or .env file")

    client = genai.Client(api_key=api_key)

    # Load data and sample
    print(f"Loading {source.upper()} consolidated data...")
    df = load_consolidated_data(source)
    print(f"Total records: {len(df)}")

    outcomes = RABA_OUTCOMES if source == "raba" else PSI_OUTCOMES

    print(f"\nSampling {samples_per_outcome} records per outcome...")
    samples = sample_records(df, outcomes, samples_per_outcome, specific_outcome, seed)
    print(f"Total samples to audit: {len(samples)}")

    # Run audits
    results = []
    for idx, row in samples.iterrows():
        inspection_id = row['inspection_id']
        current_outcome = row['outcome']
        source_file = row.get('source_file', '')

        pdf_path = get_pdf_path(source, inspection_id, source_file)

        if verbose:
            print(f"\n{'='*60}")
            print(f"Auditing: {inspection_id}")
            print(f"Current outcome: {current_outcome}")
            print(f"PDF: {pdf_path}")

        audit_result = audit_record(
            client, source, pdf_path, current_outcome, model
        )
        audit_result["inspection_id"] = inspection_id
        audit_result["inspection_type"] = row.get('inspection_type', '')
        audit_result["summary"] = row.get('summary', '')[:200]

        results.append(audit_result)

        if verbose:
            if audit_result["audit_success"]:
                match_str = "✓ MATCH" if audit_result["match"] else "✗ MISMATCH"
                print(f"Gemini outcome: {audit_result['gemini_outcome']} {match_str}")
                print(f"Reasoning: {audit_result.get('reasoning', 'N/A')}")
            else:
                print(f"Error: {audit_result.get('error', 'Unknown')}")

    # Calculate statistics
    successful = [r for r in results if r["audit_success"]]
    matches = [r for r in successful if r["match"]]
    mismatches = [r for r in successful if not r["match"]]

    # Stats by outcome
    stats_by_outcome = {}
    for outcome in outcomes:
        outcome_results = [r for r in successful if r["current_outcome"] == outcome]
        outcome_matches = [r for r in outcome_results if r["match"]]
        if outcome_results:
            stats_by_outcome[outcome] = {
                "total": len(outcome_results),
                "matches": len(outcome_matches),
                "match_rate": len(outcome_matches) / len(outcome_results) * 100
            }

    report = {
        "source": source,
        "timestamp": datetime.now().isoformat(),
        "parameters": {
            "samples_per_outcome": samples_per_outcome,
            "specific_outcome": specific_outcome,
            "model": model,
            "seed": seed
        },
        "summary": {
            "total_samples": len(results),
            "successful_audits": len(successful),
            "failed_audits": len(results) - len(successful),
            "matches": len(matches),
            "mismatches": len(mismatches),
            "match_rate": len(matches) / len(successful) * 100 if successful else 0
        },
        "stats_by_outcome": stats_by_outcome,
        "mismatches": [
            {
                "inspection_id": r["inspection_id"],
                "inspection_type": r["inspection_type"],
                "current_outcome": r["current_outcome"],
                "gemini_outcome": r["gemini_outcome"],
                "reasoning": r.get("reasoning"),
                "summary": r["summary"]
            }
            for r in mismatches
        ],
        "errors": [
            {"inspection_id": r["inspection_id"], "error": r["error"]}
            for r in results if not r["audit_success"]
        ],
        "all_results": results
    }

    return report


def print_report(report: Dict[str, Any]):
    """Print a human-readable summary of the report."""
    print("\n" + "="*60)
    print(f"SPOT-CHECK REPORT: {report['source'].upper()}")
    print("="*60)

    summary = report["summary"]
    print(f"\nTotal samples:     {summary['total_samples']}")
    print(f"Successful audits: {summary['successful_audits']}")
    print(f"Failed audits:     {summary['failed_audits']}")
    print(f"Matches:           {summary['matches']}")
    print(f"Mismatches:        {summary['mismatches']}")
    print(f"Match rate:        {summary['match_rate']:.1f}%")

    print("\nBy Outcome:")
    for outcome, stats in report["stats_by_outcome"].items():
        print(f"  {outcome}: {stats['matches']}/{stats['total']} ({stats['match_rate']:.1f}%)")

    if report["mismatches"]:
        print("\n" + "-"*60)
        print("MISMATCHES:")
        print("-"*60)
        for m in report["mismatches"]:
            print(f"\n{m['inspection_id']}: {m['current_outcome']} → {m['gemini_outcome']}")
            print(f"  Type: {m['inspection_type']}")
            print(f"  Reason: {m['reasoning']}")

    if report["errors"]:
        print("\n" + "-"*60)
        print("ERRORS:")
        print("-"*60)
        for e in report["errors"]:
            print(f"\n{e['inspection_id']}: {e['error']}")


def main():
    parser = argparse.ArgumentParser(
        description="Spot-check quality data against source documents"
    )
    parser.add_argument(
        "source",
        choices=["raba", "psi"],
        help="Data source to check"
    )
    parser.add_argument(
        "--samples", "-n",
        type=int,
        default=5,
        help="Number of samples per outcome category (default: 5)"
    )
    parser.add_argument(
        "--outcome", "-o",
        help="Check only a specific outcome (e.g., FAIL, PASS)"
    )
    parser.add_argument(
        "--output", "-f",
        help="Output file for JSON report"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show detailed output during audit"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible sampling (default: 42)"
    )
    parser.add_argument(
        "--model",
        default="gemini-2.0-flash",
        help="Gemini model to use (default: gemini-2.0-flash)"
    )

    args = parser.parse_args()

    report = run_spotcheck(
        source=args.source,
        samples_per_outcome=args.samples,
        specific_outcome=args.outcome,
        verbose=args.verbose,
        seed=args.seed,
        model=args.model
    )

    print_report(report)

    if args.output:
        output_path = Path(args.output)
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        print(f"\nFull report written to: {output_path}")


if __name__ == "__main__":
    main()
