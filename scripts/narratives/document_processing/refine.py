"""
Iterative refinement stage for improving statement location accuracy.

This script stage:
1. Identifies statements with low locate confidence (< 100% or not exact match)
2. Uses LLM to find exact verbatim quotes from the source document
3. Iterates up to max_attempts to achieve 100% locate rate
4. Verifies each refinement actually locates in the source

The goal is to replace paraphrased statements with exact quotes that can be
traced back to specific locations in the source document.
"""

import json
import os
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass

# Add project root to path
_project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from dotenv import load_dotenv
load_dotenv(_project_root / ".env")

# Import locate functions for verification
from scripts.narratives.document_processing.locate import (
    extract_document_text,
    find_statement_location,
    normalize_text,
)

# Import Gemini client
from google import genai


# Configuration
MAX_ATTEMPTS = 3  # Maximum refinement attempts per statement
MIN_CONFIDENCE = 95.0  # Minimum confidence to consider "located"
BATCH_SIZE = 5  # Process statements in batches to reduce API calls


@dataclass
class RefinementResult:
    """Result of refining a single statement."""
    original_text: str
    refined_text: Optional[str]
    attempts: int
    success: bool
    final_confidence: float
    final_match_type: str
    error: Optional[str] = None


def get_gemini_client() -> genai.Client:
    """Get authenticated Gemini client."""
    api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if not api_key:
        raise ValueError("No API key found. Set GEMINI_API_KEY in .env")
    return genai.Client(api_key=api_key)


def identify_problematic_statements(data: dict) -> List[Tuple[int, dict]]:
    """
    Identify statements that need refinement.

    Returns list of (index, statement) tuples for statements with:
    - match_type not in ["exact", "prefix"]
    - match_confidence < MIN_CONFIDENCE
    """
    statements = data.get("statements", [])
    problematic = []

    for i, stmt in enumerate(statements):
        loc = stmt.get("source_location", {})
        match_type = loc.get("match_type", "not_found")
        confidence = loc.get("match_confidence", 0)

        # Skip already well-located statements
        if match_type in ["exact", "prefix"] and confidence >= MIN_CONFIDENCE:
            continue

        # Skip empty statements
        if not stmt.get("text", "").strip():
            continue

        problematic.append((i, stmt))

    return problematic


def build_refinement_prompt(
    statements: List[dict],
    source_text: str,
) -> str:
    """
    Build prompt for LLM to find exact quotes.
    """
    statements_json = json.dumps(
        [{"index": i, "text": s.get("text", "")} for i, s in enumerate(statements)],
        indent=2
    )

    # Truncate source text if too long (keep first 50k chars)
    if len(source_text) > 50000:
        source_text = source_text[:50000] + "\n\n[... truncated ...]"

    return f"""You are a forensic document analyst. Your task is to find EXACT VERBATIM text from the source document that corresponds to each paraphrased statement.

## CRITICAL RULES - READ CAREFULLY:

1. **EXACT COPY ONLY**: Copy text EXACTLY as it appears in the source - same spacing, punctuation, capitalization
2. **NO MODIFICATIONS**: Do not "fix" typos, add/remove spaces, or change formatting
3. **SEARCH STRATEGIES**:
   - Look for key unique identifiers (IDs, codes, numbers)
   - Try variations: "Crane#2" vs "Crane #2", "Grid" vs "Gridline"
   - The source may have different formatting than the statement
4. **FULL LINE PREFERRED**: Copy the complete line/phrase from source, not just fragments
5. **NULL IF UNCERTAIN**: Return null if you cannot find text that clearly matches

## Common Differences to Watch For:
- "Grid 13" in statement might be "Gridline 13" in source
- "Crane #2" in statement might be "CRANE#2" in source
- Statement may omit prefixes like "PH1-" or "CN.XXX.XXXX"

## SOURCE DOCUMENT:
---
{source_text}
---

## STATEMENTS TO FIND:
{statements_json}

## RESPONSE FORMAT:
Return a JSON array. For each statement:
- "index": the statement index (matching input)
- "exact_quote": the EXACT text copied from source document (or null if not found)
- "confidence": 0-100 confidence this quote matches the statement's meaning

Example:
[
  {{"index": 0, "exact_quote": "PH1-ERECT TOWER CRANE#2 - Gridline 13 West", "confidence": 95}},
  {{"index": 1, "exact_quote": null, "confidence": 0}}
]

JSON response:"""


def call_refinement_llm(
    client: genai.Client,
    prompt: str,
    model: str = "gemini-2.5-flash-lite",
) -> Optional[List[dict]]:
    """
    Call LLM to get exact quotes for statements.

    Returns list of refinement results or None on error.
    """
    try:
        response = client.models.generate_content(
            model=model,
            contents=prompt,
            config={
                "response_mime_type": "application/json",
            },
        )

        result = json.loads(response.text)
        if isinstance(result, list):
            return result
        return None

    except Exception as e:
        print(f"    LLM call failed: {e}")
        return None


def verify_and_locate(
    quote: str,
    full_text: str,
    pages: list,
) -> dict:
    """
    Verify a quote exists in source and get its location.

    Returns source_location dict.
    """
    if not quote or not quote.strip():
        return {
            "page": -1,
            "char_offset": -1,
            "match_confidence": 0.0,
            "match_type": "empty",
        }

    location = find_statement_location(quote, full_text, pages)
    return {
        "page": location.page,
        "char_offset": location.char_offset,
        "match_confidence": round(location.match_confidence, 1),
        "match_type": location.match_type,
    }


def refine_statements_batch(
    client: genai.Client,
    statements: List[Tuple[int, dict]],
    full_text: str,
    pages: list,
    model: str = "gemini-2.5-flash-lite",
) -> Dict[int, RefinementResult]:
    """
    Refine a batch of statements with iteration.

    Returns dict mapping original index to RefinementResult.
    """
    results = {}

    # Track which statements still need refinement
    pending = {i: {"stmt": stmt, "attempts": 0} for i, stmt in statements}

    for attempt in range(MAX_ATTEMPTS):
        if not pending:
            break

        print(f"    Refinement attempt {attempt + 1}/{MAX_ATTEMPTS} for {len(pending)} statements")

        # Build prompt for pending statements
        pending_stmts = [(idx, info["stmt"]) for idx, info in pending.items()]
        prompt = build_refinement_prompt(
            [stmt for _, stmt in pending_stmts],
            full_text,
        )

        # Call LLM
        llm_results = call_refinement_llm(client, prompt, model)
        if not llm_results:
            # LLM failed, mark all as failed for this attempt
            for idx in list(pending.keys()):
                pending[idx]["attempts"] += 1
            continue

        # Process results
        # Map LLM response index back to original statement index
        idx_map = {i: orig_idx for i, (orig_idx, _) in enumerate(pending_stmts)}

        for llm_result in llm_results:
            llm_idx = llm_result.get("index")
            if llm_idx is None or llm_idx not in idx_map:
                continue

            orig_idx = idx_map[llm_idx]
            exact_quote = llm_result.get("exact_quote")

            pending[orig_idx]["attempts"] += 1

            if not exact_quote:
                # LLM couldn't find exact quote
                continue

            # Verify the quote locates
            location = verify_and_locate(exact_quote, full_text, pages)

            if location["match_type"] == "exact" and location["match_confidence"] >= MIN_CONFIDENCE:
                # Success! Record result and remove from pending
                results[orig_idx] = RefinementResult(
                    original_text=pending[orig_idx]["stmt"].get("text", ""),
                    refined_text=exact_quote,
                    attempts=pending[orig_idx]["attempts"],
                    success=True,
                    final_confidence=location["match_confidence"],
                    final_match_type=location["match_type"],
                )
                del pending[orig_idx]

    # Record failures for remaining pending statements
    for orig_idx, info in pending.items():
        results[orig_idx] = RefinementResult(
            original_text=info["stmt"].get("text", ""),
            refined_text=None,
            attempts=info["attempts"],
            success=False,
            final_confidence=0.0,
            final_match_type="not_found",
            error=f"Failed after {info['attempts']} attempts",
        )

    return results


def process_record(input_data: dict, source_path: Path) -> dict:
    """
    Process a record to refine low-confidence statements.

    This is the entry point called by the script stage.

    Args:
        input_data: The locate stage output (with metadata and content)
        source_path: Path to the original source document

    Returns:
        Updated content dict with refined statements
    """
    # Extract content
    data = input_data.get("content", input_data)
    statements = data.get("statements", [])

    if not statements:
        data["_refine_stats"] = {
            "total_statements": 0,
            "needed_refinement": 0,
            "refined": 0,
            "failed": 0,
        }
        return data

    # Get current locate stats
    locate_stats = data.get("_locate_stats", {})
    current_rate = locate_stats.get("locate_rate", 100.0)

    # If already at 100%, skip refinement
    if current_rate >= 100.0:
        data["_refine_stats"] = {
            "total_statements": len(statements),
            "needed_refinement": 0,
            "refined": 0,
            "failed": 0,
            "skipped": "already at 100% locate rate",
        }
        return data

    # Identify problematic statements
    problematic = identify_problematic_statements(data)

    if not problematic:
        data["_refine_stats"] = {
            "total_statements": len(statements),
            "needed_refinement": 0,
            "refined": 0,
            "failed": 0,
        }
        return data

    print(f"  Refining {len(problematic)} statements with low locate confidence")

    # Extract source document text
    try:
        full_text, pages = extract_document_text(source_path)
    except Exception as e:
        data["_refine_stats"] = {
            "total_statements": len(statements),
            "needed_refinement": len(problematic),
            "refined": 0,
            "failed": len(problematic),
            "error": f"Failed to extract source text: {e}",
        }
        return data

    # Initialize Gemini client
    try:
        client = get_gemini_client()
    except Exception as e:
        data["_refine_stats"] = {
            "total_statements": len(statements),
            "needed_refinement": len(problematic),
            "refined": 0,
            "failed": len(problematic),
            "error": f"Failed to initialize LLM client: {e}",
        }
        return data

    # Process in batches
    all_results = {}
    for batch_start in range(0, len(problematic), BATCH_SIZE):
        batch = problematic[batch_start:batch_start + BATCH_SIZE]
        batch_results = refine_statements_batch(
            client=client,
            statements=batch,
            full_text=full_text,
            pages=pages,
        )
        all_results.update(batch_results)

    # Apply refinements to statements
    refined_count = 0
    failed_count = 0

    for orig_idx, result in all_results.items():
        if result.success and result.refined_text:
            # Update statement with refined text
            statements[orig_idx]["text"] = result.refined_text
            statements[orig_idx]["_original_text"] = result.original_text
            statements[orig_idx]["_refinement"] = {
                "attempts": result.attempts,
                "success": True,
            }
            # Update source_location
            location = verify_and_locate(result.refined_text, full_text, pages)
            statements[orig_idx]["source_location"] = location
            refined_count += 1
        else:
            statements[orig_idx]["_refinement"] = {
                "attempts": result.attempts,
                "success": False,
                "error": result.error,
            }
            failed_count += 1

    # Recalculate locate stats
    found = sum(
        1 for s in statements
        if s.get("source_location", {}).get("match_type") not in ["not_found", "error", "empty"]
    )

    data["_locate_stats"] = {
        "total_statements": len(statements),
        "located": found,
        "not_found": len(statements) - found,
        "locate_rate": round(found / len(statements) * 100, 1) if statements else 0,
    }

    data["_refine_stats"] = {
        "total_statements": len(statements),
        "needed_refinement": len(problematic),
        "refined": refined_count,
        "failed": failed_count,
        "refine_rate": round(refined_count / len(problematic) * 100, 1) if problematic else 0,
    }

    return data


# Allow running standalone for testing
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Usage: python refine.py <locate_json_path> <source_document_path>")
        sys.exit(1)

    locate_path = Path(sys.argv[1])
    source_path = Path(sys.argv[2])

    with open(locate_path) as f:
        input_data = json.load(f)

    result = process_record(input_data, source_path)

    print("\nRefine stats:", json.dumps(result.get("_refine_stats", {}), indent=2))
    print("Locate stats:", json.dumps(result.get("_locate_stats", {}), indent=2))
