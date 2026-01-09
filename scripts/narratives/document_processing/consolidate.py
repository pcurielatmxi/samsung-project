"""
Consolidate narrative processing output into fact and dimension tables.

Creates:
- dim_narrative_file.csv: Dimension table for source documents
- narrative_statements.csv: Fact table with all statements

Usage:
    python consolidate.py [--config CONFIG_DIR]
"""

import csv
import fnmatch
import json
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

# Add project root to path
_project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

# Load environment variables
from dotenv import load_dotenv
load_dotenv(_project_root / ".env")


def load_config(config_dir: Path) -> dict:
    """Load config.json from config directory."""
    config_file = config_dir / "config.json"
    with open(config_file, "r", encoding="utf-8") as f:
        return json.load(f)


def expand_env_vars(path_str: str) -> str:
    """Expand ${VAR_NAME} environment variables in path."""
    import os
    import re

    def replace_var(match):
        var_name = match.group(1)
        value = os.environ.get(var_name, match.group(0))
        # Convert Windows path to WSL if needed
        if value and len(value) >= 2 and value[1] == ':':
            drive = value[0].lower()
            rest = value[2:].replace('\\', '/').lstrip('/')
            return f'/mnt/{drive}/{rest}'
        return value.replace('\\', '/')

    return re.sub(r'\$\{(\w+)\}', replace_var, path_str)


def is_excluded(filename: str, exclude_patterns: List[str]) -> bool:
    """Check if filename matches any exclusion pattern."""
    for pattern in exclude_patterns:
        if fnmatch.fnmatch(filename, pattern):
            return True
    return False


def get_relative_path(absolute_path: str) -> str:
    """
    Convert absolute path to relative path from /data/ folder.

    Example: /mnt/c/.../Data/raw/narratives/file.pdf -> raw/narratives/file.pdf
    """
    path = absolute_path.replace('\\', '/')

    # Look for common data folder markers
    markers = ['/Data/', '/data/']
    for marker in markers:
        if marker in path:
            idx = path.find(marker) + len(marker)
            return path[idx:]

    # Fallback: just return filename
    return Path(path).name


def load_refine_outputs(refine_dir: Path, exclude_patterns: List[str]) -> List[Tuple[Path, dict]]:
    """
    Load all refine stage outputs, excluding non-narrative files.

    Returns list of (filepath, data) tuples.
    """
    outputs = []

    for f in sorted(refine_dir.glob("*.refine.json")):
        # Check exclusion on original filename
        original_name = f.stem.replace(".refine", "")

        # Check against common extensions
        if any(is_excluded(original_name + ext, exclude_patterns)
               for ext in [".pdf", ".docx", ".doc", ".xlsx", ".xls", ""]):
            continue

        try:
            with open(f, "r", encoding="utf-8") as fp:
                data = json.load(fp)
            outputs.append((f, data))
        except Exception as e:
            print(f"Warning: Failed to load {f.name}: {e}")

    return outputs


def build_dimension_table(
    outputs: List[Tuple[Path, dict]],
) -> Tuple[List[dict], Dict[str, str]]:
    """
    Build dim_narrative_file records.

    Returns:
        - List of dimension records
        - Dict mapping source_file path to narrative_file_id
    """
    dim_records = []
    path_to_id = {}

    for idx, (filepath, data) in enumerate(outputs, start=1):
        metadata = data.get("metadata", {})
        content = data.get("content", data)
        document = content.get("document", {})
        locate_stats = content.get("_locate_stats", {})
        statements = content.get("statements", [])

        source_file = metadata.get("source_file", "")
        narrative_file_id = f"NAR-{idx:03d}"

        # Map source path to ID
        path_to_id[source_file] = narrative_file_id

        # Build dimension record
        record = {
            "narrative_file_id": narrative_file_id,
            "relative_path": get_relative_path(source_file),
            "filename": Path(source_file).name if source_file else filepath.stem.replace(".refine", ""),
            "document_type": document.get("type", ""),
            "document_title": document.get("title", ""),
            "document_date": document.get("document_date", ""),
            "data_date": document.get("data_date", ""),
            "author": document.get("author", ""),
            "summary": (document.get("summary", "") or "")[:500],  # Truncate long summaries
            "statement_count": len(statements),
            "locate_rate": locate_stats.get("locate_rate", 0),
            "file_extension": Path(source_file).suffix.lower() if source_file else "",
        }

        dim_records.append(record)

    return dim_records, path_to_id


def build_statements_table(
    outputs: List[Tuple[Path, dict]],
    path_to_id: Dict[str, str],
) -> List[dict]:
    """
    Build narrative_statements fact records.
    """
    stmt_records = []
    stmt_counter = 0

    for filepath, data in outputs:
        metadata = data.get("metadata", {})
        content = data.get("content", data)
        statements = content.get("statements", [])

        source_file = metadata.get("source_file", "")
        narrative_file_id = path_to_id.get(source_file, "UNKNOWN")

        for stmt_idx, stmt in enumerate(statements):
            stmt_counter += 1
            statement_id = f"STMT-{stmt_counter:05d}"

            # Get source location info
            loc = stmt.get("source_location", {})
            match_type = loc.get("match_type", "not_found")
            match_confidence = loc.get("match_confidence", 0)

            # Calculate is_located flag
            is_located = (
                match_type in ["exact", "prefix"] and
                match_confidence >= 95
            )

            # Format list fields as pipe-delimited strings
            parties = stmt.get("parties") or []
            locations = stmt.get("locations") or []
            references = stmt.get("references") or []

            record = {
                "statement_id": statement_id,
                "narrative_file_id": narrative_file_id,
                "statement_index": stmt_idx,
                "text": stmt.get("text", ""),
                "category": stmt.get("category", ""),
                "event_date": stmt.get("event_date", ""),
                "parties": "|".join(parties) if parties else "",
                "locations": "|".join(locations) if locations else "",
                "impact_days": stmt.get("impact_days", ""),
                "impact_description": stmt.get("impact_description", ""),
                "references": "|".join(references) if references else "",
                "source_page": loc.get("page", ""),
                "source_char_offset": loc.get("char_offset", ""),
                "match_confidence": match_confidence,
                "match_type": match_type,
                "is_located": is_located,
            }

            stmt_records.append(record)

    return stmt_records


def write_csv(records: List[dict], output_path: Path, fieldnames: List[str]) -> None:
    """Write records to CSV file."""
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)


def main(config_dir: Optional[Path] = None):
    """Main consolidation function."""

    # Default config directory
    if config_dir is None:
        config_dir = Path(__file__).parent

    print("=" * 60)
    print("Narrative Consolidation")
    print("=" * 60)

    # Load config
    config = load_config(config_dir)
    exclude_patterns = config.get("exclude_patterns", [])
    output_dir = Path(expand_env_vars(config.get("output_dir", "")))

    print(f"Output dir: {output_dir}")
    print(f"Exclude patterns: {len(exclude_patterns)}")

    # Find refine stage output directory
    refine_dir = output_dir / "4.refine"
    if not refine_dir.exists():
        print(f"ERROR: Refine directory not found: {refine_dir}")
        sys.exit(1)

    # Load all outputs
    print(f"\nLoading outputs from {refine_dir}...")
    outputs = load_refine_outputs(refine_dir, exclude_patterns)
    print(f"Loaded {len(outputs)} narrative files (after exclusions)")

    if not outputs:
        print("ERROR: No outputs to consolidate")
        sys.exit(1)

    # Build dimension table
    print("\nBuilding dimension table...")
    dim_records, path_to_id = build_dimension_table(outputs)

    # Build statements table
    print("Building statements table...")
    stmt_records = build_statements_table(outputs, path_to_id)

    # Define output paths
    dim_output = output_dir / "dim_narrative_file.csv"
    stmt_output = output_dir / "narrative_statements.csv"

    # Write dimension table
    dim_fieldnames = [
        "narrative_file_id", "relative_path", "filename", "document_type",
        "document_title", "document_date", "data_date", "author", "summary",
        "statement_count", "locate_rate", "file_extension"
    ]
    write_csv(dim_records, dim_output, dim_fieldnames)
    print(f"\nWrote {len(dim_records)} records to {dim_output.name}")

    # Write statements table
    stmt_fieldnames = [
        "statement_id", "narrative_file_id", "statement_index", "text",
        "category", "event_date", "parties", "locations", "impact_days",
        "impact_description", "references", "source_page", "source_char_offset",
        "match_confidence", "match_type", "is_located"
    ]
    write_csv(stmt_records, stmt_output, stmt_fieldnames)
    print(f"Wrote {len(stmt_records)} records to {stmt_output.name}")

    # Print summary statistics
    print("\n" + "=" * 60)
    print("Summary Statistics")
    print("=" * 60)

    # Document stats
    total_docs = len(dim_records)
    docs_100 = sum(1 for r in dim_records if r["locate_rate"] == 100)
    docs_partial = sum(1 for r in dim_records if 0 < r["locate_rate"] < 100)
    docs_zero = sum(1 for r in dim_records if r["locate_rate"] == 0)

    print(f"\nDocuments:")
    print(f"  Total: {total_docs}")
    print(f"  100% located: {docs_100}")
    print(f"  Partial: {docs_partial}")
    print(f"  0% located: {docs_zero}")

    # Statement stats
    total_stmts = len(stmt_records)
    located_stmts = sum(1 for r in stmt_records if r["is_located"])
    unlocated_stmts = total_stmts - located_stmts

    print(f"\nStatements:")
    print(f"  Total: {total_stmts}")
    print(f"  Located: {located_stmts} ({located_stmts/total_stmts*100:.1f}%)")
    print(f"  Unlocated: {unlocated_stmts}")

    # Category breakdown
    print(f"\nStatements by category:")
    from collections import Counter
    categories = Counter(r["category"] for r in stmt_records)
    for cat, count in categories.most_common():
        print(f"  {cat or '(empty)'}: {count}")

    # Document type breakdown
    print(f"\nDocuments by type:")
    doc_types = Counter(r["document_type"] for r in dim_records)
    for dtype, count in doc_types.most_common():
        print(f"  {dtype or '(empty)'}: {count}")

    print("\n" + "=" * 60)
    print("Consolidation complete")
    print("=" * 60)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Consolidate narrative outputs")
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Config directory (default: script directory)"
    )

    args = parser.parse_args()
    main(args.config)
