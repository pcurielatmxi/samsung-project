"""
Generate long-format idle tags CSV from enriched TBM content.

Transforms the enriched data from wide format (one row per record with multiple tags)
to long format (one row per tag) for Power BI metrics.

Output format:
    id, tag, tag_type, tag_order

Where:
- tag_type: "checklist" or "narrative" (checklist sorted first)
- tag_order: 1-based order within each type (use tag_order=1 for KPIs to avoid double-dipping)

Usage:
    python -m scripts.fieldwire.generate_idle_tags_long
"""

import argparse
import ast
import json
import logging
from pathlib import Path

import pandas as pd

from src.config.settings import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_ai_output(ai_output) -> list[str]:
    """Parse ai_output column to extract tags list."""
    if pd.isna(ai_output):
        return []

    try:
        # Handle string representation of dict
        if isinstance(ai_output, str):
            data = ast.literal_eval(ai_output)
        else:
            data = ai_output

        if isinstance(data, dict) and "tags" in data:
            return data["tags"] or []
        return []
    except (ValueError, SyntaxError, TypeError):
        return []


def transform_to_long_format(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform enriched data to long format with one row per tag.

    Args:
        df: DataFrame with columns: id, checklist_tags, ai_output

    Returns:
        DataFrame with columns: id, tag, tag_type, tag_order
    """
    rows = []

    for _, row in df.iterrows():
        record_id = row["id"]

        # Extract checklist tags (comma-separated string)
        checklist_tags = []
        if pd.notna(row.get("checklist_tags")) and str(row["checklist_tags"]).strip():
            checklist_tags = [t.strip() for t in str(row["checklist_tags"]).split(",") if t.strip()]

        # Extract narrative tags (from ai_output JSON)
        narrative_tags = parse_ai_output(row.get("ai_output"))

        # Single sequence of tag_order per ID (checklist first, then narrative)
        order = 0

        # Add checklist tags
        for tag in checklist_tags:
            order += 1
            rows.append({
                "id": record_id,
                "tag": tag,
                "tag_type": "checklist",
                "tag_order": order,
            })

        # Add narrative tags (continuing the sequence)
        for tag in narrative_tags:
            order += 1
            rows.append({
                "id": record_id,
                "tag": tag,
                "tag_type": "narrative",
                "tag_order": order,
            })

    result_df = pd.DataFrame(rows)

    # Sort by id, tag_type (checklist first), tag_order
    if not result_df.empty:
        # Create sort key for tag_type (checklist=0, narrative=1)
        result_df["_type_sort"] = result_df["tag_type"].map({"checklist": 0, "narrative": 1})
        result_df = result_df.sort_values(["id", "_type_sort", "tag_order"])
        result_df = result_df.drop(columns=["_type_sort"])
        result_df = result_df.reset_index(drop=True)

    return result_df


def main():
    parser = argparse.ArgumentParser(
        description="Generate long-format idle tags CSV from enriched TBM content"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        help="Input enriched CSV (default: processed/fieldwire/tbm_content_enriched.csv)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output CSV path (default: processed/fieldwire/idle_tags.csv)",
    )

    args = parser.parse_args()

    # Determine paths
    input_path = args.input
    if input_path is None:
        input_path = settings.FIELDWIRE_PROCESSED_DIR / "tbm_content_enriched.csv"

    output_path = args.output
    if output_path is None:
        output_path = settings.FIELDWIRE_PROCESSED_DIR / "idle_tags.csv"

    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        return

    # Load enriched data
    logger.info(f"Loading {input_path}")
    df = pd.read_csv(input_path)
    logger.info(f"Loaded {len(df)} records")

    # Transform to long format
    df_long = transform_to_long_format(df)
    logger.info(f"Generated {len(df_long)} tag rows")

    # Save output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_long.to_csv(output_path, index=False)
    logger.info(f"Saved to {output_path}")

    # Summary
    print("\n=== Idle Tags Summary ===")
    print(f"Total records:     {len(df)}")
    print(f"Total tag rows:    {len(df_long)}")
    print(f"Unique tags:       {df_long['tag'].nunique()}")

    print("\nBy tag_type:")
    type_counts = df_long["tag_type"].value_counts()
    for tag_type, count in type_counts.items():
        print(f"  {tag_type}: {count} rows")

    # First-tag only stats (tag_order=1 means first tag for that record)
    df_first = df_long[df_long["tag_order"] == 1]
    print(f"\nFirst-tag only (tag_order=1): {len(df_first)} records")

    print("\nTop 10 tags (all):")
    for tag, count in df_long["tag"].value_counts().head(10).items():
        print(f"  {tag}: {count}")

    print("\nTop 10 tags (first-tag only):")
    df_first = df_long[df_long["tag_order"] == 1]
    for tag, count in df_first["tag"].value_counts().head(10).items():
        print(f"  {tag}: {count}")


if __name__ == "__main__":
    main()
