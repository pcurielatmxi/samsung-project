"""
Extract TBM content from Fieldwire data dumps.

Parses Manpower and Progress Tracking exports, extracts:
- Checklist tags (auto-mapped from structured checklist data)
- Narratives (filtered from messages, excluding change logs)

Output: CSV ready for AI enrichment.
"""

import re
import argparse
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd

from src.config.settings import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Standard idle time tags
IDLE_TAGS = [
    "passive", "standing", "not_started", "obstruction", "phone",
    "waiting", "permit", "acting", "meeting", "talking", "delay"
]

# Mapping from checklist category names to standard tags
CHECKLIST_TAG_MAP = {
    "passive": "passive",
    "standing": "standing",
    "not started": "not_started",
    "work has not started": "not_started",
    "no manpower": "not_started",
    "obstructed": "obstruction",
    "obstruction": "obstruction",
    "phone": "phone",
    "waiting": "waiting",
    "permit": "permit",
    "acting": "acting",
    "meeting": "meeting",
    "talking": "talking",
    "delay": "delay",
}

# Keywords indicating change log entries (to filter out)
CHANGELOG_KEYWORDS = ["Changed", "Removed", "Deleted", "Added", "Edited"]
SET_PATTERN = re.compile(r"^Set\s+\w+\s+to\s+", re.IGNORECASE)


def find_latest_file(directory: Path, pattern: str) -> Path | None:
    """Find the most recent file matching a pattern."""
    files = list(directory.glob(pattern))
    if not files:
        return None
    # Sort by modification time, most recent first
    files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
    return files[0]


def parse_fieldwire_csv(filepath: Path) -> pd.DataFrame:
    """Parse a Fieldwire data dump CSV (UTF-16, tab-delimited, 3 header rows)."""
    return pd.read_csv(
        filepath,
        encoding="utf-16",
        sep="\t",
        skiprows=3,
        low_memory=False,
    )


def extract_checklist_tags(row: pd.Series) -> list[str]:
    """
    Extract idle time tags from checklist columns.

    Checklist format: "Yes: Category Name (initials) - date"
    Only extracts tags where value starts with "Yes:".
    """
    tags = set()
    checklist_cols = [c for c in row.index if c.startswith("Checklist")]

    for col in checklist_cols:
        val = row[col]
        if pd.isna(val) or not str(val).strip():
            continue

        val_str = str(val).strip()

        # Only process "Yes:" entries
        if not val_str.startswith("Yes:"):
            continue

        # Extract category name: "Yes: Category Name (initials) - date"
        match = re.match(r"Yes:\s*([^(]+)\s*\(", val_str)
        if match:
            category = match.group(1).strip().lower()
            # Map to standard tag
            if category in CHECKLIST_TAG_MAP:
                tags.add(CHECKLIST_TAG_MAP[category])

    return sorted(tags)


def is_changelog_message(content: str) -> bool:
    """Check if a message is a change log entry (should be filtered out)."""
    content = content.strip()

    # Check for changelog keywords at start of content
    first_word = content.split()[0] if content.split() else ""
    if first_word in CHANGELOG_KEYWORDS:
        return True

    # Check for "Set X to Y" pattern
    if SET_PATTERN.match(content):
        return True

    return False


def is_photo_link(content: str) -> bool:
    """Check if a message is a photo link."""
    return content.strip().startswith("=HYPERLINK")


def extract_narratives(row: pd.Series) -> list[dict]:
    """
    Extract narrative messages from message columns.

    Filters out:
    - Change log entries (Changed/Removed/Deleted/Added/Edited)
    - Set commands (Set X to Y)
    - Photo links (=HYPERLINK)

    Returns list of {person, content, column} dicts.
    """
    narratives = []
    message_cols = [c for c in row.index if c.startswith("Message")]

    for col in message_cols:
        val = row[col]
        if pd.isna(val) or not str(val).strip():
            continue

        val_str = str(val).strip()

        # Skip photo links
        if is_photo_link(val_str):
            continue

        # Parse "Person: Content" format
        match = re.match(r"^([^:]+):\s*(.+)$", val_str, re.DOTALL)
        if not match:
            continue

        person, content = match.groups()
        content = content.strip()

        # Skip change log entries
        if is_changelog_message(content):
            continue

        # Skip very short content (likely not meaningful)
        if len(content) < 5:
            continue

        narratives.append({
            "person": person.strip(),
            "content": content,
            "column": col,
        })

    return narratives


def process_manpower_file(filepath: Path) -> pd.DataFrame:
    """
    Process Manpower file (Manpower_-_SECAI_Power_BI_Data_Dump_*.csv).

    Filter: Status = "Manpower (During)"
    ID Prefix: MP-

    These are mid-day field observations of workers at locations.
    """
    logger.info(f"Processing Manpower: {filepath.name}")

    df = parse_fieldwire_csv(filepath)
    logger.info(f"  Loaded {len(df)} rows")

    # Filter: Status = "Manpower (During)"
    df_filtered = df[df["Status"] == "Manpower (During)"].copy()
    logger.info(f"  After filter (Status='Manpower (During)'): {len(df_filtered)} rows")

    return _extract_content(df_filtered, id_prefix="MP-", source_name="manpower")


def process_progress_tracking_file(filepath: Path) -> pd.DataFrame:
    """
    Process Progress Tracking file (Samsung_-_Progress_Tracking_*.csv).

    Filter: Status = "TBM" AND Category != "Manpower Count"
    ID Prefix: TBM-

    These are TBM location observations (excluding daily headcount totals).
    """
    logger.info(f"Processing Progress Tracking: {filepath.name}")

    df = parse_fieldwire_csv(filepath)
    logger.info(f"  Loaded {len(df)} rows")

    # Filter: Status = "TBM" AND Category != "Manpower Count"
    df_filtered = df[
        (df["Status"] == "TBM") &
        (df["Category"] != "Manpower Count")
    ].copy()
    logger.info(f"  After filter (Status='TBM', Category!='Manpower Count'): {len(df_filtered)} rows")

    return _extract_content(df_filtered, id_prefix="TBM-", source_name="progress_tracking")


def _extract_content(
    df: pd.DataFrame,
    id_prefix: str,
    source_name: str,
) -> pd.DataFrame:
    """
    Extract checklist tags and narratives from filtered DataFrame.

    Args:
        df: Pre-filtered DataFrame
        id_prefix: Prefix for IDs (e.g., "MP-" or "TBM-")
        source_name: Source identifier (e.g., "manpower" or "progress_tracking")

    Returns:
        DataFrame with extracted content
    """
    results = []

    for idx, row in df.iterrows():
        row_id = row.get("ID")
        if pd.isna(row_id):
            continue

        # Extract checklist tags
        checklist_tags = extract_checklist_tags(row)

        # Extract narratives
        narratives = extract_narratives(row)

        # Skip rows with no content
        if not checklist_tags and not narratives:
            continue

        # Concatenate narrative text
        narrative_text = "\n".join(
            f"[{n['person']}]: {n['content']}" for n in narratives
        ) if narratives else ""

        results.append({
            "id": f"{id_prefix}{row_id}",
            "source": source_name,
            "title": row.get("Title", ""),
            "status": row.get("Status", ""),
            "category": row.get("Category", ""),
            "start_date": row.get("Start date", ""),
            "checklist_tags": ",".join(checklist_tags) if checklist_tags else "",
            "narratives": narrative_text,
            "narrative_count": len(narratives),
        })

    result_df = pd.DataFrame(results)
    logger.info(f"  Extracted {len(result_df)} rows with content")

    return result_df


def main():
    parser = argparse.ArgumentParser(
        description="Extract TBM content from Fieldwire data dumps"
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=None,
        help="Directory containing Fieldwire CSV exports (default: settings.FIELDWIRE_RAW_DIR)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output CSV path (default: derived/fieldwire/tbm_content.csv)",
    )
    parser.add_argument(
        "--manpower-file",
        type=Path,
        default=None,
        help="Specific manpower file (default: latest Manpower_-_SECAI_...)",
    )
    parser.add_argument(
        "--progress-file",
        type=Path,
        default=None,
        help="Specific progress tracking file (default: latest Samsung_-_Progress_Tracking_...)",
    )

    args = parser.parse_args()

    # Determine input directory
    input_dir = args.input_dir
    if input_dir is None:
        # Use default from settings or fallback
        input_dir = Path("/mnt/c/Users/pdcur/OneDrive - MXI/Desktop/Samsung Dashboard/Data/raw/fieldwire")

    if not input_dir.exists():
        logger.error(f"Input directory not found: {input_dir}")
        return

    # Find files
    manpower_file = args.manpower_file
    if manpower_file is None:
        manpower_file = find_latest_file(input_dir, "Manpower_-_SECAI_Power_BI_Data_Dump_*.csv")

    progress_file = args.progress_file
    if progress_file is None:
        progress_file = find_latest_file(input_dir, "Samsung_-_Progress_Tracking_QC_Inspections_Data_Dump_*.csv")

    if not manpower_file:
        logger.warning("No Manpower file found")
    if not progress_file:
        logger.warning("No Progress Tracking file found")

    if not manpower_file and not progress_file:
        logger.error("No input files found")
        return

    # Process files
    dfs = []

    if manpower_file and manpower_file.exists():
        df_mp = process_manpower_file(manpower_file)
        dfs.append(df_mp)

    if progress_file and progress_file.exists():
        df_pt = process_progress_tracking_file(progress_file)
        dfs.append(df_pt)

    # Combine
    df_combined = pd.concat(dfs, ignore_index=True)
    logger.info(f"Combined: {len(df_combined)} total rows")

    # Determine output path
    output_path = args.output
    if output_path is None:
        output_dir = Path("/mnt/c/Users/pdcur/OneDrive - MXI/Desktop/Samsung Dashboard/Data/derived/fieldwire")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "tbm_content.csv"

    # Save
    df_combined.to_csv(output_path, index=False)
    logger.info(f"Saved to {output_path}")

    # Summary
    print("\n=== Extraction Summary ===")
    print(f"Total rows: {len(df_combined)}")
    print(f"With checklist tags: {(df_combined['checklist_tags'] != '').sum()}")
    print(f"With narratives: {(df_combined['narratives'] != '').sum()}")
    print(f"Output: {output_path}")

    # Tag distribution
    all_tags = []
    for tags in df_combined["checklist_tags"]:
        if tags:
            all_tags.extend(tags.split(","))

    if all_tags:
        print("\nChecklist tag distribution:")
        from collections import Counter
        for tag, count in Counter(all_tags).most_common():
            print(f"  {tag}: {count}")


if __name__ == "__main__":
    main()
