#!/usr/bin/env python3
"""
Generate Fieldwire Power BI tables from raw CSV exports.

This replaces the Power Query transformations in Power BI with Python processing.
Reads 3 source files and outputs 4 tables:
  - fieldwire_combined.csv (main fact table)
  - fieldwire_comments.csv (unpivoted Message columns)
  - fieldwire_checklists.csv (unpivoted Checklist columns)
  - fieldwire_related_tasks.csv (unpivoted Related task columns)

Usage:
    python -m scripts.fieldwire.process.generate_powerbi_tables
    python -m scripts.fieldwire.process.generate_powerbi_tables --dry-run
"""

import argparse
import logging
import re
from pathlib import Path
from datetime import datetime

import pandas as pd

from src.config.settings import settings
from scripts.shared.pipeline_utils import get_output_path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ============================================================================
# Mapping Tables (matching Power BI PlanMapping and StatusMapping)
# ============================================================================

PLAN_MAPPING = {
    # Manpower mappings
    ("Manpower", "1st Floor"): "1st-Floor",
    ("Manpower", "2nd Floor"): "2nd Floor",
    ("Manpower", "3rd Floor"): "3rd Floor",
    ("Manpower", "4th Floor"): "4th Floor",
    ("Manpower", "Overall level 3"): "3rd Floor",
    ("Manpower", "Random"): None,
    # QC Inspections mappings
    ("QC Inspections", "1st Floor"): "1st-Floor",
    ("QC Inspections", "2nd Floor"): "2nd Floor",
    ("QC Inspections", "3rd Floor - LOUVER & AIR SHOWER ELEVATION"): "3rd Floor",
    ("QC Inspections", "3rd Floor - A0-02J0"): "3rd Floor",
    ("QC Inspections", "4th Floor - CLEANROOM - LEVEL 02 OVERALL FLOOR PLAN"): "4th Floor",
    ("QC Inspections", "A0-02F0 - DOOR SCHEDULE LEVEL 03 - FAB"): "3rd Floor",
}

STATUS_MAPPING = {
    # Manpower status mappings
    ("Manpower", "Manpower (During)"): ("TBM", None),
    ("Manpower", "Manpower (End)"): ("TBM", None),
    ("Manpower", "Manpower (Start)"): ("TBM", None),
    ("Manpower", "Obstruction"): ("Obstructed", None),
    ("Manpower", "Verified"): ("Verified", None),
    ("Manpower", "Completed"): ("Completed", None),
    # QC Inspections status mappings
    ("QC Inspections", "Inspection - Pass"): ("Inspection Request", "Pass"),
    ("QC Inspections", "Inspection - Missed"): ("Inspection Request", "Missed"),
    ("QC Inspections", "Inspection - Failed"): ("Inspection Request", "Fail"),
    ("QC Inspections", "Verified"): ("Verified", None),
}

# Columns to EXCLUDE from fieldwire_combined (these go to separate tables)
EXCLUDE_PATTERNS = ["Message", "Checklist", "Related task"]

# ============================================================================
# File Discovery
# ============================================================================


def find_latest_file(directory: Path, pattern: str) -> Path | None:
    """Find the most recent file matching a glob pattern."""
    files = list(directory.glob(pattern))
    if not files:
        return None
    files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
    return files[0]


def parse_fieldwire_csv(filepath: Path) -> pd.DataFrame:
    """Parse a Fieldwire data dump CSV (UTF-16, tab-delimited, 3 header rows)."""
    logger.info(f"Reading: {filepath.name}")
    df = pd.read_csv(
        filepath,
        encoding="utf-16",
        sep="\t",
        skiprows=3,
        low_memory=False,
    )
    # Remove columns starting with underscore or empty names
    valid_cols = [c for c in df.columns if c and not c.startswith("_") and c.strip()]
    df = df[valid_cols]
    logger.info(f"  Loaded {len(df)} rows, {len(df.columns)} columns")
    return df


# ============================================================================
# Transformations
# ============================================================================


def apply_plan_mapping(df: pd.DataFrame, source_project: str) -> pd.DataFrame:
    """Apply plan name mappings for a given source project."""
    if "Plan" not in df.columns:
        return df

    def map_plan(plan):
        if pd.isna(plan):
            return plan
        key = (source_project, plan)
        if key in PLAN_MAPPING:
            return PLAN_MAPPING[key]
        return plan

    df = df.copy()
    df["Plan"] = df["Plan"].apply(map_plan)
    return df


def apply_status_mapping(df: pd.DataFrame, source_project: str) -> pd.DataFrame:
    """Apply status mappings for a given source project."""
    if "Status" not in df.columns:
        return df

    df = df.copy()

    def map_status(status):
        if pd.isna(status):
            return status, None
        key = (source_project, status)
        if key in STATUS_MAPPING:
            return STATUS_MAPPING[key]
        return status, None

    # Apply mapping
    mapped = df["Status"].apply(map_status)
    df["Status"] = mapped.apply(lambda x: x[0])
    df["Inspection Status"] = mapped.apply(lambda x: x[1])

    return df


def add_missing_columns(df: pd.DataFrame, all_columns: list[str]) -> pd.DataFrame:
    """Add missing columns with null values for schema alignment."""
    df = df.copy()
    for col in all_columns:
        if col not in df.columns:
            df[col] = None
    return df


def extract_plan_url(plan_link: str) -> str | None:
    """Extract URL from Plan Link field (format: =HYPERLINK("url", "text"))."""
    if pd.isna(plan_link) or not plan_link:
        return None
    match = re.search(r'"([^"]+)"', str(plan_link))
    return match.group(1) if match else None


# ============================================================================
# Unpivot Functions (for comments, checklists, related tasks)
# ============================================================================


def unpivot_messages(df: pd.DataFrame) -> pd.DataFrame:
    """
    Unpivot Message columns into fieldwire_comments table.

    Output columns: ID, Seq, Value, User, Action, Update, IsPhoto, PhotoURL, Deleted, Source
    """
    # Find Message columns
    msg_cols = [c for c in df.columns if "Message" in c]
    if not msg_cols:
        return pd.DataFrame(columns=["ID", "Seq", "Value", "User", "Action", "Update",
                                      "IsPhoto", "PhotoURL", "Deleted", "Source"])

    # Select ID, Source, and Message columns
    keep_cols = ["ID", "Source"] + msg_cols
    subset = df[[c for c in keep_cols if c in df.columns]].copy()

    # Unpivot
    id_vars = [c for c in ["ID", "Source"] if c in subset.columns]
    melted = subset.melt(id_vars=id_vars, var_name="Attribute", value_name="Value")

    # Filter empty values
    melted = melted[melted["Value"].notna() & (melted["Value"] != "")]

    # Clean attribute to get sequence number
    melted["Seq"] = melted["Attribute"].str.replace("Message ", "", regex=False)

    # Parse fields
    melted["Deleted"] = melted["Value"].str.contains(r"\(Deleted\)", case=False, na=False)
    melted["IsPhoto"] = melted["Value"].str.contains(": Picture Link", case=False, na=False)

    # Extract PhotoURL for photo entries
    def extract_photo_url(row):
        if row["IsPhoto"]:
            match = re.search(r'"([^"]+)"', str(row["Value"]))
            return match.group(1) if match else None
        return None

    melted["PhotoURL"] = melted.apply(extract_photo_url, axis=1)

    # Extract User (before first colon, if not a photo)
    def extract_user(row):
        if row["IsPhoto"]:
            return None
        val = str(row["Value"])
        if ": " in val:
            return val.split(": ")[0]
        return None

    melted["User"] = melted.apply(extract_user, axis=1)

    # Extract Action (first word after colon if it's a known action)
    ACTIONS = ["Changed", "Removed", "Deleted", "Added", "Edited"]

    def extract_action(row):
        if row["IsPhoto"]:
            return None
        val = str(row["Value"])
        if ": " not in val:
            return None
        rest = val.split(": ", 1)[1]
        first_word = rest.split(" ")[0] if rest else ""
        return first_word if first_word in ACTIONS else None

    melted["Action"] = melted.apply(extract_action, axis=1)

    # Extract Update (content after action or full content after colon)
    def extract_update(row):
        if row["IsPhoto"]:
            return None
        val = str(row["Value"])
        if ": " not in val:
            return None
        rest = val.split(": ", 1)[1]
        if row["Action"]:
            return rest.split(" ", 1)[1] if " " in rest else rest
        return rest

    melted["Update"] = melted.apply(extract_update, axis=1)

    # Select and reorder columns
    result = melted[["ID", "Seq", "Value", "User", "Action", "Update",
                     "IsPhoto", "PhotoURL", "Deleted", "Source"]].copy()

    return result


def unpivot_checklists(df: pd.DataFrame) -> pd.DataFrame:
    """
    Unpivot Checklist columns into fieldwire_checklists table.

    Format: "Response: Checklist Item (Username) - Date"
    Output columns: ID, Seq, Response, Checklist Item, Username, Date, Source
    """
    # Find Checklist columns
    checklist_cols = [c for c in df.columns if "Checklist" in c]
    if not checklist_cols:
        return pd.DataFrame(columns=["ID", "Seq", "Response", "Checklist Item",
                                      "Username", "Date", "Source"])

    # Select ID, Source, and Checklist columns
    keep_cols = ["ID", "Source"] + checklist_cols
    subset = df[[c for c in keep_cols if c in df.columns]].copy()

    # Unpivot
    id_vars = [c for c in ["ID", "Source"] if c in subset.columns]
    melted = subset.melt(id_vars=id_vars, var_name="Attribute", value_name="Value")

    # Filter empty values
    melted = melted[melted["Value"].notna() & (melted["Value"] != "")]

    # Clean attribute to get sequence number
    melted["Seq"] = melted["Attribute"].str.replace("Checklist ", "", regex=False)

    # Parse: "Response: Checklist Item (Username) - Date"
    def parse_checklist(val):
        val = str(val)
        # Extract Response (before colon)
        response = val.split(": ")[0].strip() if ": " in val else ""

        # Get rest after colon
        rest = val.split(": ", 1)[1] if ": " in val else val

        # Extract Username (in parentheses, find last occurrence)
        username_match = re.search(r"\(([^)]+)\)\s*-", rest)
        username = username_match.group(1) if username_match else None

        # Extract Date (after ") -")
        date_match = re.search(r"\)\s*-\s*(.+)$", rest)
        date_str = date_match.group(1).strip() if date_match else None

        # Parse date
        date = None
        if date_str:
            try:
                date = pd.to_datetime(date_str).date()
            except Exception:
                pass

        # Extract Checklist Item (between colon and last parenthesis)
        if username_match:
            item_end = username_match.start()
            item = rest[:item_end].strip()
        else:
            item = rest.split("(")[0].strip() if "(" in rest else rest.strip()

        return response, item, username, date

    parsed = melted["Value"].apply(parse_checklist)
    melted["Response"] = parsed.apply(lambda x: x[0])
    melted["Checklist Item"] = parsed.apply(lambda x: x[1])
    melted["Username"] = parsed.apply(lambda x: x[2])
    melted["Date"] = parsed.apply(lambda x: x[3])

    # Select and reorder columns
    result = melted[["ID", "Seq", "Response", "Checklist Item",
                     "Username", "Date", "Source"]].copy()

    return result


def unpivot_related_tasks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Unpivot Related task columns into fieldwire_related_tasks table.

    Output columns: ID, Seq, Related Task ID, Source
    """
    # Find Related task columns
    related_cols = [c for c in df.columns if c.startswith("Related task")]
    if not related_cols:
        return pd.DataFrame(columns=["ID", "Seq", "Related Task ID", "Source"])

    # Select ID, Source, and Related task columns
    keep_cols = ["ID", "Source"] + related_cols
    subset = df[[c for c in keep_cols if c in df.columns]].copy()

    # Unpivot
    id_vars = [c for c in ["ID", "Source"] if c in subset.columns]
    melted = subset.melt(id_vars=id_vars, var_name="Attribute", value_name="Related Task ID")

    # Filter empty values
    melted = melted[melted["Related Task ID"].notna() & (melted["Related Task ID"] != "")]

    # Clean attribute to get sequence number
    melted["Seq"] = melted["Attribute"].str.replace("Related task ", "", regex=False)

    # Select and reorder columns
    result = melted[["ID", "Seq", "Related Task ID", "Source"]].copy()

    return result


def filter_combined_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Remove Message, Checklist, and Related task columns from combined table."""
    exclude_cols = []
    for col in df.columns:
        for pattern in EXCLUDE_PATTERNS:
            if pattern in col:
                exclude_cols.append(col)
                break

    keep_cols = [c for c in df.columns if c not in exclude_cols]
    return df[keep_cols].copy()


# ============================================================================
# Source Processing
# ============================================================================


def process_manpower(filepath: Path) -> pd.DataFrame:
    """
    Process Manpower file (fieldwire_tbm_prev equivalent).

    Source: Manpower_-_SECAI_Power_BI_Data_Dump_*.csv
    ID Prefix: MP-
    Output Source: "TBM - Old"
    """
    df = parse_fieldwire_csv(filepath)

    # Add Source identifier
    df["Source"] = "TBM - Old"

    # Prefix ID
    df["ID"] = "MP-" + df["ID"].astype(str)

    # Apply plan mapping
    df = apply_plan_mapping(df, "Manpower")

    # Rename columns for schema alignment
    rename_map = {
        "Category": "Company",
        "Scope": "Scope Category",
        "Direct Workers": "Direct Manpower",
        "Indirect Workers": "Indirect Manpower",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Clear Plan folder and Plan Link (as in Power BI)
    if "Plan folder" in df.columns:
        df["Plan folder"] = None
    if "Plan Link" in df.columns:
        df["Plan Link"] = None

    logger.info(f"  Processed {len(df)} Manpower rows (TBM - Old)")
    return df


def process_qc_inspections(filepath: Path) -> pd.DataFrame:
    """
    Process QC Inspections file (fieldwire_qc_inspections_transformed equivalent).

    Source: QC_Inspections_Power_BI_Data_Dump_*.csv
    ID Prefix: QC-
    Output Source: "QC Inspections"
    """
    df = parse_fieldwire_csv(filepath)

    # Add Source identifier
    df["Source"] = "QC Inspections"

    # Prefix ID
    df["ID"] = "QC-" + df["ID"].astype(str)

    # Apply plan mapping
    df = apply_plan_mapping(df, "QC Inspections")

    # Apply status mapping
    df = apply_status_mapping(df, "QC Inspections")

    # Rename columns
    rename_map = {
        "Company - Requestor": "Company",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Clear Plan folder and Plan Link
    if "Plan folder" in df.columns:
        df["Plan folder"] = None
    if "Plan Link" in df.columns:
        df["Plan Link"] = None

    logger.info(f"  Processed {len(df)} QC Inspections rows")
    return df


def process_progress_tracking(filepath: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Process Progress Tracking file (main Fieldwire data source).

    Source files (in order of preference):
      - Samsung_-_Progress_Tracking_Power_BI_Data_Dump_*.csv (new, all data)
      - Samsung_-_Progress_Tracking_QC_Inspections_Data_Dump_*.csv (legacy)

    Returns two DataFrames:
    1. TBM Current (Status="TBM", filtered to >= 2025-12-12)
       ID Prefix: TBM-
       Output Source: "TBM - Current"

    2. Progress Tracking (Status="Inspection Request")
       ID Prefix: PT-
       Output Source: "Progress Tracking"
    """
    df = parse_fieldwire_csv(filepath)

    # Parse Start date for filtering
    df["Start date"] = pd.to_datetime(df["Start date"], errors="coerce")

    # --- TBM Current ---
    df_tbm = df[df["Status"] == "TBM"].copy()
    # Filter to >= 2025-12-12
    cutoff = pd.Timestamp("2025-12-12")
    df_tbm = df_tbm[df_tbm["Start date"] >= cutoff].copy()

    df_tbm["Source"] = "TBM - Current"
    df_tbm["ID"] = "TBM-" + df_tbm["ID"].astype(str)
    df_tbm["DataSource"] = "Samsung"  # Added by Power BI's fieldwire_tbm_current

    # Clear Plan folder and Plan Link
    if "Plan folder" in df_tbm.columns:
        df_tbm["Plan folder"] = None
    if "Plan Link" in df_tbm.columns:
        df_tbm["Plan Link"] = None

    logger.info(f"  Processed {len(df_tbm)} TBM Current rows (>= 2025-12-12)")

    # --- Progress Tracking ---
    df_pt = df[df["Status"] == "Inspection Request"].copy()
    df_pt["Source"] = "Progress Tracking"
    df_pt["ID"] = "PT-" + df_pt["ID"].astype(str)

    logger.info(f"  Processed {len(df_pt)} Progress Tracking rows")

    return df_tbm, df_pt


# ============================================================================
# Main Pipeline
# ============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Generate Fieldwire Power BI tables from raw CSV exports"
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=None,
        help="Directory containing raw Fieldwire CSVs (default: settings.FIELDWIRE_RAW_DIR)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory (default: processed/fieldwire/)",
    )
    parser.add_argument(
        "--staging-dir",
        type=Path,
        default=None,
        help="Write outputs to staging directory instead of final location",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without writing files",
    )
    args = parser.parse_args()

    # Determine directories
    input_dir = args.input_dir or settings.FIELDWIRE_RAW_DIR

    # Use staging_dir if provided, otherwise output_dir or default
    if args.staging_dir:
        output_dir = args.staging_dir / 'fieldwire'
        output_dir.mkdir(parents=True, exist_ok=True)
    else:
        output_dir = args.output_dir or settings.FIELDWIRE_PROCESSED_DIR

    if not input_dir.exists():
        logger.error(f"Input directory not found: {input_dir}")
        return 1

    logger.info(f"Input directory: {input_dir}")
    logger.info(f"Output directory: {output_dir}")

    # Find source files
    # Legacy files (historical data)
    manpower_file = find_latest_file(input_dir, "Manpower_-_SECAI_Power_BI_Data_Dump_*.csv")
    qc_file = find_latest_file(input_dir, "QC_Inspections_Power_BI_Data_Dump_*.csv")

    # Progress Tracking file - try new name first, fall back to legacy name
    # New: Samsung_-_Progress_Tracking_Power_BI_Data_Dump_*.csv (all fieldwire data)
    # Old: Samsung_-_Progress_Tracking_QC_Inspections_Data_Dump_*.csv (legacy, inspections only)
    progress_file = find_latest_file(
        input_dir, "Samsung_-_Progress_Tracking_Power_BI_Data_Dump_*.csv"
    )
    if not progress_file:
        progress_file = find_latest_file(
            input_dir, "Samsung_-_Progress_Tracking_QC_Inspections_Data_Dump_*.csv"
        )

    if not any([manpower_file, qc_file, progress_file]):
        logger.error("No source files found!")
        return 1

    # Process each source
    dfs = []

    if manpower_file:
        df_mp = process_manpower(manpower_file)
        dfs.append(df_mp)

    if qc_file:
        df_qc = process_qc_inspections(qc_file)
        dfs.append(df_qc)

    if progress_file:
        df_tbm, df_pt = process_progress_tracking(progress_file)
        dfs.append(df_tbm)
        dfs.append(df_pt)

    # Combine all sources
    logger.info("Combining sources...")
    df_combined = pd.concat(dfs, ignore_index=True)
    logger.info(f"  Combined: {len(df_combined)} rows")

    # Add Plan URL column
    if "Plan Link" in df_combined.columns:
        df_combined["Plan URL"] = df_combined["Plan Link"].apply(extract_plan_url)

    # Add Total Manpower column
    df_combined["Total Manpower"] = (
        df_combined.get("TBM Manpower", 0).fillna(0).astype(float)
    )

    # Add missing columns for schema alignment with Power BI
    missing_cols = ["Location Type", "Scaffold Tag #", "Obstruction - Cause", "DataSource"]
    for col in missing_cols:
        if col not in df_combined.columns:
            df_combined[col] = None

    # Convert numeric columns
    for col in ["X pos (%)", "Y pos (%)", "TBM Manpower", "Direct Manpower",
                "Indirect Manpower", "Total Idle Hours", "Total Manpower"]:
        if col in df_combined.columns:
            df_combined[col] = pd.to_numeric(df_combined[col], errors="coerce")

    # --- Generate derived tables BEFORE filtering columns ---
    logger.info("Generating derived tables...")

    df_comments = unpivot_messages(df_combined)
    logger.info(f"  fieldwire_comments: {len(df_comments)} rows")

    df_checklists = unpivot_checklists(df_combined)
    logger.info(f"  fieldwire_checklists: {len(df_checklists)} rows")

    df_related = unpivot_related_tasks(df_combined)
    logger.info(f"  fieldwire_related_tasks: {len(df_related)} rows")

    # Filter combined table (remove Message, Checklist, Related task columns)
    df_combined_filtered = filter_combined_columns(df_combined)
    logger.info(f"  fieldwire_combined: {len(df_combined_filtered)} rows, "
                f"{len(df_combined_filtered.columns)} columns")

    # Summary by Source
    print("\n=== Source Distribution ===")
    print(df_combined_filtered["Source"].value_counts().to_string())

    if args.dry_run:
        print("\n[Dry run - no files written]")
        return 0

    # Write output files
    output_dir.mkdir(parents=True, exist_ok=True)

    # fieldwire_combined
    combined_path = output_dir / "fieldwire_combined.csv"
    df_combined_filtered.to_csv(combined_path, index=False)
    logger.info(f"Wrote {combined_path}")

    # fieldwire_comments
    comments_path = output_dir / "fieldwire_comments.csv"
    df_comments.to_csv(comments_path, index=False)
    logger.info(f"Wrote {comments_path}")

    # fieldwire_checklists
    checklists_path = output_dir / "fieldwire_checklists.csv"
    df_checklists.to_csv(checklists_path, index=False)
    logger.info(f"Wrote {checklists_path}")

    # fieldwire_related_tasks
    related_path = output_dir / "fieldwire_related_tasks.csv"
    df_related.to_csv(related_path, index=False)
    logger.info(f"Wrote {related_path}")

    print("\n=== Output Summary ===")
    print(f"fieldwire_combined:      {len(df_combined_filtered):,} rows")
    print(f"fieldwire_comments:      {len(df_comments):,} rows")
    print(f"fieldwire_checklists:    {len(df_checklists):,} rows")
    print(f"fieldwire_related_tasks: {len(df_related):,} rows")

    return 0


if __name__ == "__main__":
    exit(main())
