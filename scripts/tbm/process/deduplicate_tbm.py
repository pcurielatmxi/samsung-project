#!/usr/bin/env python3
"""
Deduplicate TBM work entries by flagging duplicate company+date combinations.

Adds columns:
- is_duplicate: True if file shares company+date with another file
- duplicate_group_id: Groups files with same company+date (null if not duplicate)
- is_preferred: True for the "best" file in each duplicate group

Selection logic for is_preferred (in priority order):
1. DATE_MATCH - filename date matches report date inside
2. MORE_RECORDS - file with more records
3. ORIGINAL - non-copy file (no "-2-", "Copy of")
"""

import pandas as pd
import re
import sys
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
from src.config.settings import Settings


def extract_date_from_filename(filename: str) -> str:
    """Extract date from filename for comparison with report_date."""
    name = Path(filename).stem

    # Pattern: MM.DD.YY (2-digit year with dots)
    match = re.search(r'(\d{2})\.(\d{2})\.(\d{2})(?!\d)', name)
    if match:
        month, day, year = match.groups()
        return f'20{year}-{month}-{day}'

    # Pattern: MM-DD-YYYY (4-digit year with dashes)
    match = re.search(r'(\d{2})-(\d{2})-(\d{4})', name)
    if match:
        month, day, year = match.groups()
        return f'{year}-{month}-{day}'

    # Pattern: MM.DD.YYYY (4-digit year with dots)
    match = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', name)
    if match:
        month, day, year = match.groups()
        return f'{year}-{month}-{day}'

    # Pattern: folder structure _M-D-YY_
    match = re.search(r'_(\d{1,2})-(\d{1,2})-(\d{2})_', name)
    if match:
        month, day, year = match.groups()
        return f'20{year}-{int(month):02d}-{int(day):02d}'

    # Pattern: MMDDYYYY (8 digits)
    match = re.search(r'(\d{2})(\d{2})(\d{4})', name)
    if match:
        month, day, year = match.groups()
        return f'{year}-{month}-{day}'

    # Pattern: MMDDYY at start
    match = re.match(r'(\d{2})(\d{2})(\d{2})(?:_|$)', name)
    if match:
        month, day, year = match.groups()
        return f'20{year}-{month}-{day}'

    return None


def normalize_subcontractor_from_filename(subcontractor: str) -> str:
    """Normalize subcontractor name by removing folder path prefixes (for dedup grouping)."""
    if not subcontractor:
        return subcontractor

    # Remove common folder prefixes (e.g., Axios_December_2025 -> Axios)
    # Keep only the first component before any month/date folder
    parts = subcontractor.split('_')

    # Check if second part is a month name or date-like
    month_patterns = ['January', 'February', 'March', 'April', 'May', 'June',
                      'July', 'August', 'September', 'October', 'November', 'December']

    if len(parts) > 1:
        # Check if second part is a month
        if parts[1] in month_patterns:
            return parts[0]
        # Check if second part looks like a date folder (e.g., 1-2-26)
        if re.match(r'^\d{1,2}-\d{1,2}-\d{2}$', parts[1]):
            return parts[0]

    return subcontractor


def normalize_tier2_sc(tier2_sc: str) -> str:
    """Normalize tier2_sc (actual subcontractor from workbook) for analysis."""
    if pd.isna(tier2_sc) or not tier2_sc:
        return None

    name = str(tier2_sc).strip().upper()

    # Standardize common variations
    name_map = {
        'AXIOS': 'Axios',
        'BERG': 'Berg',
        'MK MARLOW': 'MK Marlow',
        'CHERRY COATINGS': 'Cherry Coatings',
        'INFINITY': 'Infinity',
        'BAKER': 'Baker',
        'BRAZOS URETHANE': 'Brazos Urethane',
        'KOVACH': 'Kovach',
        'PATRIOT ERECTORS, LLC': 'Patriot Erectors',
        'PATRIOT ERECTORS': 'Patriot Erectors',
        'ALERT LOCK & KEY': 'Alert Lock & Key',
        'ALPHA': 'Alpha',
        'GDA': 'GDA',
        'APACHE': 'Apache',
        'ROLLING PLAINS': 'Rolling Plains',
        'LATCON': 'Latcon',
        'SCHMIDT': 'Schmidt',
        'STI': 'STI',
        'YATES': 'Yates',
        'SECAI': 'SECAI',
        'ORCO STEEL': 'Orco Steel',
    }

    return name_map.get(name, tier2_sc.strip())


def is_copy_file(filename: str) -> bool:
    """Check if filename indicates it's a copy."""
    return bool(re.search(r'Copy of|-2-|\(\d+\)$', filename, re.IGNORECASE))


def deduplicate_tbm(entries_path: Path, files_path: Path) -> pd.DataFrame:
    """
    Add deduplication flags to TBM work entries.

    Returns:
        DataFrame with added columns: is_duplicate, duplicate_group_id, is_preferred
    """
    # Load data
    df = pd.read_csv(entries_path)
    tbm_files = pd.read_csv(files_path)

    print(f"Loaded {len(df)} work entries from {len(tbm_files)} files")

    # Build file-level metadata
    file_stats = df.groupby('file_id').agg({
        'report_date': 'first',
        'subcontractor_file': 'first',
        'tier2_sc': lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else None,  # Dominant tier2_sc from workbook
        'row_num': 'count'
    }).reset_index()
    file_stats.columns = ['file_id', 'report_date', 'subcontractor_raw', 'tier2_sc_dominant', 'record_count']

    # Merge with filenames
    file_stats = file_stats.merge(tbm_files[['file_id', 'filename']], on='file_id')

    # Normalize subcontractor from filename (for deduplication grouping)
    file_stats['subcontractor_for_dedup'] = file_stats['subcontractor_raw'].apply(normalize_subcontractor_from_filename)

    # For EML files (subcontractor_file='eml'), use tier2_sc from workbook instead
    # This ensures each actual subcontractor is grouped correctly
    file_stats['subcontractor_for_dedup'] = file_stats.apply(
        lambda row: normalize_tier2_sc(row['tier2_sc_dominant']) or row['subcontractor_for_dedup']
        if row['subcontractor_raw'] == 'eml' or row['subcontractor_for_dedup'] == 'eml'
        else row['subcontractor_for_dedup'],
        axis=1
    )

    # Extract date from filename
    file_stats['filename_date'] = file_stats['filename'].apply(extract_date_from_filename)

    # Compute flags for selection logic
    file_stats['date_matches'] = file_stats['report_date'] == file_stats['filename_date']
    file_stats['is_copy'] = file_stats['filename'].apply(is_copy_file)
    # Date mismatch: filename has a date that differs from internal date (data quality issue)
    file_stats['date_mismatch'] = (file_stats['filename_date'].notna()) & (~file_stats['date_matches'])

    # Find duplicate groups (same file subcontractor + report_date)
    dup_counts = file_stats.groupby(['subcontractor_for_dedup', 'report_date']).size().reset_index(name='group_size')
    file_stats = file_stats.merge(dup_counts, on=['subcontractor_for_dedup', 'report_date'])

    # Mark duplicates
    file_stats['is_duplicate'] = file_stats['group_size'] > 1

    # Assign duplicate group IDs
    file_stats['duplicate_group_id'] = None
    dup_groups = file_stats[file_stats['is_duplicate']].groupby(['subcontractor_for_dedup', 'report_date'])

    group_id = 0
    for (sub, date), group in dup_groups:
        group_id += 1
        file_stats.loc[group.index, 'duplicate_group_id'] = group_id

    # Determine preferred file in each duplicate group
    file_stats['is_preferred'] = True  # Default all to preferred

    for (sub, date), group in dup_groups:
        group_df = file_stats.loc[group.index].copy()

        # Score each file: higher is better
        # Priority: date_matches (100) > record_count (up to 99) > not_copy (1)
        group_df['score'] = (
            group_df['date_matches'].astype(int) * 100 +
            group_df['record_count'].clip(upper=99) +
            (~group_df['is_copy']).astype(int)
        )

        # Mark only the best as preferred
        best_idx = group_df['score'].idxmax()
        file_stats.loc[group.index, 'is_preferred'] = False
        file_stats.loc[best_idx, 'is_preferred'] = True

    # Merge flags back to entries (drop existing columns first for idempotency)
    flag_cols = ['file_id', 'is_duplicate', 'duplicate_group_id', 'is_preferred', 'date_mismatch']
    drop_cols = flag_cols + ['subcontractor_normalized']  # Also drop old subcontractor_normalized
    existing_cols = [c for c in drop_cols if c in df.columns and c != 'file_id']
    if existing_cols:
        df = df.drop(columns=existing_cols)
    df = df.merge(file_stats[flag_cols], on='file_id', how='left')

    # Fill any NaN values (shouldn't happen, but be safe)
    df['is_duplicate'] = df['is_duplicate'].fillna(False)
    df['is_preferred'] = df['is_preferred'].fillna(True)
    df['date_mismatch'] = df['date_mismatch'].fillna(False)

    # Set subcontractor_normalized from tier2_sc (actual workbook data) instead of filename
    df['subcontractor_normalized'] = df['tier2_sc'].apply(normalize_tier2_sc)

    # Second pass: Detect content duplicates (same tier2_sc + date in different filename folders)
    # This catches files misplaced in wrong folders
    file_content = df.groupby('file_id').agg({
        'report_date': 'first',
        'tier2_sc': lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else None,  # Dominant tier2_sc
        'num_employees': 'sum',
        'row_num': 'count',
        'is_preferred': 'first',
        'is_duplicate': 'first'
    }).reset_index()
    file_content.columns = ['file_id', 'date', 'dominant_tier2', 'total_mp', 'record_count', 'is_preferred', 'is_duplicate']
    file_content = file_content.merge(tbm_files[['file_id', 'filename']], on='file_id')

    # Find content duplicate groups (same dominant_tier2 + date + similar MP)
    content_dup_groups = file_content.groupby(['dominant_tier2', 'date']).filter(lambda x: len(x) > 1)

    if len(content_dup_groups) > 0:
        print(f"\nContent Duplicate Detection (same tier2_sc + date in different folders):")

        for (tier2, date), group in content_dup_groups.groupby(['dominant_tier2', 'date']):
            if len(group) <= 1:
                continue

            # Check if this is a real content duplicate (similar MP and record count)
            mp_values = group['total_mp'].values
            if len(mp_values) >= 2 and abs(mp_values[0] - mp_values[1]) < 50:  # Similar MP
                print(f"  {tier2} | {date}:")

                # Check which files have filename matching tier2_sc
                tier2_upper = str(tier2).upper() if tier2 else ''
                group = group.copy()
                group['matches_filename'] = group['filename'].str.upper().str.contains(tier2_upper, regex=False)

                # If ANY file has matching filename, mark non-matching as duplicates
                # If NO files match (e.g., EML files), keep the one with most records
                has_matching_file = group['matches_filename'].any()

                if has_matching_file:
                    # Standard case: prefer files where filename matches content
                    for _, row in group.iterrows():
                        if not row['matches_filename'] and row['is_preferred']:
                            df.loc[df['file_id'] == row['file_id'], 'is_preferred'] = False
                            df.loc[df['file_id'] == row['file_id'], 'is_duplicate'] = True
                            print(f"    [SKIP] {row['filename'][:55]} ({row['total_mp']:.0f} MP) - wrong folder")
                        else:
                            print(f"    [KEEP] {row['filename'][:55]} ({row['total_mp']:.0f} MP)")
                else:
                    # EML case: no filename matches, keep file with most records
                    best_idx = group['record_count'].idxmax()
                    for idx, row in group.iterrows():
                        if idx != best_idx and row['is_preferred']:
                            df.loc[df['file_id'] == row['file_id'], 'is_preferred'] = False
                            df.loc[df['file_id'] == row['file_id'], 'is_duplicate'] = True
                            print(f"    [SKIP] {row['filename'][:55]} ({row['total_mp']:.0f} MP) - duplicate")
                        else:
                            print(f"    [KEEP] {row['filename'][:55]} ({row['total_mp']:.0f} MP)")

    # Summary stats
    dup_files = file_stats[file_stats['is_duplicate']]
    mismatch_files = file_stats[file_stats['date_mismatch']]
    print(f"\nDeduplication Summary:")
    print(f"  Duplicate files: {len(dup_files)} in {dup_files['duplicate_group_id'].nunique()} groups")
    print(f"  Records in duplicates: {df[df['is_duplicate'] == True].shape[0]}")
    print(f"  Preferred files: {len(file_stats[file_stats['is_preferred']])}")
    print(f"  Records after dedup: {df[df['is_preferred'] == True].shape[0]}")
    print(f"\nDate Mismatch Summary:")
    print(f"  Files with date mismatch: {len(mismatch_files)}")
    print(f"  Records with date mismatch: {df[df['date_mismatch'] == True].shape[0]}")

    # Show duplicate groups
    if len(dup_files) > 0:
        print(f"\nDuplicate Groups:")
        for gid in sorted(dup_files['duplicate_group_id'].unique()):
            group = dup_files[dup_files['duplicate_group_id'] == gid]
            sub = group['subcontractor_for_dedup'].iloc[0]
            date = group['report_date'].iloc[0]
            print(f"\n  Group {gid}: {sub} | {date}")
            for _, row in group.iterrows():
                pref = "KEEP" if row['is_preferred'] else "    "
                match = "DATE_MATCH" if row['date_matches'] else ""
                copy = "COPY" if row['is_copy'] else ""
                flags = " ".join(filter(None, [match, copy]))
                flags_str = f" [{flags}]" if flags else ""
                print(f"    [{pref}] {row['record_count']:3d} records | {row['filename']}{flags_str}")

    # Show date mismatch files
    if len(mismatch_files) > 0:
        print(f"\nDate Mismatch Files (filename date ≠ internal date):")
        for _, row in mismatch_files.iterrows():
            print(f"  {row['filename'][:60]}")
            print(f"    Internal: {row['report_date']} | Filename: {row['filename_date']}")

    return df


def main():
    """Main entry point."""
    entries_path = Settings.TBM_PROCESSED_DIR / 'work_entries_enriched.csv'
    files_path = Settings.TBM_PROCESSED_DIR / 'tbm_files.csv'

    if not entries_path.exists():
        print(f"Error: {entries_path} not found. Run the parse stage first.")
        sys.exit(1)

    if not files_path.exists():
        print(f"Error: {files_path} not found. Run the parse stage first.")
        sys.exit(1)

    print("=== TBM Deduplication ===\n")

    df = deduplicate_tbm(entries_path, files_path)

    # Save back to same file
    df.to_csv(entries_path, index=False)
    print(f"\nSaved to: {entries_path}")
    print(f"Added columns: is_duplicate, duplicate_group_id, is_preferred, subcontractor_normalized, date_mismatch")


if __name__ == '__main__':
    main()
