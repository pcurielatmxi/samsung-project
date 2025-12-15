#!/usr/bin/env python3
"""
Process ProjectSight library structure JSON files into a consolidated files table.

Creates a CSV with all files from all projects, with path tiers for filtering.
"""

import json
import pandas as pd
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(project_root))

from src.config.settings import Settings


def parse_path_tiers(path: str, num_tiers: int = 4) -> dict:
    """
    Parse a file path into tier columns.

    Example: /Fireproofing quantities/RPC Billing/file.pdf
    -> tier1: Fireproofing quantities
    -> tier2: RPC Billing
    -> tier3: file.pdf
    -> tier4: None
    """
    # Remove leading slash and split
    parts = path.lstrip('/').split('/')

    tiers = {}
    for i in range(num_tiers):
        tier_key = f'tier{i + 1}'
        tiers[tier_key] = parts[i] if i < len(parts) else None

    return tiers


def load_library_structure(json_path: Path) -> tuple[str, list[dict]]:
    """Load library structure JSON and return project name and items."""
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    project_name = data.get('project', {}).get('name', json_path.stem)
    items = data.get('items', [])

    return project_name, items


def process_library_files(input_dir: Path, output_path: Path) -> pd.DataFrame:
    """
    Process all library structure JSON files into a consolidated files table.

    Args:
        input_dir: Directory containing library_structure_*.json files
        output_path: Path to output CSV file

    Returns:
        DataFrame with all files
    """
    # Find all library structure files
    json_files = list(input_dir.glob('library_structure_*.json'))

    if not json_files:
        print(f"No library structure files found in {input_dir}")
        return pd.DataFrame()

    print(f"Found {len(json_files)} library structure file(s)")

    all_files = []

    for json_path in json_files:
        print(f"  Processing: {json_path.name}")
        project_name, items = load_library_structure(json_path)

        # Extract project key from filename (e.g., taylor_fab1 from library_structure_taylor_fab1.json)
        project_key = json_path.stem.replace('library_structure_', '')

        # Filter to files only
        files = [item for item in items if item.get('type') == 'file']
        print(f"    Found {len(files)} files (from {len(items)} total items)")

        for file_item in files:
            path = file_item.get('path', '')
            tiers = parse_path_tiers(path)

            file_record = {
                'project': project_key,
                'project_name': project_name,
                'name': file_item.get('name'),
                'path': path,
                'depth': file_item.get('depth'),
                'file_id': file_item.get('fileId'),
                'parent_folder_id': file_item.get('parentFolderId'),
                'href': file_item.get('href'),
                **tiers
            }
            all_files.append(file_record)

    # Create DataFrame
    df = pd.DataFrame(all_files)

    # Reorder columns
    column_order = [
        'project', 'project_name', 'name', 'path',
        'tier1', 'tier2', 'tier3', 'tier4',
        'depth', 'file_id', 'parent_folder_id', 'href'
    ]
    df = df[[col for col in column_order if col in df.columns]]

    # Sort by project, then path
    df = df.sort_values(['project', 'path']).reset_index(drop=True)

    # Save to CSV
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"\nSaved {len(df)} files to: {output_path}")

    # Print summary
    print("\nSummary by project:")
    for project, count in df['project'].value_counts().items():
        print(f"  {project}: {count} files")

    print("\nSummary by tier1 (top-level folder):")
    tier1_counts = df.groupby(['project', 'tier1']).size().reset_index(name='count')
    for _, row in tier1_counts.head(20).iterrows():
        print(f"  [{row['project']}] {row['tier1']}: {row['count']} files")
    if len(tier1_counts) > 20:
        print(f"  ... and {len(tier1_counts) - 20} more")

    return df


def main():
    """Main entry point."""
    input_dir = Settings.PROJECTSIGHT_RAW_DIR / 'extracted'
    output_path = Settings.PROJECTSIGHT_PROCESSED_DIR / 'library_files.csv'

    print("=" * 60)
    print("ProjectSight Library Files Processor")
    print("=" * 60)
    print(f"Input directory: {input_dir}")
    print(f"Output file: {output_path}")
    print()

    df = process_library_files(input_dir, output_path)

    if not df.empty:
        print("\n" + "=" * 60)
        print("Processing complete!")
        print("=" * 60)


if __name__ == '__main__':
    main()
