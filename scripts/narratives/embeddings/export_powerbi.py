#!/usr/bin/env python3
"""
Export narrative chunks to Power BI-compatible CSV format.

Creates fact table with enriched metadata (locations, CSI codes, companies)
suitable for Power BI relationships with dim_location, dim_csi_section, dim_company.

Usage:
    python -m scripts.narratives.embeddings.export_powerbi
    python -m scripts.narratives.embeddings.export_powerbi --explode-locations
    python -m scripts.narratives.embeddings.export_powerbi --output custom.csv
"""

import argparse
import sys
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from src.config.settings import Settings
from scripts.narratives.embeddings import get_store

settings = Settings()


def export_chunks_flat(output_path: Path, limit: int = None) -> Dict[str, Any]:
    """
    Export chunks as flat table (one row per chunk, pipe-separated values).

    Suitable for Power BI with PATHCONTAINS or similar functions to filter
    multi-valued fields.

    Args:
        output_path: Output CSV path
        limit: Optional limit for testing

    Returns:
        Export stats
    """
    store = get_store()
    collection = store.get_chunks_collection()

    # Get all narrative chunks
    results = collection.get(
        where={'source_type': 'narratives'},
        limit=limit or 50000,
        include=['documents', 'metadatas']
    )

    records = []
    for i, chunk_id in enumerate(results['ids']):
        meta = results['metadatas'][i]
        text = results['documents'][i]

        records.append({
            # Primary key
            'chunk_id': chunk_id,

            # Text content
            'text': text,
            'text_length': len(text),

            # Source document info
            'source_file': meta.get('source_file', ''),
            'document_type': meta.get('document_type', ''),
            'file_date': meta.get('file_date', ''),
            'author': meta.get('author', ''),
            'page_number': meta.get('page_number', 0),
            'chunk_index': meta.get('chunk_index', 0),
            'total_chunks': meta.get('total_chunks', 0),

            # Document-level metadata (pipe-separated)
            'doc_locations': meta.get('doc_locations', ''),
            'doc_buildings': meta.get('doc_buildings', ''),
            'doc_levels': meta.get('doc_levels', ''),
            'doc_csi_codes': meta.get('doc_csi_codes', ''),
            'doc_csi_sections': meta.get('doc_csi_sections', ''),
            'doc_company_ids': meta.get('doc_company_ids', ''),
            'doc_company_names': meta.get('doc_company_names', ''),

            # Chunk-level metadata (pipe-separated)
            'chunk_locations': meta.get('chunk_locations', ''),
            'chunk_buildings': meta.get('chunk_buildings', ''),
            'chunk_levels': meta.get('chunk_levels', ''),
            'chunk_csi_codes': meta.get('chunk_csi_codes', ''),
            'chunk_csi_sections': meta.get('chunk_csi_sections', ''),
            'chunk_company_ids': meta.get('chunk_company_ids', ''),
            'chunk_company_names': meta.get('chunk_company_names', ''),
        })

    df = pd.DataFrame(records)
    df.to_csv(output_path, index=False)

    # Calculate stats
    stats = {
        'total_chunks': len(df),
        'with_doc_locations': (df['doc_locations'] != '').sum(),
        'with_doc_csi': (df['doc_csi_sections'] != '').sum(),
        'with_doc_companies': (df['doc_company_ids'] != '').sum(),
        'with_chunk_locations': (df['chunk_locations'] != '').sum(),
        'with_chunk_csi': (df['chunk_csi_sections'] != '').sum(),
        'date_range': f"{df['file_date'].min()} to {df['file_date'].max()}",
        'output_file': str(output_path),
    }

    return stats


def export_chunks_exploded(output_path: Path, limit: int = None) -> Dict[str, Any]:
    """
    Export chunks with exploded locations (one row per chunk-location pair).

    Creates normalized fact table for Power BI relationships.
    If a chunk mentions multiple locations, it appears multiple times.

    Args:
        output_path: Output CSV path
        limit: Optional limit for testing

    Returns:
        Export stats
    """
    store = get_store()
    collection = store.get_chunks_collection()

    # Get all narrative chunks
    results = collection.get(
        where={'source_type': 'narratives'},
        limit=limit or 50000,
        include=['documents', 'metadatas']
    )

    records = []
    for i, chunk_id in enumerate(results['ids']):
        meta = results['metadatas'][i]
        text = results['documents'][i]

        # Combine doc and chunk locations
        all_locations = set()

        doc_locs = meta.get('doc_locations', '').split('|')
        chunk_locs = meta.get('chunk_locations', '').split('|')

        all_locations.update([loc for loc in doc_locs if loc])
        all_locations.update([loc for loc in chunk_locs if loc])

        # If no locations, create one record with NULL location
        if not all_locations:
            all_locations = [None]

        # Create one record per location
        for location_code in all_locations:
            # Parse building/level from location code
            building = None
            level = None
            if location_code and location_code.startswith('FAB1'):
                # Extract building/level from doc metadata or location code
                doc_buildings = meta.get('doc_buildings', '')
                doc_levels = meta.get('doc_levels', '')

                # Try to infer from code
                if len(location_code) >= 6:
                    floor_char = location_code[4]
                    level = f"{floor_char}F"

                    room_num = location_code[5:]
                    if room_num.isdigit():
                        room_int = int(room_num)
                        if 1000 <= room_int < 3000:
                            building = 'SUE'
                        elif 3000 <= room_int < 6000:
                            building = 'SUW'
                        elif room_int >= 6000:
                            building = 'FAB'

            # Get CSI sections (prefer chunk-level, fallback to doc-level)
            chunk_csi = meta.get('chunk_csi_sections', '')
            doc_csi = meta.get('doc_csi_sections', '')
            csi_sections = chunk_csi if chunk_csi else doc_csi

            records.append({
                # Composite key (chunk_id + location_code)
                'chunk_id': chunk_id,
                'location_code': location_code,

                # Text content
                'text': text,
                'text_length': len(text),

                # Source document info
                'source_file': meta.get('source_file', ''),
                'document_type': meta.get('document_type', ''),
                'file_date': meta.get('file_date', ''),
                'author': meta.get('author', ''),
                'page_number': meta.get('page_number', 0),
                'chunk_index': meta.get('chunk_index', 0),

                # Location info
                'building': building,
                'level': level,

                # CSI sections (still pipe-separated)
                'csi_sections': csi_sections,

                # Metadata flags
                'location_source': 'chunk' if location_code in chunk_locs else 'doc',
                'is_multi_location': len(all_locations) > 1,
            })

    df = pd.DataFrame(records)
    df.to_csv(output_path, index=False)

    # Calculate stats
    stats = {
        'total_rows': len(df),
        'unique_chunks': df['chunk_id'].nunique(),
        'unique_locations': df['location_code'].notna().sum(),
        'multi_location_chunks': df['is_multi_location'].sum(),
        'with_csi': (df['csi_sections'] != '').sum(),
        'date_range': f"{df['file_date'].min()} to {df['file_date'].max()}",
        'output_file': str(output_path),
    }

    return stats


def main():
    parser = argparse.ArgumentParser(
        description='Export narrative chunks for Power BI'
    )
    parser.add_argument(
        '--output',
        type=Path,
        help='Output CSV path (default: fact_narrative_chunks.csv)'
    )
    parser.add_argument(
        '--explode-locations',
        action='store_true',
        help='Explode to one row per chunk-location pair (normalized)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of chunks for testing'
    )

    args = parser.parse_args()

    # Determine output path
    if args.output:
        output_path = args.output
    else:
        output_dir = settings.PROCESSED_DATA_DIR / 'narratives'
        output_dir.mkdir(parents=True, exist_ok=True)

        if args.explode_locations:
            output_path = output_dir / 'fact_narrative_chunks_exploded.csv'
        else:
            output_path = output_dir / 'fact_narrative_chunks.csv'

    print(f"\nExporting narrative chunks to Power BI format...")
    print(f"Output: {output_path}")
    print(f"Mode: {'Exploded (one row per location)' if args.explode_locations else 'Flat (pipe-separated values)'}")
    print("=" * 70)

    # Export
    if args.explode_locations:
        stats = export_chunks_exploded(output_path, args.limit)
    else:
        stats = export_chunks_flat(output_path, args.limit)

    # Print stats
    print(f"\n✓ Export complete")
    print(f"\nStats:")
    for key, value in stats.items():
        print(f"  {key}: {value}")

    # Usage guidance
    print(f"\n{'='*70}")
    print("Power BI Usage:")
    print("=" * 70)

    if args.explode_locations:
        print("""
1. Load this CSV as a fact table
2. Create relationships:
   - fact_narrative_chunks_exploded[location_code] -> dim_location[location_code]
   - fact_narrative_chunks_exploded[file_date] -> dim_date[date]

3. For CSI sections (still pipe-separated):
   - Use DAX: PATHCONTAINS(fact_narrative_chunks_exploded[csi_sections], "03")
   - Or create bridge table to normalize further
""")
    else:
        print("""
1. Load this CSV as a fact table
2. Create relationships:
   - Use dim_location[location_code] with PATHCONTAINS for doc_locations/chunk_locations
   - fact_narrative_chunks[file_date] -> dim_date[date]

3. Example DAX measures:
   - Chunks for Location =
       CALCULATE(
           COUNTROWS(fact_narrative_chunks),
           PATHCONTAINS(fact_narrative_chunks[doc_locations], dim_location[location_code])
       )

   - Chunks for CSI Section =
       CALCULATE(
           COUNTROWS(fact_narrative_chunks),
           PATHCONTAINS(fact_narrative_chunks[doc_csi_sections], dim_csi_section[csi_section])
       )
""")


if __name__ == '__main__':
    main()
