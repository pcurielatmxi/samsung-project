#!/usr/bin/env python3
"""
Enhanced Room Timeline Analysis Tool with Narrative Search

Retrieves all work entries, inspections, AND relevant narrative documents for
a specific room within a time window, leveraging enriched embeddings metadata.

Key enhancements:
- Searches narratives using enriched location/CSI/date metadata
- Filters by building, level, and room code
- Shows relevant schedule narratives, weekly reports, and claims
- Cross-references with P6 schedule timing

Usage:
    python -m scripts.integrated_analysis.room_timeline_enhanced FAB116406 --start 2024-01-01 --end 2024-03-31
    python -m scripts.integrated_analysis.room_timeline_enhanced FAB116406 --start 2024-01-01 --end 2024-03-31 --output timeline.csv
    python -m scripts.integrated_analysis.room_timeline_enhanced FAB116406 --start 2024-01-01 --end 2024-03-31 --with-context
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Tuple

import pandas as pd

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.config.settings import Settings
from scripts.narratives.embeddings import search_chunks, get_store

settings = Settings()
PROCESSED_DIR = settings.PROCESSED_DATA_DIR


def parse_location_code(location_code: str) -> Dict[str, str]:
    """
    Parse FAB location code to extract building, level, room.

    Args:
        location_code: e.g., 'FAB116406'

    Returns:
        Dict with 'building', 'level', 'room_num'
    """
    if not location_code or not location_code.startswith('FAB1'):
        return {}

    # FAB1 + floor (1 digit) + area/room (4 digits)
    # e.g., FAB116406 = FAB building, 1F, room 6406
    if len(location_code) >= 6:
        floor = location_code[4]
        room_num = location_code[5:]

        # Determine building from room number patterns
        # SUE: 1xxx-2xxx, SUW: 3xxx-5xxx, FAB: 6xxx+, FIZ: varies
        room_int = int(room_num) if room_num.isdigit() else 0
        if 1000 <= room_int < 3000:
            building = 'SUE'
        elif 3000 <= room_int < 6000:
            building = 'SUW'
        elif room_int >= 6000:
            building = 'FAB'
        else:
            building = 'FAB'  # Default

        return {
            'building': building,
            'level': f'{floor}F',
            'room_num': room_num
        }

    return {}


def search_narratives_for_room(
    location_code: str,
    start_date: datetime,
    end_date: datetime,
    limit: int = 50
) -> List[Dict[str, Any]]:
    """
    Search narrative documents for mentions of a specific room.

    Uses enriched metadata to filter by location, then returns matching chunks.

    Args:
        location_code: Room code (e.g., 'FAB116406')
        start_date: Start of time window
        end_date: End of time window
        limit: Max chunks to return

    Returns:
        List of dicts with chunk info
    """
    store = get_store()
    collection = store.get_chunks_collection()

    # Parse location for metadata filtering
    loc_info = parse_location_code(location_code)

    # Build ChromaDB where filter
    where_conditions = {'source_type': 'narratives'}

    # Add date filter if available
    # Note: file_date is stored as string in YYYY-MM-DD format
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')

    # Search strategy:
    # 1. Exact location code match (doc_locations or chunk_locations contains code)
    # 2. Building + level match (doc_buildings + doc_levels)
    # 3. Semantic search with location code as query

    results = []

    # Strategy 1: Query for chunks with this exact location code
    # ChromaDB doesn't support "contains" well, so we'll query all narratives
    # and filter in Python
    query_results = collection.get(
        where=where_conditions,
        limit=10000,  # Get many to filter
        include=['documents', 'metadatas']
    )

    for i, chunk_id in enumerate(query_results['ids']):
        meta = query_results['metadatas'][i]
        text = query_results['documents'][i]

        # Check if location code appears in metadata or text
        doc_locations = meta.get('doc_locations', '')
        chunk_locations = meta.get('chunk_locations', '')

        # Check date range
        file_date_str = meta.get('file_date', '')
        if file_date_str:
            try:
                file_date = datetime.strptime(file_date_str, '%Y-%m-%d')
                if not (start_date <= file_date <= end_date):
                    continue
            except:
                pass

        # Check if location code is in metadata or text
        if (location_code in doc_locations or
            location_code in chunk_locations or
            location_code in text):

            results.append({
                'chunk_id': chunk_id,
                'source_file': meta.get('source_file', ''),
                'document_type': meta.get('document_type', ''),
                'file_date': file_date_str,
                'author': meta.get('author', ''),
                'text': text[:500],  # First 500 chars
                'page_number': meta.get('page_number', 0),
                'doc_buildings': meta.get('doc_buildings', ''),
                'doc_levels': meta.get('doc_levels', ''),
                'doc_locations': doc_locations,
                'doc_csi_sections': meta.get('doc_csi_sections', ''),
                'chunk_locations': chunk_locations,
            })

            if len(results) >= limit:
                break

    # Strategy 2: If few results, also search by building + level
    if len(results) < 20 and loc_info:
        building = loc_info.get('building', '')
        level = loc_info.get('level', '')

        for i, chunk_id in enumerate(query_results['ids']):
            if len(results) >= limit:
                break

            meta = query_results['metadatas'][i]
            text = query_results['documents'][i]

            # Skip if already added
            if chunk_id in [r['chunk_id'] for r in results]:
                continue

            # Check date
            file_date_str = meta.get('file_date', '')
            if file_date_str:
                try:
                    file_date = datetime.strptime(file_date_str, '%Y-%m-%d')
                    if not (start_date <= file_date <= end_date):
                        continue
                except:
                    pass

            # Check building + level match
            doc_buildings = meta.get('doc_buildings', '')
            doc_levels = meta.get('doc_levels', '')

            if building in doc_buildings and level in doc_levels:
                results.append({
                    'chunk_id': chunk_id,
                    'source_file': meta.get('source_file', ''),
                    'document_type': meta.get('document_type', ''),
                    'file_date': file_date_str,
                    'author': meta.get('author', ''),
                    'text': text[:500],
                    'page_number': meta.get('page_number', 0),
                    'doc_buildings': doc_buildings,
                    'doc_levels': doc_levels,
                    'doc_locations': meta.get('doc_locations', ''),
                    'doc_csi_sections': meta.get('doc_csi_sections', ''),
                    'chunk_locations': meta.get('chunk_locations', ''),
                })

    return sorted(results, key=lambda x: x['file_date'])


def load_quality_and_tbm_data(
    location_code: str,
    start_date: datetime,
    end_date: datetime
) -> pd.DataFrame:
    """
    Load quality inspection and TBM data for a specific room.

    Args:
        location_code: Room code
        start_date: Start date
        end_date: End date

    Returns:
        Combined DataFrame with quality + TBM data
    """
    # Load quality data
    raba_path = PROCESSED_DIR / 'raba' / 'raba_consolidated.csv'
    psi_path = PROCESSED_DIR / 'psi' / 'psi_consolidated.csv'
    tbm_path = PROCESSED_DIR / 'tbm' / 'work_entries_enriched.csv'

    dfs = []

    # RABA
    if raba_path.exists():
        raba = pd.read_csv(raba_path)
        raba['event_date'] = pd.to_datetime(raba['report_date_normalized'], errors='coerce')
        raba['source'] = 'RABA'

        # Filter by date and location
        raba = raba[
            (raba['event_date'] >= start_date) &
            (raba['event_date'] <= end_date)
        ]

        # Filter by affected_rooms (JSON array)
        raba['has_room'] = raba['affected_rooms'].fillna('[]').apply(
            lambda x: location_code in str(x)
        )
        raba = raba[raba['has_room']]

        if len(raba) > 0:
            dfs.append(raba[['source', 'event_date', 'inspection_type', 'outcome',
                             'contractor', 'trade', 'summary', 'source_file']])

    # PSI
    if psi_path.exists():
        psi = pd.read_csv(psi_path)
        psi['event_date'] = pd.to_datetime(psi['report_date_normalized'], errors='coerce')
        psi['source'] = 'PSI'

        psi = psi[
            (psi['event_date'] >= start_date) &
            (psi['event_date'] <= end_date)
        ]

        psi['has_room'] = psi['affected_rooms'].fillna('[]').apply(
            lambda x: location_code in str(x)
        )
        psi = psi[psi['has_room']]

        if len(psi) > 0:
            dfs.append(psi[['source', 'event_date', 'inspection_type', 'outcome',
                           'contractor', 'trade', 'summary', 'source_file']])

    # TBM
    if tbm_path.exists():
        tbm = pd.read_csv(tbm_path)
        tbm['event_date'] = pd.to_datetime(tbm['report_date'], errors='coerce')
        tbm['source'] = 'TBM'

        # TBM doesn't have affected_rooms, so check location_code column
        if 'location_code' in tbm.columns:
            tbm = tbm[
                (tbm['event_date'] >= start_date) &
                (tbm['event_date'] <= end_date) &
                (tbm['location_code'] == location_code)
            ]

            if len(tbm) > 0:
                # Add missing columns to match schema
                tbm['inspection_type'] = tbm.get('activity_description', '')
                tbm['outcome'] = 'N/A'
                tbm['contractor'] = tbm.get('dim_company_id', '')
                tbm['trade'] = tbm.get('dim_trade_code', '')
                tbm['summary'] = tbm.get('activity_description', '')

                dfs.append(tbm[['source', 'event_date', 'inspection_type', 'outcome',
                               'contractor', 'trade', 'summary', 'source_file']])

    if dfs:
        combined = pd.concat(dfs, ignore_index=True)
        combined = combined.sort_values('event_date')
        return combined
    else:
        return pd.DataFrame()


def main():
    parser = argparse.ArgumentParser(
        description='Enhanced room timeline with narrative search'
    )
    parser.add_argument(
        'location_code',
        help='Room location code (e.g., FAB116406)'
    )
    parser.add_argument(
        '--start',
        required=True,
        help='Start date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end',
        required=True,
        help='End date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--output',
        type=Path,
        help='Output CSV file path'
    )
    parser.add_argument(
        '--with-context',
        action='store_true',
        help='Include full chunk text in output'
    )

    args = parser.parse_args()

    # Parse dates
    try:
        start_date = datetime.strptime(args.start, '%Y-%m-%d')
        end_date = datetime.strptime(args.end, '%Y-%m-%d')
    except ValueError:
        print("Error: Dates must be in YYYY-MM-DD format")
        sys.exit(1)

    print(f"\nRoom Timeline Analysis: {args.location_code}")
    print(f"Date Range: {args.start} to {args.end}")
    print("=" * 70)

    # Parse location
    loc_info = parse_location_code(args.location_code)
    if loc_info:
        print(f"\nLocation Details:")
        print(f"  Building: {loc_info.get('building', 'Unknown')}")
        print(f"  Level: {loc_info.get('level', 'Unknown')}")
        print(f"  Room: {loc_info.get('room_num', 'Unknown')}")

    # Search narratives
    print(f"\n[1/2] Searching narrative documents...")
    narratives = search_narratives_for_room(args.location_code, start_date, end_date)
    print(f"  Found {len(narratives)} relevant narrative chunks")

    # Load quality + TBM data
    print(f"\n[2/2] Loading quality inspections and TBM data...")
    quality_tbm = load_quality_and_tbm_data(args.location_code, start_date, end_date)
    print(f"  Found {len(quality_tbm)} quality/TBM entries")

    # Display results
    print("\n" + "=" * 70)
    print("TIMELINE RESULTS")
    print("=" * 70)

    # Narratives
    if narratives:
        print(f"\n📄 NARRATIVE DOCUMENTS ({len(narratives)} chunks):")
        print("-" * 70)

        for i, chunk in enumerate(narratives, 1):
            print(f"\n[{i}] {chunk['file_date']} | {chunk['document_type']}")
            print(f"    File: {chunk['source_file']}")
            if chunk.get('author'):
                print(f"    Author: {chunk['author']}")
            print(f"    Locations: {chunk['doc_locations'] or chunk['chunk_locations'] or 'N/A'}")
            print(f"    Buildings: {chunk['doc_buildings']}")
            print(f"    Levels: {chunk['doc_levels']}")
            if chunk['doc_csi_sections']:
                print(f"    CSI: {chunk['doc_csi_sections']}")

            if args.with_context:
                print(f"\n    {chunk['text'][:300]}...")
    else:
        print("\n📄 No narrative documents found.")

    # Quality/TBM
    if len(quality_tbm) > 0:
        print(f"\n\n🔍 QUALITY INSPECTIONS & TBM WORK ({len(quality_tbm)} entries):")
        print("-" * 70)

        for idx, row in quality_tbm.iterrows():
            date_str = row['event_date'].strftime('%Y-%m-%d') if pd.notna(row['event_date']) else 'N/A'
            print(f"\n{row['source']} | {date_str}")
            print(f"  Type: {row['inspection_type']}")
            if pd.notna(row['outcome']) and row['outcome'] != 'N/A':
                print(f"  Outcome: {row['outcome']}")
            if pd.notna(row['contractor']):
                print(f"  Contractor: {row['contractor']}")
            if pd.notna(row['trade']):
                print(f"  Trade: {row['trade']}")
            if pd.notna(row['summary']) and str(row['summary']) != 'nan':
                summary = str(row['summary'])[:200]
                print(f"  Summary: {summary}...")
    else:
        print("\n\n🔍 No quality inspections or TBM entries found.")

    # Save to CSV if requested
    if args.output:
        # Combine narratives and quality data
        output_data = []

        for chunk in narratives:
            output_data.append({
                'source': 'Narrative',
                'date': chunk['file_date'],
                'document_type': chunk['document_type'],
                'file': chunk['source_file'],
                'author': chunk.get('author', ''),
                'locations': chunk['doc_locations'] or chunk['chunk_locations'],
                'buildings': chunk['doc_buildings'],
                'levels': chunk['doc_levels'],
                'csi': chunk['doc_csi_sections'],
                'text': chunk['text'] if args.with_context else chunk['text'][:200]
            })

        for _, row in quality_tbm.iterrows():
            date_str = row['event_date'].strftime('%Y-%m-%d') if pd.notna(row['event_date']) else ''
            output_data.append({
                'source': row['source'],
                'date': date_str,
                'document_type': row['inspection_type'],
                'file': row.get('source_file', ''),
                'author': '',
                'locations': args.location_code,
                'buildings': loc_info.get('building', ''),
                'levels': loc_info.get('level', ''),
                'csi': '',
                'text': str(row.get('summary', ''))
            })

        output_df = pd.DataFrame(output_data)
        output_df.to_csv(args.output, index=False)
        print(f"\n✓ Saved timeline to {args.output}")

    print("\n" + "=" * 70)


if __name__ == '__main__':
    main()
