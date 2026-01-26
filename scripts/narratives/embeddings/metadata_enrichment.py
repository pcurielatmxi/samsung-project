"""
Metadata enrichment for embeddings chunks.

Extracts structured metadata (locations, CSI codes, companies) from document text
and adds it to existing chunks WITHOUT re-embedding.

This enables structured filtering by location, CSI section, and company while
preserving existing embeddings.

Strategy:
    - Document-level extraction: Scan entire document, propagate to all chunks
    - Chunk-level extraction: Extract from individual chunk text
    - Both levels stored: Enables "company mentioned in chunk 2, issue in chunk 5" queries

Metadata Schema:
    Document-level (inherited by all chunks):
        - doc_companies: [company_id, ...] - All companies mentioned in document
        - doc_locations: [location_code, ...] - All location codes in document
        - doc_buildings: [building, ...] - All buildings mentioned
        - doc_levels: [level, ...] - All levels mentioned
        - doc_csi_codes: [code, ...] - All CSI codes in document
        - doc_csi_sections: [section, ...] - Unique CSI sections (2-digit)

    Chunk-level (specific to this chunk):
        - chunk_companies: [company_id, ...] - Companies in THIS chunk
        - chunk_locations: [location_code, ...] - Locations in THIS chunk
        - chunk_csi_codes: [code, ...] - CSI codes in THIS chunk
        - chunk_csi_sections: [section, ...] - CSI sections in THIS chunk

Usage:
    # Enrich all narratives
    python -m scripts.narratives.embeddings enrich --source narratives

    # Test on subset
    python -m scripts.narratives.embeddings enrich --source narratives --limit 100

    # Force re-enrichment
    python -m scripts.narratives.embeddings enrich --source narratives --force
"""

import re
from pathlib import Path
from typing import List, Dict, Any, Set, Optional
from dataclasses import dataclass


@dataclass
class EnrichedMetadata:
    """Extracted metadata from document or chunk."""

    # Location data
    location_codes: List[str]
    buildings: List[str]
    levels: List[str]

    # CSI codes
    csi_codes: List[str]
    csi_sections: List[str]

    # Companies
    company_ids: List[int]
    company_names: List[str]  # For debugging/display


# ============================================================================
# Location Extraction
# ============================================================================

# Room code patterns
ROOM_CODE_PATTERN = re.compile(r'\bFAB1[12345][0-9]{4}\b')  # FAB112345

# Building patterns
BUILDING_PATTERNS = [
    re.compile(r'\b(SUE|SUW|FAB|FIZ|FIS)\b', re.IGNORECASE),
    re.compile(r'\b(Sub[- ]?Fab[- ]?East|Sub[- ]?Fab[- ]?West)\b', re.IGNORECASE),
]

# Level patterns
LEVEL_PATTERNS = [
    re.compile(r'\b([12345]F|[12345]th Floor|Level [12345])\b', re.IGNORECASE),
    re.compile(r'\bL-?[12345]\b'),  # L-1, L1, L-2, etc.
]


def extract_location_codes(text: str) -> List[str]:
    """
    Extract FAB1XXXXX room codes from text.

    Examples:
        "Work at FAB116101 and FAB116102" -> ["FAB116101", "FAB116102"]
    """
    matches = ROOM_CODE_PATTERN.findall(text)
    return sorted(set(matches))


def extract_buildings(text: str) -> List[str]:
    """
    Extract building codes from text.

    Examples:
        "SUE building" -> ["SUE"]
        "Sub-Fab East" -> ["SUE"]
    """
    buildings = set()

    for pattern in BUILDING_PATTERNS:
        matches = pattern.findall(text)
        for match in matches:
            # Normalize variations
            building = match.upper()
            if 'SUB' in building and 'EAST' in building:
                building = 'SUE'
            elif 'SUB' in building and 'WEST' in building:
                building = 'SUW'
            buildings.add(building)

    return sorted(buildings)


def extract_levels(text: str) -> List[str]:
    """
    Extract level/floor references from text.

    Examples:
        "1F", "2nd floor", "Level 3", "L-1" -> ["1F", "2F", "3F", "1F"]
    """
    levels = set()

    for pattern in LEVEL_PATTERNS:
        matches = pattern.findall(text)
        for match in matches:
            # Normalize to XF format
            level = match.upper()
            if 'LEVEL' in level:
                level = level.replace('LEVEL', '').strip() + 'F'
            elif 'FLOOR' in level:
                level = level[0] + 'F'
            elif level.startswith('L-') or level.startswith('L'):
                level = level.replace('L-', '').replace('L', '') + 'F'
            elif 'TH' in level or 'ST' in level or 'ND' in level or 'RD' in level:
                level = level[0] + 'F'

            if level and level[0].isdigit():
                levels.add(level)

    return sorted(levels)


# ============================================================================
# CSI Code Extraction
# ============================================================================

# CSI code patterns
CSI_6DIGIT_PATTERN = re.compile(r'\b(\d{6})\b')  # 033053
CSI_SPACED_PATTERN = re.compile(r'\b(\d{2})\s+(\d{4})\b')  # 03 3053
CSI_SECTION_PATTERN = re.compile(r'\bCSI\s+(\d{2})\b', re.IGNORECASE)  # CSI 03

# Keyword to CSI section mapping (common trades)
CSI_KEYWORDS = {
    '03': ['concrete', 'formwork', 'rebar', 'slab', 'pour', 'curing'],
    '05': ['steel', 'structural steel', 'metal deck', 'joist'],
    '07': ['waterproofing', 'insulation', 'roofing', 'sealant'],
    '08': ['door', 'frame', 'window', 'glass', 'glazing'],
    '09': ['drywall', 'gypsum', 'ceiling', 'tile', 'paint', 'flooring'],
    '21': ['fire protection', 'sprinkler', 'standpipe'],
    '22': ['plumbing', 'pipe', 'domestic water'],
    '23': ['hvac', 'duct', 'mechanical', 'air handling'],
    '26': ['electrical', 'conduit', 'cable tray', 'panel', 'switchgear'],
    '27': ['communications', 'data', 'low voltage'],
    '28': ['fire alarm', 'security'],
}


def extract_csi_codes(text: str, include_keywords=False) -> tuple[List[str], List[str]]:
    """
    Extract CSI codes from text.

    Args:
        text: Text to search
        include_keywords: If True, infer CSI sections from keywords (e.g., "concrete" -> 03)

    Returns:
        (csi_codes, csi_sections) tuple

    Examples:
        "CSI 033053" -> (["033053"], ["03"])
        "03 3053 work" -> (["033053"], ["03"])
        "concrete placement" (with keywords) -> ([], ["03"])
    """
    csi_codes = set()

    # Extract 6-digit codes
    matches = CSI_6DIGIT_PATTERN.findall(text)
    csi_codes.update(matches)

    # Extract spaced format (03 3053 -> 033053)
    matches = CSI_SPACED_PATTERN.findall(text)
    for section, detail in matches:
        csi_codes.add(section + detail)

    # Extract section-only references (CSI 03)
    section_matches = CSI_SECTION_PATTERN.findall(text)

    # Get unique sections from codes
    csi_sections = set()
    for code in csi_codes:
        if len(code) >= 2:
            csi_sections.add(code[:2])
    csi_sections.update(section_matches)

    # Keyword-based inference (optional)
    if include_keywords:
        text_lower = text.lower()
        for section, keywords in CSI_KEYWORDS.items():
            if any(kw in text_lower for kw in keywords):
                csi_sections.add(section)

    return sorted(csi_codes), sorted(csi_sections)


# ============================================================================
# Company Extraction
# ============================================================================

def extract_companies(text: str, company_aliases: Dict[str, int]) -> tuple[List[int], List[str]]:
    """
    Extract company mentions from text and resolve to company IDs.

    Args:
        text: Text to search
        company_aliases: Map of {company_alias: company_id} from dim_company

    Returns:
        (company_ids, company_names) tuple

    Examples:
        "Yates and Berg crews..." -> ([1, 5], ["Yates", "Berg"])
    """
    company_ids = set()
    company_names = set()

    # Search for each alias in text
    for alias, company_id in company_aliases.items():
        # Case-insensitive word boundary search
        pattern = re.compile(r'\b' + re.escape(alias) + r'\b', re.IGNORECASE)
        if pattern.search(text):
            company_ids.add(company_id)
            company_names.add(alias)

    return sorted(company_ids), sorted(company_names)


# ============================================================================
# Document & Chunk Enrichment
# ============================================================================

def extract_document_metadata(
    text: str,
    company_aliases: Dict[str, int],
    include_csi_keywords: bool = True
) -> Dict[str, Any]:
    """
    Extract metadata from entire document (propagated to all chunks).

    Args:
        text: Full document text
        company_aliases: Company alias -> ID mapping
        include_csi_keywords: Include keyword-based CSI inference

    Returns:
        Metadata dict with doc_* keys (lists stored as pipe-separated strings for ChromaDB)
    """
    # Extract locations
    location_codes = extract_location_codes(text)
    buildings = extract_buildings(text)
    levels = extract_levels(text)

    # Extract CSI codes
    csi_codes, csi_sections = extract_csi_codes(text, include_keywords=include_csi_keywords)

    # Extract companies
    company_ids, company_names = extract_companies(text, company_aliases)

    # ChromaDB requires scalar values in metadata (no lists)
    # Store as pipe-separated strings
    return {
        'doc_locations': '|'.join(location_codes) if location_codes else '',
        'doc_buildings': '|'.join(buildings) if buildings else '',
        'doc_levels': '|'.join(levels) if levels else '',
        'doc_csi_codes': '|'.join(csi_codes) if csi_codes else '',
        'doc_csi_sections': '|'.join(csi_sections) if csi_sections else '',
        'doc_company_ids': '|'.join(map(str, company_ids)) if company_ids else '',
        'doc_company_names': '|'.join(company_names) if company_names else '',
    }


def extract_chunk_metadata(
    text: str,
    company_aliases: Dict[str, int],
    include_csi_keywords: bool = False  # More conservative for chunks
) -> Dict[str, Any]:
    """
    Extract metadata from individual chunk (specific mentions).

    Args:
        text: Chunk text
        company_aliases: Company alias -> ID mapping
        include_csi_keywords: Include keyword-based CSI inference

    Returns:
        Metadata dict with chunk_* keys (lists stored as pipe-separated strings for ChromaDB)
    """
    # Extract locations
    location_codes = extract_location_codes(text)
    buildings = extract_buildings(text)
    levels = extract_levels(text)

    # Extract CSI codes
    csi_codes, csi_sections = extract_csi_codes(text, include_keywords=include_csi_keywords)

    # Extract companies
    company_ids, company_names = extract_companies(text, company_aliases)

    # ChromaDB requires scalar values in metadata (no lists)
    # Store as pipe-separated strings
    return {
        'chunk_locations': '|'.join(location_codes) if location_codes else '',
        'chunk_buildings': '|'.join(buildings) if buildings else '',
        'chunk_levels': '|'.join(levels) if levels else '',
        'chunk_csi_codes': '|'.join(csi_codes) if csi_codes else '',
        'chunk_csi_sections': '|'.join(csi_sections) if csi_sections else '',
        'chunk_company_ids': '|'.join(map(str, company_ids)) if company_ids else '',
        'chunk_company_names': '|'.join(company_names) if company_names else '',
    }


def load_company_aliases() -> Dict[str, int]:
    """
    Load company aliases from dim_company.csv.

    Returns:
        Dict mapping company alias -> company_id
    """
    from src.config.settings import settings
    import pandas as pd

    dim_company_path = settings.INTEGRATED_PROCESSED_DIR / 'dimensions' / 'dim_company.csv'

    if not dim_company_path.exists():
        print(f"Warning: dim_company.csv not found at {dim_company_path}")
        print("Company extraction will be skipped. Run build_dim_company.py first.")
        return {}

    df = pd.read_csv(dim_company_path)

    # Build alias map
    alias_map = {}
    for _, row in df.iterrows():
        company_id = row['company_id']
        canonical_name = row['canonical_name']

        # Add canonical name
        alias_map[canonical_name] = company_id

        # Add all aliases (if aliases column exists)
        if 'aliases' in df.columns and pd.notna(row['aliases']):
            aliases = row['aliases'].split('|')
            for alias in aliases:
                alias_map[alias.strip()] = company_id

    return alias_map
