"""
Building and Level Normalization

Consolidates all building/level normalization logic that was previously
scattered across multiple files:
- enrich_with_dimensions.py: normalize_level_value()
- fieldwire/enrich_tbm.py: normalize_building(), normalize_level()
- company_standardization.py: standardize_level()

IMPORTANT: This is the single source of truth for building/level normalization.
All other code should import from here.
"""

import re
from typing import Optional

import pandas as pd


def normalize_level(level: str) -> Optional[str]:
    """
    Normalize level values across all sources for consistent spatial filtering.

    Standardizes to format: B2, B1, 1F, 2F, 3F, 4F, 5F, 6F, 7F, ROOF, OUTSIDE

    Handles variations from:
    - RABA/PSI: "1st Floor", "Level 1", "1F", "First Floor"
    - TBM: "1", "01F", "BASEMENT", "ROOF"
    - Fieldwire: "1", "2", "3" (numeric)
    - P6: "L1", "L2", "LVL 4"

    Args:
        level: Raw level value from any source

    Returns:
        Normalized level string or None if invalid
    """
    if pd.isna(level) or level is None:
        return None

    level = str(level).upper().strip()

    # Handle empty string
    if not level:
        return None

    # Remove common prefixes/suffixes
    level = level.replace('LEVEL ', '').replace('LVL ', '').replace('L-', '')
    level = re.sub(r'^L(\d)', r'\1', level)  # L1 -> 1, L2 -> 2

    # Handle basement variations
    basement_patterns = {
        'B1': ['B1', 'B1F', 'BASEMENT', 'BASEMENT 1', '1B', 'BASEMENT1', 'UG', 'UNDERGROUND'],
        'B2': ['B2', 'B2F', 'BASEMENT 2', '2B', 'BASEMENT2'],
    }
    for normalized, patterns in basement_patterns.items():
        if level in patterns:
            return normalized

    # Handle roof variations
    roof_patterns = ['ROOF', 'RF', 'ROOFTOP', 'R', 'RTF', 'ROOFING']
    if level in roof_patterns:
        return 'ROOF'

    # Handle outside/ground variations
    outside_patterns = ['OUTSIDE', 'EXTERIOR', 'GROUND', 'EXT', 'SITE', 'YARD']
    if any(x in level for x in outside_patterns):
        return 'OUTSIDE'

    # Handle penthouse
    if 'PENTHOUSE' in level or 'PH' == level:
        return 'ROOF'  # Map penthouse to roof

    # Handle floor number variations (1F, 01F, 1ST, FIRST, etc.)
    # Pattern: optional leading zero, digit(s), optional F
    m = re.match(r'^0?(\d+)[F]?$', level)
    if m:
        return f"{int(m.group(1))}F"

    # Handle ordinal formats (1ST, 2ND, 3RD, etc.)
    m = re.match(r'^(\d+)(ST|ND|RD|TH)?\s*(FLOOR)?$', level)
    if m:
        return f"{int(m.group(1))}F"

    # Handle word formats (FIRST, SECOND, etc.)
    word_to_num = {
        'FIRST': '1F', 'SECOND': '2F', 'THIRD': '3F', 'FOURTH': '4F',
        'FIFTH': '5F', 'SIXTH': '6F', 'SEVENTH': '7F'
    }
    for word, num in word_to_num.items():
        if word in level:
            return num

    # Return as-is if numeric (with F suffix)
    if level.isdigit():
        return f"{level}F"

    # Handle float levels like "3.0" -> "3F"
    try:
        level_float = float(level)
        if level_float == int(level_float):
            return f"{int(level_float)}F"
    except ValueError:
        pass

    # Already in correct format?
    if re.match(r'^\d+F$', level):
        return level

    # Return as-is for unrecognized (may need manual review)
    return level


def normalize_building(building: str) -> Optional[str]:
    """
    Normalize building names from all sources to standard codes.

    Standard codes: FAB, SUE, SUW, FIZ, OB1, GCS, CUB, SUP

    Handles variations from:
    - Fieldwire: "FAB1", "Main FAB", "T1 FAB"
    - RABA/PSI: "FAB", "SUE", "SUW"
    - TBM: "FAB", "SUW", "SUE"
    - P6: Building codes in task names

    Args:
        building: Raw building name from any source

    Returns:
        Normalized building code or None if invalid
    """
    if not building or pd.isna(building):
        return None

    building = str(building).strip().upper()

    # Handle empty string
    if not building:
        return None

    # Direct mappings - comprehensive list
    building_map = {
        # FAB variations
        'FAB': 'FAB',
        'FAB1': 'FAB',
        'MAIN FAB': 'FAB',
        'MAIN FAB1': 'FAB',
        'T1 FAB': 'FAB',
        'T1 FAB1': 'FAB',
        'T1': 'FAB',
        'TAYLOR FAB': 'FAB',
        'TAYLOR FAB1': 'FAB',
        'FAB BUILDING': 'FAB',

        # Support buildings
        'SUP': 'SUP',
        'SUE': 'SUE',
        'SUW': 'SUW',
        'SUPPORT EAST': 'SUE',
        'SUPPORT WEST': 'SUW',
        'SUE/SUW': 'SUP',  # Both support buildings
        'SUW/SUE': 'SUP',

        # Other buildings
        'FIZ': 'FIZ',
        'FIZZ': 'FIZ',
        'CUB': 'CUB',
        'OB1': 'OB1',
        'OFFICE': 'OB1',
        'GCS': 'GCS',
        'GCSA': 'GCS',
        'GCSB': 'GCS',
        'GCS A': 'GCS',
        'GCS B': 'GCS',
    }

    # Try direct match
    if building in building_map:
        return building_map[building]

    # Try partial match for compound names
    for key, value in building_map.items():
        if key in building:
            return value

    # Return as-is if not recognized (may be valid code we don't know)
    return building


def normalize_building_level(building: str, level: str) -> Optional[str]:
    """
    Create a standardized building-level string.

    Args:
        building: Building code (raw or normalized)
        level: Level code (raw or normalized)

    Returns:
        Building-level string like "FAB-2F" or None if either is invalid
    """
    building_norm = normalize_building(building)
    level_norm = normalize_level(level)

    if not building_norm or not level_norm:
        return None

    return f"{building_norm}-{level_norm}"
