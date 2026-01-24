#!/usr/bin/env python3
"""
Enrich Data Sources with Dimension IDs

Adds dim_location_id, dim_company_id, dim_trade_id to all processed data sources.
Creates enriched copies of each file with _enriched suffix.

Usage:
    python scripts/integrated_analysis/enrich_with_dimensions.py
    python scripts/integrated_analysis/enrich_with_dimensions.py --source tbm
    python scripts/integrated_analysis/enrich_with_dimensions.py --dry-run
"""

import argparse
import re
import sys
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

import pandas as pd

# Add project root to path
_project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import Settings
from schemas.validator import validated_df_to_csv
from scripts.shared.dimension_lookup import (
    get_location_id,
    get_company_id,
    get_trade_id,
    get_trade_code,
    get_affected_rooms,
    reset_cache,
)
from scripts.integrated_analysis.add_csi_to_tbm import (
    infer_csi_from_activity,
    CSI_SECTIONS as TBM_CSI_SECTIONS,
)
from scripts.integrated_analysis.add_csi_to_projectsight import (
    infer_csi_from_projectsight,
    CSI_SECTIONS as PS_CSI_SECTIONS,
)
import json


def normalize_level_value(level: str) -> Optional[str]:
    """
    Normalize level values across all sources for consistent spatial filtering.

    Standardizes to format: B2, B1, 1F, 2F, 3F, 4F, 5F, 6F, 7F, ROOF, OUTSIDE

    Args:
        level: Raw level value from any source

    Returns:
        Normalized level string or None if invalid
    """
    if pd.isna(level):
        return None

    level = str(level).upper().strip()

    # Remove common prefixes/suffixes
    level = level.replace('LEVEL ', '').replace('LVL ', '').replace('L-', '')

    # Handle basement variations
    if level in ('B1', 'B1F', 'BASEMENT', 'BASEMENT 1', '1B'):
        return 'B1'
    if level in ('B2', 'B2F', 'BASEMENT 2', '2B'):
        return 'B2'
    if level in ('UG', 'UNDERGROUND'):
        return 'B1'  # Map underground to B1

    # Handle roof variations
    if level in ('ROOF', 'RF', 'ROOFTOP', 'R', 'RTF'):
        return 'ROOF'

    # Handle outside/ground variations
    if any(x in level for x in ['OUTSIDE', 'EXTERIOR', 'GROUND']):
        return 'OUTSIDE'

    # Handle floor number variations (1F, 01F, 1ST, FIRST, etc.)
    m = re.match(r'^0?(\d+)[F]?$', level)
    if m:
        return f"{int(m.group(1))}F"

    # Handle ordinal formats (1ST, 2ND, 3RD, etc.)
    m = re.match(r'^(\d+)(ST|ND|RD|TH)?\s*(FLOOR)?$', level)
    if m:
        return f"{int(m.group(1))}F"

    # Return as-is if no pattern matched (with F suffix if numeric)
    if level.isdigit():
        return f"{level}F"

    return level


def parse_tbm_grid(location_row: str) -> Dict[str, Any]:
    """
    Parse TBM location_row field into normalized grid components.

    Args:
        location_row: Raw location string from TBM (e.g., "G/H", "J11", "A-N/1-3")

    Returns:
        Dict with keys:
            grid_row_min, grid_row_max: Letter(s) A-N or None
            grid_col_min, grid_col_max: Numbers 1-33 or None
            grid_raw: Original value
            grid_type: POINT, ROW_ONLY, COL_ONLY, RANGE, AREA, NAMED, UNPARSED
    """
    result = {
        'grid_row_min': None,
        'grid_row_max': None,
        'grid_col_min': None,
        'grid_col_max': None,
        'grid_raw': location_row,
        'grid_type': 'UNPARSED',
    }

    if pd.isna(location_row):
        result['grid_type'] = 'EMPTY'
        return result

    val = str(location_row).strip().upper()

    # Handle empty string after stripping
    if not val or val == 'NAN':
        result['grid_type'] = 'EMPTY'
        return result

    # Strip leading/trailing whitespace and clean up
    val = ' '.join(val.split())  # Normalize multiple spaces

    # ==========================================================================
    # SUW/SUE patterns with grid coordinates (MUST be checked before prefix stripping)
    # ==========================================================================

    # SUW/SUE col-col/row-row: SUW 5-6/L-N, SUE 5-20/D
    m = re.match(r'^(SUW|SUE)\s+(\d+)[-–](\d+)[/]([A-N])[-–]?([A-N])?', val)
    if m:
        cols = sorted([float(m.group(2)), float(m.group(3))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_row_min'] = m.group(4)
        result['grid_row_max'] = m.group(5) or m.group(4)
        result['grid_type'] = 'RANGE'
        return result

    # SUW col/row format: SUW 28-24/L
    m = re.match(r'^(SUW|SUE)\s+(\d+)[-–]?(\d+)?[/]([A-N])\s*$', val)
    if m:
        result['grid_col_min'] = float(m.group(2))
        result['grid_col_max'] = float(m.group(3) or m.group(2))
        if result['grid_col_min'] > result['grid_col_max']:
            result['grid_col_min'], result['grid_col_max'] = result['grid_col_max'], result['grid_col_min']
        result['grid_row_min'] = result['grid_row_max'] = m.group(4)
        result['grid_type'] = 'RANGE'
        return result

    # SUW/SUE col/row with trailing text: SUW 28-24/L ...
    m = re.match(r'^(SUW|SUE)\s+(\d+)[-–]?(\d+)?[/]([A-N])\s+', val)
    if m:
        result['grid_col_min'] = float(m.group(2))
        result['grid_col_max'] = float(m.group(3) or m.group(2))
        if result['grid_col_min'] > result['grid_col_max']:
            result['grid_col_min'], result['grid_col_max'] = result['grid_col_max'], result['grid_col_min']
        result['grid_row_min'] = result['grid_row_max'] = m.group(4)
        result['grid_type'] = 'RANGE'
        return result

    # FAB/SUW/SUE whole-building patterns (MUST be checked before prefix stripping)
    if re.search(r'^(FAB\s+FLOOR|FAB\s+ALL|SUW/SUE)', val):
        result['grid_type'] = 'AREA'
        return result

    # SUW/SUE + location description: SUE Elect. Room (MUST be before prefix stripping)
    if re.match(r'^(SUW|SUE)\s+[A-Z]', val) and not re.match(r'^(SUW|SUE)\s+\d', val):
        result['grid_type'] = 'NAMED'
        return result

    # FAB with spaced dash: fab 3 - 25 → cols 3-25 (MUST be before prefix stripping)
    m = re.match(r'^FAB\s+(\d+)\s*[-–]\s*(\d+)', val)
    if m:
        cols = sorted([float(m.group(1)), float(m.group(2))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'COL_ONLY'
        return result

    # L## + description: L19 CJ, L28 DS (MUST be before prefix stripping)
    m = re.match(r'^L(\d+)\s+([A-Z]{2,})', val)
    if m:
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(1))
        result['grid_type'] = 'COL_ONLY'
        return result

    # Strip level prefixes like "L1- ", "1F-", "L1- SUW" but NOT gridline refs like "L18 (Stair)"
    # Require dash or comma after number, or F suffix, to distinguish from gridlines
    val = re.sub(r'^L?\d+[F][-\s]+', '', val).strip()  # "1F- " or "L1F- "
    val = re.sub(r'^L\d+[-]\s*', '', val).strip()  # "L1- " or "L2- "
    val = re.sub(r'^\d+[-,]\s*', '', val).strip()  # "1- " or "1, "
    val = re.sub(r'^LVL\s*\d+\s*', '', val, flags=re.IGNORECASE).strip()  # "Lvl 4 "
    val = re.sub(r'^L\d+[E]?\s*[-]\s*', '', val).strip()  # "L4E- " or "L3-" with space

    # Strip building prefixes like "SUW ", "SUE ", "FAB "
    val = re.sub(r'^(SUW|SUE|FAB)\s+', '', val).strip()
    # Also strip level in building prefix like "L4 N/17" but NOT "L18 (Stair)" gridline refs
    # Only strip if followed by a grid letter or digit (not parenthesis)
    val = re.sub(r'^L\d+\s+(?=[A-N\d])', '', val).strip()

    # Multi-level references like "L1,2,3,4" should be AREA
    if re.match(r'^L?\d+[,\d]+$', val):
        result['grid_type'] = 'AREA'
        return result

    # Skip descriptive values (whole building/area references)
    if re.search(r'^(ALL|VARIOUS|WHOLE|WORKING|OUTSIDE|LAYDOWN|QC\b|ACM|FIZZ|FIZ$|SUW\s*$|SUE\s*$|SUW/SUE|FAB\s|NORTH|SOUTH|EAST|WEST|NW\s|NE\s|SW\s|SE\s|DATA CENTER|PENTHOUSE|PENDHOUSE|THROUGHOUT|AREAS?\s|BUIDLING|BUILDING|LEVEL\s+\d|NS\s+TROUGH|FAB\s+FLOOR)', val):
        result['grid_type'] = 'AREA'
        return result

    # Named locations (stair, elevator, vestibule, room names, equipment)
    # Note: L\d+ patterns handled separately to capture gridline coordinate
    if re.search(r'^(STAIR|ELEVATOR|EELV|VESTIBULE|ELEV\b|ELECTRICAL|TQRLAB|BUNKER|COPING|AIRLOCK|AIR\s*LOCK|PASSAGE|CANOPY|HMDF|HDMF|BUCK\s*HOIST|FIRE\s*CAULK|FIZ\s|ROOF\s*EDGE|ROLL\s*UP|COLUMN|TROUGH|CRICKET|BATTERY|DOGHOUSE|SPRAY|OAC\s*PAD|DUCT\s*SHAFT|PEDESTAL|CATCH-UP|CONTROL\s*JOINT|ELEC$|IFRM|CMP|VEST\s+\d|CJ\s*\(|AWNING|CLEANING\s*DECK|DS$|RM\s+\d|ROOM|OVER\s*BRIDGE|TRESTLE|NCR|\d+(ST|ND|RD|TH)\s+ELEC)', val):
        result['grid_type'] = 'NAMED'
        return result

    # Sector codes: S62, S04, S05 (room codes, not grid)
    if re.match(r'^S\d+$', val):
        result['grid_type'] = 'NAMED'
        return result

    # Sector ranges: "Sectors 1-6", "Sectors 21-25"
    if re.search(r'^SECTORS?\s+\d', val):
        result['grid_type'] = 'AREA'
        return result

    # Additional named locations
    if re.search(r'^(SHOP|YARD|SITE|CONNEX|JMEG|SBTA|BOILER|MEZZ|WAFER|BREEZEWAY|DCC|CRT|SLURRY|PONDS?|TRENCH|FIRE\s*RISER|EXPANSION|FFU|OFFICE\s*\d|CE\d+$|CW\d+$|FW\d+|HCH|LCH|ACID|CRANE|COOLING|PUMP|CHILLER|GEN|GENERATOR|IW\d+|PE\d+|BAKER|WALK\s*WAY|PLENUM|LIFT\s+STATION)', val):
        result['grid_type'] = 'NAMED'
        return result

    # Room codes: OA###, Apollo, Mechanical, Janitor, Reticle, CDA, MCC, Purifier
    if re.match(r'^(OA\d+|APOLLO|MECHANICAL|JANITOR|RETICLE|WATER\s+ROOM|CDA\s|MCC\s|PURIFIER|AHU|SPEC\s*GAS|DUMBWAITER|DUMB\s*WAITER|PIPE\s+RACK|ROOF\s+TEAR)', val):
        result['grid_type'] = 'NAMED'
        return result

    # GCS building references: GCS A, GCS B, GCSB, GCS A/B
    if re.match(r'^GCS\s*[AB]?(?:[/\s-]|$)', val):
        result['grid_type'] = 'AREA'
        return result

    # AD (access door) area references
    if re.match(r'^AD\s', val):
        result['grid_type'] = 'AREA'
        return result

    # Roof reference
    if val == 'ROOF':
        result['grid_type'] = 'AREA'
        return result

    # Sec.## patterns (sector)
    if re.match(r'^SEC\.?\s*\d', val):
        result['grid_type'] = 'NAMED'
        return result

    # SUW/SUE + location description: SUE Elect. Room
    if re.match(r'^(SUW|SUE)\s+[A-Z]', val):
        result['grid_type'] = 'NAMED'
        return result

    # Row.col + FIZZ: F1.5 FIZZ
    m = re.match(r'^([A-N])(\d+(?:\.\d+)?)\s+FIZZ', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(2))
        result['grid_type'] = 'POINT'
        return result

    # Fractional row range: L17.5-M17.5
    m = re.match(r'^([A-N])(\d+(?:\.\d+)?)\s*[-–]\s*([A-N])(\d+(?:\.\d+)?)$', val)
    if m:
        result['grid_row_min'] = m.group(1)
        result['grid_row_max'] = m.group(3)
        result['grid_col_min'] = float(m.group(2))
        result['grid_col_max'] = float(m.group(4))
        result['grid_type'] = 'RANGE'
        return result

    # NA, dash-only: not meaningful
    if val in ['-', 'NA', 'N/A', 'TBD']:
        result['grid_type'] = 'AREA'  # Treat as area (whole building implied)
        return result

    # Sector comma-separated: S05,62 or S1-8
    if re.match(r'^S\d+[,\-]\d+', val):
        result['grid_type'] = 'NAMED'
        return result

    # Pattern like "L17 (Stair)" or "L18 (Elevator 3)" where L## is gridline
    m = re.match(r'^L(\d+)\s*\(', val)
    if m:
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(1))
        result['grid_type'] = 'COL_ONLY'
        return result

    # Pattern: "GL-J, N & A" → multiple rows (take range)
    m = re.match(r'^GL[-\s]?([A-N])[,\s]+([A-N])', val)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_type'] = 'ROW_ONLY'
        return result

    # Row letter + "columns" description: "K columns"
    m = re.match(r'^([A-N])\s+COLUMN', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        result['grid_type'] = 'ROW_ONLY'
        return result

    # Pattern: "NW-PH GL 33 K-L-Line" or "NE-PH-GL 33 C-D" → col 33, rows K-L (penthouse reference)
    m = re.match(r'^[NS][EW][-\s]*(PH|PENTHOUSE)?[-\s]*GL\s*(\d+)\s*([A-N])[-–]([A-N])', val)
    if m:
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(2))
        rows = sorted([m.group(3), m.group(4)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "N/ 5,16,32 LINE" or "A/ 5,16,32" → row N/A with multiple columns (take range)
    m = re.match(r'^([A-N])[/\s]+(\d+)[,\s]+(\d+)', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(3))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "A-3/N-3" → two grid points (rows A and N, col 3)
    m = re.match(r'^([A-N])[-]?(\d+)[/]([A-N])[-]?(\d+)', val)
    if m:
        rows = sorted([m.group(1), m.group(3)])
        result['grid_row_min'], result['grid_row_max'] = rows
        cols = sorted([float(m.group(2)), float(m.group(4))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "4L" or "33K" → inverted col+row
    m = re.match(r'^(\d+)([A-N])$', val)
    if m:
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(1))
        result['grid_row_min'] = result['grid_row_max'] = m.group(2)
        result['grid_type'] = 'POINT'
        return result

    # Pattern: "A-19 & N-11" → multiple grid points (take first)
    m = re.match(r'^([A-N])[-]?(\d+)\s*[&,]\s*([A-N])[-]?(\d+)', val)
    if m:
        result['grid_row_min'] = m.group(1)
        result['grid_row_max'] = m.group(3)
        cols = sorted([float(m.group(2)), float(m.group(4))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "A/6, A/16" → row A, cols 6 and 16 (take range)
    m = re.match(r'^([A-N])[/](\d+)[,\s]+([A-N])?[/]?(\d+)', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(4))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "4E description" or "4E suffix" → col 4, row E (inverted with description)
    m = re.match(r'^(\d+)([A-N])\s+[A-Z]', val)
    if m:
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(1))
        result['grid_row_min'] = result['grid_row_max'] = m.group(2)
        result['grid_type'] = 'POINT'
        return result

    # Pattern: "N-5/14" → row N, cols 5-14 (row-col/col format)
    m = re.match(r'^([A-N])[-](\d+)[/](\d+)', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(3))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "A-C/20-25" → rows A-C, cols 20-25 (row-row/col-col format)
    m = re.match(r'^([A-N])[-]([A-N])[/](\d+)[-–]?(\d+)?', val)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_col_min'] = float(m.group(3))
        result['grid_col_max'] = float(m.group(4) or m.group(3))
        result['grid_type'] = 'RANGE'
        return result

    # === ROW RANGE PATTERNS ===

    # Pattern: "E - J line / Whole Floor" → row E-J
    m = re.match(r'^([A-N])\s*[-–]\s*([A-N])\s*(LINE|LINES?)?', val, re.IGNORECASE)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_type'] = 'ROW_ONLY'
        return result

    # Pattern: "D & K lines" → rows D, K (non-contiguous treated as range)
    m = re.match(r'^([A-N])\s*[&,]\s*([A-N])', val)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_type'] = 'ROW_ONLY'
        return result

    # === ROW + COLUMN PATTERNS ===

    # Pattern: "A/B 32" or "K/L 33 LINE" → rows A-B, col 32
    m = re.match(r'^([A-N])[/]([A-N])\s+(\d+)', val)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(3))
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "B/C17 LINE" → rows B-C, col 17 (no space between letters and col)
    m = re.match(r'^([A-N])[/]([A-N])(\d+)', val)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(3))
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "A-/B 13-17" or "A/B- 13-17" → rows A-B, cols 13-17 (with misplaced dash)
    m = re.match(r'^([A-N])[-/]+([A-N])[-\s]+(\d+)[-–](\d+)', val)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        cols = sorted([float(m.group(3)), float(m.group(4))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "C/D-32 LINE" or "A/B -30" → rows C-D, col 32
    m = re.match(r'^([A-N])[/]([A-N])[-\s]+(\d+)', val)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(3))
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "23/24 C-D LINE" → cols 23-24, rows C-D
    m = re.match(r'^(\d+)[/](\d+)\s+([A-N])[-–]([A-N])', val)
    if m:
        cols = sorted([float(m.group(1)), float(m.group(2))])
        result['grid_col_min'], result['grid_col_max'] = cols
        rows = sorted([m.group(3), m.group(4)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "A/31 LINE" → row A, col 31
    m = re.match(r'^([A-N])[/](\d+)\s*LINE', val, re.IGNORECASE)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(2))
        result['grid_type'] = 'POINT'
        return result

    # Pattern: "A 30-31" → row A, col 30-31
    m = re.match(r'^([A-N])\s+(\d+)[-–](\d+)', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(3))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "D line 1-33" → row D, col 1-33
    m = re.match(r'^([A-N])\s*LINE\s+(\d+)[-–](\d+)', val, re.IGNORECASE)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(3))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "C/11 - C/22" or "L/6 - L/11" → row C, col 11-22
    m = re.match(r'^([A-N])/(\d+)\s*[-–]\s*([A-N])?/?(\d+)', val)
    if m:
        result['grid_row_min'] = m.group(1)
        result['grid_row_max'] = m.group(3) or m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(4))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "D6/D19" → row D, col 6-19
    m = re.match(r'^([A-N])(\d+)/([A-N])?(\d+)$', val)
    if m:
        result['grid_row_min'] = m.group(1)
        result['grid_row_max'] = m.group(3) or m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(4))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "A LINE" or "C LINE" → row only
    m = re.match(r'^([A-N])\s*LINE\s*$', val, re.IGNORECASE)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        result['grid_type'] = 'ROW_ONLY'
        return result

    # Pattern: "A Line / description" or "N LINE / Roof Edge" → row only with description
    m = re.match(r'^([A-N])\s*LINE\s*[/]', val, re.IGNORECASE)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        result['grid_type'] = 'ROW_ONLY'
        return result

    # Pattern: "D-E description" or "D-E Troughs" → row range with description
    m = re.match(r'^([A-N])[-–]([A-N])\s+[A-Z]', val)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_type'] = 'ROW_ONLY'
        return result

    # === COLUMN ONLY PATTERNS ===

    # Pattern: "LINE 33" or "GL 33" → col only
    m = re.match(r'^(LINE|GL)\s*(\d+)', val, re.IGNORECASE)
    if m:
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(2))
        result['grid_type'] = 'COL_ONLY'
        return result

    # Pattern: "GL L 31" or "GL N 17" → row + col
    m = re.match(r'^GL\s+([A-N])\s+(\d+)', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(2))
        result['grid_type'] = 'POINT'
        return result

    # Pattern: "GL L.5 31-33" → row L.5, cols 31-33
    m = re.match(r'^GL\s+([A-N])\.?(\d*)\s+(\d+)[-–](\d+)', val)
    if m:
        row = m.group(1) + ('.' + m.group(2) if m.group(2) else '')
        result['grid_row_min'] = result['grid_row_max'] = row
        cols = sorted([float(m.group(3)), float(m.group(4))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "L-5 LINE" or "L- 28 LINE" → row L, col 5
    m = re.match(r'^([A-N])[-]\s*(\d+)\s*LINE', val, re.IGNORECASE)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(2))
        result['grid_type'] = 'POINT'
        return result

    # Pattern: "J-1,2" → row J, cols 1-2 (comma-separated columns)
    m = re.match(r'^([A-N])[-](\d+),(\d+)', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(3))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "E.5-F" → row range with decimal
    m = re.match(r'^([A-N])\.(\d+)[-–]([A-N])', val)
    if m:
        row1 = f"{m.group(1)}.{m.group(2)}"
        result['grid_row_min'] = row1
        result['grid_row_max'] = m.group(3)
        result['grid_type'] = 'ROW_ONLY'
        return result

    # Pattern: "M .5 -13" or "M.5/13" → row M.5, col 13
    m = re.match(r'^([A-N])\s*\.?(\d+)\s*[-/]\s*(\d+)', val)
    if m:
        row_decimal = m.group(2)
        result['grid_row_min'] = result['grid_row_max'] = f"{m.group(1)}.{row_decimal}"
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(3))
        result['grid_type'] = 'POINT'
        return result

    # Pattern: "33 LINE" → col only
    m = re.match(r'^(\d+)\s*LINE', val, re.IGNORECASE)
    if m:
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(1))
        result['grid_type'] = 'COL_ONLY'
        return result

    # Pattern: "GL-33/K-N" or "GL 33/K-N" → col 33, rows K-N
    m = re.match(r'^GL[-\s]?(\d+)[/\s]+([A-N])[-–]([A-N])', val)
    if m:
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(1))
        rows = sorted([m.group(2), m.group(3)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "GL-N/30-32" → row N, cols 30-32
    m = re.match(r'^GL[-\s]?([A-N])[/\s]+(\d+)[-–](\d+)', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(3))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "33 Canopy" or "33 description" → col with description
    m = re.match(r'^(\d+)\s+[A-Z]', val)
    if m:
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(1))
        result['grid_type'] = 'COL_ONLY'
        return result

    # Pattern: "16-20 description" → col range with description
    m = re.match(r'^(\d+)[-–](\d+)\s+[A-Z]', val)
    if m:
        cols = sorted([float(m.group(1)), float(m.group(2))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'COL_ONLY'
        return result

    # Pattern: "N-5 to N33" → row N, cols 5-33
    m = re.match(r'^([A-N])[-]?(\d+)\s+TO\s+([A-N])?[-]?(\d+)', val, re.IGNORECASE)
    if m:
        result['grid_row_min'] = m.group(1)
        result['grid_row_max'] = m.group(3) or m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(4))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "K/D COLUMNS" or "K/D description" → multiple rows
    m = re.match(r'^([A-N])[/]([A-N])\s+[A-Z]', val)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_type'] = 'ROW_ONLY'
        return result

    # Pattern: "K2.5 suffix" → row K.5 with building suffix
    m = re.match(r'^([A-N])(\d+\.?\d*)\s+(SUW|SUE|FAB)', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(2))
        result['grid_type'] = 'POINT'
        return result

    # Pattern: Just "33" → col only
    m = re.match(r'^(\d+)$', val)
    if m:
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(1))
        result['grid_type'] = 'COL_ONLY'
        return result

    # Gridline: GL-33
    m = re.match(r'^GL[-]?(\d+)$', val)
    if m:
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(1))
        result['grid_type'] = 'COL_ONLY'
        return result

    # Column range only: 29-30
    m = re.match(r'^(\d+)[-–](\d+)$', val)
    if m:
        cols = sorted([float(m.group(1)), float(m.group(2))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'COL_ONLY'
        return result

    # Parenthesized col range: (3-33), (20-40)
    m = re.match(r'^\((\d+)[-–](\d+)\)$', val)
    if m:
        cols = sorted([float(m.group(1)), float(m.group(2))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'COL_ONLY'
        return result

    # Decimal row-col range: E-14.0 - 17.0, J-22.0 - 24.0
    m = re.match(r'^([A-N])[-–](\d+\.?\d*)\s*[-–]\s*(\d+\.?\d*)$', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(3))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # === SIMPLE PATTERNS ===

    # Single letter: G, D, B
    m = re.match(r'^([A-N])$', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        result['grid_type'] = 'ROW_ONLY'
        return result

    # GL-N or GL N or GL-M (gridline row letter)
    m = re.match(r'^GL[-\s]?([A-N])(?:\s|$)', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        result['grid_type'] = 'ROW_ONLY'
        return result

    # Letter/letter: G/H, D/K
    m = re.match(r'^([A-N])[/&]([A-N])$', val)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_type'] = 'ROW_ONLY'
        return result

    # Letter + space + Column: "N 17", "A 32"
    m = re.match(r'^([A-N])\s+(\d+)(?:\s|$)', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(2))
        result['grid_type'] = 'POINT'
        return result

    # Letter + Column: J11, A-19, A19, N/17
    m = re.match(r'^([A-N])[-/]?(\d+(?:\.\d+)?)$', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(2))
        result['grid_type'] = 'POINT'
        return result

    # Letter+Col with description: "K18 (Vestibule 3)", "D32 (Airlock 18)"
    m = re.match(r'^([A-N])(\d+)\s*\(', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(2))
        result['grid_type'] = 'POINT'
        return result

    # Letter+Col with trailing text: "A32 CANOPY", "N1 PASSAGE", "C28 ELEVATOR"
    m = re.match(r'^([A-N])(\d+)\s+[A-Z]', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(2))
        result['grid_type'] = 'POINT'
        return result

    # Letter+Col with comma description: "F23, Door Frames"
    m = re.match(r'^([A-N])(\d+),', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(2))
        result['grid_type'] = 'POINT'
        return result

    # Row + col range: "N-29-23" → row N, cols 23-29
    m = re.match(r'^([A-N])-(\d+)-(\d+)(?:\s|$|;)', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(3))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Letter+Col range: K20-K32, J33-K33, L5-L11
    m = re.match(r'^([A-N])(\d+)[-–]([A-N])?(\d+)', val)
    if m:
        result['grid_row_min'] = m.group(1)
        result['grid_row_max'] = m.group(3) or m.group(1)
        if result['grid_row_min'] > result['grid_row_max']:
            result['grid_row_min'], result['grid_row_max'] = result['grid_row_max'], result['grid_row_min']
        cols = sorted([float(m.group(2)), float(m.group(4))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Letter range with columns: A-N/1-3, N-P/23-29 (extended to A-Z for edge rows)
    m = re.match(r'^([A-Z])[-–]([A-Z])[/\s](\d+)[-–]?(\d+)?', val)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_col_min'] = float(m.group(3))
        result['grid_col_max'] = float(m.group(4) or m.group(3))
        result['grid_type'] = 'RANGE'
        return result

    # ==========================================================================
    # Additional patterns (Phase 2)
    # ==========================================================================

    # Fractional tilde with hyphen cols: G.3~H.8-3~8 → rows G.3-H.8, cols 3-8
    m = re.match(r'^([A-N])\.(\d+)~([A-N])\.(\d+)[-–](\d+)~(\d+)', val)
    if m:
        result['grid_row_min'] = f"{m.group(1)}.{m.group(2)}"
        result['grid_row_max'] = f"{m.group(3)}.{m.group(4)}"
        cols = sorted([float(m.group(5)), float(m.group(6))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Tilde col-only range: E/5~E/12 or E/8~9 → row E, cols 5-12 or 8-9
    m = re.match(r'^([A-N])/(\d+)~(?:[A-N]/)?(\d+)$', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(3))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Tilde row-col range: A~C/7~A~C/29 → rows A-C, cols 7-29
    m = re.match(r'^([A-N])~([A-N])/(\d+)~(?:[A-N]~[A-N]/)?(\d+)$', val)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        cols = sorted([float(m.group(3)), float(m.group(4))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Tilde fractional col range: A54~B55.5 → rows A-B, cols 54-55.5
    m = re.match(r'^([A-N])(\d+(?:\.\d+)?)~([A-N])(\d+(?:\.\d+)?)$', val)
    if m:
        rows = sorted([m.group(1), m.group(3)])
        result['grid_row_min'], result['grid_row_max'] = rows
        cols = sorted([float(m.group(2)), float(m.group(4))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Tilde compact range: E3~J18 → rows E-J, cols 3-18
    m = re.match(r'^([A-N])(\d+)~([A-N])(\d+)$', val)
    if m:
        rows = sorted([m.group(1), m.group(3)])
        result['grid_row_min'], result['grid_row_max'] = rows
        cols = sorted([float(m.group(2)), float(m.group(4))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Tilde col range: C3~4 → row C, cols 3-4
    m = re.match(r'^([A-N])(\d+)~(\d+)$', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(3))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # GL row tilde: GL K34~44 → row K, cols 34-44
    m = re.match(r'^GL\s+([A-N])(\d+)~(\d+)$', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(3))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Tilde separator: E13~E28 → rows E, cols 13-28
    m = re.match(r'^([A-N])(\d+)~([A-N])?(\d+)$', val)
    if m:
        result['grid_row_min'] = m.group(1)
        result['grid_row_max'] = m.group(3) or m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(4))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Tilde with row~row/col~col: J~G/12~18 or G~E/9~12
    m = re.match(r'^([A-N])~([A-N])[/](\d+)~(\d+)', val)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        cols = sorted([float(m.group(3)), float(m.group(4))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Tilde with fractional row: G~G.3/8~32 → rows G-G.3, cols 8-32
    m = re.match(r'^([A-N])~([A-N])\.(\d+)/(\d+)~(\d+)$', val)
    if m:
        result['grid_row_min'] = m.group(1)
        result['grid_row_max'] = f"{m.group(2)}.{m.group(3)}"
        cols = sorted([float(m.group(4)), float(m.group(5))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Tilde with comma separator: J~K,8~15 → rows J-K, cols 8-15
    m = re.match(r'^([A-N])~([A-N]),(\d+)~(\d+)$', val)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        cols = sorted([float(m.group(3)), float(m.group(4))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Fractional tilde with comma: H.8~K,6-3 → rows H.8-K, cols 3-6
    m = re.match(r'^([A-N])\.(\d+)~([A-N]),(\d+)[-–](\d+)$', val)
    if m:
        result['grid_row_min'] = f"{m.group(1)}.{m.group(2)}"
        result['grid_row_max'] = m.group(3)
        cols = sorted([float(m.group(4)), float(m.group(5))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Two-letter row + cols: LM 6,4 → rows L-M, cols 4,6
    m = re.match(r'^([A-N])([A-N])\s+(\d+),(\d+)$', val)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        cols = sorted([float(m.group(3)), float(m.group(4))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # CL column range: CL 5-12, CL 20-33
    m = re.match(r'^CL\s+(\d+)[-–](\d+)', val)
    if m:
        cols = sorted([float(m.group(1)), float(m.group(2))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'COL_ONLY'
        return result

    # CL single column: CL 11
    m = re.match(r'^CL\s+(\d+)$', val)
    if m:
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(1))
        result['grid_type'] = 'COL_ONLY'
        return result

    # COL prefix: COL 11
    m = re.match(r'^COL\s+(\d+)', val)
    if m:
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(1))
        result['grid_type'] = 'COL_ONLY'
        return result

    # Tilde col-only: 3~8, 6~32 → cols 3-8
    m = re.match(r'^(\d+)~(\d+)$', val)
    if m:
        cols = sorted([float(m.group(1)), float(m.group(2))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'COL_ONLY'
        return result

    # FAB with spaced dash: fab 3 - 25 → cols 3-25
    m = re.match(r'^FAB\s+(\d+)\s*[-–]\s*(\d+)', val)
    if m:
        cols = sorted([float(m.group(1)), float(m.group(2))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'COL_ONLY'
        return result

    # N/ space col: N/ 10-11
    m = re.match(r'^([A-N])/\s+(\d+)[-–](\d+)', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(3))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # GL-A/28, GL-N/19 format
    m = re.match(r'^GL[-]?([A-N])[/](\d+)', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(2))
        result['grid_type'] = 'POINT'
        return result

    # SUW/SUE col-col/row-row: SUW 5-6/L-N, SUE 5-20/D
    m = re.match(r'^(SUW|SUE)\s+(\d+)[-–](\d+)[/]([A-N])[-–]?([A-N])?', val)
    if m:
        cols = sorted([float(m.group(2)), float(m.group(3))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_row_min'] = m.group(4)
        result['grid_row_max'] = m.group(5) or m.group(4)
        result['grid_type'] = 'RANGE'
        return result

    # SUW col/row format: SUW 28-24/L
    m = re.match(r'^(SUW|SUE)\s+(\d+)[-–]?(\d+)?[/]([A-N])', val)
    if m:
        result['grid_col_min'] = float(m.group(2))
        result['grid_col_max'] = float(m.group(3) or m.group(2))
        if result['grid_col_min'] > result['grid_col_max']:
            result['grid_col_min'], result['grid_col_max'] = result['grid_col_max'], result['grid_col_min']
        result['grid_row_min'] = result['grid_row_max'] = m.group(4)
        result['grid_type'] = 'RANGE'
        return result

    # G/J-LINE → rows G-J
    m = re.match(r'^([A-N])/([A-N])[-\s]*LINE', val)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_type'] = 'ROW_ONLY'
        return result

    # L19 CJ, L28 (Line + description)
    m = re.match(r'^L(\d+)\s+[A-Z]', val)
    if m:
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(1))
        result['grid_type'] = 'COL_ONLY'
        return result

    # FAB 3 - 25 → cols 3-25
    m = re.match(r'^FAB\s+(\d+)\s*[-–]\s*(\d+)', val)
    if m:
        cols = sorted([float(m.group(1)), float(m.group(2))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'COL_ONLY'
        return result

    # B-4 MEZZ → row B, col 4
    m = re.match(r'^([A-N])[-](\d+)\s+(MEZZ|MEZZANINE)', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(2))
        result['grid_type'] = 'POINT'
        return result

    # CE##P patterns: CE8P, CE22P → equipment with column hint
    m = re.match(r'^CE(\d+)P', val)
    if m:
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(1))
        result['grid_type'] = 'COL_ONLY'
        return result

    # FE## patterns: FE15
    m = re.match(r'^FE(\d+)$', val)
    if m:
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(1))
        result['grid_type'] = 'COL_ONLY'
        return result

    return result


def enrich_tbm(dry_run: bool = False) -> Dict[str, Any]:
    """Enrich TBM work_entries.csv with dimension IDs."""
    input_path = Settings.PROCESSED_DATA_DIR / 'tbm' / 'work_entries.csv'
    output_path = Settings.PROCESSED_DATA_DIR / 'tbm' / 'work_entries_enriched.csv'

    if not input_path.exists():
        return {'status': 'skipped', 'reason': 'file not found'}

    df = pd.read_csv(input_path)
    original_count = len(df)

    # Normalize building codes
    building_map = {'FAB': 'FAB', 'SUP': 'SUE', 'Fab': 'FAB', 'OFFICE': None, 'Laydown': None}
    df['building_normalized'] = df['location_building'].map(
        lambda x: building_map.get(x, x) if pd.notna(x) else None
    )

    # Normalize level codes (e.g., "1F" -> "1F", "RF" -> "ROOF")
    df['level_normalized'] = df['location_level'].apply(normalize_level_value)

    # Add dimension IDs
    df['dim_location_id'] = df.apply(
        lambda row: get_location_id(row['building_normalized'], row['level_normalized']),
        axis=1
    )
    # Company lookup - try tier2_sc first, then tier1_gc, then subcontractor_file
    def get_company_from_row(row):
        # Try tier2_sc (subcontractor) first
        company_id = get_company_id(row.get('tier2_sc'))
        if company_id:
            return company_id
        # Fall back to tier1_gc (some files put company name here)
        company_id = get_company_id(row.get('tier1_gc'))
        if company_id:
            return company_id
        # Final fallback to subcontractor_file (parsed from filename)
        company_id = get_company_id(row.get('subcontractor_file'))
        return company_id

    df['dim_company_id'] = df.apply(get_company_from_row, axis=1)

    # Infer trade from work activities (enhanced mapping)
    def infer_trade_from_activity(activity):
        if pd.isna(activity):
            return None
        activity_lower = str(activity).lower()
        # General conditions (check first to skip non-trade activities)
        if any(x in activity_lower for x in ['laydown', 'yard', 'mobilization', 'demobilization', 'safety', 'cleanup']):
            return 'General'
        # Concrete (trade_id=1)
        if any(x in activity_lower for x in ['concrete', 'pour', 'slab', 'form', 'strip', 'rebar', 'topping', 'placement', 'cure', 'finishing', 'delamination', 'patching', 'chipping', 'patch', 'repair']):
            return 'Concrete'
        # Structural Steel (trade_id=2)
        if any(x in activity_lower for x in ['steel', 'erect', 'deck', 'weld', 'bolt', 'connection', 'joist', 'truss', 'iron', 'elevator front clip', 'elevator clip', 'clip']):
            return 'Structural Steel'
        # Roofing (trade_id=3)
        if any(x in activity_lower for x in ['roof', 'membrane', 'waterproof', 'eifs']):
            return 'Roofing'
        # Drywall (trade_id=4)
        if any(x in activity_lower for x in ['drywall', 'frame', 'stud', 'gyp', 'gypsum', 'framing', 'shaft', 'ceiling grid', 'metal track', 'sheathing']):
            return 'Drywall'
        # Finishes (trade_id=5)
        if any(x in activity_lower for x in ['paint', 'coat', 'finish', 'tile', 'floor', 'ceiling', 'door', 'hardware', 'casework', 'glazing', 'window']):
            return 'Finishes'
        # Fire Protection (trade_id=6)
        if any(x in activity_lower for x in ['fireproof', 'firestop', 'sfrm', 'fire caulk', 'intumescent', 'fire rating', 'fire barrier']):
            return 'Fire Protection'
        # MEP (trade_id=7)
        if any(x in activity_lower for x in ['mep', 'hvac', 'plumb', 'elec', 'pipe', 'conduit', 'duct', 'wire', 'electrical', 'mechanical', 'sprinkler']):
            return 'MEP'
        # Insulation (trade_id=8)
        if any(x in activity_lower for x in ['insul', 'thermal', 'urethane', 'wrap']):
            return 'Insulation'
        # Earthwork (trade_id=9)
        if any(x in activity_lower for x in ['excavat', 'backfill', 'grade', 'foundation', 'pier', 'pile', 'earth']):
            return 'Earthwork'
        # Precast (trade_id=10)
        if any(x in activity_lower for x in ['precast', 'tilt', 'pc panel']):
            return 'Precast'
        # Panels (trade_id=11)
        if any(x in activity_lower for x in ['panel', 'clad', 'skin', 'enclosure', 'metal wall', 'zee metal', 'corbel']):
            return 'Panels'
        # Masonry (trade_id=13)
        if any(x in activity_lower for x in ['masonry', 'cmu', 'block', 'brick', 'grout']):
            return 'Masonry'
        return None

    df['trade_inferred'] = df['work_activities'].apply(infer_trade_from_activity)
    df['dim_trade_id'] = df['trade_inferred'].apply(get_trade_id)

    # Fallback: use company's primary_trade_id if activity-based inference failed
    print("  Applying company-to-trade fallback...")
    dim_company = pd.read_csv(Settings.PROCESSED_DATA_DIR / 'integrated_analysis' / 'dimensions' / 'dim_company.csv')
    company_trade_map = dict(zip(dim_company['company_id'], dim_company['primary_trade_id']))

    def get_trade_from_company(row):
        """Get trade from company's primary_trade_id if dim_trade_id is missing."""
        if pd.notna(row['dim_trade_id']):
            return row['dim_trade_id']
        company_id = row['dim_company_id']
        if pd.notna(company_id):
            trade_id = company_trade_map.get(company_id)
            if pd.notna(trade_id):
                return int(trade_id)
        return None

    df['dim_trade_id'] = df.apply(get_trade_from_company, axis=1)
    df['dim_trade_code'] = df['dim_trade_id'].apply(get_trade_code)

    # Track source of trade inference
    df['trade_source'] = df.apply(
        lambda row: 'activity' if pd.notna(row['trade_inferred']) else ('company' if pd.notna(row['dim_trade_id']) else None),
        axis=1
    )

    # Infer CSI section from work activities and trade
    print("  Inferring CSI sections...")
    csi_results = df.apply(
        lambda row: infer_csi_from_activity(row.get('work_activities'), row.get('trade_inferred')),
        axis=1
    )
    df['dim_csi_section_id'] = csi_results.apply(lambda x: x[0])
    df['csi_section'] = csi_results.apply(lambda x: x[1])
    df['csi_inference_source'] = csi_results.apply(lambda x: x[2])
    df['csi_title'] = df['dim_csi_section_id'].apply(
        lambda x: TBM_CSI_SECTIONS[x][1] if pd.notna(x) and x in TBM_CSI_SECTIONS else None
    )

    # Parse grid from location_row
    print("  Parsing grid coordinates...")
    grid_parsed = df['location_row'].apply(parse_tbm_grid).apply(pd.Series)
    df['grid_row_min'] = grid_parsed['grid_row_min']
    df['grid_row_max'] = grid_parsed['grid_row_max']
    df['grid_col_min'] = grid_parsed['grid_col_min']
    df['grid_col_max'] = grid_parsed['grid_col_max']
    df['grid_raw'] = grid_parsed['grid_raw']
    df['grid_type'] = grid_parsed['grid_type']

    # Compute affected_rooms for records with grid coordinates
    print("  Computing affected rooms...")

    def compute_affected_rooms(row):
        """Find rooms that overlap with the record's grid bounds."""
        # Need level and some grid info (building ignored - unified grid system)
        level = row.get('level_normalized')
        if pd.isna(level):
            return None

        # Get grid bounds (may be partial)
        row_min = row.get('grid_row_min')
        row_max = row.get('grid_row_max')
        col_min = row.get('grid_col_min')
        col_max = row.get('grid_col_max')

        # Need at least some grid info
        has_row = pd.notna(row_min)
        has_col = pd.notna(col_min)
        if not has_row and not has_col:
            return None

        rooms = get_affected_rooms(
            level,
            row_min if has_row else None,
            row_max if has_row else None,
            col_min if has_col else None,
            col_max if has_col else None,
        )

        if not rooms:
            return None

        # Return as JSON string for CSV storage
        return json.dumps(rooms)

    df['affected_rooms'] = df.apply(compute_affected_rooms, axis=1)

    # Add affected_rooms_count for easy filtering (1 = single room, >1 = multiple)
    def count_rooms(json_str):
        if pd.isna(json_str):
            return None
        try:
            return len(json.loads(json_str))
        except (json.JSONDecodeError, TypeError):
            return None

    df['affected_rooms_count'] = df['affected_rooms'].apply(count_rooms)

    # Calculate coverage
    has_grid_row = df['grid_row_min'].notna()
    has_grid_col = df['grid_col_min'].notna()
    has_affected_rooms = df['affected_rooms'].notna()
    coverage = {
        'location': df['dim_location_id'].notna().mean() * 100,
        'company': df['dim_company_id'].notna().mean() * 100,
        'trade': df['dim_trade_id'].notna().mean() * 100,
        'csi_section': df['dim_csi_section_id'].notna().mean() * 100,
        'grid_row': has_grid_row.mean() * 100,
        'grid_col': has_grid_col.mean() * 100,
        'affected_rooms': has_affected_rooms.mean() * 100,
    }

    # Grid type distribution for reporting
    grid_type_dist = df['grid_type'].value_counts().to_dict()

    if not dry_run:
        validated_df_to_csv(df, output_path, index=False)

    return {
        'status': 'success',
        'records': original_count,
        'coverage': coverage,
        'grid_types': grid_type_dist,
        'output': str(output_path) if not dry_run else 'DRY RUN (validated)',
    }


def enrich_projectsight(dry_run: bool = False) -> Dict[str, Any]:
    """Enrich ProjectSight labor_entries.csv with dimension IDs."""
    input_path = Settings.PROCESSED_DATA_DIR / 'projectsight' / 'labor_entries.csv'
    output_path = Settings.PROCESSED_DATA_DIR / 'projectsight' / 'labor_entries_enriched.csv'

    if not input_path.exists():
        return {'status': 'skipped', 'reason': 'file not found'}

    print("  Loading data...")
    df = pd.read_csv(input_path)
    original_count = len(df)

    # ProjectSight has no location data - only company and trade
    df['dim_location_id'] = None  # No location available

    # Build lookup dictionaries for fast vectorized mapping
    print("  Building company lookup...")
    unique_companies = df['company'].dropna().unique()
    company_lookup = {c: get_company_id(c) for c in unique_companies}
    df['dim_company_id'] = df['company'].map(company_lookup)

    print("  Building trade lookup...")
    unique_trades = df['trade_name'].dropna().unique()
    trade_lookup = {t: get_trade_id(t) for t in unique_trades}
    trade_code_lookup = {t: get_trade_code(trade_lookup.get(t)) for t in unique_trades}
    df['dim_trade_id'] = df['trade_name'].map(trade_lookup)
    df['dim_trade_code'] = df['trade_name'].map(trade_code_lookup)

    # Track initial trade coverage before fallback
    trade_from_name = df['dim_trade_id'].notna().sum()

    # Use company's primary_trade_id, with validation against allowed trades
    print("  Applying company-based trade assignment...")
    dim_company = pd.read_csv(Settings.PROCESSED_DATA_DIR / 'integrated_analysis' / 'dimensions' / 'dim_company.csv')
    company_primary_trade = dict(zip(dim_company['company_id'], dim_company['primary_trade_id']))

    # Build mapping of company_id -> set of allowed trade_ids (primary + other_trade_ids)
    def get_allowed_trades(row):
        allowed = set()
        if pd.notna(row['primary_trade_id']):
            allowed.add(int(row['primary_trade_id']))
        if pd.notna(row.get('other_trade_ids')):
            try:
                ids = [int(x.strip()) for x in str(row['other_trade_ids']).split(',')]
                allowed.update(ids)
            except:
                pass
        return allowed

    company_allowed_trades = {row['company_id']: get_allowed_trades(row) for _, row in dim_company.iterrows()}

    def get_validated_trade(row):
        """
        Get trade_id using company's allowed trades.

        Logic:
        1. If ProjectSight trade_name maps to a trade in company's allowed list, keep it
        2. Otherwise, use company's primary_trade_id
        """
        company_id = row['dim_company_id']
        ps_trade_id = row['dim_trade_id']  # From ProjectSight trade_name

        if pd.isna(company_id):
            return ps_trade_id  # No company match, keep ProjectSight trade

        allowed = company_allowed_trades.get(company_id, set())
        primary = company_primary_trade.get(company_id)

        # If ProjectSight trade is in company's allowed trades, keep it
        if pd.notna(ps_trade_id) and int(ps_trade_id) in allowed:
            return ps_trade_id

        # Otherwise use company's primary trade
        return int(primary) if pd.notna(primary) else ps_trade_id

    # Store original for tracking
    df['trade_id_from_ps'] = df['dim_trade_id'].copy()
    df['dim_trade_id'] = df.apply(get_validated_trade, axis=1)
    df['dim_trade_code'] = df['dim_trade_id'].apply(get_trade_code)

    # Track source of trade inference
    df['trade_source'] = df.apply(
        lambda row: 'ps_validated' if pd.notna(row['trade_id_from_ps']) and row['trade_id_from_ps'] == row['dim_trade_id']
                    else ('company_primary' if pd.notna(row['dim_trade_id']) else None),
        axis=1
    )

    trade_from_ps_validated = (df['trade_source'] == 'ps_validated').sum()
    trade_from_company = (df['trade_source'] == 'company_primary').sum()

    # Get company's primary_trade_id for CSI inference (more reliable than ProjectSight trade_name)
    # This is used even when dim_trade_id comes from trade_name, because company's known trade
    # is more reliable for CSI mapping than ProjectSight's billing category
    df['company_primary_trade_id'] = df['dim_company_id'].map(company_primary_trade)

    # Infer CSI section from activity, trade_full (division), and company's primary trade
    print("  Inferring CSI sections...")
    csi_results = df.apply(
        lambda row: infer_csi_from_projectsight(
            row.get('activity'),
            row.get('trade_full'),
            row.get('company_primary_trade_id')  # Uses company's actual primary trade
        ),
        axis=1
    )
    df['dim_csi_section_id'] = csi_results.apply(lambda x: x[0])
    df['csi_section'] = csi_results.apply(lambda x: x[1])
    df['csi_inference_source'] = csi_results.apply(lambda x: x[2])
    df['csi_title'] = df['dim_csi_section_id'].apply(
        lambda x: PS_CSI_SECTIONS[x][1] if pd.notna(x) and x in PS_CSI_SECTIONS else None
    )

    # Calculate coverage
    coverage = {
        'location': 0.0,  # No location in ProjectSight
        'company': df['dim_company_id'].notna().mean() * 100,
        'trade': df['dim_trade_id'].notna().mean() * 100,
        'csi_section': df['dim_csi_section_id'].notna().mean() * 100,
        'trade_from_ps_validated': trade_from_ps_validated,  # PS trade was in company's allowed list
        'trade_from_company_primary': trade_from_company,  # Used company's primary_trade_id
    }

    if not dry_run:
        print("  Writing output (with schema validation)...")
        validated_df_to_csv(df, output_path, index=False)

    return {
        'status': 'success',
        'records': original_count,
        'coverage': coverage,
        'output': str(output_path) if not dry_run else 'DRY RUN (validated)',
    }


def enrich_weekly_labor(dry_run: bool = False) -> Dict[str, Any]:
    """Enrich Weekly Reports labor_detail_by_company.csv with dimension IDs."""
    input_path = Settings.PROCESSED_DATA_DIR / 'weekly_reports' / 'labor_detail_by_company.csv'
    output_path = Settings.PROCESSED_DATA_DIR / 'weekly_reports' / 'labor_detail_by_company_enriched.csv'

    if not input_path.exists():
        return {'status': 'skipped', 'reason': 'file not found'}

    df = pd.read_csv(input_path)
    original_count = len(df)

    # Only has company data
    df['dim_location_id'] = None
    df['dim_company_id'] = df['company'].apply(get_company_id)
    df['dim_trade_id'] = None
    df['dim_trade_code'] = None

    # Calculate coverage
    coverage = {
        'location': 0.0,
        'company': df['dim_company_id'].notna().mean() * 100,
        'trade': 0.0,
    }

    if not dry_run:
        validated_df_to_csv(df, output_path, index=False)

    return {
        'status': 'success',
        'records': original_count,
        'coverage': coverage,
        'output': str(output_path) if not dry_run else 'DRY RUN (validated)',
    }


# Define all enrichment tasks
ENRICHMENT_TASKS = {
    'tbm': ('TBM Daily Plans', enrich_tbm),
    'projectsight': ('ProjectSight Labor', enrich_projectsight),
    'weekly_labor': ('Weekly Reports Labor', enrich_weekly_labor),
}


def main():
    parser = argparse.ArgumentParser(description='Enrich data sources with dimension IDs')
    parser.add_argument('--source', choices=list(ENRICHMENT_TASKS.keys()),
                       help='Enrich only this source')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be done without writing files')
    args = parser.parse_args()

    # Reset dimension cache to ensure fresh data
    reset_cache()

    # Determine which sources to process
    if args.source:
        sources = {args.source: ENRICHMENT_TASKS[args.source]}
    else:
        sources = ENRICHMENT_TASKS

    print("=" * 70)
    print("DIMENSION ENRICHMENT")
    print("=" * 70)

    results = {}
    for key, (name, func) in sources.items():
        print(f"\nProcessing: {name}")
        print("-" * 40)

        result = func(dry_run=args.dry_run)
        results[key] = result

        if result['status'] == 'success':
            print(f"  Records: {result['records']:,}")
            print(f"  Coverage:")
            for dim, pct in result['coverage'].items():
                print(f"    {dim}: {pct:.1f}%")
            # Show grid type distribution if present (TBM only)
            if 'grid_types' in result:
                print(f"  Grid types:")
                for gtype, count in sorted(result['grid_types'].items(), key=lambda x: -x[1]):
                    pct = count / result['records'] * 100
                    print(f"    {gtype}: {count:,} ({pct:.1f}%)")
            print(f"  Output: {result['output']}")
        else:
            print(f"  Status: {result['status']}")
            print(f"  Reason: {result.get('reason', 'unknown')}")

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"\n{'Source':<20} {'Records':>12} {'Location':>10} {'Company':>10} {'Trade':>10} {'Grid Row':>10} {'Grid Col':>10}")
    print("-" * 90)

    for key, result in results.items():
        name = ENRICHMENT_TASKS[key][0]
        if result['status'] == 'success':
            grid_row = result['coverage'].get('grid_row', 0.0)
            grid_col = result['coverage'].get('grid_col', 0.0)
            print(f"{name:<20} {result['records']:>12,} {result['coverage']['location']:>9.1f}% {result['coverage']['company']:>9.1f}% {result['coverage']['trade']:>9.1f}% {grid_row:>9.1f}% {grid_col:>9.1f}%")
        else:
            print(f"{name:<20} {'SKIPPED':>12}")


if __name__ == '__main__':
    main()
