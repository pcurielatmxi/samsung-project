"""
TBM Grid Parser

Parses TBM location_row field into normalized grid components.
This parser handles 103+ TBM-specific grid patterns accumulated from field data.

This is the single source of truth for TBM grid parsing.
"""

import re
from typing import Any, Dict

import pandas as pd


def parse_tbm_grid(location_row: str) -> Dict[str, Any]:
    """
    Parse TBM location_row field into normalized grid components.

    This is a comprehensive parser handling 103+ TBM-specific grid patterns.

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

    # FAB with spaced dash: fab 3 - 25 -> cols 3-25 (MUST be before prefix stripping)
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

    # Pattern: "GL-J, N & A" -> multiple rows (take range)
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

    # Pattern: "NW-PH GL 33 K-L-Line" or "NE-PH-GL 33 C-D" -> col 33, rows K-L (penthouse reference)
    m = re.match(r'^[NS][EW][-\s]*(PH|PENTHOUSE)?[-\s]*GL\s*(\d+)\s*([A-N])[-–]([A-N])', val)
    if m:
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(2))
        rows = sorted([m.group(3), m.group(4)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "N/ 5,16,32 LINE" or "A/ 5,16,32" -> row N/A with multiple columns (take range)
    m = re.match(r'^([A-N])[/\s]+(\d+)[,\s]+(\d+)', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(3))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "A-3/N-3" -> two grid points (rows A and N, col 3)
    m = re.match(r'^([A-N])[-]?(\d+)[/]([A-N])[-]?(\d+)', val)
    if m:
        rows = sorted([m.group(1), m.group(3)])
        result['grid_row_min'], result['grid_row_max'] = rows
        cols = sorted([float(m.group(2)), float(m.group(4))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "4L" or "33K" -> inverted col+row
    m = re.match(r'^(\d+)([A-N])$', val)
    if m:
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(1))
        result['grid_row_min'] = result['grid_row_max'] = m.group(2)
        result['grid_type'] = 'POINT'
        return result

    # Pattern: "A-19 & N-11" -> multiple grid points (take first)
    m = re.match(r'^([A-N])[-]?(\d+)\s*[&,]\s*([A-N])[-]?(\d+)', val)
    if m:
        result['grid_row_min'] = m.group(1)
        result['grid_row_max'] = m.group(3)
        cols = sorted([float(m.group(2)), float(m.group(4))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "A/6, A/16" -> row A, cols 6 and 16 (take range)
    m = re.match(r'^([A-N])[/](\d+)[,\s]+([A-N])?[/]?(\d+)', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(4))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "4E description" or "4E suffix" -> col 4, row E (inverted with description)
    m = re.match(r'^(\d+)([A-N])\s+[A-Z]', val)
    if m:
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(1))
        result['grid_row_min'] = result['grid_row_max'] = m.group(2)
        result['grid_type'] = 'POINT'
        return result

    # Pattern: "N-5/14" -> row N, cols 5-14 (row-col/col format)
    m = re.match(r'^([A-N])[-](\d+)[/](\d+)', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(3))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "A-C/20-25" -> rows A-C, cols 20-25 (row-row/col-col format)
    m = re.match(r'^([A-N])[-]([A-N])[/](\d+)[-–]?(\d+)?', val)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_col_min'] = float(m.group(3))
        result['grid_col_max'] = float(m.group(4) or m.group(3))
        result['grid_type'] = 'RANGE'
        return result

    # === ROW RANGE PATTERNS ===

    # Pattern: "E - J line / Whole Floor" -> row E-J
    m = re.match(r'^([A-N])\s*[-–]\s*([A-N])\s*(LINE|LINES?)?', val, re.IGNORECASE)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_type'] = 'ROW_ONLY'
        return result

    # Pattern: "D & K lines" -> rows D, K (non-contiguous treated as range)
    m = re.match(r'^([A-N])\s*[&,]\s*([A-N])', val)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_type'] = 'ROW_ONLY'
        return result

    # === ROW + COLUMN PATTERNS ===

    # Pattern: "A/B 32" or "K/L 33 LINE" -> rows A-B, col 32
    m = re.match(r'^([A-N])[/]([A-N])\s+(\d+)', val)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(3))
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "B/C17 LINE" -> rows B-C, col 17 (no space between letters and col)
    m = re.match(r'^([A-N])[/]([A-N])(\d+)', val)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(3))
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "A-/B 13-17" or "A/B- 13-17" -> rows A-B, cols 13-17 (with misplaced dash)
    m = re.match(r'^([A-N])[-/]+([A-N])[-\s]+(\d+)[-–](\d+)', val)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        cols = sorted([float(m.group(3)), float(m.group(4))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "C/D-32 LINE" or "A/B -30" -> rows C-D, col 32
    m = re.match(r'^([A-N])[/]([A-N])[-\s]+(\d+)', val)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(3))
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "23/24 C-D LINE" -> cols 23-24, rows C-D
    m = re.match(r'^(\d+)[/](\d+)\s+([A-N])[-–]([A-N])', val)
    if m:
        cols = sorted([float(m.group(1)), float(m.group(2))])
        result['grid_col_min'], result['grid_col_max'] = cols
        rows = sorted([m.group(3), m.group(4)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "A/31 LINE" -> row A, col 31
    m = re.match(r'^([A-N])[/](\d+)\s*LINE', val, re.IGNORECASE)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(2))
        result['grid_type'] = 'POINT'
        return result

    # Pattern: "A 30-31" -> row A, col 30-31
    m = re.match(r'^([A-N])\s+(\d+)[-–](\d+)', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(3))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "D line 1-33" -> row D, col 1-33
    m = re.match(r'^([A-N])\s*LINE\s+(\d+)[-–](\d+)', val, re.IGNORECASE)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(3))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "C/11 - C/22" or "L/6 - L/11" -> row C, col 11-22
    m = re.match(r'^([A-N])/(\d+)\s*[-–]\s*([A-N])?/?(\d+)', val)
    if m:
        result['grid_row_min'] = m.group(1)
        result['grid_row_max'] = m.group(3) or m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(4))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "D6/D19" -> row D, col 6-19
    m = re.match(r'^([A-N])(\d+)/([A-N])?(\d+)$', val)
    if m:
        result['grid_row_min'] = m.group(1)
        result['grid_row_max'] = m.group(3) or m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(4))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "A LINE" or "C LINE" -> row only
    m = re.match(r'^([A-N])\s*LINE\s*$', val, re.IGNORECASE)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        result['grid_type'] = 'ROW_ONLY'
        return result

    # Pattern: "A Line / description" or "N LINE / Roof Edge" -> row only with description
    m = re.match(r'^([A-N])\s*LINE\s*[/]', val, re.IGNORECASE)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        result['grid_type'] = 'ROW_ONLY'
        return result

    # Pattern: "D-E description" or "D-E Troughs" -> row range with description
    m = re.match(r'^([A-N])[-–]([A-N])\s+[A-Z]', val)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_type'] = 'ROW_ONLY'
        return result

    # === COLUMN ONLY PATTERNS ===

    # Pattern: "LINE 33" or "GL 33" -> col only
    m = re.match(r'^(LINE|GL)\s*(\d+)', val, re.IGNORECASE)
    if m:
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(2))
        result['grid_type'] = 'COL_ONLY'
        return result

    # Pattern: "GL L 31" or "GL N 17" -> row + col
    m = re.match(r'^GL\s+([A-N])\s+(\d+)', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(2))
        result['grid_type'] = 'POINT'
        return result

    # Pattern: "GL L.5 31-33" -> row L.5, cols 31-33
    m = re.match(r'^GL\s+([A-N])\.?(\d*)\s+(\d+)[-–](\d+)', val)
    if m:
        row = m.group(1) + ('.' + m.group(2) if m.group(2) else '')
        result['grid_row_min'] = result['grid_row_max'] = row
        cols = sorted([float(m.group(3)), float(m.group(4))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "L-5 LINE" or "L- 28 LINE" -> row L, col 5
    m = re.match(r'^([A-N])[-]\s*(\d+)\s*LINE', val, re.IGNORECASE)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(2))
        result['grid_type'] = 'POINT'
        return result

    # Pattern: "J-1,2" -> row J, cols 1-2 (comma-separated columns)
    m = re.match(r'^([A-N])[-](\d+),(\d+)', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(3))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "E.5-F" -> row range with decimal
    m = re.match(r'^([A-N])\.(\d+)[-–]([A-N])', val)
    if m:
        row1 = f"{m.group(1)}.{m.group(2)}"
        result['grid_row_min'] = row1
        result['grid_row_max'] = m.group(3)
        result['grid_type'] = 'ROW_ONLY'
        return result

    # Pattern: "M .5 -13" or "M.5/13" -> row M.5, col 13
    m = re.match(r'^([A-N])\s*\.?(\d+)\s*[-/]\s*(\d+)', val)
    if m:
        row_decimal = m.group(2)
        result['grid_row_min'] = result['grid_row_max'] = f"{m.group(1)}.{row_decimal}"
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(3))
        result['grid_type'] = 'POINT'
        return result

    # Pattern: "33 LINE" -> col only
    m = re.match(r'^(\d+)\s*LINE', val, re.IGNORECASE)
    if m:
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(1))
        result['grid_type'] = 'COL_ONLY'
        return result

    # Pattern: "GL-33/K-N" or "GL 33/K-N" -> col 33, rows K-N
    m = re.match(r'^GL[-\s]?(\d+)[/\s]+([A-N])[-–]([A-N])', val)
    if m:
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(1))
        rows = sorted([m.group(2), m.group(3)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "GL-N/30-32" -> row N, cols 30-32
    m = re.match(r'^GL[-\s]?([A-N])[/\s]+(\d+)[-–](\d+)', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(3))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "33 Canopy" or "33 description" -> col with description
    m = re.match(r'^(\d+)\s+[A-Z]', val)
    if m:
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(1))
        result['grid_type'] = 'COL_ONLY'
        return result

    # Pattern: "16-20 description" -> col range with description
    m = re.match(r'^(\d+)[-–](\d+)\s+[A-Z]', val)
    if m:
        cols = sorted([float(m.group(1)), float(m.group(2))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'COL_ONLY'
        return result

    # Pattern: "N-5 to N33" -> row N, cols 5-33
    m = re.match(r'^([A-N])[-]?(\d+)\s+TO\s+([A-N])?[-]?(\d+)', val, re.IGNORECASE)
    if m:
        result['grid_row_min'] = m.group(1)
        result['grid_row_max'] = m.group(3) or m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(4))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Pattern: "K/D COLUMNS" or "K/D description" -> multiple rows
    m = re.match(r'^([A-N])[/]([A-N])\s+[A-Z]', val)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_type'] = 'ROW_ONLY'
        return result

    # Pattern: "K2.5 suffix" -> row K.5 with building suffix
    m = re.match(r'^([A-N])(\d+\.?\d*)\s+(SUW|SUE|FAB)', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(2))
        result['grid_type'] = 'POINT'
        return result

    # Pattern: Just "33" -> col only
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

    # Row + col range: "N-29-23" -> row N, cols 23-29
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
    # Additional patterns (Phase 2 - tilde notation, etc.)
    # ==========================================================================

    # Tilde col-only range: E/5~E/12 or E/8~9 -> row E, cols 5-12 or 8-9
    m = re.match(r'^([A-N])/(\d+)~(?:[A-N]/)?(\d+)$', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(3))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Tilde row-col range: A~C/7~A~C/29 -> rows A-C, cols 7-29
    m = re.match(r'^([A-N])~([A-N])/(\d+)~(?:[A-N]~[A-N]/)?(\d+)$', val)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        cols = sorted([float(m.group(3)), float(m.group(4))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Tilde compact range: E3~J18 -> rows E-J, cols 3-18
    m = re.match(r'^([A-N])(\d+)~([A-N])(\d+)$', val)
    if m:
        rows = sorted([m.group(1), m.group(3)])
        result['grid_row_min'], result['grid_row_max'] = rows
        cols = sorted([float(m.group(2)), float(m.group(4))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'RANGE'
        return result

    # Tilde col range: C3~4 -> row C, cols 3-4
    m = re.match(r'^([A-N])(\d+)~(\d+)$', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        cols = sorted([float(m.group(2)), float(m.group(3))])
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

    # Tilde col-only: 3~8, 6~32 -> cols 3-8
    m = re.match(r'^(\d+)~(\d+)$', val)
    if m:
        cols = sorted([float(m.group(1)), float(m.group(2))])
        result['grid_col_min'], result['grid_col_max'] = cols
        result['grid_type'] = 'COL_ONLY'
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

    # G/J-LINE -> rows G-J
    m = re.match(r'^([A-N])/([A-N])[-\s]*LINE', val)
    if m:
        rows = sorted([m.group(1), m.group(2)])
        result['grid_row_min'], result['grid_row_max'] = rows
        result['grid_type'] = 'ROW_ONLY'
        return result

    # B-4 MEZZ -> row B, col 4
    m = re.match(r'^([A-N])[-](\d+)\s+(MEZZ|MEZZANINE)', val)
    if m:
        result['grid_row_min'] = result['grid_row_max'] = m.group(1)
        result['grid_col_min'] = result['grid_col_max'] = float(m.group(2))
        result['grid_type'] = 'POINT'
        return result

    # CE##P patterns: CE8P, CE22P -> equipment with column hint
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
