"""
Trade and Building Mapping Tables for Task Taxonomy

Maps P6 activity codes and inferred scopes to dim_trade categories.
"""

# =============================================================================
# Trade Mappings
# =============================================================================

# Map P6 Z-TRADE values to dim_trade trade_id
# Keys are lowercase for case-insensitive matching
Z_TRADE_TO_DIM_TRADE = {
    # Concrete (trade_id=1)
    'topping': 1, 'somd': 1, 'frp': 1, '03 conc': 1, 'cip wall': 1,
    'ratslab': 1, 'rat slab': 1, 'elevated slabs': 1, 'elevated slabs-1': 1,
    'cure': 1, 'concrete cure': 1, 'crc': 1, '03 dril': 1,
    'pour concrete': 1, 'cast-in place concrete - baker concrete': 1,
    'concrete - area': 1,

    # Steel (trade_id=2)
    'ds/ad': 2, 'ds/ad steel': 2, 'decking': 2, 'decking & detail': 2,
    '05 steel erect': 2, 'steel erection': 2, '05 steel fab': 2,
    'misc steel': 2, 'misc. steel': 2, 'truss erect': 2,
    'truss erect & infill': 2, 'truss erect, infill & decking': 2,
    'stairs': 2, 'steel erector - w&w': 2, 'steel fabricator - w&w': 2,
    'fab - intermediate columns': 2, 'fab - main columns': 2,
    'fab subfab and parapets': 2, 'fab waffle slabs': 2,
    'fiz - columns': 2, 'fiz east columns': 2, 'fiz west columns': 2,
    'inner columns c&l': 2, 'main columns c,e,g,j,l & intermediate h,f': 2,
    'intermediate f&h': 2, 'sue - columns a&b': 2, 'sue columns': 2,
    'sue - inner columns - c (set with fab)': 2,
    'suw - columns m&n': 2, 'suw columns': 2,
    'suw - inner columns - l (set with fab)': 2,
    'sup east a,b': 2, 'sup west n,m': 2,

    # Roofing (trade_id=3)
    '07 roofing': 3, 'roofing': 3, '07 waterproof': 3,
    'waterproofing - preferred': 3, '07 eifs': 3,
    'eifs (possibly stucco?)': 3, 'metal panels & roofing': 3,

    # Drywall (trade_id=4)
    'drywall': 4, 'cfmf': 4, 'cold formed metal framing': 4,
    'berg drywall': 4,

    # Finishes (trade_id=5)
    'arch': 5, 'flooring': 5, 'doors': 5, 'doors & hardware': 5,
    'doors & hardware - exterior': 5, 'doors & hardware - interior': 5,
    'doors & hardware - overhead': 5, 'doors-1': 5, 'doors-2': 5,
    'exp control': 5, 'expansion control': 5, 'miscellaneous': 5,
    'tbd architectural finishes': 5, 'painting': 5,
    'floor coating': 5, 'fixtures & wall protection': 5,
    'joint sealant & caulking': 5,

    # Fireproof (trade_id=6)
    'fireproofing': 6, 'ug fire-plumb': 6,

    # MEP (trade_id=7)
    'mep': 7, '16 - u/g elect': 7, 'underground electrical - (by owner)': 7,
    '15 - plumbing': 7, 'underground plumbing - cobb': 7,
    'u/g fire & plumbing': 7,

    # Insulation (trade_id=8)
    'insulation': 8, 'insulated metal panels': 8,

    # Earthwork (trade_id=9)
    'excavate': 9, 'excavate cip': 9, 'backfill': 9,
    'drilled shaft subcontractor - ah beck': 9, 'sitework (by owner)': 9,

    # Precast (trade_id=10)
    '03 pc fab': 10, '03 pc erect': 10, '03 precast': 10, 'precast': 10,
    'precast erector - sns erectors': 10, 'precast fabrication': 10,
    'precast fabricator - coreslab': 10, 'precast fabricator - gates': 10,
    'precast fabricator - heldenfehls': 10, 'precast fabricator - tindall': 10,

    # Panels (trade_id=11)
    'skin': 11,

    # General (trade_id=12)
    'owner': 12, 'owner & design team': 12, 'mile': 12, 'key milestones': 12,
    'yates': 12, '01 towers': 12, 'tower cranes': 12,
    'preassemble': 12, 'pre-assemble trusses': 12, 'main': 12,
    'bim / vdc': 12, 'temp': 12, 'oac/ mech pads': 12,
}

# dim_trade reference table
DIM_TRADE = {
    1: {'trade_code': 'CONCRETE', 'trade_name': 'Concrete'},
    2: {'trade_code': 'STEEL', 'trade_name': 'Structural Steel'},
    3: {'trade_code': 'ROOFING', 'trade_name': 'Roofing & Waterproofing'},
    4: {'trade_code': 'DRYWALL', 'trade_name': 'Drywall & Framing'},
    5: {'trade_code': 'FINISHES', 'trade_name': 'Architectural Finishes'},
    6: {'trade_code': 'FIREPROOF', 'trade_name': 'Fire Protection'},
    7: {'trade_code': 'MEP', 'trade_name': 'MEP Systems'},
    8: {'trade_code': 'INSULATION', 'trade_name': 'Insulation'},
    9: {'trade_code': 'EARTHWORK', 'trade_name': 'Earthwork & Foundations'},
    10: {'trade_code': 'PRECAST', 'trade_name': 'Precast Concrete'},
    11: {'trade_code': 'PANELS', 'trade_name': 'Metal Panels & Cladding'},
    12: {'trade_code': 'GENERAL', 'trade_name': 'General Conditions'},
}

# Map inferred scope codes to dim_trade
SCOPE_TO_TRADE_ID = {
    'CIP': 1, 'CTG': 1,  # Concrete
    'STL': 2, 'PRC': 10,  # Steel, Precast
    'ROF': 3, 'WPF': 3,  # Roofing
    'FRM': 4, 'DRY': 4,  # Drywall
    'FIN': 5, 'DOR': 5, 'SPE': 5,  # Finishes
    'FIR': 6,  # Fireproof
    'MEP': 7, 'ELV': 7,  # MEP
    'INS': 8,  # Insulation
    'PIR': 9, 'FND': 9, 'UGD': 9,  # Earthwork
    'PNL': 11, 'GLZ': 11,  # Panels
    'OWN': 12, 'MIL': 12, 'TRK': 12, 'TMP': 12,  # General
    # Default mappings for phases
    'DES': 12, 'PRO': 12, 'SUB': 12, 'FAB': 12,  # Preconstruction -> General
    'TST': 12, 'TRN': 12,  # Commissioning -> General
}

# =============================================================================
# Building Mappings
# =============================================================================

# Map Z-BLDG activity code values to building code
Z_BLDG_TO_CODE = {
    'fab building': 'FAB',
    'east support building': 'SUE',
    'west support building': 'SUW',
    'support building overall': 'SUP',
    'bim / vdc overall': None,
    'data center (gridlines 1-3)': 'FAB',
    'drilled piers': None,
    'key milestones': None,
    'north areas (team b -gridlines 17-33)': 'FAB',
    'south areas (team a - gridlines 3-17)': 'FAB',
}

# =============================================================================
# WBS Trade Pattern Mappings (for tier_4 extraction)
# =============================================================================

# Patterns to match in tier_4 for trade inference
# Order matters - more specific patterns first
WBS_TRADE_PATTERNS = [
    (r'PRECAST', 10),                                          # PRECAST
    (r'STRUCTURAL\s*STEEL|DS/AD\s*STEEL|ERECT.*STEEL', 2),    # STEEL
    (r'\bCONCRETE\b|FOUNDATION', 1),                          # CONCRETE
    (r'\bROOFING\b|WATERPROOF', 3),                           # ROOFING
    (r'METAL\s*PANEL', 11),                                   # PANELS
    (r'ARCHITECTURAL\s*FINISH', 5),                           # FINISHES
    (r'UNDERGROUND\s*UTIL', 9),                               # EARTHWORK
]


def get_trade_details(trade_id: int) -> dict:
    """Get trade_code and trade_name for a trade_id."""
    if trade_id and trade_id in DIM_TRADE:
        return DIM_TRADE[trade_id].copy()
    return {'trade_code': None, 'trade_name': None}
