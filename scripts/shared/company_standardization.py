#!/usr/bin/env python3
"""
Company name standardization for Samsung Taylor FAB1 project.

Normalizes company names across PSI, RABA, QC Logs, and other sources
to enable cross-source analysis.

Company aliases are loaded from the generated dimension files:
- dim_company.csv: Canonical company names
- map_company_aliases.csv: Alias → company_id mapping

To update company aliases, edit:
  scripts/integrated_analysis/dimensions/build_company_dimension.py

Then rebuild with:
  python -m scripts.integrated_analysis.dimensions.build_company_dimension
"""

import re
import sys
from pathlib import Path
from typing import Optional, Dict, Tuple, List

import pandas as pd

# Add project root to path for settings
_project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import settings


# =============================================================================
# COMPANY ALIAS LOADING (from generated CSV files)
# =============================================================================

# Lazy-loaded company alias lookup
_ALIAS_LOOKUP: Optional[Dict[str, str]] = None
_COMPANY_ALIASES: Optional[Dict[str, List[str]]] = None


def _load_company_aliases():
    """
    Load company aliases from generated CSV files.

    Populates:
    - _ALIAS_LOOKUP: alias (lowercase) -> canonical_name
    - _COMPANY_ALIASES: canonical_name -> [list of aliases]
    """
    global _ALIAS_LOOKUP, _COMPANY_ALIASES

    if _ALIAS_LOOKUP is not None:
        return

    _ALIAS_LOOKUP = {}
    _COMPANY_ALIASES = {}

    # Paths to generated files
    dim_company_path = settings.PROCESSED_DATA_DIR / 'integrated_analysis' / 'dim_company.csv'
    map_aliases_path = settings.PROCESSED_DATA_DIR / 'integrated_analysis' / 'map_company_aliases.csv'

    # Check if files exist
    if not dim_company_path.exists() or not map_aliases_path.exists():
        # Fallback: return empty lookup (will use original name)
        print(f"Warning: Company dimension files not found. Run build_company_dimension.py first.")
        return

    # Load dim_company for company_id -> canonical_name mapping
    dim_company = pd.read_csv(dim_company_path)
    id_to_canonical = dict(zip(dim_company['company_id'], dim_company['canonical_name']))

    # Load map_company_aliases
    map_aliases = pd.read_csv(map_aliases_path)

    # Build lookups
    for _, row in map_aliases.iterrows():
        company_id = row['company_id']
        alias = str(row['alias']).lower().strip()
        canonical = id_to_canonical.get(company_id)

        if canonical:
            # Build reverse lookup: alias -> canonical
            _ALIAS_LOOKUP[alias] = canonical

            # Build forward lookup: canonical -> [aliases]
            if canonical not in _COMPANY_ALIASES:
                _COMPANY_ALIASES[canonical] = []
            _COMPANY_ALIASES[canonical].append(alias)


def _clean_company_name(name: str) -> str:
    """
    Clean a company name for lookup.

    - Lowercase
    - Remove extra whitespace
    - Remove common suffixes like Inc., LLC, etc.
    - Remove parenthetical notes like "(a Kiwa company)"
    """
    if not name:
        return ""

    # Lowercase and strip
    cleaned = name.lower().strip()

    # Remove parenthetical content for matching (but keep for display)
    cleaned = re.sub(r'\s*\([^)]*\)\s*', ' ', cleaned)

    # Remove common suffixes
    suffixes = [
        r',?\s*inc\.?$',
        r',?\s*llc\.?$',
        r',?\s*corp\.?$',
        r',?\s*co\.?$',
        r',?\s*ltd\.?$',
    ]
    for suffix in suffixes:
        cleaned = re.sub(suffix, '', cleaned, flags=re.IGNORECASE)

    # Normalize whitespace
    cleaned = ' '.join(cleaned.split())

    return cleaned


def standardize_company(name: Optional[str]) -> Optional[str]:
    """
    Standardize a company name to its canonical form.

    Args:
        name: Raw company name from source data

    Returns:
        Canonical company name, or original name if no match found
    """
    if not name:
        return None

    # Ensure aliases are loaded
    _load_company_aliases()

    # Clean for lookup
    cleaned = _clean_company_name(name)

    if not cleaned:
        return None

    # Direct lookup
    if cleaned in _ALIAS_LOOKUP:
        return _ALIAS_LOOKUP[cleaned]

    # Try without cleaning (in case alias includes suffixes)
    name_lower = name.lower().strip()
    if name_lower in _ALIAS_LOOKUP:
        return _ALIAS_LOOKUP[name_lower]

    # No match - return original with title case normalization
    return name.strip()


def standardize_company_with_original(name: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Standardize a company name, returning both canonical and original.

    Args:
        name: Raw company name from source data

    Returns:
        Tuple of (canonical_name, original_name)
    """
    if not name:
        return (None, None)

    canonical = standardize_company(name)
    return (canonical, name.strip())


def get_all_canonical_companies() -> List[str]:
    """Return list of all canonical company names."""
    _load_company_aliases()
    return sorted(_COMPANY_ALIASES.keys()) if _COMPANY_ALIASES else []


def get_company_aliases(canonical: str) -> List[str]:
    """Return all aliases for a canonical company name."""
    _load_company_aliases()
    return _COMPANY_ALIASES.get(canonical, []) if _COMPANY_ALIASES else []


def reload_company_aliases():
    """Force reload of company aliases from CSV files."""
    global _ALIAS_LOOKUP, _COMPANY_ALIASES
    _ALIAS_LOOKUP = None
    _COMPANY_ALIASES = None
    _load_company_aliases()


# =============================================================================
# INSPECTOR NAME STANDARDIZATION
# =============================================================================

INSPECTOR_ALIASES: Dict[str, list] = {
    "Hamidullah Sadra": [
        "hamid sadra",
    ],
    "Ferdinand Tchieme": [
        "ferdinand j. techieme",
        "ferdinand j. tchieme",
        "ferdinand techieme",
    ],
    "Ahmad Amiri": [
        "ahmed amiri",
    ],
    "Olayinka Akanni": [
        "ola akanni",
    ],
}

_INSPECTOR_LOOKUP: Dict[str, str] = {}
for canonical, aliases in INSPECTOR_ALIASES.items():
    _INSPECTOR_LOOKUP[canonical.lower()] = canonical
    for alias in aliases:
        _INSPECTOR_LOOKUP[alias.lower()] = canonical


def standardize_inspector(name: Optional[str]) -> Optional[str]:
    """
    Standardize an inspector name.

    Args:
        name: Raw inspector name

    Returns:
        Canonical inspector name, or original if no match
    """
    if not name:
        return None

    cleaned = name.lower().strip()

    if cleaned in _INSPECTOR_LOOKUP:
        return _INSPECTOR_LOOKUP[cleaned]

    # Return original with proper casing
    return name.strip()


# =============================================================================
# TRADE/INSPECTION TYPE STANDARDIZATION
# =============================================================================
# Note: Most trades are variations of Architectural/Drywall/Framing

TRADE_ALIASES: Dict[str, list] = {
    "Architectural": [
        # Simple forms
        "arch",
        "architectural",
        "architecture",
        "archi",
        "arch.",
        # Parenthetical variations
        "arch (architectural)",
        "arch (architecture)",
        "architectural (arch)",
        "architecture (arch)",
        "arch (architectural discipline)",
        "architectural (arch) discipline",
        'architectural ("arch")',
        "architectural (discipline: arch)",
        "discipline: arch",
        "discipline: arch (architectural)",
        # ARCH uppercase variations
        "arch (architectural)",
        "architectural (arch)",
        "arch (architecture)",
        "architecture (arch)",
        # X_ARCH variations
        "architectural (x_arch)",
        "architectural (discipline: x_arch)",
        "architectural (x arch)",
        "v arch",
    ],
    "Drywall": [
        # Simple forms
        "drywall",
        "dry wall",
        "interior wall (dry wall)",
        "drywall (2nd layer)",
        # Framing combinations
        "framing",
        "framing & drywall",
        "framing and drywall",
        "framing/drywall",
        "framing / drywall",
        "drywall & framing",
        "drywall and framing",
        "drywall/framing",
        "drywall / framing",
        "(framing & drywall)",
        # Architectural + Drywall combinations
        "architectural / drywall",
        "architectural/drywall",
        "arch / drywall",
        "arch/drywall",
        "drywall / architectural",
        "drywall/architectural",
        "drywall / arch",
        "drywall/arch",
        "architectural (drywall)",
        "arch (drywall)",
        "drywall (arch)",
        "drywall (architectural)",
        "drywall (arch discipline)",
        "drywall (discipline: arch)",
        "drywall (architectural discipline)",
        "drywall (architectural/arch)",
        "drywall/architectural (arch)",
        "architectural/drywall (arch)",
        "architecture / drywall",
        "architecture/drywall",
        "drywall / architecture",
        "architecture/drywall (arch)",
        "drywall / architecture (arch)",
        # Triple combinations
        "architectural / framing & drywall",
        "arch / framing & drywall",
        "architectural / framing",
        "architectural/framing",
        "arch / framing",
        "arch/framing",
        "framing / architectural",
        "architectural (framing & drywall)",
        "architectural (framing)",
        "arch (architectural) / framing & drywall",
        "arch (architectural) / drywall",
        "arch (architectural) / framing",
        "architectural (arch) / framing & drywall",
        "architectural (arch) / drywall",
        "architectural (arch) / framing",
        "architecture (arch) / framing & drywall",
        "architecture (arch) / drywall",
        "arch (architecture) / framing & drywall",
        "arch (architecture) / drywall",
        "arch (architecture) / framing",
        "arch (architectural / framing & drywall)",
        "arch (architectural / drywall)",
        "arch (architecture/drywall)",
        "arch (architectural/drywall)",
        "architectural/framing & drywall",
        "arch/framing & drywall",
        "architectural / drywall framing",
        "architectural / drywall Framing",
        "arch / drywall & framing",
        "arch / framing / drywall",
        "framing & drywall (architectural)",
        "framing & drywall (architecture)",
        "framing & drywall (discipline: arch)",
        "framing & drywall (architectural discipline)",
        "framing and drywall (architectural)",
        # ARCH uppercase variations
        "arch (architectural) / framing & drywall",
        "arch (architectural) / drywall",
        "architectural (arch) / framing & drywall",
        "architectural (arch) / drywall",
        "architectural (arch) / framing",
        "drywall / architectural (arch)",
        "drywall / arch",
        "architectural (arch) discipline",
        "architectural (arch discipline)",
        "arch (architectural) discipline",
        # CJ (ceiling joist) variations
        "framing & drywall & cj",
        "arch / framing & drywall & cj",
        "architectural / framing & drywall & cj",
        "architectural (framing & drywall & cj)",
        "arch (architectural) / framing & drywall & cj shaftliner",
        # Drywall-specific discipline notes
        "drywall (arch discipline on wir)",
        # Other arch combos
        "architectural/drywall and framing",
        "architectural (drywall/framing)",
        "architectural/framing (arch)",
        "architectural/framing (secai/axios)",
        "architectural - framing & drywall",
        "architectural (arch) - framing & drywall",
        "architectural (arch) / framing and drywall",
        "architectural (arch) / painting & drywall",
        "architectural (arch) / cleanroom gypsum panel installation",
        # Sheathing
        "drywall / sheathing",
    ],
    "Structural Steel": [
        "structural steel",
        "steel",
        "structural",
        "architectural/steel",
    ],
    "Concrete": [
        "concrete",
        "conc",
    ],
    "Mechanical": [
        "mechanical",
        "mech",
        "arch and mech",
        "architectural (arch) and mechanical (mech)",
    ],
    "Electrical": [
        "electrical",
        "elec",
        "elect",
        "electrical contractor",
    ],
    "Plumbing": [
        "plumbing",
        "plumb",
    ],
    "Fire Protection": [
        "fire protection",
        "fire",
        "fire-stopping",
        "sprinkler",
        "hvac / fire-stopping",
    ],
    "HVAC": [
        "hvac",
    ],
    "Painting": [
        "painter",
        "painting",
        "multiple painting crews",
    ],
}

_TRADE_LOOKUP: Dict[str, str] = {}
for canonical, aliases in TRADE_ALIASES.items():
    _TRADE_LOOKUP[canonical.lower()] = canonical
    for alias in aliases:
        _TRADE_LOOKUP[alias.lower()] = canonical


def standardize_trade(name: Optional[str]) -> Optional[str]:
    """
    Standardize a trade/inspection type name.

    Args:
        name: Raw trade name

    Returns:
        Canonical trade name, or original if no match
    """
    if not name:
        return None

    cleaned = name.lower().strip()

    if cleaned in _TRADE_LOOKUP:
        return _TRADE_LOOKUP[cleaned]

    # Check if it looks like a person name (not a trade)
    # Person names typically don't contain trade keywords
    trade_keywords = ['arch', 'drywall', 'steel', 'concrete', 'mech', 'elec', 'plumb', 'fire', 'hvac', 'paint', 'framing']
    has_trade_keyword = any(kw in cleaned for kw in trade_keywords)

    if not has_trade_keyword:
        # Likely a person name incorrectly in trade field - return None
        # to indicate this should not be used as a trade
        return None

    return name.strip()


def infer_trade_from_inspection_type(inspection_type: Optional[str]) -> Optional[str]:
    """
    Infer trade from inspection type when trade is not explicitly provided.

    Maps inspection types to their corresponding trades based on common patterns.

    Args:
        inspection_type: The inspection type string (normalized or raw)

    Returns:
        Inferred trade name, or None if no match
    """
    if not inspection_type:
        return None

    insp_lower = inspection_type.lower()

    # Mapping of inspection type keywords to trades
    # Order matters - more specific patterns first
    trade_inference_rules = [
        # Drywall/Framing related
        (["drywall", "gypsum", "sheetrock", "layer inspection", "1st layer", "2nd layer", "3rd layer"], "Drywall"),
        (["framing", "bottom plate", "top plate", "stud", "sliptrack", "frame inspection", "frame remediation"], "Drywall"),
        (["control joint", "cj inspection", "cj gap", "cj framing"], "Drywall"),
        (["screw inspection", "fastener"], "Drywall"),
        (["shaft wall", "shaft liner", "shaftliner"], "Drywall"),
        (["ceiling", "scp ceiling", "ceiling panel"], "Drywall"),
        (["raised access floor", "access floor"], "Drywall"),

        # Structural
        (["structural steel", "steel erection", "steel connection", "bolt inspection", "steel beam"], "Structural Steel"),
        (["welding", "weld inspection", "vt inspection", "aws"], "Structural Steel"),
        (["anchor", "dowel", "post-installed", "epoxy dowel", "coupler"], "Structural Steel"),

        # Concrete
        (["concrete", "compressive strength", "cylinder", "placement", "pour", "slab"], "Concrete"),
        (["precast", "waffle panel", "waffle slab", "double t"], "Concrete"),
        (["pier", "pile", "foundation", "caisson", "drilled"], "Concrete"),
        (["grout", "mortar", "honeycomb", "void form", "void-form"], "Concrete"),

        # Finishes
        (["paint", "coating", "primer", "topcoat", "nace", "surface preparation", "touch up"], "Painting"),
        (["firestop", "fire stop", "fireproofing", "intumescent", "penetration seal", "fire caulk"], "Fire Protection"),
        (["waterproofing", "membrane", "dampproofing"], "Waterproofing"),

        # MEP
        (["electrical", "conduit", "wiring"], "Electrical"),
        (["mechanical", "hvac", "ductwork"], "Mechanical"),
        (["plumbing", "piping", "pipe"], "Plumbing"),

        # General/Architectural
        (["architectural", "arch inspection", "visual inspection", "door and hardware"], "Architectural"),
        (["masonry", "cmu", "block", "brick"], "Masonry"),
    ]

    for keywords, trade in trade_inference_rules:
        for keyword in keywords:
            if keyword in insp_lower:
                return trade

    return None


# =============================================================================
# INSPECTION TYPE CATEGORIZATION
# =============================================================================
# Groups 2000+ inspection types into ~15 standard categories

INSPECTION_TYPE_CATEGORIES: Dict[str, list] = {
    "Drywall": [
        "drywall",
        "1st layer drywall",
        "2nd layer drywall",
        "3rd layer drywall",
        "layer drywall",
        "drywall inspection",
        "gypsum",
        "cleanroom gypsum",
        "sheetrock",
        # Layer inspections (PSI)
        "1st layer",
        "2nd layer",
        "3rd layer",
        "1st & 2nd layer",
        "layer inspection",
        "layer sheathing",
        # Control joint inspections
        "control joint",
        "cj inspection",
        "cj gap",
        "cj framing",
        # Contamination/wall inspections
        "contamination wall",
        "wall inspection",
        # Shaft wall / liner
        "shaft wall",
        "shaft liner",
        "shaftliner",
        # Ceiling
        "ceiling",
        "scp ceiling",
        "ceiling panel",
        "cement board",
        # Access floor
        "raised access floor",
        "access floor",
    ],
    "Framing": [
        "framing",
        "framing inspection",
        "frame",
        "bottom plate",
        "top plate",
        "stud",
        "sliptrack",
        "frame remediation",
    ],
    "Screw Inspection": [
        "screw inspection",
        "screw",
        "fastener",
    ],
    "Concrete": [
        "concrete",
        "compressive strength",
        "cylinder",
        "placement",
        "pre-placement",
        "pour",
        "slab",
        "mortar",
        "grout",
        # Precast elements
        "precast",
        "precast waffle",
        "precast panel",
        "precast column",
        "precast spandrel",
        "double t panel",
        "waffle panel",
        "waffle slab",
        # Storm/void forms
        "storm void",
        "void form",
        "void-form",
        # Repairs
        "honeycomb",
        "honeycomb repair",
        "honeycomb wall",
        "column patching",
        "curb repair",
    ],
    "Structural Steel": [
        "structural steel",
        "steel erection",
        "steel connection",
        "bolt",
        "high strength bolt",
        # Post-installed anchors and dowels
        "post-installed anchor",
        "post-installed dowel",
        "post installed anchor",
        "post installed dowel",
        "post installed embed",
        "anchor rod",
        "anchor inspection",
        "dowel inspection",
        "dowel remediation",
        "epoxy dowel",
        "epoxied dowel",
        "drilled epoxy",
        "drilled epoxied",
        "coupler",
        "steel beam",
        "embed",
    ],
    "Welding": [
        "welding",
        "weld",
        "visual inspection (vt)",
        "vt inspection",
        "aws",
    ],
    "Drilled Pier/Foundation": [
        "drilled pier",
        "pier",
        "pile",
        "foundation",
        "caisson",
        "shaft",
        "excavation",
    ],
    "Firestop": [
        "firestop",
        "fire stop",
        "fire resistive",
        "fireproofing",
        "intumescent",
        "penetration seal",
        "joint system",
        "fire caulk",
        "fire caulking",
        # SFRM/IFRM specific
        "sfrm",
        "ifrm",
        "sfrm-substrate",
        "substrate inspection",
        "thickness inspection",
    ],
    "Coating/Painting": [
        "coating",
        "paint",
        "nace",
        "surface preparation",
        "primer",
        "topcoat",
        "corrosion",
        "touch up",
        "dull-scraper",
        "scraper test",
    ],
    "Soil/Earthwork": [
        "soil",
        "nuclear density",
        "compaction",
        "backfill",
        "earthwork",
        "subgrade",
        "base material",
        "proctor",
        # Laboratory soil testing
        "moisture-density",
        "atterberg",
        "sieve analysis",
    ],
    "Reinforcing Steel": [
        "reinforcing",
        "rebar",
        "reinforcement",
        "epoxy coated",
        "bar placement",
    ],
    "Masonry": [
        "masonry",
        "cmu",
        "block",
        "brick",
        "mortar cube",
    ],
    "Waterproofing": [
        "waterproofing",
        "membrane",
        "dampproofing",
        "below grade",
    ],
    "MEP": [
        "mechanical",
        "electrical",
        "plumbing",
        "hvac",
        "ductwork",
        "piping",
        "conduit",
    ],
    "Visual/General": [
        "visual inspection",
        "daily field report",
        "construction operations",
        "general inspection",
        "observation",
        # Materials/general inspections
        "materials inspection",
        "material inspection",
        "visual",
        "arch inspection",
        "architectural inspection",
        "re-inspection",
        "final inspection",
        # Architectural/finishing
        "architectural",
        "door and hardware",
        "door hardware",
        "expansion joint",
        "space joint",
        "sqr pipe",
        # Quality control and testing
        "quality control",
        "laboratory testing",
        "construction inspection",
        "load bearing",
        "inspection report",
    ],
}

_INSPECTION_CATEGORY_LOOKUP: Dict[str, str] = {}
for category, keywords in INSPECTION_TYPE_CATEGORIES.items():
    for keyword in keywords:
        _INSPECTION_CATEGORY_LOOKUP[keyword.lower()] = category


def categorize_inspection_type(inspection_type: Optional[str]) -> Optional[str]:
    """
    Categorize an inspection type into a standard category.

    Args:
        inspection_type: Raw inspection type string

    Returns:
        Category name, or None if no match
    """
    if not inspection_type:
        return None

    cleaned = inspection_type.lower().strip()

    # Check each keyword
    for keyword, category in _INSPECTION_CATEGORY_LOOKUP.items():
        if keyword in cleaned:
            return category

    return None


# =============================================================================
# LEVEL STANDARDIZATION
# =============================================================================
# Standardizes level values including underground/foundation work

LEVEL_ALIASES: Dict[str, list] = {
    "1F": ["1f", "1", "level 1", "first floor", "ground", "ground floor", "g/f", "01f"],
    "2F": ["2f", "2", "level 2", "second floor", "02f"],
    "3F": ["3f", "3", "level 3", "third floor", "03f"],
    "4F": ["4f", "4", "level 4", "fourth floor", "04f"],
    "5F": ["5f", "5", "level 5", "fifth floor", "05f"],
    "6F": ["6f", "6", "level 6", "sixth floor", "06f"],
    "7F": ["7f", "7", "level 7", "seventh floor", "07f"],
    "8F": ["8f", "8", "level 8", "08f"],
    "9F": ["9f", "9", "level 9", "09f"],
    "ROOF": ["roof", "rooftop", "rf", "penthouse", "ph"],
    "B1": ["b1", "basement", "basement 1", "below grade 1", "-1", "ug", "underground", "below grade", "subgrade", "b1f"],
    "B2": ["b2", "basement 2", "below grade 2", "-2", "b2f"],
    "OUTSIDE": ["outside", "exterior", "ground level", "at grade", "site"],
    "FOUNDATION": ["foundation", "ftg", "footing", "mat foundation"],
}

_LEVEL_LOOKUP: Dict[str, str] = {}
for canonical, aliases in LEVEL_ALIASES.items():
    _LEVEL_LOOKUP[canonical.lower()] = canonical
    for alias in aliases:
        _LEVEL_LOOKUP[alias.lower()] = canonical


def standardize_level(level: Optional[str]) -> Optional[str]:
    """
    Standardize a level value.

    Args:
        level: Raw level string

    Returns:
        Standardized level, or original if no match
    """
    if not level:
        return None

    cleaned = level.lower().strip()

    if cleaned in _LEVEL_LOOKUP:
        return _LEVEL_LOOKUP[cleaned]

    # Check if it's a valid level format already (e.g., "18F")
    if re.match(r'^\d+f?$', cleaned, re.IGNORECASE):
        num = re.match(r'^(\d+)', cleaned).group(1)
        return f"{num}F"

    return level.strip()


def infer_level_from_location(location_raw: Optional[str]) -> Optional[str]:
    """
    Attempt to extract level from a raw location string.

    Args:
        location_raw: Raw location description

    Returns:
        Inferred level, or None if not found
    """
    if not location_raw:
        return None

    loc = location_raw.upper()

    # Check for explicit level mentions
    patterns = [
        r'\b(\d+)F\b',  # 1F, 2F, etc.
        r'LEVEL\s*(\d+)',  # Level 1, Level 2
        r'\bLV(\d+)\b',  # Lv4, LV4 (SECAI format)
        r'\bL(\d+)\b',  # L4, L1
        r'(\d+)(?:ST|ND|RD|TH)\s*FLOOR',  # 1st Floor, 2nd Floor
        r'FLOOR\s*(\d+)',  # Floor 1
    ]

    for pattern in patterns:
        match = re.search(pattern, loc)
        if match:
            num = match.group(1)
            return f"{num}F"

    # Check for special levels
    if 'ROOF' in loc:
        return 'ROOF'
    if 'BASEMENT' in loc or 'B1' in loc or 'B2' in loc:
        if 'B2' in loc:
            return 'B2'
        return 'B1'

    # Check for foundation/underground indicators
    foundation_keywords = ['PIER', 'PILE', 'FOUNDATION', 'FOOTING', 'EXCAVATION', 'CAISSON']
    if any(kw in loc for kw in foundation_keywords):
        return 'FOUNDATION'

    # Check for underground
    if 'UNDERGROUND' in loc or 'BELOW GRADE' in loc or 'SUBGRADE' in loc:
        return 'UG'

    return None


# =============================================================================
# FAILURE REASON CATEGORIZATION
# =============================================================================
# Categorizes failure reasons into root cause categories

FAILURE_CATEGORIES: Dict[str, list] = {
    "Workmanship - Fasteners": [
        "missing screw",
        "screw",
        "fastener",
        "nail",
        "bolt",
    ],
    "Workmanship - Alignment": [
        "alignment",
        "misalign",
        "deform",
        "bent",
        "out of plumb",
        "out of level",
        "gap",
    ],
    "Workmanship - Installation": [
        "installation",
        "not installed",
        "improper",
        "incorrect",
        "wrong location",
        "deficien",
    ],
    "Materials - Concrete Strength": [
        "compressive strength",
        "28 day",
        "7 day",
        "psi",
        "did not achieve",
    ],
    "Materials - Coating/Finish": [
        "coating",
        "paint",
        "uncured",
        "contamination",
        "holiday",
        "overspray",
        "mill",
        "bare metal",
    ],
    "Documentation": [
        "document",
        "drawing",
        "submittal",
        "not in accordance",
        "not compliant",
        "no ul system",
        "no tag",
    ],
    "Not Ready": [
        "not ready",
        "cancelled",
        "reschedul",
        "internal inspection",
        "didn't pass",
        "did not pass",
    ],
    "Access/Safety": [
        "scaffold",
        "access",
        "safety",
        "no cm present",
    ],
}

_FAILURE_CATEGORY_LOOKUP: Dict[str, str] = {}
for category, keywords in FAILURE_CATEGORIES.items():
    for keyword in keywords:
        _FAILURE_CATEGORY_LOOKUP[keyword.lower()] = category


def categorize_failure_reason(reason: Optional[str]) -> Optional[str]:
    """
    Categorize a failure reason into a root cause category.

    Args:
        reason: Raw failure reason string

    Returns:
        Category name, or None if no match
    """
    if not reason:
        return None

    cleaned = reason.lower().strip()

    # Check each keyword (order matters - more specific first)
    for keyword, category in sorted(_FAILURE_CATEGORY_LOOKUP.items(), key=lambda x: -len(x[0])):
        if keyword in cleaned:
            return category

    return "Other"


# =============================================================================
# CLI FOR TESTING
# =============================================================================

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        name = " ".join(sys.argv[1:])
        print(f"Input:     '{name}'")
        print(f"Company:   '{standardize_company(name)}'")
        print(f"Trade:     '{standardize_trade(name)}'")
    else:
        print("Company Standardization Test")
        print("=" * 60)

        # Load and show stats
        _load_company_aliases()
        print(f"\nLoaded {len(_ALIAS_LOOKUP)} company aliases")
        print(f"Covering {len(_COMPANY_ALIASES)} canonical companies")

        test_companies = [
            "Yates",
            "YATES",
            "W.G. Yates Construction",
            "Mark Hammond with Yates",
            "Samsung E&C America, Inc.",
            "SECAI",
            "Berg",
            "BERG",
            "BER G",
            "BERGO",
            "AMTS",
            "amys",
            "amt s",
            "Axios",
            "Axious",
            "JP Hi-Tech",
            "JPHI-TECH ENG INC.",
            "jp",
            "Baker Triangle",
            "Backer T",
            "Baker Concrete",
            "Rolling Plains",
            "Rolling Planes",
            "North Star",
            "NorthStar",
            "Some Unknown Company",
        ]

        print("\nCompany standardization:")
        for name in test_companies:
            canonical = standardize_company(name)
            if canonical != name:
                print(f"  '{name}' -> '{canonical}'")
            else:
                print(f"  '{name}' -> (no change)")

        print("\n" + "=" * 60)
        print("Trade standardization:")

        test_trades = [
            "Arch",
            "Architectural",
            "ARCH (Architectural) / Framing & Drywall",
            "Drywall / Architectural (Arch)",
            "Framing & Drywall",
            "Methaq Aheel",  # Person name - should return None
            "Brandon Torres",  # Person name - should return None
        ]

        for name in test_trades:
            canonical = standardize_trade(name)
            print(f"  '{name}' -> '{canonical}'")
