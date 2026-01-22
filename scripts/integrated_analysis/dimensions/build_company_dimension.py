#!/usr/bin/env python3
"""
Build Company Dimension Tables

Unified script that generates both:
- dim_company.csv: Company dimension table with metadata
- map_company_aliases.csv: Company name variants → company_id mapping

This is the SINGLE SOURCE OF TRUTH for company data. All company definitions,
aliases, and metadata are defined here.

Schema (dim_company.csv):
- company_id: Unique identifier (integer)
- canonical_name: Standardized company name
- short_code: Abbreviated code (for displays, P6)
- tier: Company tier (OWNER, GC, T1_SUB, T2_SUB, OTHER)
- primary_trade_id: FK to dim_trade for main work type
- other_trade_ids: Comma-separated secondary trade IDs
- default_csi_section_id: FK to dim_csi_section
- notes: Additional information with data source references
- parent_company_id: FK to parent company (GC for subs)
- parent_confidence: Confidence of parent relationship (HIGH, MEDIUM, LOW)
- company_type: Classification (yates_self, yates_sub, major_contractor, etc.)
- is_yates_sub: Boolean flag for Yates subcontractors
- full_name: Full legal name where known

Schema (map_company_aliases.csv):
- company_id: FK to dim_company
- alias: Name variant (lowercase, for matching)
- source: Where alias was found (manual, projectsight, raba, psi, etc.)

Trade IDs (from dim_trade):
  1=CONCRETE, 2=STEEL, 3=ROOFING, 4=DRYWALL, 5=FINISHES, 6=FIREPROOF,
  7=MEP, 8=INSULATION, 9=EARTHWORK, 10=PRECAST, 11=PANELS, 12=GENERAL, 13=MASONRY

Usage:
    python -m scripts.integrated_analysis.dimensions.build_company_dimension
    python -m scripts.integrated_analysis.dimensions.build_company_dimension --verify
    python -m scripts.integrated_analysis.dimensions.build_company_dimension --scan-sources

Output:
    {WINDOWS_DATA_DIR}/processed/integrated_analysis/dimensions/dim_company.csv
    {WINDOWS_DATA_DIR}/processed/integrated_analysis/mappings/map_company_aliases.csv
"""

import argparse
import csv
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field

# Add project root to path
_project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import settings

# Output locations
DIM_OUTPUT_DIR = settings.PROCESSED_DATA_DIR / "integrated_analysis" / "dimensions"
MAP_OUTPUT_DIR = settings.PROCESSED_DATA_DIR / "integrated_analysis" / "mappings"
DIM_OUTPUT_FILE = DIM_OUTPUT_DIR / "dim_company.csv"
MAP_OUTPUT_FILE = MAP_OUTPUT_DIR / "map_company_aliases.csv"


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class Company:
    """Company definition with all metadata."""
    company_id: int
    canonical_name: str
    short_code: str
    tier: str  # OWNER, GC, T1_SUB, T2_SUB, OTHER
    company_type: str  # yates_self, yates_sub, major_contractor, precast_supplier, testing, other
    is_yates_sub: bool
    primary_trade_id: Optional[int] = None
    other_trade_ids: Optional[str] = None  # Comma-separated, e.g., "6,8"
    default_csi_section_id: Optional[int] = None
    notes: Optional[str] = None
    parent_company_id: Optional[int] = None
    parent_confidence: Optional[str] = None  # HIGH, MEDIUM, LOW
    full_name: Optional[str] = None
    aliases: List[str] = field(default_factory=list)  # List of name variants


# =============================================================================
# COMPANY DEFINITIONS - SINGLE SOURCE OF TRUTH
# =============================================================================
# All companies and their aliases are defined here.
# When adding a new company:
#   1. Add it to COMPANIES list below with a unique company_id
#   2. Include all known aliases in the aliases list
#   3. Document the data source in notes

COMPANIES: List[Company] = [
    # =========================================================================
    # SPECIAL ENTRIES
    # =========================================================================
    Company(
        company_id=0,
        canonical_name="Unknown/Activity Code",
        short_code="UNKNOWN",
        tier="OTHER",
        company_type="other",
        is_yates_sub=False,
        notes="Placeholder for unmapped or activity description codes",
        aliases=[],
    ),

    # =========================================================================
    # OWNER
    # =========================================================================
    Company(
        company_id=1,
        canonical_name="Samsung E&C",
        short_code="SECAI",
        tier="OWNER",
        company_type="major_contractor",
        is_yates_sub=False,
        primary_trade_id=12,
        default_csi_section_id=1,
        notes="Project owner",
        full_name="Samsung E&C America, Inc. (SECAI)",
        aliases=[
            "samsung",
            "samsung e&c",
            "samsung e&c america",
            "samsung e&c america, inc.",
            "samsung e&c america, inc",
            "secai",
            "seca",
            "secal",
            "secei",
            "samsung e&c america, inc. (secai)",
            "samsung e&c america, inc. (to)",
            "secai construction",
            "secai construction team",
            # TBM variations
            "secai consolidated",
        ],
    ),

    # =========================================================================
    # GENERAL CONTRACTORS
    # =========================================================================
    Company(
        company_id=2,
        canonical_name="Yates",
        short_code="YATES",
        tier="GC",
        company_type="yates_self",
        is_yates_sub=True,  # Yates itself is included in "Yates subs" for filtering
        primary_trade_id=12,
        default_csi_section_id=1,
        notes="General contractor",
        full_name="W. G. YATES & SONS CONSTRUCTION COMPANY",
        aliases=[
            "yates",
            "yates construction",
            "yates constructions",
            "w.g. yates",
            "wg yates",
            "w.g. yates & sons",
            "w.g. yates & sons construction",
            "w.g. yates construction",
            "yates & sons",
            "yates & sons construction company",
            "yates & cons construction company",
            "yates construction team",
            "yates qc",
            "yates field personnel",
            "yates subcon",
            # Person names associated with Yates
            "mark hammond with yates",
            "mark hammond with yates construction",
            "mark hammond",
            "sam w/ yates",
            "arturo carreon with yates construction",
            "arturo carreon with yates",
            "chris plassmann with yates",
            "mohammed a (with yates)",
        ],
    ),
    Company(
        company_id=34,
        canonical_name="Hensel Phelps",
        short_code="HP",
        tier="GC",
        company_type="major_contractor",
        is_yates_sub=False,
        primary_trade_id=12,
        default_csi_section_id=1,
        notes="General contractor",
        full_name="Hensel Phelps Construction Co.",
        aliases=[
            "hensel phelps",
            "hensel phelps construction",
            "hensel phelps construction co",
            "hensel phepls construction",  # Typo
            "hansel phelps",  # Typo
            "hp",
            "jose flores with hensel phelps construction",
        ],
    ),
    Company(
        company_id=44,
        canonical_name="PCL Construction",
        short_code="PCL",
        tier="GC",
        company_type="major_contractor",
        is_yates_sub=False,
        primary_trade_id=12,
        default_csi_section_id=1,
        notes="General contractor",
        full_name="PCL Construction",
        aliases=[
            "pcl",
            "pcl construction",
        ],
    ),
    Company(
        company_id=45,
        canonical_name="McCarthy",
        short_code="MCCARTHY",
        tier="GC",
        company_type="major_contractor",
        is_yates_sub=False,
        primary_trade_id=12,
        default_csi_section_id=1,
        notes="Cleanroom contractor",
        full_name="McCarthy Building Companies",
        aliases=[
            "mccarthy",
            "mccarthy building",
        ],
    ),
    Company(
        company_id=46,
        canonical_name="Austin Bridge",
        short_code="ABR",
        tier="GC",
        company_type="major_contractor",
        is_yates_sub=False,
        primary_trade_id=9,
        default_csi_section_id=51,
        notes="Sitework contractor",
        full_name="Austin Bridge & Road / Austin Global Construction",
        aliases=[
            "austin bridge",
            "austin bridge & road",
            "austin bridge and road",
            "abr",
            "ag",
            "austin global",
            "austin global construction",
            "austin g",
            "austin globale",
            "ag; austin global",
            "ag / austin global",
            "austin commercial",
        ],
    ),

    # =========================================================================
    # YATES T1 SUBCONTRACTORS - CONCRETE (trade=1)
    # =========================================================================
    Company(
        company_id=3,
        canonical_name="Baker Concrete",
        short_code="BAKER",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=1,
        default_csi_section_id=2,
        notes="Concrete - topping slabs, SOMD, elevated slabs (824K hrs, ProjectSight)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="BAKER CONCRETE CONSTRUCTION INC",
        aliases=[
            "baker concrete",
            "baker concrete construction",
            "baker construction",
            "baker and yates",
            "baker/yates",
            "yates/baker",
            "yates-baker",
            "yates/ baker",
            "beker / baker",
            "baker / baker concrete construction",
        ],
    ),
    Company(
        company_id=15,
        canonical_name="Infinity Concrete",
        short_code="INFINITY",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=1,
        default_csi_section_id=2,
        notes="Concrete placement, slabs (412K hrs, ProjectSight)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="INFINITY CONCRETE CONSTRUCTION LLC",
        aliases=[
            "infinity",
            "infinity concrete",
        ],
    ),
    Company(
        company_id=16,
        canonical_name="Latcon",
        short_code="LATCON",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=1,
        default_csi_section_id=2,
        notes="Concrete work (37K hrs, ProjectSight)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="LATCON CORP",
        aliases=[
            "latcon",
            "latcon/veltri",
        ],
    ),
    Company(
        company_id=18,
        canonical_name="Grout Tech",
        short_code="GROUT_TECH",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=1,
        default_csi_section_id=4,
        notes="Grouting, precast grout, concrete repair (118K hrs, ProjectSight)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="GROUT TECH INC",
        aliases=[
            "grout tech",
        ],
    ),

    # =========================================================================
    # YATES T1 SUBCONTRACTORS - EARTHWORK/CIVIL (trade=9)
    # =========================================================================
    Company(
        company_id=10,
        canonical_name="Rolling Plains",
        short_code="ROLLING_PLAINS",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=9,
        other_trade_ids="1,6",  # Also does CONCRETE and FIREPROOF
        default_csi_section_id=51,
        notes="Civil/concrete, fireproofing, site work (135K hrs, ProjectSight)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="ROLLING PLAINS CONSTRUCTION INC",
        aliases=[
            "rolling plains",
            "rolling planes",  # Typo
            "rolling plaines",  # Typo
        ],
    ),
    Company(
        company_id=12,
        canonical_name="FD Thomas",
        short_code="FD_THOMAS",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=9,
        other_trade_ids="1",  # Also does CONCRETE
        default_csi_section_id=51,
        notes="Civil work, excavation, backfill, concrete (221K hrs, ProjectSight)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="F D THOMAS INC",
        aliases=[
            "fd thomas",
            "f.d. thomas",
        ],
    ),
    Company(
        company_id=19,
        canonical_name="AH Beck Foundation",
        short_code="AH_BECK",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=9,
        default_csi_section_id=52,
        notes="Deep foundations, drilled piers (6K hrs, ProjectSight)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="A H BECK FOUNDATION CO INC",
        aliases=[
            "beck",
            "beck foundation",
            "beck foundation co",
            "ah beck",
        ],
    ),
    Company(
        company_id=58,
        canonical_name="Lehne",
        short_code="LEHNE",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=9,
        default_csi_section_id=51,
        notes="Earthwork (RABA quality only)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="Lehne",
        aliases=[
            "lehne",
            "lehen",  # Typo
            "lehne carlos",
        ],
    ),
    Company(
        company_id=59,
        canonical_name="ABAR",
        short_code="ABAR",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=9,
        default_csi_section_id=51,
        notes="Earthwork (RABA quality only)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="ABAR",
        aliases=[
            "abar",
            "arab",  # Typo
            "abra",  # Typo
        ],
    ),

    # =========================================================================
    # YATES T1 SUBCONTRACTORS - STEEL (trade=2)
    # =========================================================================
    Company(
        company_id=7,
        canonical_name="Patriot Erectors",
        short_code="PATRIOT",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=2,
        default_csi_section_id=6,
        notes="Steel erection, misc steel, stairs/railings (351K hrs, ProjectSight)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="PATRIOT ERECTORS LLC",
        aliases=[
            "patriot",
            "patriot/yates",
            # TBM variations
            "patriot erectors, llc",
        ],
    ),
    Company(
        company_id=47,
        canonical_name="SNS Erectors",
        short_code="SNS",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=2,
        default_csi_section_id=6,
        notes="Steel erection (83K hrs, ProjectSight)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="SNS Erectors, Inc",
        aliases=[
            "sns",
            "sns erectors",
        ],
    ),
    Company(
        company_id=11,
        canonical_name="W&W Steel",
        short_code="WW_STEEL",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=2,
        default_csi_section_id=6,
        notes="Structural steel erection, trusses, decking (515K hrs, ProjectSight)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="W & W STEEL LLC / W & W-AFCO STEEL, LLC",
        aliases=[
            "w&w steel",
            "w&w",
            "ww steel",
            # ProjectSight variations
            "w & w steel llc",
            "w & w-afco steel, llc",
        ],
    ),
    Company(
        company_id=57,
        canonical_name="Greenberry",
        short_code="GREENBERRY",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=2,
        default_csi_section_id=6,
        notes="Welding, structural steel (RABA quality only)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="Greenberry",
        aliases=[
            "greenberry",
            "gbi/greenberry",
        ],
    ),
    Company(
        company_id=24,
        canonical_name="Gateway Fabrication",
        short_code="GATEWAY",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=2,
        default_csi_section_id=9,
        notes="Miscellaneous steel fabrication",
        parent_company_id=2,
        parent_confidence="HIGH",
        aliases=[
            "gateway",
            "gateway fabrication",
        ],
    ),
    Company(
        company_id=25,
        canonical_name="Star Building Systems",
        short_code="STAR",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=2,
        default_csi_section_id=6,
        notes="Pre-engineered metal buildings",
        parent_company_id=2,
        parent_confidence="HIGH",
        aliases=[
            "star",
            "star building",
            "star building systems",
        ],
    ),
    Company(
        company_id=64,
        canonical_name="Veltri Steel",
        short_code="VELTRI",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=2,
        default_csi_section_id=6,
        notes="Steel fabrication (RABA quality)",
        parent_company_id=2,
        parent_confidence="HIGH",
        aliases=[
            "veltri steel",
            "veltri",
            "valtri",  # Typo
        ],
    ),
    Company(
        company_id=65,
        canonical_name="Trinity Steel",
        short_code="TRINITY",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=2,
        default_csi_section_id=6,
        notes="Steel fabrication",
        parent_company_id=2,
        parent_confidence="HIGH",
        aliases=[
            "trinity steel",
            "trinity steel fabricators",
        ],
    ),

    # =========================================================================
    # YATES T1 SUBCONTRACTORS - DRYWALL/FRAMING (trade=4)
    # =========================================================================
    Company(
        company_id=4,
        canonical_name="Berg",
        short_code="BERG",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=4,
        default_csi_section_id=26,
        notes="Metal stud framing, drywall, tape & finish (780K hrs, ProjectSight)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="BERG DRYWALL LLC / BERG GROUP LLC",
        aliases=[
            "berg",
            "berg steel",
            "berg drywall",
            "berg contractors",
            "berg group",
            # ProjectSight variations
            "berg drywall llc",
            "berg group llc",
            # Typos
            "ber g",
            "ber",
            "bergo",
            "bergs",
            "bergg",
            "berr",
            "berr; yates",
            "bergc",
            "berga",
            "berg / berg",
        ],
    ),
    Company(
        company_id=9,
        canonical_name="MK Marlow",
        short_code="MK_MARLOW",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=4,
        default_csi_section_id=26,
        notes="Drywall, framing (93K hrs, ProjectSight)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="MK MARLOW CO. VICTORIA LLC",
        aliases=[
            "mk marlow",
            "mkm",
            "m.k. marlow",
            "m k marlow",
            "marlow",
            "mk",
            "mkmarlow",
            "mk m",
            "mk marlow and berg",
            # ProjectSight variations
            "mk marlow co. victoria llc dba mk marlow company llc",
        ],
    ),
    Company(
        company_id=48,
        canonical_name="Baker Triangle",
        short_code="BAKER_TRI",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=4,
        default_csi_section_id=26,
        notes="Drywall (PSI quality: 203 inspections)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="Baker Triangle / Baker Drywall",
        aliases=[
            "baker triangle",
            "baker drywall",
            "baker t",
            "baker t / baker drywall",
            "bakertriangle",
            "baker t / baker drywall / bakertriangle",
            "baker / baker drywall",
            "baker triangle / baker drywall",
            "baker (baker drywall)",
            "baker drywall / bakertriangle",
            "baker drywall / brandon torres",
            "bker triangle / baker drywall",
            # Typos
            "backer",
            "backer t",
            "backer t / baker drywall",
            "backer (baker drywall)",
            "backer triangle (also listed as baker drywall)",
            "banker triangle",
            "bakery / baker drywall",
        ],
    ),
    Company(
        company_id=49,
        canonical_name="JP Hi-Tech",
        short_code="JPHITECH",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=4,
        default_csi_section_id=26,
        notes="Drywall engineering (PSI quality: 55 inspections)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="JP Hi-Tech",
        aliases=[
            "jp hi-tech",
            "jp hi-tech eng",
            "jp hitech",
            "jphi-tech eng inc.",
            "jphi-tech eng inc",
            "jphi-tech eng",
            "jp hi-tech eng",
            "hi-tech eng inc.",
            "hi-tech eng inc",
            "hitech jp",
            "jp hi teck",
            "jp",
            "jph",
            "jphi",
            "jhp",
            "jp (jp hi-tech eng)",
            "jp / jp hi-tech eng",
            "jphi (jp hi-tech eng)",
            '"jp"',
        ],
    ),
    Company(
        company_id=53,
        canonical_name="AMTS",
        short_code="AMTS",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=4,
        default_csi_section_id=26,
        notes="Drywall/framing (PSI quality: 410 inspections)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="AMTS Inc.",
        aliases=[
            "amts",
            "amts inc",
            # Typos
            "amys",
            "ams",
            "amt s",
            "amtis",
            "amst",
            "astm",
            "mats",
        ],
    ),
    Company(
        company_id=54,
        canonical_name="Axios",
        short_code="AXIOS",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=4,
        default_csi_section_id=26,
        notes="Drywall/framing (PSI: 439, RABA: 30 inspections)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="Axios Industrial Group",
        aliases=[
            "axios",
            "axios industrial",
            "axios industrial group",
            "axios-",
            # Typos
            "axious",
            "azios",
            "axos",
            "axiox",
        ],
    ),
    Company(
        company_id=27,
        canonical_name="Marek Brothers",
        short_code="MAREK",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=4,
        default_csi_section_id=26,
        notes="Drywall and acoustical contractor",
        parent_company_id=2,
        parent_confidence="HIGH",
        aliases=[
            "marek",
            "marek brothers",
        ],
    ),
    Company(
        company_id=63,
        canonical_name="Chaparral",
        short_code="CHAPARRAL",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=4,
        default_csi_section_id=26,
        notes="Drywall/framing (PSI quality only)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="Chaparral",
        aliases=[
            "chaparral",
            "chaparral (sam rodriguez)",
        ],
    ),
    Company(
        company_id=66,
        canonical_name="CSA Drywall",
        short_code="CSA",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=4,
        default_csi_section_id=26,
        notes="Drywall contractor",
        parent_company_id=2,
        parent_confidence="HIGH",
        aliases=[
            "csa drywall",
            "csa",
        ],
    ),

    # =========================================================================
    # YATES T1 SUBCONTRACTORS - PAINTING/COATINGS (trade=5)
    # =========================================================================
    Company(
        company_id=14,
        canonical_name="Apache",
        short_code="APACHE",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=5,
        other_trade_ids="6,8",  # Also FIREPROOF and INSULATION
        default_csi_section_id=29,
        notes="Industrial coatings, fireproofing, insulation, scaffolding (805K hrs, ProjectSight)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="APACHE INDUSTRIAL SERVICES INC",
        aliases=[
            "apache",
            "apache industrial",
        ],
    ),
    Company(
        company_id=6,
        canonical_name="Cherry Coatings",
        short_code="CHERRY",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=5,
        other_trade_ids="6",  # Also FIREPROOF (intumescent)
        default_csi_section_id=29,
        notes="Painting, intumescent fireproofing, waffle coating (267K hrs, ProjectSight)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="CHERRY PAINTING COMPANY INC DBA CHERRY COATINGS",
        aliases=[
            "cherry coatings",
            "cherry",
            "cherry / cherry coatings",
            "cherry/yates",
            # ProjectSight variations
            "cherry painting company llc dba cherry coatings",
            # TBM typos
            "cherry coating s",
        ],
    ),
    Company(
        company_id=13,
        canonical_name="Alpha Painting",
        short_code="ALPHA",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=5,
        default_csi_section_id=29,
        notes="Painting, decorating (203K hrs, ProjectSight)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="ALPHA PAINTING & DECORATING COMPANY INC",
        aliases=[
            "alpha",
            "alpha insulation",
            "alpha insulation and waterproofing",
            "alpha painting",
            "alpha/yates",
            "yates/alpha",
        ],
    ),

    # =========================================================================
    # YATES T1 SUBCONTRACTORS - INSULATION (trade=8)
    # =========================================================================
    Company(
        company_id=5,
        canonical_name="Brazos Urethane",
        short_code="BRAZOS",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=8,
        other_trade_ids="5",  # Also FINISHES (waffle coating)
        default_csi_section_id=13,
        notes="Urethane insulation, spray foam, waffle coating (203K hrs, ProjectSight)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="BRAZOS URETHANE INC",
        aliases=[
            "brazos",
            "brazos urethane",
        ],
    ),
    Company(
        company_id=28,
        canonical_name="Performance Contracting",
        short_code="PCI",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=8,
        other_trade_ids="6",  # Also FIREPROOF
        default_csi_section_id=13,
        notes="Insulation and fireproofing",
        parent_company_id=2,
        parent_confidence="HIGH",
        aliases=[
            "performance",
            "performance contracting",
            "pci",
        ],
    ),

    # =========================================================================
    # YATES T1 SUBCONTRACTORS - PANELS/ENCLOSURE (trade=11)
    # =========================================================================
    Company(
        company_id=8,
        canonical_name="Kovach",
        short_code="KOVACH",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=11,
        other_trade_ids="2",  # Also STEEL (support steel for IMP)
        default_csi_section_id=15,
        notes="Metal panel systems, IMP, steel support (115K hrs, ProjectSight)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="KOVACH ENCLOSURE SYSTEMS LLC",
        aliases=[
            "kovach",
            "kovach building enclosures",
            "kovach enclosure",
        ],
    ),

    # =========================================================================
    # YATES T1 SUBCONTRACTORS - FIRE PROTECTION (trade=6)
    # =========================================================================
    Company(
        company_id=55,
        canonical_name="North Star",
        short_code="NORTHSTAR",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=6,
        default_csi_section_id=19,
        notes="Firestopping (RABA quality: 24 inspections)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="North Star",
        aliases=[
            "north star",
            "northstar",
            "north star/axios",
        ],
    ),
    Company(
        company_id=56,
        canonical_name="JMEG",
        short_code="JMEG",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=6,
        default_csi_section_id=19,
        notes="Firestopping, fire protection (RABA: 14 inspections)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="JMEG",
        aliases=[
            "jmeg",
            "j-meg",
            "jmeg electrical",
            "secai - jmeg",
        ],
    ),

    # =========================================================================
    # YATES T1 SUBCONTRACTORS - MEP (trade=7)
    # =========================================================================
    Company(
        company_id=20,
        canonical_name="Cobb Mechanical",
        short_code="COBB",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=7,
        default_csi_section_id=40,
        notes="Mechanical contractor (8K hrs, ProjectSight)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="COBB MECHANICAL CONTRACTORS",
        aliases=[
            "cobb",
            "cobb mechanical",
        ],
    ),
    Company(
        company_id=67,
        canonical_name="McDean",
        short_code="MCDEAN",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=7,
        default_csi_section_id=40,
        notes="Electrical contractor",
        parent_company_id=2,
        parent_confidence="HIGH",
        aliases=[
            "mcdean",
            "mc dean",
            "dean-cec",
            "dean cec",  # TBM variation (space instead of hyphen)
        ],
    ),
    Company(
        company_id=68,
        canonical_name="Polk Mechanical",
        short_code="POLK",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=7,
        default_csi_section_id=40,
        notes="Mechanical contractor",
        parent_company_id=2,
        parent_confidence="HIGH",
        aliases=[
            "polk",
            "polk mechanical",
        ],
    ),

    # =========================================================================
    # YATES T1 SUBCONTRACTORS - DOORS/HARDWARE/SPECIALTIES (trade=5)
    # =========================================================================
    Company(
        company_id=50,
        canonical_name="Cook & Boardman",
        short_code="COOK",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=5,
        default_csi_section_id=21,
        notes="Doors and hardware (24K hrs, ProjectSight)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="COOK & BOARDMAN LLC",
        aliases=[
            "cook & boardman",
            "cook",
            # ProjectSight variations
            "cook & boardman llc dba cook & boardman_",
        ],
    ),
    Company(
        company_id=21,
        canonical_name="Perry & Perry",
        short_code="PERRY",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=5,
        default_csi_section_id=23,
        notes="Architectural specialties, door hardware (13K hrs, ProjectSight)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="PERRY & PERRY BUILDERS INC",
        aliases=[
            "perry",
            "perry & perry",
            "perry & perry specialties",
        ],
    ),
    Company(
        company_id=22,
        canonical_name="Alert Lock & Key",
        short_code="ALERT",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=5,
        default_csi_section_id=23,
        notes="Door hardware installation",
        parent_company_id=2,
        parent_confidence="HIGH",
        aliases=[
            "alert",
            "alert lock",
            "alert lock & key",
        ],
    ),

    # =========================================================================
    # YATES T1 SUBCONTRACTORS - FINISHES (trade=5)
    # =========================================================================
    Company(
        company_id=23,
        canonical_name="Spectra Contract Flooring",
        short_code="SPECTRA",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=5,
        default_csi_section_id=28,
        notes="Flooring contractor (CSI 09 65 00)",
        parent_company_id=2,
        parent_confidence="HIGH",
        aliases=[
            "spectra",
            "spectra contract flooring",
        ],
    ),
    Company(
        company_id=26,
        canonical_name="Texas Scenic",
        short_code="TEXAS_SCENIC",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=5,
        default_csi_section_id=29,
        notes="Specialty finishes",
        parent_company_id=2,
        parent_confidence="HIGH",
        aliases=[
            "texas scenic",
        ],
    ),

    # =========================================================================
    # YATES T1 SUBCONTRACTORS - MASONRY (trade=13)
    # =========================================================================
    Company(
        company_id=69,
        canonical_name="SBTA",
        short_code="SBTA",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=13,
        default_csi_section_id=5,
        notes="Masonry contractor",
        parent_company_id=2,
        parent_confidence="HIGH",
        aliases=[
            "sbta",
            "sbta, inc.",
            "sbta (masonry contractor)",
        ],
    ),

    # =========================================================================
    # YATES T1 SUBCONTRACTORS - STAFFING/GENERAL (trade=12)
    # =========================================================================
    Company(
        company_id=51,
        canonical_name="FinishLine Staffing",
        short_code="FINISHLINE",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=12,
        default_csi_section_id=1,
        notes="Labor staffing (28K hrs, ProjectSight)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="FINISH LINE STAFFING LLC",
        aliases=[
            "finishline",
            "finishline staffing",
            "finish line staffing",
            # ProjectSight variations
            "finishline staffing llc",
        ],
    ),
    Company(
        company_id=17,
        canonical_name="GDA Construction",
        short_code="GDA",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=12,
        default_csi_section_id=1,
        notes="General construction (176 hrs, ProjectSight)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="GDA CONTRACTORS",
        aliases=[
            "gda",
            "gda construction",
        ],
    ),
    Company(
        company_id=52,
        canonical_name="Preferred Dallas",
        short_code="PREFERRED",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=12,
        default_csi_section_id=1,
        notes="General contractor (9K hrs, ProjectSight)",
        parent_company_id=2,
        parent_confidence="HIGH",
        full_name="PREFERRED DALLAS, LLC",
        aliases=[
            "preferred",
            "preferred dallas",
        ],
    ),

    # =========================================================================
    # YATES T1 SUBCONTRACTORS - MISCELLANEOUS
    # =========================================================================
    Company(
        company_id=73,
        canonical_name="STI",
        short_code="STI",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=12,
        default_csi_section_id=1,
        notes="Subcontractor (alias only)",
        parent_company_id=2,
        parent_confidence="MEDIUM",
        aliases=["sti"],
    ),
    Company(
        company_id=74,
        canonical_name="BRYCON",
        short_code="BRYCON",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=12,
        default_csi_section_id=1,
        notes="Subcontractor (alias only)",
        parent_company_id=2,
        parent_confidence="MEDIUM",
        aliases=["brycon"],
    ),
    Company(
        company_id=75,
        canonical_name="Steely Farms",
        short_code="STEELY",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=12,
        default_csi_section_id=1,
        notes="Subcontractor (alias only)",
        parent_company_id=2,
        parent_confidence="MEDIUM",
        aliases=["steely farms"],
    ),
    Company(
        company_id=76,
        canonical_name="Jacobs",
        short_code="JACOBS",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=12,
        default_csi_section_id=1,
        notes="Subcontractor (alias only)",
        parent_company_id=2,
        parent_confidence="MEDIUM",
        aliases=["jacobs"],
    ),
    Company(
        company_id=77,
        canonical_name="BrandSafway",
        short_code="BRANDSAFWAY",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=12,
        default_csi_section_id=1,
        notes="Scaffolding contractor",
        parent_company_id=2,
        parent_confidence="HIGH",
        aliases=["brandsafway"],
    ),
    Company(
        company_id=78,
        canonical_name="Shinsung",
        short_code="SHINSUNG",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=12,
        default_csi_section_id=1,
        notes="Korean subcontractor",
        parent_company_id=2,
        parent_confidence="MEDIUM",
        aliases=["shinsung"],
    ),
    Company(
        company_id=79,
        canonical_name="DootaIT",
        short_code="DOOTAIT",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=12,
        default_csi_section_id=1,
        notes="Korean subcontractor",
        parent_company_id=2,
        parent_confidence="MEDIUM",
        aliases=["dootait"],
    ),
    Company(
        company_id=80,
        canonical_name="K-ENSOL",
        short_code="KENSOL",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=12,
        default_csi_section_id=1,
        notes="Korean subcontractor",
        parent_company_id=2,
        parent_confidence="MEDIUM",
        aliases=["k-ensol"],
    ),
    Company(
        company_id=81,
        canonical_name="Central Texas Industrial",
        short_code="CTI",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=12,
        default_csi_section_id=1,
        notes="Industrial contractor",
        parent_company_id=2,
        parent_confidence="MEDIUM",
        aliases=["central texas industrial"],
    ),
    Company(
        company_id=82,
        canonical_name="Smart IT",
        short_code="SMARTIT",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=12,
        default_csi_section_id=1,
        notes="IT contractor",
        parent_company_id=2,
        parent_confidence="MEDIUM",
        aliases=["smart it"],
    ),
    Company(
        company_id=83,
        canonical_name="Prime Controls",
        short_code="PRIME",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=7,
        default_csi_section_id=40,
        notes="Controls contractor",
        parent_company_id=2,
        parent_confidence="MEDIUM",
        aliases=["prime controls", "prime"],
    ),
    Company(
        company_id=84,
        canonical_name="DSI",
        short_code="DSI",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=12,
        default_csi_section_id=1,
        notes="Subcontractor (alias only)",
        parent_company_id=2,
        parent_confidence="MEDIUM",
        aliases=["dsi"],
    ),
    Company(
        company_id=85,
        canonical_name="Southern Industrial",
        short_code="SOUTHERN",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=12,
        default_csi_section_id=1,
        notes="Industrial contractor",
        parent_company_id=2,
        parent_confidence="MEDIUM",
        aliases=["southern industrial"],
    ),
    Company(
        company_id=86,
        canonical_name="Porter",
        short_code="PORTER",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=12,
        default_csi_section_id=1,
        notes="Subcontractor (alias only)",
        parent_company_id=2,
        parent_confidence="MEDIUM",
        aliases=[
            "porter",
            "the porter co.",
            "the porter co",
            "porter co",
        ],
    ),
    Company(
        company_id=87,
        canonical_name="B&K",
        short_code="BK",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=12,
        default_csi_section_id=1,
        notes="Subcontractor (alias only)",
        parent_company_id=2,
        parent_confidence="MEDIUM",
        aliases=["b&k"],
    ),
    Company(
        company_id=88,
        canonical_name="J-Kaulk",
        short_code="JKAULK",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=12,
        default_csi_section_id=1,
        notes="Caulking contractor",
        parent_company_id=2,
        parent_confidence="MEDIUM",
        aliases=["j-kaulk"],
    ),
    Company(
        company_id=89,
        canonical_name="Alliance",
        short_code="ALLIANCE",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=12,
        default_csi_section_id=1,
        notes="Subcontractor (alias only)",
        parent_company_id=2,
        parent_confidence="MEDIUM",
        aliases=["alliance"],
    ),
    Company(
        company_id=90,
        canonical_name="PSS",
        short_code="PSS",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=12,
        default_csi_section_id=1,
        notes="Subcontractor (alias only)",
        parent_company_id=2,
        parent_confidence="MEDIUM",
        aliases=["pss"],
    ),
    Company(
        company_id=91,
        canonical_name="MSR-FSR",
        short_code="MSRFSR",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=12,
        default_csi_section_id=1,
        notes="Subcontractor (alias only)",
        parent_company_id=2,
        parent_confidence="MEDIUM",
        aliases=["msr-fsr"],
    ),
    Company(
        company_id=92,
        canonical_name="Minyard Sons Services",
        short_code="MINYARD",
        tier="T1_SUB",
        company_type="yates_sub",
        is_yates_sub=True,
        primary_trade_id=12,
        default_csi_section_id=1,
        notes="Subcontractor (alias only)",
        parent_company_id=2,
        parent_confidence="MEDIUM",
        aliases=["minyard son's services", "minyard sons services"],
    ),

    # =========================================================================
    # PRECAST SUPPLIERS (material suppliers, not direct subs)
    # =========================================================================
    Company(
        company_id=60,
        canonical_name="Gate Precast",
        short_code="GATE",
        tier="T2_SUB",
        company_type="precast_supplier",
        is_yates_sub=False,
        primary_trade_id=10,
        default_csi_section_id=3,
        notes="Precast concrete supplier",
        full_name="Gate Precast",
        aliases=[
            "gate precast",
            "gate",
        ],
    ),
    Company(
        company_id=61,
        canonical_name="Coreslab",
        short_code="CORESLAB",
        tier="T2_SUB",
        company_type="precast_supplier",
        is_yates_sub=False,
        primary_trade_id=10,
        default_csi_section_id=3,
        notes="Precast concrete supplier",
        full_name="Coreslab",
        aliases=[
            "coreslab",
            "coreslab structures",
        ],
    ),
    Company(
        company_id=62,
        canonical_name="Heldenfels",
        short_code="HELDENFELS",
        tier="T2_SUB",
        company_type="precast_supplier",
        is_yates_sub=False,
        primary_trade_id=10,
        default_csi_section_id=3,
        notes="Precast concrete supplier",
        full_name="Heldenfels",
        aliases=[
            "heldenfels",
        ],
    ),

    # =========================================================================
    # TESTING/INSPECTION COMPANIES (not Yates subs)
    # =========================================================================
    Company(
        company_id=70,
        canonical_name="Intertek PSI",
        short_code="PSI",
        tier="OTHER",
        company_type="testing",
        is_yates_sub=False,
        primary_trade_id=12,
        default_csi_section_id=1,
        notes="Third-party quality inspections",
        full_name="Professional Services Industries, Inc.",
        aliases=[
            "intertek psi",
            "intertek",
            "professional services industries",
            "professional services industries, inc.",
            "professional services industries, inc. (intertek psi)",
            "professional services industries, inc. (psi) / intertek",
            "professional services industries, inc. (psi)",
        ],
    ),
    Company(
        company_id=71,
        canonical_name="Raba Kistner",
        short_code="RKCI",
        tier="OTHER",
        company_type="testing",
        is_yates_sub=False,
        primary_trade_id=12,
        default_csi_section_id=1,
        notes="Third-party quality testing",
        full_name="Raba Kistner, Inc. (RKCI)",
        aliases=[
            "raba kistner",
            "raba kistner, inc.",
            "raba kistner, inc",
            "raba kistner inc",
            "rkci",
            "raba",
            "raba kistner, inc. (a kiwa company)",
            "raba kistner, inc. (rkci)",
        ],
    ),
    Company(
        company_id=72,
        canonical_name="Alvarez Testing",
        short_code="ATI",
        tier="OTHER",
        company_type="testing",
        is_yates_sub=False,
        primary_trade_id=12,
        default_csi_section_id=1,
        notes="Third-party testing",
        full_name="Alvarez Testing & Inspections",
        aliases=[
            "alvarez testing",
            "alvarez testing & inspections",
            "alvarez testing and inspections",
            "alvarez testing & inspections - ati",
            "ati",
            "alvarez",
        ],
    ),
]


# =============================================================================
# BUILD FUNCTIONS
# =============================================================================

def build_dim_company() -> List[dict]:
    """Generate dim_company records from company definitions."""
    records = []

    for company in COMPANIES:
        records.append({
            "company_id": company.company_id,
            "canonical_name": company.canonical_name,
            "short_code": company.short_code,
            "tier": company.tier,
            "primary_trade_id": company.primary_trade_id,
            "other_trade_ids": company.other_trade_ids,
            "default_csi_section_id": company.default_csi_section_id,
            "notes": company.notes,
            "parent_company_id": company.parent_company_id,
            "parent_confidence": company.parent_confidence,
            "company_type": company.company_type,
            "is_yates_sub": company.is_yates_sub,
            "full_name": company.full_name,
        })

    return records


def build_map_company_aliases() -> List[dict]:
    """Generate map_company_aliases records from company definitions."""
    records = []
    seen_aliases = set()  # Track to avoid duplicates

    for company in COMPANIES:
        # Add canonical name as primary alias
        canonical_lower = company.canonical_name.lower()
        if canonical_lower not in seen_aliases:
            records.append({
                "company_id": company.company_id,
                "alias": canonical_lower,
                "source": "canonical",
            })
            seen_aliases.add(canonical_lower)

        # Add full_name as alias (important for ProjectSight matching)
        if company.full_name:
            full_lower = company.full_name.lower()
            if full_lower not in seen_aliases:
                records.append({
                    "company_id": company.company_id,
                    "alias": full_lower,
                    "source": "full_name",
                })
                seen_aliases.add(full_lower)

        # Add short_code as alias
        if company.short_code:
            short_lower = company.short_code.lower()
            if short_lower not in seen_aliases:
                records.append({
                    "company_id": company.company_id,
                    "alias": short_lower,
                    "source": "short_code",
                })
                seen_aliases.add(short_lower)

        # Add all defined aliases
        for alias in company.aliases:
            alias_lower = alias.lower()
            if alias_lower not in seen_aliases:
                records.append({
                    "company_id": company.company_id,
                    "alias": alias_lower,
                    "source": "manual",
                })
                seen_aliases.add(alias_lower)

    return records


def write_dim_company(records: List[dict]):
    """Write dim_company.csv."""
    DIM_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Column order matches existing schema
    fieldnames = [
        "company_id", "canonical_name", "short_code", "tier",
        "primary_trade_id", "other_trade_ids", "default_csi_section_id",
        "notes", "parent_company_id", "parent_confidence",
        "company_type", "is_yates_sub", "full_name"
    ]

    with open(DIM_OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    print(f"Generated {DIM_OUTPUT_FILE}")
    print(f"  Total companies: {len(records)}")


def write_map_company_aliases(records: List[dict]):
    """Write map_company_aliases.csv."""
    MAP_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    fieldnames = ["company_id", "alias", "source"]

    with open(MAP_OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    print(f"Generated {MAP_OUTPUT_FILE}")
    print(f"  Total aliases: {len(records)}")


def validate_no_duplicate_aliases() -> bool:
    """
    Post-generation validation: ensure no alias maps to multiple companies.

    This is a defensive check that catches duplicates from any source:
    - Bugs in generation logic
    - Manual CSV edits
    - External data merges

    Returns True if valid, False if duplicates found.
    """
    import pandas as pd

    if not MAP_OUTPUT_FILE.exists():
        print("  ⚠️  Cannot validate - map_company_aliases.csv not found")
        return True

    df = pd.read_csv(MAP_OUTPUT_FILE)

    # Find aliases that map to multiple companies
    alias_company_counts = df.groupby('alias')['company_id'].nunique()
    duplicates = alias_company_counts[alias_company_counts > 1]

    if len(duplicates) == 0:
        print("  ✅ No duplicate aliases found")
        return True

    print(f"\n  ❌ DUPLICATE ALIASES FOUND ({len(duplicates)}):")
    print("  " + "-" * 60)

    for alias in duplicates.index:
        rows = df[df['alias'] == alias]
        company_ids = rows['company_id'].tolist()
        sources = rows['source'].tolist()
        print(f"    '{alias}' maps to {len(company_ids)} companies:")
        for cid, src in zip(company_ids, sources):
            # Find company name
            company_name = "Unknown"
            for c in COMPANIES:
                if c.company_id == cid:
                    company_name = c.canonical_name
                    break
            print(f"      [{src}] company_id={cid} ({company_name})")

    print("\n  To fix: Update COMPANIES list in build_company_dimension.py")
    print("  Either remove the duplicate alias or assign it to the correct company.")

    return False


def print_summary(dim_records: List[dict], alias_records: List[dict]):
    """Print summary statistics."""
    print("\n" + "=" * 60)
    print("COMPANY DIMENSION SUMMARY")
    print("=" * 60)

    # Count by tier
    print("\nBy Tier:")
    tier_counts = {}
    for c in dim_records:
        tier = c["tier"]
        tier_counts[tier] = tier_counts.get(tier, 0) + 1
    for tier, count in sorted(tier_counts.items()):
        print(f"  {tier:15}: {count}")

    # Count by company_type
    print("\nBy Type:")
    type_counts = {}
    for c in dim_records:
        ctype = c["company_type"]
        type_counts[ctype] = type_counts.get(ctype, 0) + 1
    for ctype, count in sorted(type_counts.items()):
        print(f"  {ctype:20}: {count}")

    # Yates subs count
    yates_subs = sum(1 for c in dim_records if c["is_yates_sub"])
    print(f"\nYates Subs (is_yates_sub=True): {yates_subs}")
    print(f"Non-Yates: {len(dim_records) - yates_subs}")

    # Multi-trade companies
    multi_trade = [c for c in dim_records if c["other_trade_ids"]]
    if multi_trade:
        print(f"\nMulti-Trade Companies ({len(multi_trade)}):")
        for c in multi_trade:
            print(f"  {c['canonical_name']:30} primary={c['primary_trade_id'] or 'N/A':>2}  other={c['other_trade_ids']}")

    # Alias statistics
    print(f"\nAlias Coverage:")
    print(f"  Total aliases: {len(alias_records)}")
    aliases_per_company = len(alias_records) / len(dim_records) if dim_records else 0
    print(f"  Avg per company: {aliases_per_company:.1f}")


def verify_consistency():
    """Verify internal consistency of company definitions."""
    print("\n" + "=" * 60)
    print("CONSISTENCY VERIFICATION")
    print("=" * 60)

    errors = []
    warnings = []

    # Check for duplicate company_ids
    ids = [c.company_id for c in COMPANIES]
    if len(ids) != len(set(ids)):
        duplicates = [id for id in ids if ids.count(id) > 1]
        errors.append(f"Duplicate company_ids: {set(duplicates)}")

    # Check for duplicate aliases
    all_aliases = []
    for company in COMPANIES:
        all_aliases.extend([(a.lower(), company.company_id) for a in company.aliases])
        all_aliases.append((company.canonical_name.lower(), company.company_id))

    alias_to_company = {}
    for alias, company_id in all_aliases:
        if alias in alias_to_company and alias_to_company[alias] != company_id:
            errors.append(f"Duplicate alias '{alias}' maps to companies {alias_to_company[alias]} and {company_id}")
        alias_to_company[alias] = company_id

    # Check parent_company_id references
    valid_ids = set(ids)
    for company in COMPANIES:
        if company.parent_company_id is not None and company.parent_company_id not in valid_ids:
            errors.append(f"Company {company.company_id} has invalid parent_company_id: {company.parent_company_id}")

    # Check for companies with no aliases
    for company in COMPANIES:
        if not company.aliases and company.company_id != 0:  # Skip Unknown placeholder
            warnings.append(f"Company {company.company_id} ({company.canonical_name}) has no aliases")

    if errors:
        print("\nERRORS:")
        for e in errors:
            print(f"  ❌ {e}")
    else:
        print("\n✅ No errors found")

    if warnings:
        print(f"\nWARNINGS ({len(warnings)}):")
        for w in warnings[:10]:
            print(f"  ⚠️  {w}")
        if len(warnings) > 10:
            print(f"  ... and {len(warnings) - 10} more")

    return len(errors) == 0


def scan_data_sources():
    """Scan data sources for unmapped company names."""
    import pandas as pd
    from collections import defaultdict

    print("\n" + "=" * 60)
    print("SCANNING DATA SOURCES FOR UNMAPPED COMPANIES")
    print("=" * 60)

    # Build alias lookup (must match build_map_company_aliases logic)
    alias_lookup = {}
    for company in COMPANIES:
        # Canonical name
        alias_lookup[company.canonical_name.lower()] = company.company_id
        # Full name
        if company.full_name:
            alias_lookup[company.full_name.lower()] = company.company_id
        # Short code
        if company.short_code:
            alias_lookup[company.short_code.lower()] = company.company_id
        # All defined aliases
        for alias in company.aliases:
            alias_lookup[alias.lower()] = company.company_id

    def lookup_company(name: str) -> Optional[int]:
        if not name or pd.isna(name):
            return None
        return alias_lookup.get(str(name).lower().strip())

    sources = {}
    unmapped_all = defaultdict(lambda: {"count": 0, "sources": []})

    # 1. ProjectSight Labor
    ps_file = settings.PROCESSED_DATA_DIR / 'projectsight' / 'labor_entries.csv'
    if ps_file.exists():
        df = pd.read_csv(ps_file, usecols=['company'])
        companies = df['company'].dropna().value_counts()
        unmapped = [(name, count) for name, count in companies.items() if lookup_company(name) is None]
        sources['projectsight_labor'] = {
            'total': len(companies),
            'unmapped': len(unmapped),
            'unmapped_records': sum(c for _, c in unmapped),
        }
        for name, count in unmapped:
            unmapped_all[name]["count"] += count
            unmapped_all[name]["sources"].append("projectsight")

    # 2. RABA Quality
    raba_file = settings.PROCESSED_DATA_DIR / 'raba' / 'raba_consolidated.csv'
    if raba_file.exists():
        df = pd.read_csv(raba_file)
        raba_companies = defaultdict(int)
        for col in ['contractor_raw', 'contractor', 'subcontractor_raw', 'subcontractor']:
            if col in df.columns:
                for name, count in df[col].dropna().value_counts().items():
                    raba_companies[name] += count
        unmapped = [(name, count) for name, count in raba_companies.items() if lookup_company(name) is None]
        sources['raba'] = {
            'total': len(raba_companies),
            'unmapped': len(unmapped),
            'unmapped_records': sum(c for _, c in unmapped),
        }
        for name, count in unmapped:
            unmapped_all[name]["count"] += count
            unmapped_all[name]["sources"].append("raba")

    # 3. PSI Quality
    psi_file = settings.PROCESSED_DATA_DIR / 'psi' / 'psi_consolidated.csv'
    if psi_file.exists():
        df = pd.read_csv(psi_file)
        psi_companies = defaultdict(int)
        for col in ['contractor_raw', 'contractor', 'subcontractor_raw', 'subcontractor']:
            if col in df.columns:
                for name, count in df[col].dropna().value_counts().items():
                    psi_companies[name] += count
        unmapped = [(name, count) for name, count in psi_companies.items() if lookup_company(name) is None]
        sources['psi'] = {
            'total': len(psi_companies),
            'unmapped': len(unmapped),
            'unmapped_records': sum(c for _, c in unmapped),
        }
        for name, count in unmapped:
            unmapped_all[name]["count"] += count
            unmapped_all[name]["sources"].append("psi")

    # 4. TBM Daily Plans
    tbm_file = settings.PROCESSED_DATA_DIR / 'tbm' / 'work_entries.csv'
    if tbm_file.exists():
        df = pd.read_csv(tbm_file)
        tbm_companies = defaultdict(int)
        for col in ['tier2_sc', 'tier1_gc', 'subcontractor_file']:
            if col in df.columns:
                for name, count in df[col].dropna().value_counts().items():
                    tbm_companies[name] += count
        unmapped = [(name, count) for name, count in tbm_companies.items() if lookup_company(name) is None]
        sources['tbm'] = {
            'total': len(tbm_companies),
            'unmapped': len(unmapped),
            'unmapped_records': sum(c for _, c in unmapped),
        }
        for name, count in unmapped:
            unmapped_all[name]["count"] += count
            unmapped_all[name]["sources"].append("tbm")

    # Print source summary
    print("\nSource Coverage:")
    for source, stats in sources.items():
        print(f"\n  {source.upper()}:")
        print(f"    Unique names: {stats['total']}")
        print(f"    Unmapped: {stats['unmapped']} names, {stats['unmapped_records']} records")

    # Print top unmapped
    if unmapped_all:
        print(f"\n\nTOP UNMAPPED COMPANY NAMES ({len(unmapped_all)} total):")
        print("-" * 80)
        print(f"{'Company Name':<45} {'Records':>10} Sources")
        print("-" * 80)

        sorted_unmapped = sorted(unmapped_all.items(), key=lambda x: -x[1]["count"])
        for name, info in sorted_unmapped[:30]:
            sources_str = ", ".join(info["sources"][:3])
            print(f"{name:<45} {info['count']:>10,} {sources_str}")

        if len(sorted_unmapped) > 30:
            print(f"... and {len(sorted_unmapped) - 30} more")

    return len(unmapped_all) == 0


def main():
    parser = argparse.ArgumentParser(description="Build company dimension tables")
    parser.add_argument("--verify", action="store_true", help="Verify consistency only (no output)")
    parser.add_argument("--scan-sources", action="store_true", help="Scan data sources for unmapped companies")
    args = parser.parse_args()

    print("=" * 60)
    print("BUILD COMPANY DIMENSION TABLES")
    print("=" * 60)

    # Always verify consistency
    if not verify_consistency():
        print("\n❌ Consistency errors found. Fix before generating output.")
        return 1

    if args.verify:
        print("\n✅ Verification complete.")
        return 0

    if args.scan_sources:
        scan_data_sources()
        return 0

    # Build and write outputs
    dim_records = build_dim_company()
    alias_records = build_map_company_aliases()

    write_dim_company(dim_records)
    write_map_company_aliases(alias_records)

    # Post-generation validation: fail if duplicates exist
    print("\nValidating output...")
    if not validate_no_duplicate_aliases():
        print("\n❌ Build failed: duplicate aliases detected.")
        print("The standalone fix script (fix_alias_duplicates.py) is DEPRECATED.")
        print("Fix duplicates in the COMPANIES list above instead.")
        return 1

    print_summary(dim_records, alias_records)

    print("\n✅ Build complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
