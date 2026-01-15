#!/usr/bin/env python3
"""
Build dim_company.csv - Company dimension table.

This script generates a dimension table with all companies/contractors identified
across the project data sources, classified by their relationship to Yates.

Columns:
- company_id: Unique identifier
- canonical_name: Standardized company name
- short_code: Abbreviated code (for P6, displays)
- tier: Company tier (OWNER, GC, T1_SUB, T2_SUB, OTHER)
- company_type: Classification (yates_self, yates_sub, major_contractor, precast_supplier)
- is_yates_sub: Boolean flag for Yates subcontractors
- primary_trade_id: FK to dim_trade for main work type
- default_csi_section_id: FK to dim_csi_section for default CSI classification
- notes: Additional information
- parent_company_id: FK to parent company (GC for subs)
- parent_confidence: Confidence of parent relationship (HIGH, MEDIUM, LOW)
- full_name: Full legal name where known

Data Sources for Classification:
- ProjectSight labor entries: All companies are confirmed Yates subs
- RABA/PSI quality records: Cross-referenced with ProjectSight to identify additional subs
- company_standardization.py: Alias resolution and standardization

Context Documentation:
- See scripts/integrated_analysis/context/yates_subcontractors.md for detailed scope info

Usage:
    python -m scripts.integrated_analysis.dimensions.build_dim_company

Output:
    {WINDOWS_DATA_DIR}/processed/integrated_analysis/dimensions/dim_company.csv
"""

import csv
from pathlib import Path
from src.config.settings import settings

# Output location
OUTPUT_DIR = settings.PROCESSED_DATA_DIR / "integrated_analysis" / "dimensions"
OUTPUT_FILE = OUTPUT_DIR / "dim_company.csv"

# ============================================================================
# COMPANY DEFINITIONS
# ============================================================================
# Format: (company_id, canonical_name, short_code, tier, company_type, is_yates_sub,
#          primary_trade_id, default_csi_section_id, notes, parent_company_id,
#          parent_confidence, full_name)
#
# Trade IDs (from dim_trade):
#   1=CONCRETE, 2=STEEL, 3=ROOFING, 4=DRYWALL, 5=FINISHES, 6=FIREPROOF,
#   7=MEP, 8=INSULATION, 9=EARTHWORK, 10=PRECAST, 11=PANELS, 12=GENERAL, 13=MASONRY
#
# CSI Section IDs (from dim_csi_section):
#   1=01 10 00 Summary, 2=03 30 00 Cast-in-Place, 3=03 41 00 Precast,
#   4=03 60 00 Grouting, 5=04 20 00 Masonry, 6=05 12 00 Steel Framing,
#   7=05 31 00 Steel Decking, 8=05 40 00 Cold-Formed Framing, 9=05 50 00 Metal Fab,
#   15=07 42 43 IMP, 18=07 81 00 Fireproofing, 19=07 84 00 Firestopping,
#   13=07 21 16 Blanket Insulation, 21=08 11 13 Hollow Metal Doors,
#   23=08 71 00 Door Hardware, 26=09 21 16 Gypsum Board, 29=09 91 26 Painting,
#   40=23 05 00 HVAC, 51=31 23 00 Excavation, 52=31 63 00 Bored Piles

COMPANIES = [
    # =========================================================================
    # SPECIAL ENTRIES
    # =========================================================================
    (0, "Unknown/Activity Code", "UNKNOWN", "OTHER", "other", False, None, None,
     "Placeholder for unmapped or activity description codes", None, None, None),

    # =========================================================================
    # OWNER
    # =========================================================================
    (1, "Samsung Electronics Co America", "SECAI", "OWNER", "major_contractor", False, 12, 1,
     "Project owner", None, None, "Samsung E&C America, Inc. (SECAI)"),

    # =========================================================================
    # GENERAL CONTRACTORS
    # =========================================================================
    (2, "W.G. Yates & Sons Construction", "YATES", "GC", "yates_self", True, 12, 1,
     "General contractor", None, None, "W. G. YATES & SONS CONSTRUCTION COMPANY"),
    (34, "Hensel Phelps Construction", "HP", "GC", "major_contractor", False, 12, 1,
     "General contractor", None, None, "Hensel Phelps Construction Co."),
    (44, "PCL Construction", "PCL", "GC", "major_contractor", False, 12, 1,
     "General contractor", None, None, "PCL Construction"),
    (45, "McCarthy Building Companies", "MCCARTHY", "GC", "major_contractor", False, 12, 1,
     "Cleanroom contractor", None, None, "McCarthy Building Companies"),
    (46, "Austin Bridge & Road", "ABR", "GC", "major_contractor", False, 9, 51,
     "Sitework contractor", None, None, "Austin Bridge & Road / Austin Global Construction"),

    # =========================================================================
    # YATES T1 SUBCONTRACTORS - CONFIRMED FROM PROJECTSIGHT LABOR
    # =========================================================================
    # Concrete (trade=1, CSI=2 for cast-in-place, 4 for grouting)
    (3, "Baker Concrete Construction", "BAKER", "T1_SUB", "yates_sub", True, 1, 2,
     "Concrete - topping slabs, SOMD, elevated slabs (824K hrs)", 2, "HIGH", "BAKER CONCRETE CONSTRUCTION INC"),
    (15, "Infinity Concrete", "INFINITY", "T1_SUB", "yates_sub", True, 1, 2,
     "Concrete placement, slabs (412K hrs)", 2, "HIGH", "INFINITY CONCRETE CONSTRUCTION LLC"),
    (16, "LATCON", "LATCON", "T1_SUB", "yates_sub", True, 1, 2,
     "Concrete work (37K hrs)", 2, "HIGH", "LATCON CORP"),
    (18, "Grout Tech", "GROUT_TECH", "T1_SUB", "yates_sub", True, 1, 4,
     "Grouting, precast grout, concrete repair (118K hrs)", 2, "HIGH", "GROUT TECH INC"),

    # Earthwork/Civil (trade=9, CSI=51 for excavation, 52 for piles)
    (10, "Rolling Plains Construction", "ROLLING_PLAINS", "T1_SUB", "yates_sub", True, 9, 51,
     "Civil/concrete, site work (135K hrs)", 2, "HIGH", "ROLLING PLAINS CONSTRUCTION INC"),
    (12, "FD Thomas", "FD_THOMAS", "T1_SUB", "yates_sub", True, 9, 51,
     "Civil work, excavation, backfill (221K hrs)", 2, "HIGH", "F D THOMAS INC"),
    (19, "AH Beck Foundation", "AH_BECK", "T1_SUB", "yates_sub", True, 9, 52,
     "Deep foundations, drilled piers (6K hrs)", 2, "HIGH", "A H BECK FOUNDATION CO INC"),
    (58, "Lehne", "LEHNE", "T1_SUB", "yates_sub", True, 9, 51,
     "Earthwork (RABA quality only)", 2, "HIGH", "Lehne"),
    (59, "ABAR", "ABAR", "T1_SUB", "yates_sub", True, 9, 51,
     "Earthwork (RABA quality only)", 2, "HIGH", "ABAR"),

    # Steel (trade=2, CSI=6 for framing, 7 for decking)
    (7, "Patriot Erectors", "PATRIOT", "T1_SUB", "yates_sub", True, 2, 6,
     "Steel erection, truss installation (351K hrs)", 2, "HIGH", "PATRIOT ERECTORS LLC"),
    (47, "SNS Erectors", "SNS", "T1_SUB", "yates_sub", True, 2, 6,
     "Steel erection (83K hrs)", 2, "HIGH", "SNS Erectors, Inc"),
    (11, "W&W Steel", "WW_STEEL", "T1_SUB", "yates_sub", True, 2, 6,
     "Structural steel erection, trusses, decking (515K hrs)", 2, "HIGH", "W & W STEEL LLC / W & W-AFCO STEEL, LLC"),
    (57, "Greenberry Industrial", "GREENBERRY", "T1_SUB", "yates_sub", True, 2, 6,
     "Welding, structural steel (RABA quality only)", 2, "HIGH", "Greenberry"),
    (24, "Gateway Fabrication", "GATEWAY", "T1_SUB", "yates_sub", True, 2, 9,
     "Miscellaneous steel fabrication", 2, "HIGH", None),
    (25, "Star Building Systems", "STAR", "T1_SUB", "yates_sub", True, 2, 6,
     "Pre-engineered metal buildings", 2, "HIGH", None),

    # Drywall/Framing (trade=4, CSI=26 for gypsum board, 8 for metal framing)
    (4, "Berg Drywall", "BERG", "T1_SUB", "yates_sub", True, 4, 26,
     "Metal stud framing, drywall, tape & finish (780K hrs)", 2, "HIGH", "BERG DRYWALL LLC / BERG GROUP LLC"),
    (9, "MK Marlow", "MK_MARLOW", "T1_SUB", "yates_sub", True, 4, 26,
     "Drywall, framing (93K hrs)", 2, "HIGH", "MK MARLOW CO. VICTORIA LLC"),
    (48, "Baker Triangle", "BAKER_TRI", "T1_SUB", "yates_sub", True, 4, 26,
     "Drywall (PSI quality: 203 inspections)", 2, "HIGH", "Baker Triangle / Baker Drywall"),
    (49, "JP Hi-Tech", "JPHITECH", "T1_SUB", "yates_sub", True, 4, 26,
     "Drywall engineering (PSI quality: 55 inspections)", 2, "HIGH", "JP Hi-Tech"),
    (53, "AMTS", "AMTS", "T1_SUB", "yates_sub", True, 4, 26,
     "Drywall/framing (PSI quality: 410 inspections)", 2, "HIGH", "AMTS Inc."),
    (54, "Axios Industrial", "AXIOS", "T1_SUB", "yates_sub", True, 4, 26,
     "Drywall/framing (PSI: 439, RABA: 30 inspections)", 2, "HIGH", "Axios Industrial Group"),
    (27, "Marek Brothers", "MAREK", "T1_SUB", "yates_sub", True, 4, 26,
     "Drywall and acoustical contractor", 2, "HIGH", None),
    (63, "Chaparral", "CHAPARRAL", "T1_SUB", "yates_sub", True, 4, 26,
     "Drywall/framing (PSI quality only)", 2, "HIGH", "Chaparral"),

    # Painting/Coatings (trade=5, CSI=29 for painting)
    (14, "Apache Industrial Services", "APACHE", "T1_SUB", "yates_sub", True, 5, 29,
     "Industrial coatings, surface prep (805K hrs)", 2, "HIGH", "APACHE INDUSTRIAL SERVICES INC"),
    (6, "Cherry Coatings", "CHERRY", "T1_SUB", "yates_sub", True, 5, 29,
     "Painting, EUV waffle coating (267K hrs)", 2, "HIGH", "CHERRY PAINTING COMPANY INC DBA CHERRY COATINGS"),
    (13, "Alpha Painting", "ALPHA", "T1_SUB", "yates_sub", True, 5, 29,
     "Painting, decorating (203K hrs)", 2, "HIGH", "ALPHA PAINTING & DECORATING COMPANY INC"),

    # Insulation (trade=8, CSI=13 for blanket insulation)
    (5, "Brazos Urethane", "BRAZOS", "T1_SUB", "yates_sub", True, 8, 13,
     "Urethane insulation, spray foam, waffle coating (203K hrs)", 2, "HIGH", "BRAZOS URETHANE INC"),
    (28, "Performance Contracting", "PCI", "T1_SUB", "yates_sub", True, 8, 13,
     "Insulation and fireproofing", 2, "HIGH", None),

    # Panels/Enclosure (trade=11, CSI=15 for IMP)
    (8, "Kovach Building Enclosures", "KOVACH", "T1_SUB", "yates_sub", True, 11, 15,
     "Metal panel systems, IMP, building enclosure (115K hrs)", 2, "HIGH", "KOVACH ENCLOSURE SYSTEMS LLC"),

    # Fire Protection (trade=6, CSI=18 for fireproofing, 19 for firestop)
    (55, "North Star", "NORTHSTAR", "T1_SUB", "yates_sub", True, 6, 19,
     "Firestopping (RABA quality: 24 inspections)", 2, "HIGH", "North Star"),
    (56, "JMEG", "JMEG", "T1_SUB", "yates_sub", True, 6, 19,
     "Firestopping, fire protection (RABA: 14 inspections)", 2, "HIGH", "JMEG"),

    # MEP (trade=7, CSI=40 for HVAC)
    (20, "Cobb Mechanical", "COBB", "T1_SUB", "yates_sub", True, 7, 40,
     "Mechanical contractor (8K hrs)", 2, "HIGH", "COBB MECHANICAL CONTRACTORS"),

    # Doors/Hardware/Specialties (trade=5, CSI=21/23 for doors/hardware)
    (50, "Cook & Boardman", "COOK", "T1_SUB", "yates_sub", True, 5, 21,
     "Doors and hardware (24K hrs)", 2, "HIGH", "COOK & BOARDMAN LLC"),
    (21, "Perry & Perry Specialties", "PERRY", "T1_SUB", "yates_sub", True, 5, 23,
     "Architectural specialties, door hardware (13K hrs)", 2, "HIGH", "PERRY & PERRY BUILDERS INC"),
    (22, "Alert Lock & Key", "ALERT", "T1_SUB", "yates_sub", True, 5, 23,
     "Door hardware installation", 2, "HIGH", None),

    # Finishes (trade=5)
    (23, "Spectra Contract Flooring", "SPECTRA", "T1_SUB", "yates_sub", True, 5, 28,
     "Flooring contractor (CSI 09 65 00)", 2, "HIGH", None),
    (26, "Texas Scenic", "TEXAS_SCENIC", "T1_SUB", "yates_sub", True, 5, 29,
     "Specialty finishes", 2, "HIGH", None),

    # Staffing/General (trade=12, CSI=1 Summary)
    (51, "FinishLine Staffing", "FINISHLINE", "T1_SUB", "yates_sub", True, 12, 1,
     "Labor staffing (28K hrs)", 2, "HIGH", "FINISH LINE STAFFING LLC"),
    (17, "GDA Construction", "GDA", "T1_SUB", "yates_sub", True, 12, 1,
     "General construction (176 hrs)", 2, "HIGH", "GDA CONTRACTORS"),
    (52, "Preferred Dallas", "PREFERRED", "T1_SUB", "yates_sub", True, 12, 1,
     "General contractor (9K hrs)", 2, "HIGH", "PREFERRED DALLAS, LLC"),

    # =========================================================================
    # PRECAST SUPPLIERS (material suppliers, not direct subs)
    # =========================================================================
    (60, "Gate Precast", "GATE", "T2_SUB", "precast_supplier", False, 10, 3,
     "Precast concrete supplier", None, None, "Gate Precast"),
    (61, "Coreslab", "CORESLAB", "T2_SUB", "precast_supplier", False, 10, 3,
     "Precast concrete supplier", None, None, "Coreslab"),
    (62, "Heldenfels", "HELDENFELS", "T2_SUB", "precast_supplier", False, 10, 3,
     "Precast concrete supplier", None, None, "Heldenfels"),

    # =========================================================================
    # TESTING/INSPECTION COMPANIES (not Yates subs)
    # =========================================================================
    (70, "Intertek PSI", "PSI", "OTHER", "testing", False, 12, 1,
     "Third-party quality inspections", None, None, "Professional Services Industries, Inc."),
    (71, "Raba Kistner", "RKCI", "OTHER", "testing", False, 12, 1,
     "Third-party quality testing", None, None, "Raba Kistner, Inc. (RKCI)"),
    (72, "Alvarez Testing", "ATI", "OTHER", "testing", False, 12, 1,
     "Third-party testing", None, None, "Alvarez Testing & Inspections"),
]


def build_dim_company():
    """Generate dim_company.csv from company definitions."""

    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Build company records
    companies = []
    for row in COMPANIES:
        (company_id, canonical_name, short_code, tier, company_type, is_yates_sub,
         primary_trade_id, default_csi_section_id, notes, parent_company_id,
         parent_confidence, full_name) = row
        companies.append({
            "company_id": company_id,
            "canonical_name": canonical_name,
            "short_code": short_code,
            "tier": tier,
            "company_type": company_type,
            "is_yates_sub": is_yates_sub,
            "primary_trade_id": primary_trade_id,
            "default_csi_section_id": default_csi_section_id,
            "notes": notes,
            "parent_company_id": parent_company_id,
            "parent_confidence": parent_confidence,
            "full_name": full_name,
        })

    # Write CSV - column order for Power BI compatibility
    fieldnames = ["company_id", "canonical_name", "short_code", "tier",
                  "primary_trade_id", "default_csi_section_id", "notes",
                  "parent_company_id", "parent_confidence",
                  "company_type", "is_yates_sub", "full_name"]

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(companies)

    print(f"Generated {OUTPUT_FILE}")
    print(f"  Total companies: {len(companies)}")

    # Print summary by tier
    print("\nCompany Summary by Tier:")
    tier_counts = {}
    for c in companies:
        t = c["tier"]
        tier_counts[t] = tier_counts.get(t, 0) + 1
    for t, count in sorted(tier_counts.items()):
        print(f"  {t:15}: {count}")

    # Print summary by company_type
    print("\nCompany Summary by Type:")
    type_counts = {}
    for c in companies:
        t = c["company_type"]
        type_counts[t] = type_counts.get(t, 0) + 1
    for t, count in sorted(type_counts.items()):
        print(f"  {t:20}: {count}")

    # Print Yates subs count
    yates_subs = sum(1 for c in companies if c["is_yates_sub"])
    print(f"\nYates Subs (is_yates_sub=True): {yates_subs}")
    print(f"Non-Yates: {len(companies) - yates_subs}")

    return companies


if __name__ == "__main__":
    build_dim_company()
