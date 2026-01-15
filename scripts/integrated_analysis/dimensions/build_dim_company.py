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
- notes: Additional information
- parent_company_id: FK to parent company (GC for subs)
- parent_confidence: Confidence of parent relationship (HIGH, MEDIUM, LOW)
- full_name: Full legal name where known

Data Sources for Classification:
- ProjectSight labor entries: All 30 companies are confirmed Yates subs
- RABA/PSI quality records: Cross-referenced with ProjectSight to identify additional subs
- company_standardization.py: Alias resolution and standardization

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
#          primary_trade_id, notes, parent_company_id, parent_confidence, full_name)

COMPANIES = [
    # =========================================================================
    # SPECIAL ENTRIES
    # =========================================================================
    (0, "Unknown/Activity Code", "UNKNOWN", "OTHER", "other", False, None,
     "Placeholder for unmapped or activity description codes", None, None, None),

    # =========================================================================
    # OWNER
    # =========================================================================
    (1, "Samsung Electronics Co America", "SECAI", "OWNER", "major_contractor", False, 12,
     "Project owner", None, None, "Samsung E&C America, Inc. (SECAI)"),

    # =========================================================================
    # GENERAL CONTRACTORS
    # =========================================================================
    (2, "W.G. Yates & Sons Construction", "YATES", "GC", "yates_self", True, 12,
     "General contractor", None, None, "W. G. YATES & SONS CONSTRUCTION COMPANY"),
    (34, "Hensel Phelps Construction", "HP", "GC", "major_contractor", False, 12,
     "General contractor", None, None, "Hensel Phelps Construction Co."),
    (44, "PCL Construction", "PCL", "GC", "major_contractor", False, 12,
     "General contractor", None, None, "PCL Construction"),
    (45, "McCarthy Building Companies", "MCCARTHY", "GC", "major_contractor", False, 12,
     "Cleanroom contractor", None, None, "McCarthy Building Companies"),
    (46, "Austin Bridge & Road", "ABR", "GC", "major_contractor", False, 9,
     "Sitework contractor", None, None, "Austin Bridge & Road / Austin Global Construction"),

    # =========================================================================
    # YATES T1 SUBCONTRACTORS - CONFIRMED FROM PROJECTSIGHT LABOR
    # =========================================================================
    # Concrete
    (3, "Baker Concrete Construction", "BAKER", "T1_SUB", "yates_sub", True, 1,
     "Concrete contractor - topping slabs SOMD elevated slabs", 2, "HIGH", "BAKER CONCRETE CONSTRUCTION INC"),
    (15, "Infinity Mechanical Insulation", "INFINITY", "T1_SUB", "yates_sub", True, 1,
     "Concrete contractor", 2, "HIGH", "INFINITY CONCRETE CONSTRUCTION LLC"),
    (16, "LATCON", "LATCON", "T1_SUB", "yates_sub", True, 1,
     "Concrete contractor", 2, "HIGH", "LATCON CORP"),
    (10, "Rolling Plains Construction", "ROLLING_PLAINS", "T1_SUB", "yates_sub", True, 9,
     "Civil/concrete contractor", 2, "HIGH", "ROLLING PLAINS CONSTRUCTION INC"),
    (12, "FD Thomas", "FD_THOMAS", "T1_SUB", "yates_sub", True, 9,
     "Civil/concrete contractor", 2, "HIGH", "F D THOMAS INC"),
    (18, "Grout Tech", "GROUT_TECH", "T1_SUB", "yates_sub", True, 1,
     "Grouting and concrete repair", 2, "HIGH", "GROUT TECH INC"),
    (19, "AH Beck Foundation", "AH_BECK", "T1_SUB", "yates_sub", True, 9,
     "Deep foundations and earthwork", 2, "HIGH", "A H BECK FOUNDATION CO INC"),

    # Steel
    (7, "Patriot Erectors", "PATRIOT", "T1_SUB", "yates_sub", True, 2,
     "Steel erection contractor", 2, "HIGH", "PATRIOT ERECTORS LLC"),
    (47, "SNS Erectors", "SNS", "T1_SUB", "yates_sub", True, 2,
     "Steel erection contractor", 2, "HIGH", "SNS Erectors, Inc"),
    (11, "W&W Steel", "WW_STEEL", "T1_SUB", "yates_sub", True, 2,
     "Structural steel fabrication and erection", 2, "HIGH", "W & W STEEL LLC / W & W-AFCO STEEL, LLC"),

    # Drywall/Framing
    (4, "Berg Drywall", "BERG", "T1_SUB", "yates_sub", True, 4,
     "Drywall and framing contractor", 2, "HIGH", "BERG DRYWALL LLC / BERG GROUP LLC"),
    (9, "MK Marlow", "MK_MARLOW", "T1_SUB", "yates_sub", True, 4,
     "Drywall contractor", 2, "HIGH", "MK MARLOW CO. VICTORIA LLC"),
    (48, "Baker Triangle", "BAKER_TRI", "T1_SUB", "yates_sub", True, 4,
     "Drywall contractor", 2, "HIGH", "Baker Triangle / Baker Drywall"),
    (49, "JP Hi-Tech", "JPHITECH", "T1_SUB", "yates_sub", True, 4,
     "Drywall contractor", 2, "HIGH", "JP Hi-Tech"),

    # Painting/Coatings
    (13, "Alpha Painting", "ALPHA", "T1_SUB", "yates_sub", True, 5,
     "Painting contractor", 2, "HIGH", "ALPHA PAINTING & DECORATING COMPANY INC"),
    (14, "Apache Industrial Services", "APACHE", "T1_SUB", "yates_sub", True, 5,
     "Coatings contractor", 2, "HIGH", "APACHE INDUSTRIAL SERVICES INC"),
    (6, "Cherry Coatings", "CHERRY", "T1_SUB", "yates_sub", True, 5,
     "Painting and coatings contractor", 2, "HIGH", "CHERRY PAINTING COMPANY INC DBA CHERRY COATINGS"),

    # Insulation/Panels
    (5, "Brazos Urethane", "BRAZOS", "T1_SUB", "yates_sub", True, 8,
     "Urethane insulation and coatings", 2, "HIGH", "BRAZOS URETHANE INC"),
    (8, "Kovach Building Enclosures", "KOVACH", "T1_SUB", "yates_sub", True, 11,
     "Metal panel and enclosure systems", 2, "HIGH", "KOVACH ENCLOSURE SYSTEMS LLC"),

    # MEP
    (20, "Cobb Mechanical", "COBB", "T1_SUB", "yates_sub", True, 7,
     "Mechanical contractor", 2, "HIGH", "COBB MECHANICAL CONTRACTORS"),

    # Doors/Hardware/Specialties
    (50, "Cook & Boardman", "COOK", "T1_SUB", "yates_sub", True, 5,
     "Doors and hardware", 2, "HIGH", "COOK & BOARDMAN LLC"),
    (21, "Perry & Perry Specialties", "PERRY", "T1_SUB", "yates_sub", True, 5,
     "Architectural specialties", 2, "HIGH", "PERRY & PERRY BUILDERS INC"),
    (22, "Alert Lock & Key", "ALERT", "T1_SUB", "yates_sub", True, 5,
     "Door hardware installation", 2, "HIGH", None),

    # Staffing/General
    (51, "FinishLine Staffing", "FINISHLINE", "T1_SUB", "yates_sub", True, 12,
     "Labor staffing", 2, "HIGH", "FINISH LINE STAFFING LLC"),
    (17, "GDA Construction", "GDA", "T1_SUB", "yates_sub", True, 12,
     "General construction", 2, "HIGH", "GDA CONTRACTORS"),
    (52, "Preferred Dallas", "PREFERRED", "T1_SUB", "yates_sub", True, 12,
     "General contractor", 2, "HIGH", "PREFERRED DALLAS, LLC"),

    # =========================================================================
    # YATES SUBS - FROM QUALITY RECORDS ONLY (not in ProjectSight labor)
    # =========================================================================
    (53, "AMTS", "AMTS", "T1_SUB", "yates_sub", True, 4,
     "Quality inspection only", 2, "HIGH", "AMTS Inc."),
    (54, "Axios Industrial", "AXIOS", "T1_SUB", "yates_sub", True, 4,
     "Quality inspection only", 2, "HIGH", "Axios Industrial Group"),
    (55, "North Star", "NORTHSTAR", "T1_SUB", "yates_sub", True, 6,
     "Firestop contractor", 2, "HIGH", "North Star"),
    (56, "JMEG", "JMEG", "T1_SUB", "yates_sub", True, 6,
     "Firestop contractor", 2, "HIGH", "JMEG"),
    (57, "Greenberry Industrial", "GREENBERRY", "T1_SUB", "yates_sub", True, 2,
     "Welding contractor", 2, "HIGH", "Greenberry"),
    (58, "Lehne", "LEHNE", "T1_SUB", "yates_sub", True, 9,
     "Earthwork contractor", 2, "HIGH", "Lehne"),
    (59, "ABAR", "ABAR", "T1_SUB", "yates_sub", True, 9,
     "Earthwork contractor", 2, "HIGH", "ABAR"),

    # =========================================================================
    # OTHER T1 SUBCONTRACTORS (from original file)
    # =========================================================================
    (23, "Spectra Contract Flooring", "SPECTRA", "T1_SUB", "yates_sub", True, 5,
     "Flooring contractor", 2, "HIGH", None),
    (24, "Gateway Fabrication", "GATEWAY", "T1_SUB", "yates_sub", True, 2,
     "Miscellaneous steel fabrication", 2, "HIGH", None),
    (25, "Star Building Systems", "STAR", "T1_SUB", "yates_sub", True, 2,
     "Pre-engineered metal buildings", 2, "HIGH", None),
    (26, "Texas Scenic", "TEXAS_SCENIC", "T1_SUB", "yates_sub", True, 5,
     "Specialty finishes", 2, "HIGH", None),
    (27, "Marek Brothers", "MAREK", "T1_SUB", "yates_sub", True, 4,
     "Drywall and acoustical contractor", 2, "HIGH", None),
    (28, "Performance Contracting", "PCI", "T1_SUB", "yates_sub", True, 8,
     "Insulation and fireproofing", 2, "HIGH", None),

    # =========================================================================
    # PRECAST SUPPLIERS (material suppliers, not direct subs)
    # =========================================================================
    (60, "Gate Precast", "GATE", "T2_SUB", "precast_supplier", False, 10,
     "Precast concrete supplier", None, None, "Gate Precast"),
    (61, "Coreslab", "CORESLAB", "T2_SUB", "precast_supplier", False, 10,
     "Precast concrete supplier", None, None, "Coreslab"),
    (62, "Heldenfels", "HELDENFELS", "T2_SUB", "precast_supplier", False, 10,
     "Precast concrete supplier", None, None, "Heldenfels"),
]


def build_dim_company():
    """Generate dim_company.csv from company definitions."""

    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Build company records
    companies = []
    for row in COMPANIES:
        (company_id, canonical_name, short_code, tier, company_type, is_yates_sub,
         primary_trade_id, notes, parent_company_id, parent_confidence, full_name) = row
        companies.append({
            "company_id": company_id,
            "canonical_name": canonical_name,
            "short_code": short_code,
            "tier": tier,
            "company_type": company_type,
            "is_yates_sub": is_yates_sub,
            "primary_trade_id": primary_trade_id,
            "notes": notes,
            "parent_company_id": parent_company_id,
            "parent_confidence": parent_confidence,
            "full_name": full_name,
        })

    # Write CSV - column order must match Power BI expectations
    # Original columns first (in exact order), then new columns at end
    fieldnames = ["company_id", "canonical_name", "short_code", "tier",
                  "primary_trade_id", "notes", "parent_company_id", "parent_confidence",
                  # New columns added at end
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
