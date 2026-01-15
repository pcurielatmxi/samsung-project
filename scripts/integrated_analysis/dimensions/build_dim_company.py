#!/usr/bin/env python3
"""
Build dim_company.csv - Company dimension table.

This script generates a dimension table with all companies/contractors identified
across the project data sources, classified by their relationship to Yates.

Company Types:
- yates_self: W.G. Yates & Sons (the GC)
- yates_sub: Subcontractors working under Yates contract
- major_contractor: GC/major contractors with direct Samsung contracts (not Yates subs)
- precast_supplier: Material suppliers (precast concrete)
- other: Unclassified or unclear

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
# Format: (company_id, canonical_name, full_name, company_type, is_yates_sub, primary_trade_id, notes)
# primary_trade_id references dim_trade.csv (1=CONCRETE, 2=STEEL, etc.)

COMPANIES = [
    # =========================================================================
    # YATES (the GC itself)
    # =========================================================================
    (1, "Yates", "W. G. YATES & SONS CONSTRUCTION COMPANY", "yates_self", True, 12, "General Contractor"),

    # =========================================================================
    # MAJOR CONTRACTORS (NOT Yates subs - direct Samsung contracts)
    # =========================================================================
    (2, "Samsung E&C", "Samsung E&C America, Inc. (SECAI)", "major_contractor", False, 12, "General Contractor - owner's rep"),
    (3, "Hensel Phelps", "Hensel Phelps Construction Co.", "major_contractor", False, 12, "General Contractor"),
    (4, "Austin Bridge", "Austin Bridge & Road / Austin Global Construction", "major_contractor", False, 9, "Sitework Contractor"),
    (5, "McCarthy", "McCarthy Building Companies", "major_contractor", False, 12, "Cleanroom Contractor"),
    (6, "PCL", "PCL Construction", "major_contractor", False, 12, "General Contractor"),

    # =========================================================================
    # YATES SUBS - CONFIRMED FROM PROJECTSIGHT LABOR
    # =========================================================================
    # Concrete
    (10, "Baker Concrete", "BAKER CONCRETE CONSTRUCTION INC", "yates_sub", True, 1, "Concrete"),
    (11, "Infinity Concrete", "INFINITY CONCRETE CONSTRUCTION LLC", "yates_sub", True, 1, "Concrete"),
    (12, "Latcon", "LATCON CORP", "yates_sub", True, 1, "Concrete"),
    (13, "Rolling Plains", "ROLLING PLAINS CONSTRUCTION INC", "yates_sub", True, 9, "Civil/Concrete"),
    (14, "FD Thomas", "F D THOMAS INC", "yates_sub", True, 9, "Civil/Concrete"),
    (15, "Grout Tech", "GROUT TECH INC", "yates_sub", True, 1, "Grouting"),
    (16, "Beck Foundation", "A H BECK FOUNDATION CO INC", "yates_sub", True, 9, "Foundations"),

    # Steel
    (20, "Patriot Erectors", "PATRIOT ERECTORS LLC", "yates_sub", True, 2, "Steel Erection"),
    (21, "SNS Erectors", "SNS Erectors, Inc", "yates_sub", True, 2, "Steel Erection"),
    (22, "W&W Steel", "W & W STEEL LLC / W & W-AFCO STEEL, LLC", "yates_sub", True, 2, "Structural Steel"),

    # Drywall/Framing
    (30, "Berg", "BERG DRYWALL LLC / BERG GROUP LLC", "yates_sub", True, 4, "Drywall"),
    (31, "MK Marlow", "MK MARLOW CO. VICTORIA LLC", "yates_sub", True, 4, "Drywall"),
    (32, "Baker Triangle", "Baker Triangle / Baker Drywall", "yates_sub", True, 4, "Drywall"),
    (33, "JP Hi-Tech", "JP Hi-Tech", "yates_sub", True, 4, "Drywall"),

    # Painting/Coatings
    (40, "Alpha Painting", "ALPHA PAINTING & DECORATING COMPANY INC", "yates_sub", True, 5, "Painting"),
    (41, "Apache Industrial", "APACHE INDUSTRIAL SERVICES INC", "yates_sub", True, 5, "Coatings"),
    (42, "Cherry Coatings", "CHERRY PAINTING COMPANY INC DBA CHERRY COATINGS", "yates_sub", True, 5, "Coatings"),

    # Insulation/Panels
    (50, "Brazos Urethane", "BRAZOS URETHANE INC", "yates_sub", True, 8, "Insulation"),
    (51, "Kovach", "KOVACH ENCLOSURE SYSTEMS LLC", "yates_sub", True, 11, "Panels"),

    # MEP
    (60, "Cobb Mechanical", "COBB MECHANICAL CONTRACTORS", "yates_sub", True, 7, "Mechanical"),

    # Doors/Hardware
    (70, "Cook & Boardman", "COOK & BOARDMAN LLC", "yates_sub", True, 5, "Doors/Hardware"),

    # Staffing/General
    (80, "FinishLine Staffing", "FINISH LINE STAFFING LLC", "yates_sub", True, 12, "Labor Staffing"),
    (81, "GDA", "GDA CONTRACTORS", "yates_sub", True, 12, "General"),
    (82, "Perry & Perry", "PERRY & PERRY BUILDERS INC", "yates_sub", True, 12, "General"),
    (83, "Preferred Dallas", "PREFERRED DALLAS, LLC", "yates_sub", True, 12, "General"),

    # =========================================================================
    # YATES SUBS - FROM QUALITY RECORDS ONLY (not in ProjectSight labor)
    # =========================================================================
    (90, "AMTS", "AMTS Inc.", "yates_sub", True, 4, "Quality inspection only"),
    (91, "Axios", "Axios Industrial Group", "yates_sub", True, 4, "Quality inspection only"),
    (92, "North Star", "North Star", "yates_sub", True, 6, "Firestop"),
    (93, "JMEG", "JMEG", "yates_sub", True, 6, "Firestop"),
    (94, "Greenberry", "Greenberry", "yates_sub", True, 2, "Welding"),
    (95, "Lehne", "Lehne", "yates_sub", True, 9, "Earthwork"),
    (96, "ABAR", "ABAR", "yates_sub", True, 9, "Earthwork"),

    # =========================================================================
    # PRECAST SUPPLIERS (material suppliers, not direct Yates subs)
    # =========================================================================
    (100, "Gate Precast", "Gate Precast", "precast_supplier", False, 10, "Precast concrete supplier"),
    (101, "Coreslab", "Coreslab", "precast_supplier", False, 10, "Precast concrete supplier"),
    (102, "Heldenfels", "Heldenfels", "precast_supplier", False, 10, "Precast concrete supplier"),
]


def build_dim_company():
    """Generate dim_company.csv from company definitions."""

    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Build company records
    companies = []
    for (company_id, canonical_name, full_name, company_type,
         is_yates_sub, primary_trade_id, notes) in COMPANIES:
        companies.append({
            "company_id": company_id,
            "canonical_name": canonical_name,
            "full_name": full_name,
            "company_type": company_type,
            "is_yates_sub": is_yates_sub,
            "primary_trade_id": primary_trade_id,
            "notes": notes,
        })

    # Write CSV
    fieldnames = ["company_id", "canonical_name", "full_name", "company_type",
                  "is_yates_sub", "primary_trade_id", "notes"]

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(companies)

    print(f"Generated {OUTPUT_FILE}")
    print(f"  Total companies: {len(companies)}")

    # Print summary by type
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
    print(f"Non-Yates Subs: {len(companies) - yates_subs}")

    return companies


if __name__ == "__main__":
    build_dim_company()
