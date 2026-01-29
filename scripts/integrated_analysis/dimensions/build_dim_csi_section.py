#!/usr/bin/env python3
"""
Build dim_csi_section.csv - CSI MasterFormat section dimension table.

This script generates a dimension table using CSI (Construction Specifications
Institute) MasterFormat section codes. The sections included are those actually
used in the Samsung Taylor FAB1 project, identified from:
- ProjectSight submittals
- Buyout/bid package lists
- Weekly reports
- Schedule narratives

CSI MasterFormat Structure:
- Division (2 digits): 01-50, e.g., "07" = Thermal & Moisture Protection
- Section (6 digits): XX YY ZZ, e.g., "07 84 00" = Firestopping

This table is separate from dim_trade.csv (which uses project-specific trade codes)
and provides maximum granularity for linking to submittals and specifications.

Usage:
    python -m scripts.integrated_analysis.dimensions.build_dim_csi_section

Output:
    {WINDOWS_DATA_DIR}/processed/integrated_analysis/dimensions/dim_csi_section.csv
"""

import csv
from pathlib import Path
from src.config.settings import settings

# Output location (flattened - all in integrated_analysis root)
OUTPUT_DIR = settings.PROCESSED_DATA_DIR / "integrated_analysis"
OUTPUT_FILE = OUTPUT_DIR / "dim_csi_section.csv"

# CSI Division definitions (MasterFormat 2020)
CSI_DIVISIONS = {
    "01": "General Requirements",
    "02": "Existing Conditions",
    "03": "Concrete",
    "04": "Masonry",
    "05": "Metals",
    "06": "Wood, Plastics, and Composites",
    "07": "Thermal and Moisture Protection",
    "08": "Openings",
    "09": "Finishes",
    "10": "Specialties",
    "11": "Equipment",
    "12": "Furnishings",
    "13": "Special Construction",
    "14": "Conveying Equipment",
    "21": "Fire Suppression",
    "22": "Plumbing",
    "23": "Heating, Ventilating, and Air Conditioning (HVAC)",
    "25": "Integrated Automation",
    "26": "Electrical",
    "27": "Communications",
    "28": "Electronic Safety and Security",
    "31": "Earthwork",
    "32": "Exterior Improvements",
    "33": "Utilities",
}

# CSI Sections used in Samsung Taylor FAB1 project
# Format: (csi_section, csi_title, notes)
# Notes indicate where this section was found in project documents
#
# DIVISION FALLBACKS: "XX 00 00" entries are fallbacks for when sources only
# provide division-level data (e.g., "Division 03") without specific sections.
# These allow FK assignment even when section granularity isn't available.
CSI_SECTIONS_FAB1 = [
    # Division 01 - General Requirements
    ("01 00 00", "General Requirements (Division)", "FALLBACK: Use when only division known"),
    ("01 10 00", "Summary", "General conditions, temporary facilities"),

    # Division 03 - Concrete
    ("03 00 00", "Concrete (Division)", "FALLBACK: Use when only division known"),
    ("03 30 00", "Cast-in-Place Concrete", "SOG, mat foundations, topping slabs"),
    ("03 41 00", "Structural Precast Concrete", "Waffle slabs, columns, girders, double-tees"),
    ("03 60 00", "Grouting", "Precast grout and caulking"),

    # Division 04 - Masonry
    ("04 00 00", "Masonry (Division)", "FALLBACK: Use when only division known"),
    ("04 20 00", "Unit Masonry", "CMU, parapet masonry"),

    # Division 05 - Metals
    ("05 00 00", "Metals (Division)", "FALLBACK: Use when only division known"),
    ("05 12 00", "Structural Steel Framing", "Trusses, beams, columns"),
    ("05 31 00", "Steel Decking", "Roof and floor decking"),
    ("05 40 00", "Cold-Formed Metal Framing", "Metal stud framing"),
    ("05 50 00", "Metal Fabrications", "Misc steel, stairs, railings, ladders, grating"),

    # Division 07 - Thermal and Moisture Protection
    ("07 00 00", "Thermal and Moisture Protection (Division)", "FALLBACK: Use when only division known"),
    ("07 11 00", "Dampproofing", "Below-grade dampproofing"),
    ("07 13 00", "Sheet Waterproofing", "Below-grade waterproofing, SikaProof"),
    ("07 21 13", "Board Insulation", "Rigid insulation"),
    ("07 21 16", "Blanket Insulation", "Mineral wool, batt insulation"),
    ("07 26 00", "Vapor Retarders", "Vapor barriers"),
    ("07 42 43", "Composite Wall Panels", "Insulated metal panels (IMP), cladding"),
    ("07 52 00", "Modified Bituminous Membrane Roofing", "TPO roofing, membrane"),
    ("07 71 00", "Roof Specialties", "Copings, flashings, expansion joints"),
    ("07 81 00", "Applied Fireproofing", "SFRM, IFRM, intumescent coatings"),
    ("07 84 00", "Firestopping", "Penetration firestop, joint firestop"),
    ("07 90 00", "Joint Protection", "Sealants, caulking, expansion joints"),

    # Division 08 - Openings
    ("08 00 00", "Openings (Division)", "FALLBACK: Use when only division known"),
    ("08 11 13", "Hollow Metal Doors and Frames", "Steel doors and frames"),
    ("08 33 23", "Overhead Coiling Doors", "Roll-up doors, coiling doors"),
    ("08 71 00", "Door Hardware", "Hinges, locksets, closers"),
    ("08 80 00", "Glazing", "Glass, glazing systems"),

    # Division 09 - Finishes
    ("09 00 00", "Finishes (Division)", "FALLBACK: Use when only division known"),
    ("09 06 65", "Chemical-Resistant Coatings", "Epoxy coatings, chemical-resistant flooring"),
    ("09 21 16", "Gypsum Board Assemblies", "Drywall, gypsum board, shaftliner"),
    ("09 51 00", "Acoustical Ceilings", "Ceiling tiles, grid systems"),
    ("09 65 00", "Resilient Flooring", "VCT, resilient tile"),
    ("09 91 26", "Painting - Building", "Interior/exterior painting"),
    ("09 91 29", "Painting - Equipment and Piping", "Equipment and pipe painting"),

    # Division 11 - Equipment
    ("11 00 00", "Equipment (Division)", "FALLBACK: Use when only division known"),
    ("11 13 19", "Loading Dock Equipment", "Dock levelers, dock bumpers"),

    # Division 13 - Special Construction
    ("13 00 00", "Special Construction (Division)", "FALLBACK: Use when only division known"),
    ("13 48 00", "Sound and Vibration Control", "Vibration isolation, acoustic treatment"),

    # Division 14 - Conveying Equipment
    ("14 00 00", "Conveying Equipment (Division)", "FALLBACK: Use when only division known"),
    ("14 21 00", "Electric Traction Elevators", "Passenger and freight elevators"),

    # Division 21 - Fire Suppression
    ("21 00 00", "Fire Suppression (Division)", "FALLBACK: Use when only division known"),
    ("21 10 00", "Water-Based Fire-Suppression Systems", "Sprinkler systems"),
    ("21 30 00", "Fire Pumps", "Fire pump systems"),

    # Division 22 - Plumbing
    ("22 00 00", "Plumbing (Division)", "FALLBACK: Use when only division known"),
    ("22 05 00", "Common Work Results for Plumbing", "General plumbing"),
    ("22 11 00", "Facility Water Distribution", "Domestic water piping"),
    ("22 13 00", "Facility Sanitary Sewerage", "Sanitary drainage, waste piping"),
    ("22 14 00", "Facility Storm Drainage", "Storm drainage systems"),

    # Division 23 - HVAC
    ("23 00 00", "HVAC (Division)", "FALLBACK: Use when only division known"),
    ("23 05 00", "Common Work Results for HVAC", "General HVAC"),
    ("23 31 00", "HVAC Ducts and Casings", "Ductwork, duct accessories"),
    ("23 36 00", "Air Terminal Units", "VAV boxes, diffusers"),
    ("23 73 00", "Indoor Central-Station Air-Handling Units", "AHUs"),

    # Division 26 - Electrical
    ("26 00 00", "Electrical (Division)", "FALLBACK: Use when only division known"),
    ("26 05 00", "Common Work Results for Electrical", "General electrical"),
    ("26 05 19", "Low-Voltage Electrical Power Conductors and Cables", "Wire and cable"),
    ("26 05 33", "Raceway and Boxes for Electrical Systems", "Conduit, junction boxes"),
    ("26 24 00", "Switchboards and Panelboards", "Electrical panels"),
    ("26 27 26", "Wiring Devices", "Receptacles, switches"),
    ("26 51 00", "Interior Lighting", "Light fixtures"),

    # Division 31 - Earthwork
    ("31 00 00", "Earthwork (Division)", "FALLBACK: Use when only division known"),
    ("31 10 00", "Site Clearing", "Demolition, clearing"),
    ("31 23 00", "Excavation and Fill", "Excavation, backfill, grading"),
    ("31 63 00", "Bored Piles", "Drilled piers, deep foundations"),
]


def build_dim_csi_section():
    """Generate dim_csi_section.csv from CSI section definitions."""

    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Build CSI section records
    sections = []
    for section_id, (csi_section, csi_title, notes) in enumerate(CSI_SECTIONS_FAB1, start=1):
        # Extract division from section (first 2 digits)
        csi_division = csi_section[:2]
        division_name = CSI_DIVISIONS.get(csi_division, "Unknown")

        sections.append({
            "csi_section_id": section_id,
            "csi_section": csi_section,
            "csi_title": csi_title,
            "csi_division": csi_division,
            "division_name": division_name,
            "notes": notes,
        })

    # Write CSV
    fieldnames = ["csi_section_id", "csi_section", "csi_title", "csi_division", "division_name", "notes"]

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(sections)

    print(f"Generated {OUTPUT_FILE}")
    print(f"  Total sections: {len(sections)}")
    print(f"  Divisions covered: {len(set(s['csi_division'] for s in sections))}")

    # Print summary by division
    print("\nSections by Division:")
    division_counts = {}
    for s in sections:
        div = f"{s['csi_division']} - {s['division_name']}"
        division_counts[div] = division_counts.get(div, 0) + 1

    for div in sorted(division_counts.keys()):
        print(f"  {div}: {division_counts[div]}")

    return sections


if __name__ == "__main__":
    build_dim_csi_section()
