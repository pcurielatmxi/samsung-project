"""
Improve embedding classification labels with role context and natural language.

This script generates enriched label descriptions for better semantic matching.

Usage:
    python -m scripts.quality.improve_labels --preview
    python -m scripts.quality.improve_labels --update
"""

import argparse
import pandas as pd
from pathlib import Path
from src.config.settings import settings


# Enhanced CSI descriptions with natural construction language
ENHANCED_CSI_LABELS = {
    '03': '''Concrete and foundation work. Cast-in-place concrete. Structural concrete.
             Foundation pours. Slab on grade. Elevated slabs. Deck pours.
             Concrete placement. Formwork and falsework. Shoring. Flying forms.
             Rebar installation. Reinforcing steel. Rebar tie. Steel placement.
             Concrete finishing. Troweling. Screeding. Curing. Concrete strength testing.
             Foundation work. Mud slab. Footings. Grade beams. Piers.
             Post-tensioned concrete. PT cables. Stressing. Tendon installation.
             Concrete walls. Columns. Beams. SOG. Elevated deck.''',

    '05': '''Structural steel fabrication and erection. Steel framing. Steel installation.
             Metal deck. Steel decking. Composite deck. Floor deck.
             Joists. Open web joists. Bar joists. Joist girders.
             Steel beams. Wide flange. Columns. HSS. Tube steel.
             Welding. Field welding. Structural welding. Weld inspection.
             Bolting. High-strength bolts. Torqued bolts. Anchor bolts.
             Steel erection. Steel frame. Structural frame. Building frame.
             Stairs. Steel stairs. Handrails. Guardrails.''',

    '07': '''Thermal and moisture protection. Waterproofing. Below-grade waterproofing.
             Insulation. Rigid insulation. Batt insulation. Spray foam.
             Roofing. Single-ply roofing. TPO. EPDM. Built-up roofing.
             Sealants. Joint sealant. Caulking. Expansion joints.
             Vapor barriers. Air barriers. Weather barriers.
             Fireproofing. Spray-applied fireproofing. SFRM. Fire protection.
             Firestopping. Fire caulk. Firestop systems. Penetration seals.
             Flashing. Sheet metal. Wall flashing. Roof flashing.''',

    '08': '''Openings. Doors. Door frames. Hollow metal doors. Aluminum doors.
             Windows. Aluminum windows. Glazing. Glass installation.
             Hardware. Door hardware. Locksets. Closers. Panic devices.
             Curtain wall. Storefront. Window wall. Glazing systems.
             Overhead doors. Roll-up doors. Sectional doors. Loading dock doors.
             Door installation. Frame installation. Hardware installation.''',

    '09': '''Finishes. Interior finishes. Wall finishes. Floor finishes.
             Drywall. Gypsum board. GWB. Sheetrock. Drywall installation.
             Taping and finishing. Joint compound. Skim coat.
             Ceiling tiles. Acoustical ceiling. T-bar ceiling. Drop ceiling. ACT.
             Suspended ceiling. Ceiling grid. Lay-in ceiling.
             Painting. Paint. Prime and paint. Wall coating. Epoxy coating.
             Flooring. VCT. Vinyl tile. Carpet. Epoxy flooring.
             Ceramic tile. Porcelain tile. Tile installation. Grout.''',

    '21': '''Fire protection systems. Fire suppression. Sprinklers. Wet pipe sprinklers.
             Sprinkler installation. Sprinkler heads. Branch lines. Mains.
             Standpipes. Fire hose connections. Hose cabinets. Siamese connections.
             Fire pumps. Jockey pumps. Fire water storage. Fire risers.
             Clean agent systems. FM200. Pre-action systems. Deluge systems.''',

    '22': '''Plumbing systems. Plumbing. Domestic water. Potable water. Water distribution.
             Sanitary. Sanitary sewer. Waste lines. Vent lines. DWV.
             Storm drain. Roof drains. Storm water. Area drains.
             Pipes. Piping. PVC. CPVC. Copper. Cast iron. HDPE.
             Fixtures. Plumbing fixtures. Water closets. Lavatories. Sinks.
             Water heaters. Boilers. Pumps. Piping installation.''',

    '23': '''HVAC systems. Mechanical. Heating and cooling. Climate control.
             Ductwork. Sheet metal. Supply ducts. Return ducts. Duct installation.
             Air handling units. AHU. Rooftop units. RTU. Package units.
             Chillers. Cooling towers. Condensing units. Evaporators.
             VAV boxes. Terminal units. Diffusers. Registers. Grilles.
             Piping. Chilled water. Hot water. Steam. Refrigerant piping.
             Controls. Building automation. BAS. Temperature controls. Thermostats.''',

    '26': '''Electrical systems. Electrical work. Power distribution. Lighting.
             Conduit. EMT. Rigid conduit. PVC conduit. Conduit installation.
             Cable tray. Wire. Cable. Cable pulling. Conductors.
             Panels. Electrical panels. Panelboards. Switchgear. Distribution boards.
             Transformers. Switchgear. Disconnects. Circuit breakers.
             Lighting fixtures. LED fixtures. Emergency lighting. Exit signs.
             Receptacles. Outlets. Switches. Devices.
             Grounding. Ground rods. Bonding. Equipment grounding.''',

    '27': '''Communications and data systems. Low voltage. Structured cabling.
             Data cables. Cat6. Fiber optic. Network cabling.
             Telecommunications. Phone systems. Data infrastructure.
             Cable pathways. Cable tray. J-hooks. Innerduct.''',

    '28': '''Fire alarm and security systems. Fire alarm. Smoke detectors. Heat detectors.
             Alarm devices. Horns. Strobes. Notification devices.
             Fire alarm control panel. FACP. Annunciator. Pull stations.
             Security systems. Access control. Card readers. Door contacts.
             CCTV. Cameras. Video surveillance. Security cameras.'''
}


# Company role keywords for classification
ROLE_KEYWORDS = {
    'general_contractor': ['general contractor', 'GC', 'prime contractor', 'main contractor', 'builder', 'construction management'],
    'subcontractor': ['subcontractor', 'sub', 'trade contractor', 'specialty contractor'],
    'inspection_company': ['inspection', 'testing', 'QA', 'QC', 'quality control', 'third-party', 'independent inspector'],
    'owner': ['owner', 'client', 'owner representative', 'project owner'],
    'engineer': ['engineer', 'engineering', 'design', 'architect', 'AE firm'],
    'supplier': ['supplier', 'fabricator', 'manufacturer', 'vendor']
}


def infer_company_role(canonical_name: str, primary_trade: str = None) -> str:
    """
    Infer company role from name and trade.

    Args:
        canonical_name: Company name
        primary_trade: Primary trade if available

    Returns:
        Inferred role string
    """
    name_lower = canonical_name.lower()
    trade_lower = (primary_trade or '').lower()

    # Check for inspection companies (highest priority - most specific)
    if any(kw in name_lower for kw in ['raba', 'psi', 'intertek', 'testing', 'inspection']):
        return 'inspection_company'

    # Check for GCs (before owner check, since Samsung E&C acts as both)
    if any(kw in name_lower for kw in ['yates', 'secai', 'samsung']):
        return 'general_contractor'

    # Check for owner/client (after GC check)
    if any(kw in name_lower for kw in ['sec', 'owner']):
        return 'owner'

    # Check for engineers
    if any(kw in name_lower for kw in ['engineer', 'architecture', 'design']):
        return 'engineer'

    # Default to subcontractor
    return 'subcontractor'


def build_enhanced_company_label(row: pd.Series) -> str:
    """
    Build enriched company label with role and trade context.

    Args:
        row: Row from dim_company DataFrame

    Returns:
        Enhanced label description
    """
    name = row['canonical_name']
    trade = row.get('primary_trade', '')
    role = infer_company_role(name, trade)

    # Base description
    parts = [name]

    # Add role context
    role_context = {
        'general_contractor': 'general contractor, prime contractor, main builder, GC',
        'subcontractor': 'subcontractor, trade contractor, specialty contractor',
        'inspection_company': 'third-party inspection, quality control testing, independent inspector, QA/QC firm',
        'owner': 'owner representative, client contact, project owner, construction management',
        'engineer': 'engineering firm, design professional, consultant, AE firm',
        'supplier': 'supplier, fabricator, manufacturer, vendor'
    }

    if role in role_context:
        parts.append(role_context[role])

    # Add trade context
    if trade and trade not in ['Unknown', 'General']:
        parts.append(f'{trade} work')

    return ', '.join(parts)


def preview_enhanced_labels():
    """Preview enhanced labels before updating."""

    dim_company_path = settings.PROCESSED_DATA_DIR / 'integrated_analysis' / 'dimensions' / 'dim_company.csv'

    if not dim_company_path.exists():
        print(f"ERROR: dim_company.csv not found at {dim_company_path}")
        return

    df = pd.read_csv(dim_company_path)

    print("\n" + "=" * 80)
    print("ENHANCED COMPANY LABELS PREVIEW")
    print("=" * 80)
    print(f"\nTotal companies: {len(df)}")
    print("\nSample enhanced labels:\n")

    # Show 10 representative examples
    samples = [
        'Samsung E&C',
        'Raba Kistner',
        'Yates',
        'SECAI',
        'Coreslab',
        'Berg',
        'Austin Bridge',
        'Intertek PSI',
        'McCarthy',
        'Rolling Plains'
    ]

    for name in samples:
        match = df[df['canonical_name'] == name]
        if not match.empty:
            row = match.iloc[0]
            enhanced = build_enhanced_company_label(row)
            print(f"{name:25} → {enhanced}")

    print("\n" + "=" * 80)
    print("ENHANCED CSI LABELS PREVIEW")
    print("=" * 80)
    print(f"\nTotal CSI sections: {len(ENHANCED_CSI_LABELS)}")
    print("\nSample enhanced CSI labels:\n")

    for section in ['03', '09', '23', '26']:
        desc = ENHANCED_CSI_LABELS[section]
        # Show first 150 chars
        preview = desc.replace('\n', ' ').strip()[:150] + '...'
        print(f"CSI {section}: {preview}\n")


def update_labels():
    """Update dim_company.csv with enhanced descriptions."""

    dim_company_path = settings.PROCESSED_DATA_DIR / 'integrated_analysis' / 'dimensions' / 'dim_company.csv'

    if not dim_company_path.exists():
        print(f"ERROR: dim_company.csv not found at {dim_company_path}")
        return

    df = pd.read_csv(dim_company_path)

    # Add enhanced_label column
    df['enhanced_label'] = df.apply(build_enhanced_company_label, axis=1)

    # Add inferred_role column
    df['inferred_role'] = df.apply(
        lambda row: infer_company_role(row['canonical_name'], row.get('primary_trade')),
        axis=1
    )

    # Save updated file
    df.to_csv(dim_company_path, index=False)

    print(f"\n✓ Updated {dim_company_path}")
    print(f"  Added columns: enhanced_label, inferred_role")
    print(f"  Total companies: {len(df)}")

    # Show role distribution
    print("\nRole distribution:")
    print(df['inferred_role'].value_counts())


def main():
    parser = argparse.ArgumentParser(
        description='Improve embedding classification labels'
    )
    parser.add_argument(
        '--preview',
        action='store_true',
        help='Preview enhanced labels without updating'
    )
    parser.add_argument(
        '--update',
        action='store_true',
        help='Update dim_company.csv with enhanced labels'
    )

    args = parser.parse_args()

    if args.update:
        update_labels()
    else:
        preview_enhanced_labels()


if __name__ == '__main__':
    main()
