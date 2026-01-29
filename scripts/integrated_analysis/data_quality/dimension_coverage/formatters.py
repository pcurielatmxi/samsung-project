"""
Report formatters for dimension coverage output.

This module handles all output formatting:
- Console output (print_* functions)
- Markdown report generation
"""

from datetime import datetime
from pathlib import Path
from typing import Dict

import pandas as pd

from scripts.integrated_analysis.data_quality.dimension_coverage.models import (
    DimensionStats,
    SourceCoverage,
)
from scripts.integrated_analysis.data_quality.dimension_coverage.config import (
    SOURCE_CONFIGS,
    LOCATION_TYPE_ORDER,
    THRESHOLDS,
)


# =============================================================================
# Console Formatting Helpers
# =============================================================================

def format_pct(
    value: float,
    threshold_warn: float = THRESHOLDS['warning'],
    threshold_good: float = THRESHOLDS['good'],
) -> str:
    """
    Format percentage with status indicator emoji.

    Args:
        value: Percentage value (0-100)
        threshold_warn: Below this is red, at or above is yellow
        threshold_good: At or above this is green

    Returns:
        Formatted string like "✅  95.0%" or "❌  22.1%"
    """
    if value >= threshold_good:
        return f"✅ {value:>5.1f}%"
    elif value >= threshold_warn:
        return f"⚠️ {value:>5.1f}%"
    elif value > 0:
        return f"❌ {value:>5.1f}%"
    else:
        return "    —    "


# =============================================================================
# Section Printers
# =============================================================================

def print_coverage_matrix(
    coverage: Dict[str, SourceCoverage],
    dim_stats: Dict[str, DimensionStats],
) -> None:
    """
    Print Section 1: Coverage Matrix.

    Shows high-level coverage percentages for each source across
    all three dimensions (location, company, CSI).
    """
    print("\n" + "=" * 100)
    print("SECTION 1: COVERAGE MATRIX (Source × Dimension)")
    print("=" * 100)

    # Dimension totals
    print(f"\nDimension Tables:")
    print(f"  • dim_location:    {dim_stats['location'].total_records:>5,} records")
    print(f"  • dim_company:     {dim_stats['company'].total_records:>5,} records")
    print(f"  • dim_csi_section: {dim_stats['csi'].total_records:>5,} records")

    # Header
    print("\n" + "-" * 100)
    print(f"{'Source':<14} {'Records':>12}  │ {'Location':>12}  {'Company':>12}  {'CSI Section':>12}")
    print("-" * 100)

    # Data rows
    for name, cov in coverage.items():
        loc_str = format_pct(cov.location_id_pct)
        comp_str = (
            format_pct(cov.company_id_pct)
            if SOURCE_CONFIGS[name]['company_id_col']
            else "    N/A    "
        )
        csi_str = format_pct(cov.csi_section_pct)

        print(f"{name:<14} {cov.total_records:>12,}  │ {loc_str}  {comp_str}  {csi_str}")

    print("-" * 100)


def print_location_granularity(
    coverage: Dict[str, SourceCoverage],
    dim_stats: Dict[str, DimensionStats],
) -> None:
    """
    Print Section 2: Location Granularity.

    Shows the distribution of location types per source, indicating
    how specific the location data is.
    """
    print("\n" + "=" * 100)
    print("SECTION 2: LOCATION GRANULARITY (Source × Location Type)")
    print("=" * 100)
    print("\nGranularity from most to least specific:")
    print("  ROOM/STAIR/ELEVATOR > GRIDLINE > LEVEL > BUILDING > UNDEFINED")

    # Dimension breakdown
    print(f"\ndim_location breakdown:")
    for loc_type, count in sorted(
        dim_stats['location'].breakdown.items(),
        key=lambda x: -x[1]
    ):
        print(f"  • {loc_type}: {count}")

    # Get location types present in source data
    source_types = set()
    for cov in coverage.values():
        source_types.update(cov.location_type_distribution.keys())

    display_types = [t for t in LOCATION_TYPE_ORDER if t in source_types]

    # Header
    col_width = 12
    print("\n" + "-" * 100)
    header = f"{'Source':<14} {'Records':>10}"
    for lt in display_types:
        header += f" │ {lt:>{col_width}}"
    print(header)
    print("-" * 100)

    # Data rows
    for name, cov in coverage.items():
        row = f"{name:<14} {cov.total_records:>10,}"
        for lt in display_types:
            count = cov.location_type_distribution.get(lt, 0)
            if count > 0:
                pct = count / cov.total_records * 100
                cell = f"{count:,} ({pct:.0f}%)"
                row += f" │ {cell:>{col_width}}"
            else:
                row += f" │ {'—':>{col_width}}"
        print(row)

    print("-" * 100)

    # Granularity summary
    print("\nGranularity Summary:")
    for name, cov in coverage.items():
        summary = cov.get_granularity_summary()
        total_with_loc = sum(cov.location_type_distribution.values())

        if total_with_loc == 0:
            print(f"  {name}: No location data")
        else:
            print(
                f"  {name}: "
                f"Room-level: {summary['room_level']:.1f}%, "
                f"Grid-level: {summary['grid_level']:.1f}%, "
                f"Coarse: {summary['coarse_level']:.1f}%"
            )


def print_csi_coverage(
    coverage: Dict[str, SourceCoverage],
    dim_csi: pd.DataFrame,
) -> None:
    """
    Print Section 3: CSI Section Coverage.

    Shows CSI section distribution and identifies gaps where
    quality data can't be joined to P6 schedule.
    """
    print("\n" + "=" * 100)
    print("SECTION 3: CSI SECTION COVERAGE")
    print("=" * 100)

    # Collect CSI sections per source
    source_csi = {
        name: set(cov.csi_distribution.keys())
        for name, cov in coverage.items()
    }

    p6_csi = source_csi.get('P6', set())
    quality_csi = source_csi.get('RABA', set()) | source_csi.get('PSI', set())

    in_quality_not_p6 = quality_csi - p6_csi

    # Summary
    print(f"\nCSI Section Coverage Summary:")
    print(f"  • Total CSI sections in dim_csi_section: {len(dim_csi)}")
    print(f"  • Sections used in P6: {len(p6_csi)}")
    print(f"  • Sections used in RABA/PSI: {len(quality_csi)}")
    print(f"  • Common sections (joinable): {len(p6_csi & quality_csi)}")

    # Gap analysis
    if in_quality_not_p6:
        print(f"\n⚠️  CSI Sections in Quality Data but NOT in P6 ({len(in_quality_not_p6)}):")
        print("   (Cannot join quality inspections to P6 tasks by CSI)")

        # Build gap details
        gap_details = []
        empty_cov = SourceCoverage('', 0, 0, 0, {}, 0, 0, set(), 0, 0, {})

        for csi in sorted(in_quality_not_p6):
            raba_count = coverage.get('RABA', empty_cov).csi_distribution.get(csi, 0)
            psi_count = coverage.get('PSI', empty_cov).csi_distribution.get(csi, 0)
            total = raba_count + psi_count

            title_match = dim_csi[dim_csi['csi_section'] == csi]['csi_title']
            title = title_match.values[0] if len(title_match) > 0 else "Unknown"

            gap_details.append((csi, title[:30], raba_count, psi_count, total))

        gap_details.sort(key=lambda x: -x[4])

        print(f"\n   {'CSI Section':<12} {'Title':<32} {'RABA':>8} {'PSI':>8} {'Total':>8}")
        print("   " + "-" * 72)

        for csi, title, raba, psi, total in gap_details[:15]:
            print(f"   {csi:<12} {title:<32} {raba:>8,} {psi:>8,} {total:>8,}")

        if len(gap_details) > 15:
            print(f"   ... and {len(gap_details) - 15} more")

    # Top CSI sections per source
    print(f"\nTop 5 CSI Sections per Source:")

    for name in ['P6', 'RABA', 'PSI', 'TBM', 'ProjectSight']:
        if name not in coverage:
            continue

        cov = coverage[name]
        if not cov.csi_distribution:
            continue

        print(f"\n  {name}:")
        sorted_csi = sorted(cov.csi_distribution.items(), key=lambda x: -x[1])[:5]

        for csi, count in sorted_csi:
            title_match = dim_csi[dim_csi['csi_section'] == csi]['csi_title']
            title = title_match.values[0][:35] if len(title_match) > 0 else "Unknown"
            pct = count / cov.total_records * 100
            print(f"    {csi} {title:<35} {count:>8,} ({pct:>5.1f}%)")


def print_company_coverage(
    coverage: Dict[str, SourceCoverage],
    dim_company: pd.DataFrame,
) -> None:
    """
    Print Section 4: Company Coverage.

    Shows company dimension coverage and lists unresolved company names.
    """
    print("\n" + "=" * 100)
    print("SECTION 4: COMPANY COVERAGE")
    print("=" * 100)

    # Dimension summary
    print(f"\nCompany Dimension Summary:")
    print(f"  • Total companies in dim_company: {len(dim_company)}")

    if 'tier' in dim_company.columns:
        tier_counts = dim_company['tier'].value_counts()
        print(f"  • By tier: {tier_counts.to_dict()}")

    if 'is_yates_sub' in dim_company.columns:
        yates_subs = dim_company['is_yates_sub'].sum()
        print(f"  • Yates subcontractors: {int(yates_subs)}")

    # Coverage table
    print(f"\nCompany Coverage by Source:")
    print(f"   {'Source':<14} {'Records':>12} {'With ID':>12} {'Coverage':>12} {'Unresolved':>12}")
    print("   " + "-" * 64)

    all_unresolved = set()

    for name, cov in coverage.items():
        if SOURCE_CONFIGS[name]['company_id_col'] is None:
            continue

        all_unresolved.update(cov.unresolved_companies)

        status = "✅" if cov.company_id_pct >= 95 else "⚠️" if cov.company_id_pct >= 80 else "❌"
        print(
            f"   {name:<14} "
            f"{cov.total_records:>12,} "
            f"{cov.company_id_count:>12,} "
            f"{status} {cov.company_id_pct:>7.1f}% "
            f"{len(cov.unresolved_companies):>10}"
        )

    # Unresolved names
    if all_unresolved:
        print(f"\n⚠️  Unresolved Company Names ({len(all_unresolved)} unique):")
        for company in sorted(all_unresolved)[:20]:
            print(f"   • {company}")
        if len(all_unresolved) > 20:
            print(f"   ... and {len(all_unresolved) - 20} more")


def print_summary(
    coverage: Dict[str, SourceCoverage],
    dim_stats: Dict[str, DimensionStats],
) -> None:
    """
    Print Summary & Recommendations section.

    Identifies issues, provides actionable recommendations with script
    references, and shows health scores.
    """
    print("\n" + "=" * 100)
    print("SUMMARY & RECOMMENDATIONS")
    print("=" * 100)

    issues = []
    recommendations = []

    # Check location coverage
    for name, cov in coverage.items():
        if cov.location_id_pct < 50 and cov.total_records > 100:
            issues.append(f"{name}: Low location coverage ({cov.location_id_pct:.1f}%)")
            script = SOURCE_CONFIGS[name].get('enrichment_scripts', {}).get('location')
            if script:
                recommendations.append(
                    f"Improve {name} location enrichment\n"
                    f"      → Edit: {script}"
                )
            else:
                recommendations.append(
                    f"Improve {name} location enrichment (source lacks location data)"
                )

    # Check company coverage
    for name, cov in coverage.items():
        if SOURCE_CONFIGS[name]['company_id_col'] and cov.company_id_pct < 90:
            issues.append(f"{name}: Low company coverage ({cov.company_id_pct:.1f}%)")
            script = SOURCE_CONFIGS[name].get('enrichment_scripts', {}).get('company')
            if script:
                recommendations.append(
                    f"Improve {name} company coverage\n"
                    f"      → Edit: {script}"
                )

    # Check CSI coverage
    for name, cov in coverage.items():
        if cov.csi_section_pct < 80 and cov.total_records > 100:
            issues.append(f"{name}: Low CSI coverage ({cov.csi_section_pct:.1f}%)")
            script = SOURCE_CONFIGS[name].get('enrichment_scripts', {}).get('csi')
            if script:
                recommendations.append(
                    f"Improve {name} CSI coverage\n"
                    f"      → Edit: {script}"
                )

    # Unresolved companies
    all_unresolved = set()
    sources_with_unresolved = []
    for name, cov in coverage.items():
        if cov.unresolved_companies:
            all_unresolved.update(cov.unresolved_companies)
            sources_with_unresolved.append(name)

    if all_unresolved:
        # Find the company_aliases path from any source that has it
        aliases_path = None
        for name in sources_with_unresolved:
            aliases_path = SOURCE_CONFIGS[name].get('enrichment_scripts', {}).get('company_aliases')
            if aliases_path:
                break

        if aliases_path:
            recommendations.append(
                f"Add {len(all_unresolved)} unresolved company names\n"
                f"      → Edit: {aliases_path}"
            )
        else:
            recommendations.append(
                f"Add {len(all_unresolved)} unresolved company names to map_company_aliases.csv"
            )

    # Print issues
    print(f"\n🔴 Issues Found: {len(issues)}")
    for issue in issues:
        print(f"   • {issue}")

    # Print recommendations with script references
    print(f"\n📋 Actionable Recommendations:")
    for i, rec in enumerate(recommendations, 1):
        print(f"   {i}. {rec}")

    # Health scores
    total_sources = len(coverage)
    good_location = sum(1 for c in coverage.values() if c.location_id_pct >= 80)
    good_company = sum(
        1 for n, c in coverage.items()
        if SOURCE_CONFIGS[n]['company_id_col'] is None or c.company_id_pct >= 80
    )
    good_csi = sum(1 for c in coverage.values() if c.csi_section_pct >= 80)

    print(f"\n📊 Health Score:")
    print(f"   • Location: {good_location}/{total_sources} sources ≥80%")
    print(f"   • Company:  {good_company}/{total_sources} sources ≥80%")
    print(f"   • CSI:      {good_csi}/{total_sources} sources ≥80%")

    # Print quick reference for all enrichment scripts
    print("\n" + "-" * 100)
    print("ENRICHMENT SCRIPT REFERENCE")
    print("-" * 100)
    print("\nTo improve coverage, edit these files:")
    print()

    for name in coverage.keys():
        scripts = SOURCE_CONFIGS[name].get('enrichment_scripts', {})
        if not scripts:
            continue

        print(f"  {name}:")
        for dim, script in scripts.items():
            if script:
                print(f"    • {dim}: {script}")
            else:
                print(f"    • {dim}: (no source data available)")
        print()


def print_full_report(
    coverage: Dict[str, SourceCoverage],
    dim_stats: Dict[str, DimensionStats],
    dim_csi: pd.DataFrame,
    dim_company: pd.DataFrame,
) -> None:
    """
    Print the complete dimension coverage report.

    Calls all section printers in order.
    """
    print_coverage_matrix(coverage, dim_stats)
    print_location_granularity(coverage, dim_stats)
    print_csi_coverage(coverage, dim_csi)
    print_company_coverage(coverage, dim_company)
    print_summary(coverage, dim_stats)


# =============================================================================
# Markdown Report Generation
# =============================================================================

def generate_markdown_report(
    coverage: Dict[str, SourceCoverage],
    dim_stats: Dict[str, DimensionStats],
    dim_csi: pd.DataFrame,
    output_path: str,
) -> None:
    """
    Generate a markdown report file.

    Args:
        coverage: Coverage results per source
        dim_stats: Dimension table statistics
        dim_csi: CSI dimension DataFrame
        output_path: Path to write markdown file
    """
    lines = [
        "# Dimension Coverage Quality Report",
        f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## Section 1: Coverage Matrix",
        "",
        "| Source | Records | Location | Company | CSI Section |",
        "|--------|--------:|:--------:|:-------:|:-----------:|",
    ]

    for name, cov in coverage.items():
        loc_str = f"{cov.location_id_pct:.1f}%"
        comp_str = (
            f"{cov.company_id_pct:.1f}%"
            if SOURCE_CONFIGS[name]['company_id_col']
            else "N/A"
        )
        csi_str = f"{cov.csi_section_pct:.1f}%"
        lines.append(f"| {name} | {cov.total_records:,} | {loc_str} | {comp_str} | {csi_str} |")

    # Section 2: Location Granularity
    source_types = set()
    for cov in coverage.values():
        source_types.update(cov.location_type_distribution.keys())

    display_types = [t for t in LOCATION_TYPE_ORDER if t in source_types]

    lines.extend([
        "",
        "## Section 2: Location Granularity",
        "",
        "| Source | " + " | ".join(display_types) + " |",
        "|--------|" + "|".join([":---:" for _ in display_types]) + "|",
    ])

    for name, cov in coverage.items():
        dist = cov.location_type_distribution
        cells = []
        for lt in display_types:
            count = dist.get(lt, 0)
            if count > 0:
                pct = count / cov.total_records * 100
                cells.append(f"{count:,} ({pct:.0f}%)")
            else:
                cells.append("—")
        lines.append(f"| {name} | " + " | ".join(cells) + " |")

    # Section 3: CSI Summary
    empty_cov = SourceCoverage('', 0, 0, 0, {}, 0, 0, set(), 0, 0, {})
    p6_csi = set(coverage.get('P6', empty_cov).csi_distribution.keys())
    quality_csi = (
        set(coverage.get('RABA', empty_cov).csi_distribution.keys()) |
        set(coverage.get('PSI', empty_cov).csi_distribution.keys())
    )

    lines.extend([
        "",
        "## Section 3: CSI Coverage",
        "",
        f"- Sections in P6: {len(p6_csi)}",
        f"- Sections in RABA/PSI: {len(quality_csi)}",
        f"- Common (joinable): {len(p6_csi & quality_csi)}",
        f"- In quality but not P6: {len(quality_csi - p6_csi)}",
    ])

    # Section 4: Company Summary
    all_unresolved = set()
    for cov in coverage.values():
        all_unresolved.update(cov.unresolved_companies)

    lines.extend([
        "",
        "## Section 4: Company Coverage",
        "",
        f"- Total companies in dim_company: {dim_stats['company'].total_records}",
        f"- Unresolved company names: {len(all_unresolved)}",
    ])

    # Section 5: Enrichment Script Reference
    lines.extend([
        "",
        "## Section 5: Enrichment Script Reference",
        "",
        "To improve coverage, edit these files:",
        "",
    ])

    for name in coverage.keys():
        scripts = SOURCE_CONFIGS[name].get('enrichment_scripts', {})
        if not scripts:
            continue

        lines.append(f"### {name}")
        lines.append("")
        lines.append("| Dimension | Script |")
        lines.append("|-----------|--------|")

        for dim, script in scripts.items():
            if script:
                lines.append(f"| {dim} | `{script}` |")
            else:
                lines.append(f"| {dim} | *(no source data available)* |")

        lines.append("")

    Path(output_path).write_text("\n".join(lines))
