#!/usr/bin/env python3
"""
Run All Data Quality Checks.

Executes all data quality checks for integrated analysis and produces
a consolidated report with issues and recommendations.

Usage:
    python -m scripts.integrated_analysis.data_quality
    python -m scripts.integrated_analysis.data_quality --verbose
    python -m scripts.integrated_analysis.data_quality --output report.md
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List

_project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from scripts.integrated_analysis.data_quality.check_csi_coverage import check_csi_coverage
from scripts.integrated_analysis.data_quality.check_location_coverage import check_location_coverage
from scripts.integrated_analysis.data_quality.check_company_coverage import check_company_coverage


def run_all_checks(verbose: bool = False, output_file: str = None) -> Dict:
    """
    Run all data quality checks and generate consolidated report.

    Args:
        verbose: Show detailed output
        output_file: Optional path to save markdown report

    Returns:
        Dict with all check results
    """
    print("\n" + "="*80)
    print("INTEGRATED ANALYSIS - DATA QUALITY REPORT")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)

    all_results = {}
    all_issues = []
    all_recommendations = []

    # Run CSI coverage check
    print("\n\n")
    csi_results = check_csi_coverage(verbose=verbose)
    all_results['csi'] = csi_results
    all_issues.extend(csi_results.get('issues', []))
    all_recommendations.extend(csi_results.get('recommendations', []))

    # Run location coverage check
    print("\n\n")
    location_results = check_location_coverage(verbose=verbose)
    all_results['location'] = location_results
    all_issues.extend(location_results.get('issues', []))
    all_recommendations.extend(location_results.get('recommendations', []))

    # Run company coverage check
    print("\n\n")
    company_results = check_company_coverage(verbose=verbose)
    all_results['company'] = company_results
    all_issues.extend(company_results.get('issues', []))
    all_recommendations.extend(company_results.get('recommendations', []))

    # Final summary
    print("\n\n" + "="*80)
    print("CONSOLIDATED DATA QUALITY SUMMARY")
    print("="*80)

    print(f"\nTotal issues found: {len(all_issues)}")
    print(f"Total recommendations: {len(all_recommendations)}")

    # Categorize issues by severity
    critical_issues = [i for i in all_issues if 'CRITICAL' in i.upper() or 'CSI 07 84 00' in i]
    warning_issues = [i for i in all_issues if 'Low' in i or 'coverage' in i.lower()]
    info_issues = [i for i in all_issues if i not in critical_issues and i not in warning_issues]

    if critical_issues:
        print(f"\n🔴 Critical Issues ({len(critical_issues)}):")
        for issue in critical_issues:
            print(f"   - {issue}")

    if warning_issues:
        print(f"\n🟡 Warnings ({len(warning_issues)}):")
        for issue in warning_issues:
            print(f"   - {issue}")

    if info_issues:
        print(f"\n🔵 Info ({len(info_issues)}):")
        for issue in info_issues[:10]:
            print(f"   - {issue}")
        if len(info_issues) > 10:
            print(f"   ... and {len(info_issues) - 10} more")

    print(f"\n📋 Top Recommendations:")
    for i, rec in enumerate(all_recommendations[:5], 1):
        print(f"   {i}. {rec}")

    # Generate markdown report if requested
    if output_file:
        generate_markdown_report(all_results, all_issues, all_recommendations, output_file)
        print(f"\n📄 Report saved to: {output_file}")

    return {
        'results': all_results,
        'issues': all_issues,
        'recommendations': all_recommendations,
        'summary': {
            'total_issues': len(all_issues),
            'critical': len(critical_issues),
            'warnings': len(warning_issues),
            'info': len(info_issues),
        }
    }


def generate_markdown_report(
    results: Dict,
    issues: List[str],
    recommendations: List[str],
    output_path: str
):
    """Generate a markdown report of all findings."""
    lines = [
        "# Data Quality Report - Integrated Analysis",
        f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## Summary",
        "",
        f"- Total issues found: {len(issues)}",
        f"- Total recommendations: {len(recommendations)}",
        "",
        "## Issues",
        "",
    ]

    for issue in issues:
        lines.append(f"- {issue}")

    lines.extend([
        "",
        "## Recommendations",
        "",
    ])

    for i, rec in enumerate(recommendations, 1):
        lines.append(f"{i}. {rec}")

    lines.extend([
        "",
        "## CSI Coverage Details",
        "",
    ])

    csi = results.get('csi', {})
    if 'comparison' in csi:
        comp = csi['comparison']
        if hasattr(comp, 'sections_in_quality_not_p6'):
            lines.append("### CSI Sections in Quality Data but Not P6")
            lines.append("")
            lines.append("| CSI Section | Title | RABA | PSI |")
            lines.append("|-------------|-------|------|-----|")
            for csi_code, counts in comp.sections_in_quality_not_p6.items():
                raba = counts.get('RABA', 0)
                psi = counts.get('PSI', 0)
                lines.append(f"| {csi_code} | - | {raba:,} | {psi:,} |")

    lines.extend([
        "",
        "## Location Coverage Details",
        "",
    ])

    location = results.get('location', {})
    if 'source_coverage' in location:
        lines.append("| Source | Records | With Location ID | Coverage |")
        lines.append("|--------|---------|------------------|----------|")
        for name, result in location['source_coverage'].items():
            pct = f"{result.coverage_pct:.1f}%"
            lines.append(f"| {name} | {result.total_records:,} | {result.with_location_id:,} | {pct} |")

    lines.extend([
        "",
        "## Company Coverage Details",
        "",
    ])

    company = results.get('company', {})
    if 'source_coverage' in company:
        lines.append("| Source | Records | With Company ID | Coverage |")
        lines.append("|--------|---------|-----------------|----------|")
        for name, result in company['source_coverage'].items():
            pct = f"{result.coverage_pct:.1f}%"
            lines.append(f"| {name} | {result.total_records:,} | {result.with_company_id:,} | {pct} |")

    Path(output_path).write_text("\n".join(lines))


def main():
    parser = argparse.ArgumentParser(description='Run all data quality checks')
    parser.add_argument('--verbose', '-v', action='store_true', help='Show detailed output')
    parser.add_argument('--output', '-o', type=str, help='Output markdown report file')
    args = parser.parse_args()

    run_all_checks(verbose=args.verbose, output_file=args.output)


if __name__ == "__main__":
    main()
