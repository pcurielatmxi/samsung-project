#!/usr/bin/env python3
"""Generate consolidated snapshot report from all data sources.

Usage:
    python -m scripts.integrated_analysis.snapshot_reports.consolidate_snapshot --latest
    python -m scripts.integrated_analysis.snapshot_reports.consolidate_snapshot --file-id 88
    python -m scripts.integrated_analysis.snapshot_reports.consolidate_snapshot --all
    python -m scripts.integrated_analysis.snapshot_reports.consolidate_snapshot --list
"""

import argparse
import sys
from pathlib import Path
from datetime import date
from typing import Dict, Any, List
import pandas as pd

_project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import Settings
from scripts.integrated_analysis.snapshot_reports.data_loaders import (
    SnapshotPeriod,
    get_all_snapshot_periods,
    get_snapshot_period,
    load_schedule_data,
    load_labor_data,
    load_quality_data,
    load_narrative_data,
)


def get_output_dir(period: SnapshotPeriod) -> Path:
    """Get output directory for a snapshot report."""
    output_base = Settings.PROJECT_ROOT / 'data' / 'analysis' / 'snapshot_reports'
    output_dir = output_base / period.label
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def format_schedule_section(schedule: Dict[str, Any], period: SnapshotPeriod) -> str:
    """Format schedule progress section."""
    lines = []
    lines.append("## 1. Schedule Progress & Delays")
    lines.append("")

    overall = schedule['overall']
    snapshots = schedule['snapshots']

    # Snapshot info
    lines.append("### Snapshots")
    lines.append("")
    lines.append(f"| Snapshot | File ID | Data Date | Tasks |")
    lines.append(f"|----------|---------|-----------|-------|")
    if snapshots['start']:
        lines.append(f"| Start | {snapshots['start']['file_id']} | {snapshots['start']['data_date']} | {snapshots['start']['task_count']:,} |")
    if snapshots['end']:
        lines.append(f"| End | {snapshots['end']['file_id']} | {snapshots['end']['data_date']} | {snapshots['end']['task_count']:,} |")
    lines.append(f"| **Period** | | **{period.duration_days} days** | |")
    lines.append("")

    # Overall progress
    lines.append("### Overall Progress")
    lines.append("")
    lines.append(f"| Metric | Value |")
    lines.append(f"|--------|-------|")
    lines.append(f"| Total Tasks | {overall.total_tasks:,} |")
    lines.append(f"| % Complete (Start) | {overall.pct_complete_start:.1f}% |")
    lines.append(f"| % Complete (End) | {overall.pct_complete_end:.1f}% |")
    lines.append(f"| Change | {overall.pct_complete_change:+.1f}% |")
    lines.append(f"| Tasks Completed This Period | {overall.completed_this_period:,} |")
    lines.append(f"| Tasks Behind Schedule | {overall.tasks_behind_schedule:,} |")
    lines.append(f"| Tasks with Negative Float | {overall.tasks_with_negative_float:,} |")
    lines.append("")

    # Progress by floor
    if schedule['by_floor']:
        lines.append("### Progress by Floor")
        lines.append("")
        lines.append("| Floor | Total | Completed | Change | % Complete | Behind Schedule |")
        lines.append("|-------|-------|-----------|--------|------------|-----------------|")

        # Sort by completed_this_period descending
        by_floor = sorted(schedule['by_floor'], key=lambda x: x.completed_this_period, reverse=True)

        for f in by_floor[:20]:  # Top 20 floors
            lines.append(f"| {f.group_name} | {f.total_tasks:,} | {f.completed_end:,} | {f.completed_this_period:+,} | {f.pct_complete_end:.1f}% | {f.tasks_behind_schedule:,} |")

        if len(by_floor) > 20:
            lines.append(f"| ... | ({len(by_floor) - 20} more floors) | | | | |")
        lines.append("")

    # Progress by scope
    if schedule['by_scope']:
        lines.append("### Progress by Scope (Trade)")
        lines.append("")
        lines.append("| Scope | Total | Completed | Change | % Complete | Behind Schedule |")
        lines.append("|-------|-------|-----------|--------|------------|-----------------|")

        by_scope = sorted(schedule['by_scope'], key=lambda x: x.total_tasks, reverse=True)
        for s in by_scope:
            lines.append(f"| {s.group_name} | {s.total_tasks:,} | {s.completed_end:,} | {s.completed_this_period:+,} | {s.pct_complete_end:.1f}% | {s.tasks_behind_schedule:,} |")
        lines.append("")

    # Delay-causing tasks
    if not schedule['delay_tasks'].empty:
        lines.append("### Delay-Causing Tasks (Top 20)")
        lines.append("")
        lines.append("Tasks with negative float or past target end date:")
        lines.append("")

        df = schedule['delay_tasks']
        cols = [c for c in ['task_code', 'task_name', 'floor', 'trade_name', 'phys_complete_pct', 'total_float_hr_cnt'] if c in df.columns]

        if cols:
            header = " | ".join(cols)
            lines.append(f"| {header} |")
            lines.append("|" + "|".join(["---"] * len(cols)) + "|")

            for _, row in df.head(20).iterrows():
                values = []
                for c in cols:
                    v = row[c]
                    if c == 'phys_complete_pct':
                        values.append(f"{v:.0f}%" if pd.notna(v) else "-")
                    elif c == 'total_float_hr_cnt':
                        values.append(f"{v:.0f}h" if pd.notna(v) else "-")
                    elif c == 'task_name':
                        # Truncate long names
                        values.append(str(v)[:50] + "..." if len(str(v)) > 50 else str(v))
                    else:
                        values.append(str(v) if pd.notna(v) else "-")
                lines.append("| " + " | ".join(values) + " |")
        lines.append("")

    return "\n".join(lines)


def format_labor_section(labor: Dict[str, Any], period: SnapshotPeriod) -> str:
    """Format labor hours section."""
    lines = []
    lines.append("## 2. Labor Hours & Consumption")
    lines.append("")

    # Summary
    total_hours = 0
    if not labor['labor'].empty and 'hours' in labor['labor'].columns:
        total_hours = labor['labor']['hours'].sum()

    lines.append("### Summary by Source")
    lines.append("")
    lines.append(f"| Source | Records | Hours |")
    lines.append(f"|--------|---------|-------|")

    for avail in labor['availability']:
        source_hours = "-"
        if avail.source == 'ProjectSight' and not labor['projectsight'].empty:
            if 'hours' in labor['projectsight'].columns:
                source_hours = f"{labor['projectsight']['hours'].sum():,.0f}"
        elif avail.source == 'TBM' and not labor['tbm'].empty:
            if 'hours' in labor['tbm'].columns:
                source_hours = f"{labor['tbm']['hours'].sum():,.0f}"
        elif avail.source == 'WeeklyReports' and not labor['weekly_reports'].empty:
            if 'hours' in labor['weekly_reports'].columns:
                source_hours = f"{labor['weekly_reports']['hours'].sum():,.0f}"

        lines.append(f"| {avail.source} | {avail.record_count:,} | {source_hours} |")

    lines.append(f"| **Total** | {len(labor['labor']):,} | {total_hours:,.0f} |")
    lines.append("")

    # Hours by company (top 20)
    if not labor['labor'].empty and 'hours' in labor['labor'].columns:
        lines.append("### Hours by Company (Top 20)")
        lines.append("")

        company_col = 'dim_company_id' if 'dim_company_id' in labor['labor'].columns else 'company'
        if company_col in labor['labor'].columns:
            by_company = labor['labor'].groupby(company_col)['hours'].sum().sort_values(ascending=False)

            lines.append("| Company | Hours | % of Total |")
            lines.append("|---------|-------|------------|")

            for company, hours in by_company.head(20).items():
                pct = (hours / total_hours * 100) if total_hours > 0 else 0
                lines.append(f"| {company} | {hours:,.0f} | {pct:.1f}% |")

            if len(by_company) > 20:
                other_hours = by_company.iloc[20:].sum()
                other_pct = (other_hours / total_hours * 100) if total_hours > 0 else 0
                lines.append(f"| (Other {len(by_company) - 20} companies) | {other_hours:,.0f} | {other_pct:.1f}% |")
            lines.append("")

    return "\n".join(lines)


def _normalize_outcome(outcome_series: pd.Series) -> pd.Series:
    """Normalize outcome values to PASS/FAIL/PARTIAL/OTHER."""
    outcome_map = {
        'PASS': 'PASS', 'Pass': 'PASS', 'pass': 'PASS', 'PASSED': 'PASS',
        'Accepted': 'PASS', 'ACCEPTED': 'PASS', 'Accept': 'PASS',
        'FAIL': 'FAIL', 'Fail': 'FAIL', 'fail': 'FAIL', 'FAILED': 'FAIL',
        'Failure': 'FAIL', 'FAILURE': 'FAIL', 'Rejected': 'FAIL',
        'PARTIAL': 'PARTIAL', 'Partial': 'PARTIAL', 'partial': 'PARTIAL',
        'Conditional': 'PARTIAL', 'CONDITIONAL': 'PARTIAL',
    }
    return outcome_series.map(
        lambda x: outcome_map.get(str(x).strip(), 'OTHER') if pd.notna(x) else 'UNKNOWN'
    )


def format_quality_section(quality: Dict[str, Any]) -> str:
    """Format quality metrics section."""
    lines = []
    lines.append("## 3. Quality Metrics & Issues")
    lines.append("")

    # Summary
    lines.append("### Summary")
    lines.append("")
    lines.append(f"| Source | Inspections | Pass | Fail | Partial |")
    lines.append(f"|--------|-------------|------|------|---------|")

    for source_name, source_df in [('RABA', quality['raba']), ('PSI', quality['psi'])]:
        if source_df.empty:
            lines.append(f"| {source_name} | 0 | - | - | - |")
        else:
            total = len(source_df)
            if 'outcome_normalized' in source_df.columns:
                pass_count = (source_df['outcome_normalized'] == 'PASS').sum()
                fail_count = (source_df['outcome_normalized'] == 'FAIL').sum()
                partial_count = (source_df['outcome_normalized'] == 'PARTIAL').sum()
                lines.append(f"| {source_name} | {total:,} | {pass_count:,} | {fail_count:,} | {partial_count:,} |")
            else:
                lines.append(f"| {source_name} | {total:,} | - | - | - |")

    combined = quality['inspections']
    if not combined.empty:
        total = len(combined)
        if 'outcome_normalized' in combined.columns:
            pass_count = (combined['outcome_normalized'] == 'PASS').sum()
            fail_count = (combined['outcome_normalized'] == 'FAIL').sum()
            partial_count = (combined['outcome_normalized'] == 'PARTIAL').sum()
            lines.append(f"| **Combined** | {total:,} | {pass_count:,} | {fail_count:,} | {partial_count:,} |")
    lines.append("")

    # Failures by location
    if not combined.empty and 'outcome_normalized' in combined.columns:
        failures = combined[combined['outcome_normalized'] == 'FAIL']

        if not failures.empty:
            lines.append("### Failures by Location")
            lines.append("")

            loc_col = 'dim_location_id' if 'dim_location_id' in failures.columns else 'building_level'
            if loc_col in failures.columns:
                by_location = failures.groupby(loc_col).size().sort_values(ascending=False)

                lines.append("| Location | Failures |")
                lines.append("|----------|----------|")

                for loc, count in by_location.head(15).items():
                    lines.append(f"| {loc} | {count:,} |")
                lines.append("")

            # Failures by company
            lines.append("### Failures by Company")
            lines.append("")

            company_col = 'dim_company_id' if 'dim_company_id' in failures.columns else 'company'
            if company_col in failures.columns:
                by_company = failures.groupby(company_col).size().sort_values(ascending=False)

                lines.append("| Company | Failures |")
                lines.append("|---------|----------|")

                for company, count in by_company.head(15).items():
                    lines.append(f"| {company} | {count:,} |")
                lines.append("")

    return "\n".join(lines)


def format_narratives_section(narratives: Dict[str, Any]) -> str:
    """Format narrative statements section."""
    lines = []
    lines.append("## 4. Narrative Statements")
    lines.append("")

    statements = narratives['statements']

    if statements.empty:
        lines.append("*No statements with dates in this period.*")
        lines.append("")
        return "\n".join(lines)

    # Summary by category
    lines.append("### Summary by Category")
    lines.append("")

    if 'category' in statements.columns:
        by_category = statements['category'].value_counts()

        lines.append("| Category | Count |")
        lines.append("|----------|-------|")

        for cat, count in by_category.items():
            lines.append(f"| {cat} | {count:,} |")
        lines.append("")

    # Helper to format dates cleanly
    def fmt_date(dt):
        if pd.isna(dt):
            return "Unknown date"
        if hasattr(dt, 'strftime'):
            return dt.strftime('%Y-%m-%d')
        return str(dt).split(' ')[0]

    # Statements with impact
    if 'impact_days' in statements.columns:
        with_impact = statements[statements['impact_days'].notna()]

        if not with_impact.empty:
            lines.append("### Statements with Schedule Impact")
            lines.append("")

            for _, row in with_impact.iterrows():
                event_date = fmt_date(row.get('event_date'))
                category = row.get('category', 'Unknown')
                impact = row.get('impact_days', 0)
                statement = row.get('statement_text', row.get('text', ''))[:200]
                source = row.get('filename', row.get('source_file', 'Unknown source'))

                lines.append(f"- **{event_date}** [{category}] ({impact:+.0f} days)")
                lines.append(f"  > {statement}")
                lines.append(f"  > *Source: {source}*")
                lines.append("")

    # Delay statements
    if 'category' in statements.columns:
        delay_statements = statements[statements['category'].str.lower().str.contains('delay', na=False)]

        if not delay_statements.empty:
            lines.append("### Delay-Related Statements")
            lines.append("")

            for _, row in delay_statements.head(10).iterrows():
                event_date = fmt_date(row.get('event_date'))
                statement = row.get('statement_text', row.get('text', ''))[:300]
                justification = row.get('delay_justified', 'UNKNOWN')

                lines.append(f"- **{event_date}** [{justification}]")
                lines.append(f"  > {statement}")
                lines.append("")

    return "\n".join(lines)


def format_availability_section(schedule: Dict, labor: Dict, quality: Dict, narratives: Dict) -> str:
    """Format data availability section."""
    lines = []
    lines.append("## 5. Data Availability")
    lines.append("")
    lines.append("| Source | Records | Date Range | Notes |")
    lines.append("|--------|---------|------------|-------|")

    def get_date_range_str(avail) -> str:
        if avail.date_range:
            return f"{avail.date_range[0]} to {avail.date_range[1]}"
        return "-"

    def get_notes_str(avail) -> str:
        return "; ".join(avail.coverage_notes[:2]) if avail.coverage_notes else "-"

    # Schedule
    avail = schedule['availability']
    lines.append(f"| P6 Schedule | {avail.record_count:,} | {get_date_range_str(avail)} | {get_notes_str(avail)} |")

    # Quality
    for avail in quality['availability']:
        lines.append(f"| {avail.source} | {avail.record_count:,} | {get_date_range_str(avail)} | {get_notes_str(avail)} |")

    # Labor
    for avail in labor['availability']:
        lines.append(f"| {avail.source} | {avail.record_count:,} | {get_date_range_str(avail)} | {get_notes_str(avail)} |")

    # Narratives
    avail = narratives['availability']
    lines.append(f"| Narratives | {avail.record_count:,} | {get_date_range_str(avail)} | {get_notes_str(avail)} |")

    lines.append("")
    return "\n".join(lines)


def generate_report(period: SnapshotPeriod) -> str:
    """Generate consolidated snapshot report."""
    print(f"Loading data for period {period.label}...")

    # Load all data
    print("  Loading schedule data...")
    schedule = load_schedule_data(period)

    print("  Loading labor data...")
    labor = load_labor_data(period)

    print("  Loading quality data...")
    quality = load_quality_data(period)

    print("  Loading narrative data...")
    narratives = load_narrative_data(period)

    # Build report
    lines = []

    # Header
    lines.append(f"# Snapshot Report: {period.label}")
    lines.append("")
    lines.append(f"**Period:** {period.start_data_date} to {period.end_data_date} ({period.duration_days} days)")
    lines.append(f"**Schedule Type:** {period.schedule_type}")
    lines.append(f"**File IDs:** {period.start_file_id} → {period.end_file_id}")
    lines.append(f"**Generated:** {date.today().isoformat()}")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Sections
    lines.append(format_schedule_section(schedule, period))
    lines.append(format_labor_section(labor, period))
    lines.append(format_quality_section(quality))
    lines.append(format_narratives_section(narratives))
    lines.append(format_availability_section(schedule, labor, quality, narratives))

    return "\n".join(lines)


def consolidate_snapshot(period: SnapshotPeriod, dry_run: bool = False) -> Path:
    """Generate and save consolidated report for a snapshot period."""
    report = generate_report(period)

    if dry_run:
        print("\n--- DRY RUN OUTPUT ---")
        print(report[:2000])
        print("...")
        print(f"\n[Total length: {len(report):,} characters]")
        return None

    output_dir = get_output_dir(period)
    output_path = output_dir / "consolidated_data.md"

    output_path.write_text(report)
    print(f"\nReport saved to: {output_path}")

    return output_path


def main():
    parser = argparse.ArgumentParser(description="Generate consolidated snapshot report")
    parser.add_argument(
        '--file-id',
        type=int,
        help='Generate report for period ending at this file_id'
    )
    parser.add_argument(
        '--data-date',
        help='Generate report for period ending at or near this date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--latest',
        action='store_true',
        help='Generate report for latest period'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Generate reports for all periods'
    )
    parser.add_argument(
        '--last',
        type=int,
        metavar='N',
        help='Generate reports for last N periods'
    )
    parser.add_argument(
        '--list',
        action='store_true',
        help='List available periods'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview output without saving'
    )

    args = parser.parse_args()

    if args.list:
        from scripts.integrated_analysis.snapshot_reports.data_loaders.base import list_periods
        list_periods()
        return

    all_periods = get_all_snapshot_periods()

    if args.all:
        print(f"Generating {len(all_periods)} reports...")
        for period in all_periods:
            try:
                consolidate_snapshot(period, dry_run=args.dry_run)
            except Exception as e:
                print(f"  Error for {period.label}: {e}")
    elif args.last:
        periods = all_periods[-args.last:]
        print(f"Generating {len(periods)} reports...")
        for period in periods:
            try:
                consolidate_snapshot(period, dry_run=args.dry_run)
            except Exception as e:
                print(f"  Error for {period.label}: {e}")
    elif args.file_id:
        period = get_snapshot_period(file_id=args.file_id)
        if period:
            consolidate_snapshot(period, dry_run=args.dry_run)
        else:
            print(f"No period found ending at file_id {args.file_id}")
    elif args.data_date:
        period = get_snapshot_period(data_date=args.data_date)
        if period:
            consolidate_snapshot(period, dry_run=args.dry_run)
        else:
            print(f"No period found near date {args.data_date}")
    elif args.latest:
        period = all_periods[-1] if all_periods else None
        if period:
            consolidate_snapshot(period, dry_run=args.dry_run)
        else:
            print("No periods found")
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
