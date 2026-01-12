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
from scripts.integrated_analysis.snapshot_reports.data_loaders.dimensions import (
    get_company_lookup,
    resolve_company_id,
)


def _load_company_trade_reference() -> pd.DataFrame:
    """Load company-trade reference from dimension tables."""
    dim_dir = Settings.DERIVED_DATA_DIR / 'integrated_analysis' / 'dimensions'

    company_path = dim_dir / 'dim_company.csv'
    trade_path = dim_dir / 'dim_trade.csv'

    if not company_path.exists() or not trade_path.exists():
        return pd.DataFrame()

    companies = pd.read_csv(company_path)
    trades = pd.read_csv(trade_path)

    # Merge to get trade names
    merged = companies.merge(
        trades[['trade_id', 'trade_name']],
        left_on='primary_trade_id',
        right_on='trade_id',
        how='left'
    )

    # Select relevant columns
    result = merged[['canonical_name', 'short_code', 'tier', 'trade_name', 'notes']].copy()
    result['trade_name'] = result['trade_name'].fillna('General')

    return result


def get_output_dir(period: SnapshotPeriod) -> Path:
    """Get output directory for a snapshot report.

    Output goes to external data directory (not git-tracked):
    {WINDOWS_DATA_DIR}/processed/integrated_analysis/1_snapshot_consolidated_reports/{period_label}/
    """
    output_base = Settings.PROCESSED_DATA_DIR / 'integrated_analysis' / '1_snapshot_consolidated_reports'
    output_dir = output_base / period.label
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def format_schedule_section(schedule: Dict[str, Any], period: SnapshotPeriod, quality_pass_rates: Dict[str, float] = None) -> str:
    """Format schedule progress section with consolidated snapshot comparison."""
    lines = []
    lines.append("## 1. Schedule Progress & Delays")
    lines.append("")

    # Metric Definitions
    lines.append("### Metric Definitions")
    lines.append("")
    lines.append("| Term | Definition |")
    lines.append("|------|------------|")
    lines.append("| **Own Delay** | Days the task's finish slipped beyond its start slip. Indicates delay caused BY this task (duration growth, execution issues). |")
    lines.append("| **Inherited Delay** | Days the task's start date moved due to predecessor delays. Delay pushed TO this task from upstream. |")
    lines.append("| **Behind Schedule** | Tasks where target end date < data date AND task is not complete. |")
    lines.append("| **Negative Float** | Tasks where late finish < early finish, requiring acceleration to meet project end. |")
    lines.append("| **Critical Path** | Tasks on the longest path determining project end date (driving path). |")
    lines.append("")

    overall = schedule['overall']
    snapshots = schedule['snapshots']

    # Consolidated Progress Table
    lines.append("### Schedule Comparison")
    lines.append("")
    lines.append("| Metric | Start Snapshot | End Snapshot | Delta |")
    lines.append("|--------|----------------|--------------|-------|")

    start = snapshots.get('start', {})
    end = snapshots.get('end', {})
    delta = snapshots.get('delta', {})

    # Data Date
    lines.append(f"| Data Date | {start.get('data_date', '-')} | {end.get('data_date', '-')} | {delta.get('days', 0):+d} days |")

    # File ID
    lines.append(f"| File ID | {start.get('file_id', '-')} | {end.get('file_id', '-')} | - |")

    # Total Tasks
    lines.append(f"| Total Tasks | {start.get('task_count', 0):,} | {end.get('task_count', 0):,} | {delta.get('task_count', 0):+,} |")

    # % Complete
    lines.append(f"| % Complete | {start.get('pct_complete', 0):.1f}% | {end.get('pct_complete', 0):.1f}% | {delta.get('pct_complete', 0):+.1f}% |")

    # Tasks Completed
    lines.append(f"| Tasks Completed | {start.get('completed_tasks', 0):,} | {end.get('completed_tasks', 0):,} | {delta.get('completed_tasks', 0):+,} |")

    # Critical Path Tasks
    lines.append(f"| Critical Path Tasks | {start.get('critical_path_tasks', 0):,} | {end.get('critical_path_tasks', 0):,} | {delta.get('critical_path_tasks', 0):+,} |")

    # Project End Date
    start_end = start.get('project_end_date', '-') or '-'
    end_end = end.get('project_end_date', '-') or '-'
    slip_days = delta.get('project_slip_days', 0)
    slip_str = f"{slip_days:+d} days" if slip_days != 0 else "0 days"
    lines.append(f"| Project End Date | {start_end} | {end_end} | {slip_str} |")

    lines.append("")

    # Schedule Health Summary
    lines.append("### Schedule Health")
    lines.append("")
    lines.append("| Metric | Count | Description |")
    lines.append("|--------|-------|-------------|")
    lines.append(f"| Tasks Behind Schedule | {overall.tasks_behind_schedule:,} | Incomplete tasks past their target end date |")
    lines.append(f"| Tasks with Negative Float | {overall.tasks_with_negative_float:,} | Tasks requiring acceleration to meet project end date |")
    lines.append(f"| Critical Path Tasks | {overall.critical_path_tasks:,} | Tasks on the driving path |")
    lines.append("")

    # Helper to format dates as MM-DD-YY
    def fmt_date_short(dt):
        if pd.isna(dt):
            return '-'
        if hasattr(dt, 'strftime'):
            return dt.strftime('%m-%d-%y')
        try:
            return pd.Timestamp(dt).strftime('%m-%d-%y')
        except Exception:
            return '-'

    # Behind Schedule Tasks (explicit list)
    tasks_end = schedule.get('tasks_end', pd.DataFrame())
    if not tasks_end.empty and overall.tasks_behind_schedule > 0:
        data_date = pd.Timestamp(end.get('data_date'))
        behind_mask = (
            (tasks_end['status_code'] != 'TK_Complete') &
            (pd.to_datetime(tasks_end['target_end_date']) < data_date)
        )
        behind_tasks = tasks_end[behind_mask].copy()

        if not behind_tasks.empty:
            lines.append("### Tasks Behind Schedule")
            lines.append("")
            lines.append("*Tasks not complete past their target end date:*")
            lines.append("")
            lines.append("| Task Code | Task Name | Trade | Target End | Days Behind | % Complete |")
            lines.append("|-----------|-----------|-------|------------|-------------|------------|")

            behind_tasks['days_behind'] = (data_date - pd.to_datetime(behind_tasks['target_end_date'])).dt.days
            behind_tasks = behind_tasks.sort_values('days_behind', ascending=False)

            for _, row in behind_tasks.head(10).iterrows():
                task_code = str(row.get('task_code', '-'))
                task_name = str(row.get('task_name', '-'))[:35]
                if len(str(row.get('task_name', ''))) > 35:
                    task_name += "..."
                trade = str(row.get('trade_name', '-'))[:12] if pd.notna(row.get('trade_name')) else '-'
                tgt_end = fmt_date_short(row.get('target_end_date'))
                days_behind = row.get('days_behind', 0)
                pct = f"{row['phys_complete_pct']:.0f}%" if pd.notna(row.get('phys_complete_pct')) else '-'
                lines.append(f"| {task_code} | {task_name} | {trade} | {tgt_end} | {days_behind:,}d | {pct} |")

            if len(behind_tasks) > 10:
                lines.append(f"| ... | *({len(behind_tasks) - 10} more tasks)* | | | | |")
            lines.append("")

    # Critical Path Tasks (top 10)
    if not tasks_end.empty and 'driving_path_flag' in tasks_end.columns:
        critical_tasks = tasks_end[tasks_end['driving_path_flag'] == 'Y'].copy()

        if not critical_tasks.empty:
            lines.append("### Critical Path Tasks (Driving Path)")
            lines.append("")
            lines.append(f"*{len(critical_tasks)} tasks on the driving path. Top 10 by target end date:*")
            lines.append("")
            lines.append("| Task Code | Task Name | Trade | Status | Target End | Float |")
            lines.append("|-----------|-----------|-------|--------|------------|-------|")

            # Sort by target end date
            if 'target_end_date' in critical_tasks.columns:
                critical_tasks = critical_tasks.sort_values('target_end_date')

            for _, row in critical_tasks.head(10).iterrows():
                task_code = str(row.get('task_code', '-'))
                task_name = str(row.get('task_name', '-'))[:35]
                if len(str(row.get('task_name', ''))) > 35:
                    task_name += "..."
                trade = str(row.get('trade_name', '-'))[:12] if pd.notna(row.get('trade_name')) else '-'
                status = str(row.get('status_code', '-'))
                status_short = {'TK_Complete': 'Done', 'TK_Active': 'Active', 'TK_NotStart': 'Wait'}.get(status, status[:6])
                tgt_end = fmt_date_short(row.get('target_end_date'))
                float_hr = row.get('total_float_hr_cnt', 0)
                float_str = f"{float_hr/8:.0f}d" if pd.notna(float_hr) else '-'
                lines.append(f"| {task_code} | {task_name} | {trade} | {status_short} | {tgt_end} | {float_str} |")

            lines.append("")

    # Float Distribution
    if not tasks_end.empty and 'total_float_hr_cnt' in tasks_end.columns:
        # Exclude completed tasks for float analysis
        active_tasks = tasks_end[tasks_end['status_code'] != 'TK_Complete'].copy()

        if not active_tasks.empty:
            float_hrs = active_tasks['total_float_hr_cnt'].dropna()
            float_days = float_hrs / 8  # Convert hours to days

            lines.append("### Float Distribution")
            lines.append("")
            lines.append("*Distribution of schedule float (buffer) across incomplete tasks:*")
            lines.append("")
            lines.append("| Float Range | Tasks | % of Active | Risk Level |")
            lines.append("|-------------|-------|-------------|------------|")

            # Define float bands
            bands = [
                ('< 0 days (Critical)', float_days < 0, 'HIGH'),
                ('0-5 days (Near-critical)', (float_days >= 0) & (float_days <= 5), 'MEDIUM'),
                ('6-15 days', (float_days > 5) & (float_days <= 15), 'LOW'),
                ('16-30 days', (float_days > 15) & (float_days <= 30), 'LOW'),
                ('> 30 days', float_days > 30, 'LOW'),
            ]

            total_active = len(active_tasks)
            for label, mask, risk in bands:
                count = mask.sum()
                pct = (count / total_active * 100) if total_active > 0 else 0
                lines.append(f"| {label} | {count:,} | {pct:.1f}% | {risk} |")

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

    # Delay-causing tasks (Top 10) - Using schedule slippage methodology
    if not schedule['delay_tasks'].empty:
        lines.append("### Top 10 Delay-Causing Tasks")
        lines.append("")

        df = schedule['delay_tasks']

        # Check if we have slippage metrics (own_delay) or just float (first snapshot)
        has_slippage_metrics = 'own_delay_days' in df.columns

        if has_slippage_metrics:
            lines.append("Tasks with highest own_delay (delay caused by the task itself, not inherited from predecessors):")
        else:
            lines.append("Tasks with most negative float (first snapshot - no comparison available):")
        lines.append("")

        # Helper to format dates as MM-DD-YY
        def fmt_date(dt):
            if pd.isna(dt):
                return '-'
            if hasattr(dt, 'strftime'):
                return dt.strftime('%m-%d-%y')
            try:
                return pd.Timestamp(dt).strftime('%m-%d-%y')
            except Exception:
                return '-'

        if has_slippage_metrics:
            # Enhanced table with slippage metrics
            lines.append("| Task Code | Task Name | Trade | Status | Own Delay | Inherited | Category | Tgt Start | Act Start | Tgt End | Act End | Plan Dur |")
            lines.append("|-----------|-----------|-------|--------|-----------|-----------|----------|-----------|-----------|---------|---------|----------|")

            for _, row in df.head(10).iterrows():
                task_code = str(row.get('task_code', '-'))
                task_name = str(row.get('task_name', '-'))[:30]
                if len(str(row.get('task_name', ''))) > 30:
                    task_name += "..."
                trade = str(row.get('trade_name', '-'))[:10] if pd.notna(row.get('trade_name')) else '-'

                # Status (abbreviated)
                status = str(row.get('status_code', '-'))
                status_short = {'TK_Complete': 'Done', 'TK_Active': 'Active', 'TK_NotStart': 'Wait'}.get(status, status[:6])

                # Slippage metrics
                own_delay = row.get('own_delay_days')
                own_delay_str = f"{own_delay:+.0f}d" if pd.notna(own_delay) else '-'

                inherited = row.get('inherited_delay_days')
                inherited_str = f"{inherited:+.0f}d" if pd.notna(inherited) else '-'

                category = str(row.get('delay_category', '-'))[:12] if pd.notna(row.get('delay_category')) else '-'

                # Date columns (MM-DD-YY format)
                tgt_start = fmt_date(row.get('target_start_date'))
                act_start = fmt_date(row.get('act_start_date'))
                tgt_end = fmt_date(row.get('target_end_date'))
                act_end = fmt_date(row.get('act_end_date'))

                # Planned duration (in hours)
                plan_dur = row.get('target_drtn_hr_cnt')
                plan_dur_str = f"{plan_dur:.0f}h" if pd.notna(plan_dur) else '-'

                lines.append(f"| {task_code} | {task_name} | {trade} | {status_short} | {own_delay_str} | {inherited_str} | {category} | {tgt_start} | {act_start} | {tgt_end} | {act_end} | {plan_dur_str} |")
        else:
            # Fallback table (no slippage metrics)
            lines.append("| Task Code | Task Name | Trade | % | Float | Tgt Start | Act Start | Tgt End | Act End | Late Start | Late End | Plan Dur |")
            lines.append("|-----------|-----------|-------|---|-------|-----------|-----------|---------|---------|------------|----------|----------|")

            for _, row in df.head(10).iterrows():
                task_code = str(row.get('task_code', '-'))
                task_name = str(row.get('task_name', '-'))[:35]
                if len(str(row.get('task_name', ''))) > 35:
                    task_name += "..."
                trade = str(row.get('trade_name', '-'))[:12] if pd.notna(row.get('trade_name')) else '-'
                pct = f"{row['phys_complete_pct']:.0f}" if pd.notna(row.get('phys_complete_pct')) else '-'
                float_hr = f"{row['total_float_hr_cnt']:.0f}h" if pd.notna(row.get('total_float_hr_cnt')) else '-'

                tgt_start = fmt_date(row.get('target_start_date'))
                act_start = fmt_date(row.get('act_start_date'))
                tgt_end = fmt_date(row.get('target_end_date'))
                act_end = fmt_date(row.get('act_end_date'))
                late_start = fmt_date(row.get('late_start_date'))
                late_end = fmt_date(row.get('late_end_date'))
                plan_dur = row.get('target_drtn_hr_cnt')
                plan_dur_str = f"{plan_dur:.0f}h" if pd.notna(plan_dur) else '-'

                lines.append(f"| {task_code} | {task_name} | {trade} | {pct} | {float_hr} | {tgt_start} | {act_start} | {tgt_end} | {act_end} | {late_start} | {late_end} | {plan_dur_str} |")

        lines.append("")

    # All Others by Trade (aggregated delay tasks beyond top 10)
    delay_by_trade = schedule.get('delay_by_trade', pd.DataFrame())
    if not delay_by_trade.empty:
        lines.append("### Other Delay Tasks by Trade")
        lines.append("")
        lines.append("Remaining delay-causing tasks aggregated by trade:")
        lines.append("")

        # Check if we have slippage metrics or float metrics
        has_own_delay = 'total_own_delay' in delay_by_trade.columns

        if has_own_delay:
            lines.append("| Trade | Tasks | Total Own Delay | Avg Own Delay | Avg % Complete |")
            lines.append("|-------|-------|-----------------|---------------|----------------|")

            for _, row in delay_by_trade.iterrows():
                trade = str(row.get('trade_name', '-'))
                count = int(row.get('task_count', 0))
                total_delay = f"{row['total_own_delay']:.0f}d" if pd.notna(row.get('total_own_delay')) else '-'
                avg_delay = f"{row['avg_own_delay']:.1f}d" if pd.notna(row.get('avg_own_delay')) else '-'
                avg_pct = f"{row['avg_complete_pct']:.1f}%" if pd.notna(row.get('avg_complete_pct')) else '-'
                lines.append(f"| {trade} | {count:,} | {total_delay} | {avg_delay} | {avg_pct} |")
        else:
            lines.append("| Trade | Tasks | Worst Float | Avg Float | Avg % Complete |")
            lines.append("|-------|-------|-------------|-----------|----------------|")

            for _, row in delay_by_trade.iterrows():
                trade = str(row.get('trade_name', '-'))
                count = int(row.get('task_count', 0))
                worst = f"{row['worst_float_hr']:.0f}h" if pd.notna(row.get('worst_float_hr')) else '-'
                avg_float = f"{row['avg_float_hr']:.0f}h" if pd.notna(row.get('avg_float_hr')) else '-'
                avg_pct = f"{row['avg_complete_pct']:.1f}%" if pd.notna(row.get('avg_complete_pct')) else '-'
                lines.append(f"| {trade} | {count:,} | {worst} | {avg_float} | {avg_pct} |")

        lines.append("")

    # Investigation Checklist for top delay tasks
    if not schedule['delay_tasks'].empty:
        df = schedule['delay_tasks']
        lines.append("### Investigation Checklist")
        lines.append("")
        lines.append("*Recommended documentation to review for root cause analysis:*")
        lines.append("")

        for _, row in df.head(3).iterrows():
            task_code = str(row.get('task_code', '-'))
            task_name = str(row.get('task_name', '-'))[:40]
            own_delay = row.get('own_delay_days', row.get('total_float_hr_cnt', 0))
            delay_str = f"{own_delay:+.0f}d" if pd.notna(own_delay) else "?"
            trade = str(row.get('trade_name', '-'))[:15] if pd.notna(row.get('trade_name')) else '-'

            lines.append(f"**{task_code}** ({task_name}) — {delay_str} delay")
            lines.append(f"- [ ] Review weekly reports for {trade} activities during period")
            lines.append(f"- [ ] Check RABA/PSI inspections for related location")
            lines.append(f"- [ ] Review RFI/submittal log for design issues")
            lines.append(f"- [ ] Identify responsible contractor from labor data")
            lines.append("")

    return "\n".join(lines)


def format_labor_section(labor: Dict[str, Any], period: SnapshotPeriod) -> str:
    """Format labor hours section.

    ProjectSight is the authoritative source for labor hours quantity.
    TBM provides location allocation context (% by building/level per company).
    Weekly Reports are excluded due to data quality concerns.
    """
    lines = []
    lines.append("## 2. Labor Hours & Consumption")
    lines.append("")

    ps_df = labor['projectsight']
    tbm_df = labor['tbm']

    # === PROJECTSIGHT SECTION: Source of truth for hours ===
    lines.append("### ProjectSight Hours")
    lines.append("")

    if ps_df.empty or 'hours' not in ps_df.columns:
        lines.append("*No ProjectSight data available for this period.*")
        lines.append("")
    else:
        ps_hours = ps_df['hours'].sum()
        ps_records = len(ps_df)

        lines.append(f"**Total:** {ps_hours:,.0f} hours from {ps_records:,} records")
        lines.append("")

        # Hours by company
        company_col = 'dim_company_id' if 'dim_company_id' in ps_df.columns else 'company'
        if company_col in ps_df.columns:
            by_company = ps_df.groupby(company_col)['hours'].sum().sort_values(ascending=False)

            lines.append("| Company | Hours | % of Total |")
            lines.append("|---------|-------|------------|")

            for company_id, hours in by_company.head(15).items():
                pct = (hours / ps_hours * 100) if ps_hours > 0 else 0
                company_name = resolve_company_id(company_id) if company_col == 'dim_company_id' else str(company_id)
                lines.append(f"| {company_name} | {hours:,.0f} | {pct:.1f}% |")

            if len(by_company) > 15:
                other_hours = by_company.iloc[15:].sum()
                other_pct = (other_hours / ps_hours * 100) if ps_hours > 0 else 0
                lines.append(f"| *(Other {len(by_company) - 15} companies)* | {other_hours:,.0f} | {other_pct:.1f}% |")

            lines.append("")

    # === TBM SECTION: Location allocation context ===
    lines.append("### TBM Location Allocation")
    lines.append("")
    lines.append("*TBM shows where each company allocated workers (% by location). Does not represent actual hours.*")
    lines.append("")

    if tbm_df.empty:
        lines.append("*No TBM data available for this period.*")
        lines.append("")
    else:
        tbm_records = len(tbm_df)
        lines.append(f"**Entries:** {tbm_records:,} work assignments")
        lines.append("")

        # Find company and location columns
        company_col = 'dim_company_id' if 'dim_company_id' in tbm_df.columns else 'company'
        location_col = None
        for col in ['building_level', 'dim_location_id', 'location', 'building']:
            if col in tbm_df.columns:
                location_col = col
                break

        if company_col in tbm_df.columns and location_col:
            # Get top companies by entry count
            company_counts = tbm_df[company_col].value_counts()
            top_companies = company_counts.head(10).index.tolist()

            lines.append("| Company | Total Entries | Top Locations (% of company's work) |")
            lines.append("|---------|---------------|-------------------------------------|")

            for company_id in top_companies:
                company_data = tbm_df[tbm_df[company_col] == company_id]
                total_entries = len(company_data)
                company_name = resolve_company_id(company_id) if company_col == 'dim_company_id' else str(company_id)

                # Get location breakdown for this company
                loc_breakdown = company_data[location_col].value_counts()
                top_locs = []
                for loc, count in loc_breakdown.head(3).items():
                    pct = (count / total_entries * 100) if total_entries > 0 else 0
                    loc_str = str(loc) if pd.notna(loc) else 'Unknown'
                    top_locs.append(f"{loc_str}: {pct:.0f}%")

                loc_summary = ", ".join(top_locs) if top_locs else "-"
                lines.append(f"| {company_name} | {total_entries:,} | {loc_summary} |")

            if len(company_counts) > 10:
                lines.append(f"| *(Other {len(company_counts) - 10} companies)* | {company_counts.iloc[10:].sum():,} | - |")

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


def format_quality_section(quality: Dict[str, Any], quality_pass_rates: Dict[str, float] = None) -> str:
    """Format quality metrics section."""
    lines = []
    lines.append("## 3. Quality Metrics & Issues")
    lines.append("")

    # Pass Rate Summary (moved from schedule section)
    if quality_pass_rates:
        curr_rate = quality_pass_rates.get('current')
        prev_rate = quality_pass_rates.get('previous')
        if curr_rate is not None:
            lines.append("### Quality Pass Rate")
            lines.append("")
            if prev_rate is not None:
                delta = curr_rate - prev_rate
                trend = "↑" if delta > 0 else "↓" if delta < 0 else "→"
                lines.append(f"**Current Period:** {curr_rate:.1f}% {trend} (Previous: {prev_rate:.1f}%, Change: {delta:+.1f}%)")
            else:
                lines.append(f"**Current Period:** {curr_rate:.1f}%")
            lines.append("")
            lines.append("*Pass rate = PASS / (PASS + FAIL + PARTIAL)*")
            lines.append("")

    # Summary
    lines.append("### Inspection Summary")
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

                for company_id, count in by_company.head(15).items():
                    # Resolve company ID to name
                    company_name = resolve_company_id(company_id) if company_col == 'dim_company_id' else str(company_id)
                    lines.append(f"| {company_name} | {count:,} |")
                lines.append("")

    return "\n".join(lines)


def format_narratives_section(narratives: Dict[str, Any]) -> str:
    """Format narrative statements section with counts and deduplication."""
    lines = []
    lines.append("## 4. Narrative Statements")
    lines.append("")
    lines.append("Statements extracted from P6 schedule narratives and weekly reports.")
    lines.append("Each statement shows: **[Source File]** Date - Statement text")
    lines.append("")

    statements = narratives['statements'].copy() if not narratives['statements'].empty else pd.DataFrame()

    if statements.empty:
        lines.append("*No statements with dates in this period.*")
        lines.append("")
        return "\n".join(lines)

    # Deduplicate statements by text content
    text_col = 'statement_text' if 'statement_text' in statements.columns else 'text'
    dedup_count = 0
    if text_col in statements.columns:
        original_count = len(statements)
        # Keep first occurrence, track duplicate sources
        statements['text_normalized'] = statements[text_col].str.strip().str.lower()
        statements = statements.drop_duplicates(subset=['text_normalized'], keep='first')
        dedup_count = original_count - len(statements)
        statements = statements.drop(columns=['text_normalized'])

    total_statements = len(statements)
    lines.append(f"**Total statements in period:** {total_statements}")
    if dedup_count > 0:
        lines.append(f"*({dedup_count} duplicate statements removed)*")
    lines.append("")

    # Summary by category (LLM-assigned during extraction)
    lines.append("### Summary by Category")
    lines.append("")
    lines.append("*Categories assigned by LLM during extraction. Review statements for accuracy.*")
    lines.append("")

    if 'category' in statements.columns:
        by_category = statements['category'].value_counts()

        lines.append("| Category | Count |")
        lines.append("|----------|-------|")

        for cat, count in by_category.items():
            lines.append(f"| {cat} | {count:,} |")
        lines.append("")

    # Summary by source file
    file_col = 'filename' if 'filename' in statements.columns else 'source_file'
    if file_col in statements.columns and 'category' in statements.columns:
        lines.append("### Summary by Source File")
        lines.append("")

        # Group by file and category
        by_file = statements.groupby(file_col)['category'].value_counts().unstack(fill_value=0)

        # Get unique categories for columns
        categories = statements['category'].unique().tolist()

        # Build header
        header = "| Source File | Total |"
        separator = "|-------------|-------|"
        for cat in categories:
            header += f" {cat} |"
            separator += "------|"
        lines.append(header)
        lines.append(separator)

        # Add rows (sorted by total count descending)
        file_totals = statements.groupby(file_col).size().sort_values(ascending=False)
        for filename in file_totals.index:
            total = file_totals[filename]
            # Truncate long filenames
            display_name = str(filename)
            if len(display_name) > 45:
                display_name = display_name[:42] + "..."
            row = f"| {display_name} | {total} |"
            for cat in categories:
                count = by_file.loc[filename, cat] if cat in by_file.columns else 0
                row += f" {count} |"
            lines.append(row)

        lines.append("")

    # Helper to format dates cleanly
    def fmt_date(dt):
        if pd.isna(dt):
            return "Unknown"
        if hasattr(dt, 'strftime'):
            return dt.strftime('%m-%d-%y')
        return str(dt).split(' ')[0]

    # Helper to get source file name (truncated)
    def get_source(row):
        source = row.get('filename', row.get('source_file', ''))
        if pd.isna(source) or not source:
            return 'Unknown'
        # Truncate long filenames
        source = str(source)
        if len(source) > 50:
            return source[:47] + "..."
        return source

    # Statements with schedule impact (Top 20)
    IMPACT_LIMIT = 20
    if 'impact_days' in statements.columns:
        with_impact = statements[statements['impact_days'].notna()].copy()
        with_impact = with_impact.sort_values('impact_days', key=abs, ascending=False)
        total_with_impact = len(with_impact)

        if not with_impact.empty:
            shown = min(IMPACT_LIMIT, total_with_impact)
            lines.append(f"### Statements with Schedule Impact (showing {shown} of {total_with_impact})")
            lines.append("")
            lines.append("*Schedule impact (days) indicates the delay or acceleration mentioned in the narrative.*")
            lines.append("")

            for _, row in with_impact.head(IMPACT_LIMIT).iterrows():
                event_date = fmt_date(row.get('event_date'))
                impact = row.get('impact_days', 0)
                statement = row.get('statement_text', row.get('text', ''))[:250]
                source = get_source(row)

                lines.append(f"- **[{source}]** {event_date} — Impact: {impact:+.0f} days")
                lines.append(f"  > {statement}")
                lines.append("")

            if total_with_impact > IMPACT_LIMIT:
                lines.append(f"*{total_with_impact - IMPACT_LIMIT} additional statements with schedule impact not shown.*")
                lines.append("")
                lines.append("<!-- LLM: To retrieve all statements with schedule impact, use:")
                lines.append("     load_narrative_data(period)['statements'][statements['impact_days'].notna()]")
                lines.append("     The full dataset is in data/processed/narratives/narrative_statements.csv -->")
                lines.append("")

    # Delay-related statements (Top 10)
    DELAY_LIMIT = 10
    if 'category' in statements.columns:
        delay_statements = statements[statements['category'].str.lower().str.contains('delay', na=False)].copy()
        total_delay = len(delay_statements)

        if not delay_statements.empty:
            shown = min(DELAY_LIMIT, total_delay)
            lines.append(f"### Delay-Related Statements (showing {shown} of {total_delay})")
            lines.append("")
            lines.append("*Statements categorized as delay-related by LLM extraction.*")
            lines.append("")

            for _, row in delay_statements.head(DELAY_LIMIT).iterrows():
                event_date = fmt_date(row.get('event_date'))
                statement = row.get('statement_text', row.get('text', ''))[:300]
                source = get_source(row)

                lines.append(f"- **[{source}]** {event_date}")
                lines.append(f"  > {statement}")
                lines.append("")

            if total_delay > DELAY_LIMIT:
                lines.append(f"*{total_delay - DELAY_LIMIT} additional delay-related statements not shown.*")
                lines.append("")
                lines.append("<!-- LLM: To retrieve all delay statements, use:")
                lines.append("     load_narrative_data(period)['statements'][statements['category'].str.contains('delay', case=False)]")
                lines.append("     The full dataset is in data/processed/narratives/narrative_statements.csv -->")
                lines.append("")

    # Other statements (Top 10 of remaining)
    OTHER_LIMIT = 10
    if 'category' in statements.columns:
        # Exclude impact and delay statements already shown
        other_mask = ~(
            (statements['impact_days'].notna() if 'impact_days' in statements.columns else False) |
            statements['category'].str.lower().str.contains('delay', na=False)
        )
        other_statements = statements[other_mask].copy()
        total_other = len(other_statements)

        if not other_statements.empty and total_other > 0:
            shown = min(OTHER_LIMIT, total_other)
            lines.append(f"### Other Statements (showing {shown} of {total_other})")
            lines.append("")
            lines.append("*Statements not categorized as delay-related or having explicit schedule impact.*")
            lines.append("")

            for _, row in other_statements.head(OTHER_LIMIT).iterrows():
                event_date = fmt_date(row.get('event_date'))
                category = row.get('category', 'Unknown')
                statement = row.get('statement_text', row.get('text', ''))[:250]
                source = get_source(row)

                lines.append(f"- **[{source}]** {event_date} — Category: {category}")
                lines.append(f"  > {statement}")
                lines.append("")

            if total_other > OTHER_LIMIT:
                lines.append(f"*{total_other - OTHER_LIMIT} additional statements not shown.*")
                lines.append("")
                lines.append("<!-- LLM: To retrieve all statements, use load_narrative_data(period)['statements']")
                lines.append("     Full dataset: data/processed/narratives/narrative_statements.csv -->")
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


def format_company_reference_section() -> str:
    """Format company-trade reference section."""
    lines = []
    lines.append("## 6. Company Reference")
    lines.append("")
    lines.append("Key contractors and their primary trades:")
    lines.append("")

    ref_df = _load_company_trade_reference()

    if ref_df.empty:
        lines.append("*Company reference data not available.*")
        lines.append("")
        return "\n".join(lines)

    # Group by tier for better organization
    tier_order = ['OWNER', 'GC', 'T1_SUB', 'T2_SUB', 'OTHER']

    lines.append("| Company | Code | Tier | Primary Trade | Notes |")
    lines.append("|---------|------|------|---------------|-------|")

    for tier in tier_order:
        tier_companies = ref_df[ref_df['tier'] == tier].sort_values('canonical_name')
        for _, row in tier_companies.iterrows():
            name = row['canonical_name']
            code = row['short_code'] if pd.notna(row['short_code']) else '-'
            trade = row['trade_name'] if pd.notna(row['trade_name']) else '-'
            notes = str(row['notes'])[:50] if pd.notna(row['notes']) else '-'
            lines.append(f"| {name} | {code} | {tier} | {trade} | {notes} |")

    lines.append("")
    return "\n".join(lines)


def _calculate_quality_pass_rate(quality: Dict[str, Any]) -> float:
    """Calculate pass rate from quality data."""
    combined = quality.get('inspections', pd.DataFrame())
    if combined.empty:
        return None

    if 'outcome_normalized' not in combined.columns:
        return None

    total = len(combined)
    if total == 0:
        return None

    passed = (combined['outcome_normalized'] == 'PASS').sum()
    return (passed / total) * 100


def generate_report(period: SnapshotPeriod, previous_period: SnapshotPeriod = None) -> str:
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

    # Calculate quality pass rates
    current_pass_rate = _calculate_quality_pass_rate(quality)

    # Try to get previous period pass rate if we have a previous period
    previous_pass_rate = None
    if previous_period:
        print("  Loading previous period quality data...")
        prev_quality = load_quality_data(previous_period)
        previous_pass_rate = _calculate_quality_pass_rate(prev_quality)

    quality_pass_rates = None
    if current_pass_rate is not None:
        quality_pass_rates = {
            'current': current_pass_rate,
            'previous': previous_pass_rate,
        }

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
    lines.append(format_schedule_section(schedule, period, quality_pass_rates))
    lines.append(format_labor_section(labor, period))
    lines.append(format_quality_section(quality, quality_pass_rates))
    lines.append(format_narratives_section(narratives))
    lines.append(format_availability_section(schedule, labor, quality, narratives))
    lines.append(format_company_reference_section())

    return "\n".join(lines)


def consolidate_snapshot(period: SnapshotPeriod, dry_run: bool = False, all_periods: List[SnapshotPeriod] = None) -> Path:
    """Generate and save consolidated report for a snapshot period.

    Args:
        period: The snapshot period to generate report for
        dry_run: If True, preview output without saving
        all_periods: Optional list of all periods (to find previous period for quality pass rate comparison)

    Returns:
        Path to saved report, or None if dry_run
    """
    # Find previous period for comparison
    previous_period = None
    if all_periods:
        try:
            idx = next(i for i, p in enumerate(all_periods) if p.label == period.label)
            if idx > 0:
                previous_period = all_periods[idx - 1]
                print(f"  Using previous period for comparison: {previous_period.label}")
        except StopIteration:
            pass

    report = generate_report(period, previous_period=previous_period)

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
                consolidate_snapshot(period, dry_run=args.dry_run, all_periods=all_periods)
            except Exception as e:
                print(f"  Error for {period.label}: {e}")
    elif args.last:
        periods = all_periods[-args.last:]
        print(f"Generating {len(periods)} reports...")
        for period in periods:
            try:
                consolidate_snapshot(period, dry_run=args.dry_run, all_periods=all_periods)
            except Exception as e:
                print(f"  Error for {period.label}: {e}")
    elif args.file_id:
        period = get_snapshot_period(file_id=args.file_id)
        if period:
            consolidate_snapshot(period, dry_run=args.dry_run, all_periods=all_periods)
        else:
            print(f"No period found ending at file_id {args.file_id}")
    elif args.data_date:
        period = get_snapshot_period(data_date=args.data_date)
        if period:
            consolidate_snapshot(period, dry_run=args.dry_run, all_periods=all_periods)
        else:
            print(f"No period found near date {args.data_date}")
    elif args.latest:
        period = all_periods[-1] if all_periods else None
        if period:
            consolidate_snapshot(period, dry_run=args.dry_run, all_periods=all_periods)
        else:
            print("No periods found")
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
