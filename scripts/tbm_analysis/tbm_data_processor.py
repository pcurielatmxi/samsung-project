#!/usr/bin/env python3
"""
TBM Data Processor
==================

Processes Fieldwire data dump into structured report content.
All calculations are deterministic - no AI generation here.

Output: JSON file with all report sections ready for styling.

Usage:
    python tbm_data_processor.py --date 12-19-25 --contractors "Berg,MK Marlow"
"""

import argparse
import json
import re
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


# =============================================================================
# CONFIGURATION
# =============================================================================

TBM_ANALYSIS_DIR = Path("/mnt/c/Users/pdcur/OneDrive - MXI/MXI - General/Field Tracking/TBM Analysis")
FIELDWIRE_DUMP_DIR = TBM_ANALYSIS_DIR / "Fieldwire Data Dump"

LABOR_RATE_PER_HOUR = 65
WORKING_DAYS_PER_MONTH = 26

CONTRACTOR_GROUPS = {
    "Berg & MK Marlow": ["Berg", "MK Marlow"],
    "Axios": ["Axios"],
}


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class TaskObservation:
    """A single field observation from an inspector."""
    inspector: str
    text: str


@dataclass
class ChecklistStatus:
    """Parsed checklist values indicating worker activity status."""
    active: bool = False
    passive: bool = False
    obstructed: bool = False
    meeting: bool = False
    no_manpower: bool = False
    inspector_initials: Optional[str] = None
    date: Optional[str] = None


@dataclass
class FieldwireTask:
    """Represents a single Fieldwire task with all extracted data."""
    id: str
    title: str
    category: str
    company: str
    level: str
    gridpoint: str
    direct_manpower: float
    indirect_manpower: float
    tbm_manpower: float
    observations: list
    checklist: ChecklistStatus
    photo_urls: list = field(default_factory=list)

    @property
    def total_verified(self) -> float:
        return self.direct_manpower + self.indirect_manpower

    @property
    def is_marker_task(self) -> bool:
        """START/END marker tasks track totals, not individual locations."""
        title_upper = self.title.upper()
        return 'START' in title_upper or 'END' in title_upper

    @property
    def status_classification(self) -> str:
        """Classify the task status based on checklist and observations."""
        if self.checklist.no_manpower:
            return "NO MANPOWER"
        if self.checklist.active:
            return "ACTIVE"
        if self.checklist.passive:
            # Check observations for more specific classification
            obs_text = ' '.join(o.text.lower() for o in self.observations)
            if 'waiting' in obs_text:
                if 'material' in obs_text:
                    return "WAITING - MATERIALS"
                if 'approval' in obs_text or 'authorization' in obs_text:
                    return "WAITING - APPROVAL"
                if 'equipment' in obs_text:
                    return "WAITING - EQUIPMENT"
                return "WAITING"
            if 'idle' in obs_text or 'standing' in obs_text:
                return "IDLE TIME"
            if 'setup' in obs_text or 'paperwork' in obs_text or 'safety' in obs_text:
                return "PREPARATION TIME"
            return "PARTIAL IDLE"
        if self.checklist.obstructed:
            return "OBSTRUCTED"
        if self.checklist.meeting:
            return "MEETING"
        return "UNKNOWN"


@dataclass
class ContractorMetrics:
    """Aggregated metrics for a single contractor."""
    name: str
    total_tasks: int
    total_planned: float
    tbm_presence: float  # From START marker
    total_verified: float
    total_direct: float
    total_indirect: float
    accuracy: float
    idle_locations: int
    idle_workers: float
    idle_cost: float
    zero_verification_count: int
    high_verification_count: int


@dataclass
class IdleObservation:
    """An idle time observation with cost calculation."""
    task_id: str
    location: str
    level: str
    company: str
    status: str
    idle_workers: float
    cost: float
    inspector: str
    observation_text: str
    analysis: str
    photo_urls: list = field(default_factory=list)


@dataclass
class ZeroVerificationLocation:
    """A location with TBM commitment but zero verified workers."""
    floor: str
    location: str
    company: str
    tbm_planned: float
    verified: float = 0


@dataclass
class HighVerificationLocation:
    """A location with >100% verification (more verified than planned)."""
    company: str
    location: str
    floor: str
    tbm_planned: float
    verified: float
    accuracy: float


@dataclass
class ReportContent:
    """Complete report content structure."""
    # Metadata
    report_date: str
    report_date_formatted: str
    contractor_group: str
    contractors: list

    # Summary stats
    total_tasks: int
    total_contractors: int
    total_floors: int

    # KPIs
    lpi: float
    lpi_target: float
    dilr: float
    dilr_target: float
    idle_time_hours: float
    idle_time_cost: float
    labor_rate: float
    tbm_compliance_rate: float
    zero_verification_rate: float
    zero_verification_count: int
    total_locations: int
    monthly_projection: float

    # TBM Summary table
    contractor_metrics: list
    total_planned: float
    total_tbm_presence: float
    total_verified: float
    total_accuracy: float

    # Category 2: Zero verification
    zero_verification_locations: list

    # Category 3: Idle observations
    idle_observations: list
    idle_summary_by_contractor: list

    # Category 4: High verification
    high_verification_locations: list

    # Category 5: Key findings (calculated bullet points)
    key_findings: list

    # Task details for chart generation
    task_details: list = field(default_factory=list)

    # Narrative placeholders - to be filled by Claude
    narratives: dict = field(default_factory=dict)


# =============================================================================
# PARSING FUNCTIONS
# =============================================================================

def find_latest_fieldwire_dump() -> Path:
    """Find the most recent Fieldwire data dump CSV."""
    csv_files = list(FIELDWIRE_DUMP_DIR.glob("Samsung_-_Progress_Tracking_TBM_Analysis_Data_Dump_*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No Fieldwire CSV files found in {FIELDWIRE_DUMP_DIR}")
    csv_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return csv_files[0]


def parse_float(val) -> float:
    """Parse float, handling NaN and empty values."""
    try:
        if val is None or str(val).lower() == 'nan' or str(val) == '':
            return 0.0
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def extract_gridpoint(tier1, title: str) -> str:
    """Extract gridpoint from tier1 or title."""
    tier1_str = str(tier1) if tier1 else ''
    if tier1_str and tier1_str != 'nan' and not tier1_str.startswith('FAB'):
        return tier1_str
    match = re.search(r'\b([A-N])-?(\d+)\b', str(title))
    if match:
        return f"{match.group(1)}-{match.group(2)}"
    return tier1_str if tier1_str and tier1_str != 'nan' else 'Unknown'


# Changelog patterns to filter out (these are system-generated, not field narratives)
CHANGELOG_PATTERNS = [
    'Changed title to',
    'Changed status to',
    'Changed Direct Manpower to',
    'Changed Indirect Manpower to',
    'Changed TBM Manpower to',
    'Changed Activity Name to',
    'Changed location to',
    'Changed category to',
    'Changed Company to',
    'Changed plan to',
    'Changed end date to',
    'Changed start date to',
    'Changed Scope Category to',
    'Set Direct Manpower to',
    'Set Indirect Manpower to',
    'Set TBM Manpower to',
    'Set Company to',
    'Removed value from',
    'Added tag',
]


def is_changelog_message(text: str) -> bool:
    """Check if a message is a changelog entry (not a field narrative)."""
    # Check for photo hyperlinks
    if 'HYPERLINK' in text:
        return True

    # Extract the content after the inspector name
    if ': ' in text:
        content = text.split(': ', 1)[1] if len(text.split(': ', 1)) > 1 else text
    else:
        content = text

    # Check against changelog patterns
    for pattern in CHANGELOG_PATTERNS:
        if content.startswith(pattern):
            return True

    return False


def extract_observations(row: dict) -> list:
    """Extract inspector observations from Message columns.

    Filters out changelog entries (Changed title to, Set Manpower to, etc.)
    and keeps only actual field narratives from inspectors.
    """
    observations = []
    for i in range(1, 63):
        col = f'Message{i}'
        val = str(row.get(col, ''))
        if val and val != 'nan' and len(val) > 10:
            # Skip changelog and system messages
            if is_changelog_message(val):
                continue

            # Extract inspector name and message
            if ': ' in val:
                parts = val.split(': ', 1)
                observations.append(TaskObservation(
                    inspector=parts[0].strip(),
                    text=parts[1].strip() if len(parts) > 1 else val
                ))
            else:
                observations.append(TaskObservation(inspector='Unknown', text=val))
    return observations


def extract_photo_urls(row: dict) -> list:
    """Extract photo URLs from Message columns containing HYPERLINK formulas."""
    photo_urls = []
    url_pattern = r'https://[^\s"]+\.(?:jpeg|jpg|png|gif)'

    for i in range(1, 63):
        col = f'Message{i}'
        val = str(row.get(col, ''))
        if val and val != 'nan' and 'HYPERLINK' in val:
            urls = re.findall(url_pattern, val, re.IGNORECASE)
            photo_urls.extend(urls)

    return photo_urls


def extract_checklist(row: dict) -> ChecklistStatus:
    """Extract checklist status from row."""
    status = ChecklistStatus()

    for i in range(1, 19):
        col = f'Checklist{i}'
        val = str(row.get(col, ''))
        if val and val != 'nan' and 'Yes:' in val:
            val_lower = val.lower()
            if 'active' in val_lower:
                status.active = True
            elif 'passive' in val_lower:
                status.passive = True
            elif 'obstructed' in val_lower:
                status.obstructed = True
            elif 'meeting' in val_lower:
                status.meeting = True
            elif 'no manpower' in val_lower:
                status.no_manpower = True

            # Extract inspector initials
            if '(' in val and ')' in val:
                status.inspector_initials = val[val.find('(')+1:val.find(')')]
            if ' - ' in val:
                status.date = val.split(' - ')[-1]

    return status


def parse_fieldwire_csv(csv_path: Path, target_date: str, contractors: list) -> list:
    """Parse and filter Fieldwire CSV dump."""
    df = pd.read_csv(csv_path, encoding='utf-16', sep='\t', skiprows=3)
    df.columns = [str(c).replace(' ', '').strip() for c in df.columns]

    # Filter by status
    df = df[df['Status'] == 'TBM']

    # Parse and filter by date
    df['Startdate'] = pd.to_datetime(df['Startdate'], errors='coerce')

    if len(target_date) == 8:  # MM-DD-YY
        parts = target_date.split('-')
        target_dt = datetime(2000 + int(parts[2]), int(parts[0]), int(parts[1]))
    else:
        target_dt = datetime.strptime(target_date, '%Y-%m-%d')

    target_str = target_dt.strftime('%Y-%m-%d')
    df = df[df['Startdate'].dt.strftime('%Y-%m-%d') == target_str]

    # Filter by contractors
    df = df[df['Company'].isin(contractors)]

    # Convert to task objects
    tasks = []
    for _, row in df.iterrows():
        row_dict = row.to_dict()
        task = FieldwireTask(
            id=str(row_dict.get('ID', '')),
            title=str(row_dict.get('Title', '')),
            category=str(row_dict.get('Category', '')),
            company=str(row_dict.get('Company', '')),
            level=str(row_dict.get('Level', '')),
            gridpoint=extract_gridpoint(row_dict.get('Tier1'), row_dict.get('Title', '')),
            direct_manpower=parse_float(row_dict.get('DirectManpower')),
            indirect_manpower=parse_float(row_dict.get('IndirectManpower')),
            tbm_manpower=parse_float(row_dict.get('TBMManpower')),
            observations=extract_observations(row_dict),
            checklist=extract_checklist(row_dict),
            photo_urls=extract_photo_urls(row_dict),
        )
        tasks.append(task)

    return tasks


# =============================================================================
# METRICS CALCULATION
# =============================================================================

def calculate_contractor_metrics(tasks: list, contractor: str) -> ContractorMetrics:
    """Calculate all metrics for a single contractor."""
    contractor_tasks = [t for t in tasks if t.company == contractor]
    location_tasks = [t for t in contractor_tasks if not t.is_marker_task]

    # Get TBM presence from START marker
    tbm_presence = 0
    for t in contractor_tasks:
        if 'START' in t.title.upper() and t.tbm_manpower > 0:
            tbm_presence = t.tbm_manpower
            break

    total_planned = sum(t.tbm_manpower for t in contractor_tasks)
    total_verified = sum(t.total_verified for t in location_tasks)
    total_direct = sum(t.direct_manpower for t in location_tasks)
    total_indirect = sum(t.indirect_manpower for t in location_tasks)

    # Idle calculations
    idle_tasks = [t for t in location_tasks if t.checklist.passive or t.checklist.no_manpower]
    idle_workers = sum(t.indirect_manpower if t.indirect_manpower > 0 else t.total_verified for t in idle_tasks)

    # Zero verification
    zero_ver = [t for t in location_tasks if t.tbm_manpower > 0 and t.total_verified == 0]

    # High verification
    high_ver = [t for t in location_tasks if t.tbm_manpower > 0 and t.total_verified > t.tbm_manpower]

    accuracy = (total_verified / total_planned * 100) if total_planned > 0 else 0

    return ContractorMetrics(
        name=contractor,
        total_tasks=len(location_tasks),
        total_planned=total_planned,
        tbm_presence=tbm_presence if tbm_presence > 0 else total_planned,
        total_verified=total_verified,
        total_direct=total_direct,
        total_indirect=total_indirect,
        accuracy=round(accuracy, 0),
        idle_locations=len(idle_tasks),
        idle_workers=idle_workers,
        idle_cost=idle_workers * LABOR_RATE_PER_HOUR,
        zero_verification_count=len(zero_ver),
        high_verification_count=len(high_ver),
    )


def build_idle_observations(tasks: list) -> list:
    """Build list of idle observations with cost calculations."""
    observations = []

    for task in tasks:
        if task.is_marker_task:
            continue
        if not (task.checklist.passive or task.checklist.no_manpower):
            continue

        status = task.status_classification
        idle_workers = task.indirect_manpower if task.indirect_manpower > 0 else task.total_verified
        cost = idle_workers * LABOR_RATE_PER_HOUR

        # Get observation text
        obs_text = task.observations[0].text if task.observations else ""
        inspector = task.observations[0].inspector if task.observations else task.checklist.inspector_initials or "Unknown"

        # Build analysis text
        analysis = f"{status} - {idle_workers:.0f} workers × 1 hour × ${LABOR_RATE_PER_HOUR}/hr = ${cost:.0f} waste."
        if 'material' in obs_text.lower():
            analysis += " Workers waiting for materials."
        elif 'approval' in obs_text.lower():
            analysis += " Workers waiting for approval."
        elif 'equipment' in obs_text.lower():
            analysis += " Workers waiting for equipment."

        observations.append(IdleObservation(
            task_id=task.id,
            location=task.gridpoint,
            level=f"Level {task.level}" if task.level and task.level != 'nan' else "",
            company=task.company,
            status=status,
            idle_workers=idle_workers,
            cost=cost,
            inspector=inspector,
            observation_text=obs_text,
            analysis=analysis,
            photo_urls=task.photo_urls,
        ))

    return observations


def build_zero_verification_list(tasks: list) -> list:
    """Build list of zero verification locations."""
    locations = []

    for task in tasks:
        if task.is_marker_task:
            continue
        if task.tbm_manpower > 0 and task.total_verified == 0:
            locations.append(ZeroVerificationLocation(
                floor=f"{task.level}F" if task.level and task.level != 'nan' else "",
                location=task.gridpoint,
                company=task.company,
                tbm_planned=task.tbm_manpower,
            ))

    return locations


def build_high_verification_list(tasks: list) -> list:
    """Build list of high verification locations (>100% accuracy)."""
    locations = []

    for task in tasks:
        if task.is_marker_task:
            continue
        if task.tbm_manpower > 0 and task.total_verified > task.tbm_manpower:
            accuracy = (task.total_verified / task.tbm_manpower) * 100
            locations.append(HighVerificationLocation(
                company=task.company,
                location=f"{task.gridpoint} ({task.level}F)" if task.level and task.level != 'nan' else task.gridpoint,
                floor=f"{task.level}F" if task.level and task.level != 'nan' else "",
                tbm_planned=task.tbm_manpower,
                verified=task.total_verified,
                accuracy=round(accuracy, 0),
            ))

    return sorted(locations, key=lambda x: x.accuracy, reverse=True)


def build_key_findings(metrics: list, idle_obs: list, zero_ver: list, high_ver: list, total_lpi: float) -> list:
    """Build key findings bullet points."""
    findings = []

    # Overall accuracy
    findings.append(f"OVERALL ACCURACY: {total_lpi:.0f}%")

    # Per-contractor accuracy
    for m in metrics:
        findings.append(f"{m.name}: {m.accuracy:.0f}% accuracy with {m.total_planned:.0f} planned, {m.total_verified:.0f} verified")

    # Idle time
    total_idle_cost = sum(o.cost for o in idle_obs)
    total_idle_workers = sum(o.idle_workers for o in idle_obs)
    findings.append(f"IDLE TIME COST: ${total_idle_cost:,.0f} in documented waste ({total_idle_workers:.0f} idle worker-hours)")

    # Per-contractor idle
    for m in metrics:
        contractor_idle = [o for o in idle_obs if o.company == m.name]
        if contractor_idle:
            cost = sum(o.cost for o in contractor_idle)
            findings.append(f"{m.name} idle: ${cost:,.0f} ({len(contractor_idle)} observations)")

    # Zero verification
    total_locations = sum(m.total_tasks for m in metrics)
    findings.append(f"{len(zero_ver)} locations with 0% verification ({len(zero_ver)/total_locations*100:.0f}% of all locations)")

    # High verification
    findings.append(f"{len(high_ver)} locations showed >100% verification (over-deployment or worker mobility)")

    # Monthly projection
    monthly = total_idle_cost * WORKING_DAYS_PER_MONTH / 1000
    findings.append(f"Monthly idle cost projection: ${monthly:.0f}K")

    return findings


# =============================================================================
# MAIN PROCESSING
# =============================================================================

def process_fieldwire_data(target_date: str, contractors: list, contractor_group: str, csv_path: Optional[Path] = None) -> ReportContent:
    """Process Fieldwire data into complete report content."""

    # Find CSV if not provided
    if csv_path is None:
        csv_path = find_latest_fieldwire_dump()

    # Parse tasks
    tasks = parse_fieldwire_csv(csv_path, target_date, contractors)
    location_tasks = [t for t in tasks if not t.is_marker_task]

    # Calculate per-contractor metrics
    contractor_metrics = [calculate_contractor_metrics(tasks, c) for c in contractors]

    # Totals
    total_planned = sum(m.total_planned for m in contractor_metrics)
    total_tbm_presence = sum(m.tbm_presence for m in contractor_metrics)
    total_verified = sum(m.total_verified for m in contractor_metrics)
    total_direct = sum(m.total_direct for m in contractor_metrics)
    total_indirect = sum(m.total_indirect for m in contractor_metrics)
    total_accuracy = (total_verified / total_planned * 100) if total_planned > 0 else 0

    # Build detailed lists
    idle_observations = build_idle_observations(tasks)
    zero_verification = build_zero_verification_list(tasks)
    high_verification = build_high_verification_list(tasks)

    # KPIs
    lpi = total_accuracy
    dilr = (total_indirect / (total_direct + total_indirect) * 100) if (total_direct + total_indirect) > 0 else 0
    total_idle_hours = sum(o.idle_workers for o in idle_observations)
    total_idle_cost = sum(o.cost for o in idle_observations)
    total_locations = len(location_tasks)
    locations_with_verification = len([t for t in location_tasks if t.total_verified > 0])

    # Idle summary by contractor
    idle_summary = []
    for c in contractors:
        c_idle = [o for o in idle_observations if o.company == c]
        idle_summary.append({
            'contractor': c,
            'observations': len(c_idle),
            'worker_hours': sum(o.idle_workers for o in c_idle),
            'cost': sum(o.cost for o in c_idle),
        })

    # Key findings
    key_findings = build_key_findings(contractor_metrics, idle_observations, zero_verification, high_verification, lpi)

    # Task details for chart
    task_details = []
    for task in location_tasks:
        if task.tbm_manpower > 0 or task.total_verified > 0:
            task_details.append({
                'company': task.company,
                'location': task.gridpoint,
                'level': task.level if task.level and task.level != 'nan' else '',
                'planned': task.tbm_manpower,
                'actual': task.total_verified,
            })

    # Format date
    if len(target_date) == 8:
        parts = target_date.split('-')
        dt = datetime(2000 + int(parts[2]), int(parts[0]), int(parts[1]))
    else:
        dt = datetime.strptime(target_date, '%Y-%m-%d')

    # Build report content
    return ReportContent(
        report_date=target_date,
        report_date_formatted=dt.strftime('%B %d, %Y'),
        contractor_group=contractor_group,
        contractors=contractors,

        total_tasks=len(location_tasks),
        total_contractors=len(contractors),
        total_floors=len(set(t.level for t in location_tasks if t.level and t.level != 'nan')),

        lpi=round(lpi, 0),
        lpi_target=80,
        dilr=round(dilr, 0),
        dilr_target=25,
        idle_time_hours=total_idle_hours,
        idle_time_cost=total_idle_cost,
        labor_rate=LABOR_RATE_PER_HOUR,
        tbm_compliance_rate=round((locations_with_verification / total_locations * 100) if total_locations > 0 else 0, 0),
        zero_verification_rate=round((len(zero_verification) / total_locations * 100) if total_locations > 0 else 0, 0),
        zero_verification_count=len(zero_verification),
        total_locations=total_locations,
        monthly_projection=round(total_idle_cost * WORKING_DAYS_PER_MONTH / 1000, 0),

        contractor_metrics=[asdict(m) for m in contractor_metrics],
        total_planned=total_planned,
        total_tbm_presence=total_tbm_presence,
        total_verified=total_verified,
        total_accuracy=round(total_accuracy, 0),

        zero_verification_locations=[asdict(z) for z in zero_verification],
        idle_observations=[asdict(o) for o in idle_observations],
        idle_summary_by_contractor=idle_summary,
        high_verification_locations=[asdict(h) for h in high_verification],
        key_findings=key_findings,
        task_details=task_details,

        narratives={},
    )


def save_report_content(content: ReportContent, output_path: Path):
    """Save report content to JSON file."""
    with open(output_path, 'w') as f:
        json.dump(asdict(content), f, indent=2, default=str)


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='Process Fieldwire data into report content')
    parser.add_argument('--date', required=True, help='Report date (MM-DD-YY or YYYY-MM-DD)')
    parser.add_argument('--contractors', required=True, help='Comma-separated contractor names or group name')
    parser.add_argument('--fieldwire-csv', help='Path to Fieldwire CSV (default: latest)')
    parser.add_argument('--output', help='Output JSON path (default: auto)')

    args = parser.parse_args()

    # Resolve contractors
    if args.contractors in CONTRACTOR_GROUPS:
        contractors = CONTRACTOR_GROUPS[args.contractors]
        contractor_group = args.contractors
    else:
        contractors = [c.strip() for c in args.contractors.split(',')]
        contractor_group = ' & '.join(contractors)

    print(f"TBM Data Processor")
    print(f"==================")
    print(f"Date: {args.date}")
    print(f"Contractors: {contractors}")

    # Process data
    csv_path = Path(args.fieldwire_csv) if args.fieldwire_csv else None
    content = process_fieldwire_data(args.date, contractors, contractor_group, csv_path)

    print(f"\nMetrics:")
    print(f"  Tasks: {content.total_tasks}")
    print(f"  LPI: {content.lpi}%")
    print(f"  DILR: {content.dilr}%")
    print(f"  Idle observations: {len(content.idle_observations)}")
    print(f"  Zero verification: {content.zero_verification_count}")
    print(f"  High verification: {len(content.high_verification_locations)}")

    # Save output
    if args.output:
        output_path = Path(args.output)
    else:
        output_dir = TBM_ANALYSIS_DIR / contractor_group / args.date
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / 'report_content.json'

    save_report_content(content, output_path)
    print(f"\nSaved: {output_path}")

    return content, output_path


if __name__ == '__main__':
    main()
