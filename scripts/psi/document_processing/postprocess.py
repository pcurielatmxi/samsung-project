"""
Postprocessing wrapper for PSI pipeline integration.

Adapts the existing postprocess_psi.py normalization logic for per-record
processing within the centralized pipeline.
"""

import sys
from pathlib import Path
from typing import Dict, Any

# Add project root to path for imports
_project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from scripts.quality.postprocess.shared_normalization import (
    normalize_date,
    normalize_role,
    normalize_inspection_type,
)
from scripts.quality.postprocess.location_parser import parse_location


def process_record(input_data: Dict[str, Any], source_path: Path) -> Dict[str, Any]:
    """
    Process a single PSI record.

    This function is called by the pipeline's script stage for each file.

    Args:
        input_data: The format stage output (content + metadata)
        source_path: Original source file path (PDF)

    Returns:
        Normalized record with parsed location, dates, and standardized fields
    """
    metadata = input_data.get('metadata', {})
    content = input_data.get('content', {})

    # Extract inspection_id from source filename
    inspection_id = source_path.stem

    # Parse location into structured components
    location_parsed = parse_location(content.get('location'))

    # Normalize parties if present
    normalized_parties = []
    parties = content.get('parties_involved', [])
    if parties:
        for party in parties:
            if isinstance(party, dict):
                normalized_parties.append({
                    'name': party.get('name'),
                    'role': normalize_role(party.get('role')),
                    'company': party.get('company'),
                })
            elif isinstance(party, str):
                normalized_parties.append({
                    'name': party,
                    'role': None,
                    'company': None,
                })

    # Normalize issues if present
    normalized_issues = []
    issues = content.get('issues', [])
    if issues:
        for issue in issues:
            if isinstance(issue, dict):
                normalized_issues.append({
                    'description': issue.get('description'),
                    'severity': issue.get('severity'),
                })
            elif isinstance(issue, str):
                normalized_issues.append({
                    'description': issue,
                    'severity': None,
                })

    # Build normalized content
    normalized_content = {
        # Identification
        'inspection_id': inspection_id,

        # Dates
        'report_date': content.get('report_date'),
        'report_date_normalized': normalize_date(content.get('report_date')),

        # Inspection type
        'inspection_type': content.get('inspection_type'),
        'inspection_type_normalized': normalize_inspection_type(content.get('inspection_type')),

        # Location fields
        'location_raw': content.get('location'),
        'building': location_parsed['building'],
        'level': location_parsed['level'],
        'area': location_parsed['area'],
        'grid': location_parsed['grid'],
        'location_id': location_parsed['location_id'],

        # Results
        'summary': content.get('summary'),
        'outcome': content.get('outcome'),
        'failure_reason': content.get('failure_reason'),

        # Follow-up
        'reinspection_required': content.get('reinspection_required'),
        'corrective_action': content.get('corrective_action'),

        # Details
        'deficiency_count': content.get('deficiency_count'),
        'issues': normalized_issues,
        'parties_involved': normalized_parties,
    }

    # Build output with updated metadata
    return {
        'metadata': {
            **metadata,
            'inspection_id': inspection_id,
            'postprocess_version': '2.0',
        },
        'content': normalized_content,
    }
