"""
Source configuration for dimension coverage analysis.

This module defines the mapping between data sources and their
column names for dimension lookups.

Each source configuration specifies:
- path: Relative path from PROCESSED_DATA_DIR
- location_id_col: Column containing dim_location_id FK
- location_type_col: Column containing location_type (if present in source)
- company_id_col: Column containing dim_company_id FK (None if N/A)
- company_name_col: Raw company name column for unresolved tracking
- csi_col: Column containing CSI section code
- enrichment_scripts: Dict of dimension -> script path for actionable recommendations
"""

from typing import Dict, Any, Optional, List

# Source configurations for fact tables
# Each config maps column names for dimension lookups
SOURCE_CONFIGS: Dict[str, Dict[str, Any]] = {
    'P6': {
        'path': 'primavera/p6_task_taxonomy.csv',
        'location_id_col': 'dim_location_id',
        'location_type_col': 'location_type',  # P6 has this column directly
        'company_id_col': None,  # P6 schedule doesn't have company
        'company_name_col': None,
        'csi_col': 'csi_section',
        'enrichment_scripts': {
            'location': 'scripts/primavera/derive/generate_task_taxonomy.py',
            'csi': 'scripts/integrated_analysis/add_csi_to_p6_tasks.py',
        },
    },
    'RABA': {
        'path': 'raba/raba_consolidated.csv',
        'location_id_col': 'dim_location_id',
        'location_type_col': None,  # Must lookup from dim_location
        'company_id_col': 'dim_company_id',
        'company_name_col': 'contractor',
        'csi_col': 'csi_section',
        'enrichment_scripts': {
            'location': 'scripts/raba/document_processing/consolidate.py',
            'company': 'scripts/raba/document_processing/consolidate.py',
            'csi': 'scripts/raba/document_processing/consolidate.py',
            'company_aliases': 'data/processed/integrated_analysis/mappings/map_company_aliases.csv',
        },
    },
    'PSI': {
        'path': 'psi/psi_consolidated.csv',
        'location_id_col': 'dim_location_id',
        'location_type_col': None,  # Must lookup from dim_location
        'company_id_col': 'dim_company_id',
        'company_name_col': 'contractor',
        'csi_col': 'csi_section',
        'enrichment_scripts': {
            'location': 'scripts/psi/document_processing/consolidate.py',
            'company': 'scripts/psi/document_processing/consolidate.py',
            'csi': 'scripts/psi/document_processing/consolidate.py',
            'company_aliases': 'data/processed/integrated_analysis/mappings/map_company_aliases.csv',
        },
    },
    'TBM': {
        'path': 'tbm/work_entries_enriched.csv',
        'location_id_col': 'dim_location_id',
        'location_type_col': None,  # Must lookup from dim_location
        'company_id_col': 'dim_company_id',
        'company_name_col': 'company',
        'csi_col': 'csi_section',
        'enrichment_scripts': {
            'location': 'scripts/integrated_analysis/enrich_with_dimensions.py',
            'company': 'scripts/integrated_analysis/enrich_with_dimensions.py',
            'csi': 'scripts/integrated_analysis/add_csi_to_tbm.py',
        },
    },
    'ProjectSight': {
        'path': 'projectsight/labor_entries_enriched.csv',
        'location_id_col': 'dim_location_id',
        'location_type_col': None,  # Must lookup from dim_location
        'company_id_col': 'dim_company_id',
        'company_name_col': 'company_name',
        'csi_col': 'csi_section',
        'enrichment_scripts': {
            'location': 'scripts/integrated_analysis/enrich_with_dimensions.py',
            'company': 'scripts/integrated_analysis/enrich_with_dimensions.py',
            'csi': 'scripts/integrated_analysis/add_csi_to_projectsight.py',
        },
    },
    'NCR': {
        'path': 'projectsight/ncr_consolidated.csv',
        'location_id_col': 'dim_location_id',
        'location_type_col': None,  # Must lookup from dim_location
        'company_id_col': 'dim_company_id',
        'company_name_col': 'responsible_contractor',
        'csi_col': 'csi_section',
        'enrichment_scripts': {
            'location': None,  # NCR lacks location data in source
            'company': 'scripts/projectsight/process/consolidate_ncr.py',
            'csi': 'scripts/projectsight/process/consolidate_ncr.py',
        },
    },
}


# Location types in order from most to least specific
# Used for display ordering in reports
LOCATION_TYPE_ORDER = [
    'ROOM',
    'STAIR',
    'ELEVATOR',
    'GRIDLINE',
    'LEVEL',
    'BUILDING',
    'AREA',
    'SITE',
    'UNDEFINED',
]


# Coverage thresholds for reporting
THRESHOLDS = {
    'good': 95.0,      # >= 95% is good (green)
    'warning': 80.0,   # >= 80% is warning (yellow)
    # < 80% is poor (red)
}
