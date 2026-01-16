"""
Schema registry mapping file names to their Pydantic schemas.

This registry enables automatic schema lookup based on file name patterns
and provides a central reference for all output data schemas.
"""

from typing import Type, Dict, Optional
from pathlib import Path
from pydantic import BaseModel

from .dimensions import DimLocation, DimCompany, DimTrade, DimCSISection
from .mappings import MapCompanyAliases, MapCompanyLocation, MapProjectSightTrade
from .quality import QCInspectionConsolidated, PsiConsolidated, RabaConsolidated
from .tbm import TbmFiles, TbmWorkEntries, TbmWorkEntriesEnriched
from .ncr import NcrConsolidated
from .weekly_reports import (
    WeeklyReports, KeyIssues, WorkProgressing, Procurement,
    LaborDetail, LaborDetailByCompany,
    AddendumFiles, AddendumManpower, AddendumRfiLog, AddendumSubmittalLog,
)


# Registry mapping file names to schemas
# Keys are file names (without path), values are Pydantic model classes
SCHEMA_REGISTRY: Dict[str, Type[BaseModel]] = {
    # Dimension tables
    'dim_location.csv': DimLocation,
    'dim_company.csv': DimCompany,
    'dim_trade.csv': DimTrade,
    'dim_csi_section.csv': DimCSISection,

    # Mapping tables
    'map_company_aliases.csv': MapCompanyAliases,
    'map_company_location.csv': MapCompanyLocation,
    'map_projectsight_trade.csv': MapProjectSightTrade,

    # Quality data (RABA has CSI columns, PSI doesn't yet)
    'raba_consolidated.csv': RabaConsolidated,
    'psi_consolidated.csv': PsiConsolidated,

    # TBM data
    'tbm_files.csv': TbmFiles,
    'work_entries.csv': TbmWorkEntries,
    'work_entries_enriched.csv': TbmWorkEntriesEnriched,
    'tbm_with_csi.csv': TbmWorkEntriesEnriched,

    # NCR data
    'ncr_consolidated.csv': NcrConsolidated,

    # Weekly reports
    'weekly_reports.csv': WeeklyReports,
    'key_issues.csv': KeyIssues,
    'work_progressing.csv': WorkProgressing,
    'procurement.csv': Procurement,
    'labor_detail.csv': LaborDetail,
    'labor_detail_by_company.csv': LaborDetailByCompany,

    # Addendum data
    'addendum_files.csv': AddendumFiles,
    'addendum_manpower.csv': AddendumManpower,
    'addendum_rfi_log.csv': AddendumRfiLog,
    'addendum_submittal_log.csv': AddendumSubmittalLog,
}


# Mapping of source directories to their expected files
SOURCE_FILES: Dict[str, Dict[str, Type[BaseModel]]] = {
    'integrated_analysis/dimensions': {
        'dim_location.csv': DimLocation,
        'dim_company.csv': DimCompany,
        'dim_trade.csv': DimTrade,
        'dim_csi_section.csv': DimCSISection,
    },
    'integrated_analysis/mappings': {
        'map_company_aliases.csv': MapCompanyAliases,
        'map_company_location.csv': MapCompanyLocation,
        'map_projectsight_trade.csv': MapProjectSightTrade,
    },
    'raba': {
        'raba_consolidated.csv': RabaConsolidated,
    },
    'psi': {
        'psi_consolidated.csv': PsiConsolidated,
    },
    'tbm': {
        'tbm_files.csv': TbmFiles,
        'work_entries.csv': TbmWorkEntries,
        'work_entries_enriched.csv': TbmWorkEntriesEnriched,
        'tbm_with_csi.csv': TbmWorkEntriesEnriched,
    },
    'projectsight': {
        'ncr_consolidated.csv': NcrConsolidated,
    },
    'weekly_reports': {
        'weekly_reports.csv': WeeklyReports,
        'key_issues.csv': KeyIssues,
        'work_progressing.csv': WorkProgressing,
        'procurement.csv': Procurement,
        'labor_detail.csv': LaborDetail,
        'labor_detail_by_company.csv': LaborDetailByCompany,
        'addendum_files.csv': AddendumFiles,
        'addendum_manpower.csv': AddendumManpower,
        'addendum_rfi_log.csv': AddendumRfiLog,
        'addendum_submittal_log.csv': AddendumSubmittalLog,
    },
}


def get_schema_for_file(file_path: str) -> Optional[Type[BaseModel]]:
    """
    Get the schema for a file based on its name.

    Args:
        file_path: Path to the file (can be full path or just filename)

    Returns:
        Pydantic model class or None if no schema registered
    """
    filename = Path(file_path).name
    return SCHEMA_REGISTRY.get(filename)


def get_all_schemas() -> Dict[str, Type[BaseModel]]:
    """Return all registered schemas."""
    return SCHEMA_REGISTRY.copy()


def list_registered_files() -> list:
    """Return list of all registered file names."""
    return sorted(SCHEMA_REGISTRY.keys())


def get_schemas_for_source(source: str) -> Dict[str, Type[BaseModel]]:
    """
    Get all schemas for a source directory.

    Args:
        source: Source directory name (e.g., 'raba', 'tbm', 'weekly_reports')

    Returns:
        Dict mapping file names to schemas for that source
    """
    return SOURCE_FILES.get(source, {})
