"""
Schema registry mapping file names to their Pydantic schemas.

This registry enables automatic schema lookup based on file name patterns
and provides a central reference for all output data schemas.
"""

from typing import Type, Dict, Optional
from pathlib import Path
from pydantic import BaseModel

from .dimensions import DimLocation, DimCompany, DimTrade, DimCSISection
from .mappings import MapCompanyAliases, MapCompanyLocation
from .quality import QCInspectionConsolidated, PsiConsolidated, RabaConsolidated
from .tbm import TbmFiles, TbmWorkEntries, TbmWorkEntriesEnriched
from .ncr import NcrConsolidated
from .projectsight import ProjectSightLaborEntries
from .bridge_tables import AffectedRoomsBridge


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

    # Quality data (RABA has CSI columns, PSI doesn't yet)
    'raba_consolidated.csv': RabaConsolidated,
    'psi_consolidated.csv': PsiConsolidated,

    # TBM data
    'tbm_files.csv': TbmFiles,
    'work_entries.csv': TbmWorkEntries,
    'work_entries_enriched.csv': TbmWorkEntriesEnriched,

    # NCR data
    'ncr_consolidated.csv': NcrConsolidated,

    # ProjectSight labor
    'labor_entries.csv': ProjectSightLaborEntries,

    # Bridge tables
    'affected_rooms_bridge.csv': AffectedRoomsBridge,
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
    },
    'projectsight': {
        'ncr_consolidated.csv': NcrConsolidated,
        'labor_entries.csv': ProjectSightLaborEntries,
    },
    'integrated_analysis/bridge_tables': {
        'affected_rooms_bridge.csv': AffectedRoomsBridge,
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
        source: Source directory name (e.g., 'raba', 'tbm', 'projectsight')

    Returns:
        Dict mapping file names to schemas for that source
    """
    return SOURCE_FILES.get(source, {})
