"""
Schema registry mapping file names to their Pydantic schemas.

This registry enables automatic schema lookup based on file name patterns
and provides a central reference for all output data schemas.
"""

from typing import Type, Dict, Optional
from pathlib import Path
from pydantic import BaseModel

# Dimension tables
from .dimensions import DimLocation, DimCompany, DimTrade, DimCSISection

# Mapping tables
from .mappings import MapCompanyAliases, MapCompanyLocation

# TBM
from .tbm import TbmFiles, TbmWorkEntries, TbmWorkEntriesEnriched

# ProjectSight
from .projectsight import ProjectSightLaborEntries
from .ncr import NcrConsolidated

# Fieldwire
from .fieldwire import (
    FieldwireChecklists,
    FieldwireCombined,
    FieldwireComments,
    FieldwireIdleTags,
    FieldwireRelatedTasks,
)

# Primavera
from .primavera import P6TaskTaxonomy, P6TaskTaxonomyDataQuality

# Quality (Yates/SECAI QC workbooks)
from .quality import (
    QCInspectionsEnriched,
    QCInspectionsCombined,
    QCInspectionsDataQuality,
)

# RABA/PSI (third-party quality inspections)
from .raba_psi import RabaPsiConsolidated, RabaPsiDataQuality

# SECAI NCR
from .secai_ncr import SecaiNcrConsolidated, SecaiNcrDataQuality

# Data quality tables
from .data_quality import (
    TbmWorkEntriesDataQuality,
    ProjectSightLaborEntriesDataQuality,
    ProjectSightNcrDataQuality,
)

# Bridge tables
from .bridge_tables import AffectedRoomsBridge


# Registry mapping file names to schemas
# Keys are file names (without path), values are Pydantic model classes
SCHEMA_REGISTRY: Dict[str, Type[BaseModel]] = {
    # =========================================================================
    # Dimension tables
    # =========================================================================
    'dim_location.csv': DimLocation,
    'dim_company.csv': DimCompany,
    'dim_trade.csv': DimTrade,
    'dim_csi_section.csv': DimCSISection,

    # =========================================================================
    # Mapping tables
    # =========================================================================
    'map_company_aliases.csv': MapCompanyAliases,
    'map_company_location.csv': MapCompanyLocation,

    # =========================================================================
    # TBM (Toolbox Meeting daily plans)
    # =========================================================================
    'tbm_files.csv': TbmFiles,
    'work_entries.csv': TbmWorkEntries,
    'work_entries_enriched.csv': TbmWorkEntriesEnriched,
    'work_entries_data_quality.csv': TbmWorkEntriesDataQuality,

    # =========================================================================
    # ProjectSight
    # =========================================================================
    'labor_entries.csv': ProjectSightLaborEntries,
    'labor_entries_data_quality.csv': ProjectSightLaborEntriesDataQuality,
    'ncr_consolidated.csv': NcrConsolidated,
    'ncr_data_quality.csv': ProjectSightNcrDataQuality,

    # =========================================================================
    # Fieldwire
    # =========================================================================
    'fieldwire_checklists.csv': FieldwireChecklists,
    'fieldwire_combined.csv': FieldwireCombined,
    'fieldwire_comments.csv': FieldwireComments,
    'fieldwire_related_tasks.csv': FieldwireRelatedTasks,
    'idle_tags.csv': FieldwireIdleTags,

    # =========================================================================
    # Primavera P6
    # =========================================================================
    'p6_task_taxonomy.csv': P6TaskTaxonomy,
    'p6_task_taxonomy_data_quality.csv': P6TaskTaxonomyDataQuality,

    # =========================================================================
    # Quality (Yates/SECAI QC workbooks)
    # =========================================================================
    'qc_inspections_enriched.csv': QCInspectionsEnriched,
    'qc_inspections_data_quality.csv': QCInspectionsDataQuality,
    'combined_qc_inspections.csv': QCInspectionsCombined,
    'yates_qc_inspections.csv': QCInspectionsCombined,
    'secai_qc_inspections.csv': QCInspectionsCombined,

    # =========================================================================
    # RABA/PSI (third-party quality inspections)
    # =========================================================================
    'raba_psi_consolidated.csv': RabaPsiConsolidated,
    'raba_psi_data_quality.csv': RabaPsiDataQuality,

    # =========================================================================
    # SECAI NCR Log
    # =========================================================================
    'secai_ncr_consolidated.csv': SecaiNcrConsolidated,
    'secai_ncr_data_quality.csv': SecaiNcrDataQuality,

    # =========================================================================
    # Bridge tables
    # =========================================================================
    'affected_rooms_bridge.csv': AffectedRoomsBridge,
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
