#!/usr/bin/env python3
"""
Pipeline Registry - Source configurations and phase definitions.

Defines all data sources and their scripts for each pipeline phase.
The daily_refresh orchestrator uses this registry to run the pipeline.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class Phase(Enum):
    """Pipeline phases in execution order."""
    PREFLIGHT = "preflight"
    PARSE = "parse"
    SCRAPE = "scrape"
    CONSOLIDATE = "consolidate"
    VALIDATE = "validate"
    COMMIT = "commit"


@dataclass
class SourceConfig:
    """Configuration for a data source in the pipeline."""

    name: str
    """Unique identifier for this source (e.g., 'tbm', 'raba')."""

    # Parse phase (file-based incremental parsing)
    parse_module: Optional[str] = None
    """Python module path for parsing (e.g., 'scripts.tbm.process.parse_tbm_daily_plans')."""

    parse_args: list[str] = field(default_factory=list)
    """Arguments to pass to parse module (e.g., ['--incremental'])."""

    # Scrape phase (web-based, manifest-tracked)
    scrape_module: Optional[str] = None
    """Python module path for scraping (e.g., 'scripts.raba.process.scrape_raba_individual')."""

    scrape_args: list[str] = field(default_factory=list)
    """Arguments to pass to scrape module (e.g., ['--headless'])."""

    # Consolidate phase (dimension enrichment + output split)
    consolidate_module: str = ""
    """Python module path for consolidation (required for all sources)."""

    consolidate_args: list[str] = field(default_factory=list)
    """Arguments to pass to consolidate module."""

    # Output files
    fact_table: str = ""
    """Path relative to processed/ for main fact table (e.g., 'tbm/work_entries.csv')."""

    data_quality_table: Optional[str] = None
    """Path relative to processed/ for data quality table (e.g., 'tbm/work_entries_data_quality.csv')."""

    # Additional output files (for sources that produce multiple tables)
    additional_outputs: list[str] = field(default_factory=list)
    """Additional output files relative to processed/ (e.g., for fieldwire's 4 tables)."""

    # Schema validation
    schema_name: Optional[str] = None
    """Pydantic schema class name for validation (e.g., 'TBMWorkEntry')."""

    # Description
    description: str = ""
    """Human-readable description of this source."""


# =============================================================================
# Dimension table builders (run in PREFLIGHT if --rebuild-dimensions)
# =============================================================================

DIMENSION_BUILDERS = [
    {
        'name': 'dim_location',
        'module': 'scripts.integrated_analysis.dimensions.build_dim_location',
        'output': 'integrated_analysis/dim_location.csv',
        'description': 'Building + Level + Grid bounds location dimension',
    },
    {
        'name': 'dim_company',
        'module': 'scripts.integrated_analysis.dimensions.build_company_dimension',
        'output': 'integrated_analysis/dim_company.csv',
        'description': 'Company dimension with canonical names',
    },
    {
        'name': 'dim_csi_section',
        'module': 'scripts.integrated_analysis.dimensions.build_dim_csi_section',
        'output': 'integrated_analysis/dim_csi_section.csv',
        'description': 'CSI MasterFormat section codes',
    },
]

# Required dimension files (checked in PREFLIGHT)
REQUIRED_DIMENSIONS = [
    'integrated_analysis/dim_location.csv',
    'integrated_analysis/dim_company.csv',
    'integrated_analysis/dim_csi_section.csv',
]


# =============================================================================
# Source configurations
# =============================================================================

SOURCES: list[SourceConfig] = [
    # -------------------------------------------------------------------------
    # TBM - Toolbox Meeting daily plans
    # -------------------------------------------------------------------------
    SourceConfig(
        name='tbm',
        description='TBM daily work plans from subcontractors',
        parse_module='scripts.tbm.process.parse_tbm_daily_plans',
        parse_args=['--incremental'],
        scrape_module=None,
        consolidate_module='scripts.tbm.process.consolidate_tbm',
        fact_table='tbm/work_entries.csv',
        data_quality_table='tbm/work_entries_data_quality.csv',
        schema_name='TBMWorkEntry',
    ),

    # -------------------------------------------------------------------------
    # RABA + PSI - Quality inspections (combined output)
    # -------------------------------------------------------------------------
    SourceConfig(
        name='raba_psi',
        description='RABA + PSI quality inspection reports',
        parse_module=None,  # Parsing happens via document_processor
        scrape_module='scripts.raba.process.scrape_raba_individual',
        scrape_args=['--headless'],
        consolidate_module='scripts.raba.document_processing.consolidate',
        fact_table='raba/raba_psi_consolidated.csv',
        data_quality_table='raba/raba_psi_data_quality.csv',
        schema_name='QualityInspection',
    ),

    # -------------------------------------------------------------------------
    # PSI scraper (runs separately but output combined with RABA)
    # -------------------------------------------------------------------------
    SourceConfig(
        name='psi',
        description='PSI quality inspection scraper',
        parse_module=None,
        scrape_module='scripts.psi.process.scrape_psi_reports',
        scrape_args=['--headless'],
        consolidate_module=None,  # Consolidated with RABA
        fact_table='',  # No separate output
        data_quality_table=None,
    ),

    # -------------------------------------------------------------------------
    # ProjectSight Labor - Daily report labor hours
    # -------------------------------------------------------------------------
    SourceConfig(
        name='projectsight_labor',
        description='ProjectSight daily report labor hours',
        parse_module='scripts.projectsight.process.parse_labor_from_json',
        parse_args=['--incremental'],
        scrape_module=None,  # Manual download for now
        consolidate_module='scripts.projectsight.process.consolidate_labor',
        fact_table='projectsight/labor_entries.csv',
        data_quality_table='projectsight/labor_entries_data_quality.csv',
        schema_name='ProjectSightLabor',
    ),

    # -------------------------------------------------------------------------
    # ProjectSight NCR - Non-conformance records
    # -------------------------------------------------------------------------
    SourceConfig(
        name='projectsight_ncr',
        description='ProjectSight NCR/QOR/SOR records',
        parse_module=None,  # Parsed from export
        scrape_module=None,
        consolidate_module='scripts.projectsight.process.consolidate_ncr',
        fact_table='projectsight/ncr_consolidated.csv',
        data_quality_table='projectsight/ncr_data_quality.csv',
        schema_name='NCRRecord',
    ),

    # -------------------------------------------------------------------------
    # Primavera P6 - Schedule taxonomy
    # -------------------------------------------------------------------------
    SourceConfig(
        name='primavera',
        description='Primavera P6 schedule task taxonomy',
        parse_module='scripts.primavera.process.batch_process_xer',
        parse_args=['--incremental'],
        scrape_module=None,
        consolidate_module='scripts.primavera.derive.generate_task_taxonomy',
        consolidate_args=[],
        fact_table='primavera/p6_task_taxonomy.csv',
        data_quality_table='primavera/p6_task_taxonomy_data_quality.csv',
        schema_name=None,  # No schema yet
    ),

    # -------------------------------------------------------------------------
    # Fieldwire - TBM/QC/Progress tracking
    # -------------------------------------------------------------------------
    SourceConfig(
        name='fieldwire',
        description='Fieldwire TBM audit and progress tracking',
        parse_module=None,
        scrape_module=None,  # Manual export
        consolidate_module='scripts.fieldwire.process.generate_powerbi_tables',
        fact_table='fieldwire/fieldwire_combined.csv',
        data_quality_table=None,  # No data quality columns currently
        additional_outputs=[
            'fieldwire/fieldwire_comments.csv',
            'fieldwire/fieldwire_checklists.csv',
            'fieldwire/fieldwire_related_tasks.csv',
        ],
        schema_name=None,
    ),

    # -------------------------------------------------------------------------
    # Quality Workbook - Yates + SECAI QC inspections (Excel-based)
    # -------------------------------------------------------------------------
    SourceConfig(
        name='quality_workbook',
        description='Yates + SECAI QC inspection workbooks',
        parse_module='scripts.quality.process.process_quality_inspections',
        parse_args=[],
        scrape_module=None,
        consolidate_module='scripts.quality.document_processing.consolidate',
        consolidate_args=[],
        fact_table='quality/qc_inspections_enriched.csv',
        data_quality_table='quality/qc_inspections_data_quality.csv',
        additional_outputs=[
            'quality/enriched/combined_qc_inspections.csv',
            'quality/enriched/yates_qc_inspections.csv',
            'quality/enriched/secai_qc_inspections.csv',
        ],
        schema_name=None,
    ),
]


# =============================================================================
# Post-consolidation steps (run after all sources consolidate)
# =============================================================================

POST_CONSOLIDATION_STEPS = [
    {
        'name': 'p6_csi',
        'module': 'scripts.integrated_analysis.add_csi_to_p6_tasks',
        'description': 'Add CSI sections to P6 task taxonomy',
        'depends_on': ['primavera'],
    },
    {
        'name': 'qc_workbook_enrich',
        'module': 'scripts.quality.process.enrich_qc_workbooks',
        'description': 'Enrich QC inspections with location and affected rooms',
        'depends_on': ['quality_workbook'],
    },
    {
        'name': 'affected_rooms_bridge',
        'module': 'scripts.integrated_analysis.generate_affected_rooms_bridge',
        'description': 'Generate affected_rooms_bridge table',
        'depends_on': ['tbm', 'raba_psi'],
    },
]


# =============================================================================
# Helper functions
# =============================================================================

def get_source_by_name(name: str) -> Optional[SourceConfig]:
    """Get a source configuration by name."""
    for source in SOURCES:
        if source.name == name:
            return source
    return None


def get_sources_for_phase(phase: Phase) -> list[SourceConfig]:
    """Get all sources that participate in a given phase."""
    if phase == Phase.PARSE:
        return [s for s in SOURCES if s.parse_module]
    elif phase == Phase.SCRAPE:
        return [s for s in SOURCES if s.scrape_module]
    elif phase == Phase.CONSOLIDATE:
        return [s for s in SOURCES if s.consolidate_module]
    elif phase == Phase.VALIDATE:
        return [s for s in SOURCES if s.fact_table]
    elif phase == Phase.COMMIT:
        return [s for s in SOURCES if s.fact_table]
    return []


def get_all_output_files() -> list[str]:
    """Get all output files from all sources."""
    files = []
    for source in SOURCES:
        if source.fact_table:
            files.append(source.fact_table)
        if source.data_quality_table:
            files.append(source.data_quality_table)
        files.extend(source.additional_outputs)
    return files
