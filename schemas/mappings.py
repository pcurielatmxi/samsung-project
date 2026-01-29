"""
Mapping table schemas.

These schemas define the structure for mapping/bridge tables that connect
dimension tables to source data.

Output Location: {WINDOWS_DATA_DIR}/processed/integrated_analysis/mappings/
"""

from typing import Optional
from pydantic import BaseModel, Field


class MapCompanyAliases(BaseModel):
    """
    Company alias mapping table schema.

    File: map_company_aliases.csv
    Purpose: Resolve company name variants to canonical company_id.

    Used by: dimension_lookup.get_company_id() for fuzzy company matching.
    """

    company_id: int = Field(description="FK to dim_company")
    alias: str = Field(description="Company name variant/alias")
    source: str = Field(description="Source where alias was found (projectsight, raba, psi, manual)")


class MapCompanyLocation(BaseModel):
    """
    Company-location work distribution mapping schema.

    File: map_company_location.csv
    Purpose: Track which companies worked at which locations over time.
    Derived from: P6 tasks, quality inspections, TBM daily plans.

    Used for: Inferring company location when not explicitly recorded.
    """

    company_id: int = Field(description="FK to dim_company")
    location_id: str = Field(description="Location identifier (building-level)")
    period_start: Optional[str] = Field(default=None, description="Period start date (YYYY-MM-DD)")
    period_end: Optional[str] = Field(default=None, description="Period end date (YYYY-MM-DD)")
    pct_of_work: Optional[float] = Field(default=None, description="Percentage of company's work at this location")
    record_count: int = Field(description="Number of records supporting this mapping")
    source: str = Field(description="Data source (P6, RABA, PSI, TBM)")


