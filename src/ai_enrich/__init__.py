"""
AI Enrich - Batch AI column generation for CSV enrichment.

Adds AI-generated columns to DataFrames using batched LLM calls with per-row caching.
"""

from .enrich import enrich_dataframe, EnrichConfig, EnrichResult

__all__ = ["enrich_dataframe", "EnrichConfig", "EnrichResult"]
