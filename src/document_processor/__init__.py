"""
Centralized document processing pipeline.

Provides N-stage document processing with LLM and script stages,
implicit stage chaining, and LLM-based quality checking.
"""

from .config import load_config, PipelineConfig, StageConfig
from .pipeline import run_pipeline

__all__ = [
    "load_config",
    "PipelineConfig",
    "StageConfig",
    "run_pipeline",
]
