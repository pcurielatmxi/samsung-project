"""
Narratives embeddings module for semantic search.

Provides CLI and Python API for searching narrative document chunks
using Gemini embeddings stored in ChromaDB.

Usage:
    # CLI
    python -m scripts.narratives.embeddings build --source narratives
    python -m scripts.narratives.embeddings search "HVAC delays"
    python -m scripts.narratives.embeddings status
    python -m scripts.narratives.embeddings backup
    python -m scripts.narratives.embeddings restore
    python -m scripts.narratives.embeddings verify

    # Python API
    from scripts.narratives.embeddings import search_chunks
    results = search_chunks("HVAC delays", source_type="narratives", limit=10)
"""

from .store import search_chunks, get_store, ChunkResult
from .builder import build_index
from .manifest import Manifest, FileEntry, compute_content_hash
from .backup import BackupManager
from .visualize import (
    prepare_visualization,
    generate_all_visualizations,
    VisualizationData,
    ClusterInfo,
)

__all__ = [
    "search_chunks",
    "build_index",
    "get_store",
    "ChunkResult",
    "Manifest",
    "FileEntry",
    "compute_content_hash",
    "BackupManager",
    "prepare_visualization",
    "generate_all_visualizations",
    "VisualizationData",
    "ClusterInfo",
]
