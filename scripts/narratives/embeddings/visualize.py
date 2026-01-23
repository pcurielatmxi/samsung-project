"""Embedding visualization with dimensionality reduction, clustering, and labeling."""

import hashlib
import json
import random
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import numpy as np

from . import config
from .store import get_store


@dataclass
class ClusterInfo:
    """Information about a cluster."""
    cluster_id: int
    size: int
    label: str
    sample_texts: List[str]
    metadata_summary: Dict[str, Dict[str, int]]  # metadata field -> value -> count
    centroid: Optional[Tuple[float, ...]] = None  # 2D or 3D centroid


@dataclass
class VisualizationData:
    """Data prepared for visualization."""
    ids: List[str]
    texts: List[str]
    embeddings: np.ndarray  # Original high-dimensional
    embeddings_reduced: np.ndarray  # Reduced to 2D or 3D
    metadata: List[Dict[str, Any]]
    cluster_labels: np.ndarray  # HDBSCAN labels (-1 for noise)
    cluster_info: Dict[int, ClusterInfo]
    n_dimensions: int = 2  # 2 or 3

    @property
    def embeddings_2d(self) -> np.ndarray:
        """Alias for backward compatibility."""
        return self.embeddings_reduced


def extract_embeddings(
    source_type: Optional[str] = None,
    limit: Optional[int] = None,
    verbose: bool = True
) -> Tuple[List[str], List[str], np.ndarray, List[Dict[str, Any]]]:
    """Extract all embeddings and metadata from ChromaDB.

    Args:
        source_type: Filter by source type (narratives, raba, psi)
        limit: Limit number of chunks (for testing)
        verbose: Print progress

    Returns:
        Tuple of (ids, texts, embeddings, metadatas)
    """
    store = get_store()
    collection = store.get_chunks_collection()

    # Build where filter
    where = {"source_type": {"$eq": source_type}} if source_type else None

    # Get all data from collection
    if verbose:
        print("Extracting embeddings from ChromaDB...")

    result = collection.get(
        where=where,
        include=["documents", "embeddings", "metadatas"]
    )

    if not result["ids"]:
        raise ValueError("No embeddings found in database")

    ids = result["ids"]
    texts = result["documents"]
    embeddings = np.array(result["embeddings"])
    metadatas = result["metadatas"]

    if limit and len(ids) > limit:
        if verbose:
            print(f"  Limiting to {limit} of {len(ids)} chunks")
        ids = ids[:limit]
        texts = texts[:limit]
        embeddings = embeddings[:limit]
        metadatas = metadatas[:limit]

    if verbose:
        print(f"  Extracted {len(ids)} embeddings ({embeddings.shape[1]} dimensions)")

    return ids, texts, embeddings, metadatas


def reduce_dimensions(
    embeddings: np.ndarray,
    n_neighbors: int = 15,
    min_dist: float = 0.1,
    metric: str = "cosine",
    random_state: int = 42,
    n_components: int = 2,
    verbose: bool = True
) -> np.ndarray:
    """Reduce embeddings to 2D or 3D using UMAP.

    Args:
        embeddings: High-dimensional embedding array
        n_neighbors: UMAP n_neighbors parameter
        min_dist: UMAP min_dist parameter
        metric: Distance metric
        random_state: Random seed for reproducibility
        n_components: Output dimensions (2 or 3)
        verbose: Print progress

    Returns:
        Reduced embedding array (n_samples, n_components)
    """
    try:
        import umap
    except ImportError:
        raise ImportError("umap-learn not installed. Run: pip install umap-learn")

    if verbose:
        print(f"Reducing dimensions with UMAP to {n_components}D (n_neighbors={n_neighbors}, min_dist={min_dist})...")

    reducer = umap.UMAP(
        n_components=n_components,
        n_neighbors=n_neighbors,
        min_dist=min_dist,
        metric=metric,
        random_state=random_state,
        verbose=verbose
    )

    embeddings_reduced = reducer.fit_transform(embeddings)

    if verbose:
        print(f"  Reduced to shape: {embeddings_reduced.shape}")

    return embeddings_reduced


def cluster_embeddings(
    embeddings_2d: np.ndarray,
    min_cluster_size: int = 50,
    min_samples: int = 10,
    verbose: bool = True
) -> np.ndarray:
    """Cluster 2D embeddings using HDBSCAN.

    Args:
        embeddings_2d: 2D embedding array from UMAP
        min_cluster_size: Minimum cluster size
        min_samples: Minimum samples for core points
        verbose: Print progress

    Returns:
        Cluster labels array (-1 for noise points)
    """
    try:
        import hdbscan
    except ImportError:
        raise ImportError("hdbscan not installed. Run: pip install hdbscan")

    if verbose:
        print(f"Clustering with HDBSCAN (min_cluster_size={min_cluster_size})...")

    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size,
        min_samples=min_samples,
        metric="euclidean"
    )

    cluster_labels = clusterer.fit_predict(embeddings_2d)

    n_clusters = len(set(cluster_labels)) - (1 if -1 in cluster_labels else 0)
    n_noise = (cluster_labels == -1).sum()

    if verbose:
        print(f"  Found {n_clusters} clusters, {n_noise} noise points ({n_noise/len(cluster_labels)*100:.1f}%)")

    return cluster_labels


def generate_cluster_labels(
    cluster_info: Dict[int, ClusterInfo],
    verbose: bool = True
) -> Dict[int, str]:
    """Generate descriptive labels for clusters using Gemini LLM.

    Args:
        cluster_info: Dict mapping cluster_id to ClusterInfo
        verbose: Print progress

    Returns:
        Dict mapping cluster_id to descriptive label
    """
    from google import genai

    if not config.GEMINI_API_KEY:
        raise ValueError("GEMINI_API_KEY not set")

    client = genai.Client(api_key=config.GEMINI_API_KEY)
    labels = {}

    if verbose:
        print(f"Generating LLM labels for {len(cluster_info)} clusters...")

    for cluster_id, info in cluster_info.items():
        if cluster_id == -1:
            labels[-1] = "Noise/Outliers"
            continue

        # Build prompt with sample texts and metadata
        sample_texts_str = "\n---\n".join(info.sample_texts[:5])

        # Summarize metadata
        meta_summary = []
        for field_name, value_counts in info.metadata_summary.items():
            top_values = sorted(value_counts.items(), key=lambda x: -x[1])[:3]
            if top_values:
                values_str = ", ".join(f"{v}({c})" for v, c in top_values)
                meta_summary.append(f"  {field_name}: {values_str}")
        meta_str = "\n".join(meta_summary) if meta_summary else "  (no metadata)"

        prompt = f"""Analyze these document chunks from a construction project and provide a short, descriptive label (3-6 words) that captures their common theme.

SAMPLE TEXTS FROM THIS CLUSTER:
{sample_texts_str}

METADATA DISTRIBUTION:
{meta_str}

Respond with ONLY the label, nothing else. Examples of good labels:
- "HVAC Installation Delays"
- "Quality Inspection Failures"
- "Schedule Narrative Updates"
- "Electrical Coordination Issues"
- "Weekly Progress Reports"
"""

        try:
            response = client.models.generate_content(
                model="gemini-2.0-flash",
                contents=prompt,
                config={"temperature": 0.3, "max_output_tokens": 50}
            )
            label = response.text.strip().strip('"').strip("'")
            # Truncate if too long
            if len(label) > 40:
                label = label[:40] + "..."
            labels[cluster_id] = label

            if verbose:
                print(f"  Cluster {cluster_id} ({info.size} pts): {label}")

        except Exception as e:
            labels[cluster_id] = f"Cluster {cluster_id}"
            if verbose:
                print(f"  Cluster {cluster_id}: Error generating label: {e}")

    return labels


def build_cluster_info(
    ids: List[str],
    texts: List[str],
    embeddings_reduced: np.ndarray,
    metadatas: List[Dict[str, Any]],
    cluster_labels: np.ndarray,
    samples_per_cluster: int = 10
) -> Dict[int, ClusterInfo]:
    """Build ClusterInfo for each cluster.

    Args:
        ids: Chunk IDs
        texts: Chunk texts
        embeddings_reduced: 2D or 3D coordinates
        metadatas: Chunk metadata dicts
        cluster_labels: Cluster assignments
        samples_per_cluster: Number of sample texts per cluster

    Returns:
        Dict mapping cluster_id to ClusterInfo
    """
    unique_clusters = set(cluster_labels)
    cluster_info = {}

    metadata_fields = ["source_type", "document_type", "author", "subfolder"]

    for cluster_id in unique_clusters:
        mask = cluster_labels == cluster_id
        indices = np.where(mask)[0]

        # Sample texts (random selection for variety)
        sample_indices = random.sample(
            list(indices),
            min(samples_per_cluster, len(indices))
        )
        sample_texts = [texts[i] for i in sample_indices]

        # Truncate long texts for labeling
        sample_texts = [t[:500] + "..." if len(t) > 500 else t for t in sample_texts]

        # Build metadata summary
        metadata_summary = {}
        for field in metadata_fields:
            value_counts: Dict[str, int] = {}
            for i in indices:
                value = metadatas[i].get(field, "unknown")
                if value:
                    value_counts[value] = value_counts.get(value, 0) + 1
            if value_counts:
                metadata_summary[field] = value_counts

        # Compute centroid (works for 2D or 3D)
        cluster_points = embeddings_reduced[mask]
        centroid = cluster_points.mean(axis=0)

        cluster_info[cluster_id] = ClusterInfo(
            cluster_id=cluster_id,
            size=len(indices),
            label=f"Cluster {cluster_id}",  # Placeholder until LLM labeling
            sample_texts=sample_texts,
            metadata_summary=metadata_summary,
            centroid=tuple(float(c) for c in centroid)
        )

    return cluster_info


def prepare_visualization(
    source_type: Optional[str] = None,
    limit: Optional[int] = None,
    n_neighbors: int = 15,
    min_dist: float = 0.1,
    min_cluster_size: int = 50,
    min_samples: int = 10,
    n_dimensions: int = 2,
    generate_labels: bool = True,
    verbose: bool = True
) -> VisualizationData:
    """Prepare data for visualization: extract, reduce, cluster, label.

    Args:
        source_type: Filter by source type
        limit: Limit chunks (for testing)
        n_neighbors: UMAP parameter
        min_dist: UMAP parameter
        min_cluster_size: HDBSCAN parameter
        min_samples: HDBSCAN parameter
        n_dimensions: Output dimensions (2 or 3)
        generate_labels: Whether to use LLM for cluster labels
        verbose: Print progress

    Returns:
        VisualizationData with all prepared data
    """
    # Extract
    ids, texts, embeddings, metadatas = extract_embeddings(
        source_type=source_type,
        limit=limit,
        verbose=verbose
    )

    # Reduce dimensions
    embeddings_reduced = reduce_dimensions(
        embeddings,
        n_neighbors=n_neighbors,
        min_dist=min_dist,
        n_components=n_dimensions,
        verbose=verbose
    )

    # Cluster (use all dimensions)
    cluster_labels = cluster_embeddings(
        embeddings_reduced,
        min_cluster_size=min_cluster_size,
        min_samples=min_samples,
        verbose=verbose
    )

    # Build cluster info
    cluster_info = build_cluster_info(
        ids, texts, embeddings_reduced, metadatas, cluster_labels
    )

    # Generate LLM labels
    if generate_labels:
        llm_labels = generate_cluster_labels(cluster_info, verbose=verbose)
        for cluster_id, label in llm_labels.items():
            if cluster_id in cluster_info:
                cluster_info[cluster_id].label = label

    return VisualizationData(
        ids=ids,
        texts=texts,
        embeddings=embeddings,
        embeddings_reduced=embeddings_reduced,
        metadata=metadatas,
        cluster_labels=cluster_labels,
        cluster_info=cluster_info,
        n_dimensions=n_dimensions
    )


def plot_clusters(
    data: VisualizationData,
    output_path: Path,
    title: str = "Document Embeddings - Cluster View",
    figsize: Tuple[int, int] = (16, 12),
    dpi: int = 150,
    show_labels: bool = True,
    verbose: bool = True
):
    """Create cluster visualization plot.

    Args:
        data: Prepared visualization data
        output_path: Path to save figure
        title: Plot title
        figsize: Figure size
        dpi: Resolution
        show_labels: Whether to show cluster labels on plot
        verbose: Print progress
    """
    try:
        import matplotlib.pyplot as plt
        import matplotlib.colors as mcolors
    except ImportError:
        raise ImportError("matplotlib not installed. Run: pip install matplotlib")

    if verbose:
        print(f"Creating cluster plot...")

    fig, ax = plt.subplots(figsize=figsize)

    # Get unique clusters (excluding noise)
    unique_clusters = sorted([c for c in set(data.cluster_labels) if c != -1])
    n_clusters = len(unique_clusters)

    # Generate colors
    if n_clusters <= 20:
        colors = plt.cm.tab20(np.linspace(0, 1, 20))[:n_clusters]
    else:
        colors = plt.cm.turbo(np.linspace(0.1, 0.9, n_clusters))

    cluster_to_color = {c: colors[i] for i, c in enumerate(unique_clusters)}
    cluster_to_color[-1] = (0.7, 0.7, 0.7, 0.3)  # Light gray for noise

    # Plot noise points first (underneath)
    noise_mask = data.cluster_labels == -1
    if noise_mask.any():
        ax.scatter(
            data.embeddings_2d[noise_mask, 0],
            data.embeddings_2d[noise_mask, 1],
            c=[cluster_to_color[-1]],
            s=5,
            alpha=0.3,
            label=f"Noise ({noise_mask.sum():,})"
        )

    # Plot each cluster
    for cluster_id in unique_clusters:
        mask = data.cluster_labels == cluster_id
        info = data.cluster_info[cluster_id]

        ax.scatter(
            data.embeddings_2d[mask, 0],
            data.embeddings_2d[mask, 1],
            c=[cluster_to_color[cluster_id]],
            s=15,
            alpha=0.6,
            label=f"{info.label} ({info.size:,})"
        )

        # Add label at centroid
        if show_labels and info.centroid:
            ax.annotate(
                info.label,
                info.centroid[:2],  # Use first 2 dimensions
                fontsize=8,
                fontweight="bold",
                ha="center",
                va="center",
                bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.7, edgecolor="gray")
            )

    ax.set_xlabel("UMAP Dimension 1", fontsize=12)
    ax.set_ylabel("UMAP Dimension 2", fontsize=12)
    ax.set_title(title, fontsize=14, fontweight="bold")

    # Legend outside plot
    ax.legend(
        loc="center left",
        bbox_to_anchor=(1.02, 0.5),
        fontsize=8,
        framealpha=0.9
    )

    plt.tight_layout()

    # Save
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    plt.savefig(output_path, dpi=dpi, bbox_inches="tight", facecolor="white")
    plt.close()

    if verbose:
        print(f"  Saved: {output_path}")


def plot_clusters_3d(
    data: VisualizationData,
    output_dir: Path,
    title: str = "Document Embeddings - 3D Cluster View",
    figsize: Tuple[int, int] = (14, 12),
    dpi: int = 150,
    angles: List[Tuple[int, int]] = None,
    verbose: bool = True
):
    """Create 3D cluster visualization from multiple angles.

    Args:
        data: Prepared visualization data (must be 3D)
        output_dir: Directory to save figures
        title: Plot title
        figsize: Figure size
        dpi: Resolution
        angles: List of (elevation, azimuth) tuples for different views
        verbose: Print progress
    """
    try:
        import matplotlib.pyplot as plt
        from mpl_toolkits.mplot3d import Axes3D
    except ImportError:
        raise ImportError("matplotlib not installed. Run: pip install matplotlib")

    if data.n_dimensions != 3:
        raise ValueError("Data must be 3D for 3D plotting")

    if angles is None:
        # Default views: front, side, top, isometric
        angles = [
            (30, 45, "isometric"),
            (30, 135, "isometric_back"),
            (90, 0, "top"),
            (0, 0, "front"),
        ]

    if verbose:
        print(f"Creating 3D cluster plots ({len(angles)} angles)...")

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Get unique clusters (excluding noise)
    unique_clusters = sorted([c for c in set(data.cluster_labels) if c != -1])
    n_clusters = len(unique_clusters)

    # Generate colors
    if n_clusters <= 20:
        colors = plt.cm.tab20(np.linspace(0, 1, 20))[:n_clusters]
    else:
        colors = plt.cm.turbo(np.linspace(0.1, 0.9, n_clusters))

    cluster_to_color = {c: colors[i] for i, c in enumerate(unique_clusters)}
    cluster_to_color[-1] = (0.7, 0.7, 0.7, 0.3)  # Light gray for noise

    for elev, azim, view_name in angles:
        fig = plt.figure(figsize=figsize)
        ax = fig.add_subplot(111, projection='3d')

        # Plot noise points first
        noise_mask = data.cluster_labels == -1
        if noise_mask.any():
            ax.scatter(
                data.embeddings_reduced[noise_mask, 0],
                data.embeddings_reduced[noise_mask, 1],
                data.embeddings_reduced[noise_mask, 2],
                c=[cluster_to_color[-1]],
                s=3,
                alpha=0.2,
                label=f"Noise ({noise_mask.sum():,})"
            )

        # Plot each cluster
        for cluster_id in unique_clusters:
            mask = data.cluster_labels == cluster_id
            info = data.cluster_info[cluster_id]

            ax.scatter(
                data.embeddings_reduced[mask, 0],
                data.embeddings_reduced[mask, 1],
                data.embeddings_reduced[mask, 2],
                c=[cluster_to_color[cluster_id]],
                s=10,
                alpha=0.6,
                label=f"{info.label} ({info.size:,})"
            )

        ax.set_xlabel("UMAP 1", fontsize=10)
        ax.set_ylabel("UMAP 2", fontsize=10)
        ax.set_zlabel("UMAP 3", fontsize=10)
        ax.set_title(f"{title} ({view_name})", fontsize=12, fontweight="bold")

        # Set viewing angle
        ax.view_init(elev=elev, azim=azim)

        # Legend
        ax.legend(
            loc="center left",
            bbox_to_anchor=(1.15, 0.5),
            fontsize=7,
            framealpha=0.9
        )

        plt.tight_layout()

        output_path = output_dir / f"clusters_3d_{view_name}.png"
        plt.savefig(output_path, dpi=dpi, bbox_inches="tight", facecolor="white")
        plt.close()

        if verbose:
            print(f"  Saved: {output_path}")


def plot_clusters_3d_gif(
    data: VisualizationData,
    output_path: Path,
    title: str = "Document Embeddings - 3D",
    figsize: Tuple[int, int] = (12, 10),
    dpi: int = 100,
    n_frames: int = 36,
    duration: int = 100,
    verbose: bool = True
):
    """Create animated GIF rotating around the 3D visualization.

    Args:
        data: Prepared visualization data (must be 3D)
        output_path: Path to save GIF
        title: Plot title
        figsize: Figure size
        dpi: Resolution
        n_frames: Number of frames in rotation
        duration: Milliseconds per frame
        verbose: Print progress
    """
    try:
        import matplotlib.pyplot as plt
        from mpl_toolkits.mplot3d import Axes3D
        from PIL import Image
        import io
    except ImportError:
        raise ImportError("matplotlib and pillow required")

    if data.n_dimensions != 3:
        raise ValueError("Data must be 3D for 3D plotting")

    if verbose:
        print(f"Creating 3D rotating GIF ({n_frames} frames)...")

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Get unique clusters
    unique_clusters = sorted([c for c in set(data.cluster_labels) if c != -1])
    n_clusters = len(unique_clusters)

    if n_clusters <= 20:
        colors = plt.cm.tab20(np.linspace(0, 1, 20))[:n_clusters]
    else:
        colors = plt.cm.turbo(np.linspace(0.1, 0.9, n_clusters))

    cluster_to_color = {c: colors[i] for i, c in enumerate(unique_clusters)}
    cluster_to_color[-1] = (0.7, 0.7, 0.7, 0.3)

    frames = []

    for i, azim in enumerate(np.linspace(0, 360, n_frames, endpoint=False)):
        fig = plt.figure(figsize=figsize)
        ax = fig.add_subplot(111, projection='3d')

        # Plot noise
        noise_mask = data.cluster_labels == -1
        if noise_mask.any():
            ax.scatter(
                data.embeddings_reduced[noise_mask, 0],
                data.embeddings_reduced[noise_mask, 1],
                data.embeddings_reduced[noise_mask, 2],
                c=[cluster_to_color[-1]],
                s=2,
                alpha=0.2
            )

        # Plot clusters
        for cluster_id in unique_clusters:
            mask = data.cluster_labels == cluster_id
            info = data.cluster_info[cluster_id]
            ax.scatter(
                data.embeddings_reduced[mask, 0],
                data.embeddings_reduced[mask, 1],
                data.embeddings_reduced[mask, 2],
                c=[cluster_to_color[cluster_id]],
                s=8,
                alpha=0.6
            )

        ax.set_xlabel("UMAP 1", fontsize=9)
        ax.set_ylabel("UMAP 2", fontsize=9)
        ax.set_zlabel("UMAP 3", fontsize=9)
        ax.set_title(title, fontsize=11, fontweight="bold")
        ax.view_init(elev=25, azim=azim)

        # Remove axis numbers for cleaner look
        ax.set_xticklabels([])
        ax.set_yticklabels([])
        ax.set_zticklabels([])

        plt.tight_layout()

        # Save frame to buffer
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=dpi, facecolor='white')
        buf.seek(0)
        frames.append(Image.open(buf).copy())
        buf.close()
        plt.close()

        if verbose and (i + 1) % 10 == 0:
            print(f"  Frame {i + 1}/{n_frames}")

    # Save as GIF
    frames[0].save(
        output_path,
        save_all=True,
        append_images=frames[1:],
        duration=duration,
        loop=0
    )

    if verbose:
        print(f"  Saved: {output_path}")


def plot_by_metadata(
    data: VisualizationData,
    output_dir: Path,
    metadata_field: str,
    title_prefix: str = "Document Embeddings",
    figsize: Tuple[int, int] = (14, 10),
    dpi: int = 150,
    verbose: bool = True
):
    """Create visualization colored by metadata field.

    Args:
        data: Prepared visualization data
        output_dir: Directory to save figures
        metadata_field: Metadata field to color by
        title_prefix: Title prefix
        figsize: Figure size
        dpi: Resolution
        verbose: Print progress
    """
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        raise ImportError("matplotlib not installed")

    if verbose:
        print(f"Creating {metadata_field} plot...")

    # Extract values
    values = [m.get(metadata_field, "unknown") for m in data.metadata]
    unique_values = sorted(set(values))

    # Generate colors
    n_values = len(unique_values)
    if n_values <= 10:
        colors = plt.cm.tab10(np.linspace(0, 1, 10))[:n_values]
    elif n_values <= 20:
        colors = plt.cm.tab20(np.linspace(0, 1, 20))[:n_values]
    else:
        colors = plt.cm.turbo(np.linspace(0.1, 0.9, n_values))

    value_to_color = {v: colors[i] for i, v in enumerate(unique_values)}

    fig, ax = plt.subplots(figsize=figsize)

    for value in unique_values:
        mask = np.array([v == value for v in values])
        count = mask.sum()

        ax.scatter(
            data.embeddings_2d[mask, 0],
            data.embeddings_2d[mask, 1],
            c=[value_to_color[value]],
            s=10,
            alpha=0.5,
            label=f"{value} ({count:,})"
        )

    ax.set_xlabel("UMAP Dimension 1", fontsize=12)
    ax.set_ylabel("UMAP Dimension 2", fontsize=12)
    ax.set_title(f"{title_prefix} by {metadata_field.replace('_', ' ').title()}",
                 fontsize=14, fontweight="bold")

    ax.legend(
        loc="center left",
        bbox_to_anchor=(1.02, 0.5),
        fontsize=9
    )

    plt.tight_layout()

    output_path = output_dir / f"embeddings_by_{metadata_field}.png"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=dpi, bbox_inches="tight", facecolor="white")
    plt.close()

    if verbose:
        print(f"  Saved: {output_path}")


def plot_cluster_composition(
    data: VisualizationData,
    output_path: Path,
    figsize: Tuple[int, int] = (14, 10),
    dpi: int = 150,
    verbose: bool = True
):
    """Create stacked bar chart showing cluster composition by metadata.

    Args:
        data: Prepared visualization data
        output_path: Path to save figure
        figsize: Figure size
        dpi: Resolution
        verbose: Print progress
    """
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        raise ImportError("matplotlib not installed")

    if verbose:
        print("Creating cluster composition chart...")

    # Get clusters sorted by size (excluding noise)
    clusters = sorted(
        [(cid, info) for cid, info in data.cluster_info.items() if cid != -1],
        key=lambda x: -x[1].size
    )[:20]  # Top 20 clusters

    if not clusters:
        if verbose:
            print("  No clusters to plot")
        return

    # Use document_type for composition
    fig, ax = plt.subplots(figsize=figsize)

    # Get all document types
    all_doc_types = set()
    for _, info in clusters:
        all_doc_types.update(info.metadata_summary.get("document_type", {}).keys())
    all_doc_types = sorted(all_doc_types)

    # Build data for stacked bars
    cluster_labels = [info.label for _, info in clusters]
    bottom = np.zeros(len(clusters))

    colors = plt.cm.Set3(np.linspace(0, 1, len(all_doc_types)))

    for i, doc_type in enumerate(all_doc_types):
        values = []
        for _, info in clusters:
            count = info.metadata_summary.get("document_type", {}).get(doc_type, 0)
            values.append(count)

        ax.barh(
            range(len(clusters)),
            values,
            left=bottom,
            label=doc_type,
            color=colors[i]
        )
        bottom += values

    ax.set_yticks(range(len(clusters)))
    ax.set_yticklabels(cluster_labels, fontsize=9)
    ax.set_xlabel("Number of Documents", fontsize=12)
    ax.set_title("Cluster Composition by Document Type", fontsize=14, fontweight="bold")

    ax.legend(
        loc="center left",
        bbox_to_anchor=(1.02, 0.5),
        fontsize=9
    )

    ax.invert_yaxis()
    plt.tight_layout()

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=dpi, bbox_inches="tight", facecolor="white")
    plt.close()

    if verbose:
        print(f"  Saved: {output_path}")


def plot_density(
    data: VisualizationData,
    output_path: Path,
    title: str = "Document Embeddings - Density View",
    figsize: Tuple[int, int] = (14, 10),
    dpi: int = 150,
    verbose: bool = True
):
    """Create kernel density estimation plot.

    Args:
        data: Prepared visualization data
        output_path: Path to save figure
        title: Plot title
        figsize: Figure size
        dpi: Resolution
        verbose: Print progress
    """
    try:
        import matplotlib.pyplot as plt
        from scipy import stats
    except ImportError:
        raise ImportError("matplotlib and scipy required")

    if verbose:
        print("Creating density plot...")

    fig, ax = plt.subplots(figsize=figsize)

    x = data.embeddings_2d[:, 0]
    y = data.embeddings_2d[:, 1]

    # Create KDE
    try:
        xy = np.vstack([x, y])
        kde = stats.gaussian_kde(xy)

        # Create grid
        xmin, xmax = x.min() - 1, x.max() + 1
        ymin, ymax = y.min() - 1, y.max() + 1
        xx, yy = np.mgrid[xmin:xmax:100j, ymin:ymax:100j]
        positions = np.vstack([xx.ravel(), yy.ravel()])

        z = kde(positions).reshape(xx.shape)

        # Plot
        ax.contourf(xx, yy, z, levels=20, cmap="viridis", alpha=0.8)
        ax.scatter(x, y, c="white", s=1, alpha=0.3)

    except Exception as e:
        # Fallback to hexbin if KDE fails
        if verbose:
            print(f"  KDE failed ({e}), using hexbin")
        hb = ax.hexbin(x, y, gridsize=50, cmap="viridis", mincnt=1)
        plt.colorbar(hb, ax=ax, label="Count")

    ax.set_xlabel("UMAP Dimension 1", fontsize=12)
    ax.set_ylabel("UMAP Dimension 2", fontsize=12)
    ax.set_title(title, fontsize=14, fontweight="bold")

    plt.tight_layout()

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=dpi, bbox_inches="tight", facecolor="white")
    plt.close()

    if verbose:
        print(f"  Saved: {output_path}")


def generate_all_visualizations(
    output_dir: Path,
    source_type: Optional[str] = None,
    limit: Optional[int] = None,
    n_neighbors: int = 15,
    min_dist: float = 0.1,
    min_cluster_size: int = 50,
    n_dimensions: int = 2,
    generate_labels: bool = True,
    create_gif: bool = False,
    verbose: bool = True
) -> VisualizationData:
    """Generate all visualization types.

    Args:
        output_dir: Directory to save all figures
        source_type: Filter by source type
        limit: Limit chunks
        n_neighbors: UMAP parameter
        min_dist: UMAP parameter
        min_cluster_size: HDBSCAN parameter
        n_dimensions: Output dimensions (2 or 3)
        generate_labels: Use LLM for cluster labels
        create_gif: Create animated GIF for 3D (slower)
        verbose: Print progress

    Returns:
        The prepared VisualizationData
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Prepare data
    data = prepare_visualization(
        source_type=source_type,
        limit=limit,
        n_neighbors=n_neighbors,
        min_dist=min_dist,
        min_cluster_size=min_cluster_size,
        n_dimensions=n_dimensions,
        generate_labels=generate_labels,
        verbose=verbose
    )

    if verbose:
        print()
        print(f"Generating {n_dimensions}D visualizations...")

    source_suffix = f"_{source_type}" if source_type else ""

    if n_dimensions == 3:
        # 3D visualizations
        plot_clusters_3d(
            data,
            output_dir,
            title=f"Document Embeddings - 3D Clusters" + (f" ({source_type})" if source_type else ""),
            verbose=verbose
        )

        if create_gif:
            plot_clusters_3d_gif(
                data,
                output_dir / f"clusters_3d_rotating{source_suffix}.gif",
                title=f"Document Embeddings - 3D" + (f" ({source_type})" if source_type else ""),
                verbose=verbose
            )
    else:
        # 2D visualizations
        plot_clusters(
            data,
            output_dir / f"clusters{source_suffix}.png",
            title=f"Document Embeddings - Clusters" + (f" ({source_type})" if source_type else ""),
            verbose=verbose
        )

        # Density view (2D only)
        plot_density(
            data,
            output_dir / f"density{source_suffix}.png",
            title=f"Document Embeddings - Density" + (f" ({source_type})" if source_type else ""),
            verbose=verbose
        )

        # By metadata fields (2D only)
        for field in ["source_type", "document_type", "author"]:
            has_data = any(m.get(field) for m in data.metadata)
            if has_data:
                plot_by_metadata(
                    data,
                    output_dir,
                    field,
                    verbose=verbose
                )

    # Cluster composition (works for both 2D and 3D)
    plot_cluster_composition(
        data,
        output_dir / f"cluster_composition{source_suffix}.png",
        verbose=verbose
    )

    # Save cluster info as JSON
    cluster_summary = {
        str(cid): {
            "label": info.label,
            "size": info.size,
            "centroid": info.centroid,
            "top_metadata": {
                field: dict(sorted(counts.items(), key=lambda x: -x[1])[:3])
                for field, counts in info.metadata_summary.items()
            }
        }
        for cid, info in data.cluster_info.items()
    }

    summary_path = output_dir / f"cluster_summary{source_suffix}.json"
    with open(summary_path, "w") as f:
        json.dump(cluster_summary, f, indent=2)

    if verbose:
        print(f"  Saved: {summary_path}")
        print()
        print(f"All visualizations saved to: {output_dir}")

    return data


# =============================================================================
# Interactive Visualizations (Plotly)
# =============================================================================

def plot_interactive_clusters(
    data: VisualizationData,
    output_path: Path,
    title: str = "Document Embeddings - Interactive",
    verbose: bool = True
):
    """Create interactive 2D or 3D cluster visualization with Plotly.

    Args:
        data: Prepared visualization data
        output_path: Path to save HTML file
        title: Plot title
        verbose: Print progress
    """
    try:
        import plotly.graph_objects as go
        import plotly.express as px
    except ImportError:
        raise ImportError("plotly not installed. Run: pip install plotly")

    if verbose:
        print(f"Creating interactive {data.n_dimensions}D cluster plot...")

    # Build hover text with document info
    hover_texts = []
    for i, (text, meta) in enumerate(zip(data.texts, data.metadata)):
        # Truncate text for hover
        preview = text[:200].replace('\n', ' ')
        if len(text) > 200:
            preview += "..."

        hover = f"<b>{meta.get('source_file', 'Unknown')}</b><br>"
        hover += f"Type: {meta.get('document_type', 'N/A')}<br>"
        hover += f"Author: {meta.get('author', 'N/A')}<br>"
        hover += f"Date: {meta.get('file_date', 'N/A')}<br>"
        hover += f"<br>{preview}"
        hover_texts.append(hover)

    # Get cluster assignments and colors
    unique_clusters = sorted(set(data.cluster_labels))

    # Color palette
    n_clusters = len([c for c in unique_clusters if c != -1])
    if n_clusters <= 10:
        colors = px.colors.qualitative.Plotly
    else:
        colors = px.colors.qualitative.Alphabet

    fig = go.Figure()

    for cluster_id in unique_clusters:
        mask = data.cluster_labels == cluster_id
        indices = np.where(mask)[0]

        if cluster_id == -1:
            name = "Noise"
            color = "rgba(180, 180, 180, 0.4)"
            size = 4
        else:
            info = data.cluster_info[cluster_id]
            name = f"{info.label} ({info.size})"
            color = colors[cluster_id % len(colors)]
            size = 6

        if data.n_dimensions == 3:
            fig.add_trace(go.Scatter3d(
                x=data.embeddings_reduced[mask, 0],
                y=data.embeddings_reduced[mask, 1],
                z=data.embeddings_reduced[mask, 2],
                mode='markers',
                name=name,
                marker=dict(size=size, color=color, opacity=0.7),
                hovertext=[hover_texts[i] for i in indices],
                hoverinfo='text'
            ))
        else:
            fig.add_trace(go.Scatter(
                x=data.embeddings_reduced[mask, 0],
                y=data.embeddings_reduced[mask, 1],
                mode='markers',
                name=name,
                marker=dict(size=size, color=color, opacity=0.7),
                hovertext=[hover_texts[i] for i in indices],
                hoverinfo='text'
            ))

    # Layout
    if data.n_dimensions == 3:
        fig.update_layout(
            title=dict(text=title, font=dict(size=16)),
            scene=dict(
                xaxis_title="UMAP 1",
                yaxis_title="UMAP 2",
                zaxis_title="UMAP 3",
            ),
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=1.02,
                font=dict(size=10)
            ),
            margin=dict(l=0, r=200, t=50, b=0),
            hoverlabel=dict(
                bgcolor="white",
                font_size=11,
                font_family="monospace"
            )
        )
    else:
        fig.update_layout(
            title=dict(text=title, font=dict(size=16)),
            xaxis_title="UMAP 1",
            yaxis_title="UMAP 2",
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=1.02,
                font=dict(size=10)
            ),
            hovermode='closest',
            hoverlabel=dict(
                bgcolor="white",
                font_size=11,
                font_family="monospace"
            )
        )

    # Save
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(str(output_path), include_plotlyjs=True)

    if verbose:
        print(f"  Saved: {output_path}")


def plot_interactive_by_metadata(
    data: VisualizationData,
    output_path: Path,
    metadata_field: str,
    title: str = None,
    verbose: bool = True
):
    """Create interactive visualization colored by metadata field.

    Args:
        data: Prepared visualization data
        output_path: Path to save HTML file
        metadata_field: Field to color by
        title: Plot title
        verbose: Print progress
    """
    try:
        import plotly.graph_objects as go
        import plotly.express as px
    except ImportError:
        raise ImportError("plotly not installed")

    if verbose:
        print(f"Creating interactive {metadata_field} plot...")

    title = title or f"Document Embeddings by {metadata_field.replace('_', ' ').title()}"

    # Get unique values
    values = [m.get(metadata_field, "unknown") for m in data.metadata]
    unique_values = sorted(set(values))

    # Color palette
    if len(unique_values) <= 10:
        colors = px.colors.qualitative.Plotly
    else:
        colors = px.colors.qualitative.Alphabet

    # Build hover text
    hover_texts = []
    for text, meta in zip(data.texts, data.metadata):
        preview = text[:200].replace('\n', ' ')
        if len(text) > 200:
            preview += "..."
        hover = f"<b>{meta.get('source_file', 'Unknown')}</b><br>"
        hover += f"Type: {meta.get('document_type', 'N/A')}<br>"
        hover += f"Author: {meta.get('author', 'N/A')}<br>"
        hover += f"<br>{preview}"
        hover_texts.append(hover)

    fig = go.Figure()

    for i, value in enumerate(unique_values):
        mask = np.array([v == value for v in values])
        indices = np.where(mask)[0]
        count = mask.sum()

        if data.n_dimensions == 3:
            fig.add_trace(go.Scatter3d(
                x=data.embeddings_reduced[mask, 0],
                y=data.embeddings_reduced[mask, 1],
                z=data.embeddings_reduced[mask, 2],
                mode='markers',
                name=f"{value} ({count})",
                marker=dict(size=5, color=colors[i % len(colors)], opacity=0.7),
                hovertext=[hover_texts[j] for j in indices],
                hoverinfo='text'
            ))
        else:
            fig.add_trace(go.Scatter(
                x=data.embeddings_reduced[mask, 0],
                y=data.embeddings_reduced[mask, 1],
                mode='markers',
                name=f"{value} ({count})",
                marker=dict(size=5, color=colors[i % len(colors)], opacity=0.7),
                hovertext=[hover_texts[j] for j in indices],
                hoverinfo='text'
            ))

    # Layout
    if data.n_dimensions == 3:
        fig.update_layout(
            title=dict(text=title, font=dict(size=16)),
            scene=dict(
                xaxis_title="UMAP 1",
                yaxis_title="UMAP 2",
                zaxis_title="UMAP 3",
            ),
            legend=dict(yanchor="top", y=0.99, xanchor="left", x=1.02),
            margin=dict(l=0, r=200, t=50, b=0),
        )
    else:
        fig.update_layout(
            title=dict(text=title, font=dict(size=16)),
            xaxis_title="UMAP 1",
            yaxis_title="UMAP 2",
            legend=dict(yanchor="top", y=0.99, xanchor="left", x=1.02),
            hovermode='closest',
        )

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(str(output_path), include_plotlyjs=True)

    if verbose:
        print(f"  Saved: {output_path}")


def generate_interactive_visualizations(
    output_dir: Path,
    source_type: Optional[str] = None,
    limit: Optional[int] = None,
    n_neighbors: int = 15,
    min_dist: float = 0.1,
    min_cluster_size: int = 50,
    n_dimensions: int = 2,
    generate_labels: bool = True,
    verbose: bool = True
) -> VisualizationData:
    """Generate interactive HTML visualizations.

    Args:
        output_dir: Directory to save HTML files
        source_type: Filter by source type
        limit: Limit chunks
        n_neighbors: UMAP parameter
        min_dist: UMAP parameter
        min_cluster_size: HDBSCAN parameter
        n_dimensions: 2 or 3
        generate_labels: Use LLM for cluster labels
        verbose: Print progress

    Returns:
        The prepared VisualizationData
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Prepare data
    data = prepare_visualization(
        source_type=source_type,
        limit=limit,
        n_neighbors=n_neighbors,
        min_dist=min_dist,
        min_cluster_size=min_cluster_size,
        n_dimensions=n_dimensions,
        generate_labels=generate_labels,
        verbose=verbose
    )

    if verbose:
        print()
        print(f"Generating interactive {n_dimensions}D visualizations...")

    source_suffix = f"_{source_type}" if source_type else ""

    # Main cluster view
    plot_interactive_clusters(
        data,
        output_dir / f"clusters_interactive{source_suffix}.html",
        title=f"Document Embeddings - Clusters ({n_dimensions}D)" + (f" [{source_type}]" if source_type else ""),
        verbose=verbose
    )

    # By metadata fields
    for field in ["document_type", "author", "source_type"]:
        has_data = any(m.get(field) for m in data.metadata)
        if has_data:
            plot_interactive_by_metadata(
                data,
                output_dir / f"interactive_by_{field}{source_suffix}.html",
                field,
                verbose=verbose
            )

    # Save cluster summary
    cluster_summary = {
        str(cid): {
            "label": info.label,
            "size": info.size,
            "centroid": info.centroid,
        }
        for cid, info in data.cluster_info.items()
    }

    summary_path = output_dir / f"cluster_summary{source_suffix}.json"
    with open(summary_path, "w") as f:
        json.dump(cluster_summary, f, indent=2)

    if verbose:
        print(f"  Saved: {summary_path}")
        print()
        print(f"Interactive visualizations saved to: {output_dir}")
        print("Open the HTML files in a browser to explore.")

    return data
