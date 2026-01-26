"""
Persistent cache for label embeddings.

Stores label embeddings on disk to avoid re-embedding unchanged labels.
Uses content-based hashing (like manifest.py) to detect label changes.

Cache structure:
{
    "status": {
        "PASS": {
            "description": "Inspection passed successfully...",
            "description_hash": "abc123...",
            "embedding": [0.123, 0.456, ...]
        }
    },
    "csi": {...},
    "company": {...}
}

Usage:
    from scripts.quality.label_embedding_cache import LabelEmbeddingCache

    cache = LabelEmbeddingCache()

    # Get embeddings (cached if available, else API call)
    status_embeddings = cache.get_embeddings('status', status_labels)
    csi_embeddings = cache.get_embeddings('csi', csi_labels)
    company_embeddings = cache.get_embeddings('company', company_labels)
"""

import json
import hashlib
import numpy as np
from pathlib import Path
from typing import Dict, Any
from dataclasses import dataclass

from scripts.narratives.embeddings.client import embed_for_query


@dataclass
class CacheStats:
    """Statistics about cache hits/misses."""
    cache_hits: int = 0
    cache_misses: int = 0
    api_calls: int = 0

    @property
    def hit_rate(self) -> float:
        total = self.cache_hits + self.cache_misses
        return self.cache_hits / total if total > 0 else 0.0


class LabelEmbeddingCache:
    """
    Persistent cache for label embeddings.

    Stores embeddings on disk and only re-embeds when descriptions change.
    """

    def __init__(self, cache_path: Path = None):
        """
        Initialize cache.

        Args:
            cache_path: Path to cache file (default: ~/.cache/samsung-embeddings/label_cache.json)
        """
        if cache_path is None:
            cache_dir = Path.home() / '.cache' / 'samsung-embeddings'
            cache_dir.mkdir(parents=True, exist_ok=True)
            cache_path = cache_dir / 'label_cache.json'

        self.cache_path = cache_path
        self.cache = self._load_cache()
        self.stats = CacheStats()

    def _load_cache(self) -> Dict[str, Dict[str, Any]]:
        """Load cache from disk."""
        if not self.cache_path.exists():
            return {}

        try:
            with open(self.cache_path, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"Warning: Failed to load cache from {self.cache_path}: {e}")
            return {}

    def _save_cache(self):
        """Save cache to disk."""
        try:
            with open(self.cache_path, 'w') as f:
                json.dump(self.cache, f, indent=2)
        except IOError as e:
            print(f"Warning: Failed to save cache to {self.cache_path}: {e}")

    @staticmethod
    def _hash_description(description: str) -> str:
        """
        Compute hash of description for change detection.

        Args:
            description: Label description text

        Returns:
            SHA-256 hash (first 16 chars)
        """
        return hashlib.sha256(description.encode('utf-8')).hexdigest()[:16]

    def get_embedding(
        self,
        category: str,
        label: str,
        description: str
    ) -> np.ndarray:
        """
        Get embedding for a single label (cached or fresh).

        Args:
            category: Category name (e.g., 'status', 'csi', 'company')
            label: Label name (e.g., 'PASS', '03', 'Yates')
            description: Label description text

        Returns:
            Embedding vector as numpy array
        """
        # Compute hash
        desc_hash = self._hash_description(description)

        # Check cache
        if category in self.cache:
            if label in self.cache[category]:
                cached_entry = self.cache[category][label]

                # Check if description matches
                if cached_entry.get('description_hash') == desc_hash:
                    # Cache hit!
                    self.stats.cache_hits += 1
                    return np.array(cached_entry['embedding'])

        # Cache miss - need to embed
        self.stats.cache_misses += 1
        self.stats.api_calls += 1

        embedding = embed_for_query(description)

        # Store in cache
        if category not in self.cache:
            self.cache[category] = {}

        self.cache[category][label] = {
            'description': description,
            'description_hash': desc_hash,
            'embedding': embedding if isinstance(embedding, list) else embedding.tolist()
        }

        # Save to disk
        self._save_cache()

        return np.array(embedding)

    def get_embeddings(
        self,
        category: str,
        labels: Dict[str, str]
    ) -> Dict[str, np.ndarray]:
        """
        Get embeddings for multiple labels in a category.

        Args:
            category: Category name (e.g., 'status', 'csi', 'company')
            labels: Dict of {label_name: description}

        Returns:
            Dict of {label_name: embedding_vector}
        """
        embeddings = {}

        for label, description in labels.items():
            embeddings[label] = self.get_embedding(category, label, description)

        return embeddings

    def clear_category(self, category: str):
        """Clear all cached embeddings for a category."""
        if category in self.cache:
            del self.cache[category]
            self._save_cache()

    def clear_all(self):
        """Clear entire cache."""
        self.cache = {}
        self._save_cache()

    def get_stats(self) -> CacheStats:
        """Get cache statistics for this session."""
        return self.stats

    def print_stats(self):
        """Print cache statistics."""
        print(f"\nLabel Embedding Cache Stats:")
        print(f"  Cache hits: {self.stats.cache_hits}")
        print(f"  Cache misses: {self.stats.cache_misses}")
        print(f"  Hit rate: {self.stats.hit_rate*100:.1f}%")
        print(f"  API calls: {self.stats.api_calls}")

        if self.stats.api_calls > 0:
            print(f"  💰 Cost: ~${self.stats.api_calls * 0.000025:.4f}")


# Convenience function for one-time use
def get_cached_embeddings(
    category: str,
    labels: Dict[str, str],
    cache: LabelEmbeddingCache = None
) -> Dict[str, np.ndarray]:
    """
    Get embeddings with caching (convenience wrapper).

    Args:
        category: Category name
        labels: Label descriptions
        cache: Optional cache instance (creates new if None)

    Returns:
        Dict of embeddings
    """
    if cache is None:
        cache = LabelEmbeddingCache()

    return cache.get_embeddings(category, labels)


# Example usage
if __name__ == '__main__':
    # Example labels
    status_labels = {
        'PASS': 'Inspection passed successfully. No defects found.',
        'FAIL': 'Inspection failed. Defects found and documented.',
        'CANCELLED': 'Inspection cancelled or voided.'
    }

    # Initialize cache
    cache = LabelEmbeddingCache()

    print("First run (will call API):")
    embeddings = cache.get_embeddings('status', status_labels)
    print(f"  Got {len(embeddings)} embeddings")
    cache.print_stats()

    print("\nSecond run (will use cache):")
    cache2 = LabelEmbeddingCache()
    embeddings2 = cache2.get_embeddings('status', status_labels)
    print(f"  Got {len(embeddings2)} embeddings")
    cache2.print_stats()

    print("\nModified label (will call API for changed label only):")
    status_labels['PASS'] = 'Inspection passed. Work approved.'  # Changed!
    cache3 = LabelEmbeddingCache()
    embeddings3 = cache3.get_embeddings('status', status_labels)
    print(f"  Got {len(embeddings3)} embeddings")
    cache3.print_stats()
