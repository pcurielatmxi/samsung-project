"""Manifest-based tracking for embeddings index.

Provides a single source of truth for which files are indexed,
enabling reliable change detection and safe partial updates.
"""

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set


MANIFEST_VERSION = 2
HASH_ALGORITHM = "sha256"
HASH_CHUNK_SIZE = 65536  # 64KB chunks for large file hashing


@dataclass
class FileEntry:
    """Metadata for an indexed file."""

    content_hash: str
    file_size: int
    chunk_count: int
    chunk_ids: List[str]
    indexed_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> "FileEntry":
        return cls(
            content_hash=data["content_hash"],
            file_size=data["file_size"],
            chunk_count=data["chunk_count"],
            chunk_ids=data["chunk_ids"],
            indexed_at=data.get("indexed_at", "")
        )


class Manifest:
    """Manifest for tracking indexed files across all sources."""

    def __init__(self, path: Path):
        self.path = path
        self.version = MANIFEST_VERSION
        self.created_at = datetime.now(timezone.utc).isoformat()
        self.updated_at = self.created_at
        self.sources: Dict[str, Dict] = {}

        # Auto-load if exists
        if path.exists():
            self.load()

    def load(self) -> None:
        """Load manifest from disk."""
        if not self.path.exists():
            return

        data = json.loads(self.path.read_text(encoding="utf-8"))
        self.version = data.get("version", 1)
        self.created_at = data.get("created_at", self.created_at)
        self.updated_at = data.get("updated_at", self.updated_at)
        self.sources = data.get("sources", {})

    def save(self) -> None:
        """Save manifest to disk atomically."""
        self.updated_at = datetime.now(timezone.utc).isoformat()

        data = {
            "version": self.version,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "sources": self.sources
        }

        # Ensure parent directory exists
        self.path.parent.mkdir(parents=True, exist_ok=True)

        # Atomic write: temp file + rename
        fd, tmp_path = tempfile.mkstemp(
            dir=self.path.parent,
            prefix=".manifest_",
            suffix=".tmp"
        )
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
            os.replace(tmp_path, self.path)
        except Exception:
            # Clean up temp file on error
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise

    def add_file(self, source: str, relative_path: str, entry: FileEntry) -> None:
        """Add or update a file entry."""
        if source not in self.sources:
            self.sources[source] = {"files": {}}

        self.sources[source]["files"][relative_path] = entry.to_dict()

    def get_file(self, source: str, relative_path: str) -> Optional[FileEntry]:
        """Get a file entry, or None if not found."""
        if source not in self.sources:
            return None

        files = self.sources[source].get("files", {})
        if relative_path not in files:
            return None

        return FileEntry.from_dict(files[relative_path])

    def remove_file(self, source: str, relative_path: str) -> Optional[FileEntry]:
        """Remove a file entry. Returns the removed entry or None."""
        if source not in self.sources:
            return None

        files = self.sources[source].get("files", {})
        if relative_path not in files:
            return None

        data = files.pop(relative_path)
        return FileEntry.from_dict(data)

    def get_all_files(self, source: str) -> Dict[str, FileEntry]:
        """Get all file entries for a source."""
        if source not in self.sources:
            return {}

        files = self.sources[source].get("files", {})
        return {path: FileEntry.from_dict(data) for path, data in files.items()}

    def get_all_chunk_ids(self, source: str) -> Set[str]:
        """Get all chunk IDs for a source."""
        chunk_ids = set()
        for entry in self.get_all_files(source).values():
            chunk_ids.update(entry.chunk_ids)
        return chunk_ids

    def get_file_count(self, source: str) -> int:
        """Get number of indexed files for a source."""
        if source not in self.sources:
            return 0
        return len(self.sources[source].get("files", {}))

    def get_chunk_count(self, source: str) -> int:
        """Get total number of chunks for a source."""
        return len(self.get_all_chunk_ids(source))


def compute_content_hash(filepath: Path) -> str:
    """Compute SHA-256 hash of file contents.

    Returns hash in format "sha256:hexdigest".
    """
    h = hashlib.sha256()

    with open(filepath, 'rb') as f:
        while chunk := f.read(HASH_CHUNK_SIZE):
            h.update(chunk)

    return f"{HASH_ALGORITHM}:{h.hexdigest()}"
