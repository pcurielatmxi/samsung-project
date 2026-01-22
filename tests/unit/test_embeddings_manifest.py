"""Tests for embeddings manifest operations."""

import json
import tempfile
from pathlib import Path
from datetime import datetime

import pytest


class TestManifest:
    """Tests for Manifest class."""

    def test_create_empty_manifest(self):
        """New manifest has correct structure."""
        from scripts.narratives.embeddings.manifest import Manifest

        with tempfile.TemporaryDirectory() as tmpdir:
            manifest = Manifest(Path(tmpdir) / "manifest.json")

            assert manifest.version == 2
            assert manifest.sources == {}
            assert manifest.created_at is not None

    def test_add_file_entry(self):
        """Can add a file entry to manifest."""
        from scripts.narratives.embeddings.manifest import Manifest, FileEntry

        with tempfile.TemporaryDirectory() as tmpdir:
            manifest = Manifest(Path(tmpdir) / "manifest.json")

            entry = FileEntry(
                content_hash="sha256:abc123",
                file_size=1000,
                chunk_count=5,
                chunk_ids=["src__file__c0000", "src__file__c0001"]
            )
            manifest.add_file("narratives", "subdir/file.pdf", entry)

            assert "narratives" in manifest.sources
            assert "subdir/file.pdf" in manifest.sources["narratives"]["files"]

    def test_get_file_entry(self):
        """Can retrieve a file entry."""
        from scripts.narratives.embeddings.manifest import Manifest, FileEntry

        with tempfile.TemporaryDirectory() as tmpdir:
            manifest = Manifest(Path(tmpdir) / "manifest.json")

            entry = FileEntry(
                content_hash="sha256:abc123",
                file_size=1000,
                chunk_count=5,
                chunk_ids=["src__file__c0000"]
            )
            manifest.add_file("narratives", "file.pdf", entry)

            retrieved = manifest.get_file("narratives", "file.pdf")
            assert retrieved is not None
            assert retrieved.content_hash == "sha256:abc123"

    def test_remove_file_entry(self):
        """Can remove a file entry."""
        from scripts.narratives.embeddings.manifest import Manifest, FileEntry

        with tempfile.TemporaryDirectory() as tmpdir:
            manifest = Manifest(Path(tmpdir) / "manifest.json")

            entry = FileEntry(
                content_hash="sha256:abc123",
                file_size=1000,
                chunk_count=5,
                chunk_ids=["src__file__c0000"]
            )
            manifest.add_file("narratives", "file.pdf", entry)

            removed = manifest.remove_file("narratives", "file.pdf")
            assert removed is not None
            assert manifest.get_file("narratives", "file.pdf") is None

    def test_save_and_load_manifest(self):
        """Manifest persists to disk and loads correctly."""
        from scripts.narratives.embeddings.manifest import Manifest, FileEntry

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "manifest.json"

            # Create and save
            manifest1 = Manifest(path)
            entry = FileEntry(
                content_hash="sha256:abc123",
                file_size=1000,
                chunk_count=5,
                chunk_ids=["src__file__c0000"]
            )
            manifest1.add_file("narratives", "file.pdf", entry)
            manifest1.save()

            # Load fresh
            manifest2 = Manifest(path)
            manifest2.load()

            retrieved = manifest2.get_file("narratives", "file.pdf")
            assert retrieved is not None
            assert retrieved.content_hash == "sha256:abc123"

    def test_atomic_save(self):
        """Save is atomic (uses temp file + rename)."""
        from scripts.narratives.embeddings.manifest import Manifest, FileEntry

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "manifest.json"

            manifest = Manifest(path)
            entry = FileEntry(
                content_hash="sha256:abc123",
                file_size=1000,
                chunk_count=5,
                chunk_ids=["src__file__c0000"]
            )
            manifest.add_file("narratives", "file.pdf", entry)
            manifest.save()

            # File should exist and be valid JSON
            assert path.exists()
            data = json.loads(path.read_text())
            assert data["version"] == 2

    def test_get_all_chunk_ids_for_source(self):
        """Can get all chunk IDs for a source."""
        from scripts.narratives.embeddings.manifest import Manifest, FileEntry

        with tempfile.TemporaryDirectory() as tmpdir:
            manifest = Manifest(Path(tmpdir) / "manifest.json")

            manifest.add_file("narratives", "a.pdf", FileEntry(
                content_hash="sha256:a",
                file_size=100,
                chunk_count=2,
                chunk_ids=["narratives__a__c0000", "narratives__a__c0001"]
            ))
            manifest.add_file("narratives", "b.pdf", FileEntry(
                content_hash="sha256:b",
                file_size=200,
                chunk_count=1,
                chunk_ids=["narratives__b__c0000"]
            ))

            all_ids = manifest.get_all_chunk_ids("narratives")
            assert len(all_ids) == 3
            assert "narratives__a__c0000" in all_ids
            assert "narratives__b__c0000" in all_ids


class TestContentHash:
    """Tests for content hashing."""

    def test_compute_content_hash(self):
        """Content hash uses SHA-256."""
        from scripts.narratives.embeddings.manifest import compute_content_hash

        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"test content")
            f.flush()

            hash1 = compute_content_hash(Path(f.name))
            hash2 = compute_content_hash(Path(f.name))

            assert hash1.startswith("sha256:")
            assert hash1 == hash2  # Deterministic

    def test_different_content_different_hash(self):
        """Different content produces different hash."""
        from scripts.narratives.embeddings.manifest import compute_content_hash

        with tempfile.TemporaryDirectory() as tmpdir:
            file1 = Path(tmpdir) / "file1.txt"
            file2 = Path(tmpdir) / "file2.txt"

            file1.write_bytes(b"content A")
            file2.write_bytes(b"content B")

            assert compute_content_hash(file1) != compute_content_hash(file2)
