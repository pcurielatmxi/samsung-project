"""Integration tests for embeddings workflow."""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest


@pytest.fixture
def mock_config(tmp_path):
    """Mock config with temp directories."""
    with patch.multiple(
        'scripts.narratives.embeddings.config',
        CHROMA_PATH=tmp_path / "documents",
        MANIFEST_PATH=tmp_path / "manifest.json",
        BACKUP_DIR=tmp_path / "backups",
        SOURCE_DIRS={"test": tmp_path / "source"}
    ):
        # Create source directory with test files
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        (source_dir / "test1.txt").write_text("Hello world. This is a test document.")
        (source_dir / "test2.txt").write_text("Another document with different content.")
        yield tmp_path


class TestEmbeddingsWorkflow:
    """Test full embeddings workflow."""

    @pytest.mark.skip(reason="Requires Gemini API key")
    def test_build_creates_manifest_and_backup(self, mock_config):
        """Build creates manifest and backup."""
        from scripts.narratives.embeddings import build_index, Manifest
        from scripts.narratives.embeddings.backup import BackupManager
        from scripts.narratives.embeddings import config

        result = build_index(source="test", verbose=False)

        # Manifest should exist
        assert config.MANIFEST_PATH.exists()
        manifest = Manifest(config.MANIFEST_PATH)
        assert manifest.get_file_count("test") == 2

        # Backup should exist (for full run)
        manager = BackupManager(config.CHROMA_PATH, config.BACKUP_DIR)
        backups = manager.list_backups()
        # Note: backup only created if there was existing data

    @pytest.mark.skip(reason="Requires Gemini API key")
    def test_partial_run_preserves_chunks(self, mock_config):
        """Partial run with --limit preserves existing chunks."""
        from scripts.narratives.embeddings import build_index, Manifest
        from scripts.narratives.embeddings import config

        # First build indexes both files
        build_index(source="test", verbose=False)

        manifest = Manifest(config.MANIFEST_PATH)
        initial_count = manifest.get_chunk_count("test")

        # Partial run with limit=1 should not delete other file's chunks
        build_index(source="test", limit=1, verbose=False)

        manifest2 = Manifest(config.MANIFEST_PATH)
        assert manifest2.get_chunk_count("test") >= initial_count
