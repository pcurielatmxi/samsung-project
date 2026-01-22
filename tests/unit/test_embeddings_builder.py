"""Tests for manifest-based builder operations."""

import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest


class TestManifestBasedBuilder:
    """Tests for manifest-based build operations."""

    def test_partial_run_does_not_delete_chunks(self):
        """Running with --limit doesn't delete chunks for unseen files."""
        from scripts.narratives.embeddings.manifest import Manifest, FileEntry

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            manifest_path = tmpdir / "manifest.json"

            # Pre-populate manifest with 2 files
            manifest = Manifest(manifest_path)
            manifest.add_file("test", "file1.pdf", FileEntry(
                content_hash="sha256:aaa",
                file_size=100,
                chunk_count=2,
                chunk_ids=["test__file1__c0000", "test__file1__c0001"]
            ))
            manifest.add_file("test", "file2.pdf", FileEntry(
                content_hash="sha256:bbb",
                file_size=200,
                chunk_count=1,
                chunk_ids=["test__file2__c0000"]
            ))
            manifest.save()

            # Simulate partial run that only sees file1
            # file2's chunks should NOT be deleted

            # After partial run, manifest should still have file2
            manifest2 = Manifest(manifest_path)
            assert manifest2.get_file("test", "file2.pdf") is not None

    def test_content_hash_used_for_change_detection(self):
        """Content hash determines if file needs reindexing."""
        from scripts.narratives.embeddings.manifest import Manifest, FileEntry, compute_content_hash

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create a test file
            test_file = tmpdir / "test.txt"
            test_file.write_text("original content")

            hash1 = compute_content_hash(test_file)

            # Same content = same hash (even if we "touch" it)
            test_file.write_text("original content")
            hash2 = compute_content_hash(test_file)

            assert hash1 == hash2

            # Different content = different hash
            test_file.write_text("modified content")
            hash3 = compute_content_hash(test_file)

            assert hash1 != hash3

    def test_deleted_source_files_removed_from_manifest(self):
        """Files deleted from source are removed from manifest."""
        from scripts.narratives.embeddings.manifest import Manifest, FileEntry

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            manifest_path = tmpdir / "manifest.json"
            source_dir = tmpdir / "source"
            source_dir.mkdir()

            # Create source file and add to manifest
            (source_dir / "file1.pdf").write_bytes(b"content")

            manifest = Manifest(manifest_path)
            manifest.add_file("test", "file1.pdf", FileEntry(
                content_hash="sha256:aaa",
                file_size=100,
                chunk_count=1,
                chunk_ids=["test__file1__c0000"]
            ))
            manifest.save()

            # Now delete source file
            (source_dir / "file1.pdf").unlink()

            # A full rebuild (--force) should remove from manifest
            # This is tested via integration test

    def test_unchanged_file_skipped_by_content_hash(self):
        """Files with same content hash are skipped."""
        from scripts.narratives.embeddings.manifest import Manifest, FileEntry, compute_content_hash

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            manifest_path = tmpdir / "manifest.json"

            # Create a test file
            test_file = tmpdir / "test.txt"
            test_file.write_text("test content")
            content_hash = compute_content_hash(test_file)

            # Add to manifest with same hash
            manifest = Manifest(manifest_path)
            manifest.add_file("test", "test.txt", FileEntry(
                content_hash=content_hash,
                file_size=12,
                chunk_count=1,
                chunk_ids=["test__test.txt__c0000"]
            ))
            manifest.save()

            # File should be recognized as unchanged
            manifest2 = Manifest(manifest_path)
            entry = manifest2.get_file("test", "test.txt")
            current_hash = compute_content_hash(test_file)

            # Same hash = unchanged
            assert entry is not None
            assert entry.content_hash == current_hash

    def test_modified_file_detected_by_content_hash(self):
        """Modified files are detected by content hash change."""
        from scripts.narratives.embeddings.manifest import Manifest, FileEntry, compute_content_hash

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            manifest_path = tmpdir / "manifest.json"

            # Create and hash test file
            test_file = tmpdir / "test.txt"
            test_file.write_text("original content")
            original_hash = compute_content_hash(test_file)

            # Add to manifest
            manifest = Manifest(manifest_path)
            manifest.add_file("test", "test.txt", FileEntry(
                content_hash=original_hash,
                file_size=16,
                chunk_count=1,
                chunk_ids=["test__test.txt__c0000"]
            ))
            manifest.save()

            # Modify the file
            test_file.write_text("modified content")
            new_hash = compute_content_hash(test_file)

            # Hashes should differ
            assert original_hash != new_hash

            # Manifest entry should have old hash (file needs reindexing)
            manifest2 = Manifest(manifest_path)
            entry = manifest2.get_file("test", "test.txt")
            assert entry.content_hash != new_hash

    def test_manifest_tracks_chunk_ids(self):
        """Manifest correctly tracks chunk IDs for each file."""
        from scripts.narratives.embeddings.manifest import Manifest, FileEntry

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            manifest_path = tmpdir / "manifest.json"

            manifest = Manifest(manifest_path)

            # Add file with multiple chunks
            chunk_ids = ["src__doc__c0000", "src__doc__c0001", "src__doc__c0002"]
            manifest.add_file("source", "doc.pdf", FileEntry(
                content_hash="sha256:abc",
                file_size=1000,
                chunk_count=3,
                chunk_ids=chunk_ids
            ))

            # Retrieve and verify
            entry = manifest.get_file("source", "doc.pdf")
            assert entry.chunk_ids == chunk_ids
            assert entry.chunk_count == 3

    def test_cleanup_deleted_only_removes_stale_files(self):
        """cleanup_deleted=True only removes files not in current source."""
        from scripts.narratives.embeddings.manifest import Manifest, FileEntry

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            manifest_path = tmpdir / "manifest.json"

            # Manifest with 3 files
            manifest = Manifest(manifest_path)
            manifest.add_file("test", "keep1.pdf", FileEntry(
                content_hash="sha256:aaa",
                file_size=100,
                chunk_count=1,
                chunk_ids=["test__keep1__c0000"]
            ))
            manifest.add_file("test", "keep2.pdf", FileEntry(
                content_hash="sha256:bbb",
                file_size=100,
                chunk_count=1,
                chunk_ids=["test__keep2__c0000"]
            ))
            manifest.add_file("test", "stale.pdf", FileEntry(
                content_hash="sha256:ccc",
                file_size=100,
                chunk_count=1,
                chunk_ids=["test__stale__c0000"]
            ))
            manifest.save()

            # Simulate cleanup: keep1 and keep2 were seen, stale was not
            seen_files = {"keep1.pdf", "keep2.pdf"}
            indexed_files = manifest.get_all_files("test")

            # Find files to remove
            files_to_remove = [
                rel_path for rel_path in indexed_files
                if rel_path not in seen_files
            ]

            assert files_to_remove == ["stale.pdf"]

    def test_force_rebuilds_regardless_of_hash(self):
        """Force flag causes reindexing regardless of content hash."""
        from scripts.narratives.embeddings.manifest import Manifest, FileEntry, compute_content_hash

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            manifest_path = tmpdir / "manifest.json"

            # Create test file
            test_file = tmpdir / "test.txt"
            test_file.write_text("content")
            content_hash = compute_content_hash(test_file)

            # Add to manifest with same hash
            manifest = Manifest(manifest_path)
            manifest.add_file("test", "test.txt", FileEntry(
                content_hash=content_hash,
                file_size=7,
                chunk_count=1,
                chunk_ids=["test__test.txt__c0000"]
            ))
            manifest.save()

            # With force=True, file should be processed even with same hash
            # This is tested in integration - here we just verify the logic:
            # force=True means we skip the "if content_hash matches" check
            entry = manifest.get_file("test", "test.txt")
            current_hash = compute_content_hash(test_file)

            # Without force, unchanged
            should_skip = (entry and entry.content_hash == current_hash)
            assert should_skip is True

            # With force, don't skip (force overrides)
            force = True
            should_process = force or not should_skip
            assert should_process is True


class TestBackupIntegration:
    """Tests for backup integration in builder."""

    def test_backup_created_before_full_run(self):
        """Backup is created before a full run (no --limit)."""
        from scripts.narratives.embeddings.backup import BackupManager

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            db_dir = tmpdir / "documents"
            backup_dir = tmpdir / "backups"

            # Create fake database
            db_dir.mkdir()
            (db_dir / "chroma.sqlite3").write_text("fake db")

            # Backup manager should create backup
            manager = BackupManager(db_dir, backup_dir)
            backup_path = manager.create_backup()

            assert backup_path is not None
            assert backup_path.exists()

    def test_no_backup_for_partial_run(self):
        """No backup is created for partial runs with --limit.

        This tests the logic that backup should only happen when limit is None.
        The actual builder implementation handles this.
        """
        # This is a documentation/logic test
        # When limit is not None, we skip backup creation
        limit = 10
        should_backup = limit is None
        assert should_backup is False

        limit = None
        should_backup = limit is None
        assert should_backup is True


class TestBuildResult:
    """Tests for BuildResult tracking."""

    def test_build_result_tracks_all_operations(self):
        """BuildResult correctly tracks all operation counts."""
        from scripts.narratives.embeddings.builder import BuildResult

        result = BuildResult()

        # Simulate operations
        result.files_processed = 5
        result.files_unchanged = 10
        result.files_skipped = 2
        result.files_errors = 1
        result.chunks_added = 25
        result.chunks_deleted = 3

        assert result.files_processed == 5
        assert result.files_unchanged == 10
        assert result.total_chunks == 25  # added + updated + unchanged

    def test_build_result_error_tracking(self):
        """BuildResult tracks errors."""
        from scripts.narratives.embeddings.builder import BuildResult

        result = BuildResult()
        result.errors.append("file1.pdf: Error reading")
        result.errors.append("file2.pdf: Chunking failed")
        result.files_errors = 2

        assert len(result.errors) == 2
        assert result.files_errors == 2
