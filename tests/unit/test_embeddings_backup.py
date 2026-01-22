"""Tests for embeddings backup operations."""

import tarfile
import tempfile
from pathlib import Path
from datetime import datetime, timedelta

import pytest


class TestBackupManager:
    """Tests for BackupManager class."""

    def test_create_backup(self):
        """Can create a backup of the database."""
        from scripts.narratives.embeddings.backup import BackupManager

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            db_dir = tmpdir / "documents"
            backup_dir = tmpdir / "backups"

            # Create fake database
            db_dir.mkdir()
            (db_dir / "chroma.sqlite3").write_text("fake db")
            (db_dir / "index").mkdir()
            (db_dir / "index" / "vectors.bin").write_bytes(b"fake vectors")

            manager = BackupManager(db_dir, backup_dir, max_backups=3)
            backup_path = manager.create_backup()

            assert backup_path.exists()
            assert backup_path.suffix == ".gz"
            assert "backup-" in backup_path.name

    def test_backup_contains_all_files(self):
        """Backup archive contains all database files."""
        from scripts.narratives.embeddings.backup import BackupManager

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            db_dir = tmpdir / "documents"
            backup_dir = tmpdir / "backups"

            db_dir.mkdir()
            (db_dir / "chroma.sqlite3").write_text("fake db")

            manager = BackupManager(db_dir, backup_dir, max_backups=3)
            backup_path = manager.create_backup()

            # Check archive contents
            with tarfile.open(backup_path, "r:gz") as tar:
                names = tar.getnames()
                assert any("chroma.sqlite3" in n for n in names)

    def test_rotate_old_backups(self):
        """Old backups are rotated when max exceeded."""
        from scripts.narratives.embeddings.backup import BackupManager

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            db_dir = tmpdir / "documents"
            backup_dir = tmpdir / "backups"

            db_dir.mkdir()
            (db_dir / "chroma.sqlite3").write_text("fake db")

            manager = BackupManager(db_dir, backup_dir, max_backups=2)

            # Create 3 backups
            for i in range(3):
                manager.create_backup()

            # Should only have 2 backups
            backups = list(backup_dir.glob("backup-*.tar.gz"))
            assert len(backups) == 2

    def test_list_backups(self):
        """Can list available backups sorted by date."""
        from scripts.narratives.embeddings.backup import BackupManager

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            db_dir = tmpdir / "documents"
            backup_dir = tmpdir / "backups"

            db_dir.mkdir()
            (db_dir / "chroma.sqlite3").write_text("fake db")

            manager = BackupManager(db_dir, backup_dir, max_backups=5)
            manager.create_backup()
            manager.create_backup()

            backups = manager.list_backups()
            assert len(backups) == 2
            # Most recent first
            assert backups[0].stat().st_mtime >= backups[1].stat().st_mtime

    def test_restore_backup(self):
        """Can restore from a backup."""
        from scripts.narratives.embeddings.backup import BackupManager

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            db_dir = tmpdir / "documents"
            backup_dir = tmpdir / "backups"

            # Create original database
            db_dir.mkdir()
            (db_dir / "chroma.sqlite3").write_text("original content")

            manager = BackupManager(db_dir, backup_dir, max_backups=3)
            backup_path = manager.create_backup()

            # Corrupt the database
            (db_dir / "chroma.sqlite3").write_text("corrupted!")

            # Restore
            manager.restore_backup(backup_path)

            # Should have original content
            assert (db_dir / "chroma.sqlite3").read_text() == "original content"

    def test_no_backup_if_db_empty(self):
        """Don't create backup if database doesn't exist."""
        from scripts.narratives.embeddings.backup import BackupManager

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            db_dir = tmpdir / "documents"  # Doesn't exist
            backup_dir = tmpdir / "backups"

            manager = BackupManager(db_dir, backup_dir, max_backups=3)
            backup_path = manager.create_backup()

            assert backup_path is None

    def test_no_backup_if_db_dir_empty(self):
        """Don't create backup if database directory exists but is empty."""
        from scripts.narratives.embeddings.backup import BackupManager

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            db_dir = tmpdir / "documents"
            backup_dir = tmpdir / "backups"

            db_dir.mkdir()  # Exists but empty

            manager = BackupManager(db_dir, backup_dir, max_backups=3)
            backup_path = manager.create_backup()

            assert backup_path is None

    def test_restore_nonexistent_backup_raises(self):
        """Restore raises FileNotFoundError for missing backup."""
        from scripts.narratives.embeddings.backup import BackupManager

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            db_dir = tmpdir / "documents"
            backup_dir = tmpdir / "backups"

            manager = BackupManager(db_dir, backup_dir, max_backups=3)

            with pytest.raises(FileNotFoundError):
                manager.restore_backup(backup_dir / "nonexistent.tar.gz")

    def test_get_latest_backup(self):
        """Can get the most recent backup."""
        from scripts.narratives.embeddings.backup import BackupManager

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            db_dir = tmpdir / "documents"
            backup_dir = tmpdir / "backups"

            db_dir.mkdir()
            (db_dir / "chroma.sqlite3").write_text("fake db")

            manager = BackupManager(db_dir, backup_dir, max_backups=5)
            manager.create_backup()
            manager.create_backup()

            latest = manager.get_latest_backup()
            # Latest should be one of the existing backups
            all_backups = manager.list_backups()
            assert latest is not None
            assert latest in all_backups
            assert latest == all_backups[0]  # Should be first (newest)

    def test_get_latest_backup_when_none(self):
        """get_latest_backup returns None when no backups exist."""
        from scripts.narratives.embeddings.backup import BackupManager

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            db_dir = tmpdir / "documents"
            backup_dir = tmpdir / "backups"

            manager = BackupManager(db_dir, backup_dir, max_backups=3)

            assert manager.get_latest_backup() is None

    def test_restore_preserves_nested_structure(self):
        """Restore preserves nested directory structure."""
        from scripts.narratives.embeddings.backup import BackupManager

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            db_dir = tmpdir / "documents"
            backup_dir = tmpdir / "backups"

            # Create nested structure
            db_dir.mkdir()
            (db_dir / "chroma.sqlite3").write_text("database")
            (db_dir / "subdir").mkdir()
            (db_dir / "subdir" / "nested.bin").write_bytes(b"nested data")

            manager = BackupManager(db_dir, backup_dir, max_backups=3)
            backup_path = manager.create_backup()

            # Delete everything
            import shutil
            shutil.rmtree(db_dir)

            # Restore
            manager.restore_backup(backup_path)

            # Verify nested structure restored
            assert (db_dir / "chroma.sqlite3").read_text() == "database"
            assert (db_dir / "subdir" / "nested.bin").read_bytes() == b"nested data"
