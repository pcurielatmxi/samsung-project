"""Backup and restore for embeddings database.

Provides rotating backups before destructive operations
to prevent data loss from corruption or failed updates.
"""

import shutil
import tarfile
from datetime import datetime
from pathlib import Path
from typing import List, Optional


class BackupManager:
    """Manages backups for the embeddings database."""

    def __init__(
        self,
        db_path: Path,
        backup_dir: Path,
        max_backups: int = 5
    ):
        """Initialize backup manager.

        Args:
            db_path: Path to ChromaDB database directory.
            backup_dir: Directory to store backups.
            max_backups: Maximum number of backups to retain.
        """
        self.db_path = db_path
        self.backup_dir = backup_dir
        self.max_backups = max_backups

    def create_backup(self) -> Optional[Path]:
        """Create a backup of the current database.

        Returns:
            Path to backup file, or None if nothing to backup.
        """
        if not self.db_path.exists() or not any(self.db_path.iterdir()):
            return None

        self.backup_dir.mkdir(parents=True, exist_ok=True)

        # Generate timestamped filename with microseconds for uniqueness
        timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S-%f")
        backup_name = f"backup-{timestamp}.tar.gz"
        backup_path = self.backup_dir / backup_name

        # Create compressed archive
        with tarfile.open(backup_path, "w:gz") as tar:
            tar.add(self.db_path, arcname="documents")

        # Rotate old backups
        self._rotate_backups()

        return backup_path

    def _rotate_backups(self) -> None:
        """Remove old backups exceeding max_backups."""
        backups = self.list_backups()

        # Delete oldest backups exceeding limit
        while len(backups) > self.max_backups:
            oldest = backups.pop()  # Last item is oldest
            oldest.unlink()

    def list_backups(self) -> List[Path]:
        """List available backups, newest first."""
        if not self.backup_dir.exists():
            return []

        backups = list(self.backup_dir.glob("backup-*.tar.gz"))
        # Sort by modification time, newest first
        backups.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return backups

    def restore_backup(self, backup_path: Path) -> None:
        """Restore database from a backup.

        Args:
            backup_path: Path to backup archive to restore.
        """
        if not backup_path.exists():
            raise FileNotFoundError(f"Backup not found: {backup_path}")

        # Remove current database
        if self.db_path.exists():
            shutil.rmtree(self.db_path)

        # Extract backup
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        with tarfile.open(backup_path, "r:gz") as tar:
            # Extract to parent dir (archive contains "documents" folder)
            tar.extractall(self.db_path.parent, filter="data")

    def get_latest_backup(self) -> Optional[Path]:
        """Get path to most recent backup."""
        backups = self.list_backups()
        return backups[0] if backups else None
