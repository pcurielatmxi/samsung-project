"""Loader for file-based outputs (CSV, Parquet, JSON)."""
from typing import Any, List, Dict, Optional
import logging
import json
from pathlib import Path
import pandas as pd

from src.loaders.base_loader import BaseLoader
from src.config.settings import settings

logger = logging.getLogger(__name__)


class FileLoader(BaseLoader):
    """Load data to file formats (CSV, Parquet, JSON)."""

    def __init__(self):
        """Initialize file loader."""
        super().__init__('file')
        self.file_path = None
        self.loaded_count = 0

    def load(
        self,
        data: List[Dict[str, Any]],
        file_path: Optional[str] = None,
        format: str = 'csv',
        **kwargs,
    ) -> bool:
        """
        Load data to a file.

        Args:
            data: List of dictionaries to load
            file_path: Output file path (relative to output_dir if not absolute)
            format: File format ('csv', 'parquet', 'json')
            **kwargs: Additional parameters passed to writer

        Returns:
            True if load successful
        """
        if not data:
            self.logger.warning('No data to load')
            return False

        try:
            # Resolve file path
            if not file_path:
                raise ValueError('file_path is required')

            path = Path(file_path)
            if not path.is_absolute():
                path = settings.OUTPUT_DATA_DIR / path

            # Create parent directories
            path.parent.mkdir(parents=True, exist_ok=True)

            # Load based on format
            loaders = {
                'csv': self._load_csv,
                'parquet': self._load_parquet,
                'json': self._load_json,
            }

            if format not in loaders:
                raise ValueError(f'Unsupported format: {format}')

            loaders[format](data, path, **kwargs)
            self.loaded_count = len(data)
            self.file_path = str(path)

            self.logger.info(
                f'Successfully loaded {self.loaded_count} records to {path}'
            )
            return True

        except Exception as e:
            self.logger.error(f'Load failed: {str(e)}')
            return False

    def _load_csv(
        self,
        data: List[Dict[str, Any]],
        file_path: Path,
        **kwargs,
    ) -> None:
        """Load data as CSV."""
        df = pd.DataFrame(data)
        df.to_csv(file_path, index=False, **kwargs)

    def _load_parquet(
        self,
        data: List[Dict[str, Any]],
        file_path: Path,
        **kwargs,
    ) -> None:
        """Load data as Parquet."""
        df = pd.DataFrame(data)
        df.to_parquet(file_path, index=False, **kwargs)

    def _load_json(
        self,
        data: List[Dict[str, Any]],
        file_path: Path,
        **kwargs,
    ) -> None:
        """Load data as JSON."""
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2, default=str)

    def validate_load(self, record_count: int) -> bool:
        """
        Validate that data was loaded.

        Args:
            record_count: Expected number of records

        Returns:
            True if loaded record count matches
        """
        return self.loaded_count == record_count

    def get_load_stats(self) -> Dict[str, Any]:
        """Get load statistics."""
        stats = super().get_load_stats()
        stats.update({
            'file_path': self.file_path,
            'loaded_count': self.loaded_count,
        })
        return stats
