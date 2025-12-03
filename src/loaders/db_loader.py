"""Loader for relational databases (PostgreSQL, etc.)."""
from typing import Any, List, Dict, Optional
import logging
import psycopg2
from psycopg2.extras import execute_values

from src.loaders.base_loader import BaseLoader
from src.config.settings import settings

logger = logging.getLogger(__name__)


class DatabaseLoader(BaseLoader):
    """Load data into PostgreSQL or other SQL databases."""

    def __init__(self):
        """Initialize database loader."""
        super().__init__('database')
        self.conn = None
        self.cursor = None
        self.loaded_count = 0

    def _connect(self) -> None:
        """Connect to the database."""
        try:
            self.conn = psycopg2.connect(
                host=settings.DB_HOST,
                port=settings.DB_PORT,
                database=settings.DB_NAME,
                user=settings.DB_USER,
                password=settings.DB_PASSWORD,
            )
            self.cursor = self.conn.cursor()
            self.logger.info('Connected to database')
        except psycopg2.Error as e:
            self.logger.error(f'Database connection failed: {str(e)}')
            raise

    def _disconnect(self) -> None:
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            self.logger.info('Disconnected from database')

    def load(
        self,
        data: List[Dict[str, Any]],
        table_name: str,
        if_exists: str = 'append',
        **kwargs,
    ) -> bool:
        """
        Load data into a database table.

        Args:
            data: List of dictionaries to load
            table_name: Target table name
            if_exists: What to do if table exists ('append', 'replace', 'fail')
            **kwargs: Additional parameters

        Returns:
            True if load successful
        """
        if not data:
            self.logger.warning('No data to load')
            return False

        try:
            self._connect()
            self.loaded_count = self._insert_records(
                data,
                table_name,
            )
            self.conn.commit()
            self.logger.info(
                f'Successfully loaded {self.loaded_count} records into {table_name}'
            )
            return True
        except Exception as e:
            if self.conn:
                self.conn.rollback()
            self.logger.error(f'Load failed: {str(e)}')
            return False
        finally:
            self._disconnect()

    def _insert_records(
        self,
        data: List[Dict[str, Any]],
        table_name: str,
    ) -> int:
        """
        Insert records into a table.

        Args:
            data: List of dictionaries to insert
            table_name: Target table name

        Returns:
            Number of records inserted
        """
        if not data:
            return 0

        # Get columns from first record
        columns = list(data[0].keys())
        placeholders = ','.join(['%s'] * len(columns))
        insert_query = (
            f'INSERT INTO {table_name} ({",".join(columns)}) '
            f'VALUES ({placeholders}) ON CONFLICT DO NOTHING'
        )

        # Prepare values tuples
        values = [tuple(record.get(col) for col in columns) for record in data]

        try:
            execute_values(
                self.cursor,
                insert_query,
                values,
                page_size=1000,
            )
            return len(values)
        except Exception as e:
            self.logger.error(f'Insert error: {str(e)}')
            raise

    def validate_load(self, record_count: int) -> bool:
        """
        Validate that data was loaded.

        Args:
            record_count: Expected number of records

        Returns:
            True if loaded record count matches
        """
        return self.loaded_count == record_count
