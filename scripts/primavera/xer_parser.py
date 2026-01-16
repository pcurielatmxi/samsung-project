"""
XER File Parser for Primavera P6 Schedule Data

This module provides utilities to parse Primavera P6 XER files and convert them
to various formats (CSV, pandas DataFrames, etc.)

XER Format:
- Tab-delimited text files
- Structure: ERMHDR (header) followed by %T (table), %F (fields), %R (rows)
- Each table represents a different entity (tasks, resources, calendars, etc.)
"""

import re
from typing import Dict, List, Optional, TextIO
from pathlib import Path
import pandas as pd


class XERParser:
    """Parse Primavera P6 XER files into structured data"""

    def __init__(self, file_path: str):
        """
        Initialize the parser with an XER file path

        Args:
            file_path: Path to the XER file
        """
        self.file_path = Path(file_path)
        self.tables: Dict[str, pd.DataFrame] = {}
        self.header: Dict[str, str] = {}

    def parse(self) -> Dict[str, pd.DataFrame]:
        """
        Parse the XER file and return all tables as DataFrames

        Returns:
            Dictionary mapping table names to pandas DataFrames
        """
        with open(self.file_path, 'r', encoding='utf-8', errors='ignore') as f:
            self._parse_header(f)
            self._parse_tables(f)

        return self.tables

    def _parse_header(self, file: TextIO) -> None:
        """Parse the ERMHDR header section"""
        line = file.readline()
        if line.startswith('ERMHDR'):
            # Header format: ERMHDR\tfield1\tfield2\t...
            parts = line.strip().split('\t')
            # Store header info (version, date, etc.)
            self.header['raw'] = line.strip()
            if len(parts) > 1:
                self.header['data'] = parts[1:]

    def _parse_tables(self, file: TextIO) -> None:
        """Parse all tables in the XER file"""
        current_table = None
        current_fields = []
        current_rows = []

        for line in file:
            line = line.strip()

            if not line:
                continue

            if line.startswith('%T'):
                # Save previous table if exists
                if current_table and current_fields and current_rows:
                    self._save_table(current_table, current_fields, current_rows)

                # Start new table
                parts = line.split('\t')
                current_table = parts[1] if len(parts) > 1 else None
                current_fields = []
                current_rows = []

            elif line.startswith('%F'):
                # Field definitions
                parts = line.split('\t')
                current_fields = parts[1:] if len(parts) > 1 else []

            elif line.startswith('%R'):
                # Data row
                parts = line.split('\t')
                row_data = parts[1:] if len(parts) > 1 else []
                current_rows.append(row_data)

            elif line.startswith('%E'):
                # End of current table
                if current_table and current_fields and current_rows:
                    self._save_table(current_table, current_fields, current_rows)
                current_table = None
                current_fields = []
                current_rows = []

        # Save last table if not ended with %E
        if current_table and current_fields and current_rows:
            self._save_table(current_table, current_fields, current_rows)

    def _save_table(self, table_name: str, fields: List[str], rows: List[List[str]]) -> None:
        """Convert table data to DataFrame and store"""
        try:
            # Ensure all rows have same length as fields
            normalized_rows = []
            for row in rows:
                # Pad short rows, truncate long rows
                if len(row) < len(fields):
                    row = row + [''] * (len(fields) - len(row))
                elif len(row) > len(fields):
                    row = row[:len(fields)]
                normalized_rows.append(row)

            df = pd.DataFrame(normalized_rows, columns=fields)
            self.tables[table_name] = df
        except Exception as e:
            print(f"Warning: Could not parse table {table_name}: {e}")

    def get_table(self, table_name: str) -> Optional[pd.DataFrame]:
        """
        Get a specific table by name

        Args:
            table_name: Name of the table to retrieve

        Returns:
            DataFrame or None if table doesn't exist
        """
        return self.tables.get(table_name)

    def list_tables(self) -> List[str]:
        """
        Get list of all available table names

        Returns:
            List of table names
        """
        return list(self.tables.keys())

    def export_table_to_csv(self, table_name: str, output_path: str) -> None:
        """
        Export a specific table to CSV

        Args:
            table_name: Name of the table to export
            output_path: Path for the output CSV file
        """
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' not found in XER file")

        df = self.tables[table_name]
        df.to_csv(output_path, index=False)
        print(f"Exported {len(df)} rows to {output_path}")

    def export_all_to_csv(self, output_dir: str) -> None:
        """
        Export all tables to separate CSV files

        Args:
            output_dir: Directory to save CSV files
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        for table_name, df in self.tables.items():
            csv_path = output_path / f"{table_name}.csv"
            df.to_csv(csv_path, index=False)
            print(f"Exported {table_name}: {len(df)} rows to {csv_path}")

    def get_tasks(self) -> Optional[pd.DataFrame]:
        """
        Get the TASK table (most commonly used)

        Returns:
            DataFrame containing task data or None
        """
        return self.get_table('TASK')

    def get_projects(self) -> Optional[pd.DataFrame]:
        """
        Get the PROJECT table

        Returns:
            DataFrame containing project data or None
        """
        return self.get_table('PROJECT')

    def get_resources(self) -> Optional[pd.DataFrame]:
        """
        Get the RSRC (Resource) table

        Returns:
            DataFrame containing resource data or None
        """
        return self.get_table('RSRC')

    def get_calendars(self) -> Optional[pd.DataFrame]:
        """
        Get the CALENDAR table

        Returns:
            DataFrame containing calendar data or None
        """
        return self.get_table('CALENDAR')

    def summary(self) -> Dict:
        """
        Get a summary of the XER file contents

        Returns:
            Dictionary with summary statistics
        """
        summary = {
            'file_path': str(self.file_path),
            'total_tables': len(self.tables),
            'tables': {}
        }

        for table_name, df in self.tables.items():
            summary['tables'][table_name] = {
                'rows': len(df),
                'columns': len(df.columns),
                'column_names': list(df.columns)
            }

        return summary


def quick_parse(file_path: str) -> Dict[str, pd.DataFrame]:
    """
    Quick utility function to parse an XER file

    Args:
        file_path: Path to the XER file

    Returns:
        Dictionary of table names to DataFrames
    """
    parser = XERParser(file_path)
    return parser.parse()
