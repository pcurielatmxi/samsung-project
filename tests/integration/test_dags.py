"""Integration tests for DAGs."""
import pytest
from pathlib import Path


class TestDAGDefinitions:
    """Test that DAGs are properly defined."""

    def test_dags_directory_exists(self):
        """Test that dags directory exists."""
        dags_dir = Path(__file__).parent.parent.parent / 'dags'
        assert dags_dir.exists()

    def test_test_dag_exists(self):
        """Test that test_dag.py exists."""
        test_dag_file = Path(__file__).parent.parent.parent / 'dags' / 'test_dag.py'
        assert test_dag_file.exists()
