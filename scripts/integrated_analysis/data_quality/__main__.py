#!/usr/bin/env python3
"""
Data Quality Check Framework - Entry Point.

Run comprehensive dimension coverage report:
    python -m scripts.integrated_analysis.data_quality
    python -m scripts.integrated_analysis.data_quality.dimension_coverage
"""

from scripts.integrated_analysis.data_quality.dimension_coverage.__main__ import main

if __name__ == "__main__":
    main()
