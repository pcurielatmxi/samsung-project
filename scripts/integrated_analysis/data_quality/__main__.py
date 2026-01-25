#!/usr/bin/env python3
"""
Data Quality Check Framework - Entry Point.

Run all checks:
    python -m scripts.integrated_analysis.data_quality

Run specific check:
    python -m scripts.integrated_analysis.data_quality.check_csi_coverage
    python -m scripts.integrated_analysis.data_quality.check_location_coverage
    python -m scripts.integrated_analysis.data_quality.check_company_coverage
"""

from scripts.integrated_analysis.data_quality.run_all_checks import main

if __name__ == "__main__":
    main()
