"""
CLI entry point for dimension coverage check.

Usage:
    python -m scripts.integrated_analysis.data_quality.dimension_coverage
    python -m scripts.integrated_analysis.data_quality.dimension_coverage --verbose
    python -m scripts.integrated_analysis.data_quality.dimension_coverage --output report.md
"""

import argparse

from scripts.integrated_analysis.data_quality.dimension_coverage import (
    check_dimension_coverage,
)


def main():
    parser = argparse.ArgumentParser(
        description='Comprehensive dimension coverage quality check',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Basic report
    python -m scripts.integrated_analysis.data_quality.dimension_coverage

    # Save markdown report
    python -m scripts.integrated_analysis.data_quality.dimension_coverage -o coverage.md

    # Verbose output
    python -m scripts.integrated_analysis.data_quality.dimension_coverage -v
        """,
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Show detailed output',
    )
    parser.add_argument(
        '--output', '-o',
        type=str,
        help='Output markdown report file',
    )
    args = parser.parse_args()

    check_dimension_coverage(verbose=args.verbose, output_file=args.output)


if __name__ == "__main__":
    main()
