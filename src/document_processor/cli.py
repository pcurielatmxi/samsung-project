"""
Unified CLI for N-stage document processing pipeline.

Usage:
    python -m src.document_processor <config_dir> [options]

Commands:
    (default)   Run pipeline
    status      Show pipeline status

Options:
    --stage NAME        Run specific stage (can repeat)
    --force             Reprocess completed files
    --retry-errors      Retry failed files only
    --limit N           Process only N files per stage
    --dry-run           Show what would be processed
    --bypass-qc-halt    Continue despite QC halt file
    --disable-qc        Skip quality checks entirely
    --status            Show status instead of running
    --errors            Show error details (with --status)
    --verbose           Show per-file status (with --status)
    --json              Output as JSON (with --status)
"""

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path

from .config import load_config, print_config, ConfigValidationError
from .pipeline import run_pipeline
from .quality_check import check_qc_halt, clear_qc_halt, get_qc_halt_path
from .utils.status import analyze_status, print_status, status_to_dict


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S",
    )


def cmd_run(args, config) -> int:
    """Run the pipeline."""
    # Parse stages argument
    stages = args.stage if args.stage else None

    # Run pipeline
    result = asyncio.run(run_pipeline(
        config=config,
        stages=stages,
        force=args.force,
        retry_errors=args.retry_errors,
        limit=args.limit,
        dry_run=args.dry_run,
        bypass_qc_halt=args.bypass_qc_halt,
        disable_qc=args.disable_qc,
        verbose=args.verbose,
    ))

    if result.get("halted"):
        return 1

    return 0


def cmd_status(args, config) -> int:
    """Show pipeline status."""
    status = analyze_status(config)

    if args.json:
        print(json.dumps(status_to_dict(status), indent=2))
    else:
        print_status(status, show_errors=args.errors, verbose=args.verbose)

    # Also show QC halt status if present
    halt_data = check_qc_halt(config.output_dir)
    if halt_data and not args.json:
        print()
        print("WARNING: QC halt file present")
        print(f"  Stage: {halt_data.get('stage', 'unknown')}")
        print(f"  Message: {halt_data.get('message', 'Unknown')}")

    return 0


def cmd_clear_halt(args, config) -> int:
    """Clear QC halt file."""
    if clear_qc_halt(config.output_dir):
        print("QC halt file removed")
        return 0
    else:
        print("No QC halt file found")
        return 1


def main() -> int:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="N-stage document processing pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "config_dir",
        help="Path to config folder containing config.json",
    )
    parser.add_argument(
        "--stage",
        action="append",
        dest="stage",
        metavar="NAME",
        help="Stage(s) to run (can repeat, default: all)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reprocess completed files",
    )
    parser.add_argument(
        "--retry-errors",
        action="store_true",
        help="Retry failed files only",
    )
    parser.add_argument(
        "--limit",
        type=int,
        metavar="N",
        help="Process only N files per stage",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be processed",
    )
    parser.add_argument(
        "--bypass-qc-halt",
        action="store_true",
        help="Continue despite QC halt file",
    )
    parser.add_argument(
        "--disable-qc",
        action="store_true",
        help="Skip quality checks entirely",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show pipeline status instead of running",
    )
    parser.add_argument(
        "--clear-halt",
        action="store_true",
        help="Remove QC halt file",
    )
    parser.add_argument(
        "--errors",
        action="store_true",
        help="Show error details (with --status)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose output",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output as JSON (with --status)",
    )
    parser.add_argument(
        "--show-config",
        action="store_true",
        help="Print config and exit",
    )

    args = parser.parse_args()

    setup_logging(args.verbose)

    # Load config
    try:
        config = load_config(args.config_dir)
    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    except ConfigValidationError as e:
        print(f"ERROR: Config validation failed:", file=sys.stderr)
        for err in e.errors:
            print(f"  - {err}", file=sys.stderr)
        return 1

    # Show config if requested
    if args.show_config:
        print_config(config)
        return 0

    # Dispatch command
    if args.clear_halt:
        return cmd_clear_halt(args, config)
    elif args.status:
        return cmd_status(args, config)
    else:
        return cmd_run(args, config)


if __name__ == "__main__":
    sys.exit(main())
