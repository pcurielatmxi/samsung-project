#!/bin/bash
# Narratives Document Processing Pipeline
# Usage: ./run.sh <command> [options]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
CONFIG_DIR="$SCRIPT_DIR"

# Activate virtual environment
source "$PROJECT_ROOT/.venv/bin/activate"

case "${1:-help}" in
    extract|run)
        # Run extract stage (only stage for narratives)
        shift
        python -m src.document_processor "$CONFIG_DIR" "$@"
        ;;
    status)
        # Show pipeline status
        shift
        python -m src.document_processor "$CONFIG_DIR" --status "$@"
        ;;
    retry)
        # Retry failed files
        shift
        python -m src.document_processor "$CONFIG_DIR" --retry-errors "$@"
        ;;
    test)
        # Test with limited files
        shift
        python -m src.document_processor "$CONFIG_DIR" --limit "${1:-5}" --dry-run
        ;;
    help|*)
        echo "Narratives Document Processing Pipeline"
        echo ""
        echo "Usage: ./run.sh <command> [options]"
        echo ""
        echo "Commands:"
        echo "  extract     Run extraction (alias: run)"
        echo "  run         Run all stages"
        echo "  status      Show pipeline status"
        echo "  retry       Retry failed files"
        echo "  test [n]    Dry run with n files (default: 5)"
        echo ""
        echo "Options (passed to pipeline):"
        echo "  --limit N           Process only N files"
        echo "  --force             Reprocess completed files"
        echo "  --dry-run           Show what would be processed"
        echo "  --retry-errors      Retry failed files only"
        echo "  --bypass-qc-halt    Continue despite QC halt"
        echo "  --disable-qc        Skip quality checks"
        echo "  --errors            Show error details (status only)"
        echo "  --verbose           Verbose output"
        echo ""
        echo "Examples:"
        echo "  ./run.sh status              # Check progress"
        echo "  ./run.sh test 10             # Dry run 10 files"
        echo "  ./run.sh extract --limit 50  # Extract 50 files"
        echo "  ./run.sh retry               # Retry failures"
        ;;
esac
