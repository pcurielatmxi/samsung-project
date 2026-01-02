#!/bin/bash
# PSI Document Processing Pipeline
# Usage: ./run.sh <command> [options]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
PIPELINE="$PROJECT_ROOT/scripts/document_processor_v2/pipeline.py"
STATUS="$PROJECT_ROOT/scripts/document_processor_v2/status.py"
CONFIG_DIR="$SCRIPT_DIR"

# Activate virtual environment
source "$PROJECT_ROOT/.venv/bin/activate"

case "${1:-help}" in
    extract)
        # Run Stage 1 only (Gemini extraction)
        shift
        python "$PIPELINE" "$CONFIG_DIR" --stage 1 "$@"
        ;;
    format)
        # Run Stage 2 only (Claude formatting)
        shift
        python "$PIPELINE" "$CONFIG_DIR" --stage 2 "$@"
        ;;
    run)
        # Run both stages
        shift
        python "$PIPELINE" "$CONFIG_DIR" "$@"
        ;;
    status)
        # Show pipeline status
        shift
        python "$STATUS" "$CONFIG_DIR" "$@"
        ;;
    retry)
        # Retry failed files
        shift
        python "$PIPELINE" "$CONFIG_DIR" --retry-errors "$@"
        ;;
    test)
        # Test with limited files
        shift
        python "$PIPELINE" "$CONFIG_DIR" --limit "${1:-5}" --dry-run
        ;;
    help|*)
        echo "PSI Document Processing Pipeline"
        echo ""
        echo "Usage: ./run.sh <command> [options]"
        echo ""
        echo "Commands:"
        echo "  extract     Run Stage 1 only (Gemini extraction)"
        echo "  format      Run Stage 2 only (Claude formatting)"
        echo "  run         Run both stages"
        echo "  status      Show pipeline status"
        echo "  retry       Retry failed files"
        echo "  test [n]    Dry run with n files (default: 5)"
        echo ""
        echo "Options (passed to pipeline):"
        echo "  --limit N       Process only N files"
        echo "  --force         Reprocess existing files"
        echo "  --dry-run       Show what would be processed"
        echo "  --errors        Show error details (status only)"
        echo ""
        echo "Examples:"
        echo "  ./run.sh status              # Check progress"
        echo "  ./run.sh test 10             # Dry run 10 files"
        echo "  ./run.sh extract --limit 50  # Extract 50 files"
        echo "  ./run.sh format              # Format all extracted"
        echo "  ./run.sh retry               # Retry failures"
        ;;
esac
