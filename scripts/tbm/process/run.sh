#!/bin/bash
# TBM Daily Plans Processing Pipeline
# Usage: ./run.sh <command> [options]
#
# Pipeline stages:
#   0. extract-eml - Extract Excel attachments from EML files (one-time)
#   1. parse       - Parse Excel files -> work_entries.csv
#   2. consolidate - Add dimension IDs (location, company) + CSI inference
#   3. dedup       - Flag duplicate company+date combinations
#
# The 'all' command runs stages 1-3 in sequence.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Activate virtual environment
source "$PROJECT_ROOT/.venv/bin/activate"

case "${1:-help}" in
    extract-eml)
        # Extract Excel attachments from EML files
        shift
        echo "=== Extracting Excel from EML Files ==="
        cd "$PROJECT_ROOT" && python -m scripts.tbm.process.extract_eml_attachments "$@"
        ;;
    sync)
        # Sync files from field team's folder
        shift
        echo "=== Syncing Field TBM Files ==="
        cd "$PROJECT_ROOT" && python -m scripts.tbm.process.sync_field_tbm "$@"
        ;;
    parse)
        # Stage 1: Parse TBM Excel files
        shift
        echo "=== Stage 1: Parsing TBM Daily Plans ==="
        cd "$PROJECT_ROOT" && python -m scripts.tbm.process.parse_tbm_daily_plans "$@"
        ;;
    consolidate)
        # Stage 2: Add dimension IDs + CSI inference
        shift
        echo "=== Stage 2: Consolidating (Dimensions + CSI) ==="
        cd "$PROJECT_ROOT" && python -m scripts.tbm.process.consolidate_tbm "$@"
        ;;
    dedup)
        # Stage 3: Flag duplicate company+date combinations
        shift
        echo "=== Stage 3: Flagging Duplicates ==="
        cd "$PROJECT_ROOT" && python -m scripts.tbm.process.deduplicate_tbm "$@"
        ;;
    all)
        # Run full pipeline
        shift
        echo "================================================"
        echo "TBM PROCESSING PIPELINE - FULL RUN"
        echo "================================================"
        echo ""

        echo "=== Stage 1: Parsing TBM Daily Plans ==="
        cd "$PROJECT_ROOT" && python -m scripts.tbm.process.parse_tbm_daily_plans "$@"
        if [ $? -ne 0 ]; then
            echo "ERROR: Parse stage failed"
            exit 1
        fi
        echo ""

        echo "=== Stage 2: Consolidating (Dimensions + CSI) ==="
        cd "$PROJECT_ROOT" && python -m scripts.tbm.process.consolidate_tbm "$@"
        if [ $? -ne 0 ]; then
            echo "ERROR: Consolidate stage failed"
            exit 1
        fi
        echo ""

        echo "=== Stage 3: Flagging Duplicates ==="
        cd "$PROJECT_ROOT" && python -m scripts.tbm.process.deduplicate_tbm "$@"
        if [ $? -ne 0 ]; then
            echo "ERROR: Dedup stage failed"
            exit 1
        fi

        echo ""
        echo "================================================"
        echo "TBM PIPELINE COMPLETE"
        echo "Output: processed/tbm/work_entries.csv"
        echo "================================================"
        ;;
    status)
        # Show file status
        shift
        source "$PROJECT_ROOT/.env" 2>/dev/null || true
        DATA_DIR="${WINDOWS_DATA_DIR:-$PROJECT_ROOT/data}"

        echo "TBM Pipeline Status"
        echo "==================="
        echo ""
        echo "Input files:"
        if [ -d "$DATA_DIR/raw/tbm" ]; then
            count=$(find "$DATA_DIR/raw/tbm" -name "*.xlsx" -o -name "*.xlsm" 2>/dev/null | wc -l)
            echo "  Excel files: $count"
        else
            echo "  Input directory not found: $DATA_DIR/raw/tbm"
        fi
        echo ""
        echo "Output files:"
        for f in "work_entries.csv" "tbm_files.csv"; do
            path="$DATA_DIR/processed/tbm/$f"
            if [ -f "$path" ]; then
                rows=$(wc -l < "$path")
                mod=$(stat -c %y "$path" 2>/dev/null | cut -d. -f1 || stat -f %Sm "$path" 2>/dev/null)
                echo "  $f: $((rows-1)) rows (modified: $mod)"
            else
                echo "  $f: NOT FOUND"
            fi
        done
        ;;
    help|*)
        echo "TBM Daily Plans Processing Pipeline"
        echo ""
        echo "Usage: ./run.sh <command> [options]"
        echo ""
        echo "Commands:"
        echo "  extract-eml   Extract Excel from EML files (one-time historical data)"
        echo "  sync          Sync files from field team's OneDrive folder"
        echo "  parse         Stage 1: Parse TBM Excel files -> work_entries.csv"
        echo "  consolidate   Stage 2: Add dimension IDs + CSI inference"
        echo "  dedup         Stage 3: Flag duplicate company+date combinations"
        echo "  all           Run full pipeline (parse -> consolidate -> dedup)"
        echo "  status        Show pipeline status and file counts"
        echo ""
        echo "Output:"
        echo "  processed/tbm/work_entries.csv - Parsed with dims, CSI, dedup flags"
        echo ""
        echo "Examples:"
        echo "  ./run.sh extract-eml --dry-run  # Preview EML extraction"
        echo "  ./run.sh extract-eml            # Extract Excel from EML files"
        echo "  ./run.sh sync --dry-run         # Preview sync from field folder"
        echo "  ./run.sh sync                   # Sync new files from field"
        echo "  ./run.sh status                 # Check current state"
        echo "  ./run.sh all                    # Run full pipeline"
        echo "  ./run.sh parse                  # Re-parse Excel files only"
        ;;
esac
