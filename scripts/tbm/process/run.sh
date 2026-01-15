#!/bin/bash
# TBM Daily Plans Processing Pipeline
# Usage: ./run.sh <command> [options]
#
# Pipeline stages:
#   1. parse    - Parse Excel files -> work_entries.csv
#   2. enrich   - Add dimension IDs (location, company, trade)
#   3. csi      - Add CSI section inference from activity descriptions
#
# The 'all' command runs stages 1-3 in sequence.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Activate virtual environment
source "$PROJECT_ROOT/.venv/bin/activate"

case "${1:-help}" in
    parse)
        # Stage 1: Parse TBM Excel files
        shift
        echo "=== Stage 1: Parsing TBM Daily Plans ==="
        python -m scripts.tbm.process.parse_tbm_daily_plans "$@"
        ;;
    enrich)
        # Stage 2: Add dimension IDs
        shift
        echo "=== Stage 2: Enriching with Dimension IDs ==="
        python -m scripts.integrated_analysis.enrich_with_dimensions --source tbm "$@"
        ;;
    csi)
        # Stage 3: Add CSI section inference
        shift
        echo "=== Stage 3: Adding CSI Section IDs ==="
        python -m scripts.integrated_analysis.add_csi_to_tbm "$@"
        ;;
    all)
        # Run full pipeline
        shift
        echo "================================================"
        echo "TBM PROCESSING PIPELINE - FULL RUN"
        echo "================================================"
        echo ""

        echo "=== Stage 1: Parsing TBM Daily Plans ==="
        python -m scripts.tbm.process.parse_tbm_daily_plans "$@"
        if [ $? -ne 0 ]; then
            echo "ERROR: Parse stage failed"
            exit 1
        fi
        echo ""

        echo "=== Stage 2: Enriching with Dimension IDs ==="
        python -m scripts.integrated_analysis.enrich_with_dimensions --source tbm "$@"
        if [ $? -ne 0 ]; then
            echo "ERROR: Enrich stage failed"
            exit 1
        fi
        echo ""

        echo "=== Stage 3: Adding CSI Section IDs ==="
        python -m scripts.integrated_analysis.add_csi_to_tbm "$@"
        if [ $? -ne 0 ]; then
            echo "ERROR: CSI stage failed"
            exit 1
        fi

        echo ""
        echo "================================================"
        echo "TBM PIPELINE COMPLETE"
        echo "Output: processed/tbm/work_entries_enriched.csv"
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
        for f in "work_entries.csv" "work_entries_enriched.csv" "tbm_files.csv"; do
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
        echo "  parse     Stage 1: Parse TBM Excel files -> work_entries.csv"
        echo "  enrich    Stage 2: Add dimension IDs (location, company, trade)"
        echo "  csi       Stage 3: Add CSI section inference from activities"
        echo "  all       Run full pipeline (parse -> enrich -> csi)"
        echo "  status    Show pipeline status and file counts"
        echo ""
        echo "Output:"
        echo "  processed/tbm/work_entries.csv          - Raw parsed data"
        echo "  processed/tbm/work_entries_enriched.csv - With dims + CSI"
        echo ""
        echo "Examples:"
        echo "  ./run.sh status           # Check current state"
        echo "  ./run.sh all              # Run full pipeline"
        echo "  ./run.sh parse            # Re-parse Excel files only"
        echo "  ./run.sh enrich --dry-run # Preview enrichment changes"
        ;;
esac
