#!/bin/bash
# =============================================================================
# P6 Primavera Processing Pipeline
# =============================================================================
# Usage: ./run.sh <command> [options]
#
# Pipeline stages:
#   1. parse    - Parse XER files -> processed/primavera/
#   2. taxonomy - Generate task taxonomy -> processed/primavera/p6_task_taxonomy.csv
#   3. csi      - Add CSI section IDs (appends to p6_task_taxonomy.csv)
#
# The 'all' command runs stages 1-3 in sequence.
#
# OUTPUT (processed/primavera/):
#   - xer_files.csv      - File metadata (auto-discovered)
#   - task.csv           - All tasks (activities) with prefixed IDs
#   - taskpred.csv       - Task predecessors/dependencies
#   - projwbs.csv        - WBS with hierarchy tier columns
#   - p6_task_taxonomy.csv - Final taxonomy with dim_location_id, dim_csi_section_id
#
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Activate virtual environment
source "$PROJECT_ROOT/.venv/bin/activate"

# Get processed directory from Python settings (handles .env parsing properly)
PROCESSED_DIR=$(python -c "from src.config.settings import Settings; print(Settings.PRIMAVERA_PROCESSED_DIR)")

case "${1:-help}" in
    parse)
        # Stage 1: Parse XER files
        shift
        echo "=== Stage 1: Parsing XER Files ==="
        python scripts/primavera/process/batch_process_xer.py "$@"
        ;;
    taxonomy)
        # Stage 2: Generate task taxonomy
        shift
        echo "=== Stage 2: Generating Task Taxonomy ==="
        python scripts/primavera/derive/generate_task_taxonomy.py "$@"
        ;;
    csi)
        # Stage 3: Add CSI section IDs
        shift
        echo "=== Stage 3: Adding CSI Section IDs ==="
        python -m scripts.integrated_analysis.add_csi_to_p6_tasks "$@"
        ;;
    all)
        # Run full pipeline
        shift
        echo "================================================"
        echo "P6 PRIMAVERA PROCESSING PIPELINE - FULL RUN"
        echo "================================================"
        echo ""

        echo "=== Stage 1: Parsing XER Files ==="
        python scripts/primavera/process/batch_process_xer.py "$@"
        if [ $? -ne 0 ]; then
            echo "ERROR: Parse stage failed"
            exit 1
        fi
        echo ""

        echo "=== Stage 2: Generating Task Taxonomy ==="
        python scripts/primavera/derive/generate_task_taxonomy.py
        if [ $? -ne 0 ]; then
            echo "ERROR: Taxonomy stage failed"
            exit 1
        fi
        echo ""

        echo "=== Stage 3: Adding CSI Section IDs ==="
        python -m scripts.integrated_analysis.add_csi_to_p6_tasks
        if [ $? -ne 0 ]; then
            echo "ERROR: CSI stage failed"
            exit 1
        fi

        echo ""
        echo "================================================"
        echo "P6 PIPELINE COMPLETE"
        echo "Output: processed/primavera/p6_task_taxonomy.csv"
        echo "================================================"
        ;;
    status)
        # Show file status
        shift
        echo "P6 Primavera Pipeline Status"
        echo "============================"
        echo ""
        echo "Processed files ($PROCESSED_DIR):"
        for f in xer_files.csv task.csv taskpred.csv projwbs.csv p6_task_taxonomy.csv; do
            path="$PROCESSED_DIR/$f"
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
        echo "P6 Primavera Processing Pipeline"
        echo ""
        echo "Usage: ./run.sh <command> [options]"
        echo ""
        echo "Commands:"
        echo "  parse     Stage 1: Parse XER files to CSV tables"
        echo "  taxonomy  Stage 2: Generate task taxonomy with dim_location_id"
        echo "  csi       Stage 3: Add CSI section IDs"
        echo "  all       Run full pipeline (parse -> taxonomy -> csi)"
        echo "  status    Show pipeline status and file counts"
        echo ""
        echo "Parse Options (passed to batch_process_xer.py):"
        echo "  --all               Process all schedules (YATES + SECAI)"
        echo "  --schedule-type X   Process only YATES or SECAI schedules"
        echo "  --current-only      Only process the latest file"
        echo ""
        echo "Taxonomy Options (passed to generate_task_taxonomy.py):"
        echo "  --latest-only       Only process the latest YATES schedule"
        echo "  --skip-location-id  Skip adding dim_location_id column"
        echo ""
        echo "Output Files:"
        echo "  processed/primavera/p6_task_taxonomy.csv - Final taxonomy with:"
        echo "    - dim_location_id, dim_csi_section_id"
        echo "    - trade_id, building, level, location_type, location_code"
        echo "    - csi_section, csi_title"
        echo ""
        echo "Examples:"
        echo "  ./run.sh status                # Check current state"
        echo "  ./run.sh all                   # Run full pipeline"
        echo "  ./run.sh parse --current-only  # Parse only latest XER"
        echo "  ./run.sh taxonomy --latest-only # Taxonomy for latest only"
        ;;
esac
