#!/bin/bash
# =============================================================================
# P6 Primavera Processing Pipeline
# =============================================================================
# Usage: ./run.sh <command> [options]
#
# Pipeline stages:
#   1. parse    - Parse XER files -> processed/primavera/
#   2. taxonomy - Generate task taxonomy -> derived/primavera/task_taxonomy.csv
#   3. csi      - Add CSI section IDs -> derived/primavera/task_taxonomy_with_csi.csv
#   4. copy     - Copy final file to processed/primavera/p6_task_taxonomy.csv
#
# The 'all' command runs stages 1-4 in sequence.
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

# Load environment and activate virtual environment
source "$PROJECT_ROOT/.env" 2>/dev/null || true
source "$PROJECT_ROOT/.venv/bin/activate"

# Determine data directories
DATA_DIR="${WINDOWS_DATA_DIR:-$PROJECT_ROOT/data}"
PROCESSED_DIR="$DATA_DIR/processed/primavera"
DERIVED_DIR="$DATA_DIR/derived/primavera"

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
    copy)
        # Stage 4: Copy final file to processed directory
        shift
        echo "=== Stage 4: Copying to Processed Directory ==="

        # Source file is task_taxonomy_with_csi.csv in derived
        SRC_FILE="$DERIVED_DIR/task_taxonomy_with_csi.csv"
        DST_FILE="$PROCESSED_DIR/p6_task_taxonomy.csv"

        if [ -f "$SRC_FILE" ]; then
            cp "$SRC_FILE" "$DST_FILE"
            rows=$(wc -l < "$DST_FILE")
            echo "  Copied: task_taxonomy_with_csi.csv -> p6_task_taxonomy.csv"
            echo "  Records: $((rows-1))"
        else
            echo "ERROR: Source file not found: $SRC_FILE"
            echo "  Run './run.sh taxonomy' and './run.sh csi' first"
            exit 1
        fi
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

        echo "=== Stage 4: Copying to Processed Directory ==="
        SRC_FILE="$DERIVED_DIR/task_taxonomy_with_csi.csv"
        DST_FILE="$PROCESSED_DIR/p6_task_taxonomy.csv"

        if [ -f "$SRC_FILE" ]; then
            cp "$SRC_FILE" "$DST_FILE"
            rows=$(wc -l < "$DST_FILE")
            echo "  Copied: task_taxonomy_with_csi.csv -> p6_task_taxonomy.csv"
            echo "  Records: $((rows-1))"
        else
            echo "ERROR: Source file not found after CSI stage"
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
        echo ""
        echo "Derived files ($DERIVED_DIR):"
        for f in task_taxonomy.csv task_taxonomy_with_csi.csv wbs_taxonomy.csv; do
            path="$DERIVED_DIR/$f"
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
        echo "  copy      Stage 4: Copy final file to processed directory"
        echo "  all       Run full pipeline (parse -> taxonomy -> csi -> copy)"
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
