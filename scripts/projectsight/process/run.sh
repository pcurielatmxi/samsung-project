#!/bin/bash
# ProjectSight Processing Pipeline
# Usage: ./run.sh <command> [options]
#
# Two data pipelines:
#
# LABOR PIPELINE (daily reports -> labor hours):
#   1. parse-labor    - Parse JSON daily reports -> labor_entries.csv
#   2. consolidate-labor - Add dimension IDs + CSI -> labor_entries.csv
#
# NCR PIPELINE (quality records):
#   1. parse-ncr    - Parse NCR Excel export -> ncr.csv
#   2. consolidate-ncr - Add dimension IDs + CSI -> ncr_consolidated.csv
#
# Use 'labor' or 'ncr' to run full respective pipelines.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Activate virtual environment
source "$PROJECT_ROOT/.venv/bin/activate"

case "${1:-help}" in
    # ==================== LABOR PIPELINE ====================
    parse-labor)
        # Stage 1: Parse daily reports JSON
        shift
        echo "=== Labor Stage 1: Parsing Daily Reports JSON ==="
        python -m scripts.projectsight.process.parse_labor_from_json "$@"
        ;;
    consolidate-labor)
        # Stage 2: Add dimension IDs + CSI
        shift
        echo "=== Labor Stage 2: Consolidating (Dimensions + CSI) ==="
        python -m scripts.projectsight.process.consolidate_labor "$@"
        ;;
    labor)
        # Run full labor pipeline
        shift
        echo "================================================"
        echo "PROJECTSIGHT LABOR PIPELINE - FULL RUN"
        echo "================================================"
        echo ""

        echo "=== Labor Stage 1: Parsing Daily Reports JSON ==="
        python -m scripts.projectsight.process.parse_labor_from_json "$@"
        if [ $? -ne 0 ]; then
            echo "ERROR: Parse stage failed"
            exit 1
        fi
        echo ""

        echo "=== Labor Stage 2: Consolidating (Dimensions + CSI) ==="
        python -m scripts.projectsight.process.consolidate_labor "$@"
        if [ $? -ne 0 ]; then
            echo "ERROR: Consolidate stage failed"
            exit 1
        fi

        echo ""
        echo "================================================"
        echo "LABOR PIPELINE COMPLETE"
        echo "Output: processed/projectsight/labor_entries.csv"
        echo "================================================"
        ;;

    # ==================== NCR PIPELINE ====================
    parse-ncr)
        # Stage 1: Parse NCR Excel export
        shift
        echo "=== NCR Stage 1: Parsing NCR Excel Export ==="
        python -m scripts.projectsight.process.process_ncr_export "$@"
        ;;
    consolidate-ncr)
        # Stage 2: Add dimensions + CSI (combined step)
        shift
        echo "=== NCR Stage 2: Consolidating with Dimensions + CSI ==="
        python -m scripts.projectsight.process.consolidate_ncr "$@"
        ;;
    ncr)
        # Run full NCR pipeline
        shift
        echo "================================================"
        echo "PROJECTSIGHT NCR PIPELINE - FULL RUN"
        echo "================================================"
        echo ""

        echo "=== NCR Stage 1: Parsing NCR Excel Export ==="
        python -m scripts.projectsight.process.process_ncr_export "$@"
        if [ $? -ne 0 ]; then
            echo "ERROR: Parse stage failed"
            exit 1
        fi
        echo ""

        echo "=== NCR Stage 2: Consolidating with Dimensions + CSI ==="
        python -m scripts.projectsight.process.consolidate_ncr "$@"
        if [ $? -ne 0 ]; then
            echo "ERROR: Consolidate stage failed"
            exit 1
        fi

        echo ""
        echo "================================================"
        echo "NCR PIPELINE COMPLETE"
        echo "Output: processed/projectsight/ncr_consolidated.csv"
        echo "================================================"
        ;;

    # ==================== COMBINED ====================
    all)
        # Run all pipelines
        shift
        echo "================================================"
        echo "PROJECTSIGHT ALL PIPELINES"
        echo "================================================"

        "$0" labor "$@"
        if [ $? -ne 0 ]; then
            echo "ERROR: Labor pipeline failed"
            exit 1
        fi

        "$0" ncr "$@"
        if [ $? -ne 0 ]; then
            echo "ERROR: NCR pipeline failed"
            exit 1
        fi

        echo ""
        echo "================================================"
        echo "ALL PROJECTSIGHT PIPELINES COMPLETE"
        echo "================================================"
        ;;

    status)
        # Show file status
        shift
        source "$PROJECT_ROOT/.env" 2>/dev/null || true
        DATA_DIR="${WINDOWS_DATA_DIR:-$PROJECT_ROOT/data}"

        echo "ProjectSight Pipeline Status"
        echo "============================"
        echo ""
        echo "LABOR DATA:"
        echo "-----------"
        if [ -d "$DATA_DIR/raw/projectsight/daily_reports" ]; then
            count=$(find "$DATA_DIR/raw/projectsight/daily_reports" -name "*.json" 2>/dev/null | wc -l)
            echo "  Daily report JSON files: $count"
        else
            echo "  Daily reports directory not found"
        fi
        for f in "labor_entries.csv"; do
            path="$DATA_DIR/processed/projectsight/$f"
            if [ -f "$path" ]; then
                rows=$(wc -l < "$path")
                mod=$(stat -c %y "$path" 2>/dev/null | cut -d. -f1 || stat -f %Sm "$path" 2>/dev/null)
                echo "  $f: $((rows-1)) rows (modified: $mod)"
            else
                echo "  $f: NOT FOUND"
            fi
        done
        echo ""
        echo "NCR DATA:"
        echo "---------"
        for f in "ncr.csv" "ncr_consolidated.csv"; do
            path="$DATA_DIR/processed/projectsight/$f"
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
        echo "ProjectSight Processing Pipeline"
        echo ""
        echo "Usage: ./run.sh <command> [options]"
        echo ""
        echo "LABOR PIPELINE (daily reports -> labor hours):"
        echo "  parse-labor       Parse JSON daily reports -> labor_entries.csv"
        echo "  consolidate-labor Add dimension IDs + CSI"
        echo "  labor             Run full labor pipeline (both stages)"
        echo ""
        echo "NCR PIPELINE (quality records):"
        echo "  parse-ncr       Parse NCR Excel export -> ncr.csv"
        echo "  consolidate-ncr Add dimensions + CSI -> ncr_consolidated.csv"
        echo "  ncr             Run full NCR pipeline (both stages)"
        echo ""
        echo "COMBINED:"
        echo "  all             Run both labor and NCR pipelines"
        echo "  status          Show pipeline status and file counts"
        echo ""
        echo "Output Files:"
        echo "  processed/projectsight/labor_entries.csv    - With dims + CSI"
        echo "  processed/projectsight/ncr.csv              - Raw parsed NCR"
        echo "  processed/projectsight/ncr_consolidated.csv - With dims + CSI"
        echo ""
        echo "Examples:"
        echo "  ./run.sh status              # Check current state"
        echo "  ./run.sh labor               # Run labor pipeline"
        echo "  ./run.sh ncr                 # Run NCR pipeline"
        echo "  ./run.sh all                 # Run everything"
        ;;
esac
