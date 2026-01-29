#!/bin/bash
# Weekly Reports Processing Pipeline
# Usage: ./run.sh <command> [options]
#
# Pipeline stages:
#   1. parse    - Parse weekly report PDFs -> various CSV files (includes company enrichment)
#
# The 'all' command runs the parse stage.
#
# NOTE: Weekly reports only contain labor by company, not by location.
# Company enrichment is built into the parse stage via parse_labor_detail.py.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Load environment and activate virtual environment
source "$PROJECT_ROOT/.env" 2>/dev/null || true
source "$PROJECT_ROOT/.venv/bin/activate"

# Determine data directories
DATA_DIR="${WINDOWS_DATA_DIR:-$PROJECT_ROOT/data}"
PROCESSED_DIR="$DATA_DIR/processed/weekly_reports"

case "${1:-help}" in
    parse)
        # Parse weekly report PDFs (includes company enrichment)
        shift
        echo "=== Parsing Weekly Report PDFs ==="
        python -m scripts.weekly_reports.process.process_weekly_reports "$@"
        ;;
    all)
        # Run full pipeline
        shift
        echo "================================================"
        echo "WEEKLY REPORTS PROCESSING PIPELINE - FULL RUN"
        echo "================================================"
        echo ""

        echo "=== Parsing Weekly Report PDFs ==="
        python -m scripts.weekly_reports.process.process_weekly_reports "$@"
        if [ $? -ne 0 ]; then
            echo "ERROR: Parse stage failed"
            exit 1
        fi

        echo ""
        echo "================================================"
        echo "WEEKLY REPORTS PIPELINE COMPLETE"
        echo "Output: processed/weekly_reports/labor_detail_by_company.csv"
        echo "================================================"
        ;;
    status)
        # Show file status
        shift
        echo "Weekly Reports Pipeline Status"
        echo "=============================="
        echo ""
        echo "Input files (raw PDFs):"
        if [ -d "$DATA_DIR/raw/weekly_reports" ]; then
            count=$(find "$DATA_DIR/raw/weekly_reports" -name "*.pdf" 2>/dev/null | wc -l)
            echo "  PDF files: $count"
        else
            echo "  Input directory not found: $DATA_DIR/raw/weekly_reports"
        fi
        echo ""
        echo "Processed output ($PROCESSED_DIR):"
        for f in weekly_reports.csv labor_detail.csv labor_detail_by_company.csv key_issues.csv; do
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
        echo "Weekly Reports Processing Pipeline"
        echo ""
        echo "Usage: ./run.sh <command> [options]"
        echo ""
        echo "Commands:"
        echo "  parse     Parse weekly report PDFs (includes company enrichment)"
        echo "  all       Run full pipeline (same as parse)"
        echo "  status    Show pipeline status and file counts"
        echo ""
        echo "Output Files (processed/weekly_reports/):"
        echo "  - weekly_reports.csv          Report metadata"
        echo "  - labor_detail.csv            Detailed labor entries"
        echo "  - labor_detail_by_company.csv Labor summary by company (with dim_company_id)"
        echo "  - key_issues.csv              Documented issues"
        echo ""
        echo "Note: Weekly reports only have company-level data (no location)."
        echo "      Company enrichment adds dim_company_id; location/trade remain null."
        echo ""
        echo "Examples:"
        echo "  ./run.sh status           # Check current state"
        echo "  ./run.sh all              # Run full pipeline"
        echo "  ./run.sh parse            # Parse PDFs"
        ;;
esac
