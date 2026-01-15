#!/bin/bash
# Weekly Reports Processing Pipeline
# Usage: ./run.sh <command> [options]
#
# Pipeline stages:
#   1. parse    - Parse weekly report PDFs -> various CSV files
#   2. copy     - Copy labor file to processed/ directory
#   3. enrich   - Add dimension IDs (company only - no location data)
#
# The 'all' command runs stages 1-3 in sequence.
#
# NOTE: Weekly reports only contain labor by company, not by location.
# Enrichment adds dim_company_id but location/trade remain null.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Load environment and activate virtual environment
source "$PROJECT_ROOT/.env" 2>/dev/null || true
source "$PROJECT_ROOT/.venv/bin/activate"

# Determine data directories
DATA_DIR="${WINDOWS_DATA_DIR:-$PROJECT_ROOT/data}"
PARSE_OUTPUT_DIR="$PROJECT_ROOT/data/weekly_reports/tables"
PROCESSED_DIR="$DATA_DIR/processed/weekly_reports"

case "${1:-help}" in
    parse)
        # Stage 1: Parse weekly report PDFs
        shift
        echo "=== Stage 1: Parsing Weekly Report PDFs ==="
        python -m scripts.weekly_reports.process.process_weekly_reports "$@"
        ;;
    copy)
        # Stage 2: Copy labor file to processed directory
        shift
        echo "=== Stage 2: Copying Labor File to Processed Directory ==="

        # Create processed directory if needed
        mkdir -p "$PROCESSED_DIR"

        # Copy labor_detail_by_company.csv
        if [ -f "$PARSE_OUTPUT_DIR/labor_detail_by_company.csv" ]; then
            cp "$PARSE_OUTPUT_DIR/labor_detail_by_company.csv" "$PROCESSED_DIR/"
            echo "  Copied: labor_detail_by_company.csv"

            # Also copy other useful files
            for f in labor_detail.csv weekly_reports.csv key_issues.csv; do
                if [ -f "$PARSE_OUTPUT_DIR/$f" ]; then
                    cp "$PARSE_OUTPUT_DIR/$f" "$PROCESSED_DIR/"
                    echo "  Copied: $f"
                fi
            done
        else
            echo "ERROR: labor_detail_by_company.csv not found in $PARSE_OUTPUT_DIR"
            echo "  Run './run.sh parse' first"
            exit 1
        fi
        ;;
    enrich)
        # Stage 3: Add dimension IDs
        shift
        echo "=== Stage 3: Enriching with Dimension IDs ==="
        python -m scripts.integrated_analysis.enrich_with_dimensions --source weekly_labor "$@"
        ;;
    all)
        # Run full pipeline
        shift
        echo "================================================"
        echo "WEEKLY REPORTS PROCESSING PIPELINE - FULL RUN"
        echo "================================================"
        echo ""

        echo "=== Stage 1: Parsing Weekly Report PDFs ==="
        python -m scripts.weekly_reports.process.process_weekly_reports "$@"
        if [ $? -ne 0 ]; then
            echo "ERROR: Parse stage failed"
            exit 1
        fi
        echo ""

        echo "=== Stage 2: Copying Labor File to Processed Directory ==="
        mkdir -p "$PROCESSED_DIR"
        if [ -f "$PARSE_OUTPUT_DIR/labor_detail_by_company.csv" ]; then
            cp "$PARSE_OUTPUT_DIR/labor_detail_by_company.csv" "$PROCESSED_DIR/"
            echo "  Copied: labor_detail_by_company.csv"
            for f in labor_detail.csv weekly_reports.csv key_issues.csv; do
                if [ -f "$PARSE_OUTPUT_DIR/$f" ]; then
                    cp "$PARSE_OUTPUT_DIR/$f" "$PROCESSED_DIR/"
                    echo "  Copied: $f"
                fi
            done
        else
            echo "ERROR: labor_detail_by_company.csv not found"
            exit 1
        fi
        echo ""

        echo "=== Stage 3: Enriching with Dimension IDs ==="
        python -m scripts.integrated_analysis.enrich_with_dimensions --source weekly_labor "$@"
        if [ $? -ne 0 ]; then
            echo "ERROR: Enrich stage failed"
            exit 1
        fi

        echo ""
        echo "================================================"
        echo "WEEKLY REPORTS PIPELINE COMPLETE"
        echo "Output: processed/weekly_reports/labor_detail_by_company_enriched.csv"
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
        echo "Parse output ($PARSE_OUTPUT_DIR):"
        for f in weekly_reports.csv labor_detail.csv labor_detail_by_company.csv key_issues.csv; do
            path="$PARSE_OUTPUT_DIR/$f"
            if [ -f "$path" ]; then
                rows=$(wc -l < "$path")
                mod=$(stat -c %y "$path" 2>/dev/null | cut -d. -f1 || stat -f %Sm "$path" 2>/dev/null)
                echo "  $f: $((rows-1)) rows (modified: $mod)"
            else
                echo "  $f: NOT FOUND"
            fi
        done
        echo ""
        echo "Processed output ($PROCESSED_DIR):"
        for f in labor_detail_by_company.csv labor_detail_by_company_enriched.csv; do
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
        echo "  parse     Stage 1: Parse weekly report PDFs"
        echo "  copy      Stage 2: Copy labor files to processed directory"
        echo "  enrich    Stage 3: Add dimension IDs (company only)"
        echo "  all       Run full pipeline (parse -> copy -> enrich)"
        echo "  status    Show pipeline status and file counts"
        echo ""
        echo "Output Files:"
        echo "  Parse outputs (data/weekly_reports/tables/):"
        echo "    - weekly_reports.csv        Report metadata"
        echo "    - labor_detail.csv          Detailed labor entries"
        echo "    - labor_detail_by_company.csv  Labor summary by company"
        echo "    - key_issues.csv            Documented issues"
        echo ""
        echo "  Processed outputs (processed/weekly_reports/):"
        echo "    - labor_detail_by_company.csv           Copied for enrichment"
        echo "    - labor_detail_by_company_enriched.csv  With dim_company_id"
        echo ""
        echo "Note: Weekly reports only have company-level data (no location)."
        echo "      Enrichment adds dim_company_id; location/trade remain null."
        echo ""
        echo "Examples:"
        echo "  ./run.sh status           # Check current state"
        echo "  ./run.sh all              # Run full pipeline"
        echo "  ./run.sh parse            # Re-parse PDFs only"
        echo "  ./run.sh enrich --dry-run # Preview enrichment"
        ;;
esac
