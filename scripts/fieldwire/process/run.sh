#!/bin/bash
# Fieldwire TBM Audit Processing Pipeline
# Usage: ./run.sh [parse|enrich|lpi|all|status]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Activate virtual environment
source "$PROJECT_ROOT/.venv/bin/activate"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

usage() {
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  parse          - Stage 1: Parse Fieldwire CSV dump"
    echo "  enrich         - Stage 2: Add dimension IDs"
    echo "  lpi            - Stage 3: Calculate LPI metrics"
    echo "  all            - Run all stages"
    echo "  status         - Show processing status"
    echo "  report         - Generate TBM metrics report"
    echo "  transform-secai - Transform SECAI data to main format"
    echo ""
}

status() {
    echo -e "${YELLOW}=== Fieldwire Processing Status ===${NC}"
    echo ""

    # Check for input files
    if [ -n "$WINDOWS_DATA_DIR" ]; then
        INPUT_DIR="$WINDOWS_DATA_DIR/processed/fieldwire"
        echo "Input directory: $INPUT_DIR"
        if [ -d "$INPUT_DIR" ]; then
            CSV_COUNT=$(find "$INPUT_DIR" -maxdepth 1 -name "*.csv" 2>/dev/null | wc -l)
            echo -e "  CSV files: ${GREEN}$CSV_COUNT${NC}"
        else
            echo -e "  ${RED}Directory not found${NC}"
        fi
    else
        echo -e "${RED}WINDOWS_DATA_DIR not set${NC}"
    fi

    echo ""

    # Check output files
    OUTPUT_DIR="$WINDOWS_DATA_DIR/processed/fieldwire"
    echo "Output files:"

    for f in tbm_audits.csv manpower_counts.csv tbm_audits_enriched.csv lpi_summary.csv lpi_weekly.csv idle_analysis.csv secai_transformed.csv secai_manpower_counts.csv; do
        if [ -f "$OUTPUT_DIR/$f" ]; then
            ROWS=$(wc -l < "$OUTPUT_DIR/$f")
            echo -e "  $f: ${GREEN}$ROWS rows${NC}"
        else
            echo -e "  $f: ${YELLOW}not created${NC}"
        fi
    done
}

parse() {
    echo -e "${GREEN}=== Stage 1: Parse Fieldwire CSV ===${NC}"
    python -m scripts.fieldwire.process.parse_fieldwire "$@"
}

enrich() {
    echo -e "${GREEN}=== Stage 2: Enrich with Dimensions ===${NC}"
    python -m scripts.fieldwire.process.enrich_tbm "$@"
}

lpi() {
    echo -e "${GREEN}=== Stage 3: Calculate LPI Metrics ===${NC}"
    python -m scripts.fieldwire.process.calculate_lpi "$@"
}

report() {
    echo -e "${GREEN}=== TBM Metrics Report ===${NC}"
    python -m scripts.fieldwire.process.tbm_metrics_report "$@"
}

transform_secai() {
    echo -e "${GREEN}=== Transform SECAI Data ===${NC}"
    python -m scripts.fieldwire.process.transform_secai "$@"
}

all() {
    echo -e "${GREEN}=== Running Full Pipeline ===${NC}"
    parse "$@"
    enrich "$@"
    lpi "$@"
    echo -e "${GREEN}=== Pipeline Complete ===${NC}"
}

# Main command dispatcher
case "${1:-}" in
    parse)
        shift
        parse "$@"
        ;;
    enrich)
        shift
        enrich "$@"
        ;;
    lpi)
        shift
        lpi "$@"
        ;;
    report)
        shift
        report "$@"
        ;;
    transform-secai)
        shift
        transform_secai "$@"
        ;;
    all)
        shift
        all "$@"
        ;;
    status)
        status
        ;;
    -h|--help|"")
        usage
        ;;
    *)
        echo -e "${RED}Unknown command: $1${NC}"
        usage
        exit 1
        ;;
esac
