#!/bin/bash
# Fieldwire Idle Time Tagging Pipeline
#
# Scheduled script that:
# 1. Extracts content from latest Fieldwire data dumps
# 2. Runs AI enrichment (idempotent - skips cached rows)
#
# Usage:
#   ./run_idle_tags.sh           # Normal run
#   ./run_idle_tags.sh --force   # Reprocess all rows (clear cache)
#   ./run_idle_tags.sh --status  # Show cache status only
#
# Schedule with cron:
#   0 6 * * * /path/to/run_idle_tags.sh >> /path/to/idle_tags.log 2>&1

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Activate virtual environment
source "$PROJECT_ROOT/.venv/bin/activate"

# Data paths
DATA_DIR="${WINDOWS_DATA_DIR:-/mnt/c/Users/pdcur/OneDrive - MXI/Desktop/Samsung Dashboard/Data}"
PROCESSED_DIR="$DATA_DIR/processed/fieldwire"
CACHE_DIR="$PROCESSED_DIR/ai_cache"
OUTPUT_FILE="$PROCESSED_DIR/tbm_content.csv"
ENRICHED_FILE="$PROCESSED_DIR/tbm_content_enriched.csv"

# AI enrichment config
PROMPT_FILE="$SCRIPT_DIR/ai_enrichment/idle_tags_prompt.txt"
SCHEMA_FILE="$SCRIPT_DIR/ai_enrichment/idle_tags_schema.json"
MODEL="gemini-3-flash-preview"
BATCH_SIZE=20

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() {
    echo -e "$(date '+%Y-%m-%d %H:%M:%S') | $1"
}

status() {
    log "${YELLOW}=== Idle Tags Pipeline Status ===${NC}"
    echo ""

    # Check extracted file
    if [ -f "$OUTPUT_FILE" ]; then
        ROWS=$(wc -l < "$OUTPUT_FILE")
        MOD_TIME=$(stat -c %y "$OUTPUT_FILE" 2>/dev/null | cut -d. -f1)
        log "Extracted file: ${GREEN}$OUTPUT_FILE${NC}"
        log "  Rows: $((ROWS - 1)) | Modified: $MOD_TIME"
    else
        log "Extracted file: ${RED}Not found${NC}"
    fi

    echo ""

    # Check cache
    if [ -d "$CACHE_DIR" ]; then
        CACHED=$(find "$CACHE_DIR" -maxdepth 1 -name "*.json" 2>/dev/null | wc -l)
        ERRORS=$(find "$CACHE_DIR/_errors" -name "*.json" 2>/dev/null | wc -l)
        log "Cache directory: ${GREEN}$CACHE_DIR${NC}"
        log "  Cached: $CACHED | Errors: $ERRORS"
    else
        log "Cache directory: ${YELLOW}Not created${NC}"
    fi

    echo ""

    # Check enriched file
    if [ -f "$ENRICHED_FILE" ]; then
        ROWS=$(wc -l < "$ENRICHED_FILE")
        MOD_TIME=$(stat -c %y "$ENRICHED_FILE" 2>/dev/null | cut -d. -f1)
        log "Enriched file: ${GREEN}$ENRICHED_FILE${NC}"
        log "  Rows: $((ROWS - 1)) | Modified: $MOD_TIME"
    else
        log "Enriched file: ${YELLOW}Not created${NC}"
    fi
}

extract() {
    log "${GREEN}=== Phase 1: Extract Content ===${NC}"
    python -m scripts.fieldwire.extract_tbm_content
}

enrich() {
    local EXTRA_ARGS="$1"

    log "${GREEN}=== Phase 2: AI Enrichment ===${NC}"

    if [ ! -f "$OUTPUT_FILE" ]; then
        log "${RED}Error: Extracted file not found. Run extract first.${NC}"
        exit 1
    fi

    python -m src.ai_enrich \
        "$OUTPUT_FILE" \
        --prompt "$PROMPT_FILE" \
        --schema "$SCHEMA_FILE" \
        --primary-key id \
        --columns narratives \
        --cache-dir "$CACHE_DIR" \
        --batch-size "$BATCH_SIZE" \
        --model "$MODEL" \
        $EXTRA_ARGS
}

# Parse arguments
FORCE=""
STATUS_ONLY=""

for arg in "$@"; do
    case $arg in
        --force)
            FORCE="--force"
            ;;
        --status)
            STATUS_ONLY="true"
            ;;
        --help|-h)
            echo "Usage: $0 [--force] [--status]"
            echo ""
            echo "Options:"
            echo "  --force   Clear cache and reprocess all rows"
            echo "  --status  Show pipeline status only"
            exit 0
            ;;
    esac
done

# Main
cd "$PROJECT_ROOT"

if [ -n "$STATUS_ONLY" ]; then
    status
    exit 0
fi

log "${GREEN}=== Fieldwire Idle Tags Pipeline ===${NC}"
log "Data dir: $DATA_DIR"
echo ""

# Phase 1: Extract
extract

echo ""

# Phase 2: Enrich
enrich "$FORCE"

echo ""
log "${GREEN}=== Pipeline Complete ===${NC}"
status
