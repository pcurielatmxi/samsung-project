#!/bin/bash
# Batch process all XER files in the data/raw directory

set -e  # Exit on error

# Configuration
XER_DIR="${1:-data/raw}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== XER Batch Processor ==="
echo "Processing XER files from: $XER_DIR"
echo ""

# Change to project root
cd "$PROJECT_ROOT"

# Find all XER files
XER_FILES=("$XER_DIR"/*.xer)

if [ ! -e "${XER_FILES[0]}" ]; then
    echo "ERROR: No XER files found in $XER_DIR"
    exit 1
fi

# Count files
FILE_COUNT=${#XER_FILES[@]}
echo "Found $FILE_COUNT XER file(s)"
echo ""

# Process each file
PROCESSED=0
FAILED=0

for xer_file in "${XER_FILES[@]}"; do
    echo "----------------------------------------"
    echo "Processing: $(basename "$xer_file")"
    echo "----------------------------------------"

    if python3 scripts/process_xer_to_csv.py "$xer_file"; then
        ((PROCESSED++))
        echo "✅ Success"
    else
        ((FAILED++))
        echo "❌ Failed"
    fi

    echo ""
done

echo "========================================"
echo "BATCH PROCESSING COMPLETE"
echo "========================================"
echo "Total files: $FILE_COUNT"
echo "Processed successfully: $PROCESSED"
echo "Failed: $FAILED"
echo ""
echo "Output files are in: data/output/xer_exports/"
