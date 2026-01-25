#!/bin/bash
# =============================================================================
# Dimension Tables Pipeline
# =============================================================================
# Usage: ./run.sh <command>
#
# Pipeline stages (for dim_location):
#   1. location-master  - Generate location_master.csv from P6 taxonomy
#   2. grid-bounds      - Populate grid bounds from Excel mapping
#   3. extract-grids    - Extract grids from P6 task names (stairs/elevators)
#   4. dim-location     - Build dim_location.csv with inference + drawing check
#
# Other dimensions:
#   - dim-company   - Build dim_company.csv
#   - dim-trade     - Build dim_trade.csv
#   - dim-csi       - Build dim_csi_section.csv
#
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Activate virtual environment
source "$PROJECT_ROOT/.venv/bin/activate"

case "${1:-help}" in
    location-master)
        # Stage 1: Generate location_master from P6 taxonomy
        shift
        echo "=== Stage 1: Generating Location Master ==="
        python scripts/primavera/derive/generate_location_master.py "$@"
        ;;
    grid-bounds)
        # Stage 2: Populate grid bounds from Excel
        shift
        echo "=== Stage 2: Populating Grid Bounds from Excel ==="
        python scripts/shared/populate_grid_bounds.py "$@"
        ;;
    extract-grids)
        # Stage 3: Extract grids from P6 task names
        shift
        echo "=== Stage 3: Extracting Grids from P6 Task Names ==="
        python scripts/shared/extract_location_grids.py "$@"
        ;;
    dim-location)
        # Stage 4: Build dim_location.csv
        shift
        echo "=== Stage 4: Building dim_location.csv ==="
        python scripts/integrated_analysis/dimensions/build_dim_location.py "$@"
        ;;
    location)
        # Full location pipeline (stages 1-4)
        shift
        echo "================================================"
        echo "LOCATION DIMENSION PIPELINE - FULL RUN"
        echo "================================================"
        echo ""

        echo "=== Stage 1: Generating Location Master ==="
        python scripts/primavera/derive/generate_location_master.py
        echo ""

        echo "=== Stage 2: Populating Grid Bounds from Excel ==="
        python scripts/shared/populate_grid_bounds.py
        echo ""

        echo "=== Stage 3: Extracting Grids from P6 Task Names ==="
        python scripts/shared/extract_location_grids.py
        echo ""

        echo "=== Stage 4: Building dim_location.csv ==="
        python scripts/integrated_analysis/dimensions/build_dim_location.py "$@"
        echo ""

        echo "================================================"
        echo "LOCATION PIPELINE COMPLETE"
        echo "Output: processed/integrated_analysis/dimensions/dim_location.csv"
        echo "================================================"
        ;;
    dim-company)
        # Build dim_company.csv
        shift
        echo "=== Building dim_company.csv ==="
        python scripts/integrated_analysis/dimensions/build_company_dimension.py "$@"
        ;;
    dim-trade)
        # Build dim_trade.csv
        shift
        echo "=== Building dim_trade.csv ==="
        python scripts/integrated_analysis/dimensions/build_dim_trade.py "$@"
        ;;
    dim-csi)
        # Build dim_csi_section.csv
        shift
        echo "=== Building dim_csi_section.csv ==="
        python scripts/integrated_analysis/dimensions/build_dim_csi_section.py "$@"
        ;;
    all)
        # Build all dimensions
        shift
        echo "================================================"
        echo "ALL DIMENSIONS PIPELINE"
        echo "================================================"
        echo ""

        # Location dimension (full pipeline)
        "$0" location

        echo ""
        echo "=== Building dim_company.csv ==="
        python scripts/integrated_analysis/dimensions/build_company_dimension.py
        echo ""

        echo "=== Building dim_trade.csv ==="
        python scripts/integrated_analysis/dimensions/build_dim_trade.py
        echo ""

        echo "=== Building dim_csi_section.csv ==="
        python scripts/integrated_analysis/dimensions/build_dim_csi_section.py
        echo ""

        echo "================================================"
        echo "ALL DIMENSIONS COMPLETE"
        echo "================================================"
        ;;
    bridge)
        # Generate affected_rooms_bridge table
        shift
        echo "=== Generating affected_rooms_bridge.csv ==="
        python -m scripts.integrated_analysis.generate_affected_rooms_bridge "$@"
        ;;
    status)
        # Show status of dimension files
        shift
        echo "Dimension Tables Status"
        echo "======================="
        echo ""

        # Get data directories
        DATA_DIRS=$(python -c "from src.config.settings import Settings; print(Settings.PROCESSED_DATA_DIR)")
        DIMS_DIR="$DATA_DIRS/integrated_analysis/dimensions"

        echo "Location: $DIMS_DIR"
        echo ""

        for f in dim_location.csv dim_company.csv dim_trade.csv dim_csi_section.csv; do
            path="$DIMS_DIR/$f"
            if [ -f "$path" ]; then
                rows=$(wc -l < "$path")
                mod=$(stat -c %y "$path" 2>/dev/null | cut -d. -f1 || stat -f %Sm "$path" 2>/dev/null)
                echo "  $f: $((rows-1)) rows (modified: $mod)"
            else
                echo "  $f: NOT FOUND"
            fi
        done

        # Bridge tables status
        echo ""
        echo "Bridge Tables:"
        BRIDGE_DIR="$DATA_DIRS/integrated_analysis/bridge_tables"
        for f in affected_rooms_bridge.csv; do
            path="$BRIDGE_DIR/$f"
            if [ -f "$path" ]; then
                rows=$(wc -l < "$path")
                mod=$(stat -c %y "$path" 2>/dev/null | cut -d. -f1 || stat -f %Sm "$path" 2>/dev/null)
                echo "  $f: $((rows-1)) rows (modified: $mod)"
            else
                echo "  $f: NOT FOUND"
            fi
        done

        # Location master status
        echo ""
        echo "Source files:"
        MASTER=$(python -c "from src.config.settings import Settings; print(Settings.RAW_DATA_DIR / 'location_mappings' / 'location_master.csv')")
        if [ -f "$MASTER" ]; then
            rows=$(wc -l < "$MASTER")
            mod=$(stat -c %y "$MASTER" 2>/dev/null | cut -d. -f1 || stat -f %Sm "$MASTER" 2>/dev/null)
            echo "  location_master.csv: $((rows-1)) rows (modified: $mod)"
        else
            echo "  location_master.csv: NOT FOUND"
        fi
        ;;
    help|*)
        echo "Dimension Tables Pipeline"
        echo ""
        echo "Usage: ./run.sh <command> [options]"
        echo ""
        echo "Location Dimension:"
        echo "  location-master  Stage 1: Generate location_master from P6 taxonomy"
        echo "  grid-bounds      Stage 2: Populate grid bounds from Excel mapping"
        echo "  extract-grids    Stage 3: Extract grids from P6 task names"
        echo "  dim-location     Stage 4: Build dim_location.csv (with inference)"
        echo "  location         Run full location pipeline (stages 1-4)"
        echo ""
        echo "Other Dimensions:"
        echo "  dim-company      Build dim_company.csv"
        echo "  dim-trade        Build dim_trade.csv"
        echo "  dim-csi          Build dim_csi_section.csv"
        echo ""
        echo "Bridge Tables:"
        echo "  bridge           Generate affected_rooms_bridge.csv (requires RABA/PSI/TBM)"
        echo ""
        echo "Meta:"
        echo "  all              Build all dimensions"
        echo "  status           Show dimension file status"
        echo ""
        echo "Options (passed to build scripts):"
        echo "  --dry-run        Preview without writing files"
        echo ""
        echo "Data Flow (dim_location):"
        echo "  P6 taxonomy -> location_master.csv -> dim_location.csv"
        echo "                      |"
        echo "                      +-- Grid bounds from Excel mapping"
        echo "                      +-- Grid extraction from P6 task names"
        echo "                      +-- Grid inference from sibling rooms"
        echo "                      +-- Drawing code extraction from PDFs"
        ;;
esac
