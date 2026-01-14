#!/bin/bash
# Quality QC Inspections Consolidation Script
#
# Enriches Yates and SECAI QC inspection data with dimension IDs.
#
# Usage:
#   ./run.sh              # Run consolidation
#   ./run.sh status       # Show current coverage stats
#   ./run.sh help         # Show this help

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

cd "$PROJECT_ROOT"

# Activate virtual environment
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
fi

case "${1:-run}" in
    run|consolidate)
        echo "Running Yates/SECAI QC inspection consolidation..."
        python -m scripts.quality.document_processing.consolidate
        ;;

    status)
        echo "Checking consolidation status..."
        python3 << 'EOF'
import json
from pathlib import Path
from src.config.settings import settings

report_path = settings.PROCESSED_DATA_DIR / 'quality' / 'enriched' / 'consolidation_report.json'

if not report_path.exists():
    print("No consolidation report found. Run './run.sh' first.")
    exit(1)

with open(report_path) as f:
    report = json.load(f)

print(f"Last consolidated: {report['generated_at']}")
print()

for source in report['sources']:
    print(f"{source['source']} ({source['total_records']} records):")
    print(f"  Building:  {source['building']['pct']:.1f}%")
    print(f"  Location:  {source['location']['pct']:.1f}%")
    print(f"  Company:   {source['company']['pct']:.1f}%")
    print(f"  Trade:     {source['trade']['pct']:.1f}%")
    print()

combined = report['combined']
print(f"COMBINED ({combined['total_records']} records):")
print(f"  Building:  {combined['building']['pct']:.1f}%")
print(f"  Location:  {combined['location']['pct']:.1f}%")
print(f"  Company:   {combined['company']['pct']:.1f}%")
print(f"  Trade:     {combined['trade']['pct']:.1f}%")
EOF
        ;;

    help|--help|-h)
        echo "Quality QC Inspections Consolidation"
        echo ""
        echo "Usage: ./run.sh [command]"
        echo ""
        echo "Commands:"
        echo "  run, consolidate  Run the consolidation (default)"
        echo "  status            Show current coverage statistics"
        echo "  help              Show this help message"
        echo ""
        echo "Input files:"
        echo "  processed/quality/yates_all_inspections.csv"
        echo "  processed/quality/secai_inspection_log.csv"
        echo ""
        echo "Output files:"
        echo "  processed/quality/enriched/yates_qc_inspections.csv"
        echo "  processed/quality/enriched/secai_qc_inspections.csv"
        echo "  processed/quality/enriched/combined_qc_inspections.csv"
        echo "  processed/quality/enriched/consolidation_report.json"
        ;;

    *)
        echo "Unknown command: $1"
        echo "Run './run.sh help' for usage"
        exit 1
        ;;
esac
