#!/bin/bash
# =============================================================================
# P6 XER Batch Processor
# =============================================================================
#
# Processes all XER files from manifest and generates:
#
# OUTPUT (processed/primavera/):
#   - xer_files.csv      - File metadata from manifest
#   - task.csv           - All tasks (activities) with prefixed IDs
#   - taskpred.csv       - Task predecessors/dependencies
#   - taskrsrc.csv       - Task resource assignments
#   - projwbs.csv        - WBS with hierarchy tier columns (depth, tier_1-6)
#   - actvcode.csv       - Activity code values
#   - ... and all other XER tables
#
# DERIVED (derived/primavera/):
#   - task_taxonomy.csv  - Phase, scope, building, level classifications
#
# CALCULATED COLUMNS (auto-generated):
#   - projwbs: depth, tier_1, tier_2, tier_3, tier_4, tier_5, tier_6
#   - task_taxonomy: phase, scope, building, level, trade, loc_type, etc.
#
# NOTES:
#   - All ID columns are prefixed with file_id (e.g., "48_715090")
#   - Reprocesses ALL files in manifest on each run (no incremental mode)
#   - Default: YATES schedules only (use --all for YATES + SECAI)
#
# =============================================================================

set -e
cd "$(dirname "$0")/../../.."

# Activate virtual environment
source .venv/bin/activate

# Default: Process YATES schedules only (General Contractor)
python scripts/primavera/process/batch_process_xer.py "$@"

# =============================================================================
# USAGE EXAMPLES:
# =============================================================================
#
# ./run.sh                      # YATES schedules only (default)
# ./run.sh --all                # All schedules (YATES + SECAI)
# ./run.sh --schedule-type SECAI  # SECAI schedules only
# ./run.sh --current-only       # Only the current/latest file
# ./run.sh -o /custom/output    # Custom output directory
# ./run.sh --quiet              # Suppress progress messages
#
# =============================================================================
