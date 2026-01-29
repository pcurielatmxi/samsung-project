# Integrated Analysis

**Last Updated:** 2026-01-29

Phase 2 of Samsung Taylor FAB1 analysis: **Cross-Dataset Integration**.

## Purpose

Create dimension and mapping tables that enable joining data across sources (P6, ProjectSight, TBM, RABA, PSI, Quality Records) to answer: *"What happened at this location, by whom, and what was the impact?"*

## Folder Structure

```
scripts/integrated_analysis/
├── CLAUDE.md            # This file
├── PLAN.md              # Detailed specification
├── location/            # ** CENTRALIZED LOCATION PROCESSING **
│   └── CLAUDE.md        # Location module docs (see below)
├── dimensions/          # Dimension table builders
├── mappings/            # Mapping table builders
├── context/             # Claims, contracts, correspondence
│   └── README.md        # Organization guidelines
├── validate/            # Coverage validation
├── schedule_slippage_analysis.py  # Schedule delay analysis
└── room_timeline_enhanced.py      # Room-level investigation
```

## Dimension Tables

| Table | Purpose | Key Fields |
|-------|---------|------------|
| `dim_company` | Master company list | company_id, canonical_name, aliases |
| `dim_location` | Building + Level + Grid bounds | location_id, building, level, grid_* |
| `dim_trade` | Trade/work type classification | trade_id, trade_code, trade_name |
| `dim_time` | Calendar dimension | date_id, year, month, week |

## Mapping Tables

| Table | Purpose |
|-------|---------|
| `map_company_aliases` | Source-specific names -> company_id |
| `map_company_location` | Company -> typical locations by period |
| `map_location_codes` | Source formats -> location_id |

## Location Processing

**CRITICAL:** All location enrichment MUST use the centralized module.

```python
from scripts.integrated_analysis.location import enrich_location

result = enrich_location(building='FAB', level='2F', grid='G/10', source='RABA')
# Returns: dim_location_id, location_type, affected_rooms, match_type
```

**Full documentation:** See `scripts/integrated_analysis/location/CLAUDE.md`

## Output Location

```
data/processed/integrated_analysis/
├── dimensions/    # dim_*.csv files
├── mappings/      # map_*.csv files
└── validation/    # Coverage reports
```

## Schedule Slippage Analysis

**Script:** `schedule_slippage_analysis.py`

Analyzes schedule slippage between P6 snapshots to identify delay drivers.

**Core metrics:**
- `finish_slip` / `start_slip` - Date movement between snapshots
- `own_delay` - Delay caused by this task (duration overrun)
- `inherited_delay` - Delay pushed from predecessors

**Key features:**
- Task categorization (ACTIVE_DELAYER, COMPLETED_DELAYER, WAITING_*)
- Fast-tracking and reopened task detection
- What-if recovery analysis with parallel path constraints
- Full slippage accounting with investigation checklist

**Usage:**
```bash
python -m scripts.integrated_analysis.schedule_slippage_analysis --year 2025 --month 9 --attribution
python -m scripts.integrated_analysis.schedule_slippage_analysis --list-schedules
```

**Full methodology:** See `docs/analysis/delay_attribution_methodology.md`

## Room Timeline Analysis

**Script:** `room_timeline_enhanced.py`

Combines quality inspections, TBM entries, and narrative documents for room-level investigation.

```bash
python -m scripts.integrated_analysis.room_timeline_enhanced FAB116406 \
  --start 2024-01-01 --end 2024-03-31 --with-context
```

## Context Documents

The `context/` folder contains contractual and legal context for analysis:
- `claims/` - Change Orders (CO) and EOT claims
- See `context/README.md` for organization guidelines

## Usage Notes

1. **Run dimension builders first** - mappings depend on dimensions
2. **Manual review required** - company aliases need verification
3. **Period-aware** - company->location mappings vary by time
4. **Confidence flags** - inferred locations should be flagged

## Integration Granularity

**Building + Level** (primary): 66-98% coverage across sources
**Room + Grid** (when available): Enables spatial joins via grid containment

## Data Traceability

Outputs are **derived** data (includes assumptions, not fully raw-traceable):
- Company aliases are curated
- Location inference uses statistical distribution
