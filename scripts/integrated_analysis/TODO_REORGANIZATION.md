# Integrated Analysis Reorganization Plan

**Created:** 2026-01-30
**Status:** Planned

---

## 1. Dimensions Folder Restructure

### Current State
```
dimensions/
├── build_dim_location.py        # ACTIVE
├── build_company_dimension.py   # ACTIVE
├── build_dim_csi_section.py     # ACTIVE
├── build_dim_location_v2.py     # DEPRECATED - never called
├── build_company_hierarchy.py   # DEPRECATED - never called
├── build_dim_csi_division.py    # DEPRECATED - never called
├── add_missing_building_levels.py # DEPRECATED - never called
├── run.sh
└── README.md
```

### Target State
```
dimensions/
├── README.md                         # Update for new structure
├── run.sh                            # Update paths
│
├── location/
│   ├── __init__.py
│   └── build_dim_location.py         # Move from root
│
├── company/
│   ├── __init__.py
│   └── build_dim_company.py          # Move + rename from build_company_dimension.py
│
└── csi_section/
    ├── __init__.py
    └── build_dim_csi_section.py      # Move from root
```

### Migration Steps

- [ ] Create `dimensions/location/` folder
- [ ] Create `dimensions/company/` folder
- [ ] Create `dimensions/csi_section/` folder
- [ ] Move `build_dim_location.py` → `location/build_dim_location.py`
- [ ] Move + rename `build_company_dimension.py` → `company/build_dim_company.py`
- [ ] Move `build_dim_csi_section.py` → `csi_section/build_dim_csi_section.py`
- [ ] Add `__init__.py` to each new folder
- [ ] Delete `build_dim_location_v2.py`
- [ ] Delete `build_company_hierarchy.py`
- [ ] Delete `build_dim_csi_division.py`
- [ ] Delete `add_missing_building_levels.py`
- [ ] Update `scripts/shared/daily_refresh.py` import paths
- [ ] Update `dimensions/run.sh` command paths
- [ ] Update `dimensions/README.md`
- [ ] Test full daily_refresh pipeline

---

## 2. Top-Level Scripts Reorganization

### Current State (17 loose scripts at root)

```
scripts/integrated_analysis/
├── add_csi_to_ncr.py
├── add_csi_to_p6_tasks.py
├── add_csi_to_projectsight.py
├── add_csi_to_quality_consolidated.py
├── add_csi_to_quality_records.py
├── add_csi_to_tbm.py
├── add_contractor_inference.py
├── enrich_fact_tables_location.py
├── extract_data_quality_columns.py
├── generate_affected_rooms_bridge.py
├── generate_company_location_mapping.py
├── period_summary.py
├── room_timeline.py
├── room_timeline_enhanced.py
├── schedule_slippage_analysis.py
├── weekly_summary.py
└── ... (various __pycache__, etc.)
```

### Target State

```
scripts/integrated_analysis/
├── __init__.py                       # NEW - make proper package
├── CLAUDE.md
├── PLAN.md
│
├── enrichment/                       # NEW - fact table enrichment
│   ├── __init__.py
│   ├── add_csi_to_ncr.py
│   ├── add_csi_to_p6_tasks.py
│   ├── add_csi_to_projectsight.py
│   ├── add_csi_to_quality_consolidated.py
│   ├── add_csi_to_quality_records.py
│   ├── add_csi_to_tbm.py
│   ├── add_contractor_inference.py
│   ├── enrich_fact_tables_location.py
│   └── extract_data_quality_columns.py
│
├── analysis/                         # NEW - investigation scripts
│   ├── __init__.py
│   ├── schedule_slippage_analysis.py
│   ├── room_timeline.py              # Consolidate room_timeline*.py
│   ├── period_summary.py
│   └── weekly_summary.py
│
├── generators/                       # NEW - bridge/mapping generators
│   ├── __init__.py
│   ├── generate_affected_rooms_bridge.py
│   └── generate_company_location_mapping.py
│
├── location/                         # KEEP - already well-organized
├── dimensions/                       # KEEP - restructure per section 1
├── data_quality/                     # KEEP - already well-organized
├── context/                          # KEEP - claims/contracts docs
└── mappings/                         # KEEP or merge into generators/
```

### Migration Steps

- [ ] Add `__init__.py` to `scripts/integrated_analysis/`
- [ ] Create `enrichment/` folder with `__init__.py`
- [ ] Create `analysis/` folder with `__init__.py`
- [ ] Create `generators/` folder with `__init__.py`
- [ ] Move `add_csi_to_*.py` (6 files) → `enrichment/`
- [ ] Move `add_contractor_inference.py` → `enrichment/`
- [ ] Move `enrich_fact_tables_location.py` → `enrichment/`
- [ ] Move `extract_data_quality_columns.py` → `enrichment/`
- [ ] Move `schedule_slippage_analysis.py` → `analysis/`
- [ ] Move `room_timeline*.py` → `analysis/` (consolidate if possible)
- [ ] Move `period_summary.py` → `analysis/`
- [ ] Move `weekly_summary.py` → `analysis/`
- [ ] Move `generate_*.py` (2 files) → `generators/`
- [ ] Update `scripts/shared/daily_refresh.py` import paths
- [ ] Update `CLAUDE.md` to reflect new structure
- [ ] Decide: merge `mappings/` into `generators/` or keep separate

---

## 3. Documentation Updates

- [ ] Update `scripts/integrated_analysis/CLAUDE.md`
  - Fix `validate/` → `data_quality/` reference
  - Document new folder structure
- [ ] Update `dimensions/README.md` for subfolder structure
- [ ] Update root `CLAUDE.md` if needed

---

## 4. Configuration Fixes

### run.sh Issues
- [ ] Remove `dim-trade` command (references deleted `build_dim_trade.py`)
- [ ] Update paths for new dimension folder structure

### daily_refresh.py Updates
After reorganization, update these calls:
```python
# Dimensions (Section 1)
# Before: scripts.integrated_analysis.dimensions.build_dim_location
# After:  scripts.integrated_analysis.dimensions.location.build_dim_location

# Before: scripts.integrated_analysis.dimensions.build_company_dimension
# After:  scripts.integrated_analysis.dimensions.company.build_dim_company

# Before: scripts.integrated_analysis.dimensions.build_dim_csi_section
# After:  scripts.integrated_analysis.dimensions.csi_section.build_dim_csi_section

# Enrichment (Section 2)
# Before: scripts.integrated_analysis.add_csi_to_p6_tasks
# After:  scripts.integrated_analysis.enrichment.add_csi_to_p6_tasks
# (and similar for other enrichment scripts)
```

---

## Priority Order

1. **Dimensions folder** (Section 1) - Most requested, clear scope
2. **Top-level scripts** (Section 2) - Larger change, more import updates
3. **Documentation** (Section 3) - After structure is stable
4. **Config fixes** (Section 4) - Alongside each section

---

## Notes

- All deprecated dimension scripts confirmed as dead code (never called from daily_refresh.py or run.sh)
- `build_company_dimension.py` renamed to `build_dim_company.py` for naming consistency
- Git history preserved through moves (use `git mv`)
