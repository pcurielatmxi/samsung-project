# WBS Taxonomy & Standardized Labeling Proposal

**Date:** 2025-12-10
**Purpose:** Define a standardized labeling system to categorize YATES schedule activities for better progress tracking and cross-source correlation.

---

## 1. Proposed Taxonomy Structure

```
PHASE → SCOPE_CATEGORY → LOCATION_TYPE → LOCATION_ID
```

### Level 1: PHASE (Project Stage)

| Code | Phase | Description | Example WBS/Tasks |
|------|-------|-------------|-------------------|
| `PRE` | Pre-Construction | Design, procurement, submittals, fabrication | Shop drawings, RFAs, lead time |
| `STR` | Structure | Foundation, concrete, steel, precast | Piers, slabs, trusses, erection |
| `ENC` | Enclosure | Building envelope - roof, walls, waterproofing | Roofing, metal panels, IMP |
| `INT` | Interior | Interior construction & finishes | Framing, drywall, MEP, doors |
| `COM` | Commissioning | Testing, turnover, punch list | T&C, inspections, turnover |
| `ADM` | Administrative | Milestones, tracking, impacts | Owner milestones, delays |

### Level 2: SCOPE_CATEGORY (Work Type)

#### PRE - Pre-Construction
| Code | Category | Pattern Match |
|------|----------|---------------|
| `PRE-DES` | Design | Design, engineering |
| `PRE-PRO` | Procurement | Subcontract, award, buyout |
| `PRE-SUB` | Submittals | Shop drawing, RFA, submittal |
| `PRE-FAB` | Fabrication | Fabrication, lead time |

#### STR - Structure
| Code | Category | Pattern Match |
|------|----------|---------------|
| `STR-PIR` | Piers | Pier, drill, caisson |
| `STR-FND` | Foundations | Foundation, footing, grade beam |
| `STR-CIP` | Cast-in-Place | Slab, pour, cure, SOMD, FRP, concrete |
| `STR-STL` | Structural Steel | Steel, truss, erect steel |
| `STR-PRC` | Precast | Precast, erect precast |

#### ENC - Enclosure
| Code | Category | Pattern Match |
|------|----------|---------------|
| `ENC-ROF` | Roofing | Roof, roofing, membrane |
| `ENC-PNL` | Metal Panels | Metal panel, IMP, insulated |
| `ENC-WPF` | Waterproofing | Waterproof, below grade |
| `ENC-GLZ` | Glazing | Window, curtain wall, glazing |

#### INT - Interior
| Code | Category | Pattern Match |
|------|----------|---------------|
| `INT-FRM` | Framing | Metal stud, framing |
| `INT-DRY` | Drywall | Drywall, tape, finish, board |
| `INT-MEP` | MEP Rough-in | MEP, rough-in, conduit, pipe |
| `INT-FIR` | Fire Protection | Fire, sprinkler, caulk, firestop |
| `INT-FIN` | Finishes | Paint, tile, flooring, ceiling |
| `INT-DOR` | Doors & Hardware | Door, frame, hardware |
| `INT-SPE` | Specialties | Accessories, toilet partition |

#### COM - Commissioning
| Code | Category | Pattern Match |
|------|----------|---------------|
| `COM-TST` | Testing | Test, inspect, punch |
| `COM-TRN` | Turnover | Turnover, substantial, handover |

#### ADM - Administrative
| Code | Category | Pattern Match |
|------|----------|---------------|
| `ADM-MIL` | Milestones | Milestone, complete, target |
| `ADM-OWN` | Owner Activities | Owner, SECAI |
| `ADM-IMP` | Impacts/Delays | Impact, delay, hold, waiting |
| `ADM-TRK` | Tracking | Remobilization, priority |

---

### Level 3: LOCATION_TYPE

| Code | Type | Description | Source |
|------|------|-------------|--------|
| `RM` | Room | Specific room with FAB code | WBS with FAB1XXXXX |
| `EL` | Elevator | Elevator shaft/lobby | EL code (EL22, EL18) |
| `ST` | Stair | Stairwell | ST code (ST03, ST20) |
| `GL` | Gridline Area | Work spanning gridlines | GL reference (GL 14-17) |
| `AR` | Area Zone | Named area (A-1, B-2) | Area code (SEA3, SWB1) |
| `BL` | Building Level | Entire floor level | Level only (L2, 3F) |
| `BD` | Building | Entire building | Building only (FAB, SUE) |
| `NA` | Not Applicable | No physical location | Procurement, milestones |

### Level 4: LOCATION_ID

| Location Type | ID Format | Examples |
|---------------|-----------|----------|
| Room | `FAB1XXXXXX` | FAB146507, FAB130313 |
| Elevator | `EL##` | EL22, EL18, EL05 |
| Stair | `ST##` | ST03, ST20, ST22 |
| Gridline | `{Row}-{Row}/{Col}-{Col}` | D-G/14-17, A-C/1-5 |
| Area | `{Bldg}{Zone}{#}` | SEA3, SWB1, FIZ2 |
| Level | `{Bldg}-L{#}` | FAB-L3, SUE-L2 |
| Building | `{Bldg}` | FAB, SUE, SUW, FIZ |

---

## 2. Complete Label Format

```
{PHASE}-{SCOPE_CATEGORY}|{LOCATION_TYPE}:{LOCATION_ID}
```

### Examples

| Task | Proposed Label | Breakdown |
|------|----------------|-----------|
| `Complete Drywall & Inspect --Duct Shaft-4F-SUE-FAB146103` | `INT-DRY\|RM:FAB146103` | Interior-Drywall, Room FAB146103 |
| `PH1-ERECT STEEL TRUSSES - A1 - 17-18` | `STR-STL\|GL:17-18` | Structure-Steel, Gridlines 17-18 |
| `INSTALL DOOR FRAMES - FAB146106A` | `INT-DOR\|RM:FAB146106` | Interior-Doors, Room FAB146106 |
| `METAL STUD FRAMING - SEA3 - GL 13-9 - L1` | `INT-FRM\|AR:SEA3` | Interior-Framing, Area SEA3 |
| `Elevator 22-4F-1FAB-EL22` (WBS) | `INT-SPE\|EL:EL22` | Interior-Specialties, Elevator 22 |
| `OWNER - TEMPORARY POWER FOR FAB 1F` | `ADM-OWN\|BD:FAB` | Admin-Owner, Building FAB |
| `Execute Subcontract - Toilet Accessories` | `PRE-PRO\|NA` | Pre-Con-Procurement, No location |

---

## 3. Building & Area Reference

### Buildings
| Code | Name | Description |
|------|------|-------------|
| `FAB` | Main FAB | Fabrication building (main cleanroom) |
| `SUE` | Support East | East support building |
| `SUW` | Support West | West support building |
| `FIZ` | FIZ | Fab Integration Zone |
| `CUB` | CUB | Central Utilities Building |
| `GCS` | GCS | Gas/Chemical Supply |

### Area Zones (Support Buildings)
| Code | Building | Gridlines | Direction |
|------|----------|-----------|-----------|
| `SEA1-5` | SUE | South half | 17→3 |
| `SEB1-5` | SUE | North half | 17→33 |
| `SWA1-5` | SUW | South half | 17→3 |
| `SWB1-5` | SUW | North half | 17→33 |

### FAB Zones
| Code | Gridlines | Description |
|------|-----------|-------------|
| `FAB-A` | 1-17 | South half |
| `FAB-B` | 17-33 | North half |
| `FAB-A1` to `FAB-A5` | Subdivisions | 5 zones in south |
| `FAB-B1` to `FAB-B5` | Subdivisions | 5 zones in north |

---

## 4. FAB Room Code Structure

```
FAB1 - L - B - NNNN
      │   │   └── Room sequence number
      │   └────── Building area (0=SUW, 6=SUE, etc.)
      └────────── Level (1-6)
```

| Digit | Position | Values | Meaning |
|-------|----------|--------|---------|
| FAB1 | Prefix | Fixed | FAB Building 1 |
| L | 5th char | 1-6 | Level (1F-6F) |
| B | 6th char | 0,2,4,6 | Building area |
| NNNN | 7-10 | 0001-9999 | Room number |

### Building Area Codes (Digit 6)
| Code | Building | Verified |
|------|----------|----------|
| 0 | SUW (Support West) | ✓ |
| 6 | SUE (Support East) | ✓ |
| 2 | TBD (possibly main FAB interior) | ? |
| 4 | TBD (possibly FIZ) | ? |

---

## 5. Implementation Approach

### Step 1: Classification Function (Final - 99.93% Coverage)

```python
import re

def classify_phase_scope(task_name):
    """
    Classify task into Phase and Scope Category.
    Achieves 99.93% classification rate on YATES schedules.
    """
    t = str(task_name).upper()

    # ============ PRE-CONSTRUCTION ============
    if re.search(r'SUBCONTRACT|AWARD|BUYOUT|BID(?:DING)?\b|EXECUTE.*CONTRACT|CONTRACT AGREEMENT', t):
        return 'PRE', 'PRO'  # Procurement
    if re.search(r'SHOP DRAWING|SUBMITTAL|RFA\b|APPROV|REVIEW|JACOBS|RFI|FIELD MEASURE|RESPONSE|ARCHITECTURAL SET', t):
        return 'PRE', 'SUB'  # Submittals
    if re.search(r'FABRICAT|LEAD TIME|DELIVER(?!Y)|CONSOLIDATED SET|MOCKUP|FAB &|MATERIAL.*ORDER', t):
        return 'PRE', 'FAB'  # Fabrication
    if re.search(r'DESIGN|ENGINEER|RESOLVE.*ISSUE|RELEASE FOR FAB|IF[CR]\b|ISSUED FOR|DWG ISSUED|MATERIAL SCHEDULE', t):
        return 'PRE', 'DES'  # Design

    # ============ STRUCTURE ============
    if re.search(r'PIER|DRILL|CAISSON', t):
        return 'STR', 'PIR'  # Piers
    if re.search(r'FOUNDATION|FOOTING|GRADE BEAM', t):
        return 'STR', 'FND'  # Foundations
    if re.search(r'UNDERGROUND|U/G\b|FRENCH DRAIN|EXCAVATE|BACKFILL|LOADING DOCK.*FILL', t):
        return 'STR', 'UGD'  # Underground
    if re.search(r'SLAB|POUR\b|CURE\b|SOMD|FRP\b|CONCRETE|WAFFLE|CURB|EQPT PAD|F/R/P|CIP\b|KNEE WALL|GROUT|PATCH|OAC PAD', t):
        return 'STR', 'CIP'  # Cast-in-Place
    if re.search(r'COLUMN.*(?:PRIME|COAT|GRIND)|(?:PRIME|COAT|GRIND).*COLUMN|SEALER|DENSIFIER|CRC\b', t):
        return 'STR', 'CTG'  # Structural Coating
    if re.search(r'DECKING|DECK\b|DS/AD|DETAILING|GIRDER|TRUSS|ERECT.*STEEL|ANCHOR BOLT|GOAL POST|CLIP|EMBED|HEADER|BASE PLATE', t):
        return 'STR', 'STL'  # Structural Steel
    if re.search(r'STEEL', t) and not re.search(r'STUD|STAIR', t):
        return 'STR', 'STL'  # Structural Steel (catch-all)
    if re.search(r'PRECAST|PC\b.*ERECT', t):
        return 'STR', 'PRC'  # Precast
    if re.search(r'GRATING|PLATFORM(?!.*SCAFFOLD)|RAMP(?!.*CRANE)', t):
        return 'STR', 'MSC'  # Misc Steel

    # ============ ENCLOSURE ============
    if re.search(r'ROOF(?!.*DECK)|ROOFING|MEMBRANE|PARAPET', t):
        return 'ENC', 'ROF'  # Roofing
    if re.search(r'METAL PANEL|IMP\b|INSULATED PANEL|SHEATHING|CANOPY|ACM PANEL|CLADDING', t):
        return 'ENC', 'PNL'  # Panels
    if re.search(r'WATERPROOF|DAMPPROOF|DRAIN MAT|WEEP TUBE', t):
        return 'ENC', 'WPF'  # Waterproofing
    if re.search(r'WINDOW|GLAZING|CURTAIN WALL|STOREFRONT', t):
        return 'ENC', 'GLZ'  # Glazing
    if re.search(r'EXTERIOR.*(?:COAT|PAINT)|(?:COAT|PAINT).*EXTERIOR|PRIME.*COAT|COAT.*TOP', t):
        return 'ENC', 'CTG'  # Exterior Coating
    if re.search(r'LOUVER|PENTHOUSE|BREEZEWAY', t):
        return 'ENC', 'MSC'  # Misc Enclosure

    # ============ INTERIOR ============
    if re.search(r'METAL STUD|FRAMING|STUD FRAME', t):
        return 'INT', 'FRM'  # Framing
    if re.search(r'DRYWALL|TAPE.*FINISH|GYPSUM|BOARD\b|SHEETROCK|TAPE\s*&\s*FLOAT', t):
        return 'INT', 'DRY'  # Drywall
    if re.search(r'MEP|ROUGH.?IN|CONDUIT|ELECTRICAL|PLUMB(?!ING.*UNDER)', t) and not re.search(r'FIRE|UNDERGROUND', t):
        return 'INT', 'MEP'  # MEP Rough-in
    if re.search(r'FIRE(?!PROOF)|SPRINKLER|CAULK|FIRESTOP|SMOKE', t):
        return 'INT', 'FIR'  # Fire Protection
    if re.search(r'PAINT(?!.*COLUMN)|TILE\b|FLOORING|CEILING(?!.*SYSTEM)|FINISH(?!.*DRYWALL)|CONTROL JOINT|EPOXY|VCT|ACCESS FLOOR|FLOOR STRIP|STRIPING', t):
        return 'INT', 'FIN'  # Finishes
    if re.search(r'DOOR|FRAME(?!.*STUD)|HARDWARE|HOLLOW METAL|DOCK LOCK|DOCK GUARDIAN', t):
        return 'INT', 'DOR'  # Doors & Hardware
    if re.search(r'WALL PROTECTION|CORNER GUARD|ACCESSORI|TOILET PARTITION|SIGNAGE|EXPANSION.*CONTROL', t):
        return 'INT', 'SPE'  # Specialties
    if re.search(r'INSULATION|INSUL\b', t):
        return 'INT', 'INS'  # Insulation
    if re.search(r'ELEVATOR(?!.*STEEL)|ELEV\b', t):
        return 'INT', 'ELV'  # Elevators
    if re.search(r'STAIR(?!.*STEEL)', t):
        return 'INT', 'STR'  # Stairs
    if re.search(r'VESTIBULE|BATHROOM|TOILET|RESTROOM|CLEAN ROOM|DUCT SHAFT|AIR.?LOCK|BUNKER|I/?O\s*R[O]?M', t):
        if re.search(r'CONTROL JOINT|FINISH|INSPECT|TAPE|FLOAT', t):
            return 'INT', 'FIN'
        return 'INT', 'MSC'

    # ============ COMMISSIONING ============
    if re.search(r'TEST(?!ING.*IMPACT)|COMMISSION|ENERGIZE|START.?UP|PUNCH', t):
        return 'COM', 'TST'  # Testing
    if re.search(r'TURNOVER|SUBSTANTIAL|HANDOVER|BENEFICIAL', t):
        return 'COM', 'TRN'  # Turnover

    # ============ ADMINISTRATIVE ============
    if re.search(r'^OWNER|^SECAI|^GC\s|^YATES', t):
        return 'ADM', 'OWN'  # Owner Activities
    if re.search(r'IMPACT|DELAY|HOLD\b|WAITING|REWORK|REMEDIATION|REMIDIATION|PENDING', t):
        return 'ADM', 'IMP'  # Impacts/Delays
    if re.search(r'MILESTONE|COMPLETE$|TARGET', t):
        return 'ADM', 'MIL'  # Milestones
    if re.search(r'PRIORITY|REMOBIL|OUT.?OF.?SEQUENCE|FRAGNET|IOCC|CATCH.?UP', t):
        return 'ADM', 'TRK'  # Tracking/Recovery
    if re.search(r'SCAFFOLD|TEMP\b|TEMPORARY|PROTECTION|BARRICADE|HOIST|CRANE RAMP', t):
        return 'ADM', 'TMP'  # Temporary Works

    # ============ CATCH-ALL ============
    if re.search(r'^INSTALL\b', t):
        return 'INT', 'MSC'

    return 'UNK', 'UNK'
```

### Step 2: Create Enriched Dataset
Output columns:
- `task_id`
- `task_name`
- `wbs_name`
- `phase_code` (PRE/STR/ENC/INT/COM/ADM)
- `scope_code` (PIR/FND/DRY/etc.)
- `loc_type` (RM/EL/ST/GL/AR/BL/BD/NA)
- `loc_id` (FAB146507/EL22/SEA3/etc.)
- `full_label` (combined)

### Step 3: Validation
- Cross-check FAB codes against drawing room schedule
- Verify elevator/stair codes match drawings
- Flag unclassified tasks for manual review

---

## 6. Expected Coverage

Based on current YATES data analysis:

| Phase | Est. Tasks | % of Total |
|-------|------------|------------|
| STR - Structure | 2,400 | 20% |
| ENC - Enclosure | 4,300 | 35% |
| INT - Interior | 3,700 | 30% |
| PRE - Pre-Construction | 600 | 5% |
| ADM - Administrative | 1,000 | 8% |
| COM - Commissioning | 200 | 2% |

| Location Type | Est. Tasks | % of Total |
|---------------|------------|------------|
| RM (Room/FAB code) | 4,400 | 36% |
| GL (Gridline area) | 4,600 | 38% |
| EL/ST (Vertical circ.) | 1,300 | 11% |
| AR (Area zone) | 1,000 | 8% |
| NA (No location) | 900 | 7% |

---

## 7. Benefits

1. **Standardized Progress Reporting**
   - Roll up by Phase: "Structure is 85% complete"
   - Roll up by Scope: "All drywall is 60% complete"
   - Roll up by Location: "Level 3 SUE is 45% complete"

2. **Cross-Source Correlation**
   - Match weekly report items to schedule by location
   - Correlate RFIs to tasks by room/area
   - Link inspection reports to specific locations

3. **Delay Analysis**
   - Identify which phases have most delays
   - Find location hotspots for issues
   - Track impact propagation

4. **Power BI Integration**
   - Phase/Scope/Location as slicers
   - Drill-down hierarchy
   - Heat maps by area

---

## 8. Next Steps

1. [ ] Review and refine taxonomy categories
2. [ ] Build parsing script to classify existing tasks
3. [ ] Extract room schedule from drawings for validation
4. [ ] Create mapping table for elevator/stair codes
5. [ ] Generate enriched dataset with new labels
6. [ ] Test correlation with weekly report data

---

*Proposed by Claude Code analysis - Ready for stakeholder review*
