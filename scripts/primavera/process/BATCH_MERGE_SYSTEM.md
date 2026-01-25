# Narrative Findings Batch Merge System

## Overview
This system prevents data loss by using individual batch files that are merged safely into the main CSV. Each batch is preserved as a backup and all merges create timestamped backups before overwriting.

## File Locations
- **Main Database:** `{WINDOWS_DATA_DIR}/processed/primavera_narratives/narrative_findings.csv`
- **Batch Files:** `batch_YYYY-MM-DD_findings.csv` (same directory as main CSV)
- **Merge Script:** `scripts/primavera/process/merge_narrative_batches.py`

## Workflow

### Step 1: Create Batch File
Create a CSV file named `batch_YYYY-MM-DD_findings.csv` with the following columns (NO finding_id column):

```
source_file,subfolder,source_date,xer_file,category,subcategory,description,impact_type,responsible_party,areas_affected,duration_days,related_rfi,verbatim_quote,analyst_notes
```

**Example:**
```csv
TFAB1-Yates - Schedule Narrative 07-17-2024.md,Delays - Narratives & Milestone variance Reports,2024-07-17,SAMSUNG-FAB-07-17-24-YATES.xer,COORDINATION,Equipment access conflict,"Starcon cranes blocking GL 1-6 A line cladding/IMP work, restricting work to weekends only",delay,SECAI,GL 1-6 A line - Cladding/IMP,14,,Starcon cranes blocking GL 1-6 A line cladding/IMP for 2 weeks - only weekend work possible,
```

### Step 2: Run Merge Script
```bash
cd /home/pcuriel/samsung-project/scripts/primavera/process
python3 merge_narrative_batches.py
```

### Step 3: Verify Results
The script will:
1. Read the current main CSV
2. Find all `batch_*.csv` files
3. Create a timestamped backup: `narrative_findings.csv.backup_YYYYMMDD_HHMMSS`
4. Assign sequential finding_ids (starting after current max)
5. Merge all findings
6. Write updated main CSV

Output example:
```
======================================================================
NARRATIVE FINDINGS BATCH MERGE
======================================================================

Reading existing findings...
  Current findings: 117
  Current max finding_id: 117

Found 1 batch files:
  - batch_2024-07-17_findings.csv

Creating backup...
✓ Backup created: narrative_findings.csv.backup_20251218_100112

Merging batch files...
  batch_2024-07-17_findings.csv: 26 findings

Assigning finding IDs (118 to 143)...
Total findings after merge: 143

Writing merged CSV...
✓ Successfully merged 26 new findings
✓ Total findings now: 143

======================================================================
```

## Column Definitions

| Column | Definition | Example |
|--------|-----------|---------|
| source_file | Original narrative document filename | TFAB1-Yates - Schedule Narrative 07-17-2024.md |
| subfolder | Folder containing the narrative | Delays - Narratives & Milestone variance Reports |
| source_date | Date of the narrative (YYYY-MM-DD) | 2024-07-17 |
| xer_file | Associated P6 schedule file | SAMSUNG-FAB-07-17-24-YATES.xer |
| category | Categorization of the finding | COORDINATION, DESIGN, RFI, QUALITY, DELIVERY, etc. |
| subcategory | More specific categorization | Equipment access conflict, Missing design detail, etc. |
| description | Summary of the finding | Starcon cranes blocking GL 1-6 A line... |
| impact_type | Type of impact | delay, cost, quality, scope |
| responsible_party | Who is responsible | YATES, SECAI, Design, Subcontractor, Owner, Other |
| areas_affected | Building areas/systems impacted | GL 1-6 A line - Cladding/IMP |
| duration_days | Impact duration in days | 14 |
| related_rfi | RFI numbers referenced | RFI 1413 |
| verbatim_quote | Direct quote from narrative | Starcon cranes blocking GL 1-6 A line... |
| analyst_notes | Additional notes/context | Optional annotation |

## Safety Features

1. **Timestamped Backups:** Each merge creates a timestamped backup before modifying the main CSV
2. **Batch File Preservation:** Individual batch files remain intact after merge
3. **Clean Merge:** Script validates all fields and handles missing data gracefully
4. **No Overwrite of Originals:** Old batch files are never deleted; you can keep them organized in a subdir

## Recovery Procedures

If something goes wrong:

```bash
# List available backups
ls -lh narrative_findings.csv.backup_*

# Restore from a specific backup (use the timestamp from the file)
cp narrative_findings.csv.backup_20251218_100112 narrative_findings.csv

# The merge script will create a new backup before the next merge attempt
```

## Current Status (as of 2025-12-18)

| Date | File | Findings | Status |
|------|------|----------|--------|
| 2022-10-10 through 2024-02-09 | Earlier narratives | 117 | ✓ Processed |
| 2024-07-17 | batch_2024-07-17_findings.csv | 26 | ✓ Merged |
| 2025-01-24 | batch_2025-01-24_findings.csv | 32 | Pending (batch file creation) |
| 2025-02-21 | batch_2025-02-21_findings.csv | 36 | Partial (10 of 36 created) |

**Total Findings:** 143 (117 + 26 merged)
**Next Step:** Create remaining batch files for 2025-01-24 and 2025-02-21

## Tips

- Use `timeline_status.py` to check current analysis progress
- `get_next_narrative.py` helps identify which files have been processed
- Keep batch files organized by date for easy reference
- Comment out or move processed batch files to an archive folder after successful merge
