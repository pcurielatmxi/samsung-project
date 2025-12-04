# Quick Start: XER Processing for Samsung TFAB1

This guide shows you how to process new XER files on a regular basis.

## 📋 Overview

You now have a complete toolset to process Primavera P6 XER schedule files and convert them to user-friendly CSV format with all context fields (area, level, building, contractor, dates, etc.).

## 🚀 Quick Usage

### Process a Single New XER File

```bash
# Add your new XER file to data/raw/
cp /path/to/new-schedule.xer data/raw/

# Process it
python scripts/process_xer_to_csv.py "data/raw/new-schedule.xer"

# Find your output in data/output/xer_exports/
# File will be named: new-schedule_tasks_YYYYMMDD_HHMMSS.csv
```

### Process All XER Files in data/raw/

```bash
# Run the batch processor
./scripts/batch_process_xer.sh

# Or specify a different directory
./scripts/batch_process_xer.sh path/to/xer/files/
```

## 📊 What You Get

Each processed XER file creates a CSV with **24 columns**:

**Basic Info:**
- Task Code
- Task Description
- Status (Complete, Active, Not Started)

**Location Context:**
- WBS
- Building
- Area
- Level
- Room
- Phase

**Responsibility:**
- Subcontractor
- Trade
- Responsible Person
- Bid Package

**Dates:**
- Planned Start/Finish
- Actual Start/Finish
- Early Start/Finish
- Late Start/Finish

**Metrics:**
- Remaining Duration (hrs)
- Planned Duration (hrs)
- Physical % Complete

## 🔍 Filtering Tasks

### Option 1: Use Excel
1. Open the CSV file
2. Click Data → Filter
3. Use dropdown menus to filter by any column

### Option 2: Use the Filter Script

```bash
# Filter for drywall tasks
python scripts/filter_tasks.py \
    data/output/xer_exports/project_tasks_20251204.csv \
    --keyword "drywall" \
    -o drywall_tasks.csv

# Filter for active tasks
python scripts/filter_tasks.py \
    data/output/xer_exports/project_tasks_20251204.csv \
    --status "Active" \
    -o active_tasks.csv

# Filter by subcontractor
python scripts/filter_tasks.py \
    data/output/xer_exports/project_tasks_20251204.csv \
    --subcontractor "BERG" \
    -o berg_tasks.csv

# Combine filters (active door tasks)
python scripts/filter_tasks.py \
    data/output/xer_exports/project_tasks_20251204.csv \
    --keyword "door" \
    --status "Active" \
    -o active_doors.csv
```

### Option 3: Use Python/Pandas

```python
import pandas as pd

# Load the full export
df = pd.read_csv('data/output/xer_exports/project_tasks.csv')

# Filter examples
drywall = df[df['Task Description'].str.contains('drywall', case=False)]
active = df[df['Status'] == 'Active']
level_2 = df[df['Level'].str.contains('L2', na=False)]
berg = df[df['Subcontractor'] == 'BERG']

# Save filtered results
drywall.to_csv('drywall_tasks.csv', index=False)
```

## 🔁 Regular Workflow

### Weekly Schedule Update

1. **Receive new XER file from scheduler**
   ```bash
   cp ~/Downloads/SAMSUNG-TFAB1-12-04-25.xer data/raw/
   ```

2. **Process it**
   ```bash
   python scripts/process_xer_to_csv.py data/raw/SAMSUNG-TFAB1-12-04-25.xer
   ```

3. **Review the output**
   ```bash
   # Output is in data/output/xer_exports/
   # Opens with any spreadsheet software
   open data/output/xer_exports/SAMSUNG-TFAB1-12-04-25_tasks_*.csv
   ```

4. **Create filtered reports as needed**
   ```bash
   # Example: Active drywall tasks
   python scripts/filter_tasks.py \
       data/output/xer_exports/SAMSUNG-TFAB1-12-04-25_tasks_*.csv \
       --keyword "drywall" \
       --status "Active" \
       -o reports/active_drywall_$(date +%Y%m%d).csv
   ```

## 📁 File Organization

```
mxi-samsung/
├── data/
│   ├── raw/                           # Put new XER files here
│   │   └── *.xer
│   └── output/
│       └── xer_exports/               # Processed CSV files go here
│           └── *_tasks_*.csv
├── scripts/
│   ├── process_xer_to_csv.py         # Main processor
│   ├── batch_process_xer.sh          # Batch processor
│   ├── filter_tasks.py               # Filter utility
│   └── README_XER_PROCESSING.md      # Detailed docs
└── notebooks/
    └── xer_to_csv_converter.ipynb    # Interactive exploration
```

## 🛠️ Scripts Reference

| Script | Purpose | Usage |
|--------|---------|-------|
| `process_xer_to_csv.py` | Process single XER file | `python scripts/process_xer_to_csv.py file.xer` |
| `batch_process_xer.sh` | Process all XER files | `./scripts/batch_process_xer.sh` |
| `filter_tasks.py` | Filter CSV by criteria | `python scripts/filter_tasks.py input.csv --keyword "door" -o output.csv` |

## 💡 Tips

- **Timestamped outputs**: Each run creates a new file with timestamp - nothing is overwritten
- **All fields included**: Even if a field is empty for your project, it's still in the CSV
- **Case-insensitive filtering**: Searches work with any capitalization
- **Excel compatible**: All CSV files open directly in Excel, Google Sheets, etc.

## 📚 Documentation

- **Detailed documentation**: See `scripts/README_XER_PROCESSING.md`
- **Interactive exploration**: Use `notebooks/xer_to_csv_converter.ipynb`
- **XER parser code**: See `src/utils/xer_parser.py`

## ❓ Common Questions

**Q: Can I process multiple files at once?**
A: Yes, use `./scripts/batch_process_xer.sh`

**Q: What if I only want specific tasks?**
A: Use `scripts/filter_tasks.py` or filter in Excel after export

**Q: Does it overwrite previous exports?**
A: No, each export has a unique timestamp in the filename

**Q: Can I automate this weekly?**
A: Yes, create a cron job or scheduled task that runs the script

**Q: What if my XER file has different activity codes?**
A: The script automatically detects and includes available codes

## 🎯 Example: Weekly Report Generation

```bash
#!/bin/bash
# weekly_report.sh - Generate weekly reports

# Get latest XER file
LATEST_XER=$(ls -t data/raw/*.xer | head -1)
echo "Processing: $LATEST_XER"

# Process to full CSV
python scripts/process_xer_to_csv.py "$LATEST_XER"

# Get the output filename (most recent)
FULL_CSV=$(ls -t data/output/xer_exports/*_tasks_*.csv | head -1)

# Create filtered reports
REPORT_DATE=$(date +%Y%m%d)
REPORT_DIR="reports/$REPORT_DATE"
mkdir -p "$REPORT_DIR"

# Active tasks
python scripts/filter_tasks.py "$FULL_CSV" \
    --status "Active" \
    -o "$REPORT_DIR/active_tasks.csv"

# Drywall - all status
python scripts/filter_tasks.py "$FULL_CSV" \
    --keyword "drywall" \
    -o "$REPORT_DIR/drywall_all.csv"

# Doors - active only
python scripts/filter_tasks.py "$FULL_CSV" \
    --keyword "door" \
    --status "Active" \
    -o "$REPORT_DIR/doors_active.csv"

# BERG tasks
python scripts/filter_tasks.py "$FULL_CSV" \
    --subcontractor "BERG" \
    -o "$REPORT_DIR/berg_tasks.csv"

echo "Reports generated in: $REPORT_DIR"
```

---

**Need help?** See detailed documentation in `scripts/README_XER_PROCESSING.md`
