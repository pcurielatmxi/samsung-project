# XER File Processing Scripts

This directory contains scripts for processing Primavera P6 XER files and converting them to user-friendly CSV formats.

## Quick Start

### Process a Single XER File

```bash
# Basic usage - auto-generates output filename with timestamp
python scripts/process_xer_to_csv.py data/raw/your-file.xer

# Specify output filename
python scripts/process_xer_to_csv.py data/raw/your-file.xer data/output/tasks.csv

# Quiet mode - only output the filename
python scripts/process_xer_to_csv.py data/raw/your-file.xer --quiet
```

### Process Multiple Files

```bash
# Process all XER files in a directory
for file in data/raw/*.xer; do
    python scripts/process_xer_to_csv.py "$file"
done
```

## Script: `process_xer_to_csv.py`

### What It Does

Extracts **ALL tasks** from a Primavera P6 XER file and exports them to CSV with full context including:

- Task codes and descriptions
- Status (Complete, Active, Not Started)
- All date fields (planned, actual, early, late)
- WBS hierarchy
- Activity codes (Area, Level, Building, Subcontractor, Trade, etc.)
- Duration and completion metrics

### Output Format

The script creates a CSV file with **24 columns**:

| Column | Description |
|--------|-------------|
| Task Code | Task identifier |
| Task Description | Full task name |
| Status | Complete, Active, or Not Started |
| WBS | Work Breakdown Structure |
| Building | Building assignment |
| Area | Building area/zone |
| Level | Floor level |
| Room | Specific room/space |
| Phase | Project phase |
| Subcontractor | Assigned subcontractor |
| Trade | Trade type |
| Responsible Person | Person responsible |
| Bid Package | Bid package assignment |
| Planned Start | Target start date |
| Planned Finish | Target end date |
| Actual Start | Actual start date |
| Actual Finish | Actual finish date |
| Early Start | Early start date |
| Early Finish | Early finish date |
| Late Start | Late start date |
| Late Finish | Late finish date |
| Remaining Duration (hrs) | Hours remaining |
| Planned Duration (hrs) | Planned hours |
| Physical % Complete | Completion percentage |

### Command-Line Options

```
usage: process_xer_to_csv.py [-h] [-q] input_file [output_file]

positional arguments:
  input_file     Path to the input XER file
  output_file    Path for the output CSV file (optional)

optional arguments:
  -h, --help     Show help message
  -q, --quiet    Suppress progress messages
```

### Examples

```bash
# Example 1: Process latest schedule export
python scripts/process_xer_to_csv.py data/raw/SAMSUNG-TFAB1-11-20-25-Live-3.xer

# Example 2: Process with custom output location
python scripts/process_xer_to_csv.py \
    data/raw/schedule.xer \
    reports/weekly_tasks.csv

# Example 3: Automated processing (quiet mode)
OUTPUT=$(python scripts/process_xer_to_csv.py data/raw/latest.xer --quiet)
echo "Created: $OUTPUT"
```

## Regular Usage Workflow

### 1. Add New XER Files

Place new XER files in the `data/raw/` directory:

```bash
cp /path/to/new-schedule.xer data/raw/
```

### 2. Process the File

```bash
python scripts/process_xer_to_csv.py data/raw/new-schedule.xer
```

### 3. Find Your Output

Output files are automatically saved to `data/output/xer_exports/` with timestamps:

```
data/output/xer_exports/
├── new-schedule_tasks_20251204_120000.csv
└── previous-schedule_tasks_20251203_150000.csv
```

## Automation

### Weekly Schedule Processing

Create a simple automation script:

```bash
#!/bin/bash
# weekly_schedule_process.sh

# Set paths
XER_DIR="data/raw"
OUTPUT_DIR="data/output/xer_exports"

# Find the most recent XER file
LATEST_XER=$(ls -t "$XER_DIR"/*.xer | head -1)

echo "Processing: $LATEST_XER"

# Process it
python scripts/process_xer_to_csv.py "$LATEST_XER"

echo "Done! Check $OUTPUT_DIR for results."
```

### Batch Processing All Files

```bash
#!/bin/bash
# process_all_xer.sh

for xer_file in data/raw/*.xer; do
    echo "Processing: $xer_file"
    python scripts/process_xer_to_csv.py "$xer_file"
done

echo "All files processed!"
```

## Filtering Specific Tasks

After generating the full task CSV, you can filter for specific work types in Excel, Python, or any tool:

### In Python

```python
import pandas as pd

# Read the full export
df = pd.read_csv('data/output/xer_exports/project_tasks_20251204_120000.csv')

# Filter for drywall tasks
drywall = df[df['Task Description'].str.contains('drywall', case=False, na=False)]
drywall.to_csv('drywall_tasks.csv', index=False)

# Filter for active tasks only
active = df[df['Status'] == 'Active']
active.to_csv('active_tasks.csv', index=False)

# Filter by subcontractor
berg_tasks = df[df['Subcontractor'] == 'BERG']
berg_tasks.to_csv('berg_tasks.csv', index=False)

# Filter by level
level_2 = df[df['Level'].str.contains('L2', na=False)]
level_2.to_csv('level_2_tasks.csv', index=False)
```

### In Excel

1. Open the CSV file
2. Use "Filter" (Data → Filter)
3. Click dropdown arrows to filter by:
   - Status (Active, Complete, Not Started)
   - Subcontractor
   - Level
   - Area
   - Any other column

## Troubleshooting

### "File not found" error

Make sure the XER file path is correct:

```bash
ls data/raw/*.xer  # Check what files exist
```

### Missing activity codes

Some XER files may not have all activity code types. The script will skip any that don't exist and only include available ones.

### Very large files

For XER files with 50,000+ tasks, processing may take 1-2 minutes. Use `--quiet` mode to reduce output:

```bash
python scripts/process_xer_to_csv.py large-file.xer --quiet
```

## Additional Resources

- See `notebooks/xer_to_csv_converter.ipynb` for interactive exploration
- See `src/utils/xer_parser.py` for the underlying parser implementation
- XER file format reference: Primavera P6 documentation

## Support

For issues or questions:
1. Check that pandas is installed: `pip install pandas`
2. Verify XER file is valid (can be opened in Primavera P6)
3. Check logs for specific error messages
