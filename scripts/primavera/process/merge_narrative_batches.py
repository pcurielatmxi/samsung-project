#!/usr/bin/env python3
"""
Safe merge script for narrative findings.

Usage:
  1. Create batch files (e.g., batch_2024-07-17_findings.csv, batch_2025-01-24_findings.csv)
  2. Run this script to merge all batches into narrative_findings.csv
  3. Individual batch files are preserved as backups

Batch file format (no finding_id column):
  source_file,subfolder,source_date,xer_file,category,subcategory,description,impact_type,responsible_party,areas_affected,duration_days,related_rfi,verbatim_quote,analyst_notes
"""

import csv
import os
import glob
import shutil
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config.settings import Settings

BASE_DIR = str(Settings.PROCESSED_DATA_DIR / 'primavera_narratives')
MAIN_CSV = os.path.join(BASE_DIR, 'narrative_findings.csv')
BATCH_PATTERN = os.path.join(BASE_DIR, 'batch_*_findings.csv')

FIELDNAMES = ['finding_id', 'source_file', 'subfolder', 'source_date', 'xer_file', 'finding_date', 
              'category', 'subcategory', 'description', 'impact_type', 'responsible_party', 
              'areas_affected', 'duration_days', 'related_rfi', 'verbatim_quote', 'analyst_notes']

BATCH_FIELDNAMES = FIELDNAMES[1:]  # Without finding_id

def read_existing_findings():
    """Read existing findings from main CSV."""
    findings = []
    if os.path.exists(MAIN_CSV):
        with open(MAIN_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Clean row: remove None keys and ensure all expected fieldnames are present
                clean_row = {k: v for k, v in row.items() if k in FIELDNAMES}
                for field in FIELDNAMES:
                    if field not in clean_row:
                        clean_row[field] = ''
                findings.append(clean_row)
    return findings

def read_batch_file(batch_file):
    """Read findings from a batch file."""
    findings = []
    if os.path.exists(batch_file):
        with open(batch_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Clean row: remove None keys
                clean_row = {k: v for k, v in row.items() if k is not None}
                # Add finding_date equal to source_date if not present
                if 'finding_date' not in clean_row or not clean_row.get('finding_date'):
                    clean_row['finding_date'] = clean_row.get('source_date', '')
                # Ensure all batch fields are present
                for field in BATCH_FIELDNAMES:
                    if field not in clean_row:
                        clean_row[field] = ''
                findings.append(clean_row)
    return findings

def get_max_finding_id(findings):
    """Get the maximum finding_id from existing findings."""
    max_id = 0
    for finding in findings:
        try:
            fid = int(finding.get('finding_id', 0))
            if fid > max_id:
                max_id = fid
        except (ValueError, TypeError):
            pass
    return max_id

def create_backup(filepath):
    """Create a backup of the file with timestamp."""
    if os.path.exists(filepath):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = f"{filepath}.backup_{timestamp}"
        shutil.copy(filepath, backup_path)
        print(f"✓ Backup created: {backup_path}")
        return backup_path
    return None

def main():
    print("\n" + "="*70)
    print("NARRATIVE FINDINGS BATCH MERGE")
    print("="*70 + "\n")
    
    # Read existing findings
    print("Reading existing findings...")
    existing_findings = read_existing_findings()
    print(f"  Current findings: {len(existing_findings)}")
    
    # Get max ID
    max_id = get_max_finding_id(existing_findings)
    print(f"  Current max finding_id: {max_id}")
    
    # Find all batch files
    batch_files = sorted(glob.glob(BATCH_PATTERN))
    print(f"\nFound {len(batch_files)} batch files:")
    for bf in batch_files:
        print(f"  - {os.path.basename(bf)}")
    
    if not batch_files:
        print("\n⚠ No batch files found. Create batch files before running merge.")
        return
    
    # Create backup
    print("\nCreating backup...")
    backup_file = create_backup(MAIN_CSV)
    
    # Read and merge all batch files
    print("\nMerging batch files...")
    new_findings = []
    total_batch_findings = 0
    
    for batch_file in batch_files:
        batch_name = os.path.basename(batch_file)
        batch_data = read_batch_file(batch_file)
        print(f"  {batch_name}: {len(batch_data)} findings")
        new_findings.extend(batch_data)
        total_batch_findings += len(batch_data)
    
    # Assign finding IDs to new findings and ensure all fields
    print(f"\nAssigning finding IDs ({max_id + 1} to {max_id + total_batch_findings})...")
    for idx, finding in enumerate(new_findings, 1):
        finding['finding_id'] = str(max_id + idx)
        # Ensure all fieldnames are present
        for field in FIELDNAMES:
            if field not in finding:
                finding[field] = ''

    # Combine all findings
    all_findings = existing_findings + new_findings
    print(f"Total findings after merge: {len(all_findings)}")

    # Validate all rows have required fields
    for finding in all_findings:
        for field in FIELDNAMES:
            if field not in finding:
                finding[field] = ''

    # Write merged CSV
    print(f"\nWriting merged CSV to {os.path.basename(MAIN_CSV)}...")
    with open(MAIN_CSV, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(all_findings)
    
    print(f"✓ Successfully merged {total_batch_findings} new findings")
    print(f"✓ Total findings now: {len(all_findings)}")
    print("\n" + "="*70 + "\n")

if __name__ == '__main__':
    main()
