"""
Process quality inspection data from SECAI and Yates sources.

Inputs:
    - SECAI: 05282025_USA T1 Project_Inspection and Test Log.xlsx
    - Yates: Yates- WORK INSPECTION REQUEST FAB1 LOG 6.25 REFINED.xlsx

Outputs:
    - secai_inspection_log.csv
    - yates_all_inspections.csv (with Category column: INTERNAL/OFFICIAL)
"""

import pandas as pd
import os
from pathlib import Path

# Paths
RAW_DIR = Path("/mnt/c/Users/pcuri/OneDrive - MXI/Desktop/Samsung Dashboard/Data/raw/quality")
OUTPUT_DIR = Path("/home/pcuriel/samsung-project/data/processed/quality")

SECAI_FILE = RAW_DIR / "05282025_USA T1 Project_Inspection and Test Log.xlsx"
YATES_FILE = RAW_DIR / "Yates- WORK INSPECTION REQUEST FAB1 LOG 6.25 REFINED.xlsx"


def process_secai_inspections():
    """Process SECAI Inspection and Test Log."""
    print("Processing SECAI Inspection and Test Log...")

    dfs = {}
    for sheet in ['ARCH', 'MECH', 'ELEC']:
        df = pd.read_excel(SECAI_FILE, sheet_name=sheet, header=0)
        df.columns = df.iloc[0].values
        df = df.iloc[1:].reset_index(drop=True)
        dfs[sheet] = df

    secai_all = pd.concat(dfs.values(), ignore_index=True)

    # Remove nan columns
    secai_all = secai_all.loc[:, secai_all.columns.notna()]

    # Handle duplicate column names
    cols = list(secai_all.columns)
    new_cols = []
    seen = {}
    for col in cols:
        col_str = str(col).strip()
        if col_str in seen:
            seen[col_str] += 1
            new_cols.append(f"{col_str}_{seen[col_str]}")
        else:
            seen[col_str] = 0
            new_cols.append(col_str)
    secai_all.columns = new_cols

    # Clean dates and filter bad data
    secai_all['Inspection Request Date'] = pd.to_datetime(
        secai_all['Inspection Request Date'], errors='coerce'
    )
    secai_clean = secai_all[secai_all['Inspection Request Date'] >= '2020-01-01'].copy()

    # Select relevant columns
    keep_cols = [
        'Discipline', 'Number', 'Request Date', 'Inspection Request Date',
        'Status', 'IR Number', 'Revision', 'Building Type',
        'System / Equip/ Location', 'Author Company', 'Template',
        'Module', 'Reasons for failure'
    ]
    secai_output = secai_clean[[c for c in keep_cols if c in secai_clean.columns]].copy()

    # Add calculated fields
    secai_output['Year'] = secai_output['Inspection Request Date'].dt.year
    secai_output['Month'] = secai_output['Inspection Request Date'].dt.month
    secai_output['Week'] = secai_output['Inspection Request Date'].dt.isocalendar().week
    secai_output['DayOfWeek'] = secai_output['Inspection Request Date'].dt.day_name()
    secai_output['Status_Normalized'] = secai_output['Status'].str.upper().str.strip()

    output_file = OUTPUT_DIR / "secai_inspection_log.csv"
    secai_output.to_csv(output_file, index=False)
    print(f"  Saved: {output_file}")
    print(f"  Records: {len(secai_output)}")

    return secai_output


def process_yates_inspections():
    """Process Yates Work Inspection Request Log (Internal + Official combined)."""
    print("Processing Yates Work Inspection Request Log...")

    # Load Internal inspections (header at row 3)
    internal = pd.read_excel(YATES_FILE, sheet_name='INTERNAL-YATES', header=3)
    internal = internal.dropna(how='all')
    internal['Category'] = 'INTERNAL'

    # Load Official inspections (header at row 2)
    official = pd.read_excel(YATES_FILE, sheet_name='OFFICIAL-YATES', header=2)
    official = official.dropna(how='all')
    official['Category'] = 'OFFICIAL'

    # Combine
    yates_combined = pd.concat([internal, official], ignore_index=True)

    # Clean dates and filter bad data
    yates_combined['Date'] = pd.to_datetime(yates_combined['Date'], errors='coerce')
    yates_clean = yates_combined[yates_combined['Date'] >= '2020-01-01'].copy()

    # Add calculated fields
    yates_clean['Year'] = yates_clean['Date'].dt.year
    yates_clean['Month'] = yates_clean['Date'].dt.month
    yates_clean['Week'] = yates_clean['Date'].dt.isocalendar().week
    yates_clean['DayOfWeek'] = yates_clean['Date'].dt.day_name()
    yates_clean['Status_Normalized'] = yates_clean['Inspection Status'].str.upper().str.strip()

    output_file = OUTPUT_DIR / "yates_all_inspections.csv"
    yates_clean.to_csv(output_file, index=False)
    print(f"  Saved: {output_file}")
    print(f"  Records: {len(yates_clean)}")

    return yates_clean


def main():
    """Main entry point."""
    print("=" * 60)
    print("Quality Inspection Data Processing")
    print("=" * 60)

    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Process both sources
    secai = process_secai_inspections()
    yates = process_yates_inspections()

    print("\n" + "=" * 60)
    print("Processing Complete")
    print("=" * 60)
    print(f"SECAI records: {len(secai)}")
    print(f"Yates records: {len(yates)}")
    print(f"Output directory: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
