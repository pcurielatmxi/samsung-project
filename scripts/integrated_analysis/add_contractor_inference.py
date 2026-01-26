#!/usr/bin/env python3
"""
Add Contractor Inference to Quality Data.

Assigns actual contractor using prioritized inference logic. Creates new columns:
- inferred_contractor_id: The actual contractor based on best available data
- contractor_inference_source: How the contractor was determined

Inference Priority:
1. Subcontractor field (from source document - most reliable)
2. Specific contractor listed (not Samsung/blank)
3. Samsung/blank + CSI in P6 scope → Assign to Yates (scope-based inference)
4. Otherwise → Keep original

This enables accurate Yates performance analysis by filtering to inferred_contractor = Yates.

Updates files in place:
    {WINDOWS_DATA_DIR}/processed/raba/raba_consolidated.csv
    {WINDOWS_DATA_DIR}/processed/psi/psi_consolidated.csv
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

_project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.config.settings import settings


def load_p6_csi_sections():
    """Load CSI sections that exist in P6 (Yates scope)."""
    p6_taxonomy = pd.read_csv(
        settings.PROCESSED_DATA_DIR / "primavera" / "p6_task_taxonomy.csv",
        low_memory=False
    )
    p6_csi = p6_taxonomy[p6_taxonomy['csi_section'].notna()]['csi_section'].unique()
    return set(p6_csi)


def load_dim_company():
    """Load company dimension to get Yates company_id."""
    dim_company = pd.read_csv(
        settings.PROCESSED_DATA_DIR / "integrated_analysis" / "dimensions" / "dim_company.csv"
    )
    return dim_company


def infer_contractor(
    df: pd.DataFrame,
    p6_csi_sections: set,
    samsung_id: int,
    yates_id: int,
    dim_company: pd.DataFrame
) -> pd.DataFrame:
    """
    Add inferred contractor columns to quality data.

    Inference Priority:
    1. If subcontractor specified in source document: Use subcontractor (most reliable)
    2. If contractor specified (not Samsung/blank): Use contractor
    3. If Samsung/blank + CSI in P6 scope: Assign to Yates (scope-based inference)
    4. Otherwise: Keep original

    Args:
        df: Quality dataframe with dim_company_id, csi_section, and subcontractor
        p6_csi_sections: Set of CSI sections in P6 (Yates scope)
        samsung_id: company_id for Samsung E&C
        yates_id: company_id for Yates
        dim_company: Company dimension for looking up subcontractor IDs

    Returns:
        DataFrame with new columns added
    """
    df = df.copy()

    # Initialize with original contractor
    df['inferred_contractor_id'] = df['dim_company_id']
    df['contractor_inference_source'] = 'original'

    # Create company name → ID lookup
    company_lookup = dim_company.set_index('canonical_name')['company_id'].to_dict()

    # Priority 1: Use subcontractor when available (most reliable - from source document)
    has_subcontractor = df['subcontractor'].notna()
    for idx in df[has_subcontractor].index:
        subcontractor_name = df.loc[idx, 'subcontractor']
        # Look up company ID
        company_id = company_lookup.get(subcontractor_name)
        if company_id:
            df.loc[idx, 'inferred_contractor_id'] = company_id
            df.loc[idx, 'contractor_inference_source'] = 'subcontractor_field'

    # Priority 2: For records without subcontractor, check if specific contractor (not Samsung)
    # If contractor is already not-Samsung and not-blank, keep it (already handled by 'original')

    # Priority 3: CSI-based inference for Samsung/blank records without subcontractor
    needs_inference = (
        ~has_subcontractor &  # No subcontractor field
        (
            (df['dim_company_id'] == samsung_id) |  # Samsung E&C
            df['dim_company_id'].isna()  # Blank contractor
        )
    )

    has_csi_mask = df['csi_section'].notna()
    in_p6_scope_mask = df['csi_section'].isin(p6_csi_sections)

    # Re-assign Samsung/blank → Yates when CSI is in P6 scope
    csi_reassign_mask = needs_inference & has_csi_mask & in_p6_scope_mask

    df.loc[csi_reassign_mask, 'inferred_contractor_id'] = yates_id
    df.loc[csi_reassign_mask, 'contractor_inference_source'] = 'csi_scope'

    # Samsung/blank without CSI
    no_csi_mask = needs_inference & ~has_csi_mask
    df.loc[no_csi_mask, 'contractor_inference_source'] = 'no_csi'

    # Samsung/blank with CSI NOT in P6 (owner self-perform work)
    owner_work_mask = needs_inference & has_csi_mask & ~in_p6_scope_mask
    df.loc[owner_work_mask, 'contractor_inference_source'] = 'owner_work'

    return df


def process_quality_source(
    source_name: str,
    file_path: Path,
    p6_csi_sections: set,
    dim_company: pd.DataFrame,
    dry_run: bool = False
):
    """Process a single quality data source."""
    print(f"\n{'='*80}")
    print(f"Processing {source_name}")
    print(f"{'='*80}")

    if not file_path.exists():
        print(f"⚠️  File not found: {file_path}")
        return

    # Load data
    print(f"Loading {file_path.name}...")
    df = pd.read_csv(file_path, low_memory=False)
    print(f"Loaded {len(df):,} records")

    # Get Samsung and Yates IDs
    samsung_row = dim_company[dim_company['canonical_name'] == 'Samsung E&C']
    yates_row = dim_company[dim_company['canonical_name'] == 'Yates']

    if len(samsung_row) == 0 or len(yates_row) == 0:
        print("⚠️  Could not find Samsung or Yates in dim_company")
        return

    samsung_id = samsung_row['company_id'].iloc[0]
    yates_id = yates_row['company_id'].iloc[0]

    print(f"Samsung E&C company_id: {samsung_id}")
    print(f"Yates company_id: {yates_id}")

    # Apply inference
    print("\nApplying contractor inference...")
    df_inferred = infer_contractor(df, p6_csi_sections, samsung_id, yates_id, dim_company)

    # Report changes
    original_samsung = (df['dim_company_id'] == samsung_id).sum()
    inferred_samsung = (df_inferred['inferred_contractor_id'] == samsung_id).sum()
    inferred_yates = (df_inferred['inferred_contractor_id'] == yates_id).sum()
    original_yates = (df['dim_company_id'] == yates_id).sum()

    reassigned = original_samsung - inferred_samsung

    print(f"\n{'='*80}")
    print("INFERENCE RESULTS")
    print(f"{'='*80}")
    print(f"Original Samsung E&C:     {original_samsung:6,} ({original_samsung/len(df)*100:5.1f}%)")
    print(f"Inferred Samsung E&C:     {inferred_samsung:6,} ({inferred_samsung/len(df)*100:5.1f}%)")
    print(f"  → Re-assigned to Yates: {reassigned:6,} ({reassigned/len(df)*100:5.1f}%)")
    print(f"\nOriginal Yates:           {original_yates:6,} ({original_yates/len(df)*100:5.1f}%)")
    print(f"Inferred Yates:           {inferred_yates:6,} ({inferred_yates/len(df)*100:5.1f}%)")
    print(f"  → Net gain:             {inferred_yates - original_yates:6,}")

    # Breakdown by inference source
    print(f"\nInference source breakdown:")
    for source, count in df_inferred['contractor_inference_source'].value_counts().items():
        pct = count / len(df) * 100
        print(f"  {source:20s} {count:6,} ({pct:5.1f}%)")

    # Save
    if not dry_run:
        print(f"\nWriting to {file_path}...")
        df_inferred.to_csv(file_path, index=False)
        print("✓ Complete")
    else:
        print("\n[DRY RUN] No changes written")


def main():
    """Run contractor inference on RABA and PSI."""
    parser = argparse.ArgumentParser(description='Add contractor inference to quality data')
    parser.add_argument('--dry-run', action='store_true', help='Preview without writing')
    args = parser.parse_args()

    print("="*80)
    print("CONTRACTOR INFERENCE - Quality Data")
    print("="*80)
    print("\nPurpose: Re-assign Samsung E&C inspections to Yates when CSI")
    print("         section indicates Yates scope.")

    # Load reference data
    print("\nLoading reference data...")
    p6_csi_sections = load_p6_csi_sections()
    dim_company = load_dim_company()
    print(f"  P6 CSI sections: {len(p6_csi_sections)}")
    print(f"  Companies: {len(dim_company)}")

    # Process each source
    sources = [
        ('RABA', settings.PROCESSED_DATA_DIR / "raba" / "raba_consolidated.csv"),
        ('PSI', settings.PROCESSED_DATA_DIR / "psi" / "psi_consolidated.csv"),
    ]

    for name, path in sources:
        process_quality_source(name, path, p6_csi_sections, dim_company, dry_run=args.dry_run)

    print("\n" + "="*80)
    print("COMPLETE")
    print("="*80)

    if not args.dry_run:
        print("\nNew columns added to quality data:")
        print("  - inferred_contractor_id: Actual contractor based on scope")
        print("  - contractor_inference_source: How contractor was determined")
        print("\nUse inferred_contractor_id for Yates performance analysis.")
    else:
        print("\n[DRY RUN] Use without --dry-run to apply changes")


if __name__ == "__main__":
    main()
