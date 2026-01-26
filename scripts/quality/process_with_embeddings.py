"""
Process RABA/PSI quality data using embedding-based classification.

This script creates a comparison dataset that uses ONLY embeddings to extract:
- General Contractor
- SubContractor
- CSI Section
- Inspection Outcome (PASS/FAIL/CANCELLED)
- Failure Reason (if applicable)

Compares against existing LLM-parsed data to validate the embedding approach.

Usage:
    python -m scripts.quality.process_with_embeddings raba
    python -m scripts.quality.process_with_embeddings psi
    python -m scripts.quality.process_with_embeddings both --limit 100
"""

import argparse
import pandas as pd
from pathlib import Path
from typing import Dict, List
from tqdm import tqdm

from src.config.settings import settings
from scripts.quality.embedding_classifier import QualityClassifier, load_company_labels_from_dim
from scripts.narratives.embeddings import get_store


def get_document_text(source: str, source_file: str) -> str:
    """
    Retrieve full document text from embeddings store.

    Args:
        source: 'raba' or 'psi'
        source_file: Filename (e.g., 'A22-016104.pdf')

    Returns:
        Full document text (concatenated chunks)
    """
    store = get_store()

    # Get all chunks for this file
    # Note: get_chunks_by_file() filters by source_file only
    # We rely on the source_file being unique across sources
    chunks = store.get_chunks_by_file(source_file)

    if not chunks:
        return ""

    # Concatenate chunk text in order
    return "\n\n".join(chunk.text for chunk in chunks)


def extract_with_embeddings(
    text: str,
    classifier: QualityClassifier,
    company_labels: Dict[str, str]
) -> Dict:
    """
    Extract all fields using embedding classification.

    Args:
        text: Full document text
        classifier: QualityClassifier instance
        company_labels: Company name -> description mapping

    Returns:
        Dict with extracted fields and confidence scores
    """
    # Classify inspection outcome
    outcome_result = classifier.classify_status(text)

    # Classify CSI section
    csi_result = classifier.classify_csi(text)

    # Classify general contractor (top company mention)
    contractor_result = classifier.classify_company(text, company_labels)

    # Extract failure reason if failed (simple text extraction)
    failure_reason = None
    failure_reason_confidence = None
    if outcome_result.label == 'FAIL':
        # Look for failure keywords in text
        failure_keywords = [
            'defect', 'deficiency', 'non-conformance', 'reject',
            'does not meet', 'failed to', 'incorrect', 'improper'
        ]

        # Find first sentence with failure keyword
        sentences = text.split('.')
        for sentence in sentences:
            sentence_lower = sentence.lower()
            if any(kw in sentence_lower for kw in failure_keywords):
                failure_reason = sentence.strip()
                # Confidence based on keyword presence
                failure_reason_confidence = 0.70
                break

    # Try to identify subcontractor (distinct from general contractor)
    # Look for secondary company mentions
    subcontractor = None
    subcontractor_confidence = None

    # Get top 3 company matches
    all_scores = contractor_result.all_scores
    sorted_companies = sorted(all_scores.items(), key=lambda x: -x[1])

    if len(sorted_companies) > 1:
        # If second company has reasonable score, consider it subcontractor
        second_company, second_score = sorted_companies[1]
        if second_score >= 0.50 and second_score < contractor_result.confidence - 0.10:
            subcontractor = second_company
            subcontractor_confidence = second_score

    return {
        'emb_outcome': outcome_result.label,
        'emb_outcome_confidence': outcome_result.confidence,
        'emb_csi_section': csi_result.label,
        'emb_csi_confidence': csi_result.confidence,
        'emb_contractor': contractor_result.label,
        'emb_contractor_confidence': contractor_result.confidence,
        'emb_subcontractor': subcontractor,
        'emb_subcontractor_confidence': subcontractor_confidence,
        'emb_failure_reason': failure_reason,
        'emb_failure_reason_confidence': failure_reason_confidence,
    }


def process_source(
    source: str,
    limit: int = None,
    output_path: Path = None,
    verbose: bool = False
) -> pd.DataFrame:
    """
    Process RABA or PSI data with embedding classification.

    Args:
        source: 'raba' or 'psi'
        limit: Max records to process (for testing)
        output_path: Where to save comparison CSV
        verbose: Print progress details

    Returns:
        DataFrame with both LLM and embedding results
    """
    # Load existing consolidated data (LLM-parsed)
    if source == 'raba':
        csv_path = settings.RABA_PROCESSED_DIR / 'raba_consolidated.csv'
    else:
        csv_path = settings.PSI_PROCESSED_DIR / 'psi_consolidated.csv'

    print(f"\nLoading {source.upper()} data from {csv_path}")
    df = pd.read_csv(csv_path)

    if limit:
        df = df.head(limit)

    print(f"Processing {len(df)} records...")

    # Initialize classifier
    classifier = QualityClassifier(confidence_threshold=0.60)
    company_labels = load_company_labels_from_dim()

    if not company_labels:
        print("WARNING: No company labels loaded. Company classification will fail.")

    # Process each record
    results = []

    for idx, row in tqdm(df.iterrows(), total=len(df), desc=f"Processing {source.upper()}"):
        source_file = row['source_file']

        # Strip .clean.json extension if present (embeddings use .pdf)
        if source_file.endswith('.clean.json'):
            source_file_base = source_file.replace('.clean.json', '.pdf')
        else:
            source_file_base = source_file

        # Get document text from embeddings
        text = get_document_text(source, source_file_base)

        if not text:
            if verbose:
                print(f"  WARNING: No text found for {source_file}")
            # Skip records without text
            continue

        # Extract fields with embeddings
        emb_fields = extract_with_embeddings(text, classifier, company_labels)

        # Combine with original row
        result = row.to_dict()
        result.update(emb_fields)
        results.append(result)

        if verbose and idx < 5:
            print(f"\n{source_file}:")
            print(f"  LLM outcome: {row['outcome']}")
            print(f"  EMB outcome: {emb_fields['emb_outcome']} (conf: {emb_fields['emb_outcome_confidence']:.2f})")
            print(f"  LLM contractor: {row.get('contractor', 'N/A')}")
            print(f"  EMB contractor: {emb_fields['emb_contractor']} (conf: {emb_fields['emb_contractor_confidence']:.2f})")

    # Check if we got any results
    if not results:
        print(f"\nERROR: No records processed successfully. Check embeddings index.")
        return None

    # Create output dataframe
    output_df = pd.DataFrame(results)

    # Add comparison columns
    output_df['outcome_match'] = output_df['outcome'] == output_df['emb_outcome']
    output_df['contractor_match'] = output_df['contractor'] == output_df['emb_contractor']
    output_df['csi_match'] = output_df['csi_section'] == output_df['emb_csi_section']

    # Save to CSV
    if output_path:
        output_df.to_csv(output_path, index=False)
        print(f"\nSaved comparison data to {output_path}")

    # Print summary statistics
    print("\n" + "=" * 70)
    print(f"{source.upper()} COMPARISON SUMMARY")
    print("=" * 70)

    total = len(output_df)

    print(f"\nTotal records processed: {total}")

    print(f"\nOutcome Match Rate: {output_df['outcome_match'].sum() / total * 100:.1f}%")
    print(f"  LLM distribution: {output_df['outcome'].value_counts().to_dict()}")
    print(f"  EMB distribution: {output_df['emb_outcome'].value_counts().to_dict()}")

    print(f"\nContractor Match Rate: {output_df['contractor_match'].sum() / total * 100:.1f}%")
    top_llm = output_df['contractor'].value_counts().head(5)
    top_emb = output_df['emb_contractor'].value_counts().head(5)
    print(f"  LLM top 5: {top_llm.to_dict()}")
    print(f"  EMB top 5: {top_emb.to_dict()}")

    print(f"\nCSI Section Match Rate: {output_df['csi_match'].sum() / total * 100:.1f}%")

    # Confidence statistics
    print(f"\nEmbedding Confidence Scores:")
    print(f"  Outcome:    {output_df['emb_outcome_confidence'].mean():.2f} avg, {output_df['emb_outcome_confidence'].min():.2f} min")
    print(f"  Contractor: {output_df['emb_contractor_confidence'].mean():.2f} avg, {output_df['emb_contractor_confidence'].min():.2f} min")
    print(f"  CSI:        {output_df['emb_csi_confidence'].mean():.2f} avg, {output_df['emb_csi_confidence'].min():.2f} min")

    # Low confidence warnings
    low_outcome = output_df[output_df['emb_outcome_confidence'] < 0.60]
    low_contractor = output_df[output_df['emb_contractor_confidence'] < 0.60]
    low_csi = output_df[output_df['emb_csi_confidence'] < 0.60]

    print(f"\nLow Confidence Flags (< 0.60):")
    print(f"  Outcome:    {len(low_outcome)} records ({len(low_outcome)/total*100:.1f}%)")
    print(f"  Contractor: {len(low_contractor)} records ({len(low_contractor)/total*100:.1f}%)")
    print(f"  CSI:        {len(low_csi)} records ({len(low_csi)/total*100:.1f}%)")

    return output_df


def main():
    parser = argparse.ArgumentParser(
        description='Process quality data with embedding classification'
    )
    parser.add_argument(
        'source',
        choices=['raba', 'psi', 'both'],
        help='Data source to process'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of records (for testing)'
    )
    parser.add_argument(
        '--output',
        type=Path,
        help='Output CSV path (default: auto-generated)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Print detailed progress'
    )

    args = parser.parse_args()

    sources_to_process = ['raba', 'psi'] if args.source == 'both' else [args.source]

    for source in sources_to_process:
        # Generate output path
        if args.output:
            output_path = args.output
        else:
            suffix = f"_limit{args.limit}" if args.limit else ""
            output_path = settings.DATA_DIR / 'processed' / source / f'{source}_embedding_comparison{suffix}.csv'

        # Process
        process_source(
            source=source,
            limit=args.limit,
            output_path=output_path,
            verbose=args.verbose
        )


if __name__ == '__main__':
    main()
