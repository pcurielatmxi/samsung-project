"""
Process RABA/PSI quality data using pre-computed embeddings (v2).

This version uses existing chunk embeddings from ChromaDB instead of re-embedding,
making it MUCH faster (no API calls during classification).

Key optimizations:
1. Label embeddings cached once at initialization
2. Document embeddings retrieved from ChromaDB (already computed)
3. Classify each chunk, then aggregate results
4. Zero API calls during processing = near-instant classification

Usage:
    python -m scripts.quality.process_with_embeddings_v2 raba --limit 100
    python -m scripts.quality.process_with_embeddings_v2 psi
    python -m scripts.quality.process_with_embeddings_v2 both
"""

import argparse
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Dict, List, Tuple
from collections import Counter
from tqdm import tqdm

from src.config.settings import settings
from scripts.quality.embedding_classifier import QualityClassifier, load_company_labels_from_dim, cosine_similarity
from scripts.narratives.embeddings import get_store


class CachedClassifier:
    """
    Classifier that uses pre-computed embeddings.

    No API calls during classification - uses existing embeddings from ChromaDB.
    """

    def __init__(self, classifier: QualityClassifier, company_labels: Dict[str, str]):
        """
        Initialize with pre-computed label embeddings.

        Args:
            classifier: QualityClassifier with cached label embeddings
            company_labels: Company name -> description mapping
        """
        self.classifier = classifier

        # Pre-compute all label embeddings
        print("Caching label embeddings...")
        self.status_embeddings = classifier._get_status_embeddings()
        self.csi_embeddings = classifier._get_csi_embeddings()
        self.company_embeddings = classifier._get_company_embeddings(company_labels)
        print(f"  Status: {len(self.status_embeddings)} labels")
        print(f"  CSI: {len(self.csi_embeddings)} sections")
        print(f"  Company: {len(self.company_embeddings)} companies")

    def classify_chunk(
        self,
        chunk_embedding: np.ndarray
    ) -> Dict:
        """
        Classify a single chunk using its pre-computed embedding.

        Args:
            chunk_embedding: Pre-computed embedding vector

        Returns:
            Dict with classification results for all tasks
        """
        # Classify status
        status_scores = {
            label: cosine_similarity(chunk_embedding, label_emb)
            for label, label_emb in self.status_embeddings.items()
        }
        status_label = max(status_scores, key=status_scores.get)
        status_confidence = status_scores[status_label]

        # Classify CSI
        csi_scores = {
            label: cosine_similarity(chunk_embedding, label_emb)
            for label, label_emb in self.csi_embeddings.items()
        }
        csi_label = max(csi_scores, key=csi_scores.get)
        csi_confidence = csi_scores[csi_label]

        # Classify company
        company_scores = {
            label: cosine_similarity(chunk_embedding, label_emb)
            for label, label_emb in self.company_embeddings.items()
        }
        company_label = max(company_scores, key=company_scores.get)
        company_confidence = company_scores[company_label]

        return {
            'status': (status_label, status_confidence, status_scores),
            'csi': (csi_label, csi_confidence, csi_scores),
            'company': (company_label, company_confidence, company_scores)
        }

    def classify_document(
        self,
        chunk_embeddings: List[np.ndarray],
        chunk_texts: List[str]
    ) -> Dict:
        """
        Classify document by aggregating chunk classifications.

        Args:
            chunk_embeddings: List of pre-computed chunk embeddings
            chunk_texts: List of chunk texts (for failure reason extraction)

        Returns:
            Dict with aggregated classification results
        """
        if not chunk_embeddings:
            return None

        # Classify each chunk
        chunk_results = [self.classify_chunk(emb) for emb in chunk_embeddings]

        # Aggregate status (majority vote, highest confidence wins ties)
        status_votes = [(r['status'][0], r['status'][1]) for r in chunk_results]
        status_counter = Counter([vote[0] for vote in status_votes])
        status_label = status_counter.most_common(1)[0][0]
        # Confidence = max confidence among winning votes
        status_confidence = max([conf for label, conf in status_votes if label == status_label])

        # Aggregate CSI (majority vote)
        csi_votes = [(r['csi'][0], r['csi'][1]) for r in chunk_results]
        csi_counter = Counter([vote[0] for vote in csi_votes])
        csi_label = csi_counter.most_common(1)[0][0]
        csi_confidence = max([conf for label, conf in csi_votes if label == csi_label])

        # Aggregate company (highest confidence across all chunks)
        all_company_scores = {}
        for result in chunk_results:
            for company, score in result['company'][2].items():
                if company not in all_company_scores:
                    all_company_scores[company] = []
                all_company_scores[company].append(score)

        # Take max score per company, then find top company
        company_max_scores = {c: max(scores) for c, scores in all_company_scores.items()}
        company_label = max(company_max_scores, key=company_max_scores.get)
        company_confidence = company_max_scores[company_label]

        # Get second company for subcontractor
        sorted_companies = sorted(company_max_scores.items(), key=lambda x: -x[1])
        subcontractor = None
        subcontractor_confidence = None
        if len(sorted_companies) > 1:
            second_company, second_score = sorted_companies[1]
            if second_score >= 0.50 and second_score < company_confidence - 0.10:
                subcontractor = second_company
                subcontractor_confidence = second_score

        # Extract failure reason if failed
        failure_reason = None
        failure_confidence = None
        if status_label == 'FAIL':
            failure_keywords = [
                'defect', 'deficiency', 'non-conformance', 'reject',
                'does not meet', 'failed to', 'incorrect', 'improper'
            ]

            # Search chunks for failure keywords
            for chunk_text in chunk_texts:
                sentences = chunk_text.split('.')
                for sentence in sentences:
                    if any(kw in sentence.lower() for kw in failure_keywords):
                        failure_reason = sentence.strip()
                        failure_confidence = 0.70
                        break
                if failure_reason:
                    break

        return {
            'emb_outcome': status_label,
            'emb_outcome_confidence': status_confidence,
            'emb_csi_section': csi_label,
            'emb_csi_confidence': csi_confidence,
            'emb_contractor': company_label,
            'emb_contractor_confidence': company_confidence,
            'emb_subcontractor': subcontractor,
            'emb_subcontractor_confidence': subcontractor_confidence,
            'emb_failure_reason': failure_reason,
            'emb_failure_reason_confidence': failure_confidence,
        }


def process_source(
    source: str,
    limit: int = None,
    output_path: Path = None,
    verbose: bool = False
) -> pd.DataFrame:
    """
    Process RABA or PSI data using cached embeddings.

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

    # Initialize classifier with cached embeddings
    base_classifier = QualityClassifier(confidence_threshold=0.60)
    company_labels = load_company_labels_from_dim()

    if not company_labels:
        print("WARNING: No company labels loaded. Company classification will fail.")
        return None

    cached_classifier = CachedClassifier(base_classifier, company_labels)

    # Get embeddings store
    store = get_store()

    # Process each record
    results = []
    skipped = 0

    for idx, row in tqdm(df.iterrows(), total=len(df), desc=f"Processing {source.upper()}"):
        source_file = row['source_file']

        # Strip .clean.json extension if present
        if source_file.endswith('.clean.json'):
            source_file_base = source_file.replace('.clean.json', '.pdf')
        else:
            source_file_base = source_file

        # Get chunks from embeddings store (with embeddings!)
        chunks = store.get_chunks_by_file(source_file_base, include_embeddings=True)

        if not chunks:
            if verbose:
                print(f"  WARNING: No chunks found for {source_file_base}")
            skipped += 1
            continue

        # Extract embeddings and text from chunks
        chunk_embeddings = []
        chunk_texts = []
        for chunk in chunks:
            if chunk.embedding is not None and len(chunk.embedding) > 0:
                chunk_embeddings.append(np.array(chunk.embedding))
                chunk_texts.append(chunk.text)

        if not chunk_embeddings:
            if verbose:
                print(f"  WARNING: No embeddings found for {source_file_base}")
            skipped += 1
            continue

        # Classify document using cached embeddings
        emb_fields = cached_classifier.classify_document(chunk_embeddings, chunk_texts)

        if emb_fields is None:
            skipped += 1
            continue

        # Combine with original row
        result = row.to_dict()
        result.update(emb_fields)
        results.append(result)

        if verbose and idx < 5:
            print(f"\n{source_file}:")
            print(f"  Chunks: {len(chunks)}")
            print(f"  LLM outcome: {row['outcome']}")
            print(f"  EMB outcome: {emb_fields['emb_outcome']} (conf: {emb_fields['emb_outcome_confidence']:.2f})")
            print(f"  LLM contractor: {row.get('contractor', 'N/A')}")
            print(f"  EMB contractor: {emb_fields['emb_contractor']} (conf: {emb_fields['emb_contractor_confidence']:.2f})")

    if skipped > 0:
        print(f"\nSkipped {skipped} records with no embeddings")

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
    print(f"{source.upper()} COMPARISON SUMMARY (v2 - Cached Embeddings)")
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
        description='Process quality data with cached embeddings (v2 - no API calls)'
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
            output_path = settings.DATA_DIR / 'processed' / source / f'{source}_embedding_comparison_v2{suffix}.csv'

        # Process
        process_source(
            source=source,
            limit=args.limit,
            output_path=output_path,
            verbose=args.verbose
        )


if __name__ == '__main__':
    main()
