"""
Process RABA/PSI quality data with ROLE-BASED classification (v4).

This version solves the "document author problem" by using 2-stage classification:
1. What role? (inspection_company, general_contractor, subcontractor, owner)
2. Which company in that role?

This prevents misclassifying inspection companies (Raba Kistner) as contractors.

Usage:
    python -m scripts.quality.process_with_embeddings_v4 raba --limit 50
    python -m scripts.quality.process_with_embeddings_v4 psi
    python -m scripts.quality.process_with_embeddings_v4 both
"""

import argparse
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Dict, List, Tuple
from collections import Counter
from tqdm import tqdm

from src.config.settings import settings
from scripts.quality.embedding_classifier import cosine_similarity
from scripts.quality.improve_labels import ENHANCED_CSI_LABELS, build_enhanced_company_label, infer_company_role
from scripts.quality.label_embedding_cache import LabelEmbeddingCache
from scripts.narratives.embeddings import get_store


# Role descriptions for first-stage classification
ROLE_LABELS = {
    'general_contractor': '''General contractor, prime contractor, main contractor, GC, construction contractor,
                            main builder, project contractor, performing the work, responsible for construction,
                            contractor completing work, work performed by contractor''',

    'subcontractor': '''Subcontractor, specialty contractor, trade contractor, sub,
                       performing specialty work, trade work, specific scope,
                       working under general contractor''',

    'inspection_company': '''Third-party inspection company, quality control firm, testing company,
                            independent inspector, QA/QC firm, inspection agency, testing agency,
                            quality assurance company, inspection report author, report prepared by,
                            inspector company, testing and inspection, materials testing''',

    'owner': '''Owner, client, owner representative, project owner, Samsung representative,
               client contact, owner contact, project management, construction management for owner'''
}


def load_enhanced_company_labels() -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Load company labels with role assignments.

    Returns:
        Tuple of (company_labels, company_roles)
        - company_labels: {company_name: enhanced_description}
        - company_roles: {company_name: role}
    """
    dim_company_path = settings.PROCESSED_DATA_DIR / 'integrated_analysis' / 'dimensions' / 'dim_company.csv'

    if not dim_company_path.exists():
        print(f"Warning: dim_company.csv not found at {dim_company_path}")
        return {}, {}

    df = pd.read_csv(dim_company_path)

    # Build labels and role mapping
    company_labels = {}
    company_roles = {}

    for _, row in df.iterrows():
        name = row['canonical_name']
        enhanced = build_enhanced_company_label(row)
        role = infer_company_role(name, row.get('primary_trade'))

        company_labels[name] = enhanced
        company_roles[name] = role

    return company_labels, company_roles


class RoleBasedClassifier:
    """
    Classifier with 2-stage role-based company classification.
    """

    def __init__(self, confidence_threshold: float = 0.60):
        """Initialize with enhanced labels and role-based filtering."""
        self.confidence_threshold = confidence_threshold

        # Load enhanced labels
        print("Loading enhanced labels...")
        self.company_labels, self.company_roles = load_enhanced_company_labels()
        self.csi_labels = ENHANCED_CSI_LABELS

        # Status labels
        self.status_labels = {
            'PASS': '''Inspection passed successfully. No defects found. Work approved and accepted.
                       Meets all requirements and specifications. Complies with drawings.
                       Quality standards satisfied. Accepted for next phase.''',
            'FAIL': '''Inspection failed. Defects found and documented. Work rejected and requires correction.
                       Does not meet requirements. Non-conforming work. Rework needed.
                       Quality issues identified. Repair required before acceptance.''',
            'CANCELLED': '''Inspection cancelled or voided. Not completed or superseded.
                           Inspection withdrawn or invalid. Void due to scope change.
                           Superseded by revision. Not applicable.'''
        }

        # Initialize cache
        cache = LabelEmbeddingCache()

        # Load embeddings
        print("Loading label embeddings...")
        self.status_embeddings = cache.get_embeddings('status', self.status_labels)
        print(f"  Status: {len(self.status_embeddings)} labels")

        self.csi_embeddings = cache.get_embeddings('csi', self.csi_labels)
        print(f"  CSI: {len(self.csi_embeddings)} sections")

        self.role_embeddings = cache.get_embeddings('role', ROLE_LABELS)
        print(f"  Roles: {len(self.role_embeddings)} roles")

        self.company_embeddings = cache.get_embeddings('company', self.company_labels)
        print(f"  Company: {len(self.company_embeddings)} companies")

        # Group companies by role for second-stage classification
        self.companies_by_role = {}
        for company, role in self.company_roles.items():
            if role not in self.companies_by_role:
                self.companies_by_role[role] = []
            self.companies_by_role[role].append(company)

        print(f"\nRole distribution:")
        for role, companies in self.companies_by_role.items():
            print(f"  {role}: {len(companies)} companies")

        # Print cache stats
        cache.print_stats()

    def classify_chunk(self, chunk_embedding: np.ndarray) -> Dict:
        """Classify a chunk using 2-stage role-based approach."""

        # Status
        status_scores = {
            label: cosine_similarity(chunk_embedding, label_emb)
            for label, label_emb in self.status_embeddings.items()
        }
        status_label = max(status_scores, key=status_scores.get)
        status_confidence = status_scores[status_label]

        # CSI
        csi_scores = {
            label: cosine_similarity(chunk_embedding, label_emb)
            for label, label_emb in self.csi_embeddings.items()
        }
        csi_label = max(csi_scores, key=csi_scores.get)
        csi_confidence = csi_scores[csi_label]

        # Company: 2-stage classification
        # Stage 1: Classify role
        role_scores = {
            role: cosine_similarity(chunk_embedding, role_emb)
            for role, role_emb in self.role_embeddings.items()
        }
        predicted_role = max(role_scores, key=role_scores.get)
        role_confidence = role_scores[predicted_role]

        # Stage 2: Classify company within that role
        # Filter to companies in the predicted role
        role_companies = self.companies_by_role.get(predicted_role, [])

        if not role_companies:
            # Fallback: use all companies
            role_companies = list(self.company_labels.keys())

        # Compute scores only for companies in this role
        company_scores = {
            company: cosine_similarity(chunk_embedding, self.company_embeddings[company])
            for company in role_companies
        }

        company_label = max(company_scores, key=company_scores.get) if company_scores else None
        company_confidence = company_scores[company_label] if company_label else 0.0

        return {
            'status': (status_label, status_confidence, status_scores),
            'csi': (csi_label, csi_confidence, csi_scores),
            'role': (predicted_role, role_confidence, role_scores),
            'company': (company_label, company_confidence, company_scores)
        }

    def classify_document(
        self,
        chunk_embeddings: List[np.ndarray],
        chunk_texts: List[str]
    ) -> Dict:
        """Classify document by aggregating chunks."""
        if not chunk_embeddings:
            return None

        # Classify each chunk
        chunk_results = [self.classify_chunk(emb) for emb in chunk_embeddings]

        # Aggregate status
        status_votes = [(r['status'][0], r['status'][1]) for r in chunk_results]
        status_counter = Counter([vote[0] for vote in status_votes])
        status_label = status_counter.most_common(1)[0][0]
        status_confidence = max([conf for label, conf in status_votes if label == status_label])

        # Aggregate CSI
        csi_votes = [(r['csi'][0], r['csi'][1]) for r in chunk_results]
        csi_counter = Counter([vote[0] for vote in csi_votes])
        csi_label = csi_counter.most_common(1)[0][0]
        csi_confidence = max([conf for label, conf in csi_votes if label == csi_label])

        # Aggregate role (for contractor)
        # Strategy: Use general_contractor role for primary, other roles for secondary
        role_votes = [(r['role'][0], r['role'][1]) for r in chunk_results]

        # Separate general_contractor votes from others
        gc_votes = [v for v in role_votes if v[0] == 'general_contractor']
        other_votes = [v for v in role_votes if v[0] != 'general_contractor' and v[0] != 'inspection_company']

        # Primary contractor: Prefer general_contractor if any votes, else highest confidence non-inspection
        if gc_votes:
            contractor_role = 'general_contractor'
        elif other_votes:
            contractor_role = max(other_votes, key=lambda x: x[1])[0]
        else:
            contractor_role = 'general_contractor'  # Default fallback

        # Aggregate company within contractor role
        contractor_companies = self.companies_by_role.get(contractor_role, [])

        # Collect all scores for companies in contractor role
        all_contractor_scores = {}
        for result in chunk_results:
            company = result['company'][0]
            confidence = result['company'][1]

            if company and company in contractor_companies:
                if company not in all_contractor_scores:
                    all_contractor_scores[company] = []
                all_contractor_scores[company].append(confidence)

        # Get best contractor
        if all_contractor_scores:
            contractor_max_scores = {c: max(scores) for c, scores in all_contractor_scores.items()}
            contractor = max(contractor_max_scores, key=contractor_max_scores.get)
            contractor_confidence = contractor_max_scores[contractor]
        else:
            # Fallback: Take highest scoring company across all roles (excluding inspection)
            all_company_scores = {}
            for result in chunk_results:
                company = result['company'][0]
                if company and self.company_roles.get(company) != 'inspection_company':
                    if company not in all_company_scores:
                        all_company_scores[company] = []
                    all_company_scores[company].append(result['company'][1])

            if all_company_scores:
                max_scores = {c: max(scores) for c, scores in all_company_scores.items()}
                contractor = max(max_scores, key=max_scores.get)
                contractor_confidence = max_scores[contractor]
            else:
                contractor = None
                contractor_confidence = 0.0

        # Get subcontractor (second highest non-inspection, non-contractor)
        subcontractor = None
        subcontractor_confidence = None

        all_sub_scores = {}
        for result in chunk_results:
            company = result['company'][0]
            if company and company != contractor and self.company_roles.get(company) != 'inspection_company':
                if company not in all_sub_scores:
                    all_sub_scores[company] = []
                all_sub_scores[company].append(result['company'][1])

        if all_sub_scores:
            sub_max_scores = {c: max(scores) for c, scores in all_sub_scores.items()}
            sorted_subs = sorted(sub_max_scores.items(), key=lambda x: -x[1])
            if len(sorted_subs) > 0:
                sub_company, sub_score = sorted_subs[0]
                if sub_score >= 0.45:
                    subcontractor = sub_company
                    subcontractor_confidence = sub_score

        # Extract failure reason
        failure_reason = None
        failure_confidence = None
        if status_label == 'FAIL':
            failure_keywords = [
                'defect', 'deficiency', 'non-conformance', 'reject',
                'does not meet', 'failed to', 'incorrect', 'improper',
                'missing', 'damaged', 'cracked', 'void'
            ]

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
            'emb_contractor': contractor,
            'emb_contractor_confidence': contractor_confidence,
            'emb_contractor_role': contractor_role,
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
    """Process with role-based classification."""

    # Load consolidated data
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
    classifier = RoleBasedClassifier(confidence_threshold=0.60)

    # Get store
    store = get_store()

    # Process
    results = []
    skipped = 0

    for idx, row in tqdm(df.iterrows(), total=len(df), desc=f"Processing {source.upper()}"):
        source_file = row['source_file']

        # Strip .clean.json
        if source_file.endswith('.clean.json'):
            source_file_base = source_file.replace('.clean.json', '.pdf')
        else:
            source_file_base = source_file

        # Get chunks with embeddings
        chunks = store.get_chunks_by_file(source_file_base, include_embeddings=True)

        if not chunks:
            if verbose:
                print(f"  WARNING: No chunks for {source_file_base}")
            skipped += 1
            continue

        # Extract embeddings
        chunk_embeddings = []
        chunk_texts = []
        for chunk in chunks:
            if chunk.embedding is not None and len(chunk.embedding) > 0:
                chunk_embeddings.append(np.array(chunk.embedding))
                chunk_texts.append(chunk.text)

        if not chunk_embeddings:
            skipped += 1
            continue

        # Classify
        emb_fields = classifier.classify_document(chunk_embeddings, chunk_texts)

        if emb_fields is None:
            skipped += 1
            continue

        # Combine
        result = row.to_dict()
        result.update(emb_fields)
        results.append(result)

        if verbose and idx < 10:
            print(f"\n{source_file}:")
            print(f"  Chunks: {len(chunks)}")
            print(f"  LLM contractor: {row.get('contractor', 'N/A')}")
            print(f"  EMB role: {emb_fields.get('emb_contractor_role', 'N/A')}")
            print(f"  EMB contractor: {emb_fields['emb_contractor']} (conf: {emb_fields['emb_contractor_confidence']:.2f})")
            if emb_fields.get('emb_subcontractor'):
                print(f"  EMB subcontractor: {emb_fields['emb_subcontractor']} (conf: {emb_fields['emb_subcontractor_confidence']:.2f})")

    if skipped > 0:
        print(f"\nSkipped {skipped} records with no embeddings")

    if not results:
        print(f"\nERROR: No records processed.")
        return None

    # Create dataframe
    output_df = pd.DataFrame(results)

    # Add comparison columns
    output_df['outcome_match'] = output_df['outcome'] == output_df['emb_outcome']
    output_df['contractor_match'] = output_df['contractor'] == output_df['emb_contractor']
    output_df['csi_match'] = output_df['csi_section'] == output_df['emb_csi_section']

    # Save
    if output_path:
        output_df.to_csv(output_path, index=False)
        print(f"\nSaved comparison data to {output_path}")

    # Print summary
    print("\n" + "=" * 70)
    print(f"{source.upper()} COMPARISON SUMMARY (v4 - Role-Based)")
    print("=" * 70)

    total = len(output_df)

    print(f"\nTotal records processed: {total}")

    print(f"\nOutcome Match Rate: {output_df['outcome_match'].sum() / total * 100:.1f}%")

    print(f"\nContractor Match Rate: {output_df['contractor_match'].sum() / total * 100:.1f}%")
    top_llm = output_df['contractor'].value_counts().head(5)
    top_emb = output_df['emb_contractor'].value_counts().head(5)
    print(f"  LLM top 5: {top_llm.to_dict()}")
    print(f"  EMB top 5: {top_emb.to_dict()}")

    # Role distribution
    if 'emb_contractor_role' in output_df.columns:
        print(f"\nInferred Contractor Roles:")
        role_dist = output_df['emb_contractor_role'].value_counts()
        for role, count in role_dist.items():
            print(f"  {role}: {count} ({count/total*100:.1f}%)")

    print(f"\nCSI Section Match Rate: {output_df['csi_match'].sum() / total * 100:.1f}%")

    # Confidence stats
    print(f"\nEmbedding Confidence Scores:")
    print(f"  Outcome:    {output_df['emb_outcome_confidence'].mean():.2f} avg")
    print(f"  Contractor: {output_df['emb_contractor_confidence'].mean():.2f} avg")
    print(f"  CSI:        {output_df['emb_csi_confidence'].mean():.2f} avg")

    return output_df


def main():
    parser = argparse.ArgumentParser(
        description='Process quality data with role-based classification (v4)'
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
            output_path = settings.DATA_DIR / 'processed' / source / f'{source}_embedding_comparison_v4{suffix}.csv'

        # Process
        process_source(
            source=source,
            limit=args.limit,
            output_path=output_path,
            verbose=args.verbose
        )


if __name__ == '__main__':
    main()
