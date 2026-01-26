"""
Production embedding-based classifier for RABA/PSI quality data.

Replaces expensive LLM parsing with fast, cheap semantic similarity.

Usage:
    from scripts.quality.embedding_classifier import QualityClassifier

    classifier = QualityClassifier()

    # Classify inspection status
    status = classifier.classify_status("No defects found. Work approved.")
    # Returns: {'label': 'PASS', 'confidence': 0.91, 'all_scores': {...}}

    # Classify company
    company = classifier.classify_company("Yates crew completed the work.")
    # Returns: {'label': 'Yates', 'confidence': 0.73, ...}

    # Classify CSI section
    csi = classifier.classify_csi("Concrete placement on Level 2 slab.")
    # Returns: {'label': '03', 'confidence': 0.69, ...}
"""

import numpy as np
from typing import Dict, Tuple
from dataclasses import dataclass

from scripts.narratives.embeddings.client import embed_for_query


@dataclass
class ClassificationResult:
    """Result of classification."""
    label: str
    confidence: float
    all_scores: Dict[str, float]
    threshold_met: bool  # Whether confidence exceeds threshold


def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """Compute cosine similarity between two vectors."""
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))


class QualityClassifier:
    """
    Embedding-based classifier for quality inspection data.

    Uses semantic similarity instead of LLM parsing for:
    - Inspection status (PASS/FAIL/CANCELLED)
    - Company identification
    - CSI section classification
    """

    # Label definitions with rich descriptions for better accuracy
    INSPECTION_STATUS_LABELS = {
        'PASS': (
            'Inspection passed successfully. No defects found. Work approved and accepted. '
            'Meets all requirements and specifications. Complies with drawings. '
            'Quality standards satisfied. Accepted for next phase.'
        ),
        'FAIL': (
            'Inspection failed. Defects found and documented. Work rejected and requires correction. '
            'Does not meet requirements. Non-conforming work. Rework needed. '
            'Quality issues identified. Repair required before acceptance.'
        ),
        'CANCELLED': (
            'Inspection cancelled or voided. Not completed or superseded. '
            'Inspection withdrawn or invalid. Void due to scope change. '
            'Superseded by revision. Not applicable.'
        )
    }

    CSI_SECTION_LABELS = {
        '03': 'Concrete work including formwork, rebar placement, concrete pouring, slabs, foundations, structural concrete, curing',
        '05': 'Structural steel fabrication and erection, metal deck, joists, steel framing, welding, bolting',
        '07': 'Thermal and moisture protection including waterproofing, insulation, roofing, sealants, vapor barriers',
        '08': 'Openings including doors, frames, windows, glass, glazing, hardware, door installation',
        '09': 'Finishes including drywall, gypsum board, ceiling tiles, painting, flooring, acoustical treatment, wall finishes',
        '21': 'Fire protection systems including sprinklers, standpipes, fire suppression, fire safety',
        '22': 'Plumbing systems including pipes, domestic water, sanitary, fixtures, plumbing installation',
        '23': 'HVAC systems including ductwork, air handling units, mechanical equipment, heating and cooling, ventilation',
        '26': 'Electrical systems including conduit, cable tray, panels, switchgear, wiring, lighting fixtures, electrical installation',
        '27': 'Communications and data systems including low voltage, structured cabling, network infrastructure',
        '28': 'Fire alarm and security systems including detection, alarms, access control'
    }

    def __init__(self, confidence_threshold: float = 0.60):
        """
        Initialize classifier.

        Args:
            confidence_threshold: Minimum confidence to consider result reliable.
                Results below threshold should be manually reviewed.
        """
        self.confidence_threshold = confidence_threshold

        # Cache label embeddings (computed once, used many times)
        self._status_embeddings = None
        self._csi_embeddings = None
        self._company_embeddings = None

    def _get_status_embeddings(self) -> Dict[str, np.ndarray]:
        """Get cached inspection status label embeddings."""
        if self._status_embeddings is None:
            self._status_embeddings = {
                label: embed_for_query(description)
                for label, description in self.INSPECTION_STATUS_LABELS.items()
            }
        return self._status_embeddings

    def _get_csi_embeddings(self) -> Dict[str, np.ndarray]:
        """Get cached CSI section label embeddings."""
        if self._csi_embeddings is None:
            self._csi_embeddings = {
                label: embed_for_query(description)
                for label, description in self.CSI_SECTION_LABELS.items()
            }
        return self._csi_embeddings

    def _get_company_embeddings(self, company_labels: Dict[str, str]) -> Dict[str, np.ndarray]:
        """
        Get company label embeddings (computed on-demand since labels may vary).

        Args:
            company_labels: Dict of {company_name: description}
        """
        return {
            label: embed_for_query(description)
            for label, description in company_labels.items()
        }

    def classify(
        self,
        text: str,
        label_embeddings: Dict[str, np.ndarray]
    ) -> ClassificationResult:
        """
        Classify text using embedding similarity.

        Args:
            text: Text to classify
            label_embeddings: Pre-computed label embeddings

        Returns:
            ClassificationResult with label, confidence, and scores
        """
        # Embed the text
        text_embedding = embed_for_query(text)

        # Compute similarity scores
        scores = {}
        for label, label_emb in label_embeddings.items():
            scores[label] = cosine_similarity(text_embedding, label_emb)

        # Find best match
        best_label = max(scores, key=scores.get)
        confidence = scores[best_label]

        return ClassificationResult(
            label=best_label,
            confidence=confidence,
            all_scores=scores,
            threshold_met=confidence >= self.confidence_threshold
        )

    def classify_status(self, text: str) -> ClassificationResult:
        """
        Classify inspection status (PASS/FAIL/CANCELLED).

        Args:
            text: Inspection report text

        Returns:
            ClassificationResult

        Example:
            result = classifier.classify_status("No defects found.")
            if result.threshold_met:
                print(f"Status: {result.label} (confident)")
            else:
                print(f"Status: {result.label} (needs review)")
        """
        embeddings = self._get_status_embeddings()
        return self.classify(text, embeddings)

    def classify_csi(self, text: str) -> ClassificationResult:
        """
        Classify CSI section.

        Args:
            text: Work description text

        Returns:
            ClassificationResult
        """
        embeddings = self._get_csi_embeddings()
        return self.classify(text, embeddings)

    def classify_company(
        self,
        text: str,
        company_labels: Dict[str, str]
    ) -> ClassificationResult:
        """
        Classify company from text.

        Args:
            text: Text mentioning company
            company_labels: Dict of {company_name: description}

        Returns:
            ClassificationResult

        Example:
            companies = {
                'Yates': 'Yates Construction Company, general contractor',
                'SECAI': 'SECAI Enterprises, subcontractor',
                'Berg': 'Berg Steel and Supply, steel fabrication'
            }
            result = classifier.classify_company("Yates crew worked...", companies)
        """
        embeddings = self._get_company_embeddings(company_labels)
        return self.classify(text, embeddings)


def load_company_labels_from_dim() -> Dict[str, str]:
    """
    Load company labels from dim_company.csv.

    Returns:
        Dict of {canonical_name: description}
    """
    import pandas as pd
    from src.config.settings import settings

    dim_company_path = settings.INTEGRATED_PROCESSED_DIR / 'dimensions' / 'dim_company.csv'

    if not dim_company_path.exists():
        print(f"Warning: dim_company.csv not found at {dim_company_path}")
        return {}

    df = pd.read_csv(dim_company_path)

    # Build company labels
    company_labels = {}
    for _, row in df.iterrows():
        name = row['canonical_name']
        # Use name + trade as description for better context
        trade = row.get('primary_trade', '')
        if trade:
            description = f"{name}, {trade}"
        else:
            description = name
        company_labels[name] = description

    return company_labels


# Example usage
if __name__ == "__main__":
    classifier = QualityClassifier(confidence_threshold=0.60)

    # Test status classification
    print("Testing inspection status...")
    test_texts = [
        "No defects observed. Work approved.",
        "Multiple defects found. Rework required.",
        "Inspection voided due to scope change."
    ]

    for text in test_texts:
        result = classifier.classify_status(text)
        confidence_str = "✓ confident" if result.threshold_met else "? review"
        print(f"{text[:50]:50} → {result.label:10} ({result.confidence:.2f}) {confidence_str}")

    print()

    # Test CSI classification
    print("Testing CSI classification...")
    test_texts = [
        "Concrete placement on Level 2 slab",
        "Steel beam erection and welding",
        "HVAC ductwork installation"
    ]

    for text in test_texts:
        result = classifier.classify_csi(text)
        confidence_str = "✓ confident" if result.threshold_met else "? review"
        print(f"{text[:50]:50} → CSI {result.label:5} ({result.confidence:.2f}) {confidence_str}")
