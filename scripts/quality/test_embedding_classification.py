"""
Test embedding-based classification for RABA/PSI quality data.

Instead of using LLM to parse inspection results, use semantic similarity
between label embeddings and document chunk embeddings.

Usage:
    python -m scripts.quality.test_embedding_classification
"""

import numpy as np
from pathlib import Path
from typing import List, Dict, Tuple
import pandas as pd

from scripts.narratives.embeddings.client import embed_for_query
from scripts.narratives.embeddings import get_store


def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """Compute cosine similarity between two vectors."""
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))


def classify_by_embedding(
    text: str,
    labels: Dict[str, str],
    verbose: bool = False
) -> Tuple[str, float, Dict[str, float]]:
    """
    Classify text using embedding similarity to label descriptions.

    Args:
        text: Text to classify
        labels: Dict of {label_name: label_description}
        verbose: Print similarity scores

    Returns:
        (predicted_label, confidence, all_scores)

    Example:
        labels = {
            'PASS': 'Inspection passed, no defects found, approved',
            'FAIL': 'Inspection failed, defects found, rejected, does not meet requirements',
            'CANCELLED': 'Inspection cancelled, not completed, void'
        }

        classify_by_embedding("No defects observed. Work approved.", labels)
        # Returns: ('PASS', 0.82, {'PASS': 0.82, 'FAIL': 0.45, 'CANCELLED': 0.31})
    """
    # Embed the text
    text_embedding = embed_for_query(text)

    # Embed each label description
    label_embeddings = {}
    for label, description in labels.items():
        label_embeddings[label] = embed_for_query(description)

    # Compute similarity scores
    scores = {}
    for label, label_emb in label_embeddings.items():
        scores[label] = cosine_similarity(text_embedding, label_emb)

    # Find best match
    best_label = max(scores, key=scores.get)
    confidence = scores[best_label]

    if verbose:
        print(f"\nText: {text[:100]}...")
        print(f"Scores:")
        for label, score in sorted(scores.items(), key=lambda x: -x[1]):
            print(f"  {label}: {score:.3f}")
        print(f"Predicted: {best_label} (confidence: {confidence:.3f})")

    return best_label, confidence, scores


def test_inspection_status():
    """Test classification of inspection status (PASS/FAIL/CANCELLED)."""

    print("=" * 70)
    print("Test 1: Inspection Status Classification")
    print("=" * 70)

    # Define labels with rich descriptions
    labels = {
        'PASS': 'Inspection passed successfully. No defects found. Work approved and accepted. Meets all requirements and specifications.',
        'FAIL': 'Inspection failed. Defects found and documented. Work rejected and requires correction. Does not meet requirements.',
        'CANCELLED': 'Inspection cancelled or voided. Not completed or superseded. Inspection withdrawn or invalid.'
    }

    # Test cases
    test_cases = [
        "No defects observed. Work approved and accepted.",
        "Multiple defects found. Rework required before acceptance.",
        "Inspection voided due to incorrect scope.",
        "All work meets specification requirements. Approved.",
        "Concrete surface has visible cracks. Failed inspection.",
        "Inspection superseded by revision. This inspection is void.",
        "Work completed satisfactorily with no issues noted.",
        "Does not conform to drawings. Reject and repair."
    ]

    correct = 0
    expected = ['PASS', 'FAIL', 'CANCELLED', 'PASS', 'FAIL', 'CANCELLED', 'PASS', 'FAIL']

    for i, text in enumerate(test_cases):
        predicted, confidence, scores = classify_by_embedding(text, labels, verbose=True)

        if predicted == expected[i]:
            correct += 1
            print(f"✓ Correct!")
        else:
            print(f"✗ Wrong! Expected: {expected[i]}")
        print()

    accuracy = correct / len(test_cases)
    print(f"\nAccuracy: {correct}/{len(test_cases)} ({accuracy*100:.1f}%)")
    print()


def test_company_classification():
    """Test classification of company names."""

    print("=" * 70)
    print("Test 2: Company Classification")
    print("=" * 70)

    # Define company labels (use actual company names + descriptions)
    labels = {
        'Yates': 'Yates Construction Company, general contractor, main builder',
        'SECAI': 'SECAI Enterprises, subcontractor, construction firm',
        'Berg': 'Berg Steel and Supply, steel fabrication and erection',
        'McCarthy': 'McCarthy Building Companies, mechanical contractor',
        'Austin': 'Austin Commercial, construction services'
    }

    # Test cases
    test_cases = [
        "Yates crew completed concrete placement on Level 3.",
        "SECAI is responsible for coordinating the HVAC installation.",
        "Berg delivered steel beams for the structural frame.",
        "McCarthy crews are installing ductwork on the roof.",
        "Austin Bridge completed deck installation."
    ]

    expected = ['Yates', 'SECAI', 'Berg', 'McCarthy', 'Austin']

    correct = 0
    for i, text in enumerate(test_cases):
        predicted, confidence, scores = classify_by_embedding(text, labels, verbose=True)

        if predicted == expected[i]:
            correct += 1
            print(f"✓ Correct!")
        else:
            print(f"✗ Wrong! Expected: {expected[i]}")
        print()

    accuracy = correct / len(test_cases)
    print(f"\nAccuracy: {correct}/{len(test_cases)} ({accuracy*100:.1f}%)")
    print()


def test_on_real_raba_chunks():
    """Test on actual RABA chunks from embeddings."""

    print("=" * 70)
    print("Test 3: Real RABA Chunks")
    print("=" * 70)

    # Get some RABA chunks
    store = get_store()

    # Search for inspection-related chunks
    from scripts.narratives.embeddings import search_chunks
    results = search_chunks(
        query="inspection result",
        source_type="raba",
        limit=5
    )

    if not results:
        print("No RABA chunks found. Run embeddings build first.")
        return

    # Define labels
    labels = {
        'PASS': 'Inspection passed successfully. No defects found. Work approved and accepted. Meets all requirements.',
        'FAIL': 'Inspection failed. Defects found and documented. Work rejected and requires correction.',
        'CANCELLED': 'Inspection cancelled or voided. Not completed or superseded.'
    }

    print(f"Found {len(results)} RABA chunks. Classifying...\n")

    for i, result in enumerate(results, 1):
        print(f"[{i}] {result.source_file}")
        print(f"Text: {result.text[:200]}...")
        print()

        predicted, confidence, scores = classify_by_embedding(result.text, labels)

        print(f"Classification:")
        for label, score in sorted(scores.items(), key=lambda x: -x[1]):
            marker = "→" if label == predicted else " "
            print(f"  {marker} {label}: {score:.3f}")
        print()
        print("-" * 70)
        print()


def test_csi_classification():
    """Test classification of CSI sections."""

    print("=" * 70)
    print("Test 4: CSI Section Classification")
    print("=" * 70)

    # Define CSI sections with rich descriptions
    labels = {
        '03': 'Concrete work including formwork, rebar placement, concrete pouring, slabs, foundations, structural concrete',
        '05': 'Structural steel fabrication and erection, metal deck, joists, steel framing',
        '07': 'Thermal and moisture protection including waterproofing, insulation, roofing, sealants',
        '08': 'Openings including doors, frames, windows, glass, glazing, hardware',
        '09': 'Finishes including drywall, gypsum board, ceiling tiles, painting, flooring, acoustical treatment',
        '23': 'HVAC systems including ductwork, air handling units, mechanical equipment, heating and cooling',
        '26': 'Electrical systems including conduit, cable tray, panels, switchgear, wiring, lighting fixtures'
    }

    # Test cases
    test_cases = [
        "Concrete placement completed on Level 2 slab with no defects.",
        "Structural steel beam installation checked and approved.",
        "Roof waterproofing membrane installed per specifications.",
        "Door frame alignment verified and hardware installed correctly.",
        "Drywall installation and taping completed on east wall.",
        "HVAC ductwork routing verified against mechanical drawings.",
        "Electrical conduit installation and panel wiring completed."
    ]

    expected = ['03', '05', '07', '08', '09', '23', '26']

    correct = 0
    for i, text in enumerate(test_cases):
        predicted, confidence, scores = classify_by_embedding(text, labels, verbose=True)

        if predicted == expected[i]:
            correct += 1
            print(f"✓ Correct!")
        else:
            print(f"✗ Wrong! Expected: {expected[i]}")
        print()

    accuracy = correct / len(test_cases)
    print(f"\nAccuracy: {correct}/{len(test_cases)} ({accuracy*100:.1f}%)")
    print()


def main():
    """Run all tests."""

    print("\n")
    print("=" * 70)
    print("EMBEDDING-BASED CLASSIFICATION TESTS")
    print("=" * 70)
    print()
    print("Testing whether embeddings can replace LLM parsing for:")
    print("  1. Inspection status (PASS/FAIL/CANCELLED)")
    print("  2. Company identification")
    print("  3. Real RABA chunks")
    print("  4. CSI section classification")
    print()
    print("Key insight: Better label descriptions = better accuracy!")
    print("=" * 70)
    print()

    # Run tests
    test_inspection_status()
    test_company_classification()
    test_csi_classification()
    test_on_real_raba_chunks()

    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print()
    print("Embedding-based classification is:")
    print("  ✓ Fast (no LLM API calls)")
    print("  ✓ Cheap (just vector math)")
    print("  ✓ Consistent (no prompt engineering)")
    print()
    print("Best practices:")
    print("  • Use rich label descriptions (not single words)")
    print("  • Include synonyms and context in labels")
    print("  • Test on real data to measure accuracy")
    print("  • Set confidence thresholds for manual review")
    print()
    print("Next steps:")
    print("  1. Test on full RABA/PSI dataset")
    print("  2. Compare accuracy vs LLM parsing")
    print("  3. Measure cost/time savings")
    print("  4. Implement as processing pipeline")
    print()


if __name__ == "__main__":
    main()
