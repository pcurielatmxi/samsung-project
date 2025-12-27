#!/usr/bin/env python3
"""
Transform RABA and PSI JSON outputs to unified quality event format.

Usage:
    python transform_to_unified.py \
        --raba-dir /path/to/raba/json \
        --psi-dir /path/to/psi/json \
        --output /path/to/unified/events.jsonl
"""

import json
import argparse
from pathlib import Path
from typing import Dict, List, Any
import re


def extract_location_components(location_str: str) -> Dict[str, str]:
    """Parse location string into components."""
    components = {
        "location_description": location_str,
        "building": None,
        "level": None,
        "grid_reference": None,
        "location_code": None
    }

    # Extract building
    if "FAB" in location_str.upper():
        components["building"] = "FAB1"

    # Extract level
    level_match = re.search(r'Level\s+(\d+)', location_str, re.IGNORECASE)
    if level_match:
        components["level"] = f"Level {level_match.group(1)}"

    # Extract grid reference (e.g., "C/25-27", "L-N/5")
    grid_match = re.search(r'([A-Z]-?[A-Z]?/\d+-?\d*)', location_str)
    if grid_match:
        components["grid_reference"] = grid_match.group(1)

    return components


def transform_raba_to_unified(raba_data: Dict[str, Any], source_file: str) -> Dict[str, Any]:
    """Transform RABA test report to unified quality event format."""

    metadata = raba_data.get("report_metadata", {})

    # Process all test sets - we'll create one unified event per report
    # with all findings aggregated
    all_findings = []
    total_passed = 0
    total_failed = 0

    contractor = None
    material_supplier = None
    location_str = None
    batch_number = None
    placement_date = None
    weather = None

    for test_set in raba_data.get("test_sets", []):
        # Extract common fields from first set
        if not contractor:
            contractor = test_set.get("contractor")
        if not material_supplier:
            material_supplier = test_set.get("material_supplier")
        if not location_str:
            location_str = test_set.get("location")
        if not batch_number:
            batch_number = test_set.get("batch_number")
        if not placement_date:
            placement_date = test_set.get("test_date")
        if not weather:
            weather = test_set.get("weather_conditions")

        # Process each test result
        for result in test_set.get("test_results", []):
            finding = {
                "finding_id": result.get("sample_id"),
                "finding_type": "Test Result",
                "description": f"{metadata.get('test_type', 'Test')} - {test_set.get('location')}",
                "result_type": "Numeric" if isinstance(result.get("result_value"), (int, float)) else "Categorical",
                "result_value": result.get("result_value"),
                "result_unit": result.get("result_unit"),
                "specification_value": result.get("specification_value"),
                "passed": result.get("passed", True),
                "failure_reason": result.get("failure_reason"),
                "test_parameters": result.get("test_parameters", {}),
                "root_cause": {}
            }

            # Add root cause if present at test set level
            if test_set.get("root_cause_indicators"):
                finding["root_cause"] = {
                    "category": test_set["root_cause_indicators"].get("category"),
                    "contributing_factors": test_set["root_cause_indicators"].get("contributing_factors", [])
                }

            if result.get("passed"):
                total_passed += 1
            else:
                total_failed += 1

            all_findings.append(finding)

    # Build unified event
    location_components = extract_location_components(location_str or "")

    unified_event = {
        "event_metadata": {
            "event_id": f"RABA-{Path(source_file).stem}",
            "event_type": "Laboratory Test",
            "source_system": "RABA",
            "event_date": metadata.get("report_date"),
            "report_date": metadata.get("report_date"),
            "discipline": metadata.get("craft"),
            "activity_type": metadata.get("test_type")
        },
        "location": location_components,
        "parties": {
            "contractor": contractor,
            "inspector_company": "RABA",
            "inspector_name": None,  # Could extract from test sets if needed
            "material_supplier": material_supplier
        },
        "overall_result": {
            "status": "Passed" if total_failed == 0 else "Failed",
            "total_items_checked": total_passed + total_failed,
            "items_passed": total_passed,
            "items_failed": total_failed,
            "corrected_during_event": False  # Lab tests don't get corrected during testing
        },
        "findings": all_findings,
        "material_traceability": {
            "batch_number": batch_number,
            "placement_date": placement_date,
            "material_specifications": {}
        },
        "environmental_context": {
            "weather_conditions": weather
        },
        "follow_up": {
            "requires_reinspection": total_failed > 0
        },
        "source_document": {
            "filename": Path(source_file).name,
            "filepath": source_file
        }
    }

    return unified_event


def transform_psi_to_unified(psi_data: Dict[str, Any], source_file: str) -> Dict[str, Any]:
    """Transform PSI field report to unified quality event format."""

    metadata = psi_data.get("report_metadata", {})
    inspection = psi_data.get("inspection_details", {})
    inspection_type = psi_data.get("inspection_type", {})
    result = psi_data.get("inspection_result", {})

    # Process deficiencies as findings
    findings = []
    for deficiency in psi_data.get("deficiencies", []):
        finding = {
            "finding_id": None,
            "finding_type": "Deficiency",
            "description": deficiency.get("description"),
            "result_type": "Pass/Fail",
            "result_value": "Failed",
            "result_unit": None,
            "specification_value": None,
            "passed": False,
            "failure_reason": deficiency.get("description"),
            "test_parameters": {},
            "deficiency_classification": {
                "deficiency_type": deficiency.get("deficiency_type"),
                "severity": deficiency.get("severity")
            },
            "root_cause": {
                "category": deficiency.get("root_cause_category"),
                "responsible_party": deficiency.get("responsible_party"),
                "contributing_factors": []
            }
        }
        findings.append(finding)

    # If no deficiencies but passed, create a passing finding
    if not findings and psi_data.get("overall_status") == "Passed":
        findings.append({
            "finding_id": None,
            "finding_type": "Deficiency",
            "description": "No deficiencies found",
            "result_type": "Pass/Fail",
            "result_value": "Passed",
            "result_unit": None,
            "passed": True,
            "failure_reason": None
        })

    # Calculate pass/fail counts
    total_items = len(findings) if findings else 1
    items_failed = sum(1 for f in findings if not f.get("passed", True))
    items_passed = total_items - items_failed

    # Check if corrected during inspection
    corrected_during = any(
        d.get("corrected_during_inspection", False)
        for d in psi_data.get("deficiencies", [])
    )

    # Parse location
    location_str = inspection.get("location", "")
    location_components = extract_location_components(location_str)

    # Determine if re-inspection was performed
    second_inspection = result.get("second_inspection", {})
    requires_reinspection = psi_data.get("overall_status") == "Re-Inspection Required"

    unified_event = {
        "event_metadata": {
            "event_id": metadata.get("report_number"),
            "event_type": "Field Inspection",
            "source_system": "PSI",
            "event_date": inspection.get("test_date"),
            "report_date": metadata.get("date_of_issue"),
            "discipline": psi_data.get("work_inspection_request", {}).get("discipline"),
            "activity_type": inspection_type.get("primary_type")
        },
        "location": location_components,
        "parties": {
            "contractor": inspection.get("contractor"),
            "inspector_company": "PSI",
            "inspector_name": result.get("inspected_by_third_party", {}).get("person"),
            "material_supplier": None
        },
        "overall_result": {
            "status": psi_data.get("overall_status"),
            "total_items_checked": total_items,
            "items_passed": items_passed,
            "items_failed": items_failed,
            "corrected_during_event": corrected_during
        },
        "findings": findings,
        "material_traceability": {},
        "environmental_context": {
            "weather_conditions": inspection.get("weather")
        },
        "follow_up": {
            "requires_reinspection": requires_reinspection,
            "reinspection_date": second_inspection.get("date"),
            "reinspection_status": second_inspection.get("result")
        },
        "source_document": {
            "filename": Path(source_file).name,
            "filepath": source_file
        }
    }

    return unified_event


def main():
    parser = argparse.ArgumentParser(description="Transform RABA and PSI data to unified format")
    parser.add_argument("--raba-dir", type=str, help="Directory with RABA JSON files")
    parser.add_argument("--psi-dir", type=str, help="Directory with PSI JSON files")
    parser.add_argument("--output", type=str, required=True, help="Output JSONL file path")
    parser.add_argument("--output-format", choices=["jsonl", "json"], default="jsonl",
                       help="Output format: jsonl (one event per line) or json (array)")

    args = parser.parse_args()

    unified_events = []

    # Process RABA files
    if args.raba_dir:
        raba_path = Path(args.raba_dir)
        print(f"Processing RABA files from {raba_path}...")
        for json_file in raba_path.glob("*.json"):
            try:
                with open(json_file, 'r') as f:
                    raba_data = json.load(f)
                unified_event = transform_raba_to_unified(raba_data, str(json_file))
                unified_events.append(unified_event)
            except Exception as e:
                print(f"Error processing {json_file}: {e}")
        print(f"Processed {len([f for f in raba_path.glob('*.json')])} RABA files")

    # Process PSI files
    if args.psi_dir:
        psi_path = Path(args.psi_dir)
        print(f"Processing PSI files from {psi_path}...")
        psi_count = 0
        for json_file in psi_path.glob("*.json"):
            try:
                with open(json_file, 'r') as f:
                    psi_data = json.load(f)
                unified_event = transform_psi_to_unified(psi_data, str(json_file))
                unified_events.append(unified_event)
                psi_count += 1
            except Exception as e:
                print(f"Error processing {json_file}: {e}")
        print(f"Processed {psi_count} PSI files")

    # Write output
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if args.output_format == "jsonl":
        with open(output_path, 'w') as f:
            for event in unified_events:
                f.write(json.dumps(event) + '\n')
        print(f"Wrote {len(unified_events)} unified events to {output_path} (JSONL format)")
    else:
        with open(output_path, 'w') as f:
            json.dump(unified_events, f, indent=2)
        print(f"Wrote {len(unified_events)} unified events to {output_path} (JSON array format)")

    print(f"\nSummary:")
    print(f"  Total events: {len(unified_events)}")
    print(f"  Lab tests: {sum(1 for e in unified_events if e['event_metadata']['event_type'] == 'Laboratory Test')}")
    print(f"  Field inspections: {sum(1 for e in unified_events if e['event_metadata']['event_type'] == 'Field Inspection')}")


if __name__ == "__main__":
    main()
