#!/usr/bin/env python3
"""
Validate XER manifest.json structure and file references.

Usage:
    python scripts/validate_xer_manifest.py
    python scripts/validate_xer_manifest.py --fix  # Auto-fix missing files entries

Exit codes:
    0 - Valid
    1 - Invalid structure or missing files
"""

import json
import sys
from pathlib import Path

MANIFEST_PATH = Path(__file__).parent.parent / "data" / "raw" / "xer" / "manifest.json"
XER_DIR = MANIFEST_PATH.parent

# Schema definition for validation
REQUIRED_SCHEMA = {
    "current": str,  # filename of current XER file
    "files": dict,   # dict of filename -> metadata
}

FILE_ENTRY_SCHEMA = {
    "date": str,         # YYYY-MM-DD format
    "description": str,  # human-readable description
    "status": str,       # "current", "archived", or "superseded"
}

VALID_STATUSES = {"current", "archived", "superseded"}


def validate_manifest(manifest_path: Path = MANIFEST_PATH, fix: bool = False) -> tuple[bool, list[str]]:
    """
    Validate manifest.json structure and file references.

    Returns:
        tuple: (is_valid, list of error messages)
    """
    errors = []
    warnings = []

    # Check file exists
    if not manifest_path.exists():
        return False, [f"Manifest not found: {manifest_path}"]

    # Parse JSON
    try:
        with open(manifest_path) as f:
            manifest = json.load(f)
    except json.JSONDecodeError as e:
        return False, [f"Invalid JSON: {e}"]

    # Validate top-level structure
    for key, expected_type in REQUIRED_SCHEMA.items():
        if key not in manifest:
            errors.append(f"Missing required key: '{key}'")
        elif not isinstance(manifest.get(key), expected_type):
            errors.append(f"'{key}' must be {expected_type.__name__}, got {type(manifest.get(key)).__name__}")

    if errors:
        return False, errors

    # Validate 'current' references an existing entry
    current = manifest["current"]
    if current not in manifest["files"]:
        errors.append(f"'current' value '{current}' not found in 'files' dict")

    # Validate each file entry
    current_count = 0
    for filename, metadata in manifest["files"].items():
        prefix = f"files['{filename}']"

        # Check metadata structure
        if not isinstance(metadata, dict):
            errors.append(f"{prefix}: must be a dict, got {type(metadata).__name__}")
            continue

        for key, expected_type in FILE_ENTRY_SCHEMA.items():
            if key not in metadata:
                errors.append(f"{prefix}: missing required key '{key}'")
            elif not isinstance(metadata.get(key), expected_type):
                errors.append(f"{prefix}.{key}: must be {expected_type.__name__}")

        # Validate status value
        status = metadata.get("status")
        if status and status not in VALID_STATUSES:
            errors.append(f"{prefix}.status: must be one of {VALID_STATUSES}, got '{status}'")

        if status == "current":
            current_count += 1

        # Check XER file exists on disk
        xer_path = XER_DIR / filename
        if not xer_path.exists():
            warnings.append(f"{prefix}: XER file not found on disk: {xer_path}")

    # Validate exactly one current file
    if current_count == 0:
        errors.append("No file has status 'current'")
    elif current_count > 1:
        errors.append(f"Multiple files have status 'current' (expected 1, found {current_count})")

    # Check current file's status matches
    if current in manifest["files"]:
        current_status = manifest["files"][current].get("status")
        if current_status != "current":
            errors.append(f"'current' points to '{current}' but its status is '{current_status}', not 'current'")

    # Check for XER files on disk not in manifest
    xer_files_on_disk = set(f.name for f in XER_DIR.glob("*.xer"))
    xer_files_in_manifest = set(manifest["files"].keys())
    untracked = xer_files_on_disk - xer_files_in_manifest

    if untracked:
        for filename in sorted(untracked):
            warnings.append(f"XER file on disk not in manifest: {filename}")

        if fix:
            print("Auto-fixing: Adding missing files to manifest...")
            for filename in untracked:
                manifest["files"][filename] = {
                    "date": "YYYY-MM-DD",
                    "description": "TODO: Add description",
                    "status": "archived"
                }
            with open(manifest_path, "w") as f:
                json.dump(manifest, f, indent=2)
                f.write("\n")
            print(f"Added {len(untracked)} files. Please update their metadata.")

    # Print warnings (non-fatal)
    for warning in warnings:
        print(f"WARNING: {warning}")

    return len(errors) == 0, errors


def main():
    fix_mode = "--fix" in sys.argv

    print(f"Validating: {MANIFEST_PATH}")
    print("-" * 50)

    is_valid, errors = validate_manifest(fix=fix_mode)

    if errors:
        print("\nERRORS:")
        for error in errors:
            print(f"  ❌ {error}")
        print(f"\nValidation FAILED with {len(errors)} error(s)")
        sys.exit(1)
    else:
        print("\n✅ Manifest is valid")
        sys.exit(0)


if __name__ == "__main__":
    main()
