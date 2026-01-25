#!/usr/bin/env python3
"""
Shared normalization utilities for quality inspection data.

Provides standardized functions for normalizing dates, roles, and inspection types.
"""

import pandas as pd
import re
from typing import Optional, List, Dict, Any


def normalize_date(date_str: Optional[str]) -> Optional[str]:
    """
    Normalize date to YYYY-MM-DD format.

    Handles:
    - YYYY-MM-DD (returns as-is)
    - MM/DD/YYYY
    - M/D/YYYY
    - Other formats (uses pandas flexible parser)

    Args:
        date_str: Date string in various formats

    Returns:
        Date in YYYY-MM-DD format, or None if unparseable
    """
    if pd.isna(date_str):
        return None

    date_str = str(date_str).strip()

    # Already in YYYY-MM-DD format
    if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
        return date_str

    # Try MM/DD/YYYY or M/D/YYYY
    try:
        dt = pd.to_datetime(date_str, format='%m/%d/%Y', errors='coerce')
        if pd.notna(dt):
            return dt.strftime('%Y-%m-%d')
    except Exception:
        pass

    # Fallback: pandas flexible parser
    try:
        dt = pd.to_datetime(date_str, errors='coerce')
        if pd.notna(dt):
            return dt.strftime('%Y-%m-%d')
    except Exception:
        pass

    # Unable to parse
    return None


def normalize_role(role: Optional[str]) -> Optional[str]:
    """
    Normalize role field to lowercase and trim whitespace.

    Args:
        role: Role string (e.g., "Inspector", "CONTRACTOR")

    Returns:
        Normalized role (lowercase), or None if input is None
    """
    if pd.isna(role):
        return None

    return str(role).lower().strip()


def normalize_inspection_type(type_str: Optional[str]) -> Optional[str]:
    """
    Normalize inspection/test type to lowercase and trim whitespace.

    Args:
        type_str: Inspection type string

    Returns:
        Normalized inspection type (lowercase), or None if input is None
    """
    if pd.isna(type_str):
        return None

    return str(type_str).lower().strip()


# =============================================================================
# Party Normalization
# =============================================================================

# Trade/discipline terms that should NOT be parties
_TRADE_DISCIPLINES = {
    'arch', 'architectural', 'architecture',
    'drywall', 'framing', 'framing & drywall', 'framing and drywall',
    'mep', 'mechanical', 'electrical', 'plumbing',
    'structural', 'civil', 'concrete',
    'fire protection', 'hvac', 'piping',
}

# Patterns that indicate a trade/discipline (not a party)
_TRADE_PATTERNS = [
    r'^arch\b',  # starts with "arch"
    r'^architectural\b',
    r'\bframing\b.*\bdrywall\b',  # "framing" and "drywall" together
    r'^drywall\b',
    r'\btrade\b',
]

# Company indicators - if name contains these, it's likely a company
_COMPANY_INDICATORS = [
    'inc', 'inc.', 'llc', 'corp', 'corporation', 'company', 'co.',
    'psi', 'intertek', 'professional services',
    'construction', 'concrete', 'drilling', 'erectors', 'coatings',
    'materials', 'chemical', 'precast', 'steel', 'painting', 'structures',
    'samsung', 'yates', 'berg', 'secai', 'raba', 'kistner', 'coreslab',
    'hensel phelps', 'austin global', 'rolling plains', 'cherry coatings',
    'fd thomas', 'grout tech', 'alpha painting', 'north star', 'gate precast',
    'lone star', 'texas concrete', 'lauren concrete', 'spartan group',
]

# Known persons who are often incorrectly assigned company roles
# Maps name -> correct role
_KNOWN_PERSONS = {
    'kevin jeong': 'client_contact',
    'mark hammond': 'contractor_rep',
    'michael adams': 'contractor_rep',
    'mohammed majeed': 'contractor_rep',
    'alex rendon': 'contractor_rep',
    'jose flores': 'contractor_rep',
    'john mamao': 'contractor_rep',
    'roberto lira': 'contractor_rep',
    'chris plassmann': 'contractor_rep',
    'herman villalobos': 'contractor_rep',
    'saad ekab': 'contractor_rep',
    'gerardo rodriguez': 'contractor_rep',
    'a.s. hashi': 'client_contact',
    'ronaldo hernandez': 'contractor_rep',
    'mohammed alsheikh': 'contractor_rep',
}

# Inspection company names (should have role 'inspection_company', not 'inspector')
_INSPECTION_COMPANIES = [
    'psi', 'intertek', 'professional services industries',
    'raba kistner', 'raba', 'kistner',
]


def _is_trade_discipline(name: str) -> bool:
    """Check if name is a trade/discipline, not a party."""
    name_lower = name.lower().strip()

    # Direct match
    if name_lower in _TRADE_DISCIPLINES:
        return True

    # Pattern match
    for pattern in _TRADE_PATTERNS:
        if re.search(pattern, name_lower, re.IGNORECASE):
            return True

    return False


def _is_company_name(name: str) -> bool:
    """Check if name appears to be a company (not a person)."""
    name_lower = name.lower()
    return any(indicator in name_lower for indicator in _COMPANY_INDICATORS)


def _is_inspection_company(name: str) -> bool:
    """Check if name is an inspection/testing company."""
    name_lower = name.lower()
    return any(company in name_lower for company in _INSPECTION_COMPANIES)


def _split_mixed_name(name: str) -> List[Dict[str, str]]:
    """
    Split mixed person/company names like "Chris Plassmann/YATES".

    Returns list of {name, entity_type} dicts.
    """
    # Pattern: "Person Name / COMPANY" or "Person Name/COMPANY"
    if '/' not in name:
        return [{'name': name, 'entity_type': None}]

    parts = [p.strip() for p in name.split('/') if p.strip()]
    if len(parts) != 2:
        return [{'name': name, 'entity_type': None}]

    results = []
    for part in parts:
        if _is_company_name(part):
            results.append({'name': part, 'entity_type': 'company'})
        else:
            # Check if it looks like a person name (2-3 words, mixed case)
            words = part.split()
            if 1 <= len(words) <= 3 and not part.isupper():
                results.append({'name': part, 'entity_type': 'person'})
            else:
                results.append({'name': part, 'entity_type': None})

    return results


def normalize_parties(
    parties: List[Dict[str, Any]],
    source: str = 'unknown'
) -> List[Dict[str, Any]]:
    """
    Normalize parties_involved list, fixing common extraction errors.

    Fixes:
    1. Filters out trade/discipline entries (e.g., "Arch" with role "trade")
    2. Fixes company names with person roles (e.g., "Intertek PSI" as "inspector")
    3. Splits mixed person/company names (e.g., "Chris Plassmann/YATES")
    4. Fixes known persons incorrectly assigned company roles

    Args:
        parties: List of party dicts with 'name' and 'role' keys
        source: Source identifier ('psi' or 'raba') for source-specific rules

    Returns:
        Normalized list of party dicts
    """
    if not parties:
        return []

    normalized = []

    for party in parties:
        if not isinstance(party, dict):
            continue

        name = party.get('name', '')
        role = party.get('role', '')
        company = party.get('company')

        if not name or pd.isna(name):
            continue

        name = str(name).strip()
        role = str(role).lower().strip() if role and not pd.isna(role) else None

        # -----------------------------------------------------------------
        # Rule 1: Filter out trade/discipline entries
        # -----------------------------------------------------------------
        if _is_trade_discipline(name):
            continue  # Skip this entry entirely

        if role and 'trade' in role:
            continue  # Skip entries with "trade" in the role

        # -----------------------------------------------------------------
        # Rule 2: Split mixed person/company names
        # -----------------------------------------------------------------
        if '/' in name and len(name) < 50:
            split_parts = _split_mixed_name(name)
            if len(split_parts) > 1:
                # Add each part as a separate party
                for part in split_parts:
                    part_name = part['name']
                    part_type = part['entity_type']

                    if part_type == 'company':
                        # Determine appropriate company role
                        if _is_inspection_company(part_name):
                            part_role = 'inspection_company'
                        else:
                            part_role = 'contractor'
                    elif part_type == 'person':
                        # Person from a mixed entry is likely a rep
                        part_role = 'contractor_rep'
                    else:
                        part_role = role

                    normalized.append({
                        'name': part_name,
                        'role': part_role,
                        'company': company,
                        'entity_type': part_type,
                    })
                continue  # Don't process original entry

        # -----------------------------------------------------------------
        # Rule 3: Fix inspection companies with "inspector" role
        # -----------------------------------------------------------------
        if _is_inspection_company(name) and role in ('inspector', 'inspected by'):
            role = 'inspection_company'

        # -----------------------------------------------------------------
        # Rule 4: Fix known persons with company roles
        # -----------------------------------------------------------------
        name_lower = name.lower().strip()
        if name_lower in _KNOWN_PERSONS:
            if role in ('contractor', 'subcontractor', 'supplier'):
                role = _KNOWN_PERSONS[name_lower]

        # -----------------------------------------------------------------
        # Rule 5: Determine entity_type for remaining entries
        # -----------------------------------------------------------------
        if _is_company_name(name):
            entity_type = 'company'
        elif name_lower in _KNOWN_PERSONS:
            entity_type = 'person'
        else:
            # Heuristic: person names are typically 2-3 words, mixed case
            words = name.split()
            if 2 <= len(words) <= 3 and not name.isupper() and any(c.islower() for c in name):
                entity_type = 'person'
            else:
                entity_type = None  # Unknown

        normalized.append({
            'name': name,
            'role': role,
            'company': company,
            'entity_type': entity_type,
        })

    return normalized
