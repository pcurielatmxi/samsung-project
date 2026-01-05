#!/usr/bin/env python3
"""
Company name fuzzy matching module for quality inspection data.

Matches company names from inspection reports to canonical company names
using fuzzy string similarity and optional dimension table lookups.
"""

import pandas as pd
from pathlib import Path
from typing import Tuple, Optional, Dict
from rapidfuzz import fuzz, process


class CompanyMatcher:
    """
    Fuzzy match company names to canonical names.

    Can work in two modes:
    1. With dimension tables (ideal) - uses existing company dimensions
    2. Without dimension tables (fallback) - builds canonical list from data
    """

    def __init__(
        self,
        company_dim_path: Optional[str] = None,
        aliases_path: Optional[str] = None,
        threshold: float = 0.85
    ):
        """
        Initialize company matcher with optional dimension tables.

        Args:
            company_dim_path: Path to dim_company.csv (optional)
            aliases_path: Path to map_company_aliases.csv (optional)
            threshold: Minimum match score (0.0-1.0), default 0.85
        """
        self.threshold = threshold
        self.alias_lookup = {}
        self.company_names = {}
        self.all_aliases = []

        # Try to load dimension tables if provided
        if company_dim_path and aliases_path:
            self._load_dimension_tables(company_dim_path, aliases_path)
        else:
            print("⚠ No dimension tables provided - will use fallback matching mode")

    def _load_dimension_tables(self, company_dim_path: str, aliases_path: str) -> None:
        """Load existing dimension tables."""
        try:
            company_dim_file = Path(company_dim_path)
            aliases_file = Path(aliases_path)

            if not company_dim_file.exists():
                print(f"⚠ Company dimension file not found: {company_dim_file}")
                return

            if not aliases_file.exists():
                print(f"⚠ Aliases file not found: {aliases_file}")
                return

            dim_company = pd.read_csv(company_dim_file)
            aliases = pd.read_csv(aliases_file)

            # Build alias lookup
            for _, row in aliases.iterrows():
                alias_key = str(row.get('alias', '')).lower().strip()
                if alias_key:
                    self.alias_lookup[alias_key] = {
                        'company_id': row.get('company_id'),
                        'confidence': float(row.get('confidence', 1.0))
                    }

            # Build company names lookup
            for _, row in dim_company.iterrows():
                self.company_names[int(row.get('company_id', 0))] = str(row.get('canonical_name', ''))

            # All known aliases for fuzzy matching
            self.all_aliases = list(self.alias_lookup.keys())

            print(f"✓ Loaded {len(dim_company)} companies and {len(aliases)} aliases")

        except Exception as e:
            print(f"⚠ Error loading dimension tables: {e}")

    def match(
        self,
        company_name: Optional[str]
    ) -> Tuple[Optional[int], Optional[str], float]:
        """
        Match a company name to canonical ID and name.

        Uses three strategies:
        1. Exact match in alias lookup (confidence = 1.0)
        2. Fuzzy match against known aliases (rapifuzz token_sort_ratio)
        3. No match (returns None, None, 0.0)

        Args:
            company_name: Raw company name from source data

        Returns:
            Tuple of (company_id, canonical_name, confidence)
            All None if no match found
        """
        if pd.isna(company_name):
            return None, None, 0.0

        normalized = str(company_name).lower().strip()

        # Strategy 1: Exact match in alias lookup
        if normalized in self.alias_lookup:
            match_info = self.alias_lookup[normalized]
            company_id = match_info['company_id']
            canonical_name = self.company_names.get(company_id)
            return company_id, canonical_name, match_info['confidence']

        # Strategy 2: Fuzzy match (only if we have aliases)
        if self.all_aliases:
            result = process.extractOne(
                normalized,
                self.all_aliases,
                scorer=fuzz.token_sort_ratio
            )

            if result and result[1] >= (self.threshold * 100):
                matched_alias = result[0]
                match_info = self.alias_lookup[matched_alias]
                company_id = match_info['company_id']
                canonical_name = self.company_names.get(company_id)
                confidence = result[1] / 100.0

                return company_id, canonical_name, confidence

        # Strategy 3: No match found
        return None, None, 0.0

    def build_from_data(self, company_names: list) -> None:
        """
        Build alias list from raw company names (fallback mode).

        Useful when dimension tables don't exist yet.
        Creates synthetic company IDs and treats each unique name as canonical.

        Args:
            company_names: List of company names from data
        """
        seen = set()
        for name in company_names:
            if pd.notna(name):
                normalized = str(name).lower().strip()
                if normalized and normalized not in seen:
                    seen.add(normalized)
                    company_id = hash(normalized) % 100000  # Synthetic ID
                    self.alias_lookup[normalized] = {
                        'company_id': company_id,
                        'confidence': 1.0
                    }
                    self.company_names[company_id] = str(name)

        self.all_aliases = list(self.alias_lookup.keys())
        print(f"✓ Built fallback company matcher with {len(seen)} unique companies")
