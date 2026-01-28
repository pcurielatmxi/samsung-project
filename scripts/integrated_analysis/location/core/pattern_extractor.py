"""
Pattern Extraction for Room/Stair/Elevator Codes

Extracts location codes from free-text fields (location_raw, summary) in RABA/PSI data.
These codes enable ROOM_DIRECT matching instead of relying solely on grid inference.

Supported Patterns:
- FAB Room Codes: FAB116109A, FAB130311A, FAB1XXXXX
- Stair: "Stair 21", "Stairwell 4", "STR-21", "STR 21"
- Elevator: "Elevator 22", "Elev 3", "ELV-22", "ELV 22"

Usage:
    from scripts.integrated_analysis.location.core.pattern_extractor import extract_location_codes

    codes = extract_location_codes("Level 3 Elevator 22 at FAB130311A")
    # Returns: {'room_codes': ['FAB130311A'], 'stair_codes': [], 'elevator_codes': ['ELV-22']}
"""

import re
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class ExtractedLocationCodes:
    """Results of pattern extraction from text."""
    room_codes: List[str] = field(default_factory=list)      # FAB1XXXXX format
    stair_codes: List[str] = field(default_factory=list)     # STR-NN format
    elevator_codes: List[str] = field(default_factory=list)  # ELV-NN format

    @property
    def has_codes(self) -> bool:
        """Returns True if any codes were extracted."""
        return bool(self.room_codes or self.stair_codes or self.elevator_codes)

    @property
    def primary_code(self) -> Optional[str]:
        """
        Returns the most specific code found (room > stair > elevator).

        Priority: Room codes are most specific, then stair/elevator.
        """
        if self.room_codes:
            return self.room_codes[0]
        if self.stair_codes:
            return self.stair_codes[0]
        if self.elevator_codes:
            return self.elevator_codes[0]
        return None

    @property
    def primary_type(self) -> Optional[str]:
        """Returns the type of the primary code."""
        if self.room_codes:
            return 'ROOM'
        if self.stair_codes:
            return 'STAIR'
        if self.elevator_codes:
            return 'ELEVATOR'
        return None


# FAB Room Code patterns
# Matches: FAB116109A, FAB130311A, FAB1XXXXX, FABXXXXX
FAB_ROOM_PATTERN = re.compile(
    r'FAB1?(\d{5,6}[A-Z]?)\b',
    re.IGNORECASE
)

# Stair patterns - extract the number
STAIR_PATTERNS = [
    # "Stair 21", "Stair #21", "STAIR 21"
    re.compile(r'\bstair\s*#?\s*(\d+)\b', re.IGNORECASE),
    # "Stairwell 4", "Stairwell #4"
    re.compile(r'\bstairwell\s*#?\s*(\d+)\b', re.IGNORECASE),
    # "STR-21", "STR21", "STR 21"
    re.compile(r'\bSTR[-\s]?(\d+)\b', re.IGNORECASE),
]

# Elevator patterns - extract the number
ELEVATOR_PATTERNS = [
    # "Elevator 22", "Elevator #22"
    re.compile(r'\belevator\s*#?\s*(\d+)\b', re.IGNORECASE),
    # "Elev 3", "Elev #3", "Elev. 3"
    re.compile(r'\belev\.?\s*#?\s*(\d+)\b', re.IGNORECASE),
    # "ELV-22", "ELV22", "ELV 22"
    re.compile(r'\bELV[-\s]?(\d+)\b', re.IGNORECASE),
]


def _normalize_fab_code(match: str) -> list:
    """
    Normalize FAB room code to possible formats for lookup.

    Input: "16109A" (captured group without FAB prefix)
    Output: ["FAB116109A", "FAB116109"] - with and without suffix letter

    Returns list of possible codes to try during lookup.
    """
    code = match.upper()
    base_code = f"FAB1{code}"
    results = [base_code]

    # Also try without trailing letter suffix (A, B, C, etc.)
    if code and code[-1].isalpha():
        results.append(f"FAB1{code[:-1]}")

    return results


def _normalize_stair_code(number: str) -> list:
    """
    Normalize stair number to possible formats for lookup.

    Input: "21" or "4"
    Output: ["FAB1-ST21", "STR-21"] or ["FAB1-ST04", "STR-04"]

    dim_location uses FAB1-STNN format, but we also try STR-NN as fallback.
    """
    num = int(number)
    return [
        f"FAB1-ST{num:02d}",  # Primary format in dim_location
        f"STR-{num:02d}",      # Fallback format
    ]


def _normalize_elevator_code(number: str) -> list:
    """
    Normalize elevator number to possible formats for lookup.

    Input: "22" or "3"
    Output: ["FAB1-EL22", "ELV-22"] or ["FAB1-EL03", "ELV-03"]

    dim_location uses FAB1-ELNN format, but we also try ELV-NN as fallback.
    """
    num = int(number)
    return [
        f"FAB1-EL{num:02d}",  # Primary format in dim_location
        f"ELV-{num:02d}",      # Fallback format
    ]


def extract_location_codes(text: str) -> ExtractedLocationCodes:
    """
    Extract room, stair, and elevator codes from free text.

    Args:
        text: Free text containing location information (location_raw, summary, etc.)

    Returns:
        ExtractedLocationCodes with normalized codes.
        Each code list contains possible formats to try during lookup
        (e.g., FAB116109A, FAB116109 for rooms with suffix letters).

    Examples:
        >>> extract_location_codes("Level 3 Elevator 22 at FAB130311A")
        ExtractedLocationCodes(room_codes=['FAB130311A', 'FAB130311'], ...)

        >>> extract_location_codes("Stair 21 and Stair 22 inspection")
        ExtractedLocationCodes(stair_codes=['FAB1-ST21', 'STR-21', 'FAB1-ST22', 'STR-22'], ...)
    """
    if not text or not isinstance(text, str):
        return ExtractedLocationCodes()

    result = ExtractedLocationCodes()

    # Extract FAB room codes
    fab_matches = FAB_ROOM_PATTERN.findall(text)
    for match in fab_matches:
        codes = _normalize_fab_code(match)
        for code in codes:
            if code not in result.room_codes:
                result.room_codes.append(code)

    # Extract stair codes
    stair_numbers = set()
    for pattern in STAIR_PATTERNS:
        matches = pattern.findall(text)
        stair_numbers.update(matches)

    for num in sorted(stair_numbers, key=int):
        codes = _normalize_stair_code(num)
        for code in codes:
            if code not in result.stair_codes:
                result.stair_codes.append(code)

    # Extract elevator codes
    elevator_numbers = set()
    for pattern in ELEVATOR_PATTERNS:
        matches = pattern.findall(text)
        elevator_numbers.update(matches)

    for num in sorted(elevator_numbers, key=int):
        codes = _normalize_elevator_code(num)
        for code in codes:
            if code not in result.elevator_codes:
                result.elevator_codes.append(code)

    return result


def extract_location_codes_for_lookup(text: str) -> List[tuple]:
    """
    Extract location codes from text, returning (code, type) tuples for lookup.

    Returns codes in priority order:
    1. Room codes (most specific) - with and without suffix variants
    2. Stair codes - FAB1-STNN and STR-NN variants
    3. Elevator codes - FAB1-ELNN and ELV-NN variants

    The caller should iterate through and use the first one found in dim_location.

    Args:
        text: Free text containing location information

    Returns:
        List of (code, type) tuples to try during lookup

    Examples:
        >>> extract_location_codes_for_lookup("Elevator 22 at FAB130311A")
        [('FAB130311A', 'ROOM'), ('FAB130311', 'ROOM'),
         ('FAB1-EL22', 'ELEVATOR'), ('ELV-22', 'ELEVATOR')]
    """
    codes = extract_location_codes(text)
    result = []

    # Add room codes first (highest priority)
    for code in codes.room_codes:
        result.append((code, 'ROOM'))

    # Then stair codes
    for code in codes.stair_codes:
        result.append((code, 'STAIR'))

    # Then elevator codes
    for code in codes.elevator_codes:
        result.append((code, 'ELEVATOR'))

    return result


def extract_primary_location_code(text: str) -> tuple:
    """
    Extract the most specific location code from text.

    Convenience function that returns just the first code and its type.
    For comprehensive lookup, use extract_location_codes_for_lookup() instead.

    Args:
        text: Free text containing location information

    Returns:
        Tuple of (code, type) where type is 'ROOM', 'STAIR', 'ELEVATOR', or (None, None)

    Examples:
        >>> extract_primary_location_code("Level 3 Elevator 22 at FAB130311A")
        ('FAB130311A', 'ROOM')

        >>> extract_primary_location_code("Stair 21 inspection")
        ('FAB1-ST21', 'STAIR')
    """
    codes = extract_location_codes_for_lookup(text)
    if codes:
        return codes[0]
    return (None, None)
