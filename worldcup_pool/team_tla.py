"""Normalize external team TLAs to FIFA-style codes used in WC2026 official draw data."""

from __future__ import annotations

# football-data.org / other feeds sometimes differ from Wikipedia/FIFA draw TLAs.
_TEAM_TLA_CANONICAL: dict[str, str] = {
    "CUR": "CUW",  # Curaçao — API often uses CUR; official draw / FIFA use CUW
    "DEU": "GER",  # ISO alpha-3 vs common football abbreviation
    "HOL": "NED",  # Historic Netherlands
    "SCT": "SCO",  # Scotland alternate in some feeds
}


def canonical_team_tla(raw: str | None, *, max_len: int = 16) -> str:
    """Uppercase TLA with known aliases folded to the canonical code."""
    s = (raw or "").strip().upper()
    if not s:
        return "?"
    if len(s) > max_len:
        s = s[:max_len]
    return _TEAM_TLA_CANONICAL.get(s, s)
