"""Official FIFA World Cup 2026 group-stage draw (12 × 4 teams).

Sourced from the Wikipedia tournament article group tables (Dec 2025 final draw),
which reference FIFA: https://www.fifa.com/en/tournaments/mens/worldcup/canadamexicousa2026/standings

Team codes (TLA) are aligned with typical football-data.org / FIFA three-letter
abbreviations used in match payloads. If the API uses a different spelling for a
nation, `/api/group-rosters` prefers display names from synced `matches` rows.
"""

from __future__ import annotations

# Order within each tuple is FIFA table order (Pos 1–4 in the draw article).
WC2026_GROUPS: dict[str, tuple[tuple[str, str], ...]] = {
    "GROUP_A": (
        ("MEX", "Mexico"),
        ("RSA", "South Africa"),
        ("KOR", "South Korea"),
        ("CZE", "Czech Republic"),
    ),
    "GROUP_B": (
        ("CAN", "Canada"),
        ("BIH", "Bosnia and Herzegovina"),
        ("QAT", "Qatar"),
        ("SUI", "Switzerland"),
    ),
    "GROUP_C": (
        ("BRA", "Brazil"),
        ("MAR", "Morocco"),
        ("HAI", "Haiti"),
        ("SCO", "Scotland"),
    ),
    "GROUP_D": (
        ("USA", "United States"),
        ("PAR", "Paraguay"),
        ("AUS", "Australia"),
        ("TUR", "Turkey"),
    ),
    "GROUP_E": (
        ("GER", "Germany"),
        ("CUW", "Curaçao"),
        ("CIV", "Ivory Coast"),
        ("ECU", "Ecuador"),
    ),
    "GROUP_F": (
        ("NED", "Netherlands"),
        ("JPN", "Japan"),
        ("SWE", "Sweden"),
        ("TUN", "Tunisia"),
    ),
    "GROUP_G": (
        ("BEL", "Belgium"),
        ("EGY", "Egypt"),
        ("IRN", "Iran"),
        ("NZL", "New Zealand"),
    ),
    "GROUP_H": (
        ("ESP", "Spain"),
        ("CPV", "Cape Verde"),
        ("KSA", "Saudi Arabia"),
        ("URU", "Uruguay"),
    ),
    "GROUP_I": (
        ("FRA", "France"),
        ("SEN", "Senegal"),
        ("IRQ", "Iraq"),
        ("NOR", "Norway"),
    ),
    "GROUP_J": (
        ("ARG", "Argentina"),
        ("ALG", "Algeria"),
        ("AUT", "Austria"),
        ("JOR", "Jordan"),
    ),
    "GROUP_K": (
        ("POR", "Portugal"),
        ("COD", "DR Congo"),
        ("UZB", "Uzbekistan"),
        ("COL", "Colombia"),
    ),
    "GROUP_L": (
        ("ENG", "England"),
        ("CRO", "Croatia"),
        ("GHA", "Ghana"),
        ("PAN", "Panama"),
    ),
}


def official_wc2026_group_teams() -> dict[str, list[tuple[str, str]]]:
    """Return mapping GROUP_XX -> [(team_code, default_english_name), ...] (four teams each)."""
    return {gk: list(teams) for gk, teams in WC2026_GROUPS.items()}
