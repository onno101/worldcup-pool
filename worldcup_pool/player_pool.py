"""World Cup player directory for top-scorer search (curated WC2026 candidates + optional football-data)."""

from __future__ import annotations

import json
import threading
from pathlib import Path

from worldcup_pool.team_tla import canonical_team_tla
from worldcup_pool.wc2026_official_groups import official_wc2026_group_teams

_cache_lock = threading.Lock()
_cached: dict[str, list[dict[str, str]]] = {}

_CANDIDATES_JSON = Path(__file__).resolve().parent / "data" / "wc2026_top_scorer_candidates.json"

# Optional headline names; keys must exist in the 2026 draw for them to be used.
_FAMOUS_RAW: dict[str, str] = {
    "MEX": "Santiago Giménez",
    "USA": "Christian Pulisic",
    "CAN": "Alphonso Davies",
    "BRA": "Vinícius Júnior",
    "ARG": "Lionel Messi",
    "FRA": "Kylian Mbappé",
    "ENG": "Harry Kane",
    "ESP": "Lamine Yamal",
    "GER": "Jamal Musiala",
    "POR": "Cristiano Ronaldo",
    "NED": "Virgil van Dijk",
    "BEL": "Kevin De Bruyne",
    "COL": "Luis Díaz",
    "URU": "Federico Valverde",
    "CRO": "Luka Modrić",
    "SUI": "Granit Xhaka",
    "AUT": "David Alaba",
    "MAR": "Achraf Hakimi",
    "SEN": "Sadio Mané",
    "EGY": "Mohamed Salah",
    "JPN": "Takefusa Kubo",
    "KOR": "Son Heung-min",
    "AUS": "Mathew Ryan",
    "IRN": "Sardar Azmoun",
    "KSA": "Salem Al-Dawsari",
    "QAT": "Akram Afif",
    "ECU": "Moises Caicedo",
    "PAR": "Miguel Almirón",
    "NOR": "Erling Haaland",
    "SWE": "Alexander Isak",
    "SCO": "Scott McTominay",
    "TUR": "Hakan Çalhanoğlu",
    "RSA": "Ronwen Williams",
    "CZE": "Patrik Schick",
    "BIH": "Edin Džeko",
    "HAI": "Melchie Dumornay",
    "CUW": "Leandro Bacuna",
    "CIV": "Sébastien Haller",
    "TUN": "Youssef Msakni",
    "NZL": "Chris Wood",
    "CPV": "Ryan Mendes",
    "IRQ": "Mohanad Ali",
    "ALG": "Riyad Mahrez",
    "JOR": "Musa Al-Taamari",
    "COD": "Yoane Wissa",
    "UZB": "Jaloliddin Masharipov",
    "GHA": "Mohammed Kudus",
    "PAN": "Aníbal Godoy",
}


def _code_to_country_name() -> dict[str, str]:
    m: dict[str, str] = {}
    for teams in official_wc2026_group_teams().values():
        for code, name in teams:
            m[canonical_team_tla(code)] = name
    return m


_CODES_OK = frozenset(_code_to_country_name().keys())
_FAMOUS = {k: v for k, v in _FAMOUS_RAW.items() if k in _CODES_OK}


def _load_curated_top_scorer_candidates() -> list[dict[str, str]] | None:
    """25 attacker-weighted names per qualified nation from bundled JSON (see scripts/build_top_scorer_candidates_json.py)."""
    if not _CANDIDATES_JSON.is_file():
        return None
    try:
        raw = json.loads(_CANDIDATES_JSON.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    c2n = _code_to_country_name()
    out: list[dict[str, str]] = []
    for code_raw, names in raw.items():
        code = canonical_team_tla(str(code_raw).strip().upper())
        if code not in c2n or not isinstance(names, list):
            continue
        cname = c2n[code]
        seen: set[str] = set()
        for n in names:
            name = str(n).strip()
            if not name:
                continue
            k = name.lower()
            if k in seen:
                continue
            seen.add(k)
            out.append({"player_name": name, "country_code": code, "country_name": cname})
    if not out:
        return None
    out.sort(key=lambda r: (r["country_name"].lower(), r["player_name"].lower()))
    return out


def _static_fallback_directory() -> list[dict[str, str]]:
    """~3 entries per qualified nation so search works without football-data token."""
    c2n = _code_to_country_name()
    out: list[dict[str, str]] = []
    for code, cname in sorted(c2n.items(), key=lambda x: x[1]):
        head = _FAMOUS.get(code)
        if head:
            out.append({"player_name": head, "country_code": code, "country_name": cname})
        out.append({"player_name": f"{cname} — squad pick A", "country_code": code, "country_name": cname})
        out.append({"player_name": f"{cname} — squad pick B", "country_code": code, "country_name": cname})
    return out


def _fetch_from_football_data(token: str, competition_id: str) -> list[dict[str, str]]:
    from worldcup_pool.football_data import FootballDataClient

    client = FootballDataClient(token)
    return client.fetch_all_squad_players(competition_id)


def get_worldcup_player_directory(token: str, competition_id: str) -> list[dict[str, str]]:
    """
    Cached list of {player_name, country_code, country_name}.
    Prefers bundled WC2026 top-scorer candidate lists (25 per nation). If that file is missing,
    uses football-data.org squads when a token is set; otherwise a small static fallback.
    """
    cid = competition_id.strip().upper()
    curated = _load_curated_top_scorer_candidates()
    try:
        mtime = int(_CANDIDATES_JSON.stat().st_mtime) if _CANDIDATES_JSON.is_file() else 0
    except OSError:
        mtime = 0
    key = f"{cid}|cur={mtime}|fd={bool(token and token.strip())}"
    with _cache_lock:
        if key in _cached:
            return _cached[key]
    merged: list[dict[str, str]] = []
    if curated is not None:
        merged = list(curated)
    elif token and token.strip():
        try:
            merged.extend(_fetch_from_football_data(token, competition_id))
        except Exception:
            merged.clear()
    if not merged:
        merged = _static_fallback_directory()

    deduped: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    c2n = _code_to_country_name()
    for row in merged:
        name = (row.get("player_name") or "").strip()
        code = canonical_team_tla((row.get("country_code") or "").strip() or "?")
        cname = (row.get("country_name") or c2n.get(code, code)).strip()
        if not name or code == "?":
            continue
        k = (name.lower(), code)
        if k in seen:
            continue
        seen.add(k)
        deduped.append({"player_name": name, "country_code": code, "country_name": cname})
    deduped.sort(key=lambda r: (r["country_name"].lower(), r["player_name"].lower()))
    with _cache_lock:
        _cached[key] = deduped
    return deduped


def filter_player_directory(rows: list[dict[str, str]], query: str, *, limit: int = 400) -> list[dict[str, str]]:
    q = (query or "").strip().lower()
    if not q:
        return rows[:limit]
    out = [
        r
        for r in rows
        if q in r["player_name"].lower()
        or q in r["country_name"].lower()
        or q in r["country_code"].lower()
    ]
    return out[:limit]
