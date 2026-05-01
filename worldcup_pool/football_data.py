"""football-data.org API client (fixtures and results)."""

from __future__ import annotations

import dataclasses
import logging
from dataclasses import field
from datetime import datetime, timezone
from typing import Any

import httpx

from worldcup_pool.knockout_rules import is_knockout_stage
from worldcup_pool.team_tla import canonical_team_tla

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class NormalizedMatch:
    external_match_id: str
    competition_code: str
    stage: str | None
    matchday: int | None
    group_key: str | None
    home_team_code: str
    away_team_code: str
    home_team_name: str
    away_team_name: str
    kickoff_utc: datetime
    status: str
    home_score: int | None
    away_score: int | None
    """Pool compares predictions to this line: full-time in group stage; after extra time (AET) in knockouts when API provides it."""
    winner_team_code: str | None = None
    """For knockouts: team that advances (penalty shoot-out winner when AET was a draw). Synced from the API."""
    goal_events: list[dict[str, str]] = field(default_factory=list)


def _parse_utc(iso: str) -> datetime:
    if iso.endswith("Z"):
        iso = iso[:-1] + "+00:00"
    return datetime.fromisoformat(iso).astimezone(timezone.utc)


def _map_status(api_status: str) -> str:
    s = (api_status or "").upper()
    if s in ("FINISHED", "AWARDED"):
        return "FINISHED"
    if s in ("IN_PLAY", "PAUSED"):
        return "LIVE"
    if s in ("POSTPONED", "CANCELLED", "SUSPENDED"):
        return "POSTPONED"
    return "SCHEDULED"


def _extract_goal_events(raw: dict[str, Any]) -> list[dict[str, str]]:
    """Goal scorers from match payload (for pool ranking)."""
    home = raw.get("homeTeam") or {}
    away = raw.get("awayTeam") or {}
    hid, aid = home.get("id"), away.get("id")
    htla = canonical_team_tla(str(home.get("tla") or home.get("shortName") or "?"))
    atla = canonical_team_tla(str(away.get("tla") or away.get("shortName") or "?"))
    out: list[dict[str, str]] = []
    for g in raw.get("goals") or []:
        scorer = g.get("scorer") or {}
        name = (scorer.get("name") or "").strip()
        if not name:
            continue
        team = g.get("team") or {}
        tid = team.get("id")
        tla = ""
        if hid is not None and tid == hid:
            tla = htla
        elif aid is not None and tid == aid:
            tla = atla
        else:
            tl = canonical_team_tla(str(team.get("tla") or team.get("shortName") or "?"))
            if tl == htla:
                tla = htla
            elif tl == atla:
                tla = atla
            else:
                continue
        if not tla or tla == "?":
            continue
        out.append({"player_name": name, "team_code": tla})
    return out


def _block_pair(block: Any) -> tuple[int | None, int | None]:
    """football-data v4 uses `home`/`away` or `homeTeam`/`awayTeam` depending on endpoint version."""
    if not isinstance(block, dict):
        return None, None
    h, a = block.get("home"), block.get("away")
    if h is None and block.get("homeTeam") is not None:
        h, a = block.get("homeTeam"), block.get("awayTeam")
    try:
        if h is not None and a is not None:
            return int(h), int(a)
    except (TypeError, ValueError):
        pass
    return None, None


def _penalty_pair(score: dict[str, Any]) -> tuple[int | None, int | None]:
    return _block_pair(score.get("penalties") or {})


def _pool_scores_and_penalties(
    stage: str | None, score: dict[str, Any], mapped_status: str
) -> tuple[int | None, int | None, int | None, int | None]:
    """
    Returns (home, away, pen_home, pen_away).

    Group stage: full-time (90') line when available.
    Knockout: after extra time (AET) when `extraTime` is present; otherwise full-time until ET exists.
    Penalties are returned separately for determining who advances when AET is a draw.
    """
    if mapped_status not in ("LIVE", "FINISHED"):
        return None, None, None, None
    ph, pa = _penalty_pair(score)
    if is_knockout_stage(stage):
        # football-data.org `fullTime` is the cumulative score (incl. extra time when played).
        # `extraTime` is only the delta (goals scored during the 30 ET minutes), NOT the total.
        ft_h, ft_a = _block_pair(score.get("fullTime") or {})
        if ft_h is not None and ft_a is not None:
            return ft_h, ft_a, ph, pa
        rt_h, rt_a = _block_pair(score.get("regularTime") or {})
        if rt_h is not None and rt_a is not None:
            return rt_h, rt_a, ph, pa
        if ph is not None and pa is not None:
            return ph, pa, None, None
        return None, None, ph, pa
    # Group / unknown: classic full-time display
    for key in ("fullTime", "regularTime", "extraTime", "penalties"):
        h, a = _block_pair(score.get(key) or {})
        if h is not None and a is not None:
            return h, a, ph, pa
    return None, None, ph, pa


def _winner_team_from_result(
    home_code: str,
    away_code: str,
    hs: int | None,
    aw: int | None,
    pen_h: int | None,
    pen_a: int | None,
) -> str | None:
    if hs is None or aw is None:
        return None
    hc = canonical_team_tla(home_code)
    ac = canonical_team_tla(away_code)
    if hs > aw:
        return hc
    if aw > hs:
        return ac
    if pen_h is not None and pen_a is not None:
        if pen_h > pen_a:
            return hc
        if pen_a > pen_h:
            return ac
    return None


def normalize_match(raw: dict[str, Any], competition_code: str) -> NormalizedMatch:
    home = raw.get("homeTeam") or {}
    away = raw.get("awayTeam") or {}
    score = raw.get("score") or {}
    utc = raw.get("utcDate") or raw.get("utc")
    if not utc:
        raise ValueError("match has no utcDate/utc")
    kickoff = _parse_utc(str(utc))
    mapped = _map_status(raw.get("status") or "SCHEDULED")
    raw_stage = raw.get("stage")
    hs, aw, pen_h, pen_a = _pool_scores_and_penalties(raw_stage, score, mapped)
    grp = raw.get("group")
    if isinstance(grp, str) and grp.strip():
        group_key = grp.strip().upper()
    elif isinstance(grp, dict):
        gname = (grp.get("name") or grp.get("code") or "") if grp else ""
        group_key = str(gname).strip().upper() or None
    else:
        group_key = None
    raw_home = (home.get("tla") or home.get("shortName") or "?")[:16]
    raw_away = (away.get("tla") or away.get("shortName") or "?")[:16]
    htla = canonical_team_tla(raw_home)
    atla = canonical_team_tla(raw_away)
    goals = _extract_goal_events(raw) if mapped in ("LIVE", "FINISHED") else []
    mid = raw.get("id")
    if mid is None:
        raise ValueError("match has no id")
    win: str | None = None
    if is_knockout_stage(raw_stage) and mapped == "FINISHED":
        win = _winner_team_from_result(htla, atla, hs, aw, pen_h, pen_a)
    return NormalizedMatch(
        external_match_id=str(mid),
        competition_code=competition_code,
        stage=raw_stage,
        matchday=raw.get("matchday"),
        group_key=group_key,
        home_team_code=htla,
        away_team_code=atla,
        home_team_name=home.get("name") or home.get("shortName") or "TBD",
        away_team_name=away.get("name") or away.get("shortName") or "TBD",
        kickoff_utc=kickoff,
        status=mapped,
        home_score=hs,
        away_score=aw,
        winner_team_code=win,
        goal_events=goals,
    )


class FootballDataClient:
    BASE = "https://api.football-data.org/v4"

    def __init__(self, token: str) -> None:
        if not token:
            raise ValueError("FOOTBALL_DATA_TOKEN is required for sync")
        self._headers = {"X-Auth-Token": token}

    def fetch_competition_matches(self, competition_id: str) -> list[NormalizedMatch]:
        """Fetch matches for a competition (requests a high limit to reduce pagination)."""
        out: list[NormalizedMatch] = []
        base_url = f"{self.BASE}/competitions/{competition_id}/matches"
        with httpx.Client(timeout=120.0) as client:
            r = client.get(base_url, headers=self._headers, params={"limit": 999})
            r.raise_for_status()
            data = r.json()
            for m in data.get("matches") or []:
                if not isinstance(m, dict):
                    continue
                try:
                    out.append(normalize_match(m, competition_id))
                except Exception as exc:
                    logger.warning(
                        "Skipping malformed match from football-data (id=%r): %s",
                        m.get("id"),
                        exc,
                    )
            if not out and (data.get("matches") or []):
                raise ValueError(
                    "football-data returned matches but none could be parsed — check API payload or token tier"
                )
        return out

    def fetch_match_goal_events(self, match_id: str | int) -> list[dict[str, str]]:
        """Fetch goal events for a single match from the detail endpoint."""
        with httpx.Client(timeout=60.0) as client:
            r = client.get(f"{self.BASE}/matches/{match_id}", headers=self._headers)
            if r.status_code == 429:
                logger.warning("Rate limited fetching match %s goals; skipping", match_id)
                return []
            r.raise_for_status()
            return _extract_goal_events(r.json())

    def enrich_goal_events(
        self, matches: list[NormalizedMatch], *, rate_limit_delay: float = 2.2
    ) -> None:
        """Fetch individual match details for FINISHED/LIVE matches to populate goal_events.

        The bulk ``/competitions/{id}/matches`` endpoint returns empty ``goals: []``.
        Goal scorer data is only available from ``/matches/{id}``.  This method enriches
        in-place, respecting the API rate limit (30 req/min on upgraded tier ≈ 2s spacing).
        """
        import time

        targets = [m for m in matches if m.status in ("FINISHED", "LIVE") and not m.goal_events]
        if not targets:
            return
        logger.info("Enriching goal events for %s matches (%.0fs est.)", len(targets), len(targets) * rate_limit_delay)
        for i, m in enumerate(targets):
            try:
                m.goal_events = self.fetch_match_goal_events(m.external_match_id)
            except Exception as exc:
                logger.warning("Failed to fetch goals for match %s: %s", m.external_match_id, exc)
            if i < len(targets) - 1:
                time.sleep(rate_limit_delay)

    def fetch_all_squad_players(self, competition_id: str) -> list[dict[str, str]]:
        """National-team squad lists: player_name, country_code (TLA), country_name (team name)."""
        out: list[dict[str, str]] = []
        with httpx.Client(timeout=120.0) as client:
            r = client.get(
                f"{self.BASE}/competitions/{competition_id}/teams",
                headers=self._headers,
            )
            r.raise_for_status()
            teams = r.json().get("teams") or []
            for t in teams:
                tid = t.get("id")
                if tid is None:
                    continue
                country_name = (t.get("name") or t.get("shortName") or "").strip() or "TBD"
                tla = canonical_team_tla(str(t.get("tla") or t.get("shortName") or "?"))
                r2 = client.get(f"{self.BASE}/teams/{int(tid)}", headers=self._headers)
                if r2.status_code != 200:
                    continue
                detail = r2.json()
                for p in detail.get("squad") or []:
                    pname = (p.get("name") or "").strip()
                    if not pname:
                        continue
                    out.append(
                        {
                            "player_name": pname,
                            "country_code": tla,
                            "country_name": country_name,
                        }
                    )
        return out
