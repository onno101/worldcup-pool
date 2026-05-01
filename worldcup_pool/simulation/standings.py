"""FIFA-style group tables from predicted scores (aligned with ui/src/groupStandings.ts core rules)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class MatchRow:
    id: str
    home_team_code: str
    away_team_code: str
    home_team_name: str
    away_team_name: str


@dataclass
class StandingRow:
    rank: int = 0
    team_code: str = ""
    team_name: str = ""
    played: int = 0
    wins: int = 0
    draws: int = 0
    losses: int = 0
    goals_for: int = 0
    goals_against: int = 0
    goal_diff: int = 0
    points: int = 0


def _draft_line(draft: dict[str, tuple[int, int]], mid: str) -> tuple[int, int] | None:
    return draft.get(mid)


def _sort_standing_rows(rows: list[StandingRow]) -> list[StandingRow]:
    rows.sort(key=lambda r: (-r.points, -r.goal_diff, -r.goals_for, r.team_code))
    for i, r in enumerate(rows):
        r.rank = i + 1
    return rows


def compute_standings_with_roster(
    group_matches: Iterable[MatchRow],
    draft: dict[str, tuple[int, int]],
    roster: list[tuple[str, str]],
) -> list[StandingRow]:
    """roster: [(code, name), ...] length 4."""
    gm = list(group_matches)
    codes = {c for c, _ in roster}
    intra = [m for m in gm if m.home_team_code in codes and m.away_team_code in codes]

    @dataclass
    class Acc:
        name: str
        p: int = 0
        w: int = 0
        d: int = 0
        l: int = 0
        gf: int = 0
        ga: int = 0

    teams: dict[str, Acc] = {}
    for code, name in roster:
        teams[code] = Acc(name=name)

    def bump(code: str, name: str, gf: int, ga: int) -> None:
        t = teams[code]
        t.p += 1
        t.gf += gf
        t.ga += ga
        if gf > ga:
            t.w += 1
        elif gf < ga:
            t.l += 1
        else:
            t.d += 1

    for m in intra:
        line = _draft_line(draft, m.id)
        if line is None:
            continue
        dh, da = line
        bump(m.home_team_code, m.home_team_name, dh, da)
        bump(m.away_team_code, m.away_team_name, da, dh)

    rows: list[StandingRow] = []
    for code, name in roster:
        t = teams[code]
        rows.append(
            StandingRow(
                rank=0,
                team_code=code,
                team_name=t.name,
                played=t.p,
                wins=t.w,
                draws=t.d,
                losses=t.l,
                goals_for=t.gf,
                goals_against=t.ga,
                goal_diff=t.gf - t.ga,
                points=t.w * 3 + t.d,
            )
        )
    return _sort_standing_rows(rows)


def first_and_second(rows: list[StandingRow]) -> tuple[str, str]:
    if len(rows) < 2:
        raise ValueError("need at least 2 teams")
    return rows[0].team_code, rows[1].team_code
