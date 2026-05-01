"""Knockout-stage helpers: AET score semantics and advancer (who reaches the next round)."""

from __future__ import annotations

from worldcup_pool.team_tla import canonical_team_tla


def is_knockout_stage(stage: str | None) -> bool:
    """True for any stage other than group (e.g. ROUND_OF_16, QUARTER_FINALS)."""
    s = (stage or "").strip().upper()
    return bool(s) and s != "GROUP_STAGE"


def predicted_advancer_team_code(
    *,
    pred_home: int,
    pred_away: int,
    home_team_code: str,
    away_team_code: str,
    advance_team_code: str | None,
) -> str | None:
    """Team the user expects to advance: from the scoreline, or tie-break pick when drawn after ET."""
    hc = canonical_team_tla(home_team_code)
    ac = canonical_team_tla(away_team_code)
    if pred_home > pred_away:
        return hc
    if pred_away > pred_home:
        return ac
    if not advance_team_code:
        return None
    adv = canonical_team_tla(advance_team_code)
    if adv == hc or adv == ac:
        return adv
    return None
