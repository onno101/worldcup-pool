"""Parse tournament prediction notes (top scorers) from DB storage — shared by API and scoring."""

from __future__ import annotations

from typing import Any

from worldcup_pool.backend.models_api import TopScorerPickOut
from worldcup_pool.pool_limits import MAX_TOP_SCORER_PICKS
from worldcup_pool.team_tla import canonical_team_tla
from worldcup_pool.wc2026_official_groups import official_wc2026_group_teams


def wc_country_code_to_name() -> dict[str, str]:
    m: dict[str, str] = {}
    for teams in official_wc2026_group_teams().values():
        for code, name in teams:
            m[canonical_team_tla(code.strip().upper())] = name
    return m


def parse_top_scorers_from_storage(
    notes: dict[str, Any] | None,
    legacy_ts: str | None,
) -> list[TopScorerPickOut]:
    c2n = wc_country_code_to_name()
    out: list[TopScorerPickOut] = []
    if isinstance(notes, dict):
        raw = notes.get("top_scorers")
        if isinstance(raw, list):
            for x in raw:
                if not isinstance(x, dict):
                    continue
                pn = str(x.get("player_name") or "").strip()
                if not pn:
                    continue
                cc_raw = str(x.get("country_code") or "").strip()
                if not cc_raw or cc_raw == "?":
                    out.append(
                        TopScorerPickOut(
                            player_name=pn,
                            country_code="?",
                            country_name=str(x.get("country_name") or "Unknown"),
                        )
                    )
                    continue
                cc = canonical_team_tla(cc_raw)
                out.append(
                    TopScorerPickOut(
                        player_name=pn,
                        country_code=cc,
                        country_name=str(x.get("country_name") or c2n.get(cc, cc)),
                    )
                )
    if not out and legacy_ts and str(legacy_ts).strip():
        out.append(
            TopScorerPickOut(
                player_name=str(legacy_ts).strip(),
                country_code="?",
                country_name="Unknown",
            )
        )
    return out[:MAX_TOP_SCORER_PICKS]
