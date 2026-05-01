"""Pool ranking: points from match predictions, top-scorer goals, and tournament winner."""

from __future__ import annotations

import json
import unicodedata
from typing import Any, Mapping

from sqlalchemy import text
from sqlalchemy.orm import Session

from worldcup_pool.backend.db import (
    match_predictions_has_advance_team_code_column,
    matches_table_has_goal_events_column,
    matches_table_has_winner_team_code_column,
    try_add_match_predictions_advance_column,
    try_add_matches_goal_events_column,
    try_add_matches_winner_team_column,
    try_ensure_user_profiles_columns,
)
from worldcup_pool.backend.models_api import PoolRankingEntryOut, TopScorerPickOut
from worldcup_pool.knockout_rules import is_knockout_stage, predicted_advancer_team_code
from worldcup_pool.team_tla import canonical_team_tla
from worldcup_pool.tournament_picks import parse_top_scorers_from_storage

# Base points (group stage). Knockout rounds use graduated multipliers (Scorito-style).
PT_OUTCOME = 2
PT_EXACT = 5
PT_SCORER_GOAL = 2
PT_ADVANCER = 3
PT_TOURNAMENT_WINNER = 25

# Graduated multipliers per round (1× group → 1.5× R32 → 2× R16 → 2.5× QF → 3× SF/Final).
_STAGE_MULT: dict[str, float] = {
    "LAST_32": 1.5,
    "ROUND_OF_32": 1.5,
    "LAST_16": 2.0,
    "ROUND_OF_16": 2.0,
    "QUARTER_FINALS": 2.5,
    "SEMI_FINALS": 3.0,
    "THIRD_PLACE": 3.0,
    "FINAL": 3.0,
}


def stage_multiplier(stage: str | None) -> float:
    """Graduated multiplier: 1× group, 1.5× R32, 2× R16, 2.5× QF, 3× SF/3rd/Final."""
    s = (stage or "").strip().upper()
    return _STAGE_MULT.get(s, 1.0)


def norm_player_name(name: str) -> str:
    s = unicodedata.normalize("NFKD", name)
    s = s.encode("ascii", "ignore").decode("ascii")
    return " ".join(s.lower().split())


def names_match_pick_to_scorer(pick_name: str, scorer_name: str) -> bool:
    a, b = norm_player_name(pick_name), norm_player_name(scorer_name)
    if not a or not b:
        return False
    if a == b:
        return True
    shorter, longer = (a, b) if len(a) <= len(b) else (b, a)
    if len(shorter) >= 5 and shorter in longer:
        return True
    return False


def scorer_pick_key(p: TopScorerPickOut) -> str:
    return f"{p.player_name.strip().lower()}\t{p.country_code}"


def outcome_correct(ph: int, pa: int, ah: int, aa: int) -> bool:
    if ph > pa and ah > aa:
        return True
    if ph < pa and ah < aa:
        return True
    if ph == pa and ah == aa:
        return True
    return False


def parse_goal_events(val: Any) -> list[dict[str, str]]:
    if val is None:
        return []
    if isinstance(val, str):
        try:
            val = json.loads(val)
        except json.JSONDecodeError:
            return []
    if not isinstance(val, list):
        return []
    out: list[dict[str, str]] = []
    for x in val:
        if not isinstance(x, dict):
            continue
        pn = str(x.get("player_name") or "").strip()
        tc = str(x.get("team_code") or "").strip()
        if pn and tc:
            out.append({"player_name": pn, "team_code": canonical_team_tla(tc)})
    return out


def actual_advancer_team_code(
    act_home: int,
    act_away: int,
    home_team_code: str,
    away_team_code: str,
    winner_team_code: str | None,
) -> str | None:
    """Team that advances from a knockout: API `winner_team_code` when set, else scoreline (no pens in DB)."""
    hc = canonical_team_tla(home_team_code)
    ac = canonical_team_tla(away_team_code)
    if winner_team_code:
        w = canonical_team_tla(str(winner_team_code))
        if w == hc or w == ac:
            return w
    if act_home > act_away:
        return hc
    if act_away > act_home:
        return ac
    return None


def fetch_actual_tournament_winner_team(session: Session) -> str | None:
    try_add_matches_winner_team_column(session)
    has_win = matches_table_has_winner_team_code_column(session)
    win_sel = "winner_team_code" if has_win else "CAST(NULL AS TEXT) AS winner_team_code"
    row = session.execute(
        text(
            f"""
            SELECT home_team_code, away_team_code, home_score, away_score, {win_sel}
            FROM matches
            WHERE status = 'FINISHED'
              AND home_score IS NOT NULL
              AND away_score IS NOT NULL
              AND UPPER(TRIM(COALESCE(stage, ''))) = 'FINAL'
            ORDER BY kickoff_utc DESC
            LIMIT 1
            """
        )
    ).mappings().first()
    if not row:
        return None
    h, a = int(row["home_score"]), int(row["away_score"])
    w = row.get("winner_team_code")
    adv = actual_advancer_team_code(h, a, str(row["home_team_code"]), str(row["away_team_code"]), w)
    return adv


def fetch_finished_match_rows(session: Session) -> list[Mapping[str, Any]]:
    """Finished matches with scores; goal_events may be absent on older DB schemas."""
    try_add_matches_goal_events_column(session)
    try_add_matches_winner_team_column(session)
    has_ge = matches_table_has_goal_events_column(session)
    ge_expr = "goal_events" if has_ge else "NULL AS goal_events"
    has_win = matches_table_has_winner_team_code_column(session)
    win_sel = "winner_team_code" if has_win else "CAST(NULL AS TEXT) AS winner_team_code"
    q = text(
        f"""
        SELECT
            id::text AS id,
            stage,
            home_team_code,
            away_team_code,
            home_score,
            away_score,
            {win_sel},
            status,
            {ge_expr}
        FROM matches
        WHERE status = 'FINISHED'
          AND home_score IS NOT NULL
          AND away_score IS NOT NULL
        """
    )
    try:
        return list(session.execute(q).mappings().all())
    except Exception:
        session.rollback()
        return []


def fetch_all_knockout_match_rows(session: Session) -> list[Mapping[str, Any]]:
    """All knockout matches (finished or not) — used to build the predicted-advancers set."""
    try_add_matches_winner_team_column(session)
    has_win = matches_table_has_winner_team_code_column(session)
    win_sel = "winner_team_code" if has_win else "CAST(NULL AS TEXT) AS winner_team_code"
    q = text(
        f"""
        SELECT
            id::text AS id,
            stage,
            home_team_code,
            away_team_code,
            home_score,
            away_score,
            {win_sel},
            status
        FROM matches
        WHERE UPPER(COALESCE(NULLIF(TRIM(stage), ''), 'GROUP_STAGE')) <> 'GROUP_STAGE'
        """
    )
    try:
        return list(session.execute(q).mappings().all())
    except Exception:
        session.rollback()
        return []


def load_user_match_predictions(
    session: Session, user_id: str
) -> dict[str, tuple[int | None, int | None, str | None]]:
    try_add_match_predictions_advance_column(session)
    has_adv = match_predictions_has_advance_team_code_column(session)
    adv_sel = "advance_team_code" if has_adv else "CAST(NULL AS TEXT) AS advance_team_code"
    out: dict[str, tuple[int | None, int | None, str | None]] = {}
    for r in session.execute(
        text(
            f"""
            SELECT match_id::text AS mid, home_goals, away_goals, {adv_sel}
            FROM match_predictions
            WHERE user_id = :u
              AND home_goals IS NOT NULL
              AND away_goals IS NOT NULL
            """
        ),
        {"u": user_id},
    ).mappings():
        adv = r.get("advance_team_code")
        adv_s = str(adv).strip() if adv is not None else None
        out[str(r["mid"])] = (r["home_goals"], r["away_goals"], adv_s or None)
    return out


def awarded_points_for_tournament_picks(
    session: Session,
    user_id: str,
    tournament_winner_team_code: str | None,
    top_scorers: list[TopScorerPickOut],
) -> tuple[int, list[TopScorerPickOut]]:
    """Tournament tab: champion bonus + per-pick scorer goal points (same rules as leaderboard)."""
    try_add_matches_goal_events_column(session)
    actual_winner = fetch_actual_tournament_winner_team(session)
    pw = 0
    if actual_winner and tournament_winner_team_code:
        if canonical_team_tla(str(tournament_winner_team_code)) == actual_winner:
            pw = PT_TOURNAMENT_WINNER
    preds = load_user_match_predictions(session, user_id)
    match_rows = fetch_finished_match_rows(session)
    enriched: list[TopScorerPickOut] = []
    for pick in top_scorers:
        total_s = 0
        for m in match_rows:
            mid = str(m["id"])
            pr = preds.get(mid)
            if not pr:
                continue
            goals = parse_goal_events(m["goal_events"])
            _o, _e, s, _adv = points_for_finished_match(
                stage=m["stage"],
                pred_home=pr[0],
                pred_away=pr[1],
                pred_advance_team_code=pr[2],
                home_team_code=str(m["home_team_code"]),
                away_team_code=str(m["away_team_code"]),
                act_home=int(m["home_score"]),
                act_away=int(m["away_score"]),
                act_winner_team_code=m.get("winner_team_code"),
                goal_events=goals,
                top_scorer_picks=[pick],
            )
            total_s += s
        enriched.append(pick.model_copy(update={"points_awarded": total_s}))
    return pw, enriched


def points_for_finished_match(
    *,
    stage: str | None,
    pred_home: int | None,
    pred_away: int | None,
    pred_advance_team_code: str | None = None,
    home_team_code: str = "",
    away_team_code: str = "",
    act_home: int,
    act_away: int,
    act_winner_team_code: str | None = None,
    goal_events: list[dict[str, str]],
    top_scorer_picks: list[TopScorerPickOut],
) -> tuple[int, int, int, int]:
    """Returns (outcome_points, exact_points, scorer_goal_points, advancer_points).

    Advancer points are always 0 here — they are computed separately at round
    level by ``compute_round_advancer_points`` so that a correctly predicted
    team earns credit regardless of which specific match it advances from.
    """
    mult = stage_multiplier(stage)
    if pred_home is None or pred_away is None:
        return 0, 0, 0, 0

    ph, pa = int(pred_home), int(pred_away)
    cmp_h, cmp_a = int(act_home), int(act_away)

    picks_matched: set[str] = set()
    for g in goal_events:
        gname = g.get("player_name") or ""
        gteam = canonical_team_tla(str(g.get("team_code") or ""))
        if not gname or not gteam:
            continue
        for pick in top_scorer_picks:
            if pick.country_code == "?":
                continue
            if canonical_team_tla(pick.country_code) != gteam:
                continue
            if names_match_pick_to_scorer(pick.player_name, gname):
                picks_matched.add(scorer_pick_key(pick))

    scorer_pts = round(len(picks_matched) * PT_SCORER_GOAL * mult)

    if ph == cmp_h and pa == cmp_a:
        return 0, round(PT_EXACT * mult), scorer_pts, 0
    if outcome_correct(ph, pa, cmp_h, cmp_a):
        return round(PT_OUTCOME * mult), 0, scorer_pts, 0
    return 0, 0, scorer_pts, 0


def compute_round_advancer_points(
    all_ko_matches: list[Mapping[str, Any]],
    user_preds: dict[str, tuple[int | None, int | None, str | None]],
) -> dict[str, int]:
    """Round-level advancer scoring: credit for predicting a team reaches the next
    round, regardless of which specific match it advances from.

    For each knockout stage:
      1. Build the set of teams the user predicts to advance (from ALL matches in
         that stage, using their predicted scores + penalty picks).
      2. For each finished match, check whether the actual advancer appears in that set.
      3. Award advancer points on the match where the team actually advanced.

    Returns ``{match_id: advancer_points}``.
    """
    from collections import defaultdict

    by_stage: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for m in all_ko_matches:
        stage = (m.get("stage") or "").strip().upper()
        if is_knockout_stage(stage):
            by_stage[stage].append(m)

    result: dict[str, int] = {}

    for stage, matches in by_stage.items():
        mult = stage_multiplier(stage)

        # 1. Predicted advancers: every team the user expects to advance from
        #    any match in this stage (including unfinished / future matches).
        predicted: set[str] = set()
        for m in matches:
            mid = str(m["id"])
            pred = user_preds.get(mid)
            if not pred or pred[0] is None or pred[1] is None:
                continue
            hc = str(m.get("home_team_code") or "")
            ac = str(m.get("away_team_code") or "")
            # For TBD matches, use the advance_team_code directly if available
            if hc == "?" or ac == "?":
                adv_raw = pred[2]
                if adv_raw:
                    tc = canonical_team_tla(adv_raw)
                    if tc and tc != "?":
                        predicted.add(tc)
                continue
            pw = predicted_advancer_team_code(
                pred_home=int(pred[0]),
                pred_away=int(pred[1]),
                home_team_code=hc,
                away_team_code=ac,
                advance_team_code=pred[2],
            )
            if pw and pw != "?":
                predicted.add(pw)

        # 2. Award points for each actual advancer that appears in the predicted set.
        #    Track consumed predictions so each predicted team only matches once.
        remaining = set(predicted)
        for m in matches:
            mid = str(m["id"])
            if (
                m.get("status") != "FINISHED"
                or m.get("home_score") is None
                or m.get("away_score") is None
            ):
                continue
            act = actual_advancer_team_code(
                int(m["home_score"]),
                int(m["away_score"]),
                str(m.get("home_team_code") or ""),
                str(m.get("away_team_code") or ""),
                m.get("winner_team_code"),
            )
            if act and act in remaining:
                result[mid] = round(PT_ADVANCER * mult)
                remaining.discard(act)

    return result


def compute_leaderboard(session: Session) -> list[PoolRankingEntryOut]:
    """
    Leaderboard rows: users who either saved a profile or saved at least one match line.

    **Scoring (base × graduated multiplier per round):**

    Base points (group stage, ×1):
    - Correct outcome (winner or draw): 2 pts
    - Exact scoreline: 5 pts (exclusive with outcome)
    - Top-scorer goal: 2 pts per pick per match

    Graduated multipliers (Scorito-style):
    - R32 ×1.5 · R16 ×2 · QF ×2.5 · SF/3rd/Final ×3

    Advancer (round-level): 3 pts base × multiplier. Credit for correctly predicting
    a team reaches the next round, regardless of which specific match it advances from.

    Tournament winner: 25 pts (flat, not multiplied).
    """
    try_add_matches_goal_events_column(session)
    try_ensure_user_profiles_columns(session)
    winner_code = fetch_actual_tournament_winner_team(session)

    match_pred_rows = session.execute(
        text(
            """
            SELECT DISTINCT user_id
            FROM match_predictions
            WHERE home_goals IS NOT NULL AND away_goals IS NOT NULL
            """
        )
    ).mappings().all()
    profile_rows_for_eligibility = session.execute(
        text("SELECT DISTINCT user_id FROM user_profiles")
    ).mappings().all()
    eligible_uids = sorted(
        {str(r["user_id"]) for r in match_pred_rows}
        | {str(r["user_id"]) for r in profile_rows_for_eligibility}
    )
    if not eligible_uids:
        return []

    try_add_match_predictions_advance_column(session)
    has_adv = match_predictions_has_advance_team_code_column(session)
    if has_adv:
        fill_rows = session.execute(
            text(
                """
                SELECT mp.user_id, COUNT(*) AS n
                FROM match_predictions mp
                INNER JOIN matches m ON m.id = mp.match_id
                WHERE mp.home_goals IS NOT NULL
                  AND mp.away_goals IS NOT NULL
                  AND (
                    UPPER(COALESCE(NULLIF(TRIM(m.stage), ''), 'GROUP_STAGE')) = 'GROUP_STAGE'
                    OR mp.home_goals <> mp.away_goals
                    OR mp.advance_team_code IS NOT NULL
                  )
                GROUP BY mp.user_id
                """
            )
        ).mappings().all()
    else:
        fill_rows = session.execute(
            text(
                """
                SELECT user_id, COUNT(*) AS n
                FROM match_predictions
                WHERE home_goals IS NOT NULL AND away_goals IS NOT NULL
                GROUP BY user_id
                """
            )
        ).mappings().all()
    filled_by_uid: dict[str, int] = {}
    for r in fill_rows:
        uid = str(r["user_id"])
        try:
            filled_by_uid[uid] = int(r["n"])
        except (TypeError, ValueError):
            filled_by_uid[uid] = 0

    preds_by_user: dict[str, dict[str, tuple[int | None, int | None, str | None]]] = {}
    adv_sel2 = "advance_team_code" if has_adv else "CAST(NULL AS TEXT) AS advance_team_code"
    for r in session.execute(
        text(
            f"""
            SELECT user_id, match_id::text AS mid, home_goals, away_goals, {adv_sel2}
            FROM match_predictions
            WHERE home_goals IS NOT NULL AND away_goals IS NOT NULL
            """
        )
    ).mappings():
        uid = str(r["user_id"])
        adv = r.get("advance_team_code")
        adv_s = str(adv).strip() if adv is not None else None
        preds_by_user.setdefault(uid, {})[str(r["mid"])] = (
            r["home_goals"],
            r["away_goals"],
            adv_s or None,
        )

    tour_by_user: dict[str, tuple[str | None, dict[str, Any], str | None]] = {}
    for r in session.execute(
        text(
            "SELECT user_id, tournament_winner_team_code, notes_json, top_scorer_player_name "
            "FROM tournament_predictions"
        )
    ).mappings():
        nj = r["notes_json"] if isinstance(r["notes_json"], dict) else {}
        tour_by_user[str(r["user_id"])] = (
            r["tournament_winner_team_code"],
            nj,
            r["top_scorer_player_name"],
        )

    display_by_uid: dict[str, str | None] = {}
    picture_by_uid: dict[str, str | None] = {}
    profile_has_picture_col = True
    try:
        profile_rows = session.execute(
            text("SELECT user_id, display_name, profile_picture FROM user_profiles")
        ).mappings().all()
    except Exception:
        session.rollback()
        profile_has_picture_col = False
        profile_rows = session.execute(
            text("SELECT user_id, display_name FROM user_profiles")
        ).mappings().all()
    for r in profile_rows:
        uid = str(r["user_id"])
        display_by_uid[uid] = r["display_name"]
        picture_by_uid[uid] = r["profile_picture"] if profile_has_picture_col else None

    email_by_uid: dict[str, str | None] = {}
    for r in session.execute(text("SELECT user_id, email, display_name FROM app_users_cache")).mappings():
        uid = str(r["user_id"])
        if r["email"]:
            email_by_uid[uid] = r["email"]
        if (display_by_uid.get(uid) is None or not str(display_by_uid.get(uid) or "").strip()) and r[
            "display_name"
        ]:
            display_by_uid[uid] = r["display_name"]

    match_rows = fetch_finished_match_rows(session)
    all_ko_matches = fetch_all_knockout_match_rows(session)

    entries: list[PoolRankingEntryOut] = []
    for uid in eligible_uids:
        po, pe, ps, p_adv, pw = 0, 0, 0, 0, 0
        picks = []
        tup = tour_by_user.get(uid)
        if tup:
            _, notes, legacy_ts = tup
            picks = parse_top_scorers_from_storage(notes, legacy_ts)

        preds = preds_by_user.get(uid, {})
        for m in match_rows:
            mid = str(m["id"])
            pr = preds.get(mid)
            if not pr:
                continue
            goals = parse_goal_events(m["goal_events"])
            o, e, s, _adv = points_for_finished_match(
                stage=m["stage"],
                pred_home=pr[0],
                pred_away=pr[1],
                pred_advance_team_code=pr[2],
                home_team_code=str(m["home_team_code"]),
                away_team_code=str(m["away_team_code"]),
                act_home=int(m["home_score"]),
                act_away=int(m["away_score"]),
                act_winner_team_code=m.get("winner_team_code"),
                goal_events=goals,
                top_scorer_picks=picks,
            )
            po += o
            pe += e
            ps += s

        # Round-level advancer: credit for predicting team reaches next round
        adv_map = compute_round_advancer_points(all_ko_matches, preds)
        p_adv = sum(adv_map.values())

        if tup and winner_code:
            w_pick, _, _ = tup
            if w_pick and canonical_team_tla(str(w_pick)) == winner_code:
                pw = PT_TOURNAMENT_WINNER

        total = po + pe + ps + p_adv + pw
        dn = display_by_uid.get(uid)
        if dn is not None and not str(dn).strip():
            dn = None
        pic = picture_by_uid.get(uid)
        if pic is not None and not str(pic).strip():
            pic = None
        entries.append(
            PoolRankingEntryOut(
                rank=0,
                user_id=uid,
                display_name=dn,
                email=email_by_uid.get(uid),
                profile_picture=pic,
                match_predictions_filled=filled_by_uid.get(uid, 0),
                total_points=total,
                points_outcome=po,
                points_exact=pe,
                points_scorer_goals=ps,
                points_advancer=p_adv,
                points_tournament_winner=pw,
            )
        )

    entries.sort(key=lambda x: (-x.total_points, (x.display_name or "").lower(), x.user_id))

    ranked: list[PoolRankingEntryOut] = []
    rank = 0
    for i, e in enumerate(entries):
        if i == 0 or e.total_points != entries[i - 1].total_points:
            rank = i + 1
        ranked.append(e.model_copy(update={"rank": rank}))

    return ranked
