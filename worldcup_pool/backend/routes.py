from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import bindparam, text

from worldcup_pool.backend.auth import UserContext, get_user_context, is_admin
from worldcup_pool.backend.config import get_settings
from worldcup_pool.backend.db import (
    match_predictions_has_advance_team_code_column,
    matches_table_has_goal_events_column,
    matches_table_has_winner_team_code_column,
    session_scope,
    try_add_match_predictions_advance_column,
    try_add_matches_goal_events_column,
    try_add_matches_group_key_column,
    try_add_matches_winner_team_column,
    try_ensure_user_profiles_columns,
)
from worldcup_pool.backend.models_api import (
    GroupRosterOut,
    GroupRostersResponse,
    GroupRosterTeam,
    MatchOut,
    MatchPredictionIn,
    MatchPredictionError,
    MeOut,
    PoolDashboardOut,
    PoolRankingOut,
    PoolSummaryOut,
    PublicParticipantProfileOut,
    PutMatchPredictionsIn,
    PutMatchPredictionsOut,
    SyncOut,
    TeamOptionOut,
    TournamentPredictionsIn,
    TournamentPredictionsOut,
    UserProfileIn,
    UserProfileOut,
    WorldcupPlayerOut,
)
from worldcup_pool.knockout_rules import is_knockout_stage
from worldcup_pool.player_pool import filter_player_directory, get_worldcup_player_directory
from worldcup_pool.scoring import (
    actual_advancer_team_code,
    awarded_points_for_tournament_picks,
    compute_leaderboard,
    compute_round_advancer_points,
    parse_goal_events,
    points_for_finished_match,
)
from worldcup_pool.services.sync import run_sync
from worldcup_pool.team_tla import canonical_team_tla
from worldcup_pool.tournament_picks import parse_top_scorers_from_storage
from worldcup_pool.wc2026_official_groups import official_wc2026_group_teams

router = APIRouter(prefix="/api")


def _sort_world_cup_group_keys(keys: list[str]) -> list[str]:
    """GROUP_A … GROUP_L first (alphabetically within), then other keys, then _OTHER."""

    def sort_key(k: str) -> tuple:
        if k == "_OTHER":
            return (2, k)
        m = re.match(r"^GROUP_([A-Z]+)$", k, re.IGNORECASE)
        if m:
            return (0, m.group(1).upper())
        return (1, k.upper())

    return sorted(keys, key=sort_key)


_group_key_col_found = False


def _matches_table_has_group_key_column(session) -> bool:
    """Cached after first positive; the column is created by init_schema and try_add_matches_group_key_column."""
    global _group_key_col_found
    if _group_key_col_found:
        return True
    try:
        row = session.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_attribute a
                    JOIN pg_class c ON a.attrelid = c.oid
                    JOIN pg_namespace n ON c.relnamespace = n.oid
                    WHERE n.nspname = current_schema()
                      AND c.relname = 'matches'
                      AND c.relkind = 'r'
                      AND a.attname = 'group_key'
                      AND a.attnum > 0
                      AND NOT a.attisdropped
                )
                """
            )
        ).scalar()
        if row:
            _group_key_col_found = True
        return bool(row)
    except Exception:
        session.rollback()
        return False


@router.get("/health", operation_id="healthCheck")
def health_check():
    return {"status": "ok"}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _prediction_lock_deadline(kickoff_utc: datetime, hours: int) -> datetime:
    """Instant when match predictions close: `hours` before kickoff (UTC-aware)."""
    return kickoff_utc - timedelta(hours=hours)


def _tournament_lock_hours_before_first_kickoff() -> int:
    """Hours before each match kickoff when that fixture's score pick locks (match tab)."""
    return get_settings().prediction_lock_before_kickoff_hours


def _tournament_picks_lock_at_utc() -> datetime:
    """Instant after which champion / top-scorer picks are read-only (from settings, ISO8601)."""
    raw = (get_settings().tournament_picks_lock_at_utc or "").strip()
    if not raw:
        raw = "2026-06-11T18:00:00+00:00"
    s = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _tournament_put_locked_detail() -> str:
    lock_at = _tournament_picks_lock_at_utc()
    return (
        "Tournament predictions are locked — the pool deadline was "
        f"{lock_at.strftime('%Y-%m-%d %H:%M')} UTC "
        f"(configure TOURNAMENT_PICKS_LOCK_AT_UTC / tournament_picks_lock_at_utc)."
    )


def _tournament_editing_open(session) -> bool:
    """True until the configured UTC lock instant (default: first-match window for WC2026)."""
    _ = session  # kept for callers that pass a DB session (tests / scripts)
    return _now() < _tournament_picks_lock_at_utc()


def _clean_adv(val: object) -> str | None:
    if val is None:
        return None
    s = str(val).strip()
    return s or None


def _normalize_tournament_winner_save(code: str | None) -> str | None:
    """Persist champion pick with the same TLA normalization as match sync (CUR→CUW, etc.)."""
    if code is None:
        return None
    s = str(code).strip()
    if not s:
        return None
    c = canonical_team_tla(s)
    return None if c == "?" else c


def _normalize_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    v = value.strip()
    return v or None


def _validate_profile_picture(value: str | None) -> str | None:
    v = _normalize_optional_text(value)
    if v is None:
        return None
    if len(v) > 1_500_000:
        raise HTTPException(status_code=400, detail="Profile picture is too large")
    if v.startswith("data:image/"):
        return v
    raise HTTPException(status_code=400, detail="Profile picture must be an uploaded image")


@router.get("/me", response_model=MeOut, operation_id="getMe")
def get_me(user: UserContext = Depends(get_user_context)):
    return MeOut(
        user_id=user.user_id,
        email=user.email,
        is_admin=is_admin(user),
    )


@router.get("/profile", response_model=UserProfileOut, operation_id="getMyProfile")
def get_my_profile(user: UserContext = Depends(get_user_context)):
    uid = user.user_id
    with session_scope() as session:
        try_ensure_user_profiles_columns(session)
        row = session.execute(
            text(
                """
                SELECT user_id, display_name, nationality, profile_picture, updated_at
                FROM user_profiles
                WHERE user_id = :u
                """
            ),
            {"u": uid},
        ).mappings().first()
    if not row:
        return UserProfileOut(
            user_id=uid,
            display_name=None,
            nationality=None,
            profile_picture=None,
            updated_at=None,
        )
    return UserProfileOut(
        user_id=row["user_id"],
        display_name=row["display_name"],
        nationality=row["nationality"],
        profile_picture=row["profile_picture"],
        updated_at=row["updated_at"],
    )


@router.put("/profile", response_model=UserProfileOut, operation_id="putMyProfile")
def put_my_profile(
    body: UserProfileIn,
    user: UserContext = Depends(get_user_context),
):
    uid = user.user_id
    display_name = _normalize_optional_text(body.display_name)
    nationality = _normalize_optional_text(body.nationality)
    picture = _validate_profile_picture(body.profile_picture)

    with session_scope() as session:
        try_ensure_user_profiles_columns(session)
        session.execute(
            text(
                """
                INSERT INTO user_profiles (
                    user_id, display_name, nationality, profile_picture
                ) VALUES (
                    :u, :display_name, :nationality, :profile_picture
                )
                ON CONFLICT (user_id) DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    nationality = EXCLUDED.nationality,
                    profile_picture = EXCLUDED.profile_picture,
                    updated_at = now()
                """
            ),
            {
                "u": uid,
                "display_name": display_name,
                "nationality": nationality,
                "profile_picture": picture,
            },
        )
        row = session.execute(
            text(
                """
                SELECT user_id, display_name, nationality, profile_picture, updated_at
                FROM user_profiles
                WHERE user_id = :u
                """
            ),
            {"u": uid},
        ).mappings().one()
    return UserProfileOut(
        user_id=row["user_id"],
        display_name=row["display_name"],
        nationality=row["nationality"],
        profile_picture=row["profile_picture"],
        updated_at=row["updated_at"],
    )


@router.get("/pool-summary", response_model=PoolSummaryOut, operation_id="getPoolSummary")
def pool_summary(user: UserContext = Depends(get_user_context)):
    uid = user.user_id
    with session_scope() as session:
        total = session.execute(text("SELECT count(*) FROM matches")).scalar_one()
        try_add_match_predictions_advance_column(session)
        has_adv_col = match_predictions_has_advance_team_code_column(session)
        if has_adv_col:
            predicted = session.execute(
                text(
                    """
                    SELECT count(*) FROM match_predictions mp
                    INNER JOIN matches m ON m.id = mp.match_id
                    WHERE mp.user_id = :u
                      AND mp.home_goals IS NOT NULL
                      AND mp.away_goals IS NOT NULL
                      AND (
                        UPPER(COALESCE(NULLIF(TRIM(m.stage), ''), 'GROUP_STAGE')) = 'GROUP_STAGE'
                        OR mp.home_goals <> mp.away_goals
                        OR mp.advance_team_code IS NOT NULL
                      )
                    """
                ),
                {"u": uid},
            ).scalar_one()
        else:
            predicted = session.execute(
                text(
                    """
                    SELECT count(*) FROM match_predictions
                    WHERE user_id = :u
                      AND home_goals IS NOT NULL
                      AND away_goals IS NOT NULL
                    """
                ),
                {"u": uid},
            ).scalar_one()
        lock_h = get_settings().prediction_lock_before_kickoff_hours
        row = session.execute(
            text(
                """
                SELECT (m.kickoff_utc - (:lock_h * interval '1 hour')) AS d
                FROM matches m
                WHERE m.kickoff_utc IS NOT NULL
                  AND m.status NOT IN ('FINISHED', 'POSTPONED')
                  AND (m.kickoff_utc - (:lock_h * interval '1 hour')) > now()
                ORDER BY d ASC
                LIMIT 1
                """
            ),
            {"lock_h": lock_h},
        ).one_or_none()
        next_d = row[0] if row else None
    label = None
    if next_d:
        label = next_d.astimezone().strftime("%Y-%m-%d %H:%M %Z")
    return PoolSummaryOut(
        total_matches=int(total),
        predicted_matches=int(predicted),
        next_deadline_utc=next_d,
        next_deadline_label=label,
    )


@router.get("/ranking", response_model=PoolRankingOut, operation_id="getPoolRanking")
def pool_ranking(_user: UserContext = Depends(get_user_context)):
    """Participant leaderboard: match points (group vs 2× KO), top-scorer goals, tournament winner."""
    with session_scope() as session:
        try_ensure_user_profiles_columns(session)
        entries = compute_leaderboard(session)
    return PoolRankingOut(entries=entries)


@router.get("/dashboard", response_model=PoolDashboardOut, operation_id="getPoolDashboard")
def pool_dashboard(_user: UserContext = Depends(get_user_context)):
    """Predictor count plus full leaderboard (UI: Leaderboard tab)."""
    with session_scope() as session:
        try_ensure_user_profiles_columns(session)
        predictors = session.execute(
            text(
                """
                SELECT COUNT(*) FROM (
                    SELECT DISTINCT user_id
                    FROM match_predictions
                    WHERE home_goals IS NOT NULL AND away_goals IS NOT NULL
                    UNION
                    SELECT DISTINCT user_id
                    FROM user_profiles
                ) p
                """
            )
        ).scalar_one()
        entries = compute_leaderboard(session)
    return PoolDashboardOut(
        predictors_count=int(predictors),
        leaderboard=PoolRankingOut(entries=entries),
    )


@router.get(
    "/profiles/{user_id}",
    response_model=PublicParticipantProfileOut,
    operation_id="getParticipantProfile",
)
def get_participant_profile(user_id: str, _user: UserContext = Depends(get_user_context)):
    """Public read-only profile for anyone who saved a profile or any pool predictions."""
    target = (user_id or "").strip()
    if not target:
        raise HTTPException(status_code=400, detail="Missing user id")

    with session_scope() as session:
        try_ensure_user_profiles_columns(session)
        has_predictions = session.execute(
            text(
                """
                SELECT (
                    EXISTS (SELECT 1 FROM user_profiles WHERE user_id = :u)
                    OR
                    EXISTS (SELECT 1 FROM match_predictions WHERE user_id = :u)
                    OR EXISTS (SELECT 1 FROM tournament_predictions WHERE user_id = :u)
                )
                """
            ),
            {"u": target},
        ).scalar()
        if not has_predictions:
            raise HTTPException(status_code=404, detail="No pool participation found for this user")

        prow = session.execute(
            text(
                """
                SELECT display_name, nationality, profile_picture, updated_at
                FROM user_profiles WHERE user_id = :u
                """
            ),
            {"u": target},
        ).mappings().first()
        trow = session.execute(
            text(
                """
                SELECT tournament_winner_team_code, top_scorer_player_name, notes_json
                FROM tournament_predictions WHERE user_id = :u
                """
            ),
            {"u": target},
        ).mappings().first()
        try_add_match_predictions_advance_column(session)
        has_adv_col = match_predictions_has_advance_team_code_column(session)
        if has_adv_col:
            mp_count = session.execute(
                text(
                    """
                    SELECT COUNT(*) FROM match_predictions mp
                    INNER JOIN matches m ON m.id = mp.match_id
                    WHERE mp.user_id = :u
                      AND mp.home_goals IS NOT NULL
                      AND mp.away_goals IS NOT NULL
                      AND (
                        UPPER(COALESCE(NULLIF(TRIM(m.stage), ''), 'GROUP_STAGE')) = 'GROUP_STAGE'
                        OR mp.home_goals <> mp.away_goals
                        OR mp.advance_team_code IS NOT NULL
                      )
                    """
                ),
                {"u": target},
            ).scalar_one()
        else:
            mp_count = session.execute(
                text(
                    """
                    SELECT COUNT(*) FROM match_predictions
                    WHERE user_id = :u AND home_goals IS NOT NULL AND away_goals IS NOT NULL
                    """
                ),
                {"u": target},
            ).scalar_one()

    nj: dict[str, Any] = {}
    legacy_ts: str | None = None
    tw: str | None = None
    if trow:
        raw_nj = trow["notes_json"]
        nj = dict(raw_nj) if isinstance(raw_nj, dict) else {}
        legacy_ts = trow["top_scorer_player_name"]
        tw = trow["tournament_winner_team_code"]
    picks = parse_top_scorers_from_storage(nj, legacy_ts)

    if prow:
        return PublicParticipantProfileOut(
            user_id=target,
            display_name=prow["display_name"],
            nationality=prow["nationality"],
            profile_picture=prow["profile_picture"],
            updated_at=prow["updated_at"],
            tournament_winner_team_code=tw,
            top_scorers=picks,
            match_predictions_saved=int(mp_count),
        )
    return PublicParticipantProfileOut(
        user_id=target,
        display_name=None,
        nationality=None,
        profile_picture=None,
        updated_at=None,
        tournament_winner_team_code=tw,
        top_scorers=picks,
        match_predictions_saved=int(mp_count),
    )


@router.get("/matches", response_model=list[MatchOut], operation_id="listMatches")
def list_matches(user: UserContext = Depends(get_user_context)):
    uid = user.user_id
    out: list[MatchOut] = []
    with session_scope() as session:
        try_add_matches_goal_events_column(session)
        try_add_matches_winner_team_column(session)
        try_add_match_predictions_advance_column(session)
        gk_expr = "m.group_key" if _matches_table_has_group_key_column(session) else "CAST(NULL AS TEXT)"
        has_ge = matches_table_has_goal_events_column(session)
        ge_sel = "m.goal_events AS goal_events" if has_ge else "NULL AS goal_events"
        has_win = matches_table_has_winner_team_code_column(session)
        winner_sel = "m.winner_team_code" if has_win else "CAST(NULL AS TEXT) AS winner_team_code"
        has_adv = match_predictions_has_advance_team_code_column(session)
        p_adv_sel = "p.advance_team_code AS p_adv" if has_adv else "CAST(NULL AS TEXT) AS p_adv"
        q = text(
            f"""
            SELECT
                m.id, m.external_match_id, m.competition_code, m.stage, m.matchday,
                {gk_expr} AS group_key,
                m.home_team_code, m.away_team_code, m.home_team_name, m.away_team_name,
                m.kickoff_utc, m.status, m.home_score, m.away_score, {winner_sel},
                {ge_sel},
                p.home_goals AS ph, p.away_goals AS pa, {p_adv_sel}
            FROM matches m
            LEFT JOIN match_predictions p ON p.match_id = m.id AND p.user_id = :uid
            ORDER BY m.kickoff_utc ASC, m.stage NULLS LAST
            """
        )
        lock_h = get_settings().prediction_lock_before_kickoff_hours
        tour_picks: list[Any] = []
        tr = session.execute(
            text(
                "SELECT tournament_winner_team_code, top_scorer_player_name, notes_json "
                "FROM tournament_predictions WHERE user_id = :u"
            ),
            {"u": uid},
        ).mappings().first()
        if tr:
            nj = tr["notes_json"] or {}
            if not isinstance(nj, dict):
                nj = {}
            tour_picks = parse_top_scorers_from_storage(nj, tr["top_scorer_player_name"])
        mapping_rows = session.execute(q, {"uid": uid}).mappings().all()

        # Build user-preds dict + all-KO-matches list for round-level advancer scoring
        user_preds_for_adv: dict[str, tuple[int | None, int | None, str | None]] = {}
        all_ko_for_adv: list[dict[str, Any]] = []
        for row in mapping_rows:
            mid = str(row["id"])
            adv_s = _clean_adv(row.get("p_adv"))
            if row["ph"] is not None and row["pa"] is not None:
                user_preds_for_adv[mid] = (row["ph"], row["pa"], adv_s)
            st_upper = (row.get("stage") or "").strip().upper()
            if st_upper and st_upper != "GROUP_STAGE":
                all_ko_for_adv.append({
                    "id": mid,
                    "stage": row["stage"],
                    "home_team_code": str(row["home_team_code"]),
                    "away_team_code": str(row["away_team_code"]),
                    "home_score": row["home_score"],
                    "away_score": row["away_score"],
                    "winner_team_code": row.get("winner_team_code"),
                    "status": row["status"],
                })
        adv_map = compute_round_advancer_points(all_ko_for_adv, user_preds_for_adv)

        for row in mapping_rows:
            now = _now()
            kick = row["kickoff_utc"]
            deadline = _prediction_lock_deadline(kick, lock_h)
            open_pred = (
                row["status"] not in ("FINISHED", "LIVE", "IN_PLAY", "PAUSED", "POSTPONED")
                and now < deadline
            )
            po = pe = ps = 0
            adv_s = _clean_adv(row.get("p_adv"))
            if (
                row["status"] == "FINISHED"
                and row["home_score"] is not None
                and row["away_score"] is not None
                and row["ph"] is not None
                and row["pa"] is not None
            ):
                goals = parse_goal_events(row.get("goal_events"))
                po, pe, ps, _adv = points_for_finished_match(
                    stage=row["stage"],
                    pred_home=row["ph"],
                    pred_away=row["pa"],
                    pred_advance_team_code=adv_s,
                    home_team_code=str(row["home_team_code"]),
                    away_team_code=str(row["away_team_code"]),
                    act_home=int(row["home_score"]),
                    act_away=int(row["away_score"]),
                    act_winner_team_code=row.get("winner_team_code"),
                    goal_events=goals,
                    top_scorer_picks=tour_picks,
                )
            p_adv = adv_map.get(str(row["id"]), 0)
            st = row.get("stage")
            out.append(
                MatchOut(
                    id=row["id"],
                    external_match_id=row["external_match_id"],
                    competition_code=row["competition_code"],
                    stage=st,
                    matchday=row["matchday"],
                    group_key=row["group_key"],
                    home_team_code=row["home_team_code"],
                    away_team_code=row["away_team_code"],
                    home_team_name=row["home_team_name"],
                    away_team_name=row["away_team_name"],
                    kickoff_utc=kick,
                    prediction_deadline_utc=deadline,
                    status=row["status"],
                    home_score=row["home_score"],
                    away_score=row["away_score"],
                    winner_team_code=row.get("winner_team_code"),
                    prediction_open=open_pred,
                    pred_home_goals=row["ph"],
                    pred_away_goals=row["pa"],
                    pred_advance_team_code=adv_s,
                    points_outcome=po,
                    points_exact=pe,
                    points_scorer_goals=ps,
                    points_advancer=p_adv,
                )
            )
    return out


@router.put(
    "/predictions/matches",
    response_model=PutMatchPredictionsOut,
    operation_id="putMatchPredictions",
)
def put_match_predictions(
    body: PutMatchPredictionsIn,
    user: UserContext = Depends(get_user_context),
):
    """Batch validate with one SELECT, then bulk upsert (executemany) to reduce DB round-trips."""
    uid = user.user_id
    preds = body.predictions
    if not preds:
        return PutMatchPredictionsOut(updated=0, errors=[])

    by_mid: dict[str, MatchPredictionIn] = {}
    for pr in preds:
        by_mid[str(pr.match_id)] = pr
    ids = list(by_mid.keys())

    errors: list[MatchPredictionError] = []
    updated = 0
    with session_scope() as session:
        try_add_match_predictions_advance_column(session)
        has_adv_col = match_predictions_has_advance_team_code_column(session)
        stmt = (
            text(
                """
                SELECT id::text AS id, kickoff_utc, status, stage,
                    home_team_code, away_team_code
                FROM matches
                WHERE id IN :ids
                """
            ).bindparams(bindparam("ids", expanding=True))
        )
        rows = session.execute(stmt, {"ids": ids}).mappings().all()
        found = {r["id"]: r for r in rows}

        now = _now()
        lock_h = get_settings().prediction_lock_before_kickoff_hours
        upsert_rows: list[dict[str, object]] = []
        delete_ids: list[str] = []
        for mid, pr in by_mid.items():
            row = found.get(mid)
            if not row:
                errors.append(MatchPredictionError(match_id=UUID(mid), detail="Unknown match"))
                continue
            if row["status"] in ("FINISHED", "LIVE", "IN_PLAY", "PAUSED", "POSTPONED"):
                errors.append(
                    MatchPredictionError(
                        match_id=UUID(mid), detail="Match no longer open for predictions"
                    )
                )
                continue
            deadline = _prediction_lock_deadline(row["kickoff_utc"], lock_h)
            if now >= deadline:
                errors.append(
                    MatchPredictionError(match_id=UUID(mid), detail="Prediction deadline has passed")
                )
                continue
            if pr.home_goals is None and pr.away_goals is None:
                delete_ids.append(mid)
                continue

            h, a = pr.home_goals, pr.away_goals
            assert h is not None and a is not None

            hc = canonical_team_tla(str(row["home_team_code"]))
            ac = canonical_team_tla(str(row["away_team_code"]))
            raw_adv = (pr.advance_team_code or "").strip()
            adv_canon = canonical_team_tla(raw_adv) if raw_adv else None
            # TBD matches have both teams as "?" — accept any non-empty advance code
            # (user picks from projected teams; validated at scoring when teams are known).
            teams_tbd = hc == "?" and ac == "?"
            if has_adv_col and is_knockout_stage(row.get("stage")) and h == a:
                if teams_tbd:
                    # Accept any non-empty advance code for TBD matches
                    if adv_canon is None or adv_canon == "?":
                        errors.append(
                            MatchPredictionError(
                                match_id=UUID(mid),
                                detail="Knockout: predicted draw — pick which team advances (penalties).",
                            )
                        )
                        continue
                elif adv_canon is None or (adv_canon != hc and adv_canon != ac):
                    errors.append(
                        MatchPredictionError(
                            match_id=UUID(mid),
                            detail="Knockout: predicted draw after extra time — pick which team advances (penalties).",
                        )
                    )
                    continue
            else:
                adv_canon = None

            upsert_rows.append(
                {"uid": uid, "mid": mid, "h": h, "a": a, "adv": adv_canon},
            )

        if delete_ids:
            session.execute(
                text(
                    "DELETE FROM match_predictions WHERE user_id = :uid AND match_id IN :mids"
                ).bindparams(bindparam("mids", expanding=True)),
                {"uid": uid, "mids": delete_ids},
            )
        if upsert_rows:
            if has_adv_col:
                ins = text(
                    """
                    INSERT INTO match_predictions (user_id, match_id, home_goals, away_goals, advance_team_code)
                    VALUES (:uid, CAST(:mid AS uuid), :h, :a, :adv)
                    ON CONFLICT (user_id, match_id) DO UPDATE SET
                        home_goals = EXCLUDED.home_goals,
                        away_goals = EXCLUDED.away_goals,
                        advance_team_code = EXCLUDED.advance_team_code,
                        updated_at = now()
                    """
                )
                session.execute(ins, upsert_rows)
            else:
                ins = text(
                    """
                    INSERT INTO match_predictions (user_id, match_id, home_goals, away_goals)
                    VALUES (:uid, CAST(:mid AS uuid), :h, :a)
                    ON CONFLICT (user_id, match_id) DO UPDATE SET
                        home_goals = EXCLUDED.home_goals,
                        away_goals = EXCLUDED.away_goals,
                        updated_at = now()
                    """
                )
                session.execute(
                    ins, [{"uid": r["uid"], "mid": r["mid"], "h": r["h"], "a": r["a"]} for r in upsert_rows]
                )
        updated = len(delete_ids) + len(upsert_rows)

    return PutMatchPredictionsOut(updated=updated, errors=errors)


@router.get(
    "/predictions/tournament",
    response_model=TournamentPredictionsOut,
    operation_id="getTournamentPredictions",
)
def get_tournament_predictions(user: UserContext = Depends(get_user_context)):
    uid = user.user_id
    lock_at = _tournament_picks_lock_at_utc()
    with session_scope() as session:
        open_ed = _tournament_editing_open(session)
        row = session.execute(
            text(
                """
                SELECT tournament_winner_team_code, top_scorer_player_name, notes_json
                FROM tournament_predictions WHERE user_id = :u
                """
            ),
            {"u": uid},
        ).mappings().first()
        if not row:
            return TournamentPredictionsOut(
                tournament_winner_team_code=None,
                top_scorer_player_name=None,
                top_scorers=[],
                notes_json={},
                tournament_open=open_ed,
                tournament_picks_lock_at_utc=lock_at,
                tournament_lock_hours_before_first_kickoff=_tournament_lock_hours_before_first_kickoff(),
                points_tournament_winner=0,
            )
        nj = row["notes_json"] or {}
        if not isinstance(nj, dict):
            nj = {}
        picks = parse_top_scorers_from_storage(nj, row["top_scorer_player_name"])
        pw, enriched = awarded_points_for_tournament_picks(
            session, uid, row["tournament_winner_team_code"], picks
        )
        return TournamentPredictionsOut(
            tournament_winner_team_code=row["tournament_winner_team_code"],
            top_scorer_player_name=row["top_scorer_player_name"],
            top_scorers=enriched,
            notes_json=nj,
            tournament_open=open_ed,
            tournament_picks_lock_at_utc=lock_at,
            tournament_lock_hours_before_first_kickoff=_tournament_lock_hours_before_first_kickoff(),
            points_tournament_winner=pw,
        )


@router.put(
    "/predictions/tournament",
    response_model=TournamentPredictionsOut,
    operation_id="putTournamentPredictions",
)
def put_tournament_predictions(
    body: TournamentPredictionsIn,
    user: UserContext = Depends(get_user_context),
):
    uid = user.user_id
    lock_at = _tournament_picks_lock_at_utc()
    with session_scope() as session:
        if not _tournament_editing_open(session):
            raise HTTPException(status_code=409, detail=_tournament_put_locked_detail())
        prev_row = session.execute(
            text("SELECT notes_json FROM tournament_predictions WHERE user_id = :u"),
            {"u": uid},
        ).mappings().first()
        prev_notes: dict[str, Any] = {}
        if prev_row and isinstance(prev_row.get("notes_json"), dict):
            prev_notes = dict(prev_row["notes_json"])

        merged_notes: dict[str, Any] = {**prev_notes, **(body.notes_json or {})}
        merged_notes.pop("golden_boot", None)
        if len(body.top_scorers) > 0:
            merged_notes["top_scorers"] = [
                {
                    "player_name": p.player_name.strip(),
                    "country_code": (
                        "?"
                        if (p.country_code or "").strip() == "?"
                        else canonical_team_tla(p.country_code)
                    ),
                }
                for p in body.top_scorers
            ]
        elif body.top_scorer_player_name and body.top_scorer_player_name.strip():
            merged_notes["top_scorers"] = [
                {"player_name": body.top_scorer_player_name.strip(), "country_code": "?"}
            ]
        else:
            merged_notes["top_scorers"] = []

        picks = merged_notes.get("top_scorers") or []
        legacy_ts = None
        if isinstance(picks, list) and picks:
            legacy_ts = "; ".join(
                f'{str(p.get("player_name", "")).strip()} ({str(p.get("country_code", "")).strip()})'
                for p in picks
                if isinstance(p, dict) and p.get("player_name")
            )
        elif body.top_scorer_player_name and body.top_scorer_player_name.strip():
            legacy_ts = body.top_scorer_player_name.strip()

        session.execute(
            text(
                """
                INSERT INTO tournament_predictions (user_id, tournament_winner_team_code, top_scorer_player_name, notes_json)
                VALUES (:uid, :w, :ts, CAST(:notes AS jsonb))
                ON CONFLICT (user_id) DO UPDATE SET
                    tournament_winner_team_code = EXCLUDED.tournament_winner_team_code,
                    top_scorer_player_name = EXCLUDED.top_scorer_player_name,
                    notes_json = EXCLUDED.notes_json,
                    updated_at = now()
                """
            ),
            {
                "uid": uid,
                "w": _normalize_tournament_winner_save(body.tournament_winner_team_code),
                "ts": legacy_ts,
                "notes": json.dumps(merged_notes),
            },
        )
        open_ed = _tournament_editing_open(session)
        row = session.execute(
            text(
                "SELECT tournament_winner_team_code, top_scorer_player_name, notes_json "
                "FROM tournament_predictions WHERE user_id = :u"
            ),
            {"u": uid},
        ).mappings().one()
        nj = row["notes_json"] or {}
        if not isinstance(nj, dict):
            nj = {}
        picks = parse_top_scorers_from_storage(nj, row["top_scorer_player_name"])
        pw, enriched = awarded_points_for_tournament_picks(
            session, uid, row["tournament_winner_team_code"], picks
        )
        return TournamentPredictionsOut(
            tournament_winner_team_code=row["tournament_winner_team_code"],
            top_scorer_player_name=row["top_scorer_player_name"],
            top_scorers=enriched,
            notes_json=nj,
            tournament_open=open_ed,
            tournament_picks_lock_at_utc=lock_at,
            tournament_lock_hours_before_first_kickoff=_tournament_lock_hours_before_first_kickoff(),
            points_tournament_winner=pw,
        )


@router.get(
    "/worldcup-players",
    response_model=list[WorldcupPlayerOut],
    operation_id="listWorldcupPlayers",
)
def list_worldcup_players(q: str = "", _user: UserContext = Depends(get_user_context)):
    """Searchable directory of World Cup squad players (football-data when token set, else static fallback)."""
    settings = get_settings()
    comp = (settings.football_data_competition or "WC").strip()
    rows = get_worldcup_player_directory(settings.football_data_token, comp)
    if not (q or "").strip():
        rows = rows[:800]
    else:
        rows = filter_player_directory(rows, q, limit=200)
    return [WorldcupPlayerOut(**r) for r in rows]


@router.get(
    "/group-rosters",
    response_model=GroupRostersResponse,
    operation_id="listGroupRosters",
)
def list_group_rosters(_user: UserContext = Depends(get_user_context)):
    """Group rosters: for competition WC, official 2026 draw (12×4) plus names from synced matches when available."""
    settings = get_settings()
    comp = settings.football_data_competition.strip().upper()
    static = official_wc2026_group_teams() if comp == "WC" else {}

    db_by_gk: dict[str, dict[str, str]] = defaultdict(dict)
    with session_scope() as session:
        if not _matches_table_has_group_key_column(session):
            if static:
                out = [
                    GroupRosterOut(
                        group_key=gk,
                        teams=[GroupRosterTeam(team_code=c, team_name=n) for c, n in static[gk]],
                        is_complete=True,
                    )
                    for gk in _sort_world_cup_group_keys(list(static.keys()))
                ]
                return GroupRostersResponse(groups=out)
            return GroupRostersResponse(groups=[])
        rows = session.execute(
            text(
                """
                WITH t AS (
                    SELECT DISTINCT m.group_key AS gk, m.home_team_code AS code, m.home_team_name AS name
                    FROM matches m
                    WHERE UPPER(COALESCE(m.stage, '')) = 'GROUP_STAGE'
                      AND m.group_key IS NOT NULL
                      AND TRIM(m.group_key) <> ''
                    UNION
                    SELECT DISTINCT m.group_key, m.away_team_code, m.away_team_name
                    FROM matches m
                    WHERE UPPER(COALESCE(m.stage, '')) = 'GROUP_STAGE'
                      AND m.group_key IS NOT NULL
                      AND TRIM(m.group_key) <> ''
                )
                SELECT gk, code, name FROM t
                ORDER BY gk, name
                """
            )
        ).all()
    for gk, code, name in rows:
        if not gk or not code:
            continue
        key = str(gk).strip().upper()
        code_u = canonical_team_tla(str(code))
        db_by_gk[key][code_u] = str(name or code_u)

    out: list[GroupRosterOut] = []
    static_keys = set(static.keys())

    for gk in _sort_world_cup_group_keys(list(static_keys)):
        teams_out: list[GroupRosterTeam] = []
        for code, default_name in static[gk]:
            code_u = code.strip().upper()
            display = db_by_gk.get(gk, {}).get(code_u, default_name)
            teams_out.append(GroupRosterTeam(team_code=code_u, team_name=display))
        out.append(GroupRosterOut(group_key=gk, teams=teams_out, is_complete=True))

    for gk in _sort_world_cup_group_keys([k for k in db_by_gk if k not in static_keys]):
        d = db_by_gk[gk]
        teams = [
            GroupRosterTeam(team_code=c, team_name=n) for c, n in sorted(d.items(), key=lambda x: (x[1], x[0]))
        ]
        out.append(GroupRosterOut(group_key=gk, teams=teams, is_complete=len(teams) == 4))

    return GroupRostersResponse(groups=out)


@router.get("/teams", response_model=list[TeamOptionOut], operation_id="listTeams")
def list_teams(_user: UserContext = Depends(get_user_context)):
    q = text(
        """
        SELECT DISTINCT home_team_code AS code, home_team_name AS name FROM matches
        UNION
        SELECT DISTINCT away_team_code, away_team_name FROM matches
        ORDER BY name
        """
    )
    with session_scope() as session:
        rows = session.execute(q).all()
    return [TeamOptionOut(code=r[0], name=r[1]) for r in rows if r[0]]


@router.post("/admin/sync-matches", response_model=SyncOut, operation_id="adminSyncMatches")
def admin_sync_matches(user: UserContext = Depends(get_user_context)):
    if not is_admin(user):
        raise HTTPException(status_code=403, detail="Admin only")
    try:
        n = run_sync()
    except ValueError as e:
        raise HTTPException(
            status_code=503,
            detail=f"Sync not configured: {e}. Set FOOTBALL_DATA_TOKEN on the Databricks App (same as the sync job secret).",
        ) from e
    return SyncOut(matches_synced=n)


@router.post("/dev/seed-demo-matches", response_model=SyncOut, operation_id="devSeedDemoMatches")
def dev_seed_demo(user: UserContext = Depends(get_user_context)):
    """Insert a few fake matches when API token is missing (local dev only)."""
    from worldcup_pool.backend.config import get_settings

    if get_settings().football_data_token:
        raise HTTPException(status_code=400, detail="Disabled when FOOTBALL_DATA_TOKEN is set")
    now = _now()
    demo = [
        (
            "demo-1",
            "WC",
            "GROUP_STAGE",
            1,
            "GROUP_A",
            "MEX",
            "KOR",
            "Mexico",
            "South Korea",
            now.replace(hour=12, minute=0, second=0, microsecond=0, tzinfo=timezone.utc),
            "SCHEDULED",
        ),
        (
            "demo-2",
            "WC",
            "GROUP_STAGE",
            1,
            "GROUP_B",
            "ENG",
            "RSA",
            "England",
            "South Africa",
            now.replace(hour=18, minute=0, second=0, microsecond=0, tzinfo=timezone.utc),
            "SCHEDULED",
        ),
    ]
    lock_h = get_settings().prediction_lock_before_kickoff_hours
    with session_scope() as session:
        has_gk = _matches_table_has_group_key_column(session)
        for d in demo:
            ext, comp, stage, md, gk, hc, ac, hn, an, kick, st = d
            deadline = kick - timedelta(hours=lock_h)
            if has_gk:
                session.execute(
                    text(
                        """
                        INSERT INTO matches (
                            external_match_id, competition_code, stage, matchday, group_key,
                            home_team_code, away_team_code, home_team_name, away_team_name,
                            kickoff_utc, prediction_deadline_utc, status, last_synced_at
                        ) VALUES (
                            :e, :c, :stg, :md, :gk, :hc, :ac, :hn, :an, :k, :dl, :st, now()
                        )
                        ON CONFLICT (external_match_id) DO NOTHING
                        """
                    ),
                    {
                        "e": ext,
                        "c": comp,
                        "stg": stage,
                        "md": md,
                        "gk": gk,
                        "hc": hc,
                        "ac": ac,
                        "hn": hn,
                        "an": an,
                        "k": kick,
                        "dl": deadline,
                        "st": st,
                    },
                )
            else:
                session.execute(
                    text(
                        """
                        INSERT INTO matches (
                            external_match_id, competition_code, stage, matchday,
                            home_team_code, away_team_code, home_team_name, away_team_name,
                            kickoff_utc, prediction_deadline_utc, status, last_synced_at
                        ) VALUES (
                            :e, :c, :stg, :md, :hc, :ac, :hn, :an, :k, :dl, :st, now()
                        )
                        ON CONFLICT (external_match_id) DO NOTHING
                        """
                    ),
                    {
                        "e": ext,
                        "c": comp,
                        "stg": stage,
                        "md": md,
                        "hc": hc,
                        "ac": ac,
                        "hn": hn,
                        "an": an,
                        "k": kick,
                        "dl": deadline,
                        "st": st,
                    },
                )
    return SyncOut(matches_synced=len(demo))
