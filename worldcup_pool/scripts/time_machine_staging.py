#!/usr/bin/env python3
"""
Time-machine playbook for staging / disposable Postgres.

Seeds isolated rows (external_match_id / user_id prefix `tm-staging-`), then:
  - one FINISHED group match with scores + goal_events,
  - one soonest future "opening" match so min(kickoff) keeps tournament picks OPEN until lock window
    (finished fixture uses a later kickoff; lock is N hours before earliest kickoff),
  - a synthetic user with exact scoreline + top-scorer pick matching the goal event,
  - runs compute_leaderboard and prints assertions.

Usage (recommended: empty local Postgres):
  export DATABASE_URL_OVERRIDE='postgresql+psycopg://USER:PASS@HOST:5432/DB'
  python -m worldcup_pool.scripts.time_machine_staging --apply

Or pass URL without persisting in shell history:
  python -m worldcup_pool.scripts.time_machine_staging --apply --postgres-url 'postgresql+psycopg://...'

Safety: refuses --apply if neither DATABASE_URL_OVERRIDE nor --postgres-url is set.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from uuid import UUID

from sqlalchemy import text

PREFIX = "tm-staging-"
USER_ID = f"{PREFIX}player1"


def _require_db_url(args: argparse.Namespace) -> None:
    if args.postgres_url:
        os.environ["DATABASE_URL_OVERRIDE"] = args.postgres_url.strip()
    if not (os.environ.get("DATABASE_URL_OVERRIDE") or "").strip():
        print(
            "ERROR: Set DATABASE_URL_OVERRIDE or pass --postgres-url (staging / disposable DB only).",
            file=sys.stderr,
        )
        sys.exit(2)


def _reset_tm_rows(session) -> None:
    session.execute(text("DELETE FROM match_predictions WHERE user_id LIKE :p"), {"p": f"{PREFIX}%"})
    session.execute(text("DELETE FROM tournament_predictions WHERE user_id LIKE :p"), {"p": f"{PREFIX}%"})
    session.execute(text("DELETE FROM user_profiles WHERE user_id LIKE :p"), {"p": f"{PREFIX}%"})
    session.execute(text("DELETE FROM app_users_cache WHERE user_id LIKE :p"), {"p": f"{PREFIX}%"})
    session.execute(text("DELETE FROM matches WHERE external_match_id LIKE :p"), {"p": f"{PREFIX}%"})


def _insert_match(
    session,
    *,
    external_id: str,
    kickoff,
    status: str,
    home_score: int | None,
    away_score: int | None,
    goal_events: list | None,
) -> UUID:
    deadline = kickoff - timedelta(hours=1)
    ge_sql = "CAST(:goal AS jsonb)" if goal_events is not None else "NULL"
    params = {
        "e": external_id,
        "k": kickoff,
        "dl": deadline,
        "st": status,
        "hs": home_score,
        "as": away_score,
    }
    if goal_events is not None:
        params["goal"] = json.dumps(goal_events)
    q = text(
        f"""
        INSERT INTO matches (
            external_match_id, competition_code, stage, matchday, group_key,
            home_team_code, away_team_code, home_team_name, away_team_name,
            kickoff_utc, prediction_deadline_utc, status, home_score, away_score,
            goal_events, last_synced_at
        ) VALUES (
            :e, 'WC', 'GROUP_STAGE', 1, 'GROUP_Z',
            'MEX', 'KOR', 'Mexico', 'South Korea',
            :k, :dl, :st, :hs, :as,
            {ge_sql},
            now()
        )
        RETURNING id
        """
    )
    row = session.execute(q, params).one()
    return row[0]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--postgres-url",
        help="postgresql+psycopg://... (written to DATABASE_URL_OVERRIDE for this process)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Run DDL seed + assertions (otherwise print plan and exit 0)",
    )
    args = parser.parse_args()

    if not args.apply:
        print(__doc__)
        print("\nDry run: pass --apply (plus DATABASE_URL_OVERRIDE or --postgres-url) to execute.")
        return

    _require_db_url(args)

    from worldcup_pool.backend.config import get_settings

    get_settings.cache_clear()

    settings = get_settings()
    if not settings.database_url_override:
        print("ERROR: database_url_override empty after env setup.", file=sys.stderr)
        sys.exit(2)

    from worldcup_pool.backend.db import init_schema, session_scope
    from worldcup_pool.backend.routes import _tournament_editing_open
    from worldcup_pool.scoring import awarded_points_for_tournament_picks, compute_leaderboard
    from worldcup_pool.tournament_picks import parse_top_scorers_from_storage

    init_schema()

    now = datetime.now(timezone.utc)
    # Earliest kickoff must stay far enough in the future that (kickoff - lock_hours) is still in the future.
    # The "finished" match can still be FINISHED with scores for scoring tests; kickoff is only a schedule field here.
    opening_kick = now + timedelta(days=1)
    finished_kick = now + timedelta(days=5)

    goal_events = [{"player_name": "Heung-Min Son", "team_code": "KOR"}]

    with session_scope() as session:
        _reset_tm_rows(session)

        opening_id = _insert_match(
            session,
            external_id=f"{PREFIX}opening",
            kickoff=opening_kick,
            status="SCHEDULED",
            home_score=None,
            away_score=None,
            goal_events=None,
        )
        finished_id = _insert_match(
            session,
            external_id=f"{PREFIX}finished",
            kickoff=finished_kick,
            status="FINISHED",
            home_score=2,
            away_score=1,
            goal_events=goal_events,
        )

        session.execute(
            text(
                """
                INSERT INTO user_profiles (user_id, display_name, nationality, profile_picture)
                VALUES (:u, 'Time Machine', 'Test', NULL)
                ON CONFLICT (user_id) DO UPDATE SET display_name = EXCLUDED.display_name
                """
            ),
            {"u": USER_ID},
        )
        session.execute(
            text(
                """
                INSERT INTO app_users_cache (user_id, email, display_name)
                VALUES (:u, 'time.machine@example.invalid', 'Time Machine')
                ON CONFLICT (user_id) DO UPDATE SET display_name = EXCLUDED.display_name
                """
            ),
            {"u": USER_ID},
        )

        session.execute(
            text(
                """
                INSERT INTO match_predictions (user_id, match_id, home_goals, away_goals)
                VALUES (:u, :m, 2, 1)
                ON CONFLICT (user_id, match_id) DO UPDATE SET home_goals = EXCLUDED.home_goals, away_goals = EXCLUDED.away_goals
                """
            ),
            {"u": USER_ID, "m": finished_id},
        )

        notes = {
            "top_scorers": [
                {"player_name": "Heung-Min Son", "country_code": "KOR", "country_name": "South Korea"},
            ]
        }
        session.execute(
            text(
                """
                INSERT INTO tournament_predictions (user_id, tournament_winner_team_code, top_scorer_player_name, notes_json)
                VALUES (:u, 'MEX', :ts, CAST(:notes AS jsonb))
                ON CONFLICT (user_id) DO UPDATE SET
                    tournament_winner_team_code = EXCLUDED.tournament_winner_team_code,
                    top_scorer_player_name = EXCLUDED.top_scorer_player_name,
                    notes_json = EXCLUDED.notes_json,
                    updated_at = now()
                """
            ),
            {
                "u": USER_ID,
                "ts": "Heung-Min Son (KOR)",
                "notes": json.dumps(notes),
            },
        )

    with session_scope() as session:
        tour_open = _tournament_editing_open(session)
        board = compute_leaderboard(session)
        row = next((e for e in board if e.user_id == USER_ID), None)

        trow = session.execute(
            text("SELECT tournament_winner_team_code, top_scorer_player_name, notes_json FROM tournament_predictions WHERE user_id = :u"),
            {"u": USER_ID},
        ).mappings().one()
        nj = trow["notes_json"] if isinstance(trow["notes_json"], dict) else {}
        picks = parse_top_scorers_from_storage(nj, trow["top_scorer_player_name"])
        pw, enriched = awarded_points_for_tournament_picks(
            session, USER_ID, trow["tournament_winner_team_code"], picks
        )

    print("=== Time machine staging check ===")
    print(f"tm-staging opening match id: {opening_id}")
    print(f"tm-staging finished match id: {finished_id}")
    print(f"Tournament picks editing open (API semantics): {tour_open}")
    print(f"Leaderboard rows: {len(board)}")
    if row is None:
        print("FAIL: synthetic user missing from leaderboard", file=sys.stderr)
        sys.exit(1)
    print(
        f"User {USER_ID}: total={row.total_points} exact={row.points_exact} "
        f"outcome={row.points_outcome} scorers={row.points_scorer_goals} tw={row.points_tournament_winner}"
    )
    print(f"Tournament tab points_tournament_winner (no final yet): {pw}")
    print(f"Top scorer pick points_awarded: {[p.model_dump() for p in enriched]}")

    assert row.points_exact == 5, f"expected 5 exact pts (group stage), got {row.points_exact}"
    assert row.points_scorer_goals == 2, f"expected 2 scorer pts for Son, got {row.points_scorer_goals}"
    assert row.points_outcome == 0
    assert tour_open is True, (
        "tournament should still be open before the lock window (N hours before earliest kickoff); "
        "if this failed, your DB likely has other matches with earlier kickoffs — use a disposable DB."
    )
    assert len(enriched) == 1 and (enriched[0].points_awarded or 0) == 2

    print("\nOK — time machine assertions passed.")


if __name__ == "__main__":
    main()
