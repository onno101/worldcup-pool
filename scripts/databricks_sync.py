#!/usr/bin/env python3
"""
Databricks Job entry: sync fixtures/results into Lakebase Autoscaling.

Self-contained (uses only job `libraries` PyPI deps + this file) so spark_python_task
does not require a wheel of the app package on the driver PYTHONPATH.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path


def _resolve_repo_root() -> Path:
    # Databricks `spark_python_task` runs this file via `exec(compile(...))`, which
    # does NOT set `__file__` — so the obvious `Path(__file__).parents[1]` blows up
    # on the cluster even though it works locally. Fall back to sys.argv[0] (the
    # script path the runtime invoked) and finally cwd so the import below always
    # resolves `worldcup_pool/…`.
    candidate = globals().get("__file__")
    if not candidate and sys.argv and sys.argv[0]:
        candidate = sys.argv[0]
    if candidate:
        try:
            return Path(candidate).resolve().parents[1]
        except Exception:
            pass
    return Path.cwd()


_REPO_ROOT = _resolve_repo_root()
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import json
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote_plus

import httpx
from databricks.sdk import WorkspaceClient
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

_CRED_LOCK = threading.Lock()
_engine_cache: dict[str, Any] = {"engine": None, "exp": 0.0, "endpoint": None}


def _get_engine():
    override = os.environ.get("DATABASE_URL_OVERRIDE", "").strip()
    if override:
        return create_engine(override, pool_pre_ping=True)

    endpoint = os.environ.get("LAKEBASE_ENDPOINT", "").strip()
    if not endpoint:
        raise RuntimeError("LAKEBASE_ENDPOINT or DATABASE_URL_OVERRIDE required")

    dbname = os.environ.get("LAKEBASE_DATABASE", "databricks_postgres")
    now = time.time()
    with _CRED_LOCK:
        if _engine_cache["engine"] and now < _engine_cache["exp"] - 120 and _engine_cache["endpoint"] == endpoint:
            return _engine_cache["engine"]

        w = WorkspaceClient()
        ep = w.postgres.get_endpoint(name=endpoint)
        host = ep.status.hosts.host
        cred = w.postgres.generate_database_credential(endpoint=endpoint)
        user = w.current_user.me().user_name
        password = cred.token
        _engine_cache["exp"] = now + 3300
        url = (
            f"postgresql+psycopg://{quote_plus(user)}:{quote_plus(password)}"
            f"@{host}:5432/{quote_plus(dbname)}"
        )
        if _engine_cache["engine"] is not None:
            _engine_cache["engine"].dispose()
        eng = create_engine(url, pool_pre_ping=True, connect_args={"sslmode": "require"})
        _engine_cache["engine"] = eng
        _engine_cache["endpoint"] = endpoint
        return eng


def _init_schema(engine) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS matches (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        external_match_id TEXT NOT NULL UNIQUE,
        competition_code TEXT NOT NULL DEFAULT 'WC',
        stage TEXT,
        matchday INT,
        group_key TEXT,
        home_team_code TEXT NOT NULL,
        away_team_code TEXT NOT NULL,
        home_team_name TEXT NOT NULL,
        away_team_name TEXT NOT NULL,
        kickoff_utc TIMESTAMPTZ NOT NULL,
        prediction_deadline_utc TIMESTAMPTZ NOT NULL,
        status TEXT NOT NULL DEFAULT 'SCHEDULED',
        home_score INT,
        away_score INT,
        last_synced_at TIMESTAMPTZ
    );
    CREATE INDEX IF NOT EXISTS idx_matches_kickoff ON matches (kickoff_utc);
    CREATE INDEX IF NOT EXISTS idx_matches_deadline ON matches (prediction_deadline_utc);
    ALTER TABLE matches ADD COLUMN IF NOT EXISTS group_key TEXT;
    CREATE INDEX IF NOT EXISTS idx_matches_stage_group ON matches (stage, group_key);
    ALTER TABLE matches ADD COLUMN IF NOT EXISTS goal_events JSONB;
    ALTER TABLE matches ADD COLUMN IF NOT EXISTS winner_team_code TEXT;
    CREATE TABLE IF NOT EXISTS match_predictions (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id TEXT NOT NULL,
        match_id UUID NOT NULL REFERENCES matches(id) ON DELETE CASCADE,
        home_goals INT,
        away_goals INT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE (user_id, match_id)
    );
    ALTER TABLE match_predictions ALTER COLUMN home_goals DROP NOT NULL;
    ALTER TABLE match_predictions ALTER COLUMN away_goals DROP NOT NULL;
    ALTER TABLE match_predictions ADD COLUMN IF NOT EXISTS advance_team_code TEXT;
    CREATE TABLE IF NOT EXISTS tournament_predictions (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id TEXT NOT NULL UNIQUE,
        tournament_winner_team_code TEXT,
        top_scorer_player_name TEXT,
        notes_json JSONB NOT NULL DEFAULT '{}',
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS app_users_cache (
        user_id TEXT PRIMARY KEY,
        email TEXT,
        display_name TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS user_profiles (
        user_id TEXT PRIMARY KEY,
        display_name TEXT,
        nationality TEXT,
        expected_winner_team_code TEXT,
        profile_picture TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """
    with engine.connect() as conn:
        for stmt in ddl.split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))
        conn.commit()


def _fetch_matches(token: str, competition: str) -> list[dict[str, Any]]:
    from worldcup_pool.football_data import normalize_match

    base = "https://api.football-data.org/v4"
    out: list[dict[str, Any]] = []
    with httpx.Client(timeout=120.0) as client:
        r = client.get(
            f"{base}/competitions/{competition}/matches",
            headers={"X-Auth-Token": token},
            params={"limit": 999},
        )
        r.raise_for_status()
        data = r.json()
        lock_h = int(os.environ.get("PREDICTION_LOCK_BEFORE_KICKOFF_HOURS", "1").strip() or "1")
        lock_h = max(1, min(168, lock_h))
        for m in data.get("matches") or []:
            nm = normalize_match(m, competition)
            kickoff = nm.kickoff_utc
            out.append(
                {
                    "external_match_id": nm.external_match_id,
                    "competition_code": nm.competition_code,
                    "stage": nm.stage,
                    "matchday": nm.matchday,
                    "group_key": nm.group_key,
                    "home_team_code": nm.home_team_code,
                    "away_team_code": nm.away_team_code,
                    "home_team_name": nm.home_team_name,
                    "away_team_name": nm.away_team_name,
                    "kickoff_utc": kickoff,
                    "prediction_deadline_utc": kickoff - timedelta(hours=lock_h),
                    "status": nm.status,
                    "home_score": nm.home_score,
                    "away_score": nm.away_score,
                    "winner_team_code": nm.winner_team_code,
                    "goal_events": json.dumps(nm.goal_events or []),
                }
            )
    return out


def main() -> None:
    token = os.environ.get("FOOTBALL_DATA_TOKEN", "").strip()
    if not token:
        raise SystemExit("FOOTBALL_DATA_TOKEN is required for sync job")
    comp = os.environ.get("FOOTBALL_DATA_COMPETITION", "WC").strip()

    engine = _get_engine()
    _init_schema(engine)
    rows = _fetch_matches(token, comp)
    now = datetime.now(timezone.utc)

    upsert = text(
        """
        INSERT INTO matches (
            external_match_id, competition_code, stage, matchday, group_key,
            home_team_code, away_team_code, home_team_name, away_team_name,
            kickoff_utc, prediction_deadline_utc, status, home_score, away_score,
            winner_team_code, goal_events, last_synced_at
        ) VALUES (
            :external_match_id, :competition_code, :stage, :matchday, :group_key,
            :home_team_code, :away_team_code, :home_team_name, :away_team_name,
            :kickoff_utc, :prediction_deadline_utc, :status, :home_score, :away_score,
            :winner_team_code, CAST(:goal_events AS jsonb), :last_synced_at
        )
        ON CONFLICT (external_match_id) DO UPDATE SET
            stage = EXCLUDED.stage,
            matchday = EXCLUDED.matchday,
            group_key = EXCLUDED.group_key,
            home_team_code = EXCLUDED.home_team_code,
            away_team_code = EXCLUDED.away_team_code,
            home_team_name = EXCLUDED.home_team_name,
            away_team_name = EXCLUDED.away_team_name,
            kickoff_utc = EXCLUDED.kickoff_utc,
            prediction_deadline_utc = EXCLUDED.prediction_deadline_utc,
            status = EXCLUDED.status,
            home_score = EXCLUDED.home_score,
            away_score = EXCLUDED.away_score,
            winner_team_code = EXCLUDED.winner_team_code,
            goal_events = EXCLUDED.goal_events,
            last_synced_at = EXCLUDED.last_synced_at
        """
    )

    with engine.begin() as conn:
        for row in rows:
            conn.execute(
                upsert,
                {**row, "last_synced_at": now},
            )

    logger.info("Synced %s matches", len(rows))


if __name__ == "__main__":
    main()
