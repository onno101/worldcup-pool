"""Lakebase Autoscaling: short-lived OAuth credentials + connection helper."""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from typing import Any, Generator

from databricks.sdk import WorkspaceClient
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from worldcup_pool.backend.config import get_settings

_CRED_LOCK = threading.Lock()
_SM_LOCK = threading.Lock()
_cached: dict[str, Any] = {
    "engine": None,
    "token_expires_at": 0.0,
    "endpoint": None,
}
# Sessionmaker bound to the current engine (rebuilt when engine is disposed).
_session_factory: sessionmaker | None = None
_session_factory_engine_id: int | None = None


def _pool_kwargs() -> dict[str, Any]:
    s = get_settings()
    return {
        "pool_pre_ping": True,
        "pool_size": s.db_pool_size,
        "max_overflow": s.db_max_overflow,
        "pool_timeout": s.db_pool_timeout,
        "pool_recycle": s.db_pool_recycle,
        "pool_use_lifo": True,
    }


def _refresh_engine_if_needed() -> Engine:
    global _session_factory, _session_factory_engine_id

    settings = get_settings()
    if settings.database_url_override:
        with _CRED_LOCK:
            if _cached.get("override_engine") is None:
                _cached["override_engine"] = create_engine(
                    settings.database_url_override,
                    **_pool_kwargs(),
                )
            return _cached["override_engine"]

    if not settings.lakebase_endpoint:
        raise RuntimeError(
            "LAKEBASE_ENDPOINT is not set. Set full endpoint name, e.g. "
            "projects/my-pool/branches/production/endpoints/ep-primary"
        )

    now = time.time()
    with _CRED_LOCK:
        if (
            _cached["engine"] is not None
            and _cached["endpoint"] == settings.lakebase_endpoint
            and now < _cached["token_expires_at"] - 120
        ):
            return _cached["engine"]

        w = WorkspaceClient()
        endpoint = w.postgres.get_endpoint(name=settings.lakebase_endpoint)
        host = endpoint.status.hosts.host
        cred = w.postgres.generate_database_credential(endpoint=settings.lakebase_endpoint)
        user = w.current_user.me().user_name
        password = cred.token
        # Token TTL ~1h; refresh early
        _cached["token_expires_at"] = now + 3300

        from urllib.parse import quote_plus

        url = (
            f"postgresql+psycopg://{quote_plus(user)}:{quote_plus(password)}"
            f"@{host}:5432/{quote_plus(settings.lakebase_database)}"
        )

        if _cached["engine"] is not None:
            _cached["engine"].dispose()

        engine = create_engine(
            url,
            connect_args={"sslmode": "require", "connect_timeout": 10},
            **_pool_kwargs(),
        )
        _cached["engine"] = engine
        _cached["endpoint"] = settings.lakebase_endpoint
        _session_factory = None
        _session_factory_engine_id = None
        return engine


def get_engine() -> Engine:
    try:
        return _refresh_engine_if_needed()
    except Exception:
        with _CRED_LOCK:
            _cached["engine"] = None
            _cached["token_expires_at"] = 0.0
        raise


def _sessionmaker() -> sessionmaker:
    """One sessionmaker per engine instance (avoids rebuilding sessionmaker every request)."""
    global _session_factory, _session_factory_engine_id

    with _SM_LOCK:
        engine = get_engine()
        eid = id(engine)
        if _session_factory is None or _session_factory_engine_id != eid:
            _session_factory = sessionmaker(autocommit=False, autoflush=False, bind=engine)
            _session_factory_engine_id = eid
        return _session_factory


@contextmanager
def session_scope() -> Generator[Session, None, None]:
    SessionLocal = _sessionmaker()
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# Once a column is confirmed present, skip the ALTER on subsequent requests.
_ddl_done: set[str] = set()


def try_add_matches_group_key_column(session: Session) -> None:
    """Idempotent DDL for older DBs; ignore if the app role lacks ALTER on matches."""
    if "matches_group_key" in _ddl_done:
        return
    try:
        session.execute(
            text("ALTER TABLE matches ADD COLUMN IF NOT EXISTS group_key TEXT")
        )
        session.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_matches_stage_group ON matches (stage, group_key)"
            )
        )
        _ddl_done.add("matches_group_key")
    except Exception:
        session.rollback()


def try_add_matches_winner_team_column(session: Session) -> None:
    """Who advances from a knockout tie (incl. penalties); ignore if ALTER is not allowed."""
    if "matches_winner_team_code" in _ddl_done:
        return
    try:
        session.execute(text("ALTER TABLE matches ADD COLUMN IF NOT EXISTS winner_team_code TEXT"))
        _ddl_done.add("matches_winner_team_code")
    except Exception:
        session.rollback()


def try_add_match_predictions_advance_column(session: Session) -> None:
    """Tie-break pick when user predicts a draw after extra time in knockouts."""
    if "match_predictions_advance" in _ddl_done:
        return
    try:
        session.execute(text("ALTER TABLE match_predictions ADD COLUMN IF NOT EXISTS advance_team_code TEXT"))
        _ddl_done.add("match_predictions_advance")
    except Exception:
        session.rollback()


def try_add_matches_goal_events_column(session: Session) -> None:
    """Store per-match goal scorers (JSON array) for ranking; ignore if ALTER is not allowed."""
    if "matches_goal_events" in _ddl_done:
        return
    try:
        session.execute(
            text("ALTER TABLE matches ADD COLUMN IF NOT EXISTS goal_events JSONB")
        )
        _ddl_done.add("matches_goal_events")
    except Exception:
        session.rollback()


def matches_table_has_winner_team_code_column(session: Session) -> bool:
    """True when matches.winner_team_code exists."""
    if "matches_winner_team_code" in _ddl_done:
        return True
    try:
        row = session.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.columns c
                    WHERE c.table_schema = ANY (current_schemas(true))
                      AND c.table_name = 'matches'
                      AND c.column_name = 'winner_team_code'
                )
                """
            )
        ).scalar()
        if row:
            _ddl_done.add("matches_winner_team_code")
        return bool(row)
    except Exception:
        session.rollback()
        return False


def match_predictions_has_advance_team_code_column(session: Session) -> bool:
    """True when match_predictions.advance_team_code exists.

    Prefer information_schema + search_path (current_schemas): some Postgres roles use a
    non-public current_schema() while the app tables live on the path, which made pg_attribute
    + current_schema() falsely report the column missing so advancer picks were never written.
    """
    if "match_predictions_advance" in _ddl_done:
        return True
    try:
        row = session.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.columns c
                    WHERE c.table_schema = ANY (current_schemas(true))
                      AND c.table_name = 'match_predictions'
                      AND c.column_name = 'advance_team_code'
                )
                """
            )
        ).scalar()
        if row:
            _ddl_done.add("match_predictions_advance")
            return True
        # Fallback for non-standard catalogs (unlikely).
        row2 = session.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_attribute a
                    JOIN pg_class c ON a.attrelid = c.oid
                    JOIN pg_namespace n ON c.relnamespace = n.oid
                    WHERE n.nspname = ANY (current_schemas(true))
                      AND c.relname = 'match_predictions'
                      AND a.attname = 'advance_team_code'
                      AND a.attnum > 0
                      AND NOT a.attisdropped
                )
                """
            )
        ).scalar()
        found = bool(row2)
        if found:
            _ddl_done.add("match_predictions_advance")
        return found
    except Exception:
        session.rollback()
        return False


def matches_table_has_goal_events_column(session: Session) -> bool:
    """True when matches.goal_events exists (skip selecting it otherwise — avoids 500 on older DBs)."""
    if "matches_goal_events" in _ddl_done:
        return True
    try:
        row = session.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.columns c
                    WHERE c.table_schema = ANY (current_schemas(true))
                      AND c.table_name = 'matches'
                      AND c.column_name = 'goal_events'
                )
                """
            )
        ).scalar()
        if row:
            _ddl_done.add("matches_goal_events")
        return bool(row)
    except Exception:
        session.rollback()
        return False


def try_ensure_user_profiles_columns(session: Session) -> None:
    """Older pools may lack profile columns; keep reads from failing. Ignores permission errors."""
    if "user_profiles_columns" in _ddl_done:
        return
    for stmt in (
        "ALTER TABLE user_profiles ADD COLUMN IF NOT EXISTS nationality TEXT",
        "ALTER TABLE user_profiles ADD COLUMN IF NOT EXISTS expected_winner_team_code TEXT",
        "ALTER TABLE user_profiles ADD COLUMN IF NOT EXISTS profile_picture TEXT",
    ):
        try:
            session.execute(text(stmt))
        except Exception:
            session.rollback()
    _ddl_done.add("user_profiles_columns")


def init_schema() -> None:
    """Create tables if they do not exist."""
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
    CREATE INDEX IF NOT EXISTS idx_match_predictions_user ON match_predictions (user_id);
    CREATE INDEX IF NOT EXISTS idx_match_predictions_match_user
        ON match_predictions (match_id, user_id);

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
    ALTER TABLE user_profiles ADD COLUMN IF NOT EXISTS nationality TEXT;
    ALTER TABLE user_profiles ADD COLUMN IF NOT EXISTS expected_winner_team_code TEXT;
    ALTER TABLE user_profiles ADD COLUMN IF NOT EXISTS profile_picture TEXT;
    """
    engine = get_engine()
    with engine.connect() as conn:
        for stmt in ddl.split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))
        conn.commit()
