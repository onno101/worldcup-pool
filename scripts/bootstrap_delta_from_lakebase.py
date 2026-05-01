#!/usr/bin/env python3
"""
One-shot Lakebase → Unity Catalog Delta bootstrap from the developer's laptop.

Use this when the scheduled `lakebase_to_delta` job hasn't produced tables yet
(e.g. right before a demo). Reads the pool tables from Lakebase locally, writes
parquet to /tmp, uploads to a staging volume, and runs CREATE OR REPLACE TABLE
against a SQL warehouse. Fast — no cluster boot required.

Requires local databricks auth + LAKEBASE_ENDPOINT (from worldcup-pool/.env).
"""

from __future__ import annotations

import os
import sys
import tempfile
import time
from pathlib import Path

import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeType
from databricks.sdk.service.sql import StatementState
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[0].parent))
from worldcup_pool.backend.db import session_scope  # noqa: E402


CATALOG = os.environ.get("DEMO_CATALOG", "main")
SCHEMA = os.environ.get("DEMO_SCHEMA", "worldcup_pool")
VOLUME = os.environ.get("DEMO_VOLUME", "_staging")
WAREHOUSE_ID = os.environ.get("DASHBOARD_WAREHOUSE_ID", "")
if not WAREHOUSE_ID:
    raise SystemExit("Set DASHBOARD_WAREHOUSE_ID env var (your SQL warehouse ID)")

_TABLES: list[tuple[str, str]] = [
    (
        "matches",
        """
        SELECT id::text AS id, external_match_id, competition_code, stage, matchday,
               group_key, home_team_code, away_team_code, home_team_name, away_team_name,
               kickoff_utc, prediction_deadline_utc, status, home_score, away_score,
               winner_team_code, goal_events::text AS goal_events_json, last_synced_at
        FROM matches
        """,
    ),
    (
        "match_predictions",
        """
        SELECT id::text AS id, user_id, match_id::text AS match_id,
               home_goals, away_goals, advance_team_code, created_at, updated_at
        FROM match_predictions
        """,
    ),
    (
        "tournament_predictions",
        """
        SELECT id::text AS id, user_id, tournament_winner_team_code,
               top_scorer_player_name, notes_json::text AS notes_json,
               created_at, updated_at
        FROM tournament_predictions
        """,
    ),
    (
        "user_profiles_public",
        """
        SELECT user_id, display_name, nationality, expected_winner_team_code,
               created_at, updated_at
        FROM user_profiles
        """,
    ),
]


def _sql(w: WorkspaceClient, stmt: str) -> None:
    r = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=stmt,
        wait_timeout="50s",
    )
    while r.status and r.status.state in (
        StatementState.PENDING,
        StatementState.RUNNING,
    ):
        time.sleep(1)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status and r.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"SQL failed: {stmt[:80]!r}: {r.status.error}")


def main() -> None:
    w = WorkspaceClient()

    print(f"Ensuring {CATALOG}.{SCHEMA} + volume {VOLUME}…")
    _sql(w, f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    _sql(w, f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

    vol_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

    with tempfile.TemporaryDirectory() as tmp:
        for table, projection in _TABLES:
            with session_scope() as session:
                df = pd.read_sql_query(text(projection), session.connection())
            local_path = Path(tmp) / f"{table}.parquet"
            df.to_parquet(local_path, index=False)
            print(f"  {table}: {len(df)} rows → {local_path.name}")

            remote = f"{vol_path}/{table}.parquet"
            with open(local_path, "rb") as fh:
                w.files.upload(remote, fh, overwrite=True)
            print(f"    uploaded to {remote}")

            _sql(
                w,
                f"""
                CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.{table} AS
                SELECT * FROM read_files('{remote}', format => 'parquet')
                """,
            )
            print(f"    wrote {CATALOG}.{SCHEMA}.{table}")

    print("\nAll four tables are live in Unity Catalog.")


if __name__ == "__main__":
    main()
