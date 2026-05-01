#!/usr/bin/env python3
"""
Databricks Job entry: Lakebase (Postgres) → Unity Catalog Delta sync.

Copies the pool's four OLTP tables from Lakebase Autoscale into Delta tables in
Unity Catalog so AI/BI dashboards, Genie, and any SQL consumer can query live
pool data without federating every request back to the OLTP instance.

Why not a native synced table: Lakebase Autoscale today only supports
Delta→Postgres sync, not Postgres→Delta. Running a small per-minute batch with
`overwriteSchema=true` gives us a materialized mirror with ~1 schedule-interval
freshness — good enough for dashboards and Genie; no schema drift to chase.

Privacy note: `user_profiles.profile_picture` (a base64 data URI) is **not**
copied. Only `user_id`, display name, nationality, and their timestamps land in
UC so the demo target stays small and dashboard-friendly.

Self-contained on purpose — `spark_python_task` does not need a wheel of the
`worldcup_pool` package on the driver PYTHONPATH to run this.
"""

from __future__ import annotations

import logging
import os
import sys
import threading
import time
from typing import Any
from urllib.parse import quote_plus

import pandas as pd
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

_CRED_LOCK = threading.Lock()
_engine_cache: dict[str, Any] = {"engine": None, "exp": 0.0, "endpoint": None}


def _get_engine():
    """Build a SQLAlchemy engine using the same OAuth pattern as the app.

    Token lives ~1h; we refresh early (3300s cushion) to keep long-running jobs
    safe. `DATABASE_URL_OVERRIDE` bypasses Lakebase for local dev.
    """
    override = os.environ.get("DATABASE_URL_OVERRIDE", "").strip()
    if override:
        return create_engine(override, pool_pre_ping=True)

    endpoint = os.environ.get("LAKEBASE_ENDPOINT", "").strip()
    if not endpoint:
        raise RuntimeError("LAKEBASE_ENDPOINT or DATABASE_URL_OVERRIDE required")

    dbname = os.environ.get("LAKEBASE_DATABASE", "databricks_postgres")
    now = time.time()
    with _CRED_LOCK:
        if (
            _engine_cache["engine"]
            and now < _engine_cache["exp"] - 120
            and _engine_cache["endpoint"] == endpoint
        ):
            return _engine_cache["engine"]

        w = WorkspaceClient()
        ep = w.postgres.get_endpoint(name=endpoint)
        host = ep.status.hosts.host
        cred = w.postgres.generate_database_credential(endpoint=endpoint)
        user = w.current_user.me().user_name
        password = cred.token

        url = (
            f"postgresql+psycopg://{quote_plus(user)}:{quote_plus(password)}"
            f"@{host}:5432/{quote_plus(dbname)}"
        )
        if _engine_cache["engine"] is not None:
            _engine_cache["engine"].dispose()
        eng = create_engine(
            url,
            pool_pre_ping=True,
            connect_args={"sslmode": "require", "connect_timeout": 10},
        )
        _engine_cache["engine"] = eng
        _engine_cache["exp"] = now + 3300
        _engine_cache["endpoint"] = endpoint
        return eng


# (target_name, projection_sql) — source of truth for what ends up in UC.
# `::text` casts on UUID/JSONB columns keep the Delta schema stable across
# Spark versions and let downstream SQL search them as strings. Explicit column
# lists are the PII barrier: profile_picture MUST stay out.
_TABLES: list[tuple[str, str]] = [
    (
        "matches",
        """
        SELECT
            id::text AS id,
            external_match_id,
            competition_code,
            stage,
            matchday,
            group_key,
            home_team_code,
            away_team_code,
            home_team_name,
            away_team_name,
            kickoff_utc,
            prediction_deadline_utc,
            status,
            home_score,
            away_score,
            winner_team_code,
            goal_events::text AS goal_events_json,
            last_synced_at
        FROM matches
        """,
    ),
    (
        "match_predictions",
        """
        SELECT
            id::text AS id,
            user_id,
            match_id::text AS match_id,
            home_goals,
            away_goals,
            advance_team_code,
            created_at,
            updated_at
        FROM match_predictions
        """,
    ),
    (
        "tournament_predictions",
        """
        SELECT
            id::text AS id,
            user_id,
            tournament_winner_team_code,
            top_scorer_player_name,
            notes_json::text AS notes_json,
            created_at,
            updated_at
        FROM tournament_predictions
        """,
    ),
    (
        "user_profiles_public",
        """
        SELECT
            user_id,
            display_name,
            nationality,
            expected_winner_team_code,
            created_at,
            updated_at
        FROM user_profiles
        """,
    ),
]


def _copy_table(
    spark: SparkSession,
    engine,
    source: str,
    projection_sql: str,
    target: str,
) -> int:
    with engine.connect() as conn:
        # SQLAlchemy 2.x refuses to execute bare strings — wrap in text() so
        # pandas.read_sql_query gets an executable clause on both SA 1.x and 2.x.
        df = pd.read_sql_query(text(projection_sql), conn)

    if df.empty:
        # Preserve the declared column set when the source table is empty so the
        # dashboard's SQL keeps resolving columns instead of erroring on a
        # missing Delta table.
        spark_df = spark.createDataFrame(df.astype("object"))
    else:
        spark_df = spark.createDataFrame(df)

    (
        spark_df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target)
    )
    logger.info("Copied %s rows from lakebase.%s → %s", len(df), source, target)
    return len(df)


def main() -> None:
    catalog = (os.environ.get("DEMO_CATALOG") or "main").strip() or "main"
    schema = (os.environ.get("DEMO_SCHEMA") or "worldcup_pool").strip() or "worldcup_pool"

    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    engine = _get_engine()

    total = 0
    failures: list[str] = []
    for table, projection in _TABLES:
        target = f"{catalog}.{schema}.{table}"
        try:
            total += _copy_table(spark, engine, table, projection, target)
        except Exception as exc:
            # Don't fail the whole job if one source table is missing (e.g. on a
            # fresh Lakebase before schema init) — the others can still publish.
            logger.exception("Failed to copy %s → %s: %s", table, target, exc)
            failures.append(table)

    logger.info(
        "Lakebase → Delta sync complete. Rows=%s, tables=%s, failed=%s",
        total,
        len(_TABLES) - len(failures),
        failures or "none",
    )


if __name__ == "__main__":
    main()
