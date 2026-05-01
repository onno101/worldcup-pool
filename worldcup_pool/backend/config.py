from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Lakebase Autoscaling — full endpoint resource name, e.g.
    # projects/worldcup-pool/branches/production/endpoints/ep-primary
    lakebase_endpoint: str = ""
    lakebase_database: str = "databricks_postgres"

    # football-data.org
    football_data_token: str = ""
    football_data_competition: str = "WC"

    # Comma-separated admin emails (admin UI, sync, knockout simulation). Set via ADMIN_EMAILS env var.
    admin_emails: str = ""

    # Match predictions close this many hours before each match kickoff (from kickoff_utc, not DB column).
    prediction_lock_before_kickoff_hours: int = Field(default=1, ge=1, le=168)

    # After this instant (UTC), champion and top-scorer picks are read-only. Edits allowed strictly before.
    # Override with env TOURNAMENT_PICKS_LOCK_AT_UTC (ISO8601, e.g. 2026-06-11T18:00:00+00:00).
    tournament_picks_lock_at_utc: str = Field(default="2026-06-11T18:00:00+00:00")

    @field_validator("tournament_picks_lock_at_utc", mode="before")
    @classmethod
    def _strip_lock_at(cls, v: object) -> object:
        if isinstance(v, str):
            return v.strip()
        return v

    # Local dev: optional direct Postgres URL (bypasses Lakebase OAuth)
    database_url_override: str = ""

    # Avoid running DDL from app process unless explicitly enabled.
    init_schema_on_start: bool = False

    # If true and FOOTBALL_DATA_TOKEN is set, run one football-data sync when matches table is empty
    # (uses pg_try_advisory_lock so only one worker syncs). Helps after empty Lakebase / failed job.
    auto_sync_matches_if_empty: bool = False

    # SQLAlchemy pool (per app process). Tune so workers * (pool_size + max_overflow)
    # stays below your Lakebase / Postgres max connections budget.
    db_pool_size: int = 8
    db_max_overflow: int = 12
    db_pool_timeout: int = 30
    # Recycle connections periodically (idle timeouts, stale NAT, etc.)
    db_pool_recycle: int = 1800


@lru_cache
def get_settings() -> Settings:
    return Settings()
