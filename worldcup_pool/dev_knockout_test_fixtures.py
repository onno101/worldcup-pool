"""
Synthetic knockout matches for manual DB tests (not exposed on the pool HTTP API).

- `external_match_id` values start with `TEST_KO_EXTERNAL_ID_PREFIX` (never issued by football-data.org).
- Revert with `revert_test_knockout_matches()` in a Python shell or ad-hoc script.
- A full `POST /api/admin/sync-matches` leaves these rows untouched (different IDs); delete test rows first if you want a clean slate.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

TEST_KO_EXTERNAL_ID_PREFIX = "__test_ko__"
KNOCKOUT_TEST_FIXTURE_COUNT = 6


def _rows(now: datetime) -> list[dict[str, Any]]:
    """Future kickoffs so predictions stay open for a while."""
    u = now.astimezone(timezone.utc)
    def kick(days: int, hour: int = 20) -> datetime:
        d = u + timedelta(days=days)
        return d.replace(hour=hour, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

    return [
        {
            "e": f"{TEST_KO_EXTERNAL_ID_PREFIX}r16-a",
            "stg": "ROUND_OF_16",
            "md": 4,
            "gk": None,
            "hc": "NED",
            "ac": "BEL",
            "hn": "Netherlands",
            "an": "Belgium",
            "k": kick(12),
        },
        {
            "e": f"{TEST_KO_EXTERNAL_ID_PREFIX}r16-b",
            "stg": "ROUND_OF_16",
            "md": 4,
            "gk": None,
            "hc": "POR",
            "ac": "SUI",
            "hn": "Portugal",
            "an": "Switzerland",
            "k": kick(13),
        },
        {
            "e": f"{TEST_KO_EXTERNAL_ID_PREFIX}qf-1",
            "stg": "QUARTER_FINALS",
            "md": 5,
            "gk": None,
            "hc": "CRO",
            "ac": "JPN",
            "hn": "Croatia",
            "an": "Japan",
            "k": kick(20),
        },
        {
            "e": f"{TEST_KO_EXTERNAL_ID_PREFIX}qf-2",
            "stg": "QUARTER_FINALS",
            "md": 5,
            "gk": None,
            "hc": "USA",
            "ac": "COL",
            "hn": "United States",
            "an": "Colombia",
            "k": kick(21),
        },
        {
            "e": f"{TEST_KO_EXTERNAL_ID_PREFIX}sf-1",
            "stg": "SEMI_FINALS",
            "md": 6,
            "gk": None,
            "hc": "URU",
            "ac": "ARG",
            "hn": "Uruguay",
            "an": "Argentina",
            "k": kick(28),
        },
        {
            "e": f"{TEST_KO_EXTERNAL_ID_PREFIX}final-x",
            "stg": "FINAL",
            "md": 7,
            "gk": None,
            "hc": "FRA",
            "ac": "GER",
            "hn": "France",
            "an": "Germany",
            "k": kick(35, hour=17),
        },
    ]


def insert_test_knockout_matches(
    session: Session,
    *,
    lock_h: int,
    now: datetime,
    has_group_key_column: bool,
) -> int:
    """Insert test KO fixtures. Idempotent: ON CONFLICT DO NOTHING."""
    comp = "WC"
    st = "SCHEDULED"
    n = 0
    for row in _rows(now):
        kick = row["k"]
        deadline = kick - timedelta(hours=lock_h)
        params = {
            "e": row["e"],
            "c": comp,
            "stg": row["stg"],
            "md": row["md"],
            "gk": row["gk"],
            "hc": row["hc"],
            "ac": row["ac"],
            "hn": row["hn"],
            "an": row["an"],
            "k": kick,
            "dl": deadline,
            "st": st,
        }
        if has_group_key_column:
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
                params,
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
                {k: v for k, v in params.items() if k != "gk"},
            )
        n += 1
    return n


def revert_test_knockout_matches(session: Session) -> int:
    """Remove all test KO fixtures; CASCADE removes match_predictions for those rows."""
    r = session.execute(
        text(
            """
            DELETE FROM matches
            WHERE external_match_id LIKE :pattern
            """
        ),
        {"pattern": TEST_KO_EXTERNAL_ID_PREFIX + "%"},
    )
    return int(r.rowcount or 0)


def knockout_simulation_active(session: Session) -> bool:
    row = session.execute(
        text(
            """
            SELECT EXISTS (
                SELECT 1 FROM matches WHERE external_match_id LIKE :pattern LIMIT 1
            )
            """
        ),
        {"pattern": TEST_KO_EXTERNAL_ID_PREFIX + "%"},
    ).scalar()
    return bool(row)


def apply_knockout_simulation(
    session: Session,
    *,
    enabled: bool,
    lock_h: int,
    now: datetime,
    has_group_key_column: bool,
) -> tuple[bool, str]:
    """Turn synthetic KO fixtures on or off; returns (active_after, human detail)."""
    if enabled:
        insert_test_knockout_matches(
            session, lock_h=lock_h, now=now, has_group_key_column=has_group_key_column
        )
        active = knockout_simulation_active(session)
        return (
            active,
            "Synthetic knockout matches are available (ids start with __test_ko__)."
            if active
            else "Seed completed; no new rows (fixtures may already exist).",
        )
    deleted = revert_test_knockout_matches(session)
    return False, f"Removed {deleted} synthetic knockout match row(s)."
