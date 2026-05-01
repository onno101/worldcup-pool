"""Match prediction lock rules (same semantics as FastAPI routes)."""

from __future__ import annotations

from datetime import datetime, timedelta


def prediction_lock_deadline(kickoff_utc: datetime, lock_hours: int) -> datetime:
    return kickoff_utc - timedelta(hours=lock_hours)


def match_allows_prediction_update(
    *,
    kickoff_utc: datetime,
    status: str,
    now: datetime,
    lock_before_kickoff_hours: int,
) -> tuple[bool, str]:
    """Return (allowed, reason_code). Mirrors list_matches / put_match_predictions."""
    st = (status or "").upper()
    if st in ("FINISHED", "LIVE", "POSTPONED"):
        return False, "match_status"
    deadline = prediction_lock_deadline(kickoff_utc, lock_before_kickoff_hours)
    if now >= deadline:
        return False, "past_deadline"
    return True, "ok"
