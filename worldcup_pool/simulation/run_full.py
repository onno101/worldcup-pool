"""
Full World Cup pool simulation: synthetic 72 group matches, 150 users × predictions,
standings / 1st–2nd advancement sample, lock-policy checks.

Requires database credentials (same as app):
  - DATABASE_URL_OVERRIDE=postgresql+psycopg://...  (recommended for local runs), or
  - LAKEBASE_ENDPOINT + Databricks auth (via databricks-sdk in app config).

Usage:
  DATABASE_URL_OVERRIDE=... python -m worldcup_pool.simulation.run_full
  python -m worldcup_pool.simulation.run_full --smoke   # no DB: policy + standings only
  python -m worldcup_pool.simulation.run_full --cleanup # remove sim-wc-full-* rows
"""

from __future__ import annotations

import argparse
import random
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from itertools import combinations
from typing import Any

from sqlalchemy import text

SIM_PREFIX = "sim-wc-full-"
SIM_USER_PREFIX = "sim-wc-u"
N_SYNTH_USERS = 150


def _build_synthetic_group_match_rows() -> list[dict[str, Any]]:
    from worldcup_pool.wc2026_official_groups import official_wc2026_group_teams

    static = official_wc2026_group_teams()
    base = datetime(2026, 6, 10, 14, 0, tzinfo=timezone.utc)
    rows: list[dict[str, Any]] = []
    tick = 0
    for gk in sorted(static.keys()):
        teams = static[gk]
        codes = [t[0] for t in teams]
        nmap = {t[0]: t[1] for t in teams}
        for hi, ai in combinations(range(4), 2):
            h, a = codes[hi], codes[ai]
            ext = f"{SIM_PREFIX}m-{gk}-{h}-{a}"
            kick = base + timedelta(hours=tick)
            tick += 1
            dl = kick - timedelta(hours=1)
            rows.append(
                {
                    "e": ext,
                    "c": "WC",
                    "stg": "GROUP_STAGE",
                    "md": 1,
                    "gk": gk,
                    "hc": h,
                    "ac": a,
                    "hn": nmap[h],
                    "an": nmap[a],
                    "k": kick,
                    "dl": dl,
                }
            )
    return rows


def seed_synthetic_matches(session) -> int:
    rows = _build_synthetic_group_match_rows()
    ins = text(
        """
        INSERT INTO matches (
            external_match_id, competition_code, stage, matchday, group_key,
            home_team_code, away_team_code, home_team_name, away_team_name,
            kickoff_utc, prediction_deadline_utc, status, last_synced_at
        ) VALUES (
            :e, :c, :stg, :md, :gk, :hc, :ac, :hn, :an, :k, :dl, 'SCHEDULED', now()
        )
        ON CONFLICT (external_match_id) DO NOTHING
        """
    )
    for r in rows:
        session.execute(ins, r)
    n = session.execute(
        text("SELECT count(*) FROM matches WHERE external_match_id LIKE :pat"),
        {"pat": f"{SIM_PREFIX}%"},
    ).scalar_one()
    return int(n)


def cleanup_simulation(session) -> tuple[int, int]:
    """Delete sim predictions then sim matches. Returns (preds_deleted, matches_deleted)."""
    pr = session.execute(
        text("DELETE FROM match_predictions WHERE user_id LIKE :pat"),
        {"pat": f"{SIM_USER_PREFIX}%"},
    ).rowcount
    mr = session.execute(
        text("DELETE FROM matches WHERE external_match_id LIKE :pat"),
        {"pat": f"{SIM_PREFIX}%"},
    ).rowcount
    return int(pr or 0), int(mr or 0)


def load_group_stage_match_rows(session) -> list[dict[str, Any]]:
    q = text(
        """
        SELECT id::text AS id, group_key, home_team_code, away_team_code,
               home_team_name, away_team_name
        FROM matches
        WHERE UPPER(COALESCE(stage, '')) = 'GROUP_STAGE'
          AND external_match_id LIKE :pat
        ORDER BY kickoff_utc
        """
    )
    return [dict(r) for r in session.execute(q, {"pat": f"{SIM_PREFIX}%"}).mappings().all()]


def bulk_seed_predictions(session, match_rows: list[dict[str, Any]], n_users: int, rng: random.Random) -> int:
    mids = [r["id"] for r in match_rows]
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
    batch: list[dict[str, Any]] = []
    total = 0
    for u in range(1, n_users + 1):
        uid = f"{SIM_USER_PREFIX}{u:03d}"
        for mid in mids:
            batch.append({"uid": uid, "mid": mid, "h": rng.randint(0, 4), "a": rng.randint(0, 4)})
            if len(batch) >= 400:
                for row in batch:
                    session.execute(ins, row)
                total += len(batch)
                batch.clear()
    for row in batch:
        session.execute(ins, row)
    total += len(batch)
    return total


def load_user_predictions(session, user_id: str) -> dict[str, tuple[int, int]]:
    q = text(
        """
        SELECT p.match_id::text AS mid, p.home_goals AS h, p.away_goals AS a
        FROM match_predictions p
        JOIN matches m ON m.id = p.match_id
        WHERE p.user_id = :u
          AND m.external_match_id LIKE :pat
          AND p.home_goals IS NOT NULL AND p.away_goals IS NOT NULL
        """
    )
    out: dict[str, tuple[int, int]] = {}
    for r in session.execute(q, {"u": user_id, "pat": f"{SIM_PREFIX}%"}).mappings():
        out[str(r["mid"])] = (int(r["h"]), int(r["a"]))
    return out


def verify_lock_policy_on_db(session, lock_h: int) -> None:
    from worldcup_pool.simulation.policy import match_allows_prediction_update

    row = session.execute(
        text(
            f"""
            SELECT id::text, kickoff_utc, status FROM matches
            WHERE external_match_id LIKE :pat
            LIMIT 1
            """
        ),
        {"pat": f"{SIM_PREFIX}%"},
    ).mappings().first()
    if not row:
        print("  (skip lock DB test: no sim matches)")
        return
    mid = row["id"]
    orig_k = row["kickoff_utc"]
    now = datetime.now(timezone.utc)
    past_k = now - timedelta(hours=3)
    session.execute(text("UPDATE matches SET kickoff_utc = :k WHERE id = CAST(:id AS uuid)"), {"k": past_k, "id": mid})
    session.flush()
    row2 = session.execute(
        text("SELECT kickoff_utc, status FROM matches WHERE id = CAST(:id AS uuid)"), {"id": mid}
    ).mappings().one()
    ok, reason = match_allows_prediction_update(
        kickoff_utc=row2["kickoff_utc"],
        status=str(row2["status"]),
        now=now,
        lock_before_kickoff_hours=lock_h,
    )
    assert not ok and reason == "past_deadline", (ok, reason)
    session.execute(text("UPDATE matches SET kickoff_utc = :k WHERE id = CAST(:id AS uuid)"), {"k": orig_k, "id": mid})
    print("  lock policy: past kickoff correctly blocks updates (restored kickoff).")


def aggregate_predicted_qualifiers(
    session,
    match_rows: list[dict[str, Any]],
    n_users: int,
    rosters: dict[str, list[tuple[str, str]]],
) -> dict[str, Counter[tuple[str, str]]]:
    """For each group, count how often each (1st, 2nd) pair appears across users."""
    from worldcup_pool.simulation.standings import MatchRow, compute_standings_with_roster, first_and_second

    by_gk: dict[str, list[MatchRow]] = defaultdict(list)
    for r in match_rows:
        gk = str(r["group_key"] or "").strip().upper()
        if not gk:
            continue
        by_gk[gk].append(
            MatchRow(
                id=r["id"],
                home_team_code=r["home_team_code"],
                away_team_code=r["away_team_code"],
                home_team_name=r["home_team_name"],
                away_team_name=r["away_team_name"],
            )
        )

    out: dict[str, Counter[tuple[str, str]]] = {gk: Counter() for gk in rosters}
    for u in range(1, n_users + 1):
        uid = f"{SIM_USER_PREFIX}{u:03d}"
        draft = load_user_predictions(session, uid)
        for gk, roster in rosters.items():
            rows = compute_standings_with_roster(by_gk.get(gk, []), draft, roster)
            if len(rows) >= 2:
                first, second = first_and_second(rows)
                out[gk][(first, second)] += 1
    return out


def run_smoke() -> None:
    from worldcup_pool.backend.config import get_settings
    from worldcup_pool.simulation.policy import match_allows_prediction_update
    from worldcup_pool.simulation.standings import MatchRow, compute_standings_with_roster, first_and_second

    now = datetime.now(timezone.utc)
    k = now + timedelta(days=7)
    lock_h = get_settings().prediction_lock_before_kickoff_hours
    assert match_allows_prediction_update(kickoff_utc=k, status="SCHEDULED", now=now, lock_before_kickoff_hours=lock_h)[
        0
    ]
    assert not match_allows_prediction_update(
        kickoff_utc=now - timedelta(hours=2),
        status="SCHEDULED",
        now=now,
        lock_before_kickoff_hours=lock_h,
    )[0]
    print("smoke: lock policy OK")

    roster = [("A", "Aa"), ("B", "Bb"), ("C", "Cc"), ("D", "Dd")]
    ms = [
        MatchRow("1", "A", "B", "Aa", "Bb"),
        MatchRow("2", "A", "C", "Aa", "Cc"),
        MatchRow("3", "A", "D", "Aa", "Dd"),
        MatchRow("4", "B", "C", "Bb", "Cc"),
        MatchRow("5", "B", "D", "Bb", "Dd"),
        MatchRow("6", "C", "D", "Cc", "Dd"),
    ]
    draft = {"1": (2, 1), "2": (1, 1), "3": (3, 0), "4": (0, 2), "5": (1, 0), "6": (0, 0)}
    tab = compute_standings_with_roster(ms, draft, roster)
    first, second = first_and_second(tab)
    print(f"smoke: standings 1st={first} 2nd={second} (full table ranks: {[r.team_code for r in tab]})")


def run_db_main(*, cleanup_only: bool) -> int:
    from worldcup_pool.backend.config import get_settings
    from worldcup_pool.backend.db import get_engine, init_schema, session_scope, try_add_matches_group_key_column
    from worldcup_pool.wc2026_official_groups import official_wc2026_group_teams

    try:
        get_engine().connect().close()
    except Exception as e:
        print("Database not available:", e, file=sys.stderr)
        print("Set DATABASE_URL_OVERRIDE or LAKEBASE_ENDPOINT and retry.", file=sys.stderr)
        return 1

    init_schema()

    lock_h = get_settings().prediction_lock_before_kickoff_hours
    rosters = official_wc2026_group_teams()

    with session_scope() as session:
        try_add_matches_group_key_column(session)
        if cleanup_only:
            pr, mr = cleanup_simulation(session)
            print(f"cleanup: removed {pr} predictions, {mr} matches (sim prefix).")
            return 0

        pr, mr = cleanup_simulation(session)
        print(f"reset: removed {pr} prior sim predictions and {mr} prior sim matches.")
        n_matches = seed_synthetic_matches(session)
        print(f"seed: synthetic group-stage match rows now in DB: {n_matches}")

        match_rows = load_group_stage_match_rows(session)
        if len(match_rows) < 60:
            print(
                f"Expected ~72 synthetic group matches, found {len(match_rows)}. "
                "Run with working DB or check SIM_PREFIX conflicts.",
                file=sys.stderr,
            )
            return 1

        rng = random.Random(42)
        n = bulk_seed_predictions(session, match_rows, N_SYNTH_USERS, rng)
        print(f"predictions: upserted {n} rows for {N_SYNTH_USERS} users × {len(match_rows)} matches.")

        verify_lock_policy_on_db(session, lock_h)

        qual = aggregate_predicted_qualifiers(session, match_rows, N_SYNTH_USERS, rosters)
        print("\nPredicted qualifiers (frequency of 1st / 2nd across simulated users, sample groups):")
        for gk in ("GROUP_A", "GROUP_E", "GROUP_L"):
            top = qual.get(gk, Counter()).most_common(3)
            print(f"  {gk}: {top}")

        ko = session.execute(
            text(
                """
                SELECT stage, count(*) AS n FROM matches
                WHERE COALESCE(UPPER(TRIM(stage)), '') <> 'GROUP_STAGE'
                GROUP BY stage ORDER BY n DESC NULLS LAST LIMIT 8
                """
            )
        ).all()
        print("\nKnockout / other stages in DB (app shows these under separate headings; pairings come from sync):")
        for st, c in ko:
            print(f"  {st!r}: {c} matches")

    print("\nDone. App behavior summary:")
    print("  - Per-user group tables use the same points/GF/GA sort as the UI.")
    print("  - 1st/2nd here are from each user’s predicted scores only (not official FIFA results).")
    print("  - Real bracket progression is driven by football-data.org fixtures in `matches`, not recomputed in-app.")
    print(f"  - To remove synthetic data: python -m worldcup_pool.simulation.run_full --cleanup")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="World Cup pool full simulation")
    p.add_argument("--smoke", action="store_true", help="Run in-memory checks only (no DB)")
    p.add_argument("--cleanup", action="store_true", help="Remove simulation rows from DB")
    args = p.parse_args()
    if args.smoke:
        run_smoke()
        return 0
    return run_db_main(cleanup_only=args.cleanup)


if __name__ == "__main__":
    raise SystemExit(main())
