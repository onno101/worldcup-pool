#!/usr/bin/env python3
"""Remove one participant's rows from the pool DB (match + tournament picks, profile, app cache).

Uses the same Lakebase / DATABASE_URL_OVERRIDE config as the app.

Usage:
  cd worldcup-pool && uv run python scripts/delete_user_pool_data.py user@example.com
"""

from __future__ import annotations

import argparse
import sys

from sqlalchemy import text

from worldcup_pool.backend.db import session_scope


def main() -> int:
    p = argparse.ArgumentParser(description="Delete pool data for one user_id (email).")
    p.add_argument("email", help="User id as stored in DB (typically JWT email)")
    args = p.parse_args()
    uid = args.email.strip()
    if not uid:
        print("Empty email", file=sys.stderr)
        return 2

    with session_scope() as session:
        mp = session.execute(
            text("DELETE FROM match_predictions WHERE user_id ILIKE :u"),
            {"u": uid},
        ).rowcount
        tp = session.execute(
            text("DELETE FROM tournament_predictions WHERE user_id ILIKE :u"),
            {"u": uid},
        ).rowcount
        pr = session.execute(
            text("DELETE FROM user_profiles WHERE user_id ILIKE :u"),
            {"u": uid},
        ).rowcount
        ac = session.execute(
            text("DELETE FROM app_users_cache WHERE user_id ILIKE :u"),
            {"u": uid},
        ).rowcount

    print(
        f"Deleted for user_id ILIKE {uid!r}: "
        f"match_predictions={mp}, tournament_predictions={tp}, "
        f"user_profiles={pr}, app_users_cache={ac}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
