"""Databricks Job entrypoint: sync fixtures and scores from football-data.org."""

from __future__ import annotations

import logging
import sys

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)


def main() -> None:
    from worldcup_pool.backend.db import init_schema
    from worldcup_pool.services.sync import run_sync

    init_schema()
    n = run_sync()
    logger.info("Synced %s matches", n)


if __name__ == "__main__":
    main()
