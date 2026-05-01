"""FastAPI application: API + SPA static files."""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from worldcup_pool.backend.routes import router

logger = logging.getLogger(__name__)

# In Databricks Apps the package often lives under site-packages; repo-root `ui/dist`
# is not next to the code. Ship the SPA inside the package (`worldcup_pool/web_dist`).
def _resolve_ui_dist() -> Path | None:
    pkg_root = Path(__file__).resolve().parent.parent  # worldcup_pool/
    repo_root = pkg_root.parent
    candidates = (
        pkg_root / "web_dist",
        repo_root / "ui" / "dist",
    )
    for p in candidates:
        if p.is_dir() and (p / "index.html").is_file():
            return p
    return None


UI_DIST = _resolve_ui_dist()
if UI_DIST is None:
    logger.warning(
        "No SPA assets found (tried worldcup_pool/web_dist, ui/dist); "
        "cwd=%s __file__=%s — ensure bundle sync.include lists worldcup_pool/web_dist/** "
        "and predeploy copies ui/dist there.",
        Path.cwd(),
        __file__,
    )


def _maybe_auto_sync_matches_if_empty() -> None:
    """One-shot sync when DB has zero fixtures (guarded by advisory lock for multi-worker)."""
    from sqlalchemy import text

    from worldcup_pool.backend.config import get_settings
    from worldcup_pool.backend.db import get_engine
    from worldcup_pool.services.sync import run_sync

    settings = get_settings()
    if not settings.auto_sync_matches_if_empty:
        return
    if not (settings.football_data_token or "").strip():
        logger.info("auto_sync_matches_if_empty: skipping (FOOTBALL_DATA_TOKEN not set on app)")
        return

    lock_key = 802154331
    eng = get_engine()
    # Never call run_sync() while holding this connection open: sync does HTTP + many writes on
    # other pool connections and can take minutes — holding a transaction here exhausts Lakebase
    # pools and makes /api/* hang (UI stuck on "Loading…").
    do_sync = False
    with eng.begin() as conn:
        locked = conn.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": lock_key}).scalar()
        if not locked:
            logger.info("auto_sync_matches_if_empty: lock held by another worker; skipping")
            return
        try:
            cnt = conn.execute(text("SELECT COUNT(*)::bigint FROM matches")).scalar() or 0
            if int(cnt) == 0:
                do_sync = True
                logger.warning(
                    "Matches table is empty; will run football-data sync after releasing advisory lock"
                )
        finally:
            conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": lock_key})

    if not do_sync:
        return
    try:
        n = run_sync()
        logger.info("auto_sync_matches_if_empty: synced %s matches", n)
    except Exception:
        logger.exception("auto_sync_matches_if_empty: football-data sync failed")


@asynccontextmanager
async def lifespan(app: FastAPI):
    from worldcup_pool.backend.config import get_settings

    settings = get_settings()
    bg_tasks: set[asyncio.Task] = set()

    # Do not await schema init before yield: Lakebase + Databricks SDK credential fetch can stall
    # for a long time and would keep every /api/* request from completing (UI stuck on "Loading…").
    if settings.init_schema_on_start:

        async def _bg_init() -> None:
            try:

                def _init() -> None:
                    from worldcup_pool.backend.db import init_schema

                    init_schema()

                await asyncio.to_thread(_init)
                logger.info("Database schema initialized")
            except Exception:
                logger.exception(
                    "Schema init failed (configure LAKEBASE_ENDPOINT or DATABASE_URL_OVERRIDE); "
                    "API may return errors until init succeeds — check logs and retry."
                )

        t = asyncio.create_task(_bg_init())
        bg_tasks.add(t)
        t.add_done_callback(bg_tasks.discard)

    # Never block request acceptance on a full football-data sync (can take minutes).
    if settings.auto_sync_matches_if_empty and (settings.football_data_token or "").strip():

        async def _bg_sync() -> None:
            try:
                await asyncio.to_thread(_maybe_auto_sync_matches_if_empty)
            except Exception:
                logger.exception("Background auto_sync_matches_if_empty failed")

        t = asyncio.create_task(_bg_sync())
        bg_tasks.add(t)
        t.add_done_callback(bg_tasks.discard)

    yield


app = FastAPI(title="World Cup Pool", lifespan=lifespan)

if os.getenv("WORLDCUP_DEV_CORS", "").lower() in ("1", "true", "yes"):
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://127.0.0.1:5173", "http://localhost:5173"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

app.include_router(router)

# Serve built React app (production / Databricks)
if UI_DIST is not None:
    app.mount("/", StaticFiles(directory=str(UI_DIST), html=True), name="spa")
else:
    @app.get("/")
    def root_stub():
        return {
            "message": "UI not built. Run: cd ui && npm ci && npm run build",
            "api": "/api/matches",
        }
