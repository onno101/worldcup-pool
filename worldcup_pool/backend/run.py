"""Production entrypoint: multi-process workers for concurrent load."""

from __future__ import annotations

import os

import uvicorn


def main() -> None:
    # Default 2 workers for small App SKUs; raise WEB_CONCURRENCY (e.g. 4–8) when CPU/RAM allows.
    workers = max(1, min(32, int(os.environ.get("WEB_CONCURRENCY", "2"))))
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(
        "worldcup_pool.backend.app:app",
        host="0.0.0.0",
        port=port,
        workers=workers,
        proxy_headers=True,
        forwarded_allow_ips="*",
        timeout_keep_alive=75,
    )


if __name__ == "__main__":
    main()
