<h1 align="center">
  <img src="ui/public/favicon.png" width="36" alt="" /><br/>
  World Cup 2026 Prediction Pool
</h1>

<p align="center">
  <strong>Run a World Cup prediction pool for your team, company, or friends — deployed in minutes on Databricks.</strong>
</p>

<p align="center">
  <a href="#deploy-in-5-commands">Quick Deploy</a> &nbsp;|&nbsp;
  <a href="#features">Features</a> &nbsp;|&nbsp;
  <a href="#how-scoring-works">Scoring</a> &nbsp;|&nbsp;
  <a href="#configuration">Configuration</a> &nbsp;|&nbsp;
  <a href="#local-development">Development</a>
</p>

---

Predict scorelines for all 104 World Cup matches, pick the tournament champion and top goal scorers, and compete on a live leaderboard against your colleagues. Match results sync automatically — just deploy and invite your team.

Built as a [Databricks App](https://docs.databricks.com/en/dev-tools/databricks-apps/) powered by [Lakebase](https://docs.databricks.com/en/oltp/) (managed PostgreSQL) and deployed via [Databricks Asset Bundles](https://docs.databricks.com/en/dev-tools/bundles/).

<br/>

<p align="center">
  <img src="docs/screenshots/forecasts.png" width="720" alt="Match forecasts — predict scores for every group and knockout match" />
</p>

<p align="center"><em>Predict scores for every match — group stage through the final</em></p>

<br/>

<p align="center">
  <img src="docs/screenshots/tournament-picks.png" width="720" alt="Tournament picks — choose your champion and top scorers" />
</p>

<p align="center"><em>Pick the tournament winner and top 3 goal scorers before kickoff</em></p>

<br/>

<p align="center">
  <img src="docs/screenshots/leaderboard.png" width="720" alt="Live leaderboard with 151 participants" />
</p>

<p align="center"><em>Live leaderboard — track rankings across 150+ participants</em></p>

<br/>

## Features

- **104 match predictions** — fill in scorelines for every group and knockout match. Knockout brackets update automatically based on your predictions.
- **Tournament picks** — choose the champion and up to 3 top goal scorers before the tournament starts.
- **Live leaderboard** — real-time rankings with detailed points breakdown (outcome, exact score, scorer goals, advancer bonus).
- **Automatic sync** — match fixtures and live scores pulled from [football-data.org](https://www.football-data.org/) every 5 minutes.
- **Custom branding** — upload your company logo and set a pool name from the admin panel.
- **Player profiles** — display name, nationality, and profile picture for each participant.
- **Scales to hundreds of users** — multi-worker FastAPI with connection pooling and batch operations.
- **One-click deploy** — Databricks Asset Bundle handles everything: app, sync job, and optional AI/BI dashboard.

## How scoring works

Points are awarded per match after results are synced. Knockout rounds are worth more.

| Round | Multiplier | Outcome | Exact score | Scorer | Advancer |
|-------|-----------|---------|-------------|--------|----------|
| Group | x1 | 2 | 5 | 2 | — |
| R32 | x1.5 | 3 | 8 | 3 | 5 |
| R16 | x2 | 4 | 10 | 4 | 6 |
| QF | x2.5 | 5 | 13 | 5 | 8 |
| SF / 3rd / Final | x3 | 6 | 15 | 6 | 9 |

**Tournament winner:** +25 pts when the final result matches your champion pick.

## Quick deploy

**Prerequisites:** A Databricks workspace with [Lakebase](https://docs.databricks.com/en/oltp/) enabled, [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/install.html) (v0.285+), Node.js 18+, `psql` and `jq`, and an API token from [football-data.org](https://www.football-data.org/client/register) — see [API token tier](#api-token-tier) below for which plan to pick.

```bash
# 1. Clone
git clone https://github.com/onno101/worldcup-pool.git && cd worldcup-pool

# 2. Store your football-data.org API token as a Databricks secret
#    (used by the 5-minute scheduled sync job)
databricks secrets create-scope worldcup_pool
databricks secrets put-secret worldcup_pool football_data_token --string-value "YOUR_TOKEN"

# 3. Deploy — pass your email as admin so you can manage the pool
databricks bundle deploy -t dev --var admin_emails=you@company.com

# 4. Grant the app's service principal access to Lakebase (one-time, idempotent)
./scripts/bootstrap_lakebase_app.sh dev

# 5. Start the app
databricks bundle run worldcup_pool_app -t dev

# 6. In the Databricks App UI → Environment, set FOOTBALL_DATA_TOKEN to your
#    football-data.org token. This enables in-app live sync (faster than the
#    5-min job) and unlocks the admin "Sync now" button.
```

> **Step 3 — admin emails must be set at deploy time.** `--var admin_emails`
> writes `ADMIN_EMAILS` into the app environment so you can access admin
> endpoints (config, logo upload, manual sync). For multiple admins, pass a
> comma-separated list: `--var admin_emails=alice@x.com,bob@x.com`. You can
> re-run `bundle deploy` with a different value any time; the script in the
> next step is idempotent so it won't redo work.

> **Why step 4?** The Databricks App runs as its own service principal.
> Lakebase requires that SP to be registered as a Postgres OAuth role and
> granted schema permissions before the app can create its tables. The
> bootstrap script reads the SP from the deployed app and provisions
> everything via the Lakebase API.

> **Why step 6 is a UI step.** The scheduled sync job reads
> `FOOTBALL_DATA_TOKEN` from your secret scope (step 2), so matches will
> populate within ~5 minutes regardless. Setting the token directly on the
> app speeds up first sync and lets the in-app admin "Sync now" button work
> immediately. If you'd rather wait for the cron, you can skip step 6.

The Lakebase endpoint defaults to `projects/worldcup-pool/branches/production/endpoints/primary`. Override with:
```bash
databricks bundle deploy -t dev --var lakebase_endpoint="projects/YOUR_PROJECT/branches/production/endpoints/primary"
```

> **Create a Lakebase project first** if you don't have one yet: in the Databricks UI, go to **Catalog > Lakebase > Create project**.

### API token tier

The pool uses [football-data.org](https://www.football-data.org/) for fixtures, live scores, and goal scorers. Pick your plan based on which features you want:

| Plan | Cost | Fixtures & live scores | Goal scorer events |
|------|------|------------------------|--------------------|
| **Free Tier** | Free | Yes | No — scorer points won't be awarded |
| **Free + Deep Data** | €29 / month | Yes | Yes — required for top-scorer scoring |
| Higher paid tiers | Paid | Yes | Yes |

If you want the **top scorer** picks and the per-match scorer points to actually score, you need the **Free + Deep Data** add-on (€29/month at the time of writing) or a higher paid tier. The standard free key returns matches and scores but omits the goal-event detail the sync job needs to credit scorer points.

Subscribe to Deep Data from your account page after registering at [football-data.org/client/register](https://www.football-data.org/client/register). Without it, the app still runs — outcome / exact-score / advancer points all work, but every match's `scorer` column will be 0.

## Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `LAKEBASE_ENDPOINT` | Lakebase Autoscale endpoint resource name | `projects/worldcup-pool/.../primary` |
| `LAKEBASE_DATABASE` | Postgres database name | `databricks_postgres` |
| `FOOTBALL_DATA_TOKEN` | API token from football-data.org | *(required)* |
| `ADMIN_EMAILS` | Comma-separated admin emails | *(empty)* |
| `WEB_CONCURRENCY` | Uvicorn worker processes | `2` |
| `PREDICTION_LOCK_BEFORE_KICKOFF_HOURS` | Hours before kickoff when predictions close | `1` |
| `TOURNAMENT_PICKS_LOCK_AT_UTC` | When tournament picks become read-only (ISO8601) | `2026-06-11T18:00:00+00:00` |
| `INIT_SCHEMA_ON_START` | Run DDL on app cold start | `false` |
| `AUTO_SYNC_MATCHES_IF_EMPTY` | Auto-sync fixtures when matches table is empty | `true` |

See `.env.example` for a complete template.

## Bundle targets

Three targets ship in `databricks.yml`. Pick the one that matches what you're doing — they differ in which Lakebase branch they point at, whether predictions are still open, and whether schema init / fixture sync run on cold start.

| Target | When to use | Tournament lock | Auto-sync fixtures | Init schema on start | Mode |
|--------|-------------|-----------------|--------------------|----------------------|------|
| `dev` (default) | Active development, internal testing, your real company pool | `2026-06-11T18:00:00Z` (first kickoff) | `true` | `true` | development |
| `simulation` | Demoing what a finished pool looks like — 150 simulated users with picks already submitted | `2026-04-01T00:00:00Z` (already past — picks read-only) | `false` (uses pre-seeded data) | `false` | development |
| `prod` | Production deployment for your live pool | `2026-06-11T18:00:00Z` | `true` | `true` | production (locks workspace path, requires explicit deploy) |

**`dev`** is the default and the lightweight option. Use it to test UI changes, try out scoring tweaks, run through the prediction flow, and generally play with the app. Deploys with a bare `databricks bundle deploy` — no flags needed. **If you don't plan to fork the code or customize much, `dev` is perfectly fine as your real, live company pool too** — share its URL, invite your team, you're done.

**`simulation`** is for showing the pool off before the tournament starts — recruiters, leadership, internal demos. It points at a separate Lakebase branch (`branches/simulation`) so it doesn't pollute your real pool's data, and tournament picks are already locked, so visitors see a fully populated leaderboard immediately. To populate the simulated users, run `scripts/simulate_data.py` against the simulation Lakebase after deploy.

**`prod`** is mostly a naming convention — it deploys an app called `worldcup-pool-prod` (separate URL from `-dev`) and runs under Databricks Asset Bundle `production` mode. As shipped, that mode mainly affects bundle-side validation (rejects deploys to per-user paths, requires `run_as` on jobs) — it doesn't restrict who can update the app or lock down the deployment. The real value is just having two side-by-side namespaces: a stable URL you share with your team, and a scratch URL for trying changes without breaking the first one. **If you're not iterating on the code, you don't need `prod` at all — `dev` is fine as your live pool.** If you do want stronger production guardrails (workspace path locking, permissions, service-principal `run_as`), add them to the `prod` target yourself.

All three targets accept `--var lakebase_endpoint=projects/<your-project>/branches/<your-branch>/endpoints/primary` to point at your own Lakebase project. The defaults in `databricks.yml` use placeholder values that you'll need to override.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                  Databricks App                     │
│  ┌───────────┐    ┌──────────────────────────────┐  │
│  │ React SPA │───▶│  FastAPI (multi-worker)       │  │
│  │  (Vite)   │    │  - Match predictions API     │  │
│  └───────────┘    │  - Tournament picks API      │  │
│                   │  - Leaderboard (cached)       │  │
│                   │  - Admin (sync, config)       │  │
│                   └──────────┬───────────────────┘  │
│                              │                      │
└──────────────────────────────┼──────────────────────┘
                               │ SQLAlchemy + OAuth
                               ▼
                    ┌──────────────────────┐
                    │  Lakebase (Postgres)  │
                    │  Auto-scaling         │
                    └──────────────────────┘
                               ▲
           ┌───────────────────┘
           │ Scheduled job (5 min)
┌──────────┴──────────┐        ┌─────────────────────┐
│   Match Sync Job    │───────▶│  football-data.org   │
│  (Databricks Job)   │        │  (scores & fixtures) │
└─────────────────────┘        └─────────────────────┘
```

## Local development

```bash
uv sync
cp .env.example .env   # fill in your values
export DATABASE_URL_OVERRIDE=postgresql://user:pass@localhost:5432/worldcup
export WORLDCUP_DEV_CORS=1
uv run uvicorn worldcup_pool.backend.app:app --reload --host 127.0.0.1 --port 8000
```

In another terminal:
```bash
cd ui && npm ci && npm run dev
```

Without a football-data token, call `POST /api/dev/seed-demo-matches` to populate sample data.

### Scaling

Budget roughly `WEB_CONCURRENCY x (DB_POOL_SIZE + DB_MAX_OVERFLOW)` connections. Keep that under your Lakebase connection limit. Match prediction saves use batch upserts for efficiency.

## AI/BI Dashboard (optional)

The bundle includes a Lakebase-to-Delta mirror that powers an AI/BI dashboard — showing that one database drives both the app and the Lakehouse.

```bash
databricks bundle deploy -t dev --var dashboard_warehouse_id=YOUR_ID
databricks bundle run worldcup_sync_matches -t dev
```

This deploys a sync task that mirrors four tables into Unity Catalog Delta, plus a dashboard with KPIs, champion pick distribution, and prediction activity.

## Troubleshooting

### `npm ci` fails during deploy

The committed `ui/package-lock.json` resolves packages from `registry.npmjs.org` (pinned via `ui/.npmrc`). If your local network can't reach the public npm registry — for example, you're behind a corporate proxy that only allows an internal mirror — the predeploy step will fail.

To regenerate the lockfile against whichever registry your machine can reach:

```bash
rm -rf ui/node_modules ui/package-lock.json
cd ui && npm install && cd ..
databricks bundle deploy -t dev --var admin_emails=you@company.com
```

The regenerated lockfile is local-only; don't commit it back if it now references an internal mirror, since that would break deploys for other people on different networks.

## License

Apache 2.0 — see [LICENSE](LICENSE).
