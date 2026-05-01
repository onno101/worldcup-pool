# World Cup 2026 Prediction Pool

A full-stack prediction pool for the FIFA World Cup 2026, built as a [Databricks App](https://docs.databricks.com/en/dev-tools/databricks-apps/) with [Lakebase](https://docs.databricks.com/en/oltp/) Autoscaling PostgreSQL.

Users predict scorelines for every match, pick a tournament winner and top scorers, and compete on a live leaderboard. Match results sync automatically from [football-data.org](https://www.football-data.org/).

**Stack:** FastAPI + React + Lakebase (PostgreSQL) + Databricks Asset Bundles

## Quick start

### Prerequisites

- A Databricks workspace with [Lakebase](https://docs.databricks.com/en/oltp/) enabled
- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/install.html) configured (`databricks auth login`)
- Node.js 18+ (the deploy step builds the React UI)
- A free API token from [football-data.org](https://www.football-data.org/client/register)

### Deploy in 5 commands

```bash
# 1. Clone the repo
git clone https://github.com/onnovanderhorst/worldcup-pool.git
cd worldcup-pool

# 2. Create a Lakebase Autoscale project (if you don't have one yet)
#    In the Databricks UI: Catalog > Lakebase > Create project

# 3. Store your football-data.org API token as a Databricks secret
databricks secrets create-scope worldcup_pool
databricks secrets put-secret worldcup_pool football_data_token --string-value "YOUR_TOKEN"

# 4. Deploy (builds the UI and deploys the app + sync job)
databricks bundle deploy -t dev

# 5. Start the app
databricks bundle run worldcup_pool_app -t dev
```

After the app starts, set your admin email in the Databricks App UI environment settings:
`ADMIN_EMAILS = you@company.com`

The Lakebase endpoint defaults to `projects/worldcup-pool/branches/production/endpoints/primary`. Override it with:
```bash
databricks bundle deploy -t dev --var lakebase_endpoint="projects/YOUR_PROJECT/branches/production/endpoints/primary"
```

## How it works

### For participants
- **Matches tab** — predict scorelines for all 104 matches (groups + knockouts). Predictions lock 1 hour before kickoff.
- **Tournament tab** — pick the tournament winner and top 3 goal scorers before the opening match.
- **Leaderboard** — live rankings based on match outcome (2 pts), exact scoreline (5 pts), scorer goals, and tournament winner (25 pts).
- **Profile** — set your display name, nationality, and profile picture.

### Under the hood
- **Sync job** runs every 5 minutes, pulling match fixtures and scores from football-data.org into Lakebase
- **Goal event enrichment** fetches individual goal scorers for top-scorer scoring
- **Multi-worker FastAPI** with connection pooling, advisory locks, and batch upserts for concurrency
- **Predeploy script** builds the React UI, copies it into the Python package, and generates `app.yaml` from the template with target-specific variables

## Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `LAKEBASE_ENDPOINT` | Lakebase Autoscale endpoint resource name | `projects/worldcup-pool/.../primary` |
| `LAKEBASE_DATABASE` | Postgres database name | `databricks_postgres` |
| `FOOTBALL_DATA_TOKEN` | API token from football-data.org | *(required)* |
| `ADMIN_EMAILS` | Comma-separated admin emails (can trigger sync, manage pool) | *(empty)* |
| `WEB_CONCURRENCY` | Uvicorn worker processes | `2` |
| `PREDICTION_LOCK_BEFORE_KICKOFF_HOURS` | Hours before kickoff when predictions close | `1` |
| `TOURNAMENT_PICKS_LOCK_AT_UTC` | UTC instant when tournament picks become read-only (ISO8601) | `2026-06-11T18:00:00+00:00` |
| `INIT_SCHEMA_ON_START` | Run DDL on app cold start | `false` |
| `AUTO_SYNC_MATCHES_IF_EMPTY` | Auto-sync fixtures when matches table is empty | `true` |
| `DATABASE_URL_OVERRIDE` | Direct Postgres URL for local dev (bypasses Lakebase OAuth) | *(empty)* |

See `.env.example` for a complete template.

### Scaling

Budget roughly `WEB_CONCURRENCY x (DB_POOL_SIZE + DB_MAX_OVERFLOW)` connections (e.g. 4x(8+12) = 80). Keep that under your Lakebase connection limit. Match prediction saves use one batch SELECT + one executemany upsert for efficiency.

## Bundle targets

| Target | Purpose |
|--------|---------|
| `dev` | Active development (default) |
| `simulation` | Pre-populated with 150 users and simulated tournament data |
| `prod` | Production deployment |

Each target overrides `lakebase_endpoint`, `auto_sync`, `init_schema`, and `tournament_lock`. Pass `--var dashboard_warehouse_id=<ID>` on deploy if you want the AI/BI demo dashboard.

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

## Demo: Lakebase to Unity Catalog (AI/BI dashboard)

The bundle includes an optional Lakebase to Delta mirror that powers an AI/BI dashboard, showing that one database powers both the app and the Lakehouse.

### What gets deployed

- **Sync task** `lakebase_to_delta` on the `worldcup_sync_matches` job (every 5 min). Mirrors four tables into Unity Catalog Delta: `matches`, `match_predictions`, `tournament_predictions`, `user_profiles_public`.
- **AI/BI dashboard** with KPIs, champion pick distribution, match prediction activity, and leaderboard.

### Bundle variables for the demo

| Variable | Default | Notes |
|----------|---------|-------|
| `demo_catalog` | `main` | Must already exist |
| `demo_schema` | `worldcup_pool` | Auto-created on first run |
| `dashboard_warehouse_id` | *(none)* | Pass via `--var` on deploy |

```bash
databricks bundle deploy -t dev --var dashboard_warehouse_id=YOUR_ID
databricks bundle run worldcup_sync_matches -t dev
```

## License

Apache 2.0. See [LICENSE](LICENSE).
