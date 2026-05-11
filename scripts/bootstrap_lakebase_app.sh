#!/usr/bin/env bash
# Register the app's service principal as a Lakebase Postgres role and grant
# schema permissions so the app can run INIT_SCHEMA on first start.
#
# Run this AFTER `databricks bundle deploy -t <target>` and BEFORE
# `databricks bundle run worldcup_pool_app -t <target>`.
#
# Idempotent: safe to re-run.
#
# Usage:
#   scripts/bootstrap_lakebase_app.sh [target] [extra args forwarded to `bundle summary`]
#
# If your bundle target requires variables to validate (e.g. prod requires
# dashboard_warehouse_id), pass them through after the target:
#   scripts/bootstrap_lakebase_app.sh prod --var dashboard_warehouse_id=YOUR_ID
#
# Or set LAKEBASE_ENDPOINT in the environment to skip the bundle lookup.
#
# Requires: databricks CLI (>=0.285), jq, psql.
set -euo pipefail

TARGET="${1:-dev}"
shift || true
PROFILE="${DATABRICKS_PROFILE:-DEFAULT}"
APP_NAME="worldcup-pool-${TARGET}"

for cmd in databricks jq psql; do
  command -v "$cmd" >/dev/null 2>&1 || { echo "ERROR: $cmd is required" >&2; exit 1; }
done

echo "Looking up app $APP_NAME..."
SP_ID=$(databricks apps get "$APP_NAME" -p "$PROFILE" -o json 2>/dev/null \
  | jq -r '.service_principal_client_id // empty')
if [ -z "$SP_ID" ]; then
  echo "ERROR: app '$APP_NAME' not found. Run 'databricks bundle deploy -t $TARGET' first." >&2
  exit 1
fi
echo "  service principal: $SP_ID"

if [ -z "${LAKEBASE_ENDPOINT:-}" ]; then
  LAKEBASE_ENDPOINT=$(databricks bundle summary -t "$TARGET" "$@" -p "$PROFILE" -o json 2>/dev/null \
    | jq -r '.variables.lakebase_endpoint.value // empty')
fi
if [ -z "${LAKEBASE_ENDPOINT:-}" ]; then
  echo "ERROR: could not resolve lakebase_endpoint from bundle target '$TARGET'." >&2
  echo "Hint: pass any required --var flags after the target, or set LAKEBASE_ENDPOINT in env." >&2
  exit 1
fi
BRANCH="${LAKEBASE_ENDPOINT%/endpoints/*}"
echo "  lakebase branch:   $BRANCH"

EXISTING=$(databricks postgres list-roles "$BRANCH" -p "$PROFILE" -o json 2>/dev/null \
  | jq -r --arg sp "$SP_ID" 'map(select(.status.postgres_role==$sp and .status.auth_method=="LAKEBASE_OAUTH_V1")) | .[0].name // empty')
if [ -n "$EXISTING" ]; then
  echo "  OAuth role already registered: $EXISTING"
else
  echo "  Registering OAuth role for service principal..."
  databricks postgres create-role "$BRANCH" \
    --json "{\"spec\": {\"postgres_role\": \"$SP_ID\", \"identity_type\": \"SERVICE_PRINCIPAL\", \"auth_method\": \"LAKEBASE_OAUTH_V1\"}}" \
    -p "$PROFILE" >/dev/null
fi

echo "Granting schema permissions..."
HOST=$(databricks postgres list-endpoints "$BRANCH" -p "$PROFILE" -o json | jq -r '.[0].status.hosts.host')
TOKEN=$(databricks postgres generate-database-credential "$LAKEBASE_ENDPOINT" -p "$PROFILE" -o json | jq -r '.token')
EMAIL=$(databricks current-user me -p "$PROFILE" -o json | jq -r '.userName')

PGPASSWORD="$TOKEN" psql "host=$HOST port=5432 dbname=databricks_postgres user=$EMAIL sslmode=require" \
  -v ON_ERROR_STOP=1 -q <<SQL
GRANT USAGE  ON SCHEMA public TO "$SP_ID";
GRANT CREATE ON SCHEMA public TO "$SP_ID";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES   TO "$SP_ID";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT, UPDATE         ON SEQUENCES TO "$SP_ID";
SQL

# The scheduled sync job may run before the app and create tables under the
# deployer's identity. Drop any worldcup_pool tables not owned by the app SP
# so the app's INIT_SCHEMA_ON_START can recreate them cleanly.
APP_TABLES="matches match_predictions tournament_predictions user_profiles app_users_cache"
echo "Resetting any pre-existing app tables not owned by the service principal..."
for tbl in $APP_TABLES; do
  PGPASSWORD="$TOKEN" psql "host=$HOST port=5432 dbname=databricks_postgres user=$EMAIL sslmode=require" \
    -v ON_ERROR_STOP=1 -tAc "
      SELECT tableowner FROM pg_tables WHERE schemaname='public' AND tablename='$tbl'
    " | while read owner; do
      if [ -n "$owner" ] && [ "$owner" != "$SP_ID" ]; then
        echo "  dropping public.$tbl (owned by $owner)"
        PGPASSWORD="$TOKEN" psql "host=$HOST port=5432 dbname=databricks_postgres user=$EMAIL sslmode=require" \
          -v ON_ERROR_STOP=1 -q -c "DROP TABLE IF EXISTS public.$tbl CASCADE;"
      fi
    done
done

echo "Done. Now run: databricks bundle run worldcup_pool_app -t $TARGET"
