#!/usr/bin/env bash
# Apply the durable consumer state schema (spec 003, T3, D11).
#
# WHY THIS EXISTS ALONGSIDE THE COMPOSE MOUNT
# docker-compose.yml mounts scripts/state_schema.sql into the postgres container's
# /docker-entrypoint-initdb.d/, which runs it automatically — but ONLY when the data
# volume is empty. After the first `docker compose up`, editing the schema and running
# `up` again does nothing at all, silently. This script is the path that works then.
#
# One schema file, two entry points, so the compose schema and the host schema cannot
# drift.
#
# Usage:
#   scripts/apply_state_schema.sh            # via `docker compose exec postgres`
#   PSQL_MODE=host scripts/apply_state_schema.sh   # via a local psql on localhost:5432
#
# Credentials come from the environment only (R3.24) — the same .env docker compose
# reads. No defaults are supplied for the password.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCHEMA="$ROOT/scripts/state_schema.sql"

[[ -f "$SCHEMA" ]] || { echo "schema file not found: $SCHEMA" >&2; exit 1; }

# shellcheck disable=SC1091
[[ -f "$ROOT/.env" ]] && set -a && source "$ROOT/.env" && set +a

: "${POSTGRES_USER:?set POSTGRES_USER (see .env.example)}"
: "${POSTGRES_PASSWORD:?set POSTGRES_PASSWORD (see .env.example)}"
: "${POSTGRES_DB:?set POSTGRES_DB (see .env.example)}"

SERVICE="${POSTGRES_SERVICE:-postgres}"
MODE="${PSQL_MODE:-compose}"

echo "applying $(basename "$SCHEMA") to database '$POSTGRES_DB' as '$POSTGRES_USER' (mode: $MODE)"

if [[ "$MODE" == "host" ]]; then
  command -v psql >/dev/null || { echo "psql not found on PATH — use the default compose mode" >&2; exit 1; }
  PGPASSWORD="$POSTGRES_PASSWORD" psql \
    --host "${POSTGRES_HOST:-localhost}" \
    --port "${POSTGRES_PORT:-5432}" \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --set ON_ERROR_STOP=1 \
    --file "$SCHEMA"
else
  if ! docker compose ps --status running --services 2>/dev/null | grep -qx "$SERVICE"; then
    echo "postgres is not running — start it with: docker compose up -d postgres" >&2
    exit 1
  fi
  docker compose exec -T \
    -e PGPASSWORD="$POSTGRES_PASSWORD" \
    "$SERVICE" \
    psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --set ON_ERROR_STOP=1 \
    < "$SCHEMA"
fi

echo
echo "order_fold is ready. Inspect it with:"
echo "  docker compose exec postgres psql -U \"\$POSTGRES_USER\" -d \"\$POSTGRES_DB\" -c 'TABLE order_fold;'"
