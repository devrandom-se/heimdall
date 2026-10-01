#!/usr/bin/env bash
# Copy a Heimdall database to a new PostgreSQL instance with pg_dump | psql, then verify.
#
# Runs ON the bastion (or any host that reaches both instances on 5432). Streams the dump, so it
# needs no local disk. Both instances must use the same user and password.
#
# Usage:
#   export PGPASSWORD=...            # or a ~/.pgpass entry for both hosts
#   nohup bash db-migrate.sh <old-host> <new-host> [dbname] [user] > ~/db-migrate.log 2>&1 &
#   tail -f ~/db-migrate.log
#
# Exit code 0 only when every table has the same row count on both sides and the indexes on
# `objects` match. See docs/database-migration.md for the full procedure.
set -euo pipefail

OLD_HOST="${1:?usage: db-migrate.sh <old-host> <new-host> [dbname] [user]}"
NEW_HOST="${2:?usage: db-migrate.sh <old-host> <new-host> [dbname] [user]}"
DB="${3:-heimdall}"
DBUSER="${4:-heimdall}"
TABLES="objects csvfiles backup_runs object_stats restore_log"

log() { printf '%s %s\n' "$(date -u +%FT%TZ)" "$*"; }
psql_old() { psql -h "$OLD_HOST" -U "$DBUSER" -d "$DB" -X -At -v ON_ERROR_STOP=1 "$@"; }
psql_new() { psql -h "$NEW_HOST" -U "$DBUSER" -d "$DB" -X -At -v ON_ERROR_STOP=1 "$@"; }

# 1. Client tools (Amazon Linux 2023 bastion)
if ! command -v pg_dump >/dev/null 2>&1; then
  log "Installing postgresql15 client tools"
  sudo dnf install -y -q postgresql15
fi
log "Using $(pg_dump --version)"

# 2. Preflight
if [ -z "${PGPASSWORD:-}" ] && [ ! -f "$HOME/.pgpass" ]; then
  log "ERROR: set PGPASSWORD or create ~/.pgpass"
  exit 1
fi
log "Old ($OLD_HOST): $(psql_old -c 'select version()')"
log "New ($NEW_HOST): $(psql_new -c 'select version()')"
log "Old database size: $(psql_old -c "select pg_size_pretty(pg_database_size('$DB'))")"
if [ -n "$(psql_new -c "select to_regclass('public.objects')")" ]; then
  log "ERROR: the target already has an objects table. This script only copies into an empty database."
  log "       If the target is a fresh instance that the app touched, reset it with:"
  log "       DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
  exit 1
fi

# 3. Copy. Plain format streams without temp files; pg_dump reads one consistent snapshot.
#    --no-owner/--no-privileges: same master user on both sides, nothing else to carry over.
#    --no-comments: avoids COMMENT ON EXTENSION, which RDS may reject for non-superusers.
#    maintenance_work_mem: speeds up the index builds that dominate the restore.
log "Copy start"
START=$(date +%s)
pg_dump -h "$OLD_HOST" -U "$DBUSER" -d "$DB" --no-owner --no-privileges --no-comments --format=plain \
  | PGOPTIONS='-c maintenance_work_mem=1GB' psql -h "$NEW_HOST" -U "$DBUSER" -d "$DB" -X -q -o /dev/null -v ON_ERROR_STOP=1
log "Copy done in $(( ($(date +%s) - START) / 60 )) min"

# 4. Verify
FAIL=0
for t in $TABLES; do
  o=$(psql_old -c "select count(*) from $t" 2>/dev/null || echo "missing")
  n=$(psql_new -c "select count(*) from $t" 2>/dev/null || echo "missing")
  if [ "$o" = "$n" ] && [ "$o" = "missing" ]; then
    log "OK   $t: table absent on both sides"
  elif [ "$o" = "$n" ]; then
    log "OK   $t: $o rows"
  else
    log "FAIL $t: old=$o new=$n"
    FAIL=1
  fi
done
o=$(psql_old -c "select count(*) from pg_indexes where tablename = 'objects'")
n=$(psql_new -c "select count(*) from pg_indexes where tablename = 'objects'")
if [ "$o" = "$n" ]; then
  log "OK   indexes on objects: $n"
else
  log "FAIL indexes on objects: old=$o new=$n"
  FAIL=1
fi

log "Running VACUUM ANALYZE on the new database (a dump carries no planner statistics)"
psql_new -c "VACUUM ANALYZE"
log "New database size: $(psql_new -c "select pg_size_pretty(pg_database_size('$DB'))")"

if [ "$FAIL" -eq 0 ]; then
  log "MIGRATION OK"
else
  log "MIGRATION FAILED - see FAIL lines above"
  exit 1
fi
