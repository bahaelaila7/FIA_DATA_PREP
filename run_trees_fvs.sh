#!/usr/bin/env bash
# Build curated_trees_fvs and print a sanity summary for the test ecoregion.
# Usage: bash run_trees_fvs.sh [DB_PATH] [ECO]
# Then tell Claude to read FIA_DATA_PREP/tmp/trees_fvs.log
set -u
cd "$(dirname "$0")"
DB="${1:-../FIASQLITE2PGSQL/FIADB.duckdb}"
ECO="${2:-8.3.5.75g}"
mkdir -p tmp
LOG=tmp/trees_fvs.log
echo "DB=$DB ECO=$ECO" | tee "$LOG"
python trees_fvs.py "$DB" "$ECO" >>"$LOG" 2>&1
echo "exit=$?" >>"$LOG"
echo "DONE -> $LOG"
