#!/usr/bin/env bash
# run_curation_v2.sh — curation-fix 2026-10-26: the full chain into NEW output DBs.
#
# Every path comes from the environment (no defaults are guessed for outputs):
#   PY              python interpreter with duckdb/polars/scikit-learn   (required)
#   FIADB           raw FIADB.duckdb, attached READ_ONLY                  (required)
#   FIA_CURATED_DB  new curated DB (written)                              (required)
#   FIA_SIMB_DB     new sim-B side DB (written)                           (required)
#   FIA_ESTAGE_DIR  directory of *_TREEAGE.csv (estage source)            (required)
#   FIA_STATES      comma STATECD list, e.g. 1,12,13,21,28,37,45,47       (required)
#   FIA_COUNTIES    optional STATECD:COUNTYCD,... subset (smoke tests)
#   SLIM_OUT        slim DB to build (optional; skipped when empty)
#   SLIM_ECOS       comma EPA-L3 list for the slim builder (with SLIM_OUT)
#   LANDIS_PROJECT  Julia project holding tools/build_slim_db_region.jl (with SLIM_OUT)
#   JULIA           julia executable (default: julia)
#
# One heavy process at a time; DuckDB is capped by dbconn (FIA_DUCKDB_THREADS=3,
# FIA_DUCKDB_MEMORY=5GB by default); everything runs under `nice`.
set -euo pipefail
: "${PY:?}" "${FIADB:?}" "${FIA_CURATED_DB:?}" "${FIA_SIMB_DB:?}" "${FIA_ESTAGE_DIR:?}" "${FIA_STATES:?}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
export POLARS_MAX_THREADS="${POLARS_MAX_THREADS:-3}" OMP_NUM_THREADS="${OMP_NUM_THREADS:-3}"
for f in "$FIA_CURATED_DB" "$FIA_SIMB_DB" ${SLIM_OUT:+"$SLIM_OUT"}; do
  [[ -e "$f" ]] && { echo "refusing to overwrite existing $f"; exit 1; }
done
run() { echo; echo "===== $* ($(date -Is))"; nice -n 10 "$PY" -I "$@"; }
CUR=(--out-db "$FIA_CURATED_DB" --fiadb "$FIADB")
SIMB=(--out-db "$FIA_SIMB_DB" --curated-db "$FIA_CURATED_DB" --fiadb "$FIADB")

run fia_curation_v3.py "${CUR[@]}" --states "$FIA_STATES" --estage-dir "$FIA_ESTAGE_DIR" \
    ${FIA_COUNTIES:+--counties "$FIA_COUNTIES"}
run age_imputation.py         "${CUR[@]}"
run cohort_tracking.py        "${CUR[@]}"
run cohorts_landis.py         "${CUR[@]}"
run build_tree_trajectories.py "${CUR[@]}"
run cohorts_landis2.py        "${CUR[@]}" --attach-only       # land_use (4-way, replaced next) + cycle
run build_stdorg.py           "${CUR[@]}"                     # -> curated_cohorts_landis_stdorg
run export_reference_tables.py "${CUR[@]}" --states "$FIA_STATES"   # COND/PLOT/REF_SPECIES/... for the slim builder
run cohorts_landis_simB.py    "${SIMB[@]}"                    # -> curated_cohorts_landis_simB (side DB)
run attach_disturbance_simB.py "${SIMB[@]}"                   # -> cohort_disturbance + disturbance_drop_pct

if [[ -n "${SLIM_OUT:-}" ]]; then
  : "${SLIM_ECOS:?}" "${LANDIS_PROJECT:?}"
  echo; echo "===== slim builder ($(date -Is))"
  nice -n 10 "${JULIA:-julia}" --project="$LANDIS_PROJECT" "$LANDIS_PROJECT/tools/build_slim_db_region.jl" \
      --out "$SLIM_OUT" --states "$FIA_STATES" --ecos "$SLIM_ECOS" \
      --fiadb "$FIA_CURATED_DB" --simb "$FIA_SIMB_DB"
fi
echo; echo "===== done ($(date -Is))"
