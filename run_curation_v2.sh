#!/usr/bin/env bash
# run_curation_v2.sh — curation-fix 2026-10-26: the full chain into NEW output DBs.
#
# Every path comes from the environment (nothing is defaulted for inputs/outputs):
#   PY              python interpreter with duckdb/polars/scikit-learn   (required)
#   FIADB           raw FIADB.duckdb, attached READ_ONLY                  (required)
#   FIA_CURATED_DB  new curated DB (written)                              (required)
#   FIA_SIMB_DB     new sim-B side DB (written)                           (required)
#   FIA_ESTAGE_DIR  directory of *_TREEAGE.csv (estage source)            (required)
#   FIA_STATES      comma STATECD list, e.g. 1,12,13,21,28,37,45,47       (required)
#   FIA_COUNTIES    optional STATECD:COUNTYCD,... subset (smoke tests)
#   FROM_STEP       resume from this step number (default 1 = fresh run; see STEPS below)
#   TO_STEP         stop after this step number (default: all; e.g. reruns of steps 4-7)
#   SLIM_OUT        slim DB to build (optional; the slim step is skipped when empty)
#   SLIM_ECOS       comma EPA-L3 list for the slim builder                (with SLIM_OUT)
#   LANDIS_PROJECT  Julia project holding tools/build_slim_db_region.jl   (with SLIM_OUT)
#   JULIA_DEPOT_PATH  explicit Julia depot list — first entry writable    (with SLIM_OUT)
#   JULIA           julia executable (default: julia)
#
# Fresh run (FROM_STEP=1) refuses to overwrite existing outputs. To resume after a failure,
# rerun with FROM_STEP=<failed step>: every python step rewrites its own tables with
# CREATE OR REPLACE, so rerunning a step on the partial DB is safe. The slim builder refuses
# to overwrite: delete a partial $SLIM_OUT before resuming at the slim step.
# One heavy process at a time; DuckDB is capped by dbconn (FIA_DUCKDB_THREADS=3,
# FIA_DUCKDB_MEMORY=5GB by default); everything runs under `nice`.
set -euo pipefail
: "${PY:?}" "${FIADB:?}" "${FIA_CURATED_DB:?}" "${FIA_SIMB_DB:?}" "${FIA_ESTAGE_DIR:?}" "${FIA_STATES:?}"
FROM_STEP="${FROM_STEP:-1}"
TO_STEP="${TO_STEP:-99}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
export POLARS_MAX_THREADS="${POLARS_MAX_THREADS:-3}" OMP_NUM_THREADS="${OMP_NUM_THREADS:-3}"
if [[ -n "${SLIM_OUT:-}" ]]; then
  : "${SLIM_ECOS:?}" "${LANDIS_PROJECT:?}"
  : "${JULIA_DEPOT_PATH:?set JULIA_DEPOT_PATH explicitly (first entry = a writable depot you own)}"
fi
if [[ "$FROM_STEP" == 1 ]]; then
  for f in "$FIA_CURATED_DB" "$FIA_SIMB_DB" ${SLIM_OUT:+"$SLIM_OUT"}; do
    [[ -e "$f" ]] && { echo "refusing to overwrite existing $f (fresh run; use FROM_STEP=N to resume)"; exit 1; }
  done
else
  [[ -e "$FIA_CURATED_DB" ]] || { echo "FROM_STEP=$FROM_STEP but $FIA_CURATED_DB does not exist"; exit 1; }
fi
CUR=(--out-db "$FIA_CURATED_DB" --fiadb "$FIADB")
SIMB=(--out-db "$FIA_SIMB_DB" --curated-db "$FIA_CURATED_DB" --fiadb "$FIADB")
STEPS=(
  "fia_curation_v3.py ${CUR[*]} --states $FIA_STATES --estage-dir $FIA_ESTAGE_DIR ${FIA_COUNTIES:+--counties $FIA_COUNTIES}"
  "age_imputation.py ${CUR[*]}"
  "cohort_tracking.py ${CUR[*]}"
  "cohorts_landis.py ${CUR[*]}"
  "build_tree_trajectories.py ${CUR[*]}"
  "cohorts_landis2.py ${CUR[*]} --attach-only"
  "build_stdorg.py ${CUR[*]}"
  "export_reference_tables.py ${CUR[*]} --states $FIA_STATES"
  "cohorts_landis_simB.py ${SIMB[*]}"
  "attach_disturbance_simB.py ${SIMB[*]}"
)
# NOTE: paths must not contain spaces (steps are word-split).
for i in "${!STEPS[@]}"; do
  n=$((i + 1))
  (( n < FROM_STEP || n > TO_STEP )) && { echo "skip step $n: ${STEPS[$i]%% *}"; continue; }
  echo; echo "===== step $n: ${STEPS[$i]} ($(date -Is))"
  # shellcheck disable=SC2086
  nice -n 10 "$PY" -I ${STEPS[$i]}
done
SLIM_STEP=$(( ${#STEPS[@]} + 1 ))

if [[ -n "${SLIM_OUT:-}" ]] && (( SLIM_STEP >= FROM_STEP && SLIM_STEP <= TO_STEP )); then
  echo; echo "===== step $SLIM_STEP: slim builder ($(date -Is))"
  # Guarded wrapper around the UNCHANGED landis_region4 builder: its defaults point at the
  # old FIADB / old simB and it silently skips a missing simB, so both sources are passed
  # explicitly and checked before, and every expected table is checked after (audit M6).
  [[ -e "$SLIM_OUT" ]] && { echo "partial $SLIM_OUT exists — delete it to rerun the slim step"; exit 1; }
  nice -n 10 "$PY" -I slim_guard.py pre  --curated-db "$FIA_CURATED_DB" --simb-db "$FIA_SIMB_DB" --fiadb "$FIADB"
  nice -n 10 "${JULIA:-julia}" --project="$LANDIS_PROJECT" "$LANDIS_PROJECT/tools/build_slim_db_region.jl" \
      --out "$SLIM_OUT" --states "$FIA_STATES" --ecos "$SLIM_ECOS" \
      --fiadb "$FIA_CURATED_DB" --simb "$FIA_SIMB_DB"
  nice -n 10 "$PY" -I slim_guard.py post --slim "$SLIM_OUT"
fi
echo; echo "===== done ($(date -Is))"
