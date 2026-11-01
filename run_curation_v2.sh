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
#   FIA_COUNTIES    optional STATECD:COUNTYCD,... subset (smoke tests; by CANONICAL county)
#   FROM_STEP       resume from this step number (default 1 = fresh run; see STEPS below)
#   TO_STEP         stop after this step number (default: all; e.g. reruns of steps 4-10)
#   SLIM_OUT        slim DB to build (optional; the slim step is skipped when empty)
#   SLIM_ECOS       comma EPA-L3 list for the slim builder                (with SLIM_OUT)
#   SLIM_REF        reference slim DB (read-only): post-check that its schema is an exact
#                   prefix of the new slim's (appended columns only)      (optional, r5 M-7)
#   LANDIS_PROJECT  Julia project holding tools/build_slim_db_region.jl   (with SLIM_OUT)
#   JULIA_DEPOT_PATH  explicit Julia depot list — first entry writable    (with SLIM_OUT)
#   JULIA           julia executable (default: julia)
#
# Fresh run (FROM_STEP=1) refuses to overwrite existing outputs. To resume after a failure,
# rerun with FROM_STEP=<failed step>: every python step rewrites its own tables with
# CREATE OR REPLACE, so rerunning a step on the partial DB is safe. r7 D44 (chain D F4 / F10):
# a step that fails after CREATING its output DB (step 1: the curated DB, step 9: the Sim B DB)
# has that partial file removed, so FROM_STEP=<that step> works; a failed slim build / post-check
# moves the partial slim to $SLIM_OUT.failed.
# r7 D44 (F7): every finished step is logged (slim_guard.py mark -> run_steps, with the step-1
# run_id); the slim step refuses sources from another run or with a stale partial rerun.
# r7 D44 (F8): every step gets --states, and a stage refuses a DB whose run_scope differs.
# One heavy process at a time; DuckDB is capped by dbconn (FIA_DUCKDB_THREADS=3,
# FIA_DUCKDB_MEMORY=5GB by default), slim_guard and the Julia builder by the same variables
# (r7 F11); everything runs under `nice`.
set -euo pipefail
: "${PY:?}" "${FIADB:?}" "${FIA_CURATED_DB:?}" "${FIA_SIMB_DB:?}" "${FIA_ESTAGE_DIR:?}" "${FIA_STATES:?}"
FROM_STEP="${FROM_STEP:-1}"
TO_STEP="${TO_STEP:-99}"
NSTEPS=10                      # python steps; the slim builder is step NSTEPS + 1
SLIM_STEP=$((NSTEPS + 1))
# r7 D44 (F4): step numbers are compared NUMERICALLY ("01" or "0" no longer bypass the
# fresh-run refusal)
for v in FROM_STEP TO_STEP; do
  [[ "${!v}" =~ ^[0-9]+$ ]] || { echo "$v=${!v} is not a step number"; exit 1; }
done
FROM_STEP=$((10#$FROM_STEP)); TO_STEP=$((10#$TO_STEP))
(( FROM_STEP >= 1 && FROM_STEP <= SLIM_STEP )) || { echo "FROM_STEP=$FROM_STEP: steps are 1..$SLIM_STEP"; exit 1; }
(( TO_STEP >= FROM_STEP )) || { echo "TO_STEP=$TO_STEP < FROM_STEP=$FROM_STEP"; exit 1; }
# r7 D44 (F6): every path is made absolute BEFORE the cd into the repository
absp() { [[ -z "$1" ]] && { echo ""; return; }; [[ "$1" = /* ]] && echo "$1" || echo "$PWD/$1"; }
[[ "$PY" == */* ]] && PY=$(absp "$PY")      # a bare name (python3) is resolved by PATH
JULIA="${JULIA:-julia}"; [[ "$JULIA" == */* ]] && JULIA=$(absp "$JULIA")
FIADB=$(absp "$FIADB"); FIA_CURATED_DB=$(absp "$FIA_CURATED_DB"); FIA_SIMB_DB=$(absp "$FIA_SIMB_DB")
FIA_ESTAGE_DIR=$(absp "$FIA_ESTAGE_DIR")
SLIM_OUT=$(absp "${SLIM_OUT:-}"); SLIM_REF=$(absp "${SLIM_REF:-}")
[[ -n "${LANDIS_PROJECT:-}" ]] && LANDIS_PROJECT=$(absp "$LANDIS_PROJECT")
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
export POLARS_MAX_THREADS="${POLARS_MAX_THREADS:-3}" OMP_NUM_THREADS="${OMP_NUM_THREADS:-3}"
export FIA_DUCKDB_THREADS="${FIA_DUCKDB_THREADS:-3}" FIA_DUCKDB_MEMORY="${FIA_DUCKDB_MEMORY:-5GB}"
if [[ -n "$SLIM_OUT" ]]; then
  : "${SLIM_ECOS:?}" "${LANDIS_PROJECT:?}"
  : "${JULIA_DEPOT_PATH:?set JULIA_DEPOT_PATH explicitly (first entry = a writable depot you own)}"
elif (( FROM_STEP > NSTEPS )); then
  # r7 D44 (F5): FROM_STEP=11 without SLIM_OUT used to exit 0 having run nothing
  echo "FROM_STEP=$FROM_STEP is the slim step, but SLIM_OUT is empty: nothing to do"; exit 1
fi
if (( FROM_STEP == 1 )); then
  for f in "$FIA_CURATED_DB" "$FIA_SIMB_DB" ${SLIM_OUT:+"$SLIM_OUT"}; do
    [[ -e "$f" ]] && { echo "refusing to overwrite existing $f (fresh run; use FROM_STEP=N to resume)"; exit 1; }
  done
else
  [[ -e "$FIA_CURATED_DB" ]] || { echo "FROM_STEP=$FROM_STEP but $FIA_CURATED_DB does not exist"; exit 1; }
  # r7 D44 (F10): step 10 (and the slim step) need the Sim B DB that step 9 writes
  if (( FROM_STEP >= 10 )) && [[ ! -e "$FIA_SIMB_DB" ]]; then
    echo "FROM_STEP=$FROM_STEP but $FIA_SIMB_DB does not exist (run from step 9)"; exit 1
  fi
fi
CUR=(--out-db "$FIA_CURATED_DB" --fiadb "$FIADB" --states "$FIA_STATES")
SIMB=(--out-db "$FIA_SIMB_DB" --curated-db "$FIA_CURATED_DB" --fiadb "$FIADB" --states "$FIA_STATES")
STEPS=(
  "fia_curation_v3.py ${CUR[*]} --estage-dir $FIA_ESTAGE_DIR ${FIA_COUNTIES:+--counties $FIA_COUNTIES}"
  "age_imputation.py ${CUR[*]}"
  "cohort_tracking.py ${CUR[*]}"
  "cohorts_landis.py ${CUR[*]}"
  "build_tree_trajectories.py ${CUR[*]}"
  "cohorts_landis2.py ${CUR[*]} --attach-only"
  "build_stdorg.py ${CUR[*]}"
  "export_reference_tables.py ${CUR[*]}"
  "cohorts_landis_simB.py ${SIMB[*]}"
  "attach_disturbance_simB.py ${SIMB[*]}"
)
(( ${#STEPS[@]} == NSTEPS )) || { echo "internal: ${#STEPS[@]} steps != NSTEPS"; exit 1; }
# step -> the DB it writes (step 1 creates the curated DB, step 9 the Sim B DB)
out_of() { (( $1 >= 9 )) && echo "$FIA_SIMB_DB" || echo "$FIA_CURATED_DB"; }
rm_db() { rm -f "$1" "$1.wal"; }
# NOTE: paths must not contain spaces (steps are word-split).
for i in "${!STEPS[@]}"; do
  n=$((i + 1))
  (( n < FROM_STEP || n > TO_STEP )) && { echo "skip step $n: ${STEPS[$i]%% *}"; continue; }
  echo; echo "===== step $n: ${STEPS[$i]} ($(date -Is))"
  out=$(out_of "$n"); fresh=0; [[ -e "$out" ]] || fresh=1
  # shellcheck disable=SC2086
  if ! nice -n 10 "$PY" -I ${STEPS[$i]}; then
    if (( fresh )); then echo "step $n failed: removing the partial $out it created"; rm_db "$out"; fi
    echo "step $n FAILED"; exit 1
  fi
  if (( n >= 9 )); then mk=(--db "$FIA_SIMB_DB" --curated-db "$FIA_CURATED_DB"); else mk=(--db "$FIA_CURATED_DB"); fi
  nice -n 10 "$PY" -I slim_guard.py mark "${mk[@]}" --step "$n" --script "${STEPS[$i]%% *}"
  # row-order test (D66): every table is rewritten in a random order before the next step reads it
  if [[ -n "${FIA_SHUFFLE_SEED:-}" ]]; then "$PY" -I fia_ids.py shuffle --db "$out" --seed "$((FIA_SHUFFLE_SEED + n))"; fi
done

if [[ -n "$SLIM_OUT" ]] && (( SLIM_STEP >= FROM_STEP && SLIM_STEP <= TO_STEP )); then
  echo; echo "===== step $SLIM_STEP: slim builder ($(date -Is))"
  # Guarded wrapper around the UNCHANGED landis_region4 builder: its defaults point at the
  # old FIADB / old simB and it silently skips a missing simB, so both sources are passed
  # explicitly and checked before, and every expected table is checked after (audit M6).
  [[ -e "$SLIM_OUT" ]] && { echo "partial $SLIM_OUT exists — delete it to rerun the slim step"; exit 1; }
  nice -n 10 "$PY" -I slim_guard.py pre --curated-db "$FIA_CURATED_DB" --simb-db "$FIA_SIMB_DB" \
      --fiadb "$FIADB" --states "$FIA_STATES"
  # r7 D44 (F11): DuckDB.jl sets DuckDB's threads to Julia's thread count, so julia runs with
  # --threads=FIA_DUCKDB_THREADS; the builder's own DuckDB.DB(path) gets memory_limit =
  # FIA_DUCKDB_MEMORY (a String method that wins dispatch over DuckDB.jl's AbstractString one;
  # the builder itself is unchanged)
  JL_CAP='import DuckDB
DuckDB.DBInterface.connect(::Type{DuckDB.DB}, f::String) = (cfg = DuckDB.Config();
    cfg["memory_limit"] = ENV["FIA_DUCKDB_MEMORY"]; DuckDB.DB(f, cfg))
const BUILDER = popfirst!(ARGS)
include(BUILDER)'
  slim_fail() { echo "slim step failed: moving the partial slim to $SLIM_OUT.failed"; rm_db "$SLIM_OUT.failed"
                [[ -e "$SLIM_OUT" ]] && mv "$SLIM_OUT" "$SLIM_OUT.failed"; rm -f "$SLIM_OUT.wal"; exit 1; }
  nice -n 10 "$JULIA" --project="$LANDIS_PROJECT" --threads="$FIA_DUCKDB_THREADS" -e "$JL_CAP" \
      "$LANDIS_PROJECT/tools/build_slim_db_region.jl" \
      --out "$SLIM_OUT" --states "$FIA_STATES" --ecos "$SLIM_ECOS" \
      --fiadb "$FIA_CURATED_DB" --simb "$FIA_SIMB_DB" || slim_fail
  nice -n 10 "$PY" -I slim_guard.py post --slim "$SLIM_OUT" --curated-db "$FIA_CURATED_DB" \
      --simb-db "$FIA_SIMB_DB" ${SLIM_REF:+--ref-slim "$SLIM_REF"} || slim_fail
fi
echo; echo "===== done ($(date -Is))"
