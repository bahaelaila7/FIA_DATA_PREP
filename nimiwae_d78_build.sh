#!/usr/bin/env bash
# D78 frame: d72 plus the l0 table, the rebuilt G1 truth and the refit direct prior (planted field-age targets). d72 is copied, never modified.
#   bash nimiwae_d78_build.sh SRC_FRAME OUT_FRAME FIADB TRUTH_DB REPORT_DIR
# Steps: copy -> nimiwae_l0 (table + per-tree l0) -> nimiwae70_refs (site refs on the new truth) -> baseline prior report (2 / 5 targets, no field ages,
# scored on the new truth: the P2 reference) -> new prior (field ages, l0 targets; exports af_mu / af_sd / age_oof) -> the D78 bars P1 / P2.
set -euo pipefail
SRC=$1; OUT=$2; FIADB=$3; DB=$4; REP=$5
HERE=$(cd "$(dirname "$0")" && pwd); PY=${PY:-python}
[ -e "$OUT" ] && { echo "$OUT exists: refusing to overwrite"; exit 1; }
mkdir -p "$REP"; cp -r "$SRC" "$OUT"
$PY -I "$HERE/nimiwae_l0.py" --fiadb "$FIADB" --data "$OUT"
$PY -I "$HERE/nimiwae70_refs.py" --data "$OUT" --fiadb "$FIADB" --db "$DB"
$PY -I "$HERE/nimiwae_age_direct.py" --data "$OUT" --fiadb "$FIADB" --db "$DB" --out "$REP/d78_prior_base.json" --targets-default --fld 0
$PY -I "$HERE/nimiwae_age_direct.py" --data "$OUT" --fiadb "$FIADB" --db "$DB" --out "$REP/d78_prior_new.json" --fld 1 --export
$PY -I "$HERE/nimiwae_age_direct.py" --out "$REP/d78_prior_new.json" --compare "$REP/d78_prior_base.json" | tee "$REP/d78_prior_bars.json"
