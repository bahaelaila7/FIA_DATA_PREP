#!/usr/bin/env bash
# D80 frame: d78 (the D79 state) plus the direct prior refit with stand-structure features. d78 is copied, never modified.
#   bash nimiwae_d80_build.sh SRC_FRAME(d78) OUT_FRAME(d80) FIADB TRUTH_DB REPORT_DIR
# Steps: copy -> D79 baseline report (no stand features; adds the D80 diagnostics) -> D80 fit (stand features; exports af_mu / af_sd / af_model /
# age_oof) -> the D80 gate S1-S3.
set -euo pipefail
SRC=$1; OUT=$2; FIADB=$3; DB=$4; REP=$5
HERE=$(cd "$(dirname "$0")" && pwd); PY=${PY:-python}
[ -e "$OUT" ] && { echo "$OUT exists: refusing to overwrite"; exit 1; }
mkdir -p "$REP"; cp -r "$SRC" "$OUT"
A="--data $OUT --fiadb $FIADB --db $DB"
$PY -I "$HERE/nimiwae_age_direct.py" $A --out "$REP/d80_prior_base.json" --fld 1 --planted-model 1 --stand 0
$PY -I "$HERE/nimiwae_age_direct.py" $A --out "$REP/d80_prior_new.json" --fld 1 --planted-model 1 --stand 1 --export
$PY -I "$HERE/nimiwae_age_direct.py" --out "$REP/d80_prior_new.json" --compare80 "$REP/d80_prior_base.json" | tee "$REP/d80_prior_gate.json"
