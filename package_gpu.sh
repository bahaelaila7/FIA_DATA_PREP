#!/usr/bin/env bash
# Bundle what a GPU machine needs for the D70 cross-validation; it needs no database. Usage: bash package_gpu.sh DATA_DIR OUT.tar
set -euo pipefail
DATA=$1; OUT=$2; W=$(mktemp -d); mkdir -p "$W/nimiwae70/data"
cp nimiwae56.py nimiwae_flow.py nimiwae70.py nimiwae70_score.py nimiwae70_cv.py nimiwae70_verify.py "$W/nimiwae70/"
cp "$DATA/seq.parquet" "$DATA/site_refs.parquet" "$DATA/planted_refs.parquet" "$W/nimiwae70/data/"
cat > "$W/nimiwae70/requirements.txt" <<R
torch
numpy==2.4.6
pandas==3.0.3
pyarrow==24.0.0
R
cat > "$W/nimiwae70/run.sh" <<'R'
#!/usr/bin/env bash
# D72 cross-validation into out/cv72 (Q=flow: the D73 flow proposal, into out/cv72flow). Usage: [Q=flow] bash run.sh [device] [OLD_CV]
# D72: run WITHOUT OLD_CV. The frame changed (lineage segments), so both arms retrain. OLD_CV (rescoring old age-off checkpoints) is only for a frame that did not change.
# Runs 5 at a time (P=...), CPU threads shared out, runs spread over GPUs. Finished runs are skipped, so it can be restarted. Copy $O back (out/cv72 or out/cv72flow).
set -uo pipefail
D=${1:-cuda}; OLD=${2:-}; Q=${Q:-gauss}; case "$Q" in gauss) O=out/cv72;; flow) O=out/cv72flow;; *) echo "Q must be gauss or flow, not $Q"; exit 1;; esac; P=${P:-5}; FOLDS=${FOLDS:-0 1 2 3 4}; N=$(nproc); T=$(( N / P > 0 ? N / P : 1 )); G=$(nvidia-smi -L 2>/dev/null | wc -l); G=$(( G > 0 ? G : 1 ))
python -I nimiwae70_verify.py data | tail -1 || exit 1
python -I nimiwae70_cv.py --selftest || exit 1
echo "q $Q into $O; $P at a time, $T CPU threads each, $G GPU(s)"
if [ -n "$OLD" ]; then
  for f in $FOLDS; do [ -f "$OLD/f${f}_age0/model_e36.pt" ] || { echo "missing $OLD/f${f}_age0/model_e36.pt: let the old run finish first"; exit 1; }; done
  ARMS="1"
else ARMS="1 0"; fi
{ for a in $ARMS; do for f in $FOLDS; do echo "$f $a train"; done; done; [ -n "$OLD" ] && for f in $FOLDS; do echo "$f 0 rescore"; done; } | xargs -P "$P" -L 1 --process-slot-var=SLOT bash -c 'R=""; [ "$2" = rescore ] && R="--rescore '"$OLD"'"; CUDA_VISIBLE_DEVICES=$((SLOT % '"$G"')) python -I nimiwae70_cv.py --data data --out '"$O"' --fold $0 --age $1 $R --q '"$Q"' --device '"$D --threads $T ${ARGS:-}"
rc=$?
python -I nimiwae70_cv.py --out "$O" --summary | tail -60
[ "$rc" -eq 0 ] || echo "SOME RUNS FAILED: rerun this script, finished runs are skipped. See $O/*/run.log"
R
tar -cf "$OUT" -C "$W" nimiwae70; rm -rf "$W"; tar -tvf "$OUT" | awk '{printf "%8.1f MB  %s\n", $3/1048576, $NF}'
