#!/usr/bin/env bash
# Bundle what a GPU machine needs for the D70 cross-validation; it needs no database. Usage: bash package_gpu.sh DATA_DIR OUT.tar
set -euo pipefail
DATA=$1; OUT=$2; W=$(mktemp -d); mkdir -p "$W/nimiwae70/data"
cp nimiwae56.py nimiwae_flow.py nimiwae70.py nimiwae70_score.py nimiwae70_cv.py nimiwae70_verify.py "$W/nimiwae70/"
cp "$DATA/seq.parquet" "$DATA/site_refs.parquet" "$DATA/site_refs_d70.parquet" "$DATA/planted_refs.parquet" "$DATA/ht_refs.parquet" "$W/nimiwae70/data/"
cat > "$W/nimiwae70/requirements.txt" <<R
torch
numpy==2.4.6
pandas==3.0.3
pyarrow==24.0.0
R
cat > "$W/nimiwae70/run.sh" <<'R'
#!/usr/bin/env bash
# Cross-validation on the GPU machine. Usage: [D74=1] [Q=flow] [G1RESCORE=out/cv72] bash run.sh [device]
#   default        Gaussian q, no D74, into out/cv72                       (Q=flow: out/cv72flow, D73)
#   D74=1          anchors + calibrated age-vigour coupling + HTCD heights, into out/cv74 (DECISIONS D74)
#   G1RESCORE=DIR  no training: score DIR's saved age-on checkpoints on both G1 sets into DIR_g1 (cv72 vs cv74 on the same trees), then exit
# Order for D74: G1RESCORE=out/cv72 bash run.sh cuda; D74=1 bash run.sh cuda; python -I nimiwae70_cv.py --out out/cv74 --compare out/cv72_g1
# Runs P at a time (P=5), CPU threads shared out, runs spread over GPUs. Finished runs are skipped, so it can be restarted. Copy the out dir back.
set -uo pipefail
D=${1:-cuda}; Q=${Q:-gauss}; D74=${D74:-0}; G1R=${G1RESCORE:-}; P=${P:-5}; FOLDS=${FOLDS:-0 1 2 3 4}; N=$(nproc); T=$(( N / P > 0 ? N / P : 1 )); G=$(nvidia-smi -L 2>/dev/null | wc -l); G=$(( G > 0 ? G : 1 ))
case "$Q" in gauss|flow) ;; *) echo "Q must be gauss or flow, not $Q"; exit 1;; esac
case "$D74" in 0) O=out/cv72;; 1) O=out/cv74;; *) echo "D74 must be 0 or 1, not $D74"; exit 1;; esac
[ "$Q" = flow ] && O=${O}flow
python -I nimiwae70_verify.py data | tail -1 || exit 1
python -I nimiwae70_cv.py --selftest || exit 1
if [ -n "$G1R" ]; then
  for f in $FOLDS; do [ -d "$G1R/f${f}_age1" ] || { echo "missing $G1R/f${f}_age1"; exit 1; }; done
  echo "rescoring $G1R on both G1 sets into ${G1R}_g1; $P at a time"
  for f in $FOLDS; do echo "$f"; done | xargs -P "$P" -L 1 --process-slot-var=SLOT bash -c 'CUDA_VISIBLE_DEVICES=$((SLOT % '"$G"')) python -I nimiwae70_cv.py --data data --out '"${G1R}_g1"' --fold $0 --rescore-g1 '"$G1R"' --device '"$D --threads $T"
  exit $?
fi
echo "q $Q, D74 $D74, into $O; $P at a time, $T CPU threads each, $G GPU(s)"
{ for a in 1 0; do for f in $FOLDS; do echo "$f $a"; done; done; } | xargs -P "$P" -L 1 --process-slot-var=SLOT bash -c 'CUDA_VISIBLE_DEVICES=$((SLOT % '"$G"')) python -I nimiwae70_cv.py --data data --out '"$O"' --fold $0 --age $1 --q '"$Q"' --d74 '"$D74"' --device '"$D --threads $T ${ARGS:-}"
rc=$?
python -I nimiwae70_cv.py --out "$O" --summary | tail -60
[ "$rc" -eq 0 ] || echo "SOME RUNS FAILED: rerun this script, finished runs are skipped. See $O/*/run.log"
R
tar -cf "$OUT" -C "$W" nimiwae70; rm -rf "$W"; tar -tvf "$OUT" | awk '{printf "%8.1f MB  %s\n", $3/1048576, $NF}'
