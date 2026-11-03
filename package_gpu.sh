#!/usr/bin/env bash
# Bundle what a GPU machine needs for the D70 cross-validation; it needs no database. Usage: bash package_gpu.sh DATA_DIR OUT.tar
set -euo pipefail
DATA=$1; OUT=$2; W=$(mktemp -d); mkdir -p "$W/nimiwae70/data"
cp nimiwae56.py nimiwae70.py nimiwae70_score.py nimiwae70_cv.py nimiwae70_verify.py "$W/nimiwae70/"
cp "$DATA/seq.parquet" "$DATA/site_refs.parquet" "$DATA/planted_refs.parquet" "$W/nimiwae70/data/"
cat > "$W/nimiwae70/requirements.txt" <<R
torch
numpy==2.4.6
pandas==3.0.3
pyarrow==24.0.0
R
cat > "$W/nimiwae70/run.sh" <<'R'
#!/usr/bin/env bash
# Usage: bash run.sh [device]   5 folds, age on and off, 36 epochs, scored at 12, 24 and 36. Finished runs are skipped, so it can be restarted.
# Runs 5 at a time by default: all age-on folds, then all age-off folds. P=2 bash run.sh cuda runs fewer at once; FOLDS="0 1" runs some folds only.
# With several GPUs, runs are spread over them. CPU threads are shared out across the runs. Each run also writes out/cv/fN_ageA/run.log. Copy the out/cv folder back.
set -uo pipefail
D=${1:-cuda}; P=${P:-5}; FOLDS=${FOLDS:-0 1 2 3 4}; N=$(nproc); T=$(( N / P > 0 ? N / P : 1 )); G=$(nvidia-smi -L 2>/dev/null | wc -l); G=$(( G > 0 ? G : 1 ))
python -I nimiwae70_verify.py data | tail -1 || exit 1
python -I nimiwae70_cv.py --selftest || exit 1
echo "$P at a time, $T CPU threads each, $G GPU(s)"
for a in 1 0; do for f in $FOLDS; do echo "$f $a"; done; done | xargs -P "$P" -L 1 --process-slot-var=SLOT bash -c 'CUDA_VISIBLE_DEVICES=$((SLOT % '"$G"')) python -I nimiwae70_cv.py --data data --out out/cv --fold $0 --age $1 --device '"$D --threads $T ${ARGS:-}"
rc=$?
python -I nimiwae70_cv.py --out out/cv --summary | tail -60
[ "$rc" -eq 0 ] || echo "SOME RUNS FAILED: rerun this script, finished runs are skipped. See out/cv/*/run.log"
R
tar -cf "$OUT" -C "$W" nimiwae70; rm -rf "$W"; tar -tvf "$OUT" | awk '{printf "%8.1f MB  %s\n", $3/1048576, $NF}'
