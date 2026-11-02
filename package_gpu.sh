#!/usr/bin/env bash
# Bundle what a GPU machine needs to train the D70 joint model. Usage: bash package_gpu.sh DATA_DIR OUT.tar
set -euo pipefail
DATA=$1; OUT=$2; W=$(mktemp -d); mkdir -p "$W/nimiwae70/data"
cp nimiwae56.py nimiwae70.py nimiwae70_dev.py nimiwae70_verify.py "$W/nimiwae70/"
cp "$DATA/seq.parquet" "$W/nimiwae70/data/"
cat > "$W/nimiwae70/requirements.txt" <<R
torch
numpy==2.4.6
pandas==3.0.3
pyarrow==24.0.0
R
cat > "$W/nimiwae70/run.sh" <<'R'
#!/usr/bin/env bash
# Usage: bash run.sh [device]   Three runs, one after the other. Copy the out folder back.
set -euo pipefail
D=${1:-cuda}
python -I nimiwae70_verify.py data | tail -1
python -I nimiwae70_dev.py --data data --out out/d70p_age1 --age 1 --opt adamw --trees 400000 --epochs 12 --device "$D" --threads 8
python -I nimiwae70_dev.py --data data --out out/d70p_age0 --age 0 --opt adamw --trees 400000 --epochs 12 --device "$D" --threads 8
python -I nimiwae70_dev.py --data data --out out/d70p_age1_s1 --age 1 --opt adamw --seed 1 --trees 400000 --epochs 12 --device "$D" --threads 8
R
tar -cf "$OUT" -C "$W" nimiwae70; rm -rf "$W"; tar -tvf "$OUT" | awk '{printf "%8.1f MB  %s\n", $3/1048576, $NF}'
