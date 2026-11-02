#!/usr/bin/env bash
# Bundle what a GPU machine needs to train the D70 joint model. Usage: bash package_gpu.sh DATA_DIR OUT.tar
set -euo pipefail
DATA=$1; OUT=$2; W=$(mktemp -d); mkdir -p "$W/nimiwae70/data"
cp nimiwae56.py nimiwae70.py nimiwae70_dev.py nimiwae70_verify.py "$W/nimiwae70/"
cp "$DATA/seq.parquet" "$DATA/age_rec.parquet" "$W/nimiwae70/data/"
cat > "$W/nimiwae70/requirements.txt" <<R
torch
numpy==2.4.6
pandas==3.0.3
polars==1.41.2
pyarrow==24.0.0
R
cat > "$W/nimiwae70/run.sh" <<'R'
#!/usr/bin/env bash
# Usage: bash run.sh [device]   Two arms, one after the other. Copy out/*/model.pt and run.log back.
set -euo pipefail
D=${1:-cuda}
python -I nimiwae70_verify.py | tail -1
python -I nimiwae70_dev.py --data data --out out/d70_dev_age1 --age 1 --trees 150000 --epochs 4 --device "$D" --threads 8
python -I nimiwae70_dev.py --data data --out out/d70_dev_age0 --age 0 --trees 150000 --epochs 4 --device "$D" --threads 8
R
tar -cf "$OUT" -C "$W" nimiwae70; rm -rf "$W"; tar -tvf "$OUT" | awk '{printf "%8.1f MB  %s\n", $3/1048576, $NF}'
