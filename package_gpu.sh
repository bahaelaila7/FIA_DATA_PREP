#!/usr/bin/env bash
# Bundle what a GPU machine needs for the D70 cross-validation; it needs no database. Usage: bash package_gpu.sh DATA_DIR OUT.tar
set -euo pipefail
DATA=$1; OUT=$2; W=$(mktemp -d); mkdir -p "$W/nimiwae70/data"
cp nimiwae56.py nimiwae_flow.py nimiwae70.py nimiwae70_score.py nimiwae70_cv.py nimiwae70_verify.py nimiwae70_flowcheck.py nimiwae70_gatepull.py "$W/nimiwae70/"
mkdir -p "$W/nimiwae70/nimiwae_jax" && cp nimiwae_jax/*.py "$W/nimiwae70/nimiwae_jax/"
cp "$DATA/seq.parquet" "$DATA/site_refs.parquet" "$DATA/site_refs_d70.parquet" "$DATA/planted_refs.parquet" "$DATA/ht_refs.parquet" "$W/nimiwae70/data/"
cat > "$W/nimiwae70/requirements.txt" <<R
torch
# BACKEND=jax only (D75): pip install -U "jax[cuda12]" flax optax   (tested with jax 0.12.0.1, flax 0.12.12, optax 0.2.8)
numpy==2.4.6
pandas==3.0.3
pyarrow==24.0.0
R
cat > "$W/nimiwae70/run.sh" <<'R'
#!/usr/bin/env bash
# Cross-validation on the GPU machine. Usage: [D74=1] [Q=flow] [BACKEND=jax] [G1RESCORE=out/cv72] [GRIDRESCORE=out/cv74flow] bash run.sh [device]
#   default        Gaussian q, no D74, into out/cv72                       (Q=flow: out/cv72flow, D73)
#   D74=1          anchors + calibrated age-vigour coupling + HTCD heights, into out/cv74 (DECISIONS D74)
#   D74=1 D76=1    D74 plus the crew-miss rate (DECISIONS D76), into out/cv76 (with Q=flow: out/cv76flow, compared against cv74flow)
#   BACKEND=jax    train with nimiwae_jax (D75), score with the same PyTorch code; output folder gets _jax (e.g. out/cv74flow_jax)
#   G1RESCORE=DIR  no training: score DIR's saved age-on checkpoints on both G1 sets into DIR_g1 (cv72 vs cv74 on the same trees), then exit
#   POST=d76       no training: everything after cv76flow in one go (needs out/cv76flow and out/cv74flow, both finished): gatepull on both,
#                  eps x0.5 and x2 on cv76flow, cv74flow's grid completion (in parallel over the GPUs; finished pieces are skipped on a rerun),
#                  then the D76 verdict (out/cv76flow/d76_compare.json) and the summary with baselines and age_limits. Then copy both folders back.
#   PROBE=d82      the capacity / data probe (DECISIONS D82) on fold 0, age on: base, deep (4 + 4 layers), wide (width 128), all trees, 24 epochs,
#                  then the rule (out/d82_compare.json). The base configuration is D74 + flow, plus D76 when D76=1 (set it from D76's verdict).
#                  Build this bundle from the frame the next CV uses (d78). Resumable; finished arms are skipped.
#   XCHECK=d78     no training: rescore out/cv76flow and out/cv74flow (copied here, finished) on THIS bundle's frame, which must be d78, with the
#                  same checkpoints: how much the new frame alone (l0 table, planted prior) moves the D76 gate-pull check. Tagged d78 (the D76
#                  verdict's untagged files are never touched), then both runs' gatepull files side by side. Build this bundle from d78.
#   DUMP=d84       no training (DECISIONS D84): the residual gate pulls of out/cv76flow on THIS bundle's frame (d78): every pulled tree plus
#                  controls with their empty-visit history (gatepull --dump, tagged d78dump), then the pre-registered prediction (--analyse-dump).
#                  Its counts must equal XCHECK's (both are printed). One GPU process; it can share the GPU with a running probe.
#   GRIDRESCORE=DIR  no training: D77 grid completion of DIR's last age-on checkpoints (DIR/f*_age1/scores_grid.json; runs from now on score it themselves), then exit
# Order for D74: G1RESCORE=out/cv72 bash run.sh cuda; D74=1 bash run.sh cuda; python -I nimiwae70_cv.py --out out/cv74 --compare out/cv72_g1
# Order for D76: Q=flow D74=1 D76=1 bash run.sh cuda; then for R in out/cv76flow out/cv74flow: python -I nimiwae70_gatepull.py --data data --run $R --device cuda;
#   python -I nimiwae70_gatepull.py --data data --run out/cv76flow --device cuda --eps-scale 0.5 (and 2: reported, not bars);
#   python -I nimiwae70_cv.py --out out/cv76flow --d76-base out/cv74flow (every D76 bar, into out/cv76flow/d76_compare.json)
#   D77 (reported): GRIDRESCORE=out/cv74flow bash run.sh cuda before the comparison, so it shows grid completion deltas too
# Any run: python -I nimiwae70_cv.py --out DIR --summary --data data prints G1 against the lookup baselines (D77)
# Runs P at a time (P=5), CPU threads shared out, runs spread over GPUs. Finished runs are skipped, so it can be restarted. Copy the out dir back.
set -uo pipefail
D=${1:-cuda}; Q=${Q:-gauss}; D74=${D74:-0}; D76=${D76:-0}; BK=${BACKEND:-torch}; G1R=${G1RESCORE:-}; GRR=${GRIDRESCORE:-}; POST=${POST:-}; PROBE=${PROBE:-}; XCHECK=${XCHECK:-}; DUMP=${DUMP:-}; P=${P:-5}; FOLDS=${FOLDS:-0 1 2 3 4}; N=$(nproc); T=$(( N / P > 0 ? N / P : 1 )); G=$(nvidia-smi -L 2>/dev/null | wc -l); G=$(( G > 0 ? G : 1 ))
[ -n "$PROBE" ] && { Q=flow; D74=1; }                                     # D82: the probe's base is always D74 + flow (D76 from the env)
case "$Q" in gauss|flow) ;; *) echo "Q must be gauss or flow, not $Q"; exit 1;; esac
case "$D74$D76" in 00) O=out/cv72;; 10) O=out/cv74;; 11) O=out/cv76;; *) echo "D74 / D76 must be 0 or 1, and D76 needs D74=1 (got D74=$D74 D76=$D76)"; exit 1;; esac
[ "$Q" = flow ] && O=${O}flow
case "$BK" in torch) ;; jax) O=${O}_jax; export XLA_PYTHON_CLIENT_PREALLOCATE=false
  python -I nimiwae_jax/equivalence.py data | tail -1 || exit 1;;   # the JAX port must match PyTorch on this machine first
  *) echo "BACKEND must be torch or jax, not $BK"; exit 1;; esac
python -I nimiwae70_verify.py data | tail -1 || exit 1
python -I nimiwae70_cv.py --selftest || exit 1
if [ -n "$DUMP" ]; then
  [ "$DUMP" = d84 ] || { echo "DUMP must be d84, not $DUMP"; exit 1; }
  python -I -c "import sys, pyarrow.parquet as pq; sys.exit(0 if 'l0' in pq.read_schema('data/seq.parquet').names else 1)" || { echo "this bundle's data is not a d78 frame: build it from d78"; exit 1; }
  r=out/cv76flow; for f in 0 1 2 3 4; do [ -f "$r/f${f}_age1/model_e36.pt" ] || { echo "missing $r/f${f}_age1/model_e36.pt (copy the finished run here)"; exit 1; }; done
  if [ -f "$r/gatepull_e36_d78dump_dump.parquet" ]; then echo "have $r/gatepull_e36_d78dump_dump.parquet"
  else
    echo "dumping the gate pulls of $r on the d78 frame (about 40 min)"
    python -I nimiwae70_gatepull.py --data data --run "$r" --epoch 36 --tag d78dump --dump --device "$D" --threads "$T" > "$r/gatepull_e36_d78dump.log" 2>&1 \
      || { echo "the dump FAILED: see $r/gatepull_e36_d78dump.log"; exit 1; }
  fi
  python -I nimiwae70_gatepull.py --analyse-dump "$r/gatepull_e36_d78dump_dump.parquet" || { echo "the analysis FAILED"; exit 1; }
  python -I nimiwae70_gatepull.py --report "$r"
  echo "done: copy $r/gatepull_e36_d78dump.json, $r/gatepull_e36_d78dump_dump.parquet and $r/gatepull_e36_d78dump_dump_analysis.json back"
  exit 0
fi
if [ -n "$XCHECK" ]; then
  [ "$XCHECK" = d78 ] || { echo "XCHECK must be d78, not $XCHECK"; exit 1; }
  python -I -c "import sys, pyarrow.parquet as pq; sys.exit(0 if 'l0' in pq.read_schema('data/seq.parquet').names else 1)" || { echo "this bundle's data is not a d78 frame: build it from d78"; exit 1; }
  for r in out/cv76flow out/cv74flow; do for f in 0 1 2 3 4; do [ -f "$r/f${f}_age1/model_e36.pt" ] || { echo "missing $r/f${f}_age1/model_e36.pt (copy the finished run here)"; exit 1; }; done; done
  pids=(); i=0
  for r in out/cv76flow out/cv74flow; do
    if [ -f "$r/gatepull_e36_d78.json" ]; then echo "have $r/gatepull_e36_d78.json"; continue; fi
    echo "gatepull on the d78 frame -> $r/gatepull_e36_d78.json"
    CUDA_VISIBLE_DEVICES=$(( i % G )) python -I nimiwae70_gatepull.py --data data --run "$r" --epoch 36 --tag d78 --device "$D" --threads "$T" > "$r/gatepull_e36_d78.log" 2>&1 &
    pids+=($!); i=$(( i + 1 ))
  done
  rc=0; for p in ${pids[@]+"${pids[@]}"}; do wait "$p" || rc=1; done
  [ "$rc" -eq 0 ] || { echo "a gatepull FAILED: see out/cv7*flow/gatepull_e36_d78.log; rerun XCHECK=d78"; exit 1; }
  for r in out/cv76flow out/cv74flow; do python -I nimiwae70_gatepull.py --report "$r"; done
  echo "done: copy out/cv76flow/gatepull_e36_d78.json and out/cv74flow/gatepull_e36_d78.json back"
  exit 0
fi
if [ -n "$PROBE" ]; then
  [ "$PROBE" = d82 ] || { echo "PROBE must be d82, not $PROBE"; exit 1; }
  echo "D82 probe on fold 0: Q=flow D74=1 D76=$D76, 24 epochs; 4 arms, $P at a time, $G GPU(s)"
  printf '%s\n' "base" "deep --layers 4" "wide --width 128" "alltrees --trees 0" | xargs -P "$P" -L 1 --process-slot-var=SLOT bash -c \
    'CUDA_VISIBLE_DEVICES=$((SLOT % '"$G"')) python -I nimiwae70_cv.py --data data --out out/d82_$0 --fold 0 --age 1 --q flow --d74 1 --d76 '"$D76"' --ckpts 12,24 --device '"$D"' --threads '"$T"' "$@"' \
    || { echo "an arm FAILED: see out/d82_*/f0_age1/run.log; rerun PROBE=d82 (finished arms are skipped, others resume)"; exit 1; }
  pids=(); i=0                                                                       # reported (D82 + the tabled D76 question): fold-0 gate pulls per arm, epoch 24
  for arm in base deep wide alltrees; do
    r=out/d82_$arm; [ -f "$r/gatepull_e24_probe.json" ] && continue
    CUDA_VISIBLE_DEVICES=$(( i % G )) python -I nimiwae70_gatepull.py --data data --run "$r" --epoch 24 --folds 0 --tag probe --device "$D" --threads "$T" > "$r/gatepull_e24_probe.log" 2>&1 &
    pids+=($!); i=$(( i + 1 ))
  done
  for p in ${pids[@]+"${pids[@]}"}; do wait "$p" || { echo "a probe gatepull FAILED: see out/d82_*/gatepull_e24_probe.log; rerun PROBE=d82"; exit 1; }; done
  python -I nimiwae70_cv.py --out out --compare82 || { echo "the D82 comparison FAILED"; exit 1; }
  echo "done: copy out/d82_* and out/d82_compare.json back"
  exit 0
fi
if [ -n "$POST" ]; then
  [ "$POST" = d76 ] || { echo "POST must be d76, not $POST"; exit 1; }
  N76=out/cv76flow; B74=out/cv74flow; ALL="0 1 2 3 4"                       # the D76 bars need every fold: FOLDS is ignored here
  for r in $N76 $B74; do for f in $ALL; do for a in 1 0; do
    [ -f "$r/f${f}_age$a/done.json" ] || { echo "missing $r/f${f}_age$a/done.json: that run has not finished (copy cv74flow here; train cv76flow first)"; exit 1; }
    [ -f "$r/f${f}_age1/model_e36.pt" ] || { echo "missing $r/f${f}_age1/model_e36.pt"; exit 1; }
  done; done; done
  complete () {   # complete FILE SCALE: a gatepull of all held plots of all five folds at epoch 36 and this eps scale (anything else is redone)
    [ -f "$1" ] && python -I -c "import json, sys; g = json.load(open(sys.argv[1])); sys.exit(0 if g.get('frac') == 1.0 and sorted(g.get('folds', {})) == list('01234') and g.get('epoch') == 36 and float(g.get('eps_scale', 1.0)) == float(sys.argv[2]) else 1)" "$1" "$2"
  }
  pids=(); i=0
  gp () {   # gp OUTPUT_JSON SCALE RUN: one gatepull on GPU i % G, skipped if a complete one exists
    local out=$1 sc=$2 run=$3
    if complete "$out" "$sc"; then echo "have $out"; return; fi
    echo "gatepull -> $out"; CUDA_VISIBLE_DEVICES=$(( i % G )) python -I nimiwae70_gatepull.py --data data --run "$run" --epoch 36 --eps-scale "$sc" --device "$D" --threads "$T" > "${out%.json}.log" 2>&1 &
    pids+=($!); i=$(( i + 1 ))
  }
  rc=0
  gp $N76/gatepull_e36.json 1 $N76; gp $B74/gatepull_e36.json 1 $B74; gp $N76/gatepull_e36_eps0.5.json 0.5 $N76; gp $N76/gatepull_e36_eps2.json 2 $N76
  for p in ${pids[@]+"${pids[@]}"}; do wait "$p" || rc=1; done              # phase 1 ends before phase 2: at most max(4, P) processes on the GPUs
  [ "$rc" -eq 0 ] || { echo "a gatepull FAILED: see $N76/gatepull_*.log and $B74/gatepull_*.log; rerun POST=d76 (complete pieces are skipped)"; exit 1; }
  echo "grid completion of $B74 (skipped where done)"
  for f in $ALL; do echo "$f"; done | xargs -P "$P" -L 1 --process-slot-var=SLOT bash -c 'CUDA_VISIBLE_DEVICES=$((SLOT % '"$G"')) python -I nimiwae70_cv.py --data data --fold $0 --rescore-grid '"$B74"' --device '"$D --threads $T" \
    || { echo "a grid rescore FAILED; rerun POST=d76"; exit 1; }
  python -I nimiwae70_cv.py --out $N76 --d76-base $B74 | tail -60 || { echo "the D76 comparison FAILED"; exit 1; }
  python -I nimiwae70_cv.py --out $N76 --summary --data data | tail -40 || { echo "the summary FAILED"; exit 1; }
  echo "done: the D76 verdict is in $N76/d76_compare.json (adopt at epoch 36); copy $N76 and $B74 back"
  exit 0
fi
if [ -n "$GRR" ]; then
  for f in $FOLDS; do [ -d "$GRR/f${f}_age1" ] || { echo "missing $GRR/f${f}_age1"; exit 1; }; done
  echo "grid completion of $GRR's last age-on checkpoints; $P at a time"
  for f in $FOLDS; do echo "$f"; done | xargs -P "$P" -L 1 --process-slot-var=SLOT bash -c 'CUDA_VISIBLE_DEVICES=$((SLOT % '"$G"')) python -I nimiwae70_cv.py --data data --fold $0 --rescore-grid '"$GRR"' --device '"$D --threads $T"
  exit $?
fi
if [ -n "$G1R" ]; then
  for f in $FOLDS; do [ -d "$G1R/f${f}_age1" ] || { echo "missing $G1R/f${f}_age1"; exit 1; }; done
  echo "rescoring $G1R on both G1 sets into ${G1R}_g1; $P at a time"
  for f in $FOLDS; do echo "$f"; done | xargs -P "$P" -L 1 --process-slot-var=SLOT bash -c 'CUDA_VISIBLE_DEVICES=$((SLOT % '"$G"')) python -I nimiwae70_cv.py --data data --out '"${G1R}_g1"' --fold $0 --rescore-g1 '"$G1R"' --device '"$D --threads $T"
  exit $?
fi
echo "q $Q, D74 $D74, D76 $D76, backend $BK, into $O; $P at a time, $T CPU threads each, $G GPU(s)"
{ for a in 1 0; do for f in $FOLDS; do echo "$f $a"; done; done; } | xargs -P "$P" -L 1 --process-slot-var=SLOT bash -c 'CUDA_VISIBLE_DEVICES=$((SLOT % '"$G"')) python -I nimiwae70_cv.py --data data --out '"$O"' --fold $0 --age $1 --q '"$Q"' --d74 '"$D74"' --d76 '"$D76"' --backend '"$BK"' --device '"$D --threads $T ${ARGS:-}"
rc=$?
python -I nimiwae70_cv.py --out "$O" --summary --data data | tail -60
[ "$rc" -eq 0 ] || echo "SOME RUNS FAILED: rerun this script, finished runs are skipped. See $O/*/run.log"
R
tar -cf "$OUT" -C "$W" nimiwae70; rm -rf "$W"; tar -tvf "$OUT" | awk '{printf "%8.1f MB  %s\n", $3/1048576, $NF}'
