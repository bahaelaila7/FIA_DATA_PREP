#!/usr/bin/env bash
# Smoke test: confirm FVSsn runs in this environment and emits a SQLite DB.
# Run me, then tell Claude to read /tmp/fvs_smoke/inspect.log
set -u

PREFIX=$(pwd)
S=$PREFIX/tmp/fvs_smoke
mkdir -p "$S"
cd "$S"
LOG="$S/inspect.log"
: >"$LOG"

# ---- build a tiny southern stand ----
# cols: plot(I4) tree(I4) PROB/TPA(F9.3) hist(I1) species=FIA-code(A3)
#       DBH DG HT THT HTG (5F7.2)  ICR(I3)
{
  printf "%4d%4d%9.3f%1d%-3s%7.2f%7.2f%7.2f%7.2f%7.2f%3d\n" 1 1 6.018 1 131 9.5 0 55 0 0 40
  printf "%4d%4d%9.3f%1d%-3s%7.2f%7.2f%7.2f%7.2f%7.2f%3d\n" 1 2 6.018 1 131 7.2 0 48 0 0 45
  printf "%4d%4d%9.3f%1d%-3s%7.2f%7.2f%7.2f%7.2f%7.2f%3d\n" 1 3 6.018 1 802 12.1 0 62 0 0 35
  printf "%4d%4d%9.3f%1d%-3s%7.2f%7.2f%7.2f%7.2f%7.2f%3d\n" 1 4 6.018 1 131 4.3 0 30 0 0 50
  printf "%4d%4d%9.3f%1d%-3s%7.2f%7.2f%7.2f%7.2f%7.2f%3d\n" 1 5 6.018 1 611 8.0 0 50 0 0 30
} >test.tre

cat >test.key <<'EOF'
SCREEN
NOAUTOES
STDIDENT
SMOKE01
INVYEAR       2000.0
NUMCYCLE         3.0
TREEFMT
(I4,I4,F9.3,I1,A3,5F7.2,I3,6I3,I3,I3,5I3,F7.1)
TREEDATA
FMIN
CARBREPT          2
CARBCALC          0         0
END
DATABASE
DSNOUT
out.db
SUMMARY           2
TREELIDB          2
CARBREDB          1
END
PROCESS
STOP
EOF

echo "==== run FVSsn ====" >>"$LOG"
$PREFIX/../ForestVegetationSimulator/bin/FVSsn --keywordfile=test.key >>"$LOG" 2>&1
echo "exit=$?" >>"$LOG"

echo "==== files in $S ====" >>"$LOG"
ls -la "$S" >>"$LOG"

echo "==== first 80 lines of FVS .out (if any) ====" >>"$LOG"
out=$(ls "$S"/*.out 2>/dev/null | head -1)
[ -n "$out" ] && head -80 "$out" >>"$LOG" 2>&1

echo "DONE -> $LOG"
