"""fia_refactor_check.py — a key refactor must not change data (DECISIONS.md D69).

  python -I fia_refactor_check.py baseline --fiadb F --estage-dir D --work W --counties 13:87,21:175
  python -I fia_refactor_check.py check    --fiadb F --estage-dir D --work W --counties 13:87,21:175 [--to-step 10]

baseline freezes steps 1-10 per county. check rebuilds and compares every baseline table on the BASELINE's columns (EXCEPT ALL both ways); new tables and appended columns
are allowed and listed. Exits 1 on any difference."""
import sys, os, argparse, shutil
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import duckdb
from fia_order_test import chain

ap = argparse.ArgumentParser(); ap.add_argument("cmd")
for k in ("--fiadb", "--estage-dir", "--work", "--counties"):
    ap.add_argument(k, required=True)
ap.add_argument("--to-step", default="10"); a = ap.parse_args()
fail = 0
for ct in a.counties.split(","):
    tag = ct.replace(":", "_"); args = argparse.Namespace(fiadb=a.fiadb, estage_dir=a.estage_dir, work=a.work, counties=ct, to_step=a.to_step)
    if a.cmd == "baseline":
        d = chain(args, f"base_{tag}"); print("baseline", d); continue
    d = chain(args, f"new_{tag}"); b = os.path.join(a.work, f"base_{tag}")
    for f in ("curated.duckdb", "simB.duckdb"):
        if not os.path.exists(f"{d}/{f}"):
            continue
        c = duckdb.connect(); c.execute(f"ATTACH '{b}/{f}' AS a (READ_ONLY); ATTACH '{d}/{f}' AS b (READ_ONLY)")
        T = lambda db: {r[0] for r in c.execute(f"SELECT table_name FROM duckdb_tables() WHERE database_name = '{db}'").fetchall()}
        A, B = T("a"), T("b"); bad = []; added = {}
        for n in sorted(A):
            if n == "run_steps":
                continue
            if n not in B:
                bad.append(f"{n} missing"); continue
            ca = [r[0] for r in c.execute(f'DESCRIBE a."{n}"').fetchall()]; cb = [r[0] for r in c.execute(f'DESCRIBE b."{n}"').fetchall()]
            if n == "run_scope":
                ca = [x for x in ca if x not in ("run_id", "created_at")]
            miss = [x for x in ca if x not in cb]
            if miss:
                bad.append(f"{n} lost columns {miss}"); continue
            extra = [x for x in cb if x not in ca]
            if extra:
                added[n] = extra
            s = ", ".join(f'"{x}"' for x in ca)
            d1 = c.execute(f'SELECT count(*) FROM (SELECT {s} FROM a."{n}" EXCEPT ALL SELECT {s} FROM b."{n}")').fetchone()[0]
            d2 = c.execute(f'SELECT count(*) FROM (SELECT {s} FROM b."{n}" EXCEPT ALL SELECT {s} FROM a."{n}")').fetchone()[0]
            if d1 or d2:
                bad.append(f"{n} ({d1} / {d2})")
        print(ct, f, len(A), "tables;", "IDENTICAL" if not bad else f"DIFFER: {bad}", "| new tables", sorted(B - A), "| added columns", added); fail += len(bad)
sys.exit(1 if fail else 0)
