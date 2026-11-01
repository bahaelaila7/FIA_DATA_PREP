"""fia_order_test.py — the chain must not depend on row order (DECISIONS.md D66).

  python -I fia_order_test.py --fiadb FIADB --estage-dir DIR --work DIR --counties 13:87 [--seed 11]

Runs steps 1-10 on a county twice: once normally, once with the loaded trees shuffled and every table rewritten in a random order after each step.
All tables of both runs must be identical (EXCEPT ALL both ways). Exits 1 on any difference."""
import sys, os, subprocess, argparse, shutil
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import duckdb
HERE = os.path.dirname(os.path.abspath(__file__))


def chain(a, tag, seed=None):
    d = os.path.join(a.work, tag); shutil.rmtree(d, ignore_errors=True); os.makedirs(d)
    env = dict(os.environ, PY=sys.executable, FIADB=a.fiadb, FIA_CURATED_DB=f"{d}/curated.duckdb", FIA_SIMB_DB=f"{d}/simB.duckdb", FIA_ESTAGE_DIR=a.estage_dir,
               FIA_STATES=a.counties.split(":")[0], FIA_COUNTIES=a.counties, TO_STEP=getattr(a, "to_step", "10"), FIA_DUCKDB_THREADS="4", FIA_DUCKDB_MEMORY="8GB", POLARS_MAX_THREADS="4")
    env.pop("FIA_SHUFFLE_SEED", None)
    if seed is not None:
        env["FIA_SHUFFLE_SEED"] = str(seed)
    r = subprocess.run(["bash", os.path.join(HERE, "run_curation_v2.sh")], env=env, stdout=open(f"{d}/chain.log", "w"), stderr=subprocess.STDOUT)
    if r.returncode:
        sys.exit(f"{tag} failed; see {d}/chain.log")
    return d


def compare(x, y):
    c = duckdb.connect(); c.execute(f"ATTACH '{x}' AS a (READ_ONLY); ATTACH '{y}' AS b (READ_ONLY)")
    t = lambda db: {r[0] for r in c.execute(f"SELECT table_name FROM duckdb_tables() WHERE database_name = '{db}'").fetchall()}
    A, B = t("a"), t("b"); bad = sorted(A ^ B)
    for n in sorted(A & B):
        if n == "run_steps":      # run ids and timestamps
            continue
        ex = " EXCLUDE (run_id, created_at)" if n == "run_scope" else ""
        d1 = c.execute(f'SELECT count(*) FROM (SELECT *{ex} FROM a."{n}" EXCEPT ALL SELECT *{ex} FROM b."{n}")').fetchone()[0]
        d2 = c.execute(f'SELECT count(*) FROM (SELECT *{ex} FROM b."{n}" EXCEPT ALL SELECT *{ex} FROM a."{n}")').fetchone()[0]
        if d1 or d2:
            bad.append(f"{n} ({d1} / {d2})")
    return len(A & B), bad


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    for k in ("--fiadb", "--estage-dir", "--work", "--counties"):
        ap.add_argument(k, required=True)
    ap.add_argument("--seed", type=int, default=11); a = ap.parse_args()
    p, q = chain(a, "plain"), chain(a, "shuffled", a.seed)
    fail = 0
    for f in ("curated.duckdb", "simB.duckdb"):
        n, bad = compare(f"{p}/{f}", f"{q}/{f}"); print(f, n, "tables;", "IDENTICAL" if not bad else f"DIFFER: {bad}"); fail += len(bad)
    sys.exit(1 if fail else 0)
