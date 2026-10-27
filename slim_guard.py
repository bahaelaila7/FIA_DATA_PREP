#!/usr/bin/env python
"""
slim_guard.py — curation-fix 2026-10-26 (audit M6): checks around the UNCHANGED
landis_region4/tools/build_slim_db_region.jl, whose own defaults point at the OLD FIADB /
old cohorts_simB.duckdb and which silently SKIPS a missing simB table.

  mark  --db D --step N --script S [--curated-db C]
                                     r7 D44 (chain D F7): record that chain step N finished, with
                                     the run_id of C's run_scope (default D's own), in D.run_steps
  pre   --curated-db C --simb-db S --fiadb F [--states X]
                                     both sources exist, are not FIADB, hold every table the
                                     builder copies (+ the curation-fix columns); r7: they come
                                     from ONE run (same run_id, every step 1-10 logged, each step
                                     at or after the one before it: no stale partial rerun), and
                                     --states equals run_scope
  post  --slim OUT --curated-db C --simb-db S [--ref-slim REF]
                                     every expected table exists and is non-empty; every cohort
                                     visit has its PLOT and COND rows; r7 D43: no PLOT / COND row
                                     of a visit outside the curated product; the provenance is
                                     re-checked and COPIED into the slim (run_scope, run_steps,
                                     plot_key_remap, plot_location); r5 M-7: with REF (a previous
                                     slim DB, read-only) every REF table's columns are an exact
                                     prefix (name, type, order) of OUT's: appended columns only
Every DuckDB connection is capped by FIA_DUCKDB_THREADS / FIA_DUCKDB_MEMORY (r7 D F11).
"""
import argparse
import datetime
import os
import sys

import os as _os, sys as _sys  # sibling modules resolve under `python -I`
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
import duckdb

CURATED_TABLES = ["curated_cohorts_landis_stdorg", "curated_trees", "COND", "PLOT",
                  "REF_SPECIES", "species_longevity_ref"]
SIMB_TABLES = ["curated_cohorts_landis_simB", "cohort_disturbance"]
SLIM_TABLES = CURATED_TABLES + SIMB_TABLES
CURATED_STEPS = list(range(1, 9))     # chain steps writing the curated DB
SIMB_STEPS = [9, 10]                  # chain steps writing the Sim B side DB
PROVENANCE_TABLES = ["run_scope", "plot_key_remap", "plot_location"]   # copied from the curated DB

THREADS = os.environ.get("FIA_DUCKDB_THREADS", "2")
MEMORY = os.environ.get("FIA_DUCKDB_MEMORY", "3GB")


def connect(path, read_only=True):
    c = duckdb.connect(path, read_only=read_only)
    c.execute(f"SET threads={int(THREADS)}")
    c.execute(f"SET memory_limit='{MEMORY}'")
    c.execute("SET enable_progress_bar=false")
    return c


def tables(path):
    c = connect(path)
    t = {r[0].lower() for r in c.execute(
        "SELECT table_name FROM duckdb_tables() WHERE database_name=current_database()").fetchall()}
    return c, t


def die(msg):
    sys.exit(f"[slim_guard] {msg}")


def run_id_of(c):
    if not c.execute("SELECT count(*) FROM duckdb_tables() WHERE database_name=current_database() "
                     "AND table_name='run_scope'").fetchone()[0]:
        return None
    cols = {r[0] for r in c.execute("SELECT column_name FROM duckdb_columns() WHERE database_name=current_database() "
                                    "AND table_name='run_scope'").fetchall()}
    return c.execute("SELECT run_id FROM run_scope").fetchone()[0] if "run_id" in cols else None


def steps_of(c):
    if not c.execute("SELECT count(*) FROM duckdb_tables() WHERE database_name=current_database() "
                     "AND table_name='run_steps'").fetchone()[0]:
        return {}
    return {s: (r, f) for s, r, f in c.execute("SELECT step, run_id, finished_at FROM run_steps").fetchall()}


def check_provenance(cur_path, simb_path, states=None):
    """One run: the curated run_id, every step logged with it, steps in chain order in time."""
    c = connect(cur_path)
    rid = run_id_of(c)
    scope = c.execute("SELECT states, counties FROM run_scope").fetchone() if rid else None
    cs = steps_of(c)
    c.close()
    if not rid:
        die(f"{cur_path} has no run_scope.run_id (a pre-r7 DB, or step 1 did not finish)")
    s = connect(simb_path)
    ss = steps_of(s)
    s.close()
    log = {**{k: v for k, v in cs.items() if k in CURATED_STEPS}, **{k: v for k, v in ss.items() if k in SIMB_STEPS}}
    miss = [k for k in CURATED_STEPS + SIMB_STEPS if k not in log]
    if miss:
        die(f"steps {miss} are not logged as finished in {cur_path} / {simb_path}: run them through "
            f"run_curation_v2.sh (FROM_STEP=<first missing>)")
    other = [k for k, (r, _) in log.items() if r != rid]
    if other:
        die(f"steps {other} were run for another step-1 run (run_id != {rid}): the curated and Sim B "
            f"DBs are not from one run")
    order = CURATED_STEPS + SIMB_STEPS
    stale = [f"{a}->{b}" for a, b in zip(order, order[1:]) if log[b][1] < log[a][1]]
    if stale:
        die(f"stale outputs: step(s) {stale} finished BEFORE the step they depend on was rerun; "
            f"rerun from the earliest of them")
    if states is not None:
        want = sorted({int(x) for x in str(states).replace(" ", "").split(",") if x})
        have = sorted({int(x) for x in (scope[0] or "").split(",") if x})
        if want != have:
            die(f"--states {want} does not match run_scope.states {scope[0]!r}")
    return rid, scope, log


ap = argparse.ArgumentParser()
ap.add_argument("mode", choices=["pre", "post", "mark"])
ap.add_argument("--curated-db"); ap.add_argument("--simb-db"); ap.add_argument("--fiadb")
ap.add_argument("--slim"); ap.add_argument("--ref-slim"); ap.add_argument("--states")
ap.add_argument("--db"); ap.add_argument("--step", type=int); ap.add_argument("--script")
a = ap.parse_args()

if a.mode == "mark":
    if not a.db or a.step is None:
        die("mark needs --db and --step")
    src = connect(a.curated_db or a.db)
    rid = run_id_of(src)
    src.close()
    if not rid:
        die(f"{a.curated_db or a.db} has no run_scope.run_id")
    c = connect(a.db, read_only=False)
    c.execute("CREATE TABLE IF NOT EXISTS run_steps (step INTEGER, script VARCHAR, run_id VARCHAR, finished_at TIMESTAMP)")
    c.execute("DELETE FROM run_steps WHERE step = ?", [a.step])
    c.execute("INSERT INTO run_steps VALUES (?, ?, ?, ?)", [a.step, a.script, rid, datetime.datetime.now()])
    c.execute("CHECKPOINT")
    c.close()
    print(f"[slim_guard] mark: step {a.step} ({a.script}) finished, run {rid}")
elif a.mode == "pre":
    from fia_curation_v3 import FIX_COLUMNS
    for p, need in ((a.curated_db, CURATED_TABLES), (a.simb_db, SIMB_TABLES)):
        if not p or not os.path.exists(p):
            die(f"missing source DB: {p}")
        if a.fiadb and os.path.exists(a.fiadb) and os.path.samefile(p, a.fiadb):
            die(f"{p} is FIADB — the builder must read the NEW curated DBs")
        c, t = tables(p)
        miss = [x for x in need if x.lower() not in t]
        if miss:
            c.close()
            die(f"{p} lacks {miss}")
        # r2 M10: the curation-fix columns must be present (a pre-fix DB is refused)
        def cols(tab):
            return {r[0].lower() for r in c.execute(
                "SELECT column_name FROM duckdb_columns() WHERE database_name=current_database() "
                "AND lower(table_name)=lower(?)", [tab]).fetchall()}
        meta = ["n_trees_natural_treeage_scale", "share_natural_treeage_scale", "age_source_major",
        "n_trees_known_bias", "share_known_bias"]
        need_cols = {"curated_trees": [x.lower() for x in FIX_COLUMNS],
                     "curated_cohorts_landis_stdorg": ["procedural_break"] + meta,
                     "curated_cohorts_landis_simB": ["procedural_break", "disturbance_drop_pct"] + meta}
        for tab, want in need_cols.items():
            if tab in need:
                lack = [w for w in want if w not in cols(tab)]
                if lack:
                    c.close()
                    die(f"{p}:{tab} lacks curation-fix columns {lack}")
        c.close()
    rid, scope, _ = check_provenance(a.curated_db, a.simb_db, a.states)
    print(f"[slim_guard] pre: sources OK (tables + curation-fix columns; one run {rid}, scope {scope})")
else:
    if not a.slim or not os.path.exists(a.slim):
        die(f"the builder wrote no slim DB at {a.slim}")
    if not a.curated_db or not a.simb_db:
        die("post needs --curated-db and --simb-db (provenance, D43)")
    c, t = tables(a.slim)
    miss = [x for x in SLIM_TABLES if x.lower() not in t]
    if miss:
        die(f"slim DB lacks {miss} (builder skipped them)")
    empty = [x for x in SLIM_TABLES if c.execute(f'SELECT count(*) FROM "{x}"').fetchone()[0] == 0]
    c.close()
    if empty:
        die(f"empty slim tables: {empty}")
    print("[slim_guard] post: all 8 slim tables present and non-empty")
    # r6 R2: every cohort visit (canonical key, measdate) has its PLOT row and >= 1 COND row
    k = connect(a.slim)
    miss = k.execute("""
        WITH v AS (SELECT DISTINCT statecd,unitcd,countycd,plot,measdate FROM curated_cohorts_landis_stdorg
                   UNION SELECT DISTINCT statecd,unitcd,countycd,plot,measdate FROM curated_cohorts_landis_simB),
        p AS (SELECT CAST(CN AS VARCHAR) cn, STATECD, UNITCD, COUNTYCD, PLOT,
                     MAKE_DATE(CAST(MEASYEAR AS BIGINT), CAST(COALESCE(NULLIF(MEASMON, 0), 6) AS BIGINT),
                               CAST(COALESCE(NULLIF(MEASDAY, 0), 15) AS BIGINT)) md FROM PLOT),
        c AS (SELECT DISTINCT CAST(PLT_CN AS VARCHAR) cn FROM COND)
        SELECT count(*), count(*) FILTER (WHERE p.cn IS NULL), count(*) FILTER (WHERE p.cn IS NOT NULL AND c.cn IS NULL)
        FROM v LEFT JOIN p ON p.STATECD=v.statecd AND p.UNITCD=v.unitcd AND p.COUNTYCD=v.countycd AND p.PLOT=v.plot
                          AND p.md=v.measdate
        LEFT JOIN c ON c.cn = p.cn""").fetchone()
    if miss[1] or miss[2]:
        k.close()
        die(f"{miss[1]} cohort visits lack their PLOT row and {miss[2]} lack COND rows (of {miss[0]:,})")
    print(f"[slim_guard] post: all {miss[0]:,} cohort visits have their PLOT row and COND rows")
    # r7 D43: no foreign PLT_CN — every slim PLOT / COND row belongs to a curated product visit
    k.execute(f"ATTACH '{a.curated_db}' AS cur (READ_ONLY)")
    foreign = k.execute("""
        WITH ok AS (SELECT PLT_CN AS cn FROM cur.main.product_visits
                    UNION SELECT CN FROM cur.main.plot_key_remap WHERE reason IS DISTINCT FROM 'duplicate_visit')
        SELECT (SELECT count(*) FROM PLOT WHERE CAST(CN AS VARCHAR) NOT IN (SELECT cn FROM ok)),
               (SELECT count(*) FROM COND WHERE CAST(PLT_CN AS VARCHAR) NOT IN (SELECT cn FROM ok)),
               (SELECT count(DISTINCT (STATECD, UNITCD, COUNTYCD, PLOT)) FROM PLOT
                 WHERE CAST(CN AS VARCHAR) NOT IN (SELECT cn FROM ok))""").fetchone()
    k.close()
    if foreign[0] or foreign[1]:
        die(f"{foreign[0]} PLOT rows / {foreign[1]} COND rows ({foreign[2]} plots) belong to visits outside "
            f"the curated product (D43: reused periodic plot keys?)")
    print("[slim_guard] post: 0 PLOT / COND rows outside the curated product visits (D43)")
    # r7 D44 (chain D F7 / F14): provenance — re-checked (nothing reran since pre), then copied
    rid, scope, log = check_provenance(a.curated_db, a.simb_db)
    w = connect(a.slim, read_only=False)
    w.execute(f"ATTACH '{a.curated_db}' AS cur (READ_ONLY)")
    w.execute(f"ATTACH '{a.simb_db}' AS simb (READ_ONLY)")
    for tab in PROVENANCE_TABLES:
        w.execute(f'CREATE OR REPLACE TABLE "{tab}" AS SELECT * FROM cur.main."{tab}" ORDER BY ALL')
    w.execute("""CREATE OR REPLACE TABLE run_steps AS
        SELECT * FROM cur.main.run_steps WHERE step BETWEEN 1 AND 8
        UNION ALL SELECT * FROM simb.main.run_steps WHERE step IN (9, 10) ORDER BY step""")
    srid = w.execute("SELECT run_id FROM run_scope").fetchone()[0]
    nrid = w.execute("SELECT count(DISTINCT run_id) FROM run_steps").fetchone()[0]
    w.execute("CHECKPOINT")
    w.close()
    if srid != rid or nrid != 1:
        die(f"slim provenance mismatch: slim run_id {srid}, curated {rid}, {nrid} run ids in run_steps")
    print(f"[slim_guard] post: provenance copied into the slim ({', '.join(PROVENANCE_TABLES)}, run_steps); "
          f"run {rid}, scope {scope}")
    if a.ref_slim:
        def schema(path):
            k = connect(path)
            out = {}
            for tn, cn, dt in k.execute("SELECT table_name, column_name, data_type FROM duckdb_columns() "
                                        "WHERE database_name=current_database() ORDER BY table_name, column_index").fetchall():
                out.setdefault(tn, []).append((cn, dt))
            k.close()
            return out
        so, sn = schema(a.ref_slim), schema(a.slim)
        bad = []
        for t, cols in so.items():
            new = sn.get(t)
            if new is None:
                bad.append(f"{t}: missing")
            elif new[:len(cols)] != cols:
                bad.append(f"{t}: old columns changed (first diff "
                           f"{next(((x, y) for x, y in zip(cols, new) if x != y), 'length')})")
        if bad:
            die(f"slim schema NOT compatible with {a.ref_slim}: {bad}")
        print(f"[slim_guard] post: schema compatible with the reference slim (old columns an exact prefix, "
              f"{sum(len(sn[t]) - len(so[t]) for t in so)} appended columns)")
