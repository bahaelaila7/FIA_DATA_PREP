#!/usr/bin/env python
"""
slim_guard.py — curation-fix 2026-10-26 (audit M6): checks around the UNCHANGED
landis_region4/tools/build_slim_db_region.jl, whose own defaults point at the OLD FIADB /
old cohorts_simB.duckdb and which silently SKIPS a missing simB table.

  pre   --curated-db C --simb-db S --fiadb F   both sources exist, are not FIADB, and hold
                                               every table the builder copies
  post  --slim OUT [--ref-slim REF]            every expected table exists and is non-empty;
                                               r5 M-7: with REF (a previous slim DB, read-only)
                                               every REF table's columns are an exact prefix
                                               (name, type, order) of OUT's: appended columns only
"""
import argparse
import os
import sys

import os as _os, sys as _sys  # sibling modules resolve under `python -I`
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
import duckdb

from fia_curation_v3 import FIX_COLUMNS

CURATED_TABLES = ["curated_cohorts_landis_stdorg", "curated_trees", "COND", "PLOT",
                  "REF_SPECIES", "species_longevity_ref"]
SIMB_TABLES = ["curated_cohorts_landis_simB", "cohort_disturbance"]
SLIM_TABLES = CURATED_TABLES + SIMB_TABLES


def tables(path):
    c = duckdb.connect(path, read_only=True)
    t = {r[0].lower() for r in c.execute(
        "SELECT table_name FROM duckdb_tables() WHERE database_name=current_database()").fetchall()}
    return c, t


ap = argparse.ArgumentParser()
ap.add_argument("mode", choices=["pre", "post"])
ap.add_argument("--curated-db"); ap.add_argument("--simb-db"); ap.add_argument("--fiadb")
ap.add_argument("--slim"); ap.add_argument("--ref-slim")
a = ap.parse_args()

if a.mode == "pre":
    for p, need in ((a.curated_db, CURATED_TABLES), (a.simb_db, SIMB_TABLES)):
        if not p or not os.path.exists(p):
            sys.exit(f"[slim_guard] missing source DB: {p}")
        if a.fiadb and os.path.exists(a.fiadb) and os.path.samefile(p, a.fiadb):
            sys.exit(f"[slim_guard] {p} is FIADB — the builder must read the NEW curated DBs")
        c, t = tables(p)
        miss = [x for x in need if x.lower() not in t]
        if miss:
            c.close()
            sys.exit(f"[slim_guard] {p} lacks {miss}")
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
                    sys.exit(f"[slim_guard] {p}:{tab} lacks curation-fix columns {lack}")
        c.close()
    print("[slim_guard] pre: sources OK (tables + curation-fix columns)")
else:
    c, t = tables(a.slim)
    miss = [x for x in SLIM_TABLES if x.lower() not in t]
    if miss:
        sys.exit(f"[slim_guard] slim DB lacks {miss} (builder skipped them)")
    empty = [x for x in SLIM_TABLES if c.execute(f'SELECT count(*) FROM "{x}"').fetchone()[0] == 0]
    c.close()
    if empty:
        sys.exit(f"[slim_guard] empty slim tables: {empty}")
    print("[slim_guard] post: all 8 slim tables present and non-empty")
    # r6 R2: every cohort visit (canonical key, measdate) has its PLOT row and >= 1 COND row
    k = duckdb.connect(a.slim, read_only=True)
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
    k.close()
    if miss[1] or miss[2]:
        sys.exit(f"[slim_guard] {miss[1]} cohort visits lack their PLOT row and {miss[2]} lack COND rows (of {miss[0]:,})")
    print(f"[slim_guard] post: all {miss[0]:,} cohort visits have their PLOT row and COND rows")
    if a.ref_slim:
        def schema(path):
            k = duckdb.connect(path, read_only=True)
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
            sys.exit(f"[slim_guard] slim schema NOT compatible with {a.ref_slim}: {bad}")
        print(f"[slim_guard] post: schema compatible with the reference slim (old columns an exact prefix, "
              f"{sum(len(sn[t]) - len(so[t]) for t in so)} appended columns)")
