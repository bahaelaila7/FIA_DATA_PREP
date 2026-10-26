#!/usr/bin/env python
"""
slim_guard.py — curation-fix 2026-10-26 (audit M6): checks around the UNCHANGED
landis_region4/tools/build_slim_db_region.jl, whose own defaults point at the OLD FIADB /
old cohorts_simB.duckdb and which silently SKIPS a missing simB table.

  pre   --curated-db C --simb-db S --fiadb F   both sources exist, are not FIADB, and hold
                                               every table the builder copies
  post  --slim OUT                             every expected table exists and is non-empty
"""
import argparse
import os
import sys

import duckdb

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
ap.add_argument("--slim")
a = ap.parse_args()

if a.mode == "pre":
    for p, need in ((a.curated_db, CURATED_TABLES), (a.simb_db, SIMB_TABLES)):
        if not p or not os.path.exists(p):
            sys.exit(f"[slim_guard] missing source DB: {p}")
        if a.fiadb and os.path.exists(a.fiadb) and os.path.samefile(p, a.fiadb):
            sys.exit(f"[slim_guard] {p} is FIADB — the builder must read the NEW curated DBs")
        c, t = tables(p)
        miss = [x for x in need if x.lower() not in t]
        c.close()
        if miss:
            sys.exit(f"[slim_guard] {p} lacks {miss}")
    print("[slim_guard] pre: sources OK")
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
