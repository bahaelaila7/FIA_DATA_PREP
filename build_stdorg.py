#!/usr/bin/env python
# Build curated_cohorts_landis_stdorg from curated_cohorts_landis (same output DB).
# land_use is the STDORGCD ground-truth label: 'artificial' iff any condition of the plot's curated
# visits has STDORGCD = 1, else 'natural' (cohorts_landis.plot_land_use_sql, r7 D43 / D44: the one
# rule shared by Sim A, _stdorg, Sim B and tree_trajectories; since r7 curated_cohorts_landis
# already carries it, so the REPLACE below is the same value). Keeps cycle (from cohorts_landis2's
# base_subplot_cond attach) and the visit metadata.
# r7 D F15: a LEFT join, as in Sim B (a plot without any STDORGCD keeps its rows, land_use NULL;
# the r6 INNER join dropped them from Sim A only).
# curation-fix 2026-10-26: explicit --out-db/--fiadb (dbconn); COND is read from FIADB (read-only).
#   python build_stdorg.py --out-db <curated.duckdb> --fiadb <FIADB.duckdb>
import os as _os, sys as _sys  # curation-fix: sibling modules resolve under `python -I`
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
import argparse

import dbconn
from cohorts_landis import add_visit_flags, plot_land_use_sql

a = dbconn.add_db_args(argparse.ArgumentParser(description="curated_cohorts_landis_stdorg")).parse_args()
dbconn.check_args(a)
con = dbconn.connect(a.out_db, a.fiadb)
dbconn.require_tables(con, "curated_cohorts_landis", "COND", "product_visits")
con.execute(f"""
CREATE OR REPLACE TABLE curated_cohorts_landis_stdorg AS
WITH stdorg AS ({plot_land_use_sql(con)})
SELECT c.* REPLACE (s.land_use AS land_use)
FROM curated_cohorts_landis c
LEFT JOIN stdorg s ON c.statecd=s.statecd AND c.unitcd=s.unitcd AND c.countycd=s.countycd AND c.plot=s.plot
""")
# curation-fix (audit I4) + r7 D39: the visit metadata (procedural_break, all_live_withdrawn_rc7),
# set in place on both tables (appended as the last columns when new)
add_visit_flags(con, "curated_cohorts_landis")
add_visit_flags(con, "curated_cohorts_landis_stdorg")
r = con.execute("SELECT count(*), count(land_use), count(cycle) FROM curated_cohorts_landis_stdorg").fetchone()
print("STDORG rows / land_use non-null / cycle non-null:", r)
print("land_use split (plots):", con.execute(
    "SELECT land_use, count(DISTINCT (statecd,unitcd,countycd,plot)) FROM curated_cohorts_landis_stdorg "
    "GROUP BY 1 ORDER BY 1").fetchall())
con.execute("CHECKPOINT"); con.close()
print("stdorg built + checkpointed + closed")
