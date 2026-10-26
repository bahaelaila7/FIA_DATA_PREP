#!/usr/bin/env python
# Build curated_cohorts_landis_stdorg from curated_cohorts_landis (same output DB).
# Replaces the (derived, unused) land_use with the STDORGCD ground-truth label — natural iff every forested
# condition has STDORGCD=0, else artificial (MAX(STDORGCD)=1). Keeps cycle (already on curated_cohorts_landis
# via cohorts_landis2's base_subplot_cond attach).
# curation-fix 2026-10-26: explicit --out-db/--fiadb (dbconn); COND is read from FIADB (read-only).
#   python build_stdorg.py --out-db <curated.duckdb> --fiadb <FIADB.duckdb>
import os as _os, sys as _sys  # curation-fix: sibling modules resolve under `python -I`
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
import argparse

import dbconn

a = dbconn.add_db_args(argparse.ArgumentParser(description="curated_cohorts_landis_stdorg")).parse_args()
dbconn.check_args(a)
con = dbconn.connect(a.out_db, a.fiadb)
dbconn.require_tables(con, "curated_cohorts_landis", "COND")
con.execute("""
CREATE OR REPLACE TABLE curated_cohorts_landis_stdorg AS
WITH stdorg AS (
  SELECT statecd,unitcd,countycd,plot,
         CASE WHEN MAX(STDORGCD)=1 THEN 'artificial' ELSE 'natural' END lu
  FROM COND WHERE STDORGCD IS NOT NULL GROUP BY 1,2,3,4)
SELECT c.* REPLACE (s.lu AS land_use)
FROM curated_cohorts_landis c
JOIN stdorg s ON c.statecd=s.statecd AND c.unitcd=s.unitcd AND c.countycd=s.countycd AND c.plot=s.plot
""")
# curation-fix (audit I4): procedural re-tally flag, appended as the LAST column of both tables
from cohorts_landis import add_procedural_break
add_procedural_break(con, "curated_cohorts_landis")
add_procedural_break(con, "curated_cohorts_landis_stdorg")
r = con.execute("SELECT count(*), count(land_use), count(cycle) FROM curated_cohorts_landis_stdorg").fetchone()
print("STDORG rows / land_use non-null / cycle non-null:", r)
print("land_use split (plots):", con.execute(
    "SELECT land_use, count(DISTINCT (statecd,unitcd,countycd,plot)) FROM curated_cohorts_landis_stdorg "
    "GROUP BY 1").fetchall())
con.execute("CHECKPOINT"); con.close()
print("stdorg built + checkpointed + closed")
