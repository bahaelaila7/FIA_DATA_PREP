#!/usr/bin/env python
# Step 6/6 of the recovery: rebuild curated_cohorts_landis_stdorg from the FRESH v3 curated_cohorts_landis.
# Replaces the (derived, unused) land_use with the STDORGCD ground-truth label — natural iff every forested
# condition has STDORGCD=0, else artificial (MAX(STDORGCD)=1). Keeps cycle (already on curated_cohorts_landis
# via cohorts_landis2's base_subplot_cond attach).
import sys, duckdb
DB = sys.argv[1] if len(sys.argv) > 1 else "/workspace/FIA/FIASQLITE2PGSQL/FIADB.duckdb"
con = duckdb.connect(DB)
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
r = con.execute("SELECT count(*), count(land_use), count(cycle) FROM curated_cohorts_landis_stdorg").fetchone()
print("STDORG rows / land_use non-null / cycle non-null:", r)
print("study-area split:", con.execute(
    "SELECT land_use, count(DISTINCT (statecd,unitcd,countycd,plot)) FROM curated_cohorts_landis_stdorg "
    "WHERE epa_l4 IN ('8.3.5.65o','8.5.3.75g','8.5.3.75e','8.5.3.75f') GROUP BY 1").fetchall())
con.execute("CHECKPOINT"); con.close()
print("STEP6 stdorg built + checkpointed + closed")
