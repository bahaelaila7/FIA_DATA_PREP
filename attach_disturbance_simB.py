#!/usr/bin/env python
"""
Attach disturbance_drop_pct onto curated_cohorts_landis_simB in the SEPARATE cohorts_simB.duckdb,
computing the per-cohort partial-disturbance biomass-drop from FIADB (read-only). Mirrors
cohort_disturbance.py --attach but targets the sim B table + never writes FIADB.

Run:  .venv/bin/python attach_disturbance_simB.py [OUT_DB] [FIADB] [STATECD ...]
  OUT_DB default: /workspace/FIA/FIASQLITE2PGSQL/cohorts_simB.duckdb
  FIADB  default: /workspace/FIA/FIASQLITE2PGSQL/FIADB.duckdb
  no STATECD args -> ALL states
"""
import sys
import duckdb

import cohort_disturbance as cd  # reuse load()/compute()

OUT_DB = sys.argv[1] if len(sys.argv) > 1 else "/workspace/FIA/FIASQLITE2PGSQL/cohorts_simB.duckdb"
FIADB  = sys.argv[2] if len(sys.argv) > 2 else "/workspace/FIA/FIASQLITE2PGSQL/FIADB.duckdb"
states = [int(a) for a in sys.argv[3:]]
TABLE  = "curated_cohorts_landis_simB"

print(f"OUT: {OUT_DB}\nFIA: {FIADB} (read-only)  states: {states or 'ALL'}")
con = duckdb.connect(OUT_DB)
con.execute(f"ATTACH '{FIADB}' AS fia (READ_ONLY)")
for t in ("tree_trajectories", "curated_trees"):
    con.execute(f'CREATE OR REPLACE VIEW "{t}" AS SELECT * FROM fia."{t}"')

coh = cd.compute(cd.load(con, states))
cd.diagnostics(coh)

con.register("_cd", coh.to_pandas())
con.execute("CREATE OR REPLACE TABLE cohort_disturbance AS SELECT * FROM _cd")
con.unregister("_cd")

has_col = any(c[0] == "disturbance_drop_pct" for c in con.execute(
    f"SELECT column_name FROM information_schema.columns WHERE table_name='{TABLE}'").fetchall())
keep = "c.* EXCLUDE (disturbance_drop_pct)" if has_col else "c.*"   # idempotent re-attach
con.execute(f"""
    CREATE OR REPLACE TABLE {TABLE} AS
    SELECT {keep}, COALESCE(d.disturbance_drop_pct, 0.0) AS disturbance_drop_pct
    FROM {TABLE} c
    LEFT JOIN cohort_disturbance d
      ON c.statecd=d.statecd AND c.unitcd=d.unitcd AND c.countycd=d.countycd AND c.plot=d.plot
      AND c.subp=d.subp AND c.species_symbol=d.species_symbol AND c.measdate=d.measdate AND c.age_calc=d.age_calc
""")
con.commit()
tot = con.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
nz = con.execute(f"SELECT COUNT(*) FROM {TABLE} WHERE disturbance_drop_pct>0").fetchone()[0]
print(f"[attach] {TABLE}.disturbance_drop_pct: {nz:,}/{tot:,} rows >0")
# study-area check
sa = con.execute(f"SELECT COUNT(*) FROM {TABLE} WHERE disturbance_drop_pct>0 AND epa_l4 IN "
                 "('8.3.5.65o','8.5.3.75g','8.5.3.75e','8.5.3.75f')").fetchone()[0]
print(f"[attach] study-area rows with drop>0: {sa:,}")
con.close()
print("[done]")
