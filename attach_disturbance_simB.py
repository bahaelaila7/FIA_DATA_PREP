#!/usr/bin/env python
"""
Attach disturbance_drop_pct onto curated_cohorts_landis_simB in the SEPARATE sim-B DB,
computing the per-cohort partial-disturbance biomass-drop from the curated DB
(tree_trajectories + curated_trees, read-only). Mirrors cohort_disturbance.py --attach
but targets the sim B table; writes only the sim-B DB.

curation-fix 2026-10-26: explicit paths (dbconn): curated tables come from --curated-db,
never from FIADB's old copies; no default paths.
Run:  python attach_disturbance_simB.py --out-db <simB.duckdb> --curated-db <curated.duckdb> \
          --fiadb <FIADB> [--states 1,12,...]
"""
import os as _os, sys as _sys  # curation-fix: sibling modules resolve under `python -I`
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
import argparse

import cohort_disturbance as cd  # reuse load()/compute()
import dbconn

TABLE  = "curated_cohorts_landis_simB"

a = dbconn.add_db_args(argparse.ArgumentParser(description="simB disturbance attach"), curated=True).parse_args()
dbconn.check_args(a, curated=True)
states = dbconn.parse_states(a.states) or []
print(f"OUT: {a.out_db}\nCURATED: {a.curated_db} (read-only)  states: {states or 'ALL'}")
con = dbconn.connect(a.out_db, a.fiadb, curated_db=a.curated_db)
dbconn.require_tables(con, "tree_trajectories", "curated_trees", TABLE)

coh = cd.compute(cd.load(con, states))
cd.diagnostics(coh)

con.register("_cd", coh.to_pandas())
con.execute("CREATE OR REPLACE TABLE cohort_disturbance AS SELECT * FROM _cd")
con.unregister("_cd")

_meta = ["n_trees_natural_treeage_scale", "share_natural_treeage_scale", "age_source_major",
        "n_trees_known_bias", "share_known_bias"]
_have = dbconn.column_names(con, TABLE)
_ex = [c for c in ["disturbance_drop_pct", "procedural_break"] + _meta if c in _have]
keep = f"c.* EXCLUDE ({', '.join(_ex)})" if _ex else "c.*"   # idempotent re-attach, column order kept
_meta_sel = "".join(f", c.{m}" for m in _meta if m in _have)   # r3 I4: after the old columns
con.execute(f"""
    CREATE OR REPLACE TABLE {TABLE} AS
    SELECT {keep}, COALESCE(d.disturbance_drop_pct, 0.0) AS disturbance_drop_pct{_meta_sel}
    FROM {TABLE} c
    LEFT JOIN cohort_disturbance d
      ON c.statecd=d.statecd AND c.unitcd=d.unitcd AND c.countycd=d.countycd AND c.plot=d.plot
      AND c.subp=d.subp AND c.species_symbol=d.species_symbol AND c.measdate=d.measdate AND c.age_calc=d.age_calc
""")
# curation-fix (audit I4): procedural re-tally flag, appended after disturbance_drop_pct
from cohorts_landis import add_procedural_break
add_procedural_break(con, TABLE)
con.commit()
tot = con.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
nz = con.execute(f"SELECT COUNT(*) FROM {TABLE} WHERE disturbance_drop_pct>0").fetchone()[0]
print(f"[attach] {TABLE}.disturbance_drop_pct: {nz:,}/{tot:,} rows >0")
con.close()
print("[done]")
