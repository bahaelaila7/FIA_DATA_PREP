#!/usr/bin/env python
"""
Attach disturbance_drop_pct onto curated_cohorts_landis_simB in the SEPARATE sim-B DB,
computing the per-cohort disturbance biomass-drop from the curated DB (tree_trajectories +
curated_trees, read-only); writes only the sim-B DB. It is the ONLY writer of `cohort_disturbance`.

cohort_disturbance (the Pan consumer contract; r7 D42):
  statecd, unitcd, countycd, plot, subp  BIGINT   canonical subplot key
  species_symbol                         VARCHAR
  measdate                               TIMESTAMP_MS  the visit (date at midnight; the type is
                                                        kept for the slim schema prefix)
  age_calc                               INTEGER  the cohort's age at the visit (Sim B axis)
  disturbance_drop_pct                   DOUBLE   partial: (dropped / ACRE_M2) / (agb + dropped / ACRE_M2)
                                                  with agb the Sim B row's g/m2 (D31); full removal: 1.0
  dropped, would_be                      DOUBLE   the drop model's sums of drybio_g x TPA (g/acre)
  n_disturbed, n_survivors               UINTEGER counted removals / surviving timber members
  full_removal                           BOOLEAN  r7 D42: TRUE = every member is gone (no Sim B row
                                                  at this key; the cohort was removed by the counted
                                                  removals); FALSE = a partial drop on an existing row

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

coh = cd.compute(cd.load(con, states), timber_only=True,   # r5 I-1: real >= 5 in members only
                 scaffold=cd.load_scaffold(con, states))   # r7 D42: the full visit axis (all-removed visits)
cd.diagnostics(coh)

con.register("_cd", coh.to_arrow())
con.execute("""CREATE OR REPLACE TABLE cohort_disturbance AS
    SELECT statecd::BIGINT AS statecd, unitcd::BIGINT AS unitcd, countycd::BIGINT AS countycd,
           plot::BIGINT AS plot, subp::BIGINT AS subp, species_symbol::VARCHAR AS species_symbol,
           measdate::TIMESTAMP_MS AS measdate,          -- r7 C F16: the slim's (pre-existing) type, explicit
           age_calc::INTEGER AS age_calc, disturbance_drop_pct::DOUBLE AS disturbance_drop_pct,
           dropped::DOUBLE AS dropped, would_be::DOUBLE AS would_be,
           n_disturbed::UINTEGER AS n_disturbed, n_survivors::UINTEGER AS n_survivors,
           full_removal::BOOLEAN AS full_removal       -- r7 D42 (appended)
    FROM _cd ORDER BY statecd, unitcd, countycd, plot, subp, species_symbol, measdate, age_calc""")
con.unregister("_cd")
# r7 D42: a full removal has no live member at the visit, so Sim B has no row at its key
n_full_row = con.execute(f"""SELECT count(*) FROM cohort_disturbance d SEMI JOIN {TABLE} c
    ON c.statecd=d.statecd AND c.unitcd=d.unitcd AND c.countycd=d.countycd AND c.plot=d.plot AND c.subp=d.subp
   AND c.species_symbol=d.species_symbol AND c.measdate=d.measdate AND c.age_calc=d.age_calc
    WHERE d.full_removal""").fetchone()[0]
if n_full_row:
    raise SystemExit(f"[D42] {n_full_row} full-removal rows have a Sim B row at their key (must be 0)")
# r5 I-1: a cohort's FIRST Sim B row has no previous Sim B state to drop from -> no drop there
# (cohort = subplot x species x birth_sim_year, birth_sim_year = sim_year - age_calc)
con.execute(f"""
    CREATE OR REPLACE TEMP TABLE _first_rows AS
    SELECT statecd,unitcd,countycd,plot,subp,species_symbol,measdate,age_calc FROM (
      SELECT *, min(measdate) OVER (PARTITION BY statecd,unitcd,countycd,plot,subp,species_symbol,
                                                 sim_year - age_calc) AS md0 FROM {TABLE})
    WHERE measdate = md0""")
n_first = con.execute("""SELECT count(*) FROM cohort_disturbance
    SEMI JOIN _first_rows USING (statecd,unitcd,countycd,plot,subp,species_symbol,measdate,age_calc)""").fetchone()[0]
con.execute("""DELETE FROM cohort_disturbance d USING _first_rows f
    WHERE d.statecd=f.statecd AND d.unitcd=f.unitcd AND d.countycd=f.countycd AND d.plot=f.plot AND d.subp=f.subp
      AND d.species_symbol=f.species_symbol AND d.measdate=f.measdate AND d.age_calc=f.age_calc""")
print(f"[I-1] {n_first:,} drops on a cohort's first Sim B row removed")
# r6 R6: Pan's consumer (landis_region4 src/data/utils.jl _weighted_cohort_drop) reconstructs the
# pre-drop biomass of a row as would_be = agb / (1 - drop), i.e. it reads drop as
#     drop = dropped / (row agb + dropped)
# with both in the row's AGB units (g/m2). The drop model's `dropped` is in drybio_g x TPA
# (g/acre) = g/m2 x ACRE_M2, and its `would_be` is the previous visit's timber members only, so a
# row with ingrowth (AGB above that base) was over-dropped. The stored drop now means exactly what
# Pan assumes; `dropped` / `would_be` keep the drop model's own quantities.
ACRE_M2 = 4046.8564224
con.execute(f"""
    UPDATE cohort_disturbance d
    SET disturbance_drop_pct = round((d.dropped / {ACRE_M2}) / (c.agb + d.dropped / {ACRE_M2}), 9)
    FROM {TABLE} c
    WHERE NOT d.full_removal AND c.statecd=d.statecd AND c.unitcd=d.unitcd AND c.countycd=d.countycd AND c.plot=d.plot
      AND c.subp=d.subp AND c.species_symbol=d.species_symbol AND c.measdate=d.measdate AND c.age_calc=d.age_calc""")

_meta = ["n_trees_natural_treeage_scale", "share_natural_treeage_scale", "age_source_major",
        "n_trees_known_bias", "share_known_bias"]
_have = dbconn.column_names(con, TABLE)
_ex = [c for c in ["disturbance_drop_pct", "procedural_break", "all_live_withdrawn_rc7"] + _meta if c in _have]
keep = f"c.* EXCLUDE ({', '.join(_ex)})" if _ex else "c.*"   # idempotent re-attach, column order kept
_meta_sel = "".join(f", c.{m}" for m in _meta if m in _have)   # r3 I4: after the old columns
con.execute(f"""
    CREATE OR REPLACE TABLE {TABLE} AS
    SELECT {keep}, COALESCE(d.disturbance_drop_pct, 0.0) AS disturbance_drop_pct{_meta_sel}
    FROM {TABLE} c
    LEFT JOIN cohort_disturbance d
      ON c.statecd=d.statecd AND c.unitcd=d.unitcd AND c.countycd=d.countycd AND c.plot=d.plot
      AND c.subp=d.subp AND c.species_symbol=d.species_symbol AND c.measdate=d.measdate AND c.age_calc=d.age_calc
      AND NOT d.full_removal
    ORDER BY c.statecd, c.unitcd, c.countycd, c.plot, c.subp, c.species_symbol, c.measdate, c.age_calc
""")
# curation-fix (audit I4) + r7 D39: the visit metadata (procedural_break, all_live_withdrawn_rc7),
# appended after the age metadata
from cohorts_landis import add_visit_flags
add_visit_flags(con, TABLE)
con.commit()
tot = con.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
nz = con.execute(f"SELECT COUNT(*) FROM {TABLE} WHERE disturbance_drop_pct>0").fetchone()[0]
nfull = con.execute("SELECT count(*) FROM cohort_disturbance WHERE full_removal").fetchone()[0]
print(f"[attach] {TABLE}.disturbance_drop_pct: {nz:,}/{tot:,} rows >0; cohort_disturbance also holds "
      f"{nfull:,} full-removal rows (D42; no Sim B row)")
con.close()
print("[done]")
