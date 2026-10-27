#!/usr/bin/env python
"""
export_reference_tables.py — curation-fix 2026-10-26.

Materialise the raw / reference tables that the Region slim-DB builder
(landis_region4/tools/build_slim_db_region.jl, which reads COND, PLOT, REF_SPECIES and
species_longevity_ref from its --fiadb) needs, as REAL tables in the curated output DB, so the
builder can be pointed at the curated DB instead of FIADB (and so the old curated tables in
FIADB can never be picked up). COND and PLOT are restricted to the states present in
curated_trees; REF_SPECIES, species_longevity_ref and data_plot_eco are copied whole.

    python export_reference_tables.py --out-db <curated.duckdb> --fiadb <FIADB.duckdb>
"""
import os as _os, sys as _sys  # curation-fix: sibling modules resolve under `python -I`
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
import argparse

import dbconn

a = dbconn.add_db_args(argparse.ArgumentParser(description="export reference tables")).parse_args()
dbconn.check_args(a)
con = dbconn.connect(a.out_db, a.fiadb)
dbconn.require_tables(con, "curated_trees")
states = dbconn.parse_states(a.states) or [r[0] for r in con.execute(
    "SELECT DISTINCT STATECD FROM curated_trees ORDER BY 1").fetchall()]
sl = ",".join(map(str, states))
for tab, where in (("COND", f"WHERE STATECD IN ({sl})"), ("PLOT", f"WHERE STATECD IN ({sl})"),
                   ("REF_SPECIES", ""), ("species_longevity_ref", ""), ("data_plot_eco", "")):
    # r5 audit R2: the source is dbconn's TEMP view (raw FIADB with the CANONICAL plot key, and
    # for data_plot_eco the canonical plot's location, D28), never fia.main directly, so the
    # copies carry the same keys as every curated table
    con.execute(f'CREATE OR REPLACE TABLE main."{tab}" AS SELECT * FROM temp.main."{tab}" {where}')
    n = con.execute(f'SELECT count(*) FROM main."{tab}"').fetchone()[0]
    print(f"  {tab:24s} {n:>10,} rows  {'(states ' + sl + ')' if where else ''}")
con.execute("CHECKPOINT")
con.close()
print("[done]")
