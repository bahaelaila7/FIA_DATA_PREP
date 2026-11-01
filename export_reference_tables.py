#!/usr/bin/env python
"""
export_reference_tables.py — curation-fix 2026-10-26.

Materialise the raw / reference tables that the Region slim-DB builder
(landis_region4/tools/build_slim_db_region.jl, which reads COND, PLOT, REF_SPECIES and
species_longevity_ref from its --fiadb) needs, as REAL tables in the curated output DB, so the
builder can be pointed at the curated DB instead of FIADB (and so the old curated tables in
FIADB can never be picked up). REF_SPECIES, species_longevity_ref and data_plot_eco are copied
whole.

r7 D43: COND and PLOT hold ONLY the rows of the product's plot visits — `product_visits`
(written by step 1: the annual visits of the curated scaffold) plus the canonical remap's raw
visits (plot_key_remap) — of the --states (default: the
states in curated_trees). The builder joins COND / PLOT to the cohort plots on the 4-part plot
key only, so copying every row of the state let a 1986-88 periodic plot that reused an annual
plot's key (KY: 700 plots) into the slim (location up to 64 km off, SITECLCD, land_use).

    python export_reference_tables.py --out-db <curated.duckdb> --fiadb <FIADB.duckdb> [--states ...]
"""
import os as _os, sys as _sys  # curation-fix: sibling modules resolve under `python -I`
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
import argparse

import dbconn

a = dbconn.add_db_args(argparse.ArgumentParser(description="export reference tables")).parse_args()
dbconn.check_args(a)
con = dbconn.connect(a.out_db, a.fiadb)
dbconn.require_tables(con, "curated_trees", "product_visits", "plot_key_remap")
db = con.execute("SELECT current_database()").fetchone()[0]
states = dbconn.parse_states(a.states) or [r[0] for r in con.execute(
    "SELECT DISTINCT STATECD FROM curated_trees ORDER BY 1").fetchall()]
sl = ",".join(map(str, states))
# D59: the exported visits by natural id (canonical key + INVYR): the product's visits and the remapped visits of its plots
con.execute(f"""CREATE OR REPLACE TEMP TABLE _export_vis AS
    SELECT STATECD, UNITCD, COUNTYCD, PLOT, INVYR FROM product_visits WHERE STATECD IN ({sl})
    UNION
    SELECT r.STATECD, r.UNITCD, r.COUNTYCD, r.PLOT, r.INVYR FROM plot_key_remap r WHERE r.STATECD IN ({sl})
      AND EXISTS (SELECT 1 FROM product_visits v WHERE v.STATECD = r.STATECD AND v.UNITCD = r.UNITCD AND v.COUNTYCD = r.COUNTYCD AND v.PLOT = r.PLOT)""")
_IN = "SEMI JOIN _export_vis v ON v.STATECD = x.STATECD AND v.UNITCD = x.UNITCD AND v.COUNTYCD = x.COUNTYCD AND v.PLOT = x.PLOT AND v.INVYR = x.INVYR"
for tab, where in (("COND", _IN), ("PLOT", _IN),
                   ("REF_SPECIES", ""), ("species_longevity_ref", ""), ("data_plot_eco", "")):
    # r5 audit R2: the source is dbconn's TEMP view (raw FIADB with the CANONICAL plot key, and
    # for data_plot_eco the canonical plot's location, D28), never fia.main directly, so the
    # copies carry the same keys as every curated table
    con.execute(f'CREATE OR REPLACE TABLE "{db}".main."{tab}" AS SELECT * FROM temp.main."{tab}" x {where} ORDER BY ALL')
    # r7 D44 (chain D F3): count the COPY in the output DB (an unqualified main."COND" resolved to
    # the TEMP view and printed whole-FIADB counts)
    n = con.execute(f'SELECT count(*) FROM "{db}".main."{tab}"').fetchone()[0]
    print(f"  {tab:24s} {n:>10,} rows  {'(product visits of states ' + sl + ')' if where else ''}")
# r7 D43: no exported PLOT / COND row may belong to a visit outside the product (or lack its PLOT)
bad = con.execute(f"""SELECT
    (SELECT count(*) FROM "{db}".main."PLOT" x ANTI JOIN _export_vis v ON v.STATECD = x.STATECD AND v.UNITCD = x.UNITCD AND v.COUNTYCD = x.COUNTYCD AND v.PLOT = x.PLOT AND v.INVYR = x.INVYR),
    (SELECT count(*) FROM "{db}".main."COND" c WHERE NOT EXISTS
        (SELECT 1 FROM "{db}".main."PLOT" p WHERE p.STATECD = c.STATECD AND p.UNITCD = c.UNITCD AND p.COUNTYCD = c.COUNTYCD AND p.PLOT = c.PLOT AND p.INVYR = c.INVYR))""").fetchone()
if bad[0] or bad[1]:
    _sys.exit(f"[export] {bad[0]} PLOT rows outside the product visits, {bad[1]} COND rows without their PLOT row")
con.execute("CHECKPOINT")
con.close()
print("[done]")
