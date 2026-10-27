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
visits (plot_key_remap, except dropped duplicate visits) — of the --states (default: the
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
con.execute(f"""CREATE OR REPLACE TEMP TABLE _export_cn AS
    SELECT PLT_CN AS CN FROM product_visits WHERE STATECD IN ({sl})
    UNION
    SELECT r.CN FROM plot_key_remap r WHERE r.STATECD IN ({sl}) AND r.reason IS DISTINCT FROM 'duplicate_visit'
      -- (plot_key_remap is national: only the raw visits of the product's plots, so a county subset exports no other county's plots)
      AND EXISTS (SELECT 1 FROM product_visits v WHERE v.STATECD = r.STATECD AND v.UNITCD = r.UNITCD AND v.COUNTYCD = r.COUNTYCD AND v.PLOT = r.PLOT)""")
for tab, where in (("COND", "WHERE CAST(PLT_CN AS VARCHAR) IN (SELECT CN FROM _export_cn)"),
                   ("PLOT", "WHERE CAST(CN AS VARCHAR) IN (SELECT CN FROM _export_cn)"),
                   ("REF_SPECIES", ""), ("species_longevity_ref", ""), ("data_plot_eco", "")):
    # r5 audit R2: the source is dbconn's TEMP view (raw FIADB with the CANONICAL plot key, and
    # for data_plot_eco the canonical plot's location, D28), never fia.main directly, so the
    # copies carry the same keys as every curated table
    con.execute(f'CREATE OR REPLACE TABLE "{db}".main."{tab}" AS SELECT * FROM temp.main."{tab}" {where} ORDER BY ALL')
    # r7 D44 (chain D F3): count the COPY in the output DB (an unqualified main."COND" resolved to
    # the TEMP view and printed whole-FIADB counts)
    n = con.execute(f'SELECT count(*) FROM "{db}".main."{tab}"').fetchone()[0]
    print(f"  {tab:24s} {n:>10,} rows  {'(product visits of states ' + sl + ')' if where else ''}")
# r7 D43: no exported PLOT / COND row may belong to a visit outside the product (or lack its PLOT)
bad = con.execute(f"""SELECT
    (SELECT count(*) FROM "{db}".main."PLOT" WHERE CAST(CN AS VARCHAR) NOT IN (SELECT CN FROM _export_cn)),
    (SELECT count(*) FROM "{db}".main."COND" c WHERE NOT EXISTS
        (SELECT 1 FROM "{db}".main."PLOT" p WHERE p.CN = c.PLT_CN))""").fetchone()
if bad[0] or bad[1]:
    _sys.exit(f"[export] {bad[0]} PLOT rows outside the product visits, {bad[1]} COND rows without their PLOT row")
con.execute("CHECKPOINT")
con.close()
print("[done]")
