#!/usr/bin/env python
"""
sim B cohort table — "real, timber-sized" cohorts.

Same pipeline as curated_cohorts_landis_stdorg (sim A) EXCEPT, before the cohort
aggregation, we drop:
  1. saplings           — tree-visits with DIA < SAPLING_DIA_MAX (5.0 in), and null-DIA
                          (reconcile-imputed) rows. Only timber/pole-sized stems remain.
  2. phantom backfill   — the synthetic pre-debut rows (intro_type='phantom_backfill')
                          are simply never generated (backfill steps skipped).
Then aggregate to one row per cohort (subplot × species × age × visit), summing agb.
Reuses cohorts_landis (v1) helpers so sim_year/age_calc/disturbance/meas-count logic
is byte-for-byte the vetted pipeline; only the two filters + aggregation differ.

Reads the curated DB and FIADB READ-ONLY (attached); writes the new table into a
SEPARATE duckdb file. Point a Pan config's cohorts_db_path at that file.

curation-fix 2026-10-26: explicit paths (dbconn) — the curated tables come from
--curated-db (the new curated DB), raw tables from --fiadb; no default paths.
Run:  python cohorts_landis_simB.py --out-db <simB.duckdb> --curated-db <curated.duckdb> --fiadb <FIADB>
"""
import os as _os, sys as _sys  # curation-fix: sibling modules resolve under `python -I`
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
import argparse

import polars as pl

import dbconn

import cohorts_landis as c1  # v1: has dia in load_base_data + sim_year/subp_meas_num in output

TABLE  = "curated_cohorts_landis_simB"
SAP    = c1.SAPLING_DIA_MAX  # 5.0 in — FIA sapling/timber boundary



def build(con) -> pl.DataFrame:
    print("=" * 70); print("sim B (real, timber-sized) cohort curation"); print("=" * 70)

    df = c1.load_base_data(con)                       # one row per live tree×visit, carries 'dia'
    n0 = df.height
    df = df.filter(pl.col("dia").is_not_null() & (pl.col("dia") >= SAP))
    print(f"[sapling-filter] dropped {n0 - df.height:,}/{n0:,} sapling/null-DIA rows "
          f"→ {df.height:,} timber tree-visits (DIA ≥ {SAP} in)")

    df = df.join(c1.build_subp_has_dstrb(con), on=c1.SUBPLOT_ID, how="left").with_columns(
        pl.col("subp_has_dstrb").fill_null(False))
    df = df.join(c1.build_plot_meas_num(df), on=c1.PLOT_KEY, how="left")
    df = df.join(c1.build_subp_meas_num(df), on=c1.SUBPLOT_ID, how="left")
    df = c1.compute_sim_year_ages(df)                 # sim_year + integer-anchored age_calc

    # NO phantom backfill — all remaining rows are real observations
    df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("intro_type"))
    df = df.with_columns(
        pl.concat_str([pl.col(c).cast(pl.Utf8) for c in c1.PLOT_KEY], separator="_").alias("plt_cn"))

    df = df.rename({"STATECD": "statecd", "UNITCD": "unitcd", "COUNTYCD": "countycd",
                    "PLOT": "plot", "SUBP": "subp", "MEASDATE": "measdate"}).select([
        "plt_cn", "statecd", "unitcd", "countycd", "plot", "subp",
        "epa_l3", "epa_l4", "ecosubcd",
        "species_symbol", "spgrpcd", "major_spgrpcd", "sftwd_hrdwd",
        "measdate", "sim_year", "age_calc", "agb",
        "subp_has_dstrb", "plot_meas_num", "subp_meas_num", "intro_type"])

    # Aggregate into cohorts: sum agb over trees sharing (subplot, species, age, visit).
    n_pre = df.height
    grp = [c for c in df.columns if c != "agb"]
    df = (df.group_by(grp, maintain_order=True)
            .agg(pl.col("agb").sum().alias("agb"))
            .select(["plt_cn", "statecd", "unitcd", "countycd", "plot", "subp",
                     "epa_l3", "epa_l4", "ecosubcd",
                     "species_symbol", "spgrpcd", "major_spgrpcd", "sftwd_hrdwd",
                     "measdate", "sim_year", "age_calc", "agb",
                     "subp_has_dstrb", "plot_meas_num", "subp_meas_num", "intro_type"]))
    print(f"[aggregate] {n_pre:,} tree-rows → {df.height:,} cohort-rows "
          f"({df.select(['statecd','unitcd','countycd','plot']).unique().height:,} plots, "
          f"{df['species_symbol'].n_unique():,} species)")
    return df


if __name__ == "__main__":
    a = dbconn.add_db_args(argparse.ArgumentParser(description="sim B cohorts"), curated=True).parse_args()
    dbconn.check_args(a, curated=True)
    print(f"OUT: {a.out_db}\nCURATED: {a.curated_db} (read-only)\nFIA: {a.fiadb} (read-only)")
    # temp views: raw tables -> FIADB, curated tables -> the curated DB (never FIADB's old ones)
    con = dbconn.connect(a.out_db, a.fiadb, curated_db=a.curated_db)
    dbconn.require_tables(con, "curated_trees", "curated_cohorts", "base_subplot_cond")

    cohorts = build(con)

    con.register("_simB_view", cohorts)
    con.execute(f"CREATE OR REPLACE TABLE {TABLE}_base AS SELECT * FROM _simB_view")
    con.unregister("_simB_view")

    # Attach land_use (STDORGCD 2-way, same rule as build_stdorg) + cycle (per plot,measdate).
    con.execute(f"""
    CREATE OR REPLACE TABLE {TABLE} AS
    WITH stdorg AS (
        SELECT statecd, unitcd, countycd, plot,
               CASE WHEN MAX(STDORGCD)=1 THEN 'artificial' ELSE 'natural' END AS lu
        FROM COND WHERE STDORGCD IS NOT NULL GROUP BY 1,2,3,4),
    plot_cycle AS (
        SELECT STATECD AS statecd, UNITCD AS unitcd, COUNTYCD AS countycd, PLOT AS plot,
               MEASDATE AS measdate, MAX(CYCLE) AS cycle
        FROM base_subplot_cond GROUP BY 1,2,3,4,5)
    SELECT c.*, s.lu AS land_use, cy.cycle
    FROM {TABLE}_base c
    LEFT JOIN stdorg s USING (statecd, unitcd, countycd, plot)
    LEFT JOIN plot_cycle cy USING (statecd, unitcd, countycd, plot, measdate)
    """)
    con.execute(f"DROP TABLE {TABLE}_base")
    con.commit()

    r = con.execute(f"SELECT count(*), count(land_use), count(cycle), "
                    f"count(DISTINCT (statecd,unitcd,countycd,plot)) FROM {TABLE}").fetchone()
    print(f"\n[save] {TABLE} → {r[0]:,} rows · land_use {r[1]:,} · cycle {r[2]:,} · {r[3]:,} plots")
    split = con.execute(
        f"SELECT land_use, count(DISTINCT (statecd,unitcd,countycd,plot)) FROM {TABLE} GROUP BY 1").fetchall()
    print("land_use split (plots):", split)
    con.close()
    print("[done]")
