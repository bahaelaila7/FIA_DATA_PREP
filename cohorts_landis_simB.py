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
from fia_ids import pick, maybe_shuffle

import dbconn

import cohorts_landis as c1  # v1: has dia in load_base_data + sim_year/subp_meas_num in output

TABLE  = "curated_cohorts_landis_simB"
SAP    = c1.SAPLING_DIA_MAX  # 5.0 in — FIA sapling/timber boundary



OUT_COLS = ["plt_cn", "statecd", "unitcd", "countycd", "plot", "subp",
            "epa_l3", "epa_l4", "ecosubcd",
            "species_symbol", "spgrpcd", "major_spgrpcd", "sftwd_hrdwd",
            "measdate", "sim_year", "age_calc", "agb",
            "subp_has_dstrb", "plot_meas_num", "subp_meas_num", "intro_type"]


def build(con) -> pl.DataFrame:
    print("=" * 70); print("sim B (real, timber-sized) cohort curation"); print("=" * 70)

    df = c1.load_base_data(con)                       # one row per live tree×visit, carries 'dia'
    # r3 M1: the time axis (sim_year, age_calc) is computed on ALL live rows (saplings
    # included) over the full visit scaffold, BEFORE dropping saplings — the same axis as the
    # Sim A table and tree_trajectories, so cohort_disturbance's (measdate, age_calc) keys
    # match exactly (re-basing it on the timber-only rows shifted ages by a year).
    scaffold = con.execute("SELECT DISTINCT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, MEASDATE "
                           "FROM clean_subplot_years").pl()
    df = c1.compute_sim_year_ages(df, scaffold)
    n0 = df.height
    df = df.filter(pl.col("dia").is_not_null() & (pl.col("dia") >= SAP))
    print(f"[sapling-filter] dropped {n0 - df.height:,}/{n0:,} sapling/null-DIA rows "
          f"→ {df.height:,} timber tree-visits (DIA ≥ {SAP} in)")

    df = df.join(c1.build_subp_has_dstrb(con), on=c1.SUBPLOT_ID, how="left").with_columns(
        pl.col("subp_has_dstrb").fill_null(False))
    df = df.join(c1.build_plot_meas_num(df), on=c1.PLOT_KEY, how="left")
    df = df.join(c1.build_subp_meas_num(df), on=c1.SUBPLOT_ID, how="left")

    # NO phantom backfill — all remaining rows are real observations
    df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("intro_type"))
    df = df.with_columns(
        pl.concat_str([pl.col(c).cast(pl.Utf8) for c in c1.PLOT_KEY], separator="_").alias("plt_cn"))

    df = df.rename({"STATECD": "statecd", "UNITCD": "unitcd", "COUNTYCD": "countycd",
                    "PLOT": "plot", "SUBP": "subp", "MEASDATE": "measdate"}).select(
        OUT_COLS + ["_age_source", "_age_domain", "_age_stratum"])   # D46: _age_stratum for IS_NATURAL_MODEL

    # Aggregate into cohorts: sum agb over trees sharing (subplot, species, age, visit).
    # r3 I4: + n_trees_natural_treeage_scale, AGB-weighted share_natural_treeage_scale and the
    # AGB-dominant age source of the member trees.
    n_pre = df.height
    grp = [c for c in OUT_COLS if c != "agb"]
    nu = c1.IS_NATURAL_MODEL
    kb = c1.IS_KNOWN_BIAS
    src = (df.group_by(grp + ["_age_source"]).agg(pl.col("agb").sum().alias("_a"))
             .pipe(pick, grp, ["_a", "_age_source"], descending=[True, False], nulls_last=True)
             .select(grp + [pl.col("_age_source").alias("age_source_major")]))
    df = (df.group_by(grp, maintain_order=True)
            .agg(pl.col("agb").sum().alias("agb"),
                 nu.cast(pl.Int64).sum().alias("n_trees_natural_treeage_scale"),
                 ((pl.col("agb") * nu.cast(pl.Float64)).sum() / pl.col("agb").sum())
                 .alias("share_natural_treeage_scale"),
                 kb.cast(pl.Int64).sum().alias("n_trees_known_bias"),
                 ((pl.col("agb") * kb.cast(pl.Float64)).sum() / pl.col("agb").sum())
                 .alias("share_known_bias"))
            # r5 M-3: float sums depend on the summation order -> round for bit-identical reruns
            .with_columns(pl.col("agb").round(6), pl.col("share_natural_treeage_scale").round(9),
                          pl.col("share_known_bias").round(9))
            .join(src, on=grp, how="left", nulls_equal=True)
            .select(OUT_COLS + c1.AGE_META_COLS)
            .sort(["statecd", "unitcd", "countycd", "plot", "subp", "species_symbol", "measdate", "age_calc"]))
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
    dbconn.require_tables(con, "curated_trees", "clean_subplot_years", "base_subplot_cond", "COND", "product_visits")

    cohorts = build(con)

    con.register("_simB_view", cohorts)
    con.execute(f"CREATE OR REPLACE TABLE {TABLE}_base AS SELECT * FROM _simB_view")
    con.unregister("_simB_view")

    # Attach land_use (STDORGCD 2-way: the shared c1.plot_land_use_sql rule, r7 D43 / D44: the
    # curated visits' COND rows only, a LEFT join exactly as Sim A's) + cycle (per plot, measdate).
    con.execute(f"""
    CREATE OR REPLACE TABLE {TABLE} AS
    WITH stdorg AS ({c1.plot_land_use_sql(con)}),
    plot_cycle AS (
        SELECT STATECD AS statecd, UNITCD AS unitcd, COUNTYCD AS countycd, PLOT AS plot,
               MEASDATE AS measdate, MAX(CYCLE) AS cycle
        FROM base_subplot_cond GROUP BY 1,2,3,4,5)
    SELECT c.* EXCLUDE (n_trees_natural_treeage_scale, share_natural_treeage_scale, age_source_major, n_trees_known_bias, share_known_bias),
           s.land_use, cy.cycle,
           c.n_trees_natural_treeage_scale, c.share_natural_treeage_scale, c.age_source_major, c.n_trees_known_bias, c.share_known_bias
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
