"""
build_tree_trajectories.py
Per-tree life trajectories for downstream plotting and cohort formation.

Builds a `tree_trajectories` table: one row per (tree x subplot-visit slot),
contiguous from the tree's first introduction to its terminal event, with gaps
filled two ways:
  - backward phantom records  (intro_type='phantom_backfill')  — tree too small
        / suppressed to be tallied at earlier visits        [reused from cohorts_landis]
  - mid-trajectory interpolation (intro_type='gap_interpolated') — tree missed at
        one visit then re-tallied later; linearly interpolated between brackets

Each trajectory carries how it ended (terminal_fate / death_cause), the subplot
disturbance/cut-event class (pre_commercial_thin / commercial_thin / clear_cut; r7: keyed by
subplot), and stratification markers: ecoregion, cycle, ownership / forest-type /
physiographic / reserved, the 2-way STDORGCD land_use (r7: the rule shared with every cohort
table) and the DERIVED 4-way management class land_use_4way (per subplot).
r7 D44 (C F3): phantom and gap-interpolated rows carry THEIR OWN visit's cycle / INVYR / condid /
COND stratifiers (condid = the subplot's single forest condition at that visit, else NULL), never
the debut's or the previous record's; their estimated_age is the tree's propagated age there.

Biomass is the per-tree above-ground dry biomass DRYBIO_AG in grams (`drybio_g`),
NOT TPA-expanded per-area — these are individual-tree trajectories.

Reuses cohorts_landis: compute_sim_year_ages, estimate_sapling_growth_rates, the backfill blocks,
the D41 disturbance definition / subp_has_dstrb, the land_use rule and the key lists.

Usage:
    python build_tree_trajectories.py --out-db <curated.duckdb> --fiadb <FIADB.duckdb> [--states 37]
"""

import os as _os, sys as _sys  # curation-fix: sibling modules resolve under `python -I`
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
import sys

import duckdb
import polars as pl
from fia_ids import pick, maybe_shuffle

from fia_curation_v3 import _next_visit   # shared next-visit / nonforest lookup (r2 M6)
from cohorts_landis import (
    PLOT_KEY, SUBPLOT_ID, TREE_ID,
    MIN_BACKFILL_DRYBIO_G,       # r7 C F5: the per-tree (grams) phantom floor
    disturbance_death_expr,      # r7 D41: one disturbance definition (cohorts_landis)
    build_subp_has_dstrb,
    plot_land_use_sql,
    add_rc7_withdrawn_flag,
    report_phantom_floor,
    compute_sim_year_ages,
    sim_year_map,
    estimate_sapling_growth_rates,
    _has_column,
    BF_FLAGS,
    add_backfill_flags,          # curation-fix 2026-10-26: shared phantom-backfill blocks
    apply_backfill_blocks,
    load_backfill_context,
)

# Forester's constant: basal area (ft²) = 0.005454 * DIA(in)² ; * TPA → ft²/acre
BA_FT2 = 0.005454
# DRYBIO_AG is per-tree above-ground dry biomass in POUNDS → grams (per tree).
LBS_TO_G = 453.592

# Death-cause: DISTURBANCE_AGENTS / disturbance_death_expr come from cohorts_landis (r7 D41):
#   AGENTCD 10 insect · 20 disease · 30 fire · 40 animal · 50 weather, or a condition DSTRBCD
#   other than 0 / 60 (60 vegetation/competition, 70 unknown, 80 human=harvest → NOT disturbance)

# Cut-event thresholds
CLEARCUT_BA_FRAC   = 0.85   # ≥ this fraction of pre-cut live BA removed → clear_cut
PRECOMMERCIAL_AGE  = 20     # mean cut-tree age < this (and not clear_cut) → pre_commercial_thin

# land_use derivation
PURITY_FRAC        = 0.80   # dominant-species live-BA share ≥ this → "pure" stand

COND_ID = ["STATECD", "UNITCD", "COUNTYCD", "PLOT", "CONDID"]
# visit-level attributes a synthetic (phantom / gap) row takes from ITS OWN visit (r7 C F3)
VISIT_ATTRS = ["CYCLE", "INVYR", "CONDID", "owngrpcd", "fortypcd", "physclcd", "reservcd", "cond_stdorgcd"]


# =============================================================================
# Step 1: load full tree history (ALL statuses) + BA + stratifiers
# =============================================================================

def load_tree_history(con, states: list[int] | None = None) -> pl.DataFrame:
    """
    One row per tree x measurement visit, ALL statuses (alive/dead/harvest).
    curated_trees is already restricted to longitudinal subplots upstream.
    Joins eco (data_plot_eco: epa_l3 / epa_l4 / ecosubcd), REF_SPECIES, and the four raw
    stratifier markers from COND.
    """
    cycle_col   = "t.CYCLE" if _has_column(con, "curated_trees", "CYCLE") else "NULL::INTEGER AS CYCLE"
    has_l4      = _has_column(con, "data_plot_eco", "epa_l4")
    epa_l4_expr = "e.epa_l4" if has_l4 else "NULL::VARCHAR AS epa_l4"
    # r7 D44 (C F6): ECOSUBCD from data_plot_eco (FIADB PLOT has no ECOSUBCD: it was 100 % NULL)
    ecosubcd_col = "e.ecosubcd::VARCHAR AS ecosubcd"

    where_state = f"AND t.STATECD IN ({','.join(map(str, states))})" if states else ""

    df = con.execute(f"""
        SELECT
            t.STATECD, t.UNITCD, t.COUNTYCD, t.PLOT, t.SUBP, t.TREE,
            t.spcd_resolved::INTEGER AS spcd,   -- resolved (mode) species, constant per tree
            t.SPCD::INTEGER          AS spcd_obs,  -- raw observed species at this visit
            t.spcd_changed,
            t.INVYR, t.CONDID, t.MEASDATE,
            {cycle_col},
            t.STATUSCD, t.tree_fate, t.AGENTCD, t.RECONCILECD,
            t.reconcile_new::INTEGER AS reconcile_new,          -- curation-fix 2026-10-26
            t.rerecord_dead,
            COALESCE(t.first_record_dead, FALSE) AS first_record_dead,   -- r7 D38
            t.RECONCILECD::INTEGER   AS rc_obs,
            t.censor_measdate, t.censor_reason,
            t.censor_last_measdate, t.censor_last_reason,
            t.DIA::DOUBLE  AS dia,
            t.HT::DOUBLE   AS ht,
            t.CR::DOUBLE   AS cr,
            t.DRYBIO_AG::DOUBLE AS drybio_ag,
            t.TPA_UNADJ::DOUBLE AS tpa,
            t.estimated_age::DOUBLE AS estimated_age,
            t.age_source,
            t.age_from_totage::DOUBLE AS age_from_totage,
            t.age_from_stdage::DOUBLE AS age_from_stdage,
            t.age_from_estage::DOUBLE AS age_from_estage,
            t.age_from_model::DOUBLE  AS age_from_model,
            t.cond_stdorgcd,
            t.DSTRBCD1, t.DSTRBCD2, t.DSTRBCD3,
            t.TRTCD1, t.TRTCD2, t.TRTCD3,
            e.epa_l3, {epa_l4_expr}, {ecosubcd_col},
            rr.SPECIES_SYMBOL::VARCHAR AS species_symbol,      -- resolved species
            ro.SPECIES_SYMBOL::VARCHAR AS species_symbol_obs,  -- observed species
            COALESCE(t.spgrpcd_resolved, t.SPGRPCD)::INTEGER AS spgrpcd,  -- resolved group
            rr.MAJOR_SPGRPCD::INTEGER  AS major_spgrpcd,
            rr.SFTWD_HRDWD::VARCHAR    AS sftwd_hrdwd,
            c.OWNGRPCD AS owngrpcd, c.FORTYPCD AS fortypcd,
            c.PHYSCLCD AS physclcd, c.RESERVCD AS reservcd
        FROM curated_trees t
        LEFT JOIN data_plot_eco e
            ON e.statecd=t.STATECD AND e.unitcd=t.UNITCD
            AND e.countycd=t.COUNTYCD AND e.plot=t.PLOT
        LEFT JOIN REF_SPECIES rr ON rr.SPCD = t.spcd_resolved
        LEFT JOIN REF_SPECIES ro ON ro.SPCD = t.SPCD
        LEFT JOIN COND c
            ON c.STATECD=t.STATECD AND c.UNITCD=t.UNITCD AND c.COUNTYCD=t.COUNTYCD
            AND c.PLOT=t.PLOT AND c.INVYR=t.INVYR AND c.CONDID=t.CONDID
        -- curation-fix (audit I1): TPA > 0 is a LIVE-record rule; dead/cut records with a
        -- null TPA are kept so they still end the trajectory as a death/removal
        WHERE (t.STATUSCD <> 1 OR t.TPA_UNADJ > 0) {where_state}
          AND NOT COALESCE(t.misrecorded_death, FALSE)   -- r6 R4: a misrecorded death is not a slot / fate
    """).pl()

    # Per-tree basal area (ft²/acre, TPA-expanded — for stand cut-event %) and
    # per-tree above-ground dry biomass in grams (NO TPA expansion).
    df = df.with_columns([
        (pl.col("dia").pow(2) * BA_FT2 * pl.col("tpa")).alias("ba"),
        (pl.col("drybio_ag") * LBS_TO_G).alias("agb"),
        # r7 D41: the ONE disturbance-death definition at this record (AGENTCD 10-50, or a
        # condition DSTRBCD other than 0 / 60), shared with subp_has_dstrb / curated_cohorts / drops
        disturbance_death_expr().alias("dist_death_def"),
    ])

    print(f"[load] {df.height:,} tree-measurements (all statuses) "
          f"across {df.select(PLOT_KEY).unique().height:,} plots")
    fates = df.group_by("tree_fate").len().sort("len", descending=True)
    for f, n in fates.iter_rows():
        print(f"        {str(f):20s}: {n:>12,}")
    return df


def subplot_visit_scaffold(con, states: list[int] | None = None) -> pl.DataFrame:
    """All clean subplot visits (SUBPLOT_ID + MEASDATE) — the visit grid a tree
    could have been tallied at."""
    where_state = f"WHERE STATECD IN ({','.join(map(str, states))})" if states else ""
    return con.execute(f"""
        SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, MEASDATE,
               MAX(forest_subpcond_prop) AS forest_subpcond_prop,
               MAX(forest_micrcond_prop) AS forest_micrcond_prop
        FROM clean_subplot_years {where_state} GROUP BY ALL
    """).pl()


# =============================================================================
# Step 2: trajectory body (live records) + sim_year + phantom backfill
# =============================================================================

def backfill_phantom_full(live: pl.DataFrame, visits: pl.DataFrame, ctx=None) -> pl.DataFrame:
    """
    Backward phantom backfill (intro_type='phantom_backfill') using the FULL
    subplot-visit scaffold (clean_subplot_years), not just visits where a live
    aged tree happens to exist. Otherwise a visit with no tallied live tree is
    invisible and leaves a hole before the tree's debut.

    Mirrors cohorts_landis.backfill_phantom_trees but with the full scaffold;
    reuses estimate_sapling_growth_rates for the biomass decay.
    """
    # sim_year for any scaffold visit, anchored to the same per-subplot first
    # live MEASDATE that compute_sim_year_ages used.
    # r3 M2: the SAME strict per-subplot axis as compute_sim_year_ages (sim_year_map)
    subp_first = live.group_by(SUBPLOT_ID).agg(pl.col("MEASDATE").min().alias("_first_measdate"))
    visit_sim = (
        sim_year_map(pl.concat([live.select(SUBPLOT_ID + ["MEASDATE"]),
                                visits.select(SUBPLOT_ID + ["MEASDATE"])]), subp_first)
        .rename({"MEASDATE": "prior_measdate", "sim_year": "prior_sim_year"})
    )

    tree_debut = live.pipe(pick, TREE_ID, ["MEASDATE"], "first")
    prior = (
        tree_debut.join(visit_sim, on=SUBPLOT_ID, how="left")
        .filter(pl.col("prior_measdate") < pl.col("MEASDATE"))
        .with_columns(
            (pl.col("age_calc") + pl.col("prior_sim_year") - pl.col("sim_year"))
            .cast(pl.Int32).alias("prior_age"))
        .filter(pl.col("prior_age") >= 1)
    )
    # curation-fix 2026-10-26: no backfill for RC6/8/9 entries, across a procedural/area
    # break, or for >=5in subplot ingrowth where the microplot represents the sapling class
    # rule (c) is a stand-expansion (TPA) double-count rule: not applied to per-tree,
    # unexpanded drybio_g trajectories (audit M3)
    prior = apply_backfill_blocks(prior, ctx, TREE_ID + ["prior_measdate"], rule_c=False)
    existing = (
        live.select(TREE_ID + ["MEASDATE"]).unique()
        .rename({"MEASDATE": "prior_measdate"}).with_columns(pl.lit(True).alias("_e"))
    )
    missing = (
        prior.join(existing, on=TREE_ID + ["prior_measdate"], how="left")
        .filter(pl.col("_e").is_null()).drop("_e").unique()
    )
    n = missing.height
    n_plots = missing.select(PLOT_KEY).unique().height if n else 0
    print(f"[backfill] {n:,} pre-debut phantom slots across {n_plots:,} plots")
    if n == 0:
        return live.with_columns(pl.lit(None).cast(pl.Utf8).alias("intro_type"))

    # r6 R1: trajectory agb is PER-TREE biomass (drybio_g), stated explicitly
    sp_rates, grp_rates, glb_rates = estimate_sapling_growth_rates(live, basis="per_tree", tag="traj")
    missing = (
        missing
        .join(sp_rates.rename({"rate": "_rs"}),  on=["species_symbol", "epa_l3"], how="left")
        .join(grp_rates.rename({"rate": "_rg"}), on=["spgrpcd",        "epa_l3"], how="left")
        .join(glb_rates.rename({"rate": "_rb"}), on="spgrpcd",                    how="left")
        .with_columns(pl.coalesce(["_rs", "_rg", "_rb"]).alias("_rate")).drop(["_rs", "_rg", "_rb"])
        .with_columns(pl.col("_rate").is_null().alias("_no_rate"))
        .with_columns(
            pl.when(pl.col("_rate").is_not_null())
            .then((pl.col("agb") *
                   (1.0 - pl.col("_rate") *
                    (pl.col("age_calc").cast(pl.Float64) - pl.col("prior_age").cast(pl.Float64))))
                  .clip(MIN_BACKFILL_DRYBIO_G, None))
            .otherwise(pl.lit(MIN_BACKFILL_DRYBIO_G))
            .clip(upper_bound=pl.col("agb"))   # r5 M-8: never above the debut value
            .alias("prior_agb")).drop("_rate")
    )
    report_phantom_floor(missing, "prior_agb", MIN_BACKFILL_DRYBIO_G, "g/tree", "backfill")
    missing = missing.drop("_no_rate")
    synthetic = (
        missing.drop(["MEASDATE", "age_calc", "agb", "sim_year"])
        .rename({"prior_measdate": "MEASDATE", "prior_age": "age_calc",
                 "prior_sim_year": "sim_year", "prior_agb": "agb"})
        .with_columns(pl.lit("phantom_backfill").cast(pl.Utf8).alias("intro_type"))
        # pre-debut size is unmeasured (only biomass is modelled) — don't carry the
        # cloned debut dia/ht/cr/ba, else they distort a diameter/height x-axis.
        .with_columns([pl.lit(None).cast(pl.Float64).alias(c)
                       for c in ("dia", "ht", "cr", "ba")])
    )
    print(f"[backfill] generated {synthetic.height:,} phantom rows")
    live = live.with_columns(pl.lit(None).cast(pl.Utf8).alias("intro_type"))
    return pl.concat([live, synthetic.select(live.columns)], how="vertical")


def build_live_body(df: pl.DataFrame, visits: pl.DataFrame, ctx=None) -> pl.DataFrame:
    """Live, aged, biomassed records → the plottable trajectory body, with
    integer sim_year/age_calc and full-scaffold backward phantom backfill."""
    live = (
        df.filter(
            (pl.col("STATUSCD") == 1) &
            pl.col("estimated_age").is_not_null() &
            pl.col("drybio_ag").is_not_null() &
            (pl.col("estimated_age").round(0, mode="half_away_from_zero") >= 1)
        )
        .with_columns(pl.col("estimated_age").round(0, mode="half_away_from_zero").cast(pl.Int32).alias("age_calc"))
    )
    print(f"[body] {live.height:,} live aged records")

    live = compute_sim_year_ages(live, visits)     # strict integer sim_year on the full scaffold
    live = add_backfill_flags(live, ctx)
    live = backfill_phantom_full(live, visits, ctx)  # full-scaffold backward backfill
    return live.drop(BF_FLAGS)


# =============================================================================
# Step 3: mid-trajectory gap interpolation
# =============================================================================

def interpolate_gaps(live: pl.DataFrame, visits: pl.DataFrame,
                     censor_events: pl.DataFrame | None = None) -> pl.DataFrame:
    """
    For every (tree, subplot-visit) strictly between a tree's first and last
    REAL record where the tree is absent, synthesise a row by linear
    interpolation (on MEASDATE) of dia/ht/cr/agb/ba between the bracketing real
    records. age_calc comes from the integer sim_year axis.
    """
    real = live.filter(pl.col("intro_type").is_null())

    bounds = real.group_by(TREE_ID).agg(
        pl.col("MEASDATE").min().alias("_first"),
        pl.col("MEASDATE").max().alias("_last"),
    )
    # sim_year for any subplot visit (taken from already-computed rows)
    # r3: sim_year of ANY scaffold visit on the strict axis (a gap visit may have no live row
    # of any tree — it used to get a null sim_year / age_calc)
    sy_map = sim_year_map(
        pl.concat([live.select(SUBPLOT_ID + ["MEASDATE"]), visits.select(SUBPLOT_ID + ["MEASDATE"])]),
        real.group_by(SUBPLOT_ID).agg(pl.col("MEASDATE").min().alias("_first_measdate")))

    # candidate gap slots: subplot visits between first & last real record
    cand = (
        bounds.join(visits, on=SUBPLOT_ID, how="left")
        .filter((pl.col("MEASDATE") > pl.col("_first")) &
                (pl.col("MEASDATE") < pl.col("_last")))
        .select(TREE_ID + ["MEASDATE"])
    )
    # drop slots that already exist (real OR phantom)
    existing = live.select(TREE_ID + ["MEASDATE"]).unique().with_columns(
        pl.lit(True).alias("_e"))
    gaps = (
        cand.join(existing, on=TREE_ID + ["MEASDATE"], how="left")
        .filter(pl.col("_e").is_null()).drop("_e")
    )
    n_gaps = gaps.height
    if n_gaps == 0:
        print("[interp] 0 mid-trajectory gaps")
        return live.with_columns(
            pl.lit(False).alias("was_missed_reintroduced"))

    # bracketing real records via asof joins (per tree, on MEASDATE)
    prev_src = real.with_columns(pl.col("MEASDATE").alias("prev_measdate"))
    nxt_cols = {"MEASDATE": "next_measdate", "dia": "next_dia", "ht": "next_ht",
                "cr": "next_cr", "agb": "next_agb", "ba": "next_ba"}
    nxt_src = (
        real.select(TREE_ID + ["MEASDATE", "dia", "ht", "cr", "agb", "ba"])
        .with_columns(pl.col("MEASDATE").alias("next_measdate"))
    )

    g = gaps.sort("MEASDATE")
    g = g.join_asof(prev_src.sort("MEASDATE"), on="MEASDATE", by=TREE_ID,
                    strategy="backward")
    g = g.join_asof(
        nxt_src.sort("MEASDATE").rename({"dia": "next_dia", "ht": "next_ht",
                                         "cr": "next_cr", "agb": "next_agb",
                                         "ba": "next_ba"}),
        on="MEASDATE", by=TREE_ID, strategy="forward")

    # curation-fix 2026-10-26: never interpolate across a censoring (STATUSCD=0 with
    # RECONCILECD 5/6/8/9 between the bracketing records): the tree was out of the sample.
    # Uses EVERY censoring event (tree_censor_events: STATUSCD=0 non-RC7 records + saplings
    # left unrecorded at a re-tally), so censor -> live ->
    # censor histories are handled (audit M8).
    n_g0 = g.height
    if censor_events is not None and censor_events.height:
        spans = (g.select(TREE_ID + ["MEASDATE", "prev_measdate", "next_measdate"])
                 .join(censor_events, on=TREE_ID, how="inner")
                 .filter((pl.col("_cdate") > pl.col("prev_measdate")) &
                         (pl.col("_cdate") <= pl.col("next_measdate")))
                 .select(TREE_ID + ["MEASDATE"]).unique()
                 .with_columns(pl.lit(True).alias("_span")))
        g = g.join(spans, on=TREE_ID + ["MEASDATE"], how="left").filter(
            pl.col("_span").is_null()).drop("_span")
    if n_g0 != g.height:
        print(f"[interp] {n_g0 - g.height:,} gap slots NOT filled (span a censoring interval)")
    # r5 M-4: never at, or across, an entirely NONFOREST subplot visit (the same forest test as
    # the cohort pass: forest subplot + microplot proportion <= 0); the tree cannot have been a
    # live forest tree there, and the land use broke the trajectory
    if {"forest_subpcond_prop", "forest_micrcond_prop"} <= set(visits.columns):
        n_g1 = g.height
        nf = (visits.filter((pl.col("forest_subpcond_prop").fill_null(0) + pl.col("forest_micrcond_prop").fill_null(0)) <= 0)
              .select(SUBPLOT_ID + [pl.col("MEASDATE").alias("_nfd")]))
        spans = (g.select(TREE_ID + ["MEASDATE", "prev_measdate", "next_measdate"])
                 .join(nf, on=SUBPLOT_ID, how="inner")
                 .filter((pl.col("_nfd") > pl.col("prev_measdate")) & (pl.col("_nfd") < pl.col("next_measdate")))
                 .select(TREE_ID + ["MEASDATE"]).unique().with_columns(pl.lit(True).alias("_span")))
        g = g.join(spans, on=TREE_ID + ["MEASDATE"], how="left").filter(pl.col("_span").is_null()).drop("_span")
        print(f"[interp] {n_g1 - g.height:,} gap slots NOT filled (at / across a nonforest visit)")
    n_gaps = g.height
    if n_gaps == 0:
        return live.with_columns(pl.lit(False).alias("was_missed_reintroduced"))

    # linear weight and interpolated values
    g = g.with_columns(
        (
            (pl.col("MEASDATE") - pl.col("prev_measdate")).dt.total_days() /
            (pl.col("next_measdate") - pl.col("prev_measdate")).dt.total_days()
        ).alias("_w")
    )
    for col, nxt in [("dia", "next_dia"), ("ht", "next_ht"), ("cr", "next_cr"),
                     ("agb", "next_agb"), ("ba", "next_ba")]:
        g = g.with_columns(
            (pl.col(col) + pl.col("_w") * (pl.col(nxt) - pl.col(col))).alias(col)
        )

    # sim_year + integer age_calc from the subplot axis
    debut = real.pipe(pick, TREE_ID, ["MEASDATE"], "first").select(
        TREE_ID + [pl.col("age_calc").alias("_dage"),
                   pl.col("sim_year").alias("_dsy")])
    g = (
        g.drop("sim_year")
        .join(sy_map, on=SUBPLOT_ID + ["MEASDATE"], how="left")
        .join(debut, on=TREE_ID, how="left")
        .with_columns(
            (pl.col("_dage") + pl.col("sim_year") - pl.col("_dsy")).alias("age_calc"))
    )

    synthetic = (
        g.with_columns([
            pl.lit(1).cast(live.schema["STATUSCD"]).alias("STATUSCD"),  # alive at the missed visit
            pl.lit("gap_interpolated").cast(pl.Utf8).alias("intro_type"),
        ])
        .select(live.columns)
    )
    n_trees = synthetic.select(TREE_ID).unique().height
    print(f"[interp] filled {n_gaps:,} mid-trajectory gap slots across {n_trees:,} trees")

    missed = synthetic.select(TREE_ID).unique().with_columns(
        pl.lit(True).alias("was_missed_reintroduced"))
    out = pl.concat([live, synthetic], how="vertical")
    out = out.join(missed, on=TREE_ID, how="left").with_columns(
        pl.col("was_missed_reintroduced").fill_null(False))
    return out


# Alternate per-source age axes (so the trajectory can be re-plotted on any
# age estimate, not just the selected `estimated_age`).
ALT_AGE_SOURCES = [
    ("age_from_totage", "age_totage"),
    ("age_from_stdage", "age_stdage"),
    ("age_from_estage", "age_estage"),
    ("age_from_model",  "age_model"),
]


def add_alt_age_axes(body: pl.DataFrame) -> pl.DataFrame:
    """
    For each age source, attach an integer, sim-year-anchored age column
    (`age_totage`, `age_stdage`, `age_estage`, `age_model`) aligned to the same
    sim_year grid as `age_calc`:
        age_<src> = round(<src> at debut) + (sim_year − debut_sim_year)
    Null where the tree never had that source. Because every source is linearly
    propagated per tree, the debut value fully determines the axis. Values < 1 are NULL (r7).
    """
    debut = (
        body.filter(pl.col("intro_type") == "real")
        .pipe(pick, TREE_ID, ["MEASDATE"], "first")
        .select(TREE_ID +
                [pl.col("sim_year").alias("_dsy0")] +
                [pl.col(src).round(0, mode="half_away_from_zero").alias(f"_d_{out}") for src, out in ALT_AGE_SOURCES])
    )
    body = body.join(debut, on=TREE_ID, how="left")
    # r7 D44 (C F12): an alternate axis is >= 1 or NULL (a phantom slot before the source's own
    # implied birth would otherwise get an age <= 0)
    body = body.with_columns([
        pl.when((pl.col(f"_d_{out}") + pl.col("sim_year") - pl.col("_dsy0")) >= 1)
        .then(pl.col(f"_d_{out}") + pl.col("sim_year") - pl.col("_dsy0"))
        .otherwise(pl.lit(None))
        .cast(pl.Int32).alias(out)
        for _, out in ALT_AGE_SOURCES
    ])
    return body.drop(["_dsy0"] + [f"_d_{out}" for _, out in ALT_AGE_SOURCES])


# =============================================================================
# Step 4: terminal fate (per tree)
# =============================================================================

def classify_terminal_fate(full: pl.DataFrame, visits: pl.DataFrame,
                           landuse_exits: pl.DataFrame | None = None) -> pl.DataFrame:
    """Per tree, derive terminal_fate + death_cause from its last real record and
    whether the subplot was visited again afterwards."""
    # r3 I1: the terminal record is the FIRST dead/cut record when the tree has one (later
    # dead records are re-records of the same death); otherwise the last (live) record
    cols = TREE_ID + ["MEASDATE", "STATUSCD", "tree_fate", "AGENTCD",
                      "dist_death_def", "censor_last_measdate", "censor_last_reason"]
    first_dead = (full.filter(pl.col("STATUSCD").is_in([2, 3]))
                  .pipe(pick, TREE_ID, ["MEASDATE"], "first").select(cols))
    last_live = (full.pipe(pick, TREE_ID, ["MEASDATE"], "last").select(cols)
                 .join(first_dead.select(TREE_ID), on=TREE_ID, how="anti"))
    last = pl.concat([first_dead, last_live]).rename({"MEASDATE": "terminal_measdate"})
    # was the subplot visited strictly after the tree's last record, and was that next
    # visit entirely nonforest / did the tree turn up live on a nonforest/water condition
    # (r2 M6 -> diverted_landuse)?
    nxt = _next_visit(last.select(TREE_ID + [pl.col("terminal_measdate").alias("_last_md")]), visits)
    last = last.join(nxt, on=TREE_ID, how="left").with_columns(
        pl.col("_later").fill_null(False).alias("_later_visit"))
    if landuse_exits is not None and landuse_exits.height:
        lex = (last.select(TREE_ID + ["terminal_measdate"]).join(landuse_exits, on=TREE_ID, how="inner")
               .filter(pl.col("exit_measdate") > pl.col("terminal_measdate"))
               .select(TREE_ID).unique().with_columns(pl.lit(True).alias("_lu_exit")))
        last = last.join(lex, on=TREE_ID, how="left")
    else:
        last = last.with_columns(pl.lit(None, dtype=pl.Boolean).alias("_lu_exit"))
    diverted = pl.col("_later_visit") & (pl.col("_lu_exit").fill_null(False) |
                                         pl.col("_next_nonforest").fill_null(False))

    is_dist = pl.col("dist_death_def")   # r7 D41 (cohorts_landis.disturbance_death_expr)

    last = last.with_columns(
        pl.when(pl.col("tree_fate").is_in(["silvicultural_death", "harvest_inferred", "diverted_landuse"]))
            .then(pl.col("tree_fate"))      # r3 M6: a cut on now-nonforest land = diverted_landuse
        .when((pl.col("STATUSCD") == 2) & is_dist)
            .then(pl.lit("dead_disturbance"))
        .when(pl.col("STATUSCD") == 2)
            .then(pl.lit("dead_natural"))
        # curation-fix 2026-10-26: STATUSCD=0 + RECONCILECD 5/6/8/9 after the last live
        # record = CENSORING (shrank / moved / procedural / nonsampled), not a removal
        .when((pl.col("STATUSCD") == 1) & pl.col("censor_last_measdate").is_not_null() &
              (pl.col("censor_last_measdate") > pl.col("terminal_measdate")))
            .then(pl.lit("censored_") + pl.col("censor_last_reason"))
        .when((pl.col("STATUSCD") == 1) & diverted)
            .then(pl.lit("diverted_landuse"))
        .when((pl.col("STATUSCD") == 1) & pl.col("_later_visit"))
            .then(pl.lit("disappeared_inferred_removal"))
        .when(pl.col("STATUSCD") == 1)
            .then(pl.lit("alive_censored"))
        .otherwise(pl.lit("unknown"))
        .alias("terminal_fate")
    ).with_columns(
        pl.when(pl.col("terminal_fate").is_in(["dead_disturbance"]))
            .then(pl.lit("disturbance"))
        .when(pl.col("terminal_fate") == "dead_natural")
            .then(pl.lit("natural"))
        .when(pl.col("terminal_fate").is_in(["silvicultural_death", "harvest_inferred"]))
            .then(pl.col("terminal_fate"))
        .when(pl.col("terminal_fate") == "disappeared_inferred_removal")
            .then(pl.lit("harvest_inferred"))
        .otherwise(pl.lit(None).cast(pl.Utf8))
        .alias("death_cause")
    )

    print("[fate] terminal_fate distribution:")
    for f, n in last.group_by("terminal_fate").len().sort("len", descending=True).iter_rows():
        print(f"        {str(f):30s}: {n:>10,}")

    return last.select(TREE_ID + ["terminal_measdate", "terminal_fate", "death_cause"])


# =============================================================================
# Step 5: cut-event classification (subplot x cut visit)
# =============================================================================

def classify_cut_events(full: pl.DataFrame) -> pl.DataFrame:
    """
    For each (SUBPLOT, cut visit) classify the removal (r7 D44 / C F11: keyed by subplot; r6 keyed
    it by CONDID, but 6 % of cut trees carry a cut-record CONDID other than their last-alive one
    and 15 % of events found no pre-cut denominator, so they could never be clear_cut):
      clear_cut          if removed BA ≥ CLEARCUT_BA_FRAC of the subplot's pre-cut live BA
      pre_commercial_thin if mean cut-tree age < PRECOMMERCIAL_AGE (and not clear_cut)
      commercial_thin     otherwise
    pre-cut BA of a cut tree = its BA at its last alive visit; denominator = the subplot's live
    BA at the latest visit before the cut. A cut tree is its first cut record (re-records are not
    cut events, r3 I1); r7 D38: a first_record_dead cut (no observed live tree) is not an event.
    """
    cut_rows = (
        full.filter(pl.col("tree_fate").is_in(["silvicultural_death", "harvest_inferred"])
                    & ~pl.col("rerecord_dead").fill_null(False)
                    & ~pl.col("first_record_dead").fill_null(False))
        .pipe(pick, TREE_ID, ["MEASDATE"], "first")
        .select(TREE_ID + ["MEASDATE"])
        .rename({"MEASDATE": "cut_measdate"})
    )
    if cut_rows.height == 0:
        print("[cut] no harvest records")
        return pl.DataFrame(schema={
            **{c: pl.Int64 for c in SUBPLOT_ID}, "cut_measdate": pl.Date,
            "cut_event_type": pl.Utf8, "cut_pct_ba_removed": pl.Float64,
            "cut_mean_age": pl.Float64})

    # each cut tree's last ALIVE ba + age (pre-cut)
    last_alive = (
        full.filter(pl.col("STATUSCD") == 1)
        .pipe(pick, TREE_ID, ["MEASDATE"], "last")
        .select(TREE_ID + [pl.col("ba").alias("_precut_ba"),
                           pl.col("estimated_age").alias("_precut_age"),
                           pl.col("MEASDATE").alias("_last_alive_measdate")])
    )
    cut = cut_rows.join(last_alive, on=TREE_ID, how="left")

    removed = (
        cut.group_by(SUBPLOT_ID + ["cut_measdate"])
        .agg(pl.col("_precut_ba").sum().alias("removed_ba"),
             pl.col("_precut_age").mean().alias("cut_mean_age"))
    )

    # live BA per (subplot, visit) — denominator is the visit immediately before the cut
    live_ba = (
        full.filter(pl.col("STATUSCD") == 1)
        .group_by(SUBPLOT_ID + ["MEASDATE"])
        .agg(pl.col("ba").sum().alias("live_ba"))
    )
    denom = (
        removed.join(live_ba, on=SUBPLOT_ID, how="left")
        .filter(pl.col("MEASDATE") < pl.col("cut_measdate"))
        .group_by(SUBPLOT_ID + ["cut_measdate"]).agg(pl.col("live_ba").sort_by("MEASDATE").last().alias("precut_live_ba"))
    )

    ev = (
        removed.join(denom, on=SUBPLOT_ID + ["cut_measdate"], how="left")
        .with_columns(
            pl.when(pl.col("precut_live_ba").is_not_null() & (pl.col("precut_live_ba") > 0))
            .then((pl.col("removed_ba") / pl.col("precut_live_ba")).clip(0.0, 1.0))
            .otherwise(pl.lit(None).cast(pl.Float64))
            .alias("cut_pct_ba_removed")
        )
        .with_columns(
            pl.when(pl.col("cut_pct_ba_removed") >= CLEARCUT_BA_FRAC)
                .then(pl.lit("clear_cut"))
            .when(pl.col("cut_mean_age") < PRECOMMERCIAL_AGE)
                .then(pl.lit("pre_commercial_thin"))
            .otherwise(pl.lit("commercial_thin"))
            .alias("cut_event_type")
        )
        .select(SUBPLOT_ID + ["cut_measdate", "cut_event_type",
                              "cut_pct_ba_removed", "cut_mean_age"])
        # r2 M2: float sums depend on thread summation order — round for determinism
        .with_columns(pl.col("cut_pct_ba_removed").round(9), pl.col("cut_mean_age").round(9))
    )
    print(f"[cut] cut-event classification (per subplot; {ev.filter(pl.col('cut_pct_ba_removed').is_null()).height:,} "
          f"of {ev.height:,} events without a pre-cut denominator):")
    for t, n in ev.group_by("cut_event_type").len().sort("len", descending=True).iter_rows():
        print(f"        {str(t):20s}: {n:>8,} events")
    return ev


# =============================================================================
# Step 6: land_use_4way derivation (per subplot)
# =============================================================================

def derive_land_use(full: pl.DataFrame, events: pl.DataFrame) -> pl.DataFrame:
    """Derive the 4-way management class `land_use_4way` per SUBPLOT (r7: the cut events are per
    subplot, C F11; r6 derived it per condition and called it land_use) from management history +
    forest-type purity, keeping the component markers. The 2-way `land_use` is the STDORGCD rule
    (cohorts_landis.plot_land_use_sql), attached in build_tree_trajectories."""
    # management history from cut events
    hist = (
        events.group_by(SUBPLOT_ID).agg([
            (pl.col("cut_event_type") == "clear_cut").any().alias("has_clearcut"),
            (pl.col("cut_event_type") == "commercial_thin").any().alias("has_commercial_thin"),
            (pl.col("cut_event_type") == "pre_commercial_thin").any().alias("has_precommercial_thin"),
        ])
    )
    # planted / reserved per subplot (over the conditions of its records)
    cond_attr = (
        full.group_by(SUBPLOT_ID).agg([
            (pl.col("cond_stdorgcd") == 1).any().alias("is_planted"),
            pl.col("reservcd").max().alias("reservcd_max"),
        ])
    )
    # dominant-species live-BA share per subplot (peak purity across visits)
    sp_ba = (
        full.filter(pl.col("STATUSCD") == 1)
        .group_by(SUBPLOT_ID + ["MEASDATE", "spcd"]).agg(pl.col("ba").sum().alias("sp_ba"))
    )
    purity = (
        sp_ba.group_by(SUBPLOT_ID + ["MEASDATE"])
        .agg((pl.col("sp_ba").max() / pl.col("sp_ba").sum()).alias("_top_share"))
        .group_by(SUBPLOT_ID).agg(pl.col("_top_share").max().round(9).alias("dominant_species_frac"))
    )

    lu = (
        cond_attr
        .join(hist, on=SUBPLOT_ID, how="left")
        .join(purity, on=SUBPLOT_ID, how="left")
        .with_columns([
            pl.col("has_clearcut").fill_null(False),
            pl.col("has_commercial_thin").fill_null(False),
            pl.col("has_precommercial_thin").fill_null(False),
            pl.col("is_planted").fill_null(False),
            (pl.col("dominant_species_frac") >= PURITY_FRAC).alias("is_pure"),
        ])
        .with_columns(
            pl.when(pl.col("reservcd_max") == 1)
                .then(pl.lit("reserved"))
            .when(pl.col("is_planted") | (pl.col("has_clearcut") & pl.col("is_pure").fill_null(False)))
                .then(pl.lit("plantation"))
            .when(pl.col("has_clearcut") | pl.col("has_commercial_thin") | pl.col("has_precommercial_thin"))
                .then(pl.lit("managed"))
            .otherwise(pl.lit("natural"))
            .alias("land_use_4way")
        )
    )
    print("[land_use_4way] distribution (per subplot):")
    for u, n in lu.group_by("land_use_4way").len().sort("len", descending=True).iter_rows():
        print(f"        {str(u):14s}: {n:>8,} subplots")
    return lu


# =============================================================================
# Step 6b: synthetic rows take their own visit's attributes (r7 D44 / C F3)
# =============================================================================

def visit_attributes(con, states: list[int] | None = None) -> pl.DataFrame:
    """Per subplot visit (SUBPLOT_ID + MEASDATE): its INVYR and CYCLE, and its condition when the
    subplot has exactly ONE forest condition (COND_STATUS_CD 1) there (else NULL: the tree's
    condition at an unobserved slot is unknown), with that condition's COND stratifiers."""
    ws = f"AND b.STATECD IN ({','.join(map(str, states))})" if states else ""
    st = ",".join(str(int(r[0])) for r in con.execute(
        f"SELECT DISTINCT STATECD FROM base_subplot_cond b WHERE TRUE {ws} ORDER BY 1").fetchall()) or "NULL"
    return con.execute(f"""
        WITH v AS (
            SELECT b.STATECD, b.UNITCD, b.COUNTYCD, b.PLOT, b.SUBP, b.MEASDATE,
                   min(b.INVYR) AS INVYR, max(b.CYCLE) AS CYCLE,   -- invyr-ok: one value per visit
                   count(DISTINCT b.CONDID) FILTER (WHERE b.COND_STATUS_CD = 1) AS _nf,
                   min(b.CONDID) FILTER (WHERE b.COND_STATUS_CD = 1) AS _cid
            FROM base_subplot_cond b WHERE TRUE {ws} GROUP BY ALL)
        SELECT v.STATECD, v.UNITCD, v.COUNTYCD, v.PLOT, v.SUBP, v.MEASDATE,
               v.INVYR::INTEGER AS _v_INVYR, v.CYCLE AS _v_CYCLE,
               CASE WHEN v._nf = 1 THEN v._cid END AS _v_CONDID,
               c.OWNGRPCD AS _v_owngrpcd, c.FORTYPCD AS _v_fortypcd, c.PHYSCLCD AS _v_physclcd,
               c.RESERVCD AS _v_reservcd, c.STDORGCD AS _v_cond_stdorgcd
        FROM v LEFT JOIN (SELECT * FROM COND WHERE STATECD IN ({st})) c   -- prune the FIADB-wide view
          ON v._nf = 1 AND c.STATECD = v.STATECD AND c.UNITCD = v.UNITCD AND c.COUNTYCD = v.COUNTYCD
         AND c.PLOT = v.PLOT AND c.INVYR = v.INVYR AND c.CONDID = v._cid
    """).pl()


def restamp_synthetic_rows(body: pl.DataFrame, vattr: pl.DataFrame) -> pl.DataFrame:
    """Phantom / gap rows: VISIT_ATTRS from their own visit (visit_attributes), and estimated_age
    = the tree's propagated age at that date (debut real estimated_age + elapsed years). Real
    rows are unchanged."""
    debut = (body.filter(pl.col("intro_type") == "real").pipe(pick, TREE_ID, ["MEASDATE"], "first")
             .select(TREE_ID + [pl.col("MEASDATE").alias("_d_md"), pl.col("estimated_age").alias("_d_age")]))
    syn = pl.col("intro_type") != "real"
    body = body.join(vattr, on=SUBPLOT_ID + ["MEASDATE"], how="left").join(debut, on=TREE_ID, how="left")
    body = body.with_columns(
        [pl.when(syn).then(pl.col(f"_v_{c}").cast(body.schema[c])).otherwise(pl.col(c)).alias(c) for c in VISIT_ATTRS]
        + [pl.when(syn).then(pl.col("_d_age") + (pl.col("MEASDATE") - pl.col("_d_md")).dt.total_days() / 365.25)
             .otherwise(pl.col("estimated_age")).alias("estimated_age")])
    n = body.filter(syn).height
    print(f"[restamp] {n:,} synthetic rows take their own visit's cycle / INVYR / condid / COND attributes "
          f"({body.filter(syn & pl.col('CONDID').is_null()).height:,} without a single forest condition -> condid NULL)")
    return body.drop([f"_v_{c}" for c in VISIT_ATTRS] + ["_d_md", "_d_age"])


# =============================================================================
# Step 7: assemble + write
# =============================================================================

def build_tree_trajectories(con, states: list[int] | None = None) -> pl.DataFrame:
    print("=" * 70)
    print("Tree Life Trajectories")
    print("=" * 70)

    full   = load_tree_history(con, states)
    visits = subplot_visit_scaffold(con, states)

    # subplot-level attributes derived from the FULL record set (r7: cut events per subplot)
    events = classify_cut_events(full)
    import dbconn as _db
    _db.require_tables(con, "tree_landuse_exits")
    _ws = f"WHERE STATECD IN ({','.join(map(str, states))})" if states else ""
    landuse_exits = con.execute(f"""SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE, exit_measdate
                                    FROM tree_landuse_exits {_ws}""").pl()
    fate   = classify_terminal_fate(full, visits, landuse_exits)
    lu     = derive_land_use(full, events)

    # trajectory body (live + phantom + interpolated)
    body = build_live_body(full, visits, load_backfill_context(con))
    import dbconn
    dbconn.require_tables(con, "tree_censor_events")
    ws = f"AND STATECD IN ({','.join(map(str, states))})" if states else ""
    censor_events = con.execute(f"""
        SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE, MEASDATE AS _cdate
        FROM tree_censor_events WHERE TRUE {ws}
        UNION ALL   -- r3: never interpolate a 'live' slot across / at a dead record (data errors)
        SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE, MEASDATE
        FROM curated_trees WHERE STATUSCD IN (2, 3) AND NOT COALESCE(misrecorded_death, FALSE) {ws}""").pl()
    body = interpolate_gaps(body, visits, censor_events)
    body = body.with_columns(pl.col("intro_type").fill_null("real"))

    # alternate per-source age axes (TOTAGE / STDAGE / estage / model)
    body = add_alt_age_axes(body)

    # r7 C F3: phantom / gap rows take their own visit's cycle / INVYR / condid / COND attributes
    body = restamp_synthetic_rows(body, visit_attributes(con, states))

    # was_gap_interpolated flag per row
    body = body.with_columns(
        (pl.col("intro_type") == "gap_interpolated").alias("was_gap_interpolated"))

    # attach tree-level terminal fate; flag the last body slot as terminal
    body = body.join(fate, on=TREE_ID, how="left")
    last_slot = body.group_by(TREE_ID).agg(pl.col("MEASDATE").max().alias("_last_slot"))
    body = body.join(last_slot, on=TREE_ID, how="left").with_columns(
        (pl.col("MEASDATE") == pl.col("_last_slot")).alias("is_terminal")).drop("_last_slot")

    # attach per-row subplot cut-event (event landing on this measdate/subplot; r7 C F11)
    ev_join = events.rename({"cut_measdate": "MEASDATE"})
    body = body.join(ev_join, on=SUBPLOT_ID + ["MEASDATE"], how="left")

    # attach land_use_4way + components per subplot (r7)
    body = body.join(lu.select(SUBPLOT_ID + [
        "land_use_4way", "is_planted", "has_clearcut", "has_commercial_thin",
        "has_precommercial_thin", "is_pure", "dominant_species_frac",
    ]), on=SUBPLOT_ID, how="left")
    # r7 D44 (C F10): `land_use` is the 2-way STDORGCD rule shared by every cohort table
    plu = con.execute(plot_land_use_sql(con)).pl().rename(
        {"statecd": "STATECD", "unitcd": "UNITCD", "countycd": "COUNTYCD", "plot": "PLOT"})
    body = body.join(plu.with_columns([pl.col(k).cast(body.schema[k]) for k in PLOT_KEY]), on=PLOT_KEY, how="left")

    # subplot disturbance flag: the ONE D41 definition (cohorts_landis.build_subp_has_dstrb), the
    # same as Sim A / Sim B (r6 used the trajectories' own terminal fates, a third definition)
    sflag = build_subp_has_dstrb(con, states)
    body = (body.join(sflag.with_columns([pl.col(k).cast(body.schema[k]) for k in SUBPLOT_ID]),
                      on=SUBPLOT_ID, how="left")
                .with_columns(pl.col("subp_has_dstrb").fill_null(False)))

    # plot / subplot measurement counts
    plot_n = (body.select(PLOT_KEY + ["MEASDATE"]).unique()
              .group_by(PLOT_KEY).agg(pl.col("MEASDATE").n_unique().alias("plot_meas_num")))
    subp_n = (body.select(SUBPLOT_ID + ["MEASDATE"]).unique()
              .group_by(SUBPLOT_ID).agg(pl.col("MEASDATE").n_unique().alias("subp_meas_num")))
    body = body.join(plot_n, on=PLOT_KEY, how="left").join(subp_n, on=SUBPLOT_ID, how="left")

    # trajectory_complete: a slot exists at every subplot VISIT between the
    # tree's first and last slot (FIA visits are ~5 yr apart, not annual).
    span = body.group_by(TREE_ID).agg(
        pl.col("MEASDATE").min().alias("_bmin"),
        pl.col("MEASDATE").max().alias("_bmax"),
        pl.col("MEASDATE").n_unique().alias("_nslots"))
    vis_in = (
        span.join(visits, on=SUBPLOT_ID, how="left")
        .filter((pl.col("MEASDATE") >= pl.col("_bmin")) &
                (pl.col("MEASDATE") <= pl.col("_bmax")))
        .group_by(TREE_ID).agg(pl.col("MEASDATE").n_unique().alias("_nvisits"))
    )
    span = (
        span.join(vis_in, on=TREE_ID, how="left")
        .with_columns((pl.col("_nslots") >= pl.col("_nvisits")).alias("trajectory_complete"))
        .select(TREE_ID + ["trajectory_complete"])
    )
    body = body.join(span, on=TREE_ID, how="left")

    # plt_cn (per-plot string key)
    body = body.with_columns(
        pl.concat_str([pl.col(c).cast(pl.Utf8) for c in PLOT_KEY], separator="_").alias("plt_cn"))

    out = body.rename({
        "STATECD": "statecd", "UNITCD": "unitcd", "COUNTYCD": "countycd",
        "PLOT": "plot", "SUBP": "subp", "TREE": "tree", "MEASDATE": "measdate",
        "CONDID": "condid", "CYCLE": "cycle", "agb": "drybio_g",
    }).select([
        "plt_cn", "statecd", "unitcd", "countycd", "plot", "subp", "tree",
        "measdate", "cycle", "sim_year", "age_calc", "estimated_age", "age_source",
        "age_totage", "age_stdage", "age_estage", "age_model",
        "dia", "ht", "cr", "ba", "drybio_g", "tpa",
        "spcd", "species_symbol", "spcd_obs", "species_symbol_obs", "spcd_changed",
        "spgrpcd", "major_spgrpcd", "sftwd_hrdwd",
        "intro_type", "was_gap_interpolated", "was_missed_reintroduced",
        "trajectory_complete", "is_terminal", "terminal_fate", "death_cause",
        "condid", "cut_event_type", "cut_pct_ba_removed", "cut_mean_age",
        "epa_l3", "epa_l4", "ecosubcd",
        "owngrpcd", "fortypcd", "physclcd", "reservcd",
        "land_use", "is_planted", "has_clearcut", "has_commercial_thin",
        "has_precommercial_thin", "is_pure", "dominant_species_frac",
        "subp_has_dstrb", "plot_meas_num", "subp_meas_num",
        "reconcile_new", "censor_reason",                  # curation-fix 2026-10-26
        "land_use_4way",                                   # r7 C F10 (land_use is 2-way)
    ]).sort(["statecd", "unitcd", "countycd", "plot", "subp", "tree", "measdate"])

    # summary
    n_trees = out.select(["statecd", "unitcd", "countycd", "plot", "subp", "tree"]).unique().height
    n_incomplete = out.filter(~pl.col("trajectory_complete")).select(
        ["statecd", "unitcd", "countycd", "plot", "subp", "tree"]).unique().height
    print(f"\n[summary] {out.height:,} rows · {n_trees:,} trees")
    print(f"          intro_type: " + ", ".join(
        f"{t}={n:,}" for t, n in out.group_by("intro_type").len().sort("len", descending=True).iter_rows()))
    print(f"          incomplete trajectories (sim_year gaps remaining): {n_incomplete:,}")
    return out


if __name__ == "__main__":
    # curation-fix 2026-10-26: explicit --out-db/--fiadb (dbconn); optional --states.
    import argparse
    import dbconn
    a = dbconn.add_db_args(argparse.ArgumentParser(description="tree_trajectories")).parse_args()
    dbconn.check_args(a)
    states = dbconn.parse_states(a.states)
    print(f"DB: {a.out_db}" + (f"  states={states}" if states else ""))
    con = dbconn.connect(a.out_db, a.fiadb)
    dbconn.require_tables(con, "curated_trees", "clean_subplot_years", "data_plot_eco")
    traj = build_tree_trajectories(con, states)

    con.register("_traj_view", traj)
    con.execute("CREATE OR REPLACE TABLE tree_trajectories AS SELECT * FROM _traj_view")
    add_rc7_withdrawn_flag(con, "tree_trajectories")   # r7 D39: visit metadata (appended)
    import cohorts_landis as _c1
    if "traj" in _c1.RATE_TABLES:   # r6 R1: the trajectory sapling growth rates (per-tree basis)
        con.register("_rate_view", _c1.RATE_TABLES["traj"])
        con.execute("CREATE OR REPLACE TABLE sapling_growth_rates_traj AS SELECT * FROM _rate_view")
        con.unregister("_rate_view")
    con.unregister("_traj_view")
    con.commit()
    print(f"[save] tree_trajectories written ({traj.height:,} rows)")
