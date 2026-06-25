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

Each trajectory carries how it ended (terminal_fate / death_cause), the condition
disturbance/cut-event class (pre_commercial_thin / commercial_thin / clear_cut),
and stratification markers: ecoregion, cycle, ownership / forest-type /
physiographic / reserved, plus a DERIVED land_use marker.

Biomass is the per-tree above-ground dry biomass DRYBIO_AG in grams (`drybio_g`),
NOT TPA-expanded per-area — these are individual-tree trajectories.

Reuses cohorts_landis: compute_sim_year_ages, estimate_sapling_growth_rates,
MIN_BACKFILL_AGB, and the key lists.

Usage:
    .venv/bin/python build_tree_trajectories.py ../FIASQLITE2PGSQL/FIADB.duckdb
    .venv/bin/python build_tree_trajectories.py <db> 37        # smoke test: STATECD=37 only
"""

import sys

import duckdb
import polars as pl

from cohorts_landis import (
    PLOT_KEY, SUBPLOT_ID, TREE_ID,
    MIN_BACKFILL_AGB,
    compute_sim_year_ages,
    estimate_sapling_growth_rates,
    _has_column,
)

# Forester's constant: basal area (ft²) = 0.005454 * DIA(in)² ; * TPA → ft²/acre
BA_FT2 = 0.005454
# DRYBIO_AG is per-tree above-ground dry biomass in POUNDS → grams (per tree).
LBS_TO_G = 453.592

# Death-cause: FIA AGENTCD codes treated as "disturbance" agents (else natural).
#   10 insect · 20 disease · 30 fire · 40 animal · 50 weather
#   (60 vegetation/competition, 70 unknown, 80 human=harvest → NOT disturbance here)
DISTURBANCE_AGENTS = [10, 20, 30, 40, 50]

# Cut-event thresholds
CLEARCUT_BA_FRAC   = 0.85   # ≥ this fraction of pre-cut live BA removed → clear_cut
PRECOMMERCIAL_AGE  = 20     # mean cut-tree age < this (and not clear_cut) → pre_commercial_thin

# land_use derivation
PURITY_FRAC        = 0.80   # dominant-species live-BA share ≥ this → "pure" stand

COND_ID = ["STATECD", "UNITCD", "COUNTYCD", "PLOT", "CONDID"]


# =============================================================================
# Step 1: load full tree history (ALL statuses) + BA + stratifiers
# =============================================================================

def load_tree_history(con, states: list[int] | None = None) -> pl.DataFrame:
    """
    One row per tree x measurement visit, ALL statuses (alive/dead/harvest).
    curated_trees is already restricted to longitudinal subplots upstream.
    Joins eco (data_plot_eco + PLOT.ecosubcd), REF_SPECIES, and the four raw
    stratifier markers from COND.
    """
    cycle_col   = "t.CYCLE" if _has_column(con, "curated_trees", "CYCLE") else "NULL::INTEGER AS CYCLE"
    has_l4      = _has_column(con, "data_plot_eco", "epa_l4")
    epa_l4_expr = "e.epa_l4" if has_l4 else "NULL::VARCHAR AS epa_l4"
    has_eco     = _has_column(con, "PLOT", "ECOSUBCD")
    if has_eco:
        ecosubcd_join = """
        LEFT JOIN (
            SELECT STATECD, UNITCD, COUNTYCD, PLOT, FIRST(ECOSUBCD) AS ecosubcd
            FROM PLOT GROUP BY STATECD, UNITCD, COUNTYCD, PLOT
        ) plt ON plt.STATECD=t.STATECD AND plt.UNITCD=t.UNITCD
            AND plt.COUNTYCD=t.COUNTYCD AND plt.PLOT=t.PLOT
        """
        ecosubcd_col = "plt.ecosubcd"
    else:
        ecosubcd_join, ecosubcd_col = "", "NULL::VARCHAR AS ecosubcd"

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
            t.SPGRPCD::INTEGER         AS spgrpcd,
            rr.MAJOR_SPGRPCD::INTEGER  AS major_spgrpcd,
            rr.SFTWD_HRDWD::VARCHAR    AS sftwd_hrdwd,
            c.OWNGRPCD AS owngrpcd, c.FORTYPCD AS fortypcd,
            c.PHYSCLCD AS physclcd, c.RESERVCD AS reservcd
        FROM curated_trees t
        LEFT JOIN data_plot_eco e
            ON e.statecd=t.STATECD AND e.unitcd=t.UNITCD
            AND e.countycd=t.COUNTYCD AND e.plot=t.PLOT
        {ecosubcd_join}
        LEFT JOIN REF_SPECIES rr ON rr.SPCD = t.spcd_resolved
        LEFT JOIN REF_SPECIES ro ON ro.SPCD = t.SPCD
        LEFT JOIN COND c
            ON c.STATECD=t.STATECD AND c.UNITCD=t.UNITCD AND c.COUNTYCD=t.COUNTYCD
            AND c.PLOT=t.PLOT AND c.INVYR=t.INVYR AND c.CONDID=t.CONDID
        WHERE t.TPA_UNADJ > 0 {where_state}
    """).pl()

    # Per-tree basal area (ft²/acre, TPA-expanded — for stand cut-event %) and
    # per-tree above-ground dry biomass in grams (NO TPA expansion).
    df = df.with_columns([
        (pl.col("dia").pow(2) * BA_FT2 * pl.col("tpa")).alias("ba"),
        (pl.col("drybio_ag") * LBS_TO_G).alias("agb"),
        # disturbed-condition flag at this row (any DSTRBCD present & not 0/60)
        (
            ((pl.col("DSTRBCD1").is_not_null()) & (~pl.col("DSTRBCD1").is_in([0, 60]))) |
            ((pl.col("DSTRBCD2").is_not_null()) & (~pl.col("DSTRBCD2").is_in([0, 60]))) |
            ((pl.col("DSTRBCD3").is_not_null()) & (~pl.col("DSTRBCD3").is_in([0, 60])))
        ).alias("row_disturbed"),
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
        SELECT DISTINCT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, MEASDATE
        FROM clean_subplot_years {where_state}
    """).pl()


# =============================================================================
# Step 2: trajectory body (live records) + sim_year + phantom backfill
# =============================================================================

def backfill_phantom_full(live: pl.DataFrame, visits: pl.DataFrame) -> pl.DataFrame:
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
    subp_first = live.group_by(SUBPLOT_ID).agg(pl.col("MEASDATE").min().alias("_f"))
    visit_sim = (
        visits.join(subp_first, on=SUBPLOT_ID, how="inner")
        .with_columns(
            ((pl.col("MEASDATE") - pl.col("_f")).dt.total_days() / 365.25)
            .round(0).cast(pl.Int32).alias("prior_sim_year"))
        .select(SUBPLOT_ID + [pl.col("MEASDATE").alias("prior_measdate"), "prior_sim_year"])
    )

    tree_debut = live.sort(TREE_ID + ["MEASDATE"]).unique(subset=TREE_ID, keep="first")
    prior = (
        tree_debut.join(visit_sim, on=SUBPLOT_ID, how="left")
        .filter(pl.col("prior_measdate") < pl.col("MEASDATE"))
        .with_columns(
            (pl.col("age_calc") + pl.col("prior_sim_year") - pl.col("sim_year"))
            .cast(pl.Int32).alias("prior_age"))
        .filter(pl.col("prior_age") >= 1)
    )
    existing = (
        live.select(TREE_ID + ["MEASDATE"]).unique()
        .rename({"MEASDATE": "prior_measdate"}).with_columns(pl.lit(True).alias("_e"))
    )
    missing = (
        prior.join(existing, on=TREE_ID + ["prior_measdate"], how="left")
        .filter(pl.col("_e").is_null()).drop("_e")
        .unique(subset=TREE_ID + ["prior_measdate"], keep="first")
    )
    n = missing.height
    n_plots = missing.select(PLOT_KEY).unique().height if n else 0
    print(f"[backfill] {n:,} pre-debut phantom slots across {n_plots:,} plots")
    if n == 0:
        return live.with_columns(pl.lit(None).cast(pl.Utf8).alias("intro_type"))

    sp_rates, grp_rates, glb_rates = estimate_sapling_growth_rates(live)
    missing = (
        missing
        .join(sp_rates.rename({"rate": "_rs"}),  on=["species_symbol", "epa_l3"], how="left")
        .join(grp_rates.rename({"rate": "_rg"}), on=["spgrpcd",        "epa_l3"], how="left")
        .join(glb_rates.rename({"rate": "_rb"}), on="spgrpcd",                    how="left")
        .with_columns(pl.coalesce(["_rs", "_rg", "_rb"]).alias("_rate")).drop(["_rs", "_rg", "_rb"])
        .with_columns(
            pl.when(pl.col("_rate").is_not_null())
            .then((pl.col("agb") *
                   (1.0 - pl.col("_rate") *
                    (pl.col("age_calc").cast(pl.Float64) - pl.col("prior_age").cast(pl.Float64))))
                  .clip(MIN_BACKFILL_AGB, None))
            .otherwise(pl.lit(MIN_BACKFILL_AGB))
            .alias("prior_agb")).drop("_rate")
    )
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


def build_live_body(df: pl.DataFrame, visits: pl.DataFrame) -> pl.DataFrame:
    """Live, aged, biomassed records → the plottable trajectory body, with
    integer sim_year/age_calc and full-scaffold backward phantom backfill."""
    live = (
        df.filter(
            (pl.col("STATUSCD") == 1) &
            pl.col("estimated_age").is_not_null() &
            pl.col("drybio_ag").is_not_null() &
            (pl.col("estimated_age").round(0) >= 1)
        )
        .with_columns(pl.col("estimated_age").round(0).cast(pl.Int32).alias("age_calc"))
    )
    print(f"[body] {live.height:,} live aged records")

    live = compute_sim_year_ages(live)             # integer sim_year + exact age_calc
    live = backfill_phantom_full(live, visits)     # full-scaffold backward backfill
    return live


# =============================================================================
# Step 3: mid-trajectory gap interpolation
# =============================================================================

def interpolate_gaps(live: pl.DataFrame, visits: pl.DataFrame) -> pl.DataFrame:
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
    sy_map = live.select(SUBPLOT_ID + ["MEASDATE", "sim_year"]).unique()

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
    debut = real.sort(TREE_ID + ["MEASDATE"]).unique(subset=TREE_ID, keep="first").select(
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
    propagated per tree, the debut value fully determines the axis.
    """
    debut = (
        body.filter(pl.col("intro_type") == "real")
        .sort(TREE_ID + ["MEASDATE"]).unique(subset=TREE_ID, keep="first")
        .select(TREE_ID +
                [pl.col("sim_year").alias("_dsy0")] +
                [pl.col(src).round(0).alias(f"_d_{out}") for src, out in ALT_AGE_SOURCES])
    )
    body = body.join(debut, on=TREE_ID, how="left")
    body = body.with_columns([
        (pl.col(f"_d_{out}") + pl.col("sim_year") - pl.col("_dsy0"))
        .cast(pl.Int32).alias(out)
        for _, out in ALT_AGE_SOURCES
    ])
    return body.drop(["_dsy0"] + [f"_d_{out}" for _, out in ALT_AGE_SOURCES])


# =============================================================================
# Step 4: terminal fate (per tree)
# =============================================================================

def classify_terminal_fate(full: pl.DataFrame, visits: pl.DataFrame) -> pl.DataFrame:
    """Per tree, derive terminal_fate + death_cause from its last real record and
    whether the subplot was visited again afterwards."""
    last = (
        full.sort(TREE_ID + ["MEASDATE"]).unique(subset=TREE_ID, keep="last")
        .select(TREE_ID + ["MEASDATE", "STATUSCD", "tree_fate", "AGENTCD",
                           "row_disturbed"])
        .rename({"MEASDATE": "terminal_measdate"})
    )
    # was the subplot visited strictly after the tree's last record?
    later = (
        last.join(visits, on=SUBPLOT_ID, how="left")
        .filter(pl.col("MEASDATE") > pl.col("terminal_measdate"))
        .group_by(TREE_ID).agg(pl.len().alias("_n_later"))
    )
    last = last.join(later, on=TREE_ID, how="left").with_columns(
        (pl.col("_n_later").fill_null(0) > 0).alias("_later_visit"))

    is_dist = pl.col("AGENTCD").is_in(DISTURBANCE_AGENTS) | pl.col("row_disturbed")

    last = last.with_columns(
        pl.when(pl.col("tree_fate").is_in(["harvest_explicit", "harvest_inferred"]))
            .then(pl.col("tree_fate"))
        .when((pl.col("STATUSCD") == 2) & is_dist)
            .then(pl.lit("dead_disturbance"))
        .when(pl.col("STATUSCD") == 2)
            .then(pl.lit("dead_natural"))
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
        .when(pl.col("terminal_fate").str.starts_with("harvest"))
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
# Step 5: cut-event classification (condition x cut visit)
# =============================================================================

def classify_cut_events(full: pl.DataFrame) -> pl.DataFrame:
    """
    For each (condition, cut visit) classify the removal:
      clear_cut          if removed BA ≥ CLEARCUT_BA_FRAC of pre-cut live BA
      pre_commercial_thin if mean cut-tree age < PRECOMMERCIAL_AGE (and not clear_cut)
      commercial_thin     otherwise
    pre-cut BA of a cut tree = its BA at its last alive visit.
    """
    # cut trees: harvest fate; cut happens at the harvest record's MEASDATE
    cut_rows = (
        full.filter(pl.col("tree_fate").is_in(["harvest_explicit", "harvest_inferred"]))
        .sort(TREE_ID + ["MEASDATE"]).unique(subset=TREE_ID, keep="last")
        .select(TREE_ID + ["CONDID", "MEASDATE"])
        .rename({"MEASDATE": "cut_measdate"})
    )
    if cut_rows.height == 0:
        print("[cut] no harvest records")
        return pl.DataFrame(schema={
            **{c: pl.Int64 for c in COND_ID}, "cut_measdate": pl.Date,
            "cut_event_type": pl.Utf8, "cut_pct_ba_removed": pl.Float64,
            "cut_mean_age": pl.Float64})

    # each cut tree's last ALIVE ba + age (pre-cut)
    last_alive = (
        full.filter(pl.col("STATUSCD") == 1)
        .sort(TREE_ID + ["MEASDATE"]).unique(subset=TREE_ID, keep="last")
        .select(TREE_ID + [pl.col("ba").alias("_precut_ba"),
                           pl.col("estimated_age").alias("_precut_age"),
                           pl.col("MEASDATE").alias("_last_alive_measdate")])
    )
    cut = cut_rows.join(last_alive, on=TREE_ID, how="left")

    removed = (
        cut.group_by(COND_ID + ["cut_measdate"])
        .agg(pl.col("_precut_ba").sum().alias("removed_ba"),
             pl.col("_precut_age").mean().alias("cut_mean_age"))
    )

    # live BA per (condition, visit) — denominator candidate is the visit
    # immediately before the cut
    live_ba = (
        full.filter(pl.col("STATUSCD") == 1)
        .group_by(COND_ID + ["MEASDATE"])
        .agg(pl.col("ba").sum().alias("live_ba"))
    )
    denom = (
        removed.join(live_ba, on=COND_ID, how="left")
        .filter(pl.col("MEASDATE") < pl.col("cut_measdate"))
        .sort(COND_ID + ["cut_measdate", "MEASDATE"])
        .group_by(COND_ID + ["cut_measdate"]).agg(pl.col("live_ba").last().alias("precut_live_ba"))
    )

    ev = (
        removed.join(denom, on=COND_ID + ["cut_measdate"], how="left")
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
        .select(COND_ID + ["cut_measdate", "cut_event_type",
                           "cut_pct_ba_removed", "cut_mean_age"])
    )
    print("[cut] cut-event classification:")
    for t, n in ev.group_by("cut_event_type").len().sort("len", descending=True).iter_rows():
        print(f"        {str(t):20s}: {n:>8,} events")
    return ev


# =============================================================================
# Step 6: land_use derivation (per condition)
# =============================================================================

def derive_land_use(full: pl.DataFrame, events: pl.DataFrame) -> pl.DataFrame:
    """Derive a land_use marker per condition from management history +
    forest-type purity, keeping the component markers."""
    # management history from cut events
    hist = (
        events.group_by(COND_ID).agg([
            (pl.col("cut_event_type") == "clear_cut").any().alias("has_clearcut"),
            (pl.col("cut_event_type") == "commercial_thin").any().alias("has_commercial_thin"),
            (pl.col("cut_event_type") == "pre_commercial_thin").any().alias("has_precommercial_thin"),
        ])
    )
    # planted / reserved per condition
    cond_attr = (
        full.group_by(COND_ID).agg([
            (pl.col("cond_stdorgcd") == 1).any().alias("is_planted"),
            pl.col("reservcd").max().alias("reservcd_max"),
        ])
    )
    # dominant-species live-BA share per condition (peak purity across visits)
    sp_ba = (
        full.filter(pl.col("STATUSCD") == 1)
        .group_by(COND_ID + ["MEASDATE", "spcd"]).agg(pl.col("ba").sum().alias("sp_ba"))
    )
    purity = (
        sp_ba.group_by(COND_ID + ["MEASDATE"])
        .agg((pl.col("sp_ba").max() / pl.col("sp_ba").sum()).alias("_top_share"))
        .group_by(COND_ID).agg(pl.col("_top_share").max().alias("dominant_species_frac"))
    )

    lu = (
        cond_attr
        .join(hist, on=COND_ID, how="left")
        .join(purity, on=COND_ID, how="left")
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
            .alias("land_use")
        )
    )
    print("[land_use] distribution (per condition):")
    for u, n in lu.group_by("land_use").len().sort("len", descending=True).iter_rows():
        print(f"        {str(u):14s}: {n:>8,} conditions")
    return lu


# =============================================================================
# Step 7: assemble + write
# =============================================================================

def build_tree_trajectories(con, states: list[int] | None = None) -> pl.DataFrame:
    print("=" * 70)
    print("Tree Life Trajectories")
    print("=" * 70)

    full   = load_tree_history(con, states)
    visits = subplot_visit_scaffold(con, states)

    # condition-level attributes derived from the FULL record set
    events = classify_cut_events(full)
    fate   = classify_terminal_fate(full, visits)
    lu     = derive_land_use(full, events)

    # trajectory body (live + phantom + interpolated)
    body = build_live_body(full, visits)
    body = interpolate_gaps(body, visits)
    body = body.with_columns(pl.col("intro_type").fill_null("real"))

    # alternate per-source age axes (TOTAGE / STDAGE / estage / model)
    body = add_alt_age_axes(body)

    # was_gap_interpolated flag per row
    body = body.with_columns(
        (pl.col("intro_type") == "gap_interpolated").alias("was_gap_interpolated"))

    # attach tree-level terminal fate; flag the last body slot as terminal
    body = body.join(fate, on=TREE_ID, how="left")
    last_slot = body.group_by(TREE_ID).agg(pl.col("MEASDATE").max().alias("_last_slot"))
    body = body.join(last_slot, on=TREE_ID, how="left").with_columns(
        (pl.col("MEASDATE") == pl.col("_last_slot")).alias("is_terminal")).drop("_last_slot")

    # attach per-row condition cut-event (event landing on this measdate/condition)
    ev_join = events.rename({"cut_measdate": "MEASDATE"})
    body = body.join(ev_join, left_on=COND_ID + ["MEASDATE"],
                     right_on=COND_ID + ["MEASDATE"], how="left")

    # attach land_use + components per condition
    body = body.join(lu.select(COND_ID + [
        "land_use", "is_planted", "has_clearcut", "has_commercial_thin",
        "has_precommercial_thin", "is_pure", "dominant_species_frac", "reservcd_max",
    ]), on=COND_ID, how="left")

    # subplot disturbance flag (from terminal fates within the subplot)
    subp_dstrb = (
        body.group_by(SUBPLOT_ID).agg(
            (pl.col("terminal_fate").is_in(
                ["harvest_explicit", "harvest_inferred", "dead_disturbance",
                 "disappeared_inferred_removal"]).any()).alias("subp_has_dstrb"))
    )
    body = body.join(subp_dstrb, on=SUBPLOT_ID, how="left")

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
        "reservcd_max": "reservcd_cond",
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
    db_path = sys.argv[1] if len(sys.argv) > 1 else "../FIASQLITE2PGSQL/FIADB.duckdb"
    states  = [int(s) for s in sys.argv[2:]] if len(sys.argv) > 2 else None

    print(f"DB: {db_path}" + (f"  states={states}" if states else ""))
    con = duckdb.connect(db_path)
    traj = build_tree_trajectories(con, states)

    con.register("_traj_view", traj)
    con.execute("CREATE OR REPLACE TABLE tree_trajectories AS SELECT * FROM _traj_view")
    con.unregister("_traj_view")
    con.commit()
    print(f"[save] tree_trajectories written ({traj.height:,} rows)")
