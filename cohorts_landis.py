"""
cohorts_landis.py
Curates FIA data into the format expected by the Pan/LANDIS-II Biomass
Succession parametrization engine (Julia project).

Source tables  : curated_trees, curated_cohorts  (from the FIA curation pipeline)
Reference tables: data_plot_eco, REF_SPECIES, PLOT (FIA raw)
Output table   : curated_cohorts_landis

Schema:
  plt_cn                      VARCHAR  — composite plot identifier (for subplot counting)
  statecd/unitcd/countycd/plot/subp  INTEGER
  epa_l3, epa_l4, ecosubcd    VARCHAR  — switchable eco field; epa_l4/ecosubcd null if absent
  species_symbol              VARCHAR  — FIA species symbol (e.g. "ABBA")
  spgrpcd                     INTEGER  — FIA species group code (tree-level, region-resolved)
  major_spgrpcd               INTEGER  — REF_SPECIES.MAJOR_SPGRPCD (broad taxonomic group)
  sftwd_hrdwd                 VARCHAR  — REF_SPECIES.SFTWD_HRDWD: "S" softwood / "H" hardwood
  measdate                    DATE
  age_calc                    INTEGER  — estimated_age rounded to nearest year
  agb                         FLOAT    — DRYBIO_AG * TPA_UNADJ, converted lbs/acre → g/m²
  subp_has_dstrb              BOOL     — True if subplot ever had harvest or disturbance death
  plot_meas_num               INTEGER  — distinct measurement years for this plot

Biomass conversion: 1 lb/acre = 453.592/4046.86 ≈ 0.11208 g/m²

Species grouping (12/4/2 tiered bucketing) lives entirely in Julia's
assign_tiered_species!(). This table exposes species_symbol, spgrpcd,
major_spgrpcd, and sftwd_hrdwd so Julia can compute effective_species on
whatever subset it loads after its own SQL filters.

Standalone usage:
    python cohorts_landis.py [path/to/fiadb.duckdb]
    # default: ../FIASQLITE2PGSQL/FIADB.duckdb
"""

import sys

import duckdb
import polars as pl

PLOT_KEY   = ["STATECD", "UNITCD", "COUNTYCD", "PLOT"]
SUBPLOT_ID = ["STATECD", "UNITCD", "COUNTYCD", "PLOT", "SUBP"]
TREE_ID    = ["STATECD", "UNITCD", "COUNTYCD", "PLOT", "SUBP", "TREE"]

# 1 lb/acre → g/m²  (453.592 g/lb ÷ 4046.86 m²/acre)
LBS_ACRE_TO_G_M2 = 453.592 / 4046.86   # ≈ 0.11208

# Phantom backfill thresholds
MIN_PHANTOM_AGE   = 5    # skip trees whose expected age at first meas < this (noise)
MIN_BACKFILL_AGB  = 2.0  # g/m² floor — don't generate rows below this biomass
SAPLING_DIA_MAX   = 5.0  # DIA threshold (inches) for microplot saplings used to estimate growth rates
MIN_RATE_TREES    = 10   # min trees for species×epa_l3 rate; else fall back to spgrpcd


# =============================================================================
# Helpers
# =============================================================================

def _has_column(con, table: str, col: str) -> bool:
    rows = con.execute(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_name = '{table}'"
    ).fetchall()
    return col.lower() in {r[0].lower() for r in rows}


def _has_table(con, table: str) -> bool:
    rows = con.execute(
        "SELECT table_name FROM information_schema.tables "
        f"WHERE table_name = '{table}'"
    ).fetchall()
    return len(rows) > 0


# =============================================================================
# Step 1: live tree records + eco + species metadata
# =============================================================================

def load_base_data(con) -> pl.DataFrame:
    """
    One row per live tree × measurement visit.
    Joins: data_plot_eco (epa_l3, optionally epa_l4), PLOT (ecosubcd), REF_SPECIES.
    """
    has_l4      = _has_column(con, "data_plot_eco", "epa_l4")
    epa_l4_expr = "e.epa_l4" if has_l4 else "NULL::VARCHAR AS epa_l4"

    has_ecosubcd = _has_table(con, "PLOT") and _has_column(con, "PLOT", "ECOSUBCD")
    if has_ecosubcd:
        ecosubcd_join = """
        LEFT JOIN (
            SELECT STATECD, UNITCD, COUNTYCD, PLOT,
                   FIRST(ECOSUBCD) AS ecosubcd
            FROM PLOT
            GROUP BY STATECD, UNITCD, COUNTYCD, PLOT
        ) plt
            ON  plt.STATECD  = t.STATECD
            AND plt.UNITCD   = t.UNITCD
            AND plt.COUNTYCD = t.COUNTYCD
            AND plt.PLOT     = t.PLOT
        """
        ecosubcd_col = "plt.ecosubcd"
    else:
        ecosubcd_join = ""
        ecosubcd_col  = "NULL::VARCHAR AS ecosubcd"

    df = con.execute(f"""
        SELECT
            t.STATECD, t.UNITCD, t.COUNTYCD, t.PLOT, t.SUBP, t.TREE,
            t.SPCD,
            t.MEASDATE,
            t.DIA::DOUBLE                          AS dia,
            ROUND(t.estimated_age)::INTEGER        AS age_calc,
            (t.DRYBIO_AG * t.TPA_UNADJ
             * {LBS_ACRE_TO_G_M2})::DOUBLE         AS agb,
            e.epa_l3,
            {epa_l4_expr},
            {ecosubcd_col},
            r.SPECIES_SYMBOL::VARCHAR              AS species_symbol,
            t.SPGRPCD::INTEGER                     AS spgrpcd,
            r.MAJOR_SPGRPCD::INTEGER               AS major_spgrpcd,
            r.SFTWD_HRDWD::VARCHAR                 AS sftwd_hrdwd
        FROM curated_trees t
        LEFT JOIN data_plot_eco e
            ON  e.statecd  = t.STATECD
            AND e.unitcd   = t.UNITCD
            AND e.countycd = t.COUNTYCD
            AND e.plot     = t.PLOT
        {ecosubcd_join}
        LEFT JOIN REF_SPECIES r
            ON  r.SPCD = t.SPCD
        WHERE
            t.STATUSCD = 1
            AND t.estimated_age IS NOT NULL
            AND t.DRYBIO_AG     IS NOT NULL
            AND t.TPA_UNADJ     IS NOT NULL
            AND ROUND(t.estimated_age) >= 1
    """).pl()

    n = df.height
    print(f"[step1] {n:,} live tree-measurements")
    print(f"         epa_l3:   {df['epa_l3'].is_not_null().sum():,}/{n:,}")
    print(f"         epa_l4:   {df['epa_l4'].is_not_null().sum():,}/{n:,}")
    print(f"         ecosubcd: {df['ecosubcd'].is_not_null().sum():,}/{n:,}")
    print(f"         species:  {df['species_symbol'].is_not_null().sum():,}/{n:,}")
    print(f"         sftwd_hrdwd: {df['sftwd_hrdwd'].is_not_null().sum():,}/{n:,}")
    return df


# =============================================================================
# Step 2: subplot-level disturbance flag
# =============================================================================

def build_subp_has_dstrb(con) -> pl.DataFrame:
    """
    subp_has_dstrb = True for any subplot where at least one cohort was ever
    harvested or suffered disturbance-related mortality (from curated_cohorts).

    This flag is conservative: once True it never resets, so all measurements
    of a disturbed subplot are excluded when Julia applies skip_disturbances=true.
    """
    cohorts = con.execute("""
        SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP,
               ever_harvested, ever_disturbed_dead
        FROM curated_cohorts
    """).pl()

    subp_flag = (
        cohorts
        .group_by(SUBPLOT_ID)
        .agg(
            (pl.col("ever_harvested").cast(pl.Boolean).any() |
             pl.col("ever_disturbed_dead").cast(pl.Boolean).any())
            .alias("subp_has_dstrb")
        )
    )

    n_total = subp_flag.height
    n_dstrb = subp_flag.filter(pl.col("subp_has_dstrb")).height
    print(f"[step2] subp_has_dstrb: {n_dstrb:,}/{n_total:,} subplots disturbed "
          f"({100 * n_dstrb / max(n_total, 1):.1f}%)")
    return subp_flag


# =============================================================================
# Step 3: plot measurement count
# =============================================================================

def build_plot_meas_num(df: pl.DataFrame) -> pl.DataFrame:
    """
    plot_meas_num = number of distinct measurement dates for this plot.
    Julia filter 'plot_meas_num > 1' selects only longitudinal plots.
    """
    return (
        df.select(PLOT_KEY + ["MEASDATE"])
        .unique()
        .group_by(PLOT_KEY)
        .agg(pl.col("MEASDATE").n_unique().alias("plot_meas_num"))
    )


def build_subp_meas_num(df: pl.DataFrame) -> pl.DataFrame:
    """
    subp_meas_num = number of distinct measurement dates for this subplot.
    Finer-grained than plot_meas_num: a subplot may have fewer visits than
    the plot if it was intermittently skipped.
    """
    return (
        df.select(SUBPLOT_ID + ["MEASDATE"])
        .unique()
        .group_by(SUBPLOT_ID)
        .agg(pl.col("MEASDATE").n_unique().alias("subp_meas_num"))
    )


# =============================================================================
# Step 4: phantom cohort diagnostics
# =============================================================================

def _diagnose_phantom_cohorts(df: pl.DataFrame) -> pl.DataFrame:
    """
    Identify trees absent at the plot visit immediately before their debut but
    appearing with an age larger than the gap to that visit — i.e., they were
    already alive when the crew last visited and should have been tallied.

    One row per phantom tree (at its debut measurement):
      TREE_ID + SPCD + species_symbol
      prev_plot_measdate  : latest plot visit before this tree appeared
      first_tree_measdate : date this tree first appears in the data
      age_calc            : tree's age at first appearance
      expected_age_at_prev: age_calc - years(first_tree_measdate - prev_plot_measdate)
      agb
    """
    # All distinct plot measurement dates (needed to find the previous visit)
    plot_dates = df.select(PLOT_KEY + ["MEASDATE"]).unique()

    # Tree's debut date
    tree_first = (
        df.group_by(TREE_ID)
        .agg(pl.col("MEASDATE").min().alias("first_tree_measdate"))
    )

    # Latest plot visit strictly before the tree's debut
    prev_meas = (
        tree_first
        .join(plot_dates, on=PLOT_KEY, how="left")
        .filter(pl.col("MEASDATE") < pl.col("first_tree_measdate"))
        .group_by(TREE_ID)
        .agg(pl.col("MEASDATE").max().alias("prev_plot_measdate"))
    )

    # Work at the debut row only — one row per tree
    debut = (
        df
        .join(tree_first, on=TREE_ID, how="left")
        .filter(pl.col("MEASDATE") == pl.col("first_tree_measdate"))
        .join(prev_meas, on=TREE_ID, how="left")
        # Phantom = there was a prior plot visit where this tree was absent
        .filter(pl.col("prev_plot_measdate").is_not_null())
        .with_columns(
            (pl.col("age_calc").cast(pl.Float64) -
             (pl.col("first_tree_measdate") - pl.col("prev_plot_measdate"))
             .dt.total_days() / 365.25)
            .alias("expected_age_at_prev")
        )
        # Keep only trees that would have been alive at the previous visit
        .filter(pl.col("expected_age_at_prev") > 0)
        .select(
            TREE_ID + ["SPCD", "species_symbol",
                        "prev_plot_measdate", "first_tree_measdate",
                        "age_calc", "expected_age_at_prev", "agb"]
        )
        .sort(PLOT_KEY + ["SUBP", "TREE"])
    )

    n_trees = debut.height
    n_plots = debut.select(PLOT_KEY).unique().height

    print(f"\n[phantom cohorts] {n_trees:,} trees / {n_plots:,} plots")

    if n_trees == 0:
        print("  none found")
        return debut

    ages = debut["expected_age_at_prev"]
    print(f"  expected_age_at_prev_meas:  "
          f"p10={ages.quantile(0.10):.0f}  p25={ages.quantile(0.25):.0f}  "
          f"p50={ages.quantile(0.50):.0f}  p75={ages.quantile(0.75):.0f}  "
          f"p90={ages.quantile(0.90):.0f}  max={ages.max():.0f}")

    by_sp = (
        debut.group_by("species_symbol")
        .agg([
            pl.struct(TREE_ID).n_unique().alias("n_trees"),
            pl.col("expected_age_at_prev").mean().alias("mean_phantom_age"),
        ])
        .sort("n_trees", descending=True)
        .head(15)
    )
    print("\n  top phantom species:")
    for row in by_sp.iter_rows(named=True):
        print(f"    {str(row['species_symbol']):<10s}  "
              f"trees={row['n_trees']:>6,}  "
              f"mean_phantom_age={row['mean_phantom_age']:.1f}")

    return debut


# =============================================================================
# Step 4b: sim_year age anchoring
# =============================================================================

def compute_sim_year_ages(df: pl.DataFrame) -> pl.DataFrame:
    """
    Replace age_calc with a sim_year-anchored integer, eliminating floating-point
    drift across remeasurements.

      sim_year = round((MEASDATE − first subplot MEASDATE).days / 365.25)
      age_calc = round(estimated_age at tree debut) + (sim_year − debut_sim_year)

    All trees on the same subplot share the same integer sim_year steps, so cohort
    ages always have exact integer jumps and perfectly align with the simulation
    time axis regardless of how many remeasurements occur.
    """
    # sim_year: integer years elapsed since first subplot visit
    first_meas = (
        df.group_by(SUBPLOT_ID)
        .agg(pl.col("MEASDATE").min().alias("_first_measdate"))
    )
    df = df.join(first_meas, on=SUBPLOT_ID, how="left").with_columns(
        (
            (pl.col("MEASDATE") - pl.col("_first_measdate")).dt.total_days() / 365.25
        ).round(0).cast(pl.Int32).alias("sim_year")
    ).drop("_first_measdate")

    # Per-tree debut: rounded age and sim_year at first measurement
    debut = (
        df.sort(TREE_ID + ["MEASDATE"])
        .unique(subset=TREE_ID, keep="first")
        .select(TREE_ID + [
            pl.col("age_calc").alias("_debut_age"),
            pl.col("sim_year").alias("_debut_sim_year"),
        ])
    )
    df = df.join(debut, on=TREE_ID, how="left")

    # Recompute age_calc as exact integer offset — no floating-point involved
    df = df.with_columns(
        (pl.col("_debut_age") + pl.col("sim_year") - pl.col("_debut_sim_year"))
        .alias("age_calc")
    ).drop(["_debut_age", "_debut_sim_year"])

    return df


# =============================================================================
# Step 4c: phantom backfill
# =============================================================================

def estimate_sapling_growth_rates(df: pl.DataFrame) -> tuple:
    """
    Estimate annual relative biomass growth rate from saplings.

    Rate = (last_agb - first_agb) / last_agb / (last_age - first_age)  [fraction/yr]

    Numerator is normalised by last (debut) biomass so the rate is scale-invariant
    across species. Negative rates are clipped to 0.

    Training set: trees with age_calc < MAX_SAPLING_AGE at first FIA appearance
    and at least 2 distinct measurement years.

    Returns three DataFrames (primary → fallback1 → fallback2):
      sp_rates   : (species_symbol, epa_l3) → rate
      grp_rates  : (spgrpcd,        epa_l3) → rate
      glb_rates  : (spgrpcd)                → rate
    """
    srt = df.sort(TREE_ID + ["MEASDATE"])

    first = (
        srt.unique(subset=TREE_ID, keep="first")
        .select(TREE_ID + [pl.col("age_calc").alias("first_age"),
                            pl.col("agb").alias("first_agb"),
                            pl.col("dia").alias("first_dia")])
    )
    last = (
        srt.unique(subset=TREE_ID, keep="last")
        .select(TREE_ID + [pl.col("age_calc").alias("last_age"),
                            pl.col("agb").alias("last_agb")])
    )
    meta = (
        df.select(TREE_ID + ["species_symbol", "spgrpcd", "epa_l3"])
        .unique(subset=TREE_ID)
    )

    tree_rates = (
        first.join(last, on=TREE_ID, how="inner")
        .join(meta, on=TREE_ID, how="left")
        .filter(
            pl.col("first_dia").is_not_null() &
            (pl.col("first_dia") <= SAPLING_DIA_MAX) &
            (pl.col("last_age") > pl.col("first_age")) &
            pl.col("last_agb").is_not_null() &
            (pl.col("last_agb") > 0)
        )
        .with_columns(
            ((pl.col("last_agb") - pl.col("first_agb")) /
             pl.col("last_agb") /
             (pl.col("last_age") - pl.col("first_age")).cast(pl.Float64))
            .clip(0.0, None)
            .alias("rel_rate")
        )
    )

    n_sap = tree_rates.height
    print(f"[sapling rates] {n_sap:,} training saplings "
          f"(first_dia ≤ {SAPLING_DIA_MAX} in, ≥2 measurements)")

    sp_rates = (
        tree_rates.group_by(["species_symbol", "epa_l3"])
        .agg([pl.col("rel_rate").mean().alias("rate"),
              pl.len().alias("_n")])
        .filter(pl.col("_n") >= MIN_RATE_TREES)
        .drop("_n")
    )
    grp_rates = (
        tree_rates.group_by(["spgrpcd", "epa_l3"])
        .agg(pl.col("rel_rate").mean().alias("rate"))
    )
    glb_rates = (
        tree_rates.group_by("spgrpcd")
        .agg(pl.col("rel_rate").mean().alias("rate"))
    )

    print(f"           {sp_rates.height:,} (species, epa_l3) pairs  "
          f"| {grp_rates.height:,} (spgrpcd, epa_l3) pairs computed")
    return sp_rates, grp_rates, glb_rates


def print_mid_measurement_gaps(df: pl.DataFrame) -> None:
    """
    Diagnostic: count subplot visits between a tree's first and last real record
    where the tree has no entry.  These are not fixed here — tracked to monitor
    upstream data quality changes.
    """
    tree_bounds = (
        df.group_by(TREE_ID)
        .agg(
            pl.col("MEASDATE").min().alias("first_measdate"),
            pl.col("MEASDATE").max().alias("last_measdate"),
        )
    )
    subp_visits  = df.select(SUBPLOT_ID + ["MEASDATE"]).unique()
    existing     = (
        df.select(TREE_ID + ["MEASDATE"]).unique()
        .with_columns(pl.lit(True).alias("_exists"))
    )
    gaps = (
        tree_bounds
        .join(subp_visits, on=SUBPLOT_ID, how="left")
        .filter(
            (pl.col("MEASDATE") > pl.col("first_measdate")) &
            (pl.col("MEASDATE") < pl.col("last_measdate"))
        )
        .join(existing, on=TREE_ID + ["MEASDATE"], how="left")
        .filter(pl.col("_exists").is_null())
    )
    n_slots = gaps.height
    n_trees = gaps.select(TREE_ID).unique().height if n_slots > 0 else 0
    n_plots = gaps.select(PLOT_KEY).unique().height if n_slots > 0 else 0
    print(f"[mid-meas gaps]  {n_slots:,} tree-visit slots  "
          f"({n_trees:,} trees / {n_plots:,} plots)  — not backfilled")


def backfill_phantom_trees(df: pl.DataFrame) -> pl.DataFrame:
    """
    For every individual tree (TREE_ID), ensure each prior subplot visit where
    that tree would have been alive has a synthetic row.

    Working at the tree level prevents a cohort biomass jump when a tree
    "appears" for the first time: its backfilled records are added regardless of
    whether another tree of the same species happened to have the same age at
    that prior visit.

    prior_age = debut_age + (prior_sim_year - debut_sim_year)

    Missing tree-visit slots get a synthetic row (intro_type='phantom_backfill')
    with biomass = max(MIN_BACKFILL_AGB, agb * (1 - rate * years_back)).
    """
    # Each tree's first (debut) measurement.
    tree_debut = (
        df.sort(TREE_ID + ["MEASDATE"])
        .unique(subset=TREE_ID, keep="first")
    )

    # All distinct subplot visits identified by MEASDATE; sim_year used only for arithmetic.
    subp_visits = (
        df.select(SUBPLOT_ID + ["MEASDATE", "sim_year"]).unique()
        .rename({"MEASDATE": "prior_measdate", "sim_year": "prior_sim_year"})
    )

    # Cross each tree debut with all strictly-earlier subplot visits.
    # prior_age = debut_age + (prior_sim_year - debut_sim_year) — pure integer arithmetic.
    prior = (
        tree_debut
        .join(subp_visits, on=SUBPLOT_ID, how="left")
        .filter(pl.col("prior_measdate") < pl.col("MEASDATE"))
        .with_columns(
            (pl.col("age_calc") + pl.col("prior_sim_year") - pl.col("sim_year"))
            .cast(pl.Int32).alias("prior_age")
        )
        .filter(pl.col("prior_age") >= 1)
    )

    # Which (tree, prior_measdate) slots already exist in the data?
    existing = (
        df.select(TREE_ID + ["MEASDATE"]).unique()
        .rename({"MEASDATE": "prior_measdate"})
        .with_columns(pl.lit(True).alias("_exists"))
    )

    missing = (
        prior
        .join(existing, on=TREE_ID + ["prior_measdate"], how="left")
        .filter(pl.col("_exists").is_null())
        .drop("_exists")
        .unique(subset=TREE_ID + ["prior_measdate"], keep="first")
    )

    n_slots = missing.height
    n_plots = missing.select(PLOT_KEY).unique().height if n_slots > 0 else 0
    print(f"[backfill trees] {n_slots:,} missing tree-slots across {n_plots:,} plots")

    if n_slots == 0:
        return df.with_columns(pl.lit(None).cast(pl.Utf8).alias("intro_type"))

    # Biomass: scale debut agb back by sapling growth rate
    sp_rates, grp_rates, glb_rates = estimate_sapling_growth_rates(df)

    missing = (
        missing
        .join(sp_rates.rename({"rate": "_r_sp"}),  on=["species_symbol", "epa_l3"], how="left")
        .join(grp_rates.rename({"rate": "_r_grp"}), on=["spgrpcd",        "epa_l3"], how="left")
        .join(glb_rates.rename({"rate": "_r_glb"}), on="spgrpcd",                    how="left")
        .with_columns(pl.coalesce(["_r_sp", "_r_grp", "_r_glb"]).alias("_rate"))
        .drop(["_r_sp", "_r_grp", "_r_glb"])
        .with_columns(
            pl.when(pl.col("_rate").is_not_null())
            .then(
                (
                    pl.col("agb") *
                    (1.0 - pl.col("_rate") *
                     (pl.col("age_calc").cast(pl.Float64) - pl.col("prior_age").cast(pl.Float64)))
                ).clip(MIN_BACKFILL_AGB, None)
            )
            .otherwise(pl.lit(MIN_BACKFILL_AGB))
            .alias("prior_agb")
        )
        .drop("_rate")
    )

    n_no_rate = missing.filter(pl.col("prior_agb") == MIN_BACKFILL_AGB).height
    if n_no_rate > 0:
        print(f"[backfill trees] {n_no_rate:,} slots used MIN_BACKFILL_AGB floor (no sapling rate)")

    # Build synthetic rows: swap debut date/age/agb/sim_year with prior values
    synthetic = (
        missing
        .drop(["MEASDATE", "age_calc", "agb", "sim_year"])
        .rename({
            "prior_measdate": "MEASDATE",
            "prior_age":      "age_calc",
            "prior_sim_year": "sim_year",
            "prior_agb":      "agb",
        })
        .with_columns(pl.lit("phantom_backfill").cast(pl.Utf8).alias("intro_type"))
    )

    print(f"[backfill trees] generated {synthetic.height:,} synthetic rows")
    df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("intro_type"))
    return pl.concat([df, synthetic.select(df.columns)], how="vertical")


def backfill_phantom_cohorts(df: pl.DataFrame) -> pl.DataFrame:
    """
    Second-pass backfill at the cohort level (subplot × species × measdate × age_calc).

    Runs after backfill_phantom_trees to catch any remaining cohort-level gaps —
    e.g. cases where two trees of the same species happen to share an age and the
    tree-level pass didn't produce a separate record for each combinatorial slot.

    Preserves any intro_type values already set by backfill_phantom_trees.
    """
    cohort_debut = (
        df.unique(subset=SUBPLOT_ID + ["species_symbol", "MEASDATE", "age_calc"],
                  keep="first")
    )

    subp_visits = (
        df.select(SUBPLOT_ID + ["MEASDATE", "sim_year"]).unique()
        .rename({"MEASDATE": "prior_measdate", "sim_year": "prior_sim_year"})
    )

    prior = (
        cohort_debut
        .join(subp_visits, on=SUBPLOT_ID, how="left")
        .filter(pl.col("prior_measdate") < pl.col("MEASDATE"))
        .with_columns(
            (pl.col("age_calc") + pl.col("prior_sim_year") - pl.col("sim_year"))
            .cast(pl.Int32).alias("prior_age")
        )
        .filter(pl.col("prior_age") >= 1)
    )

    existing = (
        df.select(SUBPLOT_ID + ["species_symbol", "MEASDATE", "age_calc"]).unique()
        .rename({"MEASDATE": "prior_measdate", "age_calc": "prior_age"})
        .with_columns(pl.lit(True).alias("_exists"))
    )

    missing = (
        prior
        .join(existing, on=SUBPLOT_ID + ["species_symbol", "prior_measdate", "prior_age"],
              how="left")
        .filter(pl.col("_exists").is_null())
        .drop("_exists")
        .unique(subset=SUBPLOT_ID + ["species_symbol", "prior_measdate", "prior_age"],
                keep="first")
    )

    n_slots = missing.height
    n_plots = missing.select(PLOT_KEY).unique().height if n_slots > 0 else 0
    print(f"[backfill cohorts] {n_slots:,} additional cohort-slots across {n_plots:,} plots")

    if n_slots == 0:
        if "intro_type" not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("intro_type"))
        return df

    sp_rates, grp_rates, glb_rates = estimate_sapling_growth_rates(df)

    missing = (
        missing
        .join(sp_rates.rename({"rate": "_r_sp"}),  on=["species_symbol", "epa_l3"], how="left")
        .join(grp_rates.rename({"rate": "_r_grp"}), on=["spgrpcd",        "epa_l3"], how="left")
        .join(glb_rates.rename({"rate": "_r_glb"}), on="spgrpcd",                    how="left")
        .with_columns(pl.coalesce(["_r_sp", "_r_grp", "_r_glb"]).alias("_rate"))
        .drop(["_r_sp", "_r_grp", "_r_glb"])
        .with_columns(
            pl.when(pl.col("_rate").is_not_null())
            .then(
                (
                    pl.col("agb") *
                    (1.0 - pl.col("_rate") *
                     (pl.col("age_calc").cast(pl.Float64) - pl.col("prior_age").cast(pl.Float64)))
                ).clip(MIN_BACKFILL_AGB, None)
            )
            .otherwise(pl.lit(MIN_BACKFILL_AGB))
            .alias("prior_agb")
        )
        .drop("_rate")
    )

    n_no_rate = missing.filter(pl.col("prior_agb") == MIN_BACKFILL_AGB).height
    if n_no_rate > 0:
        print(f"[backfill cohorts] {n_no_rate:,} slots used MIN_BACKFILL_AGB floor (no sapling rate)")

    synthetic = (
        missing
        .drop(["MEASDATE", "age_calc", "agb", "sim_year"])
        .rename({
            "prior_measdate": "MEASDATE",
            "prior_age":      "age_calc",
            "prior_sim_year": "sim_year",
            "prior_agb":      "agb",
        })
        .with_columns(pl.lit("phantom_backfill").cast(pl.Utf8).alias("intro_type"))
    )

    print(f"[backfill cohorts] generated {synthetic.height:,} additional synthetic rows")
    if "intro_type" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("intro_type"))
    return pl.concat([df, synthetic.select(df.columns)], how="vertical")


# =============================================================================
# Continuity check
# =============================================================================

def check_cohort_continuity(df: pl.DataFrame, age_tol: int = 0) -> int:
    """
    Every cohort (subplot × species_symbol × measdate × age_calc) must satisfy
    at least one of:
      1. First subplot measurement — no prior visit to compare against
      2. age_calc ≤ sim_year_gap — cohort born within the interval
      3. Same species with age = age_calc − sim_year_gap exists at the previous visit

    Uses sim_year differences (integer) instead of fractional date gaps so the
    check is perfectly aligned with how compute_sim_year_ages and
    backfill_phantom_cohorts compute ages — no rounding tolerance needed.

    Prints the violation count; expected to be 0 after phantom backfill.
    Uses lowercase column names (post-rename output of build_cohorts_landis).
    """
    subplot_id  = ["statecd", "unitcd", "countycd", "plot", "subp"]
    cohort_key  = subplot_id + ["species_symbol", "measdate", "age_calc"]

    # Per-subplot visit timeline sorted by measdate.
    # sim_year is carried for arithmetic only — visits are identified by measdate.
    visits = (
        df.select(subplot_id + ["measdate", "sim_year"]).unique()
        .sort(subplot_id + ["measdate"])
        .with_columns([
            pl.col("measdate").shift(1).over(subplot_id).alias("prev_measdate"),
            pl.col("sim_year").shift(1).over(subplot_id).alias("prev_sim_year"),
        ])
    )

    cohorts = (
        df.select(cohort_key + ["sim_year"]).unique()
        .join(visits, on=subplot_id + ["measdate"], how="left")
        .with_columns(
            (pl.col("sim_year") - pl.col("prev_sim_year")).alias("sim_year_gap")
        )
    )

    # Condition 1: no prev visit; gap=0: same sim_year step, no predecessor needed.
    # Condition 2: new ingrowth (age ≤ gap).
    needs_c3 = (
        cohorts
        .filter(
            pl.col("prev_sim_year").is_not_null() &
            (pl.col("sim_year_gap") > 0) &
            (pl.col("age_calc") > pl.col("sim_year_gap"))
        )
        .with_columns(
            (pl.col("age_calc") - pl.col("sim_year_gap")).alias("expected_prev_age")
        )
    )

    # All distinct (subplot, species, measdate, age) present in data — join by measdate only.
    prev_ages = (
        df.select(subplot_id + ["species_symbol", "measdate", "age_calc"])
        .unique()
        .rename({"measdate": "prev_measdate", "age_calc": "prev_age_calc"})
    )

    cond3 = (
        needs_c3
        .join(prev_ages,
              on=subplot_id + ["species_symbol", "prev_measdate"],
              how="left")
        .with_columns(
            (
                pl.col("prev_age_calc").is_not_null() &
                (
                    (pl.col("prev_age_calc") - pl.col("expected_prev_age"))
                    .abs() <= age_tol
                )
            ).alias("age_match")
        )
        .group_by(cohort_key)
        .agg(pl.col("age_match").any().alias("has_prev"))
    )

    violating = (
        needs_c3
        .join(cond3, on=cohort_key, how="left")
        .filter(~pl.col("has_prev").fill_null(False))
    )
    n_viol  = violating.height
    n_total = cohorts.height
    print(f"[continuity check] {n_viol:,} violations / {n_total:,} cohorts  (age_tol={age_tol})")

    if n_viol > 0:
        sample = violating.head(15)
        print("\n  sample violations:")
        for r in sample.iter_rows(named=True):
            print(f"    subp=({r['statecd']},{r['unitcd']},{r['countycd']},{r['plot']},{r['subp']})"
                  f"  sp={r['species_symbol']}"
                  f"  sim_year={r['sim_year']}  age={r['age_calc']}"
                  f"  gap={r['sim_year_gap']}  expected_prev={r['expected_prev_age']}"
                  f"  prev_sim_year={r['prev_sim_year']}")

        ages_v = violating["age_calc"]
        gaps_v = violating["sim_year_gap"]
        print(f"\n  violation age_calc:    min={ages_v.min()}  p50={int(ages_v.median())}  max={ages_v.max()}")
        print(f"  violation sim_yr_gap:  min={gaps_v.min()}  p50={int(gaps_v.median())}  max={gaps_v.max()}")

        if "intro_type" in violating.columns:
            by_intro = violating.group_by("intro_type").agg(pl.len().alias("n")).sort("n", descending=True)
            print(f"\n  by intro_type:")
            for r in by_intro.iter_rows(named=True):
                print(f"    {str(r['intro_type']):<25s}: {r['n']:,}")

        # For the first violation, show what exists for that species at the previous visit.
        v0 = violating.row(0, named=True)
        v0_subp = {k: v0[k] for k in subplot_id}
        v0_filter = pl.all_horizontal([pl.col(k) == v for k, v in v0_subp.items()])
        prev_at_v0 = (
            df.filter(v0_filter)
            .filter(pl.col("measdate") == v0["prev_measdate"])
            .filter(pl.col("species_symbol") == v0["species_symbol"])
            .select(["measdate", "sim_year", "age_calc", "intro_type"] if "intro_type" in df.columns else ["measdate", "sim_year", "age_calc"])
            .unique().sort("age_calc")
        )
        print(f"\n  first violation detail:")
        print(f"    subp={tuple(v0_subp.values())}  sp={v0['species_symbol']}")
        print(f"    violating cohort: sim_year={v0['sim_year']}  age={v0['age_calc']}  expected_prev={v0['expected_prev_age']}")
        print(f"    {v0['species_symbol']} rows at prev_measdate={v0['prev_measdate']}:")
        if prev_at_v0.is_empty():
            print(f"      (none found)")
        else:
            for r in prev_at_v0.iter_rows(named=True):
                print(f"      {r}")

    return n_viol


# =============================================================================
# Step 5: assemble
# =============================================================================

def build_cohorts_landis(con) -> pl.DataFrame:
    print("=" * 70)
    print("LANDIS Cohort Curation Pipeline")
    print("=" * 70)

    df = load_base_data(con)

    # Attach subplot disturbance flag
    subp_flag = build_subp_has_dstrb(con)
    df = df.join(subp_flag, on=SUBPLOT_ID, how="left").with_columns(
        pl.col("subp_has_dstrb").fill_null(False)
    )

    # Attach plot and subplot measurement counts
    meas_num = build_plot_meas_num(df)
    df = df.join(meas_num, on=PLOT_KEY, how="left")

    subp_meas_num_df = build_subp_meas_num(df)
    df = df.join(subp_meas_num_df, on=SUBPLOT_ID, how="left")

    # Anchor age_calc to integer sim_year steps (eliminates floating-point drift)
    df = compute_sim_year_ages(df)

    # Diagnostic: mid-measurement gaps (tracked but not fixed here)
    print_mid_measurement_gaps(df)

    # Backfill: tree level first, then cohort level to catch any remaining gaps
    df = backfill_phantom_trees(df)
    df = backfill_phantom_cohorts(df)

    # plt_cn: unique string key per plot (not per visit); Julia uses it only to
    # count subplots (subp_count = unique subp per plt_cn) for AGB normalisation
    df = df.with_columns(
        pl.concat_str(
            [pl.col(c).cast(pl.Utf8) for c in PLOT_KEY],
            separator="_"
        ).alias("plt_cn")
    )

    # Rename and reorder to match Julia's expected column names
    df = df.rename({
        "STATECD":  "statecd",
        "UNITCD":   "unitcd",
        "COUNTYCD": "countycd",
        "PLOT":     "plot",
        "SUBP":     "subp",
        "MEASDATE": "measdate",
    }).select([
        "plt_cn",
        "statecd", "unitcd", "countycd", "plot", "subp",
        "epa_l3",
        "epa_l4",
        "ecosubcd",
        "species_symbol",
        "spgrpcd",
        "major_spgrpcd",
        "sftwd_hrdwd",
        "measdate",
        "sim_year",
        "age_calc",
        "agb",
        "subp_has_dstrb",
        "plot_meas_num",
        "subp_meas_num",
        "intro_type",
    ])

    # Diagnostics
    n_rows     = df.height
    n_subplots = df.select(["statecd", "unitcd", "countycd", "plot", "subp"]).unique().height
    n_plots    = df.select(["statecd", "unitcd", "countycd", "plot"]).unique().height
    n_species  = df["species_symbol"].n_unique()
    n_ecos_l3  = df["epa_l3"].n_unique()

    print(f"\n[summary] {n_rows:,} rows")
    print(f"          {n_plots:,} plots  ·  {n_subplots:,} subplots  ·  "
          f"{n_species:,} species  ·  {n_ecos_l3:,} epa_l3 ecoregions")

    n_clean = (
        df.filter(~pl.col("subp_has_dstrb"))
        .select(["statecd", "unitcd", "countycd", "plot", "subp"])
        .unique().height
    )
    n_long = (
        df.filter(pl.col("plot_meas_num") > 1)
        .select(["statecd", "unitcd", "countycd", "plot"])
        .unique().height
    )
    n_julia = df.filter(
        (~pl.col("subp_has_dstrb")) & (pl.col("plot_meas_num") > 1)
    ).height
    print(f"\n  subp_has_dstrb=false subplots : {n_clean:,}  (succession-only)")
    print(f"  plot_meas_num > 1 plots       : {n_long:,}   (longitudinal)")
    print(f"  rows after both Julia filters : {n_julia:,}")

    check_cohort_continuity(df)

    return df


# =============================================================================
# Standalone entry point
# =============================================================================

if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else "../FIASQLITE2PGSQL/FIADB.duckdb"

    print(f"DB: {db_path}")
    con = duckdb.connect(db_path)

    cohorts = build_cohorts_landis(con)

    con.register("_landis_view", cohorts)
    con.execute(
        "CREATE OR REPLACE TABLE curated_cohorts_landis AS SELECT * FROM _landis_view"
    )
    con.unregister("_landis_view")
    con.commit()
    print(f"\n[save] curated_cohorts_landis → {cohorts.height:,} rows")

    con.close()
    print("[done]")
