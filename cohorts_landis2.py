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
MAX_SAPLING_AGE   = 30   # age at first FIA appearance to qualify as training sapling
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
            t.MEASYEAR,
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
    plot_meas_num = number of distinct measurement years for this plot.
    Julia filter 'plot_meas_num > 1' selects only longitudinal plots.
    """
    return (
        df.select(PLOT_KEY + ["MEASYEAR"])
        .unique()
        .group_by(PLOT_KEY)
        .agg(pl.col("MEASYEAR").n_unique().alias("plot_meas_num"))
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
      prev_plot_measyear  : latest plot visit before this tree appeared
      first_tree_measyear : year this tree first appears in the data
      age_calc            : tree's age at first appearance
      expected_age_at_prev: age_calc - (first_tree_measyear - prev_plot_measyear)
      agb
    """
    # All distinct plot measurement years (needed to find the previous visit)
    plot_years = df.select(PLOT_KEY + ["MEASYEAR"]).unique()

    # Tree's debut year
    tree_first = (
        df.group_by(TREE_ID)
        .agg(pl.col("MEASYEAR").min().alias("first_tree_measyear"))
    )

    # Latest plot visit strictly before the tree's debut
    prev_meas = (
        tree_first
        .join(plot_years, on=PLOT_KEY, how="left")
        .filter(pl.col("MEASYEAR") < pl.col("first_tree_measyear"))
        .group_by(TREE_ID)
        .agg(pl.col("MEASYEAR").max().alias("prev_plot_measyear"))
    )

    # Work at the debut row only — one row per tree
    debut = (
        df
        .join(tree_first, on=TREE_ID, how="left")
        .filter(pl.col("MEASYEAR") == pl.col("first_tree_measyear"))
        .join(prev_meas, on=TREE_ID, how="left")
        # Phantom = there was a prior plot visit where this tree was absent
        .filter(pl.col("prev_plot_measyear").is_not_null())
        .with_columns(
            (pl.col("age_calc") -
             (pl.col("first_tree_measyear") - pl.col("prev_plot_measyear")))
            .alias("expected_age_at_prev")
        )
        # Keep only trees that would have been alive at the previous visit
        .filter(pl.col("expected_age_at_prev") > 0)
        .select(
            TREE_ID + ["SPCD", "species_symbol",
                        "prev_plot_measyear", "first_tree_measyear",
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
        df.sort(TREE_ID + ["MEASYEAR"])
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
    srt = df.sort(TREE_ID + ["MEASYEAR"])

    first = (
        srt.unique(subset=TREE_ID, keep="first")
        .select(TREE_ID + [pl.col("age_calc").alias("first_age"),
                            pl.col("agb").alias("first_agb")])
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
            (pl.col("first_age") <= MAX_SAPLING_AGE) &
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
          f"(first_age ≤ {MAX_SAPLING_AGE}, ≥2 measurements)")

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


def backfill_phantom_cohorts(df: pl.DataFrame) -> pl.DataFrame:
    """
    For every tree at its debut, ensure each prior subplot visit where the cohort
    would have been alive (prior_age ≥ 1) has a row of the same species with the
    matching age:  prior_age = round(debut_age - gap_years).

    If such a row already exists (observed or previously backfilled) → leave it.
    Otherwise create a synthetic row (intro_type='phantom_backfill') with
    biomass = max(MIN_BACKFILL_AGB, debut_agb * (1 - rate * years_back))
    using sapling-derived relative growth rates (species×epa_l3 → spgrpcd×epa_l3 → spgrpcd).

    No minimum-age threshold — every missing slot is filled.
    """
    # One row per tree at its earliest measurement
    tree_debut = (
        df.sort(TREE_ID + ["MEASYEAR"])
        .unique(subset=TREE_ID, keep="first")
    )

    # All distinct subplot measurement visits (sim_year for integer age arithmetic)
    subp_visits = (
        df.select(SUBPLOT_ID + ["MEASDATE", "MEASYEAR", "sim_year"]).unique()
        .rename({"MEASDATE": "prior_measdate", "MEASYEAR": "prior_measyear",
                 "sim_year": "prior_sim_year"})
    )

    # Cross-join each debut tree with all prior subplot visits, compute prior age
    # age_calc here is the debut_age (sim_year-anchored); sim_year is debut_sim_year.
    # prior_age = debut_age + (prior_sim_year - debut_sim_year) — pure integer arithmetic.
    prior = (
        tree_debut
        .join(subp_visits, on=SUBPLOT_ID, how="left")
        .filter(pl.col("prior_measyear") < pl.col("MEASYEAR"))
        .with_columns(
            (pl.col("age_calc") + pl.col("prior_sim_year") - pl.col("sim_year"))
            .cast(pl.Int32).alias("prior_age")
        )
        .filter(pl.col("prior_age") >= 1)
    )

    # Existing (subplot, species, measdate, age_calc) slots in the current data
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
        # Multiple trees may require the same slot — keep one representative
        .unique(subset=SUBPLOT_ID + ["species_symbol", "prior_measdate", "prior_age"],
                keep="first")
    )

    n_slots = missing.height
    n_plots = missing.select(PLOT_KEY).unique().height if n_slots > 0 else 0
    print(f"[backfill] {n_slots:,} missing cohort-slots across {n_plots:,} plots")

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
        print(f"[backfill] {n_no_rate:,} slots used MIN_BACKFILL_AGB floor (no sapling rate)")

    # Build synthetic rows: swap debut date/year/age/agb/sim_year with prior values
    synthetic = (
        missing
        .drop(["MEASDATE", "MEASYEAR", "age_calc", "agb", "sim_year"])
        .rename({
            "prior_measdate": "MEASDATE",
            "prior_measyear": "MEASYEAR",
            "prior_age":      "age_calc",
            "prior_sim_year": "sim_year",
            "prior_agb":      "agb",
        })
        .with_columns(pl.lit("phantom_backfill").cast(pl.Utf8).alias("intro_type"))
    )

    print(f"[backfill] generated {synthetic.height:,} synthetic rows")
    df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("intro_type"))
    return pl.concat([df, synthetic.select(df.columns)], how="vertical")


# =============================================================================
# Continuity check
# =============================================================================

def check_cohort_continuity(df: pl.DataFrame, age_tol: int = 1) -> int:
    """
    Every cohort (subplot × species_symbol × measdate × age_calc) must satisfy
    at least one of:
      1. First subplot measurement — no prior visit to compare against
      2. age_calc ≤ gap to previous visit — cohort born in the interval
      3. Same species with age ≈ age_calc − gap exists at the previous visit

    Prints the violation count; expected to be 0 after phantom backfill.
    Uses lowercase column names (post-rename output of build_cohorts_landis).
    """
    subplot_id  = ["statecd", "unitcd", "countycd", "plot", "subp"]
    cohort_key  = subplot_id + ["species_symbol", "measdate", "age_calc"]

    cohorts = df.select(cohort_key).unique()

    # Previous subplot measdate via shift-within-group
    prev = (
        df.select(subplot_id + ["measdate"]).unique()
        .sort(subplot_id + ["measdate"])
        .with_columns(
            pl.col("measdate").shift(1).over(subplot_id).alias("prev_measdate")
        )
    )
    cohorts = cohorts.join(prev, on=subplot_id + ["measdate"], how="left")

    cohorts = cohorts.with_columns(
        pl.when(pl.col("prev_measdate").is_not_null())
        .then(
            (pl.col("measdate") - pl.col("prev_measdate")).dt.total_days() / 365.25
        )
        .otherwise(pl.lit(None).cast(pl.Float64))
        .alias("gap_years")
    )

    # Rows not covered by condition 1 (first visit) or condition 2 (born in interval)
    needs_c3 = (
        cohorts
        .filter(
            pl.col("prev_measdate").is_not_null() &
            (pl.col("age_calc").cast(pl.Float64) > pl.col("gap_years"))
        )
        .with_columns(
            (pl.col("age_calc").cast(pl.Float64) - pl.col("gap_years").round(0))
            .alias("expected_prev_age")
        )
    )

    # All distinct (subplot, species, measdate, age) present in the data
    prev_ages = (
        df.select(subplot_id + ["species_symbol", "measdate", "age_calc"])
        .unique()
        .rename({"measdate": "prev_measdate", "age_calc": "prev_age_calc"})
    )

    cond3 = (
        needs_c3
        .join(prev_ages, on=subplot_id + ["species_symbol", "prev_measdate"], how="left")
        .with_columns(
            (
                pl.col("prev_age_calc").is_not_null() &
                (
                    (pl.col("prev_age_calc").cast(pl.Float64) - pl.col("expected_prev_age"))
                    .abs() <= age_tol
                )
            ).alias("age_match")
        )
        .group_by(cohort_key)
        .agg(pl.col("age_match").any().alias("has_prev"))
    )

    n_viol = (
        needs_c3
        .join(cond3, on=cohort_key, how="left")
        .filter(~pl.col("has_prev").fill_null(False))
        .height
    )
    n_total = cohorts.height
    print(f"[continuity check] {n_viol:,} violations / {n_total:,} cohorts  (age_tol={age_tol})")
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

    # Attach plot measurement count
    meas_num = build_plot_meas_num(df)
    df = df.join(meas_num, on=PLOT_KEY, how="left")

    # Anchor age_calc to integer sim_year steps (eliminates floating-point drift)
    df = compute_sim_year_ages(df)

    # Backfill phantom cohorts (before plt_cn / rename)
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
        "age_calc",
        "agb",
        "subp_has_dstrb",
        "plot_meas_num",
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
