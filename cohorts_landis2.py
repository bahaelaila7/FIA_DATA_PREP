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

import os as _os, sys as _sys  # curation-fix: sibling modules resolve under `python -I`
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
import sys

import duckdb
import polars as pl
from fia_ids import pick, maybe_shuffle

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
    import dbconn
    return col.lower() in dbconn.column_names(con, table)


def _has_table(con, table: str) -> bool:
    import dbconn
    return dbconn.table_exists(con, table)


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
            t.spcd_resolved AS SPCD,               -- curation-fix: resolved species (was raw t.SPCD)
            t.MEASDATE,
            ROUND(t.estimated_age)::INTEGER        AS age_calc,
            (t.DRYBIO_AG * t.TPA_UNADJ
             * {LBS_ACRE_TO_G_M2})::DOUBLE         AS agb,
            e.epa_l3,
            {epa_l4_expr},
            {ecosubcd_col},
            r.SPECIES_SYMBOL::VARCHAR              AS species_symbol,
            COALESCE(t.spgrpcd_resolved, t.SPGRPCD)::INTEGER AS spgrpcd,
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
            ON  r.SPCD = t.spcd_resolved
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
        df.select(PLOT_KEY + ["MEASDATE"])
        .unique()
        .group_by(PLOT_KEY)
        .agg(pl.col("MEASDATE").n_unique().alias("plot_meas_num"))
    )


# =============================================================================
# Step 4: phantom cohort diagnostics
# =============================================================================

def _diagnose_phantom_cohorts(df: pl.DataFrame) -> pl.DataFrame:
    """curation-fix: the MEASYEAR-based version was replaced by the MEASDATE-based one in
    cohorts_landis (v1) — never extract/order by year."""
    import cohorts_landis
    return cohorts_landis._diagnose_phantom_cohorts(df)


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
        df.pipe(pick, TREE_ID, ["MEASDATE"], "first")
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
    srt = df

    first = (
        srt.pipe(pick, TREE_ID, ["MEASDATE"], "first")
        .select(TREE_ID + [pl.col("age_calc").alias("first_age"),
                            pl.col("agb").alias("first_agb")])
    )
    last = (
        srt.pipe(pick, TREE_ID, ["MEASDATE"], "last")
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
        df.pipe(pick, TREE_ID, ["MEASDATE"], "first")
    )

    # All distinct subplot measurement visits (sim_year for integer age arithmetic)
    subp_visits = (
        df.select(SUBPLOT_ID + ["MEASDATE", "sim_year"]).unique()
        .rename({"MEASDATE": "prior_measdate", "sim_year": "prior_sim_year"})
    )

    # Cross-join each debut tree with all prior subplot visits, compute prior age
    # age_calc here is the debut_age (sim_year-anchored); sim_year is debut_sim_year.
    # prior_age = debut_age + (prior_sim_year - debut_sim_year) — pure integer arithmetic.
    prior = (
        tree_debut
        .join(subp_visits, on=SUBPLOT_ID, how="left")
        .filter(pl.col("prior_measdate") < pl.col("MEASDATE"))   # curation-fix: MEASDATE, not MEASYEAR
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
        .pipe(pick, SUBPLOT_ID + ["species_symbol", "prior_measdate", "prior_age"], ["TREE"])
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
        .drop(["MEASDATE", "age_calc", "agb", "sim_year"])
        .rename({
            "prior_measdate": "MEASDATE",
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
        .with_columns(
            pl.col("measdate").shift(1).over(subplot_id, order_by="measdate").alias("prev_measdate")
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
# Propagate land_use (2-way) + land_use_4way + inventory cycle into curated_cohorts_landis
# =============================================================================

# r7: the attach-managed columns of curated_cohorts_landis, in schema order after the step-4
# columns. Every attach rebuilds the table as <step-4 columns> + MANAGED, so a rerun of this
# step alone keeps the column order (D F9) and the old schema stays an exact prefix:
#   land_use, cycle (old), the age metadata (r3 I4 / r4), procedural_break (curation-fix I4),
#   land_use_4way (r7 C F10), all_live_withdrawn_rc7 (r7 D39).
AGE_META = ["n_trees_natural_treeage_scale", "share_natural_treeage_scale", "age_source_major",
            "n_trees_known_bias", "share_known_bias"]
MANAGED = ["land_use", "cycle", *AGE_META, "procedural_break", "land_use_4way", "all_live_withdrawn_rc7"]


def attach_land_use_and_cycle(con) -> None:
    """Rebuild curated_cohorts_landis with (SQL join, no polars rebuild needed):
      - `land_use` (r7 D44 / C F10): the 2-way STDORGCD label, the same rule as _stdorg / Sim B /
        tree_trajectories (cohorts_landis.plot_land_use_sql; r6 carried the 4-way label here);
      - `cycle`: the FIA inventory cycle, per (plot, measdate) from base_subplot_cond;
      - `land_use_4way` (r7): the DERIVED management land use (reserved / plantation / managed /
        natural; build_tree_trajectories.derive_land_use, per subplot there), as the per-PLOT
        dominant (most frequent over trajectory rows, ties -> alphabetical) value, so it is a
        stable plot attribute;
      - the visit metadata procedural_break and all_live_withdrawn_rc7 (cohorts_landis.add_visit_flags).
    Requires tree_trajectories (land_use_4way) + base_subplot_cond (CYCLE) + COND."""
    import dbconn
    from cohorts_landis import add_visit_flags, plot_land_use_sql
    have = [r[0] for r in con.execute(
        "SELECT column_name FROM duckdb_columns() WHERE database_name = current_database() "
        "AND table_name = 'curated_cohorts_landis' ORDER BY column_index").fetchall()]
    miss = [m for m in AGE_META if m not in have]
    if miss:
        raise SystemExit(f"curated_cohorts_landis lacks {miss}: rerun step 4 (cohorts_landis.py)")
    base = ", ".join(f"c.{x}" for x in have if x.lower() not in {m.lower() for m in MANAGED})
    con.execute(f"""
    CREATE OR REPLACE TABLE curated_cohorts_landis AS
    WITH plot_lu AS ({plot_land_use_sql(con)}),
    plot_lu4 AS (
        SELECT statecd, unitcd, countycd, plot, land_use_4way FROM (
            SELECT statecd, unitcd, countycd, plot, land_use_4way,
                   ROW_NUMBER() OVER (PARTITION BY statecd, unitcd, countycd, plot
                                      ORDER BY COUNT(*) DESC, land_use_4way) AS rn
            FROM tree_trajectories
            WHERE land_use_4way IS NOT NULL
            GROUP BY statecd, unitcd, countycd, plot, land_use_4way
        ) WHERE rn = 1
    ),
    plot_cycle AS (
        SELECT STATECD AS statecd, UNITCD AS unitcd, COUNTYCD AS countycd, PLOT AS plot,
               MEASDATE AS measdate, MAX(CYCLE) AS cycle
        FROM base_subplot_cond
        GROUP BY STATECD, UNITCD, COUNTYCD, PLOT, MEASDATE
    )
    SELECT {base},
           lu.land_use, cy.cycle,
           -- r3 I4: appended AFTER the old columns (old schema stays an exact prefix)
           {", ".join(f"c.{m}" for m in AGE_META)},
           {"c.procedural_break" if "procedural_break" in have else "NULL::BOOLEAN AS procedural_break"},
           lu4.land_use_4way,
           {"c.all_live_withdrawn_rc7" if "all_live_withdrawn_rc7" in have else "NULL::BOOLEAN AS all_live_withdrawn_rc7"}
    FROM curated_cohorts_landis c
    LEFT JOIN plot_lu lu
        ON c.statecd=lu.statecd AND c.unitcd=lu.unitcd AND c.countycd=lu.countycd AND c.plot=lu.plot
    LEFT JOIN plot_lu4 lu4
        ON c.statecd=lu4.statecd AND c.unitcd=lu4.unitcd AND c.countycd=lu4.countycd AND c.plot=lu4.plot
    LEFT JOIN plot_cycle cy
        ON c.statecd=cy.statecd AND c.unitcd=cy.unitcd AND c.countycd=cy.countycd
        AND c.plot=cy.plot AND c.measdate=cy.measdate
    """)
    add_visit_flags(con, "curated_cohorts_landis")   # fills the two visit-flag columns in place
    con.commit()
    tot, n_lu, n_lu4, n_cy = con.execute(
        "SELECT COUNT(*), COUNT(land_use), COUNT(land_use_4way), COUNT(cycle) FROM curated_cohorts_landis").fetchone()
    print(f"[land_use+cycle] land_use (2-way) non-null {n_lu:,}/{tot:,}  ·  land_use_4way non-null "
          f"{n_lu4:,}/{tot:,}  ·  cycle non-null {n_cy:,}/{tot:,}")


# =============================================================================
# Standalone entry point
# =============================================================================

if __name__ == "__main__":
    # curation-fix 2026-10-26: explicit --out-db/--fiadb (dbconn). The production table is
    # built by cohorts_landis.py (v1: sim_year + subp_meas_num); this script is used with
    # --attach-only to add land_use + cycle.
    # r7 D44 (C F9): the v2 full build is STALE (pre-D18 time axis, no backfill blocks, no age
    # metadata) and refuses to run; its code is kept for reference only.
    import argparse
    import dbconn
    ap = dbconn.add_db_args(argparse.ArgumentParser(description="cohorts_landis2"))
    ap.add_argument("--attach-only", action="store_true",
                    help="skip the rebuild; only add land_use + cycle (the only supported mode)")
    a = ap.parse_args()
    dbconn.check_args(a)
    if not a.attach_only:
        sys.exit("cohorts_landis2.py: the full rebuild is stale (pre-D18 axis, no backfill blocks) and "
                 "refuses to run; build curated_cohorts_landis with cohorts_landis.py, then use --attach-only")
    con = dbconn.connect(a.out_db, a.fiadb)
    print(f"DB: {a.out_db}")

    dbconn.require_tables(con, "curated_cohorts_landis", "tree_trajectories", "base_subplot_cond", "COND", "product_visits")
    attach_land_use_and_cycle(con)

    con.close()
    print("[done]")
