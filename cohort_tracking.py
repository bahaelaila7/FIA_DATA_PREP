"""
cohort_tracking.py
Longitudinal cohort tracking for curated FIA subplot data.

A cohort is defined by (subplot, SPCD, birth_year_bin) where
  birth_year_bin = floor((MEASYEAR - estimated_age) / BIN_YEARS) * BIN_YEARS

Since all age sources are linearly propagated from a single anchor, a tree's
birth_year is stable across remeasurements — so the same cohort_id persists
over time without any year-to-year matching heuristic.

Output table: curated_cohorts  (one row per cohort × MEASYEAR)
  cohort identity : cohort_id, subplot key, SPCD, birth_year_bin
  per-year live   : n_trees_live, biomass_per_acre, estimated_age_mean/sd
  introduction    : first_measyear, intro_type
  per-interval    : n_harvested, n_disturbance_dead   (since last measurement)
  damage          : pct_biomass_damaged  (live biomass from DAMSEV > 40 trees)
  cumulative      : ever_harvested, ever_disturbed_dead

Standalone usage:
    python cohort_tracking.py [path/to/fiadb.duckdb]
    # default db path: ../data_eco_cohorts.duckdb

Library usage:
    from cohort_tracking import build_cohorts
    cohorts = build_cohorts(df)
"""

import sys

import duckdb
import numpy as np
import polars as pl

TREE_ID    = ["STATECD", "UNITCD", "COUNTYCD", "PLOT", "SUBP", "TREE"]
SUBPLOT_ID = ["STATECD", "UNITCD", "COUNTYCD", "PLOT", "SUBP"]
COHORT_KEY = SUBPLOT_ID + ["SPCD", "birth_year_bin"]

BIN_YEARS = 5   # birth-year bin width; set wider if ages are imprecise


# =============================================================================
# Step 1: assign birth_year_bin and cohort_id to every tree-measurement
# =============================================================================

def assign_cohort_ids(df: pl.DataFrame) -> pl.DataFrame:
    """
    For each tree, derive birth_year from estimated_age at its earliest
    measurement with a valid age, bin it to BIN_YEARS, and assign an integer
    cohort_id to each unique (subplot, SPCD, birth_year_bin) combination.

    Adds columns: birth_year_bin (Int32), cohort_id (UInt32).
    Trees without any estimated_age get null in both columns.
    """
    # Per-tree birth year: use earliest measurement with a valid estimated_age.
    # Since age is linearly propagated, any measurement gives the same birth_year;
    # we take the earliest for determinism.
    tree_birth = (
        df.filter(
            pl.col("estimated_age").is_not_null() &
            pl.col("estimated_age").is_not_nan() &
            (pl.col("estimated_age") > 0)
        )
        .sort(TREE_ID + ["MEASYEAR"])
        .unique(subset=TREE_ID, keep="first")
        .select(
            TREE_ID +
            [(pl.col("MEASYEAR") - pl.col("estimated_age"))
             .floordiv(BIN_YEARS)
             .mul(BIN_YEARS)
             .cast(pl.Int32)
             .alias("birth_year_bin")]
        )
    )

    df = df.join(tree_birth, on=TREE_ID, how="left")

    # Integer cohort_id for each unique COHORT_KEY
    cohort_ids = (
        df.filter(pl.col("birth_year_bin").is_not_null())
        .select(COHORT_KEY)
        .unique()
        .sort(COHORT_KEY)
        .with_row_index("cohort_id")
        .with_columns(pl.col("cohort_id").cast(pl.UInt32))
    )
    df = df.join(cohort_ids, on=COHORT_KEY, how="left")

    n_trees     = df.select(TREE_ID).unique().height
    n_assigned  = df.filter(pl.col("cohort_id").is_not_null()).select(TREE_ID).unique().height
    n_cohorts   = cohort_ids.height
    n_subplots  = cohort_ids.select(SUBPLOT_ID).unique().height
    print(f"[step1] {n_assigned:,}/{n_trees:,} trees assigned to cohorts "
          f"({n_cohorts:,} cohorts on {n_subplots:,} subplots)")
    return df


# =============================================================================
# Step 2: introduction context (first appearance of each cohort)
# =============================================================================

def _classify_intro_type(stdorgcd: pl.Expr,
                         trtcd1: pl.Expr,
                         trtcd2: pl.Expr,
                         trtcd3: pl.Expr) -> pl.Expr:
    """
    Classify the type of cohort introduction from condition context at first
    appearance:
      planted          cond_stdorgcd = 1  (planted stand; all trees co-established)
      artificial_regen TRTCD = 30         (artificial regeneration treatment)
      natural_regen    TRTCD = 40         (natural regeneration treatment)
      natural          everything else    (ingrowth / succession)
    """
    artificial = (
        (stdorgcd == 1) |
        (trtcd1 == 30) | (trtcd2 == 30) | (trtcd3 == 30)
    )
    return (
        pl.when(artificial)
          .then(pl.lit("artificial"))
        .otherwise(pl.lit("natural"))
    )


def build_cohort_intro(df: pl.DataFrame) -> pl.DataFrame:
    """
    For each cohort: first_measyear and intro_type based on condition context
    at the first measurement where live trees of that cohort appear.

    Returns a DataFrame keyed by cohort_id with columns:
      first_measyear, intro_type
    """
    first_live = (
        df.filter(
            (pl.col("STATUSCD") == 1) &
            pl.col("cohort_id").is_not_null()
        )
        .sort(["cohort_id", "MEASYEAR"])
        .unique(subset=["cohort_id"], keep="first")
        .select([
            "cohort_id",
            pl.col("MEASYEAR").alias("first_measyear"),
            "cond_stdorgcd",
            "TRTCD1", "TRTCD2", "TRTCD3",
        ])
    )

    return first_live.with_columns(
        _classify_intro_type(
            pl.col("cond_stdorgcd"),
            pl.col("TRTCD1"), pl.col("TRTCD2"), pl.col("TRTCD3"),
        ).alias("intro_type")
    ).select(["cohort_id", "first_measyear", "intro_type"])


# =============================================================================
# Step 3: per-cohort × MEASYEAR summaries
# =============================================================================

def build_cohort_measurements(df: pl.DataFrame) -> pl.DataFrame:
    """
    Aggregate tree-level records to one row per (cohort_id × MEASYEAR).

    Live-tree summary:
      n_trees_live, biomass_per_acre, estimated_age_mean, estimated_age_sd

    Per-interval events (trees whose status changed since last measurement):
      n_harvested        trees with tree_fate in {harvest_explicit, harvest_inferred}
      n_disturbance_dead trees with tree_fate == dead AND condition had a
                         disturbance code (DSTRBCD != 0 and not null)

    Damage fraction (live trees only):
      pct_biomass_damaged  % of live biomass (DRYBIO_AG × TPA_UNADJ) from trees
                           where DAMSEV1 > 40 OR DAMSEV2 > 40
                           (DAMSEV treated as 0–100 percent severity per the
                           user's threshold; verify against your FIA version)
    """
    has_cohort = pl.col("cohort_id").is_not_null()

    # ---- Live trees ----
    live = df.filter((pl.col("STATUSCD") == 1) & has_cohort)

    damaged = (
        (pl.col("DAMSEV1").is_not_null() & (pl.col("DAMSEV1") > 40)) |
        (pl.col("DAMSEV2").is_not_null() & (pl.col("DAMSEV2") > 40))
    )

    live_agg = (
        live
        .with_columns([
            (pl.col("DRYBIO_AG") * pl.col("TPA_UNADJ")).alias("_bio"),
            damaged.alias("_dmg"),
        ])
        .group_by(["cohort_id", "MEASYEAR"])
        .agg([
            pl.len().alias("n_trees_live"),
            pl.col("_bio").sum().alias("biomass_per_acre"),
            pl.col("estimated_age").mean().alias("estimated_age_mean"),
            pl.col("estimated_age").std().alias("estimated_age_sd"),
            # Damaged biomass (only where DRYBIO_AG is available)
            pl.when(pl.col("_dmg") & pl.col("_bio").is_not_null())
              .then(pl.col("_bio")).sum().alias("_damaged_bio"),
            pl.col("_bio").filter(pl.col("_bio").is_not_null()).sum().alias("_total_bio"),
        ])
        .with_columns(
            pl.when(pl.col("_total_bio") > 0)
            .then(100.0 * pl.col("_damaged_bio") / pl.col("_total_bio"))
            .otherwise(pl.lit(0.0))
            .alias("pct_biomass_damaged")
        )
        .drop(["_damaged_bio", "_total_bio"])
    )

    # ---- Dead / removed trees ----
    dead = df.filter(pl.col("STATUSCD").is_in([2, 3]) & has_cohort)

    disturbed_cond = (
        (pl.col("DSTRBCD1").is_not_null() & (pl.col("DSTRBCD1") != 0)) |
        (pl.col("DSTRBCD2").is_not_null() & (pl.col("DSTRBCD2") != 0)) |
        (pl.col("DSTRBCD3").is_not_null() & (pl.col("DSTRBCD3") != 0))
    )

    harvest_agg = (
        dead.filter(pl.col("tree_fate").str.starts_with("harvest"))
        .group_by(["cohort_id", "MEASYEAR"])
        .agg(pl.len().alias("n_harvested"))
    )

    disturbance_death_agg = (
        dead.filter((pl.col("tree_fate") == "dead") & disturbed_cond)
        .group_by(["cohort_id", "MEASYEAR"])
        .agg(pl.len().alias("n_disturbance_dead"))
    )

    # ---- Scaffold: all (cohort, year) pairs with any tree activity ----
    scaffold = (
        pl.concat([
            live.select(["cohort_id", "MEASYEAR"]),
            dead.select(["cohort_id", "MEASYEAR"]),
        ])
        .unique()
        .sort(["cohort_id", "MEASYEAR"])
    )

    meas = (
        scaffold
        .join(live_agg,              on=["cohort_id", "MEASYEAR"], how="left")
        .join(harvest_agg,           on=["cohort_id", "MEASYEAR"], how="left")
        .join(disturbance_death_agg, on=["cohort_id", "MEASYEAR"], how="left")
        .with_columns([
            pl.col("n_trees_live").fill_null(0),
            pl.col("biomass_per_acre").fill_null(0.0),
            pl.col("pct_biomass_damaged").fill_null(0.0),
            pl.col("n_harvested").fill_null(0),
            pl.col("n_disturbance_dead").fill_null(0),
        ])
    )

    return meas


# =============================================================================
# Step 4: cumulative event flags
# =============================================================================

def add_cumulative_flags(meas: pl.DataFrame) -> pl.DataFrame:
    """
    Append ever_harvested and ever_disturbed_dead: True from the first
    measurement where the respective event occurred, forward through all
    subsequent measurements of the same cohort.
    """
    return meas.with_columns([
        (pl.col("n_harvested").cum_sum().over("cohort_id") > 0)
          .alias("ever_harvested"),
        (pl.col("n_disturbance_dead").cum_sum().over("cohort_id") > 0)
          .alias("ever_disturbed_dead"),
    ])


# =============================================================================
# Continuity check
# =============================================================================

def check_tree_continuity(df: pl.DataFrame, age_tol: float = 0.5) -> int:
    """
    Two checks on alive tree-measurements:

    A. Tree was alive at the previous subplot visit → its age must equal
       current_age - gap  (within age_tol). Catches broken age propagation.

    B. Tree has no alive record at the previous subplot visit → this must be
       its first-ever record (any STATUSCD). Catches trees reappearing after
       a gap. Phantoms (first-time trees with age > gap) are not flagged here;
       that is handled by the backfill in cohorts_landis.py.
    """
    subp_prev = (
        df.select(SUBPLOT_ID + ["MEASYEAR", "MEASDATE"])
        .unique()
        .sort(SUBPLOT_ID + ["MEASYEAR"])
        .with_columns([
            pl.col("MEASDATE").shift(1).over(SUBPLOT_ID).alias("prev_measdate"),
            pl.col("MEASYEAR").shift(1).over(SUBPLOT_ID).alias("prev_measyear"),
        ])
    )

    live = (
        df.filter(pl.col("STATUSCD") == 1)
        .join(
            subp_prev.select(SUBPLOT_ID + ["MEASYEAR", "prev_measdate", "prev_measyear"]),
            on=SUBPLOT_ID + ["MEASYEAR"],
            how="left",
        )
        .with_columns(
            pl.when(pl.col("prev_measdate").is_not_null())
            .then(
                (pl.col("MEASDATE") - pl.col("prev_measdate")).dt.total_days() / 365.25
            )
            .otherwise(pl.lit(None).cast(pl.Float64))
            .alias("gap_years")
        )
    )

    # Previous alive visit per tree
    prev_alive = (
        df.filter(pl.col("STATUSCD") == 1)
        .select(TREE_ID + ["MEASYEAR", pl.col("estimated_age").alias("prev_estimated_age")])
        .rename({"MEASYEAR": "prev_measyear"})
    )
    live = live.join(prev_alive, on=TREE_ID + ["prev_measyear"], how="left")

    # First-ever record per tree (any STATUSCD)
    tree_first = (
        df.group_by(TREE_ID)
        .agg(pl.col("MEASYEAR").min().alias("first_measyear"))
    )
    live = live.join(tree_first, on=TREE_ID, how="left")

    # A: was alive at previous visit but age doesn't match
    viol_a = live.filter(
        pl.col("prev_measdate").is_not_null() &
        pl.col("prev_estimated_age").is_not_null() &
        (
            (pl.col("estimated_age") - pl.col("gap_years") - pl.col("prev_estimated_age"))
            .abs() > age_tol
        )
    )

    # B: absent at previous visit but not the tree's first-ever record
    viol_b = live.filter(
        pl.col("prev_measdate").is_not_null() &
        pl.col("prev_estimated_age").is_null() &
        (pl.col("MEASYEAR") != pl.col("first_measyear"))
    )

    n_a, n_b, n_total = viol_a.height, viol_b.height, live.height
    print(f"[tree continuity] {n_a + n_b:,} violations / {n_total:,} alive tree-measurements"
          f"  (age_tol={age_tol})")
    if n_a:
        print(f"  A — age inconsistency (broken propagation): {n_a:,}")
    if n_b:
        print(f"  B — absent at prior visit but not first record: {n_b:,}")
    return n_a + n_b


# =============================================================================
# Main assembly
# =============================================================================

def build_cohorts(df: pl.DataFrame) -> pl.DataFrame:
    """
    Full pipeline: assign IDs → build measurements → add intro context
    → add cumulative flags.

    Returns the curated_cohorts DataFrame.
    """
    print("=" * 70)
    print("Cohort Tracking Pipeline")
    print("=" * 70)

    check_tree_continuity(df)

    df    = assign_cohort_ids(df)
    intro = build_cohort_intro(df)
    meas  = build_cohort_measurements(df)
    meas  = add_cumulative_flags(meas)

    # Attach introduction context
    cohorts = meas.join(intro, on="cohort_id", how="left")

    # Attach cohort key columns (subplot + SPCD + birth_year_bin) from the id map
    cohort_keys = (
        df.filter(pl.col("cohort_id").is_not_null())
        .select(COHORT_KEY + ["cohort_id"])
        .unique()
    )
    cohorts = cohorts.join(cohort_keys, on="cohort_id", how="left")

    # Canonical column order
    cohorts = cohorts.select([
        "cohort_id",
        *SUBPLOT_ID,
        "SPCD",
        "birth_year_bin",
        "MEASYEAR",
        "first_measyear",
        "intro_type",
        "n_trees_live",
        "biomass_per_acre",
        "estimated_age_mean",
        "estimated_age_sd",
        "n_harvested",
        "n_disturbance_dead",
        "pct_biomass_damaged",
        "ever_harvested",
        "ever_disturbed_dead",
    ]).sort(["cohort_id", "MEASYEAR"])

    # Diagnostics
    n_rows     = cohorts.height
    n_cohorts  = cohorts["cohort_id"].n_unique()
    n_subplots = cohorts.select(SUBPLOT_ID).unique().height
    n_species  = cohorts["SPCD"].n_unique()

    print(f"\n[summary] {n_rows:,} cohort-measurement rows")
    print(f"          {n_cohorts:,} unique cohorts on {n_subplots:,} subplots, "
          f"{n_species:,} species")

    intro_dist = (
        cohorts.unique(subset=["cohort_id"])
        .group_by("intro_type").len()
        .sort("len", descending=True)
    )
    print("\n[intro_type distribution (unique cohorts)]:")
    for row in intro_dist.iter_rows(named=True):
        label = row['intro_type'] or "null"
        print(f"  {label:20s}: {row['len']:>10,}")

    n_ever_harv = cohorts.unique(subset=["cohort_id"]).filter(
        pl.col("ever_harvested").is_not_null() &
        pl.col("ever_harvested")
    ).height
    n_ever_dist = cohorts.unique(subset=["cohort_id"]).filter(
        pl.col("ever_disturbed_dead").is_not_null() &
        pl.col("ever_disturbed_dead")
    ).height
    print(f"\n[events (unique cohorts)]:")
    print(f"  ever_harvested       : {n_ever_harv:>10,}")
    print(f"  ever_disturbed_dead  : {n_ever_dist:>10,}")

    return cohorts


# =============================================================================
# Standalone entry point
# =============================================================================

if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else "../FIASQLITE2PGSQL/FIADB.duckdb"

    print(f"DB: {db_path}")
    con = duckdb.connect(db_path)

    print("[load] reading curated_trees ...")
    df = con.execute("SELECT * FROM curated_trees").pl()
    print(f"       {df.height:,} rows, {len(df.columns)} columns")

    cohorts = build_cohorts(df)

    con.register("_cohorts_view", cohorts)
    con.execute("CREATE OR REPLACE TABLE curated_cohorts AS SELECT * FROM _cohorts_view")
    con.unregister("_cohorts_view")
    con.commit()
    print(f"\n[save] curated_cohorts → {cohorts.height:,} rows")

    con.close()
    print("[done]")
