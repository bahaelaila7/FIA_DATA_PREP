"""
cohort_tracking.py
Longitudinal cohort tracking for curated FIA subplot data.

A cohort is defined by (subplot, SPCD, birth_date) where
  birth_date = MEASDATE - duration(round(estimated_age) * 365 days)

Since all age sources are linearly propagated from a single anchor, a tree's
birth_year is stable across remeasurements — so the same cohort_id persists
over time without any year-to-year matching heuristic.

Output table: curated_cohorts  (one row per cohort × MEASDATE)
  cohort identity : cohort_id, subplot key, SPCD, birth_date
  per-visit live  : n_trees_live, biomass_per_acre, estimated_age_mean/sd
  introduction    : first_measdate, intro_type
  per-interval    : n_harvested, n_disturbance_dead   (since last measurement)
  cumulative      : ever_harvested, ever_disturbed_dead
(r7 D44 / E25: the DAMSEV-based pct_biomass_damaged column is removed: DAMSEV is a 0-9 class
code, never > 40, so it was always 0 (D25).)

Standalone usage:
    python cohort_tracking.py --out-db <curated.duckdb> --fiadb <FIADB.duckdb>

Library usage:
    from cohort_tracking import build_cohorts
    cohorts = build_cohorts(df)
"""

import os as _os, sys as _sys  # curation-fix: sibling modules resolve under `python -I`
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
import sys

import duckdb
import numpy as np
import polars as pl
from fia_ids import pick, maybe_shuffle

from cohorts_landis import disturbance_death_expr   # r7 D41: the one disturbance definition

TREE_ID    = ["STATECD", "UNITCD", "COUNTYCD", "PLOT", "SUBP", "TREE"]
SUBPLOT_ID = ["STATECD", "UNITCD", "COUNTYCD", "PLOT", "SUBP"]
COHORT_KEY = SUBPLOT_ID + ["SPCD", "birth_date"]


# =============================================================================
# Step 1: assign birth_date and cohort_id to every tree-measurement
# =============================================================================

def assign_cohort_ids(df: pl.DataFrame) -> pl.DataFrame:
    """
    For each tree, derive birth_date from estimated_age at its earliest
    measurement with a valid age, and give each the natural cohort id
    unique (subplot, SPCD, birth_date) combination.

    Adds columns: birth_date (Date), cohort_id (the cohort's natural key as text).
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
        .pipe(pick, TREE_ID, ["MEASDATE"], "first")
        .select(
            TREE_ID +
            [(pl.col("MEASDATE") -
              pl.duration(days=pl.col("estimated_age").round(0, mode="half_away_from_zero").cast(pl.Int32) * 365))
             .cast(pl.Date)
             .alias("birth_date")]
        )
    )

    df = df.join(tree_birth, on=TREE_ID, how="left")

    # cohort_id = the natural cohort key as text (D66: never a row number)
    cohort_ids = (
        df.filter(pl.col("birth_date").is_not_null())
        .select(COHORT_KEY)
        .unique()
        .with_columns(pl.concat_str([pl.col(c).cast(pl.Utf8) for c in COHORT_KEY], separator="_").alias("cohort_id"))
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
    For each cohort: first_measdate and intro_type based on condition context
    at the first measurement where live trees of that cohort appear.

    Returns a DataFrame keyed by cohort_id with columns:
      first_measdate, intro_type
    """
    first_live = (
        df.filter(
            (pl.col("STATUSCD") == 1) &
            pl.col("cohort_id").is_not_null()
        )
        .pipe(pick, ["cohort_id"], ["MEASDATE"] + TREE_ID)     # r2 M2: full key tie-break
        .select([
            "cohort_id",
            pl.col("MEASDATE").alias("first_measdate"),
            "cond_stdorgcd",
            "TRTCD1", "TRTCD2", "TRTCD3",
        ])
    )

    return first_live.with_columns(
        _classify_intro_type(
            pl.col("cond_stdorgcd"),
            pl.col("TRTCD1"), pl.col("TRTCD2"), pl.col("TRTCD3"),
        ).alias("intro_type")
    ).select(["cohort_id", "first_measdate", "intro_type"])


# =============================================================================
# Step 3: per-cohort × MEASDATE summaries
# =============================================================================

def build_cohort_measurements(df: pl.DataFrame) -> pl.DataFrame:
    """
    Aggregate tree-level records to one row per (cohort_id × MEASDATE).

    Live-tree summary:
      n_trees_live, biomass_per_acre, estimated_age_mean, estimated_age_sd

    Per-interval events (a tree's FIRST dead/cut record; re-records, misrecorded deaths and,
    r7 D38, first_record_dead records — first-seen deaths, not mortality of the interval — are
    not events):
      n_harvested        trees with tree_fate in {silvicultural_death, harvest_inferred}
      n_disturbance_dead trees with tree_fate == dead under the ONE disturbance definition
                         (r7 D41, cohorts_landis.disturbance_death_expr): AGENTCD 10-50 or a
                         condition DSTRBCD other than 0 / 60
    """
    has_cohort = pl.col("cohort_id").is_not_null()

    # ---- Live trees ----
    live = df.filter((pl.col("STATUSCD") == 1) & has_cohort)

    live_agg = (
        live
        .with_columns((pl.col("DRYBIO_AG") * pl.col("TPA_UNADJ")).alias("_bio"))
        .group_by(["cohort_id", "INVYR"])
        .agg([
            pl.len().alias("n_trees_live"),
            pl.col("_bio").sort().sum().round(6).alias("biomass_per_acre"),          # D66: rounded, so a float sum does not carry the row order
            pl.col("estimated_age").sort().mean().round(6).alias("estimated_age_mean"),
            pl.col("estimated_age").sort().std().round(6).alias("estimated_age_sd"),
        ])
    )

    # ---- Dead / removed trees ----
    # r3 I1: only a tree's FIRST dead/cut record is an event; re-records are skipped
    # r6 D26: a misrecorded death is not a death; r7 D38: a first_record_dead record is a
    # first-seen death, not mortality of the cohort's interval
    dead = df.filter(pl.col("STATUSCD").is_in([2, 3]) & has_cohort
                     & ~pl.col("rerecord_dead").fill_null(False)
                     & ~pl.col("misrecorded_death").fill_null(False)
                     & ~pl.col("first_record_dead").fill_null(False))

    # r7 D41: THE disturbance-death definition (AGENTCD 10-50 or a DSTRBCD other than 0 / 60;
    # r3 M4: DSTRBCD 60 is not a disturbance), shared with subp_has_dstrb / trajectories / drops
    disturbed_cond = disturbance_death_expr()

    harvest_agg = (
        dead.filter(pl.col("tree_fate").is_in(["silvicultural_death", "harvest_inferred"]))
        .group_by(["cohort_id", "INVYR"])
        .agg(pl.len().alias("n_harvested"))
    )

    disturbance_death_agg = (
        dead.filter((pl.col("tree_fate") == "dead") & disturbed_cond)
        .group_by(["cohort_id", "INVYR"])
        .agg(pl.len().alias("n_disturbance_dead"))
    )

    # ---- Scaffold: all (cohort, year) pairs with any tree activity ----
    scaffold = (
        pl.concat([
            live.select(["cohort_id", "INVYR", "MEASDATE"]),
            dead.select(["cohort_id", "INVYR", "MEASDATE"]),
        ])
        .unique()
    )

    meas = (
        scaffold
        .join(live_agg,              on=["cohort_id", "INVYR"], how="left")
        .join(harvest_agg,           on=["cohort_id", "INVYR"], how="left")
        .join(disturbance_death_agg, on=["cohort_id", "INVYR"], how="left")
        .with_columns([
            pl.col("n_trees_live").fill_null(0),
            pl.col("biomass_per_acre").fill_null(0.0),
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
        (pl.col("n_harvested").cum_sum().over("cohort_id", order_by="MEASDATE") > 0)
          .alias("ever_harvested"),
        (pl.col("n_disturbance_dead").cum_sum().over("cohort_id", order_by="MEASDATE") > 0)
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
        df.select(SUBPLOT_ID + ["INVYR", "MEASDATE"])
        .unique()
        .with_columns(
            pl.col("MEASDATE").shift(1).over(SUBPLOT_ID, order_by="MEASDATE").alias("prev_measdate"),
            pl.col("INVYR").shift(1).over(SUBPLOT_ID, order_by="MEASDATE").alias("prev_invyr"),
        )
    )

    live = (
        df.filter(pl.col("STATUSCD") == 1)
        .join(
            subp_prev.select(SUBPLOT_ID + ["INVYR", "prev_measdate", "prev_invyr"]),
            on=SUBPLOT_ID + ["INVYR"],
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
        .select(TREE_ID + [pl.col("INVYR").alias("prev_invyr"), pl.col("estimated_age").alias("prev_estimated_age")])
    )
    live = live.join(prev_alive, on=TREE_ID + ["prev_invyr"], how="left")

    # First-ever record per tree (any STATUSCD)
    tree_first = (
        df.group_by(TREE_ID)
        .agg(pl.col("MEASDATE").min().alias("first_measdate"))
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
        (pl.col("MEASDATE") != pl.col("first_measdate"))
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

    # Attach cohort key columns (subplot + SPCD + birth_date) from the id map
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
        "birth_date",
        "MEASDATE",
        "first_measdate",
        "intro_type",
        "n_trees_live",
        "biomass_per_acre",
        "estimated_age_mean",
        "estimated_age_sd",
        "n_harvested",
        "n_disturbance_dead",
        "ever_harvested",
        "ever_disturbed_dead",
        "INVYR",                 # D69: the visit's natural id, appended
    ]).sort(["cohort_id", "INVYR"])

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

LOAD_COLS = TREE_ID + ["MEASDATE", "INVYR", "STATUSCD", "tree_fate", "estimated_age", "DRYBIO_AG",
                       "TPA_UNADJ", "AGENTCD", "DSTRBCD1", "DSTRBCD2", "DSTRBCD3",
                       "TRTCD1", "TRTCD2", "TRTCD3", "cond_stdorgcd", "rerecord_dead",
                       "misrecorded_death", "first_record_dead"]

if __name__ == "__main__":
    # curation-fix 2026-10-26: explicit paths (dbconn); cohort species = spcd_resolved
    # (constant per tree) instead of the raw per-visit SPCD, which flips between confused
    # species and split one tree over two cohorts; only the needed columns are loaded and
    # states are processed one at a time (cohorts never cross a subplot), with cohort_id
    import argparse
    import dbconn
    a = dbconn.add_db_args(argparse.ArgumentParser(description="cohort tracking")).parse_args()
    dbconn.check_args(a)
    con = dbconn.connect(a.out_db, a.fiadb)
    dbconn.require_tables(con, "curated_trees")
    cols = ", ".join(LOAD_COLS)
    states = [r[0] for r in con.execute(
        "SELECT DISTINCT STATECD FROM curated_trees ORDER BY 1").fetchall()]
    parts = []
    for st in states:
        print(f"\n----- STATECD {st} -----")
        df = con.execute(f"SELECT {cols}, spcd_resolved AS SPCD FROM curated_trees "
                         f"WHERE STATECD = {int(st)}").pl()
        print(f"       {df.height:,} rows, {len(df.columns)} columns")
        cohorts = build_cohorts(df)
        tab = f"_ct_{st}"
        con.register("_cohorts_view", cohorts)
        con.execute(f"CREATE OR REPLACE TABLE {tab} AS SELECT * FROM _cohorts_view")
        con.unregister("_cohorts_view")
        parts.append(tab)
        del df, cohorts
    con.execute("CREATE OR REPLACE TABLE curated_cohorts AS " +
                " UNION ALL BY NAME ".join(f"SELECT * FROM {p}" for p in parts))
    for p in parts:
        con.execute(f"DROP TABLE {p}")
    con.commit()
    n = con.execute("SELECT COUNT(*) FROM curated_cohorts").fetchone()[0]
    print(f"\n[save] curated_cohorts → {n:,} rows")
    con.close()
    print("[done]")
