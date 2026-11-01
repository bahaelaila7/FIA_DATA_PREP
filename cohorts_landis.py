"""
cohorts_landis.py
Curates FIA data into the format expected by the Pan/LANDIS-II Biomass
Succession parametrization engine (Julia project).

Source tables  : curated_trees, clean_subplot_years, procedural_breaks, tree_censor_events (curation)
Reference tables: data_plot_eco (epa_l3 / epa_l4 / ecosubcd), REF_SPECIES
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
  subp_has_dstrb              BOOL     — True if the subplot ever had a cut / disturbance death /
                                         inferred removal after its first visit (r7 D41)
  plot_meas_num               INTEGER  — distinct measurement years for this plot

Biomass conversion: 1 lb/acre = 453.592/4046.86 ≈ 0.11208 g/m²

Species grouping (12/4/2 tiered bucketing) lives entirely in Julia's
assign_tiered_species!(). This table exposes species_symbol, spgrpcd,
major_spgrpcd, and sftwd_hrdwd so Julia can compute effective_species on
whatever subset it loads after its own SQL filters.

Standalone usage:
    python cohorts_landis.py --out-db <curated.duckdb> --fiadb <FIADB.duckdb>
"""

import os as _os, sys as _sys  # curation-fix: sibling modules resolve under `python -I`
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
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
# r7 D44 (C F5): the phantom biomass floor has explicit units PER TABLE. Sim A agb is TPA-expanded
# g/m2 (one tree's stand contribution), the trajectories' drybio_g is grams PER TREE; the two
# floors are separate constants (values unchanged from r6: 2.0 in each table's own unit).
MIN_BACKFILL_AGB  = 2.0  # g/m2 (Sim A, TPA-expanded) floor of a phantom row's agb
MIN_BACKFILL_DRYBIO_G = 2.0  # g per tree (tree_trajectories drybio_g) floor of a phantom row
SAPLING_DIA_MAX   = 5.0  # DIA threshold (inches) for microplot saplings used to estimate growth rates
MIN_RATE_TREES    = 10   # min trees for species×epa_l3 rate; else fall back to spgrpcd


# =============================================================================
# Helpers
# =============================================================================

def _has_column(con, table: str, col: str) -> bool:
    """curation-fix: only the output DB / temp views are inspected — information_schema
    would also list the attached FIADB's OLD curated tables."""
    import dbconn
    return col.lower() in dbconn.column_names(con, table)


def _has_table(con, table: str) -> bool:
    import dbconn
    return dbconn.table_exists(con, table)


# r3 I4: age-source metadata carried onto the cohort tables (Sim A rows are one tree each;
# phantom rows inherit their source tree's values; Sim B aggregates them AGB-weighted)
# r4 ACCEPT + FLAG: + n_trees_known_bias / share_known_bias (members whose age_domain is any
# known_bias_* flag; one generic pair, the flag names live in age_imputation.KNOWN_BIAS_FLAGS).
# *_natural_treeage_scale = natural MODEL ages (the D11 KY special case went with D13).
AGE_META_COLS = ["n_trees_natural_treeage_scale", "share_natural_treeage_scale", "age_source_major",
                 "n_trees_known_bias", "share_known_bias"]
# D46 (r7, after D40 M-9 relabelled natural model ages with DIA < 5 in 'out_of_support'): D11's
# "natural MODEL ages" = natural_treeage_scale, plus out_of_support model ages of a NATURAL tree
# (age_stratum not 'planted'; planted out_of_support cells stay out). age_domain is null for
# non-model ages, so both terms are model ages only.
IS_NATURAL_MODEL = ((pl.col("_age_domain") == "natural_treeage_scale")
                    | ((pl.col("_age_domain") == "out_of_support")
                       & (pl.col("_age_stratum") != "planted").fill_null(True))).fill_null(False)
IS_KNOWN_BIAS = pl.col("_age_domain").str.starts_with("known_bias_").fill_null(False)
AGE_META_EXPRS = [
    IS_NATURAL_MODEL.cast(pl.Int64).alias("n_trees_natural_treeage_scale"),
    IS_NATURAL_MODEL.cast(pl.Float64).alias("share_natural_treeage_scale"),
    pl.col("_age_source").alias("age_source_major"),
    IS_KNOWN_BIAS.cast(pl.Int64).alias("n_trees_known_bias"),
    IS_KNOWN_BIAS.cast(pl.Float64).alias("share_known_bias"),
]


# =============================================================================
# Step 1: live tree records + eco + species metadata
# =============================================================================

def load_base_data(con) -> pl.DataFrame:
    """
    One row per live tree × measurement visit.
    Joins: data_plot_eco (epa_l3, optionally epa_l4, ecosubcd), REF_SPECIES.
    """
    has_l4      = _has_column(con, "data_plot_eco", "epa_l4")
    epa_l4_expr = "e.epa_l4" if has_l4 else "NULL::VARCHAR AS epa_l4"
    # r7 D44 (C F6): ECOSUBCD lives in data_plot_eco (FIADB PLOT has no such column, so the old
    # PLOT lookup was 100 % NULL); data_plot_eco is dbconn's D28 view (latest visit's location)
    ecosubcd_col = "e.ecosubcd::VARCHAR AS ecosubcd"

    df = con.execute(f"""
        SELECT
            t.STATECD, t.UNITCD, t.COUNTYCD, t.PLOT, t.SUBP, t.TREE,
            t.spcd_resolved                        AS SPCD,   -- curation-fix: resolved species
            t.MEASDATE,
            t.DIA::DOUBLE                          AS dia,
            t.TPA_UNADJ::DOUBLE                    AS tpa,
            t.reconcile_new::INTEGER               AS reconcile_new,
            t.RECONCILECD::INTEGER                 AS rc_obs,
            t.age_source                           AS _age_source,     -- r3 I4
            t.age_domain                           AS _age_domain,
            t.age_stratum                          AS _age_stratum,    -- D46
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
# Step 2: disturbance events (r7 D41: ONE definition) + the subplot flag
# =============================================================================
# A DISTURBANCE DEATH is a death (tree_fate 'dead') with AGENTCD in DISTURBANCE_AGENTS
# (10 insect, 20 disease, 30 fire, 40 animal, 50 weather) OR a condition DSTRBCD1-3 at that
# record that is present and not 0 / 60 (60 = vegetation / competition, r3 M4). 70 unknown and
# 80 silvicultural are not disturbance agents (80 = harvest, classified upstream as a cut).
# The SAME definition drives:
#   * subp_has_dstrb (Sim A, Sim B, tree_trajectories: build_subp_has_dstrb),
#   * cohort_tracking's n_disturbance_dead (curated_cohorts),
#   * tree_trajectories' terminal_fate 'dead_disturbance',
#   * the Sim B drops (cohort_disturbance: a removal counts only at its event visit).
DISTURBANCE_AGENTS = (10, 20, 30, 40, 50)
NON_DISTURBANCE_DSTRBCD = (0, 60)
HARVEST_FATES = ("silvicultural_death", "harvest_inferred")


def disturbance_death_sql(alias: str = "") -> str:
    """SQL boolean: the record's death is a disturbance death (D41); `alias` = table alias."""
    a = f"{alias}." if alias else ""
    ag = ", ".join(map(str, DISTURBANCE_AGENTS))
    nd = ", ".join(map(str, NON_DISTURBANCE_DSTRBCD))
    return (f"(COALESCE({a}AGENTCD IN ({ag}), FALSE)"
            + "".join(f" OR COALESCE({a}DSTRBCD{i} NOT IN ({nd}), FALSE)" for i in (1, 2, 3)) + ")")


def disturbance_death_expr() -> pl.Expr:
    """polars twin of disturbance_death_sql (columns AGENTCD, DSTRBCD1-3)."""
    e = pl.col("AGENTCD").is_in(list(DISTURBANCE_AGENTS)).fill_null(False)
    for i in (1, 2, 3):
        c = pl.col(f"DSTRBCD{i}")
        e = e | (c.is_not_null() & ~c.is_in(list(NON_DISTURBANCE_DSTRBCD))).fill_null(False)
    return e


def disturbance_events(con, states=None) -> pl.DataFrame:
    """r7 D41 / D38: one row per tree with a disturbance REMOVAL event (TREE_ID + event_measdate,
    event_kind, first_record_dead). The event is
      * 'cut' / 'disturbance_death': the tree's FIRST dead / cut record (misrecorded deaths, D26,
        are not records of death; a first record that is a re-record, PREV_STATUS_CD = 2, is not
        an event: the death preceded the record's interval), with tree_fate silvicultural_death /
        harvest_inferred, or tree_fate 'dead' under the disturbance definition above. A cut on
        now-nonforest land is 'diverted_landuse' (r3 M6), not a cut event.
      * 'inferred_removal': final_tree_fate 'disappeared_inferred_removal' (alive at the last
        record, the subplot revisited without it; death_cause harvest_inferred) at the FIRST
        scaffold visit after the tree's last record.
    first_record_dead (D38, from curated_trees) is carried so consumers can exclude first-seen
    deaths from mortality and drops."""
    import dbconn
    dbconn.require_tables(con, "curated_trees", "clean_subplot_years")
    need = {"first_record_dead", "misrecorded_death", "rerecord_dead", "final_tree_fate"}
    miss = need - dbconn.column_names(con, "curated_trees")
    if miss:
        raise SystemExit(f"curated_trees lacks {sorted(miss)} (r7 D38 / D41 need them; rerun step 1)")
    ws = dbconn.state_filter(states, col="STATECD", prefix="AND")
    harv = ", ".join(f"'{f}'" for f in HARVEST_FATES)
    return con.execute(f"""
        WITH fd AS (
            SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE, MEASDATE, tree_fate,
                   COALESCE(rerecord_dead, FALSE) AS _rerec,
                   COALESCE(first_record_dead, FALSE) AS first_record_dead,
                   {disturbance_death_sql()} AS _dist
            FROM curated_trees
            WHERE STATUSCD IN (2, 3) AND NOT COALESCE(misrecorded_death, FALSE) {ws}
            QUALIFY row_number() OVER (PARTITION BY STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE
                                       ORDER BY MEASDATE) = 1),
        ev_dead AS (
            SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE, MEASDATE AS event_measdate,
                   CASE WHEN tree_fate IN ({harv}) THEN 'cut' ELSE 'disturbance_death' END AS event_kind,
                   first_record_dead
            FROM fd
            WHERE NOT _rerec AND (tree_fate IN ({harv}) OR (tree_fate = 'dead' AND _dist))),
        dis AS (
            SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE, max(MEASDATE) AS _last
            FROM curated_trees
            WHERE final_tree_fate = 'disappeared_inferred_removal' AND NOT COALESCE(misrecorded_death, FALSE) {ws}
            GROUP BY ALL),
        ev_dis AS (
            SELECT d.STATECD, d.UNITCD, d.COUNTYCD, d.PLOT, d.SUBP, d.TREE, min(v.MEASDATE) AS event_measdate,
                   'inferred_removal' AS event_kind, FALSE AS first_record_dead
            FROM dis d JOIN clean_subplot_years v
              ON v.STATECD = d.STATECD AND v.UNITCD = d.UNITCD AND v.COUNTYCD = d.COUNTYCD
             AND v.PLOT = d.PLOT AND v.SUBP = d.SUBP AND v.MEASDATE > d._last
            GROUP BY ALL)
        SELECT * FROM ev_dead UNION ALL SELECT * FROM ev_dis
        ORDER BY STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE
    """).pl()


def build_subp_has_dstrb(con, states=None) -> pl.DataFrame:
    """
    subp_has_dstrb (r7 D41) = True for a subplot with at least one disturbance event
    (disturbance_events: a cut, a disturbance death, or an inferred removal) dated AFTER the
    subplot's first scaffold visit. Trees without a live / aged record (no cohort) count too,
    so a tree first seen dead or cut after the first visit sets the flag (C F17); a tree
    already dead at the subplot's first visit is not an observed event.
    first_record_dead events are NOT excluded here: the flag marks disturbed subplots, it is
    not mortality attributed to an interval (D38 governs mortality and drops).

    One definition for Sim A, Sim B and tree_trajectories (it used to come from curated_cohorts'
    DSTRBCD-only deaths, while the drops used AGENTCD too, so 8-32 % of drop rows sat on
    "undisturbed" subplots, C F1). Conservative: once True it never resets, so all
    measurements of a disturbed subplot are excluded when Julia applies skip_disturbances=true.
    Returns SUBPLOT_ID + subp_has_dstrb for every scaffold subplot.
    """
    import dbconn
    ev = disturbance_events(con, states)
    ws = dbconn.state_filter(states, col="STATECD", prefix="WHERE")
    first = con.execute(f"""SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, min(MEASDATE) AS _first_visit
                            FROM clean_subplot_years {ws} GROUP BY ALL""").pl()
    flagged = (ev.join(first, on=SUBPLOT_ID, how="inner")
                 .filter(pl.col("event_measdate") > pl.col("_first_visit"))
                 .select(SUBPLOT_ID).unique().with_columns(pl.lit(True).alias("subp_has_dstrb")))
    subp_flag = (first.select(SUBPLOT_ID).join(flagged, on=SUBPLOT_ID, how="left")
                 .with_columns(pl.col("subp_has_dstrb").fill_null(False)))
    n_total = subp_flag.height
    n_dstrb = subp_flag.filter(pl.col("subp_has_dstrb")).height
    by_kind = dict(ev.group_by("event_kind").len().iter_rows())
    print(f"[step2] subp_has_dstrb (D41): {n_dstrb:,}/{n_total:,} subplots disturbed "
          f"({100 * n_dstrb / max(n_total, 1):.1f}%); events by kind {by_kind}, "
          f"{ev.filter(pl.col('first_record_dead')).height:,} on first_record_dead trees")
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

def sim_year_map(visits: pl.DataFrame, anchors: pl.DataFrame | None = None) -> pl.DataFrame:
    """
    Per-subplot integer time axis (r3 M2: STRICTLY increasing; r5 M-1: ONE axis per subplot).
      raw_i      = round((MEASDATE_i - anchor).days / 365.25), anchor = the subplot's FIRST
                   SCAFFOLD visit = min MEASDATE of `visits` (every caller passes the full
                   clean_subplot_years scaffold, so Sim A, Sim B and the trajectories share one
                   axis with sim_year 0 at that visit; `anchors` is ignored since r5)
      sim_year_i = max(raw_i, sim_year_{i-1} + 1) over the visits in date order
                 = i + cummax_j<=i (raw_j - j)
    Two visits that round to the same year (e.g. 0.4 yr apart) therefore get consecutive
    sim_years and ages advance between them. `visits`: SUBPLOT_ID + MEASDATE (the full
    scaffold when available, so phantom and real rows share the axis); `anchors`: SUBPLOT_ID +
    _first_measdate. Returns SUBPLOT_ID + MEASDATE + sim_year.
    """
    v = (visits.select(SUBPLOT_ID + ["MEASDATE"]).unique()
         .with_columns(pl.col("MEASDATE").min().over(SUBPLOT_ID).alias("_first_measdate"))
         .with_columns(((pl.col("MEASDATE") - pl.col("_first_measdate")).dt.total_days() / 365.25)
                       .round(0).cast(pl.Int32).alias("_raw"))
         .sort(SUBPLOT_ID + ["MEASDATE"])
         .with_columns(pl.int_range(pl.len()).over(SUBPLOT_ID).cast(pl.Int32).alias("_i")))
    return (v.with_columns((pl.col("_i") + (pl.col("_raw") - pl.col("_i")).cum_max().over(SUBPLOT_ID))
                           .cast(pl.Int32).alias("sim_year"))
             .select(SUBPLOT_ID + ["MEASDATE", "sim_year"]))


def compute_sim_year_ages(df: pl.DataFrame, visits: pl.DataFrame | None = None) -> pl.DataFrame:
    """
    Replace age_calc with a sim_year-anchored integer, eliminating floating-point
    drift across remeasurements.

      sim_year = strictly increasing integer years since the subplot's first row
                 (sim_year_map; on the full visit scaffold when `visits` is given)
      age_calc = round(estimated_age at tree debut) + (sim_year − debut_sim_year)

    All trees on the same subplot share the same integer sim_year steps, so cohort
    ages always have exact integer jumps and perfectly align with the simulation
    time axis regardless of how many remeasurements occur.
    """
    first_meas = (
        df.group_by(SUBPLOT_ID)
        .agg(pl.col("MEASDATE").min().alias("_first_measdate"))
    )
    allv = df.select(SUBPLOT_ID + ["MEASDATE"])
    if visits is not None:
        allv = pl.concat([allv, visits.select(SUBPLOT_ID + ["MEASDATE"])])
    smap = sim_year_map(allv, first_meas)
    df = df.drop([c for c in ("sim_year",) if c in df.columns]).join(smap, on=SUBPLOT_ID + ["MEASDATE"], how="left")

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

RATE_TABLES: dict = {}   # r6 R1: last computed rates per caller tag (written as tables by the stages)


def estimate_sapling_growth_rates(df: pl.DataFrame, basis: str, tag: str | None = None) -> tuple:
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
    # r5 M-8: the rate is a PER-TREE biomass rate. Sim A's agb is TPA-expanded and the expansion
    # drops ~12x at the 5 in microplot -> subplot switch (74.965 -> 6.018), which made many
    # growing saplings' rates negative (clipped to 0).
    # r6 R1: the caller states the basis of `agb` EXPLICITLY ('tpa_expanded': Sim A, per-tree =
    # agb / tpa; 'per_tree': the trajectories' drybio_g). r5 inferred it from a `tpa` column,
    # which the trajectories also carry, so their per-tree biomass was divided by TPA again.
    if basis == "tpa_expanded":
        bio = pl.col("agb") / pl.col("tpa")
    elif basis == "per_tree":
        bio = pl.col("agb")
    else:
        raise ValueError(f"estimate_sapling_growth_rates: basis must be 'tpa_expanded' or 'per_tree', not {basis!r}")
    srt = df.filter(pl.col("intro_type").is_null() if "intro_type" in df.columns else pl.lit(True)) \
            .with_columns(bio.alias("_bio")).sort(TREE_ID + ["MEASDATE"])

    first = (
        srt.unique(subset=TREE_ID, keep="first")
        .select(TREE_ID + [pl.col("age_calc").alias("first_age"),
                            pl.col("_bio").alias("first_agb"),
                            pl.col("dia").alias("first_dia")])
    )
    last = (
        srt.unique(subset=TREE_ID, keep="last")
        .select(TREE_ID + [pl.col("age_calc").alias("last_age"),
                            pl.col("_bio").alias("last_agb")])
    )
    meta = (
        df.select(TREE_ID + ["species_symbol", "spgrpcd", "epa_l3"])
        .sort(TREE_ID + ["species_symbol", "spgrpcd", "epa_l3"], nulls_last=True)
        .unique(subset=TREE_ID, keep="first")
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
    n_neg = tree_rates.filter(pl.col("last_agb") < pl.col("first_agb")).height
    print(f"[sapling rates] {n_sap:,} training saplings "
          f"(first_dia ≤ {SAPLING_DIA_MAX} in, ≥2 measurements; per-tree biomass; "
          f"{n_neg:,} with a negative raw rate, clipped to 0)")

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

    # r2 M2: float means depend on summation order across threads — round for determinism
    sp_rates, grp_rates, glb_rates = (r.with_columns(pl.col("rate").round(12))
                                      for r in (sp_rates, grp_rates, glb_rates))
    print(f"           {sp_rates.height:,} (species, epa_l3) pairs  "
          f"| {grp_rates.height:,} (spgrpcd, epa_l3) pairs computed")
    if tag is not None:
        glb_n = tree_rates.group_by("spgrpcd").len().rename({"len": "n_trees"})
        RATE_TABLES[tag] = pl.concat([
            sp_rates.select(pl.lit("species_l3").alias("level"), pl.col("species_symbol").cast(pl.Utf8).alias("species_symbol"),
                            pl.col("epa_l3").cast(pl.Utf8), pl.lit(None).cast(pl.Int64).alias("spgrpcd"), "rate",
                            pl.lit(None).cast(pl.UInt32).alias("n_trees")),
            grp_rates.select(pl.lit("spgrp_l3").alias("level"), pl.lit(None).cast(pl.Utf8).alias("species_symbol"),
                             pl.col("epa_l3").cast(pl.Utf8), pl.col("spgrpcd").cast(pl.Int64), "rate",
                             pl.lit(None).cast(pl.UInt32).alias("n_trees")),
            glb_rates.join(glb_n, on="spgrpcd", how="left").select(
                pl.lit("spgrp").alias("level"), pl.lit(None).cast(pl.Utf8).alias("species_symbol"),
                pl.lit(None).cast(pl.Utf8).alias("epa_l3"), pl.col("spgrpcd").cast(pl.Int64), "rate",
                pl.col("n_trees").cast(pl.UInt32)),
        ]).with_columns(pl.lit(basis).alias("basis")).sort(["level", "spgrpcd", "species_symbol", "epa_l3"], nulls_last=True)
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


# -----------------------------------------------------------------------------
# curation-fix 2026-10-26: phantom-backfill blocking rules
# -----------------------------------------------------------------------------
# A phantom pre-debut row at prior subplot visit p for a tree debuting at D is NOT made when
#   (a) the tree entered as RECONCILECD 6 (moved in) or 9 (previously nonsampled area): it
#       was not in the sampled population before;
#   (n) the subplot was entirely NONFOREST at p (forest SUBPCOND_PROP and MICRCOND_PROP
#       both 0 — COND_STATUS 2 / SUBP_STATUS 2 visits; audit I2) — or at any visit between
#       p and D: backfill stops at the latest nonforest prior visit, so it stays contiguous;
#   (b) a PROCEDURAL BREAK lies in p < b <= D: the PLOT had STATUSCD=0 + RECONCILECD=8
#       closures at b (table procedural_breaks). Keyed per plot (audit B2): the AL/GA
#       MANUAL 1.5-2.0 sapling re-tally closed the old saplings RC8 on one subplot and
#       re-tallied them as RC3/RC8 on others. Applies to SAPLING debuts (DIA < 5 in) and to
#       RC8 entries of any size (audit M1: a RC8 entry is a re-tally only where it pairs with
#       closures; unpaired RC8 entries — e.g. FL MANUAL 9.0 species-definition additions —
#       were present but untallied and keep their backfill, like missed trees);
#   (c) [cohort tables only — not per-tree trajectories, audit M3] the tree debuted on the
#       SUBPLOT (DIA >= 5 in, TPA ~6.018) as true ingrowth (RECONCILECD 1 / null), so at the
#       earlier visits it was sapling-sized and that class is represented by the microplot
#       saplings (TPA ~74.965) wherever the microplot was sampled on forest — scaling the
#       subplot debut back at TPA 6.018 double counts it. Backfill stops at the latest prior
#       visit with a sampled forest microplot. Exempt: RC2 through-growth (a seedling at the
#       previous visit, which the microplot tree tally does not represent; audit M2) and
#       RC3/RC7/RC8 entries (present but untallied).
BF_NONE_CODES   = (6, 9)
BF_C_EXEMPT     = (2, 3, 7, 8)      # first-record RECONCILECD exempt from rule (c)
RETALLY_DIA_TOL = 2.0               # in: RC3 debut vs RC7/8 closure PREVDIA (re-tally pairing)


def load_backfill_context(con) -> dict:
    """Blocking context, all from the curated DB:
      breaks  plot-level procedural re-tally dates (procedural_breaks: break_kind,
              n_closed_rc8_prev_sapling, n_unrecorded_saplings)
      visit   the FULL subplot-visit scaffold (clean_subplot_years) with forest subplot /
              microplot proportions — candidate prior visits AND the nonforest lookup
              (r2 M1: visits with no live tree are not in the cohort rows, so a nonforest
              visit between p and D was invisible before)
      censor  every censoring event (tree_censor_events) for the cohort-pass gap members"""
    import dbconn
    dbconn.require_tables(con, "procedural_breaks", "clean_subplot_years", "tree_censor_events")
    breaks = con.execute("""
        SELECT STATECD, UNITCD, COUNTYCD, PLOT, MEASDATE AS break_date,
               bool_or(break_kind = 'rc8') AS _brk_rc8,
               bool_or(n_closed_rc8_prev_sapling > 0 OR break_kind = 'unrecorded'
                       OR n_unrecorded_saplings > 0) AS _brk_sap
        FROM procedural_breaks GROUP BY ALL
    """).pl()
    visit = con.execute("""
        SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, MEASDATE AS prior_measdate,
               MAX(COALESCE(forest_micrcond_prop, 0)) AS _micro_prop,
               MAX(COALESCE(forest_subpcond_prop, 0) + COALESCE(forest_micrcond_prop, 0)) AS _forest_prop
        FROM clean_subplot_years GROUP BY ALL
    """).pl()
    censor = con.execute("""
        SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE, MEASDATE AS _cdate
        FROM tree_censor_events""").pl()
    # dead/cut records of every tree (r3: a tree recorded dead BEFORE its live debut — a data
    # error — gets no phantom at or before that dead record)
    dead = con.execute("""
        SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE, MEASDATE AS _dd
        FROM curated_trees WHERE STATUSCD IN (2, 3) AND NOT COALESCE(misrecorded_death, FALSE)  -- r6 R4
    """).pl()
    # r3: RC3 (missed live) >= 5 in debuts that pair with a same-subplot, same-species RC7/RC8
    # closure of similar size (|PREVDIA - DIA| <= 2 in) at the debut visit are the SAME physical
    # tree re-tallied under a new number: its old records already represent it at the prior
    # visits, so it gets no backfill (treated like an RC6/8/9 entry)
    retally = con.execute(f"""
        WITH deb AS (
            SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE, min(MEASDATE) AS D,
                   arg_min(DIA, MEASDATE) AS ddia, arg_min(CAST(RECONCILECD AS INTEGER), MEASDATE) AS drc,
                   any_value(spcd_resolved) AS sp
            FROM curated_trees WHERE STATUSCD = 1 GROUP BY ALL),
        clo AS (
            SELECT e.STATECD, e.UNITCD, e.COUNTYCD, e.PLOT, e.SUBP, e.MEASDATE, e.PREVDIA, T.SPCD AS sp
            FROM tree_status0_events e
            JOIN TREE T ON T.STATECD=e.STATECD AND T.UNITCD=e.UNITCD AND T.COUNTYCD=e.COUNTYCD
                       AND T.PLOT=e.PLOT AND T.SUBP=e.SUBP AND T.TREE=e.TREE AND T.INVYR=e.INVYR
            WHERE e.RC IN (7, 8))
        SELECT DISTINCT d.STATECD, d.UNITCD, d.COUNTYCD, d.PLOT, d.SUBP, d.TREE
        FROM deb d JOIN clo c ON c.STATECD=d.STATECD AND c.UNITCD=d.UNITCD AND c.COUNTYCD=d.COUNTYCD
             AND c.PLOT=d.PLOT AND c.SUBP=d.SUBP AND c.MEASDATE=d.D AND c.sp=d.sp
             AND abs(c.PREVDIA - d.ddia) <= {RETALLY_DIA_TOL}
        WHERE d.drc = 3 AND d.ddia >= {SAPLING_DIA_MAX}""").pl().with_columns(pl.lit(True).alias("_retally_rc3"))
    print(f"[backfill ctx] {retally.height:,} RC3 >= 5 in re-tally entries (paired with an RC7/8 closure)")
    print(f"[backfill ctx] {breaks.height:,} plot-visit procedural-break dates "
          f"({breaks.filter(pl.col('_brk_sap')).height:,} sapling re-tallies), "
          f"{visit.height:,} scaffold subplot-visits")
    return {"breaks": breaks, "visit": visit, "censor": censor, "dead": dead, "retally": retally}


def add_backfill_flags(df: pl.DataFrame, ctx=None) -> pl.DataFrame:
    """Per-row tree-level flags used by the blocking rules (debut = first REAL row of the
    tree). Needs dia, reconcile_new and rc_obs (RECONCILECD)."""
    real = df.filter(pl.col("intro_type").is_null()) if "intro_type" in df.columns else df
    debut = (real.sort(TREE_ID + ["MEASDATE"]).unique(subset=TREE_ID, keep="first")
             .select(TREE_ID + [pl.col("dia").alias("_debut_dia"),
                                pl.col("rc_obs").alias("_debut_rc")]))
    df = df.drop([c for c in BF_FLAGS if c in df.columns]).join(debut, on=TREE_ID, how="left")
    if ctx is not None and "retally" in ctx:
        df = df.join(ctx["retally"], on=TREE_ID, how="left")
    else:
        df = df.with_columns(pl.lit(None, dtype=pl.Boolean).alias("_retally_rc3"))
    rn = pl.col("reconcile_new")
    return df.with_columns([
        (rn.is_in(list(BF_NONE_CODES)).fill_null(False) | pl.col("_retally_rc3").fill_null(False))
        .alias("_bf_none"),
        (rn == 8).fill_null(False).alias("_bf_rc8"),
        (pl.col("_debut_dia") < SAPLING_DIA_MAX).fill_null(False).alias("_bf_sap"),
        ((pl.col("_debut_dia") >= SAPLING_DIA_MAX) &
         ~pl.col("_debut_rc").is_in(list(BF_C_EXEMPT)).fill_null(False)).fill_null(False)
        .alias("_bf_ge5_ingrowth"),
    ]).drop(["_debut_dia", "_debut_rc", "_retally_rc3"])


BF_FLAGS = ["_bf_none", "_bf_rc8", "_bf_sap", "_bf_ge5_ingrowth"]


def scaffold_visits(df: pl.DataFrame, ctx) -> pl.DataFrame:
    """Candidate prior visits: the full scaffold (ctx['visit']) restricted to the subplots in
    df, with prior_sim_year on the SAME strict per-subplot axis as compute_sim_year_ages
    (sim_year_map anchored at the subplot's first scaffold visit). Without ctx:
    df's own visits."""
    if ctx is None:
        return (df.select(SUBPLOT_ID + ["MEASDATE", "sim_year"]).unique()
                .rename({"MEASDATE": "prior_measdate", "sim_year": "prior_sim_year"}))
    real = df.filter(pl.col("intro_type").is_null()) if "intro_type" in df.columns else df
    first = real.group_by(SUBPLOT_ID).agg(pl.col("MEASDATE").min().alias("_first_measdate"))
    allv = pl.concat([real.select(SUBPLOT_ID + ["MEASDATE"]),
                      ctx["visit"].select(SUBPLOT_ID + [pl.col("prior_measdate").alias("MEASDATE")])])
    return (sim_year_map(allv, first)
            .rename({"MEASDATE": "prior_measdate", "sim_year": "prior_sim_year"}))


def nonforest_for(is_sap: pl.Expr, fill: bool = False) -> pl.Expr:
    """r7 C F4: the (n) nonforest test of a visit for a tree: sapling debuts (microplot) look at
    the microplot forest proportion only, other trees at subplot + microplot. `fill`: a visit
    missing from the scaffold counts as nonforest."""
    micro, comb = pl.col("_micro_prop"), pl.col("_forest_prop")
    if fill:
        micro, comb = micro.fill_null(0), comb.fill_null(0)
    return pl.when(is_sap).then(micro <= 0).otherwise(comb <= 0)


def apply_backfill_blocks(prior: pl.DataFrame, ctx, key, rule_c: bool = True) -> pl.DataFrame:
    """Drop candidate phantom slots per the rules above. `prior` carries SUBPLOT_ID, the
    debut MEASDATE (D), prior_measdate and the BF_FLAGS; `key` identifies a slot and
    key minus prior_measdate identifies the source tree (the owner)."""
    if ctx is None:
        return prior
    breaks, visit = ctx["breaks"], ctx["visit"]
    owner = [k for k in key if k != "prior_measdate"]
    n0 = prior.height
    prior = prior.filter(~pl.col("_bf_none"))
    n_none = n0 - prior.height

    # (n) nonforest: latest entirely-nonforest scaffold visit before D (full scaffold, M1).
    # r7 D44 (C F4): a SAPLING debut (tallied on the microplot) tests the MICROPLOT forest
    # proportion alone (a microplot sapling cannot have been a forest tree where the microplot
    # was entirely nonforest, even if the subplot was not); other trees test subplot + microplot.
    nf = visit.select(SUBPLOT_ID + [pl.col("prior_measdate").alias("_nfd"), "_forest_prop", "_micro_prop"])
    last_nf = (prior.select(list(dict.fromkeys(owner + ["MEASDATE", "_bf_sap"]))).unique()
               .join(nf, on=SUBPLOT_ID, how="inner")
               .filter((pl.col("_nfd") < pl.col("MEASDATE")) & nonforest_for(pl.col("_bf_sap")))
               .group_by(owner).agg(pl.col("_nfd").max().alias("_last_nf")))
    prior = prior.join(last_nf, on=owner, how="left")
    n1 = prior.height
    prior = prior.filter(pl.col("_last_nf").is_null() | (pl.col("prior_measdate") > pl.col("_last_nf")))
    n_nf = n1 - prior.height
    # (z) a dead/cut record of the tree before its live debut: no phantom at or before it
    n_z = 0
    if "dead" in ctx and set(TREE_ID) <= set(owner):
        last_dead = (prior.select(TREE_ID + ["MEASDATE"]).unique()
                     .join(ctx["dead"], on=TREE_ID, how="inner")
                     .filter(pl.col("_dd") < pl.col("MEASDATE"))
                     .group_by(TREE_ID).agg(pl.col("_dd").max().alias("_last_dd")))
        n2z = prior.height
        prior = (prior.join(last_dead, on=TREE_ID, how="left")
                 .filter(pl.col("_last_dd").is_null() | (pl.col("prior_measdate") > pl.col("_last_dd")))
                 .drop("_last_dd"))
        n_z = n2z - prior.height
    prior = prior.join(visit, on=SUBPLOT_ID + ["prior_measdate"], how="left")

    # (b) plot-level procedural break in (p, D]: the SAPLING part only for sapling re-tally
    # breaks (closed RC8 saplings, or unrecorded saplings: r2 M9), the RC8-entry part for
    # RC8 closure breaks
    cand = prior.select(list(dict.fromkeys(key + ["MEASDATE", "_bf_sap", "_bf_rc8"]))).unique()
    crossed = (
        cand.join(breaks, on=PLOT_KEY, how="inner")
        .filter((pl.col("break_date") > pl.col("prior_measdate")) &
                (pl.col("break_date") <= pl.col("MEASDATE")) &
                ((pl.col("_bf_sap") & pl.col("_brk_sap")) | (pl.col("_bf_rc8") & pl.col("_brk_rc8"))))
        .select(key).unique().with_columns(pl.lit(True).alias("_crossed"))
    )
    n2 = prior.height
    prior = prior.join(crossed, on=key, how="left").filter(pl.col("_crossed").is_null()).drop("_crossed")
    n_break = n2 - prior.height

    # (c) >= 5 in subplot ingrowth where the microplot represents the sapling class
    n_c = 0
    if rule_c:
        prior = prior.with_columns(
            pl.when(pl.col("_bf_ge5_ingrowth") & (pl.col("_micro_prop").fill_null(0) > 0))
            .then(pl.col("prior_measdate")).otherwise(pl.lit(None, dtype=pl.Date))
            .max().over(owner).alias("_last_micro"))
        n3 = prior.height
        prior = prior.filter(pl.col("_last_micro").is_null() |
                             (pl.col("prior_measdate") > pl.col("_last_micro"))).drop("_last_micro")
        n_c = n3 - prior.height
    print(f"[backfill blocks] of {n0:,} candidate slots dropped: {n_none:,} RC6/9 entry, "
          f"{n_nf:,} at/before a nonforest visit, {n_z:,} at/before a pre-debut dead record, "
          f"{n_break:,} across a plot procedural break, "
          f"{n_c:,} >=5in ingrowth/microplot" + ("" if rule_c else " (rule c off)"))
    return prior.drop(["_micro_prop", "_forest_prop", "_last_nf"])


def tree_prior_candidates(df: pl.DataFrame, ctx, rule_c: bool = True) -> pl.DataFrame:
    """Allowed tree-level phantom slots: every REAL tree debut x every earlier scaffold visit
    with prior_age >= 1, minus the blocked ones. One row per (tree, prior_measdate)."""
    real = df.filter(pl.col("intro_type").is_null()) if "intro_type" in df.columns else df
    tree_debut = real.sort(TREE_ID + ["MEASDATE"]).unique(subset=TREE_ID, keep="first")
    prior = (
        tree_debut
        .join(scaffold_visits(df, ctx), on=SUBPLOT_ID, how="inner")
        .filter(pl.col("prior_measdate") < pl.col("MEASDATE"))
        .with_columns(
            (pl.col("age_calc") + pl.col("prior_sim_year") - pl.col("sim_year"))
            .cast(pl.Int32).alias("prior_age")
        )
        .filter(pl.col("prior_age") >= 1)
    )
    return apply_backfill_blocks(prior, ctx, TREE_ID + ["prior_measdate"], rule_c=rule_c)


def report_phantom_floor(missing: pl.DataFrame, col: str, floor: float, unit: str, tag: str) -> None:
    """r7 C F5: the floor log counts the two causes separately (r6 printed every floored row as
    'no sapling rate'): no rate for the tree's species / group, or the decay reached the floor."""
    at = pl.col(col) == floor
    n_nr = missing.filter(at & pl.col("_no_rate")).height
    n_dec = missing.filter(at & ~pl.col("_no_rate")).height
    print(f"[{tag}] phantom floor {floor} {unit}: {n_nr:,} slots without a sapling rate, "
          f"{n_dec:,} slots decayed to the floor (of {missing.height:,})")


def _phantom_agb(missing: pl.DataFrame, df: pl.DataFrame, tag: str) -> pl.DataFrame:
    """prior_agb = min(agb, max(MIN_BACKFILL_AGB, agb * (1 - rate * years_back)))."""
    # Sim A agb is TPA-expanded (g/m2); the rates are recorded once (tree pass) as sapling_growth_rates
    sp_rates, grp_rates, glb_rates = estimate_sapling_growth_rates(
        df, basis="tpa_expanded", tag="simA" if tag == "backfill trees" else None)
    missing = (
        missing
        .join(sp_rates.rename({"rate": "_r_sp"}),  on=["species_symbol", "epa_l3"], how="left")
        .join(grp_rates.rename({"rate": "_r_grp"}), on=["spgrpcd",        "epa_l3"], how="left")
        .join(glb_rates.rename({"rate": "_r_glb"}), on="spgrpcd",                    how="left")
        .with_columns(pl.coalesce(["_r_sp", "_r_grp", "_r_glb"]).alias("_rate"))
        .drop(["_r_sp", "_r_grp", "_r_glb"])
        .with_columns(pl.col("_rate").is_null().alias("_no_rate"))
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
            # r5 M-8: a phantom never exceeds the debut (first real) value it is scaled from
            .clip(upper_bound=pl.col("agb"))
            .alias("prior_agb")
        )
        .drop("_rate")
    )
    report_phantom_floor(missing, "prior_agb", MIN_BACKFILL_AGB, "g/m2", tag)
    return missing.drop("_no_rate")


def _to_synthetic(missing: pl.DataFrame) -> pl.DataFrame:
    return (
        missing
        .drop(["MEASDATE", "age_calc", "agb", "sim_year"])
        .rename({"prior_measdate": "MEASDATE", "prior_age": "age_calc",
                 "prior_sim_year": "sim_year", "prior_agb": "agb"})
        .with_columns(pl.lit("phantom_backfill").cast(pl.Utf8).alias("intro_type"))
    )


def backfill_phantom_trees(df: pl.DataFrame, ctx=None) -> pl.DataFrame:
    """
    For every individual tree (TREE_ID), ensure each prior subplot visit (full scaffold) where
    that tree would have been alive, and that is not blocked, has a synthetic row
    (intro_type='phantom_backfill'):
        prior_age = debut_age + (prior_sim_year - debut_sim_year)
        biomass   = max(MIN_BACKFILL_AGB, agb * (1 - rate * years_back))
    """
    if "intro_type" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("intro_type"))
    prior = tree_prior_candidates(df, ctx)
    existing = (df.select(TREE_ID + ["MEASDATE"]).unique()
                .rename({"MEASDATE": "prior_measdate"}).with_columns(pl.lit(True).alias("_exists")))
    missing = (prior.join(existing, on=TREE_ID + ["prior_measdate"], how="left")
               .filter(pl.col("_exists").is_null()).drop("_exists")
               .sort(TREE_ID + ["prior_measdate"]))
    n_plots = missing.select(PLOT_KEY).unique().height if missing.height else 0
    print(f"[backfill trees] {missing.height:,} missing tree-slots across {n_plots:,} plots")
    if missing.height == 0:
        return df
    global PHANTOM_TREE_SOURCES
    PHANTOM_TREE_SOURCES = missing.select(
        TREE_ID + ["prior_measdate", pl.col("MEASDATE").alias("debut_measdate"),
                   "species_symbol", "prior_age"])
    synthetic = _to_synthetic(_phantom_agb(missing, df, "backfill trees"))
    print(f"[backfill trees] generated {synthetic.height:,} synthetic rows")
    return pl.concat([df, synthetic.select(df.columns)], how="vertical")


def backfill_phantom_cohorts(df: pl.DataFrame, ctx=None):
    """
    Second-pass backfill at the cohort level (subplot × species × measdate × age_calc).

    r2 B1: blocks are evaluated PER MEMBER. A cohort slot (ckey at visit t, age a) gets a
    phantom at an earlier visit p (age a - (t - p)) only if at least one MEMBER tree of
    that cohort slot survives for that same p:
      * p before the member's real debut D_m: (member, p) is an allowed tree-level slot
        (tree_prior_candidates: the member's own debut, flags and blocks), or
      * p after D_m (the member was missed at p): p is a forest visit and the member has no
        censoring event in (p, t].
    The representative row is the surviving member with the smallest TREE_ID.
    Returns (df, sources) where sources = phantom_cohort_sources (one row per created slot
    with its surviving member tree ids).
    """
    ckey = SUBPLOT_ID + ["species_symbol", "MEASDATE", "age_calc"]
    if "intro_type" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("intro_type"))
    empty_src = pl.DataFrame(schema={**{k: pl.Int64 for k in SUBPLOT_ID}, "species_symbol": pl.Utf8,
                                     "src_measdate": pl.Date, "src_age_calc": pl.Int32,
                                     "prior_measdate": pl.Date, "prior_age": pl.Int32,
                                     "n_members": pl.UInt32, "member_trees": pl.Utf8})
    visits = scaffold_visits(df, ctx)
    slots = (df.select(ckey + ["sim_year"]).unique()
             .join(visits, on=SUBPLOT_ID, how="inner")
             .filter(pl.col("prior_measdate") < pl.col("MEASDATE"))
             .with_columns((pl.col("age_calc") + pl.col("prior_sim_year") - pl.col("sim_year"))
                           .cast(pl.Int32).alias("prior_age"))
             .filter(pl.col("prior_age") >= 1))
    existing = (df.select(SUBPLOT_ID + ["species_symbol", "MEASDATE", "age_calc"]).unique()
                .rename({"MEASDATE": "prior_measdate", "age_calc": "prior_age"})
                .with_columns(pl.lit(True).alias("_exists")))
    slots = (slots.join(existing, on=SUBPLOT_ID + ["species_symbol", "prior_measdate", "prior_age"], how="left")
             .filter(pl.col("_exists").is_null()).drop("_exists"))
    if slots.height == 0:
        print("[backfill cohorts] 0 additional cohort-slots")
        return df, empty_src

    # members of each source slot, with their own real debut
    real = df.filter(pl.col("intro_type").is_null())
    debut = real.group_by(TREE_ID).agg(pl.col("MEASDATE").min().alias("_D"))
    members = (df.select(ckey + TREE_ID[-1:]).unique()      # TREE (SUBPLOT_ID already in ckey)
               .join(debut, on=TREE_ID, how="left"))
    cand = slots.join(members, on=ckey, how="inner")
    allowed = (tree_prior_candidates(df, ctx).select(TREE_ID + ["prior_measdate"]).unique()
               .with_columns(pl.lit(True).alias("_ok_pre")) if ctx is not None else
               cand.select(TREE_ID + ["prior_measdate"]).unique().with_columns(pl.lit(True).alias("_ok_pre")))
    cand = cand.join(allowed, on=TREE_ID + ["prior_measdate"], how="left")
    if ctx is not None:
        cen = (cand.select(TREE_ID + ["prior_measdate", "MEASDATE"]).unique()
               .join(ctx["censor"], on=TREE_ID, how="inner")
               .filter((pl.col("_cdate") > pl.col("prior_measdate")) & (pl.col("_cdate") <= pl.col("MEASDATE")))
               .select(TREE_ID + ["prior_measdate", "MEASDATE"]).unique()
               .with_columns(pl.lit(True).alias("_cen")))
        forest = ctx["visit"].select(SUBPLOT_ID + ["prior_measdate", "_forest_prop", "_micro_prop"])
        sap = df.select(TREE_ID + ["_bf_sap"]).sort(TREE_ID + ["_bf_sap"]).unique(subset=TREE_ID, keep="first") if "_bf_sap" in df.columns \
            else df.select(TREE_ID).unique().with_columns(pl.lit(False).alias("_bf_sap"))
        dd = (cand.select(TREE_ID + ["prior_measdate", "MEASDATE"]).unique()
              .join(ctx["dead"], on=TREE_ID, how="inner")
              .filter((pl.col("_dd") >= pl.col("prior_measdate")) & (pl.col("_dd") <= pl.col("MEASDATE")))
              .select(TREE_ID + ["prior_measdate", "MEASDATE"]).unique()
              .with_columns(pl.lit(True).alias("_dead_between")))
        cand = (cand.join(cen, on=TREE_ID + ["prior_measdate", "MEASDATE"], how="left")
                .join(dd, on=TREE_ID + ["prior_measdate", "MEASDATE"], how="left")
                .join(forest, on=SUBPLOT_ID + ["prior_measdate"], how="left")
                .join(sap, on=TREE_ID, how="left"))
        # r7 C F4: a sapling member needs a forest MICROPLOT at p (same rule as (n))
        gap_ok = ((pl.col("prior_measdate") > pl.col("_D")) & pl.col("_cen").is_null()
                  & pl.col("_dead_between").is_null()
                  & ~nonforest_for(pl.col("_bf_sap").fill_null(False), fill=True))
    else:
        gap_ok = pl.lit(True)
    surv = cand.filter(
        ((pl.col("prior_measdate") < pl.col("_D")) & pl.col("_ok_pre").fill_null(False)) | gap_ok)
    n_cand_slots = slots.select(ckey + ["prior_measdate"]).unique().height
    slot_key = SUBPLOT_ID + ["species_symbol", "prior_measdate", "prior_age"]
    # one phantom per target slot; representative = smallest surviving member (deterministic)
    rep = (surv.sort(slot_key + ["MEASDATE", "TREE"])
           .unique(subset=slot_key, keep="first")
           .select(ckey + ["TREE", "prior_measdate", "prior_age", "prior_sim_year"]))
    sources = (surv.sort(slot_key + ["MEASDATE", "TREE"])
               .group_by(slot_key, maintain_order=True)
               .agg(pl.col("MEASDATE").first().alias("src_measdate"),
                    pl.col("age_calc").first().alias("src_age_calc"),
                    pl.col("TREE").n_unique().cast(pl.UInt32).alias("n_members"),
                    pl.col("TREE").unique().sort().cast(pl.Utf8).str.join(",").alias("member_trees"))
               .select(empty_src.columns))
    print(f"[backfill cohorts] {n_cand_slots:,} candidate cohort-slots, "
          f"{rep.height:,} created (a member survives its own blocks), "
          f"{n_cand_slots - rep.height:,} blocked (every member individually blocked)")
    if rep.height == 0:
        return df, empty_src
    src_rows = df.sort(ckey + ["TREE", "intro_type"], nulls_last=False).unique(subset=ckey + ["TREE"], keep="first")
    missing = rep.join(src_rows, on=ckey + ["TREE"], how="inner")
    synthetic = _to_synthetic(_phantom_agb(missing, df, "backfill cohorts"))
    print(f"[backfill cohorts] generated {synthetic.height:,} additional synthetic rows")
    return pl.concat([df, synthetic.select(df.columns)], how="vertical"), sources


def set_visit_flag(con, table: str, col: str, visits_sql: str, what: str) -> None:
    """Set a per-(plot, measdate) BOOL `col` on a cohort table: TRUE on rows whose plot visit is in
    `visits_sql` (STATECD, UNITCD, COUNTYCD, PLOT, MEASDATE). An existing column is replaced IN
    PLACE (its position kept, so step reruns never reorder the schema: r7 D F9); a new one is
    APPENDED as the last column, so readers of the old schema are unaffected."""
    import dbconn
    have = dbconn.column_names(con, table)
    flag = f"(b.PLOT IS NOT NULL) AS {col}"
    sel = f"c.* REPLACE ({flag})" if col.lower() in have else f"c.*, {flag}"
    con.execute(f"""
        CREATE OR REPLACE TABLE {table} AS
        SELECT {sel}
        FROM {table} c
        LEFT JOIN (SELECT DISTINCT STATECD, UNITCD, COUNTYCD, PLOT, MEASDATE FROM ({visits_sql})) b
          ON b.STATECD=c.statecd AND b.UNITCD=c.unitcd AND b.COUNTYCD=c.countycd
         AND b.PLOT=c.plot AND b.MEASDATE=c.measdate
    """)
    n = con.execute(f"SELECT count(*) FILTER (WHERE {col}), count(*) FROM {table}").fetchone()
    print(f"[{col}] {table}: {n[0]:,}/{n[1]:,} rows at {what}")


def add_procedural_break(con, table: str) -> None:
    """`procedural_break` (BOOL): TRUE on rows whose (plot, measdate) is a procedural re-tally
    visit (procedural_breaks), so consumers can tell a re-tally from cohort death +
    establishment (audit I4)."""
    import dbconn
    dbconn.require_tables(con, "procedural_breaks", table)
    set_visit_flag(con, table, "procedural_break", "SELECT * FROM procedural_breaks",
                   "a procedural re-tally visit")


def add_rc7_withdrawn_flag(con, table: str) -> None:
    """r7 D39: `all_live_withdrawn_rc7` (BOOL): TRUE on rows at a plot visit whose live trees FIA
    all withdrew as erroneously tallied (RC7; the flag comes from curated_trees). Such a visit
    keeps its dead / cut tally and its scaffold row, so its cohort rows are phantoms only; the
    visit is NOT dropped (consumers decide, e.g. genveg treats it as incomplete)."""
    import dbconn
    dbconn.require_tables(con, "curated_trees", table)
    if "all_live_withdrawn_rc7" not in dbconn.column_names(con, "curated_trees"):
        raise SystemExit("curated_trees lacks all_live_withdrawn_rc7 (r7 D39; rerun step 1)")
    src = "SELECT STATECD, UNITCD, COUNTYCD, PLOT, MEASDATE FROM curated_trees WHERE all_live_withdrawn_rc7"
    if dbconn.table_exists(con, "rc7_withdrawn_visits"):   # step 1's visit list: also visits left with no record
        src += " UNION SELECT STATECD, UNITCD, COUNTYCD, PLOT, MEASDATE FROM rc7_withdrawn_visits"
    set_visit_flag(con, table, "all_live_withdrawn_rc7", src,
                   "a visit whose live trees were all withdrawn (RC7)")


def add_visit_flags(con, table: str) -> None:
    """The cohort tables' visit metadata, in schema order: procedural_break, then (r7)
    all_live_withdrawn_rc7."""
    add_procedural_break(con, table)
    add_rc7_withdrawn_flag(con, table)


def plot_land_use_sql(con) -> str:
    """r7 D44 (C F10) / D43: THE 2-way land_use per plot, for every table (Sim A, _stdorg, Sim B,
    tree_trajectories): 'artificial' iff any condition of the plot's PRODUCT visits has
    STDORGCD = 1, else 'natural' (plots without any STDORGCD get no row -> NULL land_use).
    Only the COND rows whose PLT_CN is a visit of the curated product count (`product_visits`,
    step 1; the same visit set export_reference_tables copies): a periodic plot that reused the
    key (KY 1986-88) no longer flips natural plots to artificial (D43).
    Columns: statecd, unitcd, countycd, plot, land_use."""
    # the literal state list lets the scan of the (FIADB-wide) COND view prune by STATECD
    st = ",".join(str(int(r[0])) for r in con.execute(
        "SELECT DISTINCT STATECD FROM product_visits ORDER BY 1").fetchall()) or "NULL"
    return f"""
        SELECT c.STATECD AS statecd, c.UNITCD AS unitcd, c.COUNTYCD AS countycd, c.PLOT AS plot,
               CASE WHEN MAX(c.STDORGCD) = 1 THEN 'artificial' ELSE 'natural' END AS land_use
        FROM COND c
        WHERE c.STATECD IN ({st}) AND c.STDORGCD IS NOT NULL
          AND CAST(c.PLT_CN AS VARCHAR) IN (SELECT PLT_CN FROM product_visits)
        GROUP BY ALL"""


def record_rate_scope(con) -> None:
    """r7 D44 (C F13): the sapling growth rates pool every tree in the DB (species x L3, then
    species group x L3, then species group), so phantom biomass depends on the run's state set;
    record that pool in run_scope.sapling_rate_scope (= the STATECDs [and county subset] of the
    run, i.e. run_scope itself)."""
    import dbconn
    if not dbconn.table_exists(con, "run_scope"):
        raise SystemExit("run_scope missing (written by step 1)")
    st = ",".join(str(r[0]) for r in con.execute(
        "SELECT DISTINCT STATECD FROM curated_trees ORDER BY 1").fetchall())
    con.execute("ALTER TABLE run_scope ADD COLUMN IF NOT EXISTS sapling_rate_scope VARCHAR")
    con.execute("UPDATE run_scope SET sapling_rate_scope = ? || COALESCE('|counties=' || CAST(counties AS VARCHAR), '')",
                [f"pooled over all trees of states {st}"])
    print("[rate scope]", con.execute("SELECT sapling_rate_scope FROM run_scope").fetchall())


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

def build_cohorts_landis(con, backfill_blocks: bool = True) -> pl.DataFrame:
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
    ctx = load_backfill_context(con) if backfill_blocks else None
    scaffold = (ctx["visit"].select(SUBPLOT_ID + [pl.col("prior_measdate").alias("MEASDATE")])
                if ctx is not None else None)
    df = compute_sim_year_ages(df, scaffold)

    # Diagnostic: mid-measurement gaps (tracked but not fixed here)
    print_mid_measurement_gaps(df)

    # Backfill: tree level first, then cohort level to catch any remaining gaps
    df = add_backfill_flags(df.with_columns(pl.lit(None).cast(pl.Utf8).alias("intro_type")), ctx)
    df = backfill_phantom_trees(df, ctx)
    df, sources = backfill_phantom_cohorts(df, ctx)
    global PHANTOM_COHORT_SOURCES
    PHANTOM_COHORT_SOURCES = sources
    # r5 M-1: no re-base — the axis is anchored at the subplot's first scaffold visit
    # (sim_year_map), so sim_year >= 0 already and equals Sim B's / the trajectories'

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
        *AGE_META_EXPRS,
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

    # r2 M2: deterministic row order
    return df.sort(["statecd", "unitcd", "countycd", "plot", "subp", "species_symbol",
                    "measdate", "age_calc", "intro_type", "agb"], nulls_last=False)


PHANTOM_COHORT_SOURCES = None
PHANTOM_TREE_SOURCES = None


# =============================================================================
# Standalone entry point
# =============================================================================

if __name__ == "__main__":
    # curation-fix 2026-10-26: explicit --out-db / --fiadb (dbconn); no default DB path.
    import argparse
    import dbconn
    a = dbconn.add_db_args(argparse.ArgumentParser(description="curated_cohorts_landis")).parse_args()
    dbconn.check_args(a)
    con = dbconn.connect(a.out_db, a.fiadb)
    dbconn.require_tables(con, "curated_trees", "clean_subplot_years")
    print(f"DB: {a.out_db}")

    cohorts = build_cohorts_landis(con)

    con.register("_landis_view", cohorts)
    con.execute(
        "CREATE OR REPLACE TABLE curated_cohorts_landis AS SELECT * FROM _landis_view"
    )
    con.unregister("_landis_view")
    # diagnostics: the source tree(s) of every phantom row (tree pass / cohort pass)
    src = PHANTOM_COHORT_SOURCES
    con.register("_src_view", src.to_arrow())
    con.execute("CREATE OR REPLACE TABLE phantom_cohort_sources AS SELECT * FROM _src_view")
    if "simA" in RATE_TABLES:   # r6 R1: the Sim A sapling growth rates (per-tree basis)
        con.register("_rate_view", RATE_TABLES["simA"])
        con.execute("CREATE OR REPLACE TABLE sapling_growth_rates_simA AS SELECT * FROM _rate_view")
        con.unregister("_rate_view")
    record_rate_scope(con)   # r7 C F13
    con.unregister("_src_view")
    tsrc = PHANTOM_TREE_SOURCES if PHANTOM_TREE_SOURCES is not None else pl.DataFrame(
        schema={**{k: pl.Int64 for k in TREE_ID}, "prior_measdate": pl.Date,
                "debut_measdate": pl.Date, "species_symbol": pl.Utf8, "prior_age": pl.Int32})
    con.register("_tsrc_view", tsrc.to_arrow())
    con.execute("CREATE OR REPLACE TABLE phantom_tree_sources AS SELECT * FROM _tsrc_view")
    con.unregister("_tsrc_view")
    con.commit()
    print(f"\n[save] curated_cohorts_landis → {cohorts.height:,} rows; "
          f"phantom_cohort_sources → {0 if src is None else src.height:,} rows")

    con.close()
    print("[done]")
