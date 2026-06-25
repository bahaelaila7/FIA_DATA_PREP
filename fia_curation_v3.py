"""
fia_curation_v3.py
FIA longitudinal subplot curation pipeline for Pan comparison.

v3 changes from v2:
- inclusion of plot Cycle

v2 changes from v1:
- Removed erroneous STDAGE < 998 filter (that's FLDAGE, not STDAGE)
- STDAGE assigned to ALL trees in STDORGCD=1 conditions, not just STDORGSP-matching
- Tree fate classification: harvest inferred from AGENTCD=80, STATUSCD=3, and
  TRTCD=10 context (no explicit marker)
- Polars for tree-level longitudinal processing instead of SQL

SQL for bulk joins (steps 1-3), polars for tree-level logic (steps 4+).

Usage:
    import duckdb
    con = duckdb.connect('path/to/fiadb.duckdb')
    from fia_curation_v2 import run_pipeline
    df, snapshots, confusion_edges = run_pipeline(con, estage_dir='../TreeAge/14775738')
"""

import glob
import os

import duckdb
import polars as pl

TREE_ID = ["STATECD", "UNITCD", "COUNTYCD", "PLOT", "SUBP", "TREE"]
SUBPLOT_ID = ["STATECD", "UNITCD", "COUNTYCD", "PLOT", "SUBP"]
PLOT_INVYR = ["STATECD", "UNITCD", "COUNTYCD", "PLOT", "INVYR"]
COND_KEY = ["STATECD", "UNITCD", "COUNTYCD", "PLOT", "INVYR", "CONDID"]


# =============================================================================
# Steps 1-3: SQL (bulk filtering and joins)
# =============================================================================

def sql_base_measurements(con):
    """Step 1: subplot-cond measurements, annual CONUS, non-skipped."""
    con.execute("""
    DROP TABLE IF EXISTS base_subplot_cond;
    CREATE TABLE base_subplot_cond AS
    SELECT
        SUBP.STATECD, SUBP.UNITCD, SUBP.COUNTYCD, SUBP.PLOT, SUBP.INVYR, SUBP.SUBP,
        C.CONDID,
        MAKE_DATE(P.MEASYEAR,
                  COALESCE(CAST(P.MEASMON AS INTEGER), 6),
                  COALESCE(P.MEASDAY, 15)) AS MEASDATE,
        P.DESIGNCD, P.CYCLE,
        C.COND_STATUS_CD,
        C.STDAGE, C.FLDAGE, C.STDORGCD, C.STDORGSP,
        C.FLDSZCD, C.STDSZCD,
        C.SICOND, C.SIBASE, C.SISP,
        C.FORTYPCD, C.SITECLCD,
        C.BALIVE, C.ALSTK, C.ALSTKCD, C.GSSTK, C.GSSTKCD,
        C.PRESNFCD, C.SLOPE, C.ASPECT, P.ELEV, C.OWNGRPCD,
        C.CONDPROP_UNADJ,
        C_S.SUBPCOND_PROP, C_S.MICRCOND_PROP, C_S.MACRCOND_PROP,
        C.DSTRBCD1, C.DSTRBCD2, C.DSTRBCD3,
        C.DSTRBYR1, C.DSTRBYR2, C.DSTRBYR3,
        C.TRTCD1, C.TRTCD2, C.TRTCD3,
        C.TRTYR1, C.TRTYR2, C.TRTYR3,
        ((C.DSTRBCD1 IS NOT NULL AND C.DSTRBCD1 NOT IN (0, 60)) OR
         (C.DSTRBCD2 IS NOT NULL AND C.DSTRBCD2 NOT IN (0, 60)) OR
         (C.DSTRBCD3 IS NOT NULL AND C.DSTRBCD3 NOT IN (0, 60))) AS disturbed,
        (C.TRTCD1 = 10 OR C.TRTCD2 = 10 OR C.TRTCD3 = 10) AS cut,
        (C.TRTCD1 = 30 OR C.TRTCD2 = 30 OR C.TRTCD3 = 30) AS artificial_regen,
        (C.TRTCD1 = 40 OR C.TRTCD2 = 40 OR C.TRTCD3 = 40) AS natural_regen,
        (C.TRTCD1 = 50 OR C.TRTCD2 = 50 OR C.TRTCD3 = 50) AS silvicultural
    FROM PLOT P
    JOIN SURVEY SRV
        ON P.SRV_CN = SRV.CN
        AND SRV.ANN_INVENTORY = 'Y'
        AND P.STATECD <= 56 AND P.STATECD NOT IN (2, 15)
    JOIN SUBPLOT SUBP
        ON  SUBP.STATECD  = P.STATECD
        AND SUBP.UNITCD   = P.UNITCD
        AND SUBP.COUNTYCD = P.COUNTYCD
        AND SUBP.PLOT     = P.PLOT
        AND SUBP.INVYR    = P.INVYR
        AND P.PLOT_STATUS_CD < 3
        AND SUBP.SUBP_STATUS_CD <> 3
    JOIN SUBP_COND C_S
        ON  C_S.STATECD  = P.STATECD
        AND C_S.UNITCD   = P.UNITCD
        AND C_S.COUNTYCD = P.COUNTYCD
        AND C_S.PLOT     = P.PLOT
        AND C_S.INVYR    = P.INVYR
        AND C_S.SUBP     = SUBP.SUBP
    JOIN COND C
        ON  C.STATECD   = P.STATECD
        AND C.UNITCD    = P.UNITCD
        AND C.COUNTYCD  = P.COUNTYCD
        AND C.PLOT      = P.PLOT
        AND C.INVYR     = P.INVYR
        AND C_S.CONDID  = C.CONDID
    """)
    con.commit()
    n = con.execute("SELECT COUNT(*) FROM base_subplot_cond").fetchone()[0]
    print(f"[step1] base_subplot_cond: {n:,} rows")


def sql_clean_subplot_years(con):
    """Step 2: drop subplot-years where any cond has COND_STATUS_CD=3."""
    con.execute("""
    DROP TABLE IF EXISTS clean_subplot_years;
    CREATE TABLE clean_subplot_years AS
    SELECT STATECD, UNITCD, COUNTYCD, PLOT, INVYR, SUBP, MEASDATE, CYCLE
    FROM base_subplot_cond
    GROUP BY STATECD, UNITCD, COUNTYCD, PLOT, INVYR, SUBP, MEASDATE, CYCLE
    HAVING SUM(CASE WHEN COND_STATUS_CD = 3 THEN 1 ELSE 0 END) = 0
    """)
    con.commit()
    n = con.execute("SELECT COUNT(*) FROM clean_subplot_years").fetchone()[0]
    print(f"[step2] clean_subplot_years: {n:,}")


def sql_longitudinal_subplots(con):
    """Step 3: subplots with >=2 clean measurement years."""
    con.execute("""
    DROP TABLE IF EXISTS longitudinal_subplots;
    CREATE TABLE longitudinal_subplots AS
    SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP,
           COUNT(*)      AS n_measurements,
           MIN(MEASDATE) AS first_measdate,
           MAX(MEASDATE) AS last_measdate,
    FROM clean_subplot_years
    GROUP BY STATECD, UNITCD, COUNTYCD, PLOT, SUBP
    HAVING COUNT(*) >= 2
    """)
    con.commit()
    n = con.execute("SELECT COUNT(*) FROM longitudinal_subplots").fetchone()[0]
    print(f"[step3] longitudinal_subplots: {n:,}")
    dist = con.execute("""
        SELECT n_measurements, COUNT(*) FROM longitudinal_subplots
        GROUP BY n_measurements ORDER BY n_measurements
    """).fetchall()
    for nm, cnt in dist:
        print(f"        {nm} meas: {cnt:,}")


def sql_excluded_trees(con):
    """
    Identify trees to exclude from the analysis (RECONCILECD anomalies or
    STATUSCD=0), storing the reason(s) and a meaningful/trivial flag per tree.

    NOTE: RECONCILECD=5 (diameter shrank below threshold — tree still alive)
    is intentionally NOT excluded here; it is handled separately in
    handle_reconcile5() which keeps it as STATUSCD=1 with imputed DIA.

    is_meaningful = TRUE if the tree ever had at least one legitimate alive
    measurement (STATUSCD=1, RECONCILECD in {NULL, 1, 2}) in the annual CONUS
    survey. That means we are genuinely discarding real observed data.
    is_meaningful = FALSE when the tree never had a valid measurement, i.e. it
    was counted-in by mistake and we are simply correcting that error — no
    real information is lost.

    The distinction matters for the subplot exclusion flag in the scaffold: only
    meaningful exclusions warrant flagging a subplot as having dropped trees.

    Exclusion reason codes stored in exclusion_reasons (comma-separated):
      statuscd_invalid   STATUSCD = 0  (erroneous record, never a real tree)
      not_found          RECONCILECD = 3
      boundary_error     RECONCILECD = 4
      state_change       RECONCILECD = 6
      prev_missed        RECONCILECD = 7
      macroplot_removal  RECONCILECD = 8
      other              RECONCILECD = 9
    """
    con.execute("""
    DROP TABLE IF EXISTS excluded_tree_ids;
    CREATE TABLE excluded_tree_ids AS
    WITH annual_conus AS (
        -- Restrict to the same annual CONUS survey scope used everywhere else
        SELECT P.STATECD, P.UNITCD, P.COUNTYCD, P.PLOT, P.INVYR
        FROM PLOT P
        JOIN SURVEY SRV ON P.SRV_CN = SRV.CN AND SRV.ANN_INVENTORY = 'Y'
        WHERE P.STATECD <= 56 AND P.STATECD NOT IN (2, 15)
    ),
    valid_meas AS (
        -- Trees that ever had a legitimate alive measurement in scope
        SELECT DISTINCT T.STATECD, T.UNITCD, T.COUNTYCD, T.PLOT, T.SUBP, T.TREE
        FROM TREE T
        JOIN annual_conus AC
            ON  AC.STATECD = T.STATECD AND AC.UNITCD = T.UNITCD
            AND AC.COUNTYCD = T.COUNTYCD AND AC.PLOT = T.PLOT AND AC.INVYR = T.INVYR
        WHERE T.STATUSCD = 1
          AND (T.RECONCILECD IS NULL OR T.RECONCILECD IN (1, 2))
    ),
    raw_excl AS (
        SELECT
            T.STATECD, T.UNITCD, T.COUNTYCD, T.PLOT, T.SUBP, T.TREE,
            CASE
                WHEN T.STATUSCD    = 0 THEN 'statuscd_invalid'
                WHEN T.RECONCILECD = 3 THEN 'not_found'
                WHEN T.RECONCILECD = 4 THEN 'boundary_error'
                WHEN T.RECONCILECD = 6 THEN 'state_change'
                WHEN T.RECONCILECD = 7 THEN 'prev_missed'
                WHEN T.RECONCILECD = 8 THEN 'macroplot_removal'
                WHEN T.RECONCILECD = 9 THEN 'other'
            END AS reason
        FROM TREE T
        JOIN annual_conus AC
            ON  AC.STATECD = T.STATECD AND AC.UNITCD = T.UNITCD
            AND AC.COUNTYCD = T.COUNTYCD AND AC.PLOT = T.PLOT AND AC.INVYR = T.INVYR
        WHERE T.RECONCILECD IN (3, 4, 6, 7, 8, 9)
           OR T.STATUSCD = 0
    ),
    deduped AS (
        SELECT DISTINCT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE, reason
        FROM raw_excl
    )
    SELECT
        d.STATECD, d.UNITCD, d.COUNTYCD, d.PLOT, d.SUBP, d.TREE,
        STRING_AGG(d.reason, ', ' ORDER BY d.reason) AS exclusion_reasons,
        BOOL_OR(vm.TREE IS NOT NULL)                 AS is_meaningful
    FROM deduped d
    LEFT JOIN valid_meas vm
        ON  vm.STATECD  = d.STATECD AND vm.UNITCD  = d.UNITCD
        AND vm.COUNTYCD = d.COUNTYCD AND vm.PLOT   = d.PLOT
        AND vm.SUBP     = d.SUBP    AND vm.TREE    = d.TREE
    GROUP BY d.STATECD, d.UNITCD, d.COUNTYCD, d.PLOT, d.SUBP, d.TREE
    """)
    con.commit()

    n      = con.execute("SELECT COUNT(*) FROM excluded_tree_ids").fetchone()[0]
    n_mean = con.execute(
        "SELECT COUNT(*) FROM excluded_tree_ids WHERE is_meaningful"
    ).fetchone()[0]
    print(f"[step3b] excluded_tree_ids: {n:,} trees "
          f"({n_mean:,} meaningful, {n - n_mean:,} trivial)")

    reason_counts = con.execute("""
        SELECT reason, COUNT(*) AS n
        FROM (
            SELECT UNNEST(STRING_SPLIT(exclusion_reasons, ', ')) AS reason
            FROM excluded_tree_ids
        )
        GROUP BY reason ORDER BY n DESC
    """).fetchall()
    for reason, cnt in reason_counts:
        print(f"        {reason:25s}: {cnt:>8,}")


# =============================================================================
# Step 4: Load into polars
# =============================================================================

def load_trees(con) -> pl.DataFrame:
    """
    Load tree records from clean longitudinal subplots into polars,
    with condition context attached. Excludes anomalous trees.
    """
    df = con.execute("""
    SELECT
        T.STATECD, T.UNITCD, T.COUNTYCD, T.PLOT, T.SUBP, T.TREE, T.INVYR,
        T.CN,
        CSY.MEASDATE, CSY.CYCLE,
        T.CONDID, T.STATUSCD, T.RECONCILECD, T.AGENTCD,
        T.SPCD, T.SPGRPCD, SP.STOCKING_SPGRPCD, SP.JENKINS_SPGRPCD,
        T.DIA, T.HT, T.ACTUALHT, T.CR, T.CCLCD,
        T.DRYBIO_AG, T.CARBON_AG, T.TPA_UNADJ,
        T.TOTAGE, T.BHAGE, T.TREECLCD,
        T.DAMLOC1, T.DAMLOC2,
        T.DAMTYP1, T.DAMTYP2,
        T.DAMSEV1, T.DAMSEV2,
        T.ABNORMAL_TERMINATION,
        -- condition context
        C.STDAGE      AS cond_stdage,
        C.FLDAGE      AS cond_fldage,
        C.STDORGCD    AS cond_stdorgcd,
        C.STDORGSP    AS cond_stdorgsp,
        C.FLDSZCD     AS cond_fldszcd,
        C.STDSZCD     AS cond_stdszcd,
        C.SICOND      AS cond_sicond,
        C.SIBASE      AS cond_sibase,
        C.SISP        AS cond_sisp,
        C.FORTYPCD    AS cond_fortypcd,
        C.SITECLCD    AS cond_siteclcd,
        C.BALIVE      AS cond_balive,
        C.ALSTK       AS cond_alstk,
        C.ALSTKCD     AS cond_alstkcd,
        C.SLOPE       AS cond_slope,
        C.ASPECT      AS cond_aspect,
        P.ELEV        AS cond_elev,
        P.MACRO_BREAKPOINT_DIA,
        C.TRTCD1, C.TRTCD2, C.TRTCD3,
        C.TRTYR1, C.TRTYR2, C.TRTYR3,
        C.DSTRBCD1, C.DSTRBCD2, C.DSTRBCD3,
        C.DSTRBYR1, C.DSTRBYR2, C.DSTRBYR3
    FROM TREE T
    JOIN REF_SPECIES SP ON SP.SPCD = T.SPCD
    JOIN clean_subplot_years CSY
        ON  T.STATECD  = CSY.STATECD AND T.UNITCD = CSY.UNITCD
        AND T.COUNTYCD = CSY.COUNTYCD AND T.PLOT = CSY.PLOT
        AND T.INVYR    = CSY.INVYR AND T.SUBP = CSY.SUBP
    JOIN longitudinal_subplots LS
        ON  T.STATECD  = LS.STATECD AND T.UNITCD = LS.UNITCD
        AND T.COUNTYCD = LS.COUNTYCD AND T.PLOT = LS.PLOT
        AND T.SUBP     = LS.SUBP
    JOIN COND C
        ON  C.STATECD  = T.STATECD AND C.UNITCD = T.UNITCD
        AND C.COUNTYCD = T.COUNTYCD AND C.PLOT = T.PLOT
        AND C.INVYR    = T.INVYR AND C.CONDID = T.CONDID
    JOIN PLOT P
        ON  P.STATECD  = T.STATECD AND P.UNITCD = T.UNITCD
        AND P.COUNTYCD = T.COUNTYCD AND P.PLOT = T.PLOT
        AND P.INVYR    = T.INVYR
    WHERE T.TPA_UNADJ > 0
      AND NOT EXISTS (
          SELECT 1 FROM excluded_tree_ids E
          WHERE E.STATECD = T.STATECD AND E.UNITCD = T.UNITCD
            AND E.COUNTYCD = T.COUNTYCD AND E.PLOT = T.PLOT
            AND E.SUBP = T.SUBP AND E.TREE = T.TREE
      )
    """).pl()

    df = df.sort(TREE_ID + ["MEASDATE"])
    print(f"[step4] loaded {df.height:,} tree-measurements into polars")
    return df


# =============================================================================
# Step 4b: SPCD confusion analysis and resolution
# =============================================================================

def _union_find(edges: list[tuple[int, int]]) -> dict[int, int]:
    """Simple union-find returning {node: component_id}."""
    parent = {}

    def find(x):
        if x not in parent:
            parent[x] = x
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            # lower SPCD becomes root (arbitrary but stable)
            if ra > rb:
                ra, rb = rb, ra
            parent[rb] = ra

    for a, b in edges:
        union(a, b)

    # Finalize: map every node to its root
    return {x: find(x) for x in parent}


def analyze_spcd_confusion(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Detect SPCD changes across consecutive measurements of the same tree.

    Returns:
        df:             input dataframe with added columns:
                          - spcd_resolved: mode SPCD across the tree's history
                                           (tie-break: latest measurement)
                          - spcd_changed:  bool, TRUE if this tree ever had a
                                           SPCD change
                          - spcd_confusion_cluster: int, connected-component ID
                                           from the confusion graph (trees whose
                                           SPCDs are transitively confused share
                                           a cluster)
        confusion_edges: DataFrame of (spcd_a, spcd_b, n_transitions, n_trees)
                         sorted by n_trees descending
    """
    # --- Detect transitions ---
    transitions = (
        df.select(TREE_ID + ["MEASDATE", "SPCD"])
        .sort(TREE_ID + ["MEASDATE"])
        .with_columns(
            pl.col("SPCD").shift(-1).over(TREE_ID).alias("next_spcd"),
        )
        .filter(
            pl.col("next_spcd").is_not_null() &
            (pl.col("SPCD") != pl.col("next_spcd"))
        )
    )

    n_transitions = transitions.height
    n_trees_changed = (
        transitions.select(TREE_ID).unique().height
    )
    print(f"[step4b] SPCD transitions: {n_transitions:,} across {n_trees_changed:,} trees")

    if n_transitions == 0:
        df = df.with_columns([
            pl.col("SPCD").alias("spcd_resolved"),
            pl.lit(False).alias("spcd_changed"),
            pl.col("SPCD").alias("spcd_confusion_cluster"),
        ])
        return df, pl.DataFrame(schema={
            "spcd_a": pl.Int32, "spcd_b": pl.Int32,
            "n_transitions": pl.UInt32, "n_trees": pl.UInt32,
        })

    # --- Build undirected confusion edges ---
    # Normalize: always (min, max) to make undirected
    edges = (
        transitions.select([
            pl.min_horizontal("SPCD", "next_spcd").alias("spcd_a"),
            pl.max_horizontal("SPCD", "next_spcd").alias("spcd_b"),
            pl.struct(TREE_ID).alias("_tree_key"),
        ])
    )

    confusion_edges = (
        edges.group_by(["spcd_a", "spcd_b"])
        .agg([
            pl.len().alias("n_transitions"),
            pl.col("_tree_key").n_unique().alias("n_trees"),
        ])
        .sort("n_trees", descending=True)
    )

    print(f"         unique confusion pairs: {confusion_edges.height:,}")
    print(f"         top confusion pairs:")
    for r in confusion_edges.head(15).iter_rows(named=True):
        print(f"           SPCD {r['spcd_a']:>4} <-> {r['spcd_b']:>4}:  "
              f"{r['n_trees']:>6,} trees, {r['n_transitions']:>6,} transitions")

    # --- Connected components (confusion clusters) ---
    edge_list = list(zip(
        confusion_edges["spcd_a"].to_list(),
        confusion_edges["spcd_b"].to_list()
    ))
    component_map = _union_find(edge_list)

    # Ensure all SPCDs (including those never confused) get a cluster
    all_spcds = df["SPCD"].unique().to_list()
    cluster_map = {}
    for spcd in all_spcds:
        cluster_map[spcd] = component_map.get(spcd, spcd)

    cluster_df = pl.DataFrame({
        "SPCD": list(cluster_map.keys()),
        "spcd_confusion_cluster": list(cluster_map.values()),
    })

    # Count multi-species clusters
    cluster_sizes = (
        cluster_df.group_by("spcd_confusion_cluster")
        .agg(pl.len().alias("n_spcds"))
        .filter(pl.col("n_spcds") > 1)
        .sort("n_spcds", descending=True)
    )

    print(f"         confusion clusters with >1 SPCD: {cluster_sizes.height:,}")
    for r in cluster_sizes.head(10).iter_rows(named=True):
        members = (
            cluster_df.filter(
                pl.col("spcd_confusion_cluster") == r["spcd_confusion_cluster"]
            )["SPCD"]
            .sort()
            .to_list()
        )
        print(f"           cluster {r['spcd_confusion_cluster']:>4} "
              f"({r['n_spcds']} spp): {members}")

    # --- Resolve SPCD per tree: mode, tie-break by latest measurement ---
    # Count SPCD occurrences per tree, with latest MEASDATE as tie-breaker
    # Sort ascending so that the LAST row per tree is the most-frequent,
    # latest-observed SPCD. unique(keep="last") respects current row order.
    spcd_counts = (
        df.group_by(TREE_ID + ["SPCD"])
        .agg([
            pl.len().alias("_n_obs"),
            pl.col("MEASDATE").max().alias("_latest"),
        ])
        .sort(["_n_obs", "_latest"])
        .unique(subset=TREE_ID, keep="last")
        .select(TREE_ID + [pl.col("SPCD").alias("spcd_resolved")])
    )

    # Flag trees that had any SPCD change
    changed_trees = (
        transitions.select(TREE_ID).unique()
        .with_columns(pl.lit(True).alias("spcd_changed"))
    )

    # Join back
    df = (
        df.join(spcd_counts, on=TREE_ID, how="left")
        .join(changed_trees, on=TREE_ID, how="left")
        .join(cluster_df, on="SPCD", how="left")
        .with_columns(
            pl.col("spcd_changed").fill_null(False),
        )
    )

    n_resolved_diff = df.filter(pl.col("SPCD") != pl.col("spcd_resolved")).height
    print(f"         tree-measurements where SPCD != spcd_resolved: {n_resolved_diff:,}")

    return df, confusion_edges


# =============================================================================
# Step 5: Classify tree fate (alive / harvest / natural death)
# =============================================================================

def handle_reconcile5(df: pl.DataFrame) -> pl.DataFrame:
    """
    Handle RECONCILECD=5 trees (diameter fell below the subplot minimum, ~5 in).
    The tree is still alive but no longer tallied at the subplot level.

    Actions:
      1. STATUSCD → 1 (alive) for all RECONCILECD=5 rows.
      2. Impute DIA where null, using the minimum for the class the tree was
         previously in (determined from its last non-null DIA):
           prior DIA >= MACRO_BREAKPOINT_DIA  →  MACRO_BREAKPOINT_DIA
           prior DIA >= 5.0 in               →  5.0  (subplot floor)
           prior DIA >= 1.0 in               →  1.0  (microplot floor)
           no prior DIA                       →  5.0  (assume subplot)
      3. reconcile5_dia_imputed : TRUE where DIA was null and got imputed.
      4. reconcile5_recovered   : TRUE on any measurement after the first
           RECONCILECD=5 episode where the tree returns to RECONCILECD in {1,2}.
    """
    r5 = pl.col("RECONCILECD") == 5
    n_r5 = df.filter(r5).height

    if n_r5 == 0:
        return df.with_columns([
            pl.lit(None).cast(pl.Float64).alias("reconcile5_prev_dia_raw"),
            pl.lit(None).cast(pl.Float64).alias("reconcile5_prev_dia"),
            pl.lit(False).alias("reconcile5_dia_imputed"),
            pl.lit(None).cast(pl.Float64).alias("reconcile5_prev_tpa_raw"),
            pl.lit(None).cast(pl.Float64).alias("reconcile5_prev_tpa"),
            pl.lit(False).alias("reconcile5_tpa_propagated"),
            pl.lit(None).cast(pl.Date).alias("reconcile5_first_measdate"),
            pl.lit(False).alias("reconcile5_recovered"),
        ])

    # Most-recent non-null DIA before each row (within tree, sorted by MEASDATE)
    # reconcile5_prev_dia: last observed DIA before this measurement — used to
    #   determine which measurement class the tree was previously in
    df = df.sort(TREE_ID + ["MEASDATE"]).with_columns(
        pl.col("DIA").shift(1).over(TREE_ID).alias("reconcile5_prev_dia_raw")
    ).with_columns(
        pl.col("reconcile5_prev_dia_raw")
        .fill_null(strategy="forward")
        .over(TREE_ID)
        .alias("reconcile5_prev_dia")
    )

    # Build imputed DIA expression
    macro_bp = pl.col("MACRO_BREAKPOINT_DIA").cast(pl.Float64)
    prev     = pl.col("reconcile5_prev_dia")
    needs_impute = r5 & pl.col("DIA").is_null()

    df = df.with_columns([
        # DIA: impute where needed, leave unchanged elsewhere
        pl.when(needs_impute)
        .then(
            pl.when(prev.is_not_null() &
                    pl.col("MACRO_BREAKPOINT_DIA").is_not_null() &
                    (prev >= macro_bp))
            .then(macro_bp)
            .when(prev.is_not_null() & (prev >= 5.0))
            .then(pl.lit(5.0))
            .when(prev.is_not_null() & (prev >= 1.0))
            .then(pl.lit(1.0))
            .otherwise(pl.lit(5.0))
        )
        .otherwise(pl.col("DIA"))
        .alias("DIA"),

        # Track which rows had DIA imputed
        needs_impute.alias("reconcile5_dia_imputed"),
    ])

    # Override STATUSCD to 1 for all RECONCILECD=5 rows
    df = df.with_columns(
        pl.when(r5).then(pl.lit(1)).otherwise(pl.col("STATUSCD")).alias("STATUSCD")
    )

    # Propagate last non-zero TPA_UNADJ for RECONCILECD=5 rows where TPA_UNADJ
    # is 0 or null (can happen when the original STATUSCD was 0).
    # reconcile5_prev_tpa_raw : shift(1) of non-zero TPA within tree
    # reconcile5_prev_tpa     : forward-filled — the value actually used
    # reconcile5_tpa_propagated: TRUE where TPA_UNADJ was replaced
    df = df.with_columns(
        pl.when(pl.col("TPA_UNADJ").is_not_null() & (pl.col("TPA_UNADJ") > 0))
        .then(pl.col("TPA_UNADJ"))
        .otherwise(pl.lit(None))
        .alias("_tpa_nonzero")
    ).with_columns(
        pl.col("_tpa_nonzero").shift(1).over(TREE_ID).alias("reconcile5_prev_tpa_raw")
    ).with_columns(
        pl.col("reconcile5_prev_tpa_raw")
        .fill_null(strategy="forward")
        .over(TREE_ID)
        .alias("reconcile5_prev_tpa")
    ).drop("_tpa_nonzero")

    needs_tpa = r5 & (pl.col("TPA_UNADJ").is_null() | (pl.col("TPA_UNADJ") == 0))
    df = df.with_columns([
        pl.when(needs_tpa & pl.col("reconcile5_prev_tpa").is_not_null())
        .then(pl.col("reconcile5_prev_tpa"))
        .otherwise(pl.col("TPA_UNADJ"))
        .alias("TPA_UNADJ"),

        (needs_tpa & pl.col("reconcile5_prev_tpa").is_not_null())
        .alias("reconcile5_tpa_propagated"),
    ])

    # Recovery flag: back to RECONCILECD∈{1,2} after the first RECONCILECD=5 date
    # reconcile5_first_measdate: the first MEASDATE this tree dropped below threshold
    first_r5_measdate = (
        df.filter(r5)
        .group_by(TREE_ID)
        .agg(pl.col("MEASDATE").min().alias("reconcile5_first_measdate"))
    )
    df = df.join(first_r5_measdate, on=TREE_ID, how="left")
    df = df.with_columns(
        (
            pl.col("reconcile5_first_measdate").is_not_null() &
            pl.col("RECONCILECD").is_in([1, 2]) &
            (pl.col("MEASDATE") > pl.col("reconcile5_first_measdate"))
        ).alias("reconcile5_recovered")
    )

    n_imputed   = df.filter(pl.col("reconcile5_dia_imputed")).height
    n_tpa_prop  = df.filter(pl.col("reconcile5_tpa_propagated")).height
    n_recovered = df.filter(pl.col("reconcile5_recovered")).height
    print(f"[handle_r5] {n_r5:,} RECONCILECD=5 measurements "
          f"({n_imputed:,} DIA imputed, {n_tpa_prop:,} TPA propagated, "
          f"{n_recovered:,} recovery events)")

    return df


def classify_tree_fate(df: pl.DataFrame) -> pl.DataFrame:
    """
    Classify each tree-measurement's status:
      - alive: STATUSCD=1
      - harvest_explicit: STATUSCD=3 or (STATUSCD=2 + AGENTCD=80)
      - harvest_inferred: STATUSCD=2 + condition has TRTCD=10 +
            no other tree in the same condition-year has explicit harvest
      - dead: STATUSCD=2 with non-harvest cause
    """
    cond_has_cutting = (
        (pl.col("TRTCD1") == 10) |
        (pl.col("TRTCD2") == 10) |
        (pl.col("TRTCD3") == 10)
    )

    # Raw classification
    df = df.with_columns(
        pl.when(pl.col("STATUSCD") == 1)
            .then(pl.lit("alive"))
        .when(pl.col("STATUSCD") == 3)
            .then(pl.lit("harvest_explicit"))
        .when((pl.col("STATUSCD") == 2) & (pl.col("AGENTCD") == 80))
            .then(pl.lit("harvest_explicit"))
        .when(pl.col("STATUSCD") == 2)
            .then(pl.lit("dead_raw"))
        .otherwise(pl.lit("unknown"))
        .alias("_fate_raw")
    )

    # Find condition-years where at least one tree has explicit harvest
    explicit_harvest_conds = (
        df.filter(pl.col("_fate_raw") == "harvest_explicit")
        .select(COND_KEY)
        .unique()
        .with_columns(pl.lit(True).alias("_cond_has_explicit"))
    )

    df = df.join(explicit_harvest_conds, on=COND_KEY, how="left")

    # Infer harvest for dead trees on cut conditions
    # without explicit harvest markers on the condition
    df = df.with_columns(
        pl.when(pl.col("_fate_raw") == "dead_raw")
        .then(
            pl.when(
                cond_has_cutting &
                ~pl.col("_cond_has_explicit").fill_null(False)
            )
            .then(pl.lit("harvest_inferred"))
            .otherwise(pl.lit("dead"))
        )
        .otherwise(pl.col("_fate_raw"))
        .alias("tree_fate")
    )

    # ------
    tree_final = (
        df
        .sort(TREE_ID + ["MEASDATE"])
        .group_by(TREE_ID)
        .agg(
            pl.col("MEASDATE").last().alias("_tree_last_measdate"),
            pl.col("tree_fate").last().alias("_final_tree_fate_raw"),
        )
    )

    last_subp_measdates = (
        df
        .group_by(SUBPLOT_ID)
        .agg(
            pl.col("MEASDATE").max().alias("last_subp_measdate")
        )
    )

    out = (
        df
        .join(tree_final, on=TREE_ID, how="left")
        .join(last_subp_measdates, on=SUBPLOT_ID, how="left")
        .with_columns(
            pl.when(
                (pl.col("_final_tree_fate_raw") == "alive")
                & (pl.col("last_subp_measdate") > pl.col("_tree_last_measdate"))
            )
            .then(pl.lit("harvest_inferred"))
            .otherwise(pl.col("_final_tree_fate_raw"))
            .alias("final_tree_fate")
        )
    )

    # -----

    df = df.drop(["_fate_raw", "_cond_has_explicit"])

    # Print breakdown
    fate_counts = df.group_by("tree_fate").len().sort("len", descending=True)
    print("[step5] tree fate classification:")
    for row in fate_counts.iter_rows():
        print(f"        {row[0]:25s}: {row[1]:>12,}")

    return df


# =============================================================================
# Step 6: Age propagation
# =============================================================================

def propagate_totage(df: pl.DataFrame,
                     tree_bounds: pl.DataFrame) -> pl.DataFrame:
    """
    Propagate TOTAGE to all measurements using the best-anchor selection.
    See _select_best_anchor for the selection and min-age-shift logic.
    """
    sources = (
        df.filter(pl.col("TOTAGE").is_not_null())
        .select(TREE_ID + ["MEASDATE",
                            pl.col("TOTAGE").cast(pl.Float64).alias("src_age")])
    )

    if sources.height == 0:
        print("[step6a] no TOTAGE records found")
        return df.with_columns(pl.lit(None).cast(pl.Float64).alias("age_from_totage"))

    anchors = _select_best_anchor(sources, tree_bounds, apply_min_shift=True)
    df      = _propagate_from_anchor(df, anchors, "age_from_totage")

    n_trees   = anchors.height
    n_shifted = anchors.filter(pl.col("min_shift") > 0).height
    n_maxviol = anchors.filter(pl.col("max_viol") > 0).height
    n_meas    = df.filter(pl.col("age_from_totage").is_not_null()).height
    print(f"[step6a] TOTAGE: {n_trees:,} trees anchored, {n_meas:,} measurements propagated")
    print(f"         min-age shifts: {n_shifted:,}  |  residual max-age violations: {n_maxviol:,}")
    return df


def detect_condition_resets(df: pl.DataFrame) -> pl.DataFrame:
    """
    Detect conditions where a stand reset occurred, defined as any of:
      1. STDAGE decreased between consecutive measurements (explicit reset)
      2. Condition-level treatment flags: TRTCD=10 (cutting), or TRTCD=30/40 (regen)
      3. Near-total tree loss: >=80% of live trees from previous measurement are
         now dead/removed

    Returns a DataFrame of (COND_ID + MEASDATE) marking each reset event,
    with a `reset_reason` column.
    """
    COND_ID = ["STATECD", "UNITCD", "COUNTYCD", "PLOT", "CONDID"]

    # --- 1. STDAGE resets ---
    cond_traj = (
        df.select(COND_ID + ["MEASDATE", "cond_stdage", "cond_stdorgcd",
                              "TRTCD1", "TRTCD2", "TRTCD3",
                              "DSTRBCD1", "DSTRBCD2", "DSTRBCD3"])
        .unique(subset=COND_ID + ["MEASDATE"])
        .sort(COND_ID + ["MEASDATE"])
        .with_columns([
            pl.col("cond_stdage").shift(1).over(COND_ID).alias("_prev_stdage"),
            pl.col("MEASDATE").shift(1).over(COND_ID).alias("_prev_measdate"),
        ])
    )

    stdage_resets = (
        cond_traj.filter(
            pl.col("_prev_stdage").is_not_null() &
            pl.col("cond_stdage").is_not_null() &
            (pl.col("cond_stdage") < pl.col("_prev_stdage"))
        )
        .select(COND_ID + ["MEASDATE"])
        .with_columns(pl.lit("stdage_reset").alias("reset_reason"))
    )

    # --- 2. Treatment flags ---
    trt_resets = (
        cond_traj.filter(
            (pl.col("TRTCD1") == 10) | (pl.col("TRTCD2") == 10) | (pl.col("TRTCD3") == 10) |
            (pl.col("TRTCD1").is_in([30, 40])) | (pl.col("TRTCD2").is_in([30, 40])) |
            (pl.col("TRTCD3").is_in([30, 40]))
        )
        .select(COND_ID + ["MEASDATE"])
        .with_columns(pl.lit("treatment_flag").alias("reset_reason"))
    )

    # --- 3. Near-total tree loss ---
    tree_counts = (
        df.group_by(COND_ID + ["MEASDATE"])
        .agg([
            (pl.col("STATUSCD") == 1).sum().alias("n_live"),
            pl.len().alias("n_total"),
        ])
        .sort(COND_ID + ["MEASDATE"])
        .with_columns(
            pl.col("n_live").shift(1).over(COND_ID).alias("_prev_n_live"),
        )
    )

    mortality_resets = (
        tree_counts.filter(
            (pl.col("_prev_n_live").is_not_null()) &
            (pl.col("_prev_n_live") > 0) &
            (pl.col("n_live").cast(pl.Float64) / pl.col("_prev_n_live") < 0.2)
        )
        .select(COND_ID + ["MEASDATE"])
        .with_columns(pl.lit("near_total_loss").alias("reset_reason"))
    )

    # --- Combine and deduplicate ---
    all_resets = pl.concat([stdage_resets, trt_resets, mortality_resets])
    # Keep all reasons per condition-date (a reset can have multiple signals)
    reset_reasons = (
        all_resets.group_by(COND_ID + ["MEASDATE"])
        .agg(pl.col("reset_reason").unique().sort().alias("reset_reasons"))
    )
    # Flat flag for joining
    reset_flag = (
        reset_reasons.select(COND_ID + ["MEASDATE"])
        .unique()
        .with_columns(pl.lit(True).alias("is_reset"))
    )

    n = reset_flag.height
    n_stdage = stdage_resets.height
    n_trt = trt_resets.height
    n_mort = mortality_resets.height
    print(f"[step6b-reset] detected {n:,} condition-year reset events")
    print(f"               stdage_reset: {n_stdage:,}, treatment_flag: {n_trt:,}, "
          f"near_total_loss: {n_mort:,}")

    return reset_flag, reset_reasons


def assign_stdage(df: pl.DataFrame,
                  tree_bounds: pl.DataFrame) -> pl.DataFrame:
    """
    Assign STDAGE using first-measurement anchoring:
    For each tree, use the STDAGE from its first eligible measurement,
    then propagate: age = first_STDAGE + (MEASDATE - first_MEASDATE).

    Eligibility (conditions where STDAGE is trusted as individual tree age):
      - STDORGCD=1 (planted stands): always eligible. All trees were co-established
        at planting, so STDAGE == individual tree age.
      - Confirmed reset + POST-RESET DEBUT: a condition that had a detected
        reset (treatment flag, near-total tree loss, or STDAGE drop coupled with
        ≥50% tree loss) is eligible ONLY for trees whose first measurement falls
        at or after the condition's first reset year. These are genuinely new
        cohort trees; STDAGE reflects their establishment age.

    Pre-reset carry-forwards are explicitly excluded from confirmed_reset
    eligibility: the new cohort's STDAGE does not represent their individual age
    (they are older remnants), and the old stand's STDAGE represents the whole
    stand, not the specific tree.
    """
    COND_ID = ["STATECD", "UNITCD", "COUNTYCD", "PLOT", "CONDID"]

    # Detect resets
    reset_flag, reset_reasons = detect_condition_resets(df)

    # Attach per-measurement-date reset flag
    df = df.join(
        reset_flag.rename({"MEASDATE": "_reset_measdate"}),
        left_on=COND_ID + ["MEASDATE"],
        right_on=COND_ID + ["_reset_measdate"],
        how="left",
    ).with_columns(pl.col("is_reset").fill_null(False))

    # cond_ever_reset: condition had at least one reset at any date
    ever_reset_conds = (
        reset_flag.select(COND_ID).unique()
        .with_columns(pl.lit(True).alias("cond_ever_reset"))
    )
    df = df.join(ever_reset_conds, on=COND_ID, how="left").with_columns(
        pl.col("cond_ever_reset").fill_null(False)
    )

    # First reset date per condition (earliest MEASDATE a reset was detected).
    first_reset_per_cond = (
        reset_flag.group_by(COND_ID)
        .agg(pl.col("MEASDATE").min().alias("stdage_cond_first_reset_measdate"))
    )
    df = df.join(first_reset_per_cond, on=COND_ID, how="left")

    # Tree's first measurement date across all conditions.
    tree_first_meas = (
        df.group_by(TREE_ID)
        .agg(pl.col("MEASDATE").min().alias("stdage_tree_first_measdate"))
    )
    df = df.join(tree_first_meas, on=TREE_ID, how="left")

    # Last date this tree was observed alive — used to distinguish cut/removed
    # pre-reset trees from carry-forward survivors.
    tree_last_alive = (
        df.filter(pl.col("STATUSCD") == 1)
        .group_by(TREE_ID)
        .agg(pl.col("MEASDATE").max().alias("stdage_tree_last_alive_measdate"))
    )
    df = df.join(tree_last_alive, on=TREE_ID, how="left")

    # Eligibility for confirmed_reset conditions (three-way split):
    #
    #   POST-RESET DEBUT (first_measdate >= reset_measdate):
    #     New cohort tree. Post-reset STDAGE = individual age. ✓ eligible
    #
    #   PRE-RESET, DID NOT SURVIVE (last_alive_measdate <= reset_measdate):
    #     Harvested/removed cohort. Pre-reset STDAGE = their cohort age. ✓ eligible
    #     Anchor is at their first (pre-reset) measurement → correct propagation.
    #
    #   PRE-RESET, SURVIVED RESET (first_measdate < reset_measdate AND
    #                               last_alive_measdate > reset_measdate):
    #     Old remnant carry-forward. Neither pre-reset STDAGE (stand-level) nor
    #     post-reset STDAGE (new cohort) represents their individual age. ✗ excluded
    pre_reset = (
        pl.col("cond_ever_reset") &
        pl.col("stdage_cond_first_reset_measdate").is_not_null() &
        (pl.col("stdage_tree_first_measdate") < pl.col("stdage_cond_first_reset_measdate"))
    )
    survived_reset = (
        pl.col("stdage_tree_last_alive_measdate").is_not_null() &
        (pl.col("stdage_tree_last_alive_measdate") > pl.col("stdage_cond_first_reset_measdate"))
    )
    post_reset_debut = (
        pl.col("cond_ever_reset") &
        pl.col("stdage_cond_first_reset_measdate").is_not_null() &
        (pl.col("stdage_tree_first_measdate") >= pl.col("stdage_cond_first_reset_measdate"))
    )

    # FLDSZCD size gate.  STDAGE is, by FIA definition, the average age of the
    # trees in the FIELD-recorded stand-size class (FLDSZCD) of the condition, so
    # only trees that qualify within that class should inherit it. Sapling stands
    # (FLDSZCD=1) keep all trees (the saplings ARE the cohort); in pole/timber and
    # larger classes (FLDSZCD>=2, or nonstocked/unknown) only timber-sized trees
    # (DIA >= 5") qualify, excluding sub-tally understory/ingrowth. A null DIA does
    # NOT qualify: dead records routinely carry a null DIA, and allowing them would
    # let a sub-5" understory tree qualify via its death record and propagate the
    # stand age back onto its live sapling measurements.
    # (A per-class rising floor — sawtimber stands requiring >=9"/11" — was tried and
    # only nudged corr(ln biomass, STDAGE-age) 0.292 -> 0.301 while dropping ~7k
    # legitimate pole-sized cohort trees, so the flat sapling/timber line is used.)
    size_qualifies = (
        (pl.col("cond_fldszcd") == 1) |
        (pl.col("DIA") >= 5.0)
    )
    base_eligible = (
        (pl.col("cond_stdorgcd") == 1) |
        post_reset_debut |
        (pre_reset & ~survived_reset)
    )
    # Qualify/anchor on LIVE measurements only: STDAGE is the mean age of the
    # *live* trees in the size class, and a tree must not qualify (or be anchored)
    # via a dead record — e.g. a sub-5" sapling whose death record happens to read
    # DIA>=5 would otherwise sneak in and propagate stand age onto its live rows.
    alive = pl.col("STATUSCD") == 1
    df = df.with_columns(
        (base_eligible & size_qualifies & alive).alias("stdage_eligible")
    )

    # Diagnostic: trees that were otherwise STDAGE-eligible but lose it entirely
    # to the size gate (no qualifying LIVE measurement with a valid stand age).
    _basec = df.filter(base_eligible & alive & pl.col("cond_stdage").is_not_null() &
                       (pl.col("cond_stdage") > 0))
    _trees_base = _basec.select(TREE_ID).unique()
    _trees_qual = _basec.filter(size_qualifies).select(TREE_ID).unique()
    n_size_gated = _trees_base.join(_trees_qual, on=TREE_ID, how="anti").height
    print(f"[step6b] FLDSZCD size gate: {n_size_gated:,} understory trees removed from "
          f"STDAGE (sub-5in in FLDSZCD>=2 stands)")

    # Diagnostics
    n_carryforward_excl = (
        df.filter(pre_reset & survived_reset)
        .select(TREE_ID).unique().height
    )
    n_precut = (
        df.filter(pre_reset & ~survived_reset)
        .select(TREE_ID).unique().height
    )

    # STDORGSP match flag (for planted stands)
    df = df.with_columns(
        pl.when(
            (pl.col("cond_stdorgcd") == 1) &
            (pl.col("SPCD") == pl.col("cond_stdorgsp"))
        )
        .then(pl.lit(True))
        .otherwise(pl.lit(False))
        .alias("matches_stdorgsp")
    )

    # --- STDAGE anchor selection ---
    # Candidates: eligible measurements with a valid, positive cond_stdage.
    # _select_best_anchor picks the one that minimises max-age violation,
    # applying a min-age shift so the tree is at least 1 yr old at first meas.
    cand_std = (
        df.filter(
            pl.col("stdage_eligible") &
            pl.col("cond_stdage").is_not_null() &
            (pl.col("cond_stdage") > 0)
        )
        .select(TREE_ID + ["MEASDATE", "cond_stdorgcd",
                            pl.col("cond_stdage").cast(pl.Float64).alias("src_age")])
    )

    anchors_std = _select_best_anchor(cand_std, tree_bounds, apply_min_shift=True)

    # Retrieve per-tree anchor date and origcd by joining on (TREE_ID, anchor_date)
    anchor_meta = (
        cand_std
        .join(
            anchors_std.select(TREE_ID + ["anchor_date"]),
            left_on=TREE_ID + ["MEASDATE"],
            right_on=TREE_ID + ["anchor_date"],
            how="inner",
        )
        .select(TREE_ID + [
            pl.col("MEASDATE").alias("_anchor_measdate"),
            pl.col("cond_stdorgcd").alias("_anchor_stdorgcd"),
        ])
    )

    df = (
        df.join(
            anchors_std.select(TREE_ID + ["anchor_date", "anchor_age", "anchor_src_age"])
                       .rename({"anchor_date": "_std_anc_d",
                                "anchor_age":  "_std_anc_a",
                                "anchor_src_age": "_anchor_stdage"}),
            on=TREE_ID, how="left",
        )
        .join(anchor_meta, on=TREE_ID, how="left")
    )

    df = df.with_columns(
        pl.when(pl.col("_std_anc_a").is_not_null())
        .then(
            pl.col("_std_anc_a") +
            (pl.col("MEASDATE") - pl.col("_std_anc_d")).dt.total_days() / 365.25
        )
        .otherwise(pl.lit(None).cast(pl.Float64))
        .alias("age_from_stdage")
    ).drop(["_std_anc_d", "_std_anc_a"])

    n_shifted = anchors_std.filter(pl.col("min_shift") > 0).height
    n_maxviol = anchors_std.filter(pl.col("max_viol") > 0).height
    print(f"         STDAGE anchor: {n_shifted:,} trees had min-age shift  |  "
          f"{n_maxviol:,} residual max-age violations")

    # Context flag: why did this tree get STDAGE?
    df = df.with_columns(
        pl.when(pl.col("age_from_stdage").is_not_null())
        .then(
            pl.when(pl.col("_anchor_stdorgcd") == 1)
                .then(pl.lit("planted"))
            .otherwise(pl.lit("confirmed_reset"))
        )
        .alias("stdage_context")
    )

    # --- Diagnostics: restricted vs expanded ---
    n_total = df.filter(pl.col("age_from_stdage").is_not_null()).height
    n_planted = df.filter(
        pl.col("age_from_stdage").is_not_null() &
        (pl.col("stdage_context") == "planted")
    ).height
    n_reset = df.filter(
        pl.col("age_from_stdage").is_not_null() &
        (pl.col("stdage_context") == "confirmed_reset")
    ).height

    # What we'd get with STDORGCD=1 restriction only
    n_restricted = df.filter(
        pl.col("age_from_stdage").is_not_null() &
        (pl.col("_anchor_stdorgcd") == 1)   # still _anchor_stdorgcd before rename
    ).height

    print(f"[step6b] STDAGE assigned (first-measurement anchoring):")
    print(f"         total:                          {n_total:>10,}")
    print(f"           planted:                      {n_planted:>10,}")
    print(f"           confirmed_reset:              {n_reset:>10,}")
    print(f"         confirmed_reset eligibility breakdown:")
    print(f"           post-reset debut:             {n_total - n_planted:>10,}  (approx)")
    print(f"           pre-reset cut (restored):     {n_precut:>10,}")
    print(f"           pre-reset carry-fwd excluded: {n_carryforward_excl:>10,}"
          f"  (alive after reset — new cohort STDAGE ≠ individual age)")
    print(f"         if STDORGCD=1 only:             {n_restricted:>10,}  "
          f"(+{n_total - n_restricted:,} from confirmed-reset)")

    # Breakdown: STDORGCD=0 conditions with confirmed resets, by reset reason
    if n_reset > 0:
        reset_cond_dates = (
            df.filter(
                pl.col("age_from_stdage").is_not_null() &
                (pl.col("stdage_context") == "confirmed_reset")
            )
            .select(COND_ID + ["MEASDATE"]).unique()
            .join(reset_reasons, on=COND_ID + ["MEASDATE"], how="left")
        )
        # Explode reset_reasons and count
        reason_counts = (
            reset_cond_dates.explode("reset_reasons")
            .filter(pl.col("reset_reasons").is_not_null())
            .group_by("reset_reasons").len()
            .sort("len", descending=True)
        )
        print(f"         reset reasons (condition-dates):")
        for r in reason_counts.iter_rows(named=True):
            print(f"           {r['reset_reasons']:25s}: {r['len']:>8,}")

    # Keep anchor fields for decision tracking and discrepancy diagnosis.
    df = df.rename({
        "_anchor_measdate": "stdage_anchor_measdate",
        "_anchor_stdage":   "stdage_anchor_value",
        "_anchor_stdorgcd": "stdage_anchor_orgcd",
    })

    # --- Post-reset anchor flag ---
    # With the new eligibility logic all confirmed_reset trees are post-reset
    # debuts by construction, so stdage_post_reset_anchor is True for all of
    # them. Kept for documentation and downstream filtering convenience.
    df = df.with_columns(
        pl.when(
            pl.col("stdage_anchor_measdate").is_not_null() &
            pl.col("stdage_cond_first_reset_measdate").is_not_null() &
            (pl.col("stdage_anchor_measdate") >= pl.col("stdage_cond_first_reset_measdate"))
        )
        .then(pl.lit(True))
        .otherwise(pl.lit(False))
        .alias("stdage_post_reset_anchor")
    )

    return df


def merge_age_estimates(df: pl.DataFrame) -> pl.DataFrame:
    """Combine age estimates: TOTAGE > STDAGE > NULL."""
    df = df.with_columns([
        pl.coalesce(["age_from_totage", "age_from_stdage"])
            .alias("estimated_age"),
        pl.when(pl.col("age_from_totage").is_not_null())
            .then(pl.lit("TOTAGE"))
        .when(pl.col("age_from_stdage").is_not_null())
            .then(pl.lit("STDAGE_") + pl.col("stdage_context").fill_null("unknown"))
        .otherwise(pl.lit(None))
        .alias("age_source"),
    ])

    stats = df.group_by("age_source").len().sort("len", descending=True)
    print("[step6c] final age estimates:")
    for row in stats.iter_rows():
        label = row[0] if row[0] is not None else "NULL"
        print(f"         {label:25s}: {row[1]:>12,}")
    return df


# =============================================================================
# Shared anchor-selection helpers (used by all age sources)
# =============================================================================

def _compute_tree_bounds(df: pl.DataFrame,
                         subplot_years: pl.DataFrame) -> pl.DataFrame:
    """
    Compute per-tree:
      first_measdate   : earliest MEASDATE in the curated tree records
      prev_sub_measdate: latest subplot visit date strictly before first_measdate
                         (uses all subplot visits, including ones with no curated
                         trees — these bound the maximum plausible tree age)

    Returns a DataFrame keyed by TREE_ID.
    """
    tree_first = (
        df.group_by(TREE_ID)
        .agg(pl.col("MEASDATE").min().alias("first_measdate"))
    )
    sub_visits = (
        subplot_years
        .select(SUBPLOT_ID + ["MEASDATE"])
        .unique()
        .rename({"MEASDATE": "_sub_measdate"})
    )
    prev_sub = (
        tree_first
        .join(sub_visits, on=SUBPLOT_ID, how="left")
        .filter(pl.col("_sub_measdate") < pl.col("first_measdate"))
        .group_by(TREE_ID)
        .agg(pl.col("_sub_measdate").max().alias("prev_sub_measdate"))
    )
    return tree_first.join(prev_sub, on=TREE_ID, how="left")


def _select_best_anchor(candidates: pl.DataFrame,
                        tree_bounds: pl.DataFrame,
                        apply_min_shift: bool = False) -> pl.DataFrame:
    """
    Select one source measurement per tree to use as the propagation anchor.

    candidates must contain: TREE_ID + MEASDATE + "src_age"
    tree_bounds must contain: TREE_ID + first_measdate + prev_sub_measdate
                              (output of _compute_tree_bounds)

    apply_min_shift : if True, shift anchor age so tree is at least 1 yr old
                      at its first measurement.  Use only for TOTAGE, which can
                      record 0.  STDAGE/estage/model values should not be
                      silently inflated.

    Algorithm
    ---------
    1. For each candidate compute implied age at the tree's first_measdate.
    2. Optionally apply a min-age shift so that age at first_measdate >= 1
       (TOTAGE only).
    3. Compute potential max-age violation: implied age at the most recent
       subplot visit before first_measdate (after any shift).
    4. Select the candidate with the smallest max-age violation.
       Tie-break: most recent candidate MEASDATE.

    Returns TREE_ID + [anchor_date, anchor_age, anchor_src_age, min_shift,
                       max_viol] per tree.
    anchor_age     : (possibly shifted) src_age at anchor_date
    anchor_src_age : original src_age before shift (for diagnostics)
    min_shift      : years added to satisfy the age >= 1 constraint (0 when
                     apply_min_shift=False)
    max_viol       : residual potential max-age violation after shift (years)
    """
    cands = candidates.join(tree_bounds, on=TREE_ID, how="left")

    cands = cands.with_columns(
        (
            pl.col("src_age").cast(pl.Float64) +
            (pl.col("first_measdate") - pl.col("MEASDATE")).dt.total_days() / 365.25
        ).alias("_implied_at_first")
    )
    if apply_min_shift:
        cands = cands.with_columns(
            (pl.lit(1.0) - pl.col("_implied_at_first")).clip(lower_bound=0.0)
            .alias("min_shift")
        )
    else:
        cands = cands.with_columns(pl.lit(0.0).alias("min_shift"))
    cands = cands.with_columns(
        (pl.col("_implied_at_first") + pl.col("min_shift")).alias("_age_at_first")
    )
    cands = cands.with_columns(
        pl.when(pl.col("prev_sub_measdate").is_not_null())
        .then(
            (
                pl.col("_age_at_first") +
                (pl.col("prev_sub_measdate") - pl.col("first_measdate"))
                .dt.total_days() / 365.25
            ).clip(lower_bound=0.0)
        )
        .otherwise(pl.lit(0.0))
        .alias("max_viol")
    )

    best = (
        cands
        .sort(TREE_ID + ["max_viol", "MEASDATE"],
              descending=[False] * len(TREE_ID) + [False, True])
        .unique(subset=TREE_ID, keep="first")
        .with_columns(
            (pl.col("src_age").cast(pl.Float64) + pl.col("min_shift"))
            .alias("anchor_age")
        )
        .select(TREE_ID + [
            pl.col("MEASDATE").alias("anchor_date"),
            pl.col("anchor_age"),
            pl.col("src_age").cast(pl.Float64).alias("anchor_src_age"),
            pl.col("min_shift"),
            pl.col("max_viol"),
        ])
    )
    return best


def _propagate_from_anchor(df: pl.DataFrame,
                           anchors: pl.DataFrame,
                           out_col: str) -> pl.DataFrame:
    """
    Propagate anchor age linearly to all measurements of each tree:
      out_col = anchor_age + (MEASDATE − anchor_date).days / 365.25
    Rows without an anchor get null.
    """
    df = df.join(
        anchors.select(TREE_ID + ["anchor_date", "anchor_age"])
               .rename({"anchor_date": "_anc_d", "anchor_age": "_anc_a"}),
        on=TREE_ID, how="left",
    )
    df = df.with_columns(
        pl.when(pl.col("_anc_a").is_not_null())
        .then(
            pl.col("_anc_a") +
            (pl.col("MEASDATE") - pl.col("_anc_d")).dt.total_days() / 365.25
        )
        .otherwise(pl.lit(None).cast(pl.Float64))
        .alias(out_col)
    ).drop(["_anc_d", "_anc_a"])
    return df


# =============================================================================
# Step 6d: External age estimates (estage)
# =============================================================================

def load_estage_csv(csv_dir: str) -> pl.DataFrame:
    """
    Load and concatenate all *_TREEAGE.csv files from csv_dir.
    Returns a DataFrame with columns: CN (Utf8), Tree_Age (Int32).
    """
    files = sorted(glob.glob(os.path.join(csv_dir, "*_TREEAGE.csv")))
    if not files:
        raise FileNotFoundError(f"No *_TREEAGE.csv files found in {csv_dir}")
    dfs = [
        pl.read_csv(f, columns=["CN", "Tree_Age"])
        .with_columns(pl.col("CN").cast(pl.Utf8))
        for f in files
    ]
    estage_raw = pl.concat(dfs).unique(subset=["CN"])
    print(f"[step6d] loaded {estage_raw.height:,} estage records from {len(files)} state files")
    return estage_raw


def process_estage(df: pl.DataFrame, estage_raw: pl.DataFrame,
                   tree_bounds: pl.DataFrame) -> pl.DataFrame:
    """
    Join external age estimates to df by TREE.CN, compute per-raw-estimate
    violation diagnostics, select the best anchor per tree via
    _select_best_anchor (min-age shift + least max-age violation), and
    propagate to all measurements.

    Adds columns:
      estage_raw          : raw external estimate at each measurement (null if none)
      estage_min_violation: years the raw estimate falls below the min possible age
                            (elapsed since first curated appearance); diagnostic only
      estage_max_violation: implied age at the most recent prior subplot visit;
                            diagnostic only — selection already minimises this
      estage              : best-anchor age propagated to all measurements (Int32)
      estage_anchor_invyr : INVYR of the chosen anchor measurement
    """
    df = df.with_columns(pl.col("CN").cast(pl.Utf8))

    df = df.join(estage_raw.rename({"Tree_Age": "estage_raw"}), on="CN", how="left")
    n_with_est    = df.filter(pl.col("estage_raw").is_not_null()).height
    n_trees_w_est = df.filter(pl.col("estage_raw").is_not_null()).select(TREE_ID).unique().height
    print(f"[step6d] joined estage: {n_with_est:,} measurements / "
          f"{n_trees_w_est:,} trees have raw estimates")

    # --- Diagnostic violation columns (kept for downstream inspection) ---
    # Min violation: raw estimate below elapsed years since first appearance
    df = df.with_columns(
        pl.when(pl.col("estage_raw").is_not_null())
        .then(
            (
                (pl.col("MEASDATE") - tree_bounds["first_measdate"]
                 .rename(dict(zip(TREE_ID, TREE_ID)))  # placeholder — computed via join below
                 ) .dt.total_days() / 365.25
                 - pl.col("estage_raw").cast(pl.Float64)
            ).clip(lower_bound=0.0)
        )
        .otherwise(pl.lit(None).cast(pl.Float64))
        .alias("estage_min_violation")
    ) if False else df  # computed properly below after joining tree_bounds

    # Join tree_bounds for first_measdate and prev_sub_measdate
    df = df.join(
        tree_bounds.select(TREE_ID + ["first_measdate", "prev_sub_measdate"]),
        on=TREE_ID, how="left",
    )

    df = df.with_columns([
        pl.when(pl.col("estage_raw").is_not_null())
        .then(
            (
                (pl.col("MEASDATE") - pl.col("first_measdate")).dt.total_days() / 365.25
                - pl.col("estage_raw").cast(pl.Float64)
            ).clip(lower_bound=0.0)
        )
        .otherwise(pl.lit(None).cast(pl.Float64))
        .alias("estage_min_violation"),

        pl.when(
            pl.col("estage_raw").is_not_null() &
            pl.col("prev_sub_measdate").is_not_null()
        )
        .then(
            (
                pl.col("estage_raw").cast(pl.Float64) -
                (pl.col("MEASDATE") - pl.col("prev_sub_measdate")).dt.total_days() / 365.25
            ).clip(lower_bound=0.0)
        )
        .otherwise(pl.lit(None).cast(pl.Float64))
        .alias("estage_max_violation"),
    ])

    df = df.drop(["first_measdate", "prev_sub_measdate"])

    # --- Anchor selection via shared helper ---
    cand_est = (
        df.filter(pl.col("estage_raw").is_not_null())
        .select(TREE_ID + ["MEASDATE", "INVYR",
                            pl.col("estage_raw").cast(pl.Float64).alias("src_age")])
    )

    if cand_est.height == 0:
        print("[step6d] no estage estimates — skipping propagation")
        return df.with_columns([
            pl.lit(None).cast(pl.Float64).alias("estage_anchor_value"),
            pl.lit(None).cast(pl.Date).alias("estage_anchor_measdate"),
            pl.lit(None).cast(pl.Int32).alias("estage_anchor_invyr"),
            pl.lit(None).cast(pl.Int32).alias("estage"),
        ])

    anchors_est = _select_best_anchor(cand_est, tree_bounds, apply_min_shift=True)

    # Retrieve INVYR at anchor date for the diagnostic column
    anchor_invyr = (
        cand_est
        .join(
            anchors_est.select(TREE_ID + ["anchor_date"]),
            left_on=TREE_ID + ["MEASDATE"],
            right_on=TREE_ID + ["anchor_date"],
            how="inner",
        )
        .select(TREE_ID + ["INVYR"])
        .rename({"INVYR": "estage_anchor_invyr"})
    )

    df = df.join(
        anchors_est.select(TREE_ID + ["anchor_date", "anchor_age", "anchor_src_age"])
                   .rename({"anchor_date":    "estage_anchor_measdate",
                            "anchor_src_age": "estage_anchor_value"}),
        on=TREE_ID, how="left",
    ).join(anchor_invyr, on=TREE_ID, how="left")

    _propagated = (
        pl.col("anchor_age") +
        (pl.col("MEASDATE") - pl.col("estage_anchor_measdate")).dt.total_days() / 365.25
    )
    df = df.with_columns([
        # Float — used in the estimated_age priority chain; no quantization
        pl.when(pl.col("estage_anchor_value").is_not_null())
        .then(_propagated)
        .otherwise(pl.lit(None).cast(pl.Float64))
        .alias("age_from_estage"),

        # Rounded int — kept for diagnostics and discrepancy flags only
        pl.when(pl.col("estage_anchor_value").is_not_null())
        .then(_propagated.round(0).cast(pl.Int32))
        .otherwise(pl.lit(None).cast(pl.Int32))
        .alias("estage"),
    ]).drop("anchor_age")

    n_shifted = anchors_est.filter(pl.col("min_shift") > 0).height
    n_maxviol = anchors_est.filter(pl.col("max_viol") > 0).height
    n_prop    = df.filter(pl.col("estage").is_not_null()).height
    print(f"[step6d] estage: {anchors_est.height:,} trees anchored, "
          f"{n_prop:,} measurements propagated")
    print(f"         min-age shifts: {n_shifted:,}  |  "
          f"residual max-age violations: {n_maxviol:,}")

    return df


# =============================================================================
# Step 6e: Age source discrepancy flags
# =============================================================================

def flag_age_discrepancies(df: pl.DataFrame, threshold: float = 5.0) -> pl.DataFrame:
    """
    For each measurement where two age sources are both non-null, compute the
    signed difference (a - b) and flag whether |diff| > threshold years.

    Pairs checked:
      estage      vs age_from_stdage  →  age_diff_estage_stdage, age_flag_estage_stdage
      estage      vs age_from_totage  →  age_diff_estage_totage, age_flag_estage_totage
      age_from_totage vs age_from_stdage → age_diff_totage_stdage, age_flag_totage_stdage
    """
    pairs = [
        ("estage",         "age_from_stdage", "estage_stdage"),
        ("estage",         "age_from_totage", "estage_totage"),
        ("age_from_totage","age_from_stdage", "totage_stdage"),
    ]
    exprs = []
    for a, b, tag in pairs:
        diff = (pl.col(a).cast(pl.Float64) - pl.col(b).cast(pl.Float64))
        exprs += [
            pl.when(pl.col(a).is_not_null() & pl.col(b).is_not_null())
            .then(diff)
            .otherwise(pl.lit(None).cast(pl.Float64))
            .alias(f"age_diff_{tag}"),

            pl.when(pl.col(a).is_not_null() & pl.col(b).is_not_null())
            .then(diff.abs() > threshold)
            .otherwise(pl.lit(None).cast(pl.Boolean))
            .alias(f"age_flag_{tag}"),
        ]
    df = df.with_columns(exprs)

    print(f"[step6e] age discrepancies (|diff| > {threshold:.0f} yr):")
    for a, b, tag in pairs:
        col = f"age_flag_{tag}"
        n_both = df.filter(pl.col(col).is_not_null()).height
        n_disc = df.filter(pl.col(col).fill_null(False)).height
        pct = 100 * n_disc / n_both if n_both else 0.0
        print(f"  {a:20s} vs {b:20s}: "
              f"{n_both:>10,} with both, {n_disc:>8,} discrepant ({pct:.1f}%)")
    return df


# =============================================================================
# Step 7: Scaffold + Snapshots
# =============================================================================

SUBPLOT_INVYR = SUBPLOT_ID + ["INVYR"]


def build_subplot_year_scaffold(con) -> pl.DataFrame:
    """
    Build a scaffold of ALL (subplot, measurement-year) pairs for longitudinal
    subplots, with condition-level flags aggregated to the subplot level.

    This ensures that measurement years with zero live trees (e.g. post-clearcut)
    still appear in the snapshot with biomass=0 and the disturbance/treatment
    context preserved.
    """
    scaffold = con.execute("""
    WITH excl_counts AS (
        -- Count meaningful excluded trees per subplot-year
        -- (only trees whose exclusion cost us real observed data)
        SELECT
            T.STATECD, T.UNITCD, T.COUNTYCD, T.PLOT, T.SUBP, T.INVYR,
            COUNT(*) AS n_excluded_meaningful
        FROM TREE T
        JOIN excluded_tree_ids E
            ON  E.STATECD  = T.STATECD AND E.UNITCD  = T.UNITCD
            AND E.COUNTYCD = T.COUNTYCD AND E.PLOT   = T.PLOT
            AND E.SUBP     = T.SUBP    AND E.TREE    = T.TREE
            AND E.is_meaningful = TRUE
        JOIN clean_subplot_years CSY
            ON  CSY.STATECD  = T.STATECD AND CSY.UNITCD  = T.UNITCD
            AND CSY.COUNTYCD = T.COUNTYCD AND CSY.PLOT   = T.PLOT
            AND CSY.INVYR    = T.INVYR    AND CSY.SUBP   = T.SUBP
        GROUP BY T.STATECD, T.UNITCD, T.COUNTYCD, T.PLOT, T.SUBP, T.INVYR
    )
    SELECT
        BSC.STATECD, BSC.UNITCD, BSC.COUNTYCD, BSC.PLOT, BSC.SUBP,
        BSC.INVYR, BSC.MEASDATE,
        -- aggregate condition flags across conditions on this subplot-year
        -- TRUE if ANY condition on the subplot has the flag
        BOOL_OR(BSC.disturbed)        AS any_disturbed,
        BOOL_OR(BSC.cut)              AS any_cut,
        BOOL_OR(BSC.artificial_regen) AS any_artificial_regen,
        BOOL_OR(BSC.natural_regen)    AS any_natural_regen,
        BOOL_OR(BSC.silvicultural)    AS any_silvicultural,
        -- condition context: take dominant condition's values
        -- (for single-cond subplots this is exact; for multi-cond it's approximate)
        MODE(BSC.FORTYPCD)   AS fortypcd,
        MODE(BSC.STDORGCD)   AS stdorgcd,
        MAX(BSC.SICOND)      AS sicond,
        MAX(BSC.STDAGE)      AS max_stdage,
        -- number of conditions on this subplot-year
        COUNT(DISTINCT BSC.CONDID) AS n_conditions,
        -- meaningful excluded trees (real data we had to discard)
        COALESCE(MAX(EC.n_excluded_meaningful), 0)      AS n_excluded_meaningful,
        COALESCE(MAX(EC.n_excluded_meaningful), 0) > 0  AS has_excluded_trees
    FROM base_subplot_cond BSC
    JOIN clean_subplot_years CSY
        ON  CSY.STATECD  = BSC.STATECD AND CSY.UNITCD = BSC.UNITCD
        AND CSY.COUNTYCD = BSC.COUNTYCD AND CSY.PLOT = BSC.PLOT
        AND CSY.INVYR    = BSC.INVYR AND CSY.SUBP = BSC.SUBP
    JOIN longitudinal_subplots LS
        ON  LS.STATECD  = BSC.STATECD AND LS.UNITCD = BSC.UNITCD
        AND LS.COUNTYCD = BSC.COUNTYCD AND LS.PLOT = BSC.PLOT
        AND LS.SUBP     = BSC.SUBP
    LEFT JOIN excl_counts EC
        ON  EC.STATECD  = BSC.STATECD AND EC.UNITCD  = BSC.UNITCD
        AND EC.COUNTYCD = BSC.COUNTYCD AND EC.PLOT   = BSC.PLOT
        AND EC.SUBP     = BSC.SUBP    AND EC.INVYR   = BSC.INVYR
    WHERE BSC.COND_STATUS_CD <> 3
    GROUP BY BSC.STATECD, BSC.UNITCD, BSC.COUNTYCD, BSC.PLOT, BSC.SUBP,
             BSC.INVYR, BSC.MEASDATE
    """).pl()

    print(f"[step7a] subplot-year scaffold: {scaffold.height:,} rows")
    return scaffold


def build_snapshots(df: pl.DataFrame, scaffold: pl.DataFrame) -> pl.DataFrame:
    """
    Aggregate live trees to SPCD × estimated_age × biomass_per_acre
    per subplot per measurement year.

    LEFT JOINs tree aggregation onto the scaffold so that measurement years
    with zero live trees (post-clearcut, post-disturbance) appear as rows
    with biomass=0 and disturbance/treatment flags preserved.
    """
    # Aggregate tree data per subplot-year-species-age
    tree_agg = (
        df.filter(
            (pl.col("STATUSCD") == 1) &
            pl.col("DRYBIO_AG").is_not_null()
        )
        .with_columns(
            (pl.col("DRYBIO_AG") * pl.col("TPA_UNADJ")).alias("biomass_contrib")
        )
        .group_by(
            SUBPLOT_INVYR + ["MEASDATE", "SPCD", "SPGRPCD",
                             "estimated_age", "age_source"]
        )
        .agg([
            pl.col("biomass_contrib").sum().alias("biomass_per_acre_lbs"),
            pl.len().alias("n_trees"),
            pl.col("DIA").mean().alias("mean_dia"),
            pl.col("HT").mean().alias("mean_ht"),
        ])
    )

    # Also compute mortality/harvest summary per subplot-year
    fate_agg = (
        df.filter(pl.col("STATUSCD").is_in([2, 3]))
        .group_by(SUBPLOT_INVYR)
        .agg([
            pl.len().alias("n_dead_or_removed"),
            (pl.col("tree_fate").str.starts_with("harvest")).sum()
                .alias("n_harvested"),
            (pl.col("tree_fate") == "dead").sum().alias("n_natural_dead"),
        ])
    )

    # Left join: scaffold drives, tree agg fills in
    # First, identify subplot-years WITH live trees vs WITHOUT
    subp_years_with_trees = tree_agg.select(SUBPLOT_INVYR).unique()

    # Subplot-years with trees: join scaffold + tree_agg (one-to-many on species/age)
    snapshots_with = (
        scaffold.join(subp_years_with_trees, on=SUBPLOT_INVYR, how="semi")
        .join(tree_agg, on=SUBPLOT_INVYR + ["MEASDATE"], how="left")
        .join(fate_agg, on=SUBPLOT_INVYR, how="left")
    )

    # Subplot-years without any live trees: zero-biomass rows
    snapshots_empty = (
        scaffold.join(subp_years_with_trees, on=SUBPLOT_INVYR, how="anti")
        .with_columns([
            pl.lit(None).cast(pl.Int32).alias("SPCD"),
            pl.lit(None).cast(pl.Int32).alias("SPGRPCD"),
            pl.lit(None).cast(pl.Float64).alias("estimated_age"),
            pl.lit(None).cast(pl.String).alias("age_source"),
            pl.lit(0.0).alias("biomass_per_acre_lbs"),
            pl.lit(0).cast(pl.UInt32).alias("n_trees"),
            pl.lit(None).cast(pl.Float64).alias("mean_dia"),
            pl.lit(None).cast(pl.Float64).alias("mean_ht"),
        ])
        .join(fate_agg, on=SUBPLOT_INVYR, how="left")
    )

    snapshots = pl.concat([snapshots_with, snapshots_empty], how="diagonal_relaxed")
    snapshots = snapshots.sort(SUBPLOT_ID + ["MEASDATE", "SPCD", "estimated_age"])

    # Fill nulls in mortality columns for clean rows
    snapshots = snapshots.with_columns([
        pl.col("n_dead_or_removed").fill_null(0),
        pl.col("n_harvested").fill_null(0),
        pl.col("n_natural_dead").fill_null(0),
    ])

    n = snapshots.height
    n_empty = snapshots_empty.height
    n_aged = snapshots.filter(pl.col("estimated_age").is_not_null()).height
    print(f"[step7b] snapshots: {n:,} rows")
    print(f"         with live trees: {n - n_empty:,}, zero-biomass: {n_empty:,}")
    print(f"         aged: {n_aged:,}, unaged: {n - n_aged - n_empty:,}")
    return snapshots


# =============================================================================
# Diagnostics
# =============================================================================

def diagnostics(df: pl.DataFrame, snapshots: pl.DataFrame):
    """Print key diagnostics for EDA."""
    print("\n" + "=" * 70)
    print("DIAGNOSTICS")
    print("=" * 70)

    # --- Age source coverage (unique trees) ---
    print("\n--- Age source coverage (unique trees) ---")
    tree_age = (
        df.group_by(TREE_ID)
        .agg([
            pl.col("age_from_totage").is_not_null().any().alias("has_totage"),
            pl.col("age_from_stdage").is_not_null().any().alias("has_stdage"),
            pl.col("estage").is_not_null().any().alias("has_estage"),
        ])
        .with_columns(
            (pl.col("has_totage") | pl.col("has_stdage") | pl.col("has_estage"))
            .alias("has_any")
        )
    )
    n_trees     = tree_age.height
    n_with_any  = tree_age.filter(pl.col("has_any")).height
    n_no_age    = n_trees - n_with_any
    pct_any     = 100 * n_with_any / n_trees if n_trees else 0
    print(f"  Unique trees:                 {n_trees:>10,}")
    print(f"  At least one age source:      {n_with_any:>10,}  ({pct_any:.1f}%)")
    print(f"  No age source:                {n_no_age:>10,}  ({100-pct_any:.1f}%)")
    print(f"  By source (trees with ≥1 meas having it):")
    for col, label in [("has_totage", "TOTAGE"),
                       ("has_stdage", "STDAGE"),
                       ("has_estage", "estage")]:
        n = tree_age.filter(pl.col(col)).height
        print(f"    {label:8s}: {n:>10,}  ({100*n/n_trees:.1f}%)")

    # --- DRYBIO_AG coverage (live tree-measurements) ---
    print("\n--- DRYBIO_AG coverage (live trees, STATUSCD=1) ---")
    live = df.filter(pl.col("STATUSCD") == 1)
    n_live_meas    = live.height
    n_no_bio_meas  = live.filter(pl.col("DRYBIO_AG").is_null()).height
    pct_no_bio     = 100 * n_no_bio_meas / n_live_meas if n_live_meas else 0
    tree_bio = (
        live.group_by(TREE_ID)
        .agg(pl.col("DRYBIO_AG").is_not_null().any().alias("has_bio"))
    )
    n_live_trees   = tree_bio.height
    n_never_bio    = tree_bio.filter(~pl.col("has_bio")).height
    pct_never_bio  = 100 * n_never_bio / n_live_trees if n_live_trees else 0
    print(f"  Live tree-measurements:       {n_live_meas:>10,}")
    print(f"    missing DRYBIO_AG:          {n_no_bio_meas:>10,}  ({pct_no_bio:.1f}%)")
    print(f"  Unique live trees:            {n_live_trees:>10,}")
    print(f"    no DRYBIO_AG in any meas:   {n_never_bio:>10,}  ({pct_never_bio:.1f}%)")

    # --- TOTAGE availability by top species ---
    print("\n--- TOTAGE availability (top 20 SPCD by count, live trees) ---")
    stats = (
        df.filter(pl.col("STATUSCD") == 1)
        .group_by("SPCD")
        .agg([
            pl.len().alias("n"),
            (pl.col("TOTAGE").is_not_null()).sum().alias("n_totage"),
            (pl.col("age_from_stdage").is_not_null()).sum().alias("n_stdage"),
        ])
        .with_columns([
            (100.0 * pl.col("n_totage") / pl.col("n")).round(1).alias("pct_totage"),
            (100.0 * pl.col("n_stdage") / pl.col("n")).round(1).alias("pct_stdage"),
        ])
        .sort("n", descending=True)
        .head(20)
    )
    print(f"  {'SPCD':>6}  {'n':>10}  {'TOTAGE':>8}  {'%':>5}  {'STDAGE':>8}  {'%':>5}")
    for r in stats.iter_rows(named=True):
        print(f"  {r['SPCD']:>6}  {r['n']:>10,}  {r['n_totage']:>8,}  "
              f"{r['pct_totage']:>5.1f}  {r['n_stdage']:>8,}  {r['pct_stdage']:>5.1f}")

    # --- harvest inference ---
    print("\n--- Harvest vs natural mortality ---")
    dead = df.filter(pl.col("STATUSCD").is_in([2, 3]))
    fate_stats = dead.group_by("tree_fate").len().sort("len", descending=True)
    for r in fate_stats.iter_rows():
        print(f"  {r[0]:25s}: {r[1]:>10,}")

    # --- STDORGSP match rate in planted conditions ---
    print("\n--- STDORGSP match rate (planted conditions) ---")
    planted = df.filter(pl.col("cond_stdorgcd") == 1)
    if planted.height > 0:
        n_match = planted.filter(pl.col("matches_stdorgsp")).height
        print(f"  Trees in STDORGCD=1 conditions: {planted.height:,}")
        print(f"  Matching STDORGSP:               {n_match:,} "
              f"({100*n_match/planted.height:.1f}%)")
        print(f"  Other species (ingrowth/regen):   {planted.height - n_match:,} "
              f"({100*(planted.height - n_match)/planted.height:.1f}%)")

    # --- STDAGE trajectory anomalies (all conditions, by origin) ---
    print("\n--- STDAGE trajectory anomalies (all conditions) ---")
    all_with_stdage = (
        df.select(["STATECD", "UNITCD", "COUNTYCD", "PLOT", "CONDID",
                   "MEASDATE", "cond_stdage", "cond_stdorgcd"])
        .unique(subset=["STATECD", "UNITCD", "COUNTYCD", "PLOT", "CONDID", "MEASDATE"])
        .filter(pl.col("cond_stdage").is_not_null() & (pl.col("cond_stdage") > 0))
        .sort(["STATECD", "UNITCD", "COUNTYCD", "PLOT", "CONDID", "MEASDATE"])
    )
    cond_id_cols = ["STATECD", "UNITCD", "COUNTYCD", "PLOT", "CONDID"]
    if all_with_stdage.height > 0:
        consec = all_with_stdage.with_columns([
            pl.col("cond_stdage").shift(-1).over(cond_id_cols).alias("next_stdage"),
            pl.col("MEASDATE").shift(-1).over(cond_id_cols).alias("next_measdate"),
        ]).filter(pl.col("next_stdage").is_not_null())

        consec = consec.with_columns([
            ((pl.col("next_measdate") - pl.col("MEASDATE")).dt.total_days() / 365.25)
                .alias("interval"),
            (pl.col("next_stdage") - pl.col("cond_stdage")).alias("delta_stdage"),
        ])

        resets = consec.filter(pl.col("delta_stdage") < 0)
        large_discrepancy = consec.filter(
            (pl.col("delta_stdage") - pl.col("interval")).abs() > 3
        )
        print(f"  Consecutive condition-measurement pairs: {consec.height:,}")
        print(f"  STDAGE decreased (resets):                {resets.height:,}")
        print(f"  |delta_stdage - interval| > 3yr:         {large_discrepancy.height:,}")

        # Breakdown by origin
        for orgcd, label in [(1, "planted (STDORGCD=1)"), (0, "natural (STDORGCD=0)")]:
            r_sub = resets.filter(pl.col("cond_stdorgcd") == orgcd)
            d_sub = large_discrepancy.filter(pl.col("cond_stdorgcd") == orgcd)
            print(f"    {label}: {r_sub.height:,} resets, {d_sub.height:,} large discrepancies")

        if resets.height > 0:
            print("\n  Sample STDAGE resets (up to 10):")
            sample = resets.head(10)
            for r in sample.iter_rows(named=True):
                org_label = "planted" if r["cond_stdorgcd"] == 1 else "natural"
                print(f"    [{org_label}] PLOT={r['PLOT']} COND={r['CONDID']} "
                      f"{r['MEASDATE']}→{r['next_measdate']}: "
                      f"STDAGE {r['cond_stdage']}→{r['next_stdage']} "
                      f"(Δ={r['delta_stdage']})")

    # --- disturbance and age relationship ---
    print("\n--- Disturbance x age coverage ---")
    live = df.filter(pl.col("STATUSCD") == 1)
    dist_any = (
        (pl.col("DSTRBCD1").is_not_null() & (pl.col("DSTRBCD1") != 0)) |
        (pl.col("DSTRBCD2").is_not_null() & (pl.col("DSTRBCD2") != 0)) |
        (pl.col("DSTRBCD3").is_not_null() & (pl.col("DSTRBCD3") != 0))
    )
    cut_any = (
        (pl.col("TRTCD1") == 10) | (pl.col("TRTCD2") == 10) | (pl.col("TRTCD3") == 10)
    )
    regen_any = (
        (pl.col("TRTCD1").is_in([30, 40])) |
        (pl.col("TRTCD2").is_in([30, 40])) |
        (pl.col("TRTCD3").is_in([30, 40]))
    )
    for label, mask in [("undisturbed", ~dist_any & ~cut_any),
                        ("disturbed (non-cut)", dist_any & ~cut_any),
                        ("cut (TRTCD=10)", cut_any),
                        ("regen (TRTCD=30/40)", regen_any)]:
        sub = live.filter(mask)
        n_aged = sub.filter(pl.col("estimated_age").is_not_null()).height
        n_totage = sub.filter(pl.col("age_source") == "TOTAGE").height
        n_planted = sub.filter(
            pl.col("age_source").is_not_null() &
            pl.col("age_source").str.contains("planted")
        ).height
        n_reset = sub.filter(
            pl.col("age_source").is_not_null() &
            pl.col("age_source").str.contains("reset")
        ).height
        pct = 100 * n_aged / sub.height if sub.height > 0 else 0
        print(f"  {label:30s}: {sub.height:>10,} trees, {n_aged:>10,} aged ({pct:.1f}%)")
        if n_aged > 0:
            print(f"    {'':30s}  TOTAGE={n_totage:,}  STDAGE_planted={n_planted:,}  "
                  f"STDAGE_reset={n_reset:,}")

    # --- zero-biomass subplot-years analysis ---
    print("\n--- Zero-biomass subplot-years (from scaffold) ---")
    empty = snapshots.filter(pl.col("n_trees") == 0)
    if empty.height > 0:
        print(f"  Total zero-biomass subplot-years: {empty.height:,}")
        for flag, label in [("any_cut", "TRTCD=10 (cutting)"),
                            ("any_disturbed", "disturbed"),
                            ("any_artificial_regen", "artificial regen"),
                            ("any_natural_regen", "natural regen")]:
            if flag in empty.columns:
                n_flag = empty.filter(pl.col(flag).fill_null(False)).height
                print(f"    with {label:25s}: {n_flag:>8,} "
                      f"({100*n_flag/empty.height:.1f}%)")
        # mortality context on zero-biomass years
        n_with_dead = empty.filter(pl.col("n_dead_or_removed") > 0).height
        n_with_harvest = empty.filter(pl.col("n_harvested") > 0).height
        print(f"    with dead/removed trees:        {n_with_dead:>8,}")
        print(f"    with harvested trees:            {n_with_harvest:>8,}")
        unexplained = empty.filter(
            (pl.col("n_dead_or_removed") == 0) &
            ~pl.col("any_cut").fill_null(False) &
            ~pl.col("any_disturbed").fill_null(False)
        )
        n_unexplained = unexplained.height
        print(f"    unexplained (no dead, no flags): {n_unexplained:>8,}")

        if n_unexplained > 0:
            # Break unexplained zeros down by whether the subplot ever had live trees
            # and whether it had live trees specifically BEFORE this zero-biomass year.
            live_dates = (
                snapshots.filter(pl.col("n_trees") > 0)
                .group_by(SUBPLOT_ID)
                .agg([
                    pl.col("MEASDATE").min().alias("first_live_measdate"),
                    pl.col("MEASDATE").max().alias("last_live_measdate"),
                ])
            )
            unexp_detail = (
                unexplained.select(SUBPLOT_ID + ["MEASDATE"])
                .join(live_dates, on=SUBPLOT_ID, how="left")
                .with_columns([
                    pl.col("last_live_measdate").is_not_null().alias("ever_had_trees"),
                    (
                        pl.col("last_live_measdate").is_not_null() &
                        (pl.col("last_live_measdate") < pl.col("MEASDATE"))
                    ).alias("had_prior_live_trees"),
                    (
                        pl.col("first_live_measdate").is_not_null() &
                        (pl.col("first_live_measdate") > pl.col("MEASDATE"))
                    ).alias("trees_only_after"),
                ])
            )
            n_had_prior  = unexp_detail.filter(pl.col("had_prior_live_trees")).height
            n_only_after = unexp_detail.filter(pl.col("trees_only_after")).height
            n_never      = unexp_detail.filter(~pl.col("ever_had_trees")).height
            print(f"      of which, subplot had live trees before:  {n_had_prior:>8,} "
                  f"({100*n_had_prior/n_unexplained:.1f}%)  ← trees vanished undetected")
            print(f"      of which, subplot has live trees after:   {n_only_after:>8,} "
                  f"({100*n_only_after/n_unexplained:.1f}%)  ← pre-establishment zeros")
            print(f"      of which, subplot never had live trees:   {n_never:>8,} "
                  f"({100*n_never/n_unexplained:.1f}%)  ← likely non-forest")
    else:
        print("  No zero-biomass subplot-years found.")

    # --- Age source discrepancies ---
    print("\n--- Age source discrepancies (|diff| > 5 yr, measurement level) ---")
    pairs = [
        ("estage",          "age_from_stdage", "estage_stdage"),
        ("estage",          "age_from_totage", "estage_totage"),
        ("age_from_totage", "age_from_stdage", "totage_stdage"),
    ]
    for a, b, tag in pairs:
        col = f"age_flag_{tag}"
        if col not in df.columns:
            print(f"  {a} vs {b}: flag column not present (run flag_age_discrepancies)")
            continue
        with_both = df.filter(pl.col(col).is_not_null())
        n_both = with_both.height
        n_disc = with_both.filter(pl.col(col)).height
        pct    = 100 * n_disc / n_both if n_both else 0.0
        med_diff = (
            with_both.filter(pl.col(col))
            .select(pl.col(f"age_diff_{tag}").abs().median())
            .item()
        )
        print(f"  {a:20s} vs {b:20s}: "
              f"{n_both:>10,} with both, {n_disc:>8,} discrepant ({pct:.1f}%), "
              f"median |diff|={med_diff:.0f} yr")

        # For TOTAGE vs STDAGE: break down by stdage_context AND by whether the
        # STDAGE anchor was set before vs at/after the tree's first appearance.
        #
        # Trees that carried their TREE number through a reset should have a
        # pre-reset anchor (stdage_anchor_year < tree's first MEASDATE would be
        # impossible — anchor can't predate first appearance). Instead the key
        # question is: was the anchor set at the FIRST measurement ever (suggesting
        # the tree only appeared post-reset and got a young stand age), or did the
        # tree exist in an earlier measurement?
        #
        # Proxy: stdage_anchor_year == tree's overall first MEASDATE (from
        # estage_first_measdate year) → anchor is the tree's debut → likely
        # post-reset anchor for a remnant that wasn't previously tallied.
        if tag == "totage_stdage" and "stdage_context" in df.columns:
            print(f"    breakdown by stdage_context:")
            for ctx in ["planted", "confirmed_reset", None]:
                if ctx is None:
                    sub = with_both.filter(pl.col("stdage_context").is_null())
                    label = "no_context (propagated outside eligible meas)"
                else:
                    sub = with_both.filter(pl.col("stdage_context") == ctx)
                    label = ctx
                if sub.height == 0:
                    continue
                n_s = sub.height
                n_d = sub.filter(pl.col(col)).height
                med = (
                    sub.filter(pl.col(col))
                    .select(pl.col(f"age_diff_{tag}").abs().median())
                    .item() if n_d > 0 else float("nan")
                )
                print(f"      {label:45s}: {n_s:>8,} rows, "
                      f"{n_d:>6,} discrepant ({100*n_d/n_s:.1f}%), "
                      f"median |diff|={med:.0f} yr")

            # Among confirmed_reset discrepants: was the anchor pre- or post-reset?
            # Uses stdage_post_reset_anchor (anchor_year >= cond's first reset year)
            # which correctly classifies carry-forwards vs post-reset entries.
            if "stdage_post_reset_anchor" in df.columns:
                cr_disc = with_both.filter(
                    pl.col(col) & (pl.col("stdage_context") == "confirmed_reset")
                )
                if cr_disc.height > 0:
                    n_post_anchor = cr_disc.filter(pl.col("stdage_post_reset_anchor")).height
                    n_pre_anchor  = cr_disc.height - n_post_anchor
                    print(f"    of confirmed_reset discrepants ({cr_disc.height:,}):")
                    print(f"      post-reset anchor (anchor ≥ reset year):  "
                          f"{n_post_anchor:>8,} ({100*n_post_anchor/cr_disc.height:.1f}%)"
                          f"  ← young stand-age on old remnant/correction artifact")
                    print(f"      pre-reset anchor  (carry-forward, safe):  "
                          f"{n_pre_anchor:>8,} ({100*n_pre_anchor/cr_disc.height:.1f}%)"
                          f"  ← anchor pre-dates reset; discrepancy from STDAGE field correction")


# =============================================================================
# Pipeline
# =============================================================================

def run_pipeline(con, estage_dir: str = None):
    """
    Run the full curation pipeline.

    Args:
        con:        open DuckDB connection to the FIA database
        estage_dir: directory containing *_TREEAGE.csv files (step 6d).
                    If None, estage columns are added as all-null.
    """
    print("=" * 70)
    print("FIA Curation Pipeline v2")
    print("=" * 70)

    # SQL steps
    sql_base_measurements(con)
    sql_clean_subplot_years(con)
    sql_longitudinal_subplots(con)
    sql_excluded_trees(con)

    # Polars steps
    df = load_trees(con)
    df, confusion_edges = analyze_spcd_confusion(df)
    df = handle_reconcile5(df)
    df = classify_tree_fate(df)

    # All subplot measurement years — loaded early so tree_bounds can use them
    # for max-age violation bounding across all age sources.
    all_subplot_years = con.execute("""
        SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, MEASDATE
        FROM clean_subplot_years
    """).pl()
    tree_bounds = _compute_tree_bounds(df, all_subplot_years)

    df = propagate_totage(df, tree_bounds)
    df = assign_stdage(df, tree_bounds)
    df = merge_age_estimates(df)

    # Step 6d: external age estimates
    if estage_dir is not None:
        estage_raw = load_estage_csv(estage_dir)
        df = process_estage(df, estage_raw, tree_bounds)
    else:
        df = df.with_columns([
            pl.lit(None).cast(pl.Utf8).alias("estage_raw"),
            pl.lit(None).cast(pl.Float64).alias("estage_min_violation"),
            pl.lit(None).cast(pl.Float64).alias("estage_max_violation"),
            pl.lit(None).cast(pl.Float64).alias("age_from_estage"),
            pl.lit(None).cast(pl.Int32).alias("estage"),
            pl.lit(None).cast(pl.Int32).alias("estage_anchor_invyr"),
        ])

    # Step 6e: cross-source age discrepancy flags
    df = flag_age_discrepancies(df)

    scaffold = build_subplot_year_scaffold(con)

    # Filter scaffold to subplots that ever held at least one live curated tree
    # WITH a recorded DRYBIO_AG — matching the condition used in build_snapshots'
    # tree_agg (STATUSCD=1 AND DRYBIO_AG is not null). Subplots whose only live
    # trees all lack DRYBIO_AG would otherwise pass STATUSCD filter but still
    # show zero biomass in every snapshot row.
    ever_live = (
        df.filter(
            (pl.col("STATUSCD") == 1) & pl.col("DRYBIO_AG").is_not_null()
        )
        .select(SUBPLOT_ID).unique()
    )
    n_scaffold_before = scaffold.height
    scaffold = scaffold.join(ever_live, on=SUBPLOT_ID, how="semi")
    print(f"[filter] scaffold restricted to forested subplots: "
          f"{scaffold.height:,} rows (dropped {n_scaffold_before - scaffold.height:,} "
          f"from {n_scaffold_before - scaffold.height + scaffold.height:,})")

    snapshots = build_snapshots(df, scaffold)

    # Persist curated tree-level df and snapshots into the DuckDB file
    con.register("_df_view", df)
    con.execute("CREATE OR REPLACE TABLE curated_trees AS SELECT * FROM _df_view")
    con.unregister("_df_view")
    con.register("_snapshots_view", snapshots)
    con.execute("CREATE OR REPLACE TABLE curated_snapshots AS SELECT * FROM _snapshots_view")
    con.unregister("_snapshots_view")
    con.commit()
    print(f"[save] curated_trees written to DuckDB ({df.height:,} rows)")
    print(f"[save] curated_snapshots written to DuckDB ({snapshots.height:,} rows)")

    diagnostics(df, snapshots)

    print("\n[done] Returns: (curated_df, snapshots_df, confusion_edges_df)")
    print("       SQL tables: base_subplot_cond, clean_subplot_years,")
    print("       longitudinal_subplots, excluded_tree_ids,")
    print("       curated_trees, curated_snapshots")
    return df, snapshots, confusion_edges


if __name__ == "__main__":
    import sys
    db_path = sys.argv[1] if len(sys.argv) > 1 else "fiadb.duckdb"
    estage_dir = sys.argv[2] if len(sys.argv) > 2 else None
    con = duckdb.connect(db_path)
    df, snapshots, confusion_edges = run_pipeline(con, estage_dir=estage_dir)
