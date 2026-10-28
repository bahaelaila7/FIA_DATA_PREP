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

import os as _os, sys as _sys  # curation-fix: sibling modules resolve under `python -I`
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
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

def scope_sql(states=None, counties=None, alias="P") -> str:
    """Optional geographic scope (subset runs): STATECD list and/or (STATECD, COUNTYCD) pairs."""
    parts = []
    if states:
        parts.append(f"{alias}.STATECD IN ({','.join(map(str, states))})")
    if counties:
        parts.append("(" + " OR ".join(
            f"({alias}.STATECD={s} AND {alias}.COUNTYCD={c})" for s, c in counties) + ")")
    return (" AND " + " AND ".join(parts)) if parts else ""


def sql_base_measurements(con, scope: str = ""):
    """Step 1: subplot-cond measurements, annual CONUS, non-skipped.

    curation-fix 2026-10-26:
      * SUBP <= 4 (the four subplots of the national plot design). SUBP > 4 rows in annual
        inventories are auxiliary / regional points (101-105, 106+, ...) carrying a NULL
        SUBP_STATUS_CD; they are not part of the P2 sample (audit I3: the old `<> 3` happened
        to drop them only because NULL <> 3 is NULL). SUBP_STATUS_CD is then tested with
        IS DISTINCT FROM 3 (NULL no longer silently decides anything); P2A / null-TPA live
        records are excluded explicitly in load_trees.
      * condition/subplot area proportions are carried (CONDPROP/SUBPPROP/MICRPROP/MACRPROP
        of the condition; SUBPCOND/MICRCOND/MACRCOND_PROP of the subplot x condition).
    """
    con.execute(f"""
    DROP TABLE IF EXISTS base_subplot_cond;
    CREATE TABLE base_subplot_cond AS
    SELECT
        SUBP.STATECD, SUBP.UNITCD, SUBP.COUNTYCD, SUBP.PLOT, SUBP.INVYR, SUBP.SUBP,
        C.CONDID,
        MAKE_DATE(P.MEASYEAR, COALESCE(NULLIF(CAST(P.MEASMON AS INTEGER), 0), 6), COALESCE(NULLIF(CAST(P.MEASDAY AS INTEGER), 0), 15)) AS MEASDATE,
        P.DESIGNCD, P.CYCLE,
        SUBP.SUBP_STATUS_CD,
        C.COND_STATUS_CD,
        C.STDAGE, C.FLDAGE, C.STDORGCD, C.STDORGSP,
        C.FLDSZCD, C.STDSZCD,
        C.SICOND, C.SIBASE, C.SISP,
        C.FORTYPCD, C.SITECLCD,
        C.BALIVE, C.ALSTK, C.ALSTKCD, C.GSSTK, C.GSSTKCD,
        C.PRESNFCD, C.SLOPE, C.ASPECT, P.ELEV, C.OWNGRPCD,
        C.CONDPROP_UNADJ, C.SUBPPROP_UNADJ, C.MICRPROP_UNADJ, C.MACRPROP_UNADJ,
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
        AND SUBP.SUBP <= 4                             -- national design subplots only
        AND SUBP.SUBP_STATUS_CD IS DISTINCT FROM 3     -- nonsampled subplot out (explicit NULL handling)
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
    WHERE TRUE {scope}
    """)
    con.commit()
    n = con.execute("SELECT COUNT(*) FROM base_subplot_cond").fetchone()[0]
    print(f"[step1] base_subplot_cond: {n:,} rows")


def sql_clean_subplot_years(con):
    """Step 2: the subplot-visit scaffold.

    curation-fix 2026-10-26: the old rule dropped a whole subplot-visit when ANY of its
    conditions had COND_STATUS_CD=3. Per FIADB v9.4 COND_STATUS_CD 3 is noncensus WATER,
    not nonforest, so the subplot is kept; trees are filtered by their OWN condition in
    load_trees. The table name is kept for the downstream stages; it now also carries
    has_cond_status3 and the forest (COND_STATUS_CD=1) subplot / microplot proportions.
    """
    con.execute("""
    DROP TABLE IF EXISTS clean_subplot_years;
    CREATE TABLE clean_subplot_years AS
    SELECT STATECD, UNITCD, COUNTYCD, PLOT, INVYR, SUBP, MEASDATE, CYCLE,
           BOOL_OR(COND_STATUS_CD = 3)                                   AS has_cond_status3,
           SUM(CASE WHEN COND_STATUS_CD = 1 THEN SUBPCOND_PROP ELSE 0 END) AS forest_subpcond_prop,
           SUM(CASE WHEN COND_STATUS_CD = 1 THEN MICRCOND_PROP ELSE 0 END) AS forest_micrcond_prop
    FROM base_subplot_cond
    GROUP BY STATECD, UNITCD, COUNTYCD, PLOT, INVYR, SUBP, MEASDATE, CYCLE
    """)
    con.commit()
    n = con.execute("SELECT COUNT(*) FROM clean_subplot_years").fetchone()[0]
    n3 = con.execute("SELECT COUNT(*) FROM clean_subplot_years WHERE has_cond_status3").fetchone()[0]
    print(f"[step2] clean_subplot_years: {n:,} subplot-visits ({n3:,} with a COND_STATUS_CD=3 "
          f"condition — kept, previously dropped)")


def sql_longitudinal_subplots(con):
    """Step 3: visit count per subplot.

    curation-fix 2026-10-26: the old `HAVING COUNT(*) >= 2` (>= 2 clean visits) is removed
    from curation. Every subplot is kept; n_measurements is exported to curated_trees as
    n_clean_visits so downstream consumers filter themselves.
    """
    con.execute("""
    DROP TABLE IF EXISTS longitudinal_subplots;
    CREATE TABLE longitudinal_subplots AS
    SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP,
           COUNT(*)      AS n_measurements,
           MIN(MEASDATE) AS first_measdate,
           MAX(MEASDATE) AS last_measdate,
    FROM clean_subplot_years
    GROUP BY STATECD, UNITCD, COUNTYCD, PLOT, SUBP
    """)
    con.commit()
    n = con.execute("SELECT COUNT(*) FROM longitudinal_subplots").fetchone()[0]
    print(f"[step3] subplots (no >=2-visit filter): {n:,}")
    dist = con.execute("""
        SELECT n_measurements, COUNT(*) FROM longitudinal_subplots
        GROUP BY n_measurements ORDER BY n_measurements
    """).fetchall()
    for nm, cnt in dist:
        print(f"        {nm} meas: {cnt:,}")


# RECONCILECD meanings (FIADB v9.4 §3.1.82). The OLD labels in sql_excluded_trees were wrong
# (3 'not_found', 6 'state_change', 7 'prev_missed', 8 'macroplot_removal', 9 'other').
RECONCILE_LABELS = {
    1: "ingrowth", 2: "through_growth", 3: "missed_live", 4: "missed_dead", 5: "shrank",
    6: "physical_movement", 7: "cruiser_error", 8: "procedural_change", 9: "nonsampled_area",
}
# New-tree codes that are NOT growth into the tally (tree present but untallied, moved in,
# or in a previously nonsampled area) -> reconcile_new / not_ingrowth flag.
RECONCILE_NEW_CODES = (3, 4, 6, 7, 8, 9)
# STATUSCD=0 + these codes = the tree leaves observation for a non-mortality reason.
CENSOR_REASON = {5: "shrank", 6: "moved", 8: "procedural", 9: "nonsampled"}


# Silent sapling re-tally detector (audit r2 B2). Some state x MANUAL transitions re-tallied
# the microplot saplings WITHOUT closing the old records (e.g. KY MANUAL 1.0 -> 4.0: 184 plots
# lost all 1,134 saplings with no record at all). A plot-visit is an 'unrecorded' break when
#   (i)  plot tier: >= RETALLY_MIN_SHARE and >= RETALLY_MIN_N of the previous visit's live
#        saplings (SUBP <= 4, own COND_STATUS 1, subplot still sampled forest at the next
#        visit = SUBP_STATUS_CD 1) have NO tree record at all at the next visit, or
#   (ii) transition tier: the (STATECD, prev MANUAL, next MANUAL) transition loses
#        >= RETALLY_MIN_SHARE of its pooled saplings (>= RETALLY_TRANS_MIN_N saplings) and
#        this plot-visit has >= 1 unrecorded sapling (catches the 1-2 sapling plots).
RETALLY_MIN_SHARE   = 0.5
RETALLY_MIN_N       = 3
RETALLY_TRANS_MIN_N = 20


def _sql_procedural_breaks(con, scope: str = ""):
    """procedural_breaks = per (plot, MEASDATE) re-tally visits, break_kind 'rc8' (STATUSCD=0
    + RECONCILECD=8 closures) or 'unrecorded' (silent re-tally, detector above). Also leaves
    _retally_saplings (every previous-visit live sapling with its next-visit status)."""
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE _pv AS
    SELECT P.CN AS PLT_CN, P.STATECD, P.UNITCD, P.COUNTYCD, P.PLOT, P.INVYR, P.MANUAL,
           P.PLOT_STATUS_CD,
           MAKE_DATE(P.MEASYEAR, COALESCE(NULLIF(CAST(P.MEASMON AS INTEGER), 0), 6), COALESCE(NULLIF(CAST(P.MEASDAY AS INTEGER), 0), 15)) AS MEASDATE
    FROM PLOT P JOIN SURVEY SRV ON P.SRV_CN = SRV.CN AND SRV.ANN_INVENTORY = 'Y'
    WHERE P.STATECD <= 56 AND P.STATECD NOT IN (2, 15) {scope};
    CREATE OR REPLACE TEMP TABLE _pv2 AS
    SELECT *, LEAD(PLT_CN) OVER w AS next_cn, LEAD(MEASDATE) OVER w AS next_md,
              LEAD(MANUAL) OVER w AS next_manual, LEAD(PLOT_STATUS_CD) OVER w AS next_ps
    FROM _pv WINDOW w AS (PARTITION BY STATECD, UNITCD, COUNTYCD, PLOT ORDER BY MEASDATE, INVYR);
    CREATE OR REPLACE TEMP TABLE _retally_saplings AS
    SELECT T.STATECD, T.UNITCD, T.COUNTYCD, T.PLOT, T.SUBP, T.TREE, v.MEASDATE, v.next_md,
           v.MANUAL AS prev_manual, v.next_manual, (T2.CN IS NULL) AS unrecorded,
           T2.STATUSCD AS next_statuscd, CAST(T2.RECONCILECD AS INTEGER) AS next_rc
    FROM TREE T
    JOIN _pv2 v ON v.PLT_CN = T.PLT_CN
    JOIN COND C ON C.PLT_CN = T.PLT_CN AND C.CONDID = T.CONDID
    JOIN SUBPLOT SN ON SN.PLT_CN = v.next_cn AND SN.SUBP = T.SUBP AND SN.SUBP_STATUS_CD = 1
    LEFT JOIN TREE T2 ON T2.PLT_CN = v.next_cn AND T2.SUBP = T.SUBP AND T2.TREE = T.TREE
    WHERE T.STATUSCD = 1 AND T.DIA < 5 AND T.TPA_UNADJ > 0 AND T.SUBP <= 4
      AND C.COND_STATUS_CD = 1 AND COALESCE(T.P2A_GRM_FLG, '') <> 'Y'
      AND v.PLOT_STATUS_CD < 3 AND v.next_cn IS NOT NULL AND v.next_ps < 3;
    """)
    con.execute(f"""
    DROP TABLE IF EXISTS retally_transitions;
    CREATE TABLE retally_transitions AS
    SELECT STATECD, prev_manual, next_manual, COUNT(*) AS n_saplings,
           COUNT(*) FILTER (WHERE unrecorded) AS n_unrecorded,
           COUNT(*) FILTER (WHERE unrecorded) / COUNT(*) AS share_unrecorded,
           (COUNT(*) >= {RETALLY_TRANS_MIN_N}
            AND COUNT(*) FILTER (WHERE unrecorded) >= {RETALLY_MIN_SHARE} * COUNT(*)) AS is_retally_transition
    FROM _retally_saplings GROUP BY ALL;
    DROP TABLE IF EXISTS procedural_breaks;
    CREATE TABLE procedural_breaks AS
    WITH rc8 AS (
        SELECT STATECD, UNITCD, COUNTYCD, PLOT, MEASDATE,
               COUNT(*) AS n_closed_rc8,
               COUNT(*) FILTER (WHERE PREVDIA < 5) AS n_closed_rc8_prev_sapling
        FROM tree_status0_events WHERE RC = 8 GROUP BY ALL),
    pv AS (
        SELECT s.STATECD, s.UNITCD, s.COUNTYCD, s.PLOT, s.next_md AS MEASDATE,
               ANY_VALUE(s.prev_manual) AS prev_manual, ANY_VALUE(s.next_manual) AS next_manual,
               COUNT(*) AS n_prev_saplings, COUNT(*) FILTER (WHERE s.unrecorded) AS n_unrecorded_saplings
        FROM _retally_saplings s GROUP BY s.STATECD, s.UNITCD, s.COUNTYCD, s.PLOT, s.next_md),
    unrec AS (
        SELECT pv.*,
               CASE WHEN pv.n_unrecorded_saplings >= {RETALLY_MIN_N}
                         AND pv.n_unrecorded_saplings >= {RETALLY_MIN_SHARE} * pv.n_prev_saplings THEN 'plot'
                    WHEN pv.n_unrecorded_saplings >= 1 AND tr.is_retally_transition THEN 'transition'
               END AS detect_tier
        FROM pv LEFT JOIN retally_transitions tr
          ON tr.STATECD = pv.STATECD AND tr.prev_manual IS NOT DISTINCT FROM pv.prev_manual
         AND tr.next_manual IS NOT DISTINCT FROM pv.next_manual)
    SELECT COALESCE(r.STATECD, u.STATECD) AS STATECD, COALESCE(r.UNITCD, u.UNITCD) AS UNITCD,
           COALESCE(r.COUNTYCD, u.COUNTYCD) AS COUNTYCD, COALESCE(r.PLOT, u.PLOT) AS PLOT,
           COALESCE(r.MEASDATE, u.MEASDATE) AS MEASDATE,
           COALESCE(r.n_closed_rc8, 0) AS n_closed_rc8,
           COALESCE(r.n_closed_rc8_prev_sapling, 0) AS n_closed_rc8_prev_sapling,
           COALESCE(u.n_prev_saplings, 0) AS n_prev_saplings,
           COALESCE(u.n_unrecorded_saplings, 0) AS n_unrecorded_saplings,
           u.detect_tier,
           CASE WHEN r.PLOT IS NOT NULL THEN 'rc8' ELSE 'unrecorded' END AS break_kind
    FROM rc8 r
    FULL JOIN (SELECT * FROM unrec WHERE detect_tier IS NOT NULL) u
      ON u.STATECD=r.STATECD AND u.UNITCD=r.UNITCD AND u.COUNTYCD=r.COUNTYCD
     AND u.PLOT=r.PLOT AND u.MEASDATE=r.MEASDATE
    """)
    # an rc8 break plot-visit also needs its unrecorded-sapling count (they are censored too)
    con.execute("""
    UPDATE procedural_breaks b SET n_prev_saplings = pv.n, n_unrecorded_saplings = pv.u
    FROM (SELECT STATECD, UNITCD, COUNTYCD, PLOT, next_md, COUNT(*) n,
                 COUNT(*) FILTER (WHERE unrecorded) u
          FROM _retally_saplings GROUP BY ALL) pv
    WHERE b.break_kind = 'rc8' AND pv.STATECD=b.STATECD AND pv.UNITCD=b.UNITCD
      AND pv.COUNTYCD=b.COUNTYCD AND pv.PLOT=b.PLOT AND pv.next_md=b.MEASDATE
    """)


def dbconn_has_links(con) -> bool:
    """dbconn's TEMP plot_coord_links (D29) exists on this connection."""
    return con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name = 'plot_coord_links'").fetchone()[0] > 0


def build_tree_reconcile_events(con):
    """Per tree: the RC7 erroneous-tally date and the FIRST / LAST censoring event. Built from
    tree_status0_events / tree_censor_events, so after rekey_tree_events (r7 D36) it is keyed
    by the post-split tree id."""
    con.execute("""
    DROP TABLE IF EXISTS tree_reconcile_events;
    CREATE TABLE tree_reconcile_events AS
    WITH r7 AS (
        SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE,
               MIN(MEASDATE) AS rc7_erroneous_measdate, COUNT(*) AS n_rc7
        FROM tree_status0_events WHERE RC = 7 GROUP BY ALL),
    c AS (
        SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE,
               MIN(MEASDATE)                    AS censor_measdate,
               ARG_MIN(RC, MEASDATE)            AS censor_reconcilecd,
               ARG_MIN(reason, MEASDATE)        AS censor_reason,
               MAX(MEASDATE)                    AS censor_last_measdate,
               ARG_MAX(RC, MEASDATE)            AS censor_last_reconcilecd,
               ARG_MAX(reason, MEASDATE)        AS censor_last_reason,
               COUNT(*)                         AS n_censor_events
        FROM tree_censor_events GROUP BY ALL)
    SELECT * FROM r7 FULL JOIN c USING (STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE)
    """)


def sql_excluded_trees(con, scope: str = ""):
    """
    Step 3b — per-tree reconcile events (curation-fix 2026-10-26).

    OLD behaviour (removed): every record of a tree that EVER had RECONCILECD in
    (3,4,6,7,8,9) or STATUSCD=0 was excluded. Several states re-tallied their microplot
    saplings as new trees (RC3/RC8) at the first annual remeasurement, so that rule dropped
    ~half of the live saplings in AL/GA/NC/SC/TN.

    NEW behaviour, per FIADB v9.4 STATUSCD/RECONCILECD definitions:
      * the STATUSCD=0 record itself is dropped (load_trees);
      * STATUSCD=0 + RECONCILECD=7 ("cruiser error: erroneously tallied at the previous
        inventory") -> the tree's EARLIER records are dropped (it never existed).
        These trees are listed in `excluded_tree_ids` (exclusion_reasons
        'cruiser_error_prev_tally'), the only remaining tree-level exclusion;
      * STATUSCD=0 + RECONCILECD in (5,6,8,9) (or null/other) -> CENSORING: the tree leaves
        observation for a non-mortality reason (shrank / moved / procedural / nonsampled).
        Stored as censor_measdate / censor_reconcilecd in `tree_reconcile_events`;
      * new trees with RECONCILECD 3/4/6/7/8/9 are kept (flagged reconcile_new downstream).

    Tables:
      tree_status0_events    one row per STATUSCD=0 record (tree, MEASDATE, RC) — ALL of them,
                             so a censor -> live -> censor history is not collapsed (audit M8)
      tree_reconcile_events  per tree: rc7_erroneous_measdate, FIRST and LAST censoring
      procedural_breaks      per (plot, MEASDATE) re-tally visit; break_kind 'rc8'
                             (STATUSCD=0 + RECONCILECD=8 closures; keyed per PLOT because the
                             AL/GA MANUAL 1.5-2.0 sapling re-tally was plot-wide) or
                             'unrecorded' (silent re-tally, _sql_procedural_breaks)
      retally_transitions    per state x (prev MANUAL, next MANUAL): pooled unrecorded share
      tree_censor_events     every censoring event (tree, MEASDATE, RC, reason): STATUSCD=0
                             non-RC7 records + saplings left UNRECORDED at a break
                             (reason 'procedural_unrecorded')
      tree_landuse_exits     live records on a nonforest/water own condition (M6); r7: one row
                             per record (CN, PREV_TRE_CN, PREV_STATUS_CD kept)
    r7 D36: tree_status0_events, tree_censor_events and tree_landuse_exits carry TREE_RAW (FIA's
    tree number); their TREE is re-keyed to the post-split tree id in run_pipeline
    (rekey_tree_events), and tree_reconcile_events is rebuilt from the re-keyed events.
    excluded_tree_ids keeps FIA's numbers (it lists the raw records the RC7 rule drops).
    """
    # r7 D36: TREE is re-keyed to the post-split tree id by run_pipeline (rekey_tree_events);
    # TREE_RAW keeps FIA's tree number. A7-10: P2A records out, and the record must sit on a
    # condition of its visit (no COND_STATUS filter: RC9 'nonsampled' closures are on COND 5).
    con.execute(f"""
    DROP TABLE IF EXISTS tree_status0_events;
    CREATE TABLE tree_status0_events AS
    SELECT T.STATECD, T.UNITCD, T.COUNTYCD, T.PLOT, T.SUBP, T.TREE, T.INVYR,
           MAKE_DATE(P.MEASYEAR, COALESCE(NULLIF(CAST(P.MEASMON AS INTEGER), 0), 6), COALESCE(NULLIF(CAST(P.MEASDAY AS INTEGER), 0), 15)) AS MEASDATE,
           CAST(T.RECONCILECD AS INTEGER) AS RC, T.PREVDIA,
           T.TREE AS TREE_RAW, CAST(T.CN AS VARCHAR) AS CN, T.SPCD
    FROM TREE T
    JOIN PLOT P ON P.CN = T.PLT_CN
    JOIN SURVEY SRV ON P.SRV_CN = SRV.CN AND SRV.ANN_INVENTORY = 'Y'
    JOIN COND C ON C.PLT_CN = T.PLT_CN AND C.CONDID = T.CONDID
    WHERE T.STATUSCD = 0 AND T.SUBP <= 4
      AND COALESCE(T.P2A_GRM_FLG, '') <> 'Y'
      AND P.STATECD <= 56 AND P.STATECD NOT IN (2, 15) {scope}
    """)
    _sql_procedural_breaks(con, scope)
    con.execute("""
    DROP TABLE IF EXISTS tree_censor_events;
    CREATE TABLE tree_censor_events AS
    SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE, MEASDATE, RC,
           CASE RC WHEN 5 THEN 'shrank' WHEN 6 THEN 'moved' WHEN 8 THEN 'procedural'
                   WHEN 9 THEN 'nonsampled' ELSE 'not_in_sample' END AS reason,
           TREE AS TREE_RAW
    FROM tree_status0_events WHERE RC IS DISTINCT FROM 7
    UNION ALL
    SELECT s.STATECD, s.UNITCD, s.COUNTYCD, s.PLOT, s.SUBP, s.TREE, s.next_md, NULL,
           'procedural_unrecorded', s.TREE
    FROM _retally_saplings s
    JOIN procedural_breaks b ON b.STATECD=s.STATECD AND b.UNITCD=s.UNITCD
         AND b.COUNTYCD=s.COUNTYCD AND b.PLOT=s.PLOT AND b.MEASDATE=s.next_md
    WHERE s.unrecorded
    """)
    # r6 D35: a coordinate-linked chain start (D29) is a PROCEDURAL re-tally of the plot. Live
    # trees of the earlier chain end that are NOT continued at the start (same SUBP / TREE / SPCD,
    # DIA not decreasing — the D29 match) are censored 'procedural_relink' at the start visit
    # (they were renumbered or not re-found, not removed), and the start visit becomes a
    # procedural_breaks row (break_kind 'coord_link') that blocks backfill across it.
    # r7 D38: on a KINDCD 3 (replacement plot) start only a LIVE start record continues an end
    # tree; a dead / cut record there is a first-seen death (a new tree, first_record_dead), so
    # the end tree is censored here, not killed in the link interval. The end / start trees are
    # read from the raw TREE table and the (canonical) plot key from the start visit.
    # r8 D47.1: an end visit DROPPED as a D37 duplicate (plot_dup_visits) is the same physical
    # visit as the kept start, not a link end: its trees are never censored 'procedural_relink'
    # (the kept visit's trees continue normally).
    if dbconn_has_links(con):
        con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _relink AS
        SELECT * FROM (
            SELECT l.start_cn, l.end_cn, P0.STATECD, COALESCE(m.UNITCD, P0.UNITCD) AS UNITCD,
                   COALESCE(m.COUNTYCD, P0.COUNTYCD) AS COUNTYCD, COALESCE(m.PLOT, P0.PLOT) AS PLOT,
                   P0.KINDCD AS start_kindcd,
                   MAKE_DATE(P0.MEASYEAR, COALESCE(NULLIF(CAST(P0.MEASMON AS INTEGER), 0), 6),
                             COALESCE(NULLIF(CAST(P0.MEASDAY AS INTEGER), 0), 15)) AS start_md
            FROM plot_coord_links l JOIN fia.main.PLOT P0 ON P0.CN = l.start_cn
            LEFT JOIN plot_canon_map m ON m.CN = P0.CN) P
        WHERE TRUE {scope}""")
        con.execute("""
        INSERT INTO tree_censor_events BY NAME
        SELECT l.STATECD, l.UNITCD, l.COUNTYCD, l.PLOT, T.SUBP, T.TREE, l.start_md AS MEASDATE,
               NULL::INTEGER AS RC, 'procedural_relink' AS reason, T.TREE AS TREE_RAW
        FROM _relink l JOIN fia.main.TREE T ON T.PLT_CN = l.end_cn
        WHERE T.STATUSCD = 1 AND T.SUBP <= 4
          AND CAST(l.end_cn AS VARCHAR) NOT IN (SELECT CN FROM plot_dup_visits)   -- r8 D47.1
          AND NOT EXISTS (SELECT 1 FROM fia.main.TREE u WHERE u.PLT_CN = l.start_cn AND u.SUBP = T.SUBP
                          AND u.TREE = T.TREE AND u.SPCD = T.SPCD AND u.DIA >= T.DIA
                          AND (l.start_kindcd IS DISTINCT FROM 3 OR u.STATUSCD = 1))
          AND NOT EXISTS (SELECT 1 FROM tree_censor_events e WHERE e.STATECD = l.STATECD AND e.UNITCD = l.UNITCD
                          AND e.COUNTYCD = l.COUNTYCD AND e.PLOT = l.PLOT AND e.SUBP = T.SUBP AND e.TREE = T.TREE
                          AND e.MEASDATE = l.start_md)""")
        con.execute("""
        INSERT INTO procedural_breaks BY NAME
        SELECT STATECD, UNITCD, COUNTYCD, PLOT, start_md AS MEASDATE, 0 AS n_closed_rc8,
               0 AS n_closed_rc8_prev_sapling, 0 AS n_prev_saplings, 0 AS n_unrecorded_saplings,
               NULL AS detect_tier, 'coord_link' AS break_kind
        FROM _relink""")
        nrl = con.execute("SELECT count(*) FROM tree_censor_events WHERE reason = 'procedural_relink'").fetchone()[0]
        print(f"[step3b] coordinate-linked chain starts: {con.execute('SELECT count(*) FROM _relink').fetchone()[0]} "
              f"(procedural breaks); {nrl:,} earlier live trees not continued -> censored 'procedural_relink'")
    build_tree_reconcile_events(con)
    # M6: live records on a NONFOREST / WATER own condition (excluded by the live rule): the
    # tree still exists but its land left forest -> terminal fate 'diverted_landuse'.
    # r7 A7-2: ONE ROW PER such record (was: per tree with the FIRST date), so a fate looks for an
    # exit AFTER the tree's last curated record. r7 D44 (D26 land-use path): CN / PREV_TRE_CN /
    # PREV_STATUS_CD are kept, so a misrecorded death is matched by the PREV_TRE_CN -> CN link.
    # r7 D36: TREE is re-keyed to the post-split tree id (rekey_tree_events), TREE_RAW = FIA's.
    con.execute(f"""
    DROP TABLE IF EXISTS tree_landuse_exits;
    CREATE TABLE tree_landuse_exits AS
    SELECT T.STATECD, T.UNITCD, T.COUNTYCD, T.PLOT, T.SUBP, T.TREE,
           MAKE_DATE(P.MEASYEAR, COALESCE(NULLIF(CAST(P.MEASMON AS INTEGER), 0), 6), COALESCE(NULLIF(CAST(P.MEASDAY AS INTEGER), 0), 15)) AS exit_measdate,
           -- r6 D26: this live record is linked back (PREV_TRE_CN) to a dead record (PREV_STATUS_CD 2):
           -- a misrecorded death whose tree then left forest land
           CASE WHEN T.PREV_STATUS_CD = 2 AND T.PREV_TRE_CN IS NOT NULL THEN
               MAKE_DATE(P.MEASYEAR, COALESCE(NULLIF(CAST(P.MEASMON AS INTEGER), 0), 6), COALESCE(NULLIF(CAST(P.MEASDAY AS INTEGER), 0), 15))
           END AS linked_live_after_dead_md,
           T.TREE AS TREE_RAW, CAST(T.CN AS VARCHAR) AS CN, CAST(T.PREV_TRE_CN AS VARCHAR) AS PREV_TRE_CN,
           T.PREV_STATUS_CD
    FROM TREE T
    JOIN PLOT P ON P.CN = T.PLT_CN
    JOIN SURVEY SRV ON P.SRV_CN = SRV.CN AND SRV.ANN_INVENTORY = 'Y'
    JOIN COND C ON C.PLT_CN = T.PLT_CN AND C.CONDID = T.CONDID
    WHERE T.STATUSCD = 1 AND T.SUBP <= 4 AND C.COND_STATUS_CD IN (2, 3, 4)
      AND COALESCE(T.P2A_GRM_FLG, '') <> 'Y'
      AND P.STATECD <= 56 AND P.STATECD NOT IN (2, 15) {scope}
    """)
    # Backward-compatible table name: the trees whose earlier records are discarded.
    con.execute("""
    DROP TABLE IF EXISTS excluded_tree_ids;
    CREATE TABLE excluded_tree_ids AS
    SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE, rc7_erroneous_measdate,
           'cruiser_error_prev_tally' AS exclusion_reasons,
           FALSE AS is_meaningful        -- erroneous tallies carry no real observation
    FROM tree_reconcile_events
    WHERE rc7_erroneous_measdate IS NOT NULL
    """)
    con.commit()
    n = con.execute("SELECT COUNT(*) FROM tree_reconcile_events").fetchone()[0]
    n7 = con.execute("SELECT COUNT(*) FROM excluded_tree_ids").fetchone()[0]
    nb = con.execute("SELECT COUNT(*) FROM procedural_breaks").fetchone()[0]
    print(f"[step3b] tree_reconcile_events: {n:,} trees with STATUSCD=0 records; "
          f"{n7:,} RC7 erroneous-prior-tally trees (earlier records dropped); "
          f"{nb:,} plot-visit procedural breaks")
    for reason, cnt in con.execute("""
        SELECT reason, COUNT(*) FROM tree_censor_events GROUP BY 1 ORDER BY 2 DESC""").fetchall():
        print(f"        censoring events {reason:22s}: {cnt:,}")
    for kind, n, ns in con.execute("""
        SELECT break_kind, COUNT(*), SUM(n_unrecorded_saplings) FROM procedural_breaks
        GROUP BY 1 ORDER BY 1""").fetchall():
        print(f"        procedural breaks {kind:10s}: {n:,} plot-visits ({ns or 0:,} unrecorded saplings)")


def sql_rc7_withdrawn_visits(con, scope: str = ""):
    """r7 D39 (A7-4): plot-visits ALL of whose live tally trees were withdrawn by FIA as
    'erroneously tallied' (a later STATUSCD 0 + RC7 record of the same tree number; D34 drops
    those earlier records). The visit's dead / cut records and its scaffold row are kept (the dead
    tally may be valid); curated_trees flags every record of such a visit
    `all_live_withdrawn_rc7` and consumers decide. The live set is curated_trees' own live rule
    (STATUSCD 1, TPA > 0, own COND_STATUS 1, not P2A, REF_SPECIES, a scaffold subplot-visit)."""
    con.execute(f"""
    DROP TABLE IF EXISTS rc7_withdrawn_visits;
    CREATE TABLE rc7_withdrawn_visits AS
    WITH r7 AS (SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE_RAW AS TREE, MIN(MEASDATE) AS rc7_md
                FROM tree_status0_events WHERE RC = 7 GROUP BY ALL),
    lv AS (
        SELECT CAST(P.CN AS VARCHAR) AS PLT_CN, T.STATECD, T.UNITCD, T.COUNTYCD, T.PLOT, T.INVYR, CSY.MEASDATE,
               (r7.rc7_md IS NOT NULL AND CSY.MEASDATE < r7.rc7_md) AS withdrawn
        FROM TREE T
        JOIN PLOT P ON P.CN = T.PLT_CN
        JOIN REF_SPECIES SP ON SP.SPCD = T.SPCD
        JOIN COND C ON C.PLT_CN = T.PLT_CN AND C.CONDID = T.CONDID
        JOIN clean_subplot_years CSY ON CSY.STATECD = T.STATECD AND CSY.UNITCD = T.UNITCD
             AND CSY.COUNTYCD = T.COUNTYCD AND CSY.PLOT = T.PLOT AND CSY.INVYR = T.INVYR AND CSY.SUBP = T.SUBP
        LEFT JOIN r7 ON r7.STATECD = T.STATECD AND r7.UNITCD = T.UNITCD AND r7.COUNTYCD = T.COUNTYCD
             AND r7.PLOT = T.PLOT AND r7.SUBP = T.SUBP AND r7.TREE = T.TREE
        WHERE T.STATUSCD = 1 AND T.TPA_UNADJ > 0 AND C.COND_STATUS_CD = 1
          AND COALESCE(T.P2A_GRM_FLG, '') <> 'Y' {scope})
    SELECT PLT_CN, STATECD, UNITCD, COUNTYCD, PLOT, INVYR, MEASDATE, count(*) AS n_live_withdrawn
    FROM lv GROUP BY ALL HAVING bool_and(withdrawn)
    """)
    n = con.execute("SELECT count(*), COALESCE(sum(n_live_withdrawn), 0) FROM rc7_withdrawn_visits").fetchone()
    print(f"[step3c] visits whose live trees were ALL withdrawn by RC7: {n[0]:,} ({n[1]:,} live records)")


def sql_product_visits(con):
    """r7 D43: the annual plot visits (PLT_CN, canonical key, INVYR, MEASDATE) of the curated
    scaffold. export_reference_tables copies only PLOT / COND rows of these visits (plus the
    canonical remap's raw visits), so a periodic plot that reused a plot key (KY 1986-88) never
    reaches the reference tables, STDORGCD / SITECLCD / land_use or the slim DB."""
    con.execute("""
    DROP TABLE IF EXISTS product_visits;
    CREATE TABLE product_visits AS
    SELECT DISTINCT CAST(P.CN AS VARCHAR) AS PLT_CN, P.STATECD, P.UNITCD, P.COUNTYCD, P.PLOT, P.INVYR, CSY.MEASDATE
    FROM PLOT P
    JOIN SURVEY SRV ON SRV.CN = P.SRV_CN AND SRV.ANN_INVENTORY = 'Y'
    JOIN (SELECT DISTINCT STATECD, UNITCD, COUNTYCD, PLOT, INVYR, MEASDATE FROM clean_subplot_years) CSY
      ON CSY.STATECD = P.STATECD AND CSY.UNITCD = P.UNITCD AND CSY.COUNTYCD = P.COUNTYCD
     AND CSY.PLOT = P.PLOT AND CSY.INVYR = P.INVYR
    ORDER BY ALL
    """)
    print(f"[step2b] product_visits: {con.execute('SELECT count(*) FROM product_visits').fetchone()[0]:,} plot-visits")


# =============================================================================
# Step 4: Load into polars
# =============================================================================

def load_trees(con, state: int | None = None) -> pl.DataFrame:
    """
    Load tree records of the scaffold subplot-visits into polars, with condition context.

    curation-fix 2026-10-26 record rules (FIADB v9.4):
      * live trees (STATUSCD=1) must be on their OWN condition with COND_STATUS_CD=1
        (accessible forest). Dead/cut records (STATUSCD 2/3) are kept on COND_STATUS_CD 1-4
        (forest, nonforest, water): a tree removed by land clearing sits on the now-nonforest
        condition, and dropping that record would mislabel the removal as an unexplained
        disappearance (it becomes 'diverted_landuse', r3 M6);
      * the STATUSCD=0 record itself is dropped (its information is carried as censoring /
        RC7 columns: censor_* via attach_censor_columns after the split, r7 D36);
      * records BEFORE a STATUSCD=0 + RECONCILECD=7 event are dropped (erroneous tally);
      * explicit exclusion of P2A (periodic-to-annual GRM-only, P2A_GRM_FLG='Y') records;
        live records need TPA_UNADJ > 0 (sample trees). Dead/cut records are kept even with
        a NULL TPA_UNADJ (FIADB leaves TPA null on many mortality records of previously-live
        trees; dropping them turned real deaths into inferred removals — audit I1). Their
        TPA is NOT used for any live biomass/area total (all those filter STATUSCD=1);
      * annual survey only + REF_SPECIES join (unchanged);
      * NO tree-level RECONCILECD exclusion, NO >= 2-visit rule, NO subplot COND_STATUS 3 drop.
    """
    where_state = f"AND T.STATECD = {int(state)}" if state is not None else ""
    df = con.execute(f"""
    SELECT
        T.STATECD, T.UNITCD, T.COUNTYCD, T.PLOT, T.SUBP, T.TREE, T.INVYR,
        T.CN,
        CSY.MEASDATE, CSY.CYCLE,
        T.CONDID, T.STATUSCD, T.RECONCILECD, T.AGENTCD,
        T.PREV_STATUS_CD AS prev_status_cd, CAST(T.PREV_TRE_CN AS VARCHAR) AS prev_tre_cn,
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
        C.DSTRBYR1, C.DSTRBYR2, C.DSTRBYR3,
        -- curation-fix 2026-10-26: area proportions for consumer-side normalisation
        C.COND_STATUS_CD          AS cond_status_cd,
        C.CONDPROP_UNADJ          AS cond_condprop_unadj,
        C.SUBPPROP_UNADJ          AS cond_subpprop_unadj,
        C.MICRPROP_UNADJ          AS cond_micrprop_unadj,
        C.MACRPROP_UNADJ::DOUBLE  AS cond_macrprop_unadj,
        SC.SUBPCOND_PROP          AS subp_subpcond_prop,
        SC.MICRCOND_PROP          AS subp_micrcond_prop,
        SC.MACRCOND_PROP          AS subp_macrcond_prop,
        CSY.forest_subpcond_prop  AS subp_forest_subpcond_prop,
        CSY.forest_micrcond_prop  AS subp_forest_micrcond_prop,
        CSY.has_cond_status3      AS subp_has_cond_status3,
        LS.n_measurements         AS n_clean_visits
        -- r7 D36: the censor_* columns are attached AFTER the tree-number split
        -- (attach_censor_columns), keyed by the post-split tree id
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
    LEFT JOIN SUBP_COND SC
        ON  SC.STATECD = T.STATECD AND SC.UNITCD = T.UNITCD
        AND SC.COUNTYCD = T.COUNTYCD AND SC.PLOT = T.PLOT
        AND SC.INVYR = T.INVYR AND SC.SUBP = T.SUBP AND SC.CONDID = T.CONDID
    LEFT JOIN (SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE_RAW AS TREE,
                      MIN(MEASDATE) AS rc7_erroneous_measdate          -- raw FIA tree number (D34)
               FROM tree_status0_events WHERE RC = 7 GROUP BY ALL) E
        ON  E.STATECD = T.STATECD AND E.UNITCD = T.UNITCD
        AND E.COUNTYCD = T.COUNTYCD AND E.PLOT = T.PLOT
        AND E.SUBP = T.SUBP AND E.TREE = T.TREE
    WHERE COALESCE(T.P2A_GRM_FLG, '') <> 'Y'                -- explicit: P2A GRM-only records out
      AND T.STATUSCD IN (1, 2, 3)                           -- drop the STATUSCD=0 record itself
      AND (   (T.STATUSCD = 1 AND T.TPA_UNADJ > 0           -- live: a sample tree (TPA > 0) ...
               AND C.COND_STATUS_CD = 1)                    -- ... on its own forest condition
           OR (T.STATUSCD IN (2, 3)                         -- dead / cut: kept for FATE even with
               AND C.COND_STATUS_CD IN (1, 2, 3, 4)))       -- a null TPA (audit I1); also on
                                                            -- nonforest/water (r2 M5)
      AND NOT (E.rc7_erroneous_measdate IS NOT NULL         -- erroneously tallied previously
               AND CSY.MEASDATE < E.rc7_erroneous_measdate)
      {where_state}
    """).pl()

    df = df.sort(TREE_ID + ["MEASDATE"])
    print(f"[step4] loaded {df.height:,} tree-measurements into polars")
    return df


TREE_SPLIT_OFFSET = 100_000   # R4 TREE numbers are < 61,000


NEW_TREE_RC = (1, 2, 3, 4)   # r7 D36: RECONCILECD new-tree codes (ingrowth, through-growth, missed live / dead)


def split_reused_tree_numbers(df: pl.DataFrame, coord_start_cns: set | None = None,
                              relink: pl.DataFrame | None = None) -> pl.DataFrame:
    """
    r3 M3 — tree-number reuse. A tree key (STATECD..SUBP, TREE) can be re-used for a new tree,
    e.g. a dead tree's number given to a new RC1 tree. Following the PREV_TRE_CN chain, a record
    starts a NEW tree when the key's previous record (in this table)
      * was dead/cut and this record's PREV_TRE_CN is NULL, or
      * is a different CN than this record's PREV_TRE_CN, or
      * r7 D36 (A7-1): this record's PREV_TRE_CN is NULL and its RECONCILECD is a new-tree code
        (NEW_TREE_RC: 1 ingrowth / 2 through-growth / 3 missed live / 4 missed dead), whatever the
        previous record's status (a live predecessor used to merge two physically different
        trees: species relabelled, ages and cohorts mixed, censoring lost), or
      * r7 D36 (F7-1): the key was censored 'procedural_relink' (D35) at a coordinate-link start
        (`relink`: raw key + relink_md) and this is its first record at or after that date: the
        relinked key is CLOSED, a later record under that number is a new tree.
    Later segments get TREE + k * TREE_SPLIT_OFFSET (k = 1, 2, ...); every record of a split key
    has tree_key_split = TRUE. The censoring / status-0 / land-use events are re-keyed to the
    segment they belong to afterwards (rekey_tree_events, r7 D36), so a split tree never inherits
    the old id's censoring.
    """
    d = df.sort(TREE_ID + ["MEASDATE"]).with_columns([
        pl.col("CN").cast(pl.Utf8).shift(1).over(TREE_ID).alias("_pcn"),
        pl.col("STATUSCD").shift(1).over(TREE_ID).alias("_pst"),
        pl.col("MEASDATE").shift(1).over(TREE_ID).alias("_pmd"),
    ])
    # r6 S1 (D29): at the first visit of a chain that was linked to an earlier chain by
    # coordinates + tree continuity (dbconn plot_coord_links), FIA carries no PREV_TRE_CN; a
    # record there whose same-number predecessor is live is the SAME tree only if the species
    # matches and DIA did not decrease, otherwise it starts a new tree
    d = d.with_columns(pl.col("SPCD").shift(1).over(TREE_ID).alias("_psp"),
                       pl.col("DIA").shift(1).over(TREE_ID).alias("_pdia"))
    if relink is not None and relink.height:
        hit = (d.select(TREE_ID + [pl.col("CN").cast(pl.Utf8).alias("_cn"), "MEASDATE", "_pmd"])
                .join(relink.select(TREE_ID + ["relink_md"]), on=TREE_ID, how="inner")
                .filter((pl.col("_pmd") < pl.col("relink_md")) & (pl.col("relink_md") <= pl.col("MEASDATE")))
                .select("_cn").unique().with_columns(pl.lit(True).alias("_closed")))
        d = (d.with_columns(pl.col("CN").cast(pl.Utf8).alias("_cn")).join(hit, on="_cn", how="left")
              .drop("_cn").sort(TREE_ID + ["MEASDATE"]))   # F7-4: the order is re-established after the join
    else:
        d = d.with_columns(pl.lit(None, dtype=pl.Boolean).alias("_closed"))
    at_link = (pl.col("CN").cast(pl.Utf8).is_in(list(coord_start_cns or [])))
    no_link = pl.col("prev_tre_cn").is_null()
    r_dead = no_link & pl.col("_pst").is_in([2, 3])
    r_other = pl.col("prev_tre_cn").is_not_null() & (pl.col("prev_tre_cn") != pl.col("_pcn"))
    r_newrc = no_link & pl.col("RECONCILECD").cast(pl.Int32).is_in(list(NEW_TREE_RC))
    r_closed = pl.col("_closed").fill_null(False)
    r_link = (at_link & no_link & (pl.col("_pst") == 1) &
              ((pl.col("SPCD") != pl.col("_psp")).fill_null(True) | (pl.col("DIA") < pl.col("_pdia")).fill_null(False)))
    has_prev = pl.col("_pcn").is_not_null()
    d = d.with_columns([(has_prev & r).fill_null(False).alias(n) for n, r in
                        (("_s_dead", r_dead), ("_s_other", r_other), ("_s_newrc", r_newrc),
                         ("_s_closed", r_closed), ("_s_link", r_link))])
    new_seg = pl.any_horizontal("_s_dead", "_s_other", "_s_newrc", "_s_closed", "_s_link")
    d = d.with_columns(new_seg.cast(pl.Int64).cum_sum().over(TREE_ID).alias("_seg"))
    d = d.with_columns((pl.col("_seg").max().over(TREE_ID) > 0).alias("tree_key_split"))
    n_split = d.filter(pl.col("_seg") > 0).select(TREE_ID).unique().height
    n_rec = d.filter(pl.col("_seg") > 0).height
    why = d.select([pl.col(c).sum().alias(c[3:]) for c in ("_s_dead", "_s_other", "_s_newrc", "_s_closed", "_s_link")]).row(0, named=True)
    d = d.with_columns((pl.col("TREE") + pl.col("_seg") * TREE_SPLIT_OFFSET).alias("TREE")).drop(
        ["_pcn", "_pst", "_pmd", "_seg", "_psp", "_pdia", "_closed",
         "_s_dead", "_s_other", "_s_newrc", "_s_closed", "_s_link"])
    print(f"[step4s] tree-number reuse: {n_split:,} keys split, {n_rec:,} records moved to a new tree id; "
          f"segment starts by rule (a record may meet several): {why}")
    return d.sort(TREE_ID + ["MEASDATE"])


def rekey_tree_events(con, df: pl.DataFrame, st: int):
    """r7 D36 (F7-2, C F14): the SQL event tables (tree_status0_events, tree_censor_events,
    tree_landuse_exits) carry FIA's tree number; after the split each event of state `st` is
    keyed by the post-split tree id: the segment of that raw key whose FIRST record is the
    latest one STRICTLY before the event date (the tree the event closes); an event before every
    record of the key keeps the raw number. TREE_RAW keeps FIA's number."""
    seg = (df.filter(pl.col("tree_key_split"))
             .group_by(SUBPLOT_ID + ["TREE"]).agg(pl.col("MEASDATE").min().alias("first_md"))
             .with_columns((pl.col("TREE") % TREE_SPLIT_OFFSET).alias("TREE_RAW"))
             .sort(SUBPLOT_ID + ["TREE"]))
    con.register("_seg_pl", seg)
    moved = {}
    for tab, dc in (("tree_status0_events", "MEASDATE"), ("tree_censor_events", "MEASDATE"),
                    ("tree_landuse_exits", "exit_measdate")):
        before = con.execute(f"SELECT count(*) FROM {tab} WHERE STATECD = {int(st)} AND TREE <> TREE_RAW").fetchone()[0]
        con.execute(f"""CREATE OR REPLACE TABLE {tab} AS
            SELECT e.* REPLACE (CASE WHEN e.STATECD = {int(st)} THEN COALESCE(s.TREE, e.TREE_RAW) ELSE e.TREE END AS TREE)
            FROM {tab} e ASOF LEFT JOIN _seg_pl s
              ON s.STATECD = e.STATECD AND s.UNITCD = e.UNITCD AND s.COUNTYCD = e.COUNTYCD AND s.PLOT = e.PLOT
             AND s.SUBP = e.SUBP AND s.TREE_RAW = e.TREE_RAW AND e.{dc} > s.first_md
            ORDER BY e.STATECD, e.UNITCD, e.COUNTYCD, e.PLOT, e.SUBP, e.TREE_RAW, e.{dc}""")
        moved[tab] = con.execute(f"SELECT count(*) FROM {tab} WHERE STATECD = {int(st)} AND TREE <> TREE_RAW").fetchone()[0] - before
    con.unregister("_seg_pl")
    print(f"[step4k] events re-keyed to split tree ids: {moved}")


def attach_censor_columns(con, df: pl.DataFrame, st: int) -> pl.DataFrame:
    """r7 D36: censor_* columns from the RE-KEYED tree_censor_events (first and last event of each
    post-split tree; the same aggregation as tree_reconcile_events)."""
    ev = con.execute(f"""
        SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE,
               MIN(MEASDATE) AS censor_measdate,
               ARG_MIN(RC, MEASDATE) AS censor_reconcilecd,
               ARG_MIN(reason, MEASDATE) AS censor_reason,
               MAX(MEASDATE) AS censor_last_measdate,
               ARG_MAX(RC, MEASDATE) AS censor_last_reconcilecd,
               ARG_MAX(reason, MEASDATE) AS censor_last_reason
        FROM tree_censor_events WHERE STATECD = {int(st)} GROUP BY ALL""").pl()
    ev = ev.with_columns([pl.col(c).cast(df.schema[c]) for c in TREE_ID])
    return df.join(ev, on=TREE_ID, how="left")


def annotate_reconcile(df: pl.DataFrame) -> pl.DataFrame:
    """
    curation-fix 2026-10-26 per-tree reconcile / censoring flags:
      reconcile_new      RECONCILECD of the tree's first record when it is a non-growth
                         entry (3 missed live, 4 missed dead, 6 moved in, 7 cruiser error,
                         8 procedural change, 9 previously nonsampled area); else null
      not_ingrowth       reconcile_new is not null (tell these from true ingrowth RC1/RC2)
      censored           the tree has a STATUSCD=0 non-RC7 record: it left observation for
                         a non-mortality reason (NOT a death or removal)
      censor_measdate    MEASDATE of that STATUSCD=0 record
      censor_reconcilecd its RECONCILECD (5 shrank, 6 moved, 8 procedural, 9 nonsampled)
      censor_reason      label of the above ('not_in_sample' for null/other codes;
                         'procedural_unrecorded' = live sapling with NO record at the next
                         visit of a procedural re-tally plot-visit — r2 B2)
      censor_last_measdate / censor_last_reconcilecd / censor_last_reason
                         the LAST such record (a tree can be censored, re-enter, and be
                         censored again — audit M8); terminal fate uses the last one
    """
    first_rc = (
        df.sort(TREE_ID + ["MEASDATE"]).unique(subset=TREE_ID, keep="first")
        .select(TREE_ID + [pl.col("RECONCILECD").cast(pl.Int32).alias("_rc_first")])
    )
    df = df.join(first_rc, on=TREE_ID, how="left").with_columns(
        pl.when(pl.col("_rc_first").is_in(list(RECONCILE_NEW_CODES)))
        .then(pl.col("_rc_first")).otherwise(pl.lit(None, dtype=pl.Int32))
        .alias("reconcile_new")
    ).drop("_rc_first")
    # censor_reason / censor_last_reason come from tree_censor_events (SQL): RC-coded
    # STATUSCD=0 exits plus 'procedural_unrecorded' (sapling silently dropped at a re-tally)
    df = df.with_columns([
        pl.col("reconcile_new").is_not_null().alias("not_ingrowth"),
        pl.col("censor_measdate").is_not_null().alias("censored"),
    ])
    n_new = df.filter(pl.col("not_ingrowth")).select(TREE_ID).unique().height
    n_cen = df.filter(pl.col("censored")).select(TREE_ID).unique().height
    print(f"[step4r] reconcile_new trees: {n_new:,}  |  censored trees: {n_cen:,}")
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
        return _add_spgrpcd_resolved(df), pl.DataFrame(schema={
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

    return _add_spgrpcd_resolved(df), confusion_edges


def _add_spgrpcd_resolved(df: pl.DataFrame) -> pl.DataFrame:
    """curation-fix 2026-10-26: SPGRPCD that goes with spcd_resolved (taken from the tree's
    latest record whose SPCD == spcd_resolved), so cohort species AND group are per-tree
    constants instead of the raw per-visit values."""
    grp = (
        df.filter(pl.col("SPCD") == pl.col("spcd_resolved"))
        .sort(TREE_ID + ["MEASDATE"])
        .unique(subset=TREE_ID, keep="last")
        .select(TREE_ID + [pl.col("SPGRPCD").alias("spgrpcd_resolved")])
    )
    return df.join(grp, on=TREE_ID, how="left")


# =============================================================================
# Step 5: Classify tree fate (alive / harvest / natural death)
# =============================================================================

def handle_reconcile5(df: pl.DataFrame) -> pl.DataFrame:
    """
    RECONCILECD=5 ("shrank": live tree that fell below the tally threshold; only valid with
    STATUSCD=0) — curation-fix 2026-10-26.

    The old implementation (revived to STATUSCD=1 with an imputed DIA of 5.0/1.0 and a
    propagated TPA such as 6.018) would FABRICATE records once STATUSCD=0 trees are no
    longer excluded wholesale. It is NOT revived: RC5 is used as a censoring flag
    (annotate_reconcile: censored / censor_reason='shrank'). The reconcile5_* columns are
    kept for schema compatibility: no DIA/TPA is imputed or propagated (all null/False);
    reconcile5_first_measdate = date of the RC5 STATUSCD=0 record, reconcile5_recovered =
    a later live record of the same tree (re-entered the tally).
    """
    r5_date = (
        pl.when(pl.col("censor_reconcilecd") == 5).then(pl.col("censor_measdate"))
        .otherwise(pl.lit(None, dtype=pl.Date))
    )
    df = df.with_columns([
        pl.lit(None).cast(pl.Float64).alias("reconcile5_prev_dia_raw"),
        pl.lit(None).cast(pl.Float64).alias("reconcile5_prev_dia"),
        pl.lit(False).alias("reconcile5_dia_imputed"),
        pl.lit(None).cast(pl.Float64).alias("reconcile5_prev_tpa_raw"),
        pl.lit(None).cast(pl.Float64).alias("reconcile5_prev_tpa"),
        pl.lit(False).alias("reconcile5_tpa_propagated"),
        r5_date.alias("reconcile5_first_measdate"),
    ]).with_columns(
        (pl.col("reconcile5_first_measdate").is_not_null() &
         (pl.col("MEASDATE") > pl.col("reconcile5_first_measdate")) &
         (pl.col("STATUSCD") == 1)).alias("reconcile5_recovered")
    )
    n5 = df.filter(pl.col("reconcile5_first_measdate").is_not_null()).select(TREE_ID).unique().height
    print(f"[handle_r5] {n5:,} trees censored by RECONCILECD=5 (shrank) — no records fabricated")
    return df


def classify_tree_fate(df: pl.DataFrame, landuse_exits: pl.DataFrame | None = None) -> pl.DataFrame:
    """
    Classify each tree-measurement's status:
      - alive: STATUSCD=1
      - harvest_explicit: STATUSCD=3 or (STATUSCD=2 + AGENTCD=80)
      - harvest_inferred: STATUSCD=2 + condition has TRTCD=10 +
            no other tree in the same condition-year has explicit harvest +
            AGENTCD is NULL (r2 M7: a recorded non-harvest agent 10-70 is FIA's own
            mortality call — all 152 AL harvest_inferred vs GRM MORTALITY disagreements
            had one)
      - dead: STATUSCD=2 with non-harvest cause
    """
    cond_has_cutting = (
        (pl.col("TRTCD1") == 10) |
        (pl.col("TRTCD2") == 10) |
        (pl.col("TRTCD3") == 10)
    )

    # r3 I1: a dead/cut record whose PREV_STATUS_CD is 2 (already dead at the previous visit),
    # or any dead/cut record after the tree's FIRST dead/cut record, is a RE-RECORD of an
    # earlier death: kept, flagged rerecord_dead, and never an event / fate / harvest source.
    dead = pl.col("STATUSCD").is_in([2, 3])
    # r6 R4 (D26): a dead/cut record FOLLOWED by a live record of the same tree that FIA links back
    # (PREV_TRE_CN set, PREV_STATUS_CD = 2) is a MISRECORDED death: kept, flagged
    # misrecorded_death (tree_fate 'misrecorded_death'), and excluded from every fate / event /
    # dead-record set; the tree continues
    res_md = (pl.when((pl.col("STATUSCD") == 1) & (pl.col("prev_status_cd") == 2).fill_null(False)
                      & pl.col("prev_tre_cn").is_not_null()).then(pl.col("MEASDATE"))
              .max().over(TREE_ID))
    df = df.sort(TREE_ID + ["MEASDATE"]).with_columns(res_md.alias("_res_md"))
    # ... or by a linked live record on a now-nonforest condition (tree_landuse_exits: the live
    # rule keeps it out of the curated records, but FIA's link is the same evidence)
    if landuse_exits is not None and "linked_live_after_dead_md" in landuse_exits.columns:
        if "PREV_TRE_CN" in landuse_exits.columns:
            # r7 D44 (F7-12): the exit record belongs to the tree whose record its PREV_TRE_CN
            # names (the PREV_TRE_CN -> CN link), not to whatever shares the tree number
            lx = (landuse_exits.filter(pl.col("linked_live_after_dead_md").is_not_null()
                                       & pl.col("PREV_TRE_CN").is_not_null())
                  .select(pl.col("PREV_TRE_CN").cast(pl.Utf8).alias("_lcn"), "linked_live_after_dead_md")
                  .join(df.select(TREE_ID + [pl.col("CN").cast(pl.Utf8).alias("_lcn")]), on="_lcn", how="inner")
                  .group_by(TREE_ID).agg(pl.col("linked_live_after_dead_md").max().alias("_res_lx")))
        else:
            lx = (landuse_exits.filter(pl.col("linked_live_after_dead_md").is_not_null())
                  .group_by(TREE_ID).agg(pl.col("linked_live_after_dead_md").max().alias("_res_lx")))
        df = (df.join(lx, on=TREE_ID, how="left")
                .with_columns(pl.max_horizontal("_res_md", "_res_lx").alias("_res_md")).drop("_res_lx"))
    # r7 D44 (F7-4): polars joins do not guarantee row order; the cum_sum below needs it
    df = df.sort(TREE_ID + ["MEASDATE"])
    df = df.with_columns(
        (dead & pl.col("_res_md").is_not_null() & (pl.col("MEASDATE") < pl.col("_res_md"))).fill_null(False)
        .alias("misrecorded_death")).drop("_res_md")
    real_dead = dead & ~pl.col("misrecorded_death")
    df = df.with_columns(
        (real_dead & ((pl.col("prev_status_cd") == 2).fill_null(False) |
                      (real_dead.cast(pl.Int32).cum_sum().over(TREE_ID) > 1))).alias("rerecord_dead"))
    n_mis = df.filter(pl.col("misrecorded_death"))
    print(f"[step5] misrecorded deaths (dead/cut record followed by a linked live record): "
          f"{n_mis.height:,} records on {n_mis.select(TREE_ID).unique().height:,} trees")

    # Raw classification. r3 M6: a cut on a condition that is no longer accessible forest
    # (COND_STATUS 2-4 at that visit) is a land-use DIVERSION, not a harvest of forest.
    is_cut = (pl.col("STATUSCD") == 3) | ((pl.col("STATUSCD") == 2) & (pl.col("AGENTCD") == 80))
    df = df.with_columns(
        pl.when(pl.col("STATUSCD") == 1)
            .then(pl.lit("alive"))
        .when(pl.col("misrecorded_death"))
            .then(pl.lit("misrecorded_death"))
        .when(is_cut & (pl.col("cond_status_cd") != 1).fill_null(False))
            .then(pl.lit("diverted_landuse"))
        .when(is_cut)
            .then(pl.lit("harvest_explicit"))
        .when(pl.col("STATUSCD") == 2)
            .then(pl.lit("dead_raw"))
        .otherwise(pl.lit("unknown"))
        .alias("_fate_raw")
    )

    # Find condition-years where at least one tree has explicit harvest (first records only)
    explicit_harvest_conds = (
        df.filter((pl.col("_fate_raw") == "harvest_explicit") & ~pl.col("rerecord_dead"))
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
                ~pl.col("_cond_has_explicit").fill_null(False) &
                pl.col("AGENTCD").is_null() &
                ~pl.col("rerecord_dead")          # r3 I1: never on a re-record
            )
            .then(pl.lit("harvest_inferred"))
            .otherwise(pl.lit("dead"))
        )
        .otherwise(pl.col("_fate_raw"))
        .alias("tree_fate")
    )

    df = df.drop(["_fate_raw", "_cond_has_explicit"])
    return df


def _next_visit(last: pl.DataFrame, subplot_years: pl.DataFrame) -> pl.DataFrame:
    """Per tree: is there a subplot visit after its last record (`_later`), and is the FIRST
    such visit entirely nonforest/water (`_next_nonforest`: forest subplot + microplot
    proportion 0). `last` carries TREE_ID + _last_md; subplot_years needs SUBPLOT_ID +
    MEASDATE (+ forest_subpcond_prop / forest_micrcond_prop when available)."""
    sv = subplot_years.select(
        SUBPLOT_ID + ["MEASDATE"] +
        [c for c in ("forest_subpcond_prop", "forest_micrcond_prop") if c in subplot_years.columns]
    ).unique()
    if "forest_subpcond_prop" in sv.columns:
        sv = sv.with_columns(
            ((pl.col("forest_subpcond_prop").fill_null(0) + pl.col("forest_micrcond_prop").fill_null(0)) <= 0)
            .alias("_nf"))
    else:
        sv = sv.with_columns(pl.lit(False).alias("_nf"))
    return (last.join(sv, on=SUBPLOT_ID, how="inner")
            .filter(pl.col("MEASDATE") > pl.col("_last_md"))
            .sort(TREE_ID + ["MEASDATE"])
            .group_by(TREE_ID, maintain_order=True)
            .agg(pl.len().alias("_n_later"), pl.col("_nf").first().alias("_next_nonforest"))
            .with_columns((pl.col("_n_later") > 0).alias("_later"))
            .select(TREE_ID + ["_later", "_next_nonforest"]))


def final_fate_expr(fate_col: str, last_md_col: str) -> pl.Expr:
    """Shared terminal-fate decision for a tree whose last record is `fate_col` at
    `last_md_col` (needs censor_last_measdate/_reason, _later, _next_nonforest, _lu_exit)."""
    alive = pl.col(fate_col) == "alive"
    return (
        pl.when(~alive).then(pl.col(fate_col))
        .when(pl.col("censor_last_measdate").is_not_null() &
              (pl.col("censor_last_measdate") > pl.col(last_md_col)))
            .then(pl.lit("censored_") + pl.col("censor_last_reason"))
        .when(pl.col("_later").fill_null(False) &
              (pl.col("_lu_exit").fill_null(False) | pl.col("_next_nonforest").fill_null(False)))
            .then(pl.lit("diverted_landuse"))
        .when(pl.col("_later").fill_null(False))
            .then(pl.lit("disappeared_inferred_removal"))
        .otherwise(pl.lit("alive"))
    )


def add_final_tree_fate(df: pl.DataFrame, subplot_years: pl.DataFrame,
                        landuse_exits: pl.DataFrame | None = None) -> pl.DataFrame:
    """
    Per-tree terminal fate (curation-fix 2026-10-26; this was dead code in v3 before):
      a dead/cut record exists           -> the tree_fate of its FIRST dead/cut record (r3 I1:
                                         later ones are re-records); diverted_landuse for a
                                         cut on a now-nonforest condition (r3 M6)
      last record alive, and the tree was censored after it (STATUSCD=0 + RC 5/6/8/9)
                                         -> 'censored_<reason>' (NOT a removal)
      last record alive, subplot revisited later, and the land left forest: the tree has a
        live record on a nonforest/water condition after it (tree_landuse_exits), or the
        subplot's next visit is entirely nonforest/water (forest subplot + microplot
        proportion 0)                    -> 'diverted_landuse' (r2 M6; not a removal)
      last record alive, subplot revisited later without the tree, otherwise
                                         -> 'disappeared_inferred_removal'
      last record alive, no later visit  -> 'alive'
    Also adds dead_at_first_visit (r2 M8): the tree's first record is dead (STATUSCD 2) at
    the subplot's FIRST visit — standing dead when first seen, no observed mortality.
    """
    # r3 I1: a tree that ever has a dead/cut record ends at its FIRST dead/cut record (later
    # records of it are re-records); otherwise at its last (live) record
    first_dead = (
        df.filter(pl.col("STATUSCD").is_in([2, 3]) & ~pl.col("misrecorded_death"))   # r6 R4
        .sort(TREE_ID + ["MEASDATE"]).unique(subset=TREE_ID, keep="first")
        .select(TREE_ID + [pl.col("MEASDATE").alias("_dmd"), pl.col("tree_fate").alias("_dfate")])
    )
    last = (
        df.sort(TREE_ID + ["MEASDATE"]).unique(subset=TREE_ID, keep="last")
        .select(TREE_ID + [pl.col("MEASDATE").alias("_lmd"),
                           # r6 D26: a misrecorded death means the tree was ALIVE at that visit
                           pl.when(pl.col("tree_fate") == "misrecorded_death").then(pl.lit("alive"))
                           .otherwise(pl.col("tree_fate")).alias("_lfate"),
                           "censor_last_measdate", "censor_last_reason"])
        .join(first_dead, on=TREE_ID, how="left")
        .with_columns(pl.coalesce(["_dmd", "_lmd"]).alias("_last_md"),
                      pl.coalesce(["_dfate", "_lfate"]).alias("_last_fate"))
        .drop(["_dmd", "_lmd", "_dfate", "_lfate"])
    )
    nxt = _next_visit(last.select(TREE_ID + ["_last_md"]), subplot_years)
    if landuse_exits is None or landuse_exits.height == 0:
        landuse_exits = pl.DataFrame(schema={**{k: pl.Int64 for k in TREE_ID}, "exit_measdate": pl.Date})
    lex = (last.select(TREE_ID + ["_last_md"]).join(landuse_exits, on=TREE_ID, how="inner")
           .filter(pl.col("exit_measdate") > pl.col("_last_md"))
           .select(TREE_ID).unique().with_columns(pl.lit(True).alias("_lu_exit")))
    last = (last.join(nxt, on=TREE_ID, how="left").join(lex, on=TREE_ID, how="left")
            .with_columns(final_fate_expr("_last_fate", "_last_md").alias("final_tree_fate"))
            .select(TREE_ID + ["final_tree_fate"]))
    df = df.join(last, on=TREE_ID, how="left")

    first_subp = subplot_years.group_by(SUBPLOT_ID).agg(pl.col("MEASDATE").min().alias("_fsv"))
    dfirst = (df.sort(TREE_ID + ["MEASDATE"]).unique(subset=TREE_ID, keep="first")
              .join(first_subp, on=SUBPLOT_ID, how="left")
              .select(TREE_ID + [((pl.col("STATUSCD") == 2) & ~pl.col("misrecorded_death")
                                  & (pl.col("MEASDATE") == pl.col("_fsv")))
                                 .fill_null(False).alias("dead_at_first_visit")]))
    df = df.join(dfirst, on=TREE_ID, how="left")
    # r7 D38 (A7-11): first_record_dead generalises dead_at_first_visit: the tree's FIRST record
    # is dead or cut (STATUSCD 2 / 3; a misrecorded death counts as alive) at ANY visit — a
    # first-seen death (e.g. a dead record at a KINDCD 3 coordinate-link start), never observed
    # mortality of a live tree, so it never produces a mortality interval or a drop
    frd = (df.sort(TREE_ID + ["MEASDATE"]).unique(subset=TREE_ID, keep="first")
             .select(TREE_ID + [(pl.col("STATUSCD").is_in([2, 3]) & ~pl.col("misrecorded_death"))
                                .fill_null(False).alias("first_record_dead")]))
    df = df.join(frd, on=TREE_ID, how="left")
    _nf = frd.filter(pl.col("first_record_dead")).height
    print(f"[step5] first_record_dead trees: {_nf:,} "
          f"(dead_at_first_visit: {dfirst.filter(pl.col('dead_at_first_visit')).height:,})")

    # Print breakdown
    fate_counts = df.group_by("tree_fate").len().sort("len", descending=True)
    print("[step5] tree fate classification (records):")
    for row in fate_counts.iter_rows():
        print(f"        {str(row[0]):25s}: {row[1]:>12,}")
    print("        final_tree_fate (trees):")
    for row in (df.unique(subset=TREE_ID).group_by("final_tree_fate").len()
                .sort("len", descending=True).iter_rows()):
        print(f"          {str(row[0]):30s}: {row[1]:>10,}")
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


RESET_LOSS_FRAC = 0.5   # stand-replacing: >= 50 % of live trees or live BA lost since the previous visit


def detect_condition_resets(df: pl.DataFrame) -> pl.DataFrame:
    """
    r3 I2 — STAND-REPLACING resets only. A condition-visit is a reset when
      1. STDAGE dropped since the condition's previous visit AND >= RESET_LOSS_FRAC of its live
         trees (count) or live basal area was lost since then        -> 'stdage_drop_with_loss'
      2. TRTCD 10 (cutting) together with TRTCD 30/40 (artificial / natural regeneration)
                                                                      -> 'cut_with_regen'
      3. TRTCD 10 together with that >= RESET_LOSS_FRAC loss          -> 'cut_with_loss'
    TRTCD 10 alone (a partial cut), including at a condition's first visit, is NOT a reset, and
    neither is an STDAGE drop without the loss (STDAGE re-estimation).

    D40 (audit r7 M-10): the loss is measured across the subplot's CONDIDs. A tree that was
    live in the condition at its previous visit and is live at this visit in ANOTHER CONDID of
    its subplot (condition re-delineation) is not lost: it counts towards this visit's live
    count / live BA (its BA at this visit).

    Returns (reset_flag, reset_reasons) keyed by COND_ID + MEASDATE.
    """
    COND_ID = ["STATECD", "UNITCD", "COUNTYCD", "PLOT", "CONDID"]
    live = pl.col("STATUSCD") == 1
    dfb = df.with_columns((pl.col("DIA").pow(2) * 0.005454 * pl.col("TPA_UNADJ")).alias("_ba"))
    cv = (
        dfb.group_by(COND_ID + ["MEASDATE"])
        .agg([
            pl.col("cond_stdage").first().alias("cond_stdage"),
            live.sum().alias("n_live"),
            pl.col("_ba").filter(live).sum().alias("ba_live"),
            pl.any_horizontal([pl.col(c) == 10 for c in ("TRTCD1", "TRTCD2", "TRTCD3")]).any().alias("cut"),
            pl.any_horizontal([pl.col(c).is_in([30, 40]) for c in ("TRTCD1", "TRTCD2", "TRTCD3")]).any().alias("regen"),
        ])
        .sort(COND_ID + ["MEASDATE"])
    )
    # trees live in the condition at its previous visit and live in another CONDID at this visit
    live_rec = dfb.filter(live).select(TREE_ID + ["CONDID", "MEASDATE", "_ba"])
    nxt = cv.select(COND_ID + ["MEASDATE"]).with_columns(
        pl.col("MEASDATE").shift(-1).over(COND_ID).alias("_next_md")).filter(pl.col("_next_md").is_not_null())
    moved = (live_rec.select(TREE_ID + ["CONDID", "MEASDATE"])
             .join(nxt, on=COND_ID + ["MEASDATE"], how="inner")
             .join(live_rec.rename({"CONDID": "_condid_next", "MEASDATE": "_next_md", "_ba": "_ba_next"}),
                   on=TREE_ID + ["_next_md"], how="inner")
             .filter(pl.col("_condid_next") != pl.col("CONDID"))
             .group_by(COND_ID + ["_next_md"])
             .agg(pl.len().alias("_n_moved"), pl.col("_ba_next").sum().alias("_ba_moved"))
             .rename({"_next_md": "MEASDATE"}))
    cv = (
        cv.join(moved, on=COND_ID + ["MEASDATE"], how="left")
        .with_columns(pl.col("_n_moved").fill_null(0), pl.col("_ba_moved").fill_null(0.0))
        .sort(COND_ID + ["MEASDATE"])
        .with_columns([
            pl.col("cond_stdage").shift(1).over(COND_ID).alias("_prev_stdage"),
            pl.col("n_live").shift(1).over(COND_ID).alias("_prev_n"),
            pl.col("ba_live").shift(1).over(COND_ID).alias("_prev_ba"),
        ])
        .with_columns([
            (((pl.col("_prev_n") > 0) & ((pl.col("n_live") + pl.col("_n_moved"))
                                         <= (1 - RESET_LOSS_FRAC) * pl.col("_prev_n"))) |
             ((pl.col("_prev_ba") > 0) & ((pl.col("ba_live") + pl.col("_ba_moved"))
                                          <= (1 - RESET_LOSS_FRAC) * pl.col("_prev_ba"))))
            .fill_null(False).alias("_loss"),
            (((pl.col("_prev_n") > 0) & (pl.col("n_live") <= (1 - RESET_LOSS_FRAC) * pl.col("_prev_n"))) |
             ((pl.col("_prev_ba") > 0) & (pl.col("ba_live") <= (1 - RESET_LOSS_FRAC) * pl.col("_prev_ba"))))
            .fill_null(False).alias("_loss_within_condid"),
        ])
    )
    drop = (pl.col("_prev_stdage").is_not_null() & pl.col("cond_stdage").is_not_null()
            & (pl.col("cond_stdage") < pl.col("_prev_stdage")))
    parts = [
        cv.filter(drop & pl.col("_loss")).select(COND_ID + ["MEASDATE"]).with_columns(pl.lit("stdage_drop_with_loss").alias("reset_reason")),
        cv.filter(pl.col("cut") & pl.col("regen")).select(COND_ID + ["MEASDATE"]).with_columns(pl.lit("cut_with_regen").alias("reset_reason")),
        cv.filter(pl.col("cut") & pl.col("_loss")).select(COND_ID + ["MEASDATE"]).with_columns(pl.lit("cut_with_loss").alias("reset_reason")),
    ]
    all_resets = pl.concat(parts)
    reset_reasons = (all_resets.group_by(COND_ID + ["MEASDATE"])
                     .agg(pl.col("reset_reason").unique().sort().alias("reset_reasons")))
    reset_flag = reset_reasons.select(COND_ID + ["MEASDATE"]).unique().with_columns(pl.lit(True).alias("is_reset"))
    n_drop_only = cv.filter(drop & ~pl.col("_loss")).height
    n_cut_only = cv.filter(pl.col("cut") & ~pl.col("regen") & ~pl.col("_loss")).height
    n_moved_only = cv.filter(pl.col("_loss_within_condid") & ~pl.col("_loss") & (drop | pl.col("cut"))).height
    print(f"[step6b-reset] {reset_flag.height:,} stand-replacing condition-visit resets: "
          + ", ".join(f"{p.get_column('reset_reason')[0] if p.height else '?'} {p.height:,}" for p in parts)
          + f" | NOT resets: STDAGE drop without loss {n_drop_only:,}, TRTCD 10 alone {n_cut_only:,}"
          + f" | D40: {n_moved_only:,} drop/cut visits whose within-CONDID loss is trees now live in another CONDID")
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
      - Confirmed STAND-REPLACING reset + POST-RESET DEBUT (r3 I2): a condition with a
        detected reset (detect_condition_resets: STDAGE drop with >= 50 % live-tree/BA loss,
        or TRTCD 10 with TRTCD 30/40 or with that loss; TRTCD 10 alone is not a reset) is
        eligible ONLY for trees whose first measurement falls at or after the condition's
        first reset date. These are genuinely new cohort trees; STDAGE reflects their
        establishment age.

    ALL pre-reset trees are excluded (r3 I2b), whether they survived the reset or were cut /
    died: the old stand's STDAGE is a stand-level mean, not the specific tree's age.
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
    #     r3 I2b: NOT eligible any more (pre-reset STDAGE is a stand mean).
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
    # r3 I2(b): pre-reset trees NEVER inherit STDAGE — whether they survived the reset or
    # not (the old `pre_reset & ~survived_reset` branch broadcast a stand age onto them)
    base_eligible = (
        (pl.col("cond_stdorgcd") == 1) |
        post_reset_debut
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
    Select one anchor (date, age) per tree from its candidate age records.

    candidates must contain: TREE_ID + MEASDATE + "src_age"
    tree_bounds must contain: TREE_ID + first_measdate + prev_sub_measdate

    curation-fix 2026-10-26 — MEDIAN anchor:
      The old rule picked the candidate with the smallest "max-age violation" (implied age
      at the last subplot visit before the tree's debut: "the crew was there and didn't see
      it"). That bound is wrong for every new-tree class — ingrowth/through-growth trees
      were below the tally threshold (not unborn), RC3/RC7 trees were present but missed,
      RC8 trees were untallied by procedure, RC6/RC9 trees were outside the sampled area —
      and it made the selection pick the YOUNGEST candidate ~78 % of the time (-2.7 yr
      bias). The bound is therefore no longer used for selection (max_viol is kept as a
      diagnostic only).

    Algorithm
    ---------
    1. implied age at the tree's first_measdate for each candidate;
    2. median of the implied ages per tree (the anchor age at first_measdate);
    3. optional min-age shift (apply_min_shift) so the median age at first_measdate >= 1;
    4. anchor_date = the candidate closest to the median (tie: most recent MEASDATE) — a
       real record, so per-anchor metadata joins still work; anchor_age is the median age
       carried to that date: median_at_first + (anchor_date - first_measdate).

    Returns TREE_ID + [anchor_date, anchor_age, anchor_src_age, min_shift, max_viol, n_cand].
    anchor_src_age : raw src_age of the anchor-date candidate (diagnostic)
    max_viol       : DIAGNOSTIC — implied age at the last subplot visit before debut
    """
    cands = candidates.join(tree_bounds, on=TREE_ID, how="left")
    cands = cands.with_columns(
        (
            pl.col("src_age").cast(pl.Float64) +
            (pl.col("first_measdate") - pl.col("MEASDATE")).dt.total_days() / 365.25
        ).alias("_implied_at_first")
    )
    cands = cands.with_columns([
        pl.col("_implied_at_first").median().over(TREE_ID).alias("_med_at_first"),
        pl.len().over(TREE_ID).alias("n_cand"),
    ])
    if apply_min_shift:
        cands = cands.with_columns(
            (pl.lit(1.0) - pl.col("_med_at_first")).clip(lower_bound=0.0).alias("min_shift"))
    else:
        cands = cands.with_columns(pl.lit(0.0).alias("min_shift"))
    cands = cands.with_columns(
        (pl.col("_med_at_first") + pl.col("min_shift")).alias("_age_at_first"),
        (pl.col("_implied_at_first") - pl.col("_med_at_first")).abs().alias("_dev"),
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
        .sort(TREE_ID + ["_dev", "MEASDATE"],
              descending=[False] * len(TREE_ID) + [False, True])
        .unique(subset=TREE_ID, keep="first")
        .with_columns(
            (pl.col("_age_at_first") +
             (pl.col("MEASDATE") - pl.col("first_measdate")).dt.total_days() / 365.25)
            .alias("anchor_age")
        )
        .select(TREE_ID + [
            pl.col("MEASDATE").alias("anchor_date"),
            pl.col("anchor_age"),
            pl.col("src_age").cast(pl.Float64).alias("anchor_src_age"),
            pl.col("min_shift"),
            pl.col("max_viol"),
            pl.col("n_cand"),
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

def load_estage_csv(csv_dir: str, keep_cns: pl.DataFrame | None = None) -> pl.DataFrame:
    """
    Load and concatenate all *_TREEAGE.csv files from csv_dir.
    Returns a DataFrame with columns: CN (Utf8), Tree_Age (Int32).
    keep_cns (curation-fix): optional one-column frame of in-scope TREE CNs; rows are
    semi-joined while scanning so subset runs don't hold the national table in memory.
    """
    files = sorted(glob.glob(os.path.join(csv_dir, "*_TREEAGE.csv")))
    if not files:
        raise FileNotFoundError(f"No *_TREEAGE.csv files found in {csv_dir}")
    dfs = []
    for f in files:
        lf = (pl.scan_csv(f, schema_overrides={"CN": pl.Utf8})
              .select(["CN", "Tree_Age"]))
        if keep_cns is not None:
            lf = lf.join(keep_cns.lazy(), on="CN", how="semi")
        dfs.append(lf.collect())
    estage_raw = pl.concat(dfs).unique(subset=["CN"])
    print(f"[step6d] loaded {estage_raw.height:,} estage records from {len(files)} state files")
    return estage_raw


ESTAGE_MIN_DIA = 5.0   # in — below this Tree_Age is not a usable per-tree age (audit B1)
ESTAGE_MIN_AGE = 1     # yr — Tree_Age <= 1 is the sapling default value, not an estimate


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
    # curation-fix 2026-10-26 (audit B1): the external Tree_Age estimate is unusable for
    # saplings — ~70 % of live 1-3 in saplings carry Tree_Age <= 1 with a hard step at 5 in
    # — so a record is an anchor candidate only if DIA >= ESTAGE_MIN_DIA and
    # Tree_Age > ESTAGE_MIN_AGE. A sapling's age then comes from a later >= 5 in record of
    # the same tree (propagated back) or from the age model. estage_raw keeps the raw value.
    cand_est = (
        df.filter(pl.col("estage_raw").is_not_null() &
                  (pl.col("DIA") >= ESTAGE_MIN_DIA).fill_null(False) &
                  (pl.col("estage_raw") > ESTAGE_MIN_AGE))
        .select(TREE_ID + ["MEASDATE", "INVYR",
                            pl.col("estage_raw").cast(pl.Float64).alias("src_age")])
    )

    # NOTE (I2): these anchors are on the RAW TreeAge scale and are interim — age_imputation
    # (step 2) re-anchors estage on the FIELD-age scale (estage_mapped) for every state.
    df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("estage_mapped"))
    return anchor_estage(df, cand_est, tree_bounds)


def anchor_estage(df: pl.DataFrame, cand_est: pl.DataFrame,
                  tree_bounds: pl.DataFrame) -> pl.DataFrame:
    """(Re)compute the estage anchor columns from candidate records (TREE_ID + MEASDATE +
    INVYR + src_age): estage_anchor_value / _measdate / _invyr, age_from_estage (float) and
    estage (int). Existing columns of those names are replaced."""
    df = df.drop([c for c in ("estage_anchor_value", "estage_anchor_measdate", "estage_anchor_invyr",
                              "age_from_estage", "estage") if c in df.columns])
    if cand_est.height == 0:
        print("[estage] no estage candidates — skipping propagation")
        return df.with_columns([
            pl.lit(None).cast(pl.Date).alias("estage_anchor_measdate"),
            pl.lit(None).cast(pl.Float64).alias("estage_anchor_value"),
            pl.lit(None).cast(pl.Int64).alias("estage_anchor_invyr"),
            pl.lit(None).cast(pl.Float64).alias("age_from_estage"),
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
    print(f"[estage] {anchors_est.height:,} trees anchored, {n_prop:,} measurements propagated")
    print(f"         min-age shifts: {n_shifted:,}  |  residual max-age violations: {n_maxviol:,}")
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
    ),
    -- r7 D44 (A7-7 / F7-3): the dominant condition value is the MODE with ties broken by the
    -- SMALLEST code (DuckDB's MODE picked a thread-dependent value on every two-condition tie)
    mcnt AS (
        SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, INVYR, 'f' AS k, FORTYPCD AS v, count(*) AS n
        FROM base_subplot_cond WHERE COND_STATUS_CD <> 3 AND FORTYPCD IS NOT NULL GROUP BY ALL
        UNION ALL
        SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, INVYR, 's', STDORGCD, count(*)
        FROM base_subplot_cond WHERE COND_STATUS_CD <> 3 AND STDORGCD IS NOT NULL GROUP BY ALL),
    mode_det AS (
        SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, INVYR,
               first(v ORDER BY n DESC, v) FILTER (WHERE k = 'f') AS fortypcd,
               first(v ORDER BY n DESC, v) FILTER (WHERE k = 's') AS stdorgcd
        FROM mcnt GROUP BY ALL)
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
        ANY_VALUE(MD.fortypcd) AS fortypcd,
        ANY_VALUE(MD.stdorgcd) AS stdorgcd,
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
    LEFT JOIN mode_det MD
        ON  MD.STATECD  = BSC.STATECD AND MD.UNITCD  = BSC.UNITCD
        AND MD.COUNTYCD = BSC.COUNTYCD AND MD.PLOT   = BSC.PLOT
        AND MD.SUBP     = BSC.SUBP    AND MD.INVYR   = BSC.INVYR
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
    # curation-fix 2026-10-26: snapshot species = the per-tree resolved species/group
    if "spcd_resolved" in df.columns:
        df = df.with_columns([
            pl.col("spcd_resolved").alias("SPCD"),
            pl.coalesce(["spgrpcd_resolved", "SPGRPCD"]).alias("SPGRPCD"),
        ])
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
        df.filter(pl.col("STATUSCD").is_in([2, 3]) &
                  ~(pl.col("rerecord_dead") if "rerecord_dead" in df.columns else pl.lit(False)) &
                  ~(pl.col("misrecorded_death") if "misrecorded_death" in df.columns else pl.lit(False)))
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
        med_diff = float("nan") if med_diff is None else med_diff   # empty subset guard
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

def _polars_steps(con, df: pl.DataFrame, estage_raw, all_subplot_years,
                  landuse_exits=None) -> pl.DataFrame:
    """Steps 4b-6e for one batch (one state) of tree records. r7 D36: the tree-number split
    (split_reused_tree_numbers), the event re-keying and the censor columns run BEFORE this, in
    run_pipeline."""
    df = annotate_reconcile(df)
    df, _confusion_edges = analyze_spcd_confusion(df)
    df = handle_reconcile5(df)
    df = classify_tree_fate(df, landuse_exits)
    df = add_final_tree_fate(df, all_subplot_years, landuse_exits)

    tree_bounds = _compute_tree_bounds(df, all_subplot_years)
    df = propagate_totage(df, tree_bounds)
    df = assign_stdage(df, tree_bounds)
    df = merge_age_estimates(df)

    if estage_raw is not None:
        df = process_estage(df, estage_raw, tree_bounds)
    else:
        df = df.with_columns([
            pl.lit(None).cast(pl.Int64).alias("estage_raw"),
            pl.lit(None).cast(pl.Float64).alias("estage_min_violation"),
            pl.lit(None).cast(pl.Float64).alias("estage_max_violation"),
            pl.lit(None).cast(pl.Date).alias("estage_anchor_measdate"),
            pl.lit(None).cast(pl.Float64).alias("estage_anchor_value"),
            pl.lit(None).cast(pl.Int64).alias("estage_anchor_invyr"),
            pl.lit(None).cast(pl.Float64).alias("age_from_estage"),
            pl.lit(None).cast(pl.Int32).alias("estage"),
            pl.lit(None).cast(pl.Float64).alias("estage_mapped"),
        ])
    return flag_age_discrepancies(df)


# Columns appended by curation-fix 2026-10-26 (moved to the END of curated_trees so the old
# column order/types are preserved as a prefix of the new schema).
FIX_COLUMNS = [
    "cond_status_cd", "cond_condprop_unadj", "cond_subpprop_unadj", "cond_micrprop_unadj",
    "cond_macrprop_unadj", "subp_subpcond_prop", "subp_micrcond_prop", "subp_macrcond_prop",
    "subp_forest_subpcond_prop", "subp_forest_micrcond_prop", "subp_has_cond_status3",
    "n_clean_visits", "reconcile_new", "not_ingrowth", "censored", "censor_measdate",
    "censor_reconcilecd", "censor_reason", "spgrpcd_resolved", "final_tree_fate",
    "censor_last_measdate", "censor_last_reconcilecd", "censor_last_reason",
    "dead_at_first_visit", "estage_mapped", "age_domain",
    "rerecord_dead", "tree_key_split", "prev_status_cd", "prev_tre_cn",
    "age_stratum", "unitcd_raw", "countycd_raw", "plot_raw",          # r5 M-5 / M-2
    "misrecorded_death",                                               # r6 R4
    "first_record_dead", "all_live_withdrawn_rc7",                     # r7 D38 / D39
]


def add_age_stratum(df: pl.DataFrame) -> pl.DataFrame:
    """r5 M-5: ONE age scale per tree. A tree whose condition's STDORGCD flips between visits
    used to mix the planted (field-scale mapped) and natural (raw TreeAge) scales in its anchor
    candidates. age_stratum = 'planted' if the condition STDORGCD at the tree's FIRST LIVE record
    is 1, else 'natural' (null / other codes = natural); a tree without any live record takes its
    first record. Every age-scale decision in step 2 uses this column."""
    first = (df.with_columns((pl.col("STATUSCD") != 1).alias("_nl"))
               .sort(TREE_ID + ["_nl", "MEASDATE"])
               .group_by(TREE_ID, maintain_order=True).first()
               .select(TREE_ID + [pl.when(pl.col("cond_stdorgcd") == 1).then(pl.lit("planted"))
                                  .otherwise(pl.lit("natural")).alias("age_stratum")]))
    return df.join(first, on=TREE_ID, how="left")


def add_raw_plot_keys(con, df: pl.DataFrame, st: int) -> pl.DataFrame:
    """r5 M-2: the as-recorded plot key of each record (the stages see the canonical key)."""
    raw = con.execute(f"""SELECT CAST(T.CN AS VARCHAR) AS _cn, m.UNITCD_RAW, m.COUNTYCD_RAW, m.PLOT_RAW
        FROM fia.main.TREE T JOIN plot_canon_map m ON m.CN = CAST(T.PLT_CN AS VARCHAR)
        WHERE T.STATECD = {int(st)}""").pl()
    d = df.with_columns(pl.col("CN").cast(pl.Utf8).alias("_cn")).join(raw, on="_cn", how="left")
    n = d.filter(pl.col("PLOT_RAW").is_not_null()).height
    print(f"[step4p] canonical plot key: {n:,} records on renumbered plot visits "
          f"({d.filter(pl.col('PLOT_RAW').is_not_null()).select(['STATECD','UNITCD','COUNTYCD','PLOT']).unique().height} plots)")
    return d.with_columns(
        pl.coalesce("UNITCD_RAW", "UNITCD").cast(pl.Int64).alias("unitcd_raw"),
        pl.coalesce("COUNTYCD_RAW", "COUNTYCD").cast(pl.Int64).alias("countycd_raw"),
        pl.coalesce("PLOT_RAW", "PLOT").cast(pl.Int64).alias("plot_raw"),
    ).drop(["_cn", "UNITCD_RAW", "COUNTYCD_RAW", "PLOT_RAW"])


def run_pipeline(con, estage_dir: str = None, states=None, counties=None):
    """
    Run the full curation pipeline (curation-fix 2026-10-26).

    SQL steps run once for the whole scope; the polars steps run ONE STATE AT A TIME
    (every rule is tree/subplot/condition-local, all within a state) so peak memory stays
    bounded; the per-state results are unioned into curated_trees / curated_snapshots.
    Only the diagnostic spcd_confusion_cluster id is state-local.
    """
    print("=" * 70)
    print("FIA Curation Pipeline v3 (curation-fix 2026-10-26)")
    print("=" * 70)
    scope = scope_sql(states, counties, "P")
    # r6 S2: the run scope (states AND the county subset) is recorded, so the age-model fit scope
    # (KNOWN exceptions / known-bias flags) of a subset run never matches a whole-state one
    # r7 E29: both columns are VARCHAR also when NULL; r7 D44 (chain D F7): run_id identifies this
    # step-1 run; the chain's step log and slim_guard check it across curated / simB / slim
    import uuid, datetime as _dt
    con.execute("""CREATE OR REPLACE TABLE run_scope AS
        SELECT CAST(? AS VARCHAR) AS states, CAST(? AS VARCHAR) AS counties,
               CAST(? AS VARCHAR) AS run_id, CAST(? AS TIMESTAMP) AS created_at""",
                [",".join(str(int(x)) for x in sorted(states)) if states else None,
                 ",".join(f"{int(a)}:{int(b)}" for a, b in sorted(counties)) if counties else None,
                 uuid.uuid4().hex, _dt.datetime.now().replace(microsecond=0)])
    # r6: the plot-key remaps (M-2 / D29) and the plot locations (D28) of this run, for reports
    # and the verifier. r7 D37: `reason` ('prev_plt_cn' / 'coord_link' / 'duplicate_visit'); a
    # duplicate_visit row is a visit whose records were dropped (kept_cn = the visit kept)
    con.execute("""CREATE OR REPLACE TABLE plot_key_remap AS
        SELECT *, NULL::VARCHAR AS kept_cn FROM plot_canon_map UNION ALL BY NAME SELECT * FROM plot_dup_map
        ORDER BY ALL""")
    con.execute("CREATE OR REPLACE TABLE plot_key_coord_links AS SELECT * FROM plot_coord_links")
    con.execute("CREATE OR REPLACE TABLE plot_location AS SELECT * FROM plot_loc_visit")
    coord_start_cns = {r[0] for r in con.execute("""SELECT DISTINCT CAST(T.CN AS VARCHAR) FROM fia.main.TREE T
                       JOIN plot_coord_links l ON CAST(T.PLT_CN AS VARCHAR) = l.start_cn""").fetchall()}

    sql_base_measurements(con, scope)
    sql_clean_subplot_years(con)
    sql_product_visits(con)
    sql_longitudinal_subplots(con)
    sql_excluded_trees(con, scope)
    sql_rc7_withdrawn_visits(con, scope)

    estage_raw = None
    if estage_dir is not None:
        keep = con.execute("""
            SELECT DISTINCT CAST(T.CN AS VARCHAR) AS CN FROM TREE T
            JOIN clean_subplot_years CSY
              ON T.STATECD=CSY.STATECD AND T.UNITCD=CSY.UNITCD AND T.COUNTYCD=CSY.COUNTYCD
             AND T.PLOT=CSY.PLOT AND T.INVYR=CSY.INVYR AND T.SUBP=CSY.SUBP
        """).pl()
        estage_raw = load_estage_csv(estage_dir, keep)
    scaffold_all = build_subplot_year_scaffold(con)

    run_states = [r[0] for r in con.execute(
        "SELECT DISTINCT STATECD FROM clean_subplot_years ORDER BY 1").fetchall()]
    parts_t, parts_s = [], []
    for st in run_states:
        print(f"\n----- STATECD {st} -----")
        df = load_trees(con, st)
        if df.height == 0:
            continue
        all_subplot_years = con.execute(f"""
            SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, MEASDATE,
                   forest_subpcond_prop, forest_micrcond_prop
            FROM clean_subplot_years WHERE STATECD = {int(st)}
        """).pl()
        # r7 D36: split reused tree numbers first (D35 relink-censored keys are closed), then key
        # every event by the post-split id and attach the censor columns from those events
        relink = con.execute(f"""
            SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE_RAW AS TREE, MEASDATE AS relink_md
            FROM tree_censor_events WHERE reason = 'procedural_relink' AND STATECD = {int(st)}""").pl()
        df = split_reused_tree_numbers(df, coord_start_cns, relink)
        rekey_tree_events(con, df, st)
        df = attach_censor_columns(con, df, st)
        landuse_exits = con.execute(f"""
            SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE, exit_measdate, linked_live_after_dead_md,
                   PREV_TRE_CN
            FROM tree_landuse_exits WHERE STATECD = {int(st)}""").pl()
        df = _polars_steps(con, df, estage_raw, all_subplot_years, landuse_exits)
        df = add_raw_plot_keys(con, add_age_stratum(df), st)
        # r7 D39: every record of a visit whose live trees were all withdrawn (RC7)
        w7 = con.execute(f"""SELECT DISTINCT STATECD, UNITCD, COUNTYCD, PLOT, INVYR, TRUE AS all_live_withdrawn_rc7
                             FROM rc7_withdrawn_visits WHERE STATECD = {int(st)}""").pl()
        w7 = w7.with_columns([pl.col(c).cast(df.schema[c]) for c in PLOT_INVYR])
        df = (df.join(w7, on=PLOT_INVYR, how="left")
                .with_columns(pl.col("all_live_withdrawn_rc7").fill_null(False))
                .sort(TREE_ID + ["MEASDATE"]))
        df = df.select([c for c in df.columns if c not in FIX_COLUMNS]
                       + [c for c in FIX_COLUMNS if c in df.columns])   # age_domain: step 2

        scaffold = scaffold_all.filter(pl.col("STATECD") == st)
        ever_live = (df.filter((pl.col("STATUSCD") == 1) & pl.col("DRYBIO_AG").is_not_null())
                     .select(SUBPLOT_ID).unique())
        scaffold = scaffold.join(ever_live, on=SUBPLOT_ID, how="semi")
        snapshots = build_snapshots(df, scaffold)

        for name, frame, parts in (("t", df, parts_t), ("s", snapshots, parts_s)):
            tab = f"_v3_{name}_{st}"
            con.register("_part", frame)
            con.execute(f"CREATE OR REPLACE TABLE {tab} AS SELECT * FROM _part")
            con.unregister("_part")
            parts.append(tab)
        con.commit()
        print(f"[save] state {st}: {df.height:,} tree rows, {snapshots.height:,} snapshot rows")
        if st == run_states[-1]:
            diagnostics(df, snapshots)
        del df, snapshots

    for target, parts in (("curated_trees", parts_t), ("curated_snapshots", parts_s)):
        union = " UNION ALL BY NAME ".join(f"SELECT * FROM {p}" for p in parts)
        con.execute(f"CREATE OR REPLACE TABLE {target} AS {union}")
        for p in parts:
            con.execute(f"DROP TABLE {p}")
        con.commit()
        n = con.execute(f"SELECT COUNT(*) FROM {target}").fetchone()[0]
        print(f"[save] {target}: {n:,} rows")

    # r7 D36: tree_reconcile_events again, now from the RE-KEYED events (post-split tree ids);
    # excluded_tree_ids keeps FIA's tree numbers (it lists the raw records the RC7 rule drops)
    build_tree_reconcile_events(con)
    con.commit()
    print("\n[done] SQL tables: base_subplot_cond, clean_subplot_years, longitudinal_subplots,")
    print("       tree_reconcile_events, excluded_tree_ids, curated_trees, curated_snapshots")


def parse_counties(s):
    """'13:59,13:121' -> [(13, 59), (13, 121)]"""
    if not s:
        return None
    return [tuple(int(v) for v in x.split(":")) for x in s.split(",") if x]


if __name__ == "__main__":
    import argparse
    import dbconn
    ap = dbconn.add_db_args(argparse.ArgumentParser(description="FIA curation v3 (curation-fix)"))
    ap.add_argument("--estage-dir", default=os.environ.get("FIA_ESTAGE_DIR"),
                    help="directory of *_TREEAGE.csv (env FIA_ESTAGE_DIR)")
    ap.add_argument("--counties", default=os.environ.get("FIA_COUNTIES"),
                    help="subset: STATECD:COUNTYCD,... (env FIA_COUNTIES)")
    a = ap.parse_args()
    dbconn.check_args(a, scope_check=False)   # step 1 writes run_scope itself
    con = dbconn.connect(a.out_db, a.fiadb)
    run_pipeline(con, estage_dir=a.estage_dir, states=dbconn.parse_states(a.states),
                 counties=parse_counties(a.counties))
    con.close()
