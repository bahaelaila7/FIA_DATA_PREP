#!/usr/bin/env python
"""
verify_curation_v2.py — curation-fix 2026-10-26 verification (read-only on every DB).

Compares the NEW curated DB (+ optional new slim DB) against raw FIADB and the OLD curated
tables / OLD slim DB, within an optional scope (--states / --counties):
  1. live-tree TPA coverage of FIADB TREE (STATUSCD=1, TPA_UNADJ>0, own condition
     COND_STATUS_CD=1, annual survey, PLOT_STATUS_CD<3, not P2A) by state x size class
     (sapling DIA<5 / >=5 in), old curated vs new curated;
  2. record counts per table, old vs new;
  3. duplicates: curated_trees.CN unique (old and new);
  4. >=5 in live-tree difference old vs new (records, TPA);
  5. estimated_age distribution of live records, old vs new (overall and by size class);
  6. slim schema old vs new (column name/type per table, in order).
The OLD tables are read deliberately and fully qualified (old.* / fiaold.*) as the baseline.

  python verify_curation_v2.py --curated-db NEW.duckdb --fiadb FIADB.duckdb \
      [--old-slim OLD_SLIM --new-slim NEW_SLIM] [--states 1,12] [--counties 1:89]
"""
import argparse
import os

import duckdb

ap = argparse.ArgumentParser()
ap.add_argument("--curated-db", required=True)
ap.add_argument("--fiadb", required=True)
ap.add_argument("--simb-db")
ap.add_argument("--old-slim")
ap.add_argument("--new-slim")
ap.add_argument("--states")
ap.add_argument("--counties")
ap.add_argument("--determinism-db", help="a second curated DB built from the same input (cohort steps re-run)")
ap.add_argument("--determinism-simb-db", help="r5 M-3: the Sim B DB of that rerun (steps 9-10 re-run)")
a = ap.parse_args()
FAILS = []


def check(name, ok, detail=""):
    print(f"  [{'PASS' if ok else 'FAIL'}] {name} {detail}")
    if not ok:
        FAILS.append(name)


def info(name, detail=""):
    print(f"  [INFO] {name} {detail}")


# r4 ACCEPT + FLAG (user decision 2026-10-26): the ONLY cells allowed to miss their limit. An
# excepted cell prints KNOWN instead of FAIL while |value| <= |observed| + KNOWN_DRIFT; worse than
# that it FAILS, so an exception cannot drift silently. Every other cell / state stays strict.
# The records' ages are flagged in curated_trees.age_domain (age_imputation.KNOWN_BIAS_FLAGS).
# Values were observed on the whole-state r4 runs; a county-subset run may differ.
KNOWN_DRIFT = 1.0
# r5 (D14): exceptions are keyed by FIT SCOPE first (the sorted STATECDs of the curated DB: the
# age model and the TreeAge mapping are fitted once over all of them), so a value recorded on one
# fit never excuses another. A scope without entries is fully strict (e.g. a Region-4 run must be
# re-measured). The r4 single-state FL entry (-3.5) is retired: the r5 single-state FL fit
# passes at -1.6.
_FL_RTM = ("FL: model vs mapped TreeAge regression to the mean under the pooled fit; only trees "
           "WITHOUT a TreeAge record take the model age")
KNOWN_EXCEPTIONS = {
    ("12,21,28", "j", 12, "planted", "10-15"): dict(
        observed=-3.2, limit=3.0, flag="known_bias_fl_planted_model_10plus", reason=_FL_RTM),
    ("12,21,28", "j", 12, "planted", "15+"): dict(
        observed=-3.8, limit=3.0, flag="known_bias_fl_planted_model_10plus", reason=_FL_RTM),
}
FIT_SCOPE = None   # set after the curated DB is attached
KNOWN = []


def check_known(key, name, ok, value, detail=""):
    key = (FIT_SCOPE,) + tuple(key)
    exc = KNOWN_EXCEPTIONS.get(key)
    if ok or exc is None:
        check(name, ok, detail)
        if ok and exc is not None:
            info(f"{name}: known exception {key} now passes", "(consider retiring it)")
        return
    bound = abs(exc["observed"]) + KNOWN_DRIFT
    if abs(value) <= bound:
        print(f"  [KNOWN] {name} {detail} — recorded {exc['observed']:+.1f} (limit {exc['limit']}, "
              f"FAILS beyond |{bound:.1f}|); {exc['reason']}; flag age_domain={exc['flag']}")
        KNOWN.append(name)
    else:
        check(f"{name} [known exception DRIFTED: |{value:.1f}| > |{exc['observed']:.1f}|+{KNOWN_DRIFT}]", False, detail)

con = duckdb.connect(":memory:")
con.execute(f"SET threads={os.environ.get('FIA_DUCKDB_THREADS', '3')}; "
            f"SET memory_limit='{os.environ.get('FIA_DUCKDB_MEMORY', '5GB')}'; SET enable_progress_bar=false")
# r5 M-2: the raw reference uses CANONICAL plot keys (a few plots change UNITCD/COUNTYCD/PLOT
# between annual visits; canonical = the key at the PREV_PLT_CN chain's first ANNUAL visit).
# Re-derived independently of dbconn (recursive CTE): FIADB is attached as `fraw`, and an
# in-memory catalog `f` holds one view per FIADB table, remapped where the table is plot-keyed.
con.execute(f"ATTACH '{a.fiadb}' AS fraw (READ_ONLY)")
con.execute("ATTACH ':memory:' AS f")
con.execute("""CREATE TABLE f.main._canon AS
WITH RECURSIVE ap AS (SELECT P.CN, P.PREV_PLT_CN, P.UNITCD, P.COUNTYCD, P.PLOT FROM fraw.main.PLOT P
                      JOIN fraw.main.SURVEY S ON S.CN = P.SRV_CN AND S.ANN_INVENTORY = 'Y'),
roots AS (SELECT a.CN FROM ap a WHERE a.PREV_PLT_CN IS NULL OR a.PREV_PLT_CN NOT IN (SELECT CN FROM ap)),
walk(CN, root) AS (SELECT CN, CN FROM roots
                   UNION ALL SELECT c.CN, w.root FROM walk w JOIN ap c ON c.PREV_PLT_CN = w.CN)
SELECT w.CN, r.UNITCD, r.COUNTYCD, r.PLOT, a.UNITCD AS unitcd_raw, a.COUNTYCD AS countycd_raw, a.PLOT AS plot_raw
FROM walk w JOIN ap a ON a.CN = w.CN JOIN ap r ON r.CN = w.root
WHERE (a.UNITCD, a.COUNTYCD, a.PLOT) IS DISTINCT FROM (r.UNITCD, r.COUNTYCD, r.PLOT)""")
_kt = {}
for _tn, _cn, _dt in con.execute("""SELECT table_name, column_name, data_type FROM duckdb_columns()
                                    WHERE database_name='fraw' AND column_name IN ('UNITCD','COUNTYCD','PLOT','PLT_CN','CN')""").fetchall():
    _kt.setdefault(_tn, {})[_cn] = _dt
for (_t,) in con.execute("SELECT table_name FROM duckdb_tables() WHERE database_name='fraw'").fetchall():
    _k = _kt.get(_t, {})
    _on = "t.CN" if _t == "PLOT" else ("t.PLT_CN" if "PLT_CN" in _k else None)
    if _t == _t.upper() and _on and {"UNITCD", "COUNTYCD", "PLOT"} <= set(_k):
        _rep = ", ".join(f"CASE WHEN m.CN IS NULL THEN t.{x} ELSE CAST(m.{x} AS {_k[x]}) END AS {x}"
                         for x in ("UNITCD", "COUNTYCD", "PLOT"))
        con.execute(f'CREATE VIEW f.main."{_t}" AS SELECT t.* REPLACE ({_rep}) FROM fraw.main."{_t}" t '
                    f'LEFT JOIN f.main._canon m ON m.CN = CAST({_on} AS VARCHAR)')
    else:
        con.execute(f'CREATE VIEW f.main."{_t}" AS SELECT * FROM fraw.main."{_t}"')
con.execute(f"ATTACH '{a.curated_db}' AS new (READ_ONLY)")
FIT_SCOPE = ",".join(str(int(r[0])) for r in con.execute(
    "SELECT DISTINCT STATECD FROM new.curated_trees ORDER BY 1").fetchall())
print(f"fit scope (states fitted together in this DB): {FIT_SCOPE}")
if a.simb_db:
    con.execute(f"ATTACH '{a.simb_db}' AS newsimb (READ_ONLY)")
if a.old_slim:
    con.execute(f"ATTACH '{a.old_slim}' AS oslim (READ_ONLY)")
if a.new_slim:
    con.execute(f"ATTACH '{a.new_slim}' AS nslim (READ_ONLY)")

states = [int(s) for s in a.states.split(",")] if a.states else None
counties = [tuple(map(int, x.split(":"))) for x in a.counties.split(",")] if a.counties else None


def scope(alias="", s="STATECD", c="COUNTYCD"):
    p = f"{alias}." if alias else ""
    parts = []
    if states:
        parts.append(f"{p}{s} IN ({','.join(map(str, states))})")
    if counties:
        parts.append("(" + " OR ".join(f"({p}{s}={x} AND {p}{c}={y})" for x, y in counties) + ")")
    return " AND ".join(parts) or "TRUE"


def show(title, sql):
    print(f"\n=== {title}")
    print(con.execute(sql).df().to_string(index=False))


# ---- 1. coverage (reference built from raw FIADB only, independent of the pipeline) ------
con.execute(f"""
CREATE TEMP TABLE rc7 AS
SELECT T.STATECD, T.UNITCD, T.COUNTYCD, T.PLOT, T.SUBP, T.TREE,
       MIN(MAKE_DATE(P.MEASYEAR, COALESCE(NULLIF(CAST(P.MEASMON AS INTEGER), 0), 6), COALESCE(NULLIF(CAST(P.MEASDAY AS INTEGER), 0), 15))) AS d7
FROM f.TREE T JOIN f.PLOT P ON P.CN = T.PLT_CN JOIN f.SURVEY SV ON SV.CN = P.SRV_CN AND SV.ANN_INVENTORY = 'Y'
WHERE T.STATUSCD = 0 AND T.RECONCILECD = 7 AND {scope('T')}
GROUP BY ALL
""")
con.execute(f"""
CREATE TEMP TABLE ref AS
SELECT T.CN, T.STATECD, T.TPA_UNADJ, (T.DIA < 5) AS sapling
FROM f.TREE T
JOIN f.PLOT P ON P.CN = T.PLT_CN
JOIN f.SURVEY S ON S.CN = P.SRV_CN AND S.ANN_INVENTORY = 'Y'
JOIN f.COND C ON C.PLT_CN = T.PLT_CN AND C.CONDID = T.CONDID
JOIN f.SUBPLOT SU ON SU.PLT_CN = T.PLT_CN AND SU.SUBP = T.SUBP
LEFT JOIN rc7 ON rc7.STATECD=T.STATECD AND rc7.UNITCD=T.UNITCD AND rc7.COUNTYCD=T.COUNTYCD
     AND rc7.PLOT=T.PLOT AND rc7.SUBP=T.SUBP AND rc7.TREE=T.TREE
WHERE T.STATUSCD = 1 AND T.TPA_UNADJ > 0 AND T.SUBP <= 4 AND C.COND_STATUS_CD = 1
  AND P.PLOT_STATUS_CD < 3 AND COALESCE(SU.SUBP_STATUS_CD, -1) <> 3
  AND COALESCE(T.P2A_GRM_FLG, '') <> 'Y'
  AND NOT (rc7.d7 IS NOT NULL AND MAKE_DATE(P.MEASYEAR, COALESCE(NULLIF(CAST(P.MEASMON AS INTEGER), 0), 6), COALESCE(NULLIF(CAST(P.MEASDAY AS INTEGER), 0), 15)) < rc7.d7)
  AND T.SPCD IN (SELECT SPCD FROM f.REF_SPECIES)
  AND {scope('T')}
""")
con.execute(f"CREATE TEMP TABLE newct AS SELECT CN FROM new.curated_trees WHERE STATUSCD=1 AND {scope()}")
con.execute(f"CREATE TEMP TABLE oldct AS SELECT CN FROM f.main.curated_trees WHERE STATUSCD=1 AND {scope()}")
show("1. live-tree TPA coverage of FIADB TREE (independent reference: live, SUBP<=4, TPA>0, own "
     "COND_STATUS 1, PLOT_STATUS<3, SUBP_STATUS<>3, annual, not P2A, not before an RC7 "
     "erroneous tally, SPCD in REF_SPECIES)", """
SELECT r.STATECD, CASE WHEN sapling THEN 'sapling' ELSE 'ge5in' END AS size,
       count(*) AS ref_records,
       round(sum(TPA_UNADJ) FILTER (WHERE o.CN IS NOT NULL) / sum(TPA_UNADJ), 4) AS old_tpa_share,
       round(sum(TPA_UNADJ) FILTER (WHERE n.CN IS NOT NULL) / sum(TPA_UNADJ), 4) AS new_tpa_share,
       count(o.CN) AS old_records, count(n.CN) AS new_records
FROM ref r LEFT JOIN oldct o ON o.CN = r.CN LEFT JOIN newct n ON n.CN = r.CN
GROUP BY ALL ORDER BY 1, 2""")
show("1b. new live records NOT in the coverage reference (should be ~0)", """
SELECT count(*) AS n FROM newct n ANTI JOIN ref r ON r.CN = n.CN""")
show("1c. reference records not in new curated (should be 0)", """
SELECT count(*) n, round(sum(TPA_UNADJ),1) tpa FROM ref r ANTI JOIN newct n ON n.CN = r.CN""")
show("1d. curated rows on SUBP > 4 (auxiliary points; should be 0)", f"""
SELECT count(*) n FROM new.curated_trees WHERE SUBP > 4 AND {scope()}""")
# (f) dead/cut records lost — over EVERY tree of the raw reference, including trees that are
# absent from the new curated table altogether
con.execute(f"""
CREATE TEMP TABLE ref_trees AS
SELECT DISTINCT T.STATECD, T.UNITCD, T.COUNTYCD, T.PLOT, T.SUBP, T.TREE
FROM f.TREE T JOIN ref r ON r.CN = T.CN""")
show("1e. (f) dead/cut records of raw-reference trees (annual, SUBP<=4, not P2A, own COND_STATUS 1-4) "
     "and whether they are in new curated; trees of the reference absent from curated", f"""
WITH raw AS (
  SELECT T.CN, T.STATUSCD, T.TPA_UNADJ IS NULL AS tpa_null, C.COND_STATUS_CD AS cs
  FROM f.TREE T JOIN f.PLOT P ON P.CN=T.PLT_CN JOIN f.SURVEY S ON S.CN=P.SRV_CN AND S.ANN_INVENTORY='Y'
  JOIN f.COND C ON C.PLT_CN=T.PLT_CN AND C.CONDID=T.CONDID
  JOIN f.SUBPLOT SU ON SU.PLT_CN=T.PLT_CN AND SU.SUBP=T.SUBP
  JOIN ref_trees rt ON rt.STATECD=T.STATECD AND rt.UNITCD=T.UNITCD AND rt.COUNTYCD=T.COUNTYCD
       AND rt.PLOT=T.PLOT AND rt.SUBP=T.SUBP AND rt.TREE=T.TREE
  WHERE T.STATUSCD IN (2,3) AND T.SUBP<=4 AND COALESCE(T.P2A_GRM_FLG,'')<>'Y' AND C.COND_STATUS_CD IN (1,2,3,4)
    AND P.PLOT_STATUS_CD < 3 AND COALESCE(SU.SUBP_STATUS_CD, -1) <> 3)
SELECT raw.STATUSCD, raw.tpa_null, raw.cs IN (3,4) AS on_water, count(*) raw_n, count(n.CN) in_curated
FROM raw LEFT JOIN new.curated_trees n ON n.CN = raw.CN GROUP BY ALL ORDER BY ALL""")
r = con.execute("""SELECT count(*) FROM ref_trees rt ANTI JOIN (SELECT DISTINCT STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE
                   FROM new.curated_trees) n USING (STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE)""").fetchone()[0]
check("1e reference trees absent from curated", r == 0, f"({r})")

show("1f. phantom rows on entirely nonforest subplot-visits (should be 0)", """
SELECT 'curated_cohorts_landis' t, count(*) phantoms,
       count(*) FILTER (WHERE COALESCE(s.forest_subpcond_prop,0)+COALESCE(s.forest_micrcond_prop,0)=0) nonforest
FROM new.curated_cohorts_landis c JOIN new.clean_subplot_years s ON s.STATECD=c.statecd AND s.UNITCD=c.unitcd
  AND s.COUNTYCD=c.countycd AND s.PLOT=c.plot AND s.SUBP=c.subp AND s.MEASDATE=c.measdate
WHERE c.intro_type='phantom_backfill'
UNION ALL
SELECT 'tree_trajectories', count(*),
       count(*) FILTER (WHERE COALESCE(s.forest_subpcond_prop,0)+COALESCE(s.forest_micrcond_prop,0)=0)
FROM new.tree_trajectories c JOIN new.clean_subplot_years s ON s.STATECD=c.statecd AND s.UNITCD=c.unitcd
  AND s.COUNTYCD=c.countycd AND s.PLOT=c.plot AND s.SUBP=c.subp AND s.MEASDATE=c.measdate
WHERE c.intro_type='phantom_backfill'""")
# ---- independent re-derivation of the phantom-backfill block rules (spec, raw FIADB) -------
# raw subplot-visit forest proportions (scaffold: annual, PLOT_STATUS<3, SUBP<=4, SUBP_STATUS<>3)
con.execute(f"""
CREATE TEMP TABLE rv AS
SELECT P.STATECD, P.UNITCD, P.COUNTYCD, P.PLOT, SU.SUBP,
       MAKE_DATE(P.MEASYEAR, COALESCE(NULLIF(CAST(P.MEASMON AS INTEGER), 0), 6), COALESCE(NULLIF(CAST(P.MEASDAY AS INTEGER), 0), 15)) AS md,
       COALESCE(SUM(CASE WHEN C.COND_STATUS_CD=1 THEN SC.SUBPCOND_PROP END),0) AS fsub,
       COALESCE(SUM(CASE WHEN C.COND_STATUS_CD=1 THEN SC.MICRCOND_PROP END),0) AS fmic
FROM f.PLOT P JOIN f.SURVEY S ON S.CN=P.SRV_CN AND S.ANN_INVENTORY='Y'
JOIN f.SUBPLOT SU ON SU.PLT_CN=P.CN AND SU.SUBP<=4 AND SU.SUBP_STATUS_CD IS DISTINCT FROM 3
JOIN f.SUBP_COND SC ON SC.PLT_CN=P.CN AND SC.SUBP=SU.SUBP
JOIN f.COND C ON C.PLT_CN=P.CN AND C.CONDID=SC.CONDID
WHERE P.PLOT_STATUS_CD<3 AND {scope('P')} GROUP BY ALL""")
# raw RC8 closure breaks (plot, date) and whether they closed saplings
con.execute(f"""
CREATE TEMP TABLE rb AS
SELECT T.STATECD, T.UNITCD, T.COUNTYCD, T.PLOT,
       MAKE_DATE(P.MEASYEAR, COALESCE(NULLIF(CAST(P.MEASMON AS INTEGER), 0), 6), COALESCE(NULLIF(CAST(P.MEASDAY AS INTEGER), 0), 15)) AS bd,
       bool_or(T.PREVDIA < 5) AS sap_closed
FROM f.TREE T JOIN f.PLOT P ON P.CN=T.PLT_CN JOIN f.SURVEY S ON S.CN=P.SRV_CN AND S.ANN_INVENTORY='Y'
WHERE T.STATUSCD=0 AND T.RECONCILECD=8 AND T.SUBP<=4 AND {scope('T')} GROUP BY ALL""")
# raw RC7/RC8 closures (for the RC3 >= 5 in re-tally pairing of rule a)
con.execute(f"""
CREATE TEMP TABLE rclo AS
SELECT T.STATECD, T.UNITCD, T.COUNTYCD, T.PLOT, T.SUBP, T.SPCD, T.PREVDIA,
       MAKE_DATE(P.MEASYEAR, COALESCE(NULLIF(CAST(P.MEASMON AS INTEGER), 0), 6), COALESCE(NULLIF(CAST(P.MEASDAY AS INTEGER), 0), 15)) AS md
FROM f.TREE T JOIN f.PLOT P ON P.CN=T.PLT_CN JOIN f.SURVEY S ON S.CN=P.SRV_CN AND S.ANN_INVENTORY='Y'
WHERE T.STATUSCD=0 AND T.RECONCILECD IN (7,8) AND T.SUBP<=4 AND {scope('T')}""")
# r5 M-7: the re-tally candidate set comes from RAW FIADB (not from the pipeline's
# procedural_breaks): previous-visit live saplings on still-sampled subplots with NO record at the
# next visit (us), and the documented rule: a plot-visit is a silent re-tally if >= 3 and >= 50 %
# of its saplings are unrecorded (plot tier), or its state x (prev MANUAL, next MANUAL)
# transition loses >= 50 % of >= 20 pooled saplings and it has >= 1 unrecorded sapling
# (transition tier); an RC8-closure break with >= 1 unrecorded sapling counts too.
con.execute(f"""
CREATE TEMP TABLE pv AS
SELECT P.CN PLT_CN, P.STATECD,P.UNITCD,P.COUNTYCD,P.PLOT,P.INVYR,P.MANUAL,P.PLOT_STATUS_CD ps,
       MAKE_DATE(P.MEASYEAR, COALESCE(NULLIF(CAST(P.MEASMON AS INTEGER), 0), 6), COALESCE(NULLIF(CAST(P.MEASDAY AS INTEGER), 0), 15)) md
FROM f.PLOT P JOIN f.SURVEY S ON S.CN=P.SRV_CN AND S.ANN_INVENTORY='Y' WHERE {scope('P')};
CREATE TEMP TABLE pv2 AS SELECT *, lead(PLT_CN) OVER w ncn, lead(md) OVER w nmd, lead(MANUAL) OVER w nman, lead(ps) OVER w nps
FROM pv WINDOW w AS (PARTITION BY STATECD,UNITCD,COUNTYCD,PLOT ORDER BY md, INVYR);
CREATE TEMP TABLE us AS
SELECT T.STATECD,T.UNITCD,T.COUNTYCD,T.PLOT,T.SUBP,T.TREE, v.MANUAL, v.nman, v.nmd, (T2.CN IS NULL) unrec
FROM f.TREE T JOIN pv2 v ON v.PLT_CN=T.PLT_CN JOIN f.COND C ON C.PLT_CN=T.PLT_CN AND C.CONDID=T.CONDID
JOIN f.SUBPLOT SN ON SN.PLT_CN=v.ncn AND SN.SUBP=T.SUBP AND SN.SUBP_STATUS_CD=1
LEFT JOIN f.TREE T2 ON T2.PLT_CN=v.ncn AND T2.SUBP=T.SUBP AND T2.TREE=T.TREE
WHERE T.STATUSCD=1 AND T.DIA<5 AND T.TPA_UNADJ>0 AND T.SUBP<=4 AND C.COND_STATUS_CD=1 AND COALESCE(T.P2A_GRM_FLG,'')<>'Y'
  AND v.ps<3 AND v.ncn IS NOT NULL AND v.nps<3""")
con.execute("""
CREATE TEMP TABLE ub AS
WITH tr AS (SELECT STATECD, MANUAL, nman FROM us GROUP BY ALL HAVING count(*) >= 20 AND avg(unrec::INT) >= 0.5),
pvv AS (SELECT STATECD, UNITCD, COUNTYCD, PLOT, nmd, any_value(MANUAL) MANUAL, any_value(nman) nman,
               count(*) n, count(*) FILTER (WHERE unrec) nu FROM us GROUP BY STATECD, UNITCD, COUNTYCD, PLOT, nmd)
SELECT DISTINCT STATECD, UNITCD, COUNTYCD, PLOT, nmd AS bd FROM pvv
WHERE (nu >= 3 AND nu >= 0.5 * n)
   OR (nu >= 1 AND EXISTS (SELECT 1 FROM tr WHERE tr.STATECD = pvv.STATECD AND tr.MANUAL IS NOT DISTINCT FROM pvv.MANUAL
                           AND tr.nman IS NOT DISTINCT FROM pvv.nman))
   OR (nu >= 1 AND EXISTS (SELECT 1 FROM rb WHERE rb.STATECD = pvv.STATECD AND rb.UNITCD = pvv.UNITCD
                           AND rb.COUNTYCD = pvv.COUNTYCD AND rb.PLOT = pvv.PLOT AND rb.bd = pvv.nmd))""")
_ub = con.execute("""SELECT (SELECT count(*) FROM ub),
    (SELECT count(*) FROM (SELECT STATECD,UNITCD,COUNTYCD,PLOT,MEASDATE FROM new.procedural_breaks
                           WHERE break_kind='unrecorded' OR n_unrecorded_saplings > 0
                           EXCEPT SELECT STATECD,UNITCD,COUNTYCD,PLOT,bd FROM ub)),
    (SELECT count(*) FROM (SELECT STATECD,UNITCD,COUNTYCD,PLOT,bd FROM ub
                           EXCEPT SELECT STATECD,UNITCD,COUNTYCD,PLOT,MEASDATE FROM new.procedural_breaks
                           WHERE break_kind='unrecorded' OR n_unrecorded_saplings > 0))""").fetchone()
info("(b) raw re-tally candidate set vs pipeline procedural_breaks (unrecorded part)",
     f"(raw {_ub[0]:,}; pipeline-only {_ub[1]:,}; raw-only {_ub[2]:,})")
# member debut (first live record meeting the cohort-table filter)
con.execute(f"""
CREATE TEMP TABLE deb AS
SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE,
       arg_min(MEASDATE, MEASDATE) AS D, arg_min(DIA, MEASDATE) AS ddia,
       arg_min(CAST(RECONCILECD AS INT), MEASDATE) AS drc, arg_min(reconcile_new, MEASDATE) AS drn,
       any_value(spcd_resolved) AS dsp
FROM new.curated_trees
WHERE STATUSCD=1 AND estimated_age IS NOT NULL AND DRYBIO_AG IS NOT NULL AND TPA_UNADJ IS NOT NULL
  AND ROUND(estimated_age) >= 1 AND {scope()} GROUP BY ALL""")


def block_reasons(src_sql, rule_c=True):
    """Annotate (tree, p[, t]) rows with independently derived block reasons."""
    return f"""
    WITH src AS ({src_sql}),
    j AS (SELECT s.*, d.D, d.ddia, d.drc, d.drn, d.dsp FROM src s JOIN deb d USING (STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE))
    SELECT j.*,
      (j.p < j.D) AS pre_debut,
      (COALESCE(j.drn IN (6,9), FALSE)
       OR (j.drc = 3 AND j.ddia >= 5 AND EXISTS (SELECT 1 FROM rclo WHERE rclo.STATECD=j.STATECD AND rclo.UNITCD=j.UNITCD
            AND rclo.COUNTYCD=j.COUNTYCD AND rclo.PLOT=j.PLOT AND rclo.SUBP=j.SUBP AND rclo.md=j.D
            AND rclo.SPCD=j.dsp AND abs(rclo.PREVDIA-j.ddia)<=2))) AS r_a,
      EXISTS (SELECT 1 FROM rv WHERE rv.STATECD=j.STATECD AND rv.UNITCD=j.UNITCD AND rv.COUNTYCD=j.COUNTYCD
              AND rv.PLOT=j.PLOT AND rv.SUBP=j.SUBP AND rv.md >= j.p AND rv.md < j.D AND rv.fsub + rv.fmic <= 0) AS r_n,
      (j.ddia < 5 AND (EXISTS (SELECT 1 FROM rb WHERE rb.STATECD=j.STATECD AND rb.UNITCD=j.UNITCD AND rb.COUNTYCD=j.COUNTYCD
                             AND rb.PLOT=j.PLOT AND rb.bd > j.p AND rb.bd <= j.D AND rb.sap_closed)
                    OR EXISTS (SELECT 1 FROM ub WHERE ub.STATECD=j.STATECD AND ub.UNITCD=j.UNITCD AND ub.COUNTYCD=j.COUNTYCD
                             AND ub.PLOT=j.PLOT AND ub.bd > j.p AND ub.bd <= j.D)))
      OR (COALESCE(j.drn = 8, FALSE) AND EXISTS (SELECT 1 FROM rb WHERE rb.STATECD=j.STATECD AND rb.UNITCD=j.UNITCD
                             AND rb.COUNTYCD=j.COUNTYCD AND rb.PLOT=j.PLOT AND rb.bd > j.p AND rb.bd <= j.D)) AS r_b,
      {"(j.ddia >= 5 AND COALESCE(j.drc NOT IN (2,3,7,8), TRUE) AND EXISTS (SELECT 1 FROM rv WHERE rv.STATECD=j.STATECD AND rv.UNITCD=j.UNITCD AND rv.COUNTYCD=j.COUNTYCD AND rv.PLOT=j.PLOT AND rv.SUBP=j.SUBP AND rv.md >= j.p AND rv.md < j.D AND rv.fmic > 0))" if rule_c else "FALSE"} AS r_c
    FROM j"""


print("\n=== 1g. (g) phantoms whose source tree is individually blocked (independent re-derivation; all 0)")
for tab, src_sql, rc in (
    ("curated_cohorts_landis/_stdorg: tree-pass phantoms",
     "SELECT STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE, prior_measdate AS p FROM new.phantom_tree_sources", True),
    ("tree_trajectories phantoms (rule c not applied)",
     "SELECT statecd STATECD,unitcd UNITCD,countycd COUNTYCD,plot PLOT,subp SUBP,tree TREE, measdate AS p "
     "FROM new.tree_trajectories WHERE intro_type='phantom_backfill'", False)):
    r = con.execute(f"""SELECT count(*) n, count(*) FILTER (WHERE r_a) a, count(*) FILTER (WHERE r_n) nf,
                        count(*) FILTER (WHERE r_b) b, count(*) FILTER (WHERE r_c) c,
                        count(*) FILTER (WHERE r_a OR r_n OR r_b OR r_c) any_block
                        FROM ({block_reasons(src_sql, rc)})""").fetchone()
    print(f"  {tab}: phantoms={r[0]:,} blocked: a={r[1]} n={r[2]} b={r[3]} c={r[4]}  any={r[5]}")
    check(f"1g {tab.split(':')[0]} no individually-blocked source", r[5] == 0, f"({r[5]})")
n_ph = [con.execute(f"SELECT count(*) FILTER (WHERE intro_type='phantom_backfill') FROM new.{t} WHERE {scope('', 'statecd', 'countycd')}").fetchone()[0]
        for t in ("curated_cohorts_landis", "curated_cohorts_landis_stdorg")]
n_src = con.execute("SELECT (SELECT count(*) FROM new.phantom_tree_sources) + (SELECT count(*) FROM new.phantom_cohort_sources)").fetchone()[0]
check("1g phantom rows: landis == stdorg == tree sources + cohort sources", n_ph[0] == n_ph[1] == n_src,
      f"(landis {n_ph[0]:,}, stdorg {n_ph[1]:,}, sources {n_src:,})")

# ---- (a) Sim A cohort-pass phantoms: a source member survives its own blocks ---------------
r = con.execute(f"""
WITH m AS (SELECT s.STATECD,s.UNITCD,s.COUNTYCD,s.PLOT,s.SUBP, CAST(UNNEST(string_split(s.member_trees, ',')) AS BIGINT) AS TREE,
                  s.prior_measdate AS p, s.src_measdate AS t, s.species_symbol, s.prior_age
           FROM new.phantom_cohort_sources s),
br AS ({block_reasons("SELECT * FROM m", True)}),
g AS (SELECT br.*, EXISTS (SELECT 1 FROM f.TREE T JOIN f.PLOT P ON P.CN=T.PLT_CN JOIN f.SURVEY SV ON SV.CN = P.SRV_CN AND SV.ANN_INVENTORY = 'Y'
            WHERE T.STATECD=br.STATECD AND T.UNITCD=br.UNITCD AND T.COUNTYCD=br.COUNTYCD AND T.PLOT=br.PLOT
              AND T.SUBP=br.SUBP AND T.TREE=br.TREE AND T.STATUSCD=0 AND T.RECONCILECD IS DISTINCT FROM 7
              AND MAKE_DATE(P.MEASYEAR, COALESCE(NULLIF(CAST(P.MEASMON AS INTEGER), 0), 6), COALESCE(NULLIF(CAST(P.MEASDAY AS INTEGER), 0), 15)) > br.p
              AND MAKE_DATE(P.MEASYEAR, COALESCE(NULLIF(CAST(P.MEASMON AS INTEGER), 0), 6), COALESCE(NULLIF(CAST(P.MEASDAY AS INTEGER), 0), 15)) <= br.t) AS gap_cens,
            EXISTS (SELECT 1 FROM rv WHERE rv.STATECD=br.STATECD AND rv.UNITCD=br.UNITCD AND rv.COUNTYCD=br.COUNTYCD
              AND rv.PLOT=br.PLOT AND rv.SUBP=br.SUBP AND rv.md=br.p AND rv.fsub+rv.fmic<=0) AS gap_nf
      FROM br),
ok AS (SELECT *, CASE WHEN pre_debut THEN NOT (r_a OR r_n OR r_b OR r_c) ELSE NOT (gap_cens OR gap_nf) END AS survives FROM g)
SELECT count(DISTINCT (STATECD,UNITCD,COUNTYCD,PLOT,SUBP,species_symbol,p,prior_age)) slots,
       count(*) members, count(*) FILTER (WHERE NOT survives) blocked_members,
       count(DISTINCT (STATECD,UNITCD,COUNTYCD,PLOT,SUBP,species_symbol,p,prior_age)) FILTER (WHERE survives) slots_with_survivor
FROM ok""").fetchone()
print(f"\n=== (a) cohort-pass phantom slots={r[0]:,} members={r[1]:,} individually-blocked members={r[2]:,} "
      f"slots with >=1 surviving member={r[3]:,}")
check("(a) every cohort-pass phantom slot has a member that survives its own blocks", r[0] == r[3])

# ---- (b) silent re-tally: residual unrecorded saplings by state x MANUAL transition ----------
# (pv / pv2 / us: built from RAW FIADB above, with the block-rule re-derivation)
con.execute("""
CREATE TEMP TABLE us2 AS
SELECT us.*, (e.TREE IS NOT NULL) AS censored_unrec
FROM us LEFT JOIN (SELECT DISTINCT STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE,MEASDATE FROM new.tree_censor_events
                   WHERE reason='procedural_unrecorded') e
  ON e.STATECD=us.STATECD AND e.UNITCD=us.UNITCD AND e.COUNTYCD=us.COUNTYCD AND e.PLOT=us.PLOT
 AND e.SUBP=us.SUBP AND e.TREE=us.TREE AND e.MEASDATE=us.nmd""")
show("(b) unrecorded saplings by state x MANUAL transition (raw), censored by the pipeline, residual", """
SELECT STATECD, MANUAL prev_manual, nman next_manual, count(*) n_saplings,
       count(*) FILTER (WHERE unrec) n_unrecorded, round(avg(unrec::INT),3) share_unrecorded,
       count(*) FILTER (WHERE unrec AND censored_unrec) n_censored,
       count(*) FILTER (WHERE unrec AND NOT censored_unrec) n_residual,
       round(avg((unrec AND NOT censored_unrec)::INT),3) residual_share
FROM us2 GROUP BY ALL HAVING count(*) >= 20 ORDER BY 1,2,3""")
r = con.execute("""
WITH t AS (SELECT STATECD, MANUAL, nman, count(*) n, count(*) FILTER (WHERE unrec AND NOT censored_unrec) res FROM us2 GROUP BY ALL),
p AS (SELECT STATECD,UNITCD,COUNTYCD,PLOT,nmd, count(*) n, count(*) FILTER (WHERE unrec AND NOT censored_unrec) res FROM us2 GROUP BY ALL)
SELECT (SELECT count(*) FROM t WHERE n >= 20 AND res >= 0.5*n),
       (SELECT count(*) FROM p WHERE res >= 3 AND res >= 0.5*n),
       (SELECT count(*) FROM us2 WHERE censored_unrec AND NOT unrec)""").fetchone()
check("(b) no transition with >= 50 % uncensored unrecorded saplings (n >= 20)", r[0] == 0, f"({r[0]})")
check("(b) no plot-visit with >= 3 and >= 50 % uncensored unrecorded saplings", r[1] == 0, f"({r[1]})")
check("(b) censored only saplings that are really unrecorded", r[2] == 0, f"({r[2]})")
# r3 I6: independent of the detector's thresholds — in EVERY state x MANUAL transition with
# >= 20 saplings, at most 5 % of the previous visit's live saplings on still-sampled subplots
# may be unrecorded AND uncensored
bad = con.execute("""
SELECT STATECD, MANUAL, nman, count(*) n, avg((unrec AND NOT censored_unrec)::INT) res
FROM us2 GROUP BY ALL HAVING count(*) >= 20 AND avg((unrec AND NOT censored_unrec)::INT) > 0.05""").fetchall()
mx = con.execute("""SELECT max(res) FROM (SELECT avg((unrec AND NOT censored_unrec)::INT) res FROM us2
                    GROUP BY STATECD, MANUAL, nman HAVING count(*) >= 20)""").fetchone()[0]
check("(b) every transition (>= 20 saplings): unrecorded AND uncensored <= 5 %", not bad,
      f"(max {0 if mx is None else mx:.4f}; violations {bad})")
show("(b) censoring events by reason (pipeline)", """
SELECT reason, count(*) events, count(DISTINCT (STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE)) trees
FROM new.tree_censor_events GROUP BY 1 ORDER BY 2 DESC""")
show("(b) fates of procedural_unrecorded censored trees (final_tree_fate / terminal_fate / disturbance)", """
WITH t AS (SELECT DISTINCT STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE FROM new.tree_censor_events WHERE reason='procedural_unrecorded')
SELECT 'final_tree_fate' k, c.final_tree_fate v, count(DISTINCT (c.STATECD,c.UNITCD,c.COUNTYCD,c.PLOT,c.SUBP,c.TREE)) n
FROM t JOIN new.curated_trees c USING (STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE) GROUP BY ALL
UNION ALL
SELECT 'terminal_fate', tt.terminal_fate, count(DISTINCT (tt.statecd,tt.unitcd,tt.countycd,tt.plot,tt.subp,tt.tree))
FROM t JOIN new.tree_trajectories tt ON tt.statecd=t.STATECD AND tt.unitcd=t.UNITCD AND tt.countycd=t.COUNTYCD
  AND tt.plot=t.PLOT AND tt.subp=t.SUBP AND tt.tree=t.TREE GROUP BY ALL ORDER BY 1, 3 DESC""")

# ---- (c1) fates from the FIRST dead/cut record, checked from raw records -----------------------
# (replaces the circular GRM check: in these states CUT <=> AGENTCD 80)
con.execute(f"""
CREATE TEMP TABLE rawdead AS
SELECT CAST(T.CN AS VARCHAR) cn, T.STATECD,T.UNITCD,T.COUNTYCD,T.PLOT,T.SUBP,T.TREE, T.STATUSCD, T.AGENTCD,
       T.PREV_STATUS_CD ps, C.COND_STATUS_CD cs,
       MAKE_DATE(P.MEASYEAR, COALESCE(NULLIF(CAST(P.MEASMON AS INTEGER), 0), 6), COALESCE(NULLIF(CAST(P.MEASDAY AS INTEGER), 0), 15)) md
FROM f.TREE T JOIN f.PLOT P ON P.CN=T.PLT_CN JOIN f.SURVEY SV ON SV.CN = P.SRV_CN AND SV.ANN_INVENTORY = 'Y'
JOIN f.COND C ON C.PLT_CN=T.PLT_CN AND C.CONDID=T.CONDID
WHERE T.STATUSCD IN (2,3) AND T.SUBP<=4 AND COALESCE(T.P2A_GRM_FLG,'')<>'Y' AND C.COND_STATUS_CD IN (1,2,3,4)
  AND {scope('T')}""")
con.execute(f"""
CREATE TEMP TABLE curdead AS
SELECT STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE, arg_min(CAST(CN AS VARCHAR), MEASDATE) first_cn,
       any_value(final_tree_fate) final_fate
FROM new.curated_trees WHERE STATUSCD IN (2,3) AND {scope()} GROUP BY ALL""")
r = con.execute("""
WITH fr AS (SELECT STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE, min(md) md0 FROM rawdead GROUP BY ALL),
j AS (
  SELECT c.*, d.STATUSCD, d.AGENTCD, d.cs,
         CASE WHEN (d.STATUSCD=3 OR d.AGENTCD=80) AND d.cs<>1 THEN 'diverted_landuse'
              WHEN d.STATUSCD=3 OR d.AGENTCD=80 THEN 'harvest'
              ELSE 'dead' END exp,
         (d.md = fr.md0) is_first_raw
  FROM curdead c JOIN rawdead d ON d.cn=c.first_cn
  JOIN fr ON fr.STATECD=d.STATECD AND fr.UNITCD=d.UNITCD AND fr.COUNTYCD=d.COUNTYCD AND fr.PLOT=d.PLOT
         AND fr.SUBP=d.SUBP AND fr.TREE=d.TREE)
SELECT count(*) n,
       count(*) FILTER (WHERE (exp='harvest' AND final_fate='harvest_explicit') OR (exp='diverted_landuse' AND final_fate='diverted_landuse')
                        OR (exp='dead' AND final_fate IN ('dead','harvest_inferred'))) agree,
       count(*) FILTER (WHERE is_first_raw) first_raw
FROM j""").fetchone()
check("(c1) final fate = fate of the FIRST raw dead/cut record (>= 99 %)", r[0] == 0 or r[1] / r[0] >= 0.99,
      f"({r[1]:,}/{r[0]:,} agree; {r[2]:,}/{r[0]:,} are the raw key's first dead record)")
show("(c1) final fate vs raw class of the tree's first dead/cut record", """
SELECT CASE WHEN (d.STATUSCD=3 OR d.AGENTCD=80) AND d.cs<>1 THEN 'diverted_landuse' WHEN d.STATUSCD=3 OR d.AGENTCD=80 THEN 'harvest' ELSE 'dead' END raw_class,
       c.final_fate, count(*) n FROM curdead c JOIN rawdead d ON d.cn=c.first_cn GROUP BY ALL ORDER BY 1, 3 DESC""")

# ---- (c2) re-records never produce events -------------------------------------------------------
r = con.execute("""
SELECT count(*) FILTER (WHERE d.ps = 2) rerec_raw, count(*) FILTER (WHERE d.ps = 2 AND NOT t.rerecord_dead) unflagged
FROM rawdead d JOIN new.curated_trees t ON CAST(t.CN AS VARCHAR) = d.cn""").fetchone()
check("(c2) every raw PREV_STATUS_CD=2 dead record in curated is flagged rerecord_dead", r[1] == 0,
      f"({r[0]:,} re-records, {r[1]} unflagged)")
r = con.execute(f"""
WITH ev AS (SELECT STATECD,UNITCD,COUNTYCD,PLOT,SUBP,MEASDATE, sum(n_harvested + n_disturbance_dead) e
            FROM new.curated_cohorts WHERE {scope()} GROUP BY ALL HAVING sum(n_harvested + n_disturbance_dead) > 0),
fd AS (SELECT DISTINCT STATECD,UNITCD,COUNTYCD,PLOT,SUBP,MEASDATE FROM new.curated_trees
       WHERE STATUSCD IN (2,3) AND NOT rerecord_dead AND {scope()})
SELECT count(*), sum(e) FROM ev ANTI JOIN fd USING (STATECD,UNITCD,COUNTYCD,PLOT,SUBP,MEASDATE)""").fetchone()
check("(c2) no cohort harvest/disturbance event at a subplot-visit whose dead records are all re-records",
      (r[0] or 0) == 0, f"({r[0]} subplot-visits, {r[1]} events)")
r = con.execute(f"""
WITH s AS (SELECT DISTINCT statecd,unitcd,countycd,plot,subp FROM new.curated_cohorts_landis WHERE subp_has_dstrb AND {scope('', 'statecd', 'countycd')}),
e AS (SELECT DISTINCT STATECD,UNITCD,COUNTYCD,PLOT,SUBP FROM new.curated_trees
      WHERE STATUSCD IN (2,3) AND NOT rerecord_dead AND {scope()})
SELECT count(*) FROM s ANTI JOIN e ON e.STATECD=s.statecd AND e.UNITCD=s.unitcd AND e.COUNTYCD=s.countycd AND e.PLOT=s.plot AND e.SUBP=s.subp""").fetchone()[0]
check("(c2) every subp_has_dstrb subplot has a first (non-re-record) dead/cut record", r == 0, f"({r})")
# r5 M-7: re-derived from RAW records, with ATTRIBUTION: a subp_has_dstrb subplot needs a tree whose
# FIRST raw dead/cut record (not a PREV_STATUS_CD=2 re-record) is a cut (STATUSCD 3 or AGENTCD 80) on
# forest, a harvest-inferred death (STATUSCD 2, AGENTCD null, condition TRTCD 10), or a death on a
# condition with a disturbance code (DSTRBCD1-3 not 0 / 60). Natural deaths and diversions never count.
con.execute(f"""
CREATE TEMP TABLE rawattr AS
WITH fr AS (SELECT T.STATECD,T.UNITCD,T.COUNTYCD,T.PLOT,T.SUBP,T.TREE, T.STATUSCD, T.AGENTCD, T.PREV_STATUS_CD ps,
                   C.COND_STATUS_CD cs, C.TRTCD1, C.TRTCD2, C.TRTCD3, C.DSTRBCD1, C.DSTRBCD2, C.DSTRBCD3,
                   MAKE_DATE(P.MEASYEAR, COALESCE(NULLIF(CAST(P.MEASMON AS INTEGER), 0), 6), COALESCE(NULLIF(CAST(P.MEASDAY AS INTEGER), 0), 15)) md
            FROM f.TREE T JOIN f.PLOT P ON P.CN=T.PLT_CN JOIN f.SURVEY SV ON SV.CN = P.SRV_CN AND SV.ANN_INVENTORY = 'Y'
            JOIN f.COND C ON C.PLT_CN=T.PLT_CN AND C.CONDID=T.CONDID
            WHERE T.STATUSCD IN (2,3) AND T.SUBP<=4 AND COALESCE(T.P2A_GRM_FLG,'')<>'Y' AND C.COND_STATUS_CD IN (1,2,3,4)
              AND {scope('T')}
            QUALIFY row_number() OVER (PARTITION BY T.STATECD,T.UNITCD,T.COUNTYCD,T.PLOT,T.SUBP,T.TREE ORDER BY md) = 1)
SELECT DISTINCT STATECD,UNITCD,COUNTYCD,PLOT,SUBP FROM fr
WHERE ps IS DISTINCT FROM 2 AND cs = 1 AND (
      STATUSCD = 3 OR AGENTCD = 80
   OR (STATUSCD = 2 AND AGENTCD IS NULL AND 10 IN (COALESCE(TRTCD1,-1), COALESCE(TRTCD2,-1), COALESCE(TRTCD3,-1)))
   OR (STATUSCD = 2 AND (COALESCE(DSTRBCD1,0) NOT IN (0,60) OR COALESCE(DSTRBCD2,0) NOT IN (0,60)
                         OR COALESCE(DSTRBCD3,0) NOT IN (0,60))))""")
r = con.execute(f"""
WITH s AS (SELECT DISTINCT statecd,unitcd,countycd,plot,subp FROM new.curated_cohorts_landis WHERE subp_has_dstrb AND {scope('', 'statecd', 'countycd')})
SELECT (SELECT count(*) FROM s), (SELECT count(*) FROM s ANTI JOIN rawattr e ON e.STATECD=s.statecd AND e.UNITCD=s.unitcd
          AND e.COUNTYCD=s.countycd AND e.PLOT=s.plot AND e.SUBP=s.subp),
       (SELECT count(*) FROM rawattr e WHERE e.STATECD || '_' || e.UNITCD || '_' || e.COUNTYCD || '_' || e.PLOT || '_' || e.SUBP IN
          (SELECT DISTINCT statecd || '_' || unitcd || '_' || countycd || '_' || plot || '_' || subp FROM new.curated_cohorts_landis)
          AND NOT EXISTS (SELECT 1 FROM s WHERE s.statecd=e.STATECD AND s.unitcd=e.UNITCD AND s.countycd=e.COUNTYCD
                          AND s.plot=e.PLOT AND s.subp=e.SUBP))""").fetchone()
check("(c2) every subp_has_dstrb subplot has a raw disturbance- or cut-attributed FIRST dead/cut record",
      r[1] == 0, f"({r[1]} of {r[0]:,} flagged subplots lack one)")
info("(c2) raw-attributed Sim A subplots NOT flagged subp_has_dstrb (tree without an age / cohort, or a split key)",
     f"({r[2]:,})")
n_rr = con.execute("""
SELECT count(*) FROM new.tree_trajectories tt JOIN new.curated_trees t
  ON t.STATECD=tt.statecd AND t.UNITCD=tt.unitcd AND t.COUNTYCD=tt.countycd AND t.PLOT=tt.plot AND t.SUBP=tt.subp
 AND t.TREE=tt.tree AND t.MEASDATE=tt.measdate
WHERE t.rerecord_dead""").fetchone()[0]
check("(c2) re-records are never trajectory slots", n_rr == 0, f"({n_rr})")

# ---- (c3) DIVERSION handling (M6): cuts on now-nonforest land -----------------------------------
r = con.execute("""
SELECT count(*) n, count(*) FILTER (WHERE c.final_fate = 'diverted_landuse') ok
FROM curdead c JOIN rawdead d ON d.cn = c.first_cn
WHERE (d.STATUSCD = 3 OR d.AGENTCD = 80) AND d.cs <> 1""").fetchone()
check("(c3) every first cut record on a nonforest/water condition is diverted_landuse", r[0] == r[1],
      f"({r[1]:,}/{r[0]:,})")
r = con.execute("""
SELECT count(*) FROM new.tree_trajectories
WHERE cut_event_type IS NOT NULL AND terminal_fate = 'diverted_landuse' AND is_terminal""").fetchone()[0]
info("(c3) diverted trees whose terminal slot carries a cut-event class of OTHER trees of the condition", f"({r})")
con.execute(f"""
CREATE TEMP TABLE grm AS
SELECT CAST(TRE_CN AS VARCHAR) cn,
       regexp_replace(CASE WHEN SUBP_COMPONENT_AL_FOREST IS NOT NULL AND SUBP_COMPONENT_AL_FOREST <> 'NOT USED'
                           THEN SUBP_COMPONENT_AL_FOREST ELSE MICR_COMPONENT_AL_FOREST END, '[0-9]', '') comp
FROM f.TREE_GRM_COMPONENT WHERE {scope()}""")
show("(c3) INFO: GRM component of each tree's first dead/cut record vs final_tree_fate", """
SELECT g.comp, c.final_fate, count(*) n FROM curdead c JOIN grm g ON g.cn = c.first_cn
WHERE g.comp IN ('CUT','MORTALITY','DIVERSION') GROUP BY ALL ORDER BY 1, 3 DESC""")

# ---- (d) median age monotone in DIA class for the top-25 species ------------------------------
# Evaluated per stratum. ENFORCED for planted stands. r4: re-enforcement for natural stands (now
# on one per-tree scale, raw TreeAge) was tried per the user's rule "keep INFO only if it fails";
# it FAILED in the r4 whole-state runs (GA 1/25, FL 5/23 species non-monotone), so natural stays
# INFO. The pooled result is printed as INFO.
def _mono(where):
    d = con.execute(f"""
    WITH l AS (SELECT spcd_resolved sp, estimated_age a,
                 CASE WHEN DIA>=1 AND DIA<3 THEN 1 WHEN DIA>=3 AND DIA<5 THEN 2 WHEN DIA>=5 AND DIA<7 THEN 3
                      WHEN DIA>=7 AND DIA<9 THEN 4 END k
               FROM new.curated_trees WHERE STATUSCD=1 AND estimated_age IS NOT NULL AND {where} AND {scope()}),
    top AS (SELECT sp FROM l GROUP BY sp ORDER BY count(*) DESC, sp LIMIT 25),
    m AS (SELECT sp, k, count(*) n, median(a) med FROM l WHERE k IS NOT NULL AND sp IN (SELECT sp FROM top) GROUP BY ALL)
    SELECT sp, list(med ORDER BY k) meds, min(n) min_n, count(*) nk FROM m GROUP BY sp ORDER BY sp""").fetchall()
    viol, tested = [], 0
    for sp, meds, mn, nk in d:
        if nk < 4 or mn < 50:
            continue
        tested += 1
        if any(meds[i] > meds[i + 1] for i in range(len(meds) - 1)):
            viol.append((sp, [round(x, 1) for x in meds]))
    return viol, tested
for label, where, enforce in (("planted", "cond_stdorgcd = 1", True),
                              ("natural", "COALESCE(cond_stdorgcd, 0) <> 1", False),
                              ("pooled", "TRUE", False)):
    viol, tested = _mono(where)
    print(f"\n=== (d) {label}: median age by DIA class 1-3/3-5/5-7/7-9 in: {tested} species tested, {len(viol)} non-monotone")
    for v in viol:
        print("    ", v)
    name = f"(d) {label}: median age monotone in DIA class (top-25 species, >= 50 records/class)"
    if enforce:
        check(name, not viol, f"({len(viol)}/{tested})")
    else:
        info(name, f"({len(viol)}/{tested} non-monotone; not enforced)")
show("(d) negative / < 1 yr live ages", f"""
SELECT age_source, count(*) FILTER (WHERE estimated_age < 0) neg, count(*) FILTER (WHERE estimated_age < 1) lt1,
       count(*) FILTER (WHERE ROUND(estimated_age) < 1) round_lt1, count(*) n
FROM new.curated_trees WHERE STATUSCD=1 AND {scope()} GROUP BY 1 ORDER BY 1""")

# ---- (h) pre-registered age-step acceptance (r3) ----------------------------------------------
# Per state x stratum (planted = STDORGCD 1 / natural): the median within-condition-visit step
# from 3-5 in to 5-7 in must be <= 2 x the 5-7 -> 7-9 in step + 2 yr (all species).
def _step(lo, mid, hi):
    return f"""
    WITH t AS (SELECT STATECD,UNITCD,COUNTYCD,PLOT,CONDID,MEASDATE, (cond_stdorgcd = 1) AS planted,
                 CASE WHEN DIA>={lo} AND DIA<{mid} THEN 's' WHEN DIA>={mid} AND DIA<{hi} THEN 'p' END k,
                 estimated_age a
               FROM new.curated_trees WHERE STATUSCD=1 AND estimated_age IS NOT NULL AND DIA>={lo} AND DIA<{hi}
                 AND {scope()}),
    g AS (SELECT STATECD,UNITCD,COUNTYCD,PLOT,CONDID,MEASDATE, COALESCE(any_value(planted), FALSE) planted,
            median(a) FILTER (WHERE k='s') sm, median(a) FILTER (WHERE k='p') lg,
            count(*) FILTER (WHERE k='s') ns, count(*) FILTER (WHERE k='p') np
          FROM t GROUP BY STATECD,UNITCD,COUNTYCD,PLOT,CONDID,MEASDATE HAVING ns>=1 AND np>=1)
    SELECT STATECD, planted, count(*) cv, median(lg-sm) step FROM g GROUP BY ALL"""
st1 = {(r[0], r[1]): (r[2], r[3]) for r in con.execute(_step(3, 5, 7)).fetchall()}
st2 = {(r[0], r[1]): (r[2], r[3]) for r in con.execute(_step(5, 7, 9)).fetchall()}
# User decision (ACCEPT + FLAG): the step test assumes saplings and 5-7 in trees form ONE growth
# sequence, which holds in even-aged PLANTED stands but not in uneven-aged natural stands (the
# step exists even among TreeAge-consistent saplings there: mapped-estage-only ~9 yr). So it is
# ENFORCED only for planted strata with >= H_MIN_VISITS condition-visits in both steps, and
# reported as INFO for natural strata or thinner planted strata. Natural model ages carry
# age_domain = 'natural_treeage_scale'.
H_MIN_VISITS = 50
print("\n=== (h) within-condition-visit median age steps: 3-5 -> 5-7 in vs 5-7 -> 7-9 in")
for k in sorted(set(st1) & set(st2)):
    (n1, s1), (n2, s2) = st1[k], st2[k]
    lim = 2 * s2 + 2
    name = f"(h) state {k[0]} {'planted' if k[1] else 'natural'}: step(3-5->5-7)={s1:.1f} <= 2*{s2:.1f}+2={lim:.1f}"
    detail = f"(cond-visits {n1:,} / {n2:,}; {'holds' if s1 <= lim else 'exceeds'})"
    if k[1] and min(n1, n2) >= H_MIN_VISITS:
        check(name, s1 <= lim, detail)
    else:
        info(name, detail + (" — natural stand: not enforced" if not k[1]
                             else f" — < {H_MIN_VISITS} condition-visits: not enforced"))
show("(h) age_domain of model ages (live records)", f"""
SELECT age_domain, count(*) n FROM new.curated_trees WHERE STATUSCD=1 AND age_source='model' AND {scope()}
GROUP BY 1 ORDER BY 2 DESC""")

# ---- (i) I3b: TreeAge coverage — every tree with a valid TreeAge record has estage everywhere ----
r = con.execute(f"""
WITH t AS (SELECT DISTINCT STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE FROM new.curated_trees
           WHERE estage_raw > 1 AND DIA >= 5 AND {scope()})
SELECT count(*) FROM new.curated_trees c JOIN t USING (STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE)
WHERE c.age_from_estage IS NULL""").fetchone()[0]
check("(i) every record of a tree with a valid TreeAge record carries the propagated estage", r == 0, f"({r})")
show("(i) share of live >= 5 in records whose final age source is the model, by INVYR era (a10)", f"""
SELECT CASE WHEN INVYR <= 1998 THEN 'a <=1998' WHEN INVYR <= 2009 THEN 'b 1999-2009'
            WHEN INVYR <= 2016 THEN 'c 2010-2016' ELSE 'd 2017+' END era, count(*) n,
       round(avg((age_source='model')::INT),3) share_model, round(avg((estage_raw IS NOT NULL)::INT),3) share_has_treeage
FROM new.curated_trees WHERE STATUSCD=1 AND DIA>=5 AND {scope()} GROUP BY 1 ORDER BY 1""")
show("(i) model - mapped-estage gap on records that have both, by era x DIA class (median yr)", f"""
SELECT CASE WHEN INVYR <= 1998 THEN 'a <=1998' WHEN INVYR <= 2009 THEN 'b 1999-2009'
            WHEN INVYR <= 2016 THEN 'c 2010-2016' ELSE 'd 2017+' END era,
       CASE WHEN DIA<10 THEN 'a 5-10' WHEN DIA<15 THEN 'b 10-15' ELSE 'c 15+' END dia_cls, count(*) n,
       round(median(age_from_model - age_from_estage),1) med_gap
FROM new.curated_trees WHERE STATUSCD=1 AND DIA>=5 AND age_from_model IS NOT NULL AND age_from_estage IS NOT NULL
  AND {scope()} GROUP BY ALL ORDER BY 1,2""")
show("(i) age model out-of-fold MAE / bias by age decile and DIA class (all rows, outliers included)",
     "SELECT model, \"by\", source, stratum_pn, stratum, n, round(median_target,1) median_target, round(mae,2) mae, round(bias,2) bias FROM new.age_model_cv ORDER BY 1, 2, 3, 4, 5")

# ---- (j) I3c pre-registered: median model - mapped-estage gap within +-3 yr ------------------------
rows = con.execute(f"""
SELECT STATECD, (age_stratum = 'planted') planted,   -- r5 M-5: the tree's age-scale stratum
       CASE WHEN DIA<10 THEN 'a 5-10' WHEN DIA<15 THEN 'b 10-15' ELSE 'c 15+' END dia_cls,
       count(DISTINCT (STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE)) trees, median(age_from_model - age_from_estage) gap
FROM new.curated_trees WHERE STATUSCD=1 AND DIA>=5 AND age_from_model IS NOT NULL AND age_from_estage IS NOT NULL
  AND {scope()} GROUP BY ALL ORDER BY ALL""").fetchall()
print("\n=== (j) median model - mapped-estage gap (yr) per state x stratum x DIA class (>= 200 trees enforced)")
for st_, pl_, dc, nt, gap in rows:
    name = f"(j) state {st_} {'planted' if pl_ else 'natural'} DIA {dc[2:]}: median gap {gap:+.1f} within +-3"
    if nt >= 200:
        check_known(("j", st_, "planted" if pl_ else "natural", dc[2:]), name, abs(gap) <= 3, gap, f"({nt:,} trees)")
    else:
        info(name, f"({nt:,} trees < 200: not enforced)")

# ---- (k) I6: independent age truth from FIADB SITETREE bored ages ---------------------------------
# AGEDIA = breast-height age. Linkage (SITETREE has no TRE_CN): (1) TREE row with the same
# PLT_CN + SUBP + TREE number, same SPCD and |DIA diff| <= 1 in; else (2) the UNIQUE live tally tree
# on the same PLT_CN + CONDID with the same SPCD and the same DIA. Truth = AGEDIA + years to breast
# height: 2 yr softwoods / 5 yr hardwoods (REF_SPECIES.SFTWD_HRDWD). That allowance is the
# audit-specified rule of thumb (southern pines reach 4.5 ft in ~2 yr on average sites, hardwoods
# in ~3-6); FIADB carries no years-to-breast-height field. Site trees are DOMINANT / codominant
# free-growing trees chosen for site index, so they are younger-than-stand-mean in uneven-aged
# stands and the check says nothing about suppressed understory trees.
con.execute(f"""
CREATE TEMP TABLE st AS
SELECT S.CN scn, S.PLT_CN, S.CONDID, S.SPCD, S.DIA, S.AGEDIA, CAST(S.SUBP AS BIGINT) subp_i, S.TREE
FROM f.SITETREE S JOIN f.PLOT P ON P.CN=S.PLT_CN JOIN f.SURVEY SV ON SV.CN = P.SRV_CN AND SV.ANN_INVENTORY = 'Y'
WHERE S.AGEDIA > 0 AND {scope('S')}""")
con.execute("""
CREATE TEMP TABLE stl AS
WITH k1 AS (SELECT st.scn, CAST(T.CN AS VARCHAR) tcn FROM st JOIN f.TREE T
            ON T.PLT_CN=st.PLT_CN AND T.SUBP=st.subp_i AND T.TREE=st.TREE AND T.SPCD=st.SPCD
           AND abs(T.DIA-st.DIA)<=1 AND T.STATUSCD=1),
k2 AS (SELECT st.scn, any_value(CAST(T.CN AS VARCHAR)) tcn, count(*) nm FROM st JOIN f.TREE T
            ON T.PLT_CN=st.PLT_CN AND T.CONDID=st.CONDID AND T.SPCD=st.SPCD AND T.DIA=st.DIA
           AND T.STATUSCD=1 AND T.SUBP<=4
       WHERE st.scn NOT IN (SELECT scn FROM k1) GROUP BY st.scn HAVING count(*)=1)
SELECT st.*, l.tcn, l.how FROM st JOIN (SELECT scn, tcn, 'key' how FROM k1 UNION ALL SELECT scn, tcn, 'plot_cond_sp_dia' FROM k2) l USING (scn)""")
con.execute("""
CREATE TEMP TABLE stc AS
SELECT x.STATECD, (x.cond_stdorgcd = 1) planted, l.how, CASE WHEN r.SFTWD_HRDWD='S' THEN 'softwood' ELSE 'hardwood' END wood,
       l.AGEDIA + CASE WHEN r.SFTWD_HRDWD='S' THEN 2 ELSE 5 END truth,
       x.estimated_age, x.age_from_estage, x.estage_raw, x.age_from_model, x.age_source
FROM stl l JOIN new.curated_trees x ON CAST(x.CN AS VARCHAR) = l.tcn
LEFT JOIN f.REF_SPECIES r ON r.SPCD = x.spcd_resolved WHERE x.STATUSCD = 1""")
n_st, n_link = con.execute("SELECT (SELECT count(*) FROM st), (SELECT count(*) FROM stc)").fetchone()
print(f"\n=== (k) SITETREE bored ages: {n_st:,} aged site trees in scope, {n_link:,} linked to a curated live record")
show("(k) SITETREE linkage method", "SELECT how, count(*) n FROM stc GROUP BY 1")
show("(k) ages vs SITETREE truth (AGEDIA + 2 yr softwood / 5 yr hardwood): MAE and median bias", """
SELECT STATECD, planted, count(*) n,
  round(avg(abs(estimated_age-truth)),1) mae_final, round(median(estimated_age-truth),1) bias_final,
  round(avg(abs(age_from_estage-truth)),1) mae_mapped, round(median(age_from_estage-truth),1) bias_mapped,
  round(avg(abs(estage_raw-truth)) FILTER (WHERE estage_raw>1),1) mae_raw, round(median(estage_raw-truth) FILTER (WHERE estage_raw>1),1) bias_raw,
  round(avg(abs(age_from_model-truth)),1) mae_model, round(median(age_from_model-truth),1) bias_model
FROM stc GROUP BY ALL ORDER BY ALL""")
# D13: natural strata are INFO, reported per state x wood group x link method (raw TreeAge runs
# OLD for softwoods and YOUNG for hardwoods against site trees, and most links are proxies);
# PLANTED strata stay ENFORCED (|median bias| <= 5 yr, >= 100 linked site trees).
show("(k) NATURAL stands (INFO, D13): final / raw-TreeAge median bias by state x wood group x link method", """
SELECT STATECD, wood, how, count(*) n,
  round(avg(abs(estimated_age-truth)),1) mae_final, round(median(estimated_age-truth),1) bias_final,
  round(median(estage_raw-truth) FILTER (WHERE estage_raw>1),1) bias_raw
FROM stc WHERE NOT COALESCE(planted, false) GROUP BY ALL ORDER BY ALL""")
for st_, pl_, n, b in con.execute("SELECT STATECD, COALESCE(planted,false), count(*), median(estimated_age-truth) FROM stc GROUP BY ALL ORDER BY ALL").fetchall():
    name = f"(k) state {st_} {'planted' if pl_ else 'natural'}: |median bias of final age vs SITETREE| = {abs(b):.1f} <= 5"
    if not pl_:
        info(name, f"(n {n:,}; natural: INFO per D13, see the wood-group table)")
    elif n >= 100:
        check_known(("k", st_, "planted"), name, abs(b) <= 5, b, f"(n {n:,})")
    else:
        info(name, f"(n {n:,} < 100: not enforced)")

# ---- (l) r4 ACCEPT + FLAG: known-bias age_domain flags match their rules exactly ---------------
# (flag, rule, fit scopes); outside its scopes a flag must not appear at all
_kb_rules = {
    "known_bias_fl_planted_model_10plus": ("STATECD=12 AND age_stratum='planted' AND age_source='model' AND DIA>=10",
                                           ("12,21,28",)),
}
for flag_, (rule_, scopes_) in _kb_rules.items():
    if FIT_SCOPE not in scopes_:
        rule_ = "FALSE"
    r = con.execute(f"""SELECT count(*) FILTER (WHERE ({rule_}) AND age_domain IS DISTINCT FROM '{flag_}'),
                               count(*) FILTER (WHERE NOT ({rule_}) AND age_domain = '{flag_}'),
                               count(*) FILTER (WHERE age_domain = '{flag_}' AND STATUSCD=1)
                        FROM new.curated_trees WHERE {scope()}""").fetchone()
    check(f"(l) known-bias flag {flag_} set exactly on its rule", r[0] == 0 and r[1] == 0,
          f"(missing {r[0]:,}, extra {r[1]:,}; live flagged {r[2]:,})")

# ---- (l) M1 / M2 / M3 / I4 --------------------------------------------------------------------------
if a.simb_db:
    # an unmatched drop is MISALIGNED only if its cohort has a real live >= 5 in member at that
    # visit (such a cohort exists in Sim B, so the keys must match); otherwise it is a
    # sapling-only cohort, absent from Sim B by design
    r = con.execute(f"""
    WITH d AS (SELECT * FROM newsimb.cohort_disturbance WHERE {scope('', 'statecd', 'countycd')}),
    m AS (SELECT d.* FROM d ANTI JOIN newsimb.curated_cohorts_landis_simB c USING (statecd,unitcd,countycd,plot,subp,species_symbol,measdate,age_calc)),
    tr AS (SELECT DISTINCT statecd,unitcd,countycd,plot,subp,species_symbol,measdate,age_calc FROM new.tree_trajectories
           WHERE intro_type='real' AND dia >= 5)
    SELECT (SELECT count(*) FROM d), (SELECT count(*) FROM m),
           (SELECT count(*) FROM m SEMI JOIN tr USING (statecd,unitcd,countycd,plot,subp,species_symbol,measdate,age_calc))""").fetchone()
    check("(l) M1: every disturbance drop of a cohort with a >= 5 in member matches Sim B exactly", r[2] == 0,
          f"({r[1]:,}/{r[0]:,} drops unmatched: {r[2]} misaligned, the rest sapling-only cohorts absent from Sim B)")
    n_b = con.execute(f"""
    WITH tr AS (SELECT DISTINCT statecd,unitcd,countycd,plot,subp,species_symbol,measdate,age_calc FROM new.tree_trajectories
                WHERE intro_type='real' AND dia >= 5)
    SELECT count(*) FROM newsimb.curated_cohorts_landis_simB c ANTI JOIN tr USING (statecd,unitcd,countycd,plot,subp,species_symbol,measdate,age_calc)
    WHERE {scope('c', 'statecd', 'countycd')}""").fetchone()[0]
    check("(l) M1: every Sim B cohort row sits on the trajectory time axis (same measdate, age_calc)", n_b == 0, f"({n_b})")
for t, db in (("curated_cohorts_landis", "new"), ("tree_trajectories", "new")) + ((("curated_cohorts_landis_simB", "newsimb"),) if a.simb_db else ()):
    n = con.execute(f"""SELECT count(*) FROM (SELECT statecd,unitcd,countycd,plot,subp,sim_year FROM {db}.{t}
                       WHERE {scope('', 'statecd', 'countycd')} GROUP BY ALL HAVING count(DISTINCT measdate) > 1)""").fetchone()[0]
    check(f"(l) M2: {t}: sim_year strictly increasing per subplot (no shared sim_year)", n == 0, f"({n})")
show("(l) M3: tree-number reuse split (trees / records on a split key)", f"""
SELECT STATECD, count(DISTINCT (STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE)) trees, count(*) records,
       count(DISTINCT (STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE)) FILTER (WHERE TREE >= 100000) new_tree_ids
FROM new.curated_trees WHERE tree_key_split AND {scope()} GROUP BY 1""")
for t, db in (("curated_cohorts_landis_stdorg", "new"),) + ((("curated_cohorts_landis_simB", "newsimb"),) if a.simb_db else ()):
    cols = dbcols = {r[0] for r in con.execute(f"SELECT column_name FROM duckdb_columns() WHERE database_name='{db}' AND table_name='{t}'").fetchall()}
    need = {"n_trees_natural_treeage_scale", "share_natural_treeage_scale", "age_source_major",
            "n_trees_known_bias", "share_known_bias", "procedural_break"}
    check(f"(l) I4: {t} carries the age metadata columns", need <= cols, f"(missing {sorted(need - cols)})")
    r = con.execute(f"""SELECT count(*), count(*) FILTER (WHERE age_source_major IS NULL),
                        round(sum(agb*share_natural_treeage_scale)/sum(agb),3),
                        round(sum(agb*share_known_bias)/sum(agb),3)
                        FROM {db}.{t} WHERE {scope('', 'statecd', 'countycd')}""").fetchone()
    info(f"(l) I4: {t}", f"(rows {r[0]:,}; age_source_major null {r[1]:,}; AGB share natural_treeage_scale {r[2]}; "
                         f"AGB share known_bias {r[3]})")
n_ph_null = con.execute(f"""SELECT count(*) FROM new.curated_cohorts_landis WHERE intro_type='phantom_backfill'
                           AND age_source_major IS NULL AND {scope('', 'statecd', 'countycd')}""").fetchone()[0]
check("(l) I4: phantom rows inherit age metadata from their source tree", n_ph_null == 0, f"({n_ph_null} null)")

# ---- (n) r5 audit-batch checks ----------------------------------------------------------------
TRK = "statecd,unitcd,countycd,plot,subp"
# n1 (M-1): ONE sim_year axis per subplot, shared by Sim A, Sim B and the trajectories, 0 at the
# subplot's first scaffold visit (clean_subplot_years)
r = con.execute(f"""
WITH a AS (SELECT DISTINCT {TRK},measdate,sim_year FROM new.curated_cohorts_landis WHERE {scope('', 'statecd', 'countycd')}),
t AS (SELECT DISTINCT {TRK},measdate,sim_year FROM new.tree_trajectories WHERE {scope('', 'statecd', 'countycd')}),
f0 AS (SELECT STATECD statecd, UNITCD unitcd, COUNTYCD countycd, PLOT plot, SUBP subp, min(MEASDATE) md0
       FROM new.clean_subplot_years GROUP BY ALL)
SELECT (SELECT count(*) FROM a JOIN t USING ({TRK},measdate) WHERE a.sim_year <> t.sim_year),
       (SELECT count(*) FROM a JOIN t USING ({TRK},measdate)),
       (SELECT count(*) FROM (SELECT * FROM a UNION ALL SELECT * FROM t) x JOIN f0 USING ({TRK})
         WHERE x.measdate = f0.md0 AND x.sim_year <> 0),
       (SELECT min(sim_year) FROM (SELECT sim_year FROM a UNION ALL SELECT sim_year FROM t))""").fetchone()
check("(n1) M-1: Sim A and trajectories share sim_year on every shared subplot-visit", r[0] == 0, f"({r[0]} of {r[1]:,} differ)")
check("(n1) M-1: sim_year = 0 at the subplot's first scaffold visit, never negative", r[2] == 0 and (r[3] or 0) >= 0,
      f"({r[2]} rows at the first visit with sim_year <> 0; min sim_year {r[3]})")
if a.simb_db:
    r = con.execute(f"""
    WITH a AS (SELECT DISTINCT {TRK},measdate,sim_year FROM new.curated_cohorts_landis WHERE {scope('', 'statecd', 'countycd')}),
    b AS (SELECT DISTINCT {TRK},measdate,sim_year FROM newsimb.curated_cohorts_landis_simB WHERE {scope('', 'statecd', 'countycd')}),
    t AS (SELECT DISTINCT {TRK},measdate,sim_year FROM new.tree_trajectories WHERE {scope('', 'statecd', 'countycd')})
    SELECT (SELECT count(*) FROM a JOIN b USING ({TRK},measdate) WHERE a.sim_year <> b.sim_year),
           (SELECT count(*) FROM a JOIN b USING ({TRK},measdate)),
           (SELECT count(*) FROM b JOIN t USING ({TRK},measdate) WHERE b.sim_year <> t.sim_year)""").fetchone()
    check("(n1) M-1: Sim B shares sim_year with Sim A and the trajectories on every shared subplot-visit",
          r[0] == 0 and r[2] == 0, f"(vs Sim A {r[0]} of {r[1]:,}; vs trajectories {r[2]})")
    # n2 (I-1): every Sim B drop > 0 is backed by a REAL >= 5 in member of that cohort (same subplot,
    # species, birth sim_year) at the previous visit that was removed / died by the visit, or is
    # alive there with DAMSEV > 40 (the drop model's damaged-alive branch); never on a cohort's
    # first Sim B row
    con.execute(f"""
    CREATE TEMP TABLE simb_drop AS
    WITH d AS (SELECT {TRK}, species_symbol, measdate, sim_year, sim_year - age_calc AS b
               FROM newsimb.curated_cohorts_landis_simB WHERE disturbance_drop_pct > 0 AND {scope('', 'statecd', 'countycd')}),
    vis AS (SELECT DISTINCT {TRK}, measdate FROM new.tree_trajectories),
    pv AS (SELECT {TRK}, measdate, lag(measdate) OVER (PARTITION BY {TRK} ORDER BY measdate) pmd FROM vis),
    mem AS (SELECT {TRK}, tree, species_symbol, measdate, sim_year - age_calc AS b FROM new.tree_trajectories
            WHERE intro_type = 'real' AND dia >= 5),
    lv AS (SELECT tt.statecd, tt.unitcd, tt.countycd, tt.plot, tt.subp, tt.tree, tt.measdate FROM new.tree_trajectories tt
           LEFT JOIN new.curated_trees ct ON ct.STATECD=tt.statecd AND ct.UNITCD=tt.unitcd AND ct.COUNTYCD=tt.countycd
            AND ct.PLOT=tt.plot AND ct.SUBP=tt.subp AND ct.TREE=tt.tree AND ct.MEASDATE=tt.measdate
           WHERE tt.intro_type = 'gap_interpolated' OR (tt.intro_type = 'real' AND ct.STATUSCD = 1)),   -- alive slots only
    dmg AS (SELECT STATECD statecd, UNITCD unitcd, COUNTYCD countycd, PLOT plot, SUBP subp, TREE tree, MEASDATE measdate
            FROM new.curated_trees WHERE STATUSCD = 1 AND (DAMSEV1 > 40 OR DAMSEV2 > 40))
    SELECT d.*,
      EXISTS (SELECT 1 FROM mem m WHERE m.statecd=d.statecd AND m.unitcd=d.unitcd AND m.countycd=d.countycd AND m.plot=d.plot
              AND m.subp=d.subp AND m.species_symbol=d.species_symbol AND m.b=d.b AND m.measdate=p.pmd
              AND NOT EXISTS (SELECT 1 FROM lv WHERE lv.statecd=m.statecd AND lv.unitcd=m.unitcd AND lv.countycd=m.countycd
                              AND lv.plot=m.plot AND lv.subp=m.subp AND lv.tree=m.tree AND lv.measdate=d.measdate)) AS has_removed,
      EXISTS (SELECT 1 FROM mem m JOIN dmg ON dmg.statecd=m.statecd AND dmg.unitcd=m.unitcd AND dmg.countycd=m.countycd
              AND dmg.plot=m.plot AND dmg.subp=m.subp AND dmg.tree=m.tree AND dmg.measdate=d.measdate
              WHERE m.statecd=d.statecd AND m.unitcd=d.unitcd AND m.countycd=d.countycd AND m.plot=d.plot
              AND m.subp=d.subp AND m.species_symbol=d.species_symbol AND m.b=d.b AND m.measdate=p.pmd) AS has_damaged,
      NOT EXISTS (SELECT 1 FROM newsimb.curated_cohorts_landis_simB c WHERE c.statecd=d.statecd AND c.unitcd=d.unitcd
              AND c.countycd=d.countycd AND c.plot=d.plot AND c.subp=d.subp AND c.species_symbol=d.species_symbol
              AND c.sim_year - c.age_calc = d.b AND c.measdate < d.measdate) AS is_first
    FROM d LEFT JOIN pv p USING ({TRK}, measdate)""")
    r = con.execute("""SELECT count(*), count(*) FILTER (WHERE NOT has_removed AND NOT has_damaged),
                              count(*) FILTER (WHERE NOT has_removed AND has_damaged), count(*) FILTER (WHERE is_first)
                       FROM simb_drop""").fetchone()
    check("(n2) I-1: every Sim B drop > 0 has a real >= 5 in member removed / died (or severely damaged) since the previous visit",
          r[1] == 0, f"({r[1]} of {r[0]:,} drops unbacked; {r[2]} backed only by damaged-alive members)")
    check("(n2) I-1: no Sim B drop on a cohort's first Sim B row", r[3] == 0, f"({r[3]})")
# n3 (M-4): no gap-interpolated live slot at, or between bracketing real records across, an
# entirely nonforest subplot visit (raw forest proportions: rv)
r = con.execute(f"""
WITH g AS (SELECT {TRK}, tree, measdate FROM new.tree_trajectories WHERE intro_type='gap_interpolated' AND {scope('', 'statecd', 'countycd')}),
r AS (SELECT {TRK}, tree, measdate FROM new.tree_trajectories WHERE intro_type='real'),
br AS (SELECT g.*, (SELECT max(r.measdate) FROM r WHERE r.statecd=g.statecd AND r.unitcd=g.unitcd AND r.countycd=g.countycd
                     AND r.plot=g.plot AND r.subp=g.subp AND r.tree=g.tree AND r.measdate < g.measdate) p,
                   (SELECT min(r.measdate) FROM r WHERE r.statecd=g.statecd AND r.unitcd=g.unitcd AND r.countycd=g.countycd
                     AND r.plot=g.plot AND r.subp=g.subp AND r.tree=g.tree AND r.measdate > g.measdate) n FROM g)
SELECT count(*), count(*) FILTER (WHERE EXISTS (SELECT 1 FROM rv WHERE rv.STATECD=br.statecd AND rv.UNITCD=br.unitcd
       AND rv.COUNTYCD=br.countycd AND rv.PLOT=br.plot AND rv.SUBP=br.subp AND rv.md > br.p AND rv.md < br.n
       AND rv.fsub + rv.fmic <= 0)) FROM br""").fetchone()
check("(n3) M-4: no gap-interpolated slot at / across an entirely nonforest subplot visit", r[1] == 0,
      f"({r[1]} of {r[0]:,} gap slots)")
# n4 (M-8): phantoms never exceed the debut (first real) value they are scaled from. Sim A rows
# carry no tree id and several trees' phantoms can share one cohort key, so the Sim A test is per
# cohort key: sum of tree-pass phantom AGB <= sum of the source trees' debut AGB (keys that the
# cohort pass also touched are left out); the trajectory test below is per tree
r = con.execute(f"""
WITH deb AS (SELECT STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE,MEASDATE, DRYBIO_AG*TPA_UNADJ*{453.592/4046.86} agb
             FROM new.curated_trees WHERE STATUSCD=1),
src AS (SELECT s.STATECD,s.UNITCD,s.COUNTYCD,s.PLOT,s.SUBP,s.prior_measdate,s.species_symbol,s.prior_age, sum(d.agb) dsum
        FROM new.phantom_tree_sources s JOIN deb d ON d.STATECD=s.STATECD AND d.UNITCD=s.UNITCD AND d.COUNTYCD=s.COUNTYCD
         AND d.PLOT=s.PLOT AND d.SUBP=s.SUBP AND d.TREE=s.TREE AND d.MEASDATE=s.debut_measdate GROUP BY ALL),
ph AS (SELECT statecd,unitcd,countycd,plot,subp,measdate,species_symbol,age_calc, sum(agb) psum FROM new.curated_cohorts_landis
       WHERE intro_type='phantom_backfill' AND {scope('', 'statecd', 'countycd')} GROUP BY ALL),
cp AS (SELECT DISTINCT STATECD,UNITCD,COUNTYCD,PLOT,SUBP,prior_measdate,species_symbol,prior_age FROM new.phantom_cohort_sources)
SELECT count(*), count(*) FILTER (WHERE ph.psum > src.dsum * (1 + 1e-6) + 1e-6)
FROM ph JOIN src ON src.STATECD=ph.statecd AND src.UNITCD=ph.unitcd AND src.COUNTYCD=ph.countycd AND src.PLOT=ph.plot
 AND src.SUBP=ph.subp AND src.prior_measdate=ph.measdate AND src.species_symbol=ph.species_symbol AND src.prior_age=ph.age_calc
WHERE NOT EXISTS (SELECT 1 FROM cp WHERE cp.STATECD=ph.statecd AND cp.UNITCD=ph.unitcd AND cp.COUNTYCD=ph.countycd
                  AND cp.PLOT=ph.plot AND cp.SUBP=ph.subp AND cp.prior_measdate=ph.measdate
                  AND cp.species_symbol=ph.species_symbol AND cp.prior_age=ph.age_calc)""").fetchone()
check("(n4) M-8: Sim A tree-pass phantom AGB <= the source trees' debut AGB (per cohort key)", r[1] == 0, f"({r[1]} of {r[0]:,} keys)")
r = con.execute(f"""
WITH fr AS (SELECT {TRK}, tree, arg_min(drybio_g, measdate) d0 FROM new.tree_trajectories WHERE intro_type='real' GROUP BY ALL)
SELECT count(*), count(*) FILTER (WHERE t.drybio_g > fr.d0 * (1 + 1e-6) + 1e-6)
FROM new.tree_trajectories t JOIN fr USING ({TRK}, tree) WHERE t.intro_type='phantom_backfill' AND {scope('t', 'statecd', 'countycd')}""").fetchone()
check("(n4) M-8: trajectory phantom biomass <= the tree's first real biomass", r[1] == 0, f"({r[1]} of {r[0]:,})")
show("(n4) phantom AGB share (Sim A curated_cohorts_landis, trajectories)", f"""
SELECT 'curated_cohorts_landis' t, round(sum(agb) FILTER (WHERE intro_type='phantom_backfill') / sum(agb), 4) phantom_agb_share,
       count(*) FILTER (WHERE intro_type='phantom_backfill') phantom_rows
FROM new.curated_cohorts_landis WHERE {scope('', 'statecd', 'countycd')}
UNION ALL SELECT 'tree_trajectories', round(sum(drybio_g*tpa) FILTER (WHERE intro_type='phantom_backfill') / sum(drybio_g*tpa), 4),
       count(*) FILTER (WHERE intro_type='phantom_backfill') FROM new.tree_trajectories WHERE {scope('', 'statecd', 'countycd')}""")
# n5 (M-5): ONE age scale per tree — age_stratum re-derived (STDORGCD of the condition at the tree's
# first live record; a tree without a live record: its first record)
r = con.execute(f"""
WITH o AS (SELECT STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE, CASE WHEN cond_stdorgcd = 1 THEN 'planted' ELSE 'natural' END s
           FROM new.curated_trees WHERE {scope()}
           QUALIFY row_number() OVER (PARTITION BY STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE ORDER BY (STATUSCD <> 1), MEASDATE) = 1),
n AS (SELECT STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE, count(DISTINCT COALESCE(cond_stdorgcd, -1)) FILTER (WHERE STATUSCD = 1) n_codes
      FROM new.curated_trees WHERE {scope()} GROUP BY ALL),
f AS (SELECT * FROM o JOIN n USING (STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE))
SELECT count(*) FILTER (WHERE t.age_stratum IS DISTINCT FROM f.s), count(DISTINCT (f.STATECD,f.UNITCD,f.COUNTYCD,f.PLOT,f.SUBP,f.TREE)) FILTER (WHERE f.n_codes > 1)
FROM new.curated_trees t JOIN f USING (STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE)""").fetchone()
check("(n5) M-5: age_stratum = STDORGCD stratum at the tree's first live record, on every record", r[0] == 0,
      f"({r[0]} records differ; {r[1]:,} trees whose live records span two STDORGCD codes)")
r = con.execute(f"""SELECT count(*) FROM (SELECT STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE FROM new.curated_trees
                    WHERE estage_mapped IS NOT NULL AND {scope()} GROUP BY ALL
                    HAVING count(DISTINCT (estage_mapped = estage_raw)) > 1 AND any_value(age_stratum) = 'natural')""").fetchone()[0]
check("(n5) M-5: natural-stratum trees carry raw TreeAge on every estage candidate (no mixed scale)", r == 0, f"({r} trees)")
# n6 (M-2): curated keys are the CANONICAL plot keys (re-derived: f.* views), plot_raw = as recorded
r = con.execute(f"""
SELECT count(*) FILTER (WHERE (ct.UNITCD,ct.COUNTYCD,ct.PLOT) IS DISTINCT FROM (ft.UNITCD,ft.COUNTYCD,ft.PLOT)),
       count(*) FILTER (WHERE (ct.unitcd_raw,ct.countycd_raw,ct.plot_raw) IS DISTINCT FROM (rt.UNITCD,rt.COUNTYCD,rt.PLOT)),
       count(*) FILTER (WHERE (ct.PLOT, ct.COUNTYCD) <> (ct.plot_raw, ct.countycd_raw)),
       count(DISTINCT (ct.STATECD,ct.UNITCD,ct.COUNTYCD,ct.PLOT)) FILTER (WHERE (ct.PLOT, ct.COUNTYCD, ct.UNITCD) <> (ct.plot_raw, ct.countycd_raw, ct.unitcd_raw))
FROM new.curated_trees ct JOIN f.TREE ft ON CAST(ft.CN AS VARCHAR) = CAST(ct.CN AS VARCHAR)
JOIN fraw.main.TREE rt ON rt.CN = ft.CN AND rt.STATECD = ft.STATECD WHERE {scope('ct')} AND {scope('ft')}""").fetchone()
check("(n6) M-2: curated plot keys = canonical keys (PREV_PLT_CN chain), *_raw = as recorded", r[0] == 0 and r[1] == 0,
      f"({r[0]} key / {r[1]} raw-key mismatches; {r[2]:,} records on {r[3]} renumbered plots remapped)")
# n7 (suspected, not confirmed in r5): tree-number splits through a dropped STATUSCD 0 record
# without evidence of a different tree (same species, no DIA drop, no dead/cut record before)
r = con.execute(f"""
WITH s AS (SELECT STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE, CN, prev_tre_cn, SPCD, DIA, STATUSCD,
             lag(STATUSCD) OVER w pst, lag(SPCD) OVER w psp, lag(DIA) OVER w pdia, TREE // 100000 sg
           FROM new.curated_trees WHERE tree_key_split AND {scope()}
           WINDOW w AS (PARTITION BY STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE % 100000 ORDER BY MEASDATE)),
f1 AS (SELECT * FROM s WHERE sg > 0 QUALIFY row_number() OVER (PARTITION BY STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE ORDER BY CN) = 1)
SELECT count(*), count(*) FILTER (WHERE T.STATUSCD = 0), count(*) FILTER (WHERE T.STATUSCD = 0 AND COALESCE(f1.pst,1) NOT IN (2,3)
       AND f1.SPCD = f1.psp AND NOT (f1.DIA < f1.pdia - 1))
FROM f1 LEFT JOIN fraw.main.TREE T ON CAST(T.CN AS VARCHAR) = f1.prev_tre_cn""").fetchone()
info("(n7) tree-number splits: segments / linked through a STATUSCD 0 record / of those WITHOUT evidence of a different tree",
     f"({r[0]} / {r[1]} / {r[2]})")

# ---- (m) I2 reports: STDAGE field-truth classes ----------------------------------------------------
show("(m) calibration-set composition (live >= 5 in, Tree_Age > 1, field age) by class", f"""
SELECT STATECD, cls, n, round(100.0*n/sum(n) OVER (PARTITION BY STATECD),1) pct FROM (
  SELECT STATECD, COALESCE(CASE WHEN age_from_totage IS NOT NULL THEN 'TOTAGE' END, stdage_context) cls, count(*) n
  FROM new.curated_trees WHERE STATUSCD=1 AND DIA>=5 AND estage_raw>1 AND COALESCE(age_from_totage, age_from_stdage) IS NOT NULL
    AND {scope()} GROUP BY ALL) ORDER BY 1, 3 DESC""")
show("(m) within-condition-visit SD of field age vs raw TreeAge vs DIA, by class (a12/a23; cond-visits with >= 5 records)", f"""
SELECT cls, count(*) cond_visits, round(median(sd_field),2) sd_field, round(median(sd_raw),1) sd_treeage, round(median(sd_dia),2) sd_dia FROM (
  SELECT stdage_context cls, STATECD,UNITCD,COUNTYCD,PLOT,CONDID,MEASDATE, stddev(age_from_stdage) sd_field,
         stddev(estage_raw) sd_raw, stddev(DIA) sd_dia
  FROM new.curated_trees WHERE STATUSCD=1 AND age_from_stdage IS NOT NULL AND DIA>=5 AND estage_raw>1 AND {scope()}
  GROUP BY ALL HAVING count(*) >= 5) GROUP BY 1 ORDER BY 1""")

# ---- (e) determinism ---------------------------------------------------------------------------
if a.determinism_db:
    con.execute(f"ATTACH '{a.determinism_db}' AS rep (READ_ONLY)")
    for t in ("curated_cohorts_landis", "curated_cohorts_landis_stdorg", "tree_trajectories"):
        x = con.execute(f"SELECT count(*) FROM (SELECT * FROM new.{t} EXCEPT ALL SELECT * FROM rep.{t})").fetchone()[0]
        y = con.execute(f"SELECT count(*) FROM (SELECT * FROM rep.{t} EXCEPT ALL SELECT * FROM new.{t})").fetchone()[0]
        check(f"(e) {t} identical across two runs (EXCEPT ALL both ways)", x == 0 and y == 0, f"({x}, {y})")
if a.determinism_simb_db and a.simb_db:   # r5 M-3: steps 9-10 (Sim B + its disturbance drops)
    con.execute(f"ATTACH '{a.determinism_simb_db}' AS repsimb (READ_ONLY)")
    for t in ("curated_cohorts_landis_simB", "cohort_disturbance"):
        x = con.execute(f"SELECT count(*) FROM (SELECT * FROM newsimb.{t} EXCEPT ALL SELECT * FROM repsimb.{t})").fetchone()[0]
        y = con.execute(f"SELECT count(*) FROM (SELECT * FROM repsimb.{t} EXCEPT ALL SELECT * FROM newsimb.{t})").fetchone()[0]
        check(f"(e) Sim B {t} identical across two runs (EXCEPT ALL both ways)", x == 0 and y == 0, f"({x}, {y})")

# ---- 2. record counts ---------------------------------------------------------------------
rows = []
for tab in ("base_subplot_cond", "clean_subplot_years", "longitudinal_subplots", "excluded_tree_ids",
            "curated_trees", "curated_snapshots", "curated_cohorts", "curated_cohorts_landis",
            "curated_cohorts_landis_stdorg", "tree_trajectories"):
    sc, cc = ("statecd", "countycd")
    try:
        o = con.execute(f"SELECT count(*) FROM f.main.{tab} WHERE {scope('', sc, cc)}").fetchone()[0]
    except duckdb.Error:
        o = None
    try:
        n = con.execute(f"SELECT count(*) FROM new.main.{tab} WHERE {scope('', sc, cc)}").fetchone()[0]
    except duckdb.Error:
        n = None
    rows.append((tab, o, n))
if a.simb_db:
    for tab in ("curated_cohorts_landis_simB", "cohort_disturbance"):
        o = None
        oldsimb = os.path.join(os.path.dirname(a.fiadb), "cohorts_simB.duckdb")
        if os.path.exists(oldsimb):
            try:
                con.execute(f"ATTACH IF NOT EXISTS '{oldsimb}' AS oldsimb (READ_ONLY)")
                o = con.execute(f"SELECT count(*) FROM oldsimb.main.{tab} WHERE {scope('', 'statecd', 'countycd')}").fetchone()[0]
            except duckdb.Error:
                o = None
        n = con.execute(f"SELECT count(*) FROM newsimb.main.{tab} WHERE {scope('', 'statecd', 'countycd')}").fetchone()[0]
        rows.append((tab + " (simB)", o, n))
print("\n=== 2. record counts in scope (old FIADB curated / new curated)")
for t, o, n in rows:
    d = "" if o in (None, 0) or n is None else f"  ({100 * (n - o) / o:+.1f}%)"
    print(f"  {t:36s} old={o if o is not None else '-':>12}  new={n if n is not None else '-':>12}{d}")

# ---- 3. duplicates ------------------------------------------------------------------------
show("3. curated_trees CN uniqueness", f"""
SELECT 'old' AS db, count(*) n, count(DISTINCT CN) n_cn FROM f.main.curated_trees WHERE {scope()}
UNION ALL
SELECT 'new', count(*), count(DISTINCT CN) FROM new.curated_trees WHERE {scope()}""")
show("3b. new tree-visit key uniqueness (tree x MEASDATE)", f"""
SELECT count(*) n, count(DISTINCT (STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE,MEASDATE)) n_key
FROM new.curated_trees WHERE {scope()}""")

# ---- 4. >=5 in difference -----------------------------------------------------------------
show("4. live >=5in trees old vs new", f"""
WITH o AS (SELECT STATECD, count(*) n, sum(TPA_UNADJ) tpa FROM f.main.curated_trees
           WHERE STATUSCD=1 AND DIA>=5 AND {scope()} GROUP BY 1),
     n AS (SELECT STATECD, count(*) n, sum(TPA_UNADJ) tpa FROM new.curated_trees
           WHERE STATUSCD=1 AND DIA>=5 AND {scope()} GROUP BY 1)
SELECT STATECD, o.n old_n, n.n new_n, round(100.0*(n.n-o.n)/o.n,2) pct_n,
       round(100.0*(n.tpa-o.tpa)/o.tpa,2) pct_tpa
FROM o FULL JOIN n USING (STATECD) ORDER BY 1""")

# ---- 5. ages ------------------------------------------------------------------------------
q = "quantile_cont(estimated_age, [0.1,0.25,0.5,0.75,0.9])"
show("5. estimated_age of live records (p10,p25,p50,p75,p90), old vs new", f"""
SELECT 'old' db, CASE WHEN DIA<5 THEN 'sapling' ELSE 'ge5in' END size, count(*) n,
       round(avg(estimated_age),2) mean, {q} q FROM f.main.curated_trees
WHERE STATUSCD=1 AND {scope()} GROUP BY ALL
UNION ALL
SELECT 'new', CASE WHEN DIA<5 THEN 'sapling' ELSE 'ge5in' END, count(*), round(avg(estimated_age),2), {q}
FROM new.curated_trees WHERE STATUSCD=1 AND {scope()} GROUP BY ALL ORDER BY 2, 1""")
show("5b. same trees (CN in both): new minus old estimated_age", f"""
SELECT n.age_source, count(*) n, round(avg(n.estimated_age - o.estimated_age),2) mean_diff,
       round(median(n.estimated_age - o.estimated_age),2) med_diff
FROM new.curated_trees n JOIN f.main.curated_trees o USING (CN)
WHERE n.STATUSCD=1 AND {scope('n')} GROUP BY 1 ORDER BY 2 DESC""")
show("5c. age_source mix of live records", f"""
SELECT age_source, count(*) FILTER (WHERE db='old') old_n, count(*) FILTER (WHERE db='new') new_n FROM (
  SELECT 'old' db, age_source FROM f.main.curated_trees WHERE STATUSCD=1 AND {scope()}
  UNION ALL SELECT 'new', age_source FROM new.curated_trees WHERE STATUSCD=1 AND {scope()})
GROUP BY 1 ORDER BY 3 DESC""")
show("5d. new-rule flags", f"""
SELECT count(*) FILTER (WHERE not_ingrowth) reconcile_new_rows,
       count(DISTINCT (STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE)) FILTER (WHERE censored) censored_trees,
       count(*) FILTER (WHERE n_clean_visits=1) single_visit_rows,
       count(*) FILTER (WHERE subp_has_cond_status3) cs3_subplot_rows
FROM new.curated_trees WHERE {scope()}""")

show("5e. sapling ages: live records by DIA class — estimated_age quantiles, share <= 1.5 yr, "
     "age_source mix (new; old for comparison)", f"""
SELECT db, dcls, count(*) n, round(quantile_cont(estimated_age,0.1),1) p10, round(median(estimated_age),1) p50,
       round(quantile_cont(estimated_age,0.9),1) p90,
       round(avg((estimated_age <= 1.5)::INT),3) share_le_1_5yr,
       round(avg((age_source='estage')::INT),3) share_estage, round(avg((age_source='model')::INT),3) share_model
FROM (SELECT 'new' db, estimated_age, age_source, DIA FROM new.curated_trees WHERE STATUSCD=1 AND {scope()}
      UNION ALL SELECT 'old', estimated_age, age_source, DIA FROM f.main.curated_trees WHERE STATUSCD=1 AND {scope()})
CROSS JOIN LATERAL (SELECT CASE WHEN DIA<1 THEN 'a <1' WHEN DIA<3 THEN 'b 1-3' WHEN DIA<5 THEN 'c 3-5'
                                WHEN DIA<7 THEN 'd 5-7' WHEN DIA<10 THEN 'e 7-10' ELSE 'f 10+' END dcls)
GROUP BY ALL ORDER BY 2, 1""")
show("5f. procedural_break flag on cohort tables", """
SELECT 'stdorg' t, count(*) FILTER (WHERE procedural_break) n_break, count(*) n FROM new.curated_cohorts_landis_stdorg""" +
     (""" UNION ALL SELECT 'simB', count(*) FILTER (WHERE procedural_break), count(*) FROM newsimb.curated_cohorts_landis_simB""" if a.simb_db else ""))

# ---- 6. slim schema -----------------------------------------------------------------------
if a.old_slim and a.new_slim:
    def schema(alias):
        return {t: [(c, ty) for c, ty in con.execute(
            "SELECT column_name, data_type FROM duckdb_columns() WHERE database_name=? AND table_name=? "
            "ORDER BY column_index", [alias, t]).fetchall()]
            for (t,) in con.execute("SELECT table_name FROM duckdb_tables() WHERE database_name=?",
                                    [alias]).fetchall()}
    so, sn = schema("oslim"), schema("nslim")
    print("\n=== 6. slim schema old vs new")
    print(f"  tables old={sorted(so)}\n         new={sorted(sn)}")
    for t in sorted(set(so) | set(sn)):
        o, n = so.get(t, []), sn.get(t, [])
        if o == n:
            print(f"  {t:32s} IDENTICAL ({len(o)} cols)")
        elif n[:len(o)] == o:
            print(f"  {t:32s} old columns identical (name/type/order) + {len(n) - len(o)} appended: "
                  f"{[c for c, _ in n[len(o):]]}")
        else:
            diff = [(x, y) for x, y in zip(o, n) if x != y][:10]
            print(f"  {t:32s} DIFFERENT: old {len(o)} cols, new {len(n)}; first diffs {diff}; "
                  f"missing={[c for c in o if c not in n]}")
        # r5 M-7: ENFORCED — every old table survives with its columns as an exact prefix
        if t in so:
            check(f"6. slim {t}: old columns unchanged (name/type/order), appended columns only",
                  t in sn and n[:len(o)] == o, f"(old {len(o)}, new {len(n)})")
    for t in sorted(sn):
        oc = con.execute(f"SELECT count(*) FROM oslim.main.{t} WHERE "
                         + (scope('', 'statecd', 'countycd') if any(c.lower() == 'countycd' for c, _ in sn[t]) else "TRUE")).fetchone()[0] if t in so else None
        nc = con.execute(f"SELECT count(*) FROM nslim.main.{t}").fetchone()[0]
        print(f"  rows {t:32s} old(in scope)={oc}  new={nc}")
con.close()
print(f"\n=== KNOWN exceptions shown ({len(KNOWN)}): {KNOWN}")
print(f"\n=== SUMMARY: {len(FAILS)} failed check(s): {FAILS}")
