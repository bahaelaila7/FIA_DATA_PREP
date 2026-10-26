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
a = ap.parse_args()

con = duckdb.connect(":memory:")
con.execute(f"SET threads={os.environ.get('FIA_DUCKDB_THREADS', '3')}; "
            f"SET memory_limit='{os.environ.get('FIA_DUCKDB_MEMORY', '5GB')}'; SET enable_progress_bar=false")
con.execute(f"ATTACH '{a.fiadb}' AS f (READ_ONLY)")
con.execute(f"ATTACH '{a.curated_db}' AS new (READ_ONLY)")
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
       MIN(MAKE_DATE(P.MEASYEAR, COALESCE(CAST(P.MEASMON AS INTEGER),6), COALESCE(P.MEASDAY,15))) AS d7
FROM f.TREE T JOIN f.PLOT P ON P.CN = T.PLT_CN
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
  AND NOT (rc7.d7 IS NOT NULL AND MAKE_DATE(P.MEASYEAR, COALESCE(CAST(P.MEASMON AS INTEGER),6),
                                            COALESCE(P.MEASDAY,15)) < rc7.d7)
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
show("1e. dead/cut records lost: raw STATUSCD 2/3 (annual, SUBP<=4, not P2A, COND_STATUS 1/2) of "
     "trees present in new curated, missing from it (should be ~0)", f"""
WITH tr AS (SELECT DISTINCT STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE FROM new.curated_trees WHERE {scope()}),
raw AS (
  SELECT T.CN, T.STATUSCD, T.TPA_UNADJ IS NULL AS tpa_null
  FROM f.TREE T JOIN f.PLOT P ON P.CN=T.PLT_CN JOIN f.SURVEY S ON S.CN=P.SRV_CN AND S.ANN_INVENTORY='Y'
  JOIN f.COND C ON C.PLT_CN=T.PLT_CN AND C.CONDID=T.CONDID
  JOIN tr ON tr.STATECD=T.STATECD AND tr.UNITCD=T.UNITCD AND tr.COUNTYCD=T.COUNTYCD AND tr.PLOT=T.PLOT
         AND tr.SUBP=T.SUBP AND tr.TREE=T.TREE
  WHERE T.STATUSCD IN (2,3) AND T.SUBP<=4 AND COALESCE(T.P2A_GRM_FLG,'')<>'Y' AND C.COND_STATUS_CD IN (1,2)
    AND {scope('T')})
SELECT raw.STATUSCD, raw.tpa_null, count(*) raw_n, count(n.CN) in_curated
FROM raw LEFT JOIN new.curated_trees n ON n.CN = raw.CN GROUP BY ALL ORDER BY ALL""")
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
show("1g. phantom rows at a procedural re-tally (break) visit's PRIOR visit, by source tree entry "
     "class: double-count risk (sapling debuts should be 0)", """
WITH ph AS (SELECT * FROM new.tree_trajectories WHERE intro_type='phantom_backfill'),
deb AS (SELECT statecd,unitcd,countycd,plot,subp,tree, min(measdate) d FROM new.tree_trajectories
        WHERE intro_type='real' GROUP BY ALL),
ddia AS (SELECT t.statecd,t.unitcd,t.countycd,t.plot,t.subp,t.tree, t.dia FROM new.tree_trajectories t
         JOIN deb USING (statecd,unitcd,countycd,plot,subp,tree) WHERE t.measdate=deb.d AND t.intro_type='real')
SELECT COALESCE(CAST(ph.reconcile_new AS VARCHAR),'rc1/2/null') entry, ddia.dia < 5 AS sap_debut,
       count(*) phantom_rows,
       count(*) FILTER (WHERE EXISTS (SELECT 1 FROM new.procedural_breaks b WHERE b.STATECD=ph.statecd
            AND b.UNITCD=ph.unitcd AND b.COUNTYCD=ph.countycd AND b.PLOT=ph.plot
            AND b.MEASDATE > ph.measdate AND b.MEASDATE <= deb.d)) crossing_a_break
FROM ph JOIN deb USING (statecd,unitcd,countycd,plot,subp,tree) JOIN ddia USING (statecd,unitcd,countycd,plot,subp,tree)
GROUP BY ALL ORDER BY ALL""")

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
    for t in sorted(sn):
        oc = con.execute(f"SELECT count(*) FROM oslim.main.{t} WHERE "
                         + (scope('', 'statecd', 'countycd') if any(c.lower() == 'countycd' for c, _ in sn[t]) else "TRUE")).fetchone()[0] if t in so else None
        nc = con.execute(f"SELECT count(*) FROM nslim.main.{t}").fetchone()[0]
        print(f"  rows {t:32s} old(in scope)={oc}  new={nc}")
con.close()
