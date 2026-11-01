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

r7 (DECISIONS D36-D45): from consistency checker to independent RE-DERIVATION. The verifier
imports only duckdb / os / sys (the n9 fixtures in selftest_fixtures.py import pipeline code: the
documented exception) and re-derives from raw FIADB and the decision text: the canonical plot map
(PREV_PLT_CN chains only since r8 D48); tree identity (D36 split, n19); censoring
events and final / terminal fates (n20, c1, n21); the scaffold and the D18 time axis (n15, n16);
record values and units (n17); phantom completeness (n18); the D41 disturbance events,
subp_has_dstrb and every Sim B drop with its magnitude and the D42 full removals (n21, n22, n8);
product attributes (n23); D38 / D39 flags (n24); D43 reference rows (n25); slim content (n26);
determinism over every table, incl. steps 1-10 on a subset at two thread counts (e). Display-only
invariants are now checks; a check on an empty set where data is expected FAILs; skipped sections
print SKIP; the KNOWN drift guard is signed; below-floor (h) cells beyond their limit are
FAIL-SMALL. The process exits 1 on any FAIL (2 when the canonical map cannot be built).

  python verify_curation_v2.py --curated-db NEW.duckdb --fiadb FIADB.duckdb [--simb-db SIMB.duckdb] \
      [--old-slim OLD_SLIM --new-slim NEW_SLIM --slim-ecos L3,...] [--states 1,12] [--counties 1:89] \
      [--determinism-db REP --determinism-simb-db REP_SIMB] [--determinism-subset A,B[,SA,SB]]
"""
import argparse
import os
import sys

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
ap.add_argument("--slim-ecos", help="r7 E24: the slim builder's --ecos (EPA L3 list), for the slim content check")
ap.add_argument("--determinism-subset", help="r7 D40/D45: CUR_A,CUR_B[,SIMB_A,SIMB_B] — the same one-county subset built "
                "by steps 1-10 twice at two thread counts; every table present in both is compared")
a = ap.parse_args()
FAILS = []
SKIPS = []
N_PASS = [0]


def check(name, ok, detail=""):
    print(f"  [{'PASS' if ok else 'FAIL'}] {name} {detail}")
    if ok:
        N_PASS[0] += 1
    else:
        FAILS.append(name)


def check_n(name, ok, n, detail=""):
    """r7 E14: a check evaluated on an EMPTY set where data is expected FAILS (vacuous pass)."""
    if not n:
        check(f"{name} [EMPTY: no rows to test where data is expected]", False, detail)
    else:
        check(name, ok, detail)


def info(name, detail=""):
    print(f"  [INFO] {name} {detail}")


def skip(name, why):
    """r7 E14: a section that cannot run says so (it is counted in the summary)."""
    print(f"  [SKIP] {name} ({why})")
    SKIPS.append(name)


def fail_small(name, detail=""):
    """r7 D40: a below-floor cell that exceeds its limit (counts as a FAIL)."""
    print(f"  [FAIL-SMALL] {name} {detail}")
    FAILS.append(f"FAIL-SMALL {name}")


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
# r8 D47.5 / D47.6: re-measured 2026-10-28 on the D40 / D46 age model (whole-state r8 runs). The
# pooled FL 10-15 in entry is retired (-2.9, passes); the others are new, with the same signed
# +-KNOWN_DRIFT guard. (d) entries are keyed by stratum and species (or "empty"), so a NEW (d)
# failure still FAILS.
_KY_D46 = ("KY: D46 rejected every TreeAge mapping curve, so planted estage is raw TreeAge (+7.5 yr out of fold at "
           "9+ in, D46 residual); the 10-15 in gap flips sign with the fit scope (-4.9 single-state, +4.2 pooled): "
           "those ages are not stable to the training set (D47; deferred DIA-class model, D46 option d)")
_TN_RTM = ("TN: model vs mapped TreeAge regression to the mean at 15+ in planted (266 trees); only trees WITHOUT a "
           "TreeAge record take the model age (D47)")
_R4_RTM = ("model vs mapped TreeAge under the regional fit: regression to the mean toward the regional planted bulk (loblolly-"
           "dominated); only trees WITHOUT a TreeAge record take the model age (D60)")
KNOWN_EXCEPTIONS = {
    ("12,21,28", "j", 12, "planted", "15+"): dict(
        observed=-4.3, limit=3.0, flag="known_bias_fl_planted_model_15plus", reason=_FL_RTM),
    ("12,21,28", "j", 21, "planted", "10-15"): dict(
        observed=+4.2, limit=3.0, flag="known_bias_ky_planted_model_10to15", reason=_KY_D46),
    ("12", "j", 12, "planted", "15+"): dict(
        observed=-3.7, limit=3.0, flag="known_bias_fl_planted_model_15plus",
        reason=_FL_RTM.replace("under the pooled fit", "under the single-state fit")),
    ("21", "j", 21, "planted", "10-15"): dict(
        observed=-4.9, limit=3.0, flag="known_bias_ky_planted_model_10to15", reason=_KY_D46),
    ("47", "j", 47, "planted", "15+"): dict(
        observed=-3.1, limit=3.0, flag="known_bias_tn_planted_model_15plus", reason=_TN_RTM),
    # D60: the R4 regional fit (one DB per region, D59) re-measured; model ages only, 86 FL + 123 KY live records
    ("1,12,13,21,28,37,45,47", "j", 12, "planted", "10-15"): dict(
        observed=-3.6, limit=3.0, flag="known_bias_fl_planted_model_10plus", reason=_R4_RTM),
    ("1,12,13,21,28,37,45,47", "j", 12, "planted", "15+"): dict(
        observed=-4.9, limit=3.0, flag="known_bias_fl_planted_model_10plus", reason=_R4_RTM),
    ("1,12,13,21,28,37,45,47", "j", 21, "planted", "5-10"): dict(
        observed=-4.4, limit=3.0, flag="known_bias_ky_planted_model_5to10", reason=_R4_RTM),
    ("1,12,13,21,28,37,45,47", "h", 21, "planted"): dict(
        observed=5.6, limit=4.4, flag="none", reason="KY planted: 31 condition-visits, below the reliability floor (D60)"),
    # (d): value = number of species tested for "empty"; the largest median inversion (yr) for a species
    ("21", "d", "planted", "empty"): dict(
        observed=0.0, limit=0.0, flag="none",
        reason="KY: no planted top-25 species has >= 50 records in every DIA class (data outcome, D47.6)"),
    ("47", "d", "planted", 132): dict(
        observed=0.0, limit=0.0, flag="none",
        reason="TN: planted Virginia pine (132) 5-7 vs 7-9 in medians 22.799 vs 22.796 yr, a < 0.05-yr near-tie (data outcome, D47.6)"),
}
FIT_SCOPE = None   # set after the curated DB is attached
KNOWN = []


def check_known(key, name, ok, value, detail=""):
    """r7 D40 (M-3 / E17): the drift guard compares SIGNED values — an excepted cell prints KNOWN
    while |value - recorded| <= KNOWN_DRIFT; a cell that moved further, or flipped sign past the
    band, FAILS as DRIFTED."""
    key = (FIT_SCOPE,) + tuple(key)
    exc = KNOWN_EXCEPTIONS.get(key)
    if ok or exc is None:
        check(name, ok, detail)
        if ok and exc is not None:
            info(f"{name}: known exception {key} now passes", "(consider retiring it)")
        return
    lo, hi = exc["observed"] - KNOWN_DRIFT, exc["observed"] + KNOWN_DRIFT
    if lo <= value <= hi:
        print(f"  [KNOWN] {name} {detail} — recorded {exc['observed']:+.1f} (limit {exc['limit']}, "
              f"FAILS outside [{lo:+.1f}, {hi:+.1f}]); {exc['reason']}; flag age_domain={exc['flag']}")
        KNOWN.append(name)
    else:
        check(f"{name} [known exception DRIFTED: {value:+.1f} outside {exc['observed']:+.1f} +- {KNOWN_DRIFT}]", False, detail)


def below_floor(key, name, ok, value, detail=""):
    """r7 D40: a cell below its reliability floor. Within its limit: INFO. Beyond it: FAIL-SMALL
    (a FAIL) unless an explicit KNOWN entry (with a reason) covers it."""
    k = (FIT_SCOPE,) + tuple(key)
    if ok:
        info(name, detail + " — below the reliability floor; holds")
        if k in KNOWN_EXCEPTIONS:
            info(f"{name}: known exception {k} is attached to a below-floor cell", "(now passes; consider retiring it)")
    elif k in KNOWN_EXCEPTIONS:
        check_known(key, name, ok, value, detail + " — below the reliability floor")
    else:
        fail_small(name, detail + " — below the reliability floor and beyond its limit (no KNOWN entry)")

con = duckdb.connect(":memory:")
con.execute(f"SET threads={os.environ.get('FIA_DUCKDB_THREADS', '3')}; "
            f"SET memory_limit='{os.environ.get('FIA_DUCKDB_MEMORY', '5GB')}'; SET enable_progress_bar=false")
# r5 M-2: the raw reference uses CANONICAL plot keys (a few plots change UNITCD/COUNTYCD/PLOT
# between annual visits; canonical = the key at the PREV_PLT_CN chain's first ANNUAL visit).
# Re-derived independently of dbconn (recursive CTE): FIADB is attached as `fraw`, and an
# in-memory catalog `f` holds one view per FIADB table, remapped where the table is plot-keyed.
con.execute(f"ATTACH '{a.fiadb}' AS fraw (READ_ONLY)")
con.execute("ATTACH ':memory:' AS f")
# r8 D48: identity uses FIA's own links only: the canonical key follows the PREV_PLT_CN chain (the
# r6 D29 coordinate links and the r7 D37 duplicate-visit collapse are removed)
con.execute("""CREATE TABLE f.main._ap AS SELECT P.CN, P.PREV_PLT_CN, P.STATECD, P.UNITCD, P.COUNTYCD, P.PLOT, P.INVYR, P.LAT, P.LON,
    MAKE_DATE(CAST(P.MEASYEAR AS BIGINT), CAST(COALESCE(NULLIF(P.MEASMON,0),6) AS BIGINT), CAST(COALESCE(NULLIF(P.MEASDAY,0),15) AS BIGINT)) md
    FROM fraw.main.PLOT P JOIN fraw.main.SURVEY S ON S.CN = P.SRV_CN AND S.ANN_INVENTORY = 'Y'""")
def _canon_walk():
    con.execute("""CREATE OR REPLACE TABLE f.main._walk AS
    WITH RECURSIVE ev AS (SELECT T.PLT_CN c, count(*) n, count(*) FILTER (WHERE abs(T.PREVDIA - O.DIA) <= 0.05) ok
                          FROM fraw.main.TREE T JOIN fraw.main.TREE O ON O.CN = T.PREV_TRE_CN WHERE T.PREVDIA IS NOT NULL GROUP BY 1),
    ed AS (SELECT a.CN c, a.PREV_PLT_CN p FROM f.main._ap a JOIN f.main._ap b ON b.CN = a.PREV_PLT_CN LEFT JOIN ev ON ev.c = a.CN
           -- D59: a changed plot key is accepted only on tree evidence (here FIA's own tree links, independent of dbconn's natural-key pairs)
           WHERE (a.UNITCD, a.COUNTYCD, a.PLOT) IS NOT DISTINCT FROM (b.UNITCD, b.COUNTYCD, b.PLOT) OR COALESCE(ev.n >= 2 AND ev.ok >= 0.9 * ev.n, FALSE)),
    roots AS (SELECT CN FROM f.main._ap ANTI JOIN ed ON ed.c = CN),
    walk(CN, root, depth) AS (SELECT CN, CN, 0 FROM roots UNION ALL
                              SELECT ed.c, w.root, w.depth + 1 FROM walk w JOIN ed ON ed.p = w.CN WHERE w.depth < 200)
    SELECT CN, root FROM walk""")
    # r7 E27: cycle guard — every visit must have exactly one root (a cycle has no root; a
    # branching chain gives a visit two roots)
    bad = con.execute("""SELECT (SELECT count(*) FROM f.main._ap) - (SELECT count(DISTINCT CN) FROM f.main._walk),
                                (SELECT count(*) - count(DISTINCT CN) FROM f.main._walk)""").fetchone()
    if bad != (0, 0):
        print(f"FATAL: canonical walk is not a forest (visits without a root {bad[0]}, with > 1 root {bad[1]})")
        sys.exit(2)


_canon_walk()
# the canonical key must not collide on a plot-INVYR (two visits of one canonical plot in one INVYR)
_ncoll = con.execute("""SELECT count(*) FROM (SELECT a.STATECD, o.UNITCD, o.COUNTYCD, o.PLOT, a.INVYR FROM f.main._walk w
    JOIN f.main._ap a ON a.CN = w.CN JOIN f.main._ap o ON o.CN = w.root GROUP BY ALL HAVING count(*) > 1)""").fetchone()[0]
if _ncoll:
    print(f"FATAL: canonical plot keys collide on {_ncoll} plot-INVYRs")
    sys.exit(2)
print("canonical map: PREV_PLT_CN chains only (r8 D48: no coordinate links, no duplicate-visit collapse)")
con.execute("""CREATE TABLE f.main._canon AS
SELECT w.CN, r.UNITCD, r.COUNTYCD, r.PLOT, a.UNITCD AS unitcd_raw, a.COUNTYCD AS countycd_raw, a.PLOT AS plot_raw
FROM f.main._walk w JOIN f.main._ap a ON a.CN = w.CN JOIN f.main._ap r ON r.CN = w.root
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
        con.execute(f'CREATE VIEW f.main."{_t}" AS SELECT t.* FROM fraw.main."{_t}" t')
con.execute(f"ATTACH '{a.curated_db}' AS new (READ_ONLY)")
FIT_SCOPE = ",".join(str(int(r[0])) for r in con.execute(
    "SELECT DISTINCT STATECD FROM new.curated_trees ORDER BY 1").fetchall())
if con.execute("SELECT count(*) FROM duckdb_tables() WHERE database_name='new' AND table_name='run_scope'").fetchone()[0]:
    _cty = con.execute("SELECT counties FROM new.run_scope").fetchone()[0]
    if _cty:   # r6 S2
        FIT_SCOPE += f"|counties={_cty}"
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
# r6 (genveg T1m finding, D34): plots with live FIADB trees that are absent from curated_trees.
# The reference plot set is every plot with a live tree (STATUSCD 1, TPA > 0, SUBP <= 4, own
# COND_STATUS 1, annual, PLOT_STATUS < 3, SUBP_STATUS <> 3, not P2A); a plot is EXPLAINED when all
# its live trees precede a STATUSCD 0 + RECONCILECD 7 "erroneously tallied previously" record
# (the documented RC7 rule drops those records). Unexplained absences must be 0.
r = con.execute(f"""
WITH lt AS (SELECT T.CN, T.STATECD, T.UNITCD, T.COUNTYCD, T.PLOT,
                   (rc7.d7 IS NOT NULL AND MAKE_DATE(P.MEASYEAR, COALESCE(NULLIF(CAST(P.MEASMON AS INTEGER), 0), 6), COALESCE(NULLIF(CAST(P.MEASDAY AS INTEGER), 0), 15)) < rc7.d7) pre_rc7
            FROM f.TREE T JOIN f.PLOT P ON P.CN = T.PLT_CN JOIN f.SURVEY S ON S.CN = P.SRV_CN AND S.ANN_INVENTORY = 'Y'
            JOIN f.COND C ON C.PLT_CN = T.PLT_CN AND C.CONDID = T.CONDID
            JOIN f.SUBPLOT SU ON SU.PLT_CN = T.PLT_CN AND SU.SUBP = T.SUBP
            LEFT JOIN rc7 ON rc7.STATECD=T.STATECD AND rc7.UNITCD=T.UNITCD AND rc7.COUNTYCD=T.COUNTYCD
                 AND rc7.PLOT=T.PLOT AND rc7.SUBP=T.SUBP AND rc7.TREE=T.TREE
            WHERE T.STATUSCD = 1 AND T.TPA_UNADJ > 0 AND T.SUBP <= 4 AND C.COND_STATUS_CD = 1
              AND P.PLOT_STATUS_CD < 3 AND COALESCE(SU.SUBP_STATUS_CD, -1) <> 3 AND COALESCE(T.P2A_GRM_FLG, '') <> 'Y'
              AND {scope('T')}),
pl AS (SELECT STATECD, UNITCD, COUNTYCD, PLOT, bool_and(pre_rc7) all_rc7 FROM lt GROUP BY ALL),
cp AS (SELECT DISTINCT STATECD, UNITCD, COUNTYCD, PLOT FROM new.curated_trees WHERE {scope()})
SELECT pl.STATECD, count(*) n, count(*) FILTER (WHERE cp.PLOT IS NULL) absent,
       count(*) FILTER (WHERE cp.PLOT IS NULL AND all_rc7) absent_rc7, count(*) FILTER (WHERE cp.PLOT IS NULL AND NOT all_rc7) unexplained
FROM pl LEFT JOIN cp USING (STATECD, UNITCD, COUNTYCD, PLOT) GROUP BY 1 ORDER BY 1""").fetchall()
for st_, n_, ab_, rc_, un_ in r:
    check(f"(n13) state {st_}: every FIADB live-tree plot is in curated_trees, or absent only by the RC7 rule",
          un_ == 0, f"({n_:,} plots; absent {ab_} = {100*ab_/max(n_,1):.2f} %: {rc_} all-trees-erroneously-tallied (RC7), {un_} unexplained)")
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
# r7 E2 / E3: the D1 coverage invariants are ENFORCED (they were display-only)
r = con.execute("""SELECT (SELECT count(*) FROM ref), (SELECT count(*) FROM newct),
    (SELECT count(*) FROM newct n ANTI JOIN ref r ON r.CN = n.CN),
    (SELECT count(*) FROM ref r ANTI JOIN newct n ON n.CN = r.CN),
    (SELECT round(sum(TPA_UNADJ), 1) FROM ref r ANTI JOIN newct n ON n.CN = r.CN)""").fetchone()
check_n("1b. every curated live record is in the independent coverage reference", r[2] == 0, r[1],
        f"({r[2]:,} of {r[1]:,} curated live records outside it)")
check_n("1c. record-level live coverage: every reference live record is curated", r[3] == 0, r[0],
        f"({r[3]:,} of {r[0]:,} reference records missing, TPA {r[4] or 0})")
r = con.execute(f"SELECT count(*) FROM new.curated_trees WHERE SUBP > 4 AND {scope()}").fetchone()[0]
check("1d. no curated rows on SUBP > 4 (auxiliary points)", r == 0, f"({r})")
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
# r7 E3: ENFORCED — no dead/cut record of a reference tree is lost (records that precede an RC7
# "erroneously tallied" record of the same tree are dropped by the documented rule;
# r8 D48: no duplicate visits are dropped any more)
r = con.execute(f"""
WITH raw AS (
  SELECT T.CN, T.STATECD, T.UNITCD, T.COUNTYCD, T.PLOT, T.SUBP, T.TREE, P.CN pcn,
         MAKE_DATE(P.MEASYEAR, COALESCE(NULLIF(CAST(P.MEASMON AS INTEGER), 0), 6), COALESCE(NULLIF(CAST(P.MEASDAY AS INTEGER), 0), 15)) md
  FROM f.TREE T JOIN f.PLOT P ON P.CN=T.PLT_CN JOIN f.SURVEY S ON S.CN=P.SRV_CN AND S.ANN_INVENTORY='Y'
  JOIN f.COND C ON C.PLT_CN=T.PLT_CN AND C.CONDID=T.CONDID
  JOIN f.SUBPLOT SU ON SU.PLT_CN=T.PLT_CN AND SU.SUBP=T.SUBP
  JOIN ref_trees rt ON rt.STATECD=T.STATECD AND rt.UNITCD=T.UNITCD AND rt.COUNTYCD=T.COUNTYCD
       AND rt.PLOT=T.PLOT AND rt.SUBP=T.SUBP AND rt.TREE=T.TREE
  WHERE T.STATUSCD IN (2,3) AND T.SUBP<=4 AND COALESCE(T.P2A_GRM_FLG,'')<>'Y' AND C.COND_STATUS_CD IN (1,2,3,4)
    AND P.PLOT_STATUS_CD < 3 AND COALESCE(SU.SUBP_STATUS_CD, -1) <> 3)
SELECT count(*), count(*) FILTER (WHERE n.CN IS NULL)
FROM raw LEFT JOIN rc7 USING (STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE) LEFT JOIN new.curated_trees n ON n.CN = raw.CN
WHERE NOT (rc7.d7 IS NOT NULL AND raw.md < rc7.d7)""").fetchone()
check_n("1e. (f) no dead/cut record of a raw-reference tree is lost", r[1] == 0, r[0], f"({r[1]:,} of {r[0]:,} lost)")
r = con.execute("""SELECT count(*) FROM ref_trees rt ANTI JOIN (SELECT DISTINCT STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE
                   FROM new.curated_trees) n USING (STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE)""").fetchone()[0]
check("1e reference trees absent from curated", r == 0, f"({r})")

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
# r7 E4: nonforest phantoms are checked in the PRODUCT tables (raw forest proportions), not only
# in the provenance tables
for _t in ("curated_cohorts_landis", "curated_cohorts_landis_stdorg", "tree_trajectories"):
    r = con.execute(f"""SELECT count(*), count(*) FILTER (WHERE rv.md IS NULL OR rv.fsub + rv.fmic <= 0)
        FROM new.{_t} c LEFT JOIN rv ON rv.STATECD=c.statecd AND rv.UNITCD=c.unitcd AND rv.COUNTYCD=c.countycd
         AND rv.PLOT=c.plot AND rv.SUBP=c.subp AND rv.md=c.measdate
        WHERE c.intro_type='phantom_backfill' AND {scope('c', 'statecd', 'countycd')}""").fetchone()
    check_n(f"1f. {_t}: no phantom row on an entirely nonforest (or unsampled) raw subplot-visit", r[1] == 0, r[0],
            f"({r[1]:,} of {r[0]:,} phantom rows)")
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
SELECT T.STATECD,T.UNITCD,T.COUNTYCD,T.PLOT,T.SUBP,T.TREE, CAST(T.CN AS VARCHAR) pcn, v.MANUAL, v.nman, v.nmd, (T2.CN IS NULL) unrec
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
check("(b) raw re-tally candidate set = pipeline procedural_breaks (unrecorded part; r7 E13: enforced)",
      _ub[1] == 0 and _ub[2] == 0, f"(raw {_ub[0]:,}; pipeline-only {_ub[1]:,}; raw-only {_ub[2]:,})")
# member debut (first live record meeting the cohort-table filter)
con.execute(f"""
CREATE TEMP TABLE deb AS
SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE,
       min(MEASDATE) AS D, arg_min_null(DIA, MEASDATE) AS ddia,   -- r7 E19: arg_min skips NULL values
       arg_min_null(CAST(RECONCILECD AS INT), MEASDATE) AS drc, arg_min_null(reconcile_new, MEASDATE) AS drn,
       any_value(spcd_resolved) AS dsp
FROM new.curated_trees
WHERE STATUSCD=1 AND estimated_age IS NOT NULL AND DRYBIO_AG IS NOT NULL AND TPA_UNADJ IS NOT NULL
  AND ROUND(estimated_age) >= 1 AND {scope()} GROUP BY ALL""")
# r7 rule (z) (zombie trees, D26-relaxed): no phantom at or before a dead/cut record that precedes
# the tree's live debut; misrecorded deaths do not count
con.execute(f"""CREATE TEMP TABLE zdead AS SELECT STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE, MEASDATE zmd FROM new.curated_trees
    WHERE STATUSCD IN (2,3) AND NOT COALESCE(misrecorded_death, FALSE) AND {scope()}""")


def block_reasons(src_sql, rule_c=True):
    """Annotate (tree, p[, t]) rows with independently derived block reasons."""
    return f"""
    WITH src AS ({src_sql}),
    j AS (SELECT s.*, d.D, d.ddia, d.drc, d.drn, d.dsp FROM src s LEFT JOIN deb d USING (STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE))
    -- r7 E5: a source tree without a qualifying debut record is a blocked source (no_debut), not a dropped row
    SELECT j.*, (j.D IS NULL) AS no_debut,
      (j.p < j.D) AS pre_debut,
      EXISTS (SELECT 1 FROM zdead z WHERE z.STATECD=j.STATECD AND z.UNITCD=j.UNITCD AND z.COUNTYCD=j.COUNTYCD AND z.PLOT=j.PLOT
              AND z.SUBP=j.SUBP AND z.TREE=j.TREE AND z.zmd >= j.p AND z.zmd < j.D) AS r_z,
      -- r8 D47.3: NULL-safe (a NULL debut RECONCILECD must not blank the term)
      COALESCE(COALESCE(j.drn IN (6,9), FALSE)
       OR (j.drc = 3 AND j.ddia >= 5 AND EXISTS (SELECT 1 FROM rclo WHERE rclo.STATECD=j.STATECD AND rclo.UNITCD=j.UNITCD
            AND rclo.COUNTYCD=j.COUNTYCD AND rclo.PLOT=j.PLOT AND rclo.SUBP=j.SUBP AND rclo.md=j.D
            AND rclo.SPCD=j.dsp AND abs(rclo.PREVDIA-j.ddia)<=2)), FALSE) AS r_a,
      -- r7 D44 (C F4): a SAPLING phantom tests the MICROPLOT forest proportion
      EXISTS (SELECT 1 FROM rv WHERE rv.STATECD=j.STATECD AND rv.UNITCD=j.UNITCD AND rv.COUNTYCD=j.COUNTYCD
              AND rv.PLOT=j.PLOT AND rv.SUBP=j.SUBP AND rv.md >= j.p AND rv.md < j.D
              AND CASE WHEN j.ddia < 5 THEN rv.fmic <= 0 ELSE rv.fsub + rv.fmic <= 0 END) AS r_n,
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
                        count(*) FILTER (WHERE r_z) z, count(*) FILTER (WHERE no_debut) nd,
                        count(*) FILTER (WHERE r_a OR r_n OR r_b OR r_c OR r_z OR no_debut OR NOT pre_debut) any_block
                        FROM ({block_reasons(src_sql, rc)})""").fetchone()
    print(f"  {tab}: phantoms={r[0]:,} blocked: a={r[1]} n={r[2]} b={r[3]} c={r[4]} z={r[5]} no_debut={r[6]}  any(incl. not pre-debut)={r[7]}")
    check_n(f"1g {tab.split(':')[0]} no individually-blocked source", r[7] == 0, r[0], f"({r[7]})")
n_ph = [con.execute(f"SELECT count(*) FILTER (WHERE intro_type='phantom_backfill') FROM new.{t} WHERE {scope('', 'statecd', 'countycd')}").fetchone()[0]
        for t in ("curated_cohorts_landis", "curated_cohorts_landis_stdorg")]
n_src = con.execute("SELECT (SELECT count(*) FROM new.phantom_tree_sources) + (SELECT count(*) FROM new.phantom_cohort_sources)").fetchone()[0]
check("1g phantom rows: landis == stdorg == tree sources + cohort sources", n_ph[0] == n_ph[1] == n_src,
      f"(landis {n_ph[0]:,}, stdorg {n_ph[1]:,}, sources {n_src:,})")
# r7 E4: the PRODUCT phantom rows are the provenance rows, key by key (multiset, both directions),
# so a phantom edited / added / moved in the product alone is caught; phantom ages are >= 1
con.execute("""CREATE TEMP TABLE prov AS
    SELECT STATECD statecd, UNITCD unitcd, COUNTYCD countycd, PLOT plot, SUBP subp, species_symbol, prior_measdate measdate, prior_age age_calc
    FROM new.phantom_tree_sources
    UNION ALL SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, species_symbol, prior_measdate, prior_age FROM new.phantom_cohort_sources""")
for _t in ("curated_cohorts_landis", "curated_cohorts_landis_stdorg"):
    r = con.execute(f"""WITH pr AS (SELECT statecd,unitcd,countycd,plot,subp,species_symbol,measdate,age_calc FROM new.{_t}
                                   WHERE intro_type='phantom_backfill' AND {scope('', 'statecd', 'countycd')})
        SELECT (SELECT count(*) FROM (SELECT * FROM pr EXCEPT ALL SELECT * FROM prov)),
               (SELECT count(*) FROM (SELECT * FROM prov WHERE {scope('', 'statecd', 'countycd')} EXCEPT ALL SELECT * FROM pr)),
               (SELECT count(*) FROM pr WHERE age_calc < 1), (SELECT count(*) FROM pr)""").fetchone()
    check(f"1g {_t}: product phantom rows = provenance rows key by key (both directions), ages >= 1",
          r[0] == 0 and r[1] == 0 and r[2] == 0, f"(product-only {r[0]}, provenance-only {r[1]}, age < 1: {r[2]}; {r[3]:,} rows)")
r = con.execute(f"""SELECT count(*), count(*) FILTER (WHERE age_calc < 1 OR age_calc IS NULL) FROM new.tree_trajectories
                    WHERE intro_type='phantom_backfill' AND {scope('', 'statecd', 'countycd')}""").fetchone()
check("1g tree_trajectories: phantom ages >= 1", r[1] == 0, f"({r[1]} of {r[0]:,})")

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
              AND rv.PLOT=br.PLOT AND rv.SUBP=br.SUBP AND rv.md=br.p AND rv.fsub+rv.fmic<=0) AS gap_nf,
            -- r7 (C F8): a gap member must not be dead between the visit and the source row
            EXISTS (SELECT 1 FROM zdead z WHERE z.STATECD=br.STATECD AND z.UNITCD=br.UNITCD AND z.COUNTYCD=br.COUNTYCD
              AND z.PLOT=br.PLOT AND z.SUBP=br.SUBP AND z.TREE=br.TREE AND z.zmd >= br.D AND z.zmd <= br.p) AS gap_dead
      FROM br),
ok AS (SELECT *, CASE WHEN no_debut THEN FALSE WHEN pre_debut THEN NOT (r_a OR r_n OR r_b OR r_c OR r_z)
                      ELSE NOT (gap_cens OR gap_nf OR gap_dead) END AS survives FROM g)
SELECT count(DISTINCT (STATECD,UNITCD,COUNTYCD,PLOT,SUBP,species_symbol,p,prior_age)) slots,
       count(*) members, count(*) FILTER (WHERE NOT survives) blocked_members,
       count(DISTINCT (STATECD,UNITCD,COUNTYCD,PLOT,SUBP,species_symbol,p,prior_age)) FILTER (WHERE survives) slots_with_survivor
FROM ok""").fetchone()
print(f"\n=== (a) cohort-pass phantom slots={r[0]:,} members={r[1]:,} individually-blocked members={r[2]:,} "
      f"slots with >=1 surviving member={r[3]:,}")
if r[0]:
    check("(a) every cohort-pass phantom slot has a member that survives its own blocks", r[0] == r[3], f"({r[3]:,} of {r[0]:,})")
else:
    info("(a) no cohort-pass phantom slots in this run", "(nothing to test)")

# ---- r7 E30 / E7: scaffold and time axis re-derived from RAW visits ------------------------
r = con.execute(f"""
WITH c AS (SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, MEASDATE md, forest_subpcond_prop fs, forest_micrcond_prop fm
           FROM new.clean_subplot_years WHERE {scope()}),
j AS (SELECT * FROM c FULL JOIN rv USING (STATECD, UNITCD, COUNTYCD, PLOT, SUBP, md))
SELECT count(*) FILTER (WHERE fsub IS NULL), count(*) FILTER (WHERE fs IS NULL),
       count(*) FILTER (WHERE fs IS NOT NULL AND fsub IS NOT NULL AND (abs(COALESCE(fs,0) - fsub) > 1e-9 OR abs(COALESCE(fm,0) - fmic) > 1e-9)),
       count(*) FROM j""").fetchone()
check_n("(n15) E30: clean_subplot_years = the raw subplot-visit scaffold (keys and forest proportions)",
        r[0] == 0 and r[1] == 0 and r[2] == 0, r[3], f"(pipeline-only {r[0]}, raw-only {r[1]}, proportion differs {r[2]}; {r[3]:,} visits)")
# D18 rule from MEASDATE alone: raw_i = round half away from zero (days since the subplot's first
# scaffold visit / 365.25), sim_year_i = max(raw_i, sim_year_{i-1} + 1) = i + cummax_{j<=i}(raw_j - j)
con.execute("""
CREATE TEMP TABLE sy AS
WITH v AS (SELECT STATECD statecd, UNITCD unitcd, COUNTYCD countycd, PLOT plot, SUBP subp, md measdate,
                  CAST(ROUND(datediff('day', min(md) OVER w, md) / 365.25) AS INTEGER) raw,
                  CAST(row_number() OVER w - 1 AS INTEGER) i
           FROM rv WINDOW w AS (PARTITION BY STATECD, UNITCD, COUNTYCD, PLOT, SUBP ORDER BY md))
SELECT statecd, unitcd, countycd, plot, subp, measdate,
       CAST(i + max(raw - i) OVER (PARTITION BY statecd, unitcd, countycd, plot, subp ORDER BY measdate
                                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS INTEGER) sim_year
FROM v""")
for _t, _db in (("curated_cohorts_landis", "new"), ("curated_cohorts_landis_stdorg", "new"), ("tree_trajectories", "new")) + \
        ((("curated_cohorts_landis_simB", "newsimb"),) if a.simb_db else ()):
    r = con.execute(f"""SELECT count(*), count(*) FILTER (WHERE s.sim_year IS NULL),
                               count(*) FILTER (WHERE s.sim_year IS DISTINCT FROM c.sim_year)
        FROM {_db}.{_t} c LEFT JOIN sy s USING (statecd, unitcd, countycd, plot, subp, measdate)
        WHERE {scope('c', 'statecd', 'countycd')}""").fetchone()
    check_n(f"(n16) E7: {_t}: sim_year = the D18 rule re-derived from raw MEASDATEs", r[2] == 0, r[0],
            f"({r[2]:,} of {r[0]:,} rows differ; {r[1]:,} rows on a visit absent from the raw scaffold)")
# age_calc: one birth year per tree (sim_year - age_calc constant), and at the tree's first real
# record age_calc = round(estimated_age) of that curated record
r = con.execute(f"""
WITH t AS (SELECT statecd, unitcd, countycd, plot, subp, tree, count(DISTINCT sim_year - age_calc) nb
           FROM new.tree_trajectories WHERE {scope('', 'statecd', 'countycd')} GROUP BY ALL),
d AS (SELECT statecd, unitcd, countycd, plot, subp, tree, arg_min(age_calc, measdate) a0, min(measdate) md0
      FROM new.tree_trajectories WHERE intro_type = 'real' AND {scope('', 'statecd', 'countycd')} GROUP BY ALL)
SELECT (SELECT count(*) FROM t), (SELECT count(*) FROM t WHERE nb > 1),
       (SELECT count(*) FROM d JOIN new.curated_trees c ON c.STATECD=d.statecd AND c.UNITCD=d.unitcd AND c.COUNTYCD=d.countycd
           AND c.PLOT=d.plot AND c.SUBP=d.subp AND c.TREE=d.tree AND c.MEASDATE=d.md0
        WHERE c.estimated_age IS NOT NULL AND CAST(ROUND(c.estimated_age) AS INTEGER) <> d.a0)""").fetchone()
check_n("(n16) E7: tree_trajectories: one birth year per tree and debut age_calc = round(estimated_age)",
        r[1] == 0 and r[2] == 0, r[0], f"({r[0]:,} trees; {r[1]} with > 1 birth year; {r[2]} debut ages differ)")

# ---- r7 E9: record values and units re-derived from RAW TREE (by CN) ---------------------------
LB_G, M2_AC = 453.592, 4046.86   # lbs -> g; m2 per acre (the products' documented constants)
con.execute(f"""CREATE TEMP TABLE rt AS SELECT CAST(T.CN AS VARCHAR) cn, T.STATUSCD, T.SPCD, T.DIA, T.TPA_UNADJ, T.DRYBIO_AG
                FROM f.TREE T WHERE {scope('T')}""")
r = con.execute(f"""SELECT count(*),
       count(*) FILTER (WHERE r.cn IS NULL),
       count(*) FILTER (WHERE r.STATUSCD IS DISTINCT FROM c.STATUSCD OR r.SPCD IS DISTINCT FROM c.SPCD OR r.DIA IS DISTINCT FROM c.DIA),
       count(*) FILTER (WHERE r.TPA_UNADJ IS DISTINCT FROM c.TPA_UNADJ OR r.DRYBIO_AG IS DISTINCT FROM c.DRYBIO_AG)
    FROM new.curated_trees c LEFT JOIN rt r ON r.cn = CAST(c.CN AS VARCHAR) WHERE {scope('c')}""").fetchone()
check_n("(n17) E9: curated_trees STATUSCD / SPCD / DIA / TPA_UNADJ / DRYBIO_AG equal raw TREE (by CN)",
        r[1] == 0 and r[2] == 0 and r[3] == 0, r[0], f"({r[0]:,} records; CN not in raw {r[1]}; status/species/DIA differ {r[2]}; TPA/biomass differ {r[3]})")
# trajectories: a REAL row is one curated record; drybio_g = DRYBIO_AG * 453.592 (per tree), tpa = TPA_UNADJ
con.execute(f"""CREATE TEMP TABLE trr AS
SELECT t.statecd, t.unitcd, t.countycd, t.plot, t.subp, t.tree, t.measdate, t.species_symbol, t.age_calc, t.dia, t.drybio_g, t.tpa,
       r.STATUSCD rs, r.DIA rdia, r.DRYBIO_AG * {LB_G} rbio, r.TPA_UNADJ rtpa, c.estimated_age
FROM new.tree_trajectories t
LEFT JOIN new.curated_trees c ON c.STATECD=t.statecd AND c.UNITCD=t.unitcd AND c.COUNTYCD=t.countycd AND c.PLOT=t.plot
     AND c.SUBP=t.subp AND c.TREE=t.tree AND c.MEASDATE=t.measdate
LEFT JOIN rt r ON r.cn = CAST(c.CN AS VARCHAR)
WHERE t.intro_type = 'real' AND {scope('t', 'statecd', 'countycd')}""")
r = con.execute("""SELECT count(*), count(*) FILTER (WHERE rs IS NULL),
       count(*) FILTER (WHERE rs = 1 AND (abs(drybio_g - rbio) > 1e-6 * rbio + 1e-6 OR tpa IS DISTINCT FROM rtpa OR dia IS DISTINCT FROM rdia))
    FROM trr""").fetchone()
check_n("(n17) E9: tree_trajectories real live rows: drybio_g = raw DRYBIO_AG x 453.592, tpa = raw TPA_UNADJ, dia = raw DIA",
        r[1] == 0 and r[2] == 0, r[0], f"({r[0]:,} real rows; no raw record {r[1]}; value differs {r[2]})")
# Sim A / _stdorg real rows: one row per live aged tree record, agb = DRYBIO_AG * TPA_UNADJ * 453.592 / 4046.86
# (g/m2); compared per cohort key (subplot, species, measdate, age_calc): row count and AGB sum
con.execute(f"""CREATE TEMP TABLE exp_a AS SELECT statecd, unitcd, countycd, plot, subp, species_symbol, measdate, age_calc,
       count(*) n, sum(rbio * rtpa / {M2_AC}) agb FROM trr
    WHERE rs = 1 AND rbio IS NOT NULL AND rtpa IS NOT NULL AND estimated_age IS NOT NULL GROUP BY ALL""")
for _t in ("curated_cohorts_landis", "curated_cohorts_landis_stdorg"):
    r = con.execute(f"""WITH p AS (SELECT statecd, unitcd, countycd, plot, subp, species_symbol, measdate, age_calc, count(*) n, sum(agb) agb
                                   FROM new.{_t} WHERE intro_type IS NULL AND {scope('', 'statecd', 'countycd')} GROUP BY ALL)
        SELECT count(*), count(*) FILTER (WHERE p.n IS NULL), count(*) FILTER (WHERE e.n IS NULL),
               count(*) FILTER (WHERE p.n <> e.n OR abs(p.agb - e.agb) > 1e-6 * e.agb + 1e-6)
        FROM p FULL JOIN exp_a e USING (statecd, unitcd, countycd, plot, subp, species_symbol, measdate, age_calc)""").fetchone()
    check_n(f"(n17) E9: {_t} real rows = raw DRYBIO_AG x TPA_UNADJ (g/m2) per cohort key (both directions)",
            r[1] == 0 and r[2] == 0 and r[3] == 0, r[0], f"({r[0]:,} keys; raw-only {r[1]}, product-only {r[2]}, count/AGB differ {r[3]})")
if a.simb_db:
    r = con.execute(f"""
    WITH e AS (SELECT statecd, unitcd, countycd, plot, subp, species_symbol, measdate, age_calc, sum(rbio * rtpa / {M2_AC}) agb
               FROM trr WHERE rs = 1 AND rdia >= 5 AND rbio IS NOT NULL AND rtpa IS NOT NULL AND estimated_age IS NOT NULL GROUP BY ALL),
    p AS (SELECT statecd, unitcd, countycd, plot, subp, species_symbol, measdate, age_calc, count(*) n, sum(agb) agb
          FROM newsimb.curated_cohorts_landis_simB WHERE {scope('', 'statecd', 'countycd')} GROUP BY ALL)
    SELECT count(*), count(*) FILTER (WHERE p.agb IS NULL), count(*) FILTER (WHERE e.agb IS NULL),
           count(*) FILTER (WHERE p.n > 1 OR abs(p.agb - e.agb) > 1e-6 * e.agb + 1e-6)
    FROM p FULL JOIN e USING (statecd, unitcd, countycd, plot, subp, species_symbol, measdate, age_calc)""").fetchone()
    check_n("(n17) E9: Sim B rows = raw DRYBIO_AG x TPA_UNADJ of the real live >= 5 in members (one row per cohort key, both directions)",
            r[1] == 0 and r[2] == 0 and r[3] == 0, r[0], f"({r[0]:,} keys; raw-only {r[1]}, product-only {r[2]}, duplicate/AGB differ {r[3]})")

# ---- r7 E6: phantom COMPLETENESS — the allowed slot set re-derived (debut x earlier raw scaffold
# visits with prior_age >= 1, minus the D-rule blocks) equals the phantoms, in both directions -----
con.execute(f"""CREATE TEMP TABLE tdeb AS
SELECT statecd STATECD, unitcd UNITCD, countycd COUNTYCD, plot PLOT, subp SUBP, tree TREE,
       min(measdate) dmd, arg_min(age_calc, measdate) a0, arg_min(sim_year, measdate) s0
FROM new.tree_trajectories WHERE intro_type = 'real' AND {scope('', 'statecd', 'countycd')} GROUP BY ALL""")
con.execute("""CREATE TEMP TABLE cand AS
SELECT d.STATECD, d.UNITCD, d.COUNTYCD, d.PLOT, d.SUBP, d.TREE, s.measdate p, d.a0 + s.sim_year - d.s0 prior_age
FROM tdeb d JOIN sy s ON s.statecd=d.STATECD AND s.unitcd=d.UNITCD AND s.countycd=d.COUNTYCD AND s.plot=d.PLOT AND s.subp=d.SUBP
WHERE s.measdate < d.dmd AND d.a0 + s.sim_year - d.s0 >= 1""")
for _lab, _act, _rc in (
        ("tree_trajectories phantoms", "SELECT statecd STATECD, unitcd UNITCD, countycd COUNTYCD, plot PLOT, subp SUBP, tree TREE, measdate p "
                                      "FROM new.tree_trajectories WHERE intro_type='phantom_backfill'", False),
        ("Sim A tree-pass phantoms (phantom_tree_sources)", "SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE, prior_measdate p "
                                                            "FROM new.phantom_tree_sources", True)):
    con.execute(f"""CREATE OR REPLACE TEMP TABLE expv AS SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE, p FROM ({block_reasons("SELECT * FROM cand", _rc)})
                    WHERE NOT (r_a OR r_n OR r_b OR r_c OR r_z OR no_debut)""")
    r = con.execute(f"""WITH act AS ({_act})
        SELECT (SELECT count(*) FROM expv), (SELECT count(*) FROM act WHERE {scope()}),
               (SELECT count(*) FROM (SELECT * FROM expv EXCEPT SELECT * FROM act)),
               (SELECT count(*) FROM (SELECT * FROM act WHERE {scope()} EXCEPT SELECT * FROM expv))""").fetchone()
    check_n(f"(n18) E6: {_lab}: the re-derived allowed slot set equals the phantoms (both directions)",
            r[2] == 0 and r[3] == 0, r[0], f"(expected {r[0]:,}, actual {r[1]:,}; missing {r[2]:,}, unexpected {r[3]:,})")
    if r[2] or r[3]:
        show(f"(n18) {_lab}: first differences", f"""
        (SELECT 'missing' k, * FROM (SELECT * FROM expv EXCEPT SELECT * FROM ({_act})) ORDER BY ALL LIMIT 5)
        UNION ALL (SELECT 'unexpected', * FROM (SELECT * FROM ({_act}) WHERE {scope()} EXCEPT SELECT * FROM expv) ORDER BY ALL LIMIT 5)""")

# ---- r7 D36: tree identity re-derived independently from raw TREE + the verifier's own links --------
# A curated record r starts a NEW tree (TREE + 100000 k) when its same-number predecessor q (previous
# curated record of the raw key) exists and
#   (a) q is dead/cut and r.PREV_TRE_CN is NULL                         (D7 / r3 M3)
#   (b) r.PREV_TRE_CN is set and is not q's CN                          (r3 M3)
#   (c) r.PREV_TRE_CN is NULL and r.RECONCILECD is a new-tree code 1-4  (D36, A7-1)
# (r8 D48: the D29 link-start rule and the D35 relink-closed rule are removed)
con.execute(f"""
CREATE TEMP TABLE myseg AS
WITH r AS (SELECT CAST(c.CN AS VARCHAR) cn, c.STATECD, c.UNITCD, c.COUNTYCD, c.PLOT, c.SUBP, c.TREE ptree, c.MEASDATE md,
                  c.tree_key_split pflag, T.TREE rtree, T.STATUSCD st, T.SPCD sp, T.DIA dia,
                  CAST(T.PREV_TRE_CN AS VARCHAR) ptc, CAST(T.RECONCILECD AS INTEGER) rc, CAST(T.PLT_CN AS VARCHAR) pcn,
                  c.INVYR invyr
           FROM new.curated_trees c JOIN f.TREE T ON CAST(T.CN AS VARCHAR) = CAST(c.CN AS VARCHAR) WHERE {scope('c')}),
vb AS (SELECT STATECD, UNITCD, COUNTYCD, PLOT, INVYR FROM (
         SELECT P.STATECD, P.UNITCD, P.COUNTYCD, P.PLOT, P.INVYR, P.KINDCD, P.DESIGNCD,
                lag(P.DESIGNCD) OVER w pd, row_number() OVER w k
         FROM f.PLOT P JOIN f.SURVEY SV ON SV.CN = P.SRV_CN AND SV.ANN_INVENTORY = 'Y'
         WINDOW w AS (PARTITION BY P.STATECD, P.UNITCD, P.COUNTYCD, P.PLOT ORDER BY P.MEASYEAR, P.INVYR))
       WHERE k > 1 AND (KINDCD IN (1, 3) OR DESIGNCD IS DISTINCT FROM pd)),
q AS (SELECT r.*, lag(cn) OVER w qcn, lag(st) OVER w qst, lag(sp) OVER w qsp, lag(dia) OVER w qdia, lag(md) OVER w qmd
      FROM r WINDOW w AS (PARTITION BY STATECD, UNITCD, COUNTYCD, PLOT, SUBP, rtree ORDER BY md, cn)),
s AS (SELECT q.*,
        COALESCE(qst IN (2,3) AND ptc IS NULL, FALSE) s_dead,
        COALESCE(ptc IS NOT NULL AND ptc <> qcn, FALSE) s_other,
        COALESCE(ptc IS NULL AND rc IN (1,2,3,4), FALSE) s_newrc,
        EXISTS (SELECT 1 FROM vb WHERE vb.STATECD = q.STATECD AND vb.UNITCD = q.UNITCD AND vb.COUNTYCD = q.COUNTYCD
                AND vb.PLOT = q.PLOT AND vb.INVYR = q.invyr) s_visit
      FROM q),
g AS (SELECT s.*, qcn IS NOT NULL AND (s_dead OR s_other OR s_newrc OR s_visit) newseg FROM s)
SELECT g.*, CAST(sum(newseg::INT) OVER (PARTITION BY STATECD, UNITCD, COUNTYCD, PLOT, SUBP, rtree ORDER BY md, cn
                                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS BIGINT) seg
FROM g""")
con.execute("""CREATE OR REPLACE TEMP TABLE myseg AS SELECT *, rtree + 100000 * seg mytree,
                  max(seg) OVER (PARTITION BY STATECD, UNITCD, COUNTYCD, PLOT, SUBP, rtree) > 0 myflag FROM myseg""")
r = con.execute("""SELECT count(*), count(*) FILTER (WHERE mytree <> ptree), count(*) FILTER (WHERE myflag IS DISTINCT FROM pflag),
       count(DISTINCT (STATECD, UNITCD, COUNTYCD, PLOT, SUBP, rtree)) FILTER (WHERE mytree <> ptree),
       sum(s_dead::INT) FILTER (WHERE newseg), sum(s_other::INT) FILTER (WHERE newseg), sum(s_newrc::INT) FILTER (WHERE newseg)
       FROM myseg""").fetchone()
check_n("(n19) D36: tree ids (TREE + 100000 k) and tree_key_split = the split rule re-derived from raw TREE (every record)",
        r[1] == 0 and r[2] == 0, r[0], f"({r[0]:,} records; id differs {r[1]:,} on {r[3]} raw keys; flag differs {r[2]:,}; "
                                      f"segment starts dead {r[4]} / other-link {r[5]} / new-tree RC {r[6]})")
if r[1]:
    show("(n19) first id differences", """SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, rtree, md, st, sp, dia, rc, ptc IS NULL no_prev, ptree, mytree,
        s_dead, s_other, s_newrc FROM myseg WHERE (STATECD, UNITCD, COUNTYCD, PLOT, SUBP, rtree) IN
        (SELECT (STATECD, UNITCD, COUNTYCD, PLOT, SUBP, rtree) FROM myseg WHERE mytree <> ptree LIMIT 4) ORDER BY ALL LIMIT 30""")
# ---- r7 E10 / D36: censoring events and final fates re-derived from raw STATUSCD 0 records -------
# events are keyed by the post-split tree id: the segment of the raw key whose FIRST record is the
# latest one strictly before the event date (the tree the event closes); before every record of
# the key the raw number stays (myseg: the verifier's own split)
con.execute("""CREATE TEMP TABLE segfirst AS SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, rtree, mytree, min(md) fmd
               FROM myseg GROUP BY ALL""")


def _rekey(src, dcol):
    """src: a query with STATECD..SUBP, rtree and a date column -> + TREE (post-split id)"""
    return f"""SELECT e.*, COALESCE((SELECT arg_max(s.mytree, s.fmd) FROM segfirst s WHERE s.STATECD = e.STATECD AND s.UNITCD = e.UNITCD
                   AND s.COUNTYCD = e.COUNTYCD AND s.PLOT = e.PLOT AND s.SUBP = e.SUBP AND s.rtree = e.rtree AND s.fmd < e.{dcol}),
                   e.rtree) TREE FROM ({src}) e"""


con.execute(f"""CREATE TEMP TABLE rs0 AS {_rekey(f'''
    SELECT T.STATECD, T.UNITCD, T.COUNTYCD, T.PLOT, T.SUBP, T.TREE rtree, CAST(T.RECONCILECD AS INTEGER) RC,
           MAKE_DATE(P.MEASYEAR, COALESCE(NULLIF(CAST(P.MEASMON AS INTEGER), 0), 6), COALESCE(NULLIF(CAST(P.MEASDAY AS INTEGER), 0), 15)) md
    FROM f.TREE T JOIN f.PLOT P ON P.CN = T.PLT_CN JOIN f.SURVEY S ON S.CN = P.SRV_CN AND S.ANN_INVENTORY = 'Y'
    JOIN f.COND C ON C.PLT_CN = T.PLT_CN AND C.CONDID = T.CONDID
    WHERE T.STATUSCD = 0 AND T.SUBP <= 4 AND COALESCE(T.P2A_GRM_FLG, '') <> 'Y' AND {scope('T')}''', 'md')}""")
r = con.execute(f"""
WITH e AS (SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE, md MEASDATE,
                  CASE RC WHEN 5 THEN 'shrank' WHEN 6 THEN 'moved' WHEN 8 THEN 'procedural' WHEN 9 THEN 'nonsampled'
                          ELSE 'not_in_sample' END reason
           FROM rs0 WHERE RC IS DISTINCT FROM 7),
p AS (SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE, MEASDATE, reason FROM new.tree_censor_events
      WHERE reason <> 'procedural_unrecorded' AND {scope()})
SELECT (SELECT count(*) FROM e), (SELECT count(*) FROM (SELECT * FROM e EXCEPT ALL SELECT * FROM p)),
       (SELECT count(*) FROM (SELECT * FROM p EXCEPT ALL SELECT * FROM e))""").fetchone()
check_n("(n20) E10 / D36: STATUSCD 0 censoring events (shrank / moved / procedural / nonsampled / not_in_sample) = raw, "
        "keyed by the post-split tree id", r[1] == 0 and r[2] == 0, r[0], f"({r[0]:,} raw events; missing {r[1]}, extra {r[2]})")
# final_tree_fate re-derived (trees without a dead/cut record; the dead/cut ones are (c1)): the
# last record (a misrecorded death counts as alive there), then 'censored_<reason>' if the last
# censoring follows it, else — if the subplot is revisited — 'diverted_landuse' when the tree is
# live on a nonforest / water condition after it or the next visit is entirely nonforest,
# 'disappeared_inferred_removal' otherwise; 'alive' when it is never revisited
con.execute(f"""CREATE TEMP TABLE rlx AS {_rekey(f'''
    SELECT T.STATECD, T.UNITCD, T.COUNTYCD, T.PLOT, T.SUBP, T.TREE rtree,
           MAKE_DATE(P.MEASYEAR, COALESCE(NULLIF(CAST(P.MEASMON AS INTEGER), 0), 6), COALESCE(NULLIF(CAST(P.MEASDAY AS INTEGER), 0), 15)) md
    FROM f.TREE T JOIN f.PLOT P ON P.CN = T.PLT_CN JOIN f.SURVEY S ON S.CN = P.SRV_CN AND S.ANN_INVENTORY = 'Y'
    JOIN f.COND C ON C.PLT_CN = T.PLT_CN AND C.CONDID = T.CONDID
    WHERE T.STATUSCD = 1 AND T.SUBP <= 4 AND C.COND_STATUS_CD IN (2, 3, 4) AND COALESCE(T.P2A_GRM_FLG, '') <> 'Y' AND {scope('T')}''', 'md')}""")
con.execute(f"""
CREATE TEMP TABLE myfate AS
WITH t AS (SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE, max(MEASDATE) lmd, any_value(final_tree_fate) pf,
                  bool_or(STATUSCD IN (2,3) AND NOT COALESCE(misrecorded_death, FALSE)) has_dead
           FROM new.curated_trees WHERE {scope()} GROUP BY ALL),
c AS (SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE, max(MEASDATE) cmd, arg_max(reason, (MEASDATE, reason)) creason
      FROM new.tree_censor_events GROUP BY ALL),
x AS (SELECT t.*, c.cmd, c.creason,
             (SELECT min(rv.md) FROM rv WHERE rv.STATECD = t.STATECD AND rv.UNITCD = t.UNITCD AND rv.COUNTYCD = t.COUNTYCD
              AND rv.PLOT = t.PLOT AND rv.SUBP = t.SUBP AND rv.md > t.lmd) nmd,
             EXISTS (SELECT 1 FROM rlx WHERE rlx.STATECD = t.STATECD AND rlx.UNITCD = t.UNITCD AND rlx.COUNTYCD = t.COUNTYCD
                     AND rlx.PLOT = t.PLOT AND rlx.SUBP = t.SUBP AND rlx.TREE = t.TREE AND rlx.md > t.lmd) lux
      FROM t LEFT JOIN c USING (STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE) WHERE NOT t.has_dead)
SELECT x.*, CASE WHEN cmd > lmd THEN 'censored_' || creason
                 WHEN nmd IS NOT NULL AND (lux OR (SELECT rv.fsub + rv.fmic <= 0 FROM rv WHERE rv.STATECD = x.STATECD AND rv.UNITCD = x.UNITCD
                      AND rv.COUNTYCD = x.COUNTYCD AND rv.PLOT = x.PLOT AND rv.SUBP = x.SUBP AND rv.md = x.nmd)) THEN 'diverted_landuse'
                 WHEN nmd IS NOT NULL THEN 'disappeared_inferred_removal'
                 ELSE 'alive' END ef
FROM x""")
r = con.execute("SELECT count(*), count(*) FILTER (WHERE ef IS DISTINCT FROM pf) FROM myfate").fetchone()
check_n("(n20) E10: final_tree_fate of trees without a dead/cut record = re-derived (censoring, land-use exit, revisit)",
        r[1] == 0, r[0], f"({r[1]:,} of {r[0]:,} trees differ)")
if r[1]:
    show("(n20) final_tree_fate differences", "SELECT ef expected, pf curated, count(*) n FROM myfate WHERE ef IS DISTINCT FROM pf GROUP BY ALL ORDER BY 3 DESC LIMIT 10")
show("(n20) final_tree_fate of trees without a dead/cut record (re-derived)", "SELECT ef, count(*) n FROM myfate GROUP BY 1 ORDER BY 2 DESC, 1")
# D36 (F7-2 / C F14): curated_trees' censor_* columns are the tree's OWN (post-split) first and
# last censoring events: a split tree never inherits the old id's censoring
r = con.execute(f"""
WITH c AS (SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE, min(MEASDATE) c0, arg_min(reason, (MEASDATE, reason)) r0,
                  max(MEASDATE) c1, arg_max(reason, (MEASDATE, reason)) r1 FROM new.tree_censor_events GROUP BY ALL),
t AS (SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE, any_value(censored) cz, any_value(censor_measdate) m0, any_value(censor_reason) q0,
             any_value(censor_last_measdate) m1, any_value(censor_last_reason) q1, any_value(tree_key_split) spl
      FROM new.curated_trees WHERE {scope()} GROUP BY ALL)
SELECT count(*), count(*) FILTER (WHERE COALESCE(cz, FALSE) <> (c.c0 IS NOT NULL) OR m0 IS DISTINCT FROM c0 OR m1 IS DISTINCT FROM c1
                                   OR (c.c0 IS NOT NULL AND (q0 IS DISTINCT FROM r0 OR q1 IS DISTINCT FROM r1))),
       count(*) FILTER (WHERE spl AND (COALESCE(cz, FALSE) <> (c.c0 IS NOT NULL) OR m0 IS DISTINCT FROM c0 OR m1 IS DISTINCT FROM c1))
FROM t LEFT JOIN c USING (STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE)""").fetchone()
check_n("(n20) D36: curated_trees censored / censor_* = the tree's own (post-split) first and last censoring events",
        r[1] == 0, r[0], f"({r[1]:,} of {r[0]:,} trees differ; {r[2]} of them on split keys)")

# ---- (b) silent re-tally: residual unrecorded saplings by state x MANUAL transition ----------
# (pv / pv2 / us: built from RAW FIADB above, with the block-rule re-derivation)
# r7 E10: a sapling counts as censored only by 'procedural_unrecorded' (r8 D48: no relink censoring);
# censor events are keyed by the POST-split tree id (D36), taken from the curated record of the sapling
con.execute("""
CREATE TEMP TABLE us2 AS
WITH u AS (SELECT us.*, COALESCE(ct.TREE, us.TREE) ctree
           FROM us LEFT JOIN new.curated_trees ct ON CAST(ct.CN AS VARCHAR) = us.pcn)
SELECT u.*, (e.TREE IS NOT NULL AND e.pu) AS censored_unrec, COALESCE(e.pu, FALSE) AS censored_pu
FROM u LEFT JOIN (SELECT STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE,MEASDATE, bool_or(reason = 'procedural_unrecorded') pu
                   FROM new.tree_censor_events
                   WHERE reason = 'procedural_unrecorded' GROUP BY ALL) e
  ON e.STATECD=u.STATECD AND e.UNITCD=u.UNITCD AND e.COUNTYCD=u.COUNTYCD AND e.PLOT=u.PLOT
 AND e.SUBP=u.SUBP AND e.TREE=u.ctree AND e.MEASDATE=u.nmd""")
# r7 E10: the 'procedural_unrecorded' censoring events re-derived (an unrecorded sapling at a
# raw re-tally plot-visit, ub) equal the pipeline's, in both directions
r = con.execute("""
WITH e AS (SELECT DISTINCT STATECD,UNITCD,COUNTYCD,PLOT,SUBP,ctree TREE,nmd MEASDATE FROM us2
           WHERE unrec AND EXISTS (SELECT 1 FROM ub WHERE ub.STATECD=us2.STATECD AND ub.UNITCD=us2.UNITCD
                 AND ub.COUNTYCD=us2.COUNTYCD AND ub.PLOT=us2.PLOT AND ub.bd=us2.nmd)),
p AS (SELECT DISTINCT c.STATECD,c.UNITCD,c.COUNTYCD,c.PLOT,c.SUBP,c.TREE,c.MEASDATE FROM new.tree_censor_events c
      WHERE c.reason = 'procedural_unrecorded')
SELECT (SELECT count(*) FROM e), (SELECT count(*) FROM (SELECT * FROM e EXCEPT SELECT * FROM p)),
       (SELECT count(*) FROM (SELECT * FROM p EXCEPT SELECT * FROM e))""").fetchone()
if r[0] or r[2]:
    check("(b) 'procedural_unrecorded' censoring events = re-derived from raw (both directions)", r[1] == 0 and r[2] == 0,
          f"({r[0]:,} re-derived; missing {r[1]}, extra {r[2]})")
else:
    info("(b) no unrecorded saplings at a re-tally visit in this run", "(no procedural_unrecorded events expected; none found)")
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
       (SELECT count(*) FROM us2 WHERE censored_pu AND NOT unrec)""").fetchone()   # relink censoring covers recorded, non-continued trees
check("(b) no transition with >= 50 % uncensored unrecorded saplings (n >= 20)", r[0] == 0, f"({r[0]})")
check("(b) no plot-visit with >= 3 and >= 50 % uncensored unrecorded saplings", r[1] == 0, f"({r[1]})")
check("(b) censored only saplings that are really unrecorded", r[2] == 0, f"({r[2]})")
# r3 I6: independent of the detector's thresholds — in EVERY state x MANUAL transition with
# >= 20 saplings, at most 5 % of the previous visit's live saplings on still-sampled subplots
# may be unrecorded AND uncensored
bad = con.execute("""
SELECT STATECD, MANUAL, nman, count(*) n, avg((unrec AND NOT censored_unrec)::INT) res
FROM us2 GROUP BY ALL HAVING count(*) >= 20 AND avg((unrec AND NOT censored_unrec)::INT) > 0.01""").fetchall()
mx = con.execute("""SELECT max(res) FROM (SELECT avg((unrec AND NOT censored_unrec)::INT) res FROM us2
                    GROUP BY STATECD, MANUAL, nman HAVING count(*) >= 20)""").fetchone()[0]
# r7 E16: tightened from 5 % to 1 % (observed maximum 0.27 % in R4; tightening after seeing the data is conservative)
check("(b) every transition (>= 20 saplings): unrecorded AND uncensored <= 1 %", not bad,
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
       T.PREV_STATUS_CD ps, C.COND_STATUS_CD cs, T.INVYR, T.CONDID, C.TRTCD1, C.TRTCD2, C.TRTCD3,
       C.DSTRBCD1, C.DSTRBCD2, C.DSTRBCD3,
       MAKE_DATE(P.MEASYEAR, COALESCE(NULLIF(CAST(P.MEASMON AS INTEGER), 0), 6), COALESCE(NULLIF(CAST(P.MEASDAY AS INTEGER), 0), 15)) md
FROM f.TREE T JOIN f.PLOT P ON P.CN=T.PLT_CN JOIN f.SURVEY SV ON SV.CN = P.SRV_CN AND SV.ANN_INVENTORY = 'Y'
JOIN f.COND C ON C.PLT_CN=T.PLT_CN AND C.CONDID=T.CONDID
WHERE T.STATUSCD IN (2,3) AND T.SUBP<=4 AND COALESCE(T.P2A_GRM_FLG,'')<>'Y' AND C.COND_STATUS_CD IN (1,2,3,4)
  AND {scope('T')}""")
# r7 (D36): the tree id of a raw record is the POST-split id of its curated record (raw number if absent)
con.execute("""CREATE OR REPLACE TEMP TABLE rawdead AS SELECT d.*, COALESCE(c.TREE, d.TREE) ctree
               FROM rawdead d LEFT JOIN new.curated_trees c ON CAST(c.CN AS VARCHAR) = d.cn""")
# r6 R4 (D26), re-derived from raw: a dead/cut record FOLLOWED by a live record of the same tree
# key that FIA links back (PREV_TRE_CN set, PREV_STATUS_CD = 2) is a misrecorded death
con.execute(f"""
CREATE TEMP TABLE rawmis AS
SELECT d.cn FROM rawdead d WHERE EXISTS (
  SELECT 1 FROM f.TREE L JOIN f.PLOT P ON P.CN = L.PLT_CN JOIN f.SURVEY SV ON SV.CN = P.SRV_CN AND SV.ANN_INVENTORY = 'Y'
  WHERE L.STATECD = d.STATECD AND L.UNITCD = d.UNITCD AND L.COUNTYCD = d.COUNTYCD AND L.PLOT = d.PLOT
    AND L.SUBP = d.SUBP AND L.TREE = d.TREE AND L.STATUSCD = 1 AND L.PREV_STATUS_CD = 2 AND L.PREV_TRE_CN IS NOT NULL
    AND MAKE_DATE(P.MEASYEAR, COALESCE(NULLIF(CAST(P.MEASMON AS INTEGER), 0), 6), COALESCE(NULLIF(CAST(P.MEASDAY AS INTEGER), 0), 15)) > d.md)""")
con.execute("DELETE FROM rawdead WHERE cn IN (SELECT cn FROM rawmis)")   # fates / first records ignore them
con.execute(f"""
CREATE TEMP TABLE curdead AS
SELECT STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE, arg_min(CAST(CN AS VARCHAR), MEASDATE) first_cn,
       any_value(final_tree_fate) final_fate
FROM new.curated_trees WHERE STATUSCD IN (2,3) AND NOT misrecorded_death AND {scope()} GROUP BY ALL""")
r = con.execute(f"""SELECT (SELECT count(*) FROM rawmis),
    (SELECT count(*) FROM new.curated_trees WHERE misrecorded_death AND {scope()}),
    (SELECT count(*) FROM rawmis WHERE cn NOT IN (SELECT CAST(CN AS VARCHAR) FROM new.curated_trees WHERE misrecorded_death)
       AND cn IN (SELECT CAST(CN AS VARCHAR) FROM new.curated_trees)),   -- raw records the curation drops (RC7) are out
    (SELECT count(*) FROM new.curated_trees WHERE misrecorded_death AND {scope()} AND CAST(CN AS VARCHAR) NOT IN (SELECT cn FROM rawmis)),
    (SELECT count(DISTINCT (STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE)) FROM new.curated_trees WHERE misrecorded_death AND {scope()})""").fetchone()
check("(c1) R4: misrecorded deaths flagged exactly as re-derived from raw (dead/cut record, later linked live record)",
      r[2] == 0 and r[3] == 0, f"(raw {r[0]:,}, curated {r[1]:,} records on {r[4]:,} trees; missing {r[2]}, extra {r[3]})")
r = con.execute("""SELECT count(*) FILTER (WHERE tt.intro_type = 'real'), count(*) FILTER (WHERE tt.intro_type = 'gap_interpolated'),
  (SELECT count(*) FROM new.curated_trees WHERE misrecorded_death)
  FROM new.tree_trajectories tt JOIN new.curated_trees t
  ON t.STATECD=tt.statecd AND t.UNITCD=tt.unitcd AND t.COUNTYCD=tt.countycd AND t.PLOT=tt.plot AND t.SUBP=tt.subp
 AND t.TREE=tt.tree AND t.MEASDATE=tt.measdate AND t.STATUSCD IN (2,3) WHERE t.misrecorded_death""").fetchone()
check("(c1) R4: a misrecorded death is never a REAL trajectory slot (the tree continues)", r[0] == 0,
      f"({r[0]}; {r[1]} of {r[2]} such visits are now gap-interpolated live slots)")
# r7 E12: the raw class of every dead/cut record, with the harvest_inferred rule re-derived: a
# STATUSCD 2 record with AGENTCD NULL on a condition with TRTCD 10, on a condition-visit (plot, INVYR,
# CONDID) without an explicit first-record cut, and not a re-record (PREV_STATUS_CD 2 or a later dead
# record of the tree); a cut (STATUSCD 3 / AGENTCD 80) off forest is diverted_landuse
con.execute("""
CREATE TEMP TABLE rawcls AS
WITH o AS (SELECT d.*, row_number() OVER (PARTITION BY STATECD,UNITCD,COUNTYCD,PLOT,SUBP,ctree ORDER BY md, cn) k FROM rawdead d),
o2 AS (SELECT *, (k > 1 OR ps IS NOT DISTINCT FROM 2) rr, (STATUSCD = 3 OR AGENTCD = 80) is_cut FROM o),
ex AS (SELECT DISTINCT STATECD,UNITCD,COUNTYCD,PLOT,INVYR,CONDID FROM o2 WHERE is_cut AND cs = 1 AND NOT rr)
SELECT o2.*, (k = 1) is_first,
       CASE WHEN is_cut AND cs <> 1 THEN 'diverted_landuse'
            WHEN is_cut THEN 'silvicultural_death'
            WHEN STATUSCD = 2 AND AGENTCD IS NULL AND NOT rr AND 10 IN (COALESCE(TRTCD1,-1), COALESCE(TRTCD2,-1), COALESCE(TRTCD3,-1))
                 AND ex.PLOT IS NULL THEN 'harvest_inferred'
            ELSE 'dead' END cls
FROM o2 LEFT JOIN ex USING (STATECD,UNITCD,COUNTYCD,PLOT,INVYR,CONDID)""")
r = con.execute("""
SELECT count(*) n, count(*) FILTER (WHERE d.cls = c.final_fate) agree, count(*) FILTER (WHERE d.is_first) first_raw,
       count(*) FILTER (WHERE d.cls = 'harvest_inferred'), count(*) FILTER (WHERE c.final_fate = 'harvest_inferred')
FROM curdead c JOIN rawcls d ON d.cn = c.first_cn""").fetchone()
check_n("(c1) final fate = the re-derived class of the tree's FIRST raw dead/cut record (exact; r7 E12)",
        r[1] == r[0] and r[2] == r[0], r[0],
        f"({r[1]:,}/{r[0]:,} agree; {r[2]:,}/{r[0]:,} are the tree's first raw dead record; harvest_inferred raw {r[3]:,} / curated {r[4]:,})")
show("(c1) final fate vs raw class of the tree's first dead/cut record", """
SELECT d.cls raw_class, c.final_fate, count(*) n FROM curdead c JOIN rawcls d ON d.cn=c.first_cn GROUP BY ALL ORDER BY 1, 3 DESC, 2""")

# ---- (c2) re-records never produce events -------------------------------------------------------
r = con.execute("""
SELECT count(*) FILTER (WHERE d.rr) rerec_raw, count(*) FILTER (WHERE d.rr AND NOT t.rerecord_dead) unflagged,
       count(*) FILTER (WHERE NOT d.rr AND t.rerecord_dead) extra, count(*) FILTER (WHERE d.ps = 2) ps2
FROM rawcls d JOIN new.curated_trees t ON CAST(t.CN AS VARCHAR) = d.cn""").fetchone()
# r7 E30: both halves of the r3 I1 rule (PREV_STATUS_CD = 2, or any dead record after the tree's
# first), both directions
check("(c2) rerecord_dead = re-derived (PREV_STATUS_CD 2, or a dead/cut record after the tree's first; both directions)",
      r[1] == 0 and r[2] == 0, f"({r[0]:,} re-records ({r[3]:,} by PREV_STATUS_CD 2); {r[1]} unflagged, {r[2]} extra)")
r = con.execute(f"""
WITH ev AS (SELECT STATECD,UNITCD,COUNTYCD,PLOT,SUBP,MEASDATE, sum(n_harvested + n_disturbance_dead) e
            FROM new.curated_cohorts WHERE {scope()} GROUP BY ALL HAVING sum(n_harvested + n_disturbance_dead) > 0),
fd AS (SELECT DISTINCT STATECD,UNITCD,COUNTYCD,PLOT,SUBP,MEASDATE FROM new.curated_trees
       WHERE STATUSCD IN (2,3) AND NOT rerecord_dead AND NOT COALESCE(misrecorded_death, FALSE) AND {scope()})   -- r7 F-9
SELECT count(*), sum(e) FROM ev ANTI JOIN fd USING (STATECD,UNITCD,COUNTYCD,PLOT,SUBP,MEASDATE)""").fetchone()
check("(c2) no cohort harvest/disturbance event at a subplot-visit whose dead records are all re-records",
      (r[0] or 0) == 0, f"({r[0]} subplot-visits, {r[1]} events)")
# r7 D41: the two former subp_has_dstrb checks (first non-re-record dead record; raw attribution
# with the old two-definition rule) are replaced by the both-direction re-derivation (n21) below
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
FROM f.TREE_GRM_COMPONENT WHERE {f"STATECD IN ({','.join(map(str, states))})" if states else "TRUE"}""")   # (GRM has no COUNTYCD: state scope only; joined to the scoped curdead)
show("(c3) INFO: GRM component of each tree's first dead/cut record vs final_tree_fate", """
SELECT g.comp, c.final_fate, count(*) n FROM curdead c JOIN grm g ON g.cn = c.first_cn
WHERE g.comp IN ('CUT','MORTALITY','DIVERSION') GROUP BY ALL ORDER BY 1, 3 DESC""")

# ---- r7 D41 / E6 / E8: disturbance events, subp_has_dstrb and the Sim B drops re-derived from raw ----
# ONE definition (D41): a disturbance death is a death (raw class 'dead') with AGENTCD in
# {10, 20, 30, 40, 50} or a DSTRBCD1-3 of its condition other than 0 / 60. A tree's removal EVENT
# is its FIRST dead/cut record when that record is not a re-record and is a cut (silvicultural_death /
# harvest_inferred) or a disturbance death; diversions and natural deaths are not events; a tree
# that disappeared (final fate disappeared_inferred_removal, D19 DISTURB_FATES) is removed at the
# first scaffold visit after its last record. Misrecorded deaths (D26) are not records of death.
# r8 D47.2 (D34): records BEFORE the tree's RC7 "erroneously tallied" record (rc7.d7) are withdrawn
# by FIA, so they are not raw events
con.execute("""
CREATE TEMP TABLE myev AS
SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, ctree TREE, md ev_md,
       CASE WHEN cls IN ('silvicultural_death', 'harvest_inferred') THEN 'cut' ELSE 'disturbance_death' END kind
FROM rawcls
WHERE is_first AND NOT rr
  AND NOT EXISTS (SELECT 1 FROM rc7 WHERE rc7.STATECD = rawcls.STATECD AND rc7.UNITCD = rawcls.UNITCD AND rc7.COUNTYCD = rawcls.COUNTYCD
                  AND rc7.PLOT = rawcls.PLOT AND rc7.SUBP = rawcls.SUBP AND rc7.TREE = rawcls.TREE AND rawcls.md < rc7.d7)
  AND (cls IN ('silvicultural_death', 'harvest_inferred')
      OR (cls = 'dead' AND (COALESCE(AGENTCD IN (10, 20, 30, 40, 50), FALSE) OR COALESCE(DSTRBCD1 NOT IN (0, 60), FALSE)
                            OR COALESCE(DSTRBCD2 NOT IN (0, 60), FALSE) OR COALESCE(DSTRBCD3 NOT IN (0, 60), FALSE))))
UNION ALL
SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE, nmd, 'inferred_removal' FROM myfate WHERE ef = 'disappeared_inferred_removal'""")
# trajectories' terminal_fate / death_cause (D41 / E23): the tree's final fate, with a death split
# into dead_disturbance / dead_natural by the ONE disturbance definition on its first dead record
# and 'alive' as alive_censored; death_cause follows (disappeared -> harvest_inferred)
r = con.execute(f"""
WITH ft AS (SELECT STATECD statecd, UNITCD unitcd, COUNTYCD countycd, PLOT plot, SUBP subp, TREE tree, any_value(final_tree_fate) ff
            FROM new.curated_trees WHERE {scope()} GROUP BY ALL),
fd AS (SELECT STATECD statecd, UNITCD unitcd, COUNTYCD countycd, PLOT plot, SUBP subp, ctree tree,
              (COALESCE(AGENTCD IN (10, 20, 30, 40, 50), FALSE) OR COALESCE(DSTRBCD1 NOT IN (0, 60), FALSE)
               OR COALESCE(DSTRBCD2 NOT IN (0, 60), FALSE) OR COALESCE(DSTRBCD3 NOT IN (0, 60), FALSE)) dist
       FROM rawcls WHERE is_first),
e AS (SELECT ft.*, CASE WHEN ff = 'dead' AND fd.dist THEN 'dead_disturbance' WHEN ff = 'dead' THEN 'dead_natural'
                        WHEN ff = 'alive' THEN 'alive_censored' ELSE ff END etf FROM ft LEFT JOIN fd USING (statecd, unitcd, countycd, plot, subp, tree)),
t AS (SELECT statecd, unitcd, countycd, plot, subp, tree, count(DISTINCT terminal_fate) nf, any_value(terminal_fate) tf,
             any_value(death_cause) dc FROM new.tree_trajectories WHERE {scope('', 'statecd', 'countycd')} GROUP BY ALL)
SELECT count(*), count(*) FILTER (WHERE t.nf > 1 OR t.tf IS DISTINCT FROM e.etf),
       count(*) FILTER (WHERE t.dc IS DISTINCT FROM CASE WHEN e.etf = 'dead_disturbance' THEN 'disturbance' WHEN e.etf = 'dead_natural' THEN 'natural'
                                                   WHEN e.etf LIKE 'harvest%' THEN e.etf WHEN e.etf = 'disappeared_inferred_removal' THEN 'harvest_inferred' END)
FROM t LEFT JOIN e USING (statecd, unitcd, countycd, plot, subp, tree)""").fetchone()
check_n("(n21) D41 / E23: tree_trajectories terminal_fate / death_cause = the final fate with the one disturbance definition",
        r[1] == 0 and r[2] == 0, r[0], f"({r[0]:,} trees; terminal_fate differs {r[1]:,}; death_cause differs {r[2]:,})")
if r[1]:
    show("(n21) terminal_fate differences", f"""WITH ft AS (SELECT STATECD statecd, UNITCD unitcd, COUNTYCD countycd, PLOT plot, SUBP subp, TREE tree,
        any_value(final_tree_fate) ff FROM new.curated_trees WHERE {scope()} GROUP BY ALL)
        SELECT ft.ff, t.terminal_fate, count(DISTINCT (t.statecd, t.unitcd, t.countycd, t.plot, t.subp, t.tree)) n FROM new.tree_trajectories t
        JOIN ft USING (statecd, unitcd, countycd, plot, subp, tree) GROUP BY ALL ORDER BY 3 DESC LIMIT 12""")
# subp_has_dstrb (D41): the subplot has a removal event AFTER its first scaffold visit (trees
# without a live / aged record count too) — compared in BOTH directions on every product table
con.execute("""CREATE TEMP TABLE mydst AS
SELECT DISTINCT e.STATECD statecd, e.UNITCD unitcd, e.COUNTYCD countycd, e.PLOT plot, e.SUBP subp FROM myev e
WHERE e.ev_md > (SELECT min(rv.md) FROM rv WHERE rv.STATECD = e.STATECD AND rv.UNITCD = e.UNITCD AND rv.COUNTYCD = e.COUNTYCD
                 AND rv.PLOT = e.PLOT AND rv.SUBP = e.SUBP)""")
for _t, _db in (("curated_cohorts_landis", "new"), ("curated_cohorts_landis_stdorg", "new"), ("tree_trajectories", "new")) + \
        ((("curated_cohorts_landis_simB", "newsimb"),) if a.simb_db else ()):
    r = con.execute(f"""WITH s AS (SELECT statecd, unitcd, countycd, plot, subp, bool_or(subp_has_dstrb) f, bool_and(subp_has_dstrb) fa
                                   FROM {_db}.{_t} WHERE {scope('', 'statecd', 'countycd')} GROUP BY ALL)
        SELECT count(*), count(*) FILTER (WHERE f), count(*) FILTER (WHERE f AND m.subp IS NULL),
               count(*) FILTER (WHERE NOT f AND m.subp IS NOT NULL), count(*) FILTER (WHERE f <> fa)
        FROM s LEFT JOIN mydst m USING (statecd, unitcd, countycd, plot, subp)""").fetchone()
    check_n(f"(n21) D41: {_t}: subp_has_dstrb = re-derived from raw (one disturbance definition, both directions)",
            r[2] == 0 and r[3] == 0 and r[4] == 0, r[0],
            f"({r[0]:,} subplots, {r[1]:,} flagged; flagged without a raw event {r[2]:,}; raw event but unflagged {r[3]:,}; "
            f"flag not constant per subplot {r[4]})")
if a.simb_db:
    # Sim B drops (D19 / D25 / D31 / D41 / D42), magnitudes from RAW biomass. A cohort is (subplot,
    # species, birth year = sim_year - age_calc); its members at a visit p are the REAL live records
    # with DIA >= 5 in. At the next scaffold visit t a member is a survivor (real live >= 5 in record),
    # excluded (alive but not a real >= 5 in record: shrank, or only a gap slot), or REMOVED when its
    # removal event is at t; anything else (natural death, censoring, diversion) is neither. Survivor
    # growth gf = clip(sum w_t / sum w_p, 0.5, 3) (1 without survivors), w = DRYBIO_AG x 453.592 x TPA;
    # dropped = sum of removed w_p x gf; would_be = dropped + survivors' w_t; a row exists iff dropped
    # > 0, and with no survivor it is a FULL removal (D42, drop 1.0).
    con.execute("""
    CREATE TEMP TABLE mydrop AS
    WITH ax AS (SELECT statecd, unitcd, countycd, plot, subp, measdate, lead(measdate) OVER w nmd, lead(sim_year) OVER w nsy
                FROM sy WINDOW w AS (PARTITION BY statecd, unitcd, countycd, plot, subp ORDER BY measdate)),
    mem AS (SELECT t.*, t.rbio * t.rtpa w, x.nmd, x.nsy FROM trr t JOIN ax x USING (statecd, unitcd, countycd, plot, subp, measdate)
            WHERE t.rs = 1 AND t.rdia >= 5 AND t.rbio IS NOT NULL AND t.rtpa IS NOT NULL AND x.nmd IS NOT NULL),
    st AS (SELECT m.statecd, m.unitcd, m.countycd, m.plot, m.subp, m.species_symbol, m.nmd, m.nsy,
                  m.measdate, m.tree, m.w w_prev, (SELECT s.sim_year FROM sy s WHERE s.statecd = m.statecd AND s.unitcd = m.unitcd
                      AND s.countycd = m.countycd AND s.plot = m.plot AND s.subp = m.subp AND s.measdate = m.measdate) - m.age_calc nb,
                  c.w w_cur, (c.tree IS NULL AND (g.tree IS NOT NULL OR s5.tree IS NOT NULL)) excl,
                  (c.tree IS NULL AND EXISTS (SELECT 1 FROM myev e WHERE e.STATECD = m.statecd AND e.UNITCD = m.unitcd AND e.COUNTYCD = m.countycd
                        AND e.PLOT = m.plot AND e.SUBP = m.subp AND e.TREE = m.tree AND e.ev_md = m.nmd)) removed
           FROM mem m
           LEFT JOIN (SELECT *, rbio * rtpa w FROM trr WHERE rs = 1 AND rdia >= 5) c
             ON c.statecd = m.statecd AND c.unitcd = m.unitcd AND c.countycd = m.countycd AND c.plot = m.plot AND c.subp = m.subp
            AND c.tree = m.tree AND c.measdate = m.nmd
           LEFT JOIN (SELECT * FROM trr WHERE rs = 1 AND (rdia < 5 OR rdia IS NULL)) s5
             ON s5.statecd = m.statecd AND s5.unitcd = m.unitcd AND s5.countycd = m.countycd AND s5.plot = m.plot AND s5.subp = m.subp
            AND s5.tree = m.tree AND s5.measdate = m.nmd
           LEFT JOIN new.tree_trajectories g ON g.intro_type = 'gap_interpolated' AND g.statecd = m.statecd AND g.unitcd = m.unitcd
            AND g.countycd = m.countycd AND g.plot = m.plot AND g.subp = m.subp AND g.tree = m.tree AND g.measdate = m.nmd),
    co AS (SELECT statecd, unitcd, countycd, plot, subp, species_symbol, nmd, any_value(nsy) nsy, nb,
                  sum(w_prev) FILTER (WHERE w_cur IS NOT NULL) sp, sum(w_cur) FILTER (WHERE w_cur IS NOT NULL) sc,
                  count(*) FILTER (WHERE w_cur IS NOT NULL) n_surv, count(*) FILTER (WHERE removed AND NOT excl) n_dist,
                  sum(w_prev) FILTER (WHERE removed AND NOT excl) wrem
           FROM st GROUP BY statecd, unitcd, countycd, plot, subp, species_symbol, nmd, nb)
    SELECT statecd, unitcd, countycd, plot, subp, species_symbol, nmd measdate, CAST(nsy - nb AS INTEGER) age_calc,
           wrem * CASE WHEN sp > 0 THEN least(greatest(sc / sp, 0.5), 3.0) ELSE 1.0 END dropped, n_dist, n_surv,
           wrem * CASE WHEN sp > 0 THEN least(greatest(sc / sp, 0.5), 3.0) ELSE 1.0 END + COALESCE(sc, 0) would_be,
           (n_surv = 0) full_removal,
           -- a no-survivor cohort-visit at which another tree of the cohort key is alive in ANY form
           -- (a member below 5 in / as a gap slot, an ingrowth tree, a phantom): D42 does not say whether
           -- that is a full removal (there may be a Sim B row) — reported, not compared
           (n_surv = 0 AND EXISTS (SELECT 1 FROM new.tree_trajectories z LEFT JOIN trr r USING (statecd, unitcd, countycd, plot, subp, tree, measdate)
                WHERE z.statecd = co.statecd AND z.unitcd = co.unitcd AND z.countycd = co.countycd AND z.plot = co.plot AND z.subp = co.subp
                  AND z.species_symbol = co.species_symbol AND z.measdate = co.nmd AND z.sim_year - z.age_calc = co.nb
                  AND (z.intro_type IN ('phantom_backfill', 'gap_interpolated') OR r.rs = 1))) amb
    FROM co WHERE n_dist > 0 AND wrem > 0""")
    _cdcols = {r[0] for r in con.execute("SELECT column_name FROM duckdb_columns() WHERE database_name='newsimb' "
                                         "AND table_name='cohort_disturbance'").fetchall()}
    _fr = "COALESCE(d.full_removal, FALSE)" if "full_removal" in _cdcols else "(d.n_survivors = 0)"
    if "full_removal" not in _cdcols:
        check("(n22) D42: cohort_disturbance carries full_removal", False, "(column missing)")
    r = con.execute(f"""
    WITH d AS (SELECT *, CAST(measdate AS DATE) md FROM newsimb.cohort_disturbance WHERE {scope('', 'statecd', 'countycd')}),
    j AS (SELECT m.*, d.dropped d_dropped, d.would_be d_would_be, d.n_disturbed d_nd, d.n_survivors d_ns, {_fr} d_full,
                 d.disturbance_drop_pct d_drop, d.statecd d_st
          FROM (SELECT * FROM mydrop WHERE NOT amb) m FULL JOIN (SELECT d.* FROM d ANTI JOIN (SELECT * FROM mydrop WHERE amb) x
                 ON d.statecd = x.statecd AND d.unitcd = x.unitcd AND d.countycd = x.countycd AND d.plot = x.plot AND d.subp = x.subp
                AND d.species_symbol = x.species_symbol AND d.md = x.measdate AND d.age_calc = x.age_calc) d
            ON d.statecd = m.statecd AND d.unitcd = m.unitcd AND d.countycd = m.countycd AND d.plot = m.plot
           AND d.subp = m.subp AND d.species_symbol = m.species_symbol AND d.md = m.measdate AND d.age_calc = m.age_calc)
    SELECT count(*) FILTER (WHERE statecd IS NOT NULL), count(*) FILTER (WHERE d_st IS NOT NULL),
           count(*) FILTER (WHERE d_st IS NULL), count(*) FILTER (WHERE statecd IS NULL),
           count(*) FILTER (WHERE statecd IS NOT NULL AND d_st IS NOT NULL AND (abs(dropped - d_dropped) > 1e-6 * dropped + 1e-5
                            OR abs(would_be - d_would_be) > 1e-6 * would_be + 1e-5 OR n_dist <> d_nd OR n_surv <> d_ns OR full_removal <> d_full)),
           count(*) FILTER (WHERE full_removal), count(*) FILTER (WHERE d_full)
    FROM j""").fetchone()
    check_n("(n22) E6 / E8 / D41 / D42: cohort_disturbance = the drop set re-derived from raw (keys both directions; dropped, "
            "would_be, members, full removals)", r[2] == 0 and r[3] == 0 and r[4] == 0, r[0],
            f"(re-derived {r[0]:,} rows ({r[5]:,} full removals), pipeline {r[1]:,} ({r[6]:,}); missing {r[2]:,}, extra {r[3]:,}, "
            f"values differ {r[4]:,})")
    _amb = con.execute("""SELECT count(*), count(*) FILTER (WHERE EXISTS (SELECT 1 FROM newsimb.cohort_disturbance d
        WHERE d.statecd = m.statecd AND d.unitcd = m.unitcd AND d.countycd = m.countycd AND d.plot = m.plot AND d.subp = m.subp
          AND d.species_symbol = m.species_symbol AND CAST(d.measdate AS DATE) = m.measdate AND d.age_calc = m.age_calc))
        FROM mydrop m WHERE amb""").fetchone()
    info("(n22) D42 open case: no surviving member, but another tree of the cohort key alive at the visit",
         f"({_amb[0]:,} cohort-visits; {_amb[1]:,} of them have a cohort_disturbance row) — not compared")
    if r[2] or r[3] or r[4]:
        show("(n22) first drop differences", f"""
        WITH d AS (SELECT *, CAST(measdate AS DATE) md FROM newsimb.cohort_disturbance WHERE {scope('', 'statecd', 'countycd')})
        (SELECT 'missing' k, m.statecd, m.countycd, m.plot, m.subp, m.species_symbol, m.measdate, m.age_calc, round(m.dropped, 1) dropped,
                NULL::DOUBLE p_dropped, m.n_dist, m.n_surv FROM mydrop m ANTI JOIN d ON d.statecd = m.statecd AND d.unitcd = m.unitcd
           AND d.countycd = m.countycd AND d.plot = m.plot AND d.subp = m.subp AND d.species_symbol = m.species_symbol
           AND d.md = m.measdate AND d.age_calc = m.age_calc ORDER BY ALL LIMIT 5)
        UNION ALL
        (SELECT 'extra', d.statecd, d.countycd, d.plot, d.subp, d.species_symbol, d.md, d.age_calc, NULL, round(d.dropped, 1),
                d.n_disturbed, d.n_survivors FROM d ANTI JOIN mydrop m ON d.statecd = m.statecd AND d.unitcd = m.unitcd
           AND d.countycd = m.countycd AND d.plot = m.plot AND d.subp = m.subp AND d.species_symbol = m.species_symbol
           AND d.md = m.measdate AND d.age_calc = m.age_calc ORDER BY ALL LIMIT 5)""")
    # the stored drop (D31 / D42): partial = (dropped / 4046.8564224) / (agb + dropped / 4046.8564224) with agb the
    # Sim B row (verified against raw above); a full removal has no Sim B row and drop 1.0. The Sim B
    # attached drop equals it on every row, and no Sim B row carries a drop the re-derivation lacks.
    r = con.execute(f"""
    WITH b AS (SELECT * FROM newsimb.curated_cohorts_landis_simB WHERE {scope('', 'statecd', 'countycd')}),
    j AS (SELECT m.*, b.agb, b.disturbance_drop_pct b_drop, d.disturbance_drop_pct d_drop,
                 -- a full removal whose cohort still has a Sim B row (ingrowth members only) keeps Pan's formula,
                 -- since agb / (1 - 1.0) cannot be inverted
                 CASE WHEN b.agb IS NULL THEN 1.0 ELSE (m.dropped / 4046.8564224) / (b.agb + m.dropped / 4046.8564224) END e_drop
          FROM (SELECT * FROM mydrop WHERE NOT amb) m
          LEFT JOIN b USING (statecd, unitcd, countycd, plot, subp, species_symbol, measdate, age_calc)
          LEFT JOIN newsimb.cohort_disturbance d ON d.statecd = m.statecd AND d.unitcd = m.unitcd AND d.countycd = m.countycd
           AND d.plot = m.plot AND d.subp = m.subp AND d.species_symbol = m.species_symbol AND CAST(d.measdate AS DATE) = m.measdate
           AND d.age_calc = m.age_calc)
    SELECT count(*), count(*) FILTER (WHERE NOT full_removal AND agb IS NULL), count(*) FILTER (WHERE full_removal AND agb IS NULL AND d_drop <> 1.0),
           count(*) FILTER (WHERE abs(d_drop - e_drop) > 1e-7 OR d_drop IS NULL),
           count(*) FILTER (WHERE agb IS NOT NULL AND (abs(b_drop - e_drop) > 1e-7 OR b_drop IS NULL)),
           (SELECT count(*) FROM b WHERE disturbance_drop_pct > 0 AND NOT EXISTS (SELECT 1 FROM mydrop m WHERE m.statecd = b.statecd
              AND m.unitcd = b.unitcd AND m.countycd = b.countycd AND m.plot = b.plot AND m.subp = b.subp AND m.species_symbol = b.species_symbol
              AND m.measdate = b.measdate AND m.age_calc = b.age_calc))
    FROM j""").fetchone()
    check_n("(n8) F7-9: the stored and attached drops = Pan's formula on the RE-DERIVED dropped biomass and the Sim B AGB "
            "(full removals without a Sim B row: 1.0)", all(x == 0 for x in r[1:]), r[0],
            f"({r[0]:,} drops; partial without a Sim B row {r[1]}, full removal not 1.0 {r[2]}; cohort_disturbance drop differs {r[3]}, "
            f"Sim B drop differs {r[4]}, Sim B drops without a re-derived drop {r[5]})")

# ---- r7 E23 / D39 / D43 / D44: product attributes re-derived -------------------------------------
_TABS = (("curated_cohorts_landis", "new"), ("curated_cohorts_landis_stdorg", "new"), ("tree_trajectories", "new")) + \
        ((("curated_cohorts_landis_simB", "newsimb"),) if a.simb_db else ())
# raw annual visits of the product scaffold (canonical key + MEASDATE -> PLT_CN, CYCLE, KINDCD)
con.execute(f"""CREATE TEMP TABLE pvis AS
SELECT DISTINCT CAST(P.CN AS VARCHAR) cn, P.STATECD statecd, P.UNITCD unitcd, P.COUNTYCD countycd, P.PLOT plot, P.INVYR, P.CYCLE, P.KINDCD,
       MAKE_DATE(P.MEASYEAR, COALESCE(NULLIF(CAST(P.MEASMON AS INTEGER), 0), 6), COALESCE(NULLIF(CAST(P.MEASDAY AS INTEGER), 0), 15)) measdate
FROM f.PLOT P JOIN f.SURVEY S ON S.CN = P.SRV_CN AND S.ANN_INVENTORY = 'Y'
WHERE {scope('P')}
  AND EXISTS (SELECT 1 FROM rv WHERE rv.STATECD = P.STATECD AND rv.UNITCD = P.UNITCD AND rv.COUNTYCD = P.COUNTYCD AND rv.PLOT = P.PLOT
              AND rv.md = MAKE_DATE(P.MEASYEAR, COALESCE(NULLIF(CAST(P.MEASMON AS INTEGER), 0), 6), COALESCE(NULLIF(CAST(P.MEASDAY AS INTEGER), 0), 15)))""")
# D43: the allowed reference visits = the product's annual scaffold visits (pvis) plus the canonical
# remap's raw visits
con.execute(f"""CREATE TEMP TABLE okcn AS SELECT cn FROM pvis UNION SELECT CN FROM f.main._canon m
                WHERE m.CN IN (SELECT CAST(P.CN AS VARCHAR) FROM f.PLOT P WHERE {scope('P')})""")
# land_use (D44 C F10 / D43): 2-way everywhere — 'artificial' iff a condition of one of the plot's
# allowed reference visits has STDORGCD 1, 'natural' if it has STDORGCD values and none is 1
con.execute("""CREATE TEMP TABLE lux AS
SELECT P.STATECD statecd, P.UNITCD unitcd, P.COUNTYCD countycd, P.PLOT plot,
       CASE WHEN max(C.STDORGCD) = 1 THEN 'artificial' ELSE 'natural' END lu
FROM f.PLOT P JOIN okcn o ON o.cn = CAST(P.CN AS VARCHAR) JOIN f.COND C ON C.PLT_CN = P.CN
WHERE C.STDORGCD IS NOT NULL GROUP BY ALL""")
for _t, _db in _TABS:
    r = con.execute(f"""SELECT count(*), count(*) FILTER (WHERE c.land_use IS DISTINCT FROM x.lu),
                               count(*) FILTER (WHERE c.land_use NOT IN ('artificial', 'natural'))
        FROM {_db}.{_t} c LEFT JOIN lux x USING (statecd, unitcd, countycd, plot) WHERE {scope('c', 'statecd', 'countycd')}""").fetchone()
    check_n(f"(n23) D44 / D43: {_t}: land_use is the 2-way STDORGCD label of the plot's product-visit conditions",
            r[1] == 0, r[0], f"({r[1]:,} of {r[0]:,} rows differ; {r[2]:,} not artificial / natural)")
_tc = {r[0] for r in con.execute("SELECT column_name FROM duckdb_columns() WHERE database_name='new' AND table_name='tree_trajectories'").fetchall()}
check("(n23) D44 C F10: tree_trajectories keeps the 4-way management class as land_use_4way", "land_use_4way" in _tc, "")
# procedural_breaks re-derived: RC8 closure visits (rb), silent re-tallies (ub) (r8 D48: no coord_link breaks)
r = con.execute(f"""
WITH e AS (SELECT STATECD, UNITCD, COUNTYCD, PLOT, bd FROM rb UNION SELECT STATECD, UNITCD, COUNTYCD, PLOT, bd FROM ub),
p AS (SELECT DISTINCT STATECD, UNITCD, COUNTYCD, PLOT, MEASDATE bd FROM new.procedural_breaks WHERE {scope()})
SELECT (SELECT count(*) FROM e), (SELECT count(*) FROM (SELECT * FROM e EXCEPT SELECT * FROM p)),
       (SELECT count(*) FROM (SELECT * FROM p EXCEPT SELECT * FROM e))""").fetchone()
check_n("(n23) procedural_breaks = re-derived (RC8 closures, silent re-tallies; both directions)",
        r[1] == 0 and r[2] == 0, r[0], f"({r[0]:,} re-derived plot-visits; missing {r[1]}, extra {r[2]})")
con.execute(f"""CREATE TEMP TABLE pbx AS SELECT STATECD statecd, UNITCD unitcd, COUNTYCD countycd, PLOT plot, bd measdate FROM rb
                UNION SELECT STATECD, UNITCD, COUNTYCD, PLOT, bd FROM ub""")
for _t, _db in _TABS:
    if _t == "tree_trajectories":
        continue
    r = con.execute(f"""SELECT count(*), count(*) FILTER (WHERE c.procedural_break IS DISTINCT FROM (x.plot IS NOT NULL))
        FROM {_db}.{_t} c LEFT JOIN pbx x USING (statecd, unitcd, countycd, plot, measdate) WHERE {scope('c', 'statecd', 'countycd')}""").fetchone()
    check_n(f"(n23) E23: {_t}: procedural_break = the row's visit is a re-derived procedural break", r[1] == 0, r[0],
            f"({r[1]:,} of {r[0]:,} rows differ)")
# cycle (D44 C F3): a row carries its OWN visit's CYCLE (synthetic rows: their visit's, or NULL — never
# the debut's); plot_meas_num / subp_meas_num = distinct measurement dates of the plot / subplot in the
# table — over its REAL rows in Sim A / _stdorg (counted before the backfill), over all rows in the
# trajectories (as built; Sim B has no synthetic rows)
for _t, _db in _TABS:
    _mf = "intro_type IS NULL" if _t.startswith("curated_cohorts_landis") and not _t.endswith("simB") else "TRUE"
    r = con.execute(f"""
    WITH c AS (SELECT * FROM {_db}.{_t} WHERE {scope('', 'statecd', 'countycd')}),
    pm AS (SELECT statecd, unitcd, countycd, plot, count(DISTINCT measdate) n FROM c WHERE {_mf} GROUP BY ALL),
    sm AS (SELECT statecd, unitcd, countycd, plot, subp, count(DISTINCT measdate) n FROM c WHERE {_mf} GROUP BY ALL)
    SELECT count(*),
           count(*) FILTER (WHERE c.cycle IS DISTINCT FROM v.CYCLE AND NOT (c.cycle IS NULL AND COALESCE(c.intro_type, 'real') <> 'real')),
           count(*) FILTER (WHERE c.plot_meas_num IS DISTINCT FROM pm.n), count(*) FILTER (WHERE c.subp_meas_num IS DISTINCT FROM sm.n)
    FROM c LEFT JOIN pvis v USING (statecd, unitcd, countycd, plot, measdate)
    LEFT JOIN pm USING (statecd, unitcd, countycd, plot) LEFT JOIN sm USING (statecd, unitcd, countycd, plot, subp)""").fetchone()
    check_n(f"(n23) E23 / D44: {_t}: cycle = the row's own visit CYCLE; plot_meas_num / subp_meas_num = distinct visits",
            r[1] == 0 and r[2] == 0 and r[3] == 0, r[0], f"({r[0]:,} rows; cycle differs {r[1]:,}; plot_meas_num {r[2]:,}; subp_meas_num {r[3]:,})")
# synthetic trajectory rows carry their own visit's condition (D44 C F3): condid NULL or a condition of that visit
r = con.execute(f"""SELECT count(*), count(*) FILTER (WHERE t.condid IS NOT NULL AND NOT EXISTS (SELECT 1 FROM f.COND C
                         WHERE CAST(C.PLT_CN AS VARCHAR) = v.cn AND C.CONDID = t.condid))
    FROM new.tree_trajectories t JOIN pvis v USING (statecd, unitcd, countycd, plot, measdate)
    WHERE t.intro_type IN ('phantom_backfill', 'gap_interpolated') AND {scope('t', 'statecd', 'countycd')}""").fetchone()
if r[0]:
    check("(n23) D44 C F3: synthetic trajectory rows carry a CONDID of their own visit (or NULL)", r[1] == 0, f"({r[1]:,} of {r[0]:,})")
# alternate age axes (D44 C F12): >= 1 or NULL
r = con.execute(f"""SELECT count(*), count(*) FILTER (WHERE age_totage < 1 OR age_stdage < 1 OR age_estage < 1 OR age_model < 1)
                    FROM new.tree_trajectories WHERE {scope('', 'statecd', 'countycd')}""").fetchone()
check_n("(n23) D44 C F12: alternate age axes are >= 1 or NULL", r[1] == 0, r[0], f"({r[1]:,} of {r[0]:,} rows)")
# age-source priority TOTAGE > estage > model > STDAGE and the age_domain rules that need no training counts
r = con.execute(f"""SELECT count(*),
    count(*) FILTER (WHERE age_source IS DISTINCT FROM CASE WHEN age_from_totage IS NOT NULL THEN 'TOTAGE' WHEN age_from_estage IS NOT NULL THEN 'estage'
                                                            WHEN age_from_model IS NOT NULL THEN 'model' WHEN age_from_stdage IS NOT NULL THEN 'STDAGE' END
                     AND NOT (age_source LIKE 'STDAGE_%' AND age_from_totage IS NULL AND age_from_estage IS NULL AND age_from_model IS NULL
                              AND age_from_stdage IS NOT NULL)),
    count(*) FILTER (WHERE estimated_age IS DISTINCT FROM COALESCE(age_from_totage, age_from_estage, age_from_model, age_from_stdage)),
    count(*) FILTER (WHERE age_domain IS NOT NULL AND age_domain NOT IN ('natural_treeage_scale', 'in_support', 'out_of_support', 'unmapped_estage')
                     AND age_domain NOT LIKE 'known_bias_%'),
    count(*) FILTER (WHERE age_domain NOT LIKE 'known_bias_%' AND age_domain IS DISTINCT FROM 'unmapped_estage' AND (
          (age_source IS DISTINCT FROM 'model' AND age_domain IS NOT NULL)
       OR (age_source = 'model' AND age_stratum = 'natural' AND DIA < 5 AND age_domain IS DISTINCT FROM 'out_of_support')
       OR (age_source = 'model' AND age_stratum = 'natural' AND NOT (DIA < 5) AND age_domain IS DISTINCT FROM 'natural_treeage_scale')
       OR (age_source = 'model' AND age_stratum = 'planted' AND age_domain NOT IN ('in_support', 'out_of_support'))
       OR (age_source = 'model' AND age_domain IS NULL)))
    FROM new.curated_trees WHERE {scope()}""").fetchone()
check_n("(n23) E23: age source priority TOTAGE > estage > model > STDAGE (estimated_age and age_source)", r[1] == 0 and r[2] == 0, r[0],
        f"({r[0]:,} records; age_source differs {r[1]:,}; estimated_age differs {r[2]:,})")
check("(n23) E23 / D40 M-9: age_domain values and rules (null off-model; natural model: out_of_support < 5 in, else natural_treeage_scale; "
      "planted model: in / out_of_support)", r[3] == 0 and r[4] == 0, f"(unknown values {r[3]:,}; rule violations {r[4]:,})")
# cohort age metadata (I4 / D10 / D11), REAL rows: per cohort key the member counts and AGB shares
# re-derived from the members' curated age_domain / age_source (Sim A: one row per tree record)
con.execute(f"""CREATE TEMP TABLE memx AS
SELECT t.statecd, t.unitcd, t.countycd, t.plot, t.subp, t.species_symbol, t.measdate, t.age_calc, t.rdia,
       t.rbio * t.rtpa / {M2_AC} agb, c.age_source src,
       -- D11 / D46: "natural model ages" = natural-stratum model ages, natural out_of_support (M-9) included
       (c.age_source = 'model' AND (c.age_domain = 'natural_treeage_scale' OR (c.age_domain = 'out_of_support' AND c.age_stratum = 'natural'))) is_nat,
       COALESCE(c.age_domain LIKE 'known_bias_%', FALSE) is_kb
FROM trr t JOIN new.curated_trees c ON c.STATECD=t.statecd AND c.UNITCD=t.unitcd AND c.COUNTYCD=t.countycd AND c.PLOT=t.plot
     AND c.SUBP=t.subp AND c.TREE=t.tree AND c.MEASDATE=t.measdate
WHERE t.rs = 1 AND t.rbio IS NOT NULL AND t.rtpa IS NOT NULL AND t.estimated_age IS NOT NULL""")
for _t, _db, _flt in (("curated_cohorts_landis", "new", "TRUE"), ("curated_cohorts_landis_stdorg", "new", "TRUE")) + \
        ((("curated_cohorts_landis_simB", "newsimb", "rdia >= 5"),) if a.simb_db else ()):
    r = con.execute(f"""
    WITH e AS (SELECT statecd, unitcd, countycd, plot, subp, species_symbol, measdate, age_calc, sum(is_nat::INT) nn, sum(is_kb::INT) nk,
                      sum(agb) FILTER (WHERE is_nat) / sum(agb) sn, sum(agb) FILTER (WHERE is_kb) / sum(agb) sk FROM memx WHERE {_flt} GROUP BY ALL),
    sa AS (SELECT statecd, unitcd, countycd, plot, subp, species_symbol, measdate, age_calc, src, sum(agb) w FROM memx WHERE {_flt} GROUP BY ALL),
    mx AS (SELECT statecd, unitcd, countycd, plot, subp, species_symbol, measdate, age_calc, max(w) wmax FROM sa GROUP BY ALL),
    p AS (SELECT statecd, unitcd, countycd, plot, subp, species_symbol, measdate, age_calc, sum(n_trees_natural_treeage_scale) nn,
                 sum(n_trees_known_bias) nk, sum(agb * share_natural_treeage_scale) / sum(agb) sn, sum(agb * share_known_bias) / sum(agb) sk,
                 list(DISTINCT age_source_major) maj
          FROM {_db}.{_t} WHERE intro_type IS NULL AND {scope('', 'statecd', 'countycd')} GROUP BY ALL)
    SELECT count(*), count(*) FILTER (WHERE p.nn <> e.nn OR p.nk <> e.nk OR abs(COALESCE(p.sn, 0) - COALESCE(e.sn, 0)) > 1e-6
                                       OR abs(COALESCE(p.sk, 0) - COALESCE(e.sk, 0)) > 1e-6),
           count(*) FILTER (WHERE EXISTS (SELECT 1 FROM UNNEST(p.maj) u(m) WHERE NOT EXISTS (SELECT 1 FROM sa JOIN mx USING
                  (statecd, unitcd, countycd, plot, subp, species_symbol, measdate, age_calc) WHERE sa.statecd = p.statecd AND sa.unitcd = p.unitcd
                  AND sa.countycd = p.countycd AND sa.plot = p.plot AND sa.subp = p.subp AND sa.species_symbol = p.species_symbol
                  AND sa.measdate = p.measdate AND sa.age_calc = p.age_calc AND sa.src IS NOT DISTINCT FROM u.m
                  AND (sa.w >= mx.wmax * (1 - 1e-9) OR {'FALSE' if _t.endswith('simB') else 'TRUE'}))))
    FROM p JOIN e USING (statecd, unitcd, countycd, plot, subp, species_symbol, measdate, age_calc)""").fetchone()
    check_n(f"(n23) E23 / D10: {_t}: real-row age metadata (n_trees / AGB shares natural_treeage_scale and known_bias, "
            "age_source_major) = re-derived from the members", r[1] == 0 and r[2] == 0, r[0],
            f"({r[0]:,} cohort keys; counts / shares differ {r[1]:,}; age_source_major not a dominant member source {r[2]:,})")
# D44 C F9 / E25: the DAMSEV-based pct_biomass_damaged column is removed
_cc = {r[0] for r in con.execute("SELECT column_name FROM duckdb_columns() WHERE database_name='new' AND table_name='curated_cohorts'").fetchall()}
check("(n23) D44 C F9: curated_cohorts no longer carries the always-0 pct_biomass_damaged", "pct_biomass_damaged" not in _cc, "")
# run_scope (D33 / D44 E29 / D F7): the verifier's --states equals run_scope.states; counties is
# VARCHAR; a run_id is recorded and every attached DB of the run carries the same one
_rs = {r[0]: r[1] for r in con.execute("SELECT column_name, data_type FROM duckdb_columns() WHERE database_name='new' AND table_name='run_scope'").fetchall()}
if _rs:
    _st, _ct = con.execute("SELECT states, counties FROM new.run_scope").fetchone()
    _have = sorted({int(x) for x in str(_st or "").split(",") if x})
    if states:
        check("(n23) D33: --states equals run_scope.states", sorted(states) == _have, f"(--states {sorted(states)}, run_scope {_have})")
    else:
        info("(n23) D33: no --states given", f"(run_scope.states {_have})")
    check("(n23) D44 E29: run_scope.counties is VARCHAR", _rs.get("counties") == "VARCHAR", f"({_rs.get('counties')})")
    if counties:
        _want = ",".join(f"{x}:{y}" for x, y in counties)
        check("(n23) D33: --counties equals run_scope.counties", str(_ct or "") == _want, f"(--counties {_want}, run_scope {_ct})")
    if "run_id" in _rs:
        _rid = con.execute("SELECT run_id FROM new.run_scope").fetchone()[0]
        _others = {}
        for _al in ("newsimb", "nslim"):
            if con.execute(f"SELECT count(*) FROM duckdb_databases() WHERE database_name='{_al}'").fetchone()[0]:
                for _tb in ("run_scope", "run_steps"):
                    if con.execute(f"SELECT count(*) FROM duckdb_tables() WHERE database_name='{_al}' AND table_name='{_tb}'").fetchone()[0]:
                        _others[f"{_al}.{_tb}"] = sorted({x[0] for x in con.execute(f"SELECT DISTINCT run_id FROM {_al}.{_tb}").fetchall()})
        if con.execute("SELECT count(*) FROM duckdb_tables() WHERE database_name='new' AND table_name='run_steps'").fetchone()[0]:
            _others["new.run_steps"] = sorted({x[0] for x in con.execute("SELECT DISTINCT run_id FROM new.run_steps").fetchall()})
        check("(n23) D44 D F7: one run — every run_scope / run_steps of the curated, Sim B and slim DBs carries the curated run_id",
              bool(_rid) and all(v == [_rid] for v in _others.values()), f"(run_id {_rid}; {_others})")
    else:
        check("(n23) D44 D F7: run_scope carries a run_id", False, "(missing)")
else:
    check("(n23) D33: run_scope present", False, "(missing)")

# ---- r7 D38 / D39: first-seen deaths and fully withdrawn visits ---------------------------------
_ctc = {r[0] for r in con.execute("SELECT column_name FROM duckdb_columns() WHERE database_name='new' AND table_name='curated_trees'").fetchall()}
if "first_record_dead" in _ctc:
    # D38 (A7-11): first_record_dead = the tree's FIRST curated record is dead / cut (a misrecorded
    # death is not a record of death), on every record of the tree
    r = con.execute(f"""
    WITH f AS (SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE,
                      arg_min(STATUSCD IN (2,3) AND NOT COALESCE(misrecorded_death, FALSE), (MEASDATE, CN)) e,
                      bool_or(first_record_dead) fo, bool_and(first_record_dead) fa
               FROM new.curated_trees WHERE {scope()} GROUP BY ALL)
    SELECT count(*), count(*) FILTER (WHERE e), count(*) FILTER (WHERE fo <> fa OR fo IS DISTINCT FROM e) FROM f""").fetchone()
    check_n("(n24) D38: first_record_dead = the tree's first record is dead / cut (constant per tree)", r[2] == 0, r[0],
            f"({r[0]:,} trees, {r[1]:,} first seen dead; {r[2]:,} differ)")
else:
    check("(n24) D38: curated_trees carries first_record_dead", False, "(column missing)")
# D39 (A7-4): a visit ALL of whose live tally records (the coverage reference's live rule, without the
# RC7 exclusion) precede an RC7 'erroneously tallied' record of the same tree is flagged
# all_live_withdrawn_rc7 on its curated records and in the cohort tables' visit metadata
con.execute(f"""
CREATE TEMP TABLE wdv AS
WITH lt AS (SELECT T.STATECD, T.UNITCD, T.COUNTYCD, T.PLOT,
                   MAKE_DATE(P.MEASYEAR, COALESCE(NULLIF(CAST(P.MEASMON AS INTEGER), 0), 6), COALESCE(NULLIF(CAST(P.MEASDAY AS INTEGER), 0), 15)) md,
                   (rc7.d7 IS NOT NULL AND MAKE_DATE(P.MEASYEAR, COALESCE(NULLIF(CAST(P.MEASMON AS INTEGER), 0), 6), COALESCE(NULLIF(CAST(P.MEASDAY AS INTEGER), 0), 15)) < rc7.d7) pre
            FROM f.TREE T JOIN f.PLOT P ON P.CN = T.PLT_CN JOIN f.SURVEY S ON S.CN = P.SRV_CN AND S.ANN_INVENTORY = 'Y'
            JOIN f.COND C ON C.PLT_CN = T.PLT_CN AND C.CONDID = T.CONDID
            JOIN f.SUBPLOT SU ON SU.PLT_CN = T.PLT_CN AND SU.SUBP = T.SUBP
            LEFT JOIN rc7 ON rc7.STATECD=T.STATECD AND rc7.UNITCD=T.UNITCD AND rc7.COUNTYCD=T.COUNTYCD
                 AND rc7.PLOT=T.PLOT AND rc7.SUBP=T.SUBP AND rc7.TREE=T.TREE
            WHERE T.STATUSCD = 1 AND T.TPA_UNADJ > 0 AND T.SUBP <= 4 AND C.COND_STATUS_CD = 1
              AND P.PLOT_STATUS_CD < 3 AND COALESCE(SU.SUBP_STATUS_CD, -1) <> 3 AND COALESCE(T.P2A_GRM_FLG, '') <> 'Y'
              AND T.SPCD IN (SELECT SPCD FROM f.REF_SPECIES) AND {scope('T')})
SELECT STATECD statecd, UNITCD unitcd, COUNTYCD countycd, PLOT plot, md measdate, count(*) n FROM lt GROUP BY ALL HAVING bool_and(pre)""")
_nwd = con.execute("SELECT count(*) FROM wdv").fetchone()[0]
if "all_live_withdrawn_rc7" in _ctc:
    r = con.execute(f"""SELECT count(*), count(*) FILTER (WHERE COALESCE(c.all_live_withdrawn_rc7, FALSE) <> (w.plot IS NOT NULL)),
                               count(DISTINCT (c.STATECD, c.UNITCD, c.COUNTYCD, c.PLOT, c.MEASDATE)) FILTER (WHERE c.all_live_withdrawn_rc7)
        FROM new.curated_trees c LEFT JOIN wdv w ON w.statecd = c.STATECD AND w.unitcd = c.UNITCD AND w.countycd = c.COUNTYCD
         AND w.plot = c.PLOT AND w.measdate = c.MEASDATE WHERE {scope('c')}""").fetchone()
    check("(n24) D39: curated_trees all_live_withdrawn_rc7 = re-derived (every live tally record of the visit precedes an RC7 record)",
          r[1] == 0, f"({_nwd} re-derived visits; {r[2]} flagged visits in curated_trees; {r[1]:,} records differ)")
else:
    check("(n24) D39: curated_trees carries all_live_withdrawn_rc7", False, f"(column missing; {_nwd} re-derived visits)")
for _t, _db in _TABS:
    _cols = {r[0] for r in con.execute(f"SELECT column_name FROM duckdb_columns() WHERE database_name='{_db}' AND table_name='{_t}'").fetchall()}
    if "all_live_withdrawn_rc7" not in _cols:
        check(f"(n24) D39: {_t} carries all_live_withdrawn_rc7 (visit metadata)", False, "(column missing)")
        continue
    r = con.execute(f"""SELECT count(*), count(*) FILTER (WHERE COALESCE(c.all_live_withdrawn_rc7, FALSE) <> (w.plot IS NOT NULL))
        FROM {_db}.{_t} c LEFT JOIN wdv w USING (statecd, unitcd, countycd, plot, measdate) WHERE {scope('c', 'statecd', 'countycd')}""").fetchone()
    check_n(f"(n24) D39: {_t}: all_live_withdrawn_rc7 = the row's visit is re-derived withdrawn", r[1] == 0, r[0], f"({r[1]:,} of {r[0]:,} rows differ)")

# ---- r7 D43: no PLOT / COND row of a visit outside the curated product ---------------------------
# allowed = okcn (the product's annual scaffold visits plus the canonical remap's raw visits)
for _cat, _lab in (("new", "curated DB"),) + ((("nslim", "slim DB"),) if a.new_slim else ()):
    r = con.execute(f"""SELECT (SELECT count(*) FROM {_cat}.PLOT), (SELECT count(*) FROM {_cat}.PLOT WHERE CAST(CN AS VARCHAR) NOT IN (SELECT cn FROM okcn)),
                               (SELECT count(*) FROM {_cat}.COND), (SELECT count(*) FROM {_cat}.COND WHERE CAST(PLT_CN AS VARCHAR) NOT IN (SELECT cn FROM okcn))""").fetchone()
    check_n(f"(n25) D43: {_lab}: no foreign PLT_CN — every PLOT / COND row belongs to a product visit or a remapped raw visit",
            r[1] == 0 and r[3] == 0, r[0], f"(PLOT {r[1]:,} of {r[0]:,} foreign; COND {r[3]:,} of {r[2]:,} foreign)")
r = con.execute(f"""SELECT count(*), count(*) FILTER (WHERE CAST(p.CN AS VARCHAR) IS NULL) FROM pvis v LEFT JOIN new.PLOT p ON CAST(p.CN AS VARCHAR) = v.cn""").fetchone()
check_n("(n25) D43: every product visit has its PLOT row in the curated DB", r[1] == 0, r[0], f"({r[1]:,} of {r[0]:,} missing)")

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
    viol, tested, inv = [], 0, {}
    for sp, meds, mn, nk in d:
        if nk < 4 or mn < 50:
            continue
        tested += 1
        inv[sp] = max(meds[i] - meds[i + 1] for i in range(len(meds) - 1))   # > 0: an inversion (yr)
        if any(meds[i] > meds[i + 1] for i in range(len(meds) - 1)):
            viol.append((sp, [round(x, 1) for x in meds]))
    return viol, tested, inv
for label, where, enforce in (("planted", "cond_stdorgcd = 1", True),
                              ("natural", "COALESCE(cond_stdorgcd, 0) <> 1", False),
                              ("pooled", "TRUE", False)):
    viol, tested, inv = _mono(where)
    print(f"\n=== (d) {label}: median age by DIA class 1-3/3-5/5-7/7-9 in: {tested} species tested, {len(viol)} non-monotone")
    for v in viol:
        print("    ", v)
    name = f"(d) {label}: median age monotone in DIA class (top-25 species, >= 50 records/class)"
    if enforce:   # r7 M-5: a vacuous 0/0 is a FAIL (check_n)
        # r8 D47.6: KNOWN (d) cells, keyed by fit scope + stratum + species (or "empty"); every
        # other species is still enforced, and a known cell is drift-guarded on its inversion
        _kd = {k[3] for k in KNOWN_EXCEPTIONS if k[0] == FIT_SCOPE and k[1] == "d" and k[2] == label}
        for _sp in sorted((x for x in _kd if x != "empty"), key=str):
            if _sp in inv:
                check_known(("d", label, _sp), f"(d) {label}: species {_sp} median age monotone in DIA class",
                            inv[_sp] <= 0, inv[_sp], f"(largest inversion {inv[_sp]:+.3f} yr)")
            else:
                info(f"(d) {label}: known exception for species {_sp} is attached but the species is not tested", "(re-measure / retire it)")
        _new = [v for v in viol if v[0] not in _kd]
        if tested == 0 and "empty" in _kd:
            check_known(("d", label, "empty"), f"{name} [EMPTY]", False, 0.0, "(0 species tested)")
        else:
            if "empty" in _kd:
                info(f"{name}: known exception 'empty' is attached but {tested} species are tested", "(consider retiring it)")
            check_n(name, not _new, tested, f"({len(_new)}/{tested} outside the known cells)")
    else:
        info(name, f"({len(viol)}/{tested} non-monotone; not enforced)")
show("(d) negative / < 1 yr live ages", f"""
SELECT age_source, count(*) FILTER (WHERE estimated_age < 0) neg, count(*) FILTER (WHERE estimated_age < 1) lt1,
       count(*) FILTER (WHERE ROUND(estimated_age) < 1) round_lt1, count(*) n
FROM new.curated_trees WHERE STATUSCD=1 AND {scope()} GROUP BY 1 ORDER BY 1""")
r = con.execute(f"""SELECT count(*), count(*) FILTER (WHERE estimated_age < 0) FROM new.curated_trees
                    WHERE STATUSCD=1 AND estimated_age IS NOT NULL AND {scope()}""").fetchone()
check_n("(d) no negative live ages (r7 E3)", r[1] == 0, r[0], f"({r[1]:,} of {r[0]:,} aged live records)")

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
    elif k[1]:   # r7 D40: below the H_MIN_VISITS floor -> FAIL-SMALL if beyond the limit
        below_floor(("h", k[0], "planted"), name, s1 <= lim, s1, detail + f" — < {H_MIN_VISITS} condition-visits")
    else:
        info(name, detail + " — natural stand: not enforced")
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
# r7 M-15: gate and statistic in ONE unit, the tree: a tree's gap is the median over its records in
# the cell (both ages propagate linearly from one anchor, so it is constant per tree), the cell
# statistic is the median over trees
rows = con.execute(f"""
WITH t AS (SELECT STATECD, (age_stratum = 'planted') planted,   -- r5 M-5: the tree's age-scale stratum
                  CASE WHEN DIA<10 THEN 'a 5-10' WHEN DIA<15 THEN 'b 10-15' ELSE 'c 15+' END dia_cls,
                  UNITCD, COUNTYCD, PLOT, SUBP, TREE, median(age_from_model - age_from_estage) g
           FROM new.curated_trees WHERE STATUSCD=1 AND DIA>=5 AND age_from_model IS NOT NULL AND age_from_estage IS NOT NULL
             AND {scope()} GROUP BY ALL)
SELECT STATECD, planted, dia_cls, count(*) trees, median(g) gap FROM t GROUP BY ALL ORDER BY ALL""").fetchall()
print("\n=== (j) median per-tree model - mapped-estage gap (yr) per state x stratum x DIA class (>= 200 trees enforced)")
for st_, pl_, dc, nt, gap in rows:
    name = f"(j) state {st_} {'planted' if pl_ else 'natural'} DIA {dc[2:]}: median gap {gap:+.1f} within +-3"
    if nt >= 200:
        check_known(("j", st_, "planted" if pl_ else "natural", dc[2:]), name, abs(gap) <= 3, gap, f"({nt:,} trees)")
    else:
        info(name, f"({nt:,} trees < 200: not enforced)")
        _k = (FIT_SCOPE, "j", st_, "planted" if pl_ else "natural", dc[2:])
        if _k in KNOWN_EXCEPTIONS:   # r7 M-3: say so when an excepted cell falls below the gate
            info(f"{name}: known exception {_k} is attached but the cell is below 200 trees", "(re-measure / retire it)")

# ---- (k) I6: independent age truth from FIADB SITETREE bored ages ---------------------------------
# AGEDIA = breast-height age. Linkage (SITETREE has no TRE_CN): the UNIQUE live tally tree on the
# same PLT_CN + CONDID with the same SPCD and the same DIA (r7 E28: the former SUBP + TREE number
# link is dropped, see below). Truth = AGEDIA + years to breast
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
# r7 E28 (investigated): SITETREE.TREE is the SITE-tree number of the condition, not the tally tree
# number. Of the R4 annual aged site trees with a SUBP, a same-numbered tally tree on the plot has
# the same species in only 30-60 % (per state) and a DIA within 1 in in ~20 %, so the old "key"
# link (same PLT_CN + SUBP + TREE, same SPCD, |dDIA| <= 1) was a coincidence filter. Only the
# documented proxy link remains: the UNIQUE live tally tree on the same PLT_CN + CONDID with the
# same SPCD and the same DIA.
con.execute("""
CREATE TEMP TABLE stl AS
WITH k2 AS (SELECT st.scn, any_value(CAST(T.CN AS VARCHAR)) tcn, count(*) nm FROM st JOIN f.TREE T
            ON T.PLT_CN=st.PLT_CN AND T.CONDID=st.CONDID AND T.SPCD=st.SPCD AND T.DIA=st.DIA
           AND T.STATUSCD=1 AND T.SUBP<=4
       GROUP BY st.scn HAVING count(*)=1)
SELECT st.*, l.tcn, l.how FROM st JOIN (SELECT scn, tcn, 'plot_cond_sp_dia' how FROM k2) l USING (scn)""")
con.execute("""
CREATE TEMP TABLE stc AS
SELECT x.STATECD, (x.cond_stdorgcd = 1) planted, l.how, CASE WHEN r.SFTWD_HRDWD='S' THEN 'softwood' ELSE 'hardwood' END wood,
       l.AGEDIA + CASE WHEN r.SFTWD_HRDWD='S' THEN 2 ELSE 5 END truth,
       x.estimated_age, x.age_from_estage, x.estage_raw, x.age_from_model, x.age_source
FROM stl l JOIN new.curated_trees x ON CAST(x.CN AS VARCHAR) = l.tcn
LEFT JOIN f.REF_SPECIES r ON r.SPCD = x.spcd_resolved WHERE x.STATUSCD = 1""")
n_st, n_link = con.execute("SELECT (SELECT count(*) FROM st), (SELECT count(*) FROM stc)").fetchone()
print(f"\n=== (k) SITETREE bored ages: {n_st:,} aged site trees in scope, {n_link:,} linked to a curated live record")
show("(k) SITETREE linkage method", "SELECT how, count(*) n FROM stc GROUP BY 1 ORDER BY 1")
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
_kb_rules = {   # r8 D47.5 (age_imputation.KNOWN_BIAS_FLAGS, re-derived here)
    "known_bias_fl_planted_model_15plus": ("STATECD=12 AND age_stratum='planted' AND age_source='model' AND DIA>=15",
                                           ("12", "12,21,28")),
    "known_bias_ky_planted_model_10to15": ("STATECD=21 AND age_stratum='planted' AND age_source='model' AND DIA>=10 AND DIA<15",
                                           ("21", "12,21,28")),
    "known_bias_tn_planted_model_15plus": ("STATECD=47 AND age_stratum='planted' AND age_source='model' AND DIA>=15",
                                           ("47",)),
    "known_bias_fl_planted_model_10plus": ("STATECD=12 AND age_stratum='planted' AND age_source='model' AND DIA>=10",
                                           ("1,12,13,21,28,37,45,47",)),
    "known_bias_ky_planted_model_5to10": ("STATECD=21 AND age_stratum='planted' AND age_source='model' AND DIA>=5 AND DIA<10",
                                          ("1,12,13,21,28,37,45,47",)),
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

# ---- (l2) the fate labels reach every product (a prefix test on "harvest" lost silvicultural_death, D60) ---------
r = con.execute(f"""SELECT count(*) FILTER (WHERE terminal_fate IN ('silvicultural_death', 'harvest_inferred') AND death_cause IS NULL),
                           count(*) FILTER (WHERE terminal_fate = 'silvicultural_death')
                    FROM new.tree_trajectories WHERE {scope('', 'statecd', 'countycd')}""").fetchone()
check("(l2) every harvest fate carries its death_cause in the trajectories", r[0] == 0, f"({r[1]:,} silvicultural rows; missing {r[0]:,})")
r = con.execute(f"""WITH f AS (SELECT count(*) n FROM new.curated_trees WHERE {scope()} AND STATUSCD IN (2, 3) AND NOT COALESCE(rerecord_dead, FALSE)
                               AND tree_fate IN ('silvicultural_death', 'harvest_inferred'))
                    SELECT (SELECT n FROM f), (SELECT sum(n_harvested) FROM new.curated_snapshots WHERE {scope()})""").fetchone()
check("(l2) curated_snapshots.n_harvested counts every first harvest record", r[0] > 0 and r[0] == r[1], f"(records {r[0]:,}, snapshots {r[1]:,})")

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
FROM new.curated_trees WHERE tree_key_split AND {scope()} GROUP BY 1 ORDER BY 1""")
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
    # species, birth sim_year) at the previous visit that is gone by the visit; never on a cohort's
    # first Sim B row (r7: the fate / event / magnitude of the removal is re-derived in n22 / n8)
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
           WHERE tt.intro_type = 'gap_interpolated' OR (tt.intro_type = 'real' AND ct.STATUSCD = 1))   -- alive slots only
    SELECT d.*,
      EXISTS (SELECT 1 FROM mem m WHERE m.statecd=d.statecd AND m.unitcd=d.unitcd AND m.countycd=d.countycd AND m.plot=d.plot
              AND m.subp=d.subp AND m.species_symbol=d.species_symbol AND m.b=d.b AND m.measdate=p.pmd
              AND NOT EXISTS (SELECT 1 FROM lv WHERE lv.statecd=m.statecd AND lv.unitcd=m.unitcd AND lv.countycd=m.countycd
                              AND lv.plot=m.plot AND lv.subp=m.subp AND lv.tree=m.tree AND lv.measdate=d.measdate)) AS has_removed,
      NOT EXISTS (SELECT 1 FROM newsimb.curated_cohorts_landis_simB c WHERE c.statecd=d.statecd AND c.unitcd=d.unitcd
              AND c.countycd=d.countycd AND c.plot=d.plot AND c.subp=d.subp AND c.species_symbol=d.species_symbol
              AND c.sim_year - c.age_calc = d.b AND c.measdate < d.measdate) AS is_first
    FROM d LEFT JOIN pv p USING ({TRK}, measdate)""")
    r = con.execute("""SELECT count(*), count(*) FILTER (WHERE NOT has_removed), count(*) FILTER (WHERE is_first)
                       FROM simb_drop""").fetchone()
    # r6 D25: removals only (the damaged-alive branch is gone)
    check("(n2) I-1: every Sim B drop > 0 has a real >= 5 in member removed / died since the previous visit",
          r[1] == 0, f"({r[1]} of {r[0]:,} drops unbacked)")
    check("(n2) I-1: no Sim B drop on a cohort's first Sim B row", r[2] == 0, f"({r[2]})")
    # n8 (R6): Pan's consumer reconstructs a row's pre-drop biomass as agb / (1 - drop)
    # (landis_region4 _weighted_cohort_drop); applying it must give back the drop model's dropped
    # biomass (g/acre -> g/m2: / 4046.8564224)
    r = con.execute(f"""
    SELECT count(*), count(*) FILTER (WHERE abs((c.agb / (1 - c.disturbance_drop_pct) - c.agb) - d.dropped / 4046.8564224)
                                           > 1e-5 * (d.dropped / 4046.8564224) + 1e-6),
           max(abs((c.agb / (1 - c.disturbance_drop_pct) - c.agb) / NULLIF(d.dropped / 4046.8564224, 0) - 1))
    FROM newsimb.curated_cohorts_landis_simB c JOIN newsimb.cohort_disturbance d
      USING (statecd, unitcd, countycd, plot, subp, species_symbol, measdate, age_calc)
    WHERE c.disturbance_drop_pct > 0 AND {scope('c', 'statecd', 'countycd')}""").fetchone()
    check("(n8) R6: Pan's formula agb/(1-drop) - agb reconstructs the dropped biomass on every Sim B drop row",
          r[1] == 0, f"({r[1]} of {r[0]:,} rows off by > 1e-5; max rel err {0 if r[2] is None else r[2]:.2e})")
    r = con.execute(f"""SELECT count(*) FROM newsimb.curated_cohorts_landis_simB c
        LEFT JOIN newsimb.cohort_disturbance d USING (statecd, unitcd, countycd, plot, subp, species_symbol, measdate, age_calc)
        WHERE c.disturbance_drop_pct > 0 AND (d.disturbance_drop_pct IS NULL OR d.disturbance_drop_pct <> c.disturbance_drop_pct)
          AND {scope('c', 'statecd', 'countycd')}""").fetchone()[0]
    check("(n8) R6: the attached drop equals cohort_disturbance's on every drop row", r == 0, f"({r})")
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
if r[0] == 0:   # r6 R5 (D27): nothing to check in the data; the fixtures below exercise the code
    info("(n3) M-4: no gap-interpolated slots in this run (no candidates) — see the (n9) fixtures")
else:
    check("(n3) M-4: no gap-interpolated slot at / across an entirely nonforest subplot visit", r[1] == 0,
          f"({r[1]} of {r[0]:,} gap slots)")
# r7 E18 (zombie rule on data): no gap-interpolated slot at, or between its bracketing real records
# across, a dead / cut record of the same tree (misrecorded deaths, D26, do not count)
r = con.execute(f"""
WITH g AS (SELECT {TRK}, tree, measdate FROM new.tree_trajectories WHERE intro_type='gap_interpolated' AND {scope('', 'statecd', 'countycd')}),
r AS (SELECT {TRK}, tree, measdate FROM new.tree_trajectories WHERE intro_type='real'),
br AS (SELECT g.*, (SELECT max(r.measdate) FROM r WHERE r.statecd=g.statecd AND r.unitcd=g.unitcd AND r.countycd=g.countycd
                     AND r.plot=g.plot AND r.subp=g.subp AND r.tree=g.tree AND r.measdate < g.measdate) p,
                   (SELECT min(r.measdate) FROM r WHERE r.statecd=g.statecd AND r.unitcd=g.unitcd AND r.countycd=g.countycd
                     AND r.plot=g.plot AND r.subp=g.subp AND r.tree=g.tree AND r.measdate > g.measdate) n FROM g)
SELECT count(*), count(*) FILTER (WHERE EXISTS (SELECT 1 FROM zdead z WHERE z.STATECD=br.statecd AND z.UNITCD=br.unitcd
       AND z.COUNTYCD=br.countycd AND z.PLOT=br.plot AND z.SUBP=br.subp AND z.TREE=br.tree AND z.zmd > br.p AND z.zmd <= br.n)) FROM br""").fetchone()
if r[0]:
    check("(n3) E18: no gap-interpolated slot at / across a dead or cut record of the tree", r[1] == 0, f"({r[1]} of {r[0]:,} gap slots)")
# n9 (R5, D27): synthetic fixtures for the interpolation (incl. the nonforest / censoring rules)
# and the timber-only drop branch — code paths the R4 data never reaches
import sys as _sys
_sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import selftest_fixtures
for _nm, _ok, _det in selftest_fixtures.run():
    check(f"(n9) fixture {_nm}", _ok, _det)
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
       count(*) FILTER (WHERE intro_type='phantom_backfill') FROM new.tree_trajectories WHERE {scope('', 'statecd', 'countycd')} ORDER BY 1""")
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
# r7 E11 / M-6 (D5, D40): natural trees are NEVER mapped (raw TreeAge on every candidate); planted
# trees are ALWAYS mapped, unless no mapping exists for the fit scope — then planted estage ages
# carry age_domain 'unmapped_estage' and the run FAILS unless the scope is listed below
UNMAPPED_ESTAGE_ALLOWED = set()   # fit scopes allowed to run without a TreeAge mapping (none)
r = con.execute(f"""WITH t AS (SELECT STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE, any_value(age_stratum) st,
                           count(*) n, count(*) FILTER (WHERE estage_mapped IS DISTINCT FROM estage_raw) nmap
                    FROM new.curated_trees WHERE estage_mapped IS NOT NULL AND {scope()} GROUP BY ALL)
    SELECT count(*) FILTER (WHERE st = 'natural'), count(*) FILTER (WHERE st = 'natural' AND nmap > 0),
           count(*) FILTER (WHERE st = 'planted'), count(*) FILTER (WHERE st = 'planted' AND nmap < n),
           count(*) FILTER (WHERE st = 'planted' AND nmap = 0) FROM t""").fetchone()
check("(n5) D5: natural-stratum trees carry RAW TreeAge on every estage candidate (none mapped)", r[1] == 0,
      f"({r[1]:,} of {r[0]:,} natural trees have a mapped candidate)")
_ecc = {r[0] for r in con.execute("SELECT column_name FROM duckdb_columns() WHERE database_name='new' AND table_name='estage_calibration'").fetchall()}
_unm_row = con.execute("SELECT count(*) FROM new.estage_calibration WHERE level = 'unmapped'").fetchone()[0] if _ecc else 0
# r8 D47.4 (D46 contract): UNMAPPED = a level='unmapped' row (no mapping could be fitted). A scope
# whose curves were all REJECTED (applied = false) is identity everywhere but NOT unmapped (not flagged)
_unmapped_scope = _unm_row > 0
_nflag = con.execute(f"""SELECT count(*) FILTER (WHERE age_domain = 'unmapped_estage'),
    count(*) FILTER (WHERE COALESCE(age_domain = 'unmapped_estage', FALSE) IS DISTINCT FROM
                           ({'TRUE' if _unmapped_scope else 'FALSE'} AND age_stratum = 'planted' AND age_source = 'estage'))
    FROM new.curated_trees WHERE {scope()}""").fetchone()
_appcol = next((c for c in ("applied", "curve_applied", "accepted") if c in _ecc), None)
if _unmapped_scope:
    # D40 (M-6): no mapping at all (< 200 planted calibration rows): FAIL unless the scope is listed
    check(f"(n5) D40: no TreeAge mapping for fit scope {FIT_SCOPE}: allowed only if listed (UNMAPPED_ESTAGE_ALLOWED)",
          FIT_SCOPE in UNMAPPED_ESTAGE_ALLOWED and r[4] == r[2],   # listed, and no planted candidate mapped
          f"({r[2]:,} planted trees on the raw scale; {_nflag[0]:,} records flagged unmapped_estage; "
          f"estage_calibration 'unmapped' row {'present' if _unm_row else 'MISSING'})")
elif _appcol:
    # D46: each curve (spgrpcd -> major_spgrpcd -> global) is applied only if its nested out-of-fold
    # MAE on its planted calibration rows is strictly below raw TreeAge's; a rejected curve is the
    # identity and its rows fall through to the next APPLIED level (or stay raw). Re-derived per
    # planted estage candidate: the expected curve from the table's per-curve decisions, then
    # mapped <> raw iff a curve applies (the curve's values themselves are not re-fitted)
    r2 = con.execute(f"""SELECT count(*), count(*) FILTER (WHERE {_appcol}), count(*) FILTER (WHERE {_appcol} AND NOT (oof_mae_mapped < mae_raw)),
                                count(*) FILTER (WHERE {_appcol} IS NULL)
                         FROM new.estage_calibration WHERE level IN ('spgrpcd', 'major_spgrpcd', 'global')""").fetchone()
    check_n("(n5) D46: every APPLIED mapping curve beats raw TreeAge (nested out-of-fold MAE strictly lower)",
            r2[2] == 0 and r2[3] == 0, r2[0], f"({r2[0]} curves, {r2[1]} applied; {r2[2]} applied without beating raw; {r2[3]} undecided)")
    r2 = con.execute(f"""
    WITH cv AS (SELECT level, "group" g, {_appcol} ap FROM new.estage_calibration WHERE level IN ('spgrpcd', 'major_spgrpcd', 'global')),
    c AS (SELECT t.estage_mapped, t.estage_raw, CAST(t.spgrpcd_resolved AS VARCHAR) sg, CAST(rs.MAJOR_SPGRPCD AS VARCHAR) mj
          FROM new.curated_trees t LEFT JOIN f.REF_SPECIES rs ON rs.SPCD = t.spcd_resolved
          WHERE t.age_stratum = 'planted' AND t.estage_mapped IS NOT NULL AND {scope('t')}),
    e AS (SELECT c.*, COALESCE((SELECT ap FROM cv WHERE level = 'spgrpcd' AND g = c.sg),
                               (SELECT ap FROM cv WHERE level = 'major_spgrpcd' AND g = c.mj), FALSE) a1,
                 (SELECT ap FROM cv WHERE level = 'spgrpcd' AND g = c.sg) s1, (SELECT ap FROM cv WHERE level = 'major_spgrpcd' AND g = c.mj) m1,
                 (SELECT ap FROM cv WHERE level = 'global') g1 FROM c)
    SELECT count(*), count(*) FILTER (WHERE COALESCE(s1, FALSE) OR COALESCE(m1, FALSE) OR COALESCE(g1, FALSE)) exp_mapped,
           count(*) FILTER (WHERE (COALESCE(s1, FALSE) OR COALESCE(m1, FALSE) OR COALESCE(g1, FALSE)) AND estage_mapped = estage_raw),
           count(*) FILTER (WHERE NOT (COALESCE(s1, FALSE) OR COALESCE(m1, FALSE) OR COALESCE(g1, FALSE)) AND estage_mapped <> estage_raw)
    FROM e""").fetchone()
    check_n("(n5) D46: planted estage candidates are mapped iff an applied curve covers them (a rejected curve is identity)",
            r2[2] == 0 and r2[3] == 0, r2[0], f"({r2[0]:,} candidates, {r2[1]:,} under an applied curve; unmapped there {r2[2]:,}; "
                                              f"mapped without an applied curve {r2[3]:,})")
else:
    check("(n5) D46: estage_calibration carries the per-curve applied decision", False, f"(no applied column; columns {sorted(_ecc)})")
    check_n("(n5) D40: planted-stratum trees carry MAPPED TreeAge on every estage candidate", r[3] == 0, r[2],
            f"({r[3]:,} of {r[2]:,} planted trees have an unmapped candidate)")
check("(n5) D40: estage_calibration no longer carries planted_split_used (the stratum split is moot)",
      "planted_split_used" not in _ecc, f"({'present' if 'planted_split_used' in _ecc else 'absent'})")
# D40 (M-7): the isotonic recalibration is applied per stratum x DIA-class cell only if its nested
# out-of-fold MAE decreases (age_recal_cells); age_model_cv reports the 'final' model
_rcc = {r[0] for r in con.execute("SELECT column_name FROM duckdb_columns() WHERE database_name='new' AND table_name='age_recal_cells'").fetchall()}
if _rcc:
    r2 = con.execute("""SELECT count(*), count(*) FILTER (WHERE applied),
        count(*) FILTER (WHERE applied AND NOT (mae_recal < mae_raw)),
        count(*) FILTER (WHERE NOT applied AND mae_recal < mae_raw AND COALESCE(n_fit_production, 0) >= 50)
        FROM new.age_recal_cells""").fetchone()
    check_n("(n5) D40 M-7: a recalibration cell is applied iff its nested out-of-fold MAE decreases (age_recal_cells)",
            r2[2] == 0 and r2[3] == 0, r2[0], f"({r2[0]} cells, {r2[1]} applied; applied without a decrease {r2[2]}; "
                                              f"a fittable decrease not applied {r2[3]})")
else:
    check("(n5) D40 M-7: age_recal_cells present", False, "(missing)")
r2 = con.execute("SELECT count(*) FILTER (WHERE model = 'final') FROM new.age_model_cv").fetchone()[0]
check("(n5) D40: age_model_cv reports the 'final' model (recalibration only in accepted cells)", r2 > 0, f"({r2} rows)")
check("(n5) D40: age_domain 'unmapped_estage' set exactly on planted estage ages of an unmapped scope", _nflag[1] == 0,
      f"({_nflag[0]:,} flagged; {_nflag[1]:,} differ from the rule)")
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
# r8 D48: the coordinate-link layer is removed — no link / relink / duplicate-visit artefact remains
_pkr = {r[0] for r in con.execute("SELECT column_name FROM duckdb_columns() WHERE database_name='new' AND table_name='plot_key_remap'").fetchall()}
r = con.execute(f"""SELECT
    (SELECT count(*) FROM new.tree_censor_events WHERE reason = 'procedural_relink'),
    (SELECT count(*) FROM new.procedural_breaks WHERE break_kind = 'coord_link'),
    (SELECT count(*) FROM new.plot_key_remap WHERE {"reason IS DISTINCT FROM 'prev_plt_cn'" if "reason" in _pkr else "FALSE"}
            AND {scope()}),   -- (plot_key_remap is national: the run's scope only)
    (SELECT count(*) FROM duckdb_tables() WHERE database_name = 'new' AND table_name = 'plot_key_coord_links')""").fetchone()
check("(n6) D48: no coordinate-link artefacts (procedural_relink censorings, coord_link breaks, coord_link / duplicate_visit "
      "remaps, plot_key_coord_links)", r == (0, 0, 0, 0) and "kept_cn" not in _pkr,
      f"(relink censorings {r[0]}, coord_link breaks {r[1]}, non-prev_plt_cn remaps {r[2]}, plot_key_coord_links {r[3]}, "
      f"kept_cn column {'present' if 'kept_cn' in _pkr else 'absent'})")
# every raw annual visit's records are in the views (no visit is dropped as a duplicate any more)
r = con.execute(f"""SELECT count(*) FROM fraw.main.TREE T JOIN fraw.main.PLOT P ON P.CN = T.PLT_CN
    JOIN fraw.main.SURVEY S ON S.CN = P.SRV_CN AND S.ANN_INVENTORY = 'Y' WHERE {scope('P')}""").fetchone()[0], \
    con.execute(f"""SELECT count(*) FROM f.TREE T JOIN f.PLOT P ON P.CN = T.PLT_CN
    JOIN f.SURVEY S ON S.CN = P.SRV_CN AND S.ANN_INVENTORY = 'Y' WHERE {scope('P')}""").fetchone()[0]
check("(n6) D48: the canonical views drop no raw annual TREE record (no duplicate-visit collapse)", r[0] == r[1],
      f"(raw {r[0]:,}, views {r[1]:,})")
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
# r7 E13: D21 says act if this is ever > 0, so it is enforced
check("(n7) tree-number splits linked through a STATUSCD 0 record WITHOUT evidence of a different tree = 0", r[2] == 0,
      f"(segments {r[0]} / linked through a STATUSCD 0 record {r[1]} / of those without evidence {r[2]})")

# n10 (R2): every Sim A / Sim B / trajectory (canonical key, measdate) visit has its PLOT row and
# >= 1 COND row in the curated DB (export_reference_tables) and in the slim DB
def _visit_cov(cat):
    vsrc = [f"SELECT DISTINCT statecd,unitcd,countycd,plot,measdate FROM new.{t} WHERE {scope('', 'statecd', 'countycd')}"
            for t in ("curated_cohorts_landis", "tree_trajectories")]
    if a.simb_db:
        vsrc.append(f"SELECT DISTINCT statecd,unitcd,countycd,plot,measdate FROM newsimb.curated_cohorts_landis_simB WHERE {scope('', 'statecd', 'countycd')}")
    return con.execute(f"""
    WITH v AS ({' UNION '.join(vsrc)}),
    p AS (SELECT CAST(CN AS VARCHAR) cn, STATECD, UNITCD, COUNTYCD, PLOT,
                 MAKE_DATE(CAST(MEASYEAR AS BIGINT), CAST(COALESCE(NULLIF(MEASMON, 0), 6) AS BIGINT),
                           CAST(COALESCE(NULLIF(MEASDAY, 0), 15) AS BIGINT)) md FROM {cat}.PLOT),
    c AS (SELECT DISTINCT CAST(PLT_CN AS VARCHAR) cn FROM {cat}.COND)
    SELECT count(*), count(*) FILTER (WHERE p.cn IS NULL), count(*) FILTER (WHERE p.cn IS NOT NULL AND c.cn IS NULL)
    FROM v LEFT JOIN p ON p.STATECD=v.statecd AND p.UNITCD=v.unitcd AND p.COUNTYCD=v.countycd AND p.PLOT=v.plot AND p.md=v.measdate
    LEFT JOIN c ON c.cn = p.cn""").fetchone()
for _cat, _lab in (("new", "curated DB"),) + ((("nslim", "slim DB"),) if a.new_slim else ()):
    r = _visit_cov(_cat)
    check(f"(n10) R2: every Sim A / Sim B / trajectory visit has its PLOT row and >= 1 COND row in the {_lab}",
          r[1] == 0 and r[2] == 0, f"({r[0]:,} visits; no PLOT {r[1]}, no COND {r[2]})")
# n11 (R1): the Sim A and trajectory sapling growth rates. r7 M-14 / E21: both are per-tree
# biomass rates from the same real rows, so they must be IDENTICAL at EVERY level (species x L3,
# spgrp x L3, spgrp), and each table must carry its own documented basis
_nt = {r[0] for r in con.execute("SELECT table_name FROM duckdb_tables() WHERE database_name='new'").fetchall()}
if {"sapling_growth_rates_simA", "sapling_growth_rates_traj"} <= _nt:
    r = con.execute("""
    WITH a AS (SELECT * FROM new.sapling_growth_rates_simA), b AS (SELECT * FROM new.sapling_growth_rates_traj),
    j AS (SELECT a.level, a.rate ra, b.rate rb, a.n_trees na, b.n_trees nb FROM a FULL JOIN b
          ON a.level = b.level AND a.species_symbol IS NOT DISTINCT FROM b.species_symbol
         AND a.epa_l3 IS NOT DISTINCT FROM b.epa_l3 AND a.spgrpcd IS NOT DISTINCT FROM b.spgrpcd)
    SELECT count(*), count(*) FILTER (WHERE ra IS NULL OR rb IS NULL),
           count(*) FILTER (WHERE abs(ra - rb) > 1e-9 * greatest(abs(ra), abs(rb), 1e-9) OR na <> nb),
           count(DISTINCT level), (SELECT list(DISTINCT basis) FROM a), (SELECT list(DISTINCT basis) FROM b) FROM j""").fetchone()
    check_n("(n11) R1: Sim A and trajectory sapling growth rates identical at every level (keys, rates, n_trees)",
            r[1] == 0 and r[2] == 0, r[0], f"({r[0]} rows over {r[3]} levels; unmatched {r[1]}, differ {r[2]})")
    check("(n11) E21: rate bases are the documented ones (Sim A tpa_expanded, trajectories per_tree)",
          r[4] == ["tpa_expanded"] and r[5] == ["per_tree"], f"(Sim A {r[4]}, trajectories {r[5]})")
    show("(n11) sapling growth rate per species group: Sim A (agb / tpa) vs trajectories (drybio_g)", """
    SELECT a.spgrpcd, a.n_trees n_simA, round(a.rate, 5) rate_simA, b.n_trees n_traj, round(b.rate, 5) rate_traj
    FROM new.sapling_growth_rates_simA a JOIN new.sapling_growth_rates_traj b USING (level, spgrpcd)
    WHERE level = 'spgrp' ORDER BY a.n_trees DESC, a.spgrpcd LIMIT 12""")
else:
    check("(n11) R1: sapling growth rate tables present", False, "(sapling_growth_rates_simA / _traj missing)")
# n12 (R7, D28; r7 E20 / D44 C F6): a plot whose chain has a remapped visit takes its LOCATION from
# its most recent annual visit — LAT / LON / ELEV on every curated PLOT row and in plot_location, and
# the data_plot_eco row of that visit's RAW key; every other plot keeps its own key's eco row. The
# cohort tables' epa_l3 / epa_l4 / ecosubcd and the exported data_plot_eco must carry those values.
con.execute(f"""
CREATE TEMP TABLE locx AS
WITH k AS (SELECT DISTINCT P.STATECD, P.UNITCD, P.COUNTYCD, P.PLOT FROM f.PLOT P JOIN f.main._canon m ON m.CN = CAST(P.CN AS VARCHAR)),
v AS (SELECT P.STATECD, P.UNITCD, P.COUNTYCD, P.PLOT, R.LAT, R.LON, R.ELEV, R.UNITCD ru, R.COUNTYCD rc, R.PLOT rp,
             MAKE_DATE(CAST(R.MEASYEAR AS BIGINT), CAST(COALESCE(NULLIF(R.MEASMON,0),6) AS BIGINT), CAST(COALESCE(NULLIF(R.MEASDAY,0),15) AS BIGINT)) md
      FROM f.PLOT P JOIN k USING (STATECD, UNITCD, COUNTYCD, PLOT) JOIN fraw.main.PLOT R ON R.CN = P.CN
      JOIN fraw.main.SURVEY S ON S.CN = R.SRV_CN AND S.ANN_INVENTORY = 'Y')
SELECT STATECD, UNITCD, COUNTYCD, PLOT, arg_max(LAT, md) lat, arg_max(LON, md) lon, arg_max(ELEV, md) elev,
       arg_max(ru, md) ru, arg_max(rc, md) rc, arg_max(rp, md) rp, count(*) nv FROM v GROUP BY ALL""")
r = con.execute(f"""SELECT count(*), count(*) FILTER (WHERE (c.LAT, c.LON, c.ELEV) IS DISTINCT FROM (l.lat, l.lon, l.elev)),
                           count(DISTINCT (l.STATECD,l.UNITCD,l.COUNTYCD,l.PLOT))
    FROM new.PLOT c JOIN locx l ON l.STATECD=c.STATECD AND l.UNITCD=c.UNITCD AND l.COUNTYCD=c.COUNTYCD AND l.PLOT=c.PLOT
    WHERE {scope('c')}""").fetchone()
_nloc = con.execute(f"SELECT count(*) FROM locx WHERE {scope()}").fetchone()[0]
if _nloc:
    check_n("(n12) R7: renumbered plots carry their most recent visit's LAT / LON / ELEV on every curated PLOT row",
            r[1] == 0, r[0], f"({r[0]:,} PLOT rows of {r[2]} plots; {r[1]} differ)")
    if "plot_location" in _nt:
        r = con.execute(f"""SELECT count(*), count(*) FILTER (WHERE (p.LAT, p.LON, p.ELEV) IS DISTINCT FROM (l.lat, l.lon, l.elev)
                                                        OR (p.LOC_UNITCD, p.LOC_COUNTYCD, p.LOC_PLOT) IS DISTINCT FROM (l.ru, l.rc, l.rp))
            FROM new.plot_location p JOIN locx l USING (STATECD, UNITCD, COUNTYCD, PLOT) WHERE {scope('p')}""").fetchone()
        check_n("(n12) D28: plot_location = the most recent visit's coordinates and raw key", r[1] == 0, r[0], f"({r[1]} of {r[0]} rows differ)")
    else:
        check("(n12) D28: plot_location table present", False, "(missing)")
else:
    info("(n12) R7: no renumbered plots in scope", "(nothing to test)")
con.execute(f"""
CREATE TEMP TABLE ecox AS
WITH pk AS (SELECT DISTINCT statecd, unitcd, countycd, plot FROM new.curated_cohorts_landis WHERE {scope('', 'statecd', 'countycd')}
            UNION SELECT DISTINCT statecd, unitcd, countycd, plot FROM new.tree_trajectories WHERE {scope('', 'statecd', 'countycd')})
SELECT pk.statecd, pk.unitcd, pk.countycd, pk.plot, e.epa_l3, e.epa_l4, e.ecosubcd, e.lat, e.lon, (l.PLOT IS NOT NULL) moved
FROM pk LEFT JOIN locx l ON l.STATECD=pk.statecd AND l.UNITCD=pk.unitcd AND l.COUNTYCD=pk.countycd AND l.PLOT=pk.plot
LEFT JOIN fraw.main.data_plot_eco e ON e.statecd = pk.statecd AND e.unitcd = COALESCE(l.ru, pk.unitcd)
     AND e.countycd = COALESCE(l.rc, pk.countycd) AND e.plot = COALESCE(l.rp, pk.plot)""")
for _t, _db in (("curated_cohorts_landis", "new"), ("curated_cohorts_landis_stdorg", "new"), ("tree_trajectories", "new")) + \
        ((("curated_cohorts_landis_simB", "newsimb"),) if a.simb_db else ()):
    r = con.execute(f"""SELECT count(*), count(*) FILTER (WHERE (c.epa_l3, c.epa_l4, c.ecosubcd) IS DISTINCT FROM (x.epa_l3, x.epa_l4, x.ecosubcd)),
                               count(*) FILTER (WHERE x.moved AND (c.epa_l3, c.epa_l4, c.ecosubcd) IS DISTINCT FROM (x.epa_l3, x.epa_l4, x.ecosubcd)),
                               count(*) FILTER (WHERE (c.epa_l3, c.epa_l4) IS DISTINCT FROM (x.epa_l3, x.epa_l4)),
                               count(*) FILTER (WHERE c.ecosubcd IS DISTINCT FROM x.ecosubcd)
        FROM {_db}.{_t} c LEFT JOIN ecox x USING (statecd, unitcd, countycd, plot) WHERE {scope('c', 'statecd', 'countycd')}""").fetchone()
    check_n(f"(n12) D28 / D44: {_t}: epa_l3 / epa_l4 / ecosubcd = data_plot_eco of the plot's location key", r[1] == 0, r[0],
            f"({r[1]:,} of {r[0]:,} rows differ ({r[3]:,} in epa_l3 / l4, {r[4]:,} in ecosubcd); {r[2]} of them on renumbered plots)")
r = con.execute("""SELECT count(*), count(*) FILTER (WHERE d.statecd IS NULL OR (d.epa_l3, d.epa_l4, d.ecosubcd, d.lat, d.lon)
                                                     IS DISTINCT FROM (x.epa_l3, x.epa_l4, x.ecosubcd, x.lat, x.lon))
    FROM ecox x LEFT JOIN new.data_plot_eco d USING (statecd, unitcd, countycd, plot) WHERE x.epa_l3 IS NOT NULL""").fetchone()
check_n("(n12) D28: the exported data_plot_eco carries the location key's eco row for every product plot", r[1] == 0, r[0],
        f"({r[1]} of {r[0]:,} plots differ or are missing)")
# D44 (F7-11): the exported data_plot_eco keeps FIADB's key column types
def _keytypes(db):
    return con.execute(f"""SELECT list(column_name || ':' || data_type ORDER BY column_name) FROM duckdb_columns()
        WHERE database_name = '{db}' AND table_name = 'data_plot_eco' AND column_name IN ('statecd','unitcd','countycd','plot')""").fetchone()[0]
_tys = {db: _keytypes(db) for db in ("new", "fraw")}
check("(n12) D44 F7-11: exported data_plot_eco key types = FIADB's", _tys.get("new") == _tys.get("fraw"),
      f"(curated {_tys.get('new')}, FIADB {_tys.get('fraw')})")

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

# ---- r7 E24: slim CONTENT = the curated / Sim B tables under the builder's region rule ------------
# (build_slim_db_region.jl: stdorg / Sim B / curated_trees rows with epa_l3 in --ecos and statecd in
# --states; COND / PLOT / cohort_disturbance rows of that plot universe; REF_SPECIES /
# species_longevity_ref whole; the provenance tables copied whole by slim_guard post)
def _cmp_rows(name, sel_a, sel_b):
    r = con.execute(f"""SELECT (SELECT count(*) FROM ({sel_a})), (SELECT count(*) FROM ({sel_b})),
        (SELECT count(*) FROM (({sel_a}) EXCEPT ALL ({sel_b}))), (SELECT count(*) FROM (({sel_b}) EXCEPT ALL ({sel_a})))""").fetchone()
    check_n(name, r[2] == 0 and r[3] == 0, r[0], f"(slim {r[0]:,} rows, expected {r[1]:,}; slim-only {r[2]:,}, missing {r[3]:,})")


if a.new_slim and a.slim_ecos:
    _ecos = ",".join("'" + e.strip() + "'" for e in a.slim_ecos.split(",") if e.strip())
    _sts = ",".join(map(str, states)) if states else "SELECT DISTINCT STATECD FROM new.curated_trees"
    _filt = f"epa_l3 IN ({_ecos}) AND statecd IN ({_sts})"
    _src = {"curated_cohorts_landis_stdorg": ("new", "eco"), "curated_cohorts_landis_simB": ("newsimb", "eco"),
            "curated_trees": ("new", "eco"), "COND": ("new", "plots"), "PLOT": ("new", "plots"),
            "cohort_disturbance": ("newsimb", "plots"), "REF_SPECIES": ("new", "all"), "species_longevity_ref": ("new", "all"),
            "run_scope": ("new", "all"), "run_steps": ("new", "all"), "plot_key_remap": ("new", "all"), "plot_location": ("new", "all")}
    _parts = [f"SELECT DISTINCT statecd, unitcd, countycd, plot FROM new.curated_cohorts_landis_stdorg WHERE {_filt}"]
    if a.simb_db:
        _parts.append(f"SELECT DISTINCT statecd, unitcd, countycd, plot FROM newsimb.curated_cohorts_landis_simB WHERE {_filt}")
    con.execute("CREATE TEMP TABLE slimpl AS " + " UNION ".join(_parts))
    _slim_tabs = [r[0] for r in con.execute("SELECT table_name FROM duckdb_tables() WHERE database_name='nslim' ORDER BY 1").fetchall()]
    for _t in _slim_tabs:
        if _t not in _src:
            info(f"(n26) slim table {_t}: no source rule", "(not compared)")
            continue
        _db, _mode = _src[_t]
        if _db == "newsimb" and not a.simb_db:
            skip(f"(n26) slim {_t} content", "no --simb-db")
            continue
        if not con.execute(f"SELECT count(*) FROM duckdb_tables() WHERE database_name='{_db}' AND table_name='{_t}'").fetchone()[0]:
            check(f"(n26) E24: slim {_t} has a source table", False, f"({_db}.{_t} missing)")
            continue
        _cols = [r[0] for r in con.execute(f"SELECT column_name FROM duckdb_columns() WHERE database_name='nslim' AND table_name='{_t}' ORDER BY column_index").fetchall()]
        _scols = {r[0] for r in con.execute(f"SELECT column_name FROM duckdb_columns() WHERE database_name='{_db}' AND table_name='{_t}'").fetchall()}
        if set(_cols) - _scols:
            check(f"(n26) E24: slim {_t} columns all exist in its source", False, f"(slim-only columns {sorted(set(_cols) - _scols)})")
            _cols = [c for c in _cols if c in _scols]
        _cl = ", ".join(f't."{c}"' for c in _cols)
        if _mode == "eco":
            _exp = f"SELECT {_cl} FROM {_db}.{_t} t WHERE {_filt.replace('epa_l3', 't.epa_l3').replace('statecd IN', 't.statecd IN')}"
        elif _mode == "plots":
            _exp = (f"SELECT {_cl} FROM {_db}.{_t} t JOIN slimpl p ON t.statecd = p.statecd AND t.unitcd = p.unitcd "
                    f"AND t.countycd = p.countycd AND t.plot = p.plot")
        elif _t == "run_steps" and a.simb_db:   # slim_guard post: curated steps 1-8 + Sim B steps 9-10
            _exp = (f"SELECT {_cl} FROM new.run_steps t WHERE t.step BETWEEN 1 AND 8 UNION ALL "
                    f"SELECT {_cl} FROM newsimb.run_steps t WHERE t.step IN (9, 10)")
        else:
            _exp = f"SELECT {_cl} FROM {_db}.{_t} t"
        _cmp_rows(f"(n26) E24: slim {_t} content = its source under the region rule (EXCEPT ALL both ways)", f"SELECT {_cl} FROM nslim.{_t} t", _exp)
elif a.new_slim:
    skip("(n26) E24: slim content", "no --slim-ecos given")

# ---- r7 D40 / D45 / C F8: determinism over EVERY table ------------------------------------------
def _same_tables(lab, x, y):
    tx = {r[0] for r in con.execute(f"SELECT table_name FROM duckdb_tables() WHERE database_name='{x}'").fetchall()}
    ty = {r[0] for r in con.execute(f"SELECT table_name FROM duckdb_tables() WHERE database_name='{y}'").fetchall()}
    check(f"(e) {lab}: the same tables in both runs", tx == ty, f"(only first {sorted(tx - ty)}, only second {sorted(ty - tx)})")
    bad = []
    for t in sorted(tx & ty):
        if t == "run_steps" or t == "run_scope":   # provenance: run ids / timestamps differ by design
            continue
        cx = [r[0] for r in con.execute(f"SELECT column_name FROM duckdb_columns() WHERE database_name='{x}' AND table_name='{t}' ORDER BY column_index").fetchall()]
        cy = [r[0] for r in con.execute(f"SELECT column_name FROM duckdb_columns() WHERE database_name='{y}' AND table_name='{t}' ORDER BY column_index").fetchall()]
        if cx != cy:
            bad.append((t, "columns differ"))
            continue
        n1 = con.execute(f'SELECT count(*) FROM (SELECT * FROM {x}."{t}" EXCEPT ALL SELECT * FROM {y}."{t}")').fetchone()[0]
        n2 = con.execute(f'SELECT count(*) FROM (SELECT * FROM {y}."{t}" EXCEPT ALL SELECT * FROM {x}."{t}")').fetchone()[0]
        if n1 or n2:
            bad.append((t, n1, n2))
    check(f"(e) {lab}: every table identical (EXCEPT ALL both ways; {len(tx & ty)} tables)", not bad, f"(differ: {bad})")

if a.determinism_db:   # rerun of the cohort steps on a copy of the finished DB: EVERY table (r7 C F8)
    con.execute(f"ATTACH '{a.determinism_db}' AS rep (READ_ONLY)")
    _same_tables("curated DB rerun", "new", "rep")
else:
    skip("(e) determinism of the curated DB", "no --determinism-db")
if a.determinism_simb_db and a.simb_db:   # r5 M-3: steps 9-10 (Sim B + its disturbance drops)
    con.execute(f"ATTACH '{a.determinism_simb_db}' AS repsimb (READ_ONLY)")
    _same_tables("Sim B DB rerun", "newsimb", "repsimb")
else:
    skip("(e) determinism of the Sim B DB", "no --determinism-simb-db / --simb-db")
if a.determinism_subset:   # r7 D40 / D45: steps 1-10 on a one-county subset at two thread counts
    _ds = [x for x in a.determinism_subset.split(",") if x]
    for _k, _p in enumerate(_ds):
        con.execute(f"ATTACH '{_p}' AS dsub{_k} (READ_ONLY)")
    _same_tables("subset steps 1-10, two thread counts: curated DB", "dsub0", "dsub1")
    if len(_ds) >= 4:
        _same_tables("subset steps 1-10, two thread counts: Sim B DB", "dsub2", "dsub3")
    else:
        skip("(e) subset determinism of the Sim B DB", "only two DBs given")
else:
    skip("(e) D40: steps 1-10 determinism on a subset at two thread counts", "no --determinism-subset")

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
SELECT 'new', count(*), count(DISTINCT CN) FROM new.curated_trees WHERE {scope()} ORDER BY 1 DESC""")
show("3b. new tree-visit key uniqueness (tree x MEASDATE)", f"""
SELECT count(*) n, count(DISTINCT (STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE,MEASDATE)) n_key
FROM new.curated_trees WHERE {scope()}""")
r = con.execute(f"""SELECT count(*), count(DISTINCT CN), count(DISTINCT (STATECD,UNITCD,COUNTYCD,PLOT,SUBP,TREE,MEASDATE))
                    FROM new.curated_trees WHERE {scope()}""").fetchone()
check_n("3. curated_trees CN unique (r7 E3)", r[0] == r[1], r[0], f"({r[0]:,} rows, {r[1]:,} CNs)")
check_n("3b. curated_trees tree x MEASDATE unique (r7 E3)", r[0] == r[2], r[0], f"({r[0]:,} rows, {r[2]:,} keys)")

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
print(f"\n=== SKIPPED sections ({len(SKIPS)}): {SKIPS}")
print(f"\n=== SUMMARY: {N_PASS[0]} passed, {len(FAILS)} failed check(s): {FAILS}")
sys.exit(1 if FAILS else 0)   # r7 E1: the verifier exits non-zero on any FAIL
