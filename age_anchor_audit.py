"""age_anchor_audit.py — measured age anchors for the joint NIMIWAE age model (DECISIONS.md D70).

  python -I age_anchor_audit.py --fiadb FIADB --out docs/AGE_ANCHOR_AUDIT.json
  python -I age_anchor_audit.py --selftest

Read-only. Two questions.
1. Which SITETREE records can be tied to a tally record? A candidate is a live tally tree at the same visit (natural id), same CONDID and
   SPCD, same DIA to 0.1 in, one-to-one in both directions. FIADB has no key for this (SITETREE.TREE is not the tally number).
   False links are estimated by placebo: the same match after shifting tally DIA by +-0.2 to +-0.5 in. CCLCD is an independent check,
   because site trees are dominant or codominant. Ages are never used to choose or score a link.
2. What do TOTAGE and BHAGE offer by region: counts by size, crown class, and previous age."""
import argparse, json, os
import duckdb

KV = "STATECD, UNITCD, COUNTYCD, PLOT, INVYR"
R4 = (1, 12, 13, 21, 28, 37, 45, 47)
SHIFTS = (-5, -4, -3, -2, 2, 3, 4, 5)    # tenths of an inch; +-1 is left out as a near miss rather than chance
BAR = 0.95                               # D70, set before the first run: estimated precision a tier needs to be an anchor
TIERS = {"dia": "TRUE", "dia_subp": "subp_same", "dia_ht": "ht_near", "dia_subp_ht": "subp_same and ht_near", "dia_exact_ht": "dht == 0", "dia_subp_exact_ht": "subp_same and dht == 0"}   # the last two added after the first run (D70 am. 1)


def links(c, shift):
    """one-to-one site tree <-> tally matches with tally DIA moved by `shift` tenths of an inch"""
    return c.execute(f"""
      WITH m AS (SELECT s.scn, t.tid, t.CCLCD cc, COALESCE(s.SUBP = t.SUBP, FALSE) subp_same, COALESCE(abs(s.HT - t.HT) <= 5, FALSE) ht_near, s.HT - t.HT dht
                 FROM st s JOIN tt t USING ({KV}, CONDID, SPCD) WHERE s.d10 = t.d10 + ({shift})),
      u AS (SELECT * FROM m QUALIFY count(*) OVER (PARTITION BY scn) = 1 AND count(*) OVER (PARTITION BY tid) = 1)
      SELECT * FROM u""").df()


def tier_table(c):
    real = links(c, 0); plc = [links(c, s) for s in SHIFTS]; out = {}
    for name, f in TIERS.items():
        r = real.query(f) if f != "TRUE" else real
        ps = [(p.query(f) if f != "TRUE" else p) for p in plc]
        chance = sum(map(len, ps)) / len(ps)
        pool = [x for x in ps if len(x)]
        cc = lambda d: float(d.cc.isin([2, 3]).mean()) if len(d) else None
        n = len(r); prec = 1 - chance / n if n else None
        out[name] = dict(links=n, placebo_mean=round(chance, 1), precision=round(prec, 4) if prec is not None else None, passes_bar=bool(prec is not None and prec >= BAR),
                         dom_codom_real=cc(r), dom_codom_placebo=cc(__import__("pandas").concat(pool)) if pool else None,
                         median_abs_dht=float(r.dht.abs().median()) if n and r.dht.notna().any() else None,
                         exact_ht_real=float((r.dht == 0).mean()) if n else None,      # diagnostic only: not part of the D70 bar
                         exact_ht_placebo=float((__import__("pandas").concat(pool).dht == 0).mean()) if pool else None)
    return out


def load(c, fiadb):
    c.execute(f"ATTACH '{fiadb}' AS f (READ_ONLY)")
    st = ",".join(map(str, R4))
    a = "(SELECT DISTINCT STATECD, INVYR FROM f.main.SURVEY WHERE ANN_INVENTORY = 'Y' AND P3_OZONE_IND = 'N')"
    c.execute(f"""CREATE TEMP TABLE st AS SELECT row_number() OVER (ORDER BY S.STATECD, S.UNITCD, S.COUNTYCD, S.PLOT, S.INVYR, S.CONDID, S.TREE, S.SPCD, S.DIA, S.HT, S.AGEDIA) scn,
          S.STATECD, S.UNITCD, S.COUNTYCD, S.PLOT, S.INVYR, S.CONDID, S.SPCD, S.SUBP, S.HT, round(S.DIA * 10)::INT d10
        FROM f.main.SITETREE S JOIN {a} A USING (STATECD, INVYR)
        WHERE S.STATECD IN ({st}) AND TRY_CAST(S.AGEDIA AS DOUBLE) > 0
          AND NOT EXISTS (SELECT 1 FROM f.main.SITETREE E WHERE E.STATECD = S.STATECD AND E.UNITCD = S.UNITCD AND E.COUNTYCD = S.COUNTYCD AND E.PLOT = S.PLOT
                          AND E.TREE = S.TREE AND E.SPCD = S.SPCD AND E.INVYR < S.INVYR AND E.AGEDIA = S.AGEDIA AND E.DIA = S.DIA AND E.HT = S.HT)""")
    c.execute(f"""CREATE TEMP TABLE tt AS SELECT T.STATECD, T.UNITCD, T.COUNTYCD, T.PLOT, T.INVYR, T.CONDID, T.SPCD, T.SUBP, T.TREE,
          T.STATECD || '_' || T.UNITCD || '_' || T.COUNTYCD || '_' || T.PLOT || '_' || T.INVYR || '_' || T.SUBP || '_' || T.TREE tid,
          T.HT, T.CCLCD, round(T.DIA * 10)::INT d10
        FROM f.main.TREE T JOIN {a} A USING (STATECD, INVYR)
        WHERE T.STATECD IN ({st}) AND T.STATUSCD = 1 AND T.SUBP <= 4 AND T.DIA IS NOT NULL AND COALESCE(T.P2A_GRM_FLG, '') <> 'Y'""")


def coverage(c):
    """TOTAGE / BHAGE by region, annual visits. A value counts when it is above 0."""
    has = {r[0] for r in c.execute("SELECT column_name FROM duckdb_columns() WHERE database_name = 'f' AND table_name = 'TREE'").fetchall()}
    prev = [x for x in ("PREV_TOTAGE_RMRS", "PREV_BHAGE_RMRS") if x in has]
    pv = ", ".join(f"count(*) FILTER (WHERE TRY_CAST({x} AS DOUBLE) > 0 AND a > 0) {x.lower()}" for x in prev)
    q = lambda col: c.execute(f"""WITH r AS (SELECT s.RSCD, T.DIA, T.CCLCD, T.STATUSCD, TRY_CAST({col} AS DOUBLE) a, T.{prev[0]}, T.{prev[1]}
                                              FROM f.main.TREE T JOIN f.main.PLOT P ON P.CN = T.PLT_CN JOIN f.main.SURVEY s ON s.CN = P.SRV_CN
                                              WHERE s.ANN_INVENTORY = 'Y' AND s.RSCD IN (22, 26))
        SELECT RSCD, count(*) FILTER (WHERE a > 0) n, count(*) FILTER (WHERE a > 0 AND STATUSCD = 1) live, count(*) FILTER (WHERE a > 0 AND DIA < 1) lt1, count(*) FILTER (WHERE a > 0 AND DIA >= 1 AND DIA < 3) d1_3,
               count(*) FILTER (WHERE a > 0 AND DIA >= 3 AND DIA < 5) d3_5, count(*) FILTER (WHERE a > 0 AND DIA >= 5) ge5, count(*) FILTER (WHERE a > 0 AND CCLCD IN (1, 2, 3)) open_dom_cod,
               count(*) FILTER (WHERE a > 0 AND CCLCD = 4) intermediate, count(*) FILTER (WHERE a > 0 AND CCLCD = 5) overtopped, {pv} FROM r GROUP BY 1 ORDER BY 1""").df().to_dict("records")
    step = lambda col, pc: c.execute(f"""WITH r AS (SELECT s.RSCD, TRY_CAST({col} AS DOUBLE) a, TRY_CAST({pc} AS DOUBLE) pa, P.REMPER m
                                              FROM f.main.TREE T JOIN f.main.PLOT P ON P.CN = T.PLT_CN JOIN f.main.SURVEY s ON s.CN = P.SRV_CN
                                              WHERE s.ANN_INVENTORY = 'Y' AND s.RSCD IN (22, 26) AND T.STATUSCD = 1)
        SELECT RSCD, count(*) n, round(median(a - pa), 1) med_step, round(median(m), 1) med_remper, count(*) FILTER (WHERE a = pa) same,
               count(*) FILTER (WHERE abs(a - pa - m) <= 1) within1, count(*) FILTER (WHERE abs(a - pa - m) <= 3) within3, count(*) FILTER (WHERE a < pa) younger
        FROM r WHERE a > 0 AND pa > 0 AND m > 0 GROUP BY 1 ORDER BY 1""").df().to_dict("records")
    return dict(TOTAGE=q("T.TOTAGE"), BHAGE=q("T.BHAGE"), step_totage=step("T.TOTAGE", "T.PREV_TOTAGE_RMRS"), step_bhage=step("T.BHAGE", "T.PREV_BHAGE_RMRS"))


def selftest():
    c = duckdb.connect()
    c.execute("""CREATE TEMP TABLE st (scn INT, STATECD INT, UNITCD INT, COUNTYCD INT, PLOT INT, INVYR INT, CONDID INT, SPCD INT, SUBP INT, HT INT, d10 INT)""")
    c.execute("""CREATE TEMP TABLE tt (STATECD INT, UNITCD INT, COUNTYCD INT, PLOT INT, INVYR INT, CONDID INT, SPCD INT, SUBP INT, TREE INT, tid VARCHAR, HT INT, CCLCD INT, d10 INT)""")
    v = "1, 1, 1, 1, 2010, 1, 131"
    c.execute(f"INSERT INTO st VALUES (1, {v}, 1, 60, 100), (2, {v}, 2, 70, 120), (3, {v}, 3, 80, 150), (4, 1, 1, 1, 1, 2010, 2, 131, 1, 60, 100)")
    c.execute(f"""INSERT INTO tt VALUES ({v}, 1, 1, 'a', 62, 3, 100), ({v}, 2, 2, 'b', 70, 2, 120), ({v}, 3, 3, 'c', 70, 3, 120),
                  ({v}, 4, 4, 'd', 30, 5, 123)""")
    r = links(c, 0)
    ok = [set(r.scn) == {1}, r.tid.tolist() == ["a"]]                 # 2 is ambiguous, 3 has no DIA match, 4 is another condition
    s = links(c, -3)                                                  # d: 123 - 3 = 120 is the only tally at site tree 2's DIA once shifted
    ok.append(set(s.scn) == {2})
    s2 = links(c, 30)                                                 # 120 + 30 = 150: tallies b and c both match site tree 3, ambiguous
    ok.append(3 not in set(s2.scn))
    t = tier_table(c)
    ok += [t["dia"]["links"] == 1, t["dia_ht"]["links"] == 1, t["dia"]["dom_codom_real"] == 1.0]
    assert all(ok), ok
    print(f"selftest: {len(ok)} checks PASS")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--fiadb"); ap.add_argument("--out"); ap.add_argument("--selftest", action="store_true"); a = ap.parse_args()
    if a.selftest:
        selftest(); raise SystemExit
    c = duckdb.connect(); c.execute("SET threads = 3"); load(c, a.fiadb)
    R = dict(n_site_first_core=c.execute("SELECT count(*) FROM st").fetchone()[0], bar=BAR, tiers=tier_table(c), coverage=coverage(c))
    json.dump(R, open(a.out, "w"), indent=1, default=float); print(json.dumps(R, indent=0, default=float)[:3500])
