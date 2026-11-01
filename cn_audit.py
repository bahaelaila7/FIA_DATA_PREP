"""cn_audit.py — can natural ids replace every CN in the curation? (DECISIONS.md D67, D68)

  python -I cn_audit.py --fiadb FIADB --out docs/CN_AUDIT.json

Read-only. A plot's visits are ordered by INVYR (D68 am. 1): INVYR is part of the natural id and orders records chronologically.
CNs appear here only as the thing being tested. Scope: R4 and all states with records."""
import json, argparse
import duckdb

K = "STATECD, UNITCD, COUNTYCD, PLOT"
R4 = "(1, 12, 13, 21, 28, 37, 45, 47)"


def plots(c, where):
    c.execute(f"""CREATE OR REPLACE TEMP TABLE ap AS
      SELECT P.CN, P.PREV_PLT_CN, P.STATECD, P.UNITCD, P.COUNTYCD, P.PLOT, P.INVYR, A.a IS NOT NULL annual
      FROM PLOT P LEFT JOIN (SELECT DISTINCT STATECD s, INVYR y, 1 a FROM SURVEY WHERE ANN_INVENTORY = 'Y' AND P3_OZONE_IND = 'N') A ON A.s = P.STATECD AND A.y = P.INVYR
      {where}""")


def previous_visit(c, flt):
    c.execute(f"""CREATE OR REPLACE TEMP TABLE q AS SELECT *, lag(CN) OVER w nat_prev, lag(annual) OVER w nat_ann
                  FROM ap WHERE {flt} WINDOW w AS (PARTITION BY {K} ORDER BY INVYR)""")
    t = c.execute("""SELECT count(*) n, count(*) FILTER (WHERE PREV_PLT_CN IS NOT NULL AND nat_prev = PREV_PLT_CN) same, count(*) FILTER (WHERE PREV_PLT_CN IS NULL AND nat_prev IS NULL) both_none,
        count(*) FILTER (WHERE PREV_PLT_CN IS NOT NULL AND nat_prev IS NOT NULL AND nat_prev <> PREV_PLT_CN) differ,
        count(*) FILTER (WHERE PREV_PLT_CN IS NULL AND nat_prev IS NOT NULL) natural_only, count(*) FILTER (WHERE PREV_PLT_CN IS NOT NULL AND nat_prev IS NULL) link_only FROM q""").df().iloc[0].to_dict()
    b = c.execute(f"""WITH d AS (SELECT q.*, p.UNITCD pu, p.COUNTYCD pc, p.PLOT pp, p.INVYR py, p.annual pann FROM q LEFT JOIN ap p ON p.CN = q.PREV_PLT_CN)
        SELECT CASE WHEN nat_prev IS NOT NULL AND PREV_PLT_CN IS NOT NULL AND nat_prev <> PREV_PLT_CN THEN 'differ' WHEN nat_prev IS NULL AND PREV_PLT_CN IS NOT NULL THEN 'link_only'
                    WHEN PREV_PLT_CN IS NULL AND nat_prev IS NOT NULL THEN 'natural_only' END kind,
               CASE WHEN py IS NULL THEN 'target not in scope' WHEN (UNITCD, COUNTYCD, PLOT) IS DISTINCT FROM (pu, pc, pp) THEN 'key changed' ELSE 'same key' END link_target,
               CASE WHEN annual AND COALESCE(nat_ann, pann) THEN 'annual-annual' WHEN annual THEN 'periodic-annual' ELSE 'periodic-periodic' END boundary, count(*) n
        FROM d GROUP BY 1, 2, 3 HAVING kind IS NOT NULL ORDER BY 1, 4 DESC""").fetchall()
    return dict(summary=t, mismatches=[list(r) for r in b])


def previous_status(c):
    c.execute(f"CREATE OR REPLACE TEMP TABLE v AS SELECT {K}, INVYR, lag(INVYR) OVER (PARTITION BY {K} ORDER BY INVYR) py FROM ap WHERE annual")
    return c.execute(f"""WITH j AS (SELECT x.PREV_STATUS_CD ps, x.RECONCILECD rc, p.STATUSCD pst FROM TREE x JOIN v USING ({K}, INVYR)
          LEFT JOIN TREE p ON p.STATECD = x.STATECD AND p.UNITCD = x.UNITCD AND p.COUNTYCD = x.COUNTYCD AND p.PLOT = x.PLOT AND p.INVYR = v.py AND p.SUBP = x.SUBP AND p.TREE = x.TREE
          WHERE v.py IS NOT NULL)
        SELECT count(*) n, count(*) FILTER (WHERE ps IS NOT NULL AND pst IS NOT NULL AND ps = pst) agree, count(*) FILTER (WHERE ps IS NOT NULL AND pst IS NOT NULL AND ps <> pst) disagree,
               count(*) FILTER (WHERE ps IS NOT NULL AND pst IS NULL) status_no_record, count(*) FILTER (WHERE ps IS NULL AND pst IS NOT NULL) record_no_status, count(*) FILTER (WHERE ps IS NULL AND pst IS NULL) new_tree FROM j""").df().iloc[0].to_dict()


def renumbered(c):
    """FIA's key-changing links, and a search from tree records alone: a plot key's first annual visit against every earlier-ending key of the state."""
    c.execute("""CREATE OR REPLACE TEMP TABLE kc AS SELECT a.STATECD, a.UNITCD, a.COUNTYCD, a.PLOT, a.INVYR, b.UNITCD pu, b.COUNTYCD pc, b.PLOT pp, b.INVYR py
                 FROM ap a JOIN ap b ON b.CN = a.PREV_PLT_CN WHERE a.annual AND b.annual AND (a.UNITCD, a.COUNTYCD, a.PLOT) IS DISTINCT FROM (b.UNITCD, b.COUNTYCD, b.PLOT)""")
    c.execute(f"CREATE OR REPLACE TEMP TABLE sp AS SELECT *, row_number() OVER w k, count(*) OVER (PARTITION BY {K}) n FROM ap WHERE annual WINDOW w AS (PARTITION BY {K} ORDER BY INVYR)")
    c.execute(f"CREATE OR REPLACE TEMP TABLE xs AS SELECT t.STATECD, t.UNITCD, t.COUNTYCD, t.PLOT, t.INVYR, t.SUBP, t.TREE, t.PREVDIA FROM TREE t JOIN sp s USING ({K}, INVYR) WHERE s.k = 1 AND t.PREVDIA IS NOT NULL AND t.PREV_STATUS_CD IS NOT NULL")
    c.execute(f"CREATE OR REPLACE TEMP TABLE xe AS SELECT t.STATECD, t.UNITCD, t.COUNTYCD, t.PLOT, t.INVYR, t.SUBP, t.TREE, t.DIA FROM TREE t JOIN sp s USING ({K}, INVYR) WHERE s.k = s.n AND t.DIA IS NOT NULL")
    c.execute("""CREATE OR REPLACE TEMP TABLE hit AS WITH m AS (SELECT s.STATECD, s.UNITCD, s.COUNTYCD, s.PLOT, s.INVYR, e.UNITCD eu, e.COUNTYCD ec, e.PLOT ep, count(*) n
           FROM xs s JOIN xe e ON e.STATECD = s.STATECD AND e.SUBP = s.SUBP AND e.TREE = s.TREE AND abs(s.PREVDIA - e.DIA) <= .05 AND e.INVYR < s.INVYR
           WHERE (e.UNITCD, e.COUNTYCD, e.PLOT) IS DISTINCT FROM (s.UNITCD, s.COUNTYCD, s.PLOT) GROUP BY ALL),
        tot AS (SELECT STATECD, UNITCD, COUNTYCD, PLOT, INVYR, count(*) nt FROM xs GROUP BY ALL)
        SELECT m.*, t.nt FROM m JOIN tot t USING (STATECD, UNITCD, COUNTYCD, PLOT, INVYR) WHERE m.n >= 2 AND m.n >= .9 * t.nt""")
    return c.execute("""SELECT (SELECT count(*) FROM kc) fia_links, (SELECT count(*) FROM hit) natural_hits,
        (SELECT count(*) FROM hit h JOIN kc k ON k.STATECD = h.STATECD AND k.UNITCD = h.UNITCD AND k.COUNTYCD = h.COUNTYCD AND k.PLOT = h.PLOT AND k.INVYR = h.INVYR AND k.pu = h.eu AND k.pc = h.ec AND k.pp = h.ep) also_fia_links""").df().iloc[0].to_dict()


def conditions(c):
    c.execute("""CREATE OR REPLACE TEMP TABLE mt AS SELECT p.STATECD, p.UNITCD, p.COUNTYCD, p.PLOT, p.INVYR, m.CONDID, m.PREVCOND, sum(m.SUBPTYP_PROP_CHNG) prop
                 FROM SUBP_COND_CHNG_MTRX m JOIN ap p ON p.CN = m.PLT_CN WHERE p.annual AND m.SUBPTYP = 1 GROUP BY ALL HAVING sum(m.SUBPTYP_PROP_CHNG) > 0""")
    c.execute(f"""CREATE OR REPLACE TEMP TABLE tl AS SELECT t.STATECD, t.UNITCD, t.COUNTYCD, t.PLOT, t.INVYR, t.CONDID, t.PREVCOND, count(*) n
                  FROM TREE t JOIN ap p USING ({K}, INVYR) WHERE p.annual AND t.PREVCOND IS NOT NULL AND t.PREV_STATUS_CD IS NOT NULL GROUP BY ALL""")
    k = f"{K}, INVYR, CONDID"
    return c.execute(f"""WITH a AS (SELECT {k}, first(PREVCOND ORDER BY prop DESC, PREVCOND) mp FROM mt GROUP BY ALL), b AS (SELECT {k}, first(PREVCOND ORDER BY n DESC, PREVCOND) tp FROM tl GROUP BY ALL)
        SELECT (SELECT count(*) FROM a) conditions_matrix, count(*) with_tree_link, count(*) FILTER (WHERE mp = tp) dominant_agree FROM a JOIN b USING ({K}, INVYR, CONDID)""").df().iloc[0].to_dict()


if __name__ == "__main__":
    ap_ = argparse.ArgumentParser(); ap_.add_argument("--fiadb", required=True); ap_.add_argument("--out", required=True); a = ap_.parse_args()
    c = duckdb.connect(a.fiadb, read_only=True); c.execute("SET threads = 6")
    R = {}
    for name, where in (("R4", f"WHERE P.STATECD IN {R4}"), ("all_states", "WHERE P.STATECD NOT IN (2, 15) AND P.STATECD <= 56")):
        plots(c, where)
        R[name] = dict(previous_visit_annual=previous_visit(c, "annual"), previous_visit_all=previous_visit(c, "TRUE"), previous_status=previous_status(c), renumbered=renumbered(c), conditions=conditions(c))
        print(name, json.dumps(R[name], default=float)[:2600], flush=True)
    json.dump(R, open(a.out, "w"), indent=1, default=float)
