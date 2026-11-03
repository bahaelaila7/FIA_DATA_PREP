"""site_links.py — the one rule tying an R4 SITETREE record to a tally record (DECISIONS.md D70 am. 1, am. 17; D74).

Candidates: a live tally tree (STATUSCD 1, SUBP <= 4, not P2A) at the same visit (plot, INVYR, annual), the same CONDID (within the visit: CONDID
is visit-scoped), the same SPCD and DIA to 0.1 in, one-to-one in both directions. SITETREE.TREE is not the tally number, so attributes link them.
Site trees repeating an earlier visit's tree, species, age, DIA and height are carried forward and left out. Placebo precision: the same match
with tally DIA shifted, `links(c, shift)`. Anchor rule (am. 17), `anchors(c)`: exactly the same HT, and SITETREE.SUBP equal to the tally SUBP or
blank (core optional: a blank is a missing field, a different value excludes the link). Used by age_anchor_audit.py, the G1 truth set
(nimiwae70_refs.py) and the D74 anchors."""
import duckdb

KV = "STATECD, UNITCD, COUNTYCD, PLOT, INVYR"
R4 = (1, 12, 13, 21, 28, 37, 45, 47)


def load(c, fiadb):
    c.execute(f"ATTACH '{fiadb}' AS f (READ_ONLY)")
    st = ",".join(map(str, R4))
    a = "(SELECT DISTINCT STATECD, INVYR FROM f.main.SURVEY WHERE ANN_INVENTORY = 'Y' AND P3_OZONE_IND = 'N')"
    c.execute(f"""CREATE TEMP TABLE st AS SELECT row_number() OVER (ORDER BY S.STATECD, S.UNITCD, S.COUNTYCD, S.PLOT, S.INVYR, S.CONDID, S.TREE, S.SPCD, S.DIA, S.HT, S.AGEDIA) scn,
          S.STATECD, S.UNITCD, S.COUNTYCD, S.PLOT, S.INVYR, S.CONDID, S.SPCD, S.SUBP, S.HT, round(S.DIA * 10)::INT d10, TRY_CAST(S.AGEDIA AS DOUBLE) agedia, S.TREE stree
        FROM f.main.SITETREE S JOIN {a} A USING (STATECD, INVYR)
        WHERE S.STATECD IN ({st}) AND TRY_CAST(S.AGEDIA AS DOUBLE) > 0
          AND NOT EXISTS (SELECT 1 FROM f.main.SITETREE E WHERE E.STATECD = S.STATECD AND E.UNITCD = S.UNITCD AND E.COUNTYCD = S.COUNTYCD AND E.PLOT = S.PLOT
                          AND E.TREE = S.TREE AND E.SPCD = S.SPCD AND E.INVYR < S.INVYR AND E.AGEDIA = S.AGEDIA AND E.DIA = S.DIA AND E.HT = S.HT)""")
    c.execute(f"""CREATE TEMP TABLE tt AS SELECT T.STATECD, T.UNITCD, T.COUNTYCD, T.PLOT, T.INVYR, T.CONDID, T.SPCD, T.SUBP, T.TREE,
          T.STATECD || '_' || T.UNITCD || '_' || T.COUNTYCD || '_' || T.PLOT || '_' || T.INVYR || '_' || T.SUBP || '_' || T.TREE tid,
          T.HT, T.CCLCD, round(T.DIA * 10)::INT d10
        FROM f.main.TREE T JOIN {a} A USING (STATECD, INVYR)
        WHERE T.STATECD IN ({st}) AND T.STATUSCD = 1 AND T.SUBP <= 4 AND T.DIA IS NOT NULL AND COALESCE(T.P2A_GRM_FLG, '') <> 'Y'""")


def links(c, shift):
    """one-to-one site tree <-> tally matches with tally DIA moved by `shift` tenths of an inch"""
    return c.execute(f"""
      WITH m AS (SELECT s.scn, t.tid, t.CCLCD cc, COALESCE(s.SUBP = t.SUBP, FALSE) subp_same, s.SUBP IS NULL ssubp_null, COALESCE(abs(s.HT - t.HT) <= 5, FALSE) ht_near, s.HT - t.HT dht
                 FROM st s JOIN tt t USING ({KV}, CONDID, SPCD) WHERE s.d10 = t.d10 + ({shift})),
      u AS (SELECT * FROM m QUALIFY count(*) OVER (PARTITION BY scn) = 1 AND count(*) OVER (PARTITION BY tid) = 1)
      SELECT * FROM u""").df()


def anchors(c):
    """am. 17 links at shift 0: one row per link, natural keys of the tally record (tally SUBP and TREE) and the site tree's AGEDIA"""
    r = links(c, 0)
    r = r[(r.dht == 0) & (r.subp_same | r.ssubp_null)]
    s = c.execute(f"SELECT scn, {KV}, agedia FROM st").df(); t = c.execute("SELECT tid, SUBP, TREE FROM tt").df()
    out = r[['scn', 'tid', 'cc', 'ssubp_null']].merge(s, on='scn').merge(t, on='tid')
    assert not out.duplicated('scn').any() and not out.duplicated('tid').any(), 'links are one-to-one'
    return out.rename(columns=str.lower)[['statecd', 'unitcd', 'countycd', 'plot', 'subp', 'tree', 'invyr', 'agedia', 'cc', 'ssubp_null']]
