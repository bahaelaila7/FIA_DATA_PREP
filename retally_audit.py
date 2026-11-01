"""retally_audit.py — one clean re-derivation of the sapling microplot-move analysis (DECISIONS.md D65).

Independent of the session's scratch scripts: cohorts and outcomes are SQL over PLOT and TREE, matching and weights are new code.

  python -I retally_audit.py --selftest
  python -I retally_audit.py --db FIA/truth/fia_truth_r4.duckdb [--out docs/RETALLY_AUDIT.json]

A tree number is one tree only while its chain is unbroken: a record with no PREV_STATUS_CD after an earlier record of that number
is a new tree, so a reused number ends the chain and that record counts as a debut.

Identity: a tree is (STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE, chain). Saplings and debuts carry that id on one footing, a pair is true when the two ids
are equal, and nothing about the truth is encoded in an id or a row position. Ties are resolved from a hash of the pair's ids and folds from the plot's id, so
results do not depend on row order: `--shuffle SEED` reorders the inputs and must reproduce the output byte for byte."""
import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import argparse, itertools, json, zlib
import duckdb, numpy as np, pandas as pd
from scipy.optimize import linear_sum_assignment

K = ["STATECD", "UNITCD", "COUNTYCD", "PLOT"]
PK = ", ".join(K)
BINS, BLAB = [0, 2, 3, 4, 5.01], ["<2", "2-3", "3-4", "4-5"]
PRIOR = {"<2": .01, "2-3": .03, "3-4": .1, "4-5": .45}
FEAT = ["d0", "h0", "dn", "hn", "yrs", "r", "rh", "wood", "nd", "nt", "live"]
TPA_MICRO = 50.0
SHUFFLE = None


def tree_id(tree, chn):
    return tree.astype(int).astype(str) + "." + chn.astype(int).astype(str)


def tie(*ids):
    """a small cost offset fixed by the ids alone."""
    return zlib.crc32("|".join(map(str, ids)).encode()) / 2 ** 32 * 1e-6


def prepare(c):
    """visits + records with visit order and chn breaks. c holds PLOT and TREE in its search path."""
    c.execute(f"""CREATE OR REPLACE TEMP TABLE v AS
      SELECT {PK}, INVYR, MICROPLOT_LOC loc, make_date(MEASYEAR::INT, COALESCE(NULLIF(MEASMON, 0), 6)::INT, COALESCE(NULLIF(MEASDAY, 0), 15)::INT) md
      FROM PLOT""")
    c.execute(f"""CREATE OR REPLACE TEMP TABLE vv AS
      SELECT *, row_number() OVER w - 1 vn, lag(loc) OVER w ploc, max(CASE WHEN loc = 'CENTER' THEN 1 ELSE 0 END) OVER (PARTITION BY {PK}) ever_center
      FROM v WINDOW w AS (PARTITION BY {PK} ORDER BY md)""")
    c.execute(f"""CREATE OR REPLACE TEMP TABLE t AS
      SELECT r.STATECD, r.UNITCD, r.COUNTYCD, r.PLOT, r.INVYR, r.SUBP, r.TREE, r.SPCD, r.DIA, r.HT, r.STATUSCD st, r.RECONCILECD rc,
             r.PREV_STATUS_CD ps, r.TPA_UNADJ tpa, r.AGENTCD agent, vv.vn, vv.md
      FROM TREE r JOIN vv USING ({PK}, INVYR) WHERE r.SUBP <= 4""")
    # chain id: a record without PREV_STATUS_CD after an earlier record of the number starts a new tree
    c.execute(f"""CREATE OR REPLACE TEMP TABLE tc AS
      SELECT *, sum(brk) OVER (PARTITION BY {PK}, SUBP, TREE ORDER BY vn ROWS UNBOUNDED PRECEDING) chn FROM (
        SELECT *, CASE WHEN ps IS NULL AND lag(vn) OVER (PARTITION BY {PK}, SUBP, TREE ORDER BY vn) IS NOT NULL THEN 1 ELSE 0 END brk FROM t)""")


def windows(c, min_b_gap=2):
    """one A per plot per group: B = next visit, H = first visit 10-15 yr after A, at least min_b_gap visits on."""
    c.execute(f"""CREATE OR REPLACE TEMP TABLE h AS
      SELECT * FROM (SELECT a.*, b.vn vh, b.md mdh, date_diff('day', a.md, b.md) / 365.25 ey,
                            row_number() OVER (PARTITION BY a.STATECD, a.UNITCD, a.COUNTYCD, a.PLOT, a.vn ORDER BY b.vn) rk
                     FROM vv a JOIN vv b USING ({PK})
                     WHERE b.vn >= a.vn + {min_b_gap} AND date_diff('day', a.md, b.md) / 365.25 BETWEEN 10 AND 15) WHERE rk = 1""")
    c.execute(f"""CREATE OR REPLACE TEMP TABLE w AS
      SELECT h.*, n.md mdb, n.ploc nplo, n.loc nloc, (h.ploc = 'CENTER' AND h.loc = 'OFFSET') at_move, (n.ploc = 'CENTER' AND n.loc = 'OFFSET') next_move
      FROM h JOIN vv n ON n.STATECD = h.STATECD AND n.UNITCD = h.UNITCD AND n.COUNTYCD = h.COUNTYCD AND n.PLOT = h.PLOT AND n.vn = h.vn + 1""")


def group_sql(group):
    if group == "moved":
        return "SELECT * FROM w WHERE next_move"
    if group == "after":
        return "SELECT * FROM w WHERE at_move"
    if group == "normal":
        return f"SELECT * EXCLUDE (rn) FROM (SELECT *, row_number() OVER (PARTITION BY {PK} ORDER BY vn) rn FROM w WHERE ever_center = 0) WHERE rn = 1"
    raise ValueError(group)


def cohort(c, group_query):
    """one row per live microplot sapling at A with its outcome. Classes partition the cohort."""
    return c.execute(f"""
    WITH g AS ({group_query}),
    a AS (SELECT s.STATECD, s.UNITCD, s.COUNTYCD, s.PLOT, s.SUBP, s.TREE, s.chn, s.SPCD, s.DIA, s.HT, g.vn va, g.vh, g.md mda, g.mdb, g.ey
          FROM tc s JOIN g ON s.STATECD = g.STATECD AND s.UNITCD = g.UNITCD AND s.COUNTYCD = g.COUNTYCD AND s.PLOT = g.PLOT AND s.vn = g.vn
          WHERE s.st = 1 AND s.DIA < 5 AND s.tpa > {TPA_MICRO}),
    b AS (SELECT a.*, r.st bst, r.rc brc, r.ps bps, r.DIA bd, r.HT bh, r.agent bag
          FROM a LEFT JOIN tc r ON r.STATECD = a.STATECD AND r.UNITCD = a.UNITCD AND r.COUNTYCD = a.COUNTYCD AND r.PLOT = a.PLOT
               AND r.SUBP = a.SUBP AND r.TREE = a.TREE AND r.chn = a.chn AND r.vn = a.va + 1),
    later AS (SELECT a.STATECD, a.UNITCD, a.COUNTYCD, a.PLOT, a.SUBP, a.TREE, a.va,
                     max(CASE WHEN r.DIA >= 5 AND r.st IN (1, 2, 3) THEN 1 ELSE 0 END) ever5,
                     min(CASE WHEN r.DIA >= 5 AND r.st = 1 AND r.vn > a.va + 1 THEN r.vn END) vlive5,
                     max(CASE WHEN r.st IN (2, 3) AND r.vn > a.va + 1 THEN 1 ELSE 0 END) died_later,
                     max(CASE WHEN r.vn = a.vh AND r.st = 1 AND r.DIA < 5 THEN 1 ELSE 0 END) sap_at_h
              FROM a JOIN tc r ON r.STATECD = a.STATECD AND r.UNITCD = a.UNITCD AND r.COUNTYCD = a.COUNTYCD AND r.PLOT = a.PLOT
                   AND r.SUBP = a.SUBP AND r.TREE = a.TREE AND r.chn = a.chn AND r.vn > a.va AND r.vn <= a.vh GROUP BY ALL)
    SELECT b.*, COALESCE(l.ever5, 0) ever5, l.vlive5, COALESCE(l.died_later, 0) died_later, COALESCE(l.sap_at_h, 0) sap_at_h,
      CASE WHEN bst IN (2, 3) OR (bst = 0 AND brc = 8 AND bag IS NOT NULL) THEN 'dead'
           WHEN bst = 1 AND bd >= 5 AND bps IS NOT NULL THEN 'ge5'
           WHEN bst = 1 AND bd < 5 AND bps IS NOT NULL THEN 'sap'
           WHEN bst = 0 AND brc = 8 AND bd IS NOT NULL THEN 'closed_sized'
           ELSE 'lost' END cls
    FROM b LEFT JOIN later l USING (STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE, va)""").df().assign(
        bin=lambda d: pd.cut(d.DIA, BINS, right=False, labels=BLAB).astype(str), wood=lambda d: (d.SPCD < 300).astype(int))


def debuts(c, group_query):
    """new subplot-level trees after B and up to H: first record of a chn, >= 5 in, not RC 4 when dead."""
    return c.execute(f"""
    WITH g AS ({group_query}),
    f AS (SELECT {PK}, SUBP, TREE, chn, min(vn) fv FROM tc GROUP BY ALL)
    SELECT r.STATECD, r.UNITCD, r.COUNTYCD, r.PLOT, r.SUBP, r.TREE dt, r.chn dc, r.SPCD sp1, r.DIA dn, r.HT hn, r.st dst, r.vn vD, r.md mdD, g.vn va, g.vh
    FROM tc r JOIN f USING ({PK}, SUBP, TREE, chn) JOIN g USING ({PK})
    WHERE r.vn = f.fv AND r.st IN (1, 2, 3) AND r.DIA >= 5 AND (r.tpa IS NULL OR r.tpa < {TPA_MICRO})
      AND NOT (r.st <> 1 AND r.rc IS NOT DISTINCT FROM 4) AND r.vn > g.vn AND r.vn <= g.vh""").df()


def tracked_debuts(c, a, after_visit_offset):
    """the first live record >= 5 in of each hidden tree, as the debut it would be without a number."""
    if a.empty:
        return pd.DataFrame()
    c.register("_h", a[K + ["SUBP", "TREE", "chn", "va", "vh"]])
    d = c.execute(f"""SELECT * EXCLUDE (rn) FROM (
        SELECT h.STATECD, h.UNITCD, h.COUNTYCD, h.PLOT, h.SUBP, h.TREE dt, h.chn dc, r.SPCD sp1, r.DIA dn, r.HT hn, 1 dst, r.vn vD, r.md mdD, h.va, h.vh,
               row_number() OVER (PARTITION BY h.STATECD, h.UNITCD, h.COUNTYCD, h.PLOT, h.SUBP, h.TREE ORDER BY r.vn) rn
        FROM _h h JOIN tc r ON r.STATECD = h.STATECD AND r.UNITCD = h.UNITCD AND r.COUNTYCD = h.COUNTYCD AND r.PLOT = h.PLOT AND r.SUBP = h.SUBP
             AND r.TREE = h.TREE AND r.chn = h.chn
        WHERE r.st = 1 AND r.DIA >= 5 AND r.vn > h.va + {after_visit_offset} AND r.vn <= h.vh) WHERE rn = 1""").df()
    c.unregister("_h")
    return d


def one_to_one(edges):
    """edges: (i, j, cost). Maximum pairs first, then minimum cost. -> list of (i, j)."""
    if not edges:
        return []
    I = sorted({e[0] for e in edges}, key=str); J = sorted({e[1] for e in edges}, key=str); ii = {v: n for n, v in enumerate(I)}; jj = {v: n for n, v in enumerate(J)}
    big = 1 + sum(abs(e[2]) for e in edges) * 2
    C = np.full((len(I), len(J)), big * 10.0)
    for i, j, co in edges:
        C[ii[i], jj[j]] = min(C[ii[i], jj[j]], co)
    r, s = linear_sum_assignment(C)
    return [(I[a], J[b]) for a, b in zip(r, s) if C[a, b] < big * 10.0]


def brute(edges):
    best, bc = [], 0.0
    for n in range(len(edges), 0, -1):
        found = False
        for cmb in itertools.combinations(edges, n):
            if len({e[0] for e in cmb}) == n and len({e[1] for e in cmb}) == n:
                co = sum(e[2] for e in cmb)
                if not found or co < bc:
                    best, bc, found = [(e[0], e[1]) for e in cmb], co, True
        if found:
            return best, bc
    return [], 0.0


def pairs(hid, pool):
    """same subplot + same species candidates in the debut window, from size d0, height h0 at date t0."""
    on = K + ["SUBP"]
    if SHUFFLE is not None:
        hid, pool = hid.sample(frac=1, random_state=SHUFFLE), pool.sample(frac=1, random_state=SHUFFLE + 1)
    x = hid[on + ["va", "vh", "TREE", "chn", "SPCD", "d0", "h0", "t0", "v0", "binB", "wood"]].merge(pool.drop(columns=["va", "vh"]), on=on)
    x = x[(x.SPCD == x.sp1) & (x.vD > x.v0) & (x.vD <= x.vh)].copy()
    x["yrs"] = (pd.to_datetime(x.mdD) - pd.to_datetime(x.t0)).dt.days / 365.25
    x = x[x.yrs > 0].copy()
    x["r"] = (x.dn - x.d0) / x.yrs; x["rh"] = (x.hn - x.h0) / x.yrs; x["live"] = (x.dst == 1).astype(int)
    x["sap"] = tree_id(x.TREE, x.chn); x["deb"] = tree_id(x.dt, x.dc); x["true"] = x.sap == x.deb
    return x[(x.r >= 0) & (x.r <= 1.0)].reset_index(drop=True)


def links_from(x, cost):
    """one-to-one links per subplot; x with sap, deb and a cost column name."""
    out = []
    for key, e in x.groupby(K + ["SUBP"], sort=False):
        ed = [(a, b, float(co) + tie(key, a, b)) for a, b, co in zip(e.sap, e.deb, e[cost])]
        out += [(*key, a, b) for a, b in one_to_one(ed)]
    L = pd.DataFrame(out, columns=K + ["SUBP", "sap", "deb"])
    return L.assign(TREE=L.sap.str.split(".").str[0].astype(int)) if len(L) else L.assign(TREE=[])


def rule_links(x, prior=PRIOR):
    y = x[(x.r <= .6) & ((x.dst != 1) | (x.hn >= x.h0 - 5))]
    return links_from(y.assign(cost=-np.log(y.binB.map(prior).astype(float))), "cost")


def prob_links(x, col, tau):
    y = x[x[col] >= tau]
    return links_from(y.assign(cost=-y[col]), "cost")


def limits(g):
    """p99 growth by wood and size class from tracked saplings, 3-12 yr intervals."""
    g = g.assign(binB=pd.cut(g.DIA, BINS, right=False, labels=BLAB).astype(str), wood=(g.SPCD < 300).astype(int))
    return g.groupby(["wood", "binB"]).agg(r99=("rd", lambda v: v.quantile(.99)), h1=("rhh", lambda v: v.quantile(.01)), h99=("rhh", lambda v: v.quantile(.99))).reset_index()


def tight(x, Q):
    y = x.merge(Q, on=["wood", "binB"], how="left")
    y = y[(y.r <= y.r99) & ((y.dst != 1) | y.rh.between(y.h1, y.h99))].copy()
    tk, dk = K + ["SUBP", "sap"], K + ["SUBP", "deb"]
    y["nd"] = y.groupby(tk).deb.transform("nunique"); y["nt"] = y.groupby(dk).sap.transform("nunique")
    return y


def fold(df, n=5):
    return df[K].astype(str).agg("|".join, axis=1).map(lambda s: zlib.crc32(s.encode()) % n)


def fit_scores(train_sets, score_sets, n=5, feat=FEAT, col="p"):
    from sklearn.ensemble import HistGradientBoostingClassifier as HGB
    tr = pd.concat(train_sets, ignore_index=True); tr["f"] = fold(tr, n)
    ms = [HGB(max_iter=200, max_depth=4, learning_rate=.05, min_samples_leaf=40, early_stopping=False, random_state=0).fit(tr.loc[tr.f != i, feat], tr.loc[tr.f != i, "true"]) for i in range(n)]
    for s in train_sets:
        f = fold(s, n); s[col] = 0.0
        for i in range(n):
            s.loc[f == i, col] = ms[i].predict_proba(s.loc[f == i, feat])[:, 1]
    for s in score_sets:
        s[col] = np.mean([m.predict_proba(s[feat])[:, 1] for m in ms], axis=0)
    return ms


RICH = ["a_cc", "a_cr", "d_cc", "d_cr", "d_dmg", "spcd", "ba0", "ba1", "tpa1", "nb_inc", "a_org", "a_site", "d_org", "d_site", "d_fty", "d_age", "d_balive", "d_slope", "d_dst", "d_trt", "l3"]


def enrich(c, x):
    """the tree and context inputs NIMIWAE conditions on, as far as they exist at A (sapling) and at the debut visit. Hidden trees add nothing after A."""
    rec = c.execute(f"""SELECT r.STATECD, r.UNITCD, r.COUNTYCD, r.PLOT, r.SUBP, r.TREE, v.vn, r.CCLCD cc, r.CR cr, r.CONDID, r.INVYR,
        CASE WHEN r.DAMAGE_AGENT_CD1 > 0 THEN 1 WHEN r.DAMAGE_AGENT_CD1 = 0 THEN 0 END dmg FROM TREE r JOIN vv v USING ({PK}, INVYR) WHERE r.SUBP <= 4""").df()
    cd = c.execute(f"SELECT {PK}, INVYR, CONDID, STDORGCD org, SITECLCD site, FORTYPCD fty, STDAGE age, BALIVE balive, SLOPE slope, COALESCE(DSTRBCD1, 0) dst, COALESCE(TRTCD1, 0) trt FROM COND").df()
    rec = rec.merge(cd, on=K + ["INVYR", "CONDID"], how="left")
    y = x.merge(rec[K + ["SUBP", "TREE", "vn", "cc", "cr", "org", "site"]].rename(columns={"vn": "va", "cc": "a_cc", "cr": "a_cr", "org": "a_org", "site": "a_site"}), on=K + ["SUBP", "TREE", "va"], how="left")
    y["_t"] = y.dt
    y = y.merge(rec[K + ["SUBP", "TREE", "vn", "cc", "cr", "dmg", "org", "site", "fty", "age", "balive", "slope", "dst", "trt"]].rename(columns={"TREE": "_t", "vn": "vD", "cc": "d_cc", "cr": "d_cr", "dmg": "d_dmg", "org": "d_org", "site": "d_site", "fty": "d_fty", "age": "d_age", "balive": "d_balive", "slope": "d_slope", "dst": "d_dst", "trt": "d_trt"}), on=K + ["SUBP", "_t", "vD"], how="left")
    big = c.execute(f"SELECT {PK}, SUBP, TREE, chn, vn, DIA, md, tpa FROM tc WHERE st = 1 AND DIA >= 5").df()
    ba = big.assign(b=0.005454 * big.DIA ** 2 * big.tpa.fillna(0)).groupby(K + ["SUBP", "vn"]).agg(ba=("b", "sum"), tpa=("tpa", "sum")).reset_index()
    y = y.merge(ba.rename(columns={"vn": "va", "ba": "ba0"})[K + ["SUBP", "va", "ba0"]], on=K + ["SUBP", "va"], how="left")
    y = y.merge(ba.rename(columns={"vn": "vD", "ba": "ba1", "tpa": "tpa1"}), on=K + ["SUBP", "vD"], how="left")
    tri = y[K + ["SUBP", "va", "vD"]].drop_duplicates()
    m = tri.merge(big.rename(columns={"vn": "va", "DIA": "d_a", "md": "m_a"})[K + ["SUBP", "TREE", "chn", "va", "d_a", "m_a"]], on=K + ["SUBP", "va"]).merge(
        big.rename(columns={"vn": "vD", "DIA": "d_b", "md": "m_b"})[K + ["SUBP", "TREE", "chn", "vD", "d_b", "m_b"]], on=K + ["SUBP", "TREE", "chn", "vD"])
    m["inc"] = (m.d_b - m.d_a) / ((pd.to_datetime(m.m_b) - pd.to_datetime(m.m_a)).dt.days / 365.25).clip(lower=.5)
    y = y.merge(m.groupby(K + ["SUBP", "va", "vD"]).inc.mean().rename("nb_inc").reset_index(), on=K + ["SUBP", "va", "vD"], how="left")
    eco = c.execute("SELECT statecd STATECD, unitcd UNITCD, countycd COUNTYCD, plot PLOT, any_value(epa_l3) l3 FROM data_plot_eco GROUP BY ALL").df()
    y = y.merge(eco, on=K, how="left"); y["l3"] = y.l3.map(lambda v: np.nan if pd.isna(v) else float(sum(int(n) * 100 ** (2 - i) for i, n in enumerate(str(v).split(".")[:3])))); y["spcd"] = y.SPCD.astype(float)   # the code itself, not its position
    return y.drop(columns="_t").reset_index(drop=True)


def rich_report(c, T, G, X, wt, keys):
    from sklearn.metrics import average_precision_score as ap, log_loss
    R = {g: enrich(c, T[g]) for g in T}
    fit_scores([R["normal"], R["after"]], [R["moved"]], feat=FEAT + RICH, col="p2")
    # context only, no tree-specific inputs (sizes stay): how much is the tree record vs the place
    C_ = [f for f in RICH if f not in ("a_cc", "a_cr", "d_cc", "d_cr", "d_dmg")]
    fit_scores([R["normal"], R["after"]], [R["moved"]], feat=FEAT + C_, col="p3")
    fit_scores([R["normal"], R["after"]], [R["moved"]], feat=FEAT + ["a_cc", "a_cr", "d_cc", "d_cr", "d_dmg"], col="p4")
    o = {}
    for g in ("normal", "after"):
        r = R[g]; o[g] = {}
        for nm, col in (("base inputs", "p"), ("+ NIMIWAE tree and context", "p2"), ("+ context only", "p3"), ("+ tree only", "p4")):
            e = {"avg_precision": round(ap(r.true, r[col]), 4), "log_loss": round(log_loss(r.true, r[col].clip(1e-6, 1 - 1e-6)), 4)}
            for tau in (.5, .7):
                A = accuracy(prob_links(r, col, tau), X[g]["n_live"]); e[f"p>={tau}"] = (A["links"], round(A["precision"], 3), round(A["recall"], 3))
            z = G[g].merge(soft_weights(r.assign(p=r[col])), on=K + ["SUBP", "TREE"], how="left").fillna({"psoft": 0})
            z["G_p"] = (z.cls == 'ge5').astype(float) + z.psoft.clip(upper=1); e["graduated"] = round(standard(z, "G_p", wt, keys), 1); o[g][nm] = e
    print(json.dumps(o, indent=0))
    z = G["moved"].merge(soft_weights(R["moved"].assign(p=R["moved"].p2)), on=K + ["SUBP", "TREE"], how="left").fillna({"psoft": 0}); z["G_p"] = (z.cls == 'ge5').astype(float) + z.psoft.clip(upper=1)
    print("moved graduated, rich model", round(standard(z, "G_p", wt, keys), 1), "null shares", {f: round(R["moved"][f].isna().mean(), 2) for f in RICH})
    print("calibration rich, after", R["after"].assign(b=pd.cut(R["after"].p2, [0, .1, .25, .5, .75, 1])).groupby("b", observed=True).agg(p=("p2", "mean"), t=("true", "mean"), n=("p2", "size")).round(3).values.tolist())
    return o


def soft_weights(x):
    tk, dk = K + ["SUBP", "TREE"], K + ["SUBP", "deb"]
    w = x.p / x.groupby(dk).p.transform("sum").clip(lower=1)
    w = w / w.groupby([x[c_] for c_ in tk]).transform("sum").clip(lower=1)
    return x.assign(w=w).groupby(tk).w.sum().rename("psoft").reset_index()


def standard(a, col, wt, keys):
    z = a.groupby(keys, observed=True)[col].mean().reset_index().merge(wt, on=keys)
    z = z[z[col].notna()]
    return float((z[col] * z.wt).sum() / z.wt.sum() * 100)


def accuracy(links, truth_n):
    right = int((links.sap == links.deb).sum())
    return dict(links=len(links), right=right, precision=right / max(len(links), 1), recall=right / max(truth_n, 1))


def selftest():
    c = duckdb.connect()
    c.execute("CREATE TABLE PLOT (STATECD INT, UNITCD INT, COUNTYCD INT, PLOT INT, INVYR INT, MICROPLOT_LOC VARCHAR, MEASYEAR INT, MEASMON INT, MEASDAY INT)")
    c.execute("CREATE TABLE TREE (STATECD INT, UNITCD INT, COUNTYCD INT, PLOT INT, INVYR INT, SUBP INT, TREE INT, SPCD INT, DIA DOUBLE, HT DOUBLE, STATUSCD INT, RECONCILECD INT, PREV_STATUS_CD INT, TPA_UNADJ DOUBLE, AGENTCD INT)")
    for p_, y, l in [(1, 2005, 'OFFSET'), (1, 2010, 'OFFSET'), (1, 2017, 'OFFSET'), (2, 2000, 'CENTER'), (2, 2005, 'OFFSET'), (2, 2012, 'OFFSET')]:
        c.execute("INSERT INTO PLOT VALUES (1,1,1,?,?,?,?,6,15)", [p_, y, l, y])
    T = [  # plot 1, normal: ge5 at B / dead at B / graduates later / stays a sapling / no record / number reused after death
        (1, 2005, 1, 1, 316, 4.8, 30, 1, None, None, 75, None), (1, 2010, 1, 1, 316, 5.5, 35, 1, None, 1, 6, None),
        (1, 2005, 1, 2, 316, 2.0, 20, 1, None, None, 75, None), (1, 2010, 1, 2, 316, 2.1, 20, 2, None, 1, None, 60),
        (1, 2005, 1, 3, 316, 4.0, 30, 1, None, None, 75, None), (1, 2010, 1, 3, 316, 4.6, 33, 1, None, 1, 75, None), (1, 2017, 1, 3, 316, 5.4, 37, 1, None, 1, 6, None),
        (1, 2005, 1, 4, 316, 1.0, 12, 1, None, None, 75, None), (1, 2010, 1, 4, 316, 1.2, 13, 1, None, 1, 75, None), (1, 2017, 1, 4, 316, 1.4, 14, 1, None, 1, 75, None),
        (1, 2005, 1, 5, 316, 3.0, 25, 1, None, None, 75, None),
        (1, 2005, 1, 6, 316, 3.0, 25, 1, None, None, 75, None), (1, 2010, 1, 6, 316, 3.1, 25, 2, None, 1, None, 60), (1, 2017, 1, 6, 316, 6.0, 40, 1, 1, None, 6, None),
        # plot 2, moved: sized closure / agent closure / neither / continues >= 5 / no record / new list sapling / debut in reach of tree 1
        (2, 2000, 2, 1, 316, 4.4, 30, 1, None, None, 75, None), (2, 2005, 2, 1, 316, 4.6, 32, 0, 8, 1, 75, None),
        (2, 2000, 2, 2, 316, 2.0, 20, 1, None, None, 75, None), (2, 2005, 2, 2, 316, None, None, 0, 8, 1, None, 80),
        (2, 2000, 2, 3, 316, 2.0, 20, 1, None, None, 75, None), (2, 2005, 2, 3, 316, None, None, 0, 8, 1, None, None),
        (2, 2000, 2, 4, 316, 4.8, 30, 1, None, None, 75, None), (2, 2005, 2, 4, 316, 5.2, 34, 1, None, 1, 6, None),
        (2, 2000, 2, 5, 316, 2.0, 20, 1, None, None, 75, None),
        (2, 2005, 2, 6, 694, 1.0, 14, 1, 3, None, 75, None),
        (2, 2012, 2, 9, 316, 5.3, 38, 1, 1, None, 6, None)]
    for r in T:
        c.execute("INSERT INTO TREE VALUES (1,1,1,?,?,?,?,?,?,?,?,?,?,?,?)", list(r))
    prepare(c); windows(c)
    m = cohort(c, group_sql("moved")).set_index("TREE").cls.to_dict()
    assert m == {1: 'closed_sized', 2: 'dead', 3: 'lost', 4: 'ge5', 5: 'lost'}, m
    n = cohort(c, group_sql("normal")).set_index("TREE")
    assert n.cls.to_dict() == {1: 'ge5', 2: 'dead', 3: 'sap', 4: 'sap', 5: 'lost', 6: 'dead'}, n.cls.to_dict()
    assert n.ever5.to_dict() == {1: 1, 2: 0, 3: 1, 4: 0, 5: 0, 6: 0}, n.ever5.to_dict()    # tree 6's number is reused by a new tree: not a graduate
    d = debuts(c, group_sql("normal")); assert d[["dt", "dc"]].values.tolist() == [[6, 1]], d    # the reused number is a new tree, chain 1
    d = debuts(c, group_sql("moved")); assert d[["dt", "dc"]].values.tolist() == [[9, 0]], d
    h = cohort(c, group_sql("moved")); h = h[h.cls == 'closed_sized'].assign(d0=lambda z: z.bd, h0=lambda z: z.bh, t0=lambda z: z.mdb, v0=lambda z: z.va + 1, binB=lambda z: pd.cut(z.bd, BINS, right=False, labels=BLAB).astype(str))
    x = pairs(h, debuts(c, group_sql("moved"))); assert len(x) == 1 and x.deb[0] == "9.0" and not x.true[0], x
    assert len(rule_links(x)) == 1
    # a hidden tree's own later record is its true debut, by id; shuffling rows changes nothing
    h2 = cohort(c, group_sql("normal")); h2 = h2[h2.cls == 'sap'].assign(d0=lambda z: z.bd, h0=lambda z: z.bh, t0=lambda z: z.mdb, v0=lambda z: z.va + 1, binB=lambda z: pd.cut(z.bd, BINS, right=False, labels=BLAB).astype(str))
    tr = tracked_debuts(c, h2, 1); y = pairs(h2, pd.concat([debuts(c, group_sql("normal")), tr], ignore_index=True))
    assert y[y.true].sap.tolist() == ["3.0"], y[["sap", "deb", "true"]]
    k_ = lambda L: sorted(map(tuple, L[["sap", "deb"]].values.tolist()))
    assert k_(rule_links(y)) == k_(rule_links(y.sample(frac=1, random_state=3))), "row order changed the links"
    rng = np.random.default_rng(0)
    for _ in range(400):
        e = list({(int(a), int(b)): float(c_) for a, b, c_ in zip(rng.integers(0, 4, 7), rng.integers(0, 4, 7), rng.random(7))}.items())
        e = [(a, b, c_) for (a, b), c_ in e]
        got = one_to_one(e); want, wc = brute(e); co = {(a, b): c_ for a, b, c_ in e}
        assert len(got) == len(want) and abs(sum(co[p] for p in got) - wc) < 1e-9, (e, got, want)
    print("selftest: 10 fixture checks and 400 matching cases PASS")


def facts(c):
    f = {}
    f["moves"] = c.execute("SELECT count(*) FROM vv WHERE ploc = 'CENTER' AND loc = 'OFFSET'").fetchone()[0]
    f["reversals_offset_to_center"] = c.execute("SELECT count(*) FROM vv WHERE ploc = 'OFFSET' AND loc = 'CENTER'").fetchone()[0]
    f["center_visits_by_manual"] = c.execute("SELECT MANUAL, count(*) FROM PLOT WHERE MICROPLOT_LOC = 'CENTER' GROUP BY 1 ORDER BY 1").fetchall()
    f["manual1_not_center"] = c.execute("SELECT count(*) FROM PLOT WHERE MANUAL = 1.0 AND MICROPLOT_LOC IS DISTINCT FROM 'CENTER'").fetchone()[0]
    f["reused_numbers_in_chain"] = c.execute("SELECT sum(brk) FROM tc").fetchone()[0]
    c.execute(f"""CREATE OR REPLACE TEMP TABLE o AS
      SELECT a.STATECD, a.UNITCD, a.COUNTYCD, a.PLOT, a.SUBP, a.TREE, m.vn vb, a.DIA, r.st, r.rc, r.ps, r.DIA bd, r.agent
      FROM vv m JOIN tc a ON a.STATECD = m.STATECD AND a.UNITCD = m.UNITCD AND a.COUNTYCD = m.COUNTYCD AND a.PLOT = m.PLOT AND a.vn = m.vn - 1
      LEFT JOIN tc r ON r.STATECD = a.STATECD AND r.UNITCD = a.UNITCD AND r.COUNTYCD = a.COUNTYCD AND r.PLOT = a.PLOT AND r.SUBP = a.SUBP AND r.TREE = a.TREE AND r.chn = a.chn AND r.vn = m.vn
      WHERE m.ploc = 'CENTER' AND m.loc = 'OFFSET' AND a.st = 1 AND a.DIA < 5 AND a.tpa > {TPA_MICRO}""")
    f["old_list"] = c.execute("""SELECT count(*) n, count(*) FILTER (WHERE st = 0 AND rc = 8 AND bd IS NOT NULL) closed_sized, count(*) FILTER (WHERE st = 0 AND rc = 8 AND bd IS NULL AND agent IS NOT NULL) closed_agent,
        count(*) FILTER (WHERE st = 0 AND rc = 8 AND bd IS NULL AND agent IS NULL) closed_neither, count(*) FILTER (WHERE st = 0 AND rc IS DISTINCT FROM 8) closed_other,
        count(*) FILTER (WHERE st IS NULL) no_record, count(*) FILTER (WHERE st = 1 AND bd >= 5) live_ge5, count(*) FILTER (WHERE st = 1 AND bd < 5) live_sap, count(*) FILTER (WHERE st IN (2, 3)) dead,
        count(*) FILTER (WHERE st = 0 AND rc = 8 AND bd >= 5) closed_ge5 FROM o""").df().iloc[0].to_dict()
    f["agents"] = c.execute("SELECT agent, count(*) FROM o WHERE st = 0 AND rc = 8 AND bd IS NULL AND agent IS NOT NULL GROUP BY 1 ORDER BY 2 DESC").fetchall()
    return f


def one_interval(c):
    """share >= 5 in one visit later, tracked by number, 3-9 yr intervals; any status and alive only."""
    return c.execute(f"""
    WITH s AS (SELECT a.DIA, a.SPCD, b.st, b.DIA nd, b.ps, p.ever_center, n.ploc nplo, n.loc nloc, p.loc aloc, date_diff('day', a.md, n.md) / 365.25 y
      FROM tc a JOIN vv p ON p.STATECD = a.STATECD AND p.UNITCD = a.UNITCD AND p.COUNTYCD = a.COUNTYCD AND p.PLOT = a.PLOT AND p.vn = a.vn
      JOIN vv n ON n.STATECD = a.STATECD AND n.UNITCD = a.UNITCD AND n.COUNTYCD = a.COUNTYCD AND n.PLOT = a.PLOT AND n.vn = a.vn + 1
      LEFT JOIN tc b ON b.STATECD = a.STATECD AND b.UNITCD = a.UNITCD AND b.COUNTYCD = a.COUNTYCD AND b.PLOT = a.PLOT AND b.SUBP = a.SUBP AND b.TREE = a.TREE AND b.chn = a.chn AND b.vn = a.vn + 1
      WHERE a.st = 1 AND a.DIA < 5 AND a.tpa > {TPA_MICRO})
    SELECT CASE WHEN ever_center = 0 THEN 'normal' WHEN aloc = 'CENTER' AND nloc = 'OFFSET' THEN 'CENTER across the move' WHEN aloc = 'OFFSET' THEN 'OFFSET, ever-CENTER plots' END grp,
           CASE WHEN DIA < 2 THEN '<2' WHEN DIA < 3 THEN '2-3' WHEN DIA < 4 THEN '3-4' WHEN DIA < 4.5 THEN '4-4.5' ELSE '4.5+' END sz, count(*) n,
           round(avg((ps IS NOT NULL AND st IN (1, 2, 3) AND nd >= 5)::INT), 3) any_status, round(avg((ps IS NOT NULL AND st = 1 AND nd >= 5)::INT), 3) alive
    FROM s WHERE y BETWEEN 3 AND 9 GROUP BY 1, 2 HAVING grp IS NOT NULL ORDER BY 1, 2""").df()


def no_candidate(c):
    """the below-5-in signal, with truth: every sapling alive at B on OFFSET ground, numbers hidden, debuts through B + k."""
    ROWS = []
    base = c.execute(f"""SELECT s.STATECD, s.UNITCD, s.COUNTYCD, s.PLOT, s.SUBP, s.TREE, s.chn, s.SPCD, s.DIA d0, s.HT h0, s.md t0, s.vn v0, s.vn va, m.vm
       FROM tc s JOIN (SELECT {PK}, min(vn) FILTER (WHERE loc = 'OFFSET') vb, max(vn) vm FROM vv GROUP BY ALL) m USING ({PK})
       WHERE s.vn = m.vb AND m.vm > m.vb AND s.st = 1 AND s.DIA < 5 AND s.tpa > {TPA_MICRO}""").df()
    base["binB"] = pd.cut(base.d0, [0, 2, 3, 3.5, 4, 4.5, 5.01], right=False, labels=["<2", "2-3", "3-3.5", "3.5-4", "4-4.5", "4.5+"]).astype(str)
    base["wood"] = (base.SPCD < 300).astype(int)
    prior = {"<2": .01, "2-3": .03, "3-3.5": .06, "3.5-4": .14, "4-4.5": .28, "4.5+": .65}
    c.execute(f"CREATE OR REPLACE TEMP TABLE f AS SELECT {PK}, SUBP, TREE, chn, min(vn) fv FROM tc GROUP BY ALL")
    all_d = c.execute(f"""SELECT r.STATECD, r.UNITCD, r.COUNTYCD, r.PLOT, r.SUBP, r.TREE dt, r.chn dc, r.SPCD sp1, r.DIA dn, r.HT hn, r.st dst, r.vn vD, r.md mdD
       FROM tc r JOIN f USING ({PK}, SUBP, TREE, chn) WHERE r.vn = f.fv AND r.st IN (1, 2, 3) AND r.DIA >= 5 AND (r.tpa IS NULL OR r.tpa < {TPA_MICRO}) AND NOT (r.st <> 1 AND r.rc IS NOT DISTINCT FROM 4)""").df()
    h_ = base.assign(vh=lambda d: d.vm); tr = tracked_debuts(c, h_, 0)
    for k in (1, 2, 3):
        hid = base[base.vm >= base.v0 + k].assign(vh=lambda d: d.v0 + k)
        pool = pd.concat([all_d.assign(va=0, vh=0), tr.assign(va=0, vh=0)], ignore_index=True)
        m = rule_links(pairs(hid, pool), prior).assign(lk=1)
        z = hid.merge(m[K + ["SUBP", "TREE", "lk"]], on=K + ["SUBP", "TREE"], how="left").merge(tr.rename(columns={"dt": "TREE"})[K + ["SUBP", "TREE", "vD"]], on=K + ["SUBP", "TREE"], how="left")
        z["live_grad"] = z.vD.notna() & (z.vD <= z.v0 + k); none = z[z.lk.isna()]
        t_ = none.groupby("binB").agg(none=("live_grad", "size"), hidden=("live_grad", "mean")).round(3); t_["k"] = k
        ROWS.append(t_.reset_index())
    return pd.concat(ROWS)


def coverage(c):
    return c.execute(f"""WITH m AS (SELECT DISTINCT n.STATECD, n.UNITCD, n.COUNTYCD, n.PLOT, n.md, n.ploc = 'CENTER' AND n.loc = 'OFFSET' mv
         FROM vv n JOIN tc s ON s.STATECD = n.STATECD AND s.UNITCD = n.UNITCD AND s.COUNTYCD = n.COUNTYCD AND s.PLOT = n.PLOT AND s.vn = n.vn - 1
         WHERE s.st = 1 AND s.DIA < 5 AND s.tpa > {TPA_MICRO}),
       b AS (SELECT STATECD, UNITCD, COUNTYCD, PLOT, MEASDATE md, break_kind FROM procedural_breaks)
       SELECT count(*) FILTER (WHERE m.mv AND b.md IS NOT NULL) move_and_break, count(*) FILTER (WHERE m.mv AND b.md IS NULL) move_not_break,
              count(*) FILTER (WHERE NOT m.mv AND b.md IS NOT NULL) break_not_move, (SELECT count(*) FROM b) all_breaks
       FROM m LEFT JOIN b USING ({PK}, md)""").df().iloc[0].to_dict()


def run(db, out):
    c = duckdb.connect(db, read_only=True); c.execute("SET threads = 6")
    prepare(c); windows(c); F = facts(c); print(json.dumps(F, default=float, indent=0))
    F["coverage"] = coverage(c); print(F["coverage"])
    OI = one_interval(c); print(OI.to_string()); NC = no_candidate(c); print(NC.pivot(index="binB", columns="k", values=["hidden", "none"]).to_string())
    G = {g: cohort(c, group_sql(g)) for g in ("normal", "after", "moved")}
    print({g: dict(n=len(a), plots=a[K].drop_duplicates().shape[0], ey=round(a.ey.mean(), 1), by=a.cls.value_counts().to_dict()) for g, a in G.items()})
    g_ = c.execute(f"""WITH s AS (SELECT a.SPCD, a.DIA, a.HT, b.DIA d2, b.HT h2, date_diff('day', a.md, b.md) / 365.25 y
        FROM tc a JOIN tc b ON b.STATECD = a.STATECD AND b.UNITCD = a.UNITCD AND b.COUNTYCD = a.COUNTYCD AND b.PLOT = a.PLOT AND b.SUBP = a.SUBP AND b.TREE = a.TREE AND b.chn = a.chn AND b.vn = a.vn + 1
        JOIN vv p ON p.STATECD = a.STATECD AND p.UNITCD = a.UNITCD AND p.COUNTYCD = a.COUNTYCD AND p.PLOT = a.PLOT AND p.vn = a.vn
        WHERE a.st = 1 AND b.st = 1 AND a.DIA < 5 AND a.tpa > {TPA_MICRO} AND p.ever_center = 0)
        SELECT SPCD, DIA, (d2 - DIA) / y rd, (h2 - HT) / y rhh FROM s WHERE y BETWEEN 3 AND 12""").df()
    Q = limits(g_)
    R, X = {}, {}
    for g, a in G.items():
        q = group_sql(g); pool = debuts(c, q)
        hid = a[a.cls == 'closed_sized'] if g == "moved" else a[a.cls == 'sap']
        hid = hid.assign(d0=hid.bd, h0=hid.bh, t0=hid.mdb, v0=hid.va + 1, binB=pd.cut(hid.bd, BINS, right=False, labels=BLAB).astype(str))
        tr = tracked_debuts(c, hid, 1) if g != "moved" else pd.DataFrame()
        if len(tr):
            pool = pd.concat([pool, tr], ignore_index=True)
        x = pairs(hid, pool); X[g] = dict(x=x, n_live=len(tr), hid=hid); print(g, "pool", len(pool), "hidden", len(hid), "pairs", len(x), "live graduates hidden", len(tr))
    T = {g: tight(v["x"], Q) for g, v in X.items()}
    fit_scores([T["normal"], T["after"]], [T["moved"]])
    keys = ["STATECD", "wood", "bin"]; wt = G["moved"].groupby(keys, observed=True).size().rename("wt").reset_index()
    RR = rich_report(c, T, G, X, wt, keys)
    rows, acc = {}, {}
    for g, a in G.items():
        lk = rule_links(X[g]["x"]).assign(lk=1); s = soft_weights(T[g])
        z = a.merge(lk[K + ["SUBP", "TREE", "lk"]], on=K + ["SUBP", "TREE"], how="left").merge(s, on=K + ["SUBP", "TREE"], how="left").fillna({"lk": 0, "psoft": 0})
        z["dead"] = z.cls == 'dead'; z["lost"] = z.cls == 'lost'; z["g5"] = z.cls == 'ge5'
        z["G_rel"] = (z.g5 | (z.lk > 0)).astype(float); z["G_p"] = z.g5.astype(float) + z.psoft.clip(upper=1)
        z["G_true"] = (z.g5 | ((z.cls == 'sap') & (z.ever5 == 1))).astype(float)
        z["U_true"] = (z.cls == 'sap') & (z.ever5 == 0); z["U_sap"] = z.U_true & (z.sap_at_h == 1); z["U_dead"] = z.U_true & (z.died_later == 1)
        r = {k: round(standard(z, k, wt, keys), 1) for k in ("dead", "lost", "G_true", "G_rel", "G_p", "U_true", "U_sap", "U_dead")}
        r["notG_rel"] = round(100 - r["dead"] - r["lost"] - r["G_rel"], 1); r["notG_p"] = round(100 - r["dead"] - r["lost"] - r["G_p"], 1)
        r["by_size"] = {b: {k: round(z.loc[z.bin == b, k].mean() * 100, 1) for k in ("G_true", "G_rel", "G_p")} for b in BLAB}
        rows[g] = r
        if g != "moved":
            acc[g] = {"relinked": accuracy(rule_links(X[g]["x"]), X[g]["n_live"])}
            for tau in (.3, .5, .7):
                acc[g][f"p>={tau}"] = accuracy(prob_links(T[g], "p", tau), X[g]["n_live"])
            acc[g]["calibration"] = T[g].assign(b=pd.cut(T[g].p, [0, .1, .25, .5, .75, 1])).groupby("b", observed=True).agg(p=("p", "mean"), t=("true", "mean"), n=("p", "size")).round(3).values.tolist()
    print(json.dumps(rows, indent=0)); print(json.dumps(acc, indent=0))
    # silent moves: nothing keeps its number, hide everything from A
    sil = c.execute(f"SELECT {PK}, MEASDATE FROM procedural_breaks WHERE break_kind = 'unrecorded'").df()
    c.register("_s", sil); windows(c, 1)
    sq = f"SELECT w.* FROM w JOIN _s s ON s.STATECD = w.STATECD AND s.UNITCD = w.UNITCD AND s.COUNTYCD = w.COUNTYCD AND s.PLOT = w.PLOT AND s.MEASDATE = w.mdb"
    S = {"silent": cohort(c, sq), "normal": cohort(c, group_sql("normal")), "after": cohort(c, group_sql("after"))}
    SX = {}
    for g, a in S.items():
        hid = a[a.bst.isna()] if g == "silent" else a
        hid = hid.assign(d0=hid.DIA, h0=hid.HT, t0=hid.mda, v0=hid.va, binB=hid.bin)
        pool = debuts(c, sq if g == "silent" else group_sql(g))
        tr = tracked_debuts(c, hid, 0) if g != "silent" else pd.DataFrame()
        if len(tr):
            pool = pd.concat([pool, tr], ignore_index=True)
        SX[g] = pairs(hid, pool)
    ST = {g: tight(v, Q) for g, v in SX.items()}; fit_scores([ST["normal"], ST["after"]], [ST["silent"]])
    wk = ["wood", "bin"]; ws = S["silent"].groupby(wk, observed=True).size().rename("wt").reset_index(); sr = {}
    for g, a in S.items():
        z = a.merge(rule_links(SX[g]).assign(lk=1)[K + ["SUBP", "TREE", "lk"]], on=K + ["SUBP", "TREE"], how="left").merge(soft_weights(ST[g]), on=K + ["SUBP", "TREE"], how="left").fillna({"lk": 0, "psoft": 0})
        z["G_true"] = (z.ever5 == 1).astype(float); z["G_rel"] = (z.lk > 0).astype(float); z["G_p"] = z.psoft.clip(upper=1)
        q = z if g != "normal" else z[z.STATECD.isin([21, 47])]
        sr[g] = dict(n=len(a), G_true=round(standard(q, "G_true", ws, wk), 1), G_rel=round(standard(q, "G_rel", ws, wk), 1), G_p=round(standard(q, "G_p", ws, wk), 1))
    print(json.dumps(sr, indent=0), {"silent_no_record": int(S["silent"].bst.isna().sum()), "states": S["silent"].STATECD.value_counts().to_dict(), "A_to_B": round(float(((pd.to_datetime(S["silent"].mdb) - pd.to_datetime(S["silent"].mda)).dt.days / 365.25).mean()), 1)})
    if out:
        json.dump(dict(facts=F, table=rows, accuracy=acc, silent=sr, rich=RR, one_interval=OI.to_dict('records'), no_candidate=NC.to_dict('records')), open(out, "w"), indent=1, default=float)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--db"); ap.add_argument("--out"); ap.add_argument("--selftest", action="store_true"); ap.add_argument("--shuffle", type=int)
    a_ = ap.parse_args(); SHUFFLE = a_.shuffle
    selftest() if a_.selftest else run(a_.db, a_.out)
