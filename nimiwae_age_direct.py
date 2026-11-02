"""nimiwae_age_direct.py — the gate for the direct age model (DECISIONS.md D70 am. 12).

  python -I nimiwae_age_direct.py --data DIR --fiadb FIADB --db TRUTH --out report.json [--selftest]

Fits age from measurements on TRAIN age records only and scores on VAL. No lineage model. Three fits:
  CR       Chapman-Richards height-age per species group on site trees: H = A (1 - exp(-k a)) ^ p, inverted with H / A capped at 0.98.
  f        gradient boosting of log total age on DIA, height, crown, species, soil, climate, region.
  f+g      the same plus annual relative diameter growth where known. Only western tally records have it in training.
Total age = TOTAGE, or BHAGE / AGEDIA + l0 (2 yr softwood, 5 yr hardwood, fixed here). Width: a second booster on the squared log residual, fitted out of fold.
Scored as bars G1, G2, G3, G5 of D70 am. 4, whose thresholds do not change."""
import sys, os, json, argparse
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd, duckdb
from scipy.optimize import least_squares
from sklearn.ensemble import HistGradientBoostingRegressor as HGB

L0 = dict(S=2.0, H=5.0)
SOIL = [f'soil_{c}' for c in ('clay', 'sand', 'silt', 'soc', 'bd', 'ph', 'cec', 'depth_restriction', 'rock_frag', 'depth_bedrock')]
NUM = ['ldia', 'lht', 'tm_n', 'pp_n', 'vp_n'] + SOIL
CAT = ['spcd', 'spgrpcd', 'cc', 'west']
CC_SITE = 6


def cr_h(a, A, k, p):
    return A * (1 - np.exp(-k * a)) ** p

def cr_inv(h, A, k, p, cap=0.98):
    r = np.clip(h / A, 1e-4, cap)
    return -np.log(1 - r ** (1 / p)) / k

def fit_cr(a, h):
    """log H least squares; A from the upper heights, k and p free"""
    f = lambda t: np.log(cr_h(a, np.exp(t[0]), np.exp(t[1]), np.exp(t[2]))) - np.log(h)
    r = least_squares(f, [np.log(np.quantile(h, .95) * 1.2), np.log(0.03), np.log(1.2)], loss='soft_l1', f_scale=0.2, max_nfev=200)
    return tuple(np.exp(r.x))

def features(d):
    d = d.copy(); d['ldia'] = np.log(d.dia); d['lht'] = np.log(d.ht.clip(lower=1)); d['cc'] = d.cc.fillna(0).astype(int)
    d['west'] = d.west.astype(int); return d

def fit(X, y, cats, seed=0):
    return HGB(max_iter=400, learning_rate=0.05, max_leaf_nodes=31, min_samples_leaf=40, l2_regularization=1.0, categorical_features=cats, early_stopping=False, random_state=seed).fit(X, y)

def encode(train, others, cols):
    maps = {c: {v: i for i, v in enumerate(sorted(train[c].dropna().unique()))} for c in CAT}
    enc = lambda d: d[cols].assign(**{c: d[c].map(maps[c]).astype(float) for c in CAT})
    return [enc(train)] + [enc(o) for o in others]

def score(p, truth, lo=None, hi=None):
    e = p - truth; o = dict(n=int(len(e)), mae=float(np.abs(e).mean()), bias=float(np.median(e)))
    if lo is not None: o['cover80'] = float(((truth >= lo) & (truth <= hi)).mean())
    return o

def linked(c, val):
    st = c.execute("""WITH s AS (SELECT S.STATECD statecd, S.UNITCD unitcd, S.COUNTYCD countycd, S.PLOT plot, S.INVYR invyr, S.CONDID condid, S.SPCD spcd, S.DIA dia_s, S.HT ht_s, any_value(TRY_CAST(S.AGEDIA AS DOUBLE)) a_obs
                               FROM f.main.SITETREE S WHERE S.STATECD IN (1, 12, 13, 21, 28, 37, 45, 47) AND TRY_CAST(S.AGEDIA AS DOUBLE) > 0 GROUP BY ALL),
        t AS (SELECT T.STATECD statecd, T.UNITCD unitcd, T.COUNTYCD countycd, T.PLOT plot, T.INVYR invyr, T.CONDID condid, T.SPCD spcd, T.DIA dia_s, T.HT ht_s, T.SUBP subp, T.TREE tree
              FROM f.main.TREE T WHERE T.STATECD IN (1, 12, 13, 21, 28, 37, 45, 47) AND T.STATUSCD = 1 AND T.SUBP <= 4)
        SELECT s.*, t.subp, t.tree FROM s JOIN t USING (statecd, unitcd, countycd, plot, invyr, condid, spcd, dia_s, ht_s)
        QUALIFY count(*) OVER (PARTITION BY s.statecd, s.unitcd, s.countycd, s.plot, s.invyr, s.condid, s.spcd, s.dia_s, s.ht_s) = 1""").df()
    k = ['statecd', 'unitcd', 'countycd', 'plot', 'subp', 'tree', 'invyr']
    v = val[val.slot_kind == 'real'].sort_values(k[:-1] + ['measdate'])
    g = v.groupby(k[:-1]); v = v.assign(pdia=g.dia.shift(1), pmd=g.measdate.shift(1))
    yrs = (pd.to_datetime(v.measdate) - pd.to_datetime(v.pmd)).dt.days / 365.25
    v = v.assign(growth=(np.log(v.dia) - np.log(v.pdia)) / yrs.where(yrs > 0.5))
    return v.merge(st[k + ['a_obs']], on=k)


def main(a):
    rec = pd.read_parquet(f'{a.data}/age_rec.parquet'); rec = rec[rec.ht.notna() & (rec.ht > 0) & rec.fit_ok].copy()
    rec['west'] = rec.source != 'site'; rec['l0'] = rec.sftwd_hrdwd.map(L0).fillna(5.0)
    rec['total'] = rec.a_obs + np.where(rec.source == 'tot', 0.0, rec.l0)
    rec.loc[rec.source == 'site', 'cc'] = CC_SITE
    rec['growth'] = np.where((rec.prev_dia > 0) & (rec.remper > 0.5), (np.log(rec.dia) - np.log(rec.prev_dia.clip(lower=0.1))) / rec.remper, np.nan)
    rec = features(rec); tr = rec[(rec.split == 'train') & rec.first_of_tree]; va = rec[(rec.split == 'val') & rec.first_of_tree]
    seq = pd.read_parquet(f'{a.data}/seq.parquet'); val = seq[seq.split == 'val']; del seq
    c = duckdb.connect(); c.execute("SET threads = 4"); c.execute(f"ATTACH '{a.fiadb}' AS f (READ_ONLY)")
    ln = linked(c, val); ln['total'] = ln.a_obs + ln.sftwd_hrdwd.map(L0).fillna(5.0); ln['west'] = False; ln['cc'] = ln.cclcd
    ln = features(ln[ln.ht.notna() & (ln.ht > 0)])
    t = duckdb.connect(a.db, read_only=True); cur = t.execute("SELECT STATECD statecd, UNITCD unitcd, COUNTYCD countycd, PLOT plot, SUBP subp, TREE tree, INVYR invyr, estimated_age FROM curated_trees").df()
    ln = ln.merge(cur, on=['statecd', 'unitcd', 'countycd', 'plot', 'subp', 'tree', 'invyr'], how='inner'); ln = ln[ln.estimated_age.notna()]
    R = dict(n_train=len(tr), G1_current=score(ln.estimated_age.to_numpy(), ln.total.to_numpy()))
    # Chapman-Richards on site trees
    st = tr[tr.source == 'site']; cr = {}
    for g, d in st.groupby('spgrpcd'):
        if len(d) >= 200: cr[g] = fit_cr(d.total.to_numpy(), d.ht.to_numpy())
    allp = fit_cr(st.total.to_numpy(), st.ht.to_numpy())
    crp = lambda d: np.array([cr_inv(h, *cr.get(g, allp)) for h, g in zip(d.ht, d.spgrpcd)])
    R['CR'] = dict(groups=len(cr), site_val=score(crp(va[va.source == 'site']), va[va.source == 'site'].total.to_numpy()), G1=score(crp(ln), ln.total.to_numpy()))
    # boosted
    def run(cols, name):
        Xt, Xv, Xl = encode(tr, [va, ln], cols); cats = [cols.index(x) for x in CAT]
        yt = np.log(tr.total.to_numpy()); fold = (pd.util.hash_pandas_object(tr.plot_key, index=False).to_numpy() % 2).astype(int); oof = np.zeros(len(tr))
        for k_ in (0, 1): oof[fold == k_] = fit(Xt[fold != k_], yt[fold != k_], cats).predict(Xt[fold == k_])
        m = fit(Xt, yt, cats); s = fit(Xt, np.log((yt - oof) ** 2 + 1e-4), cats)
        def pr(X):
            # a booster on log r^2 estimates E log r^2 = log var - 1.27 for a normal residual, so var = exp(pred + 1.27)
            mu = m.predict(X); sd = np.sqrt(np.exp(s.predict(X) + 1.2704)); return np.exp(mu), np.exp(mu - 1.2816 * sd), np.exp(mu + 1.2816 * sd)
        pv, lv, hv = pr(Xv); pl_, ll, hl = pr(Xl); v = va.assign(p=pv, lo=lv, hi=hv)
        G2 = {s_: score(g.p.to_numpy(), g.total.to_numpy(), g.lo.to_numpy(), g.hi.to_numpy()) for s_, g in v.groupby('source')}
        w = v[v.west == 1]; crown = {int(c_): score(g.p.to_numpy(), g.total.to_numpy()) for c_, g in w.groupby('cc')}
        sm = w[(w.dia >= 1) & (w.dia < 5)]; R[name] = dict(G1=score(pl_, ln.total.to_numpy(), ll, hl), G2=G2, G2_crown=crown, G3=score(sm.p.to_numpy(), sm.total.to_numpy()), max_pred_linked=float(pl_.max()))
    run(NUM + CAT, 'f'); run(NUM + ['growth'] + CAT, 'f+g')
    json.dump(R, open(a.out, 'w'), indent=1, default=float); print(json.dumps(R, indent=0, default=float))


def selftest():
    rng = np.random.default_rng(0); a_ = rng.uniform(5, 120, 3000); h = cr_h(a_, 90.0, 0.03, 1.3) * np.exp(rng.normal(0, 0.03, 3000))
    A, k, p = fit_cr(a_, h); e = cr_inv(cr_h(a_, 90.0, 0.03, 1.3), A, k, p) - a_
    assert abs(A - 90) < 6 and abs(k - 0.03) < 0.008 and abs(p - 1.3) < 0.25, (A, k, p)
    assert np.abs(e[a_ < 80]).mean() < 3, np.abs(e[a_ < 80]).mean()
    assert abs(cr_inv(np.array([200.0]), 90.0, 0.03, 1.3)[0] - cr_inv(np.array([90.0 * 0.98]), 90.0, 0.03, 1.3)[0]) < 1e-9       # capped
    s = score(np.array([10., 20.]), np.array([12., 20.]), np.array([5., 25.]), np.array([15., 30.])); assert s['mae'] == 1 and s['cover80'] == 0.5
    print('selftest: 4 checks PASS')


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    for x in ('--data', '--fiadb', '--db', '--out'): ap.add_argument(x)
    ap.add_argument('--selftest', action='store_true'); a = ap.parse_args()
    selftest() if a.selftest else main(a)
