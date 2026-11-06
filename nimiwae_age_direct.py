"""nimiwae_age_direct.py — the gate for the direct age model (DECISIONS.md D70 am. 12).

  python -I nimiwae_age_direct.py --data DIR --fiadb FIADB --db TRUTH --out report.json [--selftest]

Fits age from measurements on TRAIN age records only and scores on VAL. No lineage model. Three fits:
  CR       Chapman-Richards height-age per species group on site trees: H = A (1 - exp(-k a)) ^ p, inverted with H / A capped at 0.98.
  f        gradient boosting of log total age on DIA, height, crown, species, soil, climate, region.
  f+p      the same plus stand origin (STDORGCD 1 = planted), a field observation.
Total age = TOTAGE, or BHAGE / AGEDIA + l0: from DIR/l0_table.parquet when present (nimiwae_l0.py, D78), else 2 yr softwood, 5 yr hardwood.
Width: a second booster on the squared log residual, fitted out of fold.
D78: --fld 1 adds planted field ages as targets (TRAIN plots, planted conditions); --targets-default trains on the 2 / 5 targets while scoring on the
table's truth (the P2 baseline); --compare BASE.json evaluates the D78 bars P1 / P2 of --out against BASE.json.
Scored as bars G1, G2, G3, G5 of D70 am. 4, whose thresholds do not change."""
import sys, os, json, argparse
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import zlib
import numpy as np, pandas as pd, duckdb
import nimiwae_l0 as L0T, nimiwae_stand as ST
from scipy.optimize import least_squares
from sklearn.ensemble import HistGradientBoostingRegressor as HGB

L0 = L0T.DEFAULT
FLD_PER_VISIT = 5                        # D78: at most this many field-age targets per plot-visit (they share one stand age)
K7 = ['statecd', 'unitcd', 'countycd', 'plot', 'subp', 'tree', 'invyr']
P1_BIAS, P1_COVER, P1_MIN_N = 1.0, (0.70, 0.90), 200
P2_G1, P2_G3, P2_G2 = 0.10, 0.25, 0.10
# D79 (user, 2026-11-06): the planted 15+ in class is accepted as a known exception at its measured values; a refit FAILS if it drifts past them by more
# than KNOWN_DRIFT (bias, yr) / KNOWN_DRIFT_COVER (coverage above the 0.90 cap), and gets an INFO line if it starts passing
KNOWN_P1 = {'[15, 99)': dict(bias=-1.0293, cover80=0.9075, flag='nimiwae70.KNOWN_BIAS_DIA: B[age_known_bias]')}
KNOWN_DRIFT, KNOWN_DRIFT_COVER = 0.25, 0.02
S1_OLD, OLD_AGE = 0.25, 60.0            # D80: old-tree G1 (truth >= OLD_AGE) must improve by at least S1_OLD yr
S2_BIAS, LEGACY = 0.25, 1.5             # D80: |G1 bias| may grow by at most S2_BIAS; legacy = old and >= LEGACY x the plot's other linked trees (reported)
SP_GROUPS = {'pines': (110, 111, 121, 131, 132), 'yellow-poplar': (621,), 'sweetgum': (611,), 'white oak': (802,)}
SOIL = [f'soil_{c}' for c in ('clay', 'sand', 'silt', 'soc', 'bd', 'ph', 'cec', 'depth_restriction', 'rock_frag', 'depth_bedrock')]
NUM = ['ldia', 'lht', 'tm_n', 'pp_n', 'vp_n'] + SOIL
CAT = ['spcd', 'spgrpcd', 'cc', 'west']
CC_SITE = 6
ORIGIN = lambda d: 'stdorgcd_own' if 'stdorgcd_own' in d else 'stdorgcd'     # D72 am. 1: the record's own condition, as the age records carry it


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
    rec['west'] = rec.source != 'site'
    tab = pd.read_parquet(f'{a.data}/l0_table.parquet') if os.path.exists(f'{a.data}/l0_table.parquet') else pd.DataFrame(columns=['scope', 'spcd', 'planted', 'l0'])
    l0_of = lambda d, default=False: (np.where(d.sftwd_hrdwd == 'S', L0['S'], L0['H']) if default else
                                      np.where(d.source == 'site', L0T.lookup(tab, 'r4', d.spcd, (d.stdorgcd == 1).astype(int), d.sftwd_hrdwd),
                                               L0T.lookup(tab, 'west', d.spcd, (d.stdorgcd == 1).astype(int), d.sftwd_hrdwd)))
    rec['total'] = rec.a_obs + np.where(rec.source == 'tot', 0.0, l0_of(rec))                          # the truth, always on the table's l0
    rec['total_fit'] = rec.a_obs + np.where(rec.source == 'tot', 0.0, l0_of(rec, default=a.targets_default))   # the fitting target
    rec.loc[rec.source == 'site', 'cc'] = CC_SITE
    rec['planted'] = (rec.stdorgcd == 1).astype(int)
    rec = features(rec); tr = rec[(rec.split == 'train') & rec.first_of_tree]; va = rec[(rec.split == 'val') & rec.first_of_tree]
    seq = pd.read_parquet(f'{a.data}/seq.parquet'); val = seq[seq.split == 'val']
    if a.fld: tr = pd.concat([tr, field_records(seq, pd.read_parquet(f'{a.data}/planted_refs.parquet'))], ignore_index=True)
    del seq
    c = duckdb.connect(); c.execute("SET threads = 4"); c.execute(f"ATTACH '{a.fiadb}' AS f (READ_ONLY)")
    ln = linked(c, val); ln['west'] = False; ln['cc'] = ln.cclcd
    ln = features(ln[ln.ht.notna() & (ln.ht > 0)]); ln['planted'] = (ln[ORIGIN(ln)] == 1).astype(int)
    ln['total'] = ln.a_obs + (ln.l0.to_numpy(float) if 'l0' in ln else L0T.lookup(tab, 'r4', ln.spcd, ln.planted, ln.sftwd_hrdwd))   # the same truth as site_refs: the per-tree l0 (D78)
    t = duckdb.connect(a.db, read_only=True); cur = t.execute("SELECT STATECD statecd, UNITCD unitcd, COUNTYCD countycd, PLOT plot, SUBP subp, TREE tree, INVYR invyr, estimated_age FROM curated_trees").df()
    ln = ln.merge(cur, on=['statecd', 'unitcd', 'countycd', 'plot', 'subp', 'tree', 'invyr'], how='inner'); ln = ln[ln.estimated_age.notna()]
    R = dict(n_train=len(tr), n_train_by_source=tr.source.value_counts().to_dict(), targets_default=bool(a.targets_default), fld=bool(a.fld), G1_current=score(ln.estimated_age.to_numpy(), ln.total.to_numpy()))
    # R4 saplings have no measured age. Planted stands' field age (STDAGE) is the only outside check there: a diagnostic, never a target or an input
    fa = t.execute("SELECT STATECD statecd, UNITCD unitcd, COUNTYCD countycd, PLOT plot, SUBP subp, TREE tree, INVYR invyr, age_from_stdage fld FROM curated_trees WHERE age_from_stdage IS NOT NULL AND cond_stdorgcd = 1").df()
    vr = features(val[(val.slot_kind == 'real') & (val.dia > 0) & (val.ht > 0)].assign(west=False, cc=lambda d: d.cclcd)).merge(fa, on=['statecd', 'unitcd', 'countycd', 'plot', 'subp', 'tree', 'invyr'])
    vr['planted'] = (vr[ORIGIN(vr)] == 1).astype(int); vr['cls'] = pd.cut(vr.dia, [0, 3, 5, 7, 9, 15, 99], right=False).astype(str)   # as the export reads it (D79 routing)
    R['transfer_current'] = {c: float((g.merge(cur, on=['statecd', 'unitcd', 'countycd', 'plot', 'subp', 'tree', 'invyr']).pipe(lambda d: d.estimated_age - d.fld)).median()) for c, g in vr.groupby('cls')}
    if a.stand:                                                                       # D80: stand structure, one FIADB query for every record set
        st_tab = ST.stand_table(c, pd.concat([d[ST.PV] for d in (tr, va, ln, vr)]))
        tr, va, ln, vr = (ST.attach(d, st_tab) for d in (tr, va, ln, vr))
        R['stand_coverage'] = {nm: float(d.st_n5.notna().mean()) for nm, d in (('train', tr), ('val_records', va), ('val_linked', ln), ('val_planted', vr))}
    # Chapman-Richards on site trees
    st = tr[tr.source == 'site']; cr = {}
    for g, d in st.groupby('spgrpcd'):
        if len(d) >= 200: cr[g] = fit_cr(d.total.to_numpy(), d.ht.to_numpy())
    allp = fit_cr(st.total.to_numpy(), st.ht.to_numpy())
    crp = lambda d: np.array([cr_inv(h, *cr.get(g, allp)) for h, g in zip(d.ht, d.spgrpcd)])
    R['CR'] = dict(groups=len(cr), site_val=score(crp(va[va.source == 'site']), va[va.source == 'site'].total.to_numpy()), G1=score(crp(ln), ln.total.to_numpy()))
    # boosted. D79: with --planted-model 1, model P (R4 records in a planted condition: planted site records + field ages) serves those records and
    # model N (every record but the field ages: the D78 baseline fit) everything else
    split_p = bool(a.planted_model)
    def models(cols):
        return {'N': Fitted(tr[tr.source != 'fld'] if split_p else tr, cols)} | ({'P': Fitted(tr[is_planted_r4(tr)], cols)} if split_p else {})
    def routed(mods, d, crossfit=False):
        """(mu, sd) on log age for the rows of d, each from its model"""
        mu, sd = np.zeros(len(d)), np.zeros(len(d)); usep = is_planted_r4(d) if 'P' in mods else np.zeros(len(d), bool)
        for nm, ix in (('N', ~usep), ('P', usep)):
            if ix.any(): mu[ix], sd[ix] = mods[nm].pred(d[ix], crossfit)
        return mu, sd
    def run(cols, name):
        mods = models(cols)
        def pr(d):
            mu, sd = routed(mods, d); return np.exp(mu), np.exp(mu - 1.2816 * sd), np.exp(mu + 1.2816 * sd)
        pv, lv, hv = pr(va); pl_, ll, hl = pr(ln); v = va.assign(p=pv, lo=lv, hi=hv); pf, lf, hf = pr(vr); xf = vr.assign(p=pf, lo=lf, hi=hf)
        transfer = {c: dict(n=len(g), field=float(g.fld.median()), bias=float((g.p - g.fld).median()), cover80=float(((g.fld >= g.lo) & (g.fld <= g.hi)).mean())) for c, g in xf.groupby('cls')}
        G2 = {s_: score(g.p.to_numpy(), g.total.to_numpy(), g.lo.to_numpy(), g.hi.to_numpy()) for s_, g in v.groupby('source')}
        w = v[v.west == 1]; crown = {int(c_): score(g.p.to_numpy(), g.total.to_numpy()) for c_, g in w.groupby('cc')}
        sm = w[(w.dia >= 1) & (w.dia < 5)]; st_ = sm[sm.source == 'tot']; lp = is_planted_r4(ln)
        R[name] = dict(G1=score(pl_, ln.total.to_numpy(), ll, hl), G1_planted=score(pl_[lp], ln.total.to_numpy()[lp]), G1_natural=score(pl_[~lp], ln.total.to_numpy()[~lp]),
                       G2=G2, G2_crown=crown, G3=score(sm.p.to_numpy(), sm.total.to_numpy()), G3_tot=score(st_.p.to_numpy(), st_.total.to_numpy()),
                       max_pred_linked=float(pl_.max()), transfer=transfer, models={k: int(len(m.oof)) for k, m in mods.items()})
        tv, old = ln.total.to_numpy(), ln.total.to_numpy() >= OLD_AGE
        R[name]['G1_old'] = score(pl_[old], tv[old])                                  # D80 S1
        R[name]['G1_old_by_species'] = {g_: score(pl_[old & ln.spcd.isin(sp_).to_numpy()], tv[old & ln.spcd.isin(sp_).to_numpy()]) for g_, sp_ in
                                         list(SP_GROUPS.items()) + [('other oaks', tuple(c_ for c_ in range(800, 900) if c_ != 802))]}
        lg = legacy_mask(ln); R[name]['G1_legacy'] = score(pl_[lg], tv[lg])          # D80, reported
        if name == 'f+p' and a.stand:                                                 # permutation importance of the stand features on G1 (reported)
            g_ = np.random.default_rng(0); base_ = float(np.abs(pl_ - tv).mean()); imp = {}
            for x in ST.STAND:
                sh = ln.assign(**{x: g_.permutation(ln[x].to_numpy())}); imp[x] = float(np.abs(np.exp(routed(mods, sh)[0]) - tv).mean()) - base_
            R[name]['stand_importance_G1'] = imp
        return mods
    run(NUM + CAT, 'f'); mods = run(NUM + ['planted'] + CAT + (ST.STAND if a.stand else []), 'f+p')   # D80: the stand features join the f+p set
    if a.export:                                                                     # D70 am. 12: prior on log total age at live records: f+p, read from one record, so no hidden record feeds it
        sq = pd.read_parquet(f'{a.data}/seq.parquet')
        keep = (sq.slot_kind == 'real') & (sq.dia > 0) & (sq.ht > 0) & ~(pd.to_datetime(sq.measdate) >= pd.to_datetime(sq.dead_md)); real = features(sq[keep].assign(west=False, cc=lambda d: d.cclcd, planted=lambda d: (d[ORIGIN(d)] == 1).astype(int)))
        if a.stand: real = ST.attach(real, ST.stand_table(c, real))
        mu, sd = routed(mods, real, crossfit=True)                                   # train plots take the fold that left them out, so a frame tree never sees its own age record
        pd.concat([m.trs[[c for c in K7 + ['source', 'plot_key', 'spcd', 'dia', 'a_obs', 'total_fit'] if c in m.trs]].assign(mu_oof=m.oof, model=k) for k, m in mods.items()],
                  ignore_index=True).to_parquet(f'{a.data}/age_oof.parquet')     # D78: the sapling test (out-of-fold log age of every training record, by model)
        sq['af_mu'] = np.nan; sq['af_sd'] = np.nan; sq.loc[real.index, 'af_mu'] = mu; sq.loc[real.index, 'af_sd'] = sd
        if split_p: sq['af_model'] = None; sq.loc[real.index, 'af_model'] = np.where(is_planted_r4(real), 'P', 'N')   # D79: which direct model gave the prior
        sq.to_parquet(f'{a.data}/seq.parquet')
        print('exported', int(sq.af_mu.notna().sum()), 'of', int((sq.slot_kind == 'real').sum()), 'real slots; median age', round(float(np.exp(sq.af_mu.median())), 1), 'median sd', round(float(sq.af_sd.median()), 3),
              ('; by model ' + str(sq.af_model.value_counts().to_dict())) if split_p else '')
    json.dump(R, open(a.out, 'w'), indent=1, default=float); print(json.dumps(R, indent=0, default=float))


def hash2(d):
    return (pd.util.hash_pandas_object(d.plot_key, index=False).to_numpy() % 2).astype(int)

def is_planted_r4(d):
    """D79: R4 records in a planted condition (west 0: R4 site, field-age and frame records; planted from the record's own condition)"""
    return ((d.planted == 1) & (d.west.astype(int) == 0)).to_numpy()

class Fitted:
    """one direct model on the training records trs: the booster on log total age (fit_total), its 2-fold cross-fit by plot (oof, and the fold models
    that serve TRAIN-plot frame records), and the width booster on the out-of-fold squared log residual"""
    def __init__(s, trs, cols):
        s.trs, s.cols = trs, cols; s.maps = {c: {v: i for i, v in enumerate(sorted(trs[c].dropna().unique()))} for c in CAT}; s.cats = [cols.index(x) for x in CAT]
        X = s.enc(trs); y = np.log(trs.total_fit.to_numpy()); f = hash2(trs); s.oof = np.zeros(len(trs)); s.mk = {}
        for k in (0, 1): s.mk[k] = fit(X[f != k], y[f != k], s.cats); s.oof[f == k] = s.mk[k].predict(X[f == k])
        s.m = fit(X, y, s.cats); s.s = fit(X, np.log((y - s.oof) ** 2 + 1e-4), s.cats)
    def enc(s, d):
        return d[s.cols].assign(**{c: d[c].map(s.maps[c]).astype(float) for c in CAT})
    def pred(s, d, crossfit=False):
        """(mu, sd) of log age; crossfit: rows on TRAIN plots take the fold model that left their plot out. A booster on log r^2 estimates
        E log r^2 = log var - 1.27 for a normal residual, so var = exp(pred + 1.27)."""
        X = s.enc(d); mu = s.m.predict(X)
        if crossfit:
            f = hash2(d); t = (d.split == 'train').to_numpy()
            for k in (0, 1):
                ix = t & (f == k)
                if ix.any(): mu[ix] = s.mk[k].predict(X[ix])
        return mu, np.sqrt(np.exp(s.s.predict(X) + 1.2704))

def field_records(seq, planted):
    """D78: planted field ages as direct-prior targets. Live real records on TRAIN plots in a planted condition (the record's own STDORGCD 1) that carry the
    curation's eligible field age (planted_refs: TRAIN plots, FLDSZCD gate), DIA > 0 and HT > 0; each tree's first such record; at most FLD_PER_VISIT
    trees per plot-visit, the smallest crc32 of the natural id (no row order). Features as a tally tree: west 0, its crown class, planted 1."""
    s = seq[(seq.split == 'train') & (seq.slot_kind == 'real') & (seq.dia > 0) & (seq.ht > 0)]
    s = s[(s[ORIGIN(s)] == 1) & ~(pd.to_datetime(s.measdate) >= pd.to_datetime(s.dead_md))].merge(planted[K7 + ['fld']], on=K7)
    s = s[s.fld > 0].sort_values(K7[:-1] + ['measdate', 'invyr']).groupby(K7[:-1], sort=False).head(1)
    s = s.assign(_h=[zlib.crc32('_'.join(map(str, r)).encode()) for r in s[K7].itertuples(index=False)]).sort_values(['plot_key', 'invyr', '_h'])
    s = s.groupby(['plot_key', 'invyr'], sort=False).head(FLD_PER_VISIT).drop(columns='_h')
    s = features(s.assign(west=False, cc=lambda d: d.cclcd, planted=1, source='fld', a_obs=s.fld, total=s.fld, total_fit=s.fld, first_of_tree=True))
    assert (s.split == 'train').all() and not s.duplicated(K7[:-1]).any()
    return s

def legacy_mask(ln):
    """D80 (reported): linked trees >= OLD_AGE yr whose truth is >= LEGACY x the mean truth of the OTHER linked trees of their plot (any visit, per tree)"""
    pk = ln[['statecd', 'unitcd', 'countycd', 'plot']].astype(str).agg('_'.join, axis=1); tk = pk + '_' + ln.subp.astype(str) + '_' + ln.tree.astype(str)
    t = pd.DataFrame(dict(pk=pk, tk=tk, a=ln.total)).groupby(['pk', 'tk']).a.mean().reset_index(); s = t.groupby('pk').a.agg(['sum', 'count'])
    t = t.join(s, on='pk'); t['other'] = np.where(t['count'] > 1, (t['sum'] - t.a) / (t['count'] - 1).clip(lower=1), np.nan)
    o = pd.DataFrame(dict(tk=tk)).merge(t[['tk', 'other']], on='tk', how='left').other.to_numpy()
    return (ln.total.to_numpy() >= OLD_AGE) & (ln.total.to_numpy() >= LEGACY * o)

def compare80(new, base):
    """the D80 gate: S1 old trees, S2 no harm (against the D79 prior on the same truth), S3 planted (D79's P1 with its known-exception guard)"""
    N, B = json.load(open(new))['f+p'], json.load(open(base))['f+p']
    s1 = dict(new=N['G1_old']['mae'], base=B['G1_old']['mae'], gain=B['G1_old']['mae'] - N['G1_old']['mae'], need=S1_OLD); s1['ok'] = bool(s1['gain'] >= S1_OLD)
    s2 = dict(G1=dict(new=N['G1']['mae'], base=B['G1']['mae'], ok=bool(N['G1']['mae'] <= B['G1']['mae'])),
              G1_bias=dict(new=N['G1']['bias'], base=B['G1']['bias'], ok=bool(abs(N['G1']['bias']) - abs(B['G1']['bias']) <= S2_BIAS)),
              G2_site=dict(new=N['G2']['site']['mae'], base=B['G2']['site']['mae'], ok=bool(N['G2']['site']['mae'] - B['G2']['site']['mae'] <= P2_G2)),
              G3_tot=dict(new=N['G3_tot']['mae'], base=B['G3_tot']['mae'], ok=bool(N['G3_tot']['mae'] - B['G3_tot']['mae'] <= P2_G3)))
    p1 = p1_check(N)
    R = dict(S1=s1, S2=s2, S3=p1, S1_pass=s1['ok'], S2_pass=all(v['ok'] for v in s2.values()), S3_pass=all(v['ok'] for v in p1.values()) and bool(p1),
             reported=dict(legacy=(N['G1_legacy'], B['G1_legacy']), old_by_species={k: (N['G1_old_by_species'][k], B['G1_old_by_species'][k]) for k in N['G1_old_by_species']},
                           importance=N.get('stand_importance_G1')))
    R['adopt'] = bool(R['S1_pass'] and R['S2_pass'] and R['S3_pass'])
    print(json.dumps(R, indent=1, default=float)); return R

def p1_check(N):
    """D78 P1 on a report's VAL planted transfer, with D79's known exception under its drift guard"""
    p1 = {c: dict(**v, ok=bool(abs(v['bias']) <= P1_BIAS and P1_COVER[0] <= v['cover80'] <= P1_COVER[1])) for c, v in N['transfer'].items() if v['n'] >= P1_MIN_N}
    for c, kn in KNOWN_P1.items():                                                   # D79 known exception, guarded against drift
        if c not in p1: continue
        if p1[c]['ok']: p1[c]['info'] = 'known exception passes now: consider retiring it'; continue
        within = abs(p1[c]['bias']) <= abs(kn['bias']) + KNOWN_DRIFT and P1_COVER[0] <= p1[c]['cover80'] <= max(P1_COVER[1], kn['cover80']) + KNOWN_DRIFT_COVER
        p1[c].update(known=kn, ok=bool(within), status='KNOWN' if within else 'known exception DRIFTED')
    return p1

def compare(new, base):
    """the D78 bars on two reports of this script (the f+p fit): P1 on the new one's VAL planted transfer, P2 against the baseline on the same truth"""
    N, B = json.load(open(new))['f+p'], json.load(open(base))['f+p']
    p1 = p1_check(N)
    p2 = dict(G1=(N['G1']['mae'], B['G1']['mae'], P2_G1), G3_tot=(N['G3_tot']['mae'], B['G3_tot']['mae'], P2_G3), G2_site=(N['G2']['site']['mae'], B['G2']['site']['mae'], P2_G2))
    p2 = {k: dict(new=v[0], base=v[1], worse=v[0] - v[1], limit=v[2], ok=bool(v[0] - v[1] <= v[2])) for k, v in p2.items()}
    R = dict(P1=p1, P1_pass=all(v['ok'] for v in p1.values()) and bool(p1), P2=p2, P2_pass=all(v['ok'] for v in p2.values()))
    print(json.dumps(R, indent=1)); return R

def selftest():
    rng = np.random.default_rng(0); a_ = rng.uniform(5, 120, 3000); h = cr_h(a_, 90.0, 0.03, 1.3) * np.exp(rng.normal(0, 0.03, 3000))
    A, k, p = fit_cr(a_, h); e = cr_inv(cr_h(a_, 90.0, 0.03, 1.3), A, k, p) - a_
    assert abs(A - 90) < 6 and abs(k - 0.03) < 0.008 and abs(p - 1.3) < 0.25, (A, k, p)
    assert np.abs(e[a_ < 80]).mean() < 3, np.abs(e[a_ < 80]).mean()
    assert abs(cr_inv(np.array([200.0]), 90.0, 0.03, 1.3)[0] - cr_inv(np.array([90.0 * 0.98]), 90.0, 0.03, 1.3)[0]) < 1e-9       # capped
    s = score(np.array([10., 20.]), np.array([12., 20.]), np.array([5., 25.]), np.array([15., 30.])); assert s['mae'] == 1 and s['cover80'] == 0.5
    # D78 field-age targets: planted, TRAIN, live, first record per tree, at most FLD_PER_VISIT per plot-visit
    n = 8; base = dict(statecd=1, unitcd=1, countycd=1, plot=1, subp=1, plot_key='1_1_1_1', spcd=131, spgrpcd=1, sftwd_hrdwd='S', cclcd=3.0, ht=30.0, tm_n=0.0, pp_n=0.0, vp_n=0.0,
                             slot_kind='real', dead_md=pd.NaT, **{c: 0.0 for c in SOIL})
    rows = [dict(base, tree=t, invyr=y, measdate=pd.Timestamp(f'{y}-06-01'), dia=4.0 + t, stdorgcd_own=1.0 if t != 7 else 2.0, split='train' if t != 6 else 'val') for t in range(n) for y in (2000, 2005)]
    sq = pd.DataFrame(rows); pr = sq[K7].assign(fld=12.0); fr = field_records(sq, pr)
    assert len(fr) == 5 and set(fr.invyr) == {2000} and not fr.tree.isin([6, 7]).any() and (fr.total_fit == 12.0).all() and (fr.planted == 1).all() and not fr.west.any(), fr[K7]
    assert fr.tree.tolist() == sorted(fr.tree.tolist(), key=lambda t: zlib.crc32('_'.join(map(str, (1, 1, 1, 1, 1, t, 2000))).encode()))   # picked by the natural-id hash
    # D80 legacy rule: old and >= 1.5x the mean of the plot's OTHER linked trees; a lone tree has no reference
    ln_ = pd.DataFrame(dict(statecd=1, unitcd=1, countycd=1, plot=[1, 1, 1, 1, 2], subp=1, tree=[1, 1, 2, 3, 1], total=[100.0, 104.0, 40.0, 40.0, 90.0]))
    assert legacy_mask(ln_).tolist() == [True, True, False, False, False], legacy_mask(ln_)
    print('selftest: 7 checks PASS')


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    for x in ('--data', '--fiadb', '--db', '--out'): ap.add_argument(x)
    ap.add_argument('--selftest', action='store_true'); ap.add_argument('--export', action='store_true'); ap.add_argument('--fld', type=int, default=0)
    ap.add_argument('--targets-default', action='store_true'); ap.add_argument('--compare'); ap.add_argument('--planted-model', type=int, default=0)
    ap.add_argument('--stand', type=int, default=0); ap.add_argument('--compare80'); a = ap.parse_args()
    assert not (a.planted_model and not a.fld), 'D79: the planted model needs the field-age records (--fld 1)'
    selftest() if a.selftest else compare80(a.out, a.compare80) if a.compare80 else compare(a.out, a.compare) if a.compare else main(a)
