"""D70 am. 13: which reading of the direct age model fits a tree's birth date best, with no lineage model involved.
  python -I nimiwae70_prior_read.py --data DIR --fiadb FIADB --db TRUTH --age1 MODEL_DIR --out report.json
Scored on the G1 trees of VAL, at the linked visit. Each reading gives one birth date per tree from its visible records; age at a visit is then the date minus that birth.
  at_visit   the prediction at the scored record itself: the gate's number, not a birth date
  last       am. 12: the last record, walked back by elapsed time
  first      the first record, walked forward
  mean_birth the mean of the birth dates each record implies, exp(af_mu) years before it"""
import sys, os, json, argparse
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd, torch, duckdb
import nimiwae56 as M, nimiwae70_eval as E

def births(r):
    """r: real slots of the trees, with yr (decimal years) and af_mu -> one row per tree. Sorted on natural ids; records of one tree have distinct dates or the sort is by invyr."""
    r = r.sort_values(M.K_ + ['invyr']).assign(b=lambda d: d.yr - np.exp(d.af_mu)); g = r.groupby(M.K_)
    return pd.DataFrame(dict(last=g.b.last(), first=g.b.first(), mean_birth=g.b.mean(), n_rec=g.size(), span=g.yr.max() - g.yr.min())).reset_index()

def selftest():
    r = pd.DataFrame({**{k: 1 for k in M.K_}, 'invyr': [2000, 2010], 'yr': [2000.0, 2010.0], 'af_mu': np.log([20.0, 25.0])})
    b = births(r).iloc[0]
    assert b['last'] == 1985 and b['first'] == 1980 and b.mean_birth == 1982.5 and b.n_rec == 2 and b.span == 10
    print('selftest: 1 check PASS')

def row(e, t):
    return dict(n=len(e), mae=float((e - t).abs().mean()), bias=float((e - t).median()))

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    for x in ('--data', '--fiadb', '--db', '--age1', '--out'): ap.add_argument(x)
    ap.add_argument('--selftest', action='store_true'); a = ap.parse_args()
    if a.selftest: selftest(); raise SystemExit
    seq = pd.read_parquet(f'{a.data}/seq.parquet'); val = seq[seq.split == 'val'].copy(); del seq
    val['yr'] = (pd.to_datetime(val.measdate) - pd.Timestamp('2000-01-01')) / pd.Timedelta(days=365.25)
    c = duckdb.connect(); c.execute("SET threads = 4"); c.execute(f"ATTACH '{a.fiadb}' AS f (READ_ONLY)"); ln = E.linked(c, val)
    t = duckdb.connect(a.db, read_only=True); cur = t.execute("SELECT STATECD statecd, UNITCD unitcd, COUNTYCD countycd, PLOT plot, SUBP subp, TREE tree, INVYR invyr, estimated_age FROM curated_trees").df()
    ln['truth'] = ln.a_obs + np.where(ln.sftwd_hrdwd == 'S', 2.0, 5.0)
    j = ln.merge(cur, on=M.K_ + ['invyr']); j = j[j.estimated_age.notna()]
    real = val[(val.slot_kind == 'real') & val.af_mu.notna()]; j = j.merge(real[M.K_ + ['invyr', 'yr', 'af_mu']], on=M.K_ + ['invyr'])
    tr = real.merge(j[M.K_].drop_duplicates(), on=M.K_); b = births(tr); j = j.merge(b, on=M.K_)
    R = dict(current=row(j.estimated_age, j.truth), at_visit=row(np.exp(j.af_mu), j.truth))
    for k in ('last', 'first', 'mean_birth'):
        R[k] = row(j.yr - j[k], j.truth); j[f'p_{k}'] = j.yr - j[k]
    m, _ = E.load(a.age1); L = E.lineage_age(m, val, j); j = j.merge(L, on=M.K_ + ['invyr']); R['joint'] = row(j.q50, j.truth)
    R['joint_minus_last'] = float((j.q50 - j.p_last).median())
    atl = (j.yr + 1e-6 >= tr.groupby(M.K_).yr.max().rename('ymax').reset_index().merge(j[M.K_], on=M.K_).ymax.to_numpy())
    R['by_visit'] = {x: {k: row(g[c_], g.truth) for k, c_ in (('at_visit', 'at'), ('last', 'p_last'), ('first', 'p_first'), ('mean_birth', 'p_mean_birth'), ('joint', 'q50'))} for x, g in j.assign(at=np.exp(j.af_mu), w=np.where(atl, 'scored at last record', 'scored earlier')).groupby('w')}
    # does the model's prediction keep pace with time? slope of predicted age on elapsed years between a tree's first and last record
    g = tr.sort_values(M.K_ + ['invyr']).groupby(M.K_); d = pd.DataFrame(dict(da=np.exp(g.af_mu.last()) - np.exp(g.af_mu.first()), dt=g.yr.last() - g.yr.first()))
    d = d[d.dt > 1]; R['pace'] = dict(n=len(d), slope=float((d.da * d.dt).sum() / (d.dt ** 2).sum()), median_ratio=float((d.da / d.dt).median()))
    va = real.sort_values(M.K_ + ['invyr']).groupby(M.K_); dv = pd.DataFrame(dict(da=np.exp(va.af_mu.last()) - np.exp(va.af_mu.first()), dt=va.yr.last() - va.yr.first())); dv = dv[dv.dt > 1]
    R['pace_all_val'] = dict(n=len(dv), slope=float((dv.da * dv.dt).sum() / (dv.dt ** 2).sum()), median_ratio=float((dv.da / dv.dt).median()))
    # the scored record left out: age at a visit from the tree's OTHER records only, so a reading cannot win by being the scored record
    o = j[M.K_ + ['invyr', 'yr', 'truth']].merge(tr[M.K_ + ['invyr', 'yr', 'af_mu', 'af_sd']].rename(columns=dict(invyr='iv2', yr='y2')), on=M.K_); o = o[o.iv2 != o.invyr]
    o['b'] = o.y2 - np.exp(o.af_mu); o['p'] = o.yr - o.b; o['gap'] = o.y2 - o.yr; o['dir'] = np.where(o.gap > 0, 'from_later', 'from_earlier')
    R['left_out'] = {x: dict(row(g.p, g.truth), median_gap=float(g.gap.abs().median())) for x, g in o.groupby('dir')}
    o['gp'] = pd.cut(o.gap.abs(), [0, 7, 12, 40]).astype(str); R['left_out_by_gap'] = {f'{d_} {g_}': row(g.p, g.truth) for (d_, g_), g in o.groupby(['dir', 'gp'])}
    om = o.groupby(M.K_ + ['invyr']).agg(b=('b', 'mean'), yr=('yr', 'first'), truth=('truth', 'first')); R['left_out_mean_birth'] = row(om.yr - om.b, om.truth)
    # coverage of the 80 % interval and the planted check, per reading. Field age is a diagnostic only
    def reading(r, how):
        r = r.sort_values(M.K_ + ['invyr']).assign(b=lambda d: d.yr - np.exp(d.af_mu)); g = r.groupby(M.K_); p = g.last() if how == 'last' else g.first()
        o_ = p[['af_mu', 'af_sd', 'yr']].rename(columns=dict(yr='yr_r')).reset_index()
        return o_ if how != 'mean_birth' else o_.assign(b_m=g.b.mean().to_numpy())
    fa = t.execute("SELECT STATECD statecd, UNITCD unitcd, COUNTYCD countycd, PLOT plot, SUBP subp, TREE tree, INVYR invyr, age_from_stdage fld FROM curated_trees WHERE age_from_stdage IS NOT NULL AND cond_stdorgcd = 1").df()
    vk = real[M.K_ + ['invyr', 'yr', 'dia']].merge(fa, on=M.K_ + ['invyr']); vk['cls'] = pd.cut(vk.dia, [0, 3, 5, 7, 9, 15, 99], right=False).astype(str); rv = real.merge(vk[M.K_].drop_duplicates(), on=M.K_)
    for how in ('last', 'first'):
        q = reading(tr, how); jj = j[M.K_ + ['yr', 'truth']].merge(q, on=M.K_); sh = jj.yr - jj.yr_r
        lo, hi = np.exp(jj.af_mu - 1.2816 * jj.af_sd) + sh, np.exp(jj.af_mu + 1.2816 * jj.af_sd) + sh; R[f'cover_{how}'] = float(((jj.truth >= lo) & (jj.truth <= hi)).mean())
        q2 = reading(rv, how); pv = vk.merge(q2, on=M.K_); pv['p'] = np.exp(pv.af_mu) + pv.yr - pv.yr_r; lo, hi = pv.p * np.exp(-1.2816 * pv.af_sd), pv.p * np.exp(1.2816 * pv.af_sd)
        R[f'planted_{how}'] = {c_: dict(n=len(g), field=float(g.fld.median()), bias=float((g.p - g.fld).median())) for c_, g in pv.groupby('cls')}
    json.dump(R, open(a.out, 'w'), indent=1); print(json.dumps(R, indent=0))
