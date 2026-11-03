"""D70 scoring shared by the dev eval and the cross-validation. Needs no database, so it runs on a GPU machine from the bundle."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd, torch
import nimiwae56 as M, nimiwae70 as A

def reliability(p, y, nmin=200):
    p, y = np.asarray(p, float), np.asarray(y, float); ok = ~np.isnan(p) & ~np.isnan(y); p, y = p[ok], y[ok]
    bins = np.clip((p * 10).astype(int), 0, 9); cells = []
    for b in range(10):
        m = bins == b
        if m.sum() >= nmin: cells.append(dict(decile=b, n=int(m.sum()), mean_pred=float(p[m].mean()), observed=float(y[m].mean())))
    passed = all(abs(c['mean_pred'] - c['observed']) <= 0.05 for c in cells)
    return dict(n=int(len(p)), brier=float(np.mean((p - y) ** 2)), base_rate=float(y.mean()) if len(y) else None, cells=cells, pass_=passed)

def load(d, name='model.pt'):
    ck = torch.load(f'{d}/{name}', weights_only=False, map_location='cpu'); m = A.make_model(ck['vocab'], age=ck['cfg']['age']); m.load_state_dict(ck['state']); m.eval(); return m, ck

def med(x): return float(np.median(x)) if len(x) else None

def completion(m, val, seed=0):
    """size error on the hidden visits, E1 on the same lineages under '< limit only', E2 and E3 on censored ends"""
    torch.manual_seed(seed); out = {}
    h, _ = M.predictions(m, M.bench_sizes(val)); hh = h[h.hidden & h.dia.notna() & h.nim_lD.notna()]; out['mae_lD'] = float((hh.nim_lD - np.log(hh.dia)).abs().mean())
    x1, _ = M.predictions(m, M.bench_sizes(val, interval=False)); x1 = x1[x1.truth_E.notna()]; r1 = reliability(x1.nim_pE, x1.truth_E); out['E1'] = r1['brier']; out['E1_n'] = r1['n']; out['E1_base'] = r1['base_rate']
    assert 0 < r1['base_rate'] < 1, 'E1 benchmark needs both TRUE and FALSE labels'
    b = M.bench_end(val); e, _ = M.predictions(m, b); x = e[e.truth_E.notna()]; out['E2'] = reliability(x.nim_pE, x.truth_E)['brier']
    g = x[x.truth_E == 0]; g = g[g.truth_cut.notna()]; out['E3'] = reliability(g.nim_pCut / (g.nim_pCut + g.nim_pDie).clip(lower=1e-9), g.truth_cut)['brier']
    return out

@torch.no_grad()
def lineage_age(m, val, keys, K=100):
    """age at the linked visits: 10, 50, 90 % of the exact grid posterior of z0 (am. 14)"""
    trees = keys[M.K_].drop_duplicates(); b = val.merge(trees, on=M.K_); B, _, sq = A.build(b, m.vocab); dev = next(m.parameters()).device
    out = []
    for i in range(0, B['v'].shape[0], 512):
        s = {k: v.to(dev) for k, v in M.sub(B, slice(i, i + 512)).items()}; g, p = A.age_posterior(m, s, K=K)
        zq = A.z_quantiles(g.cpu(), p.cpu(), [0.1, 0.5, 0.9]); z = torch.zeros(3, zq.shape[1], M.DZ, device=dev); z[..., 0] = zq.to(dev)
        out.append(m.ages(z, s).cpu())                                                     # [3, n, T]
    q = torch.cat(out, 1).numpy(); ti, po = sq._tid.to_numpy(), sq._pos.to_numpy()
    return sq.assign(q10=q[0, ti, po], q50=q[1, ti, po], q90=q[2, ti, po])[M.K_ + ['invyr', 'q10', 'q50', 'q90']]

def prior_first(held):
    """the direct model alone, read at the first record: the joint must not be worse than this"""
    r = held[(held.slot_kind == 'real') & held.af_mu.notna()].assign(yr=lambda d: (pd.to_datetime(d.measdate) - pd.Timestamp('2000-01-01')) / pd.Timedelta(days=365.25))
    f = r.sort_values(M.K_ + ['invyr']).groupby(M.K_).first()[['yr', 'af_mu']].rename(columns=dict(yr='y0', af_mu='mu0')).reset_index()
    return r[M.K_ + ['invyr', 'yr']].merge(f, on=M.K_).assign(prior=lambda d: np.exp(d.mu0) + d.yr - d.y0)[M.K_ + ['invyr', 'prior']]

def age_scores(m, held, site, planted, seed=0, planted_frac=0.25):
    """G1, G5 and the planted diagnostic on one held-out set. site: linked site trees; planted: field age, a diagnostic only"""
    torch.manual_seed(seed); k = M.K_ + ['invyr']
    j = site.merge(held[held.slot_kind == 'real'][k].drop_duplicates(), on=k); j = j[j.estimated_age.notna()]
    j = j.merge(lineage_age(m, held, j), on=k).merge(prior_first(held), on=k).merge(held[held.slot_kind == 'real'][k + ['af_mu']], on=k); t = j.truth; direct = np.exp(j.af_mu)   # f+p at the scored record, as am. 12 registered it
    o = dict(G1=dict(n=len(j), mae_joint=float((j.q50 - t).abs().mean()), bias_joint=med(j.q50 - t), mae_direct=float((direct - t).abs().mean()), bias_direct=med(direct - t), mae_prior=float((j.prior - t).abs().mean()), bias_prior=med(j.prior - t), mae_current=float((j.estimated_age - t).abs().mean()), bias_current=med(j.estimated_age - t)),
             G5=dict(cover80=float(((t >= j.q10) & (t <= j.q90)).mean()), width=float((j.q90 / j.q10.clip(lower=0.1)).median())))
    p = planted.merge(held[held.slot_kind == 'real'][k + ['dia', 'plot_key']], on=k); p = p[p.plot_key.isin(p.plot_key.drop_duplicates().sample(frac=planted_frac, random_state=3))]
    q = lineage_age(m, held[held.plot_key.isin(p.plot_key)], p).merge(p, on=k); q['cls'] = pd.cut(q.dia, [0, 3, 5, 7, 9, 15, 99], right=False).astype(str)
    o['planted_field'] = {c: dict(n=len(g), field=med(g.fld), bias=med(g.q50 - g.fld), cover80=float(((g.fld >= g.q10) & (g.fld <= g.q90)).mean())) for c, g in q.groupby('cls')}
    return o
