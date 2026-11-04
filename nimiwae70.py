"""D70 — joint NIMIWAE age model: NIMIWAE56 with flexible growth and the known design rule, plus one birth date per tree (D70 am. 12-16, D72).

Age       log a_l = af_mu + af_sd z0, z0 ~ N(0, 1) under the prior, at the tree's first visible live measured record l (am. 13). af_mu and af_sd come from the direct age model
          (nimiwae_age_direct.py), fitted on TRAIN measured ages only and fixed here. z0 is a true latent, so the width of age is honest.
Birth     b = t_l - a_l; a_t = t - b at every visit, from measurement dates, never INVYR. Breast height at b + l0 (2 yr softwood, 5 yr hardwood, as the targets define total age).
Size      the decoder never reads z0, so growth is not read as age.

Observation model (D72): every factor, once. g_t = P(at breast height at t | z0) = sigma(KAPPA_B (a_t - l0)); s_t = P(D_t at the smallest level | z) = sigma(KAPPA x_e).
  existence evidence, TRUE / FALSE     P(exists) = g_t s_t (the D55 step times the gate; soft labels with error rate rho as in D55)
  tallied record                       log sigma(x_t)        given existence (D >= limit); existence itself is the TRUE label
  missed visit (codes 3 / 6 / 7)       log sigma(x_t)        given existence, which the soft TRUE label carries
  sampled, no record (cut-off mode)    log(1 - p_s g_t sigma(x_t))   a tree too short to measure cannot be tallied either
  measurements, crown, damage          as D55, at visible records
There is no separate short-tree factor: before breast height the gate removes existence and the tally, so the size path there is unobserved (am. 16's term double-counted g_t
at every TRUE slot and was not a likelihood). Age off: g_t = 1 everywhere, the D55 model.
E1        a microplot debut on a moved plot: slots before the move carry no FALSE label (D65 item 2).
D74       (opt-in) measured-age anchors: log AGEDIA ~ N(log(a_t - l0), S_ANCHOR) at linked visits on fit plots; the age prior shifted by the tree's observed
          log DIA growth rate, log a_l = af_mu + delta_c (r - m_c) + af_sd w_c z0, delta, m and w fixed by calibrate_coupling() before training; height
          residuals Student-t (nu 4) with one scale per height method (HTCD 1 / null vs 2, 3), HTCD 4 (modelled) heights masked.
q         'gauss' (diagonal Gaussian, the default) or 'flow' (D73): z0 keeps its own Gaussian, the other dimensions go through a conditional rational-quadratic spline
          coupling flow on the encoder's tree state (nimiwae_flow.py). q(z) = q(z0) q(z_rest) either way, which the exact age grid needs."""
import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import math, time, json, contextlib
import numpy as np, pandas as pd, torch, torch.nn as nn, torch.nn.functional as F
import nimiwae56 as M
from nimiwae56 import T_MAX, DZ, D_MODEL, COV, sub
from torch.func import functional_call
import nimiwae_flow as NF
FLOW_DEFAULT = dict(layers=6, hidden=64, bins=8, bound=5.0)

SOIL_COLS = [f'soil_{c}' for c in ('clay', 'sand', 'silt', 'soc', 'bd', 'ph', 'cec', 'depth_restriction', 'rock_frag', 'depth_bedrock')]
NSOIL = len(SOIL_COLS)
KAPPA_B = 2.0                 # per year: the birth step is about half a year wide
AF_MU0, AF_SD0 = math.log(25.0), 0.8     # trees with no usable record: wide default, not a measurement
L0 = dict(S=2.0, H=5.0)                  # years from germination to breast height; the age targets use the same values
ITYR = COV.index('tyr')
S_ANCHOR = 0.10                          # D74: log-scale width of a ring-count breast-height age (sensitivity 0.05 / 0.20 reported)
T_NU = 4.0                               # D74: Student-t degrees of freedom for log HT residuals
RATE_FLOOR = 0.02                        # D74: in / yr; an observed DIA change at or below it reads as this rate (12 % of anchor increments are <= 0)
COUPLE_MIN_DIA = 5.0                     # D74: no coupling below 5 in at the prior's slot (no R4 sapling has an age)
COUPLE_MIN_YEARS = 1.0                   # D74: a growth rate needs measured DIA at least this far apart
WEST_RATIO = {4: 0.61, 5: 0.43}          # D74: coupling strength of intermediate / overtopped relative to classes 1-3 (western BHAGE, 229k trees)

def soil_vocab(seq):
    return {c: (float(seq[c].mean()), float(seq[c].std()) or 1.0) for c in SOIL_COLS}

def e1_mask(seq):
    """Slots whose FALSE label is dropped: before the plot's move, for a tree first recorded at or after it."""
    if 'move_invyr' not in seq: return pd.Series(False, index=seq.index)
    vis = (seq.slot_kind == 'real') & ~(seq.hidden.fillna(False).astype(bool) if 'hidden' in seq else False)   # visible records only: a hidden debut must not decide which labels drop
    first = seq[vis].groupby(M.K_).invyr.min().rename('_first')
    deb = seq.join(first, on=M.K_)._first.to_numpy(float)
    mv = seq.move_invyr.to_numpy(float); iy = seq.invyr.to_numpy(float)
    return pd.Series((iy < mv) & (deb >= mv), index=seq.index)

def build(seq, vocab=None):
    seq = seq.copy()
    seq['drop_false'] = (seq.drop_false.fillna(False).astype(bool) if 'drop_false' in seq else False) | e1_mask(seq)
    B, vocab, sq = M.build(seq, vocab)
    if '_soil' not in vocab: vocab['_soil'] = soil_vocab(seq)
    st = sq.drop_duplicates('_tid').sort_values('_tid')
    B['soil'] = torch.tensor(np.stack([np.nan_to_num((st[c].to_numpy(float) - vocab['_soil'][c][0]) / vocab['_soil'][c][1], nan=0.0) for c in SOIL_COLS], 1), dtype=torch.float32)
    n = B['v'].shape[0]; ti, po = sq._tid.to_numpy(), sq._pos.to_numpy()
    mu, sd, la = np.full(n, AF_MU0, np.float32), np.full(n, AF_SD0, np.float32), B['f'].clone()      # no prior: still anchored at the first visible record
    B['l0'] = torch.tensor(np.where(st.sftwd_hrdwd.to_numpy() == 'S', L0['S'], L0['H']), dtype=torch.float32)
    if 'af_mu' in sq:                                                                          # the first visible slot with a prior; hidden records never feed it
        hid = sq.hidden.fillna(False).to_numpy(bool) if 'hidden' in sq else np.zeros(len(sq), bool)
        ok = (sq.slot_kind == 'real').to_numpy() & ~hid & sq.af_mu.notna().to_numpy()
        o = pd.DataFrame(dict(t=ti[ok], p=po[ok], m=sq.af_mu.to_numpy(float)[ok], s=sq.af_sd.to_numpy(float)[ok])).sort_values(['t', 'p']).groupby('t').first()
        mu[o.index.to_numpy()] = o.m.to_numpy(); sd[o.index.to_numpy()] = o.s.to_numpy(); la[torch.tensor(o.index.to_numpy())] = torch.tensor(o.p.to_numpy(), dtype=la.dtype)
        B['af_has'] = torch.zeros(n); B['af_has'][torch.tensor(o.index.to_numpy())] = 1.0
    else:
        B['af_has'] = torch.zeros(n)
    B['af_mu'], B['af_sd'], B['la'] = torch.tensor(mu), torch.tensor(sd), la
    d74_features(B, seq_cols=sq, ti=ti, po=po)
    if 'age_coupling' in vocab: apply_coupling(B, vocab['age_coupling'])
    return B, vocab, sq

def d74_features(B, seq_cols, ti, po):
    """D74 per-tree and per-slot inputs, all from visible records: observed log DIA growth rate, DIA and crown class at the prior's slot la,
    anchor ages (column anc_agedia, fit plots only), height method classes (column htcd; HTCD 4 heights masked). Absent columns leave the model as before."""
    n, T = B['v'].shape; t = B['cov'][..., ITYR] * 20.0; m = B['mD'] > 0; ar = torch.arange(T)[None].expand(n, -1)
    first = torch.where(m, ar, torch.full_like(ar, T)).min(1).values; last = torch.where(m, ar, torch.full_like(ar, -1)).max(1).values
    ok = (first < T) & (last > first); fi, la_ = first.clamp(max=T - 1)[:, None], last.clamp(min=0)[:, None]
    dt = (t.gather(1, la_) - t.gather(1, fi))[:, 0]; D = torch.exp(B['lD'])
    rate = (D.gather(1, la_) - D.gather(1, fi))[:, 0] / dt.clamp(min=1e-6)
    B['has_rate'] = (ok & (dt >= COUPLE_MIN_YEARS)).float(); B['lrate'] = torch.log(rate.clamp(min=RATE_FLOOR)) * B['has_rate']
    li = B['la'][:, None]
    B['dia_la'] = torch.where(B['mD'].gather(1, li)[:, 0] > 0, D.gather(1, li)[:, 0], torch.zeros(n))
    B['cc_la'] = torch.where(B['mcc'].gather(1, li)[:, 0] > 0, B['cc'].gather(1, li)[:, 0], torch.zeros(n)).long()
    real_vis = (seq_cols.slot_kind == 'real').to_numpy() & ~(seq_cols.hidden.fillna(False).to_numpy(bool) if 'hidden' in seq_cols else np.zeros(len(seq_cols), bool))
    B['anc_y'], B['anc_m'] = torch.zeros(n, T), torch.zeros(n, T)
    if 'anc_agedia' in seq_cols:                                                              # breast-height ages of linked site trees (fit plots only)
        y = seq_cols.anc_agedia.to_numpy(float); k = real_vis & np.isfinite(y) & (np.nan_to_num(y) > 0)
        B['anc_y'][ti[k], po[k]] = torch.tensor(np.log(y[k]), dtype=torch.float32); B['anc_m'][ti[k], po[k]] = 1.0
    B['htcls'] = torch.zeros(n, T, dtype=torch.long)
    if 'htcd' in seq_cols:
        h = seq_cols.htcd.to_numpy(float)
        vis = np.isin(h, (2.0, 3.0)); mdl = h == 4.0
        B['htcls'][ti[vis], po[vis]] = 1; B['mH'][ti[mdl], po[mdl]] = 0.0                      # modelled heights repeat the previous height: not observations
    B['af_shift'], B['af_sdf'] = torch.zeros(n), torch.ones(n)

def coupling_eligible(B):
    """trees the coupling applies to: a prior, an observed growth rate, DIA >= 5 in and a crown class at the prior's slot"""
    return (B['af_has'] > 0) & (B['has_rate'] > 0) & (B['dia_la'] >= COUPLE_MIN_DIA) & (B['cc_la'] >= 1) & (B['cc_la'] <= 5)

def apply_coupling(B, cp):
    """log a_l = af_mu + delta_c (lrate - m_c) + af_sd w_c z0 on eligible trees; others unchanged"""
    el = coupling_eligible(B); c = B['cc_la'].clamp(0, 5)
    dl = torch.tensor([0.0] + [cp['delta'][str(k)] for k in range(1, 6)]); mm = torch.tensor([0.0] + [cp['m'][str(k)] for k in range(1, 6)]); ww = torch.tensor([1.0] + [cp['w'][str(k)] for k in range(1, 6)])
    B['af_shift'] = torch.where(el, dl[c] * (B['lrate'] - mm[c]), torch.zeros_like(B['af_mu']))
    B['af_sdf'] = torch.where(el, ww[c], torch.ones_like(B['af_sd']))

def calibrate_coupling(B, log=print):
    """D74: fixed coupling from the anchors in B (fit plots), before training; nothing here is learned in the joint.
    Total age at la from each anchor: AGEDIA + l0 - (t_anchor - t_la). Regress log total age - af_mu on the log rate (classes 1-3 pooled): delta, and the
    width factor w = residual sd with the rate / without. Classes 4, 5: delta x WEST_RATIO, w = 1. m_c = mean log rate of every eligible tree of class c,
    so each class keeps its prior level and the coupling only reorders trees within it."""
    el = coupling_eligible(B); t = B['cov'][..., ITYR] * 20.0; li = B['la'][:, None]
    ti, pj = torch.nonzero(B['anc_m'] > 0, as_tuple=True)
    keep = el[ti] & (B['cc_la'][ti] <= 3); ti, pj = ti[keep], pj[keep]
    tot = torch.exp(B['anc_y'][ti, pj]) + B['l0'][ti] - (t[ti, pj] - t.gather(1, li)[ti, 0])
    ok = tot > 0.5; ti, tot = ti[ok], tot[ok]
    y = (torch.log(tot) - B['af_mu'][ti]).double().numpy(); x = B['lrate'][ti].double().numpy()
    assert len(y) >= 200, f'too few anchors to calibrate the coupling: {len(y)}'
    X = np.column_stack([np.ones_like(x), x]); b, *_ = np.linalg.lstsq(X, y, rcond=None); r1 = y - X @ b; r0 = y - y.mean()
    d = float(b[1]); w = float(r1.std() / r0.std())
    cp = dict(delta={str(k): d for k in (1, 2, 3)} | {str(k): d * WEST_RATIO[k] for k in (4, 5)}, w={str(k): w for k in (1, 2, 3)} | {'4': 1.0, '5': 1.0},
              m={str(k): float(B['lrate'][el & (B['cc_la'] == k)].double().mean()) if bool((el & (B['cc_la'] == k)).any()) else 0.0 for k in range(1, 6)},
              n_anchor_rows=int(len(y)), n_anchor_trees=int(len(torch.unique(ti))), intercept=float(b[0]), rate_floor=RATE_FLOOR)
    log(f'D74 coupling calibrated on {cp["n_anchor_rows"]:,} anchor rows ({cp["n_anchor_trees"]:,} trees): delta {d:+.3f} per log (in / yr), width factor {w:.3f}, '
        f'intercept {b[0]:+.3f}; m by class {", ".join(f"{k}: {v:.2f}" for k, v in cp["m"].items())}')
    return cp

class Static70(M.Static):
    """Soil joins the statics of the prior and the decoder. The encoder also reads the age prior, so it knows how close a tree sits to its birth limits."""
    def __init__(s, nsp, nl3, nmaj, nfty, nspc, use_summ=True, use_af=False):
        super().__init__(nsp, nl3, nmaj, nfty, nspc, use_summ); s.use_af = use_af
        s.out = nn.Linear(s.out.in_features + NSOIL + (2 if use_af else 0), D_MODEL)
    def forward(s, B):
        x = [s.e[0](B['sp']), s.e[1](B['l3']), s.e[2](B['maj']), s.e[3](B['fty']), s.e[4](B['scl']), s.e[5](B['phy']), s.e[6](B['spc']), B['snum'], B['soil']]
        if s.use_af: x.append(torch.stack([B['af_mu'] + B.get('af_shift', 0.0) - AF_MU0, B['af_sd'] * B.get('af_sdf', 1.0)], -1))   # D74: the prior the model uses
        return s.out(torch.cat(x + ([B['summ']] if s.use_summ else []), -1))

class NIMIWAE70(M.NIMIWAE56):
    build = staticmethod(lambda seq, vocab: build(seq, vocab))                             # predictions and stand totals use the joint build
    def __init__(s, nsp, nl3, nmaj, nfty, nspc, age=True, q='gauss', flow=None, height='gauss'):
        super().__init__(nsp, nl3, nmaj, nfty, nspc, censor='known', growth='flex', noise=False, prior_ctx=False)
        assert q in ('gauss', 'flow') and height in ('gauss', 't')
        s.age, s.qfam, s.height = age, q, height
        if height == 't': s.lsHc = nn.Parameter(torch.full((2,), -2.0))                       # D74: log scale per height method (1 / null, 2-3)
        s.st_q = Static70(nsp, nl3, nmaj, nfty, nspc, use_af=age); s.st_p = Static70(nsp, nl3, nmaj, nfty, nspc, use_summ=False)
        if q == 'flow':                                                                       # D73: z0 (age) stays Gaussian and separate; age off, every dimension flows
            fc = {**FLOW_DEFAULT, **(flow or {})}; s.f0 = 1 if age else 0
            s.flow = NF.CondFlow(DZ - s.f0, D_MODEL, layers=fc['layers'], hidden=fc['hidden'], bins=fc['bins'], B=fc['bound'])
    def height_ll(s, lHp, B):
        """D74: Student-t log density of log HT, scale by height method"""
        if s.height != 't': return super().height_ll(lHp, B)
        ls = s.lsHc[B['htcls']][None]; r = (B['lH'][None] - lHp) / torch.exp(ls); nu = T_NU
        return (math.lgamma((nu + 1) / 2) - math.lgamma(nu / 2) - 0.5 * math.log(nu * math.pi)) - ls - (nu + 1) / 2 * torch.log1p(r ** 2 / nu)
    def extra_lw(s, lDp, z, B, mp, lp):
        """D74: measured breast-height ages of linked site trees, at their visits (fit plots only; zero where B has none)"""
        if not s.age or 'anc_m' not in B: return 0.0
        bh = s.bh_age(z, B).clamp(min=0.1)
        ll = -0.5 * ((B['anc_y'][None] - torch.log(bh)) / S_ANCHOR) ** 2 - math.log(S_ANCHOR) - 0.5 * math.log(2 * math.pi)
        return (ll * B['anc_m'][None]).sum(-1)
    def enc_params(s):
        return super().enc_params() + (list(s.flow.parameters()) if s.qfam == 'flow' else [])
    def sample_z(s, mq, lq, K):
        """flow: e ~ N(mq, exp(lq)), z = (e_0, T(e_rest; c)); log q(z) = log N(e) - log |dT/de|"""
        if s.qfam != 'flow': return super().sample_z(mq, lq, K)
        e = mq + torch.exp(0.5 * lq) * torch.randn(K, *mq.shape, device=mq.device)
        zr, ld = s.flow(e[..., s.f0:], s._hq)
        return torch.cat([e[..., :s.f0], zr], -1), M.NIMIWAE56.log_qz(s, e, mq, lq) - ld
    def log_qz(s, z, mq, lq, detach=False):
        """flow: e_rest = T^-1(z_rest; c); log q(z) = log N((z_0, e_rest)) + log |de/dz|. detach: Gaussian, context and flow weights held fixed, z keeps its path (DReG)"""
        if s.qfam != 'flow': return super().log_qz(z, mq, lq, detach)
        c = s._hq
        if detach:
            mq, lq, c = mq.detach(), lq.detach(), c.detach()
            er, ld = functional_call(s.flow, {k: v.detach() for k, v in s.flow.named_parameters()}, (z[..., s.f0:], c), dict(inverse=True))
        else:
            er, ld = s.flow(z[..., s.f0:], c, inverse=True)
        return M.NIMIWAE56.log_qz(s, torch.cat([z[..., :s.f0], er], -1), mq, lq) + ld
    def q(s, B):
        return s._cache['q'] if getattr(s, '_cache', None) else super().q(B)
    def p_z(s, B):
        mu, lv = super().p_z(B)
        if not s.age: return mu, lv
        keep = torch.ones_like(mu); keep[..., 0] = 0.0                                        # z0 ~ N(0, 1) exactly
        return mu * keep, lv * keep
    def decode(s, z, B, u=None):
        if getattr(s, '_cache', None): return s._cache['dec']
        if s.age:
            z = z.clone(); z[..., 0] = 0.0
        return super().decode(z, B, u)
    def ages(s, z, B):
        """a_t [K, n, T]: one birth date per tree. a_l > 0 at the prior's slot, the debut or the first visible record; other slots add elapsed years exactly, so a_t < 0 before birth."""
        mu = B['af_mu'] + B.get('af_shift', 0.0); sd = B['af_sd'] * B.get('af_sdf', 1.0)        # D74: the coupled prior (no shift when not calibrated)
        al = torch.exp((mu[None] + sd[None] * z[..., 0]).clamp(max=7.0))                          # [K, n]; up to about 1100 yr
        t = B['cov'][..., ITYR] * 20.0                                                        # [n, T] years since 2000, from measurement dates
        return al[..., None] + (t - t.gather(1, B['la'][:, None]))[None]
    def bh_age(s, z, B):
        """years since reaching breast height [K, n, T]; negative before it"""
        return s.ages(z, B) - B['l0'][None, :, None]
    def born_gate(s, lp1, lp0, z, B):
        if not s.age: return super().born_gate(lp1, lp0, z, B)
        u = s.bh_age(z, B); l1 = lp1 + F.logsigmoid(KAPPA_B * u)
        return l1, torch.log(-torch.expm1(l1.clamp(max=-1e-6))), u > 0
    def cutoff_term(s, xt, ps, z, B):
        """log(1 - p_s g_t sigma(x_t)): unrecorded at a sampled slot, gated by breast height (D72)"""
        if not s.age: return super().cutoff_term(xt, ps, z, B)
        lq = torch.log(ps.clamp(1e-6, 1.0)) + F.logsigmoid(KAPPA_B * s.bh_age(z, B))            # log(p_s g_t)
        return torch.logaddexp(torch.log((-torch.expm1(lq)).clamp(min=1e-12)), lq + F.logsigmoid(-xt))

@torch.no_grad()
def age_posterior(model, B, K=100, G=61, zmax=3.6):
    """Exact posterior of z0 on a grid: [n, G] probabilities. The decoder never reads z0, so the rest of the model is scored once and only the birth limits vary along the grid.
    Importance resampling is not used here: with ESS near 7 of K it moved ages by 0.24 sd where the exact answer is the prior (am. 14). lw divides by q(z0 = g), which the grid replaces, so that term goes back in."""
    assert model.age, 'the grid is over the age latent z0'
    mq, lq = M.NIMIWAE56.q(model, B); g = torch.linspace(-zmax, zmax, G, device=mq.device); rest, lqs = model.sample_z(mq, lq, K)   # flow q: z_rest from the flow (D73)
    zz = rest.clone(); zz[..., 0] = 0.0; model._cache = None; dec = model.decode(zz, B); model._cache = dict(q=(mq, lq), dec=dec)
    c2 = 0.5 * math.log(2 * math.pi); out = []
    lq_rest = lqs - (-0.5 * (rest[..., 0] - mq[:, 0]) ** 2 / torch.exp(lq[:, 0]) - 0.5 * lq[:, 0] - c2)   # exact log q(z_rest) from the draw itself: no inverse pass (D73: float32 inverse of a flat spline is off)
    try:
        for gi in g:
            z = rest.clone(); z[..., 0] = gi
            lq0 = -0.5 * (gi - mq[:, 0]) ** 2 / torch.exp(lq[:, 0]) - 0.5 * lq[:, 0] - c2
            out.append(torch.logsumexp(model.joint(B, K, z=z, lqz=lq0[None] + lq_rest)['lw'] + lq0[None], 0))
    finally:
        model._cache = None
    return g, torch.softmax(torch.stack(out, 1), 1)

def z_quantiles(g, p, q):
    """quantiles [len(q), n] of a grid posterior, each node holding its cell; piecewise linear in the cumulative probability"""
    step = g[1] - g[0]; edges = torch.cat([g[:1] - step / 2, g + step / 2]); cdf = torch.cat([torch.zeros_like(p[:, :1]), p.cumsum(1)], 1).contiguous()
    q_ = torch.as_tensor(q, dtype=cdf.dtype); i = torch.stack([torch.searchsorted(cdf, x.expand(cdf.shape[0], 1).contiguous()).clamp(1, g.shape[0])[:, 0] for x in q_])
    n_ = torch.arange(cdf.shape[0]); lo, hi = cdf[n_, i - 1], cdf[n_, i]
    return edges[i - 1] + (q_[:, None] - lo) / (hi - lo).clamp(min=1e-12) * step

def make_model(vocab, age=True, q='gauss', flow=None, height='gauss'):
    m = NIMIWAE70(len(vocab['spgrpcd']), len(vocab['epa_l3']), len(vocab['major_spgrpcd']), len(vocab['fortypcd']) + 1, len(vocab['spcd']), age=age, q=q, flow=flow, height=height)
    m.vocab = vocab; m.cfg = dict(censor='known', growth='flex', noise=False, prior_ctx=False, age=age, q=q, height=height, **({'flow': {**FLOW_DEFAULT, **(flow or {})}} if q == 'flow' else {})); return m

def model_from_cfg(vocab, cfg):
    """rebuild a saved model's architecture; checkpoints from before D73 have no 'q' and are Gaussian"""
    return make_model(vocab, age=cfg['age'], q=cfg.get('q', 'gauss'), flow=cfg.get('flow'), height=cfg.get('height', 'gauss'))

class ScheduleFreeAdamW:
    """Schedule-Free AdamW (Defazio et al. 2024, arXiv 2405.15682). Three sequences: z takes the Adam step, x is the weighted average of z and the model to evaluate,
    y = beta x + (1 - beta) z is where gradients are taken, so the parameters hold y during training. c_t = w_t / W_t, w_t = gamma_t ^ 2, gamma_t = lr with a linear warmup.
    No inner momentum, no decay schedule. Call finish() once: it leaves x in the parameters."""
    def __init__(s, params, lr=1e-3, beta=0.9, b2=0.999, eps=1e-8, wd=1e-5, warmup=100):
        s.ps = [p for p in params]; s.lr, s.beta, s.b2, s.eps, s.wd, s.warmup = lr, beta, b2, eps, wd, warmup
        s.z = [p.detach().clone() for p in s.ps]; s.x = [p.detach().clone() for p in s.ps]; s.v = [torch.zeros_like(p) for p in s.ps]; s.t, s.W = 0, 0.0
    def zero_grad(s):
        for p in s.ps: p.grad = None
    @torch.no_grad()
    def step(s):
        s.t += 1; g_ = s.lr * min(1.0, s.t / max(1, s.warmup)); w = g_ ** 2; s.W += w; c = w / s.W
        bc = 1 - s.b2 ** s.t
        for p, z, x, v in zip(s.ps, s.z, s.x, s.v):
            if p.grad is not None:
                v.mul_(s.b2).addcmul_(p.grad, p.grad, value=1 - s.b2)
                z.add_(p, alpha=-g_ * s.wd).addcdiv_(p.grad, torch.sqrt(v / bc) + s.eps, value=-g_)
            x.lerp_(z, c)
            p.copy_(s.beta * x + (1 - s.beta) * z)
    def state_dict(s):
        """everything a resumed run needs: the step count, the weight sum and the z, x and second-moment tensors"""
        return dict(t=s.t, W=s.W, z=[a.clone() for a in s.z], x=[a.clone() for a in s.x], v=[a.clone() for a in s.v])
    @torch.no_grad()
    def load_state_dict(s, d):
        assert len(d['z']) == len(s.z), 'optimizer state for another model'
        s.t, s.W = d['t'], d['W']
        for dst, src in zip(s.z + s.x + s.v, d['z'] + d['x'] + d['v']): dst.copy_(src.to(dst.device))
    @torch.no_grad()
    def finish(s):
        for p, x in zip(s.ps, s.x): p.copy_(x)
    @contextlib.contextmanager
    def averaged(s):
        """x in the parameters inside the block, y restored after it: score or save at any epoch, then keep training."""
        y = [p.detach().clone() for p in s.ps]
        with torch.no_grad():
            for p, x in zip(s.ps, s.x): p.copy_(x)
        try: yield
        finally:
            with torch.no_grad():
                for p, y_ in zip(s.ps, y): p.copy_(y_)

def save_atomic(obj, path):
    """torch.save to a temporary file, then rename: an interruption never leaves a half-written state"""
    tmp = f'{path}.tmp'; torch.save(obj, tmp); os.replace(tmp, path)

def to_dev(b, device):
    return {k: v.to(device, non_blocking=True) for k, v in b.items()}

def train(tr, epochs=12, k=32, bs=512, max_trees=0, seed=0, threads=7, lr=1e-3, log=print, age=True, device='cpu', opt_name='adamw', on_epoch=None, q='gauss', flow=None, d74=False,
          state_path=None):
    """state_path: after every completed epoch (and its hook) the full training state is saved there; if the file exists, training resumes after its
    epoch. It holds the parameters, the optimizer (and scheduler) state, the batch-order and noise RNG states and the history, plus a fingerprint of the
    setup; a different setup refuses to resume."""
    assert set(tr.split.unique()) <= {'train'}, 'validation and test plots never enter fitting'
    torch.manual_seed(seed); torch.set_num_threads(threads); rng = np.random.default_rng(seed)
    B, vocab, _ = build(tr); n = B['v'].shape[0]
    if d74: assert 'htcd' in tr, 'D74 needs HTCD in the fitting rows'                         # both arms: the height model (G6, G8 compare like with like)
    if d74 and age:                                                                          # age on: anchors and the coupling, calibrated once on every fit tree, before any subsample
        assert 'anc_agedia' in tr, 'D74 age on needs anchors in the fitting rows'
        vocab['age_coupling'] = calibrate_coupling(B, log=log); apply_coupling(B, vocab['age_coupling'])
        log(f'D74 coupling shifts {int((B["af_shift"] != 0).sum()):,} of {n:,} trees (mean shift {float(B["af_shift"][B["af_shift"] != 0].mean()):+.3f}); anchors on {int((B["anc_m"].sum(1) > 0).sum()):,} trees')
    else:
        assert float(B['anc_m'].sum()) == 0, 'anchors in the fitting rows without D74 age on'
    if max_trees and max_trees < n: B = sub(B, torch.tensor(rng.permutation(n)[:max_trees])); n = max_trees
    model = make_model(vocab, age=age, q=q, flow=flow, height='t' if d74 else 'gauss').to(device)
    phi = model.enc_params(); pid = {id(p) for p in phi}; theta = [p for p in model.parameters() if id(p) not in pid]
    nb_ep = (n + bs - 1) // bs
    if opt_name == 'sf':                                                                     # warmup = the 5 % of steps OneCycleLR uses
        opt = ScheduleFreeAdamW(model.parameters(), lr=lr, wd=1e-5, warmup=int(0.05 * epochs * nb_ep)); sched = None
    else:
        opt = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=1e-5)
        sched = torch.optim.lr_scheduler.OneCycleLR(opt, max_lr=lr, total_steps=epochs * nb_ep, pct_start=0.05, anneal_strategy='cos')
    log(f'train trees {n:,}; slots {int(B["v"].sum()):,}; prior on {int(B["af_has"].sum()):,} trees; K {k} (DReG); age {age}; q {q}{" " + json.dumps(model.cfg["flow"]) if q == "flow" else ""}; device {device}; optimizer {opt_name}')
    hist, start = [], 0
    fp = dict(backend='torch', n=n, epochs=epochs, k=k, bs=bs, seed=seed, lr=lr, age=age, q=q, cfg=model.cfg, d74=d74, opt=opt_name, coupling=vocab.get('age_coupling'))
    cuda = str(device).startswith('cuda')
    if state_path and os.path.exists(state_path):                                            # resume after the last completed epoch
        ck = torch.load(state_path, weights_only=False, map_location=device)
        assert ck['fingerprint'] == fp, f'{state_path} was saved by another setup: {ck["fingerprint"]} vs {fp}'
        model.load_state_dict(ck['model']); opt.load_state_dict(ck['opt'])
        if sched is not None: sched.load_state_dict(ck['sched'])
        rng.bit_generator.state = ck['np_rng']; torch.set_rng_state(ck['torch_rng'])
        if cuda and ck.get('cuda_rng') is not None: torch.cuda.set_rng_state(ck['cuda_rng'], device)
        hist, start = ck['hist'], ck['epoch'] + 1
        log(f'resumed after epoch {start} of {epochs} from {state_path}')
    for ep in range(start, epochs):
        t0, tot, nb = time.time(), 0.0, 0
        for ix in torch.split(torch.tensor(rng.permutation(n)), bs):
            b = to_dev(sub(B, ix), device); o = model.joint(b, k, dreg=True)
            loss_t = -(torch.logsumexp(o['lw'], 0) - math.log(k)).mean()
            wt = torch.softmax(o['lw'], 0).detach()
            loss_p = -(wt ** 2 * o['lw_dreg']).sum(0).mean()
            gt = torch.autograd.grad(loss_t, theta, retain_graph=True, allow_unused=True)
            gp = torch.autograd.grad(loss_p, phi, allow_unused=True)
            opt.zero_grad()
            for p_, g_ in list(zip(theta, gt)) + list(zip(phi, gp)): p_.grad = g_
            nn.utils.clip_grad_norm_(model.parameters(), 5.0); opt.step()
            if sched is not None: sched.step()
            tot += loss_t.item(); nb += 1
        ess = float((1 / (torch.softmax(o['lw'].detach(), 0) ** 2).sum(0)).median())
        e0 = {}
        if age:
            with torch.no_grad():
                w = torch.softmax(o['lw'], 0); z0 = o['z'][..., 0]; al = torch.exp(b['af_mu'][None] + b['af_shift'][None] + b['af_sd'][None] * b['af_sdf'][None] * z0)   # age at the prior's slot, coupled prior
                pm = (w * z0).sum(0); e0 = dict(z0_mean=pm.mean().item(), z0_abs=pm.abs().mean().item(), z0_sd=((w * z0 ** 2).sum(0) - pm ** 2).clamp(min=0).sqrt().median().item(), age_med=al.median().item(), age_max=al.max().item())
        hist.append(dict(epoch=ep, loss=tot / nb, ess_last_batch=ess, sd_disc=torch.exp(model.lsdisc).item(), **e0))
        log(json.dumps(hist[-1]) + f' {time.time() - t0:.0f}s')
        if on_epoch:                                                                         # hist entry first: the hook may add held-out numbers to it
            with (opt.averaged() if opt_name == 'sf' else contextlib.nullcontext()): on_epoch(ep + 1, model, hist[-1])
        if state_path:                                                                       # after the hook: an interrupted hook redoes its epoch
            save_atomic(dict(epoch=ep, fingerprint=fp, model=model.state_dict(), opt=opt.state_dict(), sched=sched.state_dict() if sched is not None else None,
                             np_rng=rng.bit_generator.state, torch_rng=torch.get_rng_state(), cuda_rng=torch.cuda.get_rng_state(device) if cuda else None, hist=hist), state_path)
    if opt_name == 'sf': opt.finish()                                                        # evaluate and save the averaged weights
    return model.to('cpu'), hist
