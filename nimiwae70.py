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
ARCH_DEFAULT = dict(layers=2, d=64)        # D82: transformer depth (encoder and decoder) and width; feed-forward = 2 d, 4 heads
FLOW_DEFAULT = dict(layers=6, hidden=64, bins=8, bound=5.0)

SOIL_COLS = [f'soil_{c}' for c in ('clay', 'sand', 'silt', 'soc', 'bd', 'ph', 'cec', 'depth_restriction', 'rock_frag', 'depth_bedrock')]
NSOIL = len(SOIL_COLS)
KAPPA_B = 2.0                 # per year: the birth step is about half a year wide
AF_MU0, AF_SD0 = math.log(25.0), 0.8     # trees with no usable record: wide default, not a measurement
L0 = dict(S=2.0, H=5.0)                  # years from germination to breast height when the frame has no `l0` column (frames before D78); the age targets use the same values
ITYR = COV.index('tyr')
S_ANCHOR = 0.10                          # D74: log-scale width of a ring-count breast-height age (sensitivity 0.05 / 0.20 reported)
T_NU = 4.0                               # D74: Student-t degrees of freedom for log HT residuals
RATE_FLOOR = 0.02                        # D74: in / yr; an observed DIA change at or below it reads as this rate (12 % of anchor increments are <= 0)
KNOWN_BIAS_DIA = 15.0                    # D79 (user): planted-model priors at DIA >= this are a known exception (VAL bias -1.03 yr, coverage 0.908)
COUPLE_MIN_DIA = 5.0                     # D74: no coupling below 5 in at the prior's slot (no R4 sapling has an age)
COUPLE_MIN_YEARS = 1.0                   # D74: a growth rate needs measured DIA at least this far apart
WEST_RATIO = {4: 0.61, 5: 0.43}          # D74: coupling strength of intermediate / overtopped relative to classes 1-3 (western BHAGE, 229k trees)
SAP_DELTA = -0.1689                      # D78: sapling coupling per log (in / yr), western whorl saplings (nimiwae_sapling_coupling.py; CI -0.204 to -0.135)
SAP_RATE = (0.02, 0.1518)                # D78: its calibration's 1-99 % rate range (in / yr): sapling rates are clamped to it, no extrapolation
MISS_DIA = (5.0, 7.0)                    # D76: G_MAX comes from the measured growth of trees this size (just above the subplot limit)
MISS_Q = 0.99                            # D76: G_MAX = this quantile of their annual DIA growth

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
    B['l0'] = torch.tensor(st.l0.to_numpy(float) if 'l0' in st else np.where(st.sftwd_hrdwd.to_numpy() == 'S', L0['S'], L0['H']), dtype=torch.float32)   # D78: per tree (nimiwae_l0)
    if 'af_mu' in sq:                                                                          # the first visible slot with a prior; hidden records never feed it
        hid = sq.hidden.fillna(False).to_numpy(bool) if 'hidden' in sq else np.zeros(len(sq), bool)
        ok = (sq.slot_kind == 'real').to_numpy() & ~hid & sq.af_mu.notna().to_numpy()
        pm = (sq.af_model.to_numpy(object) == 'P') if 'af_model' in sq else np.zeros(len(sq), bool)                     # D79: which direct model gave the prior
        o = pd.DataFrame(dict(t=ti[ok], p=po[ok], m=sq.af_mu.to_numpy(float)[ok], s=sq.af_sd.to_numpy(float)[ok], pm=pm[ok], d=sq.dia.to_numpy(float)[ok])).sort_values(['t', 'p']).groupby('t').first()
        mu[o.index.to_numpy()] = o.m.to_numpy(); sd[o.index.to_numpy()] = o.s.to_numpy(); la[torch.tensor(o.index.to_numpy())] = torch.tensor(o.p.to_numpy(), dtype=la.dtype)
        B['af_has'] = torch.zeros(n); B['af_has'][torch.tensor(o.index.to_numpy())] = 1.0
        B['age_known_bias'] = torch.zeros(n); B['age_known_bias'][torch.tensor(o.index.to_numpy())] = torch.tensor((o.pm.astype(bool) & (o.d >= KNOWN_BIAS_DIA)).to_numpy(), dtype=torch.float32)   # D79 flag
        B['af_P'] = torch.zeros(n); B['af_P'][torch.tensor(o.index.to_numpy())] = torch.tensor(o.pm.astype(bool).to_numpy(), dtype=torch.float32)   # D85 am. 4: the prior came from the planted model
    else:
        B['af_has'] = torch.zeros(n)
        B['age_known_bias'] = torch.zeros(n); B['af_P'] = torch.zeros(n)
    B['af_mu'], B['af_sd'], B['la'] = torch.tensor(mu), torch.tensor(sd), la
    d74_features(B, seq_cols=sq, ti=ti, po=po)
    if 'age_coupling' in vocab: apply_coupling(B, vocab['age_coupling'])
    if 'miss_eps' in vocab: apply_miss(B, vocab['miss_eps'])
    if 'miss_rho' in vocab: apply_corr_miss(B, vocab['miss_eps'], vocab['miss_rho'])            # D85: replaces the static factor by the per-draw one
    if 'age_tail' in vocab: apply_tail(B, vocab['age_tail'])                                     # D85
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
    B['miss_keep'] = torch.ones(n, T)                                                           # D76: 1 - eps where a not-yet-listed tree can be missed
    B['age_sl'] = torch.ones(n)                                                                  # D85: lower-side width factor of the age prior (1 = Gaussian)
    sw = np.zeros(n, np.float32)
    if 'sftwd_hrdwd' in seq_cols: sw[ti] = (seq_cols.sftwd_hrdwd.to_numpy(object) == 'S').astype(np.float32)
    B['softwood'] = torch.tensor(sw)                                                             # D85: wood type (rho calibration fallback rates)

def pre_debut_subplot(B):
    """[n, T] slots before the first visible record whose tally limit is above the existence level: where an unlisted tree on a sampled spot
    has no label and only the "sampled, no record" term speaks (D76)"""
    ar = torch.arange(B['v'].shape[1], device=B['v'].device)[None]
    return (ar < B['f'][:, None]) & (B['llim'] > B['lex'] + 1e-6) & (B['v'] > 0)

def apply_miss(B, eps):
    """D76: P(no record) = 1 - p_s (1 - eps) g sigma(x) at pre-debut subplot-level slots, i.e. the term's p_s scaled by miss_keep = 1 - eps there"""
    B['miss_keep'] = torch.where(pre_debut_subplot(B), torch.full_like(B['v'], 1.0 - eps), torch.ones_like(B['v']))

def calibrate_miss(B, log=print):
    """D76: eps, the rate at which a not-yet-listed, tally-size tree on a fully sampled spot goes unrecorded without a code, fixed from the data
    in B (fit plots) before training. G_MAX = the MISS_Q quantile of annual DIA growth between consecutive measured visible records of trees
    MISS_DIA in at the first of them. eps = the share of uncoded subplot-level debuts after a fully sampled earlier visit whose growth above that
    visit's limit, per year since it, exceeds G_MAX (they could not have been below the limit then: a miss)."""
    n, T = B['v'].shape; t = B['cov'][..., ITYR] * 20.0; D = torch.exp(B['lD']); m = B['mD'] > 0; ii = torch.arange(n)
    rates = []                                                                                  # consecutive measured visible records
    for j in range(T - 1):
        nxt = torch.full((n,), -1, dtype=torch.long)
        for jj in range(T - 1, j, -1): nxt = torch.where(m[:, jj], torch.full_like(nxt, jj), nxt)
        ok = m[:, j] & (nxt > j) & (D[:, j] >= MISS_DIA[0]) & (D[:, j] < MISS_DIA[1]); nj = nxt.clamp(min=0)
        dt = t[ii, nj] - t[:, j]; ok &= dt >= 1.0
        rates.append(((D[ii, nj] - D[:, j]) / dt.clamp(min=1.0))[ok])
    r = torch.cat(rates); assert len(r) >= 1000, f'too few growth pairs for G_MAX: {len(r)}'
    gmax = float(torch.quantile(r.double(), MISS_Q))
    f = B['f']; fp = (f - 1).clamp(min=0)
    prev = (f > 0) & (B['sampled'][ii, fp] > 0) & (B['psamp'][ii, fp] >= 1) & (B['real'][ii, fp] == 0) & (B['llim'][ii, fp] > B['lex'][ii, fp] + 1e-6)
    uncoded = prev & (B['mode'][ii, fp] == M.MODE['cutoff'])                                    # coded misses (3 / 6 / 7) carry the missed mode instead
    lim = torch.exp(B['llim'][ii, fp]) + M.HALF_UNIT; dt = (t[ii, f] - t[ii, fp]).clamp(min=0.5)
    implausible = uncoded & (B['mD'][ii, f] > 0) & ((D[ii, f] - lim) / dt > gmax)
    nu = int(uncoded.sum()); assert nu >= 500, f'too few uncoded subplot-level debuts to calibrate eps: {nu}'
    eps = float(implausible.sum()) / nu
    log(f'D76 crew-miss rate calibrated: G_MAX {gmax:.3f} in / yr (q{MISS_Q} of {len(r):,} growth pairs of {MISS_DIA[0]}-{MISS_DIA[1]} in trees); '
        f'{int(implausible.sum()):,} of {nu:,} uncoded subplot-level debuts need more: eps {eps:.4f}')
    return eps

def z_age(z0, B):
    """D85: the age-scale z of the latent z0, sigma_L z0 below the prior median (B['age_sl'] per tree, 1 when absent)"""
    sl = B['age_sl'] if 'age_sl' in B else torch.ones_like(z0[0] if z0.dim() > 1 else z0)
    while sl.dim() < z0.dim(): sl = sl[None]
    return torch.where(z0 < 0, sl * z0, z0)

def apply_tail(B, sl):
    """D85: the lower-side width factor per tree. sl is one value for every tree (the D85 test) or, am. 4, {'N': .., 'P': ..} per direct model:
    trees whose prior came from the planted model take 'P', natural-model priors 'N', and trees without a prior keep 1 (their default prior is not
    what the widths were calibrated on)"""
    if isinstance(sl, dict):
        P = B['af_P'] if 'af_P' in B else torch.zeros_like(B['af_mu']); H = B['af_has'] if 'af_has' in B else torch.ones_like(B['af_mu'])
        B['age_sl'] = torch.where(H > 0, torch.where(P > 0, torch.full_like(B['af_mu'], float(sl['P'])), torch.full_like(B['af_mu'], float(sl['N']))), torch.ones_like(B['af_mu']))
    else: B['age_sl'] = torch.full_like(B['af_mu'], float(sl))

TAIL_MIN = 200                                                                                  # D85: anchors below the prior median a width needs

def calibrate_tail(B, log=print):
    """D85 (am. 4: per direct model): sigma_L from the anchors in B (fit plots), against the coupled prior at the prior's slot: the half-normal fit
    sqrt(mean z^2) of the anchors below the prior median, clipped to [0.5, 1]; total age at la from each anchor as calibrate_coupling. One width per
    direct model, 'N' (natural-model priors) and 'P' (planted-model priors, B['af_P']), anchors of trees with a prior only; a model with fewer than TAIL_MIN
    anchors below its median takes the pooled width (logged). The upper side is reported, not applied. Returns {'N': .., 'P': ..}."""
    t = B['cov'][..., ITYR] * 20.0; li = B['la'][:, None]; ti, pj = torch.nonzero(B['anc_m'] > 0, as_tuple=True)
    keep = B['af_has'][ti] > 0 if 'af_has' in B else torch.ones_like(ti, dtype=torch.bool); ti, pj = ti[keep], pj[keep]   # a tree without a prior sits on the default, not a measurement
    tot = torch.exp(B['anc_y'][ti, pj]) + B['l0'][ti] - (t[ti, pj] - t.gather(1, li)[ti, 0]); ok = tot > 0.5; ti, tot = ti[ok], tot[ok]
    z = ((torch.log(tot) - B['af_mu'][ti] - B['af_shift'][ti]) / (B['af_sd'][ti] * B['af_sdf'][ti])).double()
    P = (B['af_P'][ti] > 0) if 'af_P' in B else torch.zeros_like(z, dtype=torch.bool)
    width = lambda zz: float(min(1.0, max(0.5, torch.sqrt((zz[zz < 0] ** 2).mean()))))
    assert int((z < 0).sum()) >= TAIL_MIN, f'too few anchors below the prior median to calibrate the lower tail: {int((z < 0).sum())}'
    pooled, out = width(z), {}
    for k, sel in (('N', ~P), ('P', P)):
        zk = z[sel]; nlo = int((zk < 0).sum())
        out[k] = width(zk) if nlo >= TAIL_MIN else pooled
        log(f'D85 lower tail, {k} model: {len(zk):,} anchor rows, {nlo:,} below the prior median' + (f': sigma_L {out[k]:.3f}' if nlo >= TAIL_MIN else f' (< {TAIL_MIN}): the pooled sigma_L {pooled:.3f}')
            + (f'; upper side {float(torch.sqrt((zk[zk >= 0] ** 2).mean())):.3f} (not applied)' if int((zk >= 0).sum()) else ''))
    log(f'D85 lower tail: sigma_L N {out["N"]:.3f}, P {out["P"]:.3f} (pooled {pooled:.3f}, {len(z):,} anchor rows)')
    return out

def apply_corr_miss(B, eps, rho):
    """D85: correlated misses at the D76 slots (pre-debut, subplot level, cutoff mode): the static factor goes, the cutoff term builds 1 - eps_t per draw"""
    B['miss_keep'] = torch.ones_like(B['v'])
    B['miss_elig'] = (pre_debut_subplot(B) & (B['mode'] == M.MODE['cutoff']) & (B['sampled'] > 0)).float()
    B['miss_eps_t'] = torch.full_like(B['af_mu'], float(eps)); B['miss_rho_t'] = torch.full_like(B['af_mu'], float(rho))

def corr_keep(ps, lg, xt, B):
    """D85 (am. 1): [K, n, T] keep factor 1 - eps_t at the eligible slots (1 elsewhere), eps_t = eps + (rho - eps) Q_t, where Q_t is the probability,
    given the absences at the earlier eligible slots, that the tree was already missed at one of them. At slot s the tree is present, past breast height
    and tally-size with q_s = p_s g_s sigma(x_s); given that it was unrecorded there, it was missed with m_s = q_s eps_s / (1 - q_s (1 - eps_s)) (Bayes:
    a missed tree, against one not there yet); Q_{t+1} = 1 - (1 - Q_t)(1 - m_t). m_s -> 1 when the tree was surely there, -> 0 when it surely was not."""
    el = B['miss_elig'][None]; q = (ps.clamp(0.0, 1.0) * torch.exp(lg) * torch.sigmoid(xt) * el).clamp(max=1 - 1e-6)
    eps, rho = B['miss_eps_t'][None], B['miss_rho_t'][None]; Q = torch.zeros_like(q[..., 0]); keep = []
    for t in range(q.shape[-1]):                                                              # T_MAX slots: sequential, eps_t depends on the earlier misses
        e = eps + (rho - eps) * Q; elt = el[..., t] > 0
        keep.append(torch.where(elt, 1 - e, torch.ones_like(e)))
        m = q[..., t] * e / (1 - q[..., t] * (1 - e)).clamp(min=1e-12)
        Q = torch.where(elt, 1 - (1 - Q) * (1 - m), Q)
    return torch.stack(keep, -1)

def calibrate_rho(B, log=print):
    """D85: rho from the coded misses in B (fit plots): trees with the missed mode at the visit before a debut >= 5 in at subplot level; their sampled
    pre-debut subplot-level visits at which the diameter, backcast from the debut with the tree's observed rate (else its wood type's median), was at or
    above the visit's limit are tally-size empty visits; rho = N(>= 2) / N(>= 1) (a tree missed at its first tally-size visit is recorded or missed again)"""
    n, T = B['v'].shape; ii = torch.arange(n); f = B['f']; fp = (f - 1).clamp(min=0); t = B['cov'][..., ITYR] * 20.0; D = torch.exp(B['lD'])
    coded = (f > 0) & (B['mode'][ii, fp] == M.MODE['missed']) & (B['mD'][ii, f] > 0) & (D[ii, f] >= 5.0) & (B['llim'][ii, fp] > B['lex'][ii, fp] + 1e-6)
    rate = torch.exp(B['lrate']); has = B['has_rate'] > 0
    med = {w: float(rate[coded & has & (B['softwood'] == w)].median()) if bool((coded & has & (B['softwood'] == w)).any()) else 0.2 for w in (0.0, 1.0)}
    r = torch.where(has, rate, torch.where(B['softwood'] > 0, torch.tensor(med[1.0]), torch.tensor(med[0.0])))
    ar = torch.arange(T)[None]; slot = (ar < f[:, None]) & (B['v'] > 0) & (B['sampled'] > 0) & (B['llim'] > B['lex'] + 1e-6)
    back = D[ii, f][:, None] - r[:, None] * (t[ii, f][:, None] - t); lim = torch.exp(B['llim']) + M.HALF_UNIT
    k = (slot & (back >= lim)).sum(1)[coded]; n1, n2 = int((k >= 1).sum()), int((k >= 2).sum())
    assert n1 >= 500, f'too few coded misses with a tally-size empty visit to calibrate rho: {n1}'
    rho = n2 / n1
    log(f'D85 repeated-miss rate calibrated on {int(coded.sum()):,} coded misses: {n2:,} of {n1:,} with >= 1 tally-size empty visit had >= 2: rho {rho:.3f} (fallback rates {med})')
    return rho

def coupling_eligible(B):
    """trees the coupling applies to: a prior, an observed growth rate, DIA >= 5 in and a crown class at the prior's slot"""
    return (B['af_has'] > 0) & (B['has_rate'] > 0) & (B['dia_la'] >= COUPLE_MIN_DIA) & (B['cc_la'] >= 1) & (B['cc_la'] <= 5)

def sapling_eligible(B):
    """D78: saplings the sapling coupling applies to: a prior, an observed growth rate, 0 < DIA < 5 in at the prior's slot"""
    return (B['af_has'] > 0) & (B['has_rate'] > 0) & (B['dia_la'] > 0) & (B['dia_la'] < COUPLE_MIN_DIA)

def sapling_x(B, cp):
    """the clamped log rate the sapling coupling reads"""
    return B['lrate'].clamp(cp['sapling']['lo'], cp['sapling']['hi'])

def apply_coupling(B, cp):
    """log a_l = af_mu + delta_c (lrate - m_c) + af_sd w_c z0 on eligible trees; others unchanged. D78: with cp['sapling'], saplings get
    delta_s (clamp(lrate) - m_s) and no width change"""
    el = coupling_eligible(B); c = B['cc_la'].clamp(0, 5)
    dl = torch.tensor([0.0] + [cp['delta'][str(k)] for k in range(1, 6)]); mm = torch.tensor([0.0] + [cp['m'][str(k)] for k in range(1, 6)]); ww = torch.tensor([1.0] + [cp['w'][str(k)] for k in range(1, 6)])
    B['af_shift'] = torch.where(el, dl[c] * (B['lrate'] - mm[c]), torch.zeros_like(B['af_mu']))
    B['af_sdf'] = torch.where(el, ww[c], torch.ones_like(B['af_sd']))
    if 'sapling' in cp:
        es = sapling_eligible(B); sp = cp['sapling']
        B['af_shift'] = torch.where(es, sp['delta'] * (sapling_x(B, cp) - sp['m']), B['af_shift'])

def calibrate_coupling(B, log=print, sapling=False):
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
    if sapling:                                                                         # D78: fixed delta from the western test; m_s from these fit trees
        cs = dict(sapling=dict(delta=SAP_DELTA, lo=math.log(SAP_RATE[0]), hi=math.log(SAP_RATE[1]), m=0.0)); es = sapling_eligible(B)
        assert int(es.sum()) >= 200, f'too few saplings with a rate: {int(es.sum())}'
        cs['sapling']['m'] = float(sapling_x(B, cs)[es].double().mean()); cs['sapling']['n'] = int(es.sum()); cp |= cs
        log(f'D78 sapling coupling: delta {SAP_DELTA:+.4f}, rate clamped to {SAP_RATE}, m_s {cs["sapling"]["m"]:.3f} over {cs["sapling"]["n"]:,} saplings')
    log(f'D74 coupling calibrated on {cp["n_anchor_rows"]:,} anchor rows ({cp["n_anchor_trees"]:,} trees): delta {d:+.3f} per log (in / yr), width factor {w:.3f}, '
        f'intercept {b[0]:+.3f}; m by class {", ".join(f"{k}: {v:.2f}" for k, v in cp["m"].items())}')
    return cp

class Static70(M.Static):
    """Soil joins the statics of the prior and the decoder. The encoder also reads the age prior, so it knows how close a tree sits to its birth limits."""
    def __init__(s, nsp, nl3, nmaj, nfty, nspc, use_summ=True, use_af=False, d=D_MODEL):
        super().__init__(nsp, nl3, nmaj, nfty, nspc, use_summ, d=d); s.use_af = use_af
        s.out = nn.Linear(s.out.in_features + NSOIL + (2 if use_af else 0), d)
    def forward(s, B):
        x = [s.e[0](B['sp']), s.e[1](B['l3']), s.e[2](B['maj']), s.e[3](B['fty']), s.e[4](B['scl']), s.e[5](B['phy']), s.e[6](B['spc']), B['snum'], B['soil']]
        if s.use_af: x.append(torch.stack([B['af_mu'] + B.get('af_shift', 0.0) - AF_MU0, B['af_sd'] * B.get('af_sdf', 1.0)], -1))   # D74: the prior the model uses
        return s.out(torch.cat(x + ([B['summ']] if s.use_summ else []), -1))

class NIMIWAE70(M.NIMIWAE56):
    build = staticmethod(lambda seq, vocab: build(seq, vocab))                             # predictions and stand totals use the joint build
    def __init__(s, nsp, nl3, nmaj, nfty, nspc, age=True, q='gauss', flow=None, height='gauss', arch=None):
        ar = {**ARCH_DEFAULT, **(arch or {})}                                                  # D82: width d and transformer depth
        super().__init__(nsp, nl3, nmaj, nfty, nspc, censor='known', growth='flex', noise=False, prior_ctx=False, d_model=ar['d'], layers=ar['layers'])
        assert q in ('gauss', 'flow') and height in ('gauss', 't')
        s.age, s.qfam, s.height = age, q, height
        if height == 't': s.lsHc = nn.Parameter(torch.full((2,), -2.0))                       # D74: log scale per height method (1 / null, 2-3)
        s.st_q = Static70(nsp, nl3, nmaj, nfty, nspc, use_af=age, d=ar['d']); s.st_p = Static70(nsp, nl3, nmaj, nfty, nspc, use_summ=False, d=ar['d'])
        if q == 'flow':                                                                       # D73: z0 (age) stays Gaussian and separate; age off, every dimension flows
            fc = {**FLOW_DEFAULT, **(flow or {})}; s.f0 = 1 if age else 0
            s.flow = NF.CondFlow(DZ - s.f0, ar['d'], layers=fc['layers'], hidden=fc['hidden'], bins=fc['bins'], B=fc['bound'])   # context = the encoder token
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
    def posterior_grid(s, b, K):                                                              # impute(..., age='grid')
        return posterior_grid(s, b, K)
    def lineage_draws_grid(s, b, K, n_draws):                                                 # sample_lineages(..., age='grid')
        return lineage_draws_grid(s, b, K, n_draws)
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
        al = torch.exp((mu[None] + sd[None] * z_age(z[..., 0], B)).clamp(max=7.0))                # [K, n]; up to about 1100 yr; D85 lower tail
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
        """log(1 - p_s g_t sigma(x_t)): unrecorded at a sampled slot, gated by breast height (D72). D85: with B['miss_rho_t'], p_s carries the per-draw
        keep factor 1 - eps_t at the eligible slots, eps_t = eps + (rho - eps) Q_t (corr_keep)"""
        lg = F.logsigmoid(KAPPA_B * s.bh_age(z, B)) if s.age else torch.zeros_like(xt)
        if 'miss_rho_t' in B: ps = ps * corr_keep(ps, lg, xt, B)
        if not s.age: return super().cutoff_term(xt, ps, z, B)
        lq = torch.log(ps.clamp(1e-6, 1.0)) + lg                                                   # log(p_s g_t)
        return torch.logaddexp(torch.log((-torch.expm1(lq)).clamp(min=1e-12)), lq + F.logsigmoid(-xt))

SCORE_GRID = dict(G=61, zmax=3.6)       # age_posterior: the grid the CV scored G1 / G5 on (kept for comparability across runs)
PRODUCT_GRID = dict(G=135, zmax=8.04)   # products: the same 0.12 spacing (16.08 / 134), wide enough that no held tree leaves mass at the edges (D70 am. 18)

class AgeGrid:
    """The exact grid over the age latent z0 (D70 am. 14) for one batch, shared by age_posterior, posterior_grid and lineage_draws_grid.

    The decoder never reads z0, so the rest of z is drawn once (K draws from q), the decoder runs once on those draws and is cached, and only the
    cheap age terms (birth gate, gated cut-off term, anchors) are re-scored at each of the G grid values of z0. joint(gi) scores the draws with
    z0 = g[gi]; its 'lwg' = log p(x, z0 = g, rest_k) - log q(rest_k): lw divides by q(z0 = g, rest_k), and the grid replaces the z0 draw, so
    log q(z0 = g) goes back in. A posterior over (g, k) is then softmax(lwg) over both axes (a Riemann sum over the uniform grid).
    Use as a context manager: the decoder cache is always cleared on exit."""
    def __init__(s, model, B, K=100, G=61, zmax=3.6):
        assert model.age, 'the grid is over the age latent z0'
        s.m, s.B, s.K = model, B, K
        s.mq, s.lq = M.NIMIWAE56.q(model, B); s.g = torch.linspace(-zmax, zmax, G, device=s.mq.device)
        s.rest, lqs = model.sample_z(s.mq, s.lq, K)                                           # flow q: z_rest from the flow (D73)
        s.c2 = 0.5 * math.log(2 * math.pi)
        s.lq_rest = lqs - s._lq0(s.rest[..., 0])                                              # exact log q(z_rest) of the draws: no inverse pass (D73)
    def _lq0(s, x):
        return -0.5 * (x - s.mq[:, 0]) ** 2 / torch.exp(s.lq[:, 0]) - 0.5 * s.lq[:, 0] - s.c2
    def __enter__(s):
        zz = s.rest.clone(); zz[..., 0] = 0.0; s.m._cache = None
        s.m._cache = dict(q=(s.mq, s.lq), dec=s.m.decode(zz, s.B)); return s
    def __exit__(s, *exc):
        s.m._cache = None
    def joint(s, gi):
        z = s.rest.clone(); z[..., 0] = s.g[gi]; lq0 = s._lq0(s.g[gi])
        o = s.m.joint(s.B, s.K, z=z, lqz=lq0[None] + s.lq_rest); o['lwg'] = o['lw'] + lq0[None]; return o
    def log_weights(s):
        """[G, K, n] log weights of every (grid age, draw of the rest) pair"""
        return torch.stack([s.joint(gi)['lwg'] for gi in range(len(s.g))])

@torch.no_grad()
def age_posterior(model, B, K=100, G=SCORE_GRID['G'], zmax=SCORE_GRID['zmax']):
    """Exact posterior of z0 on a grid: [n, G] probabilities. The decoder never reads z0, so the rest of the model is scored once and only the birth limits vary along the grid.
    Importance resampling is not used here: with ESS near 7 of K it moved ages by 0.24 sd where the exact answer is the prior (am. 14)."""
    with AgeGrid(model, B, K, G, zmax) as ag:
        return ag.g, torch.softmax(torch.logsumexp(ag.log_weights(), 1).T, 1)

@torch.no_grad()
def posterior_grid(model, b, K=100, G=PRODUCT_GRID['G'], zmax=PRODUCT_GRID['zmax']):
    """impute's posterior summaries (nimiwae56.posterior_draws) with z0 integrated on the exact grid:
    W[k, e, f] = sum_g P(g, k | x) P(e, f | g, k, x) and WaP1[k, t] = sum_g sum_e P(g, k | x) P(e | g, k, x) alive(e, t) P(exists_t | g, k).
    The sizes D, H depend on k only (the decoder never reads z0). Two passes over the grid: weights first, then the accumulation."""
    with AgeGrid(model, b, K, G, zmax) as ag:
        L = ag.log_weights(); Gn, Kk, n = L.shape
        P = torch.softmax(L.reshape(Gn * Kk, n), 0).reshape(Gn, Kk, n)                         # P(g, k | x) per tree
        am = M.alive_mask(b['v']); W = WaP1 = 0.0
        for gi in range(Gn):
            o = ag.joint(gi); post = torch.exp(o['lpost']) * P[gi][..., None, None]              # [K, n, E, 2]
            W = W + post; WaP1 = WaP1 + (post.sum(-1)[..., None] * am).sum(2) * o['p1']
        return dict(W=W, WaP1=WaP1, D=o['D'], H=o['H'], lsdisc=o['lsdisc'])

@torch.no_grad()
def lineage_draws_grid(model, b, K=100, n_draws=10, G=PRODUCT_GRID['G'], zmax=PRODUCT_GRID['zmax']):
    """nimiwae56.lineage_draws with z0 on the exact grid: each draw picks (grid age g, draw k of the rest) from P(g, k | x), then (e, f) from
    P(e, f | g, k, x). Its existence (birth gate at g, size path of k) and its ages come from that one draw."""
    with AgeGrid(model, b, K, G, zmax) as ag:
        L = ag.log_weights(); Gn, Kk, n = L.shape; T = T_MAX
        idx = torch.multinomial(torch.softmax(L.reshape(Gn * Kk, n), 0).T + 1e-30, n_draws, replacement=True)   # [n, d] over (g, k)
        gi_d, k_d = idx // Kk, idx % Kk; nidx = torch.arange(n, device=idx.device)[:, None].expand(-1, n_draws)
        e_ = torch.full((n, n_draws), -1, dtype=torch.long, device=idx.device); f_ = e_.clone(); born = torch.zeros(n, n_draws, T, dtype=torch.bool, device=idx.device)
        for gi in torch.unique(gi_d).tolist():
            o = ag.joint(gi); sel = gi_d == gi
            post = torch.exp(o['lpost'])[k_d, nidx][sel]                                          # [m, E, 2]
            pick = torch.multinomial(post.reshape(post.shape[0], -1) + 1e-30, 1)[:, 0]
            e_[sel], f_[sel] = pick // 2, pick % 2; born[sel] = o['born'][k_d, nidx][sel]; D, H = o['D'], o['H']
        z = torch.zeros(n_draws, n, DZ, device=idx.device); z[..., 0] = ag.g[gi_d].T            # only z0 enters ages()
        return dict(Dp=D[k_d, nidx], Hp=H[k_d, nidx], e=e_, f=f_, born=born, age=model.ages(z, b).permute(1, 0, 2), g=ag.g[gi_d])

def z_quantiles(g, p, q):
    """quantiles [len(q), n] of a grid posterior, each node holding its cell; piecewise linear in the cumulative probability"""
    step = g[1] - g[0]; edges = torch.cat([g[:1] - step / 2, g + step / 2]); cdf = torch.cat([torch.zeros_like(p[:, :1]), p.cumsum(1)], 1).contiguous()
    q_ = torch.as_tensor(q, dtype=cdf.dtype); i = torch.stack([torch.searchsorted(cdf, x.expand(cdf.shape[0], 1).contiguous()).clamp(1, g.shape[0])[:, 0] for x in q_])
    n_ = torch.arange(cdf.shape[0]); lo, hi = cdf[n_, i - 1], cdf[n_, i]
    return edges[i - 1] + (q_[:, None] - lo) / (hi - lo).clamp(min=1e-12) * step

def arch_of(arch):
    """D82: the architecture to record, None for the default (so default models, checkpoints and fingerprints are those of before D82)"""
    ar = {**ARCH_DEFAULT, **(arch or {})}
    return None if ar == ARCH_DEFAULT else ar

def make_model(vocab, age=True, q='gauss', flow=None, height='gauss', arch=None):
    m = NIMIWAE70(len(vocab['spgrpcd']), len(vocab['epa_l3']), len(vocab['major_spgrpcd']), len(vocab['fortypcd']) + 1, len(vocab['spcd']), age=age, q=q, flow=flow, height=height, arch=arch)
    m.vocab = vocab; m.cfg = dict(censor='known', growth='flex', noise=False, prior_ctx=False, age=age, q=q, height=height, **({'flow': {**FLOW_DEFAULT, **(flow or {})}} if q == 'flow' else {}),
                                  **({'arch': arch_of(arch)} if arch_of(arch) else {})); return m

def model_from_cfg(vocab, cfg):
    """rebuild a saved model's architecture; checkpoints from before D73 have no 'q' and are Gaussian"""
    return make_model(vocab, age=cfg['age'], q=cfg.get('q', 'gauss'), flow=cfg.get('flow'), height=cfg.get('height', 'gauss'), arch=cfg.get('arch'))

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
          state_path=None, d76=False, sapling=False, arch=None, d85=False):
    """state_path: after every completed epoch (and its hook) the full training state is saved there; if the file exists, training resumes after its
    epoch. It holds the parameters, the optimizer (and scheduler) state, the batch-order and noise RNG states and the history, plus a fingerprint of the
    setup; a different setup refuses to resume."""
    assert set(tr.split.unique()) <= {'train'}, 'validation and test plots never enter fitting'
    torch.manual_seed(seed); torch.set_num_threads(threads); rng = np.random.default_rng(seed)
    B, vocab, _ = build(tr); n = B['v'].shape[0]
    if d74: assert 'htcd' in tr, 'D74 needs HTCD in the fitting rows'                         # both arms: the height model (G6, G8 compare like with like)
    if d74 and age:                                                                          # age on: anchors and the coupling, calibrated once on every fit tree, before any subsample
        assert 'anc_agedia' in tr, 'D74 age on needs anchors in the fitting rows'
        vocab['age_coupling'] = calibrate_coupling(B, log=log, sapling=sapling); apply_coupling(B, vocab['age_coupling'])
        log(f'D74 coupling shifts {int((B["af_shift"] != 0).sum()):,} of {n:,} trees (mean shift {float(B["af_shift"][B["af_shift"] != 0].mean()):+.3f}); anchors on {int((B["anc_m"].sum(1) > 0).sum()):,} trees')
    else:
        assert float(B['anc_m'].sum()) == 0, 'anchors in the fitting rows without D74 age on'
    if d76:                                                                                  # both arms: the crew-miss rate, fixed from the fit plots before any subsample
        vocab['miss_eps'] = calibrate_miss(B, log=log); apply_miss(B, vocab['miss_eps'])
    if d85:                                                                                  # D85, before any subsample: correlated misses (both arms), the lower tail (age on)
        assert d76, 'D85 builds on D76 (eps)'
        vocab['miss_rho'] = calibrate_rho(B, log=log); apply_corr_miss(B, vocab['miss_eps'], vocab['miss_rho'])
        if age: assert d74, 'the D85 lower tail is calibrated on the D74 anchors'; vocab['age_tail'] = calibrate_tail(B, log=log); apply_tail(B, vocab['age_tail'])
    if max_trees and max_trees < n: B = sub(B, torch.tensor(rng.permutation(n)[:max_trees])); n = max_trees
    model = make_model(vocab, age=age, q=q, flow=flow, height='t' if d74 else 'gauss', arch=arch).to(device)   # D82: arch None = the default network
    phi = model.enc_params(); pid = {id(p) for p in phi}; theta = [p for p in model.parameters() if id(p) not in pid]
    nb_ep = (n + bs - 1) // bs
    if opt_name == 'sf':                                                                     # warmup = the 5 % of steps OneCycleLR uses
        opt = ScheduleFreeAdamW(model.parameters(), lr=lr, wd=1e-5, warmup=int(0.05 * epochs * nb_ep)); sched = None
    else:
        opt = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=1e-5)
        sched = torch.optim.lr_scheduler.OneCycleLR(opt, max_lr=lr, total_steps=epochs * nb_ep, pct_start=0.05, anneal_strategy='cos')
    log(f'train trees {n:,}; slots {int(B["v"].sum()):,}; prior on {int(B["af_has"].sum()):,} trees; K {k} (DReG); age {age}; q {q}{" " + json.dumps(model.cfg["flow"]) if q == "flow" else ""}; device {device}; optimizer {opt_name}')
    hist, start = [], 0
    fp = dict(backend='torch', n=n, epochs=epochs, k=k, bs=bs, seed=seed, lr=lr, age=age, q=q, cfg=model.cfg, d74=d74, opt=opt_name, coupling=vocab.get('age_coupling'), miss_eps=vocab.get('miss_eps'), **{x: vocab[x] for x in ('miss_rho', 'age_tail') if x in vocab})   # D85 only when on: older setups keep their fingerprints
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
        if cuda: torch.cuda.reset_peak_memory_stats(device)                                 # D82: gpu_mem_gb = this epoch's training peak, not the scoring hook's
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
                w = torch.softmax(o['lw'], 0); z0 = o['z'][..., 0]; al = torch.exp(b['af_mu'][None] + b['af_shift'][None] + b['af_sd'][None] * b['af_sdf'][None] * z_age(z0, b))   # age at the prior's slot, coupled prior
                pm = (w * z0).sum(0); e0 = dict(z0_mean=pm.mean().item(), z0_abs=pm.abs().mean().item(), z0_sd=((w * z0 ** 2).sum(0) - pm ** 2).clamp(min=0).sqrt().median().item(), age_med=al.median().item(), age_max=al.max().item())
        hist.append(dict(epoch=ep, loss=tot / nb, ess_last_batch=ess, sd_disc=torch.exp(model.lsdisc).item(), **e0, seconds=round(time.time() - t0, 1), steps=nb,
                         **({'gpu_mem_gb': round(torch.cuda.max_memory_allocated(device) / 1e9, 2)} if cuda else {})))   # D82: cost per epoch
        log(json.dumps(hist[-1]) + f' {time.time() - t0:.0f}s')
        if on_epoch:                                                                         # hist entry first: the hook may add held-out numbers to it
            with (opt.averaged() if opt_name == 'sf' else contextlib.nullcontext()): on_epoch(ep + 1, model, hist[-1])
        if state_path:                                                                       # after the hook: an interrupted hook redoes its epoch
            save_atomic(dict(epoch=ep, fingerprint=fp, model=model.state_dict(), opt=opt.state_dict(), sched=sched.state_dict() if sched is not None else None,
                             np_rng=rng.bit_generator.state, torch_rng=torch.get_rng_state(), cuda_rng=torch.cuda.get_rng_state(device) if cuda else None, hist=hist), state_path)
    if opt_name == 'sf': opt.finish()                                                        # evaluate and save the averaged weights
    return model.to('cpu'), hist
