"""D53 — NIMIWAE phantom backfill with an explicit EXISTENCE state, no age, bidirectional transformers, pure IWAE objective (DECISIONS.md D53).

Per tree: every clean visit of its subplot up to its last real record (slot_kind real / pre / gap; <= 6 slots).
Latents  z ~ p(z | static covariates)  (conditional prior: species, ecoregion, site, climate normals, forest type, planted; NO age)
         τ ∈ {0 .. f}: the first slot at which the tree exists (f = first real slot), p(τ | z, covariates) a discrete-time hazard;
         E_t = 1[t >= τ] (exactly one flip), summed out EXACTLY.
Decoder  a transformer over the slots on (z, always-known covariates, time) — never the tree's own measurements — giving softplus
         increments of DIA and HT cumulated from τ (size 0 before τ, HT starts at 4.5 ft): monotone by construction; crown ratio / class.
Obs      log DIA ~ N(path, sqrt(σ_meas(DIA)² + σ_disc²)) (σ_meas fixed at FIA precision, σ_disc learned + monitored); log HT ~ N(path, σ_H);
         CR ~ N, CCLCD ~ categorical; log AGB ~ allometry(MEASURED DIA, HT) (D51 am. 5).
Missing  real: E = 1, P(tallied | D) = (1 − ε) σ(κ (log D − log limit));  unrecorded & sampled: E = 0, or E = 1 and below the limit / missed;
         missed-live (RECONCILECD 3) previous slot: E = 1 and D >= limit;  ingrowth (RECONCILECD 1) previous slot: E = 0 or below (no miss);
         known-interval slots (benchmark): E = 1 and D in [lo, limit);  unsampled: no information.
Bound    log p(x_o, m) >= E_q[log (1/K) Σ_k p(x_o, m | z_k) p(z_k) / q(z_k)],  p(x_o, m | z) = Σ_τ p(τ | z) p(x_o, m | z, τ).
Outputs  per slot: P(E_t); weighted posterior MEDIAN of the unconditional DIA / HT / AGB (0 if not existing; monotone); mean AGB; 80 % interval.
"""
import math, time, json
import numpy as np, pandas as pd, torch, torch.nn as nn, torch.nn.functional as F

T_MAX, DZ, D_MODEL = 6, 16, 64
KAPPA = 20.0
KNOTS = torch.log(torch.tensor([0.5, 1.0, 2.0, 3.0, 5.0]))
K_ = ['statecd', 'unitcd', 'countycd', 'plot', 'subp', 'tree']
NORM_COLS = ('sicond', 'slope', 'tm_n', 'pp_n', 'vp_n')
COV = ['comp', 'stems', 'rls', 'tma', 'ppa', 'vpa', 'nbs', 'mnbs', 'nbp', 'mnbp', 'nbn', 'bal', 'alk', 'can', 'mcan', 'ssz', 'tyr', 'dt']
NCOV = len(COV)
NEG = -1e4

def ls_dia(lD):
    """Fixed FIA DIA measurement sd on the log scale (D52): sqrt(0.02² + (0.06 in / DIA)²)."""
    return 0.5 * torch.log(0.02 ** 2 + (0.06 / torch.exp(lD)) ** 2)

# ------------------------------------------------------------------------------------------------------------------------------- data
def build(seq: pd.DataFrame, vocab=None):
    """Padded per-tree tensors [n, T_MAX]. `seq` rows = slots; columns from nimiwae_prep (D53 frame). Optional benchmark columns:
    hidden (real visit hidden), limit_in (per-slot tally limit override), lo_in (known lower bound), e_known (bool override)."""
    seq = seq.sort_values(K_ + ['measdate']).copy()
    if vocab is None:
        vocab = {c: {v: i + 1 for i, v in enumerate(sorted(seq[c].dropna().unique()))} for c in ('spgrpcd', 'epa_l3', 'major_spgrpcd', 'fortypcd')}
        vocab['_norm'] = {c: (float(seq[c].mean()), float(seq[c].std()) or 1.0) for c in NORM_COLS}
    t = (pd.to_datetime(seq.measdate) - pd.Timestamp('2000-01-01')) / pd.Timedelta(days=1) / 365.25
    g = seq.groupby(K_, sort=False); tid = g.ngroup().to_numpy(); pos = g.cumcount().to_numpy()
    assert pos.max() < T_MAX, 'more slots than T_MAX'
    n = int(tid.max()) + 1
    def mat(v, fill=0.0):
        out = np.full((n, T_MAX), fill, np.float32)
        out[tid, pos] = np.nan_to_num(np.asarray(v, np.float64), nan=fill).astype(np.float32); return out
    hid = seq.hidden.fillna(False).to_numpy(bool) if 'hidden' in seq else np.zeros(len(seq), bool)
    real = (seq.slot_kind == 'real').to_numpy() & ~hid                        # recorded AND visible to the model
    rec_any = (seq.slot_kind == 'real').to_numpy()                            # truly tallied (for the own-tree competition subtraction)
    dia, ht = seq.dia.to_numpy(float), seq.ht.to_numpy(float)
    mD = real & ~np.isnan(dia); mH = real & ~np.isnan(ht)
    mcr = real & seq.cr.notna().to_numpy(); mcc = real & seq.cclcd.notna().to_numpy()
    f_idx = np.full(n, -1)
    fr = pd.Series(pos[real], index=tid[real]).groupby(level=0).min(); f_idx[fr.index.to_numpy()] = fr.to_numpy()
    assert (f_idx >= 0).all(), 'every tree needs a visible real record'
    limit = seq.limit_in.to_numpy(float) if 'limit_in' in seq else seq.thr_in.to_numpy(float)
    lo = seq.lo_in.to_numpy(float) if 'lo_in' in seq else np.full(len(seq), np.nan)
    # existence known: real / gap slots and any slot at or after the first VISIBLE real record; benchmark override via e_known
    after_first = pos >= f_idx[tid]
    eknown = real | after_first
    if 'e_known' in seq: eknown = eknown | seq.e_known.fillna(False).to_numpy(bool)
    micro = (seq.thr_in.to_numpy(float) <= 1.0)
    samp = np.where(micro, seq.forest_micrcond_prop.to_numpy(float), seq.forest_subpcond_prop.to_numpy(float))
    sampled = real | np.isnan(samp) | (samp > 0)
    rcd = seq.rcd0.to_numpy(float); prev_first = pos == (f_idx[tid] - 1)
    missed = prev_first & (rcd == 3) & sampled & ~hid
    ingrowth = prev_first & (rcd == 1) & sampled & ~hid
    own_ba = np.where(rec_any, seq.own_ba.fillna(0).to_numpy(), 0.0); own_tpa = np.where(rec_any, seq.tpa.fillna(0).to_numpy(), 0.0)
    trt = seq.trtcd1.fillna(0).to_numpy(); dst = seq.dstrbcd1.fillna(0).to_numpy()
    dt = t.groupby([seq[c] for c in K_], sort=False).diff().fillna(5.0).to_numpy()
    cov = dict(comp=np.log1p(np.clip(seq.sub_ba.fillna(0).to_numpy() - own_ba, 0, None)) / 5,
               stems=np.log1p(np.clip(seq.sub_tpa.fillna(0).to_numpy() - own_tpa, 0, None)) / 8,
               rls=((trt == 10) | ((dst > 0) & (dst != 60))).astype(float),
               tma=seq.tm_anom.to_numpy(float) / 0.5, ppa=seq.pp_anom.to_numpy(float) / 0.1, vpa=seq.vp_anom.to_numpy(float) / 0.5,
               nbs=seq.nb_inc_sub.to_numpy(float) * 10, mnbs=seq.nb_inc_sub.notna().to_numpy(float),
               nbp=seq.nb_inc_plt.to_numpy(float) * 10, mnbp=seq.nb_inc_plt.notna().to_numpy(float),
               nbn=np.log1p(seq.nb_n_plt.fillna(0).to_numpy(float)) / 3, bal=np.log1p(seq.balive.fillna(0).to_numpy(float)) / 5,
               alk=seq.alstk.to_numpy(float) / 100, can=seq.canopy_pct.to_numpy(float) / 100, mcan=seq.canopy_pct.notna().to_numpy(float),
               ssz=seq.stdszcd.to_numpy(float) / 5, tyr=t.to_numpy() / 20, dt=dt / 10)
    X = dict(lD=mat(np.log(np.clip(np.nan_to_num(dia, nan=1.0), 0.05, None))), lH=mat(np.log(np.clip(np.nan_to_num(ht, nan=4.5), 1.0, None))),
             lA=mat(np.log(np.clip(seq.drybio_g.fillna(1.0).to_numpy(float), 0.1, None))),
             tD=mat(np.log(np.clip(np.nan_to_num(dia, nan=1.0), 0.05, None))), tH=mat(np.log(np.clip(np.nan_to_num(ht, nan=4.5), 1.0, None))),
             hasD=mat(rec_any & ~np.isnan(dia)), hasH=mat(rec_any & ~np.isnan(ht)),
             mD=mat(mD), mH=mat(mH), cr=mat(np.where(mcr, seq.cr.to_numpy(float) / 100, 0)), mcr=mat(mcr),
             cc=mat(np.where(mcc, seq.cclcd.to_numpy(float), 0)), mcc=mat(mcc), v=mat(np.ones(len(seq))), hid=mat(hid),
             real=mat(real), eknown=mat(eknown), sampled=mat(sampled), missed=mat(missed), ingrowth=mat(ingrowth),
             llim=mat(np.log(limit)), llo=mat(np.log(lo), -20.0), haslo=mat(~np.isnan(lo)),
             cov=np.stack([mat(cov[c]) for c in COV], -1))
    st = g.head(1)
    def ids(c): return st[c].map(vocab[c]).fillna(0).astype(int).to_numpy()
    def z_(c): mu, sd = vocab['_norm'][c]; return np.nan_to_num((st[c].to_numpy(dtype=float) - mu) / sd, nan=0.0)
    asp = np.deg2rad(st.aspect.fillna(0).to_numpy(dtype=float)); flat = st.slope.fillna(0).to_numpy(dtype=float) < 5
    S = dict(sp=ids('spgrpcd'), l3=ids('epa_l3'), maj=ids('major_spgrpcd'), fty=ids('fortypcd'),
             scl=np.clip(st.siteclcd.fillna(0).to_numpy(dtype=float), 0, 7).astype(int), phy=np.clip(st.physclcd.fillna(0).to_numpy(dtype=float), 0, 49).astype(int),
             snum=np.stack([st.planted.fillna(False).astype(float).to_numpy(), z_('sicond'), st.sicond.isna().to_numpy(float), z_('slope'),
                            np.where(flat, 0, np.sin(asp)), np.where(flat, 0, np.cos(asp)), z_('tm_n'), z_('pp_n'), z_('vp_n')], 1),
             rcd=np.nan_to_num(st.rcd0.to_numpy(float), nan=0).clip(0, 9).astype(int), f=f_idx)
    longs = ('sp', 'l3', 'maj', 'fty', 'scl', 'phy', 'rcd', 'f')
    T = {k: torch.tensor(np.array(v), dtype=torch.long if k in longs else torch.float32) for k, v in {**X, **S}.items()}
    return T, vocab, seq.assign(_tid=tid, _pos=pos)

def sub(B, ix): return {k: v[ix] for k, v in B.items()}

# ------------------------------------------------------------------------------------------------------------------------------ model
class Static(nn.Module):
    def __init__(s, nsp, nl3, nmaj, nfty):
        super().__init__()
        s.e = nn.ModuleList([nn.Embedding(nsp + 1, 8), nn.Embedding(nl3 + 1, 4), nn.Embedding(nmaj + 1, 2), nn.Embedding(nfty + 1, 4),
                             nn.Embedding(8, 3), nn.Embedding(50, 3)])
        s.out = nn.Linear(8 + 4 + 2 + 4 + 3 + 3 + 9, D_MODEL)
    def forward(s, B):
        return s.out(torch.cat([s.e[0](B['sp']), s.e[1](B['l3']), s.e[2](B['maj']), s.e[3](B['fty']), s.e[4](B['scl']), s.e[5](B['phy']), B['snum']], -1))

def tenc(d, layers=2):
    return nn.TransformerEncoder(nn.TransformerEncoderLayer(d, 4, 2 * d, dropout=0.0, batch_first=True, norm_first=True), layers)

class NIMIWAE53(nn.Module):
    def __init__(s, nsp, nl3, nmaj, nfty):
        super().__init__()
        d = D_MODEL
        s.st_q, s.st_p = Static(nsp, nl3, nmaj, nfty), Static(nsp, nl3, nmaj, nfty)
        s.ercd, s.ecc = nn.Embedding(10, 4), nn.Embedding(6, 3)
        s.qin = nn.Linear(NCOV + 17 + 4 + 3, d); s.qtr = tenc(d); s.qout = nn.Linear(d, 2 * DZ)       # encoder (bidirectional)
        s.prior = nn.Sequential(nn.Linear(d, d), nn.GELU(), nn.Linear(d, 2 * DZ))                        # p(z | static)
        s.din = nn.Linear(NCOV, d); s.dz = nn.Linear(DZ, d); s.dtr = tenc(d)                              # decoder: z + covariates only
        s.dout = nn.Linear(d, 1 + 1 + 1 + 1 + 5)                                                          # dD, dH, hazard, cr, cclcd(5)
        with torch.no_grad():
            s.dout.bias.zero_(); s.dout.bias[0] = -0.5; s.dout.bias[1] = 1.5
        s.a, s.b, s.c, s.ls = (nn.Embedding(nsp + 1, 1) for _ in range(4))                              # allometry (measured sizes)
        nn.init.constant_(s.a.weight, 4.4); nn.init.constant_(s.b.weight, math.log(math.expm1(2.0))); nn.init.constant_(s.c.weight, math.log(math.expm1(0.8)))
        nn.init.constant_(s.ls.weight, -1.5)
        s.hw = nn.Embedding(nmaj + 1, 2 * len(KNOTS)); nn.init.constant_(s.hw.weight, -4.0)
        s.lsdisc, s.lsH, s.lscr = nn.Parameter(torch.tensor(-4.0)), nn.Parameter(torch.tensor(-2.0)), nn.Parameter(torch.tensor(-2.0))
        s.leps = nn.Parameter(torch.tensor(-4.0))
    # ---- encoder q(z | x_o, m, covariates)
    def q(s, B):
        m = B['mD']; cc = s.ecc(B['cc'].long().clamp(0, 5)) * B['mcc'][..., None]
        x = torch.cat([B['cov'], torch.stack([B['lD'] * m, m, B['lH'] * B['mH'], B['mH'], B['cr'], B['mcr'], B['mcc'], B['real'],
                                              B['eknown'], B['sampled'], B['missed'], B['ingrowth'], B['llim'], B['haslo'],
                                              B['llo'].clamp(min=-1) * B['haslo'], (torch.arange(T_MAX)[None] == B['f'][:, None]).float(),
                                              B['v']], -1),
                       s.ercd(B['rcd'])[:, None].expand(-1, T_MAX, -1), cc], -1)
        h = s.qin(x); cls = s.st_q(B)[:, None]
        pad = torch.cat([torch.zeros_like(B['v'][:, :1]), 1 - B['v']], 1).bool()
        o = s.qtr(torch.cat([cls, h], 1), src_key_padding_mask=pad)[:, 0]
        mu, lv = s.qout(o).chunk(2, -1); return mu, lv.clamp(-8, 4)
    def p_z(s, B):
        mu, lv = s.prior(s.st_p(B)).chunk(2, -1); return mu, lv.clamp(-6, 3)
    # ---- decoder p(x | z, covariates): per-slot heads, [K, n, T, ...]
    def decode(s, z, B):
        K, n = z.shape[:2]; d = D_MODEL
        h = s.din(B['cov'])[None] + s.dz(z)[:, :, None] + s.st_p(B)[None, :, None]
        pad = (1 - B['v']).bool()[None].expand(K, -1, -1).reshape(K * n, T_MAX)
        o = s.dout(s.dtr(h.reshape(K * n, T_MAX, d), src_key_padding_mask=pad)).reshape(K, n, T_MAX, -1)
        dD = F.softplus(o[..., 0]) * B['v']; dH = F.softplus(o[..., 1]) * B['v']                     # inches, feet; >= 0
        return dD, dH, o[..., 2], torch.sigmoid(o[..., 3]), o[..., 4:]
    def allom(s, lD, lH, B, shape):
        a, b, c = s.a(B['sp'])[:, 0], F.softplus(s.b(B['sp'])[:, 0]), F.softplus(s.c(B['sp'])[:, 0])
        w = F.softplus(s.hw(B['maj'])); t = KNOTS
        ph = torch.cat([F.relu(lD[..., None] - t), -F.relu(t - lD[..., None])], -1)
        bc = lambda x: x.reshape((1,) * (lD.dim() - 2) + (-1, 1)) if lD.dim() >= 2 else x
        wv = w.reshape((1,) * (lD.dim() - 2) + (w.shape[0], 1, w.shape[1]))
        return bc(a) + bc(b) * lD + bc(c) * lH + (wv * ph).sum(-1), bc(s.ls(B['sp'])[:, 0]).expand(shape)
    def paths(s, dD, dH):
        """Sizes for every flip position j: D[k, n, j, t] = Σ_{u=j..t} dD_u for t >= j, else 0; HT = 4.5 ft + Σ dH."""
        CD, CH = torch.cumsum(dD, -1), torch.cumsum(dH, -1)
        prevD = torch.cat([torch.zeros_like(CD[..., :1]), CD[..., :-1]], -1); prevH = torch.cat([torch.zeros_like(CH[..., :1]), CH[..., :-1]], -1)
        t = torch.arange(T_MAX); ex = (t[None, :] >= t[:, None]).float()                               # [j, t]
        D = (CD[..., None, :] - prevD[..., :, None]) * ex; H = (4.5 + CH[..., None, :] - prevH[..., :, None]) * ex
        return D, H, ex
    def joint(s, B, K):
        """-> per-sample log weights lw [K, n], per-(k, j) log posterior over τ, sizes, and the observed-allometry NLL."""
        mq, lq = s.q(B); mp, lp = s.p_z(B)
        z = mq + torch.exp(0.5 * lq) * torch.randn(K, *mq.shape)
        dD, dH, hz, crm, ccl = s.decode(z, B)
        D, H, ex = s.paths(dD, dH)                                                                     # [K, n, J, T]
        lDp, lHp = torch.log(D.clamp(min=1e-3)), torch.log(H.clamp(min=1.0))
        c2 = 0.5 * math.log(2 * math.pi); nl = lambda x, mu, ls: -0.5 * ((x - mu) / torch.exp(ls)) ** 2 - ls - c2
        v, real, ek, smp = B['v'], B['real'], B['eknown'], B['sampled']
        sdD = 0.5 * torch.log(torch.exp(2 * ls_dia(B['lD'])) + torch.exp(2 * s.lsdisc))[None, :, None]
        ll = (nl(B['lD'][None, :, None], lDp, sdD) * B['mD'][None, :, None]
              + nl(B['lH'][None, :, None], lHp, s.lsH) * B['mH'][None, :, None])
        ptal = torch.sigmoid(KAPPA * (lDp - B['llim'][None, :, None]))                                   # above the limit
        e1 = torch.sigmoid(-s.leps)
        lrec = torch.log((e1 * ptal).clamp(min=1e-12))                                                   # tallied
        lmis = torch.log((1 - e1 * ptal).clamp(min=1e-12))                                               # not tallied (below / missed)
        lmissed = torch.log(ptal.clamp(min=1e-12))                                                       # crew miss: existed, >= limit
        lbelow = torch.log((1 - ptal).clamp(min=1e-12))                                                  # ingrowth previous slot
        lint = torch.log((torch.sigmoid(KAPPA * (lDp - B['llo'][None, :, None])) * (1 - ptal)).clamp(min=1e-12))   # known interval
        unrec = (1 - real) * v
        ex4 = ex[None, None]                                                                             # [1, 1, J, T]
        miss_term = torch.where(B['missed'][None, :, None] > 0, lmissed,
                    torch.where(B['ingrowth'][None, :, None] > 0, lbelow,
                    torch.where(B['haslo'][None, :, None] > 0, lint, lmis)))
        ll = ll + real[None, :, None] * lrec + (unrec * smp)[None, :, None] * ex4 * miss_term
        # existence known -> impossible before the flip
        ll = ll + (ek * v)[None, :, None] * (1 - ex4) * NEG + B['missed'][None, :, None] * (1 - ex4) * NEG
        llt = ll.sum(-1)                                                                                 # [K, n, J]
        # p(τ = j | z): hazard on j < f, remaining mass at j = f; j > f impossible
        hlog, h1log = F.logsigmoid(hz), F.logsigmoid(-hz)
        surv = torch.cat([torch.zeros_like(h1log[..., :1]), torch.cumsum(h1log, -1)[..., :-1]], -1)    # log Π_{u<j} (1 - h_u)
        j = torch.arange(T_MAX)[None, None]; f = B['f'][None, :, None]
        lpt = torch.where(j < f, hlog + surv, torch.where(j == f, surv, torch.full_like(surv, NEG)))
        lpj = lpt + llt                                                                                  # [K, n, J]
        lpx = torch.logsumexp(lpj, -1)
        # crown reconstruction (independent of τ)
        lcr = (nl(B['cr'][None], crm, s.lscr) * B['mcr'][None]).sum(-1)
        lcc = (torch.log_softmax(ccl, -1).gather(-1, (B['cc'].long().clamp(1, 5) - 1)[None, ..., None].expand(K, -1, -1, 1))[..., 0] * B['mcc'][None]).sum(-1)
        lpz = (-0.5 * (z - mp) ** 2 / torch.exp(lp) - 0.5 * lp - c2).sum(-1)
        lqz = (-0.5 * (z - mq) ** 2 / torch.exp(lq) - 0.5 * lq - c2).sum(-1)
        lw = lpx + lcr + lcc + lpz - lqz
        # observed allometry: AGB from MEASURED DIA / HT (latent HT where HT unmeasured is not needed: rows without HT are skipped)
        mA = B['mD'] * B['mH']
        muA, lsA = s.allom(B['lD'], B['lH'], B, B['lD'].shape)
        nll_allom = -(nl(B['lA'], muA, lsA) * mA).sum() / mA.sum().clamp(min=1)
        return lw, lpj - lpx[..., None], D, H, nll_allom

def make_model(vocab):
    m = NIMIWAE53(len(vocab['spgrpcd']), len(vocab['epa_l3']), len(vocab['major_spgrpcd']), len(vocab['fortypcd']) + 1); m.vocab = vocab; return m

def init_allometry(model, tr):
    t = tr[(tr.slot_kind == 'real') & tr.ht.notna() & tr.drybio_g.notna() & tr.dia.notna()]
    X = np.c_[np.ones(len(t)), np.log(t.dia), np.log(t.ht)]; y = np.log(t.drybio_g.clip(lower=0.1)).to_numpy()
    coef = np.linalg.lstsq(X, y, rcond=None)[0]; r = pd.Series(y - X @ coef, index=t.index)
    inv = lambda x: math.log(math.expm1(max(x, 1e-3)))
    with torch.no_grad():
        model.a.weight.fill_(coef[0]); model.b.weight.fill_(inv(coef[1])); model.c.weight.fill_(inv(coef[2]))
        for sp, med in r.groupby(t.spgrpcd).median().items():
            if sp in model.vocab['spgrpcd']: model.a.weight[model.vocab['spgrpcd'][sp], 0] = coef[0] + med
    return coef

# --------------------------------------------------------------------------------------------------------------------------- inference
def wmedian(X, w):
    """Weighted pointwise median over samples: X [S, n, T], w [S, n]."""
    Xs, ix = torch.sort(X, 0); cw = torch.cumsum(w[..., None].expand_as(X).gather(0, ix), 0)
    return Xs.gather(0, (cw < 0.5).sum(0, keepdim=True).clamp(max=X.shape[0] - 1))[0]

@torch.no_grad()
def impute(model, B, K=100, bs=1024, nsamp=200, w_int=(1.0, 1.0)):
    """Per slot: P(E), posterior median DIA / HT / AGB (unconditional: 0 if not existing; monotone), mean AGB, 10 / 90 % of log AGB."""
    out = {k: [] for k in ('pE', 'D', 'H', 'lD', 'lH', 'lA', 'A', 'q10', 'q90')}
    for i in range(0, B['v'].shape[0], bs):
        b = sub(B, slice(i, i + bs)); lw, lpost, D, H, _ = model.joint(b, K)
        W = (torch.softmax(lw, 0)[..., None] * torch.exp(lpost))                                     # [K, n, J] posterior over (k, j)
        Kk, n, J, T = D.shape
        Wf = W.permute(0, 2, 1).reshape(Kk * J, n)                                                       # samples = (k, j)
        Df = D.permute(0, 2, 1, 3).reshape(Kk * J, n, T); Hf = H.permute(0, 2, 1, 3).reshape(Kk * J, n, T)
        ex = (Df > 0).float()
        lDf, lHf = torch.log(Df.clamp(min=1e-3)), torch.log(Hf.clamp(min=1.0))
        muA, lsA = model.allom(lDf, lHf, b, lDf.shape)
        Af = torch.exp(muA + 0.5 * torch.exp(lsA) ** 2) * ex
        pE = (Wf[..., None] * ex).sum(0)
        Dm, Hm = wmedian(Df, Wf), wmedian(Hf * ex, Wf)
        lAm = wmedian(torch.where(ex > 0, muA, torch.full_like(muA, math.log(0.1))), Wf)
        A = (Wf[..., None] * Af).sum(0)
        ks = torch.multinomial(Wf.T.clamp(min=0) + 1e-12, nsamp, replacement=True); ar = torch.arange(n)[:, None]
        smu = torch.where(ex > 0, muA, torch.full_like(muA, math.log(0.1))).permute(1, 0, 2)[ar, ks]; ssd = torch.exp(lsA.permute(1, 0, 2)[ar, ks])
        draw = smu + ssd * torch.randn_like(smu)
        wt = torch.where(torch.log(Dm.clamp(min=1e-3)) < math.log(2.0), torch.tensor(float(w_int[0])), torch.tensor(float(w_int[1])))
        draw = lAm[:, None] + wt[:, None] * (draw - lAm[:, None])
        q = torch.quantile(draw, torch.tensor([0.1, 0.9]), dim=1)
        f = b['f'][:, None]; capD = b['lD'].gather(1, f); capA = b['lA'].gather(1, f)
        lD = torch.minimum(torch.log(Dm.clamp(min=1e-3)), capD)
        for k_, v_ in (('pE', pE), ('D', torch.exp(lD)), ('H', Hm), ('lD', lD), ('lH', torch.log(Hm.clamp(min=1.0))),
                       ('lA', torch.minimum(lAm, capA)), ('A', A), ('q10', q[0]), ('q90', q[1])): out[k_].append(v_)
    return {k: torch.cat(v) for k, v in out.items()}

def monotone_violations(X, B, tol=1e-6):
    v, f = B['v'], B['f']; t = torch.arange(T_MAX)[None]; pre = (t <= f[:, None]) & (v > 0)
    d = X[:, 1:] - X[:, :-1]; pair = pre[:, 1:] & pre[:, :-1]
    return int(((d < -tol) & pair).any(1).sum())

# -------------------------------------------------------------------------------------------------------------------------- benchmark
def bench_trees(seq: pd.DataFrame, limit: float, interval: bool) -> pd.DataFrame:
    """Microplot lineages pseudo-censored at `limit` (D53): microplot debut < limit, >= 3 real visits, a real visit < limit and a later one
    >= limit. Hidden = real visits before the first >= limit one (truth kept). The whole tree is treated as a `limit`-tally tree (pre
    slots unrecorded under `limit`, existence unknown). interval: hidden visits known to exist and lie in [1 in, limit)."""
    r = seq[seq.slot_kind == 'real'].sort_values(K_ + ['measdate'])
    nv = (r.dia >= limit).astype(int).groupby([r[c] for c in K_], sort=False).cumsum()
    r = r.assign(_nv=nv.to_numpy(), _h=(nv == 0).astype(int).to_numpy())
    g = r.groupby(K_, sort=False)
    t = pd.DataFrame({'thr': g.thr_in.first(), 'dia0': g.dia.first(), 'n': g.size(), 'nh': g._h.sum()}); t['nv'] = t.n - t.nh
    ok = t[(t.thr <= 1.0) & (t.dia0 < limit) & (t.n >= 3) & (t.nh >= 1) & (t.nv >= 1)].reset_index()[K_]
    b = seq.merge(ok, on=K_, how='inner').merge(r[K_ + ['measdate', '_h']], on=K_ + ['measdate'], how='left')
    b['hidden'] = (b._h == 1); b['limit_in'] = limit
    b['lo_in'] = np.where(b.hidden & interval, 1.0, np.nan); b['e_known'] = b.hidden & interval
    return b.drop(columns='_h')

def predictions(model, bb, K=100, w_int=(1.0, 1.0)):
    B, _, sq = build(bb, model.vocab)
    R = impute(model, B, K, w_int=w_int)
    ti, po = sq._tid.to_numpy(), sq._pos.to_numpy()
    for k in ('pE', 'lD', 'lA', 'A', 'q10', 'q90'): sq[f'nim_{k}'] = R[k][ti, po].numpy()
    viol = (monotone_violations(R['lD'], B), monotone_violations(R['lH'], B), monotone_violations(R['lA'], B))
    return sq[sq.hidden].copy(), viol, B, R

def fit_width(model, bbv, lo=1.0, hi=12.0, iters=10):
    """Per imputed size class (< 2 in, >= 2 in): the width factor giving 80 % coverage on the validation benchmark, by bisection."""
    def cov(cls, w):
        torch.manual_seed(7); h, *_ = predictions(model, bbv, w_int=(w, 1.0) if cls == 0 else (1.0, w))
        sel = (np.exp(h.nim_lD) < 2) if cls == 0 else (np.exp(h.nim_lD) >= 2)
        la = np.log(h.drybio_g); inside = ((la >= h.nim_q10) & (la <= h.nim_q90))[sel]
        return 1.0 if len(inside) == 0 else float(inside.mean())
    out = []
    for cls in (0, 1):
        a_, b_ = lo, hi
        if cov(cls, a_) >= 0.80: out.append(a_); continue
        for _ in range(iters):
            mid = 0.5 * (a_ + b_)
            if cov(cls, mid) >= 0.80: b_ = mid
            else: a_ = mid
        out.append(round(b_, 3))
    return tuple(out)

# ---------------------------------------------------------------------------------------------------------------------------- training
def train(tr, epochs=4, k=16, bs=512, max_trees=0, seed=0, threads=7, lr=1e-3, log=print):
    torch.manual_seed(seed); torch.set_num_threads(threads); rng = np.random.default_rng(seed)
    B, vocab, _ = build(tr); n = B['v'].shape[0]
    if max_trees and max_trees < n: B = sub(B, torch.tensor(rng.permutation(n)[:max_trees])); n = max_trees
    model = make_model(vocab); log(f'allometry OLS start {init_allometry(model, tr).round(3)}')
    opt = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=1e-5)
    nb_ep = (n + bs - 1) // bs
    sched = torch.optim.lr_scheduler.OneCycleLR(opt, max_lr=lr, total_steps=epochs * nb_ep, pct_start=0.05, anneal_strategy='cos')
    log(f'train trees {n:,}; slots {int(B["v"].sum()):,}; visible real {int(B["real"].sum()):,}')
    hist = []
    for ep in range(epochs):
        t0, tot, nb = time.time(), 0.0, 0
        for ix in torch.split(torch.tensor(rng.permutation(n)), bs):
            b = sub(B, ix); lw, _, _, _, na = model.joint(b, k)
            loss = -(torch.logsumexp(lw, 0) - math.log(k)).mean() + na
            opt.zero_grad(); loss.backward(); nn.utils.clip_grad_norm_(model.parameters(), 5.0); opt.step(); sched.step()
            tot += loss.item(); nb += 1
        hist.append(dict(epoch=ep, loss=tot / nb, sd_disc=torch.exp(model.lsdisc).item(), sd_H=torch.exp(model.lsH).item(),
                         field_miss=torch.sigmoid(model.leps).item()))
        log(json.dumps(hist[-1]) + f' {time.time() - t0:.0f}s')
    return model, hist
