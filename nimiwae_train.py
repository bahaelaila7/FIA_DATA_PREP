"""D51 — NIMIWAE phantom backfill with a CAUSAL forward growth model and growth drivers (DECISIONS.md D49–D51).

Generative model per tree (slots = real visits + pre-debut phantom slots, in time order):
  latents   z ~ N(0, I_6) (vigour / microsite);  δ ~ N(μ(x_f), σ(x_f)²) learned from the first observed record's covariates: A_f = proxy(first record) · e^δ
  forward   establishment at t_f − A_f; effective age a_eff accumulates FORWARD over the intervals ending at each slot,
            Δa_t = L_t · m_t · ε_t  (L_t interval years, the first interval from establishment; ε_t log-normal process noise)
            m_t = exp(f(z, site, competition / stems / release at the START of the interval, climate anomaly OVER the interval))
  height    H = Hmax (1 − e^{−k a_eff})^p, Hmax / k / p from (z, species, site index, site class, physiography, slope / aspect, climate normals)
  size      DIA = e^{c_sp + ζ(z)} (H − 4.5 ft)_+^{b_sp} + 0.01 in (breast-height anchor); log AGB = monotone hinge allometry in (log DIA, log HT)
  observed  log DIA ~ N(·, σ_D), log HT ~ N(·, σ_H) where measured; log AGB ~ N(allometry(MEASURED DIA, HT), σ_sp) (FIA computes DRYBIO
            from the measured sizes); crown ratio / class are encoder inputs only
  missing   P(tallied | DIA) = (1 − ε) σ(KAPPA (log DIA − log limit)): FIA's deterministic tally limit with a learned field-miss rate
Every path is non-decreasing in time by construction (Δa >= 0, H increasing in a_eff, DIA increasing in H, AGB increasing in both).
Competition is the subplot BA / stems of the OTHER trees (own tree subtracted: no leak); release = TRTCD1 10 or DSTRBCD1 not in {0, 60}.
The encoder q(z, δ | observed sizes, crown measures, all per-slot drivers, site) does the heavy lifting; bound
  log p(x_o, m) ≥ E[log (1/K) Σ_k p(x_o | z_k, δ_k, ε_k) Π_t p(m_t | ·) p(z_k) p(δ_k) / q(z_k, δ_k)]   (ε from its prior).
Training (D50 protocol): AdamW + cosine decay, the FINAL epoch is the model; interval width factors per imputed size class fitted on VAL.
Usage: python -I nimiwae_train.py --data DIR [--epochs N] [--k K]"""
import argparse, json, math, time
import numpy as np, pandas as pd, torch, torch.nn as nn, torch.nn.functional as F

T_MAX, DZ, YRS_S = 13, 6, 20.0
KNOTS = torch.log(torch.tensor([0.5, 1.0, 2.0, 3.0, 5.0]))     # allometry hinge knots (log in)
KAPPA = 20.0                                                    # tally step steepness per log unit of DIA
K_ = ['statecd', 'unitcd', 'countycd', 'plot', 'subp', 'tree']
NORM_COLS = ('sicond', 'slope', 'tm_n', 'pp_n', 'vp_n')         # site / climate-normal covariates, standardised on TRAIN
NSLOT, NSTAT, NDRV = 35, 47, 14                                          # per-slot encoder features; static covariate width

def age_table(seq_train: pd.DataFrame) -> pd.DataFrame:
    """Leak-free age proxy: median curated age at a tree's FIRST real record by (major group, planted, 0.5-in DIA bin), train only."""
    f = seq_train[seq_train.obs].sort_values(K_ + ['measdate']).groupby(K_, sort=False).head(1).copy()
    f['dbin'] = np.floor(f.dia.clip(0, 40) / 0.5).astype(int)
    t = f.groupby(['major_spgrpcd', 'planted', 'dbin']).age_calc.agg(['median', 'size']).reset_index()
    return t[t['size'] >= 20]

def proxy_age(first: pd.DataFrame, tab: pd.DataFrame) -> np.ndarray:
    f = first[['major_spgrpcd', 'planted', 'dia']].copy(); f['dbin'] = np.floor(f.dia.clip(0, 40) / 0.5).astype(int)
    m = f.merge(tab, on=['major_spgrpcd', 'planted', 'dbin'], how='left')
    out = m['median'].to_numpy(dtype=float).copy()
    for i in np.where(np.isnan(out))[0]:                                                      # nearest bin of the same group / planted
        c = tab[(tab.major_spgrpcd == f.major_spgrpcd.iat[i]) & (tab.planted == f.planted.iat[i])]
        if len(c) == 0: c = tab
        out[i] = c.iloc[np.argmin(np.abs(c.dbin.to_numpy() - f.dbin.iat[i]))]['median']
    return np.clip(out, 1.0, None)

def build(seq: pd.DataFrame, vocab=None, atab=None):
    """Padded per-tree tensors, slots in time order, padding at the end."""
    seq = seq.sort_values(K_ + ['measdate']).copy()
    if vocab is None:
        vocab = {c: {v: i + 1 for i, v in enumerate(sorted(seq[c].dropna().unique()))} for c in ('spgrpcd', 'epa_l3', 'major_spgrpcd', 'fortypcd')}
        vocab['_norm'] = {c: (float(seq[c].mean()), float(seq[c].std()) or 1.0) for c in NORM_COLS}
    seq['t'] = (pd.to_datetime(seq['measdate']) - pd.Timestamp('1970-01-01')) / pd.Timedelta(days=1) / 365.25   # unit-safe (ms / ns dates)
    first = seq[seq.obs].groupby(K_, sort=False).head(1)
    fr = first[K_ + ['t', 'dia', 'major_spgrpcd', 'planted']].copy(); fr['age_f'] = proxy_age(fr, atab)
    seq = seq.merge(fr[K_ + ['t', 'age_f']].rename(columns={'t': 't_f'}), on=K_, how='inner').sort_values(K_ + ['measdate'])
    seq['rel'] = seq.t - seq.t_f
    g = seq.groupby(K_, sort=False); tid = g.ngroup().to_numpy(); pos = g.cumcount().to_numpy(); keep = pos < T_MAX
    n = int(tid.max()) + 1
    def mat(v, fill=0.0):
        out = np.full((n, T_MAX), fill, np.float32)
        out[tid[keep], pos[keep]] = np.nan_to_num(np.asarray(v, np.float64), nan=fill).astype(np.float32)[keep]; return out
    obs = seq.obs.to_numpy(bool)
    hid = seq.hidden.fillna(False).to_numpy(bool) if 'hidden' in seq else np.zeros(len(seq), bool)
    hv = obs & seq.ht.notna().to_numpy(); crv = obs & seq.cr.notna().to_numpy(); ccv = obs & seq.cclcd.notna().to_numpy()
    tallied = obs | hid                                                                        # the tree itself is in the subplot sums
    own_ba = np.where(tallied, seq.own_ba.fillna(0).to_numpy(), 0.0); own_tpa = np.where(tallied, seq.tpa.fillna(0).to_numpy(), 0.0)
    comp = np.log1p(np.clip(seq.sub_ba.fillna(0).to_numpy() - own_ba, 0, None)) / 5
    stems = np.log1p(np.clip(seq.sub_tpa.fillna(0).to_numpy() - own_tpa, 0, None)) / 8
    trt = seq.trtcd1.fillna(0).to_numpy(); dst = seq.dstrbcd1.fillna(0).to_numpy()
    rls = ((trt == 10) | ((dst > 0) & (dst != 60))).astype(float)
    X = dict(lD=mat(np.log(seq.dia.fillna(1.0).clip(lower=0.05))), lH=mat(np.log(seq.ht.fillna(4.5).clip(lower=1.0))),
             lA=mat(np.log(seq.drybio_g.fillna(1.0).clip(lower=0.1))), m=mat(obs), mH=mat(hv), hasH=mat(seq.ht.notna().to_numpy()),
             v=mat(np.ones(len(seq))), hid=mat(hid), thr_slot=mat(np.log(seq.thr_in.to_numpy())),
             lo_slot=mat(np.log(seq.lo_in.to_numpy(dtype=float)) if 'lo_in' in seq else np.full(len(seq), np.nan), -20.0),
             rel=mat(seq.rel.to_numpy() / YRS_S), comp=mat(comp), stems=mat(stems), rls=mat(rls),
             tma=mat(seq.tm_anom.to_numpy() / 0.5), ppa=mat(seq.pp_anom.to_numpy() / 0.1), vpa=mat(seq.vp_anom.to_numpy() / 0.5),
             cr=mat(np.where(crv, seq.cr.to_numpy() / 100, 0)), mcr=mat(crv), cc=mat(np.where(ccv, seq.cclcd.to_numpy(), 0)), mcc=mat(ccv),
             # D52: the local growth environment and stand context at each visit (observable whenever the visit happened)
             nbs=mat(seq.nb_inc_sub.to_numpy(dtype=float) * 10), mnbs=mat(seq.nb_inc_sub.notna().to_numpy()),
             nbp=mat(seq.nb_inc_plt.to_numpy(dtype=float) * 10), mnbp=mat(seq.nb_inc_plt.notna().to_numpy()),
             nbn=mat(np.log1p(seq.nb_n_plt.fillna(0).to_numpy(dtype=float)) / 3),
             sda=mat(seq.stdage.to_numpy(dtype=float) / 50), msda=mat(seq.stdage.notna().to_numpy()),
             fda=mat(seq.fldage.to_numpy(dtype=float) / 50), mfda=mat(seq.fldage.notna().to_numpy()),
             bal=mat(np.log1p(seq.balive.fillna(0).to_numpy(dtype=float)) / 5), alk=mat(seq.alstk.to_numpy(dtype=float) / 100),
             can=mat(seq.canopy_pct.to_numpy(dtype=float) / 100), mcan=mat(seq.canopy_pct.notna().to_numpy()),
             ssz=mat(seq.stdszcd.to_numpy(dtype=float) / 5),
             lof=mat(seq.lo_in.notna().to_numpy() if 'lo_in' in seq else np.zeros(len(seq))))
    st = g.head(1)
    def ids(c): return st[c].map(vocab[c]).fillna(0).astype(int).to_numpy()
    def z_(c): mu, sd = vocab['_norm'][c]; return np.nan_to_num((st[c].to_numpy(dtype=float) - mu) / sd, nan=0.0)
    asp = np.deg2rad(st.aspect.fillna(0).to_numpy(dtype=float)); flat = (st.slope.fillna(0).to_numpy(dtype=float) < 5)
    S = dict(sp=ids('spgrpcd'), l3=ids('epa_l3'), maj=ids('major_spgrpcd'), fty=ids('fortypcd'),
             scl=np.clip(st.siteclcd.fillna(0).to_numpy(dtype=float), 0, 7).astype(int),
             phy=np.clip(st.physclcd.fillna(0).to_numpy(dtype=float), 0, 49).astype(int),
             snum=np.stack([st.planted.fillna(False).astype(float).to_numpy(),          # (the tally limit is design, not a growth driver)
                            z_('sicond'), st.sicond.isna().to_numpy(float), z_('slope'),
                            np.where(flat, 0, np.sin(asp)), np.where(flat, 0, np.cos(asp)), z_('tm_n'), z_('pp_n'), z_('vp_n')], 1),
             lage=np.log(st.age_f.to_numpy(dtype=float)))
    T = {k: torch.tensor(np.asarray(v), dtype=torch.long if k in ('sp', 'l3', 'maj', 'scl', 'phy', 'fty') else torch.float32) for k, v in {**X, **S}.items()}
    return T, vocab, seq.assign(_tid=tid, _pos=pos)

class NIMIWAE(nn.Module):
    def __init__(s, nsp, nl3, nmaj, h=128, nfty=200):
        super().__init__()
        s.esp, s.el3, s.emaj = nn.Embedding(nsp + 1, 8), nn.Embedding(nl3 + 1, 4), nn.Embedding(nmaj + 1, 2)
        s.escl, s.ephy, s.ecc, s.efty = nn.Embedding(8, 3), nn.Embedding(50, 3), nn.Embedding(6, 3), nn.Embedding(nfty + 1, 4)
        s.phi = nn.Sequential(nn.Linear(NSLOT, h), nn.GELU(), nn.Linear(h, h))
        s.gru = nn.GRU(h, h, batch_first=True, bidirectional=True)                              # D52: ordered encoder over the slots
        s.enc = nn.Sequential(nn.Linear(4 * h + NSTAT + 1, h), nn.GELU(), nn.Linear(h, h), nn.GELU(), nn.Linear(h, 2 * DZ + 2))
        s.qe = nn.Sequential(nn.Linear(2 * h + NSTAT, h), nn.GELU(), nn.Linear(h, 2))            # D52: q(log ε_t) per interval (amortised)
        nn.init.zeros_(s.qe[-1].weight)
        with torch.no_grad(): s.qe[-1].bias.copy_(torch.tensor([0.0, math.log(0.2)]))
        s.fm = nn.Sequential(nn.Linear(DZ + NSTAT + NDRV, h), nn.GELU(), nn.Linear(h, 1))       # log growth modifier per interval
        s.fc = nn.Sequential(nn.Linear(DZ + NSTAT, h), nn.GELU(), nn.Linear(h, 3))              # Chapman-Richards log Hmax, log k, p
        nn.init.zeros_(s.fm[-1].weight); nn.init.zeros_(s.fm[-1].bias); nn.init.zeros_(s.fc[-1].weight)
        with torch.no_grad(): s.fc[-1].bias.copy_(torch.tensor([math.log(80.0), math.log(0.03), 0.0]))
        s.dc, s.db = nn.Embedding(nsp + 1, 1), nn.Embedding(nsp + 1, 1); s.zeta = nn.Linear(DZ, 1)   # DIA = e^{dc + ζ(z)} (H − 4.5)_+^{softplus(db)}
        nn.init.constant_(s.dc.weight, -2.1); nn.init.constant_(s.db.weight, 0.55)
        s.a, s.b, s.c, s.ls = (nn.Embedding(nsp + 1, 1) for _ in range(4))
        nn.init.constant_(s.a.weight, 3.0); nn.init.constant_(s.b.weight, 1.8); nn.init.constant_(s.c.weight, 0.3); nn.init.constant_(s.ls.weight, -1.5)
        s.hw = nn.Embedding(nmaj + 1, 2 * len(KNOTS)); nn.init.constant_(s.hw.weight, -4.0)          # hinge weights (softplus >= 0)
        s.lsD, s.lsH = nn.Parameter(torch.tensor(-2.3)), nn.Parameter(torch.tensor(-2.0))
        s.leps = nn.Parameter(torch.tensor(-4.0))                                              # logit of the field-miss rate ε
        s.lsP = nn.Parameter(torch.tensor(math.log(0.3)))                                       # log sd of the growth process noise
        s.rP = nn.Parameter(torch.tensor(1.1))                                                  # D52: AR(1) persistence of growth, ρ = tanh(rP)
        s.fa = nn.Sequential(nn.Linear(NSTAT + 7 + 3 + 4, h), nn.GELU(), nn.Linear(h, 2))        # conditional age prior (first record only)
        nn.init.zeros_(s.fa[-1].weight)
        with torch.no_grad(): s.fa[-1].bias.copy_(torch.tensor([0.0, math.log(0.4)]))
    def age_prior(s, B, st):
        """Empirical-Bayes prior of δ = log A_f − log proxy from the FIRST observed record's covariates only (size, crown ratio / class,
        competition, stems) + site / species: curated ages are compressed for suppressed advance regeneration, so the prior is learned."""
        f = torch.argmax(B['m'], 1)[:, None]; at = lambda k: B[k].gather(1, f)
        x = torch.cat([st, at('lD'), at('lH') * at('mH'), at('mH'), at('cr'), at('mcr'), at('comp'), at('stems'),
                       s.ecc(at('cc')[:, 0].long().clamp(0, 5)) * at('mcc'), at('sda'), at('msda'), at('fda'), at('mfda')], -1)
        o = s.fa(x); return o[:, 0], o[:, 1].clamp(math.log(0.05), math.log(1.5))
    def static(s, B):
        """Conditioning covariates of the GENERATIVE model (D52 fix: in an IWAE, information seen only by the encoder cannot move the
        posterior, so the first record's crown and stand context must be here): species / site / climate normals + at the tree's first
        observed record: crown ratio / class, stand age (STDAGE, FLDAGE), canopy cover, stocking, stand size class."""
        f = torch.argmax(B['m'], 1)[:, None]; at = lambda k: B[k].gather(1, f)
        return torch.cat([s.esp(B['sp']), s.el3(B['l3']), s.emaj(B['maj']), s.escl(B['scl']), s.ephy(B['phy']), s.efty(B['fty']), B['snum'],
                          at('cr'), at('mcr'), s.ecc(at('cc')[:, 0].long().clamp(0, 5)) * at('mcc'), at('mcc'), at('sda'), at('msda'),
                          at('fda'), at('mfda'), at('can'), at('mcan'), at('alk'), at('ssz')], -1)
    def q(s, B, st):
        m = B['m']
        x = torch.cat([torch.stack([B['lD'] * m, B['lH'] * B['mH'], B['lA'] * m, m, B['mH'], B['cr'], B['mcr'], B['mcc'], B['rel'],
                                    B['comp'], B['stems'], B['rls'], B['tma'], B['ppa'], B['vpa'],
                                    # D52: stand / neighbour context + the slot's censoring bounds (inference must know what "missing" means)
                                    B['nbs'], B['mnbs'], B['nbp'], B['mnbp'], B['nbn'], B['sda'], B['msda'], B['fda'], B['mfda'],
                                    B['bal'], B['alk'], B['can'], B['mcan'], B['ssz'], B['thr_slot'] * (1 - m), B['lof'],
                                    B['lo_slot'].clamp(min=-1) * B['lof']], -1),
                       s.ecc(B['cc'].long().clamp(0, 5)) * B['mcc'][..., None]], -1)
        vv = B['v'][..., None]; h = s.phi(x) * vv
        L = B['v'].sum(1).long().clamp(min=1)
        pk = nn.utils.rnn.pack_padded_sequence(h, L.cpu(), batch_first=True, enforce_sorted=False)
        out, hn = s.gru(pk); out, _ = nn.utils.rnn.pad_packed_sequence(out, batch_first=True, total_length=h.shape[1])
        mean = (out * vv).sum(1) / vv.sum(1).clamp(min=1)
        o = s.enc(torch.cat([mean, hn[0], hn[1], st, B['lage'][:, None]], -1))
        e = s.qe(torch.cat([out, st[:, None].expand(-1, out.shape[1], -1)], -1))
        return (o[:, :DZ], o[:, DZ:2 * DZ].clamp(-6, 3), o[:, 2 * DZ], o[:, 2 * DZ + 1].clamp(-6, 1),
                e[..., 0].clamp(-3, 3), e[..., 1].clamp(math.log(0.01), math.log(1.5)))
    def grow(s, z, dl, B, st, le=None):
        """Forward (causal) effective age and the latent size paths, [K, n, T]."""
        K, n, T = z.shape[0], z.shape[1], B['v'].shape[1]
        age_f = torch.exp(B['lage'][None] + dl)                                                # age at the first observed record
        rel = B['rel'] * YRS_S; v = B['v']
        L0 = (age_f[..., None] + rel[None, :, :1]).clamp(min=0)                                # establishment -> first slot
        Lt = ((rel[:, 1:] - rel[:, :-1]).clamp(min=0) * v[:, 1:])[None].expand(K, n, T - 1)    # between slots (0 on padding)
        L = torch.cat([L0, Lt], 2)
        prv = lambda x: torch.cat([x[:, :1], x[:, :-1]], 1)                                     # state at the START of the interval
        drv = torch.stack([prv(B['comp']), prv(B['stems']), prv(B['rls']), B['tma'], B['ppa'], B['vpa'],
                           B['nbs'], B['mnbs'], B['nbp'], B['mnbp'], prv(B['bal']),             # neighbours: growth over the SAME interval
                           prv(B['alk']), prv(B['can']), prv(B['mcan'])], -1)                   # stand state at the interval start
        inp = torch.cat([z[:, :, None].expand(K, n, T, DZ), st[None, :, None].expand(K, n, T, st.shape[-1]), drv[None].expand(K, n, T, NDRV)], -1)
        mod = torch.exp(s.fm(inp)[..., 0].clamp(-2.0, 1.5))
        sp_ = torch.exp(s.lsP)
        eps = torch.exp(le) if le is not None else torch.exp(sp_ * torch.randn_like(mod) - 0.5 * sp_ ** 2)   # ε_t > 0: monotone
        aeff = torch.cumsum(L * mod * eps, 2)
        cp = s.fc(torch.cat([z, st[None].expand(K, n, st.shape[-1])], -1))
        Hmax, k, p = torch.exp(cp[..., 0:1].clamp(2.0, 5.3)), torch.exp(cp[..., 1:2].clamp(-8, 0)), 1 + F.softplus(cp[..., 2:3])
        H = Hmax * (1 - torch.exp(-k * aeff)).clamp(min=1e-12) ** p
        sH = torch.log(H.clamp(min=0.01))
        b = F.softplus(s.db(B['sp'])[:, 0])[None, :, None]
        sD = torch.log(torch.exp(s.dc(B['sp'])[:, 0][None, :, None] + s.zeta(z)) * (H - 4.5).clamp(min=0) ** b + 0.01)
        return sD, sH
    def allom(s, sD, sH, B):
        a, b, c = s.a(B['sp'])[:, 0], F.softplus(s.b(B['sp'])[:, 0]), F.softplus(s.c(B['sp'])[:, 0])
        w = F.softplus(s.hw(B['maj']))[None, :, None, :]; t = KNOTS[None, None, None, :]
        phi = torch.cat([F.relu(sD[..., None] - t), -F.relu(t - sD[..., None])], -1)
        return (a[None, :, None] + b[None, :, None] * sD + c[None, :, None] * sH + (w * phi).sum(-1), s.ls(B['sp'])[:, 0][None, :, None].expand_as(sD))
    def logw(s, B, K):
        st = s.static(B); mz, lz, md, ld, me, lse = s.q(B, st); pm, pls = s.age_prior(B, st)
        z = mz + torch.exp(lz) * torch.randn(K, *mz.shape); dl = md + torch.exp(ld) * torch.randn(K, *md.shape)
        le = me + torch.exp(lse) * torch.randn(K, *me.shape)                                  # log ε_t from the amortised proposal
        sD, sH = s.grow(z, dl, B, st, le); muA, lsA = s.allom(sD, sH, B)          # AGB of the latent sizes (imputation)
        # FIA's DRYBIO is a function of the MEASURED DIA / HT: p(D, H, A | latent) = p(D, H | latent) p(A | D_obs, H_obs), so the
        # allometry is fitted on the observed sizes (latent HT where HT is unmeasured), not on the smooth latent path (errors-in-variables)
        muO, lsO = s.allom(B['lD'][None].expand_as(sD), torch.where(B['mH'][None] > 0, B['lH'][None], sH), B)
        m, mH, v = B['m'], B['mH'], B['v']
        nl = lambda x, mu_, ls_: -0.5 * ((x - mu_) / torch.exp(ls_)) ** 2 - ls_ - 0.5 * math.log(2 * math.pi)
        lpo = ((nl(B['lD'][None], sD, ls_dia(B['lD'])[None]) + nl(B['lA'][None], muO, lsO)) * m * v).sum(-1) + (nl(B['lH'][None], sH, s.lsH) * mH * v).sum(-1)
        p_obs = torch.sigmoid(-s.leps) * torch.sigmoid(KAPPA * (sD - B['thr_slot'][None]))
        lpm = ((m * torch.log(p_obs.clamp(min=1e-12)) + (1 - m) * torch.log((1 - p_obs).clamp(min=1e-12))) * v).sum(-1)
        lpm = lpm + ((1 - m) * F.logsigmoid(KAPPA * (sD - B['lo_slot'][None])) * v).sum(-1)    # known lower bound (interval censoring)
        c2 = 0.5 * math.log(2 * math.pi)
        lpz = (-0.5 * z ** 2 - c2).sum(-1) + (-0.5 * ((dl - pm) / torch.exp(pls)) ** 2 - pls - c2)
        lqz = (-0.5 * ((z - mz) / torch.exp(lz)) ** 2 - lz - c2).sum(-1) + (-0.5 * ((dl - md) / torch.exp(ld)) ** 2 - ld - c2)
        # prior: log ε_t is AR(1) over the intervals (growth is PERSISTENT: a tree's post-debut growth informs its hidden intervals),
        # stationary N(−σ²/2, σ²) marginals, ρ learned
        sp_, rho = torch.exp(s.lsP), torch.tanh(s.rP); x = le + 0.5 * sp_ ** 2
        mu_p = torch.cat([torch.zeros_like(x[..., :1]), rho * x[..., :-1]], -1)
        sd_p = torch.cat([sp_.expand_as(x[..., :1]), (sp_ * torch.sqrt(1 - rho ** 2)).expand_as(x[..., 1:])], -1)
        lpe = ((-0.5 * ((x - mu_p) / sd_p) ** 2 - torch.log(sd_p) - c2) * v).sum(-1)
        lqe = ((-0.5 * ((le - me) / torch.exp(lse)) ** 2 - lse - c2) * v).sum(-1)
        return lpo + lpm + lpz - lqz + lpe - lqe, sD, sH, muA, lsA

def ls_dia(lD):
    """D52: FIXED log-scale DIA measurement sd at FIA precision (~0.1 in): sqrt(0.02² + (0.06 in / DIA)²) -> 0.028 at 3 in, 0.063 at 1 in.
    A learned sd absorbed curve misfit (0.11), which made a tree's observed growth rate indistinguishable from noise."""
    return 0.5 * torch.log(0.02 ** 2 + (0.06 / torch.exp(lD)) ** 2)

def make_model(vocab):
    m = NIMIWAE(len(vocab['spgrpcd']), len(vocab['epa_l3']), len(vocab['major_spgrpcd']), nfty=len(vocab.get('fortypcd', {})) + 1)
    m.vocab = vocab; return m

def init_allometry(model, tr, vocab):
    """Data-driven start of the allometry: global OLS log AGB ~ log DIA + log HT on TRAIN measured records, per-species intercepts = the
    median OLS residual of the species (training then refines it, hinges start ~0)."""
    t = tr[tr.obs & tr.ht.notna() & tr.drybio_g.notna()]
    X = np.c_[np.ones(len(t)), np.log(t.dia), np.log(t.ht)]; y = np.log(t.drybio_g.clip(lower=0.1)).to_numpy()
    coef = np.linalg.lstsq(X, y, rcond=None)[0]; r = pd.Series(y - X @ coef, index=t.index)
    inv = lambda x: math.log(math.expm1(max(x, 1e-3)))                                       # softplus^-1
    with torch.no_grad():
        model.a.weight.fill_(coef[0]); model.b.weight.fill_(inv(coef[1])); model.c.weight.fill_(inv(coef[2]))
        for sp, med in r.groupby(t.spgrpcd).median().items():
            if sp in vocab['spgrpcd']: model.a.weight[vocab['spgrpcd'][sp], 0] = coef[0] + med
    return coef

def batches(n, bs, rng):
    p = rng.permutation(n)
    for i in range(0, n, bs): yield torch.tensor(p[i:i + bs])

def sub(B, ix): return {k: v[ix] for k, v in B.items()}

def wmedian(X, w):
    """Weighted pointwise median over importance samples, X [K, n, T], w [K, n]. A pointwise quantile of non-decreasing paths with shared
    weights is non-decreasing."""
    Xs, ix = torch.sort(X, 0); cw = torch.cumsum(w[..., None].expand_as(X).gather(0, ix), 0)
    return Xs.gather(0, (cw < 0.5).sum(0, keepdim=True).clamp(max=X.shape[0] - 1))[0]

@torch.no_grad()
def impute(model, B, K=100, bs=2048, nsamp=200, w_int=1.0):
    """Posterior summaries per slot (weighted posterior MEDIAN log DIA / HT / AGB, linear mean AGB, 10 / 90 % predictive quantiles of log AGB), capped at the first
    observed value; interval widths scaled per imputed size class (DIA < 2 in, >= 2 in)."""
    res = {k: [] for k in ('lD', 'lH', 'lA', 'A', 'q10', 'q90')}
    for i in range(0, B['m'].shape[0], bs):
        b = sub(B, slice(i, i + bs)); lw, sD, sH, muA, lsA = model.logw(b, K)
        w = torch.softmax(lw, 0); f = torch.argmax(b['m'], 1)
        cap = lambda x, X: torch.minimum(x, X.gather(1, f[:, None]))
        eD, eH, eA = wmedian(sD, w), wmedian(sH, w), wmedian(muA, w)          # Bayes point for absolute error; monotone paths -> monotone
        A = (w[..., None] * torch.exp(muA + 0.5 * torch.exp(lsA) ** 2)).sum(0)
        ks = torch.multinomial(w.T, nsamp, replacement=True); ar = torch.arange(ks.shape[0])[:, None]
        smu = muA.permute(1, 0, 2)[ar, ks]; ssd = torch.exp(lsA.permute(1, 0, 2)[ar, ks])
        draw = smu + ssd * torch.randn_like(smu)
        ws, wl = (w_int, w_int) if np.isscalar(w_int) else w_int
        wt = torch.where(eD < math.log(2.0), torch.tensor(float(ws)), torch.tensor(float(wl)))
        draw = eA[:, None] + wt[:, None] * (draw - eA[:, None])
        q = torch.quantile(draw, torch.tensor([0.1, 0.9]), dim=1)
        res['lD'].append(cap(eD, b['lD'])); res['lH'].append(cap(eH, b['lH'])); res['lA'].append(cap(eA, b['lA']))
        res['A'].append(torch.minimum(A, torch.exp(b['lA'].gather(1, f[:, None])))); res['q10'].append(q[0]); res['q90'].append(q[1])
    return {k: torch.cat(v) for k, v in res.items()}

def monotone_violations(lX, B, tol=1e-6):
    """Imputed slots up to and including the first observed one must be non-decreasing (P1)."""
    v = B['v']; f = torch.argmax(B['m'], 1); t = torch.arange(v.shape[1])[None]
    pre = (t <= f[:, None]) & (v > 0)
    d = lX[:, 1:] - lX[:, :-1]; pair = pre[:, 1:] & pre[:, :-1]
    return int(((d < -tol) & pair).any(1).sum())

def bench_eval(model, data, split, atab, K=100, w_int=1.0, lower=True):
    bb = pd.read_parquet(f'{data}/bench.parquet')
    return bench_eval_df(model, bb[bb.split == split], atab, K, w_int, lower)

def bench_eval_df(model, bb, atab, K=100, w_int=1.0, lower=True):
    """Benchmark trees (real visits with a `hidden` flag): hidden visits pseudo-censored at 3 in (+ the 1-in lower bound if `lower`)."""
    bb = bb.copy(); bb['obs'] = ~bb['hidden']; bb['thr_in'] = 3.0
    bb['lo_in'] = np.where(bb['hidden'], 1.0, np.nan) if lower else np.nan   # lower: the visit WAS tallied at the real 1-in microplot limit
    B, _, seq = build(bb, model.vocab, atab)
    R = impute(model, B, K, w_int=w_int)
    hid = B['hid'].bool()
    eA = (R['lA'] - B['lA'])[hid].numpy(); eD = (R['lD'] - B['lD'])[hid].numpy()
    hh = hid & B['hasH'].bool(); eH = (R['lH'] - B['lH'])[hh].numpy()
    cov = ((B['lA'] >= R['q10']) & (B['lA'] <= R['q90']))[hid].float().numpy()
    return dict(mae_lA=float(np.abs(eA).mean()), bias_lA=float(np.median(eA)), bias_lD=float(np.median(eD)), bias_lH=float(np.median(eH)),
                cov80=float(cov.mean()), viol=(monotone_violations(R['lD'], B), monotone_violations(R['lH'], B), monotone_violations(R['lA'], B))), B, R, seq

def fit_width(model, data, atab, **kw):
    bb = pd.read_parquet(f'{data}/bench.parquet'); return fit_width_df(model, bb[bb.split == 'val'], atab, **kw)

def fit_width_df(model, bbv, atab, lo=1.0, hi=12.0, iters=12):
    """Per imputed size class (DIA < 2 in, >= 2 in), the width factor whose 80 % coverage on the validation bench trees `bbv` in that
    class is 0.80, by bisection (decisive protocol)."""
    def cov(cls, w):
        torch.manual_seed(7)
        r, B, R, _ = bench_eval_df(model, bbv, atab, w_int=(w, 1.0) if cls == 0 else (1.0, w))
        hid = B['hid'].bool(); small = (R['lD'] < math.log(2.0)) & hid; sel = small if cls == 0 else (hid & ~small)
        inside = ((B['lA'] >= R['q10']) & (B['lA'] <= R['q90']))[sel].float()
        return 1.0 if inside.numel() == 0 else float(inside.mean())
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

PSEUDO_IN, MICRO_TPA = 3.0, 50.0

def pseudo_bench(tr: pd.DataFrame) -> pd.DataFrame:
    """TRAIN-split pseudo-censored trees by the benchmark's exact rules (nimiwae_prep.py): microplot debut < 3 in, >= 3 real visits, a visit
    < 3 in and a later one >= 3 in; hidden = real visits before the first >= 3-in one, interval-censored in [1 in, 3 in)."""
    r = tr[tr.obs].sort_values(K_ + ['measdate']).copy()
    r['_nvis'] = (r.dia >= PSEUDO_IN).astype(int).groupby([r[c] for c in K_], sort=False).cumsum()
    g = r.groupby(K_, sort=False)
    r['_h'] = (r._nvis == 0).astype(int); r['_v'] = 1 - r._h
    t = pd.DataFrame({'tpa0': g.tpa.first(), 'dia0': g.dia.first(), 'n': g.size(), 'nh': g._h.sum(), 'nv': g._v.sum()})
    ok = t[(t.tpa0 > MICRO_TPA) & (t.dia0 < PSEUDO_IN) & (t.n >= 3) & (t.nh >= 1) & (t.nv >= 1)].reset_index()[K_]
    b = r.merge(ok, on=K_, how='inner'); b['hidden'] = b._nvis == 0
    b['obs'] = ~b.hidden; b['thr_in'] = PSEUDO_IN; b['lo_in'] = np.where(b.hidden, 1.0, np.nan)
    return b.drop(columns=['_nvis', '_h', '_v'])

def predictive(model, B, K):
    """log p(x_hidden | x_obs) ≈ log Σ_k w_k(x_obs) p(x_hidden | path_k): the posterior is inferred WITHOUT the hidden values (as at test)."""
    lw, sD, sH, muA, lsA = model.logw(B, K); h = B['hid'][None]
    nl = lambda x, mu_, ls_: -0.5 * ((x - mu_) / torch.exp(ls_)) ** 2 - ls_ - 0.5 * math.log(2 * math.pi)
    lph = ((nl(B['lD'][None], sD, ls_dia(B['lD'])[None]) + nl(B['lA'][None], muA, lsA)) * h).sum(-1) + (nl(B['lH'][None], sH, model.lsH) * h * B['hasH'][None]).sum(-1)
    return torch.logsumexp(torch.log_softmax(lw, 0) + lph, 0), lw

def train(tr, a, val_fn=None, log=print):
    """Fit on the training slots `tr` (+ their pseudo-censored trees); cosine schedule, the FINAL epoch is the model. -> model, atab, hist."""
    torch.manual_seed(a.seed); torch.set_num_threads(a.threads); rng = np.random.default_rng(a.seed)
    atab = age_table(tr)
    B, vocab, _ = build(tr, None, atab); n = B['m'].shape[0]
    if a.max_trees and a.max_trees < n: B = sub(B, torch.tensor(rng.permutation(n)[:a.max_trees])); n = a.max_trees
    pb = pseudo_bench(tr); P, _, _ = build(pb, vocab, atab); n_p = P['m'].shape[0]
    log(f'pseudo-censored training trees {n_p:,}, hidden visits {int(P["hid"].sum()):,}')
    model = make_model(vocab)
    log(f'allometry OLS start (a, b, c): {init_allometry(model, tr, vocab).round(3)}')
    opt = torch.optim.AdamW(model.parameters(), lr=2e-3, weight_decay=1e-5)
    sched = torch.optim.lr_scheduler.CosineAnnealingLR(opt, T_max=a.epochs * ((n + a.bs - 1) // a.bs), eta_min=1e-5)
    log(f'train trees {n:,}; slots {int(B["v"].sum()):,}, observed {int(B["m"].sum()):,}')
    hist = []
    for ep in range(a.epochs):
        t0, tot, nb = time.time(), 0.0, 0
        for ix in batches(n, a.bs, rng):
            b = sub(B, ix); lw, *_ = model.logw(b, a.k)
            loss = -(torch.logsumexp(lw, 0) - math.log(a.k)).mean()
            pp = sub(P, torch.tensor(rng.integers(0, n_p, a.bs_p)))                      # + supervised predictive term (training plots only)
            lpred, lwp = predictive(model, pp, a.k)
            loss = loss + a.lam * (-(torch.logsumexp(lwp, 0) - math.log(a.k)).mean() - lpred.mean()) * a.bs_p / a.bs
            opt.zero_grad(); loss.backward(); nn.utils.clip_grad_norm_(model.parameters(), 5.0); opt.step(); sched.step()
            tot += loss.item(); nb += 1
        r = val_fn(model, atab) if val_fn else {}
        hist.append(dict(epoch=ep, loss=tot / nb, lr=sched.get_last_lr()[0], **{f'val_{k}': v for k, v in r.items()},
                         field_miss=torch.sigmoid(model.leps).item(), proc_sd=torch.exp(model.lsP).item(), rho=torch.tanh(model.rP).item()))
        log(json.dumps(hist[-1]) + f' {time.time() - t0:.0f}s')
    return model, atab, hist

def add_train_args(ap):
    ap.add_argument('--epochs', type=int, default=10); ap.add_argument('--k', type=int, default=16); ap.add_argument('--bs', type=int, default=1024)
    ap.add_argument('--seed', type=int, default=0); ap.add_argument('--threads', type=int, default=7); ap.add_argument('--max-trees', type=int, default=0)
    ap.add_argument('--bs-p', type=int, default=256); ap.add_argument('--lam', type=float, default=1.0)

def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--data', required=True); add_train_args(ap); a = ap.parse_args()
    seq = pd.read_parquet(f'{a.data}/seq.parquet'); tr = seq[seq.split == 'train']
    model, atab, hist = train(tr, a, val_fn=lambda m, at: bench_eval(m, a.data, 'val', at)[0], log=lambda x: print(x, flush=True))
    atab.to_parquet(f'{a.data}/age_proxy_table.parquet')
    torch.save({'state': model.state_dict(), 'vocab': model.vocab, 'epoch': a.epochs - 1}, f'{a.data}/nimiwae.pt')
    w = fit_width(model, a.data, atab); ck = torch.load(f'{a.data}/nimiwae.pt', weights_only=False); ck['w_int'] = w; torch.save(ck, f'{a.data}/nimiwae.pt')
    r, *_ = bench_eval(model, a.data, 'val', atab, w_int=w)
    json.dump(dict(hist=hist, width=w, args=vars(a)), open(f'{a.data}/train_log.json', 'w'), indent=1)
    print(f'final epoch {a.epochs - 1}; VAL width factors (imputed < 2 in, >= 2 in) = {w} -> VAL 80 % coverage {r["cov80"]:.3f} -> {a.data}/nimiwae.pt')

if __name__ == '__main__':
    main()
