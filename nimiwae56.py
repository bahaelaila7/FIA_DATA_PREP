"""D56 — D55 plus per-interval process noise on growth with its own proposal (DECISIONS.md D56; everything else as D55 + amendments 1-3).
Noise     u_t ∈ R² ~ N(0, I) per slot: increment (flex) or rate (struct) × exp(σ u_t − σ²/2), σ_D, σ_H learned; monotone kept.
Proposal  q(u_t | z, x_obs, r) from the z encoder's per-slot states and z (NIMIWAE encoder 2); DReG with both q densities' parameters detached.
--- D55 ---
Latents   z ~ p(z | statics); end e (first slot at which the tree is gone; T = alive through the span) and cause f ∈ {death, cut}, summed out.
Size      latent DIA path D_t = Σ_{u<=t} δD_u (δ >= 0; may stay below 1 in, unobservable), HT likewise: monotone.
Existence E_t = 1[D_t >= 1 in] · 1[t < e]  (soft step σ(κ (log D_t − log 0.95 in)), κ = 40): exactly one appearance, at most one end.
Censoring 'known': FIA's design rule as a fixed mask model on the latent size (no learned parameter; D55 am. 1-2);
          'mar': no mask likelihood, only the OBSERVED existence variable (TRUE / FALSE, hard / soft).
Structure missing sizes = the decoder's latent path D(z) (no second encoder, as in not-MIWAE); q(z | x_obs, r, c) sees the mask.
At risk   from the first slot at which the tree exists; competing per-interval (survive, die, cut).
Bound     IWAE; encoder gradients DReG."""
import math, time, json
from torch.func import functional_call
import numpy as np, pandas as pd, torch, torch.nn as nn, torch.nn.functional as F

T_MAX, DZ, D_MODEL = 8, 16, 64
KAPPA, NEG, RHO_MAX = 40.0, -1e4, 0.05
HALF_UNIT = 0.05            # DIA is recorded to 0.1 in: the tally step sits half a unit below the limit (amendment 3)
BA_K = 0.005454             # ft² per in² (basal area = BA_K · DIA²)
K_ = ['statecd', 'unitcd', 'countycd', 'plot', 'subp', 'tree']
NORM_COLS = ('sicond', 'slope', 'tm_n', 'pp_n', 'vp_n')
TRT = [10, 20, 30, 40, 50]; DST = [10, 20, 30, 40, 50, 60, 70, 80, 90]
COV = ['comp', 'stems', 'tma', 'ppa', 'vpa', 'nbs', 'mnbs', 'nbp', 'mnbp', 'nbn', 'can', 'mcan', 'tyr', 'dt'] + \
      [f'trt{c}' for c in TRT] + [f'dst{c}' for c in DST]
NCOV = len(COV)
DMG_GROUPS = [1, 2, 3, 4, 5, 6, 7, 8, 9]       # DAMAGE_AGENT_CD // 10000: insect, disease, fire, animal, abiotic, competition, human, multi, other
NDQ = len(DMG_GROUPS) + 1 + 2 + 3 + 1          # encoder damage channels: groups, recorded mask, broken (+mask), cull class one-hot, mask
NSUMM = 9
EV = {'none': 0, 'true_hard': 1, 'false_hard': 2, 'true_soft': 3, 'false_soft': 4}
MODE = {'cutoff': 0, 'ignore': 1, 'missed': 2, 'interval': 3}
CUT_FATES = ('harvest_explicit', 'harvest_inferred', 'disappeared_inferred_removal')

def ls_dia(lD):
    return 0.5 * torch.log(0.02 ** 2 + (0.06 / torch.exp(lD)) ** 2)

# ------------------------------------------------------------------------------------------------------------------------------- data
def build(seq: pd.DataFrame, vocab=None):
    """Per-tree tensors [n, T_MAX]. Optional benchmark columns: hidden (real visit hidden), limit_in, lo_in, e_known, drop_false (tree),
    rcd_override (tree), end_hidden (tree: hide the end evidence; absence uninformative from j on), cut_at (slot measdate: truncate)."""
    seq = seq.sort_values(K_ + ['measdate']).copy()
    if vocab is None:
        vocab = {c: {v: i + 1 for i, v in enumerate(sorted(seq[c].dropna().unique()))} for c in ('spcd', 'spgrpcd', 'epa_l3', 'major_spgrpcd', 'fortypcd')}
        vocab['_norm'] = {c: (float(seq[c].mean()), float(seq[c].std()) or 1.0) for c in NORM_COLS}
    t = (pd.to_datetime(seq.measdate) - pd.Timestamp('2000-01-01')) / pd.Timedelta(days=1) / 365.25
    g = seq.groupby(K_, sort=False); tid = g.ngroup().to_numpy(); pos = g.cumcount().to_numpy()
    assert pos.max() < T_MAX, f'more slots ({pos.max() + 1}) than T_MAX'
    n = int(tid.max()) + 1; N = len(seq)
    def mat(v, fill=0.0):
        out = np.full((n, T_MAX), fill, np.float32)
        out[tid, pos] = np.nan_to_num(np.asarray(v, np.float64), nan=fill).astype(np.float32); return out
    tcol = lambda c, d=None: seq[c].to_numpy() if c in seq else np.full(N, d, dtype=object if d is None else type(d))
    hid = seq.hidden.fillna(False).to_numpy(bool) if 'hidden' in seq else np.zeros(N, bool)
    kind = seq.slot_kind.to_numpy()
    rec_any = kind == 'real'; real = rec_any & ~hid
    dia, ht = seq.dia.to_numpy(float), seq.ht.to_numpy(float)
    mD = real & ~np.isnan(dia); mH = real & ~np.isnan(ht)
    mcr = real & seq.cr.notna().to_numpy(); mcc = real & seq.cclcd.notna().to_numpy()
    def per_tree_minmax(mask, fn):
        out = np.full(n, -1); s = pd.Series(pos[mask], index=tid[mask]).groupby(level=0).agg(fn); out[s.index.to_numpy()] = s.to_numpy(); return out
    f_idx = per_tree_minmax(real, 'min'); l_idx = per_tree_minmax(real, 'max')
    assert (f_idx >= 0).all(), 'every tree needs a visible real record'
    F_, L_ = f_idx[tid], l_idx[tid]
    rcd = seq.rcd_override.to_numpy(float) if 'rcd_override' in seq else seq.rcd0.to_numpy(float)
    thr_micro = seq.thr_micro.to_numpy(float)                                                       # design levels from the data (am. 9)
    limit = seq.limit_in.to_numpy(float) if 'limit_in' in seq else np.where(rcd == 2, thr_micro, seq.thr_in.to_numpy(float))
    micro = limit <= thr_micro + 1e-9                                                                 # tallied at the design's smallest level
    lo = seq.lo_in.to_numpy(float) if 'lo_in' in seq else np.full(N, np.nan)
    cst = seq.cond_status.to_numpy(float)
    fsp, fmp = seq.forest_subpcond_prop.to_numpy(float), seq.forest_micrcond_prop.to_numpy(float)
    # D55 am. 6 (user): a no-record visit's sampling is known only at subplot / microplot level (no record ties the tree to a condition):
    # psamp = the forested share of the tree's tally area (microplot for microplot-level slots, else subplot); 1 at real records
    psamp = np.where(real, 1.0, np.clip(np.nan_to_num(np.where(micro, fmp, fsp), nan=0.0), 0.0, 1.0))
    sampled = psamp > 0
    fate = seq.terminal_fate.fillna('').to_numpy(str)
    end_hidden = seq.end_hidden.fillna(False).to_numpy(bool) if 'end_hidden' in seq else np.zeros(N, bool)
    dead_md = pd.to_datetime(seq.dead_md); s0_md = pd.to_datetime(seq.s0_md); md = pd.to_datetime(seq.measdate)
    agent = seq.dead_agent.to_numpy(float)
    # definitive end slot (first slot at which the tree is gone) and its cause
    is_dead_end = (md >= dead_md).to_numpy() & ~end_hidden                                       # first slot ON OR AFTER the dead record
    is_inf_cut = np.isin(fate, ['harvest_inferred', 'disappeared_inferred_removal']) & (kind == 'post') & ~end_hidden
    is_moved = (fate == 'censored_moved') & (md >= s0_md).to_numpy() & ~end_hidden
    end_here = is_dead_end | is_moved
    first_post = (kind == 'post') & (pos == L_ + 1)
    end_here = end_here | (is_inf_cut & first_post)
    endpos = per_tree_minmax(end_here, 'min'); EP = endpos[tid]
    cause_cut = np.isin(fate, CUT_FATES) | (agent == 80)
    tree_cause = pd.Series(np.where(cause_cut, 1, 0), index=tid).groupby(level=0).max().reindex(range(n)).fillna(0).to_numpy().astype(int)
    tree_moved = pd.Series(fate == 'censored_moved', index=tid).groupby(level=0).max().reindex(range(n)).fillna(False).to_numpy(bool)
    # existence evidence
    ev = np.zeros(N, int)
    e_known = seq.e_known.fillna(False).to_numpy(bool) if 'e_known' in seq else np.zeros(N, bool)
    drop_false = seq.drop_false.fillna(False).to_numpy(bool) if 'drop_false' in seq else np.zeros(N, bool)
    ev[(pos >= F_) & (pos <= L_)] = EV['true_hard']                                         # debut -> last visible record, gaps included
    ev[hid & e_known] = EV['true_hard']
    clean_pre = (pos < F_) & micro & np.isin(rcd, [1, 2]) & sampled & (np.nan_to_num(fmp, nan=0) >= 1.0) & ~drop_false & (kind == 'pre')
    ev[clean_pre] = EV['false_soft']
    missed_prev = (pos == F_ - 1) & np.isin(rcd, [3, 6, 7]) & sampled & ~hid
    ev[missed_prev] = EV['true_soft']
    ev[(EP >= 0) & (pos >= EP)] = EV['false_hard']
    shrank = (fate == 'censored_shrank') & (md == s0_md).to_numpy() & ~end_hidden
    ev[shrank] = EV['true_soft']
    # missingness mode for unrecorded slots
    mode = np.full(N, MODE['cutoff'])
    mode[(pos > F_) & (pos < L_)] = MODE['ignore']                                          # gaps: exist, no cutoff penalty
    mode[~sampled] = MODE['ignore']
    post = pos > L_
    ign_after = np.isin(fate, ['censored_procedural', 'censored_procedural_unrecorded', 'censored_shrank', 'censored_nonsampled']) | end_hidden   # am. 5: code 9 = no information
    mode[post & ign_after] = MODE['ignore']
    mode[missed_prev] = MODE['missed']
    # pre-debut visits that carry NO information (user): procedural change (8) / previously non-sampled area (9): all earlier visits;
    # missed tallies (3 / 6 / 7): every visit BEFORE the missed one (the tree may have been missed earlier too; inferred back)
    pre_ = pos < F_
    mode[pre_ & np.isin(rcd, [8, 9])] = MODE['ignore']
    mode[pre_ & np.isin(rcd, [3, 6, 7]) & (pos < F_ - 1)] = MODE['ignore']
    mode[~np.isnan(lo)] = MODE['interval']
    own_ba = np.where(rec_any, seq.own_ba.fillna(0).to_numpy(), 0.0); own_tpa = np.where(rec_any, seq.tpa.fillna(0).to_numpy(), 0.0)
    dt = t.groupby([seq[c] for c in K_], sort=False).diff().fillna(5.0).to_numpy()
    tr_ = np.stack([seq[c].fillna(0).to_numpy(float) for c in ('trtcd1', 'trtcd2', 'trtcd3')], 1)
    ds_ = np.stack([seq[c].fillna(0).to_numpy(float) for c in ('dstrbcd1', 'dstrbcd2', 'dstrbcd3')], 1)
    cov = dict(comp=np.log1p(np.clip(seq.sub_ba.fillna(0).to_numpy() - own_ba, 0, None)) / 5,
               stems=np.log1p(np.clip(seq.sub_tpa.fillna(0).to_numpy() - own_tpa, 0, None)) / 8,
               tma=seq.tm_anom.to_numpy(float) / 0.5, ppa=seq.pp_anom.to_numpy(float) / 0.1, vpa=seq.vp_anom.to_numpy(float) / 0.5,
               nbs=seq.nb_inc_sub.to_numpy(float) * 10, mnbs=seq.nb_inc_sub.notna().to_numpy(float),
               nbp=seq.nb_inc_plt.to_numpy(float) * 10, mnbp=seq.nb_inc_plt.notna().to_numpy(float),
               nbn=np.log1p(seq.nb_n_plt.fillna(0).to_numpy(float)) / 3,
               can=seq.canopy_pct.to_numpy(float) / 100, mcan=seq.canopy_pct.notna().to_numpy(float), tyr=t.to_numpy() / 20, dt=dt / 10,
               # D55 am. 7: subplot area share carrying each code group (prep trt_s* / dst_s*); legacy frames: the slot's own codes, 0 / 1
               **{f'trt{c}': (seq[f'trt_s{c}'].fillna(0).to_numpy(float) if f'trt_s{c}' in seq else ((tr_ >= c) & (tr_ < c + 10)).any(1).astype(float)) for c in TRT},
               **{f'dst{c}': (seq[f'dst_s{c}'].fillna(0).to_numpy(float) if f'dst_s{c}' in seq else ((ds_ >= c) & (ds_ < c + 10)).any(1).astype(float)) for c in DST})
    # completion span (user): unsampled visits are completed only when sandwiched between the tree's sampled visits
    sfirst = per_tree_minmax(sampled, 'min'); slast = per_tree_minmax(sampled, 'max')
    in_span = (pos >= sfirst[tid]) & (pos <= slast[tid])
    # D54 am. 5: damage observations at VISIBLE real records only (masked like any measurement); DAMAGE_AGENT_CD recorded 2013+
    dcodes = np.stack([seq[c].to_numpy(float) if c in seq else np.full(N, np.nan) for c in ('dmg1', 'dmg2', 'dmg3')], 1)
    m_dmg = real & ~np.isnan(dcodes[:, 0])
    dgrp = np.floor(np.nan_to_num(dcodes, nan=0) / 10000).astype(int)
    dmg = np.stack([((dgrp == g).any(1) & m_dmg).astype(float) for g in DMG_GROUPS], 1)
    aht = seq.actualht.to_numpy(float) if 'actualht' in seq else np.full(N, np.nan)
    m_brk = real & ~np.isnan(aht) & ~np.isnan(ht); brk = (m_brk & (aht < ht)).astype(float)
    tcl = seq.treeclcd.to_numpy(float) if 'treeclcd' in seq else np.full(N, np.nan)
    m_cl = real & np.isin(tcl, [2, 3, 4]); cl = np.where(m_cl, np.clip(np.nan_to_num(tcl, nan=2) - 2, 0, 2), 0)
    lgD = np.log(np.clip(np.nan_to_num(dia, nan=1.0), 0.05, None)); lgH = np.log(np.clip(np.nan_to_num(ht, nan=4.5), 1.0, None))
    X = dict(lD=mat(lgD), lH=mat(lgH),
             mD=mat(mD), mH=mat(mH), cr=mat(np.where(mcr, seq.cr.to_numpy(float) / 100, 0)), mcr=mat(mcr),
             cc=mat(np.where(mcc, seq.cclcd.to_numpy(float), 0)), mcc=mat(mcc), v=mat(np.ones(N)), hid=mat(hid), real=mat(real),
             sampled=mat(sampled), psamp=mat(psamp), llim=mat(np.log(limit - HALF_UNIT)), llo=mat(np.log(lo - HALF_UNIT), -20.0), fmp=mat(fmp),
             lex=mat(np.log(thr_micro - HALF_UNIT)),                                                   # existence = qualifies at the smallest level
             ev=mat(ev), mode=mat(mode), cov=np.stack([mat(cov[c]) for c in COV], -1),
             in_span=mat(in_span),
             dmg=np.stack([mat(dmg[:, j]) for j in range(len(DMG_GROUPS))], -1), m_dmg=mat(m_dmg), brk=mat(brk), m_brk=mat(m_brk),
             cl=mat(cl), m_cl=mat(m_cl))
    st = g.head(1)
    def ids(c): return st[c].map(vocab[c]).fillna(0).astype(int).to_numpy()
    def z_(c): mu, sd = vocab['_norm'][c]; return np.nan_to_num((st[c].to_numpy(dtype=float) - mu) / sd, nan=0.0)
    asp = np.deg2rad(st.aspect.fillna(0).to_numpy(dtype=float)); flat = st.slope.fillna(0).to_numpy(dtype=float) < 5
    rc0 = pd.Series(rcd, index=tid).groupby(level=0).first().reindex(range(n)).to_numpy()
    tmax = lambda a_: pd.Series(np.asarray(a_, float), index=tid).groupby(level=0).max().reindex(range(n)).fillna(0).to_numpy()
    revised = st.sp_revised.fillna(False).astype(float).to_numpy() if 'sp_revised' in st else np.zeros(n)
    summ = np.stack([revised, tmax(m_dmg), tmax(dmg.sum(1) > 0), tmax(dmg[:, 0]), tmax(dmg[:, 1]), tmax(dmg[:, 4]), tmax(dmg[:, 8]),
                     tmax(brk), tmax(m_cl & (cl > 0))], 1)          # visible records only; 'damage ever recorded' disambiguates 0 (pre-2013)
    S = dict(sp=ids('spgrpcd'), l3=ids('epa_l3'), maj=ids('major_spgrpcd'), fty=ids('fortypcd'),
             scl=np.clip(st.siteclcd.fillna(0).to_numpy(dtype=float), 0, 7).astype(int), phy=np.clip(st.physclcd.fillna(0).to_numpy(dtype=float), 0, 49).astype(int),
             snum=np.stack([st.planted.fillna(False).astype(float).to_numpy(), z_('sicond'), st.sicond.isna().to_numpy(float), z_('slope'),
                            np.where(flat, 0, np.sin(asp)), np.where(flat, 0, np.cos(asp)), z_('tm_n'), z_('pp_n'), z_('vp_n')], 1),
             summ=summ, spc=ids('spcd'),
             rcd=np.nan_to_num(rc0, nan=0).clip(0, 9).astype(int), f=f_idx, l=l_idx, cause=tree_cause, moved=tree_moved.astype(int),
             ndef=(endpos >= 0).astype(int))
    longs = ('spc', 'sp', 'l3', 'maj', 'fty', 'scl', 'phy', 'rcd', 'f', 'l', 'cause', 'moved', 'ndef')
    T = {k: torch.tensor(np.array(v), dtype=torch.long if k in longs else torch.float32) for k, v in {**X, **S}.items()}
    T['ev'] = T['ev'].long(); T['mode'] = T['mode'].long(); T['cl'] = T['cl'].long()
    return T, vocab, seq.assign(_tid=tid, _pos=pos)

def sub(B, ix): return {k: v[ix] for k, v in B.items()}

# ------------------------------------------------------------------------------------------------------------------------------ model
class Static(nn.Module):
    """Per-tree statics -> 64-d token. use_summ: the per-tree record summaries (species revised, damage / broken / cull ever recorded) are
    functions of the tree's OWN record sequence (they leak its survival: D55 am. 4) -> encoder only, never the prior / decoder."""
    def __init__(s, nsp, nl3, nmaj, nfty, nspc, use_summ=True):
        super().__init__()
        s.use_summ = use_summ
        s.e = nn.ModuleList([nn.Embedding(nsp + 1, 8), nn.Embedding(nl3 + 1, 4), nn.Embedding(nmaj + 1, 2), nn.Embedding(nfty + 1, 4),
                             nn.Embedding(8, 3), nn.Embedding(50, 3), nn.Embedding(nspc + 1, 8)])   # + species (resolved SPCD); unseen -> 0
        s.out = nn.Linear(8 + 4 + 2 + 4 + 3 + 3 + 8 + 9 + (NSUMM if use_summ else 0), D_MODEL)
    def forward(s, B):
        x = [s.e[0](B['sp']), s.e[1](B['l3']), s.e[2](B['maj']), s.e[3](B['fty']), s.e[4](B['scl']), s.e[5](B['phy']), s.e[6](B['spc']), B['snum']]
        return s.out(torch.cat(x + ([B['summ']] if s.use_summ else []), -1))

def tenc(d, layers=2):
    return nn.TransformerEncoder(nn.TransformerEncoderLayer(d, 4, 2 * d, dropout=0.0, batch_first=True, norm_first=True), layers,
                                 enable_nested_tensor=False)

NQ = 15 + len(EV) + len(MODE)

class NIMIWAE56(nn.Module):
    """censor: 'known' (FIA design rule as an observed inequality, no learned missingness) | 'mar' (existence labels only).
    growth: 'flex' (per-slot increments from the decoder on z + context) | 'struct' (Δt · exp(vigour(z) + g(size, species) + c(context)))."""
    def __init__(s, nsp, nl3, nmaj, nfty, nspc, censor='known', growth='flex', noise=True, prior_ctx=False):
        super().__init__()
        assert censor in ('known', 'mar') and growth in ('flex', 'struct')
        s.censor, s.growth, s.noise, s.prior_ctx = censor, growth, noise, prior_ctx
        d = D_MODEL
        s.st_q = Static(nsp, nl3, nmaj, nfty, nspc); s.ercd = nn.Embedding(10, 4); s.ecc = nn.Embedding(6, 3)
        s.qin = nn.Linear(NCOV + NQ + 4 + 3 + NDQ, d); s.qtr = tenc(d); s.qout = nn.Linear(d, 2 * DZ)
        s.st_p = Static(nsp, nl3, nmaj, nfty, nspc, use_summ=False)                             # am. 4: no tree-own summaries
        s.prior = nn.Sequential(nn.Linear(d, d), nn.GELU(), nn.Linear(d, 2 * DZ))
        if prior_ctx:                                                                                 # D57: prior over the known context sequence
            s.pin = nn.Linear(NCOV, d); s.ptr = tenc(d)
        s.din = nn.Linear(NCOV, d); s.dz = nn.Linear(DZ, d); s.dtr = tenc(d)
        s.dout = nn.Linear(d, 2 + 3 + 1 + 5 + len(DMG_GROUPS) + 1 + 3)            # dD, dH (flex) | (survive, die, cut) | cr | cclcd | damage | broken | cull
        with torch.no_grad():
            s.dout.bias.zero_(); s.dout.bias[0] = -0.5; s.dout.bias[1] = 1.5; s.dout.bias[2] = 3.0
        if growth == 'struct':                                                        # shared responses + per-tree vigour
            s.gv = nn.Linear(DZ, 2)                                                   # vigour (log scale) for DIA, HT
            s.gs = nn.Sequential(nn.Linear(2 + 8, 32), nn.GELU(), nn.Linear(32, 2))   # size response: [log D_prev, log H_prev, species emb]
            s.gsp = nn.Embedding(nspc + 1, 8)
            s.gc = nn.Sequential(nn.Linear(NCOV + d, 64), nn.GELU(), nn.Linear(64, 2))   # context response (no z)
            s.g0 = nn.Linear(DZ + d, 2)                                               # initial size at slot 0 (DIA may be < 1 in)
            with torch.no_grad():
                for m_ in (s.gs[-1], s.gc[-1]): m_.weight.zero_(); m_.bias.zero_()
                s.gv.bias.copy_(torch.tensor([math.log(0.1), math.log(0.8)])); s.g0.bias.copy_(torch.tensor([0.0, 2.0]))
        s.lsdisc, s.lsH, s.lscr = nn.Parameter(torch.tensor(-4.0)), nn.Parameter(torch.tensor(-2.0)), nn.Parameter(torch.tensor(-2.0))
        s.lrho = nn.Parameter(torch.tensor(-2.0))
        if noise:                                                                                     # D56: process noise + encoder 2
            s.lsu = nn.Parameter(torch.full((2,), math.log(0.3)))                                     # σ_D, σ_H (log)
            s.u_net = nn.Sequential(nn.Linear(d + DZ, d), nn.GELU(), nn.Linear(d, 4))               # per slot: μ_u (2), log σ²_u (2)
            with torch.no_grad(): s.u_net[-1].weight.zero_(); s.u_net[-1].bias.zero_()                # starts at the prior
    def enc_params(s):
        mods = (s.st_q, s.ercd, s.ecc, s.qin, s.qtr, s.qout) + ((s.u_net,) if s.noise else ())
        return [p for m in mods for p in m.parameters()]
    def qu(s, hs, z, params=None):
        """q(u_t | z, x_obs, r): hs [n, T, d] per-slot encoder states, z [K, n, DZ] -> μ_u, log σ²_u [K, n, T, 2].
        params: a (detached) parameter dict for the DReG density (stop-gradient on the weights, z keeps its path)."""
        K = z.shape[0]
        x = torch.cat([hs[None].expand(K, -1, -1, -1), z[:, :, None].expand(-1, -1, T_MAX, -1)], -1)
        o = s.u_net(x) if params is None else functional_call(s.u_net, params, (x,))
        mu, lv = o.chunk(2, -1); return mu, lv.clamp(-8, 2)
    def q(s, B):
        m = B['mD']; cc = s.ecc(B['cc'].long().clamp(0, 5)) * B['mcc'][..., None]
        haslo = (B['mode'] == MODE['interval']).float()
        T = T_MAX; ar = torch.arange(T)[None]
        x = torch.cat([B['cov'], torch.stack([B['lD'] * m, m, B['lH'] * B['mH'], B['mH'], B['cr'], B['mcr'], B['mcc'], B['real'], B['psamp'],
                                              B['llim'], haslo, B['llo'].clamp(min=-1) * haslo, (ar == B['f'][:, None]).float(),
                                              (ar == B['l'][:, None]).float(), B['v']], -1),
                       F.one_hot(B['ev'], len(EV)).float(), F.one_hot(B['mode'], len(MODE)).float(),
                       s.ercd(B['rcd'])[:, None].expand(-1, T, -1), cc,
                       B['dmg'], B['m_dmg'][..., None], (B['brk'] * B['m_brk'])[..., None], B['m_brk'][..., None],
                       F.one_hot(B['cl'], 3).float() * B['m_cl'][..., None], B['m_cl'][..., None]], -1)
        h = s.qin(x); cls = s.st_q(B)[:, None]
        pad = torch.cat([torch.zeros_like(B['v'][:, :1]), 1 - B['v']], 1).bool()
        oo = s.qtr(torch.cat([cls, h], 1), src_key_padding_mask=pad)
        mu, lv = s.qout(oo[:, 0]).chunk(2, -1); s._hs = oo[:, 1:]                                    # per-slot states for encoder 2
        return mu, lv.clamp(-8, 4)
    def p_z(s, B):
        tok = s.st_p(B)
        if s.prior_ctx:                                                                               # context only (B10: invariant to the tree's records)
            pad = torch.cat([torch.zeros_like(B['v'][:, :1]), 1 - B['v']], 1).bool()
            tok = s.ptr(torch.cat([tok[:, None], s.pin(B['cov'])], 1), src_key_padding_mask=pad)[:, 0]
        mu, lv = s.prior(tok).chunk(2, -1); return mu, lv.clamp(-6, 3)
    def mult(s, u, B):
        """Noise multipliers [K, n, T, 2] = exp(σ u − σ²/2) (1 when noise is off or u is None)."""
        if not s.noise or u is None: return None
        sg = torch.exp(s.lsu); return torch.exp(sg * u - 0.5 * sg ** 2)
    def decode(s, z, B, u=None):
        K, n = z.shape[:2]; mu_ = s.mult(u, B)
        tok = s.st_p(B)
        h = s.din(B['cov'])[None] + s.dz(z)[:, :, None] + tok[None, :, None]
        pad = (1 - B['v']).bool()[None].expand(K, -1, -1).reshape(K * n, T_MAX)
        o = s.dout(s.dtr(h.reshape(K * n, T_MAX, D_MODEL), src_key_padding_mask=pad)).reshape(K, n, T_MAX, -1)
        nd = len(DMG_GROUPS)
        if s.growth == 'flex':
            dD, dH = F.softplus(o[..., 0]), F.softplus(o[..., 1])
            if mu_ is not None: dD, dH = dD * mu_[..., 0], dH * mu_[..., 1]
            D, H = torch.cumsum(dD * B['v'], -1), torch.cumsum(dH * B['v'], -1)
        else:
            D, H = s.struct_paths(z, B, tok, mu_)
        return D, H, torch.log_softmax(o[..., 2:5], -1), torch.sigmoid(o[..., 5]), o[..., 6:11], o[..., 11:11 + nd], o[..., 11 + nd], o[..., 12 + nd:15 + nd]
    def struct_paths(s, z, B, tok, mu_=None):
        """D_0 = softplus(g0(z, tree)); D_t = D_{t-1} + Δt_t · exp(vigour(z) + g(log D_{t-1}, log H_{t-1}, species) + c(context_t)); HT likewise."""
        K, n = z.shape[:2]
        dt = (B['cov'][..., COV.index('dt')] * 10.0).clamp(min=0.5)                                   # years
        cxt = s.gc(torch.cat([B['cov'], tok[:, None].expand(-1, T_MAX, -1)], -1))                       # [n, T, 2] shared context response
        vg = s.gv(z)                                                                                    # [K, n, 2]
        sp = s.gsp(B['spc'])[None].expand(K, -1, -1)
        d0 = F.softplus(s.g0(torch.cat([z, tok[None].expand(K, -1, -1)], -1)))                          # [K, n, 2]
        Ds, Hs = [d0[..., 0]], [d0[..., 1] + 1.0]
        for t in range(1, T_MAX):
            g = s.gs(torch.cat([torch.log(Ds[-1].clamp(min=1e-3))[..., None], torch.log(Hs[-1].clamp(min=1.0))[..., None], sp], -1))
            rate = torch.exp((vg + g + cxt[None, :, t]).clamp(-8, 3))
            if mu_ is not None: rate = rate * mu_[:, :, t]
            Ds.append(Ds[-1] + dt[None, :, t] * rate[..., 0] * B['v'][None, :, t]); Hs.append(Hs[-1] + dt[None, :, t] * rate[..., 1] * B['v'][None, :, t])
        return torch.stack(Ds, -1), torch.stack(Hs, -1)
    def joint(s, B, K, dreg=False):
        """-> log weights lw [K, n], log posterior over (e, f) [K, n, E, 2], latent paths D, H [K, n, T], existence step p1."""
        mq, lq = s.q(B); hs = s._hs; mp, lp = s.p_z(B)
        z = mq + torch.exp(0.5 * lq) * torch.randn(K, *mq.shape)
        c2 = 0.5 * math.log(2 * math.pi)
        u, lpu, lqu = None, 0.0, 0.0
        if s.noise:
            muu, lvu = s.qu(hs, z)
            u = muu + torch.exp(0.5 * lvu) * torch.randn_like(muu)
            vv = B['v'][None, :, :, None]
            lpu = ((-0.5 * u ** 2 - c2) * vv).sum((-1, -2))
            lqu = ((-0.5 * (u - muu) ** 2 / torch.exp(lvu) - 0.5 * lvu - c2) * vv).sum((-1, -2))
        D, H, lend, crm, ccl, dml, brl, cll = s.decode(z, B, u)
        T = T_MAX; ar = torch.arange(T)
        lDp, lHp = torch.log(D.clamp(min=1e-3)), torch.log(H.clamp(min=1.0))
        xe = KAPPA * (lDp - B['lex'][None])                                                             # qualifies at the smallest level
        lp1, lp0 = F.logsigmoid(xe), F.logsigmoid(-xe)
        nl = lambda x, mu, ls: -0.5 * ((x - mu) / torch.exp(ls)) ** 2 - ls - c2
        rho = RHO_MAX * torch.sigmoid(s.lrho); lr, l1r = torch.log(rho), torch.log(1 - rho)
        v, real, ev, mode = B['v'][None], B['real'][None], B['ev'][None], B['mode'][None]
        sdD = 0.5 * torch.log(torch.exp(2 * ls_dia(B['lD'])) + torch.exp(2 * s.lsdisc))
        meas = (nl(B['lD'][None], lDp, sdD[None]) * B['mD'][None] + nl(B['lH'][None], lHp, s.lsH) * B['mH'][None])
        lse = torch.logaddexp
        ev_alive = torch.where(ev == EV['true_hard'], lp1,
                   torch.where(ev == EV['false_hard'], lp0,
                   torch.where(ev == EV['true_soft'], lse(lp1 + l1r, lp0 + lr),
                   torch.where(ev == EV['false_soft'], lse(lp0 + l1r, lp1 + lr), torch.zeros_like(lp1)))))
        ev_gone = torch.where(ev == EV['true_hard'], torch.full_like(lp1, NEG),
                  torch.where(ev == EV['true_soft'], lr + torch.zeros_like(lp1),
                  torch.where(ev == EV['false_soft'], l1r + torch.zeros_like(lp1), torch.zeros_like(lp1))))
        ev_gone = ev_gone + real * NEG
        cens = torch.zeros_like(lp1)
        if s.censor == 'known':                                                                         # FIA design rule, no learned parameter
            xt = KAPPA * (lDp - B['llim'][None])                                                        # at / above the tally limit
            post_ = (ar[None] > B['l'][:, None])                                                        # after the last visible record
            above_ex = ((B['llim'] > B['lex'] + 1e-6) | post_)[None]   # pre-debut microplot level: the soft FALSE label carries it
            unrec = (1 - real) * B['sampled'][None]
            ps = B['psamp'][None].expand_as(xt)                                                          # am. 6: P(the tree's spot was sampled)
            psc = ps.clamp(1e-6, 1 - 1e-6)                                                               # both where-branches finite (gradients)
            lcut = torch.where(ps >= 1, F.logsigmoid(-xt), torch.logaddexp(torch.log(psc) + F.logsigmoid(-xt), torch.log1p(-psc)))
            cens = (real * F.logsigmoid(xt)                                                             # tallied: at or above the limit
                    + unrec * torch.where((mode == MODE['cutoff']) & above_ex, lcut, torch.zeros_like(xt))   # not tallied: below (if the spot was sampled)
                    + unrec * torch.where(mode == MODE['missed'], F.logsigmoid(xt), torch.zeros_like(xt))                  # missed: at / above
                    + unrec * torch.where(mode == MODE['interval'], F.logsigmoid(-xt), torch.zeros_like(xt)))             # benchmark interval
        la = (meas + ev_alive + cens) * v; lg = ev_gone * v
        Ca = torch.cat([torch.zeros_like(la[..., :1]), torch.cumsum(la, -1)], -1)
        Cg = torch.cat([torch.zeros_like(lg[..., :1]), torch.cumsum(lg, -1)], -1); Cg = Cg[..., -1:] - Cg
        lle = Ca + Cg                                                                                    # [K, n, E = T+1]
        l, nv = B['l'][None, :, None], B['v'].sum(1).long()[None, :, None]
        with torch.no_grad():
            exists = (xe > 0) & (v > 0)
            a_ = torch.where(exists.any(-1), torch.argmax(exists.float(), -1), B['f'][None].expand(K, -1))
            a_ = torch.minimum(a_, B['f'][None].expand(K, -1))[..., None]
        lsv, ldie, lcut = lend[..., 0], lend[..., 1], lend[..., 2]
        risk = (ar[None, None] > a_).float() * B['v'][None]
        csum = torch.cumsum(lsv * risk, -1); cprev = torch.cat([torch.zeros_like(csum[..., :1]), csum[..., :-1]], -1)
        j = ar[None, None]; valid = (j > l) & (j < nv)
        mv = (B['moved'] == 1)[None, :, None]
        lpe_die = torch.where(valid, cprev + torch.where(mv, torch.zeros_like(ldie), ldie), torch.full_like(ldie, NEG))
        lpe_cut = torch.where(valid & ~mv, cprev + lcut, torch.full_like(lcut, NEG))
        alive = csum.gather(-1, (nv - 1).clamp(min=0).expand(K, -1, 1))
        lpe = torch.stack([torch.cat([lpe_die, alive], -1), torch.cat([lpe_cut, torch.full_like(alive, NEG)], -1)], -1)
        known = (B['ndef'] == 1) & (B['moved'] == 0)
        cmask = torch.zeros(1, B['v'].shape[0], 1, 2); cmask[0, known, 0, 1 - B['cause'][known]] = NEG
        lpe = lpe + cmask * (torch.arange(T_MAX + 1)[None, None] < T_MAX)[..., None].float()
        lj = lpe + lle[..., None]
        lpx = torch.logsumexp(lj.reshape(K, lj.shape[1], -1), -1)
        lcr = (nl(B['cr'][None], crm, s.lscr) * B['mcr'][None]).sum(-1)
        lcc = (torch.log_softmax(ccl, -1).gather(-1, (B['cc'].long().clamp(1, 5) - 1)[None, ..., None].expand(K, -1, -1, 1))[..., 0] * B['mcc'][None]).sum(-1)
        ldm = (-F.binary_cross_entropy_with_logits(dml, B['dmg'][None].expand_as(dml), reduction='none').sum(-1) * B['m_dmg'][None]).sum(-1)
        lbk = (-F.binary_cross_entropy_with_logits(brl, B['brk'][None].expand_as(brl), reduction='none') * B['m_brk'][None]).sum(-1)
        lcl = (torch.log_softmax(cll, -1).gather(-1, B['cl'][None, ..., None].expand(K, -1, -1, 1))[..., 0] * B['m_cl'][None]).sum(-1)
        lrec = lcr + lcc + ldm + lbk + lcl
        lpz = (-0.5 * (z - mp) ** 2 / torch.exp(lp) - 0.5 * lp - c2).sum(-1)
        lqz = (-0.5 * (z - mq) ** 2 / torch.exp(lq) - 0.5 * lq - c2).sum(-1)
        lw = lpx + lrec + lpz - lqz + lpu - lqu
        out = dict(lw=lw, lpost=lj - lpx[..., None, None], D=D, H=H, p1=torch.exp(lp1), lsdisc=s.lsdisc, z=z, u=u)
        if dreg:
            mqd, lqd = mq.detach(), lq.detach()
            lqu_d = 0.0
            if s.noise:                                                                               # stop-gradient on encoder-2 weights and slot states; z keeps its path
                muu_d, lvu_d = s.qu(hs.detach(), z, params={k: v.detach() for k, v in s.u_net.named_parameters()})
                lqu_d = ((-0.5 * (u - muu_d) ** 2 / torch.exp(lvu_d) - 0.5 * lvu_d - c2) * B['v'][None, :, :, None]).sum((-1, -2))
            out['lw_dreg'] = lpx + lrec + lpz - (-0.5 * (z - mqd) ** 2 / torch.exp(lqd) - 0.5 * lqd - c2).sum(-1) + lpu - lqu_d
        return out

def make_model(vocab, censor='known', growth='flex', noise=True, prior_ctx=False):
    m = NIMIWAE56(len(vocab['spgrpcd']), len(vocab['epa_l3']), len(vocab['major_spgrpcd']), len(vocab['fortypcd']) + 1, len(vocab['spcd']),
                  censor=censor, growth=growth, noise=noise, prior_ctx=prior_ctx)
    m.vocab = vocab; m.cfg = dict(censor=censor, growth=growth, noise=noise, prior_ctx=prior_ctx); return m

def wmedian(X, w):
    Xs, ix = torch.sort(X, 0); cw = torch.cumsum(w[..., None].expand_as(X).gather(0, ix), 0)
    return Xs.gather(0, (cw < 0.5).sum(0, keepdim=True).clamp(max=X.shape[0] - 1))[0]

@torch.no_grad()
def impute(model, B, K=100, bs=512, nsamp=200, w_int=None):
    """Per slot: P(exists), P(gone by death / cut); unconditional latent DIA median (P1, monotone); size given existence (median log DIA /
    HT; undefined where no sample exists), 10 / 90 % of the measured log DIA (per slot-type widths), expected basal area; bounded by the
    tree's own measurements (am. 7); in_span."""
    w_int = w_int or {'pre': (1.0, 1.0), 'post': (1.0, 1.0)}
    keys = ('pE', 'pDie', 'pCut', 'lD_u', 'lD_raw', 'BA_raw', 'lD', 'lH', 'q10', 'q90', 'BA', 'in_span')
    out = {k: [] for k in keys}
    ar = torch.arange(T_MAX)
    for i in range(0, B['v'].shape[0], bs):
        b = sub(B, slice(i, i + bs)); o = model.joint(b, K)
        W = torch.softmax(o['lw'], 0)[..., None, None] * torch.exp(o['lpost'])                        # [K, n, E, 2]
        Kk, n = W.shape[:2]
        nv = b['v'].sum(1).long()
        eI = torch.arange(T_MAX + 1)[None, None, :, None]; tI = ar[None, None, None, :]
        alive_w = ((tI < eI) | (eI == T_MAX)).float() * (tI < nv[None, :, None, None]).float()        # [1, n, E, T]
        We = W.sum(-1)                                                                                 # [K, n, E]
        Walive = (We[..., None] * alive_w).sum(2)                                                      # [K, n, T] weight alive at t
        p1 = o['p1']                                                                                   # [K, n, T]
        pE = (Walive * p1).sum(0)
        gone = ((tI >= eI) & (eI < T_MAX)).float()
        pDie = (W[..., 0][..., None] * gone).sum((0, 2)); pCut = (W[..., 1][..., None] * gone).sum((0, 2))
        Wk = We.sum(-1)                                                                                # [K, n]
        D, H = o['D'], o['H']
        lD_u = torch.log(wmedian(D, Wk).clamp(min=1e-3))
        Wc = Walive * p1; wsum = Wc.sum(0); Wc = Wc / wsum[None].clamp(min=1e-12); defined = wsum > 1e-9
        def cmed(X):
            Xs, ix = torch.sort(X, 0); cw = torch.cumsum(Wc.gather(0, ix), 0)
            return Xs.gather(0, (cw < 0.5).sum(0, keepdim=True).clamp(max=X.shape[0] - 1))[0]
        lDf, lHf = torch.log(D.clamp(min=1e-3)), torch.log(H.clamp(min=1.0))
        lD, lH = cmed(lDf), cmed(lHf)
        BA = (Wc * BA_K * D ** 2).sum(0)
        sdm = torch.sqrt(torch.exp(2 * ls_dia(lDf)) + torch.exp(2 * o['lsdisc']))
        q10, q90 = [], []
        typ_pre = (ar[None] < b['f'][:, None]); typ_post = (ar[None] > b['l'][:, None])
        for tt in range(T_MAX):
            ks = torch.multinomial(Wc[:, :, tt].T + 1e-12, nsamp, replacement=True); arn = torch.arange(n)[:, None]
            draw = lDf[:, :, tt].T[arn, ks] + sdm[:, :, tt].T[arn, ks] * torch.randn(n, nsamp)
            small = lD[:, tt] < math.log(2.0)
            wpre = torch.where(small, torch.tensor(float(w_int['pre'][0])), torch.tensor(float(w_int['pre'][1])))
            wpost = torch.where(small, torch.tensor(float(w_int['post'][0])), torch.tensor(float(w_int['post'][1])))
            wt = torch.where(typ_pre[:, tt], wpre, torch.where(typ_post[:, tt], wpost, torch.ones_like(wpre)))
            draw = lD[:, tt, None] + wt[:, None] * (draw - lD[:, tt, None])
            q = torch.quantile(draw, torch.tensor([0.1, 0.9]), dim=1); q10.append(q[0]); q90.append(q[1])
        lD_b = obs_bounds(lD, b['lD'], b['mD'], monotone=True); lH_b = obs_bounds(lH, b['lH'], b['mH'], monotone=True)
        lD_ub = obs_bounds(lD_u, b['lD'], b['mD'], lower=False)
        sh = lD_b - lD; BAb = BA * torch.exp(2 * sh)
        q10_, q90_ = torch.stack(q10, 1) + sh, torch.stack(q90, 1) + sh
        undef = lambda X: torch.where(defined | (b['mD'] > 0), X, torch.full_like(X, float('nan')))
        for k_, v_ in (('pE', pE), ('pDie', pDie), ('pCut', pCut), ('lD_u', lD_ub), ('lD_raw', undef(lD)), ('BA_raw', undef(BA)),
                       ('lD', undef(lD_b)), ('lH', undef(lH_b)), ('q10', undef(q10_)), ('q90', undef(q90_)), ('BA', undef(BAb)), ('in_span', b['in_span'])):
            out[k_].append(v_)
    return {k: torch.cat(v) for k, v in out.items()}

def obs_bounds(X, obs, m, monotone=False, lower=True):
    """Observation-consistent bounds (D54 am. 7) for imputed slots: lower = largest measurement at or before t, upper = smallest at or
    after t (running max / min, so a monotone path stays monotone). Where a tree's own records decrease the bounds cross; the slot is then
    clamped between them. Observed slots keep their measured value."""
    NEGI, POSI = -1e9, 1e9
    lo = torch.cummax(torch.where(m > 0, obs, torch.full_like(obs, NEGI)), 1).values
    hi = torch.flip(torch.cummin(torch.flip(torch.where(m > 0, obs, torch.full_like(obs, POSI)), [1]), 1).values, [1])
    a, b = torch.minimum(lo, hi), torch.maximum(lo, hi)
    a = torch.where(lo <= NEGI / 2, torch.full_like(a, NEGI), a); b = torch.where(hi >= POSI / 2, torch.full_like(b, POSI), b)
    a = torch.where((lo > NEGI / 2) & (hi >= POSI / 2), lo, a); b = torch.where((hi < POSI / 2) & (lo <= NEGI / 2), hi, b)
    if not lower: a = torch.full_like(a, NEGI)
    out = torch.minimum(torch.maximum(X, a), b)
    if monotone:                                                     # per-slot medians can dip as the mix of existing samples changes
        out = torch.cummax(out, 1).values                            # running max stays within the (non-decreasing) upper bound
    return torch.where(m > 0, obs, out)

@torch.no_grad()
def sample_lineages(model, B, n_draws=10, K=100, bs=512):
    """Coherent lineage draws (the cohort product): (z_k, end e, cause f) from the posterior; existence = (latent DIA >= the smallest
    tally level) and t < e — one appearance, at most one end; DIA / HT bounded by the tree's own measurements; NaN outside the span."""
    outs = {k: [] for k in ('exists', 'dia', 'ht', 'start', 'end', 'cause')}
    ar = torch.arange(T_MAX)
    for i in range(0, B['v'].shape[0], bs):
        b = sub(B, slice(i, i + bs)); o = model.joint(b, K)
        W = torch.softmax(o['lw'], 0)[..., None, None] * torch.exp(o['lpost'])                        # [K, n, E, 2]
        Kk, n = W.shape[:2]
        flat = W.permute(1, 0, 2, 3).reshape(n, -1)
        idx = torch.multinomial(flat + 1e-30, n_draws, replacement=True)
        k_, rem = idx // ((T_MAX + 1) * 2), idx % ((T_MAX + 1) * 2)
        e_, f_ = rem // 2, rem % 2
        nidx = torch.arange(n)[:, None].expand(-1, n_draws)
        Dp = o['D'][k_, nidx]; Hp = o['H'][k_, nidx]                                                   # [n, d, T]
        nv = b['v'].sum(1).long()[:, None, None]
        thr = torch.exp(b['lex'][:, :1])[:, None]                                                      # smallest-level threshold (- 0.05)
        # one contiguous existence interval: from the first slot at the level OR with a visible record, until the end
        cand = (Dp >= thr) | ((b['real'] > 0)[:, None].expand(-1, n_draws, -1))
        first = torch.where(cand.any(-1), torch.argmax(cand.float(), -1), torch.full_like(e_, T_MAX))
        ex = (ar[None, None] >= first[..., None]) & ((ar[None, None] < e_[..., None]) | (e_[..., None] == T_MAX)) & (ar[None, None] < nv)
        lDp = torch.log(Dp.clamp(min=1e-3)); lHp = torch.log(Hp.clamp(min=1.0))
        rs = lambda X: X.reshape(n * n_draws, T_MAX)
        rep_ = lambda X: X[:, None].expand(-1, n_draws, -1).reshape(n * n_draws, T_MAX)
        lDb = obs_bounds(rs(lDp), rep_(b['lD']), rep_(b['mD'])).reshape(n, n_draws, T_MAX)
        lHb = obs_bounds(rs(lHp), rep_(b['lH']), rep_(b['mH'])).reshape(n, n_draws, T_MAX)
        lDb = torch.maximum(lDb, torch.where(ex, torch.log(thr.expand_as(lDb) + HALF_UNIT), torch.full_like(lDb, -50.0)))   # >= level while existing
        span = (b['in_span'] > 0)[:, None].expand(-1, n_draws, -1)
        nan = torch.full_like(lDb, float('nan'))
        outs['exists'].append(torch.where(span, ex.float(), nan))
        outs['dia'].append(torch.where(span & ex, torch.exp(lDb), torch.where(span, torch.zeros_like(lDb), nan)))
        outs['ht'].append(torch.where(span & ex, torch.exp(lHb), torch.where(span, torch.zeros_like(lHb), nan)))
        start = torch.where(ex.any(-1), torch.argmax(ex.float(), -1), torch.full_like(e_, T_MAX))
        outs['start'].append(start); outs['end'].append(e_); outs['cause'].append(torch.where(e_ < T_MAX, f_, torch.full_like(f_, -1)))
    return {k: torch.cat(v).numpy() for k, v in outs.items()}

def expansion(exists, tpa_tree):
    """Per-acre expansion of one tree at one visit (D54 am. 9; nothing hard-coded): the TPA_UNADJ of the tree's record on the LARGEST
    nested plot it was ever tallied on = its smallest observed TPA_UNADJ, at every visit where it exists, else 0. FIA plots are nested
    (microplot within subplot within macroplot), so the trees that reach a larger plot's tally level are enumerated over that whole plot
    (observed or completed), and the rest are represented by the smaller plot they stand on: every tree-visit is counted in exactly one
    group (no double count), for any design whose TPA_UNADJ encodes the nested plot areas."""
    return np.where(np.asarray(exists, bool), np.asarray(tpa_tree, float), 0.0)

@torch.no_grad()
def stand_totals(model, seq, n_draws=50, K=100, by=('statecd', 'unitcd', 'countycd', 'plot', 'subp', 'measdate'), group_cols=()):
    """Stand totals per visit from completed lineages (export step, not part of the model). For each posterior draw (sample_lineages)
    every tree-visit gets its expansion (see `expansion`); TPA and basal area (ft2/ac = 0.005454 DIA^2 x expansion) are summed per
    `by` (+ `group_cols`) and summarised over draws (mean, 10 / 90 %). Saplings (DIA below the design's subplot-level threshold, thr_subp)
    and larger trees are reported separately. Slots outside a tree's sampled span contribute nothing (not completed)."""
    B, _, sq = build(seq, model.vocab)
    L = sample_lineages(model, B, n_draws=n_draws, K=K)
    ti, po = sq._tid.to_numpy(), sq._pos.to_numpy()
    real = (sq.slot_kind == 'real').to_numpy() & ~(sq.hidden.fillna(False).to_numpy(bool) if 'hidden' in sq else np.zeros(len(sq), bool))
    tpa_tree = pd.Series(np.where(real, sq.tpa.to_numpy(float), np.nan), index=ti).groupby(level=0).min().reindex(range(B['v'].shape[0])).to_numpy()[ti]
    ex = np.nan_to_num(L['exists'][ti, :, po], nan=0) > 0; dia = np.nan_to_num(L['dia'][ti, :, po], nan=0)    # [rows, draws]
    w = expansion(ex, tpa_tree[:, None])
    ba = BA_K * dia ** 2 * w
    sap = dia < sq.thr_subp.to_numpy(float)[:, None]
    keys = sq[list(by) + list(group_cols)].reset_index(drop=True)
    parts = {'tpa_sapling': w * sap, 'tpa_tree': w * ~sap, 'ba_sapling': ba * sap, 'ba_tree': ba * ~sap}
    out = keys.drop_duplicates().reset_index(drop=True)
    gid = keys.merge(out.reset_index(), on=list(keys.columns), how='left')['index'].to_numpy()
    for name, M_ in parts.items():
        agg = np.zeros((len(out), M_.shape[1])); np.add.at(agg, gid, M_)
        out[f'{name}_mean'] = agg.mean(1); out[f'{name}_q10'] = np.quantile(agg, 0.1, 1); out[f'{name}_q90'] = np.quantile(agg, 0.9, 1)
    return out

def monotone_violations(X, B, tol=1e-6):
    v, f = B['v'], B['f']; t = torch.arange(T_MAX)[None]; pre = (t <= f[:, None]) & (v > 0)
    d = X[:, 1:] - X[:, :-1]; pair = pre[:, 1:] & pre[:, :-1]
    return int(((d < -tol) & pair).any(1).sum())

# -------------------------------------------------------------------------------------------------------------------------- benchmarks
def _real_rank(seq):
    r = seq[seq.slot_kind == 'real'].sort_values(K_ + ['measdate'])
    return r

def bench_sizes(seq: pd.DataFrame, limit=None, interval: bool = True) -> pd.DataFrame:
    """(i)/(ii) Clean microplot lineages turned into subplot-like trees (limit `limit`): microplot debut < limit, >= 3 real visits, a real
    visit < limit and a later one >= limit. Hidden = real visits before the first >= limit one. RECONCILECD set to 1 (what a subplot tree
    would show); the pre-debut FALSE labels hidden; truth for establishment: hidden real visits TRUE, originally-clean pre slots FALSE."""
    r = _real_rank(seq)
    lim_r = r.thr_subp.to_numpy(float) if limit is None else np.full(len(r), float(limit))         # the design's subplot level (am. 9)
    nv = pd.Series((r.dia.to_numpy() >= lim_r).astype(int), index=r.index).groupby([r[c] for c in K_], sort=False).cumsum()
    r = r.assign(_h=(nv == 0).astype(int).to_numpy())
    g = r.groupby(K_, sort=False)
    t = pd.DataFrame({'thr': g.thr_in.first(), 'tmi': g.thr_micro.first(), 'tsu': g.thr_subp.first(), 'dia0': g.dia.first(), 'n': g.size(), 'nh': g._h.sum()})
    t['nvis'] = t.n - t.nh; t['lim'] = t.tsu if limit is None else float(limit)
    ok = t[(t.thr <= t.tmi + 1e-9) & (t.dia0 < t.lim) & (t.n >= 3) & (t.nh >= 1) & (t.nvis >= 1)].reset_index()[K_]
    b = seq.merge(ok, on=K_, how='inner').merge(r[K_ + ['measdate', '_h']], on=K_ + ['measdate'], how='left')
    b['hidden'] = b._h == 1
    # originally clean FALSE pre slots (truth FALSE for establishment scoring), computed with the ORIGINAL microplot semantics
    first = b[b.slot_kind == 'real'].groupby(K_).measdate.min().rename('_d0')
    b = b.join(first, on=K_)
    b['truth_E'] = np.where(b.hidden, 1.0, np.nan)
    clean = (b.slot_kind == 'pre') & b.rcd0.isin([1, 2]) & (b.forest_micrcond_prop.fillna(0) >= 1.0) & (b.cond_status == 1)   # microplot debut (selected above)
    b.loc[clean, 'truth_E'] = 0.0
    b['limit_in'] = b.thr_subp if limit is None else float(limit)
    b['lo_in'] = np.where(b.hidden & interval, b.thr_micro, np.nan); b['e_known'] = b.hidden & interval
    b['drop_false'] = True; b['rcd_override'] = 1.0
    return b.drop(columns=['_h', '_d0'])

def bench_end(seq: pd.DataFrame) -> pd.DataFrame:
    """(iii)+(iv) censored-end benchmark (D54 amendments 1-2; D55 amendment 3): EVERY at-risk interval (j-1, j] with a known outcome is one
    instance — a real record at j (alive) or a definitive death / cut at j (gone) — provided the tree has >= 2 visible real records before j.
    Each instance is a copy of the lineage with the FULL frame kept (as for a censored tree in production: the subplot's later visits and
    their context exist) and the tree's own information hidden from j on: its records, end evidence and codes (absence uninformative).
    So the benchmark's gone share equals the population's per-interval rate. Instance trees get tree = tree * 16 + instance."""
    s = seq.sort_values(K_ + ['measdate']).copy()
    real = s[s.slot_kind == 'real']
    rk = real.groupby(K_).cumcount().rename('_rk')
    s = s.join(rk)
    dead_j = (pd.to_datetime(s.measdate) == pd.to_datetime(s.dead_md)) & (s.terminal_fate != 'censored_moved')
    nreal = real.groupby(K_).size().rename('_nr'); s = s.join(nreal, on=K_)
    alive_j = (s.slot_kind == 'real') & (s._rk >= 2)
    gone_j = dead_j & (s._nr >= 2)
    inst = s[alive_j | gone_j][K_ + ['measdate']].copy(); inst['_truth'] = np.where(alive_j[alive_j | gone_j], 1.0, 0.0)
    inst['_i'] = inst.groupby(K_).cumcount()
    out = []
    for i, part in inst.groupby('_i'):
        b = s.merge(part.rename(columns={'measdate': '_j'}), on=K_, how='inner')
        at = b.measdate == b._j
        b['hidden'] = (b.measdate >= b._j) & (b.slot_kind == 'real')                                 # the tree's own records from j on
        b['truth_cut'] = np.where(np.isin(b.terminal_fate, CUT_FATES) | (b.dead_agent == 80), 1.0, 0.0)
        b['end_hidden'] = True; b['terminal_fate'] = 'censored_procedural'; b['dead_md'] = pd.NaT; b['dead_agent'] = np.nan; b['s0_md'] = pd.NaT
        b['truth_E'] = np.where(at, b._truth, np.nan); b['truth_cut'] = np.where(at & (b._truth == 0), b.truth_cut, np.nan)
        b['tree'] = b.tree * 16 + int(i)
        out.append(b.drop(columns=['_rk', '_nr', '_truth', '_i']))
    return pd.concat(out, ignore_index=True)

def predictions(model, bb, K=100, w_int=None):
    B, _, sq = build(bb, model.vocab)
    R = impute(model, B, K, w_int=w_int)
    ti, po = sq._tid.to_numpy(), sq._pos.to_numpy()
    for k in ('pE', 'pDie', 'pCut', 'lD', 'lH', 'q10', 'q90', 'BA', 'lD_u'): sq[f'nim_{k}'] = R[k][ti, po].numpy()
    viol = (monotone_violations(R['lD_u'], B),)
    return sq, viol

def fit_width(model, b_pre, b_post, lo=0.5, hi=8.0, iters=10):
    """Width factors per slot type and imputed size class (< 2 in, >= 2 in): 80 % coverage of the measured log DIA on the validation
    benchmarks (pre: hidden visits of the 5-in interval benchmark; post: alive instances of the censored-end benchmark), by bisection."""
    def cov(typ, cls, w):
        torch.manual_seed(7)
        wi = {'pre': (1.0, 1.0), 'post': (1.0, 1.0)}; wi[typ] = (w, 1.0) if cls == 0 else (1.0, w)
        sq, _ = predictions(model, b_pre if typ == 'pre' else b_post, w_int=wi)
        h = sq[sq.hidden] if typ == 'pre' else sq[sq.truth_E == 1]
        h = h[h.dia.notna() & h.nim_lD.notna()]
        sel = (np.exp(h.nim_lD) < 2) if cls == 0 else (np.exp(h.nim_lD) >= 2)
        ld = np.log(h.dia); inside = ((ld >= h.nim_q10) & (ld <= h.nim_q90))[sel]
        return None if inside.sum() + (~inside).sum() < 50 else float(inside.mean())
    tk = b_post[b_post.truth_E == 1][K_].drop_duplicates()                                          # alive instances only (sizes)
    if len(tk) > 8000: tk = tk.sample(8000, random_state=0)
    b_post = b_post.merge(tk, on=K_)
    out = {}
    for typ in ('pre', 'post'):
        ws = []
        for cls in (0, 1):
            c_lo = cov(typ, cls, lo)
            if c_lo is None: ws.append(1.0); continue
            if c_lo >= 0.80: ws.append(lo); continue
            a_, b_ = lo, hi
            for _ in range(iters):
                mid = 0.5 * (a_ + b_)
                if cov(typ, cls, mid) >= 0.80: b_ = mid
                else: a_ = mid
            ws.append(round(b_, 3))
        out[typ] = tuple(ws)
    return out

# ---------------------------------------------------------------------------------------------------------------------------- training
def train(tr, epochs=4, k=32, bs=512, max_trees=0, seed=0, threads=7, lr=1e-3, log=print, censor='known', growth='flex', noise=True, prior_ctx=False):
    torch.manual_seed(seed); torch.set_num_threads(threads); rng = np.random.default_rng(seed)
    B, vocab, _ = build(tr); n = B['v'].shape[0]
    if max_trees and max_trees < n: B = sub(B, torch.tensor(rng.permutation(n)[:max_trees])); n = max_trees
    model = make_model(vocab, censor=censor, growth=growth, noise=noise, prior_ctx=prior_ctx)
    phi = model.enc_params(); phi_ids = {id(p) for p in phi}; theta = [p for p in model.parameters() if id(p) not in phi_ids]
    opt = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=1e-5)
    nb_ep = (n + bs - 1) // bs
    sched = torch.optim.lr_scheduler.OneCycleLR(opt, max_lr=lr, total_steps=epochs * nb_ep, pct_start=0.05, anneal_strategy='cos')
    log(f'train trees {n:,}; slots {int(B["v"].sum()):,}; visible real {int(B["real"].sum()):,}; K {k} (DReG)')
    hist = []
    for ep in range(epochs):
        t0, tot, nb = time.time(), 0.0, 0
        for ix in torch.split(torch.tensor(rng.permutation(n)), bs):
            b = sub(B, ix); o = model.joint(b, k, dreg=True)
            loss_t = -(torch.logsumexp(o['lw'], 0) - math.log(k)).mean()
            wt = torch.softmax(o['lw'], 0).detach()
            loss_p = -(wt ** 2 * o['lw_dreg']).sum(0).mean()                                          # DReG surrogate (encoder)
            gt = torch.autograd.grad(loss_t, theta, retain_graph=True, allow_unused=True)
            gp = torch.autograd.grad(loss_p, phi, allow_unused=True)
            opt.zero_grad()
            for p_, g_ in list(zip(theta, gt)) + list(zip(phi, gp)): p_.grad = g_
            nn.utils.clip_grad_norm_(model.parameters(), 5.0); opt.step(); sched.step()
            tot += loss_t.item(); nb += 1
        ess = float((1 / (torch.softmax(o['lw'].detach(), 0) ** 2).sum(0)).median())
        hist.append(dict(epoch=ep, loss=tot / nb, ess_last_batch=ess, sd_disc=torch.exp(model.lsdisc).item(), sd_H=torch.exp(model.lsH).item(),
                         rho=(RHO_MAX * torch.sigmoid(model.lrho)).item(),
                         **({'sd_uD': torch.exp(model.lsu[0]).item(), 'sd_uH': torch.exp(model.lsu[1]).item()} if model.noise else {}), **model.cfg))
        log(json.dumps(hist[-1]) + f' {time.time() - t0:.0f}s')
    return model, hist
