"""D54 — NIMIWAE lineage completion: existence start AND end, tree fate, future records; DReG encoder gradients (DECISIONS.md D54).

Lineage = every counted visit of the tree's subplot span (nimiwae_prep --frame d54).
Latents  z ~ p(z | statics); start s (first slot at which the tree is >= 1 in), end e (first slot at which it is gone; T = alive through
         the span), cause f ∈ {death, cut}; existence E_t = 1[s <= t < e] (one contiguous interval). (s, e, f) summed out exactly.
Decoder  bidirectional transformer on (z, always-known context) -> establishment hazard, competing per-interval (survive, die, cut),
         softplus DIA / HT increments (DIA = 1 in + cumulative from s), crown ratio / class.
Evidence per slot: TRUE / FALSE hard or soft (label error ρ learned, capped at 0.05); per unrecorded slot a missingness mode:
         cutoff (below the tally limit or a crew miss), ignore (uninformative), missed (existed, >= limit), interval (benchmark).
Bound    IWAE; encoder gradients DReG (Tucker et al. 2018), decoder / prior standard.
"""
import math, time, json
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
        vocab = {c: {v: i + 1 for i, v in enumerate(sorted(seq[c].dropna().unique()))} for c in ('spgrpcd', 'epa_l3', 'major_spgrpcd', 'fortypcd')}
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
    limit = seq.limit_in.to_numpy(float) if 'limit_in' in seq else np.where((seq.thr_in.to_numpy(float) <= 1.0) | (rcd == 2), 1.0, 5.0)
    micro = limit <= 1.0
    lo = seq.lo_in.to_numpy(float) if 'lo_in' in seq else np.full(N, np.nan)
    cst = seq.cond_status.to_numpy(float)
    fsp, fmp = seq.forest_subpcond_prop.to_numpy(float), seq.forest_micrcond_prop.to_numpy(float)
    sampled = rec_any | np.where(np.isnan(cst), np.nan_to_num(fsp, nan=0) > 0, cst == 1)
    fate = seq.terminal_fate.fillna('').to_numpy(str)
    end_hidden = seq.end_hidden.fillna(False).to_numpy(bool) if 'end_hidden' in seq else np.zeros(N, bool)
    dead_md = pd.to_datetime(seq.dead_md); s0_md = pd.to_datetime(seq.s0_md); md = pd.to_datetime(seq.measdate)
    agent = seq.dead_agent.to_numpy(float)
    # definitive end slot (first slot at which the tree is gone) and its cause
    is_dead_end = (md == dead_md).to_numpy() & ~end_hidden
    is_inf_cut = np.isin(fate, ['harvest_inferred', 'disappeared_inferred_removal']) & (kind == 'post') & ~end_hidden
    is_moved = (fate == 'censored_moved') & (md == s0_md).to_numpy() & ~end_hidden
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
    ign_after = np.isin(fate, ['censored_procedural', 'censored_procedural_unrecorded', 'censored_shrank']) | end_hidden
    mode[post & ign_after] = MODE['ignore']
    mode[missed_prev] = MODE['missed']
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
               **{f'trt{c}': ((tr_ >= c) & (tr_ < c + 10)).any(1).astype(float) for c in TRT},
               **{f'dst{c}': ((ds_ >= c) & (ds_ < c + 10)).any(1).astype(float) for c in DST})
    lgD = np.log(np.clip(np.nan_to_num(dia, nan=1.0), 0.05, None)); lgH = np.log(np.clip(np.nan_to_num(ht, nan=4.5), 1.0, None))
    X = dict(lD=mat(lgD), lH=mat(lgH),
             mD=mat(mD), mH=mat(mH), cr=mat(np.where(mcr, seq.cr.to_numpy(float) / 100, 0)), mcr=mat(mcr),
             cc=mat(np.where(mcc, seq.cclcd.to_numpy(float), 0)), mcc=mat(mcc), v=mat(np.ones(N)), hid=mat(hid), real=mat(real),
             sampled=mat(sampled), llim=mat(np.log(limit - HALF_UNIT)), llo=mat(np.log(lo - HALF_UNIT), -20.0), fmp=mat(fmp),
             ev=mat(ev), mode=mat(mode), cov=np.stack([mat(cov[c]) for c in COV], -1))
    st = g.head(1)
    def ids(c): return st[c].map(vocab[c]).fillna(0).astype(int).to_numpy()
    def z_(c): mu, sd = vocab['_norm'][c]; return np.nan_to_num((st[c].to_numpy(dtype=float) - mu) / sd, nan=0.0)
    asp = np.deg2rad(st.aspect.fillna(0).to_numpy(dtype=float)); flat = st.slope.fillna(0).to_numpy(dtype=float) < 5
    rc0 = pd.Series(rcd, index=tid).groupby(level=0).first().reindex(range(n)).to_numpy()
    S = dict(sp=ids('spgrpcd'), l3=ids('epa_l3'), maj=ids('major_spgrpcd'), fty=ids('fortypcd'),
             scl=np.clip(st.siteclcd.fillna(0).to_numpy(dtype=float), 0, 7).astype(int), phy=np.clip(st.physclcd.fillna(0).to_numpy(dtype=float), 0, 49).astype(int),
             snum=np.stack([st.planted.fillna(False).astype(float).to_numpy(), z_('sicond'), st.sicond.isna().to_numpy(float), z_('slope'),
                            np.where(flat, 0, np.sin(asp)), np.where(flat, 0, np.cos(asp)), z_('tm_n'), z_('pp_n'), z_('vp_n')], 1),
             rcd=np.nan_to_num(rc0, nan=0).clip(0, 9).astype(int), f=f_idx, l=l_idx, cause=tree_cause, moved=tree_moved.astype(int),
             ndef=(endpos >= 0).astype(int))
    longs = ('sp', 'l3', 'maj', 'fty', 'scl', 'phy', 'rcd', 'f', 'l', 'cause', 'moved', 'ndef')
    T = {k: torch.tensor(np.array(v), dtype=torch.long if k in longs else torch.float32) for k, v in {**X, **S}.items()}
    T['ev'] = T['ev'].long(); T['mode'] = T['mode'].long()
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
    return nn.TransformerEncoder(nn.TransformerEncoderLayer(d, 4, 2 * d, dropout=0.0, batch_first=True, norm_first=True), layers,
                                 enable_nested_tensor=False)

NQ = 15 + len(EV) + len(MODE)

class NIMIWAE54(nn.Module):
    def __init__(s, nsp, nl3, nmaj, nfty):
        super().__init__()
        d = D_MODEL
        # encoder (phi)
        s.st_q = Static(nsp, nl3, nmaj, nfty); s.ercd = nn.Embedding(10, 4); s.ecc = nn.Embedding(6, 3)
        s.qin = nn.Linear(NCOV + NQ + 4 + 3, d); s.qtr = tenc(d); s.qout = nn.Linear(d, 2 * DZ)
        # generative model (theta)
        s.st_p = Static(nsp, nl3, nmaj, nfty)
        s.prior = nn.Sequential(nn.Linear(d, d), nn.GELU(), nn.Linear(d, 2 * DZ))
        s.din = nn.Linear(NCOV, d); s.dz = nn.Linear(DZ, d); s.dtr = tenc(d)
        s.dout = nn.Linear(d, 2 + 1 + 3 + 1 + 5)                                  # dD, dH | establishment | (survive, die, cut) | cr | cclcd
        with torch.no_grad():
            s.dout.bias.zero_(); s.dout.bias[0] = -0.5; s.dout.bias[1] = 1.5; s.dout.bias[3] = 3.0
        s.lsdisc, s.lsH, s.lscr = nn.Parameter(torch.tensor(-4.0)), nn.Parameter(torch.tensor(-2.0)), nn.Parameter(torch.tensor(-2.0))
        s.leps, s.lrho = nn.Parameter(torch.tensor(-4.0)), nn.Parameter(torch.tensor(-2.0))
    def enc_params(s):
        return [p for m in (s.st_q, s.ercd, s.ecc, s.qin, s.qtr, s.qout) for p in m.parameters()]
    def q(s, B):
        m = B['mD']; cc = s.ecc(B['cc'].long().clamp(0, 5)) * B['mcc'][..., None]
        haslo = (B['mode'] == MODE['interval']).float()
        T = T_MAX; ar = torch.arange(T)[None]
        x = torch.cat([B['cov'], torch.stack([B['lD'] * m, m, B['lH'] * B['mH'], B['mH'], B['cr'], B['mcr'], B['mcc'], B['real'], B['sampled'],
                                              B['llim'], haslo, B['llo'].clamp(min=-1) * haslo, (ar == B['f'][:, None]).float(),
                                              (ar == B['l'][:, None]).float(), B['v']], -1),
                       F.one_hot(B['ev'], len(EV)).float(), F.one_hot(B['mode'], len(MODE)).float(),
                       s.ercd(B['rcd'])[:, None].expand(-1, T, -1), cc], -1)
        h = s.qin(x); cls = s.st_q(B)[:, None]
        pad = torch.cat([torch.zeros_like(B['v'][:, :1]), 1 - B['v']], 1).bool()
        o = s.qtr(torch.cat([cls, h], 1), src_key_padding_mask=pad)[:, 0]
        mu, lv = s.qout(o).chunk(2, -1); return mu, lv.clamp(-8, 4)
    def p_z(s, B):
        mu, lv = s.prior(s.st_p(B)).chunk(2, -1); return mu, lv.clamp(-6, 3)
    def decode(s, z, B):
        K, n = z.shape[:2]
        h = s.din(B['cov'])[None] + s.dz(z)[:, :, None] + s.st_p(B)[None, :, None]
        pad = (1 - B['v']).bool()[None].expand(K, -1, -1).reshape(K * n, T_MAX)
        o = s.dout(s.dtr(h.reshape(K * n, T_MAX, D_MODEL), src_key_padding_mask=pad)).reshape(K, n, T_MAX, -1)
        return (F.softplus(o[..., 0]) * B['v'], F.softplus(o[..., 1]) * B['v'], o[..., 2], torch.log_softmax(o[..., 3:6], -1),
                torch.sigmoid(o[..., 6]), o[..., 7:])
    def joint(s, B, K, dreg=False):
        """-> log weights lw [K, n] (and lw with q detached for DReG), log posterior over (s, e, f) [K, n, S, E, 2], sizes."""
        mq, lq = s.q(B); mp, lp = s.p_z(B)
        z = mq + torch.exp(0.5 * lq) * torch.randn(K, *mq.shape)
        dD, dH, hz, lend, crm, ccl = s.decode(z, B)
        T = T_MAX; ar = torch.arange(T)
        exs = (ar[None, :] >= ar[:, None]).float()                                                     # [s, t]: t >= s
        CD, CH = torch.cumsum(dD, -1), torch.cumsum(dH, -1)
        pD = torch.cat([torch.zeros_like(CD[..., :1]), CD[..., :-1]], -1); pH = torch.cat([torch.zeros_like(CH[..., :1]), CH[..., :-1]], -1)
        D = (1.0 + CD[..., None, :] - pD[..., :, None]) * exs; H = (4.5 + CH[..., None, :] - pH[..., :, None]) * exs   # [K, n, S, T]
        lDp, lHp = torch.log(D.clamp(min=1e-3)), torch.log(H.clamp(min=1.0))
        c2 = 0.5 * math.log(2 * math.pi); nl = lambda x, mu, ls: -0.5 * ((x - mu) / torch.exp(ls)) ** 2 - ls - c2
        e1 = torch.sigmoid(-s.leps); rho = RHO_MAX * torch.sigmoid(s.lrho)
        bc = lambda x: x[None, :, None]                                                                # [n, T] -> [1, n, 1, T]
        sdD = 0.5 * torch.log(torch.exp(2 * ls_dia(B['lD'])) + torch.exp(2 * s.lsdisc))
        ptal = torch.sigmoid(KAPPA * (lDp - bc(B['llim'])))
        real, v, mode, ev = bc(B['real']), bc(B['v']), B['mode'][None, :, None], B['ev'][None, :, None]
        unrec = (1 - real) * v
        meas = nl(bc(B['lD']), lDp, bc(sdD)) * bc(B['mD']) + nl(bc(B['lH']), lHp, s.lsH) * bc(B['mH'])
        lrec = torch.log((e1 * ptal).clamp(min=1e-12))
        lmiss = torch.where(mode == MODE['cutoff'], torch.log((1 - e1 * ptal).clamp(min=1e-12)),
                torch.where(mode == MODE['missed'], torch.log(ptal.clamp(min=1e-12)),
                torch.where(mode == MODE['interval'], torch.log((torch.sigmoid(KAPPA * (lDp - bc(B['llo']))) * (1 - ptal)).clamp(min=1e-12)),
                            torch.zeros_like(ptal))))
        lr, l1r = torch.log(rho), torch.log(1 - rho)
        ev1 = torch.where(ev == EV['false_hard'], torch.tensor(NEG),
              torch.where(ev == EV['false_soft'], lr, torch.where(ev == EV['true_soft'], l1r, torch.tensor(0.0))))
        ev0 = torch.where(ev == EV['true_hard'], torch.tensor(NEG),
              torch.where(ev == EV['true_soft'], lr, torch.where(ev == EV['false_soft'], l1r, torch.tensor(0.0))))
        ev0 = ev0 + real * NEG                                                                         # a visible record cannot be E = 0
        ll1 = (meas + real * lrec + unrec * lmiss + ev1) * v * exs[None, None]                       # [K, n, S, T]  (E = 1, start s)
        ll0 = (ev0 * v)[..., 0, :]                                                                     # [1, n, T]
        diff = (ll1 - ll0[:, :, None, :]) * exs[None, None]
        Cs = torch.cat([torch.zeros_like(diff[..., :1]), torch.cumsum(diff, -1)], -1)                 # [K, n, S, T+1]: Σ_{u<e} diff_u
        base = ll0.sum(-1)[..., None, None]                                                            # [1, n, 1, 1]
        llse = base + Cs                                                                               # [K, n, S, E=T+1]
        # priors over s and (e, f)
        f, l, nv = B['f'][None, :, None], B['l'][None, :, None], B['v'].sum(1).long()[None, :, None]
        hlog, h1log = F.logsigmoid(hz), F.logsigmoid(-hz)
        surv = torch.cat([torch.zeros_like(h1log[..., :1]), torch.cumsum(h1log, -1)[..., :-1]], -1)
        j = ar[None, None]
        lps = torch.where(j < f, hlog + surv, torch.where(j == f, surv, torch.full_like(surv, NEG)))  # [K, n, S]
        # end prior: the tree is AT RISK from its start s on; every interval it survives (during its record too) enters the likelihood,
        # p(e = u, f | s) = Π_{s<v<u} P(survive_v) · P(f at u), p(alive through the span | s) = Π_{s<v<nv} P(survive_v)
        lsv, ldie, lcut = lend[..., 0], lend[..., 1], lend[..., 2]
        csum = torch.cumsum(lsv * B['v'][None], -1)                                                     # [K, n, T]: Σ_{v<=t}
        cprev = torch.cat([torch.zeros_like(csum[..., :1]), csum[..., :-1]], -1)                        # Σ_{v<t}
        sI, uI = ar[:, None], ar[None, :]
        valid = (uI > sI)[None, None] & (uI[None, None] > l[..., None]) & (uI[None, None] < nv[..., None])   # [1, n, S, U]
        surv_su = cprev[..., None, :] - csum[..., :, None]                                               # [K, n, S, U]: Σ_{s<v<u}
        mv = (B['moved'] == 1)[None, :, None, None]                                                     # a move is not a death / cut
        lpe_die = torch.where(valid, surv_su + torch.where(mv, torch.zeros_like(ldie[..., None, :]), ldie[..., None, :]), torch.full_like(surv_su, NEG))
        lpe_cut = torch.where(valid & ~mv, surv_su + lcut[..., None, :], torch.full_like(surv_su, NEG))
        alive = csum.gather(-1, (nv - 1).clamp(min=0).expand(K, -1, 1)) - csum                           # [K, n, S]: Σ_{s<v<nv}
        e_idx = torch.arange(T_MAX + 1)[None, None, None]
        lpe = torch.stack([torch.cat([lpe_die, alive[..., None]], -1), torch.cat([lpe_cut, torch.full_like(alive[..., None], NEG)], -1)], -1)
        # cause evidence for definitive ends
        known = (B['ndef'] == 1) & (B['moved'] == 0)
        cmask = torch.zeros(1, B['v'].shape[0], 1, 1, 2)
        cmask[0, known, 0, 0, 1 - B['cause'][known]] = NEG
        lpe = lpe + cmask * (e_idx < T_MAX)[..., None].float()                                         # [K, n, S, E, 2]
        # e = T_MAX ("alive through the span") = llse[..., T_MAX]: padding slots contribute 0, so it equals the sum over the tree's slots
        lj = lps[..., :, None, None] + lpe + llse[..., None]                                            # [K, n, S, E, 2]
        lpx = torch.logsumexp(lj.reshape(K, lj.shape[1], -1), -1)
        lcr = (nl(B['cr'][None], crm, s.lscr) * B['mcr'][None]).sum(-1)
        lcc = (torch.log_softmax(ccl, -1).gather(-1, (B['cc'].long().clamp(1, 5) - 1)[None, ..., None].expand(K, -1, -1, 1))[..., 0] * B['mcc'][None]).sum(-1)
        lpz = (-0.5 * (z - mp) ** 2 / torch.exp(lp) - 0.5 * lp - c2).sum(-1)
        lqz = (-0.5 * (z - mq) ** 2 / torch.exp(lq) - 0.5 * lq - c2).sum(-1)
        lw = lpx + lcr + lcc + lpz - lqz
        out = dict(lw=lw, lpost=lj - lpx[..., None, None, None], D=D, H=H, sdD=sdD, lsdisc=s.lsdisc)
        if dreg:
            mqd, lqd = mq.detach(), lq.detach()
            out['lw_dreg'] = lpx + lcr + lcc + lpz - (-0.5 * (z - mqd) ** 2 / torch.exp(lqd) - 0.5 * lqd - c2).sum(-1)
        return out

def make_model(vocab):
    m = NIMIWAE54(len(vocab['spgrpcd']), len(vocab['epa_l3']), len(vocab['major_spgrpcd']), len(vocab['fortypcd']) + 1); m.vocab = vocab; return m

# --------------------------------------------------------------------------------------------------------------------------- inference
def wmedian(X, w):
    Xs, ix = torch.sort(X, 0); cw = torch.cumsum(w[..., None].expand_as(X).gather(0, ix), 0)
    return Xs.gather(0, (cw < 0.5).sum(0, keepdim=True).clamp(max=X.shape[0] - 1))[0]

@torch.no_grad()
def impute(model, B, K=100, bs=512, nsamp=200, w_int=None):
    """Per slot: P(exists), P(gone by death / cut by t); unconditional posterior median log DIA (0 when not existing; P1); size given
    existence (median log DIA / HT); 10 / 90 % predictive quantiles of the MEASURED log DIA (path + FIA measurement error + discrepancy),
    widths scaled per slot type (pre: before the debut, post: after the last record) and imputed size class; expected basal area."""
    w_int = w_int or {'pre': (1.0, 1.0), 'post': (1.0, 1.0)}
    keys = ('pE', 'pDie', 'pCut', 'lD_u', 'lD', 'lH', 'q10', 'q90', 'BA')
    out = {k: [] for k in keys}
    ar = torch.arange(T_MAX)
    for i in range(0, B['v'].shape[0], bs):
        b = sub(B, slice(i, i + bs)); o = model.joint(b, K)
        W = torch.softmax(o['lw'], 0)[..., None, None, None] * torch.exp(o['lpost'])                # [K, n, S, E, 2]
        Kk, n = W.shape[:2]
        sI = ar[:, None, None]; eI = torch.arange(T_MAX + 1)[None, :, None]; tI = ar[None, None, :]
        nv = b['v'].sum(1).long()
        Ex = ((tI >= sI) & ((tI < eI) | (eI == T_MAX))).float()
        Ex = Ex[None, None].expand(Kk, n, -1, -1, -1) * (tI[None, None] < nv[None, :, None, None, None]).float()
        Ws = W.sum(-1)
        pE = (Ws[..., None] * Ex).sum((0, 2, 3))
        gone = ((tI >= eI) & (eI < T_MAX)).float()[None, None]
        pDie = (W[..., 0][..., None] * gone).sum((0, 2, 3)); pCut = (W[..., 1][..., None] * gone).sum((0, 2, 3))
        Wks = Ws.sum(-1)
        Df = o['D'].permute(0, 2, 1, 3).reshape(Kk * T_MAX, n, T_MAX); Hf = o['H'].permute(0, 2, 1, 3).reshape(Kk * T_MAX, n, T_MAX)
        Wf = Wks.permute(0, 2, 1).reshape(Kk * T_MAX, n)
        lD_u = torch.log(wmedian(Df, Wf).clamp(min=1e-3))
        ex = (Df > 0).float(); Wc = Wf[..., None] * ex; Wc = Wc / Wc.sum(0, keepdim=True).clamp(min=1e-12)
        def cmed(X):
            Xs, ix = torch.sort(X, 0); cw = torch.cumsum(Wc.gather(0, ix), 0)
            return Xs.gather(0, (cw < 0.5).sum(0, keepdim=True).clamp(max=X.shape[0] - 1))[0]
        lDf, lHf = torch.log(Df.clamp(min=1e-3)), torch.log(Hf.clamp(min=1.0))
        lD, lH = cmed(torch.where(ex > 0, lDf, torch.full_like(lDf, -9.0))), cmed(lHf)
        BA = (Wc * BA_K * Df ** 2).sum(0)
        sdm = torch.sqrt(torch.exp(2 * ls_dia(lDf)) + torch.exp(2 * o['lsdisc']))                    # measurement + discrepancy
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
        fI = b['f'][:, None]
        for k_, v_ in (('pE', pE), ('pDie', pDie), ('pCut', pCut), ('lD_u', torch.minimum(lD_u, b['lD'].gather(1, fI))),
                       ('lD', lD), ('lH', lH), ('q10', torch.stack(q10, 1)), ('q90', torch.stack(q90, 1)), ('BA', BA)):
            out[k_].append(v_)
    return {k: torch.cat(v) for k, v in out.items()}

def monotone_violations(X, B, tol=1e-6):
    v, f = B['v'], B['f']; t = torch.arange(T_MAX)[None]; pre = (t <= f[:, None]) & (v > 0)
    d = X[:, 1:] - X[:, :-1]; pair = pre[:, 1:] & pre[:, :-1]
    return int(((d < -tol) & pair).any(1).sum())

# -------------------------------------------------------------------------------------------------------------------------- benchmarks
def _real_rank(seq):
    r = seq[seq.slot_kind == 'real'].sort_values(K_ + ['measdate'])
    return r

def bench_sizes(seq: pd.DataFrame, limit: float, interval: bool) -> pd.DataFrame:
    """(i)/(ii) Clean microplot lineages turned into subplot-like trees (limit `limit`): microplot debut < limit, >= 3 real visits, a real
    visit < limit and a later one >= limit. Hidden = real visits before the first >= limit one. RECONCILECD set to 1 (what a subplot tree
    would show); the pre-debut FALSE labels hidden; truth for establishment: hidden real visits TRUE, originally-clean pre slots FALSE."""
    r = _real_rank(seq)
    nv = (r.dia >= limit).astype(int).groupby([r[c] for c in K_], sort=False).cumsum()
    r = r.assign(_h=(nv == 0).astype(int).to_numpy())
    g = r.groupby(K_, sort=False)
    t = pd.DataFrame({'thr': g.thr_in.first(), 'dia0': g.dia.first(), 'n': g.size(), 'nh': g._h.sum()}); t['nvis'] = t.n - t.nh
    ok = t[(t.thr <= 1.0) & (t.dia0 < limit) & (t.n >= 3) & (t.nh >= 1) & (t.nvis >= 1)].reset_index()[K_]
    b = seq.merge(ok, on=K_, how='inner').merge(r[K_ + ['measdate', '_h']], on=K_ + ['measdate'], how='left')
    b['hidden'] = b._h == 1
    # originally clean FALSE pre slots (truth FALSE for establishment scoring), computed with the ORIGINAL microplot semantics
    first = b[b.slot_kind == 'real'].groupby(K_).measdate.min().rename('_d0')
    b = b.join(first, on=K_)
    b['truth_E'] = np.where(b.hidden, 1.0, np.nan)
    clean = (b.slot_kind == 'pre') & b.rcd0.isin([1, 2]) & (b.forest_micrcond_prop.fillna(0) >= 1.0) & (b.cond_status == 1)
    b.loc[clean, 'truth_E'] = 0.0
    b['limit_in'] = limit; b['lo_in'] = np.where(b.hidden & interval, 1.0, np.nan); b['e_known'] = b.hidden & interval
    b['drop_false'] = True; b['rcd_override'] = 1.0
    return b.drop(columns=['_h', '_d0'])

def bench_end(seq: pd.DataFrame) -> pd.DataFrame:
    """(iii)+(iv) censored-end benchmark (D54 amendments 1-2): EVERY at-risk interval (j-1, j] with a known outcome is one instance — a real
    record at j (alive) or a definitive death / cut at j (gone) — provided the tree has >= 2 visible real records before j. Each instance is
    a copy of the lineage truncated after j, with j hidden procedural-style (absence uninformative, end evidence / codes hidden). So the
    benchmark's gone share equals the population's per-interval rate. Instance trees get tree = tree * 16 + instance."""
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
        b = b[b.measdate <= b._j].copy()
        at = b.measdate == b._j
        b['hidden'] = at & (b.slot_kind == 'real')
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
        h = h[h.dia.notna()]
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
def train(tr, epochs=4, k=32, bs=512, max_trees=0, seed=0, threads=7, lr=1e-3, log=print):
    torch.manual_seed(seed); torch.set_num_threads(threads); rng = np.random.default_rng(seed)
    B, vocab, _ = build(tr); n = B['v'].shape[0]
    if max_trees and max_trees < n: B = sub(B, torch.tensor(rng.permutation(n)[:max_trees])); n = max_trees
    model = make_model(vocab)
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
                         field_miss=torch.sigmoid(model.leps).item(), rho=(RHO_MAX * torch.sigmoid(model.lrho)).item()))
        log(json.dumps(hist[-1]) + f' {time.time() - t0:.0f}s')
    return model, hist
