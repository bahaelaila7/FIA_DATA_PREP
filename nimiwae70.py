"""D70 — joint NIMIWAE age model: NIMIWAE56 with flexible growth and the known design rule (D70 am. 3) plus one birth date per tree.

Birth     b = t_N - a_N(z, tree), a_N > 0, N = the frame's last slot (known context; never the tree's own last record, which would leak its record pattern).
          Age at a slot is a_t = t - b at every visit, negative before birth, so it advances exactly with elapsed time. Dates, never INVYR.
Existence E_t needs t >= b as well as the size step and t < e: the D55 step is multiplied by sigma(KAPPA_B a_t).
Curve     log S(a; x) + delta(crown) is shared by lineages and age records. x = species, group, soil, climate normals, region; no SICOND (computed from site trees).
          Monotone in age: alpha(x) + sum_j softplus(w_j(x)) phi_j(log a) with fixed sigmoid phi_j.
Unborn    a slot before birth is scored below the smallest tally level, so the size path cannot carry a tree that does not exist.
E1        a microplot debut on a moved plot: slots before the move carry no FALSE label, because that spot was not sampled then (D65 item 2). The birth date decides instead.
Slots     log D_t ~ N(log S(t - b; x) + delta(cc_t) + v, sigma_S) on slots up to the last visible record with a_t >= 0.5 yr.
Vigour    v = tau * (z_0 - mu_0) / sigma_0: under the prior v ~ N(0, tau), the between-tree spread the age records measure. Am. 7: a free v absorbed all size and left b constant.
Records   log DIA_obs ~ N(log S(a_obs + l; x) + delta(cc), sqrt(sigma_meas^2 + sigma_S^2 + tau^2)); l = years to breast height, 0 for TOTAGE; tau replaces v, which a record has no z for.
Years BH  per species group, log l ~ N(log l0, 0.3), l0 = 2 softwood, 5 hardwood (D70 am. 6): one prior term per group, not per record, so groups with data can move and the rest stay near l0.
          Site trees use their own crown class (6), so their selection is a learned offset. Western tally records carry CCLCD, which identifies delta.
Objective (mean lineage IWAE) + (N_rec / N_lin) (mean record log likelihood); records reach only the curve parameters."""
import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import math, time, json
import numpy as np, pandas as pd, polars as pl, torch, torch.nn as nn, torch.nn.functional as F
import nimiwae56 as M
from nimiwae56 import T_MAX, DZ, D_MODEL, COV, sub

SOIL_COLS = [f'soil_{c}' for c in ('clay', 'sand', 'silt', 'soc', 'bd', 'ph', 'cec', 'depth_restriction', 'rock_frag', 'depth_bedrock')]
NSOIL = len(SOIL_COLS)
KAPPA_B = 2.0                 # per year: the birth step is about half a year wide
CC_SITE, NCC = 6, 7           # 0 unknown, 1-5 CCLCD, 6 site tree
NKNOT, NCX = 8, NSOIL + 3 + 1
L0_SOFT, L0_HARD, SD_LOGL = 2.0, 5.0, 0.3
TOT, BHA, SITE = 0, 1, 2
ITYR = COV.index('tyr')

def curve_vocab(seq, rec):
    spc = sorted(set(seq.spcd.dropna().astype(int)) | set(rec.spcd.dropna().astype(int)))
    grp = sorted(set(seq.spgrpcd.dropna().astype(int)) | set(rec.spgrpcd.dropna().astype(int)))
    both = pd.concat([seq[SOIL_COLS + ['tm_n', 'pp_n', 'vp_n']], rec[SOIL_COLS + ['tm_n', 'pp_n', 'vp_n']]])
    wood = pd.concat([seq[['spgrpcd', 'sftwd_hrdwd']], rec[['spgrpcd', 'sftwd_hrdwd']]]).dropna()
    soft = wood.assign(s=(wood.sftwd_hrdwd == 'S').astype(float)).groupby('spgrpcd').s.mean()
    return dict(spc={v: i + 1 for i, v in enumerate(spc)}, grp={v: i + 1 for i, v in enumerate(grp)},
                l0=[L0_HARD] + [L0_SOFT if soft.get(v, 0.0) >= 0.5 else L0_HARD for v in grp],
                norm={c: (float(both[c].mean()), float(both[c].std()) or 1.0) for c in both.columns})

def curve_x(d, cv, west):
    z = lambda c: np.nan_to_num((d[c].to_numpy(float) - cv['norm'][c][0]) / cv['norm'][c][1], nan=0.0)
    num = np.stack([z(c) for c in SOIL_COLS + ['tm_n', 'pp_n', 'vp_n']] + [west.astype(float)], 1)
    ids = lambda c, m: d[c].map(m).fillna(0).astype(int).to_numpy()
    return dict(c_spc=torch.tensor(ids('spcd', cv['spc']), dtype=torch.long), c_grp=torch.tensor(ids('spgrpcd', cv['grp']), dtype=torch.long),
                c_num=torch.tensor(num, dtype=torch.float32))

def e1_mask(seq):
    """Slots whose FALSE label is dropped: before the plot's move, for a tree first recorded at or after it."""
    if 'move_invyr' not in seq: return pd.Series(False, index=seq.index)
    first = seq[seq.slot_kind == 'real'].groupby(M.K_).invyr.min().rename('_first')
    deb = seq.join(first, on=M.K_)._first.to_numpy(float)
    mv = seq.move_invyr.to_numpy(float); iy = seq.invyr.to_numpy(float)
    return pd.Series((iy < mv) & (deb >= mv), index=seq.index)

def build(seq, vocab=None, cv=None):
    seq = seq.copy()
    seq['drop_false'] = (seq.drop_false.fillna(False).astype(bool) if 'drop_false' in seq else False) | e1_mask(seq)
    B, vocab, sq = M.build(seq, vocab)
    if '_curve' not in vocab: vocab['_curve'] = cv
    st = sq.drop_duplicates('_tid').sort_values('_tid')
    B.update(curve_x(st, vocab['_curve'], np.zeros(len(st), bool)))
    return B, vocab, sq

def build_rec(rec, cv):
    src = rec.source.map({'tot': TOT, 'bha': BHA, 'site': SITE}).to_numpy()
    cc = np.where(src == SITE, CC_SITE, np.nan_to_num(rec.cc.to_numpy(float), nan=0.0)).astype(int).clip(0, NCC - 1)
    R = curve_x(rec, cv, (src != SITE))
    R.update(src=torch.tensor(src, dtype=torch.long), cc=torch.tensor(cc, dtype=torch.long),
             a_obs=torch.tensor(rec.a_obs.to_numpy(float), dtype=torch.float32), lD=torch.tensor(np.log(rec.dia.to_numpy(float)), dtype=torch.float32))
    return R

class Static70(M.Static):
    """Soil joins the statics of the prior and the decoder through the curve's numeric block."""
    def __init__(s, nsp, nl3, nmaj, nfty, nspc, use_summ=True):
        super().__init__(nsp, nl3, nmaj, nfty, nspc, use_summ)
        s.out = nn.Linear(s.out.in_features + NCX, D_MODEL)
    def forward(s, B):
        x = [s.e[0](B['sp']), s.e[1](B['l3']), s.e[2](B['maj']), s.e[3](B['fty']), s.e[4](B['scl']), s.e[5](B['phy']), s.e[6](B['spc']), B['snum'], B['c_num']]
        return s.out(torch.cat(x + ([B['summ']] if s.use_summ else []), -1))

class AgeCurve(nn.Module):
    def __init__(s, nspc, ngrp, l0=None):
        super().__init__()
        s.register_buffer('l0', torch.tensor(l0 if l0 is not None else [L0_HARD] * (ngrp + 1), dtype=torch.float32))
        s.eps_l = nn.Embedding(ngrp + 1, 1); nn.init.zeros_(s.eps_l.weight)
        s.es, s.eg = nn.Embedding(nspc + 1, 12), nn.Embedding(ngrp + 1, 6)
        s.net = nn.Sequential(nn.Linear(12 + 6 + NCX, 64), nn.GELU(), nn.Linear(64, 64), nn.GELU())
        s.out = nn.Linear(64, 1 + NKNOT)                                                 # alpha, knot weights
        s.delta = nn.Embedding(NCC, 1)
        s.register_buffer('knots', torch.linspace(math.log(1.0), math.log(250.0), NKNOT))
        with torch.no_grad():
            s.out.bias.zero_(); s.out.bias[0] = -1.0; s.out.bias[1:1 + NKNOT] = 0.0; s.delta.weight.zero_()
    def token(s, X):
        return s.out(s.net(torch.cat([s.es(X['c_spc']), s.eg(X['c_grp']), X['c_num']], -1)))
    def log_s(s, tok, a):
        """log S at ages a [..., n] or [..., n, T], tok [n, 1 + NKNOT]: monotone non-decreasing in a by construction."""
        w = F.softplus(tok[..., 1:1 + NKNOT]); al = tok[..., 0]
        if a.dim() == tok.dim(): w, al = w[:, None], al[:, None]
        phi = torch.sigmoid((torch.log(a.clamp(min=0.25))[..., None] - s.knots) / 0.5)
        return al + (w * phi).sum(-1)
    def years_bh(s, grp):
        return s.l0[grp] * torch.exp(s.eps_l(grp)[..., 0])
    def log_prior_bh(s):
        return -0.5 * (s.eps_l.weight ** 2).sum() / SD_LOGL ** 2

class NIMIWAE70(M.NIMIWAE56):
    build = staticmethod(lambda seq, vocab: build(seq, vocab))                             # predictions and stand totals use the joint build
    def __init__(s, nsp, nl3, nmaj, nfty, nspc, nc_spc, nc_grp, age=True, l0=None):
        super().__init__(nsp, nl3, nmaj, nfty, nspc, censor='known', growth='flex', noise=False, prior_ctx=False)
        s.age = age
        s.st_q = Static70(nsp, nl3, nmaj, nfty, nspc); s.st_p = Static70(nsp, nl3, nmaj, nfty, nspc, use_summ=False)
        s.curve = AgeCurve(nc_spc, nc_grp, l0)
        s.bhead = nn.Linear(DZ + D_MODEL, 1)
        s.lsS, s.lstau = nn.Parameter(torch.tensor(-1.5)), nn.Parameter(torch.tensor(-1.5))
        with torch.no_grad(): s.bhead.bias.fill_(math.log(math.expm1(25.0)))
    def ages(s, z, B):
        """a_t [K, n, T]: one birth date per tree. a_N > 0 at the last slot; earlier slots subtract elapsed years exactly, so a_t < 0 before birth."""
        K = z.shape[0]; tok = s.st_p(B)[None].expand(K, -1, -1)
        aN = F.softplus(s.bhead(torch.cat([z, tok], -1)))[..., 0]                         # [K, n]
        t = B['cov'][..., ITYR] * 20.0                                                     # [n, T] years since 2000, from measurement dates
        last = (B['v'].sum(1).long() - 1).clamp(min=0)
        tN = t.gather(1, last[:, None])                                                    # [n, 1]
        return aN[..., None] + (t - tN)[None]
    def born_gate(s, lp1, lp0, z, B):
        if not s.age: return super().born_gate(lp1, lp0, z, B)
        a = s.ages(z, B); lb = F.logsigmoid(KAPPA_B * a)
        l1 = lp1 + lb
        return l1, torch.log(-torch.expm1(l1.clamp(max=-1e-6))), a > 0
    def vigour(s, z, mp, lp):
        """[K, n]: distributed N(0, tau) under the prior, so a tree leaves the curve only at a real cost"""
        return torch.exp(s.lstau) * (z[..., 0] - mp[..., 0]) * torch.exp(-0.5 * lp[..., 0])
    def extra_lw(s, lDp, z, B, mp, lp):
        if not s.age: return 0.0
        a = s.ages(z, B); tok = s.curve.token(B); K = z.shape[0]
        ls = torch.stack([s.curve.log_s(tok, a[k]) for k in range(K)])                     # [K, n, T]
        cc = (B['cc'].long().clamp(0, 5) * (B['mcc'] > 0).long())
        v_ = s.vigour(z, mp, lp)[..., None]                                                 # [K, n, 1]
        ar = torch.arange(T_MAX, device=z.device)[None, None]
        m = (B['v'][None] > 0) & (ar <= B['l'][None, :, None]) & (a >= 0.5)
        mu = ls + s.curve.delta(cc)[None, ..., 0] + v_
        sd = torch.exp(s.lsS)
        fit = ((-0.5 * ((lDp - mu) / sd) ** 2 - s.lsS - 0.5 * math.log(2 * math.pi)) * m).sum(-1)
        unborn = (F.logsigmoid(-M.KAPPA * (lDp - B['lex'][None])) * ((a < 0) & (B['v'][None] > 0))).sum(-1)
        return fit + unborn
    def rec_ll(s, R):
        """log likelihood per age record [m]; touches only the curve and its two scale parameters."""
        tok = s.curve.token(R); l = s.curve.years_bh(R['c_grp'])
        a = R['a_obs'] + torch.where(R['src'] == TOT, torch.zeros_like(l), l)
        mu = s.curve.log_s(tok, a) + s.curve.delta(R['cc'])[..., 0]
        var = torch.exp(2 * M.ls_dia(R['lD'])) + torch.exp(2 * s.lsS) + torch.exp(2 * s.lstau)
        return -0.5 * (R['lD'] - mu) ** 2 / var - 0.5 * torch.log(var) - 0.5 * math.log(2 * math.pi)

def make_model(vocab, age=True):
    cv = vocab['_curve']
    m = NIMIWAE70(len(vocab['spgrpcd']), len(vocab['epa_l3']), len(vocab['major_spgrpcd']), len(vocab['fortypcd']) + 1, len(vocab['spcd']), len(cv['spc']), len(cv['grp']), age=age, l0=cv['l0'])
    m.vocab = vocab; m.cfg = dict(censor='known', growth='flex', noise=False, prior_ctx=False, age=age); return m

@torch.no_grad()
def predict_age(model, R, grid=None):
    """Age whose curve size is nearest the record's DIA (the record's own age is not used); [m], on the recorded age's own scale."""
    grid = grid if grid is not None else torch.exp(torch.linspace(0.0, math.log(400.0), 256))
    out = []
    for i in range(0, R['lD'].shape[0], 4096):
        r = sub(R, slice(i, i + 4096)); tok = model.curve.token(r)
        ls = torch.stack([model.curve.log_s(tok, a.expand(tok.shape[0])) for a in grid], 1) + model.curve.delta(r['cc'])
        ix = (ls - r['lD'][:, None]).abs().argmin(1); a = grid[ix]
        out.append(a - torch.where(r['src'] == TOT, torch.zeros_like(a), model.curve.years_bh(r['c_grp'])))
    return torch.cat(out)

def to_dev(b, device):
    return {k: v.to(device, non_blocking=True) for k, v in b.items()}

def train(tr, rec, epochs=4, k=32, bs=512, max_trees=0, seed=0, threads=7, lr=1e-3, log=print, age=True, device='cpu'):
    assert set(tr.split.unique()) <= {'train'} and set(rec.split.unique()) <= {'train'}, 'validation and test plots never enter fitting'
    assert rec.first_of_tree.all() and rec.fit_ok.all(), 'one age record per tree in fitting, within the curve range; repeats test the step'
    torch.manual_seed(seed); torch.set_num_threads(threads); rng = np.random.default_rng(seed)
    cv = curve_vocab(tr, rec); B, vocab, _ = build(tr, cv=cv); n = B['v'].shape[0]; R = build_rec(rec, cv); nr = R['lD'].shape[0]
    if max_trees and max_trees < n: B = sub(B, torch.tensor(rng.permutation(n)[:max_trees])); n = max_trees
    model = make_model(vocab, age=age).to(device)
    phi = model.enc_params(); pid = {id(p) for p in phi}; theta = [p for p in model.parameters() if id(p) not in pid]
    opt = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=1e-5)
    nb_ep = (n + bs - 1) // bs
    sched = torch.optim.lr_scheduler.OneCycleLR(opt, max_lr=lr, total_steps=epochs * nb_ep, pct_start=0.05, anneal_strategy='cos')
    log(f'train trees {n:,}; slots {int(B["v"].sum()):,}; age records {nr:,}; K {k} (DReG); age {age}; device {device}')
    hist = []
    for ep in range(epochs):
        t0, tot, totr, nb = time.time(), 0.0, 0.0, 0
        for ix in torch.split(torch.tensor(rng.permutation(n)), bs):
            b = to_dev(sub(B, ix), device); o = model.joint(b, k, dreg=True)
            loss_t = -(torch.logsumexp(o['lw'], 0) - math.log(k)).mean()
            if age:
                r = to_dev(sub(R, torch.tensor(rng.integers(0, nr, bs))), device)
                lr_ = -model.rec_ll(r).mean(); loss_t = loss_t + (nr / n) * lr_ - model.curve.log_prior_bh() / n; totr += lr_.item()
            wt = torch.softmax(o['lw'], 0).detach()
            loss_p = -(wt ** 2 * o['lw_dreg']).sum(0).mean()
            gt = torch.autograd.grad(loss_t, theta, retain_graph=True, allow_unused=True)
            gp = torch.autograd.grad(loss_p, phi, allow_unused=True)
            opt.zero_grad()
            for p_, g_ in list(zip(theta, gt)) + list(zip(phi, gp)): p_.grad = g_
            nn.utils.clip_grad_norm_(model.parameters(), 5.0); opt.step(); sched.step()
            tot += loss_t.item(); nb += 1
        ess = float((1 / (torch.softmax(o['lw'].detach(), 0) ** 2).sum(0)).median())
        hist.append(dict(epoch=ep, loss=tot / nb, rec_nll=totr / nb, ess_last_batch=ess, sd_S=torch.exp(model.lsS).item(), tau=torch.exp(model.lstau).item(), sd_disc=torch.exp(model.lsdisc).item(), age_sd=float(model.ages(o['z'].detach(), b)[..., -1].std()) if age else 0.0))
        log(json.dumps(hist[-1]) + f' {time.time() - t0:.0f}s')
    return model.to('cpu'), hist
