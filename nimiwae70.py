"""D70 — joint NIMIWAE age model: NIMIWAE56 with flexible growth and the known design rule, plus one birth date per tree (design of D70 am. 12; the curve version is nimiwae70_curve.py).

Age       log a_l = af_mu + af_sd z0, z0 ~ N(0, 1) under the prior, at the tree's first visible live measured record l (am. 13: the direct model's age rises 0.5-0.6 yr per year, so reading a later record and walking back runs young). af_mu and af_sd come from the direct age model
          (nimiwae_age_direct.py), fitted on TRAIN measured ages only and fixed here, so lineages cannot move them. z0 is a true latent, so the width of age is honest: a penalty on a
          function of z would reward collapsing onto af_mu.
Birth     b = t_l - a_l. Age at a slot is a_t = t - b at every visit, negative before birth, so it advances exactly with elapsed time. Dates, never INVYR.
Existence E_t needs t >= b as well as the size step and t < e: the D55 step is multiplied by sigma(KAPPA_B a_t).
Size      the decoder never reads z0, so growth is not read as age and age does not bend the size path. Only the birth gate and the unborn term below tie the two.
Unborn    a slot before birth is scored below the smallest tally level, so the size path cannot carry a tree that does not exist.
E1        a microplot debut on a moved plot: slots before the move carry no FALSE label, because that spot was not sampled then (D65 item 2). The birth date decides instead."""
import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import math, time, json
import numpy as np, pandas as pd, torch, torch.nn as nn, torch.nn.functional as F
import nimiwae56 as M
from nimiwae56 import T_MAX, DZ, D_MODEL, COV, sub

SOIL_COLS = [f'soil_{c}' for c in ('clay', 'sand', 'silt', 'soc', 'bd', 'ph', 'cec', 'depth_restriction', 'rock_frag', 'depth_bedrock')]
NSOIL = len(SOIL_COLS)
KAPPA_B = 2.0                 # per year: the birth step is about half a year wide
AF_MU0, AF_SD0 = math.log(25.0), 0.8     # trees with no usable record: wide default, not a measurement
ITYR = COV.index('tyr')

def soil_vocab(seq):
    return {c: (float(seq[c].mean()), float(seq[c].std()) or 1.0) for c in SOIL_COLS}

def e1_mask(seq):
    """Slots whose FALSE label is dropped: before the plot's move, for a tree first recorded at or after it."""
    if 'move_invyr' not in seq: return pd.Series(False, index=seq.index)
    first = seq[seq.slot_kind == 'real'].groupby(M.K_).invyr.min().rename('_first')
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
    mu, sd, la = np.full(n, AF_MU0, np.float32), np.full(n, AF_SD0, np.float32), B['l'].clone()
    if 'af_mu' in sq:                                                                          # the first visible slot with a prior; hidden records never feed it
        hid = sq.hidden.fillna(False).to_numpy(bool) if 'hidden' in sq else np.zeros(len(sq), bool)
        ok = (sq.slot_kind == 'real').to_numpy() & ~hid & sq.af_mu.notna().to_numpy()
        o = pd.DataFrame(dict(t=ti[ok], p=po[ok], m=sq.af_mu.to_numpy(float)[ok], s=sq.af_sd.to_numpy(float)[ok])).sort_values(['t', 'p']).groupby('t').first()
        mu[o.index.to_numpy()] = o.m.to_numpy(); sd[o.index.to_numpy()] = o.s.to_numpy(); la[torch.tensor(o.index.to_numpy())] = torch.tensor(o.p.to_numpy(), dtype=la.dtype)
        B['af_has'] = torch.zeros(n); B['af_has'][torch.tensor(o.index.to_numpy())] = 1.0
    else:
        B['af_has'] = torch.zeros(n)
    B['af_mu'], B['af_sd'], B['la'] = torch.tensor(mu), torch.tensor(sd), la
    return B, vocab, sq

class Static70(M.Static):
    """Soil joins the statics of the prior and the decoder. The encoder also reads the age prior, so it knows how close a tree sits to its birth limits."""
    def __init__(s, nsp, nl3, nmaj, nfty, nspc, use_summ=True, use_af=False):
        super().__init__(nsp, nl3, nmaj, nfty, nspc, use_summ); s.use_af = use_af
        s.out = nn.Linear(s.out.in_features + NSOIL + (2 if use_af else 0), D_MODEL)
    def forward(s, B):
        x = [s.e[0](B['sp']), s.e[1](B['l3']), s.e[2](B['maj']), s.e[3](B['fty']), s.e[4](B['scl']), s.e[5](B['phy']), s.e[6](B['spc']), B['snum'], B['soil']]
        if s.use_af: x.append(torch.stack([B['af_mu'] - AF_MU0, B['af_sd']], -1))
        return s.out(torch.cat(x + ([B['summ']] if s.use_summ else []), -1))

class NIMIWAE70(M.NIMIWAE56):
    build = staticmethod(lambda seq, vocab: build(seq, vocab))                             # predictions and stand totals use the joint build
    def __init__(s, nsp, nl3, nmaj, nfty, nspc, age=True):
        super().__init__(nsp, nl3, nmaj, nfty, nspc, censor='known', growth='flex', noise=False, prior_ctx=False)
        s.age = age
        s.st_q = Static70(nsp, nl3, nmaj, nfty, nspc, use_af=age); s.st_p = Static70(nsp, nl3, nmaj, nfty, nspc, use_summ=False)
    def p_z(s, B):
        mu, lv = super().p_z(B)
        if not s.age: return mu, lv
        keep = torch.ones_like(mu); keep[..., 0] = 0.0                                        # z0 ~ N(0, 1) exactly
        return mu * keep, lv * keep
    def decode(s, z, B, u=None):
        if s.age:
            z = z.clone(); z[..., 0] = 0.0
        return super().decode(z, B, u)
    def ages(s, z, B):
        """a_t [K, n, T]: one birth date per tree. a_l > 0 at the prior's slot, the debut or the first visible record; other slots add elapsed years exactly, so a_t < 0 before birth."""
        al = torch.exp((B['af_mu'][None] + B['af_sd'][None] * z[..., 0]).clamp(max=7.0))     # [K, n]; up to about 1100 yr
        t = B['cov'][..., ITYR] * 20.0                                                        # [n, T] years since 2000, from measurement dates
        return al[..., None] + (t - t.gather(1, B['la'][:, None]))[None]
    def born_gate(s, lp1, lp0, z, B):
        if not s.age: return super().born_gate(lp1, lp0, z, B)
        a = s.ages(z, B); l1 = lp1 + F.logsigmoid(KAPPA_B * a)
        return l1, torch.log(-torch.expm1(l1.clamp(max=-1e-6))), a > 0
    def extra_lw(s, lDp, z, B, mp, lp):
        if not s.age: return 0.0
        return (F.logsigmoid(-M.KAPPA * (lDp - B['lex'][None])) * torch.sigmoid(-KAPPA_B * s.ages(z, B)) * B['v'][None]).sum(-1)   # same soft step as the gate, so z0 feels it

def make_model(vocab, age=True):
    m = NIMIWAE70(len(vocab['spgrpcd']), len(vocab['epa_l3']), len(vocab['major_spgrpcd']), len(vocab['fortypcd']) + 1, len(vocab['spcd']), age=age)
    m.vocab = vocab; m.cfg = dict(censor='known', growth='flex', noise=False, prior_ctx=False, age=age); return m

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
    @torch.no_grad()
    def finish(s):
        for p, x in zip(s.ps, s.x): p.copy_(x)

def to_dev(b, device):
    return {k: v.to(device, non_blocking=True) for k, v in b.items()}

def train(tr, epochs=12, k=32, bs=512, max_trees=0, seed=0, threads=7, lr=1e-3, log=print, age=True, device='cpu', opt_name='adamw'):
    assert set(tr.split.unique()) <= {'train'}, 'validation and test plots never enter fitting'
    torch.manual_seed(seed); torch.set_num_threads(threads); rng = np.random.default_rng(seed)
    B, vocab, _ = build(tr); n = B['v'].shape[0]
    if max_trees and max_trees < n: B = sub(B, torch.tensor(rng.permutation(n)[:max_trees])); n = max_trees
    model = make_model(vocab, age=age).to(device)
    phi = model.enc_params(); pid = {id(p) for p in phi}; theta = [p for p in model.parameters() if id(p) not in pid]
    nb_ep = (n + bs - 1) // bs
    if opt_name == 'sf':                                                                     # warmup = the 5 % of steps OneCycleLR uses
        opt = ScheduleFreeAdamW(model.parameters(), lr=lr, wd=1e-5, warmup=int(0.05 * epochs * nb_ep)); sched = None
    else:
        opt = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=1e-5)
        sched = torch.optim.lr_scheduler.OneCycleLR(opt, max_lr=lr, total_steps=epochs * nb_ep, pct_start=0.05, anneal_strategy='cos')
    log(f'train trees {n:,}; slots {int(B["v"].sum()):,}; prior on {int(B["af_has"].sum()):,} trees; K {k} (DReG); age {age}; device {device}; optimizer {opt_name}')
    hist = []
    for ep in range(epochs):
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
                w = torch.softmax(o['lw'], 0); z0 = o['z'][..., 0]; al = torch.exp(b['af_mu'][None] + b['af_sd'][None] * z0)
                pm = (w * z0).sum(0); e0 = dict(z0_mean=pm.mean().item(), z0_abs=pm.abs().mean().item(), z0_sd=((w * z0 ** 2).sum(0) - pm ** 2).clamp(min=0).sqrt().median().item(), age_med=al.median().item(), age_max=al.max().item())
        hist.append(dict(epoch=ep, loss=tot / nb, ess_last_batch=ess, sd_disc=torch.exp(model.lsdisc).item(), **e0))
        log(json.dumps(hist[-1]) + f' {time.time() - t0:.0f}s')
    if opt_name == 'sf': opt.finish()                                                        # evaluate and save the averaged weights
    return model.to('cpu'), hist
