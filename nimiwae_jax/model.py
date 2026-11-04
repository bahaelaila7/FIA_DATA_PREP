"""The D70 joint model in Flax NNX: an `encoder` subtree (the proposal q) and a `generative` subtree (prior, decoder, likelihood scales).

The split is the gradient split of the PyTorch training loop: the generative side learns from the IWAE bound, the encoder side from the DReG
surrogate (Tucker et al. 2018, arXiv 1810.04152). With the two subtrees separate, train.py routes the two gradients by subtree.

Every function here mirrors a method of nimiwae56.NIMIWAE56 / nimiwae70.NIMIWAE70 for the configuration the cross-validation trains:
censor 'known', growth 'flex', no process noise, static prior. Shapes: n trees, T = 8 slots, K samples of z, DZ = 16 latent dimensions,
D_MODEL = 64. A batch `B` is the dict nimiwae70.build produces, as jnp arrays."""
import math
import jax, jax.numpy as jnp
from flax import nnx

import nimiwae56 as M, nimiwae70 as A
from nimiwae56 import T_MAX, DZ, D_MODEL, KAPPA, NEG, RHO_MAX, EV, MODE, DMG_GROUPS, NCOV, NSUMM, NDQ, NQ
from .flow import CondFlow

C2 = 0.5 * math.log(2 * math.pi)
gelu = lambda x: jax.nn.gelu(x, approximate=False)           # torch.nn.GELU default


def log_normal(x, mean, logvar):
    """sum over the last axis of log N(x; mean, exp(logvar))"""
    return (-0.5 * (x - mean) ** 2 / jnp.exp(logvar) - 0.5 * logvar - C2).sum(-1)


# ---------------------------------------------------------------------------------------------------------------------- building blocks
class EncoderLayer(nnx.Module):
    """torch.nn.TransformerEncoderLayer(d, 4 heads, ff 2d, dropout 0, batch_first, norm_first=True, activation relu):
    x + SelfAttention(LayerNorm(x)), then x + FF(LayerNorm(x))."""

    def __init__(self, d: int, heads: int, *, rngs: nnx.Rngs):
        self.norm1 = nnx.LayerNorm(d, epsilon=1e-5, rngs=rngs)       # torch LayerNorm eps
        self.attn = nnx.MultiHeadAttention(heads, d, d, d, decode=False, rngs=rngs)
        self.norm2 = nnx.LayerNorm(d, epsilon=1e-5, rngs=rngs)
        self.ff1 = nnx.Linear(d, 2 * d, rngs=rngs)
        self.ff2 = nnx.Linear(2 * d, d, rngs=rngs)

    def __call__(self, x, keep):
        """x [N, L, d]; keep [N, L] True for real positions (torch's key_padding_mask is its negation)"""
        mask = keep[:, None, None, :]                                 # [N, heads, q, k]: every query may attend to every kept key
        x = x + self.attn(self.norm1(x), mask=mask, deterministic=True)
        return x + self.ff2(jax.nn.relu(self.ff1(self.norm2(x))))


class Transformer(nnx.Module):
    """nimiwae56.tenc: two pre-norm encoder layers, no final norm"""

    def __init__(self, d: int = D_MODEL, layers: int = 2, heads: int = 4, *, rngs: nnx.Rngs):
        self.layers = nnx.List([EncoderLayer(d, heads, rngs=rngs) for _ in range(layers)])

    def __call__(self, x, keep):
        for layer in self.layers: x = layer(x, keep)
        return x


class Static(nnx.Module):
    """nimiwae70.Static70: per-tree statics -> a 64-d tree token. Embeddings of species, ecoregion, groups, forest type, site and physiographic
    classes, then the numeric statics and soil; the encoder's version adds the (coupled) age prior and the tree's record summaries."""

    def __init__(self, vocab_sizes, use_summ: bool, use_af: bool, *, rngs: nnx.Rngs):
        nsp, nl3, nmaj, nfty, nspc = vocab_sizes
        dims = [(nsp + 1, 8), (nl3 + 1, 4), (nmaj + 1, 2), (nfty + 1, 4), (8, 3), (50, 3), (nspc + 1, 8)]
        self.emb = nnx.List([nnx.Embed(n, f, rngs=rngs) for n, f in dims])
        self.use_summ, self.use_af = use_summ, use_af
        n_in = sum(f for _, f in dims) + 9 + A.NSOIL + (2 if use_af else 0) + (NSUMM if use_summ else 0)
        self.out = nnx.Linear(n_in, D_MODEL, rngs=rngs)

    def __call__(self, B):
        ids = ('sp', 'l3', 'maj', 'fty', 'scl', 'phy', 'spc')
        x = [e(B[k]) for e, k in zip(self.emb, ids)] + [B['snum'], B['soil']]
        if self.use_af:                                               # D74: the prior the model uses (shift 0, factor 1 when uncalibrated)
            x.append(jnp.stack([B['af_mu'] + B['af_shift'] - A.AF_MU0, B['af_sd'] * B['af_sdf']], -1))
        if self.use_summ: x.append(B['summ'])                         # tree-own summaries: encoder only (D55 am. 4)
        return self.out(jnp.concatenate(x, -1))


# -------------------------------------------------------------------------------------------------------------------------- the encoder
class Encoder(nnx.Module):
    """q(z | x_obs, r, c): a bidirectional transformer over a tree token and every slot's inputs. Optionally (D73) the non-age dimensions go
    through a conditional spline flow on the tree token; z0 (age) stays a separate Gaussian, so the exact age grid keeps working."""

    def __init__(self, vocab_sizes, age: bool, qfam: str, flow_cfg, *, rngs: nnx.Rngs):
        self.st = Static(vocab_sizes, use_summ=True, use_af=age, rngs=rngs)
        self.ercd = nnx.Embed(10, 4, rngs=rngs)                       # RECONCILECD of the tree's debut
        self.ecc = nnx.Embed(6, 3, rngs=rngs)                         # crown class at a visible record
        self.qin = nnx.Linear(NCOV + NQ + 4 + 3 + NDQ, D_MODEL, rngs=rngs)
        self.tr = Transformer(rngs=rngs)
        self.qout = nnx.Linear(D_MODEL, 2 * DZ, rngs=rngs)
        self.qfam, self.f0 = qfam, (1 if age else 0)
        self.flow = CondFlow(DZ - self.f0, D_MODEL, **flow_cfg, rngs=rngs) if qfam == 'flow' else None

    def __call__(self, B):
        """-> mean, log variance [n, DZ] of the Gaussian, and the tree token [n, 64] (the flow's context)"""
        n, T = B['v'].shape; ar = jnp.arange(T)[None]; f32 = lambda a: a.astype(jnp.float32)
        m = B['mD']; haslo = f32(B['mode'] == MODE['interval'])
        cc = self.ecc(jnp.clip(B['cc'].astype(jnp.int32), 0, 5)) * B['mcc'][..., None]
        x = jnp.concatenate([
            B['cov'],                                                 # known context, 28 channels
            jnp.stack([B['lD'] * m, m, B['lH'] * B['mH'], B['mH'], B['cr'], B['mcr'], B['mcc'], B['real'], B['psamp'], B['llim'], haslo,
                       jnp.maximum(B['llo'], -1) * haslo, f32(ar == B['f'][:, None]), f32(ar == B['l'][:, None]), B['v']], -1),
            jax.nn.one_hot(B['ev'], len(EV)), jax.nn.one_hot(B['mode'], len(MODE)),   # existence evidence and missingness mode
            jnp.broadcast_to(self.ercd(B['rcd'])[:, None], (n, T, 4)), cc,
            B['dmg'], B['m_dmg'][..., None], (B['brk'] * B['m_brk'])[..., None], B['m_brk'][..., None],   # damage, broken top
            jax.nn.one_hot(B['cl'], 3) * B['m_cl'][..., None], B['m_cl'][..., None]], -1)                    # cull class
        tok = self.st(B)[:, None]
        keep = jnp.concatenate([jnp.ones((n, 1), bool), B['v'] > 0.5], 1)  # the tree token is never padded
        oo = self.tr(jnp.concatenate([tok, self.qin(x)], 1), keep)
        mu, lv = jnp.split(self.qout(oo[:, 0]), 2, -1)
        return mu, jnp.clip(lv, -8, 4), oo[:, 0]

    def sample(self, mq, lq, c, eps):
        """K draws and their log density. eps [K, n, DZ] standard normal. Gaussian: z = e. Flow: z = (e_0, T(e_rest; c)), log q = log N(e) - log |dT/de|."""
        e = mq + jnp.exp(0.5 * lq) * eps
        if self.flow is None: return e, log_normal(e, mq, lq)
        zr, ld = self.flow(e[..., self.f0:], c)
        return jnp.concatenate([e[..., :self.f0], zr], -1), log_normal(e, mq, lq) - ld

    def log_q_detached(self, z, mq, lq, c):
        """The DReG density: log q(z) with every q parameter held fixed (Gaussian, context and flow weights) while z keeps its gradient path."""
        sg = jax.lax.stop_gradient; mq, lq, c = sg(mq), sg(lq), sg(c)
        if self.flow is None: return log_normal(z, mq, lq)
        gdef, state = nnx.split(self.flow)                            # a copy of the flow whose weights carry no gradient
        er, ld = nnx.merge(gdef, jax.tree.map(sg, state))(z[..., self.f0:], c, inverse=True)
        return log_normal(jnp.concatenate([z[..., :self.f0], er], -1), mq, lq) + ld


# ----------------------------------------------------------------------------------------------------------------------- the generative side
class Generative(nnx.Module):
    """p(z | statics) and p(x | z, c): the conditional prior, the decoder (never sees the tree's own records), and the likelihood's scales."""

    def __init__(self, vocab_sizes, age: bool, height: str, *, rngs: nnx.Rngs):
        self.st = Static(vocab_sizes, use_summ=False, use_af=False, rngs=rngs)   # prior / decoder token: no tree-own information (B10)
        self.prior0 = nnx.Linear(D_MODEL, D_MODEL, rngs=rngs); self.prior1 = nnx.Linear(D_MODEL, 2 * DZ, rngs=rngs)
        self.din = nnx.Linear(NCOV, D_MODEL, rngs=rngs); self.dz = nnx.Linear(DZ, D_MODEL, rngs=rngs)
        self.tr = Transformer(rngs=rngs)
        nd = len(DMG_GROUPS)
        self.dout = nnx.Linear(D_MODEL, 2 + 3 + 1 + 5 + nd + 1 + 3, rngs=rngs)   # dD, dH | survive, die, cut | CR | CCLCD | damage | broken | cull
        self.lsdisc, self.lscr, self.lrho = nnx.Param(jnp.float32(-4.0)), nnx.Param(jnp.float32(-2.0)), nnx.Param(jnp.float32(-2.0))
        self.height = height
        if height == 't': self.lsHc = nnx.Param(jnp.full((2,), -2.0))  # D74: log HT scale per method (1 / null, 2-3)
        else: self.lsH = nnx.Param(jnp.float32(-2.0))
        self.age = age

    def prior(self, B):
        """mean, log variance of p(z | statics); with age on, z0 ~ N(0, 1) exactly"""
        mu, lv = jnp.split(self.prior1(gelu(self.prior0(self.st(B)))), 2, -1); lv = jnp.clip(lv, -6, 3)
        if self.age:
            keep = jnp.ones(DZ).at[0].set(0.0); mu, lv = mu * keep, lv * keep
        return mu, lv

    def decode(self, z, B):
        """-> D, H [K, n, T] latent size paths (cumulative softplus increments), log hazards [K, n, T, 3], CR, CCLCD, damage, broken, cull logits"""
        K, n = z.shape[:2]
        if self.age: z = z.at[..., 0].set(0.0)                        # the decoder never reads z0: growth cannot be read as age
        h = self.din(B['cov'])[None] + self.dz(z)[:, :, None] + self.st(B)[None, :, None]
        keep = jnp.broadcast_to(B['v'][None] > 0.5, (K, n, T_MAX)).reshape(K * n, T_MAX)
        o = self.dout(self.tr(h.reshape(K * n, T_MAX, D_MODEL), keep)).reshape(K, n, T_MAX, -1)
        nd = len(DMG_GROUPS)
        D = jnp.cumsum(jax.nn.softplus(o[..., 0]) * B['v'], -1); H = jnp.cumsum(jax.nn.softplus(o[..., 1]) * B['v'], -1)
        return D, H, jax.nn.log_softmax(o[..., 2:5], -1), jax.nn.sigmoid(o[..., 5]), o[..., 6:11], o[..., 11:11 + nd], o[..., 11 + nd], o[..., 12 + nd:15 + nd]

    def height_ll(self, lHp, B):
        """log density of measured log HT given the path: Gaussian, or (D74) Student-t, nu = 4, with a scale per height method"""
        if self.height == 't':
            ls = self.lsHc[...][B['htcls']][None]; r = (B['lH'][None] - lHp) / jnp.exp(ls); nu = A.T_NU
            return math.lgamma((nu + 1) / 2) - math.lgamma(nu / 2) - 0.5 * math.log(nu * math.pi) - ls - (nu + 1) / 2 * jnp.log1p(r ** 2 / nu)
        ls = self.lsH[...]; return -0.5 * ((B['lH'][None] - lHp) / jnp.exp(ls)) ** 2 - ls - C2


# ------------------------------------------------------------------------------------------------------------------------------ the model
class NIMIWAE70(nnx.Module):
    """The joint model. `joint(B, eps)` returns the IWAE log weights, the DReG log weights and the draws, exactly as nimiwae56.NIMIWAE56.joint
    with nimiwae70.NIMIWAE70's hooks (birth gate, gated cut-off term, anchors, height model, flow)."""

    def __init__(self, vocab_sizes, age: bool = True, qfam: str = 'gauss', flow_cfg=None, height: str = 'gauss', *, rngs: nnx.Rngs):
        self.age = age
        self.encoder = Encoder(vocab_sizes, age, qfam, flow_cfg or {}, rngs=rngs)
        self.generative = Generative(vocab_sizes, age, height, rngs=rngs)

    # --- age (D70): one birth date per tree
    def ages(self, z, B):
        """a_t [K, n, T]: log a_l = af_mu + shift + af_sd w z0 at the prior's slot la; every other slot adds elapsed years exactly"""
        mu = B['af_mu'] + B['af_shift']; sd = B['af_sd'] * B['af_sdf']
        al = jnp.exp(jnp.minimum(mu[None] + sd[None] * z[..., 0], 7.0))
        t = B['cov'][..., A.ITYR] * 20.0                              # years since 2000, from measurement dates
        return al[..., None] + (t - jnp.take_along_axis(t, B['la'][:, None], 1))[None]

    def joint(self, B, eps):
        """eps [K, n, DZ] standard normal -> (lw, lw_dreg) [K, n] and z [K, n, DZ]. The end slot e and the cause f are summed out exactly."""
        enc, gen = self.encoder, self.generative
        mq, lq, hq = enc(B); mp, lp = gen.prior(B); K = eps.shape[0]; T = T_MAX; ar = jnp.arange(T)
        z, lqz = enc.sample(mq, lq, hq, eps)
        D, H, lend, crm, ccl, dml, brl, cll = gen.decode(z, B)
        lDp, lHp = jnp.log(jnp.maximum(D, 1e-3)), jnp.log(jnp.maximum(H, 1.0))
        # existence: the size step at the smallest tally level, gated by breast height when age is on (D70 am. 16)
        xe = KAPPA * (lDp - B['lex'][None])
        lp1, lp0 = jax.nn.log_sigmoid(xe), jax.nn.log_sigmoid(-xe)
        if self.age:
            bh = self.ages(z, B) - B['l0'][None, :, None]              # years since breast height
            lp1 = lp1 + jax.nn.log_sigmoid(A.KAPPA_B * bh); lp0 = jnp.log(-jnp.expm1(jnp.minimum(lp1, -1e-6))); born = bh > 0
        else:
            born = jnp.ones_like(lp1, bool)
        nl = lambda x, mu, ls: -0.5 * ((x - mu) / jnp.exp(ls)) ** 2 - ls - C2
        rho = RHO_MAX * jax.nn.sigmoid(gen.lrho[...]); lr, l1r = jnp.log(rho), jnp.log(1 - rho)   # soft-label error rate
        v, real, ev, mode = B['v'][None], B['real'][None], B['ev'][None], B['mode'][None]
        # measurements at visible records: log DIA with FIA's measurement error plus a learned discrepancy; log HT by the height model
        sdD = 0.5 * jnp.log(0.02 ** 2 + (0.06 / jnp.exp(B['lD'])) ** 2 + jnp.exp(2 * gen.lsdisc[...]))
        meas = nl(B['lD'][None], lDp, sdD[None]) * B['mD'][None] + gen.height_ll(lHp, B) * B['mH'][None]
        # existence evidence per slot, while alive (ev_alive) and once gone (ev_gone); soft labels are wrong with rate rho
        lse = jnp.logaddexp
        ev_alive = jnp.where(ev == EV['true_hard'], lp1, jnp.where(ev == EV['false_hard'], lp0, jnp.where(ev == EV['true_soft'], lse(lp1 + l1r, lp0 + lr),
                   jnp.where(ev == EV['false_soft'], lse(lp0 + l1r, lp1 + lr), 0.0))))
        ev_gone = jnp.where(ev == EV['true_hard'], NEG, jnp.where(ev == EV['true_soft'], lr, jnp.where(ev == EV['false_soft'], l1r, 0.0))) + real * NEG
        ev_gone = jnp.broadcast_to(ev_gone, lp1.shape)
        # FIA's design rule as a fixed mask model on the latent size (no learned parameter)
        xt = KAPPA * (lDp - B['llim'][None])                           # at / above the tally limit
        above_ex = ((B['llim'] > B['lex'] + 1e-6) | (ar[None] > B['l'][:, None]))[None]
        unrec = (1 - real) * B['sampled'][None]; ps = jnp.broadcast_to(B['psamp'][None], xt.shape)
        if self.age:                                                   # D72: sampled, no record: log(1 - p_s g_t sigma(x_t))
            lq_ = jnp.log(jnp.clip(ps, 1e-6, 1.0)) + jax.nn.log_sigmoid(A.KAPPA_B * bh)
            lcutoff = jnp.logaddexp(jnp.log(jnp.maximum(-jnp.expm1(lq_), 1e-12)), lq_ + jax.nn.log_sigmoid(-xt))
        else:                                                          # D55 am. 6: the spot was sampled with probability p_s
            psc = jnp.clip(ps, 1e-6, 1 - 1e-6)
            lcutoff = jnp.where(ps >= 1, jax.nn.log_sigmoid(-xt), jnp.logaddexp(jnp.log(psc) + jax.nn.log_sigmoid(-xt), jnp.log1p(-psc)))
        cens = (real * jax.nn.log_sigmoid(xt)
                + unrec * jnp.where((mode == MODE['cutoff']) & above_ex, lcutoff, 0.0)
                + unrec * jnp.where(mode == MODE['missed'], jax.nn.log_sigmoid(xt), 0.0)
                + unrec * jnp.where(mode == MODE['interval'], jax.nn.log_sigmoid(-xt), 0.0))
        # per end slot e: log p(slots before e | alive) + log p(slots from e on | gone), by prefix sums  [K, n, E = T + 1]
        la = (meas + ev_alive + cens) * v; lg = ev_gone * v
        Ca = jnp.concatenate([jnp.zeros_like(la[..., :1]), jnp.cumsum(la, -1)], -1)
        Cg = jnp.concatenate([jnp.zeros_like(lg[..., :1]), jnp.cumsum(lg, -1)], -1); Cg = Cg[..., -1:] - Cg
        lle = Ca + Cg
        # end and cause prior: at risk from the first slot at which the tree exists (never after its first record)
        l, nv = B['l'][None, :, None], B['v'].sum(1).astype(jnp.int32)[None, :, None]
        exists = jax.lax.stop_gradient((xe > 0) & (v > 0) & born)
        a_ = jnp.where(exists.any(-1), jnp.argmax(exists, -1), B['f'][None])
        a_ = jnp.minimum(a_, B['f'][None])[..., None]
        lsv, ldie, lcut = lend[..., 0], lend[..., 1], lend[..., 2]
        risk = (ar[None, None] > a_) * B['v'][None]
        csum = jnp.cumsum(lsv * risk, -1); cprev = jnp.concatenate([jnp.zeros_like(csum[..., :1]), csum[..., :-1]], -1)
        valid = (ar[None, None] > l) & (ar[None, None] < nv); mv = (B['moved'] == 1)[None, :, None]
        lpe_die = jnp.where(valid, cprev + jnp.where(mv, 0.0, ldie), NEG)   # a move-out ends the lineage without a cause hazard
        lpe_cut = jnp.where(valid & ~mv, cprev + lcut, NEG)
        alive = jnp.take_along_axis(csum, jnp.broadcast_to(jnp.maximum(nv - 1, 0), (K,) + nv.shape[1:]), -1)
        lpe = jnp.stack([jnp.concatenate([lpe_die, alive], -1), jnp.concatenate([lpe_cut, jnp.full_like(alive, NEG)], -1)], -1)
        known = (B['ndef'] == 1) & (B['moved'] == 0)                  # a recorded end fixes the cause
        cmask = jnp.where(known[:, None] & (jnp.arange(2)[None] == (1 - B['cause'])[:, None]), NEG, 0.0)[None, :, None, :]
        lpe = lpe + cmask * (jnp.arange(T + 1)[None, None] < T)[..., None]
        lpx = jax.nn.logsumexp((lpe + lle[..., None]).reshape(K, lpe.shape[1], -1), -1)
        # crown, damage, broken top and cull class at visible records
        bce = lambda x, y: -(y * jax.nn.log_sigmoid(x) + (1 - y) * jax.nn.log_sigmoid(-x))
        pick = lambda logits, idx: jnp.take_along_axis(jax.nn.log_softmax(logits, -1), jnp.broadcast_to(idx[None, ..., None], logits.shape[:-1] + (1,)), -1)[..., 0]
        lrec = ((nl(B['cr'][None], crm, gen.lscr[...]) * B['mcr'][None]).sum(-1)
                + (pick(ccl, jnp.clip(B['cc'].astype(jnp.int32), 1, 5) - 1) * B['mcc'][None]).sum(-1)
                + (-bce(dml, B['dmg'][None]).sum(-1) * B['m_dmg'][None]).sum(-1)
                + (-bce(brl, B['brk'][None]) * B['m_brk'][None]).sum(-1)
                + (pick(cll, B['cl']) * B['m_cl'][None]).sum(-1))
        lpz = log_normal(z, mp, lp)
        lanc = 0.0
        if self.age:                                                   # D74: measured breast-height ages of linked site trees (zero without anchors)
            bha = jnp.maximum(self.ages(z, B) - B['l0'][None, :, None], 0.1)
            lanc = ((-0.5 * ((B['anc_y'][None] - jnp.log(bha)) / A.S_ANCHOR) ** 2 - math.log(A.S_ANCHOR) - C2) * B['anc_m'][None]).sum(-1)
        lw = lpx + lrec + lpz - lqz + lanc
        lw_dreg = lpx + lrec + lpz - enc.log_q_detached(z, mq, lq, hq) + lanc
        return lw, lw_dreg, z
