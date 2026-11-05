"""The JAX port against the PyTorch code it replaces: one set of weights in both, the same noise, then the log weights, the DReG log weights, the
draws and both gradient sets (bound -> generative side, DReG -> encoder side) must agree. Also round-trips the weights Flax -> PyTorch.

  python -I nimiwae_jax/equivalence.py DATA_DIR    # a real batch from the frame, every configuration the cross-validation trains"""
import sys, os, math, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np, torch
import jax, jax.numpy as jnp
from flax import nnx
jax.config.update('jax_default_matmul_precision', 'highest')       # full float32 matmuls: no TF32 on GPUs (as PyTorch's matmul default)

import nimiwae56 as M, nimiwae70 as A
from nimiwae_jax import weights as W, train as TR     # absolute: this file also runs as a script


def _rel(a, b):
    a, b = np.asarray(a, np.float64), np.asarray(b, np.float64)
    return float(np.max(np.abs(a - b)) / (np.max(np.abs(b)) + 1e-12))


def check(tm, B, K=4, seed=0, log=print):
    """-> dict of max relative differences; 'pass' when all are within float32 noise. tm: a PyTorch NIMIWAE70; B: a batch from nimiwae70.build."""
    tm.eval(); m = W.from_torch(tm)
    torch.manual_seed(seed); noise = torch.randn(K, B['v'].shape[0], M.DZ)
    torch.manual_seed(seed); o = tm.joint(B, K, dreg=True)              # PyTorch's first random draw is exactly `noise`
    Bj = TR.to_device(B); eps = jnp.asarray(noise.numpy())
    gdef, params, rest = nnx.split(m, nnx.Param, ...)
    f = lambda p: nnx.merge(gdef, p, rest).joint(Bj, eps)
    lw, lwd, z = jax.jit(f)(params)
    R = dict(z=_rel(z, o['z'].detach()), lw=_rel(lw, o['lw'].detach()), lw_dreg=_rel(lwd, o['lw_dreg'].detach()))
    # PyTorch gradients exactly as nimiwae70.train computes them
    phi = tm.enc_params(); pid = {id(x) for x in phi}
    th = [(k, x) for k, x in tm.named_parameters() if id(x) not in pid]; ph = [(k, x) for k, x in tm.named_parameters() if id(x) in pid]
    loss_t = -(torch.logsumexp(o['lw'], 0) - math.log(K)).mean(); wt = torch.softmax(o['lw'], 0).detach(); loss_p = -(wt ** 2 * o['lw_dreg']).sum(0).mean()
    gt = torch.autograd.grad(loss_t, [x for _, x in th], retain_graph=True, allow_unused=True); gp = torch.autograd.grad(loss_p, [x for _, x in ph], allow_unused=True)
    tg = {k: g for (k, _), g in zip(th + ph, list(gt) + list(gp))}
    # JAX gradients through the training step's own routing
    (lw_, lwd_), vjp = jax.vjp(lambda p: f(p)[:2], params); n = lw_.shape[1]; w = jax.nn.softmax(lw_, 0)
    (gb,) = vjp((-w / n, jnp.zeros_like(lwd_))); (gd,) = vjp((jnp.zeros_like(lw_), -w ** 2 / n))
    jg = W.torch_layout(nnx.merge(gdef, TR._route(gb, gd), rest))
    worst, worst_name, unused_ok = 0.0, None, True
    for k_, g in tg.items():
        if k_ not in jg: continue                                         # lsH under the t height model: not a Flax parameter
        if g is None: unused_ok &= float(np.abs(jg[k_]).max()) == 0.0; continue
        r = _rel(jg[k_], g.numpy())
        if r > worst: worst, worst_name = r, k_
    R.update(grad=worst, grad_worst=worst_name, unused_zero=unused_ok)
    # round trip: Flax -> PyTorch gives back the original state
    tm2 = A.model_from_cfg(tm.vocab, tm.cfg) if hasattr(tm, 'vocab') else None
    if tm2 is not None:
        W.to_torch(m, tm2); R['roundtrip'] = max(_rel(tm2.state_dict()[k_].numpy(), v.detach().numpy()) for k_, v in tm.named_parameters() if k_ in jg)
    R['pass'] = bool(max(R['z'], R['lw'], R['lw_dreg']) < 1e-4 and R['grad'] < 5e-3 and unused_ok and R.get('roundtrip', 0.0) == 0.0)
    log(json.dumps(R)); return R


def check_optimizer(steps=5, lr=1e-2, wd=1e-5, seed=0, log=print):
    """optax clip_by_global_norm + contrib.schedule_free_adamw against nimiwae70.ScheduleFreeAdamW with clip_grad_norm_: the same gradient sequence
    through both, then the training weights y and the averaged weights x must agree. PyTorch's warmup is 1 step and optax's off, so both run at the
    full learning rate from the first step (their warmups differ by design, see train.py)."""
    import optax
    g = np.random.default_rng(seed); shapes = dict(a=(4, 3), b=(5,), c=())
    p0 = {k: g.normal(size=s_).astype(np.float32) for k, s_ in shapes.items()}
    grads = [{k: (g.normal(size=s_) * (8.0 if i % 2 else 0.3)).astype(np.float32) for k, s_ in shapes.items()} for i in range(steps)]   # big ones get clipped
    tp = [torch.nn.Parameter(torch.tensor(p0[k])) for k in shapes]; opt = A.ScheduleFreeAdamW(tp, lr=lr, wd=wd, warmup=1)
    for gi in grads:
        for p_, k in zip(tp, shapes): p_.grad = torch.tensor(gi[k])
        torch.nn.utils.clip_grad_norm_(tp, 5.0); opt.step()
    tx = optax.chain(optax.clip_by_global_norm(5.0), optax.contrib.schedule_free_adamw(lr, warmup_steps=None, b1=0.9, b2=0.999, eps=1e-8, weight_decay=wd))
    jp = {k: jnp.asarray(v) for k, v in p0.items()}; st = tx.init(jp)
    for gi in grads:
        up, st = tx.update({k: jnp.asarray(v) for k, v in gi.items()}, st, jp); jp = optax.apply_updates(jp, up)
    jx = optax.contrib.schedule_free_eval_params(st[1], jp)
    R = dict(optimizer_y=max(_rel(jp[k], p_.detach().numpy()) for p_, k in zip(tp, shapes)), optimizer_x=max(_rel(jx[k], x.numpy()) for x, k in zip(opt.x, shapes)))
    R['pass'] = bool(max(R['optimizer_y'], R['optimizer_x']) < 1e-5); log(json.dumps(R)); return R


def _perturb(tm, seed=1):
    """move zero-initialised parameters off zero so the check exercises them: flow weights by 0.02 (a trained flow's last layers average
    |w| ~ 0.004; at 0.2 the splines are so steep that inverting them is ill-conditioned in both frameworks, even in float64), others by 0.2"""
    g = torch.Generator().manual_seed(seed)
    with torch.no_grad():
        for k, p in tm.named_parameters():
            if k.startswith('flow.'): p.add_(0.02 * torch.randn(p.shape, generator=g))
            elif p.abs().sum() == 0: p.add_(0.2 * torch.randn(p.shape, generator=g))
    return tm


if __name__ == '__main__':
    # a real batch: trees of a few TRAIN plots with linked site trees, HTCD and anchors attached exactly as the D74 cross-validation does
    import pandas as pd
    data = sys.argv[1]; k = M.K_ + ['invyr']
    tr = pd.read_parquet(f'{data}/seq.parquet', filters=[('split', '==', 'train')])
    site = pd.read_parquet(f'{data}/site_refs.parquet')
    plots = site.merge(tr[k + ['plot_key']].drop_duplicates(k), on=k).plot_key.drop_duplicates().sample(40, random_state=0)
    b = tr[tr.plot_key.isin(set(plots))].merge(pd.read_parquet(f'{data}/ht_refs.parquet'), on=k, how='left')
    b = b.merge(site[k + ['a_obs']].rename(columns={'a_obs': 'anc_agedia'}), on=k, how='left')
    keep = b[M.K_].drop_duplicates().sample(n=300, random_state=0); b = b.merge(keep, on=M.K_)
    B, vocab, _ = A.build(b)
    cp = dict(delta={str(c): -0.17 for c in (1, 2, 3)} | {'4': -0.10, '5': -0.07}, w={str(c): 0.9 for c in (1, 2, 3)} | {'4': 1.0, '5': 1.0}, m={str(c): -2.0 for c in range(1, 6)})
    A.apply_coupling(B, cp); A.apply_miss(B, 0.05)                         # D74 coupling and a D76 crew-miss factor, both non-trivial
    print(f'batch: {B["v"].shape[0]} trees, {int(B["anc_m"].sum())} anchors, {int((B["af_shift"] != 0).sum())} coupled, {int((B["htcls"] == 1).sum())} visual heights, {int((B["miss_keep"] < 1).sum())} crew-miss slots')
    ok = True
    for age, q, height in ((True, 'gauss', 'gauss'), (True, 'flow', 'gauss'), (True, 'gauss', 't'), (True, 'flow', 't'), (False, 'gauss', 't'), (False, 'flow', 'gauss')):
        torch.manual_seed(0); tm = _perturb(A.make_model(vocab, age=age, q=q, height=height))
        print(f'age {age}, q {q}, height {height}:', end=' '); ok &= check(tm, B)['pass']
    # branches this batch lacks: moved trees (no cause hazard) and the benchmark-only interval mode
    Bf = {k_: v.clone() for k_, v in B.items()}; Bf['moved'][:20] = 1
    un = ((Bf['real'] == 0) & (Bf['sampled'] > 0) & (Bf['v'] > 0)).nonzero()[:30]
    Bf['mode'][un[:, 0], un[:, 1]] = M.MODE['interval']; Bf['llo'][un[:, 0], un[:, 1]] = 0.0
    for q in ('gauss', 'flow'):
        torch.manual_seed(0); tm = _perturb(A.make_model(vocab, age=True, q=q, height='t'))
        print(f'forced branches (20 moved, 30 interval slots), q {q}:', end=' '); ok &= check(tm, Bf)['pass']
    print('optimizer:', end=' '); ok &= check_optimizer()['pass']
    print('equivalence', 'PASS' if ok else 'FAIL'); sys.exit(0 if ok else 1)
