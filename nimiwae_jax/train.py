"""Training: the DReG step and the loop, with the same data, calibration, subsample and batch order as nimiwae70.train.

The step (one forward pass, two backward passes through one vjp, as in PyTorch):
  * the bound  L_theta = -mean_n log (1/K) sum_k w_k            ->  cotangent of lw:      -softmax_k(lw) / n   ->  generative subtree
  * DReG       L_phi   = -mean_n sum_k sg(w~_k)^2 log w_k(z(phi)) ->  cotangent of lw_dreg: -sg(w~)^2 / n       ->  encoder subtree
Optimizer: optax clip_by_global_norm(5) then optax.contrib.schedule_free_adamw (Defazio et al. 2024): the same algorithm as
nimiwae70.ScheduleFreeAdamW (y = beta x + (1 - beta) z, averaging weight lr^2, Adam without momentum, decoupled weight decay at y), except that
optax's warmup starts at learning rate 0 where the PyTorch one starts at lr / warmup. Evaluation and checkpoints use the averaged x
(optax.contrib.schedule_free_eval_params), as PyTorch's opt.averaged()."""
import os, math, json, time, pickle
import numpy as np, torch
import jax, jax.numpy as jnp, optax
from flax import nnx
jax.config.update('jax_default_matmul_precision', 'highest')       # full float32 matmuls: no TF32 on GPUs (as PyTorch's matmul default)

import nimiwae56 as M, nimiwae70 as A
from nimiwae56 import DZ
from . import weights as W

INT_KEYS = ('ev', 'mode', 'cl', 'htcls', 'la', 'f', 'l', 'cause', 'moved', 'ndef', 'rcd', 'sp', 'l3', 'maj', 'fty', 'scl', 'phy', 'spc', 'cc_la')


def to_device(B):
    """nimiwae70.build's tensors -> jnp arrays on the default device (index tensors as int32, everything else float32)"""
    out = {}
    for k, v in B.items():
        a = v.numpy()
        out[k] = jnp.asarray(a.astype(np.int32) if (k in INT_KEYS or a.dtype.kind in 'iub') else a.astype(np.float32))
    return out


def _route(g_bound, g_dreg):
    """the encoder subtree takes the DReG gradient, the generative subtree the bound's"""
    return jax.tree_util.tree_map_with_path(lambda path, a, b: b if getattr(path[0], 'key', None) == 'encoder' else a, g_bound, g_dreg)


def make_optimizer(model, lr, warmup, weight_decay=1e-5, clip=5.0):
    tx = optax.chain(optax.clip_by_global_norm(clip),
                     optax.contrib.schedule_free_adamw(lr, warmup_steps=warmup, b1=0.9, b2=0.999, eps=1e-8, weight_decay=weight_decay))
    return nnx.Optimizer(model, tx, wrt=nnx.Param)


@nnx.jit(static_argnames=('K',))
def train_step(model, optimizer, B, idx, key, K):
    """one DReG step on the trees `idx` of the device-resident frame `B`; updates model and optimizer in place; -> (loss, lw)"""
    b = jax.tree.map(lambda a: a[idx], B)
    eps = jax.random.normal(key, (K, idx.shape[0], DZ))
    gdef, params, rest = nnx.split(model, nnx.Param, ...)
    (lw, lwd), vjp = jax.vjp(lambda p: nnx.merge(gdef, p, rest).joint(b, eps)[:2], params)
    n = lw.shape[1]; w = jax.nn.softmax(lw, 0)
    (g_bound,) = vjp((-w / n, jnp.zeros_like(lwd)))                     # d L_theta
    (g_dreg,) = vjp((jnp.zeros_like(lw), -jax.lax.stop_gradient(w) ** 2 / n))   # d L_phi
    optimizer.update(model, _route(g_bound, g_dreg))
    return -(jax.nn.logsumexp(lw, 0) - math.log(K)).mean(), lw


def eval_params(model, optimizer):
    """Schedule-Free's averaged weights x = (y - (1 - beta) z) / beta (optax.contrib.schedule_free_eval_params), as a State of nnx.Param.
    NNX wraps parameters in Param and optimizer state in OptVariable, so the formula is applied to the unwrapped values and rewrapped."""
    isvar = lambda v: isinstance(v, nnx.Variable)
    raw = lambda t: jax.tree.map(lambda v: v.value if isvar(v) else v, t, is_leaf=isvar)
    params = nnx.state(model, nnx.Param)
    sf = optimizer.opt_state[1]                                         # chain(clip, schedule_free): the second transformation's state
    b1 = raw(sf.b1)
    x = jax.tree.map(lambda y, z: (y - (1.0 - b1) * z) / b1, raw(params), raw(sf.z))
    return jax.tree.map(lambda p, xv: p.replace(xv), params, x, is_leaf=isvar)


def save_state(path, epoch, fingerprint, model, optimizer, key, rng, hist):
    """the full training state as plain NumPy dicts (nnx.to_pure_dict: Flax's checkpoint-agnostic form), written to a temporary file and renamed"""
    to_np = lambda st: jax.tree.map(np.asarray, nnx.to_pure_dict(st))
    blob = dict(epoch=epoch, fingerprint=fingerprint, model=to_np(nnx.state(model)), optimizer=to_np(nnx.state(optimizer)),
                key=np.asarray(jax.random.key_data(key)), np_rng=rng.bit_generator.state, torch_rng=torch.get_rng_state(), hist=hist)
    tmp = f'{path}.tmp'
    with open(tmp, 'wb') as f: pickle.dump(blob, f)
    os.replace(tmp, path)

def load_state(path, fingerprint, model, optimizer, rng):
    """restore what save_state wrote into model, optimizer and rng; -> (next epoch, key, history)"""
    with open(path, 'rb') as f: blob = pickle.load(f)
    assert blob['fingerprint'] == fingerprint, f'{path} was saved by another setup: {blob["fingerprint"]} vs {fingerprint}'
    for obj, pure in ((model, blob['model']), (optimizer, blob['optimizer'])):
        st = nnx.state(obj); nnx.replace_by_pure_dict(st, pure); nnx.update(obj, st)
    rng.bit_generator.state = blob['np_rng']; torch.set_rng_state(blob['torch_rng'])
    return blob['epoch'] + 1, jax.random.wrap_key_data(jnp.asarray(blob['key'])), blob['hist']


def train(tr, epochs=12, k=32, bs=512, max_trees=0, seed=0, threads=7, lr=1e-3, log=print, age=True, device='cpu', opt_name='sf', on_epoch=None,
          q='gauss', flow=None, d74=False, state_path=None):
    """nimiwae70.train with this step. Returns (a PyTorch NIMIWAE70 with the averaged weights, the history). `device` is where the PyTorch model the
    hook scores lives; JAX trains on its default device. state_path: after every completed epoch (and its hook) the full training state is saved
    there, and an existing file resumes training after its epoch (the setup must match the saved fingerprint)."""
    assert opt_name == 'sf', 'the JAX path implements Schedule-Free AdamW only'
    assert set(tr.split.unique()) <= {'train'}, 'validation and test plots never enter fitting'
    torch.manual_seed(seed); torch.set_num_threads(threads); rng = np.random.default_rng(seed)
    # the same build, D74 calibration and subsample as nimiwae70.train (PyTorch / NumPy preprocessing)
    B, vocab, _ = A.build(tr); n = B['v'].shape[0]
    if d74: assert 'htcd' in tr, 'D74 needs HTCD in the fitting rows'
    if d74 and age:
        assert 'anc_agedia' in tr, 'D74 age on needs anchors in the fitting rows'
        vocab['age_coupling'] = A.calibrate_coupling(B, log=log); A.apply_coupling(B, vocab['age_coupling'])
        log(f'D74 coupling shifts {int((B["af_shift"] != 0).sum()):,} of {n:,} trees; anchors on {int((B["anc_m"].sum(1) > 0).sum()):,} trees')
    else:
        assert float(B['anc_m'].sum()) == 0, 'anchors in the fitting rows without D74 age on'
    if max_trees and max_trees < n: B = M.sub(B, torch.tensor(rng.permutation(n)[:max_trees])); n = max_trees
    # PyTorch initialisation (the same distributions and zero-initialised heads), copied into Flax
    tm = A.make_model(vocab, age=age, q=q, flow=flow, height='t' if d74 else 'gauss')
    model = W.from_torch(tm); nb_ep = (n + bs - 1) // bs
    tm.to(device)                                                       # the hook's PyTorch model lives where the CV scores (load_state_dict keeps devices)
    opt = make_optimizer(model, lr, warmup=max(1, int(0.05 * epochs * nb_ep)))   # optax: 0 warmup steps would hold the learning rate at 0
    Bd = to_device(B); key = jax.random.key(seed)
    log(f'train trees {n:,}; slots {int(B["v"].sum()):,}; prior on {int(B["af_has"].sum()):,} trees; K {k} (DReG); age {age}; q {q}; '
        f'backend jax {jax.__version__} on {jax.devices()[0].platform}; scoring on {device}; optimizer schedule-free adamw (optax)')
    hist, start = [], 0
    fp = dict(backend='jax', n=n, epochs=epochs, k=k, bs=bs, seed=seed, lr=lr, age=age, q=q, cfg=tm.cfg, d74=d74, coupling=vocab.get('age_coupling'))
    if state_path and os.path.exists(state_path):                       # resume after the last completed epoch
        start, key, hist = load_state(state_path, fp, model, opt, rng); log(f'resumed after epoch {start} of {epochs} from {state_path}')
    for ep in range(start, epochs):
        t0, tot, nb = time.time(), jnp.zeros(()), 0
        for ix in np.array_split(rng.permutation(n), range(bs, n, bs)):      # the batch order of nimiwae70.train
            key, sk = jax.random.split(key)
            loss, lw = train_step(model, opt, Bd, jnp.asarray(ix, jnp.int32), sk, K=k); tot = tot + loss; nb += 1   # stays on device: no per-step sync
        tot = float(tot)
        ess = float(jnp.median(1 / (jax.nn.softmax(lw, 0) ** 2).sum(0)))
        hist.append(dict(epoch=ep, loss=tot / nb, ess_last_batch=ess, sd_disc=float(jnp.exp(model.generative.lsdisc.value))))
        log(json.dumps(hist[-1]) + f' {time.time() - t0:.0f}s')
        if on_epoch: on_epoch(ep + 1, W.to_torch(model, tm, eval_params(model, opt)), hist[-1])   # the hook scores the averaged weights
        if state_path: save_state(state_path, ep, fp, model, opt, key, rng, hist)                   # after the hook: an interrupted hook redoes its epoch
    return W.to_torch(model, tm, eval_params(model, opt)).to('cpu'), hist
