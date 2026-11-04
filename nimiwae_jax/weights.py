"""PyTorch <-> Flax parameters for the D70 joint model. One table, `pairs()`, used in both directions.

Each entry pairs a PyTorch state_dict name with the Flax variable that holds the same numbers and the reshape between the two layouts:
  * nn.Linear weight [out, in]                    <-> nnx.Linear kernel [in, out] (transpose); bias as is
  * nn.Embedding weight                           <-> nnx.Embed embedding
  * nn.LayerNorm weight / bias                    <-> nnx.LayerNorm scale / bias
  * nn.MultiheadAttention in_proj_weight [3d, d]  <-> query / key / value kernels [d, heads, head_dim] (rows split in that order, heads contiguous)
    in_proj_bias [3d]                             <-> query / key / value biases [heads, head_dim]
    out_proj.weight [d, d]                        <-> out kernel [heads, head_dim, d]
  * scalars (lsdisc, lscr, lrho, lsH / lsHc)      <-> nnx.Param scalars
Every PyTorch parameter must be paired, except lsH under the Student-t height model, which PyTorch never trains."""
import numpy as np, torch, jax.numpy as jnp
from flax import nnx

from .model import NIMIWAE70


def _linear(pre_t, mod):
    return [(f'{pre_t}.weight', mod.kernel, lambda w: w.T, lambda k: k.T), (f'{pre_t}.bias', mod.bias, None, None)]

def _embed(pre_t, mod):
    return [(f'{pre_t}.weight', mod.embedding, None, None)]

def _norm(pre_t, mod):
    return [(f'{pre_t}.weight', mod.scale, None, None), (f'{pre_t}.bias', mod.bias, None, None)]

def _attention(pre_t, att, d, heads):
    dh = d // heads
    out = []
    for j, name in enumerate(('query', 'key', 'value')):
        lm = getattr(att, name)
        # rows j*d:(j+1)*d of the fused in-projection are this projection's [out, in] matrix; heads are contiguous blocks of dh rows
        out.append((f'{pre_t}.in_proj_weight', lm.kernel, (lambda w, j=j: w[j * d:(j + 1) * d].T.reshape(d, heads, dh)), ('fused_w', j)))
        out.append((f'{pre_t}.in_proj_bias', lm.bias, (lambda b, j=j: b[j * d:(j + 1) * d].reshape(heads, dh)), ('fused_b', j)))
    out.append((f'{pre_t}.out_proj.weight', att.out.kernel, lambda w: w.T.reshape(heads, dh, d), lambda k: k.reshape(heads * dh, d).T))
    out.append((f'{pre_t}.out_proj.bias', att.out.bias, None, None))
    return out

def _transformer(pre_t, tr, d=64, heads=4):
    out = []
    for i, L in enumerate(tr.layers):
        q = f'{pre_t}.layers.{i}'
        out += _attention(q + '.self_attn', L.attn, d, heads) + _linear(q + '.linear1', L.ff1) + _linear(q + '.linear2', L.ff2)
        out += _norm(q + '.norm1', L.norm1) + _norm(q + '.norm2', L.norm2)
    return out

def _static(pre_t, st):
    return [x for i, e in enumerate(st.emb) for x in _embed(f'{pre_t}.e.{i}', e)] + _linear(f'{pre_t}.out', st.out)

def pairs(m: NIMIWAE70):
    """(torch name, Flax variable, torch -> jax reshape or None, jax -> torch reshape or a fused-slice marker or None) for every parameter"""
    enc, gen = m.encoder, m.generative
    P = _static('st_q', enc.st) + _embed('ercd', enc.ercd) + _embed('ecc', enc.ecc) + _linear('qin', enc.qin) + _transformer('qtr', enc.tr) + _linear('qout', enc.qout)
    if enc.flow is not None:
        for i, L in enumerate(enc.flow.layers):
            P += _linear(f'flow.layers.{i}.net.0', L.l0) + _linear(f'flow.layers.{i}.net.2', L.l1) + _linear(f'flow.layers.{i}.net.4', L.l2)
    P += _static('st_p', gen.st) + _linear('prior.0', gen.prior0) + _linear('prior.2', gen.prior1) + _linear('din', gen.din) + _linear('dz', gen.dz)
    P += _transformer('dtr', gen.tr) + _linear('dout', gen.dout)
    P += [('lsdisc', gen.lsdisc, None, None), ('lscr', gen.lscr, None, None), ('lrho', gen.lrho, None, None)]
    P += [('lsHc', gen.lsHc, None, None)] if gen.height == 't' else [('lsH', gen.lsH, None, None)]
    return P


def from_torch(tm, rngs=None) -> NIMIWAE70:
    """a Flax NIMIWAE70 holding the weights of the PyTorch nimiwae70.NIMIWAE70 `tm` (same architecture, built from its attributes)"""
    assert tm.censor == 'known' and tm.growth == 'flex' and not tm.noise and not tm.prior_ctx, 'the JAX port covers the D70 configuration only'
    sizes = (tm.st_q.e[0].num_embeddings - 1, tm.st_q.e[1].num_embeddings - 1, tm.st_q.e[2].num_embeddings - 1, tm.st_q.e[3].num_embeddings - 1, tm.st_q.e[6].num_embeddings - 1)
    flow_cfg = None
    if tm.qfam == 'flow':
        L = tm.flow.layers
        flow_cfg = dict(layers=len(L), hidden=L[0].net[0].out_features, bins=L[0].K, bound=L[0].B)
    m = NIMIWAE70(sizes, age=tm.age, qfam=tm.qfam, flow_cfg=flow_cfg, height=tm.height, rngs=rngs or nnx.Rngs(0))
    if tm.qfam == 'flow':                                              # the coupling masks are structure: they must agree with PyTorch's buffers
        for Lj, Lt in zip(m.encoder.flow.layers, tm.flow.layers):
            assert Lj.ib == tuple(int(i) for i in torch.nonzero(Lt.mask).flatten()), 'coupling masks differ'
    sd = {k: v.detach().cpu().numpy() for k, v in tm.state_dict().items()}
    used = set()
    for name, var, to_j, _ in pairs(m):
        a = sd[name]; a = to_j(a) if to_j else a
        assert tuple(a.shape) == tuple(var.value.shape), f'{name}: {a.shape} vs {var.value.shape}'
        var.value = jnp.asarray(a, jnp.float32); used.add(name)
    params = {k for k, _ in tm.named_parameters()}
    missing = params - used - ({'lsH'} if tm.height == 't' else set())
    assert not missing, f'PyTorch parameters without a Flax twin: {sorted(missing)}'
    return m


def torch_layout(m: NIMIWAE70):
    """the model's Flax variables as {torch name: numpy array in PyTorch's layout} (fused in-projections reassembled)"""
    out, fused = {}, {}
    for name, var, _, to_t in pairs(m):
        a = np.asarray(var.value)
        if isinstance(to_t, tuple):                                    # one third of a fused in-projection: collect, assemble below
            kind, j = to_t; fused.setdefault(name, {})[j] = a.reshape(a.shape[0], -1).T if kind == 'fused_w' else a.reshape(-1)
            continue
        out[name] = np.ascontiguousarray(to_t(a) if to_t else a)
    for name, parts in fused.items(): out[name] = np.concatenate([parts[j] for j in range(3)], 0)
    return out


def to_torch(m: NIMIWAE70, tm, params=None):
    """copy Flax weights (or the parameter State `params`, e.g. Schedule-Free's evaluation weights) into the PyTorch module `tm`; returns tm"""
    if params is not None: m = nnx.merge(nnx.graphdef(m), params, nnx.state(m, nnx.Not(nnx.Param)))
    sd = tm.state_dict()
    for name, a in torch_layout(m).items(): sd[name] = torch.from_numpy(a).to(sd[name].dtype)
    tm.load_state_dict(sd); return tm
