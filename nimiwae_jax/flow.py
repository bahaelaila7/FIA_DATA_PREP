"""Conditional rational-quadratic spline coupling flow (D73), the JAX twin of nimiwae_flow.py.

The proposal for the non-age latents is z_rest = T(e_rest; c), with e drawn from the encoder's diagonal Gaussian and c the encoder's tree
token. T is a stack of coupling layers. Each layer leaves some coordinates untouched and passes the others through a monotone spline whose bin
widths, heights and knot slopes come from a small MLP of the untouched coordinates and c (Durkan et al. 2019, arXiv 1906.04032). Outside
[-B, B] every spline is the identity. The MLP's last layer starts at zero, so at initialisation T is exactly the identity."""
import math
import jax, jax.numpy as jnp
from flax import nnx

MIN_W = MIN_H = MIN_D = 1e-3                    # smallest bin width, bin height and knot slope (keeps the spline strictly monotone)
D_ID = math.log(math.expm1(1.0 - MIN_D))        # raw slope whose softplus + MIN_D is exactly 1: unit slopes, the identity spline


def rq_spline(x, uw, uh, ud, inverse: bool, bound: float):
    """Monotone rational-quadratic spline on [-bound, bound], identity outside (Durkan et al. 2019, eqs. 4-8).

    x: [...] points; uw, uh: [..., K] unnormalised bin widths and heights; ud: [..., K - 1] unnormalised interior knot slopes.
    Forward: returns y = g(x) and log |dg/dx|. inverse=True: x holds y values, returns g^-1(y) and log |dg^-1/dy|.
    """
    K = uw.shape[-1]
    pad_last = lambda a, before, after, value=0.0: jnp.pad(a, [(0, 0)] * (a.ndim - 1) + [(before, after)], constant_values=value)
    # bin widths / heights: softmax keeps them positive and summing to 1, MIN_W keeps every bin open; knots on [-bound, bound]
    w = MIN_W + (1 - MIN_W * K) * jax.nn.softmax(uw, -1)
    h = MIN_H + (1 - MIN_H * K) * jax.nn.softmax(uh, -1)
    cw = (pad_last(jnp.cumsum(w, -1), 1, 0) * 2 * bound - bound).at[..., -1].set(bound)     # x knots, the last pinned to bound exactly
    ch = (pad_last(jnp.cumsum(h, -1), 1, 0) * 2 * bound - bound).at[..., -1].set(bound)     # y knots
    w, h = jnp.diff(cw, axis=-1), jnp.diff(ch, axis=-1)
    # knot slopes: interior ones positive via softplus; the two end slopes are 1, so the spline joins the identity tails smoothly
    d = pad_last(MIN_D + jax.nn.softplus(ud), 1, 1, 1.0)
    inside = (x > -bound) & (x < bound)
    xc = jnp.clip(x, -bound + 1e-6, bound - 1e-6)          # evaluate on a clipped copy so outside points stay finite (then discarded)
    # bin index = number of right bin edges strictly below the point (torch.searchsorted, side='left', on the right edges)
    knots = ch if inverse else cw
    k = jnp.minimum(jnp.sum(knots[..., 1:] < xc[..., None], -1), K - 1)[..., None]
    at = lambda t: jnp.take_along_axis(t, k, -1)[..., 0]
    xk, wk, yk, hk, dk, dk1 = at(cw), at(w), at(ch), at(h), at(d[..., :-1]), at(d[..., 1:])
    s = hk / wk                                            # the bin's mean slope
    if not inverse:
        xi = (xc - xk) / wk; om = xi * (1 - xi)            # position inside the bin, in [0, 1]
        den = s + (dk1 + dk - 2 * s) * om
        y = yk + hk * (s * xi ** 2 + dk * om) / den
        ld = 2 * jnp.log(s) + jnp.log(dk1 * xi ** 2 + 2 * s * om + dk * (1 - xi) ** 2) - 2 * jnp.log(den)
    else:
        # invert the rational quadratic: a xi^2 + b xi + c = 0, the numerically stable root 2c / (-b - sqrt(b^2 - 4ac))
        dy = xc - yk; t = dk1 + dk - 2 * s
        a = hk * (s - dk) + dy * t; b = hk * dk - dy * t; c = -s * dy
        xi = (2 * c) / (-b - jnp.sqrt(jnp.maximum(b ** 2 - 4 * a * c, 0.0)))
        y = xi * wk + xk; om = xi * (1 - xi); den = s + t * om
        ld = -(2 * jnp.log(s) + jnp.log(dk1 * xi ** 2 + 2 * s * om + dk * (1 - xi) ** 2) - 2 * jnp.log(den))
    return jnp.where(inside, y, x), jnp.where(inside, ld, 0.0)


class Coupling(nnx.Module):
    """One coupling step: coordinates `mask = False` pass through and, with the context, set the splines of the coordinates `mask = True`.
    The index sets are static (tuples), so they are part of the module's structure, not its state."""

    def __init__(self, mask, ctx: int, hidden: int, bins: int, bound: float, *, rngs: nnx.Rngs):
        self.ib = tuple(int(i) for i, m in enumerate(mask) if m)          # transformed coordinates
        self.ia = tuple(int(i) for i, m in enumerate(mask) if not m)      # conditioning coordinates
        self.bins, self.bound = bins, bound
        n_out = len(self.ib) * (3 * bins - 1)                              # per transformed coordinate: K widths, K heights, K - 1 slopes
        self.l0 = nnx.Linear(len(self.ia) + ctx, hidden, rngs=rngs)
        self.l1 = nnx.Linear(hidden, hidden, rngs=rngs)
        self.l2 = nnx.Linear(hidden, n_out, kernel_init=nnx.initializers.zeros, bias_init=nnx.initializers.zeros, rngs=rngs)   # identity at start

    def __call__(self, x, c, inverse: bool = False):
        """x [..., dim], c [n, ctx] broadcast over leading sample axes -> (y [..., dim], log |det| [...])"""
        xa, xb = x[..., self.ia], x[..., self.ib]
        cond = jnp.concatenate([xa, jnp.broadcast_to(c, xa.shape[:-1] + c.shape[-1:])], -1)
        gelu = lambda v: jax.nn.gelu(v, approximate=False)                 # torch.nn.GELU default (exact erf form)
        p = self.l2(gelu(self.l1(gelu(self.l0(cond))))).reshape(xb.shape + (3 * self.bins - 1,))
        K = self.bins
        yb, ld = rq_spline(xb, p[..., :K], p[..., K:2 * K], p[..., 2 * K:] + D_ID, inverse, self.bound)
        return x.at[..., self.ib].set(yb), ld.sum(-1)


class CondFlow(nnx.Module):
    """T = layer_L o ... o layer_1. Masks cycle even / odd / low half / high half, so every coordinate is transformed given every other one
    within four layers (the same masks as nimiwae_flow.CondFlow, so PyTorch weights load layer for layer)."""

    def __init__(self, dim: int, ctx: int, layers: int = 6, hidden: int = 64, bins: int = 8, bound: float = 5.0, *, rngs: nnx.Rngs):
        idx = list(range(dim))
        masks = [[i % 2 == 0 for i in idx], [i % 2 == 1 for i in idx], [i < dim // 2 for i in idx], [i >= dim // 2 for i in idx]]
        self.layers = nnx.List([Coupling(masks[i % 4], ctx, hidden, bins, bound, rngs=rngs) for i in range(layers)])

    def __call__(self, x, c, inverse: bool = False):
        """forward: e -> z, log |dz/de|; inverse: z -> e, log |de/dz|"""
        ld = jnp.zeros(x.shape[:-1])
        for layer in (reversed(self.layers) if inverse else self.layers):
            x, l = layer(x, c, inverse=inverse); ld = ld + l
        return x, ld
