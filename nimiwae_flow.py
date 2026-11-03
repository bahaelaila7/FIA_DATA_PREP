"""nimiwae_flow.py — a conditional rational-quadratic spline coupling flow for the NIMIWAE proposal q(z | x) (DECISIONS.md D73).

Why this family (literature check, 2026-11-03):
  * IWAE weights need log q at the proposal's own samples, and DReG needs log q again with the parameters held fixed (Tucker et al. 2018,
    arXiv 1810.04152), i.e. an inverse pass. Coupling flows invert in one pass; inverse-autoregressive flows need one pass per dimension;
    continuous flows and flow matching need an ODE solve per density. Agrawal, Sheldon, Domke (NeurIPS 2020, arXiv 2006.10343) used a
    real-NVP coupling flow with importance weighting and DReG / STL for this reason, and found flows beat full-rank Gaussians by >= 1 nat on
    about half of 30 models.
  * Rational-quadratic splines (Durkan et al. 2019, arXiv 1906.04032) make each coupling step far more flexible than an affine one at
    the same analytic inverse; they are the default conditional flow of current posterior-estimation tools (sbi / zuko NSF).
  * Image-scale flows (TarFlow, Zhai et al., ICML 2025) are transformer autoregressive flows: built for thousands of dimensions, with a
    sequential inverse, so not suited to a 15-dimensional proposal that DReG must invert.

Design. z = (z0, z_rest). z0 (age) stays a diagonal Gaussian, independent of the rest, so the exact grid posterior of z0 (D70 am. 14)
keeps working: q(z) = N(z0) q(z_rest). z_rest = T(e; c), e ~ N(mu, sigma) (the encoder's diagonal Gaussian), T = L coupling layers with
alternating masks, each a rational-quadratic spline per transformed coordinate on [-B, B] with identity tails, its bin widths, heights
and interior slopes from an MLP of the untouched coordinates and the encoder's tree context c. The last MLP layer starts at zero, which
makes every spline the identity: at initialisation q is exactly the Gaussian proposal.
log q(z_rest) = log N(e; mu, sigma) - log |dT/de|."""
import math
import torch, torch.nn as nn, torch.nn.functional as F

MIN_W = MIN_H = MIN_D = 1e-3
D_ID = math.log(math.expm1(1.0 - MIN_D))          # softplus(D_ID) + MIN_D = 1: unit slopes, the identity spline


def rq_spline(x, uw, uh, ud, inverse=False, B=5.0):
    """Monotone rational-quadratic spline on [-B, B] with K bins, identity outside (Durkan et al. 2019, eqs. 4-8).
    x [...]; uw, uh [..., K]; ud [..., K - 1] (interior knot slopes). Returns (y, log |dy/dx|) elementwise; inverse=True maps y -> x and
    returns log |dx/dy|."""
    K = uw.shape[-1]
    w = MIN_W + (1 - MIN_W * K) * F.softmax(uw, -1)
    h = MIN_H + (1 - MIN_H * K) * F.softmax(uh, -1)
    cw = F.pad(torch.cumsum(w, -1), (1, 0)) * 2 * B - B; cw[..., -1] = B
    ch = F.pad(torch.cumsum(h, -1), (1, 0)) * 2 * B - B; ch[..., -1] = B
    w, h = cw[..., 1:] - cw[..., :-1], ch[..., 1:] - ch[..., :-1]
    d = F.pad(MIN_D + F.softplus(ud), (1, 1), value=1.0)                          # unit slope at both ends: continuous with the tails
    inside = (x > -B) & (x < B)
    xc = x.clamp(-B + 1e-6, B - 1e-6)
    knots = ch if inverse else cw
    k = (torch.searchsorted(knots[..., 1:].contiguous(), xc[..., None].contiguous()).clamp(max=K - 1))   # bin index [..., 1]
    g = lambda t: t.gather(-1, k)[..., 0]
    xk, wk, yk, hk, dk, dk1 = g(cw), g(w), g(ch), g(h), g(d[..., :-1]), g(d[..., 1:])
    s = hk / wk
    if not inverse:
        xi = (xc - xk) / wk; om = xi * (1 - xi)
        den = s + (dk1 + dk - 2 * s) * om
        y = yk + hk * (s * xi ** 2 + dk * om) / den
        ld = 2 * torch.log(s) + torch.log(dk1 * xi ** 2 + 2 * s * om + dk * (1 - xi) ** 2) - 2 * torch.log(den)
    else:
        dy = xc - yk; t = dk1 + dk - 2 * s
        a = hk * (s - dk) + dy * t; b = hk * dk - dy * t; c = -s * dy
        disc = (b ** 2 - 4 * a * c).clamp(min=0)
        xi = (2 * c) / (-b - torch.sqrt(disc))
        y = xi * wk + xk
        om = xi * (1 - xi); den = s + t * om
        ld = -(2 * torch.log(s) + torch.log(dk1 * xi ** 2 + 2 * s * om + dk * (1 - xi) ** 2) - 2 * torch.log(den))
    return torch.where(inside, y, x), torch.where(inside, ld, torch.zeros_like(ld))


class Coupling(nn.Module):
    """One coupling step: coordinates `mask` = 0 pass through and, with the context, set the spline of the coordinates mask = 1."""
    def __init__(s, dim, ctx, mask, hidden=64, bins=8, B=5.0):
        super().__init__()
        s.register_buffer('mask', mask.bool()); s.K, s.B = bins, B
        n_in, n_out = int((~s.mask).sum()), int(s.mask.sum())
        s.net = nn.Sequential(nn.Linear(n_in + ctx, hidden), nn.GELU(), nn.Linear(hidden, hidden), nn.GELU(), nn.Linear(hidden, n_out * (3 * bins - 1)))
        with torch.no_grad(): s.net[-1].weight.zero_(); s.net[-1].bias.zero_()      # identity at initialisation
    def forward(s, x, c, inverse=False):
        xa, xb = x[..., ~s.mask], x[..., s.mask]
        p = s.net(torch.cat([xa, c.expand(*xa.shape[:-1], c.shape[-1])], -1)).view(*xb.shape, 3 * s.K - 1)
        uw, uh, ud = p[..., :s.K], p[..., s.K:2 * s.K], p[..., 2 * s.K:] + D_ID
        yb, ld = rq_spline(xb, uw, uh, ud, inverse=inverse, B=s.B)
        y = x.clone(); y[..., s.mask] = yb
        return y, ld.sum(-1)


class CondFlow(nn.Module):
    """T(e; c) = layer_L o ... o layer_1. Masks cycle even / odd / low half / high half coordinates, so every coordinate is transformed
    given every other one within four layers. One entry point, forward(x, c, inverse), so torch.func.functional_call can run either
    direction with detached weights (the DReG density)."""
    def __init__(s, dim, ctx, layers=6, hidden=64, bins=8, B=5.0):
        super().__init__()
        idx = torch.arange(dim)
        masks = [idx % 2 == 0, idx % 2 == 1, idx < dim // 2, idx >= dim // 2]
        s.layers = nn.ModuleList([Coupling(dim, ctx, masks[i % 4], hidden, bins, B) for i in range(layers)])
    def forward(s, x, c, inverse=False):
        """inverse False: e -> z, log |dz/de|; True: z -> e, log |de/dz|"""
        ld = torch.zeros(x.shape[:-1], device=x.device)
        for L in (reversed(s.layers) if inverse else s.layers):
            x, l = L(x, c, inverse=inverse); ld = ld + l
        return x, ld
