"""D49 stage 2 — NIMIWAE (not-missing-at-random importance-weighted autoencoder, Lim et al. 2021) for the phantom backfill.
Semantics (D49 + amendments; review fixes 1–7):
  * A tree is a sequence of slots on its subplot: real visits (DIA, HT, AGB observed) and its pre-debut phantom slots (missing because the
    tree was below the tally limit: 1 in on the microplot, 5 in on the subplot), plus one virtual BREAST-HEIGHT slot (fix 1).
  * Latent TRUE size paths, monotone by construction: log DIA s_t and log HT h_t at the last slot, integrated BACKWARDS by size-dependent
    growth ODE for height, x_t = x_{t+1} − softplus(r(z, c, age_t, ba_t, h_{t+1})) · e^{σ_p ε_t − σ_p²/2} · Δyears, ε_t ~ N(0, 1) (process
    noise per interval, sampled from its prior inside each importance sample; never decreasing forward in time); DIA = f(HT − 4.5 ft).
  * Lower anchor (fix 1): at the breast-height slot (the tree reaches 4.5 ft, A_BH = 2 yr softwood / 5 yr hardwood after establishment)
    log HT ~ N(log 4.5 ft, 0.15) and log DIA ~ N(log 0.1 in, 0.7): missingness alone only says "below the limit", not how far.
  * Ages (fix 2): a LEAK-FREE proxy from the tree's FIRST OBSERVED record only (train lookup: median curated age at a tree's first real
    record by major group × planted × 0.5-in DIA bin), propagated by elapsed time. Time origin = the first observed record (fix 3).
  * Observations: log DIA ~ N(s_t, σ_D); log HT ~ N(h_t, σ_H) where measured; log AGB ~ N(a_sp + b_sp s_t + c_sp h_t, σ_sp), b, c > 0
    (fix 6: FIA's DRYBIO is a function of DIA and HT, so the allometry is monotone in both); missingness P(tallied | DIA) =
    (1 − ε) σ(KAPPA (s_t − log thr)), a near-step at FIA's deterministic tally limit with a learned field-miss rate ε.
  * Bound: log p(x_o, m) ≥ E[log (1/K) Σ_k p(x_o | z_k) Π p(m_t | s_t^k) p(anchor | z_k) p(z_k) / q(z_k | x_o, m)]; paths are deterministic
    given z, so each importance sample is a monotone path.
  * Outputs (fix 4/5): posterior-mean log DIA / log HT / log AGB; linear-scale posterior mean AGB (for totals); predictive samples of
    log AGB (mixture over the weighted paths + allometry noise) for interval coverage. Capped at the first observed value.
  * Epoch selection on the VAL benchmark: minimise |median bias log AGB|; then one interval width factor w fitted on VAL so the 80 %
    interval covers 0.80 there, frozen into the checkpoint (D49 amendment 3). TEST scored once by nimiwae_bench.py.
Usage: python -I nimiwae_train.py --data DIR [--epochs N] [--k K]"""
import argparse, json, math, time
import numpy as np, pandas as pd, torch, torch.nn as nn, torch.nn.functional as F

T_MAX, DZ, YRS_S = 13, 4, 20.0
KAPPA = 20.0                                    # steepness of the tally step per log unit of DIA (FIA records DIA to 0.1 in)
A_BH = {'S': 2.0, 'H': 5.0}                     # years from establishment to breast height (softwood / hardwood; curation I6 rule)
LOG_BH_HT, LOG_BH_DIA = math.log(4.5), math.log(0.1)
K_ = ['statecd', 'unitcd', 'countycd', 'plot', 'subp', 'tree']

def age_table(seq_train: pd.DataFrame) -> pd.DataFrame:
    """Leak-free age proxy: median curated age at a tree's FIRST real record by (major group, planted, 0.5-in DIA bin), train only."""
    f = seq_train[seq_train.obs].sort_values(K_ + ['measdate']).groupby(K_, sort=False).head(1).copy()
    f['dbin'] = np.floor(f.dia.clip(0, 40) / 0.5).astype(int)
    t = f.groupby(['major_spgrpcd', 'planted', 'dbin']).age_calc.agg(['median', 'size']).reset_index()
    return t[t['size'] >= 20]

def proxy_age(first: pd.DataFrame, tab: pd.DataFrame) -> np.ndarray:
    f = first.copy(); f['dbin'] = np.floor(f.dia.clip(0, 40) / 0.5).astype(int)
    m = f.merge(tab, on=['major_spgrpcd', 'planted', 'dbin'], how='left')
    # fallback: nearest bin of the same group / planted
    out = m['median'].to_numpy(dtype=float).copy()
    miss = np.isnan(out)
    if miss.any():
        for i in np.where(miss)[0]:
            c = tab[(tab.major_spgrpcd == f.major_spgrpcd.iat[i]) & (tab.planted == f.planted.iat[i])]
            if len(c) == 0: c = tab
            out[i] = c.iloc[np.argmin(np.abs(c.dbin.to_numpy() - f.dbin.iat[i]))]['median']
    return out

def build(seq: pd.DataFrame, vocab=None, atab=None):
    """Padded per-tree tensors with the virtual breast-height slot. Slots in time order, padding at the end."""
    seq = seq.sort_values(K_ + ['measdate']).copy()
    if vocab is None:
        vocab = {c: {v: i + 1 for i, v in enumerate(sorted(seq[c].dropna().unique()))} for c in ('spgrpcd', 'epa_l3', 'major_spgrpcd')}
    seq['t'] = (pd.to_datetime(seq['measdate']) - pd.Timestamp('1970-01-01')) / pd.Timedelta(days=1) / 365.25   # unit-safe (ms / ns dates)
    first = seq[seq.obs].groupby(K_, sort=False).head(1).set_index(K_)
    seq = seq.join(first[['t', 'dia', 'major_spgrpcd', 'planted']].rename(columns=lambda c: c + '_f'), on=K_)
    seq = seq[seq['t_f'].notna()]
    fr = first.reset_index(); fr['age_f'] = proxy_age(fr, atab); fr['abh'] = fr.sftwd_hrdwd.map(A_BH).fillna(4.0)
    seq = seq.merge(fr[K_ + ['age_f', 'abh']], on=K_, how='left')
    seq['rel'] = seq.t - seq.t_f; seq['age'] = seq.age_f + seq.rel; seq['bh'] = False
    bh = fr[K_ + ['age_f', 'abh']].copy(); bh['rel'] = -(bh.age_f - bh.abh).clip(lower=0.5); bh['age'] = bh.abh; bh['bh'] = True
    st0 = seq.groupby(K_, sort=False).head(1)[K_ + ['spgrpcd', 'major_spgrpcd', 'epa_l3', 'planted', 'thr_in', 'sftwd_hrdwd']]
    bh = bh.merge(st0, on=K_, how='left')
    for c, v in (('obs', False), ('dia', np.nan), ('ht', np.nan), ('drybio_g', np.nan), ('sub_ba', 0.0)): bh[c] = v
    if 'hidden' in seq: bh['hidden'] = False
    seq = seq.sort_values(K_ + ['rel'])      # (review 2: no time-placed breast-height slot; the lower anchor is the D(H) relation)
    g = seq.groupby(K_, sort=False); tid = g.ngroup().to_numpy(); pos = g.cumcount().to_numpy(); keep = pos < T_MAX
    n = int(tid.max()) + 1
    def mat(v, fill=0.0):
        out = np.full((n, T_MAX), fill, np.float32); out[tid[keep], pos[keep]] = np.asarray(v, np.float32)[keep]; return out
    obs = seq.obs.to_numpy(bool); hv = obs & seq.ht.notna().to_numpy()
    X = dict(lD=mat(np.log(seq.dia.fillna(1.0).clip(lower=0.05))), lH=mat(np.log(seq.ht.fillna(4.5).clip(lower=1.0))),
             lA=mat(np.log(seq.drybio_g.fillna(1.0).clip(lower=0.1))), m=mat(obs), mH=mat(hv), hasH=mat(seq.ht.notna().to_numpy()), v=mat(np.ones(len(seq))),
             bh=mat(seq.bh.to_numpy(float)), age=mat(seq.age.to_numpy() / 50), yrs=mat(seq.rel.to_numpy() / YRS_S),
             ba=mat(np.log1p(seq.sub_ba.fillna(0).to_numpy()) / 5),
             hid=mat(seq.hidden.fillna(False).to_numpy(float)) if 'hidden' in seq else None,
             thr_slot=mat(np.log(seq.thr_in.to_numpy())))
    st = g.head(1)
    def ids(c): return st[c].map(vocab[c]).fillna(0).astype(int).to_numpy()
    S = dict(sp=ids('spgrpcd'), l3=ids('epa_l3'), maj=ids('major_spgrpcd'), pl=st.planted.fillna(False).astype(float).to_numpy(),
             lthr=np.log(st.thr_in.to_numpy().astype(float)))
    T = {k: torch.tensor(v, dtype=torch.long if k in ('sp', 'l3', 'maj') else torch.float32) for k, v in {**X, **S}.items() if v is not None}
    return T, vocab, seq.assign(_tid=tid, _pos=pos)

class NIMIWAE(nn.Module):
    def __init__(s, nsp, nl3, nmaj, h=64):
        super().__init__()
        s.esp, s.el3, s.emaj = nn.Embedding(nsp + 1, 8), nn.Embedding(nl3 + 1, 4), nn.Embedding(nmaj + 1, 2)
        ns = 8 + 4 + 2 + 2
        s.phi = nn.Sequential(nn.Linear(8, h), nn.GELU(), nn.Linear(h, h))
        s.enc = nn.Sequential(nn.Linear(2 * h + ns, h), nn.GELU(), nn.Linear(h, 2 * DZ))
        s.lev = nn.Sequential(nn.Linear(DZ + ns + 2, h), nn.GELU(), nn.Linear(h, 1))          # log HT (ft) at the last slot
        s.rate = nn.Sequential(nn.Linear(DZ + ns + 3, h), nn.GELU(), nn.Linear(h, 1))         # growth per year of log HT (size-dependent)
        nn.init.constant_(s.rate[-1].bias, -2.5); nn.init.constant_(s.lev[-1].bias, 3.0)
        s.dc, s.db = nn.Embedding(nsp + 1, 1), nn.Embedding(nsp + 1, 1)                     # DIA = e^{dc + zeta(z)} (HT - 4.5)_+^{softplus(db)} + 0.01
        nn.init.constant_(s.dc.weight, -2.1); nn.init.constant_(s.db.weight, 0.55); s.zeta = nn.Linear(DZ, 1)
        s.a, s.b, s.c, s.ls = (nn.Embedding(nsp + 1, 1) for _ in range(4))
        nn.init.constant_(s.a.weight, 3.0); nn.init.constant_(s.b.weight, 1.8); nn.init.constant_(s.c.weight, 0.3); nn.init.constant_(s.ls.weight, -1.5)
        s.lsD, s.lsH = nn.Parameter(torch.tensor(-2.3)), nn.Parameter(torch.tensor(-2.0))
        s.leps = nn.Parameter(torch.tensor(-4.0))                                              # logit of the field-miss rate ε
        s.lsP = nn.Parameter(torch.tensor(math.log(0.3)))                                       # log sd of the growth process noise
    def static(s, B): return torch.cat([s.esp(B['sp']), s.el3(B['l3']), s.emaj(B['maj']), B['pl'][:, None], B['lthr'][:, None]], -1)
    def q(s, B, st):
        m = B['m']; x = torch.stack([B['lD'] * m, B['lH'] * B['mH'], B['lA'] * m, m, B['mH'], B['age'], B['yrs'], B['ba']], -1)
        h = s.phi(x); mm = m[..., None]
        mean = (h * mm).sum(1) / mm.sum(1).clamp(min=1); mx = (h + (mm - 1) * 1e4).max(1).values
        o = s.enc(torch.cat([mean, mx, st], -1)); return o[:, :DZ], o[:, DZ:].clamp(-6, 3)
    def paths(s, z, B, st):
        """Monotone height path (log HT, ft) integrated BACKWARDS from the last valid slot by a size-dependent growth ODE; DIA follows
        from height above breast height, DIA = e^{c_sp + zeta(z)} (HT - 4.5 ft)_+^{b_sp} + 0.01 in: monotone in HT (so in time) and ~0
        at breast height, which is the physical lower anchor (review fix 1)."""
        K, n, T = z.shape[0], z.shape[1], B['v'].shape[1]
        base = torch.cat([z[:, :, None].expand(K, n, T, DZ), st[None, :, None].expand(K, n, T, st.shape[-1]),
                          torch.stack([B['age'], B['ba']], -1)[None].expand(K, n, T, 2)], -1)
        v = B['v']; L = (v.sum(1).long() - 1).clamp(min=0)
        lev = s.lev(base)[..., 0].gather(2, L[None, :, None].expand(K, n, 1))[..., 0]         # [K, n]
        dy = ((B['yrs'][:, 1:] - B['yrs'][:, :-1]) * YRS_S).clamp(min=0) * v[:, 1:]
        out = [None] * T; cur = lev; out[T - 1] = cur
        for t in range(T - 2, -1, -1):
            r = F.softplus(s.rate(torch.cat([base[:, :, t], cur[..., None] / 4], -1))[..., 0])
            sp_ = torch.exp(s.lsP)                                                            # process noise: log-normal factor per interval
            fac = torch.exp(sp_ * torch.randn_like(r) - 0.5 * sp_ ** 2)                       # (mean 1; increments stay >= 0, the path monotone)
            cur = cur - r * fac * dy[None, :, t]; out[t] = cur
        sH = torch.stack(out, 2)
        exc = (torch.exp(sH) - 4.5).clamp(min=0)
        c = s.dc(B['sp'])[:, 0][None, :, None] + s.zeta(z)                                     # [K, n, 1]
        sD = torch.log(torch.exp(c) * exc ** F.softplus(s.db(B['sp'])[:, 0])[None, :, None] + 0.01)
        return sD, sH
    def allom(s, sD, sH, B):
        a, b, c = s.a(B['sp'])[:, 0], F.softplus(s.b(B['sp'])[:, 0]), F.softplus(s.c(B['sp'])[:, 0])
        return a[None, :, None] + b[None, :, None] * sD + c[None, :, None] * sH, s.ls(B['sp'])[:, 0][None, :, None].expand_as(sD)
    def logw(s, B, K):
        st = s.static(B); mu, lsd = s.q(B, st)
        z = mu + torch.exp(lsd) * torch.randn(K, *mu.shape)
        sD, sH = s.paths(z, B, st); muA, lsA = s.allom(sD, sH, B)
        m, mH, v, bh = B['m'], B['mH'], B['v'], B['bh']
        nl = lambda x, mu_, ls_: -0.5 * ((x - mu_) / torch.exp(ls_)) ** 2 - ls_ - 0.5 * math.log(2 * math.pi)
        lpo = ((nl(B['lD'][None], sD, s.lsD) + nl(B['lA'][None], muA, lsA)) * m * v).sum(-1) + (nl(B['lH'][None], sH, s.lsH) * mH * v).sum(-1)
        lpa = 0.0                                                                             # (anchor = the D(H) relation)
        mv = v * (1 - bh)
        # FIA's tally rule is deterministic: P(tallied | DIA) = (1 − ε) σ(KAPPA (log DIA − log limit)), steepness fixed, field-miss rate ε
        # learned; a missing slot's likelihood is flat below the limit (no pressure to impute ever-smaller trees)
        p_obs = torch.sigmoid(-s.leps) * torch.sigmoid(KAPPA * (sD - B['thr_slot'][None]))
        lpm = ((m * torch.log(p_obs.clamp(min=1e-12)) + (1 - m) * torch.log((1 - p_obs).clamp(min=1e-12))) * mv).sum(-1)
        lpz = (-0.5 * z ** 2 - 0.5 * math.log(2 * math.pi)).sum(-1)
        lqz = (-0.5 * ((z - mu) / torch.exp(lsd)) ** 2 - lsd - 0.5 * math.log(2 * math.pi)).sum(-1)
        return lpo + lpa + lpm + lpz - lqz, sD, sH, muA, lsA

def batches(n, bs, rng):
    p = rng.permutation(n)
    for i in range(0, n, bs): yield torch.tensor(p[i:i + bs])

def sub(B, ix): return {k: v[ix] for k, v in B.items()}

@torch.no_grad()
def impute(model, B, K=100, bs=2048, nsamp=200, w_int=1.0):
    """Posterior summaries per slot: mean log DIA / log HT / log AGB, linear mean AGB, and 10 % / 90 % predictive quantiles of log AGB.
    Capped at the tree's first observed value (the rule's cap)."""
    res = {k: [] for k in ('lD', 'lH', 'lA', 'A', 'q10', 'q90')}
    for i in range(0, B['m'].shape[0], bs):
        b = sub(B, slice(i, i + bs)); lw, sD, sH, muA, lsA = model.logw(b, K)
        w = torch.softmax(lw, 0)
        f = torch.argmax(b['m'], 1)
        cap = lambda x, X: torch.minimum(x, X.gather(1, f[:, None]))
        eD = (w[..., None] * sD).sum(0); eH = (w[..., None] * sH).sum(0); eA = (w[..., None] * muA).sum(0)
        A = (w[..., None] * torch.exp(muA + 0.5 * torch.exp(lsA) ** 2)).sum(0)
        ks = torch.multinomial(w.T, nsamp, replacement=True)                                    # [n, nsamp] sample indices per tree
        n_ = ks.shape[0]; ar = torch.arange(n_)[:, None]
        smu = muA.permute(1, 0, 2)[ar, ks]; ssd = torch.exp(lsA.permute(1, 0, 2)[ar, ks])        # [n, nsamp, T]
        draw = smu + ssd * torch.randn_like(smu)
        draw = eA[:, None] + w_int * (draw - eA[:, None])                                       # interval width factor (fitted on VAL, D49 am. 3)
        q = torch.quantile(draw, torch.tensor([0.1, 0.9]), dim=1)                              # [2, n, T]
        res['lD'].append(cap(eD, b['lD'])); res['lH'].append(cap(eH, b['lH'])); res['lA'].append(cap(eA, b['lA']))
        res['A'].append(torch.minimum(A, torch.exp(b['lA'].gather(1, f[:, None])))); res['q10'].append(q[0]); res['q90'].append(q[1])
    return {k: torch.cat(v) for k, v in res.items()}

def monotone_violations(lX, B, tol=1e-6):
    """Imputed slots up to and including the first observed one must be non-decreasing (P1)."""
    v = B['v'] * (1 - B['bh']); f = torch.argmax(B['m'], 1); t = torch.arange(v.shape[1])[None]
    pre = (t <= f[:, None]) & (v > 0)
    d = lX[:, 1:] - lX[:, :-1]; pair = pre[:, 1:] & pre[:, :-1]
    return int(((d < -tol) & pair).any(1).sum())

def bench_eval(model, data, split, atab, K=100, w_int=1.0):
    bb = pd.read_parquet(f'{data}/bench.parquet'); bb = bb[bb.split == split].copy()
    bb['obs'] = ~bb['hidden']; bb['thr_in'] = 3.0
    B, _, seq = build(bb, model.vocab, atab)
    R = impute(model, B, K, w_int=w_int)
    hid = B['hid'].bool()
    eA = (R['lA'] - B['lA'])[hid].numpy(); eD = (R['lD'] - B['lD'])[hid].numpy()
    cov = ((B['lA'] >= R['q10']) & (B['lA'] <= R['q90']))[hid].float().numpy()
    hh = hid & B['hasH'].bool(); eH = (R['lH'] - B['lH'])[hh].numpy()
    with torch.no_grad():
        muT, _ = model.allom(B['lD'][None], B['lH'][None], B)                                  # allometry at the TRUE size
    eAl = (muT[0] - B['lA'])[hh].numpy()
    return dict(mae_lA=float(np.abs(eA).mean()), bias_lA=float(np.median(eA)), bias_lD=float(np.median(eD)), bias_lH=float(np.median(eH)),
                bias_allom_true_size=float(np.median(eAl)), cov80=float(cov.mean()),
                viol=(monotone_violations(R['lD'], B), monotone_violations(R['lH'], B), monotone_violations(R['lA'], B))), B, R, seq

def fit_width(model, data, atab, grid=np.arange(1.0, 12.01, 0.05)):
    """Smallest interval width factor whose VAL 80 % coverage reaches 0.80 (split-conformal style, D49 amendment 3)."""
    torch.manual_seed(7)
    for w in grid:
        r, *_ = bench_eval(model, data, 'val', atab, w_int=float(w))
        if r['cov80'] >= 0.80: return float(w)
    return float(grid[-1])

def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--data', required=True); ap.add_argument('--epochs', type=int, default=6)
    ap.add_argument('--k', type=int, default=8); ap.add_argument('--bs', type=int, default=1024); ap.add_argument('--seed', type=int, default=0)
    ap.add_argument('--threads', type=int, default=6); a = ap.parse_args()
    torch.manual_seed(a.seed); torch.set_num_threads(a.threads); rng = np.random.default_rng(a.seed)
    seq = pd.read_parquet(f'{a.data}/seq.parquet'); tr = seq[seq.split == 'train']
    atab = age_table(tr); atab.to_parquet(f'{a.data}/age_proxy_table.parquet')
    B, vocab, _ = build(tr, None, atab); n = B['m'].shape[0]
    model = NIMIWAE(len(vocab['spgrpcd']), len(vocab['epa_l3']), len(vocab['major_spgrpcd'])); model.vocab = vocab
    opt = torch.optim.Adam(model.parameters(), lr=2e-3)
    print(f'train trees {n:,}; slots {int(B["v"].sum()):,}, observed {int(B["m"].sum()):,}, height obs {int(B["mH"].sum()):,}', flush=True)
    best, hist = (1e9, None), []
    for ep in range(a.epochs):
        t0, tot, nb = time.time(), 0.0, 0
        for ix in batches(n, a.bs, rng):
            b = sub(B, ix); lw, *_ = model.logw(b, a.k)
            loss = -(torch.logsumexp(lw, 0) - math.log(a.k)).mean()
            opt.zero_grad(); loss.backward(); nn.utils.clip_grad_norm_(model.parameters(), 5.0); opt.step()
            tot += loss.item(); nb += 1
        r, *_ = bench_eval(model, a.data, 'val', atab)
        score = abs(r['bias_lA'])                                                             # (D49 am. 3: coverage is set by the VAL width factor)
        hist.append(dict(epoch=ep, loss=tot / nb, **{f'val_{k}': v for k, v in r.items()}, select=score, field_miss=float(torch.sigmoid(model.leps)), proc_sd=float(torch.exp(model.lsP))))
        print(json.dumps(hist[-1]), f'{time.time() - t0:.0f}s', flush=True)
        if score < best[0]:
            best = (score, ep); torch.save({'state': model.state_dict(), 'vocab': vocab, 'epoch': ep}, f'{a.data}/nimiwae.pt')
    json.dump(dict(hist=hist, best_epoch=best[1], best_select=best[0], args=vars(a)), open(f'{a.data}/train_log.json', 'w'), indent=1)
    print(f'best epoch {best[1]} (|median bias log AGB| = {best[0]:.4f}) -> {a.data}/nimiwae.pt')
    ck = torch.load(f'{a.data}/nimiwae.pt', weights_only=False); model.load_state_dict(ck['state'])
    w = fit_width(model, a.data, atab); ck['w_int'] = w; torch.save(ck, f'{a.data}/nimiwae.pt')
    r, *_ = bench_eval(model, a.data, 'val', atab, w_int=w)
    print(f'VAL width factor w = {w:.2f} -> VAL 80 % coverage {r["cov80"]:.3f}; frozen into {a.data}/nimiwae.pt')

if __name__ == '__main__':
    main()
