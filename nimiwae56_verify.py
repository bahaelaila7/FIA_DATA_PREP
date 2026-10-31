"""D56 verification (process noise + encoder 2; DECISIONS.md D56), extending the D55 one: the (e, f) sum and posterior vs a float64 brute force from the written spec, for every censor x growth variant; the
structured growth recurrence recomputed independently; existence = size step; lineage coherence; gradient routing. Reuses the D54 data /
benchmark checks (nimiwae54_verify parts A, B, F, G, H, K) through the shared build.
Usage: python -I nimiwae56_verify.py --data DIR"""
import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import argparse, math
import numpy as np, pandas as pd, torch, torch.nn.functional as F
import nimiwae56 as M
import nimiwae55 as M55

RES = []
def check(name, ok, detail=''):
    RES.append((name, bool(ok))); print(f"{'PASS' if ok else 'FAIL'}  {name}  {detail}", flush=True)

def brute(model, b, D, H, lend):
    """log p(x_obs, ev | z) and the posterior over (e, f) by explicit loops, float64."""
    sig = lambda x: 1 / (1 + np.exp(-x)); lsg = lambda u: -np.logaddexp(0, -u)
    lN = lambda x, mu, sd: -0.5 * ((x - mu) / sd) ** 2 - math.log(sd) - 0.5 * math.log(2 * math.pi)
    rho = float(M.RHO_MAX * torch.sigmoid(model.lrho)); sdisc = float(torch.exp(model.lsdisc)); sH = float(torch.exp(model.lsH))
    D, H, lend = D.double().numpy(), H.double().numpy(), lend.double().numpy()
    T = M.T_MAX; n = D.shape[0]; lpx = np.zeros(n); post = {}
    for i in range(n):
        nv = int(b['v'][i].sum()); l = int(b['l'][i]); f = int(b['f'][i]); ev = b['ev'][i].numpy(); mode = b['mode'][i].numpy()
        real = b['real'][i].numpy(); smp = b['sampled'][i].numpy(); mD = b['mD'][i].numpy(); mH = b['mH'][i].numpy()
        lD = b['lD'][i].double().numpy(); lH = b['lH'][i].double().numpy(); llim = b['llim'][i].double().numpy(); lex = float(b['lex'][i, 0])
        known = int(b['ndef'][i]) == 1 and int(b['moved'][i]) == 0; cause = int(b['cause'][i]); moved = int(b['moved'][i]) == 1
        ex_slots = [t for t in range(nv) if math.log(max(D[i, t], 1e-3)) > lex]
        a = min(ex_slots[0] if ex_slots else f, f)
        terms = {}
        for e in list(range(l + 1, nv)) + [T]:
            end = e if e < T else nv
            for fc in ((0, 1) if e < T else (0,)):
                if e < T and moved and fc == 1: continue
                if e < T and known and fc != cause: continue
                lpe = sum(lend[i, v, 0] for v in range(a + 1, end)) + ((0.0 if moved else lend[i, e, 1 + fc]) if e < T else 0.0)
                ll = 0.0
                for t in range(nv):
                    lDp = math.log(max(D[i, t], 1e-3)); lHp = math.log(max(H[i, t], 1.0))
                    xe = M.KAPPA * (lDp - lex); lp1, lp0 = lsg(xe), lsg(-xe)
                    if t < end:
                        if mD[t]: ll += lN(lD[t], lDp, math.sqrt(0.02 ** 2 + (0.06 / math.exp(lD[t])) ** 2 + sdisc ** 2))
                        if mH[t]: ll += lN(lH[t], lHp, sH)
                        evt = int(ev[t])
                        ll += {M.EV['true_hard']: lp1, M.EV['false_hard']: lp0,
                               M.EV['true_soft']: np.logaddexp(lp1 + math.log(1 - rho), lp0 + math.log(rho)),
                               M.EV['false_soft']: np.logaddexp(lp0 + math.log(1 - rho), lp1 + math.log(rho))}.get(evt, 0.0)
                        if model.censor == 'known':
                            xt = M.KAPPA * (lDp - llim[t])
                            if real[t]: ll += lsg(xt)
                            elif smp[t]:
                                mo = int(mode[t])
                                if mo == M.MODE['cutoff'] and (llim[t] > lex + 1e-6 or t > l): ll += lsg(-xt)
                                elif mo == M.MODE['missed']: ll += lsg(xt)
                                elif mo == M.MODE['interval']: ll += lsg(-xt)
                    else:
                        if real[t]: ll += -np.inf
                        evt = int(ev[t])
                        ll += {M.EV['true_hard']: -np.inf, M.EV['true_soft']: math.log(rho), M.EV['false_soft']: math.log(1 - rho)}.get(evt, 0.0)
                terms[(e, fc)] = lpe + ll
        vals = np.array(list(terms.values())); fin = vals[np.isfinite(vals)]
        if len(fin) == 0: lpx[i] = -np.inf; post[i] = {}; continue
        mx = fin.max(); lp_ = mx + math.log(np.exp(vals - mx).sum()); lpx[i] = lp_
        post[i] = {k: math.exp(v_ - lp_) for k, v_ in terms.items() if np.isfinite(v_)}
    return lpx, post

def run(seq, censor, growth):
    tag = f'{censor}+{growth}+noise'
    va = seq[seq.split == 'val']; x = va[va.plot_key.isin(va.plot_key.drop_duplicates().sample(50, random_state=5))]
    mix = pd.concat([x, M.bench_sizes(va[va.plot_key.isin(va.plot_key.drop_duplicates().sample(1500, random_state=6))], None, True).pipe(lambda d: d[d[M.K_].apply(tuple, axis=1).isin(set(map(tuple, d[M.K_].drop_duplicates().head(30).values)))]),
                     M.bench_end(va[va.plot_key.isin(va.plot_key.drop_duplicates().sample(400, random_state=7))]).pipe(lambda d: d[d[M.K_].apply(tuple, axis=1).isin(set(map(tuple, d[M.K_].drop_duplicates().head(40).values)))])],
                    ignore_index=True).drop_duplicates(M.K_ + ['measdate'])
    tr = seq[seq.split == 'train']; vocab = M.build(tr[tr.plot_key.isin(tr.plot_key.drop_duplicates().sample(3000, random_state=1))])[1]   # whole plots
    torch.manual_seed(3); model = M.make_model(vocab, censor=censor, growth=growth, noise=True); model.eval()
    with torch.no_grad():                                                     # non-trivial encoder 2 and σ (the init is the prior)
        for p_ in model.u_net[-1].parameters(): p_.normal_(0, 0.05)
        model.lsu.fill_(math.log(0.4))
    B, _, sq = M.build(mix, vocab)
    torch.manual_seed(0)
    with torch.no_grad(): o = model.joint(B, 1)
    with torch.no_grad():
        mq, lq = model.q(B); hs = model._hs; z, u = o['z'], o['u']
        Dd, Hd, lend, crm, ccl, dml, brl, cll = model.decode(z, B, u)
        c2 = 0.5 * math.log(2 * math.pi)
        muu, lvu = model.qu(hs, z); vv = B['v'][None, :, :, None]
        lpu = ((-0.5 * u ** 2 - c2) * vv).sum((-1, -2)); lqu = ((-0.5 * (u - muu) ** 2 / torch.exp(lvu) - 0.5 * lvu - c2) * vv).sum((-1, -2))
        lrec = ((-0.5 * ((B['cr'][None] - crm) / torch.exp(model.lscr)) ** 2 - model.lscr - c2) * B['mcr'][None]).sum(-1)
        lrec = lrec + (torch.log_softmax(ccl, -1).gather(-1, (B['cc'].long().clamp(1, 5) - 1)[None, ..., None])[..., 0] * B['mcc'][None]).sum(-1)
        lrec = lrec + (-F.binary_cross_entropy_with_logits(dml, B['dmg'][None].expand_as(dml), reduction='none').sum(-1) * B['m_dmg'][None]).sum(-1)
        lrec = lrec + (-F.binary_cross_entropy_with_logits(brl, B['brk'][None].expand_as(brl), reduction='none') * B['m_brk'][None]).sum(-1)
        lrec = lrec + (torch.log_softmax(cll, -1).gather(-1, B['cl'][None, ..., None])[..., 0] * B['m_cl'][None]).sum(-1)
        mp, lp = model.p_z(B)
        lpz = (-0.5 * (z - mp) ** 2 / torch.exp(lp) - 0.5 * lp - c2).sum(-1); lqz = (-0.5 * (z - mq) ** 2 / torch.exp(lq) - 0.5 * lq - c2).sum(-1)
        lpx_code = (o['lw'] - lrec - lpz + lqz - lpu + lqu)[0].double().numpy()
    lpx_b, post_b = brute(model, B, Dd[0], Hd[0], lend[0])
    ok = np.isfinite(lpx_b) & (lpx_b > -5e3); err = np.abs(lpx_code - lpx_b)
    tol = np.maximum(1e-2, 2e-5 * np.abs(lpx_b))
    check(f'[{tag}] C1 (e, f) sum = brute force', bool((err[ok] <= tol[ok]).all()), f'max |Δ| {np.nanmax(err[ok]):.2e} median {np.median(err[ok]):.1e} over {ok.sum()} trees')
    lpost = o['lpost'][0].double().numpy(); worst = 0.0
    for i, d in post_b.items():
        if not ok[i]: continue
        for (e_, f_), p in d.items(): worst = max(worst, abs(math.exp(lpost[i, e_, f_]) - p))
    check(f'[{tag}] C2 posterior over (e, f) = brute force', worst < 5e-3, f'max |Δp| {worst:.2e}')
    # paths: monotone; struct recurrence recomputed
    Dn = Dd[0].numpy(); v = B['v'].numpy() > 0
    mono = bool((np.diff(Dn, axis=1)[v[:, 1:]] >= -1e-6).all())
    check(f'[{tag}] G1 latent DIA / HT paths never decrease', mono and bool((np.diff(Hd[0].numpy(), axis=1)[v[:, 1:]] >= -1e-6).all()), '')
    if growth == 'flex':
        with torch.no_grad():
            m_ = torch.exp(torch.exp(model.lsu) * u - 0.5 * torch.exp(model.lsu) ** 2)
            dec_D = torch.diff(torch.cat([torch.zeros_like(Dd[..., :1]), Dd], -1), dim=-1) / m_[..., 0]   # the decoder's softplus increment
        check(f'[{tag}] G3 flexible path: increment = softplus(decoder) · exp(σ_D u − σ_D²/2) (noise-free increment ≥ 0, multiplier recomputed)',
              bool((dec_D[B['v'][None] > 0] >= -1e-6).all()), '')
    if growth == 'struct':
        with torch.no_grad():
            tok = model.st_p(B); dt = (B['cov'][..., M.COV.index('dt')] * 10).clamp(min=0.5)
            cx = model.gc(torch.cat([B['cov'], tok[:, None].expand(-1, M.T_MAX, -1)], -1)); vg = model.gv(z)[0]; sp = model.gsp(B['spc'])
            d0 = F.softplus(model.g0(torch.cat([z[0], tok], -1)))
            Dr = [d0[:, 0]]; Hr = [d0[:, 1] + 1.0]
            for t in range(1, M.T_MAX):
                g = model.gs(torch.cat([torch.log(Dr[-1].clamp(min=1e-3))[:, None], torch.log(Hr[-1].clamp(min=1.0))[:, None], sp], -1))
                r = torch.exp((vg + g + cx[:, t]).clamp(-8, 3)) * torch.exp(torch.exp(model.lsu) * u[0, :, t] - 0.5 * torch.exp(model.lsu) ** 2); Dr.append(Dr[-1] + dt[:, t] * r[:, 0] * B['v'][:, t]); Hr.append(Hr[-1] + dt[:, t] * r[:, 1] * B['v'][:, t])
        err_s = float((torch.stack(Dr, -1) - Dd[0]).abs().max())
        check(f'[{tag}] G2 structured recurrence D_t = D_(t-1) + Δt·exp(vigour + g(size) + c(context))·exp(σu − σ²/2) recomputed', err_s < 1e-4, f'max |Δ| {err_s:.1e}')
    # existence = size step: P(exists) from impute equals Σ_k w Σ_e post · p1 · 1[t<e]
    torch.manual_seed(1); R = M.impute(model, B, K=1, bs=10 ** 7)
    torch.manual_seed(1)
    with torch.no_grad(): o2 = model.joint(B, 1)
    W = torch.exp(o2['lpost'])[0].sum(-1).double(); p1 = o2['p1'][0].double(); nvv = B['v'].sum(1).long()
    pe = torch.zeros_like(p1)
    for t in range(M.T_MAX):
        al = torch.tensor([[1.0 if (t < e or e == M.T_MAX) and t < int(nvv[i]) else 0.0 for e in range(M.T_MAX + 1)] for i in range(len(nvv))], dtype=torch.float64)
        pe[:, t] = (W * al).sum(-1) * p1[:, t]
    check(f'[{tag}] D1 P(exists) = Σ post(e) · P(DIA ≥ smallest level) · 1[alive]', float((pe - R['pE'].double()).abs().max()) < 1e-4, '')
    # lineages
    torch.manual_seed(2); L = M.sample_lineages(model, B, n_draws=6, K=30)
    ex = np.nan_to_num(L['exists'], nan=0); dia = L['dia']; bad = 0; badsz = 0
    for i in range(ex.shape[0]):
        nv = int(v[i].sum())
        o_ = [float(B['lD'][i, t]) for t in range(nv) if B['mD'][i, t] > 0]
        inc = not any(o_[j + 1] < o_[j] for j in range(len(o_) - 1))                              # own records decrease: kept by design
        for d_ in range(ex.shape[1]):
            on = np.where(ex[i, d_, :nv] > 0)[0]
            bad += bool(len(on) and (on.max() - on.min() + 1 != len(on)))
            dd = dia[i, d_, :nv][ex[i, d_, :nv] > 0]
            badsz += bool(inc and len(dd) > 1 and (np.diff(dd) < -1e-6).any())
    check(f'[{tag}] J1 lineages: one contiguous existence interval, non-decreasing DIA', bad == 0 and badsz == 0, f'{bad} / {badsz}')
    # gradient routing (DReG)
    model.train(); phi = model.enc_params(); ids = {id(p) for p in phi}; theta = [p for p in model.parameters() if id(p) not in ids]
    o3 = model.joint(sub(B, slice(0, 200)), 8, dreg=True)
    lt = -(torch.logsumexp(o3['lw'], 0) - math.log(8)).mean(); w = torch.softmax(o3['lw'], 0).detach(); lpp = -(w ** 2 * o3['lw_dreg']).sum(0).mean()
    gt = torch.autograd.grad(lt, theta, retain_graph=True, allow_unused=True); gp = torch.autograd.grad(lpp, phi, allow_unused=True)
    check(f'[{tag}] E1 finite gradients reach decoder (IWAE) and encoder (DReG)',
          all(g is None or torch.isfinite(g).all() for g in list(gt) + list(gp)) and sum(g is not None and g.abs().sum() > 0 for g in gt) > len(theta) // 2
          and sum(g is not None and g.abs().sum() > 0 for g in gp) > len(phi) // 2, '')
    b2 = sub(B, slice(0, 100)); mq2, lq2 = model.q(b2); hs2 = model._hs
    zf = (mq2 + torch.exp(0.5 * lq2) * torch.randn(4, *mq2.shape)).detach().requires_grad_(True)
    det = {k: v.detach() for k, v in model.u_net.named_parameters()}
    mud, lvd = model.qu(hs2.detach(), zf, params=det)
    uu = torch.randn_like(mud); dens = ((-0.5 * (uu - mud) ** 2 / torch.exp(lvd) - 0.5 * lvd)).sum()
    gz = torch.autograd.grad(dens, zf, retain_graph=True)[0]
    gw = torch.autograd.grad(dens, list(model.u_net.parameters()) + [p for p in model.enc_params() if p is not None], allow_unused=True)
    check(f'[{tag}] U1 DReG u-density: no gradient to encoder weights at fixed (z, u); pathwise gradient through z kept',
          all(g is None for g in gw) and float(gz.abs().sum()) > 0, '')

def mode_rules(seq):
    """B7: the missingness mode of every unrecorded slot recomputed from the written rules (independent of build)."""
    x = seq[seq.plot_key.isin(seq.plot_key.drop_duplicates().sample(300, random_state=4))]
    B, _, sq = M.build(x)
    ti, po = sq._tid.to_numpy(), sq._pos.to_numpy(); mode = B['mode'][ti, po].numpy(); f = B['f'].numpy()[ti]; l = B['l'].numpy()[ti]
    real = B['real'][ti, po].numpy() > 0; smp = B['sampled'][ti, po].numpy() > 0; bad = 0; ex_ = []
    for i, r in enumerate(sq.itertuples()):
        if real[i]: continue
        p = po[i]; rcd = r.rcd0
        if not smp[i]: want = 'ignore'
        elif f[i] < p < l[i]: want = 'ignore'
        elif p > l[i]: want = 'ignore' if r.terminal_fate in ('censored_procedural', 'censored_procedural_unrecorded', 'censored_shrank') else 'cutoff'
        elif rcd in (8, 9): want = 'ignore'
        elif rcd in (3, 6, 7): want = 'missed' if p == f[i] - 1 else 'ignore'
        else: want = 'cutoff'
        if M.MODE[want] != mode[i]:
            bad += 1
            if len(ex_) < 3: ex_.append(dict(pos=p, f=f[i], l=l[i], rcd=rcd, fate=r.terminal_fate, want=want, got=int(mode[i])))
    check('B7 missingness mode of every unrecorded slot follows the written rules (8/9: none; before a missed visit: none; after the record: informative unless procedural / shrank)',
          bad == 0, f'{bad} mismatches {ex_}')

def end_bench(seq):
    """B8 (D55 am. 3): censored-end instances keep the full frame and hide the tree's own information from j on — alive and gone
    instances must look identical in every tree-own input at every slot >= j, and their frames must extend equally."""
    va = seq[seq.split == 'val']; va = va[va.plot_key.isin(va.plot_key.drop_duplicates().sample(400, random_state=8))]
    be = M.bench_end(va); B, _, sq = M.build(be, None)
    ti, po = sq._tid.to_numpy(), sq._pos.to_numpy()
    j = pd.Series(po, index=ti)[sq.truth_E.notna().to_numpy()]; y = pd.Series(sq.truth_E.to_numpy(), index=ti).dropna()
    jj = np.full(B['v'].shape[0], -1); jj[j.index.to_numpy()] = j.to_numpy()
    yy = np.full(B['v'].shape[0], np.nan); yy[y.index.to_numpy()] = y.to_numpy()
    after = (torch.arange(M.T_MAX)[None] >= torch.tensor(jj)[:, None]) & (B['v'] > 0)
    rows, worst = [], 0.0
    for k_ in ('real', 'mD', 'mH', 'mcr', 'mcc', 'm_dmg', 'm_brk', 'm_cl', 'ev', 'mode'):
        T_ = B[k_].float()
        a_ = float(T_[after & torch.tensor(yy == 1)[:, None]].mean()); g_ = float(T_[after & torch.tensor(yy == 0)[:, None]].mean())
        rows.append(f'{k_}: {a_:.3f}/{g_:.3f}'); worst = max(worst, abs(a_ - g_))
    ok_ev = bool((B['ev'][after] == M.EV['none']).all()) and bool((B['mode'][after] == M.MODE['ignore']).all())
    check('B8 censored-end: no tree-own record / evidence / code at or after j; identical for alive vs gone', worst < 1e-6 and ok_ev, '; '.join(rows))
    nl = (B['v'].sum(1).numpy() - 1 - jj); a_, g_ = nl[yy == 1].mean(), nl[yy == 0].mean()
    check('B9 censored-end: full frame kept (slots after j exist) and similar for alive vs gone', nl.max() > 0 and abs(a_ - g_) < 0.5,
          f'mean slots after j: alive {a_:.2f} gone {g_:.2f}; share with later slots {float((nl > 0).mean()):.2f}')

def sub(B, ix): return {k: v[ix] for k, v in B.items()}

def noise_off(seq):
    """N1: with noise off, nimiwae56 with nimiwae55's weights reproduces the D55 log weights and posterior exactly."""
    va = seq[seq.split == 'val']; x = va[va.plot_key.isin(va.plot_key.drop_duplicates().sample(40, random_state=11))]
    tr = seq[seq.split == 'train']; vocab = M.build(tr[tr.plot_key.isin(tr.plot_key.drop_duplicates().sample(3000, random_state=1))])[1]
    for censor, growth in (('known', 'flex'), ('mar', 'struct')):
        torch.manual_seed(3); m55 = M55.make_model(vocab, censor=censor, growth=growth); m55.eval()
        m56 = M.make_model(vocab, censor=censor, growth=growth, noise=False); m56.load_state_dict(m55.state_dict(), strict=True); m56.eval()
        B, _, _ = M.build(x, vocab)
        with torch.no_grad():
            torch.manual_seed(0); a_ = m55.joint(B, 4); torch.manual_seed(0); b_ = m56.joint(B, 4)
        d = max(float((a_['lw'] - b_['lw']).abs().max()), float((torch.exp(a_['lpost']) - torch.exp(b_['lpost'])).abs().max()))
        check(f'[{censor}+{growth}] N1 noise off reproduces D55 exactly', d < 1e-5, f'max |Δ| {d:.1e}')

def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--data', required=True); a = ap.parse_args()
    torch.set_num_threads(4); seq = pd.read_parquet(f'{a.data}/seq.parquet')
    mode_rules(seq); end_bench(seq); noise_off(seq)
    for censor in ('known', 'mar'):
        for growth in ('flex', 'struct'): run(seq, censor, growth); outputs(seq, censor, growth)
    print(f"\n{sum(o for _, o in RES)}/{len(RES)} checks passed")


def outputs(seq, censor, growth):
    """Output-layer checks on the D56 impute / sample_lineages / stand_totals (bounds, monotone, undefined, intervals, expansion)."""
    tag = f'{censor}+{growth}'
    va = seq[seq.split == 'val']; x = va[va.plot_key.isin(va.plot_key.drop_duplicates().sample(30, random_state=9))]
    tr = seq[seq.split == 'train']; vocab = M.build(tr[tr.plot_key.isin(tr.plot_key.drop_duplicates().sample(3000, random_state=1))])[1]
    torch.manual_seed(3); model = M.make_model(vocab, censor=censor, growth=growth, noise=True); model.eval()
    B, _, sq = M.build(x, vocab); torch.manual_seed(0); R = M.impute(model, B, K=20, bs=10 ** 7)
    und = (B['v'] > 0) & torch.isnan(R['lD'])
    check(f'[{tag}] I0 undefined size only where P(exists) = 0; observed slots defined', bool((R['pE'][und] < 1e-6).all()) and bool((~torch.isnan(R['lD'][B['mD'] > 0])).all()), f'{int(und.sum())} undefined')
    v = (B['v'] > 0) & (B['mD'] == 0) & ~torch.isnan(R['lD'])
    inside = ((R['q10'] <= R['lD'] + 1e-6) & (R['lD'] <= R['q90'] + 1e-6))[v]
    check(f'[{tag}] I2 intervals bracket the point (imputed slots)', float(inside.float().mean()) > 0.99, f'{float(inside.float().mean()):.4f}')
    bad = 0; mono = 0
    for X, obs, m in ((torch.nan_to_num(R['lD'], nan=-50.0), B['lD'], B['mD']), (torch.nan_to_num(R['lH'], nan=-50.0), B['lH'], B['mH'])):
        for i in range(X.shape[0]):
            o_ = [(t, float(obs[i, t])) for t in range(M.T_MAX) if m[i, t] > 0]
            if any(o_[j + 1][1] < o_[j][1] for j in range(len(o_) - 1)): continue
            nv = int(B['v'][i].sum()); xs = [float(X[i, t]) for t in range(nv)]
            mono += any(xs[t + 1] < xs[t] - 1e-6 for t in range(nv - 1) if xs[t] > -49 and xs[t + 1] > -49)
            for t in range(nv):
                if m[i, t] > 0: bad += abs(xs[t] - float(obs[i, t])) > 1e-6; continue
                if xs[t] <= -49: continue
                later = [v_ for (u, v_) in o_ if u > t]; earlier = [v_ for (u, v_) in o_ if u < t]
                bad += (bool(later) and xs[t] > min(later) + 1e-6) or (bool(earlier) and xs[t] < max(earlier) - 1e-6)
    check(f'[{tag}] I3/I4 imputed DIA / HT within the tree\'s measurements, never decreasing, observed kept', bad == 0 and mono == 0, f'{bad} / {mono}')
    t = M.stand_totals(model, x, n_draws=5, K=20)
    check(f'[{tag}] K3 stand totals: mean within its 10-90 % band', bool(((t.tpa_sapling_mean >= t.tpa_sapling_q10 - 1e-9) & (t.tpa_sapling_mean <= t.tpa_sapling_q90 + 1e-9)).all()), f'{len(t)} subplot-visits')


if __name__ == '__main__':
    main()
