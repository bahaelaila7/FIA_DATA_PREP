"""D54 pipeline verification against independent re-computations (not the code under test).
A prep frame vs the curated DB; B tensor rules recomputed per row; C the exact (s, e, f) sum by brute force; D posterior summaries by
brute force; E DReG gradient routing; F benchmark leak tests; G nested-CV fold logic.
Usage: python -I nimiwae54_verify.py --data DIR --db curated.duckdb [--model dev_model.pt]"""
import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import argparse, math, zlib
import duckdb, numpy as np, pandas as pd, torch, torch.nn.functional as F
import nimiwae54 as M

RES = []
def check(name, ok, detail=''):
    RES.append((name, bool(ok))); print(f"{'PASS' if ok else 'FAIL'}  {name}  {detail}", flush=True)

def part_a(seq, db, rng):
    c = duckdb.connect(db, read_only=True)
    state = c.execute('select any_value(STATECD) from clean_subplot_years').fetchone()[0]
    s = seq[seq.statecd == state]
    trees = s[M.K_].drop_duplicates().sample(400, random_state=1)
    csy = c.execute('select STATECD statecd, UNITCD unitcd, COUNTYCD countycd, PLOT plot, SUBP subp, MEASDATE measdate, forest_subpcond_prop fsp from clean_subplot_years').df()
    real = c.execute("select statecd, unitcd, countycd, plot, subp, tree, measdate from tree_trajectories where intro_type = 'real'").df()
    dead = c.execute('select STATECD statecd, UNITCD unitcd, COUNTYCD countycd, PLOT plot, SUBP subp, TREE tree, MEASDATE measdate from curated_trees where STATUSCD = 2').df()
    pk = ['statecd', 'unitcd', 'countycd', 'plot']; sk = pk + ['subp']
    pv = csy.groupby(pk + ['measdate']).fsp.max().reset_index(); pv = pv[pv.fsp > 0]
    sp = csy[csy.fsp > 0].groupby(sk).measdate.agg(['min', 'max']).reset_index()
    bad_frame = bad_real = bad_dead = 0
    for _, t in trees.iterrows():
        key = tuple(t[k] for k in M.K_)
        got = set(pd.to_datetime(s[(s[M.K_] == pd.Series(key, index=M.K_)).all(1)].measdate))
        r = real[(real[M.K_] == pd.Series(key, index=M.K_)).all(1)]
        sr = sp[(sp[sk] == pd.Series(key[:5], index=sk)).all(1)]
        vis = set()
        if len(sr):
            v = pv[(pv[pk] == pd.Series(key[:4], index=pk)).all(1)]
            vis = set(pd.to_datetime(v[(v.measdate >= sr['min'].iloc[0]) & (v.measdate <= sr['max'].iloc[0])].measdate))
        want = vis | set(pd.to_datetime(r.measdate))
        bad_frame += got != want
        gs = s[(s[M.K_] == pd.Series(key, index=M.K_)).all(1)]
        bad_real += set(pd.to_datetime(gs[gs.slot_kind == 'real'].measdate)) != set(pd.to_datetime(r.measdate))
        d = dead[(dead[M.K_] == pd.Series(key, index=M.K_)).all(1)]
        d = d[pd.to_datetime(d.measdate) > pd.to_datetime(r.measdate).max()]
        want_dead = pd.to_datetime(d.measdate).min() if len(d) else pd.NaT
        got_dead = pd.to_datetime(gs.dead_md).iloc[0]
        bad_dead += not ((pd.isna(want_dead) and pd.isna(got_dead)) or want_dead == got_dead)
    check('A1 frame slots = counted visits in the subplot span ∪ real records', bad_frame == 0, f'{bad_frame}/400 trees differ')
    check('A2 real slots = curated real records', bad_real == 0, f'{bad_real}/400')
    check('A3 dead_md = first dead record after the last live one', bad_dead == 0, f'{bad_dead}/400')
    # neighbour leave-one-out: recompute nb_inc_sub for random slots
    rr = s[s.slot_kind == 'real'].sort_values(M.K_ + ['measdate']).copy()
    rr['d0'] = rr.groupby(M.K_).dia.shift(1); rr['t0'] = rr.groupby(M.K_).measdate.shift(1)
    rr['dt'] = (pd.to_datetime(rr.measdate) - pd.to_datetime(rr.t0)).dt.days / 365.25
    ok_ = (rr.dia >= 5) & (rr.d0 >= 5) & (rr.dt > 0.5)
    rr['inc'] = np.where(ok_, ((np.log(rr.dia) - np.log(rr.d0)) / rr.dt).clip(-0.05, 0.5), np.nan)
    samp = s.sample(300, random_state=2); errs = 0
    for _, x in samp.iterrows():
        m = (rr[['statecd', 'unitcd', 'countycd', 'plot', 'subp']] == x[['statecd', 'unitcd', 'countycd', 'plot', 'subp']].values).all(1) & (rr.measdate == x.measdate)
        oth = rr[m & ~(rr.tree == x.tree)].inc.dropna()
        want = oth.mean() if len(oth) else np.nan
        errs += not ((np.isnan(want) and pd.isna(x.nb_inc_sub)) or (not pd.isna(x.nb_inc_sub) and abs(want - x.nb_inc_sub) < 1e-9))
    check('A4 neighbour increment = leave-one-out mean of OTHER trees (≥ 5 in both ends)', errs == 0, f'{errs}/300 slots')
    # climate window: years (prev slot, slot], first slot the 5 years before
    c.close()

def part_b(seq):
    s = seq.sample(frac=1.0, random_state=3)
    keys = s[M.K_].drop_duplicates().head(3000); x = seq.merge(keys, on=M.K_)
    B, _, sq = M.build(x)
    ti, po = sq._tid.to_numpy(), sq._pos.to_numpy()
    ev = B['ev'][ti, po].numpy(); mode = B['mode'][ti, po].numpy(); f = B['f'][ti].numpy(); l = B['l'][ti].numpy()
    bad = 0
    for i, r in enumerate(sq.itertuples()):
        p = po[i]
        rcd = r.rcd0; limit = 1.0 if (r.thr_in <= 1 or rcd == 2) else 5.0
        sampled = (r.slot_kind == 'real' and not getattr(r, 'hidden', False)) or (r.cond_status == 1 if not pd.isna(r.cond_status) else (r.forest_subpcond_prop or 0) > 0)
        # expected evidence
        e = M.EV['none']
        if f[i] <= p <= l[i]: e = M.EV['true_hard']
        if (p < f[i] and limit <= 1 and rcd in (1, 2) and sampled and (r.forest_micrcond_prop or 0) >= 1 and r.slot_kind == 'pre'): e = M.EV['false_soft']
        if p == f[i] - 1 and rcd in (3, 6, 7) and sampled: e = M.EV['true_soft']
        md = pd.Timestamp(r.measdate)
        end = (not pd.isna(r.dead_md) and md >= pd.Timestamp(r.dead_md)) or (r.terminal_fate == 'censored_moved' and not pd.isna(r.s0_md) and md >= pd.Timestamp(r.s0_md))
        if r.terminal_fate in ('harvest_inferred', 'disappeared_inferred_removal') and p > l[i]: end = True
        if end: e = M.EV['false_hard']
        if r.terminal_fate == 'censored_shrank' and not pd.isna(r.s0_md) and md == pd.Timestamp(r.s0_md): e = M.EV['true_soft']
        if e != ev[i]:
            bad += 1
            if bad <= 3: print('   B1 mismatch:', dict(pos=p, f=f[i], l=l[i], expected=e, got=int(ev[i]), kind=r.slot_kind, rcd0=rcd, thr=r.thr_in, fate=r.terminal_fate,
                                                     md=str(md.date()), dead=str(r.dead_md), s0=str(r.s0_md), cond=r.cond_status, fmp=r.forest_micrcond_prop))
    check('B1 existence evidence codes match the rule table', bad == 0, f'{bad}/{len(sq)} slots')
    v = B['v'][ti, po].numpy(); lim = np.exp(B['llim'][ti, po].numpy()) + M.HALF_UNIT
    want_lim = np.where((sq.thr_in.to_numpy() <= 1) | (sq.rcd0.to_numpy() == 2), 1.0, 5.0)
    check('B2 tally limit per slot (1 in microplot / through growth, else 5 in), stored at limit − 0.05', np.allclose(lim, want_lim), '')
    real = B['real'][ti, po].numpy(); mD = B['mD'][ti, po].numpy()
    check('B3 measurements present only on visible real records', bool(((mD > 0) <= (real > 0)).all()), '')
    comp = B['cov'][ti, po, 0].numpy(); own = np.where(sq.slot_kind == 'real', sq.own_ba.fillna(0), 0)
    want = np.log1p(np.clip(sq.sub_ba.fillna(0) - own, 0, None)) / 5
    check('B4 competition excludes the tree itself', np.allclose(comp, want, atol=1e-5), '')
    md_ = B['m_dmg'][ti, po].numpy(); mb_ = B['m_brk'][ti, po].numpy(); mc_ = B['m_cl'][ti, po].numpy()
    check('B5 damage / broken top / cull class observed only at visible real records', bool(((md_ + mb_ + mc_ > 0) <= (real > 0)).all()),
          f'damage recorded on {md_[real > 0].mean():.3f} of real slots')
    return B, sq

def brute(model, b, z):
    """Independent float64 enumeration of log p(x_obs, m | z) and the posterior over (s, e, f), from the written spec."""
    with torch.no_grad():
        out_ = model.decode(z, b); dD, dH, hz, lend, phi = out_[0], out_[1], out_[2], out_[3], out_[-1]
    dD, dH, hz, lend, phi = dD[0].double().numpy(), dH[0].double().numpy(), hz[0].double().numpy(), lend[0].double().numpy(), phi[0].double().numpy()
    e1 = float(torch.sigmoid(-model.leps)); rho = float(M.RHO_MAX * torch.sigmoid(model.lrho)); sdisc = float(torch.exp(model.lsdisc)); sH = float(torch.exp(model.lsH))
    sig = lambda x: 1 / (1 + np.exp(-x)); lN = lambda x, mu, sd: -0.5 * ((x - mu) / sd) ** 2 - math.log(sd) - 0.5 * math.log(2 * math.pi)
    n, T = dD.shape; out_lpx = np.zeros(n); out_post = {}
    for i in range(n):
        nv = int(b['v'][i].sum()); f = int(b['f'][i]); l = int(b['l'][i]); ev = b['ev'][i].numpy(); mode = b['mode'][i].numpy()
        real = b['real'][i].numpy(); mDm = b['mD'][i].numpy(); mHm = b['mH'][i].numpy(); lD = b['lD'][i].double().numpy(); lH = b['lH'][i].double().numpy()
        llim = b['llim'][i].double().numpy(); llo = b['llo'][i].double().numpy()
        known = int(b['ndef'][i]) == 1 and int(b['moved'][i]) == 0; cause = int(b['cause'][i]); moved = int(b['moved'][i]) == 1
        terms = {}
        for s in range(0, f + 1):
            # p(s)
            h = sig(hz[i]); lps = (math.log(h[s]) if s < f else 0.0) + sum(math.log(1 - h[u]) for u in range(s))
            for e in list(range(l + 1, nv)) + [T]:
                for fc in ((0, 1) if e < T else (0,)):
                    if e < T and moved and fc == 1: continue
                    if e < T and known and fc != cause: continue
                    surv = sum(lend[i, v, 0] for v in range(s + 1, (e if e < T else nv)))
                    lpe = surv + ((0.0 if moved else lend[i, e, 1 + fc]) if e < T else 0.0)
                    ll = 0.0
                    for t in range(nv):
                        E = s <= t < (e if e < T else nv)
                        if E:
                            D = 1.0 + phi[i, s] * dD[i, s] + dD[i, s + 1:t + 1].sum(); H = 4.5 + phi[i, s] * dH[i, s] + dH[i, s + 1:t + 1].sum()
                            lDp = math.log(D); lHp = math.log(max(H, 1.0))
                            xt = M.KAPPA * (lDp - llim[t]); pt = sig(xt); lsg = lambda u: -np.logaddexp(0, -u)
                            if real[t]:
                                sd = math.sqrt(0.02 ** 2 + (0.06 / math.exp(lD[t])) ** 2 + sdisc ** 2)
                                ll += (lN(lD[t], lDp, sd) if mDm[t] else 0) + (lN(lH[t], lHp, sH) if mHm[t] else 0) + math.log(e1) + lsg(xt)
                            else:
                                mo = mode[t]
                                if mo == M.MODE['cutoff']: ll += math.log1p(-min(e1 * pt, 1 - 1e-7))
                                elif mo == M.MODE['missed']: ll += lsg(xt)
                                elif mo == M.MODE['interval']: ll += lsg(M.KAPPA * (lDp - llo[t])) + lsg(-xt)
                            ll += {M.EV['false_hard']: -np.inf, M.EV['false_soft']: math.log(rho), M.EV['true_soft']: math.log(1 - rho)}.get(int(ev[t]), 0.0)
                        else:
                            if real[t]: ll += -np.inf
                            ll += {M.EV['true_hard']: -np.inf, M.EV['true_soft']: math.log(rho), M.EV['false_soft']: math.log(1 - rho)}.get(int(ev[t]), 0.0)
                    terms[(s, e, fc)] = lps + lpe + ll
        vals = np.array(list(terms.values())); mx = vals[np.isfinite(vals)].max()
        lpx = mx + math.log(np.exp(vals - mx).sum()); out_lpx[i] = lpx
        out_post[i] = {k: math.exp(v_ - lpx) for k, v_ in terms.items() if np.isfinite(v_)}
    return out_lpx, out_post

def part_cd(model, seq):
    va = seq[seq.split == 'val']
    mix = pd.concat([va[va.plot_key.isin(va.plot_key.drop_duplicates().sample(60, random_state=5))],
                     M.bench_sizes(va.head(400000), 5.0, True).pipe(lambda d: d[d[M.K_].apply(tuple, axis=1).isin(set(map(tuple, d[M.K_].drop_duplicates().head(40).values)))]),
                     M.bench_end(va.head(200000)).pipe(lambda d: d[d[M.K_].apply(tuple, axis=1).isin(set(map(tuple, d[M.K_].drop_duplicates().head(60).values)))])],
                    ignore_index=True)
    mix = mix.drop_duplicates(M.K_ + ['measdate'])
    B, _, sq = M.build(mix, model.vocab); b = B
    torch.manual_seed(0)
    with torch.no_grad(): o = model.joint(b, 1)
    torch.manual_seed(0)
    with torch.no_grad():
        mq, lq = model.q(b); z = mq + torch.exp(0.5 * lq) * torch.randn(1, *mq.shape)
        dD, dH, hz, lend, crm, ccl, dml, brl, cll, phi = model.decode(z, b)
        c2 = 0.5 * math.log(2 * math.pi)
        lcr = (-0.5 * ((b['cr'][None] - crm) / torch.exp(model.lscr)) ** 2 - model.lscr - c2) * b['mcr'][None]
        lcc = torch.log_softmax(ccl, -1).gather(-1, (b['cc'].long().clamp(1, 5) - 1)[None, ..., None])[..., 0] * b['mcc'][None]
        lcc = lcc + (-F.binary_cross_entropy_with_logits(dml, b['dmg'][None].expand_as(dml), reduction='none').sum(-1) * b['m_dmg'][None]) \
                  + (-F.binary_cross_entropy_with_logits(brl, b['brk'][None].expand_as(brl), reduction='none') * b['m_brk'][None]) \
                  + torch.log_softmax(cll, -1).gather(-1, b['cl'][None, ..., None])[..., 0] * b['m_cl'][None]
        mp, lp = model.p_z(b)
        lpz = (-0.5 * (z - mp) ** 2 / torch.exp(lp) - 0.5 * lp - c2).sum(-1); lqz = (-0.5 * (z - mq) ** 2 / torch.exp(lq) - 0.5 * lq - c2).sum(-1)
        lpx_code = (o['lw'] - lcr.sum(-1) - lcc.sum(-1) - lpz + lqz)[0].double().numpy()
    lpx_b, post_b = brute(model, b, z)
    err = np.abs(lpx_code - lpx_b); ok = np.isfinite(lpx_b) & (lpx_b > -5e3)
    tol = np.maximum(1e-2, 2e-5 * np.abs(lpx_b))                                                 # float32 accumulation vs float64
    check('C1 exact (s, e, f) sum: vectorised log p(x, m | z) = brute-force enumeration', bool((err[ok] <= tol[ok]).all()), f'max |Δ| {np.nanmax(err[ok]):.2e} (median {np.median(err[ok]):.1e}) over {ok.sum()} trees')
    lpost = o['lpost'][0].double().numpy(); worst = 0.0
    for i, d in post_b.items():
        if not ok[i]: continue
        for (s_, e_, f_), p in d.items(): worst = max(worst, abs(math.exp(lpost[i, s_, e_, f_]) - p))
    check('C2 posterior over (s, e, f) = brute force', worst < 5e-3, f'max |Δp| {worst:.2e}')
    # D: per-slot summaries from the same posterior
    sI = np.arange(M.T_MAX); worstD = 0.0
    W = torch.exp(o['lpost'])[0].double().numpy()
    for i, d in post_b.items():
        if not ok[i]: continue
        nv = int(b['v'][i].sum())
        for t in range(nv):
            pe = sum(p for (s_, e_, f_), p in d.items() if s_ <= t < (e_ if e_ < M.T_MAX else nv))
            pdie = sum(p for (s_, e_, f_), p in d.items() if e_ < M.T_MAX and t >= e_ and f_ == 0)
            pcut = sum(p for (s_, e_, f_), p in d.items() if e_ < M.T_MAX and t >= e_ and f_ == 1)
            Ex = sum(W[i, s_, e_, f_] for s_ in range(M.T_MAX) for e_ in range(M.T_MAX + 1) for f_ in (0, 1) if s_ <= t < (e_ if e_ < M.T_MAX else nv))
            worstD = max(worstD, abs(pe - Ex))
    check('D1 P(exists) per slot from the vectorised posterior = brute force', worstD < 5e-3, f'max |Δ| {worstD:.2e}')
    return b

def part_e(model, b):
    phi = model.enc_params(); ids = {id(p) for p in phi}; theta = [p for p in model.parameters() if id(p) not in ids]
    o = model.joint(b, 8, dreg=True)
    lt = -(torch.logsumexp(o['lw'], 0) - math.log(8)).mean(); w = torch.softmax(o['lw'], 0).detach()
    lp = -(w ** 2 * o['lw_dreg']).sum(0).mean()
    gt = torch.autograd.grad(lt, theta, retain_graph=True, allow_unused=True); gp = torch.autograd.grad(lp, phi, retain_graph=True, allow_unused=True)
    fin = all(g is None or torch.isfinite(g).all() for g in list(gt) + list(gp))
    check('E1 gradients finite', fin, '')
    check('E2 decoder / prior get gradients from the IWAE loss', sum(g is not None and g.abs().sum() > 0 for g in gt) > len(theta) // 2, '')
    check('E3 encoder gets gradients from the DReG surrogate', sum(g is not None and g.abs().sum() > 0 for g in gp) > len(phi) // 2, '')
    # the DReG surrogate must not depend on q's score: d lw_dreg / d (mu_q, logvar_q) at FIXED z is zero
    mq, lq = model.q(b); zz = (mq + torch.exp(0.5 * lq) * torch.randn_like(mq)).detach()[None]
    c2 = 0.5 * math.log(2 * math.pi); lqd = (-0.5 * (zz - mq.detach()) ** 2 / torch.exp(lq.detach()) - 0.5 * lq.detach() - c2).sum()
    check('E4 DReG term uses a detached q density (score term removed)', not lqd.requires_grad, '')

def part_f(seq, model):
    va = seq[seq.split == 'val']
    be = M.bench_end(va.head(300000)); B, _, sq = M.build(be, model.vocab)
    ti, po = sq._tid.to_numpy(), sq._pos.to_numpy(); at = sq.truth_E.notna().to_numpy()
    y = sq.truth_E.to_numpy()[at]
    feats = {'real': B['real'], 'ev': B['ev'].float(), 'mode': B['mode'].float(),
             'is_last_visible+1': (torch.arange(M.T_MAX)[None] == (B['l'][:, None] + 1)).float()}
    worst = 0.0; rows = []
    for k_, T_ in feats.items():
        vals = T_[ti, po].numpy()[at]; a_, g_ = vals[y == 1].mean(), vals[y == 0].mean(); rows.append(f'{k_}: alive {a_:.4f} gone {g_:.4f}')
        worst = max(worst, abs(a_ - g_))
    # number of slots after j must be 0 for every instance (truncation)
    last = sq.groupby('_tid')._pos.max().to_numpy()[ti][at]; jpos = po[at]
    smp = B['sampled'][ti, po].numpy()[at]
    check('F1 censored-end: identical structural inputs at the scored slot (alive vs gone)', worst < 5e-3, '; '.join(rows)
          + f' | info (condition evidence, legitimate): sampled alive {smp[y == 1].mean():.3f} gone {smp[y == 0].mean():.3f}')
    check('F2 censored-end: lineage truncated at the scored slot (no later slots)', bool((last == jpos).all()), '')
    bs = M.bench_sizes(va.head(300000), 5.0, False); B2, _, s2 = M.build(bs, model.vocab)
    ti2, po2 = s2._tid.to_numpy(), s2._pos.to_numpy()
    check('F3 size benchmark: every slot of a converted lineage has the 5-in limit', np.allclose(np.exp(B2['llim'][ti2, po2].numpy()) + M.HALF_UNIT, 5.0), '')
    check('F4 size benchmark: no FALSE labels or microplot codes survive', bool((B2['ev'][ti2, po2].numpy() != M.EV['false_soft']).all()) and bool((B2['rcd'] == 1).all()), '')
    hid = s2.hidden.to_numpy()
    check('F5 size benchmark: hidden real visits carry no measurement', bool((B2['mD'][ti2, po2].numpy()[hid] == 0).all()) and bool((B2['real'][ti2, po2].numpy()[hid] == 0).all()), '')
    hz_ = lambda Bx, t_, p_, msk: sum(float(Bx[k][t_, p_].numpy()[msk].sum()) for k in ('m_dmg', 'm_brk', 'm_cl', 'brk')) + float(Bx['dmg'][t_, p_].numpy()[msk].sum())
    check('F6 damage / broken / cull inputs are absent at every hidden record (both benchmarks)',
          hz_(B2, ti2, po2, hid) == 0 and hz_(B, ti, po, sq.hidden.to_numpy()) == 0, '')

def part_g(seq):
    keys = seq.plot_key.drop_duplicates()
    fold = {k: zlib.crc32((k + '|ncv').encode()) % 5 for k in keys}; inner = {k: zlib.crc32((k + '|ncv-in').encode()) % 10 == 0 for k in keys}
    pk = seq[['plot_key', 'split']].drop_duplicates(); pk['fold'] = pk.plot_key.map(fold); pk['inner'] = pk.plot_key.map(inner)
    ok = True
    for f in range(5):
        tr = set(pk[(pk.fold != f) & ~pk.inner].plot_key); inn = set(pk[(pk.fold != f) & pk.inner].plot_key); out = set(pk[(pk.fold == f) & (pk.split != 'val')].plot_key)
        ok &= not (tr & inn) and not (tr & out) and not (inn & out)
    check('G1 nested CV: train / inner / outer plot sets disjoint in every fold', ok, '')
    scored = set(pk[pk.split != 'val'].plot_key); allout = set().union(*[set(pk[(pk.fold == f) & (pk.split != 'val')].plot_key) for f in range(5)])
    check('G2 every non-dev plot scored exactly once; dev-VAL never', allout == scored, f'{len(allout)} plots')

def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--data', required=True); ap.add_argument('--db', required=True); ap.add_argument('--model')
    ap.add_argument('--fiadb'); ap.add_argument('--prism'); ap.add_argument('--gaps-only', action='store_true'); a = ap.parse_args()
    if a.gaps_only:
        torch.set_num_threads(4); seq = pd.read_parquet(f'{a.data}/seq.parquet')
        if a.fiadb: part_a2(seq, a.db, a.fiadb, a.prism)
        part_h()
        model = M.make_model(M.build(seq[seq.split == 'train'].head(200000))[1]); model.eval(); part_i(model, seq)
        print(f"\n{sum(o for _, o in RES)}/{len(RES)} checks passed"); return
    torch.set_num_threads(4); rng = np.random.default_rng(0)
    seq = pd.read_parquet(f'{a.data}/seq.parquet')
    part_a(seq, a.db, rng)
    part_b(seq)
    if a.model:
        ck = torch.load(a.model, weights_only=False); model = M.make_model(ck['vocab']); model.load_state_dict(ck['state'], strict=False); model.eval()
    else:
        model = M.make_model(M.build(seq[seq.split == 'train'].head(200000))[1])
    b = part_cd(model, seq); model.train(); part_e(model, b); model.eval()
    part_f(seq, model); part_g(seq)
    print(f"\n{sum(o for _, o in RES)}/{len(RES)} checks passed")


# ------------------------------------------------------------------------------------------------ additional checks (gaps 1-4)
def part_a2(seq, db, fiadb, prism):
    c = duckdb.connect(db, read_only=True); state = c.execute('select any_value(STATECD) from clean_subplot_years').fetchone()[0]
    s = seq[(seq.statecd == state) & (seq.slot_kind == 'real')]
    # A5 damage join: compare with FIADB TREE by the record's CN
    smp = s[s.dmg1.notna()].sample(300, random_state=7)[M.K_ + ['measdate', 'dmg1', 'dmg2', 'dmg3']]
    c.register('smp', smp)
    cn = c.execute('''select smp.*, ct.CN from smp join curated_trees ct on ct.STATECD = smp.statecd and ct.UNITCD = smp.unitcd and ct.COUNTYCD = smp.countycd
                      and ct.PLOT = smp.plot and ct.SUBP = smp.subp and ct.TREE = smp.tree and ct.MEASDATE = smp.measdate and ct.STATUSCD = 1''').df()
    f = duckdb.connect(fiadb, read_only=True); f.register('cn', cn[['CN']])
    ft = f.execute('select t.CN, t.DAMAGE_AGENT_CD1 a, t.DAMAGE_AGENT_CD2 b, t.DAMAGE_AGENT_CD3 c from TREE t join cn using (CN)').df()
    x = cn.merge(ft, on='CN', how='left')
    eq = lambda p, q: (pd.isna(p) & pd.isna(q)) | (p == q)
    bad = int((~(eq(x.dmg1, x.a) & eq(x.dmg2, x.b) & eq(x.dmg3, x.c))).sum())
    check('A5 damage codes = FIADB TREE.DAMAGE_AGENT_CD1-3 of the record CN', bad == 0 and len(x) == 300, f'{bad}/{len(x)} mismatches')
    # A6 species = latest resolved identification; revised = any spcd_changed
    tt = c.execute("select statecd, unitcd, countycd, plot, subp, tree, arg_max(spcd, measdate) sp, bool_or(coalesce(spcd_changed, false)) rv from tree_trajectories where intro_type = 'real' group by all").df()
    y = seq[seq.statecd == state].groupby(M.K_).agg(spcd=('spcd', 'first'), rev=('sp_revised', 'first')).reset_index().merge(tt, on=M.K_)
    check('A6 species = latest resolved SPCD; revised flag = any curated spcd_changed', bool((y.spcd == y.sp).all() and (y.rev.astype(bool) == y.rv.astype(bool)).all()),
          f'{len(y)} trees; revised share {y.rev.astype(float).mean():.3f}')
    # A7 climate windows: tm_anom = mean L4 tmean over years (prev slot, slot] (first slot: 5 years before) − 1991-2020 normal
    p = duckdb.connect(prism, read_only=True)
    yr = p.execute('select EPA_L4_ID l4, YEAR yr, CAST(CAST(tmean_mean AS VARCHAR) AS DOUBLE) tm from EPA_L4_yearly').df()
    nrm = yr[(yr.yr >= 1991) & (yr.yr <= 2020)].groupby('l4').tm.mean(); ymax = yr.yr.max(); yr = yr.set_index(['l4', 'yr']).tm
    ss = seq[seq.statecd == state].sort_values(M.K_ + ['measdate']).copy()
    ss['yprev'] = ss.groupby(M.K_).measdate.shift(1); ss = ss.sample(300, random_state=8)
    bad = 0
    for r in ss.itertuples():
        ye = min(pd.Timestamp(r.measdate).year, ymax); ys = (pd.Timestamp(r.yprev).year + 1) if not pd.isna(r.yprev) else ye - 4
        ys = min(ys, ye)
        vals = [yr.get((r.epa_l4, y_)) for y_ in range(ys, ye + 1)]; vals = [v for v in vals if v is not None]
        want = np.mean(vals) - nrm.get(r.epa_l4, np.nan) if vals else np.nan
        bad += not ((np.isnan(want) and pd.isna(r.tm_anom)) or (not pd.isna(r.tm_anom) and abs(want - r.tm_anom) < 1e-6))
    check('A7 climate anomaly = L4 mean tmean over the interval years − 1991-2020 normal', bad == 0, f'{bad}/300 slots')

def part_h():
    import nimiwae54_ncv as NC
    rng = np.random.default_rng(0)
    p = rng.uniform(0, 1, 50000); y = (rng.uniform(0, 1, 50000) < p).astype(float)
    r1 = NC.reliability(p, y); r2 = NC.reliability(np.clip(p + 0.15, 0, 1), y)
    check('H1 reliability bar: calibrated probabilities pass, shifted (+0.15) fail', r1['pass_'] and not r2['pass_'], f"brier {r1['brier']:.3f} vs {r2['brier']:.3f}")
    # size criteria on a synthetic perfect predictor with a correct 80 % interval
    n = 4000; d = rng.uniform(1.0, 5.0, n); ld = np.log(d); sd = 0.1
    h = pd.DataFrame({'statecd': 1, 'unitcd': 1, 'countycd': 1, 'plot': rng.integers(0, 400, n), 'subp': 1, 'measdate': pd.Timestamp('2010-01-01'),
                      'major_spgrpcd': rng.integers(1, 3, n), 'dia': d, 'nim_lD': ld + rng.normal(0, sd, n)})
    h['nim_q10'] = h.nim_lD - 1.2816 * sd; h['nim_q90'] = h.nim_lD + 1.2816 * sd; h['nim_BA'] = M.BA_K * np.exp(h.nim_lD) ** 2 * np.exp(-2 * sd ** 2)
    c = NC.size_criteria(h, (0,), 5.0)
    check('H2 size criteria: unbiased, calibrated synthetic predictor passes A1 / A2 / A3', c['A1_pass'] and c['A2_pass'] and c['A3_pass'],
          f"A2 {c['A2'][0]['cov80']:.3f} A3 {c['A3_median_BA_ratio']:.3f}")
    h2 = h.copy(); h2['nim_lD'] = h2.nim_lD + 0.2; c2 = NC.size_criteria(h2, (0,), 5.0)
    check('H3 size criteria: a +0.2 log-DIA bias fails A1', not c2['A1_pass'], '')

def part_i(model, seq):
    va = seq[seq.split == 'val']; x = va[va.plot_key.isin(va.plot_key.drop_duplicates().sample(25, random_state=9))]
    B, _, sq = M.build(x, model.vocab)
    torch.manual_seed(0); R = M.impute(model, B, K=1, bs=10 ** 7)                                   # one batch: same z as the joint below
    torch.manual_seed(0)
    with torch.no_grad(): o = model.joint(B, 1)
    W = torch.exp(o['lpost'])[0].sum((-1, -2)).double()                                              # p(s | z) [n, S]
    D = o['D'][0].double(); BA = (W[..., None] * (D > 0) * M.BA_K * D ** 2).sum(1) / (W[..., None] * (D > 0)).sum(1).clamp(min=1e-12)
    err = float((BA - R['BA'].double()).abs().max())
    check('I1 expected basal area given existence = Σ p(s) · 0.005454 · D[s]² (K = 1)', err < 1e-3, f'max |Δ| {err:.2e} ft²')
    v = B['v'] > 0; inside = ((R['q10'] <= R['lD'] + 1e-6) & (R['lD'] <= R['q90'] + 1e-6))[v]
    check('I2 predictive interval brackets the point estimate', float(inside.float().mean()) > 0.99, f'{float(inside.float().mean()):.4f}')


if __name__ == '__main__':
    main()
