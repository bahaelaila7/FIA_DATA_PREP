"""D70 am. 15: cross-validation of the joint age model on TRAIN plots, self-contained for a GPU machine.
  python -I nimiwae70_cv.py --data data --out out/cv --fold 0 --age 1 --device cuda        one run: trains, tracks held-out plots by epoch, scores checkpoints
  python -I nimiwae70_cv.py --out out/cv --summary                                         pools the finished runs against the bars
Folds: crc32(plot_key|ncv) % 5, the D54 rule, on TRAIN plots. VAL carried design choices and TEST stays untouched. Monitor: 10 % of the fitting plots, crc32(plot_key|ncv-in) % 10 == 0, never trained on and never scored.
--d74 1 (D74): measured-age anchors on fit plots, the calibrated age-vigour coupling and the HTCD height model; its own --out (cv74).
--rescore-g1 SRC: score SRC's saved age-on checkpoints on both G1 sets (am. 17 and the D70 set), so cv72 and cv74 compare on the same trees.
--d76 1 (D76): a crew-miss rate for not-yet-listed trees at pre-debut subplot-level slots, fixed from the fit plots; its own --out (cv76flow).
--backend jax (D75): train with nimiwae_jax (JAX / Flax NNX / optax), score with the same PyTorch code; its own --out.
--q flow (D73): the proposal's non-age dimensions go through a conditional spline flow; give it its own --out, a run directory refuses a different q.
Schedule-Free AdamW, so the same run can be read at any epoch with no decay schedule tied to its length: epoch 12 of a 36-epoch run is a real 12-epoch model. The curve file answers overtraining directly: bound on fitted trees against bound on monitor trees."""
import sys, os, json, argparse, time, zlib, glob
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd, torch
import nimiwae56 as M, nimiwae70 as A, nimiwae70_score as S

NF = 5
BARS = dict(G1_mae=9.4, G1_bias=2.0, G5=(0.75, 0.85), G6_size=1.05, G6_brier=0.005, G8=-0.005)

def split_plots(keys, fold):
    """-> held, monitor, fit: three disjoint lists of plot keys"""
    f = {k: zlib.crc32((k + '|ncv').encode()) % NF for k in keys}; i = {k: zlib.crc32((k + '|ncv-in').encode()) % 10 == 0 for k in keys}
    return [k for k in keys if f[k] == fold], [k for k in keys if f[k] != fold and i[k]], [k for k in keys if f[k] != fold and not i[k]]

@torch.no_grad()
def bound(m, B, k, dev, bs=512):
    """mean IWAE bound (nats per tree) and median ESS on one fixed tree set"""
    tot, n, ess = 0.0, 0, []
    for i in range(0, B['v'].shape[0], bs):
        o = m.joint({a: v.to(dev) for a, v in M.sub(B, slice(i, i + bs)).items()}, k); lw = o['lw']
        tot += (torch.logsumexp(lw, 0) - np.log(k)).sum().item(); n += lw.shape[1]; ess.append(1 / (torch.softmax(lw, 0) ** 2).sum(0))
    return tot / n, torch.cat(ess).median().item()

def run(a):
    d = f'{a.out}/f{a.fold}_age{a.age}'; os.makedirs(d, exist_ok=True)
    qc = dict(q=a.q, flow=dict(layers=a.flow_layers, hidden=a.flow_hidden, bins=a.flow_bins) if a.q == 'flow' else None, d74=bool(a.d74), backend=a.backend, d76=bool(a.d76))
    arch = A.arch_of(dict(layers=a.layers, d=a.width))
    if arch: qc['arch'] = arch                                                                # D82: recorded only off the default, so older run folders still match
    if os.path.exists(f'{d}/q.json'): assert {'d74': False, 'backend': 'torch', 'd76': False, **json.load(open(f'{d}/q.json'))} == qc, f'{d} holds a run with another setup: {open(f"{d}/q.json").read()}'
    elif os.path.exists(f'{d}/run.log'): assert a.q == 'gauss' and not a.d74 and a.backend == 'torch' and not a.d76, f'{d} holds a run from before D73 (Gaussian q, no D74, PyTorch)'
    json.dump(qc, open(f'{d}/q.json', 'w'))
    if os.path.exists(f'{d}/done.json'): print('already done', d); return
    tr = pd.read_parquet(f'{a.data}/seq.parquet', filters=[('split', '==', 'train')])       # TRAIN rows only: several runs load this at once
    site, planted = pd.read_parquet(f'{a.data}/site_refs.parquet'), pd.read_parquet(f'{a.data}/planted_refs.parquet'); site_old = pd.read_parquet(f'{a.data}/site_refs_d70.parquet')
    k = M.K_ + ['invyr']
    if a.d74: tr = tr.merge(pd.read_parquet(f'{a.data}/ht_refs.parquet'), on=k, how='left')        # HTCD for every slot it exists on (an input)
    held_k, mon_k, fit_k = split_plots(sorted(tr.plot_key.unique()), a.fold)
    held, mon, fit = (tr[tr.plot_key.isin(set(x))] for x in (held_k, mon_k, fit_k))
    if a.d74 and a.age:                                                                       # anchors: fit plots only, age on only; held anchors are the G1 truth
        fit = fit.merge(site[k + ['a_obs']].rename(columns={'a_obs': 'anc_agedia'}), on=k, how='left')
        assert 'anc_agedia' not in held and 'anc_agedia' not in mon
    log_f = open(f'{d}/run.log', 'a'); pr = lambda s: (print(f'[f{a.fold} age{a.age}] {s}', flush=True), log_f.write(s + '\n'), log_f.flush())
    pr(f'plots held {len(held_k):,}, monitor {len(mon_k):,}, fit {len(fit_k):,}')
    score_plots = held[held.plot_key.isin(pd.Series(held_k).sample(frac=a.score_frac, random_state=2))]; ckpts = sorted(int(x) for x in a.ckpts.split(',')); state = dict(curve=[], ckpt={}, Bm=None, Bt=None)
    sp = f'{d}/train_state.{"pkl" if a.backend == "jax" else "pt"}'                         # resumable training state, saved after every epoch
    if os.path.exists(sp):                                                                    # resuming: keep what earlier epochs wrote, append to it
        if os.path.exists(f'{d}/curve.json'): state['curve'] = json.load(open(f'{d}/curve.json'))
        if os.path.exists(f'{d}/scores.json'): state['ckpt'] = json.load(open(f'{d}/scores.json'))
        pr(f'resuming from {sp}: {len(state["curve"])} curve points and checkpoints {sorted(state["ckpt"])} kept')
    rng = np.random.default_rng(a.seed); mon_s = mon[mon.plot_key.isin(set(rng.permutation(mon_k)[:a.curve_plots]))]; fit_s = fit[fit.plot_key.isin(set(rng.permutation(fit_k)[:a.curve_plots]))]
    def hook(ep, m, h):
        if ep % a.curve_every and ep not in ckpts: return
        m.eval()
        if state['Bm'] is None: state['Bm'] = A.build(mon_s, m.vocab)[0]; state['Bt'] = A.build(fit_s.drop(columns=['anc_agedia'], errors='ignore'), m.vocab)[0]   # bounds without anchor terms, like the monitor
        dev = next(m.parameters()).device; lm, em = bound(m, state['Bm'], a.k, dev); lt, et = bound(m, state['Bt'], a.k, dev)
        state['curve'] = [c for c in state['curve'] if c['epoch'] != ep]                        # a resumed run may redo an epoch whose hook was cut short
        state['curve'].append(dict(epoch=ep, fit_bound=lt, monitor_bound=lm, gap=lt - lm, fit_ess=et, monitor_ess=em)); pr(json.dumps(state['curve'][-1])); json.dump(state['curve'], open(f'{d}/curve.json', 'w'), indent=0)
        if ep in ckpts:
            if 'age_coupling' in m.vocab and not os.path.exists(f'{d}/coupling.json'): json.dump(m.vocab['age_coupling'], open(f'{d}/coupling.json', 'w'), indent=1)
            if 'miss_eps' in m.vocab and not os.path.exists(f'{d}/miss.json'): json.dump(dict(eps=m.vocab['miss_eps']), open(f'{d}/miss.json', 'w'))   # D76: eps per fold (reported)
            t0 = time.time(); o = dict(completion=S.completion(m, score_plots))
            if a.age and ep == max(ckpts): o['completion_grid'] = S.completion(m, score_plots, age='grid')   # D77: as the products integrate age (reported)
            rows = []
            if a.age: o.update(S.age_scores(m, held, site, planted, planted_frac=a.planted_frac, site_old=site_old, rows=rows)); pd.concat(rows).to_parquet(f'{d}/g1_e{ep}.parquet')
            state['ckpt'][str(ep)] = o; json.dump(state['ckpt'], open(f'{d}/scores.json', 'w'), indent=1, default=float); pr(f'scored epoch {ep} in {time.time() - t0:.0f} s: {json.dumps(o, default=float)[:300]}')
            torch.save(dict(state={k: v.cpu() for k, v in m.state_dict().items()}, vocab=m.vocab, cfg=m.cfg), f'{d}/model_e{ep}.pt')
        m.train()
    trainer = A.train
    if a.backend == 'jax':                                                                    # D75: the same training in JAX; scoring stays in PyTorch
        from nimiwae_jax import train as JT; trainer = JT.train
    t0 = time.time(); trainer(fit, epochs=max(ckpts), k=a.k, bs=a.bs, max_trees=a.trees, seed=a.seed, threads=a.threads, log=pr, age=bool(a.age), device=a.device, opt_name='sf', on_epoch=hook, q=qc['q'], flow=qc['flow'], d74=bool(a.d74), state_path=sp, d76=bool(a.d76), arch=arch)
    json.dump(dict(seconds=time.time() - t0), open(f'{d}/done.json', 'w')); pr(f'done {time.time() - t0:.0f} s')

def rescore(a):
    """score saved checkpoints of a finished run with the current code, without training: the age-off arm, which am. 16 does not change"""
    src, d = f'{a.rescore}/f{a.fold}_age{a.age}', f'{a.out}/f{a.fold}_age{a.age}'; os.makedirs(d, exist_ok=True)
    if os.path.exists(f'{d}/done.json'): print('already done', d); return
    assert not a.age, 'only the age-off arm is unchanged by am. 16; the age-on arm must be retrained'
    tr = pd.read_parquet(f'{a.data}/seq.parquet', filters=[('split', '==', 'train')]); held_k, _, _ = split_plots(sorted(tr.plot_key.unique()), a.fold); held = tr[tr.plot_key.isin(set(held_k))]
    score_plots = held[held.plot_key.isin(pd.Series(held_k).sample(frac=a.score_frac, random_state=2))]; out = {}
    for f in sorted(glob.glob(f'{src}/model_e*.pt'), key=lambda x: int(x.split('_e')[-1][:-3])):
        ep = f.split('_e')[-1][:-3]; m, _ = S.load(src, os.path.basename(f)); m.to(a.device); torch.manual_seed(0)
        out[ep] = dict(completion=S.completion(m, score_plots)); print(f'[f{a.fold} age0 rescore] epoch {ep}: {json.dumps(out[ep], default=float)[:200]}', flush=True)
    import shutil
    if os.path.exists(f'{src}/curve.json'): shutil.copy(f'{src}/curve.json', f'{d}/curve.json')
    json.dump(out, open(f'{d}/scores.json', 'w'), indent=1, default=float); json.dump(dict(rescored_from=src), open(f'{d}/done.json', 'w'))

def rescore_g1(a):
    """score the saved age-on checkpoints of a finished run on both G1 sets with the current code (no training): cv72 against cv74 on the same trees"""
    src, d = f'{a.rescore_g1}/f{a.fold}_age1', f'{a.out}/f{a.fold}_age1'; os.makedirs(d, exist_ok=True)
    tr = pd.read_parquet(f'{a.data}/seq.parquet', filters=[('split', '==', 'train')]); held_k, _, _ = split_plots(sorted(tr.plot_key.unique()), a.fold); held = tr[tr.plot_key.isin(set(held_k))]
    site, planted, site_old = (pd.read_parquet(f'{a.data}/{x}.parquet') for x in ('site_refs', 'planted_refs', 'site_refs_d70')); out = {}
    for f in sorted(glob.glob(f'{src}/model_e*.pt'), key=lambda x: int(x.split('_e')[-1][:-3])):
        ep = f.split('_e')[-1][:-3]; m, ck = S.load(src, os.path.basename(f)); m.to(a.device); rows = []
        assert not ck['cfg'].get('d74', False) and 'age_coupling' not in m.vocab, 'rescore-g1 is for runs without D74'
        out[ep] = S.age_scores(m, held, site, planted, planted_frac=a.planted_frac, site_old=site_old, rows=rows); pd.concat(rows).to_parquet(f'{d}/g1_e{ep}.parquet')
        print(f'[f{a.fold} rescore-g1] epoch {ep}: G1 {out[ep]["G1"]["mae_joint"]:.3f} (n {out[ep]["G1"]["n"]:,}), G1_d70 {out[ep]["G1_d70"]["mae_joint"]:.3f}', flush=True)
    json.dump(out, open(f'{d}/scores_g1.json', 'w'), indent=1, default=float); json.dump(dict(rescored_from=src), open(f'{d}/done_g1.json', 'w'))

def rescore_grid(a):
    """D77: completion with age on the exact grid (S.completion(age='grid'), what the products use) for the last saved age-on checkpoint of a finished
    run, on the same held inputs and score plots as its own scoring; written next to its scores as scores_grid.json (nothing else is touched)"""
    d = f'{a.rescore_grid}/f{a.fold}_age1'; qc = json.load(open(f'{d}/q.json')) if os.path.exists(f'{d}/q.json') else {}
    ep = str(max(int(x.split('_e')[-1][:-3]) for x in glob.glob(f'{d}/model_e*.pt'))); g = f'{d}/scores_grid.json'
    old = json.load(open(g)) if os.path.exists(g) else {}
    if ep in old: print('already scored', d, ep); return
    tr = pd.read_parquet(f'{a.data}/seq.parquet', filters=[('split', '==', 'train')]); k = M.K_ + ['invyr']
    if qc.get('d74'): tr = tr.merge(pd.read_parquet(f'{a.data}/ht_refs.parquet'), on=k, how='left')      # the inputs the run scored with
    held_k, _, _ = split_plots(sorted(tr.plot_key.unique()), a.fold); held = tr[tr.plot_key.isin(set(held_k))]
    score_plots = held[held.plot_key.isin(pd.Series(held_k).sample(frac=a.score_frac, random_state=2))]   # = run()'s score plots
    m, _ = S.load(d, f'model_e{ep}.pt'); m.to(a.device); t0 = time.time()
    old[ep] = S.completion(m, score_plots, age='grid'); json.dump(old, open(g, 'w'), indent=1, default=float)
    print(f'[f{a.fold} rescore-grid] epoch {ep} in {time.time() - t0:.0f} s: {json.dumps(old[ep], default=float)}', flush=True)

def grid_completion(run, f, e):
    """the D77 grid completion of run's age-on fold f at epoch e: from its own scoring, else from rescore_grid; None if neither"""
    s = json.load(open(f'{run}/f{f}_age1/scores.json')).get(str(e), {}) if os.path.exists(f'{run}/f{f}_age1/scores.json') else {}
    if 'completion_grid' in s: return s['completion_grid']
    g = f'{run}/f{f}_age1/scores_grid.json'
    return json.load(open(g)).get(str(e)) if os.path.exists(g) else None

D74_BARS = dict(g1_gain=0.2, g1_fold_worse=0.2, g1_bias=2.0, g5=(0.75, 0.85), resid_slope=0.5)

def compare(a):
    """D74's pre-registered bars: cv74 (a.out) against cv72 rescored on both G1 sets (a.compare), same folds, epoch by epoch. G6 / G8 come from
    cv74's own summary (age on vs age off, both with the D74 height model)."""
    R = {}
    for e in sorted({int(x) for f in glob.glob(f'{a.out}/f*_age1/scores.json') for x in json.load(open(f))}):
        fo = {}
        for f in range(NF):
            p74, p72 = f'{a.out}/f{f}_age1/scores.json', f'{a.compare}/f{f}_age1/scores_g1.json'
            if not (os.path.exists(p74) and os.path.exists(p72)): continue
            s74, s72 = json.load(open(p74)).get(str(e)), json.load(open(p72)).get(str(e))
            if s74 and s72: fo[f] = dict(g1_74=s74['G1']['mae_joint'], g1_72=s72['G1']['mae_joint'], bias_74=s74['G1']['bias_joint'], direct_74=s74['G1']['mae_direct'], g5_74=s74['G5']['cover80'],
                                       g1d70_74=s74['G1_d70']['mae_joint'], g1d70_72=s72['G1_d70']['mae_joint'])
        if not fo: continue
        rows = pd.concat([pd.read_parquet(x) for x in glob.glob(f'{a.out}/f*_age1/g1_e{e}.parquet')]); c = rows[rows.af_shift != 0]
        y = np.log(c.truth.to_numpy()) - np.log(c.q50.to_numpy()); X = np.column_stack([np.ones(len(c)), c.lrate.to_numpy()]); sl = float(np.linalg.lstsq(X, y, rcond=None)[0][1]) if len(c) > 50 else None
        cps = [json.load(open(x)) for x in glob.glob(f'{a.out}/f*_age1/coupling.json')]; dl = float(np.mean([cp['delta']['3'] for cp in cps])) if cps else None
        gain = np.mean([v['g1_72'] - v['g1_74'] for v in fo.values()])
        bars = dict(G1_gain=bool(gain >= D74_BARS['g1_gain'] and all(v['g1_74'] - v['g1_72'] <= D74_BARS['g1_fold_worse'] for v in fo.values())),
                    G1_bias=bool(abs(np.mean([v['bias_74'] for v in fo.values()])) <= D74_BARS['g1_bias']), G1_direct=bool(np.mean([v['g1_74'] for v in fo.values()]) <= np.mean([v['direct_74'] for v in fo.values()])),
                    G5=bool(D74_BARS['g5'][0] <= np.mean([v['g5_74'] for v in fo.values()]) <= D74_BARS['g5'][1]),
                    residual_slope=bool(sl is not None and dl is not None and abs(sl) <= D74_BARS['resid_slope'] * abs(dl)))
        R[e] = dict(folds=fo, mean_gain_G1=float(gain), residual_slope=sl, delta_c3=dl, n_coupled_G1=int(len(c)), bars=bars)
    json.dump(R, open(f'{a.out}/d74_compare.json', 'w'), indent=1, default=float); print(json.dumps(R, indent=1, default=float)[:5000])
    print('G6 / G8: see the cv74 --summary (age on vs age off within cv74).')

D76_BARS = dict(primary=0.0010, g1_worse=0.05, g1_bias=2.0, g5=(0.75, 0.85), brier_worse=0.002, size_ratio=1.01, resid_slope=0.5)   # pre-registered in D76

def compare_d76(a):
    """D76's pre-registered bars: --out (cv76flow) against --d76-base (cv74flow), same folds, epoch by epoch.
    Primary: gatepull_e{epoch}.json of --out and of the base (nimiwae70_gatepull.py; only files over all held plots of all five folds, as trained,
    count): the pooled subplot share below -2 falls from the base's value to <= 0.10 %.
    No harm, on means over the age-on folds present in both runs (adoption needs all five): G1 mean MAE (both link sets) not worse by more than
    0.05 yr, |G1 median-bias mean| <= 2, G5 coverage 0.75-0.85, E1 / E2 / E3 Brier not worse by more than 0.002, size MAE not worse by more than 1 %,
    the D74 residual slope <= 0.5 |delta|. Reported: eps per fold, the same completion deltas for the age-off arm, the D70-set bias and coverage."""
    b = a.d76_base; R = {}
    for e in sorted({int(x) for f in glob.glob(f'{a.out}/f*_age1/scores.json') for x in json.load(open(f))}):
        fo = {}
        for f in range(NF):
            for arm in (1, 0):
                p6, p4 = f'{a.out}/f{f}_age{arm}/scores.json', f'{b}/f{f}_age{arm}/scores.json'
                if not (os.path.exists(p6) and os.path.exists(p4)): continue
                s6, s4 = json.load(open(p6)).get(str(e)), json.load(open(p4)).get(str(e))
                if not (s6 and s4): continue
                c6, c4 = s6['completion'], s4['completion']; r = dict(dE1=c6['E1'] - c4['E1'], dE2=c6['E2'] - c4['E2'], dE3=c6['E3'] - c4['E3'], size_ratio=c6['mae_lD'] / c4['mae_lD'])
                if arm: r.update(g1_76=s6['G1']['mae_joint'], g1_74=s4['G1']['mae_joint'], g1d70_76=s6['G1_d70']['mae_joint'], g1d70_74=s4['G1_d70']['mae_joint'],
                                 bias_76=s6['G1']['bias_joint'], g5_76=s6['G5']['cover80'], bias_d70_76=s6['G1_d70']['bias_joint'], g5_d70_76=s6['G5_d70']['cover80'])
                fo[f'f{f}_age{arm}'] = r
        on = {k: v for k, v in fo.items() if k.endswith('age1')}; off = {k: v for k, v in fo.items() if k.endswith('age0')}
        if not on: continue
        mean = lambda d, x: float(np.mean([v[x] for v in d.values()])) if d else None
        gf = [f'{a.out}/{k}/g1_e{e}.parquet' for k in sorted(on)]; gf = [x for x in gf if os.path.exists(x)]   # the same folds as the other bars
        rows = pd.concat([pd.read_parquet(x) for x in gf]) if gf else pd.DataFrame(dict(af_shift=[], truth=[], q50=[], lrate=[]))
        c = rows[rows.af_shift != 0]; y = np.log(c.truth.to_numpy()) - np.log(c.q50.to_numpy()); X = np.column_stack([np.ones(len(c)), c.lrate.to_numpy()])
        sl = float(np.linalg.lstsq(X, y, rcond=None)[0][1]) if len(c) > 50 else None
        cps = [json.load(open(f'{a.out}/{k}/coupling.json')) for k in sorted(on) if os.path.exists(f'{a.out}/{k}/coupling.json')]
        dl = float(np.mean([cp['delta']['3'] for cp in cps])) if cps else None
        gp = {r_: json.load(open(f'{r_}/gatepull_e{e}.json')) if os.path.exists(f'{r_}/gatepull_e{e}.json') else None for r_ in (a.out, b)}
        full = lambda g: g is not None and g['frac'] == 1.0 and sorted(g['folds']) == [str(f) for f in range(NF)] and g.get('eps_scale', 1.0) == 1.0 and g['epoch'] == e
        prim = {r_: (g['pooled']['subplot']['share_below_-2'] if full(g) else None) for r_, g in gp.items()}
        same_trees = bool(full(gp[a.out]) and full(gp[b]) and all(gp[a.out]['folds'][f]['subplot']['n'] == gp[b]['folds'][f]['subplot']['n'] for f in gp[b]['folds']))   # data-defined population
        g1w = mean(on, 'g1_76') - mean(on, 'g1_74'); g1dw = mean(on, 'g1d70_76') - mean(on, 'g1d70_74')
        bars = dict(complete=bool(len(on) == NF and len(gf) == NF and len(cps) == NF),
                    primary=bool(same_trees and prim[a.out] < prim[b] and prim[a.out] <= D76_BARS['primary']),
                    G1=bool(g1w <= D76_BARS['g1_worse'] and g1dw <= D76_BARS['g1_worse']), G1_bias=bool(abs(mean(on, 'bias_76')) <= D76_BARS['g1_bias']),
                    G5=bool(D76_BARS['g5'][0] <= mean(on, 'g5_76') <= D76_BARS['g5'][1]),
                    brier=bool(max(mean(on, 'dE1'), mean(on, 'dE2'), mean(on, 'dE3')) <= D76_BARS['brier_worse']), size=bool(mean(on, 'size_ratio') <= D76_BARS['size_ratio']),
                    residual_slope=bool(sl is not None and dl is not None and abs(sl) <= D76_BARS['resid_slope'] * abs(dl)))
        eps = {os.path.basename(os.path.dirname(x)): json.load(open(x))['eps'] for x in sorted(glob.glob(f'{a.out}/f*_age*/miss.json'))}
        gr = {f: (grid_completion(a.out, f, e), grid_completion(b, f, e)) for f in range(NF)}; gr = {f: v for f, v in gr.items() if None not in v}
        grid = dict(n_folds=len(gr), **{x: (float(np.mean([v[0][x] - v[1][x] for v in gr.values()])) if gr else None) for x in ('E1', 'E2', 'E3')},
                    size_ratio=float(np.mean([v[0]['mae_lD'] / v[1]['mae_lD'] for v in gr.values()])) if gr else None)   # D77, reported: deltas out - base
        R[e] = dict(grid_completion_reported=grid, folds=fo, n_folds_age1=len(on), n_folds_age0=len(off), primary_share_below_m2={'out': prim[a.out], 'base': prim[b]}, primary_same_trees=same_trees, G1_worse=g1w, G1_d70_worse=g1dw,
                    age_on={x: mean(on, x) for x in ('dE1', 'dE2', 'dE3', 'size_ratio', 'bias_76', 'g5_76', 'bias_d70_76', 'g5_d70_76')},
                    age_off_reported={x: mean(off, x) for x in ('dE1', 'dE2', 'dE3', 'size_ratio')}, residual_slope=sl, n_coupled_G1=int(len(c)), delta_c3=dl, eps=eps,
                    bars=bars, adopt=bool(all(bars.values())))
    json.dump(R, open(f'{a.out}/d76_compare.json', 'w'), indent=1, default=float); print(json.dumps(R, indent=1, default=float)[:6000])
    for e in R:
        if None in R[e]['primary_share_below_m2'].values(): print(f'epoch {e}: the primary needs nimiwae70_gatepull.py --epoch {e} on --out and on the base (all folds, --frac 1)')
        if not R[e]['bars']['complete']: print(f'epoch {e}: {R[e]["n_folds_age1"]} of {NF} age-on folds in both runs: no adoption')

D82_ARMS = ('base', 'deep', 'wide', 'alltrees')
D82_RULE = dict(bound=0.05, e1=0.002, size=1.01, g1=0.05)       # pre-registered (D82): epoch 24, fold 0, against base

def _hist(d):
    """the per-epoch training records of a run (its run.log JSON lines with a loss)"""
    out = []
    for line in open(f'{d}/run.log'):
        if line.startswith('{') and '"loss"' in line: out.append(json.loads(line[:line.rindex('}') + 1]))
    return out

def compare82(a, epoch=24):
    """D82: the capacity / data probe. Each arm against base at `epoch` on fold 0: monitor bound gain >= 0.05 nats / tree, E1 not worse by more than
    0.002, size MAE ratio <= 1.01, G1 not worse by more than 0.05 yr. Reported: epoch 12, the all-trees arm at equal optimizer steps, seconds per epoch,
    peak GPU memory, the fit - monitor gap."""
    R, base = {}, None
    for arm in D82_ARMS:
        d = f'{a.out}/d82_{arm}/f0_age1'
        if not os.path.exists(f'{d}/scores.json'): R[arm] = None; continue
        sc, cu, hi = json.load(open(f'{d}/scores.json')), json.load(open(f'{d}/curve.json')), _hist(d)
        cb = {r['epoch']: r for r in cu}; e = str(epoch)
        r = dict(epochs_scored=sorted(sc, key=int), monitor_bound={k: cb[k]['monitor_bound'] for k in (12, epoch) if k in cb}, gap={k: cb[k]['gap'] for k in (12, epoch) if k in cb},
                 E1={k: sc[k]['completion']['E1'] for k in ('12', e) if k in sc}, size={k: sc[k]['completion']['mae_lD'] for k in ('12', e) if k in sc},
                 E2={k: sc[k]['completion']['E2'] for k in ('12', e) if k in sc}, E3={k: sc[k]['completion']['E3'] for k in ('12', e) if k in sc},
                 G1={k: sc[k]['G1']['mae_joint'] for k in ('12', e) if k in sc}, steps_per_epoch=hi[-1].get('steps') if hi else None,
                 seconds_per_epoch=float(np.median([h['seconds'] for h in hi if 'seconds' in h])) if any('seconds' in h for h in hi) else None,
                 gpu_mem_gb=max((h.get('gpu_mem_gb', 0) for h in hi), default=None), curve={k: cb[k]['monitor_bound'] for k in sorted(cb)})
        gp = f'{a.out}/d82_{arm}/gatepull_e24_probe.json'                                # reported: fold-0 gate pulls at epoch 24 (the tabled D76 question)
        if os.path.exists(gp): g = json.load(open(gp))['pooled']['subplot']; r['gatepull_fold0'] = dict(n=g['n'], below_2=g['below_-2'], share_below_2=g['share_below_-2'])
        R[arm] = r
    b = R.get('base'); e = str(epoch)
    if not b or epoch not in b['monitor_bound'] or e not in b['E1']: print(json.dumps(R, indent=1, default=float)); print(f'base has no epoch {epoch} yet'); return R
    wins = {}
    for arm in D82_ARMS[1:]:
        r = R[arm]
        if not r or epoch not in r['monitor_bound'] or e not in r['E1']: wins[arm] = None; continue
        c = dict(bound_gain=r['monitor_bound'][epoch] - b['monitor_bound'][epoch], e1_worse=r['E1'][e] - b['E1'][e], size_ratio=r['size'][e] / b['size'][e], g1_worse=r['G1'][e] - b['G1'][e])
        c['wins'] = bool(c['bound_gain'] >= D82_RULE['bound'] and c['e1_worse'] <= D82_RULE['e1'] and c['size_ratio'] <= D82_RULE['size'] and c['g1_worse'] <= D82_RULE['g1'])
        wins[arm] = c
    at = R.get('alltrees')
    if at and at['steps_per_epoch'] and b['steps_per_epoch']:                           # reported: all trees at the base's optimizer steps
        eq = epoch * b['steps_per_epoch'] / at['steps_per_epoch']; ce = min(at['curve'], key=lambda k: abs(k - eq))
        R['alltrees_equal_steps'] = dict(equivalent_epoch=eq, nearest_curve_epoch=ce, monitor_bound=at['curve'][ce], base_monitor_bound=b['monitor_bound'][epoch])
    ok = {k: v for k, v in wins.items() if v and v['wins']}
    R['rule'] = wins; R['winners'] = sorted(ok, key=lambda k: -ok[k]['bound_gain'])
    R['chosen'] = (['alltrees'] if 'alltrees' in ok else []) + [x for x in R['winners'] if x != 'alltrees'][:1]
    R['note'] = 'both data and a capacity arm won: their combination is untested' if len(R['chosen']) == 2 else ''
    R['gatepull_fold0'] = {k: (R[k] or {}).get('gatepull_fold0') for k in D82_ARMS}
    json.dump(R, open(f'{a.out}/d82_compare.json', 'w'), indent=1, default=float); print(json.dumps({k: R[k] for k in ('rule', 'winners', 'chosen', 'note', 'gatepull_fold0')} | ({'alltrees_equal_steps': R['alltrees_equal_steps']} if 'alltrees_equal_steps' in R else {}), indent=1, default=float))
    return R

def summary(a):
    """fold means per epoch of every score and the D70 bars; with --data also the G1 lookup baselines on the same folds and trees
    (nimiwae70_score.baselines), and per epoch the joint model's G1 MAE next to them"""
    BL = S.baselines(a.data, range(NF), NF) if a.data else None
    R = {}
    for f in sorted(glob.glob(f'{a.out}/f*_age*/scores.json')):
        n = os.path.basename(os.path.dirname(f)); R[n] = json.load(open(f))
    eps = sorted({int(e) for v in R.values() for e in v}); out = {}
    ms = lambda v: dict(mean=float(np.mean(v)), sd=float(np.std(v, ddof=1)) if len(v) > 1 else None, n=len(v)) if v else None
    for e in map(str, eps):
        on = {n[:n.index('_')]: v[e] for n, v in R.items() if n.endswith('age1') and e in v}; off = {n[:n.index('_')]: v[e] for n, v in R.items() if n.endswith('age0') and e in v}
        pair = sorted(set(on) & set(off)); g = lambda p: ms([p(v) for v in on.values()])
        gs = lambda nm, x: ms([v[nm][x] for v in on.values() if nm in v])                    # sets older runs do not have (G1_d70) are skipped
        o = dict(folds_age1=len(on), folds_age0=len(off), G1_mae=g(lambda v: v['G1']['mae_joint']), G1_bias=g(lambda v: v['G1']['bias_joint']), G1_direct=g(lambda v: v['G1']['mae_direct']), G1_prior=g(lambda v: v['G1']['mae_prior']), G1_current=g(lambda v: v['G1']['mae_current']), G5=g(lambda v: v['G5']['cover80']),
                 size_ratio=ms([on[f]['completion']['mae_lD'] / off[f]['completion']['mae_lD'] for f in pair]), dE1=ms([on[f]['completion']['E1'] - off[f]['completion']['E1'] for f in pair]), dE2=ms([on[f]['completion']['E2'] - off[f]['completion']['E2'] for f in pair]), dE3=ms([on[f]['completion']['E3'] - off[f]['completion']['E3'] for f in pair]),
                 size_age1=g(lambda v: v['completion']['mae_lD']), E1_age1=g(lambda v: v['completion']['E1']),
                 G1_d70_mae=gs('G1_d70', 'mae_joint'), G1_d70_bias=gs('G1_d70', 'bias_joint'), G5_d70=gs('G5_d70', 'cover80'))
        o['planted_bias'] = {c: ms([v['planted_field'][c]['bias'] for v in on.values() if c in v['planted_field']]) for c in sorted({c for v in on.values() for c in v['planted_field']})}
        o['planted_cover'] = {c: ms([v['planted_field'][c]['cover80'] for v in on.values() if c in v['planted_field']]) for c in o['planted_bias']}
        gc = {f: grid_completion(a.out, int(f[1:]), e) for f in on}; gc = {f: v for f, v in gc.items() if v}
        o['grid_completion'] = {x: ms([v[x] for v in gc.values()]) for x in ('mae_lD', 'E1', 'E2', 'E3')} if gc else None   # D77, reported
        if a.data:                                                                        # D81: the documented old-tree limitation, re-measured every run
            gf = [f'{a.out}/{f}_age1/g1_e{e}.parquet' for f in sorted(on)]; gf = [x for x in gf if os.path.exists(x)]
            if gf: o['age_limits'] = S.age_limits(pd.concat([pd.read_parquet(x) for x in gf]), a.data)
        if BL:                                                                            # the same folds as the joint scores, and the same trees (checked)
            o['baselines'] = {}
            for nm in ('G1', 'G1_d70'):
                fs = [f for f in on if nm in on[f]]
                if not fs: continue
                bad = [f for f in fs if BL[nm][f[1:]]['n'] != on[f][nm]['n']]
                o['baselines'][nm] = dict(joint=ms([on[f][nm]['mae_joint'] for f in fs]), same_trees=not bad, folds_n_mismatch=bad,
                                          **{x: ms([BL[nm][f[1:]][x] for f in fs]) for x in ('training_mean', 'training_median', 'species_median', 'species_dia_median', 'direct_prior', 'current_ages')})
        if o['G1_mae'] and pair:
            o['bars'] = dict(G1=bool(o['G1_mae']['mean'] <= BARS['G1_mae'] and abs(o['G1_bias']['mean']) <= BARS['G1_bias'] and o['G1_mae']['mean'] <= o['G1_direct']['mean']), G5=bool(BARS['G5'][0] <= o['G5']['mean'] <= BARS['G5'][1]),
                             G6=bool(o['size_ratio']['mean'] <= BARS['G6_size'] and o['dE2']['mean'] <= BARS['G6_brier'] and o['dE3']['mean'] <= BARS['G6_brier']), G8=bool(o['dE1']['mean'] <= BARS['G8']))
        out[e] = o
    c = {}
    for f in sorted(glob.glob(f'{a.out}/f*_age*/curve.json')):
        for r in json.load(open(f)): c.setdefault((os.path.basename(os.path.dirname(f)).split('_')[1], r['epoch']), []).append(r)
    cu = {f'{k[0]} e{k[1]}': {x: float(np.mean([r[x] for r in v])) for x in ('fit_bound', 'monitor_bound', 'gap', 'monitor_ess')} for k, v in sorted(c.items())}
    json.dump(dict(by_epoch=out, curve=cu, baselines_by_fold=BL), open(f'{a.out}/cv_summary.json', 'w'), indent=1); print(json.dumps(out, indent=0)[:6000]); [print(k, {x: round(y, 3) for x, y in v.items()}) for k, v in cu.items()]
    names = ('training_mean', 'training_median', 'species_median', 'species_dia_median', 'current_ages', 'direct_prior', 'joint')
    for e, o in out.items():                                                              # the G1 table: lookups, today's ages, the direct prior, the joint model
        for nm, bl in (o.get('baselines') or {}).items():
            print(f'G1 MAE (yr), epoch {e}, {nm} ({bl["joint"]["n"]} folds{"" if bl["same_trees"] else ", TREE COUNTS DIFFER: " + str(bl["folds_n_mismatch"])}): '
                  + ' | '.join(f'{x.replace("_", " ")} {bl[x]["mean"]:.2f}' for x in names))

def selftest():
    keys = [f'1_1_{i}_{j}' for i in range(40) for j in range(40)]; seen = set()
    for f in range(NF):
        h, m, t = split_plots(keys, f); assert not (set(h) & set(m)) and not (set(h) & set(t)) and not (set(m) & set(t)) and len(h) + len(m) + len(t) == len(keys) and not (seen & set(h)); seen |= set(h)
    assert seen == set(keys); print('selftest: folds partition the plots PASS')

if __name__ == '__main__':
    ap = argparse.ArgumentParser(); ap.add_argument('--data'); ap.add_argument('--out'); ap.add_argument('--fold', type=int, default=0); ap.add_argument('--age', type=int, default=1); ap.add_argument('--device', default='cpu')
    ap.add_argument('--trees', type=int, default=400000); ap.add_argument('--ckpts', default='12,24,36'); ap.add_argument('--curve-every', type=int, default=2); ap.add_argument('--curve-plots', type=int, default=500); ap.add_argument('--score-frac', type=float, default=0.5); ap.add_argument('--planted-frac', type=float, default=0.25)
    ap.add_argument('--k', type=int, default=32); ap.add_argument('--bs', type=int, default=512); ap.add_argument('--threads', type=int, default=8); ap.add_argument('--seed', type=int, default=0)
    ap.add_argument('--d74', type=int, default=0); ap.add_argument('--d76', type=int, default=0); ap.add_argument('--backend', choices=('torch', 'jax'), default='torch'); ap.add_argument('--compare', help='cv72 rescored with --rescore-g1: evaluate the D74 bars of --out against it'); ap.add_argument('--rescore-g1', help='a finished run without D74: score its age-on checkpoints on both G1 sets'); ap.add_argument('--q', choices=('gauss', 'flow'), default='gauss'); ap.add_argument('--flow-layers', type=int, default=6); ap.add_argument('--flow-hidden', type=int, default=64); ap.add_argument('--flow-bins', type=int, default=8)
    ap.add_argument('--summary', action='store_true'); ap.add_argument('--layers', type=int, default=2); ap.add_argument('--width', type=int, default=64); ap.add_argument('--compare82', action='store_true', help='D82: compare out/d82_* (fold 0)')
    ap.add_argument('--rescore-grid', help='a finished run: D77 grid completion of its last age-on checkpoint (scores_grid.json)'); ap.add_argument('--d76-base', help='cv74flow: evaluate the D76 bars of --out (cv76flow) against it'); ap.add_argument('--rescore', help='old out/cv: rescore its saved age-off checkpoints'); ap.add_argument('--selftest', action='store_true'); a = ap.parse_args()
    selftest() if a.selftest else compare82(a) if a.compare82 else rescore_grid(a) if a.rescore_grid else compare_d76(a) if a.d76_base else compare(a) if a.compare else summary(a) if a.summary else rescore_g1(a) if a.rescore_g1 else rescore(a) if a.rescore else run(a)
