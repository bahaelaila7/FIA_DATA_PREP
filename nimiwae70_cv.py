"""D70 am. 15: cross-validation of the joint age model on TRAIN plots, self-contained for a GPU machine.
  python -I nimiwae70_cv.py --data data --out out/cv --fold 0 --age 1 --device cuda        one run: trains, tracks held-out plots by epoch, scores checkpoints
  python -I nimiwae70_cv.py --out out/cv --summary                                         pools the finished runs against the bars
Folds: crc32(plot_key|ncv) % 5, the D54 rule, on TRAIN plots. VAL carried design choices and TEST stays untouched. Monitor: 10 % of the fitting plots, crc32(plot_key|ncv-in) % 10 == 0, never trained on and never scored.
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
    if os.path.exists(f'{d}/done.json'): print('already done', d); return
    tr = pd.read_parquet(f'{a.data}/seq.parquet', filters=[('split', '==', 'train')])       # TRAIN rows only: several runs load this at once
    site, planted = pd.read_parquet(f'{a.data}/site_refs.parquet'), pd.read_parquet(f'{a.data}/planted_refs.parquet')
    held_k, mon_k, fit_k = split_plots(sorted(tr.plot_key.unique()), a.fold)
    held, mon, fit = (tr[tr.plot_key.isin(set(x))] for x in (held_k, mon_k, fit_k))
    log_f = open(f'{d}/run.log', 'a'); pr = lambda s: (print(f'[f{a.fold} age{a.age}] {s}', flush=True), log_f.write(s + '\n'), log_f.flush())
    pr(f'plots held {len(held_k):,}, monitor {len(mon_k):,}, fit {len(fit_k):,}')
    score_plots = held[held.plot_key.isin(pd.Series(held_k).sample(frac=a.score_frac, random_state=2))]; ckpts = sorted(int(x) for x in a.ckpts.split(',')); state = dict(curve=[], ckpt={}, Bm=None, Bt=None)
    rng = np.random.default_rng(a.seed); mon_s = mon[mon.plot_key.isin(set(rng.permutation(mon_k)[:a.curve_plots]))]; fit_s = fit[fit.plot_key.isin(set(rng.permutation(fit_k)[:a.curve_plots]))]
    def hook(ep, m, h):
        if ep % a.curve_every and ep not in ckpts: return
        m.eval()
        if state['Bm'] is None: state['Bm'] = A.build(mon_s, m.vocab)[0]; state['Bt'] = A.build(fit_s, m.vocab)[0]
        dev = next(m.parameters()).device; lm, em = bound(m, state['Bm'], a.k, dev); lt, et = bound(m, state['Bt'], a.k, dev)
        state['curve'].append(dict(epoch=ep, fit_bound=lt, monitor_bound=lm, gap=lt - lm, fit_ess=et, monitor_ess=em)); pr(json.dumps(state['curve'][-1])); json.dump(state['curve'], open(f'{d}/curve.json', 'w'), indent=0)
        if ep in ckpts:
            t0 = time.time(); o = dict(completion=S.completion(m, score_plots))
            if a.age: o.update(S.age_scores(m, held, site, planted, planted_frac=a.planted_frac))
            state['ckpt'][str(ep)] = o; json.dump(state['ckpt'], open(f'{d}/scores.json', 'w'), indent=1, default=float); pr(f'scored epoch {ep} in {time.time() - t0:.0f} s: {json.dumps(o, default=float)[:300]}')
            torch.save(dict(state={k: v.cpu() for k, v in m.state_dict().items()}, vocab=m.vocab, cfg=m.cfg), f'{d}/model_e{ep}.pt')
        m.train()
    t0 = time.time(); A.train(fit, epochs=max(ckpts), k=a.k, bs=a.bs, max_trees=a.trees, seed=a.seed, threads=a.threads, log=pr, age=bool(a.age), device=a.device, opt_name='sf', on_epoch=hook)
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

def summary(a):
    R = {}
    for f in sorted(glob.glob(f'{a.out}/f*_age*/scores.json')):
        n = os.path.basename(os.path.dirname(f)); R[n] = json.load(open(f))
    eps = sorted({int(e) for v in R.values() for e in v}); out = {}
    ms = lambda v: dict(mean=float(np.mean(v)), sd=float(np.std(v, ddof=1)) if len(v) > 1 else None, n=len(v)) if v else None
    for e in map(str, eps):
        on = {n[:n.index('_')]: v[e] for n, v in R.items() if n.endswith('age1') and e in v}; off = {n[:n.index('_')]: v[e] for n, v in R.items() if n.endswith('age0') and e in v}
        pair = sorted(set(on) & set(off)); g = lambda p: ms([p(v) for v in on.values()])
        o = dict(folds_age1=len(on), folds_age0=len(off), G1_mae=g(lambda v: v['G1']['mae_joint']), G1_bias=g(lambda v: v['G1']['bias_joint']), G1_direct=g(lambda v: v['G1']['mae_direct']), G1_prior=g(lambda v: v['G1']['mae_prior']), G1_current=g(lambda v: v['G1']['mae_current']), G5=g(lambda v: v['G5']['cover80']),
                 size_ratio=ms([on[f]['completion']['mae_lD'] / off[f]['completion']['mae_lD'] for f in pair]), dE1=ms([on[f]['completion']['E1'] - off[f]['completion']['E1'] for f in pair]), dE2=ms([on[f]['completion']['E2'] - off[f]['completion']['E2'] for f in pair]), dE3=ms([on[f]['completion']['E3'] - off[f]['completion']['E3'] for f in pair]),
                 size_age1=g(lambda v: v['completion']['mae_lD']), E1_age1=g(lambda v: v['completion']['E1']))
        o['planted_bias'] = {c: ms([v['planted_field'][c]['bias'] for v in on.values() if c in v['planted_field']]) for c in sorted({c for v in on.values() for c in v['planted_field']})}
        o['planted_cover'] = {c: ms([v['planted_field'][c]['cover80'] for v in on.values() if c in v['planted_field']]) for c in o['planted_bias']}
        if o['G1_mae'] and pair:
            o['bars'] = dict(G1=bool(o['G1_mae']['mean'] <= BARS['G1_mae'] and abs(o['G1_bias']['mean']) <= BARS['G1_bias'] and o['G1_mae']['mean'] <= o['G1_direct']['mean']), G5=bool(BARS['G5'][0] <= o['G5']['mean'] <= BARS['G5'][1]),
                             G6=bool(o['size_ratio']['mean'] <= BARS['G6_size'] and o['dE2']['mean'] <= BARS['G6_brier'] and o['dE3']['mean'] <= BARS['G6_brier']), G8=bool(o['dE1']['mean'] <= BARS['G8']))
        out[e] = o
    c = {}
    for f in sorted(glob.glob(f'{a.out}/f*_age*/curve.json')):
        for r in json.load(open(f)): c.setdefault((os.path.basename(os.path.dirname(f)).split('_')[1], r['epoch']), []).append(r)
    cu = {f'{k[0]} e{k[1]}': {x: float(np.mean([r[x] for r in v])) for x in ('fit_bound', 'monitor_bound', 'gap', 'monitor_ess')} for k, v in sorted(c.items())}
    json.dump(dict(by_epoch=out, curve=cu), open(f'{a.out}/cv_summary.json', 'w'), indent=1); print(json.dumps(out, indent=0)[:6000]); [print(k, {x: round(y, 3) for x, y in v.items()}) for k, v in cu.items()]

def selftest():
    keys = [f'1_1_{i}_{j}' for i in range(40) for j in range(40)]; seen = set()
    for f in range(NF):
        h, m, t = split_plots(keys, f); assert not (set(h) & set(m)) and not (set(h) & set(t)) and not (set(m) & set(t)) and len(h) + len(m) + len(t) == len(keys) and not (seen & set(h)); seen |= set(h)
    assert seen == set(keys); print('selftest: folds partition the plots PASS')

if __name__ == '__main__':
    ap = argparse.ArgumentParser(); ap.add_argument('--data'); ap.add_argument('--out'); ap.add_argument('--fold', type=int, default=0); ap.add_argument('--age', type=int, default=1); ap.add_argument('--device', default='cpu')
    ap.add_argument('--trees', type=int, default=400000); ap.add_argument('--ckpts', default='12,24,36'); ap.add_argument('--curve-every', type=int, default=2); ap.add_argument('--curve-plots', type=int, default=500); ap.add_argument('--score-frac', type=float, default=0.5); ap.add_argument('--planted-frac', type=float, default=0.25)
    ap.add_argument('--k', type=int, default=32); ap.add_argument('--bs', type=int, default=512); ap.add_argument('--threads', type=int, default=8); ap.add_argument('--seed', type=int, default=0)
    ap.add_argument('--summary', action='store_true'); ap.add_argument('--rescore', help='old out/cv: rescore its saved age-off checkpoints'); ap.add_argument('--selftest', action='store_true'); a = ap.parse_args()
    selftest() if a.selftest else summary(a) if a.summary else rescore(a) if a.rescore else run(a)
