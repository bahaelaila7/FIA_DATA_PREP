"""D76's primary check: how often the age gate pulls a large late-debut tree implausibly young.
  python -I nimiwae70_gatepull.py --data data --run out/cv74flow --epoch 36 --device cuda          the baseline (cv74flow)
  python -I nimiwae70_gatepull.py --data data --run out/cv76flow --epoch 36 --device cuda          the D76 run
Held trees of each fold, DIA >= 5 in at the prior's slot, whose visit just before their first visible record was sampled, unrecorded, in the
cutoff mode (no missed code) and at subplot level: the share whose posterior median of the age latent z0 is below -2 (also -1, -3), on the product
grid (nimiwae70.PRODUCT_GRID, wide enough that no posterior is clipped). Microplot-level debuts are reported alongside (not a bar).
Writes RUN/gatepull_e{epoch}.json: counts per fold and pooled (pooled share = sum of counts / sum of trees).
--eps-scale S (D76's reported sensitivity, not a bar): rescore a D76 run with its calibrated eps times S (no retraining), into gatepull_e{epoch}_eps{S}.json.
--tag T: the same check under another name, gatepull_e{epoch}[_eps{S}]_T.json, e.g. a run's checkpoints rescored on another frame (--data) or a probe arm;
  untagged files (the D76 verdict's inputs) are never overwritten by a tagged run.
--report RUN: every gatepull file of RUN side by side (tag, frame, eps scale, coverage, pooled shares)."""
import sys, os, json, argparse, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd, torch
import nimiwae56 as M, nimiwae70 as A, nimiwae70_cv as C, nimiwae70_score as S

CUTS = (-1.0, -2.0, -3.0)
BAR = 0.0010                      # D76: pooled share below -2 must be at most 0.10 %


def late_debut_masks(B):
    """-> (subplot-level, microplot-level) [n] masks of trees >= 5 in at la whose pre-debut visit was sampled, unrecorded and in the cutoff mode"""
    n = B['v'].shape[0]; ii = torch.arange(n); f = B['f']; fp = (f - 1).clamp(min=0)
    base = (f > 0) & (B['sampled'][ii, fp] > 0) & (B['real'][ii, fp] == 0) & (B['mode'][ii, fp] == M.MODE['cutoff']) & (B['dia_la'] >= 5.0)
    sub = B['llim'][ii, fp] > B['lex'][ii, fp] + 1e-6
    return base & sub, base & ~sub


@torch.no_grad()
def z50(m, B, dev, bs=512, K=100):
    out = []
    for i in range(0, B['v'].shape[0], bs):
        b = {k: v.to(dev) for k, v in M.sub(B, slice(i, i + bs)).items()}
        g, p = A.age_posterior(m, b, K=K, **A.PRODUCT_GRID); out.append(A.z_quantiles(g.cpu(), p.cpu(), [0.5])[0])
    return torch.cat(out) if out else torch.zeros(0)


def report(run):
    """every gatepull_e*.json of a run, side by side"""
    import glob
    rows = []
    for f in sorted(glob.glob(f'{run}/gatepull_e*.json')):
        g = json.load(open(f)); p = g['pooled']['subplot']
        rows.append(dict(file=os.path.basename(f), tag=g.get('tag', ''), frame=g.get('frame', 'pre-d78'), epoch=g['epoch'], eps_scale=g.get('eps_scale', 1.0), frac=g['frac'],
                         folds=','.join(sorted(g['folds'])), trees=p['n'], trees_by_fold='/'.join(str(g['folds'][k]['subplot']['n']) for k in sorted(g['folds'])), below_2=p['below_-2'], share_below_2=round(100 * p['share_below_-2'], 3), share_below_3=round(100 * p['share_below_-3'], 3)))
    print(f'{run}: subplot-level late large debuts, shares in % (the population depends on the data only: trees_by_fold must agree across frames)')
    print(pd.DataFrame(rows).to_string(index=False) if rows else '  no gatepull files')

def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--data', required=True); ap.add_argument('--run', required=True); ap.add_argument('--epoch', type=int, default=36)
    ap.add_argument('--folds', default='0,1,2,3,4'); ap.add_argument('--frac', type=float, default=1.0, help='share of each fold\'s held plots (1 = all)')
    ap.add_argument('--eps-scale', type=float, default=1.0); ap.add_argument('--tag', default=''); ap.add_argument('--device', default='cpu'); ap.add_argument('--threads', type=int, default=8); a = ap.parse_args(); torch.set_num_threads(a.threads)
    k = M.K_ + ['invyr']; tr = pd.read_parquet(f'{a.data}/seq.parquet', filters=[('split', '==', 'train')]); keys = sorted(tr.plot_key.unique())
    import pyarrow.parquet as pq
    frame = 'd78+' if 'l0' in pq.read_schema(f'{a.data}/seq.parquet').names else 'pre-d78'    # D78 frames carry the per-tree l0 (and the D79 prior)
    R = dict(run=a.run, epoch=a.epoch, frac=a.frac, bar=BAR, eps_scale=a.eps_scale, tag=a.tag, frame=frame, eps={}, folds={})
    for f in map(int, a.folds.split(',')):
        t0 = time.time(); m, ck = S.load(f'{a.run}/f{f}_age1', f'model_e{a.epoch}.pt'); m.to(a.device)
        held_k, _, _ = C.split_plots(keys, f); hk = pd.Series(held_k)
        if a.frac < 1: hk = hk.sample(frac=a.frac, random_state=5)
        held = tr[tr.plot_key.isin(set(hk))]
        if m.height == 't': held = held.merge(pd.read_parquet(f'{a.data}/ht_refs.parquet'), on=k, how='left')   # the inputs the model trained with
        B, _, _ = A.build(held, m.vocab); R['eps'][str(f)] = m.vocab.get('miss_eps')                    # build applies the run's own eps (D76 runs)
        if a.eps_scale != 1.0:
            assert 'miss_eps' in m.vocab, '--eps-scale needs a D76 run'; A.apply_miss(B, m.vocab['miss_eps'] * a.eps_scale)
        sub, mic = late_debut_masks(B); o = {}
        for lab, msk in (('subplot', sub), ('microplot', mic)):
            ix = torch.nonzero(msk).flatten(); torch.manual_seed(0); z = z50(m, M.sub(B, ix), a.device)
            o[lab] = dict(n=int(len(z)), **{f'below_{c:g}': int((z < c).sum()) for c in CUTS})
        o['trees'] = int(B['v'].shape[0]); o['seconds'] = round(time.time() - t0); R['folds'][str(f)] = o
        print(f'fold {f}: {json.dumps(o)}', flush=True)
    pool = {lab: {x: sum(R['folds'][f][lab][x] for f in R['folds']) for x in R['folds'][next(iter(R['folds']))][lab]} for lab in ('subplot', 'microplot')}
    for lab in pool: pool[lab].update({f'share_below_{c:g}': pool[lab][f'below_{c:g}'] / max(1, pool[lab]['n']) for c in CUTS})
    R['pooled'] = pool; R['pass'] = bool(pool['subplot']['share_below_-2'] <= BAR) if a.eps_scale == 1.0 else None   # the bar is on the run as trained
    out = f'{a.run}/gatepull_e{a.epoch}' + ('' if a.eps_scale == 1.0 else f'_eps{a.eps_scale:g}') + (f'_{a.tag}' if a.tag else '') + '.json'
    if a.tag: R['pass'] = None                                                            # a tagged run is a reading, not the D76 bar
    json.dump(R, open(out, 'w'), indent=1); print(json.dumps(R['pooled'], indent=1))
    if a.tag: print(f'tagged reading "{a.tag}" on a {frame} frame (not the D76 bar) -> {out}')
    elif R['pass'] is not None: print('D76 primary (subplot share below -2 <= 0.10 %):', 'PASS' if R['pass'] else 'FAIL')
    else: print(f'sensitivity eps x{a.eps_scale:g} (reported, not a bar) -> {out}')


if __name__ == '__main__':
    if '--report' in sys.argv: report(sys.argv[sys.argv.index('--report') + 1])
    else: main()
