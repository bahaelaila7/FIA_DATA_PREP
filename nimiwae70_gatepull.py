"""D76's primary check: how often the age gate pulls a large late-debut tree implausibly young.
  python -I nimiwae70_gatepull.py --data data --run out/cv74flow --epoch 36 --device cuda          the baseline (cv74flow)
  python -I nimiwae70_gatepull.py --data data --run out/cv76flow --epoch 36 --device cuda          the D76 run
Held trees of each fold, DIA >= 5 in at the prior's slot, whose visit just before their first visible record was sampled, unrecorded, in the
cutoff mode (no missed code) and at subplot level: the share whose posterior median of the age latent z0 is below -2 (also -1, -3), on the product
grid (nimiwae70.PRODUCT_GRID, wide enough that no posterior is clipped). Microplot-level debuts are reported alongside (not a bar).
Writes RUN/gatepull_e{epoch}.json: counts per fold and pooled (pooled share = sum of counts / sum of trees)."""
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


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--data', required=True); ap.add_argument('--run', required=True); ap.add_argument('--epoch', type=int, default=36)
    ap.add_argument('--folds', default='0,1,2,3,4'); ap.add_argument('--frac', type=float, default=1.0, help='share of each fold\'s held plots (1 = all)')
    ap.add_argument('--device', default='cpu'); ap.add_argument('--threads', type=int, default=8); a = ap.parse_args(); torch.set_num_threads(a.threads)
    k = M.K_ + ['invyr']; tr = pd.read_parquet(f'{a.data}/seq.parquet', filters=[('split', '==', 'train')]); keys = sorted(tr.plot_key.unique())
    R = dict(run=a.run, epoch=a.epoch, frac=a.frac, bar=BAR, folds={})
    for f in map(int, a.folds.split(',')):
        t0 = time.time(); m, ck = S.load(f'{a.run}/f{f}_age1', f'model_e{a.epoch}.pt'); m.to(a.device)
        held_k, _, _ = C.split_plots(keys, f); hk = pd.Series(held_k)
        if a.frac < 1: hk = hk.sample(frac=a.frac, random_state=5)
        held = tr[tr.plot_key.isin(set(hk))]
        if m.height == 't': held = held.merge(pd.read_parquet(f'{a.data}/ht_refs.parquet'), on=k, how='left')   # the inputs the model trained with
        B, _, _ = A.build(held, m.vocab); sub, mic = late_debut_masks(B); o = {}
        for lab, msk in (('subplot', sub), ('microplot', mic)):
            ix = torch.nonzero(msk).flatten(); torch.manual_seed(0); z = z50(m, M.sub(B, ix), a.device)
            o[lab] = dict(n=int(len(z)), **{f'below_{c:g}': int((z < c).sum()) for c in CUTS})
        o['trees'] = int(B['v'].shape[0]); o['seconds'] = round(time.time() - t0); R['folds'][str(f)] = o
        print(f'fold {f}: {json.dumps(o)}', flush=True)
    pool = {lab: {x: sum(R['folds'][f][lab][x] for f in R['folds']) for x in R['folds'][next(iter(R['folds']))][lab]} for lab in ('subplot', 'microplot')}
    for lab in pool: pool[lab].update({f'share_below_{c:g}': pool[lab][f'below_{c:g}'] / max(1, pool[lab]['n']) for c in CUTS})
    R['pooled'] = pool; R['pass'] = bool(pool['subplot']['share_below_-2'] <= BAR)
    json.dump(R, open(f'{a.run}/gatepull_e{a.epoch}.json', 'w'), indent=1); print(json.dumps(R['pooled'], indent=1), '\nD76 primary (subplot share below -2 <= 0.10 %):', 'PASS' if R['pass'] else 'FAIL')


if __name__ == '__main__':
    main()
