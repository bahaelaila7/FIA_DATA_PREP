"""D76's primary check: how often the age gate pulls a large late-debut tree implausibly young.
  python -I nimiwae70_gatepull.py --data data --run out/cv74flow --epoch 36 --device cuda          the baseline (cv74flow)
  python -I nimiwae70_gatepull.py --data data --run out/cv76flow --epoch 36 --device cuda          the D76 run
D85: every z here is on the age scale (z_age = sigma_L z0 below the prior median; identical to the latent z0 when sigma_L = 1).
Held trees of each fold, DIA >= 5 in at the prior's slot, whose visit just before their first visible record was sampled, unrecorded, in the
cutoff mode (no missed code) and at subplot level: the share whose posterior median of the age latent z0 is below -2 (also -1, -3), on the product
grid (nimiwae70.PRODUCT_GRID, wide enough that no posterior is clipped). Microplot-level debuts are reported alongside (not a bar).
Writes RUN/gatepull_e{epoch}.json: counts per fold and pooled (pooled share = sum of counts / sum of trees).
--eps-scale S (D76's reported sensitivity, not a bar): rescore a D76 run with its calibrated eps times S (no retraining), into gatepull_e{epoch}_eps{S}.json.
--tag T: the same check under another name, gatepull_e{epoch}[_eps{S}]_T.json, e.g. a run's checkpoints rescored on another frame (--data) or a probe arm;
  untagged files (the D76 verdict's inputs) are never overwritten by a tagged run.
--report RUN: every gatepull file of RUN side by side (tag, frame, eps scale, coverage, pooled shares).
--dump (D84): also write every pulled subplot-level tree (posterior median z0 < -2) plus a fixed random control set of the others (CONTROLS per fold,
  seed = fold) with their empty-visit history, sizes, growth, prior and posterior, to <output>_dump.parquet.
--analyse-dump FILE (D84): the pre-registered prediction (>= 80 % of z0 < -3 pulls have >= 2 sampled empty pre-debut visits) and the tables behind it."""
import sys, os, json, argparse, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd, torch
import nimiwae56 as M, nimiwae70 as A, nimiwae70_cv as C, nimiwae70_score as S

CUTS = (-1.0, -2.0, -3.0)
CONTROLS = 2000                   # D84: non-pulled late large debuts kept per fold as the comparison set
BAND = (-2.84, -2.0)              # D84: one empty visit can pull z0 this far (z0^2 / 2 < -ln eps ~ 4.0 nats): the model obeying eps
PRED_SHARE = 0.80                 # D84 am. 1 (pre-registered): share of z0 < -3 pulls with >= 2 TALLY-SIZE empty pre-debut visits ...
PRED_LIFT = 0.20                  # ... and at least this much above the controls' share (the base rate of empty visits is high)
ENVELOPE = ((15, 1.08), (25, 0.91), (40, 0.67), (1e9, 0.50))   # D84 (reported): 99.5 % of site trees' DIA / AGEDIA by AGEDIA class (in / yr)
BAR = 0.0010                      # D76: pooled share below -2 must be at most 0.10 %


def late_debut_masks(B):
    """-> (subplot-level, microplot-level) [n] masks of trees >= 5 in at la whose pre-debut visit was sampled, unrecorded and in the cutoff mode"""
    n = B['v'].shape[0]; ii = torch.arange(n); f = B['f']; fp = (f - 1).clamp(min=0)
    base = (f > 0) & (B['sampled'][ii, fp] > 0) & (B['real'][ii, fp] == 0) & (B['mode'][ii, fp] == M.MODE['cutoff']) & (B['dia_la'] >= 5.0)
    sub = B['llim'][ii, fp] > B['lex'][ii, fp] + 1e-6
    return base & sub, base & ~sub


def empty_visits(B):
    """D84: [n, T] sampled, unrecorded, cutoff-mode, subplot-level slots before the first visible record (where the miss term and the birth gate
    compete), and the fully sampled ones among them"""
    ar = torch.arange(B['v'].shape[1])[None]
    e = (ar < B['f'][:, None]) & (B['v'] > 0) & (B['sampled'] > 0) & (B['real'] == 0) & (B['mode'] == M.MODE['cutoff']) & (B['llim'] > B['lex'] + 1e-6)
    return e, e & (B['psamp'] >= 1)

def tree_table(B, sq, ix, zq, fold):
    """D84: one row per tree of ix (indices into B) with z0 quantiles zq [3, len(ix)]"""
    ix = ix.numpy(); e, ef = empty_visits(B); t = (B['cov'][..., A.ITYR] * 20.0).numpy(); f = B['f'].numpy(); la = B['la'].numpy(); ii = np.arange(B['v'].shape[0])
    te = np.where(e.numpy(), t, np.nan); first_e, last_e = np.nanmin(te, 1), np.nanmax(te, 1)
    tf, tla = t[ii, f], t[ii, la]; mu = (B['af_mu'] + B['af_shift']).numpy(); sd = (B['af_sd'] * B['af_sdf']).numpy()
    deb = sq.assign(_f=f[sq._tid.to_numpy()]); deb = deb[(deb._pos == deb._f) & (deb.slot_kind == 'real')].drop_duplicates('_tid').set_index('_tid')
    cols = [c for c in ('spcd', 'stdorgcd_own', 'sftwd_hrdwd', 'rcd0', 'cclcd', 'dia') if c in deb]
    d = pd.DataFrame({k: deb[k].reindex(ix).to_numpy() for k in M.K_ + cols}).rename(columns={'dia': 'dia_debut', 'rcd0': 'rcd_debut', 'cclcd': 'cc_debut'})
    z = zq.numpy()
    d = d.assign(fold=fold, z10=z[0], z50=z[1], z90=z[2], k_empty=e.numpy()[ix].sum(1), k_empty_full=ef.numpy()[ix].sum(1),
                 yrs_since_first_empty=(tf - first_e)[ix], yrs_since_last_empty=(tf - last_e)[ix], dia_la=B['dia_la'].numpy()[ix], has_rate=B['has_rate'].numpy()[ix],
                 rate=np.where(B['has_rate'].numpy()[ix] > 0, np.exp(B['lrate'].numpy()[ix]), np.nan), cc_la=B['cc_la'].numpy()[ix], l0=B['l0'].numpy()[ix],
                 prior_median_at_debut=(np.exp(mu) + tf - tla)[ix], prior_sd=sd[ix], posterior_median_at_debut=np.exp(mu[ix] + sd[ix] * z[1]) + (tf - tla)[ix])
    lim = (torch.exp(B['llim']) + M.HALF_UNIT).numpy(); dd = np.where(B['mD'].numpy()[ii, f] > 0, np.exp(B['lD'].numpy()[ii, f]), np.nan)
    nr = np.where(e.numpy(), (dd[:, None] - lim) / np.clip(tf[:, None] - t, 0.1, None), np.nan)   # growth rate "too small at that visit" would need
    d['needed_rates'] = [sorted(r_[~np.isnan(r_)].round(4).tolist(), reverse=True) for r_ in nr[ix]]
    d['needed_rate_last'] = [nr[i_][~np.isnan(nr[i_])][np.argmax(t[i_][~np.isnan(nr[i_])])] if (~np.isnan(nr[i_])).any() else np.nan for i_ in ix]
    bh = d.posterior_median_at_debut - d.l0; env = np.select([bh < c for c, _ in ENVELOPE], [g for _, g in ENVELOPE], ENVELOPE[-1][1])
    d['implied_bh_rate'] = d.dia_debut / bh.clip(lower=0.5); d['above_envelope'] = d.implied_bh_rate > env    # reported: growth since breast height the posterior implies
    return d

def analyse(path):
    """D84: the pre-registered prediction and the tables behind it"""
    d = pd.read_parquet(path); pulled = d.z50 < -2.0
    grp = np.select([d.z50 < -3.0, (d.z50 < BAND[1]) & (d.z50 >= BAND[0]), pulled], ['z0 < -3', 'band -2.84..-2', '-3..-2.84'], 'control')
    d = d.assign(group=grp, k_bucket=np.where(d.k_empty >= 3, '3+', d.k_empty.astype(str)), stratum=np.where(d.stdorgcd_own == 1, 'planted', 'natural') + '/' + d.sftwd_hrdwd.astype(str))
    # tally-size empty visits: "too small then" would need more than the tree's own observed rate (else its stratum's median observed rate, from the controls)
    sr = d[(d.group == 'control') & (d.has_rate > 0)].groupby('stratum').rate.median(); r_eff = np.where(d.has_rate > 0, d.rate, d.stratum.map(sr).fillna(float(sr.median())))
    d['k_tally'] = [int(sum(x > r for x in nrs)) for nrs, r in zip(d.needed_rates, r_eff)]; d['kt_bucket'] = np.where(d.k_tally >= 3, '3+', d.k_tally.astype(str))
    ext = d[d.group == 'z0 < -3']; ctl = d[d.group == 'control']
    share = float((ext.k_tally >= 2).mean()) if len(ext) else None; base = float((ctl.k_tally >= 2).mean()) if len(ctl) else None
    tab = lambda c: (pd.crosstab(d.group, d[c], normalize='index') * 100).round(1)
    R = dict(file=os.path.basename(path), n_by_group=d.group.value_counts().to_dict(),
             prediction=dict(share_z_below_m3_with_2plus_tally_empty=share, controls_share=base, needed=PRED_SHARE, needed_lift=PRED_LIFT, n=int(len(ext)),
                             holds=bool(share is not None and base is not None and share >= PRED_SHARE and share - base >= PRED_LIFT)),
             stratum_median_rates_used=sr.round(3).to_dict(), k_tally_pct=tab('kt_bucket').to_dict(),
             k_empty_pct=tab('k_bucket').to_dict(), stratum_pct=tab('stratum').to_dict(), rcd_debut_pct=tab('rcd_debut').to_dict(),
             medians=d.groupby('group')[['dia_debut', 'rate', 'yrs_since_last_empty', 'prior_median_at_debut', 'posterior_median_at_debut', 'prior_sd']].median().round(3).to_dict(),
             above_envelope_pct=(d.groupby('group').above_envelope.mean() * 100).round(1).to_dict())
    out = path.replace('.parquet', '_analysis.json'); json.dump(R, open(out, 'w'), indent=1, default=str)
    print(json.dumps(R['prediction'], indent=1)); print('tally-size empty visits (% of group):'); print(tab('kt_bucket').to_string()); print('all empty visits (% of group):'); print(tab('k_bucket').to_string()); print('stratum (% of group):'); print(tab('stratum').to_string())
    print('medians:'); print(d.groupby('group')[['dia_debut', 'rate', 'yrs_since_last_empty', 'prior_median_at_debut', 'posterior_median_at_debut']].median().round(2).to_string())
    print('implied growth since breast height above the site-tree envelope (%):', R['above_envelope_pct']); print('->', out)
    return R

def selftest():
    n, T = 3, M.T_MAX; B = dict(v=torch.zeros(n, T), sampled=torch.zeros(n, T), psamp=torch.zeros(n, T), real=torch.zeros(n, T), mode=torch.zeros(n, T, dtype=torch.long),
                                llim=torch.full((n, T), np.log(4.95)), lex=torch.full((n, T), np.log(0.95)), f=torch.tensor([3, 2, 0]))
    B['v'][:, :5] = 1; B['sampled'][:, :5] = 1; B['psamp'][:, :5] = 1; B['mode'][:] = M.MODE['cutoff']
    B['psamp'][0, 1] = 0.5; B['llim'][1, 0] = B['lex'][1, 0]; B['mode'][0, 2] = M.MODE['missed']
    e, ef = empty_visits(B)
    assert e.sum(1).tolist() == [2, 1, 0] and ef.sum(1).tolist() == [1, 1, 0], (e.sum(1), ef.sum(1))   # tree 0: slots 0, 1 (slot 2 coded missed), one partly sampled
    print('selftest: empty-visit rule PASS')

@torch.no_grad()
def zq(m, B, dev, bs=512, K=100, qs=(0.1, 0.5, 0.9)):
    """z0 quantiles [len(qs), n] of the exact grid posterior on the product grid (z50's computation, more quantiles)"""
    out = []
    for i in range(0, B['v'].shape[0], bs):
        b = {k: v.to(dev) for k, v in M.sub(B, slice(i, i + bs)).items()}
        g, p = A.age_posterior(m, b, K=K, **A.PRODUCT_GRID); out.append(A.z_quantiles(g.cpu(), p.cpu(), list(qs)))
    return torch.cat(out, 1) if out else torch.zeros(len(qs), 0)

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
        g = json.load(open(f))
        if 'pooled' not in g or 'folds' not in g: continue                               # e.g. an --analyse-dump summary in the same folder: not a result
        p = g['pooled']['subplot']
        rows.append(dict(file=os.path.basename(f), tag=g.get('tag', ''), frame=g.get('frame', 'pre-d78'), epoch=g['epoch'], eps_scale=g.get('eps_scale', 1.0), frac=g['frac'],
                         folds=','.join(sorted(g['folds'])), trees=p['n'], trees_by_fold='/'.join(str(g['folds'][k]['subplot']['n']) for k in sorted(g['folds'])), below_2=p['below_-2'], share_below_2=round(100 * p['share_below_-2'], 3), share_below_3=round(100 * p['share_below_-3'], 3)))
    print(f'{run}: subplot-level late large debuts, shares in % (the population depends on the data only: trees_by_fold must agree across frames)')
    print(pd.DataFrame(rows).to_string(index=False) if rows else '  no gatepull files')

def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--data', required=True); ap.add_argument('--run', required=True); ap.add_argument('--epoch', type=int, default=36)
    ap.add_argument('--folds', default='0,1,2,3,4'); ap.add_argument('--frac', type=float, default=1.0, help='share of each fold\'s held plots (1 = all)')
    ap.add_argument('--eps-scale', type=float, default=1.0); ap.add_argument('--tag', default=''); ap.add_argument('--dump', action='store_true'); ap.add_argument('--device', default='cpu'); ap.add_argument('--threads', type=int, default=8); a = ap.parse_args(); torch.set_num_threads(a.threads)
    k = M.K_ + ['invyr']; tr = pd.read_parquet(f'{a.data}/seq.parquet', filters=[('split', '==', 'train')]); keys = sorted(tr.plot_key.unique())
    import pyarrow.parquet as pq
    frame = 'd78+' if 'l0' in pq.read_schema(f'{a.data}/seq.parquet').names else 'pre-d78'    # D78 frames carry the per-tree l0 (and the D79 prior)
    R = dict(run=a.run, epoch=a.epoch, frac=a.frac, bar=BAR, eps_scale=a.eps_scale, tag=a.tag, frame=frame, eps={}, folds={}); dumps = []
    for f in map(int, a.folds.split(',')):
        t0 = time.time(); m, ck = S.load(f'{a.run}/f{f}_age1', f'model_e{a.epoch}.pt'); m.to(a.device)
        held_k, _, _ = C.split_plots(keys, f); hk = pd.Series(held_k)
        if a.frac < 1: hk = hk.sample(frac=a.frac, random_state=5)
        held = tr[tr.plot_key.isin(set(hk))]
        if m.height == 't': held = held.merge(pd.read_parquet(f'{a.data}/ht_refs.parquet'), on=k, how='left')   # the inputs the model trained with
        B, _, sq_ = A.build(held, m.vocab); R['eps'][str(f)] = m.vocab.get('miss_eps')                    # build applies the run's own eps (D76 runs)
        if a.eps_scale != 1.0:
            assert 'miss_eps' in m.vocab, '--eps-scale needs a D76 run'; A.apply_miss(B, m.vocab['miss_eps'] * a.eps_scale)
        sub, mic = late_debut_masks(B); o = {}
        for lab, msk in (('subplot', sub), ('microplot', mic)):
            ix = torch.nonzero(msk).flatten(); torch.manual_seed(0)
            if a.dump and lab == 'subplot':                                                  # D84: the same posterior, three quantiles, the trees kept
                q3 = A.z_age(zq(m, M.sub(B, ix), a.device), M.sub(B, ix)); z = q3[1]; pul = (z < -2.0).numpy(); g_ = np.random.default_rng(f)   # D85: age scale
                ctl = np.flatnonzero(~pul); ctl = np.sort(g_.choice(ctl, size=min(CONTROLS, len(ctl)), replace=False)); keep = np.sort(np.concatenate([np.flatnonzero(pul), ctl]))
                dumps.append(tree_table(B, sq_, ix[keep], q3[:, keep], f))
            else: z = A.z_age(z50(m, M.sub(B, ix), a.device)[None], M.sub(B, ix))[0]       # D85: on the age scale (= the latent when sigma_L = 1)
            o[lab] = dict(n=int(len(z)), **{f'below_{c:g}': int((z < c).sum()) for c in CUTS})
        o['trees'] = int(B['v'].shape[0]); o['seconds'] = round(time.time() - t0); R['folds'][str(f)] = o
        print(f'fold {f}: {json.dumps(o)}', flush=True)
    pool = {lab: {x: sum(R['folds'][f][lab][x] for f in R['folds']) for x in R['folds'][next(iter(R['folds']))][lab]} for lab in ('subplot', 'microplot')}
    for lab in pool: pool[lab].update({f'share_below_{c:g}': pool[lab][f'below_{c:g}'] / max(1, pool[lab]['n']) for c in CUTS})
    R['pooled'] = pool; R['pass'] = bool(pool['subplot']['share_below_-2'] <= BAR) if a.eps_scale == 1.0 else None   # the bar is on the run as trained
    out = f'{a.run}/gatepull_e{a.epoch}' + ('' if a.eps_scale == 1.0 else f'_eps{a.eps_scale:g}') + (f'_{a.tag}' if a.tag else '') + '.json'
    if a.tag: R['pass'] = None                                                            # a tagged run is a reading, not the D76 bar
    if a.dump: pd.concat(dumps, ignore_index=True).to_parquet(out.replace('.json', '_dump.parquet')); print('dump ->', out.replace('.json', '_dump.parquet'))
    json.dump(R, open(out, 'w'), indent=1); print(json.dumps(R['pooled'], indent=1))
    if a.tag: print(f'tagged reading "{a.tag}" on a {frame} frame (not the D76 bar) -> {out}')
    elif R['pass'] is not None: print('D76 primary (subplot share below -2 <= 0.10 %):', 'PASS' if R['pass'] else 'FAIL')
    else: print(f'sensitivity eps x{a.eps_scale:g} (reported, not a bar) -> {out}')


if __name__ == '__main__':
    if '--report' in sys.argv: report(sys.argv[sys.argv.index('--report') + 1])
    elif '--analyse-dump' in sys.argv: analyse(sys.argv[sys.argv.index('--analyse-dump') + 1])
    elif '--selftest' in sys.argv: selftest()
    else: main()
