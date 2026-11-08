"""nimiwae70_d85test.py — D85's scoring-only test (DECISIONS.md D85): a trained run's checkpoints rescored with the lower tail and / or correlated misses.

  python -I nimiwae70_d85test.py --data DATA --run out/cv76flow --variant both --out out/d85test --device cuda      one variant (run them in parallel)
  python -I nimiwae70_d85test.py --summarise out/d85test                                                          the comparison and the expectations

Variants: base (the run as trained), tail (sigma_L = --sl), rho (correlated misses with --rho; needs the run's eps), both. No training: the settings are
switched on in each checkpoint's vocabulary, so every build (gate pulls, G1 / G5, completion) applies them as a trained run would. Test constants, not
calibrations: sigma_L 0.80 (site trees: 0.81), rho 0.363 (FIADB coded misses). Per fold: gate pulls on the age scale over the subplot-level late large
debuts (gatepull's population), palms reported separately (a breakdown, not an exclusion); G1 / G5 on both link sets and the age-limit tables (age_scores
on the fold's held plots); completion (E1, E2, E3, size) on --completion-frac of fold --completion-fold's held plots."""
import sys, os, json, argparse, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd, torch
import nimiwae56 as M, nimiwae70 as A, nimiwae70_cv as C, nimiwae70_score as S, nimiwae70_gatepull as GP

PALMS = (906, 907, 908, 909, 911, 912, 913, 914, 915)      # R4 palm SPCDs (REF_SPECIES): reported apart, never excluded
VARIANTS = ('base', 'tail', 'rho', 'both')
EXPECT = dict(base_pulls=632, tail_below=-2.26, share_bar=0.0010, g1_within=0.05, g5=(0.75, 0.85))   # pre-registered (D85)


def configure(m, variant, sl, rho):
    if variant in ('tail', 'both'): m.vocab['age_tail'] = float(sl)
    if variant in ('rho', 'both'):
        assert 'miss_eps' in m.vocab, 'correlated misses build on the run\'s D76 eps'; m.vocab['miss_rho'] = float(rho)
    return m


def gate_pulls(m, held, dev):
    B, _, sq = A.build(held, m.vocab); sub, _ = GP.late_debut_masks(B); ix = torch.nonzero(sub).flatten()
    torch.manual_seed(0); z = A.z_age(GP.zq(m, M.sub(B, ix), dev), M.sub(B, ix))[1].numpy()
    f = B['f'].numpy(); deb = sq.assign(_f=f[sq._tid.to_numpy()]); deb = deb[(deb._pos == deb._f) & (deb.slot_kind == 'real')].drop_duplicates('_tid').set_index('_tid')
    palm = deb.spcd.reindex(ix.numpy()).isin(PALMS).to_numpy()
    cnt = lambda msk: dict(n=int(msk.sum()), **{f'below_{c:g}': int(((z < c) & msk).sum()) for c in (-1.0, -2.0, -2.26, -3.0)})
    return dict(all=cnt(np.ones(len(z), bool)), palms=cnt(palm), non_palms=cnt(~palm))


def main(a):
    os.makedirs(a.out, exist_ok=True); out = f'{a.out}/{a.variant}.json'
    R = json.load(open(out)) if os.path.exists(out) else dict(variant=a.variant, sl=a.sl, rho=a.rho, folds={})
    k = M.K_ + ['invyr']; tr = pd.read_parquet(f'{a.data}/seq.parquet', filters=[('split', '==', 'train')]); keys = sorted(tr.plot_key.unique())
    site, planted, site_old = (pd.read_parquet(f'{a.data}/{x}.parquet') for x in ('site_refs', 'planted_refs', 'site_refs_d70'))
    ht = pd.read_parquet(f'{a.data}/ht_refs.parquet')
    for f in map(int, a.folds.split(',')):
        if str(f) in R['folds']: print(f'fold {f}: done'); continue
        t0 = time.time(); m, _ = S.load(f'{a.run}/f{f}_age1', f'model_e{a.epoch}.pt'); m = configure(m, a.variant, a.sl, a.rho); m.to(a.device)
        held_k, _, _ = C.split_plots(keys, f); held = tr[tr.plot_key.isin(set(held_k))]
        if m.height == 't': held = held.merge(ht, on=k, how='left')
        o = dict(gate=gate_pulls(m, held, a.device)); rows = []
        torch.manual_seed(0); sc = S.age_scores(m, held, site, planted, site_old=site_old, rows=rows)
        o['G1'], o['G5'], o['G1_d70'], o['G5_d70'] = sc['G1'], sc['G5'], sc.get('G1_d70'), sc.get('G5_d70'); o['planted_field'] = sc['planted_field']
        o['age_limits'] = S.age_limits(pd.concat(rows), a.data)
        if f == a.completion_fold:
            sp = held[held.plot_key.isin(pd.Series(held_k).sample(frac=a.completion_frac, random_state=2))]; torch.manual_seed(0); o['completion'] = S.completion(m, sp)
        o['seconds'] = round(time.time() - t0); R['folds'][str(f)] = o; json.dump(R, open(out, 'w'), indent=1, default=float)
        print(f'[{a.variant}] fold {f}: pulls {o["gate"]["all"]["below_-2"]} (palms {o["gate"]["palms"]["below_-2"]}), G1 {o["G1"]["mae_joint"]:.3f}, G5 {o["G5"]["cover80"]:.3f} in {o["seconds"]} s', flush=True)


def summarise(d):
    V = {v: json.load(open(f'{d}/{v}.json')) for v in VARIANTS if os.path.exists(f'{d}/{v}.json')}
    rows = {}
    for v, R in V.items():
        F = R['folds']; g = lambda grp, key: sum(F[f]['gate'][grp][key] for f in F)
        n = g('all', 'n'); rows[v] = dict(folds=len(F), trees=n, pulls=g('all', 'below_-2'), share=g('all', 'below_-2') / max(1, n), below_226=g('all', 'below_-2.26'), below_3=g('all', 'below_-3'),
                                         palm_pulls=g('palms', 'below_-2'), non_palm_pulls=g('non_palms', 'below_-2'), G1=float(np.mean([F[f]['G1']['mae_joint'] for f in F])),
                                         G1_bias=float(np.mean([F[f]['G1']['bias_joint'] for f in F])), G5=float(np.mean([F[f]['G5']['cover80'] for f in F])),
                                         G1_d70=float(np.mean([F[f]['G1_d70']['mae_joint'] for f in F if F[f].get('G1_d70')])) if any(F[f].get('G1_d70') for f in F) else None,
                                         completion=next((F[f]['completion'] for f in F if 'completion' in F[f]), None))
    print(pd.DataFrame({v: {k: (round(x, 5) if isinstance(x, float) else x) for k, x in r.items() if k != 'completion'} for v, r in rows.items()}).to_string())
    for v, r in rows.items():
        if r['completion']: print(f'  completion {v}:', {k: round(x, 4) for k, x in r['completion'].items() if isinstance(x, float)})
    b = rows.get('base'); E = {}
    if b:
        E['base_reproduces_xcheck'] = dict(pulls=b['pulls'], expected=EXPECT['base_pulls'], ok=b['pulls'] == EXPECT['base_pulls'])
        if 'tail' in rows: E['tail_removes_most_below_2.26'] = dict(base=b['below_226'], tail=rows['tail']['below_226'], ok=rows['tail']['below_226'] <= 0.5 * b['below_226'])
        if 'both' in rows:
            E['both_share_below_bar'] = dict(share=rows['both']['share'], bar=EXPECT['share_bar'], ok=rows['both']['share'] <= EXPECT['share_bar'])
        for v in ('tail', 'rho', 'both'):
            if v in rows: E[f'{v}_G1_within'] = dict(diff=rows[v]['G1'] - b['G1'], ok=abs(rows[v]['G1'] - b['G1']) <= EXPECT['g1_within']); E[f'{v}_G5'] = dict(G5=rows[v]['G5'], ok=EXPECT['g5'][0] <= rows[v]['G5'] <= EXPECT['g5'][1])
    print(json.dumps(E, indent=1, default=float)); json.dump(dict(rows=rows, expectations=E), open(f'{d}/summary.json', 'w'), indent=1, default=float)


if __name__ == '__main__':
    ap = argparse.ArgumentParser(); ap.add_argument('--data'); ap.add_argument('--run'); ap.add_argument('--epoch', type=int, default=36); ap.add_argument('--variant', choices=VARIANTS)
    ap.add_argument('--sl', type=float, default=0.80); ap.add_argument('--rho', type=float, default=0.363); ap.add_argument('--folds', default='0,1,2,3,4')
    ap.add_argument('--completion-fold', type=int, default=0); ap.add_argument('--completion-frac', type=float, default=0.25); ap.add_argument('--out', default='out/d85test')
    ap.add_argument('--device', default='cpu'); ap.add_argument('--threads', type=int, default=8); ap.add_argument('--summarise'); a = ap.parse_args()
    torch.set_num_threads(a.threads)
    summarise(a.summarise) if a.summarise else main(a)
