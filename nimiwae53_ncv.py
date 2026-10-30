"""D53 — nested-CV (and dev) driver for nimiwae53 (DECISIONS.md D52 folds / rules, D53 benchmark).

Benchmarks: microplot lineages pseudo-censored at 5 in (DECISIVE) and 3 in; protocols 'interval' (hidden visits exist, in [1 in, limit);
decisive) and 'lt' (existence unknown, < limit only). Bars A1-A3 + P1 on sizes (A1 truth-binned, 1-in classes); diagnostics: A1 binned by
prediction, P(E) at hidden visits under 'lt' (truth E = 1; FIA has no recorded absences).
Folds: crc32(plot_key|ncv) % 5; inner validation (width factors, not trained on) crc32(plot_key|ncv-in) % 10 == 0; dev-VAL plots never scored.
Usage: python -I nimiwae53_ncv.py --data DIR --out DIR [--dev] [--epochs 4 --max-trees 400000]"""
import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import argparse, json, time, zlib
import numpy as np, pandas as pd, torch
import nimiwae53 as M

NF = 5
CONFIGS = [(5.0, 'interval'), (5.0, 'lt'), (3.0, 'interval'), (3.0, 'lt')]
DECISIVE = (5.0, 'interval')

def criteria(h, viol, limit):
    h = h.copy(); la = np.log(h.drybio_g)
    h['eA'] = h.nim_lA - la; h['eD'] = h.nim_lD - np.log(h.dia); h['inside'] = (la >= h.nim_q10) & (la <= h.nim_q90)
    edges = np.arange(1.0, limit + 1e-9, 1.0)
    lab = lambda x: pd.cut(x, list(edges) + [np.inf], right=False, labels=[f'{int(a)}-{int(a) + 1} in' for a in edges]).astype(str)
    h['dcls'] = lab(h.dia); h['pcls'] = lab(np.exp(h.nim_lD).clip(lower=1.0))
    a1 = [dict(cell='overall', n=len(h), bias_lA=float(h.eA.median()), bias_lD=float(h.eD.median()))]
    for col in ('dcls', 'major_spgrpcd'):
        for val, d in h.groupby(col):
            if len(d) >= 200: a1.append(dict(cell=f'{col}={val}', n=len(d), bias_lA=float(d.eA.median()), bias_lD=float(d.eD.median())))
    ok1 = all(abs(c['bias_lA']) <= 0.10 and abs(c['bias_lD']) <= 0.10 for c in a1)
    a1p = [dict(cell=f'pred {val}', n=len(d), bias_lA=float(d.eA.median()), bias_lD=float(d.eD.median())) for val, d in h.groupby('pcls') if len(d) >= 200]
    a2 = [dict(cell='overall', n=len(h), cov80=float(h.inside.mean()))] + \
         [dict(cell=f'dcls={val}', n=len(d), cov80=float(d.inside.mean())) for val, d in h.groupby('dcls') if len(d) >= 200]
    ok2 = all(0.75 <= c['cov80'] <= 0.85 for c in a2)
    s = ['statecd', 'unitcd', 'countycd', 'plot', 'subp', 'measdate']
    tot = h.groupby(s).agg(t=('drybio_g', 'sum'), n=('nim_A', 'sum')); ratio = float((tot.n / tot.t).median()); ok3 = 0.9 <= ratio <= 1.1
    ok4 = tuple(viol) == (0, 0, 0)
    return dict(n_hidden=len(h), A1=a1, A1_pass=ok1, A1_by_prediction_diagnostic=a1p, A2=a2, A2_pass=ok2, A3_median_total_ratio=ratio,
                A3_pass=ok3, P1_violations=list(viol), P1_pass=ok4, mae_lA=float(h.eA.abs().mean()), mae_lD=float(h.eD.abs().mean()),
                mean_pE_hidden=float(h.nim_pE.mean()), benchmark_pass=bool(ok1 and ok2 and ok3 and ok4))

def score(model, seq_eval, w, tag, log):
    out, preds = {}, []
    for lim, prot in CONFIGS:
        torch.manual_seed(11); bb = M.bench_trees(seq_eval, lim, prot == 'interval')
        h, viol, *_ = M.predictions(model, bb, K=100, w_int=w); h['config'] = f'{lim:g}in-{prot}'
        out[f'{lim:g}in-{prot}'] = dict(viol=list(viol)); preds.append(h)
        c = criteria(h, viol, lim)
        log(f"  [{tag}] {lim:g} in / {prot}: n {c['n_hidden']} MAE lA {c['mae_lA']:.3f} lD {c['mae_lD']:.3f} | A1 {c['A1_pass']} "
            + ' '.join(f"{x['cell'].split('=')[-1]}:{x['bias_lA']:+.2f}" for x in c['A1'])
            + f" | A2 {c['A2_pass']} " + ' '.join(f"{x['cell'].split('=')[-1]}:{x['cov80']:.2f}" for x in c['A2'])
            + f" | A3 {c['A3_median_total_ratio']:.3f} P1 {c['P1_pass']} pE {c['mean_pE_hidden']:.3f}")
    return out, pd.concat(preds)

def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--data', required=True); ap.add_argument('--out', required=True)
    ap.add_argument('--dev', action='store_true'); ap.add_argument('--folds', default='0,1,2,3,4')
    ap.add_argument('--epochs', type=int, default=4); ap.add_argument('--max-trees', type=int, default=400000)
    ap.add_argument('--k', type=int, default=16); ap.add_argument('--bs', type=int, default=512); ap.add_argument('--threads', type=int, default=7)
    a = ap.parse_args(); os.makedirs(a.out, exist_ok=True)
    log = lambda x: (print(x, flush=True), open(f'{a.out}/run.log', 'a').write(x + '\n'))
    seq = pd.read_parquet(f'{a.data}/seq.parquet')
    if a.dev:                                                     # dev: train on TRAIN, widths + scores on VAL (design work only)
        model, hist = M.train(seq[seq.split == 'train'], a.epochs, a.k, a.bs, a.max_trees, threads=a.threads, log=log)
        bv = M.bench_trees(seq[seq.split == 'val'], DECISIVE[0], True)
        w = M.fit_width(model, bv); log(f'VAL width factors {w}')
        res, P = score(model, seq[seq.split == 'val'], w, 'dev-VAL', log)
        torch.save({'state': model.state_dict(), 'vocab': model.vocab, 'w_int': w, 'hist': hist}, f'{a.out}/dev_model.pt')
        P.to_parquet(f'{a.out}/dev_val_preds.parquet'); return
    keys = seq.plot_key.drop_duplicates()
    fold = {k: zlib.crc32((k + '|ncv').encode()) % NF for k in keys}; inner = {k: zlib.crc32((k + '|ncv-in').encode()) % 10 == 0 for k in keys}
    seq['_fold'] = seq.plot_key.map(fold); seq['_inner'] = seq.plot_key.map(inner)
    for f in map(int, a.folds.split(',')):
        if os.path.exists(f'{a.out}/fold{f}_preds.parquet'): log(f'fold {f}: done, skipped'); continue
        t0 = time.time()
        tr = seq[(seq._fold != f) & ~seq._inner]; inn = seq[(seq._fold != f) & seq._inner]; outer = seq[(seq._fold == f) & (seq.split != 'val')]
        log(f'fold {f}: train slots {len(tr):,}; inner slots {len(inn):,}; outer slots {len(outer):,}')
        model, hist = M.train(tr, a.epochs, a.k, a.bs, a.max_trees, threads=a.threads, log=lambda x: log(f'  [f{f}] {x}'))
        w = M.fit_width(model, M.bench_trees(inn, DECISIVE[0], True)); log(f'  [f{f}] inner-val width factors {w}')
        res, P = score(model, outer, w, f'f{f}', log); P['fold'] = f
        torch.save({'state': model.state_dict(), 'vocab': model.vocab, 'w_int': w, 'hist': hist}, f'{a.out}/fold{f}_model.pt')
        P.to_parquet(f'{a.out}/fold{f}_preds.parquet'); json.dump(dict(res=res, width=w, hist=hist), open(f'{a.out}/fold{f}_meta.json', 'w'), indent=1)
        log(f'fold {f}: {time.time() - t0:.0f}s')
    if not all(os.path.exists(f'{a.out}/fold{f}_preds.parquet') for f in range(NF)): return
    P = pd.concat([pd.read_parquet(f'{a.out}/fold{f}_preds.parquet') for f in range(NF)])
    Mt = [json.load(open(f'{a.out}/fold{f}_meta.json')) for f in range(NF)]
    rep = {}
    for lim, prot in CONFIGS:
        cfg = f'{lim:g}in-{prot}'; viol = tuple(int(sum(m['res'][cfg]['viol'][i] for m in Mt)) for i in range(3))
        h = P[P.config == cfg]
        rep[cfg] = dict(pooled=criteria(h, viol, lim), per_fold=[{k: v for k, v in criteria(h[h.fold == f], tuple(Mt[f]['res'][cfg]['viol']), lim).items()
                                                                  if k in ('n_hidden', 'mae_lA', 'A3_median_total_ratio', 'benchmark_pass')} for f in range(NF)])
    rep['decisive'] = f'{DECISIVE[0]:g}in-{DECISIVE[1]}'; rep['widths'] = [m['width'] for m in Mt]
    json.dump(rep, open(f'{a.out}/ncv_report.json', 'w'), indent=1)
    p = rep[rep['decisive']]['pooled']
    log(f"POOLED decisive {rep['decisive']}: pass={p['benchmark_pass']} A1={p['A1_pass']} A2={p['A2_pass']} A3={p['A3_median_total_ratio']:.3f} "
        f"P1={p['P1_pass']} MAE lA {p['mae_lA']:.3f} lD {p['mae_lD']:.3f}")

if __name__ == '__main__':
    main()
