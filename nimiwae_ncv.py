"""D52 — nested cross-validation of the NIMIWAE phantom backfill (DECISIONS.md D52), instead of a single holdout.

Benchmark trees = every plot's trees meeting the benchmark rules (nimiwae_train.pseudo_bench: microplot debut < 3 in, >= 3 real visits,
a visit < 3 in and a later one >= 3 in; hidden = real visits before the first >= 3-in one).
Outer folds: crc32(plot_key|ncv) % 5. For outer fold f:
  inner validation = the other folds' plots with crc32(plot_key|ncv-in) % 10 == 0 (width factors fitted on their benchmark trees; these
                     plots are NOT trained on);
  training         = all remaining plots (their slots + their pseudo-censored trees), the frozen D52 design and schedule, final epoch;
  scored           = fold f's benchmark trees EXCEPT the dev-VAL plots (split == 'val'), which shaped the design and are never scored.
Each fold is scored once (both protocols); the bars A1-A3 + P1 are computed on the POOLED out-of-fold predictions, per-fold spread reported.
Usage: python -I nimiwae_ncv.py --data DIR --out DIR [train args]"""
import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import argparse, json, time, zlib
import numpy as np, pandas as pd, torch
import nimiwae_train as N, nimiwae_bench as NB

NF = 5

def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--data', required=True); ap.add_argument('--out', required=True)
    ap.add_argument('--folds', default='0,1,2,3,4'); N.add_train_args(ap); a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    log = lambda x: (print(x, flush=True), open(f'{a.out}/ncv.log', 'a').write(x + '\n'))
    seq = pd.read_parquet(f'{a.data}/seq.parquet')
    keys = seq.plot_key.drop_duplicates()
    fold = {k: zlib.crc32((k + '|ncv').encode()) % NF for k in keys}
    inner = {k: zlib.crc32((k + '|ncv-in').encode()) % 10 == 0 for k in keys}
    seq['_fold'] = seq.plot_key.map(fold); seq['_inner'] = seq.plot_key.map(inner)
    bench = N.pseudo_bench(seq)
    log(f'benchmark trees {bench.groupby(N.K_).ngroups:,}, hidden visits {int(bench.hidden.sum()):,}; '
        f'scored (non dev-VAL) hidden visits {int((bench.hidden & (bench.split != "val")).sum()):,}')
    for f in map(int, a.folds.split(',')):
        if os.path.exists(f'{a.out}/fold{f}_preds.parquet'): log(f'fold {f}: done, skipped'); continue
        t0 = time.time()
        tr = seq[(seq._fold != f) & ~seq._inner]
        bin_ = bench[(bench._fold != f) & bench._inner]; bout = bench[(bench._fold == f) & (bench.split != 'val')]
        log(f'fold {f}: train slots {len(tr):,}; inner-val bench trees {bin_.groupby(N.K_).ngroups:,}; outer scored trees {bout.groupby(N.K_).ngroups:,}')
        model, atab, hist = N.train(tr, a, log=lambda x: log(f'  [f{f}] {x}'))
        w = N.fit_width_df(model, bin_, atab); log(f'  [f{f}] inner-val width factors {w}')
        torch.save({'state': model.state_dict(), 'vocab': model.vocab, 'epoch': a.epochs - 1, 'w_int': w}, f'{a.out}/fold{f}_nimiwae.pt')
        atab.to_parquet(f'{a.out}/fold{f}_age_proxy_table.parquet')
        out, viol = [], {}
        for lower in (True, False):
            torch.manual_seed(11)
            r, B, R, sq = N.bench_eval_df(model, bout, atab, K=100, w_int=w, lower=lower)
            h = NB.hidden_predictions(B, R, sq); h['protocol'] = 'interval' if lower else 'lt3'; h['fold'] = f
            out.append(h); viol['interval' if lower else 'lt3'] = list(r['viol'])
        pd.concat(out).to_parquet(f'{a.out}/fold{f}_preds.parquet')
        json.dump(dict(viol=viol, width=w, hist=hist), open(f'{a.out}/fold{f}_meta.json', 'w'), indent=1)
        log(f'fold {f}: scored in {time.time() - t0:.0f}s')
    done = [f for f in range(NF) if os.path.exists(f'{a.out}/fold{f}_preds.parquet')]
    if len(done) < NF: log(f'folds done {done}; pooled report after all {NF}'); return
    P = pd.concat([pd.read_parquet(f'{a.out}/fold{f}_preds.parquet') for f in range(NF)])
    M = [json.load(open(f'{a.out}/fold{f}_meta.json')) for f in range(NF)]
    rep = {}
    for prot in ('interval', 'lt3'):
        h = P[P.protocol == prot]
        viol = tuple(int(sum(m['viol'][prot][i] for m in M)) for i in range(3))
        rep[prot] = dict(pooled=NB.criteria(h, viol),
                         per_fold=[{k: v for k, v in NB.criteria(h[h.fold == f], tuple(M[f]['viol'][prot])).items() if k in
                                    ('n_hidden', 'A1', 'A2', 'A3_median_total_ratio', 'accuracy', 'benchmark_pass')} for f in range(NF)])
    rep['decisive'] = 'interval'; rep['widths'] = [m['width'] for m in M]
    json.dump(rep, open(f'{a.out}/ncv_report.json', 'w'), indent=1)
    p = rep['interval']['pooled']
    log(f"POOLED (interval, decisive): pass={p['benchmark_pass']} A1={p['A1_pass']} A2={p['A2_pass']} A3={p['A3_median_total_ratio']:.3f} "
        f"P1={p['P1_pass']} MAE lA {p['accuracy']['mae_lA']:.3f} lD {p['accuracy']['mae_lD']:.3f}")

if __name__ == '__main__':
    main()
