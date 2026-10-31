"""D54 — dev and nested-CV driver for nimiwae54 (DECISIONS.md D54 + amendments 1-2).

Benchmarks per scored fold: (i) sizes: clean microplot lineages pseudo-censored at 5 in, interval protocol (A1-A3 + P1, decisive for
sizes); (ii) establishment: the same lineages under '< limit only' (E1); (iii)+(iv) censored end: every at-risk interval (E2 existence,
E3 cause, E4 future sizes). Reliability bars: |mean predicted - observed frequency| <= 0.05 in every decile with n >= 200.
No biomass (amendment 3): A1 log DIA bias, A2 log DIA coverage (measured), A3 basal-area totals; widths per slot type.
Folds: crc32(plot_key|ncv) % 5; inner validation crc32(plot_key|ncv-in) % 10 == 0 (width factors; not trained on); dev-VAL never scored.
Usage: python -I nimiwae54_ncv.py --data DIR --out DIR [--dev] [--epochs 4 --max-trees 400000 --k 32]"""
import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import argparse, json, time, zlib
import numpy as np, pandas as pd, torch
import importlib
import nimiwae54 as M

NF = 5

def reliability(p, y, nmin=200):
    p, y = np.asarray(p, float), np.asarray(y, float); ok = ~np.isnan(p) & ~np.isnan(y); p, y = p[ok], y[ok]
    bins = np.clip((p * 10).astype(int), 0, 9); cells = []
    for b in range(10):
        m = bins == b
        if m.sum() >= nmin: cells.append(dict(decile=b, n=int(m.sum()), mean_pred=float(p[m].mean()), observed=float(y[m].mean())))
    passed = all(abs(c['mean_pred'] - c['observed']) <= 0.05 for c in cells)
    return dict(n=int(len(p)), brier=float(np.mean((p - y) ** 2)), base_rate=float(y.mean()) if len(y) else None, cells=cells, pass_=passed)

def size_criteria(h, viol, limit):
    """A1 |median log DIA error| <= 0.10 overall / per true 1-in class / per major group (n >= 200); A2 80 % coverage of the measured log
    DIA in [0.75, 0.85] overall and per class; A3 median predicted / true basal area per subplot-visit in [0.9, 1.1]; P1 no violations.
    Diagnostic: bias binned by the predicted class."""
    h = h[h.dia.notna() & h.nim_lD.notna()].copy(); ld = np.log(h.dia)
    h['eD'] = h.nim_lD - ld; h['inside'] = (ld >= h.nim_q10) & (ld <= h.nim_q90); h['ba'] = M.BA_K * h.dia ** 2
    edges = np.arange(1.0, limit + 1e-9, 1.0)
    lab = lambda x: pd.cut(x, list(edges) + [np.inf], right=False, labels=[f'{int(a)}-{int(a) + 1} in' for a in edges]).astype(str)
    h['dcls'] = lab(h.dia); h['pcls'] = lab(np.exp(h.nim_lD).clip(lower=1.0))
    a1 = [dict(cell='overall', n=len(h), bias_lD=float(h.eD.median()))]
    for col in ('dcls', 'major_spgrpcd'):
        for val, d in h.groupby(col):
            if len(d) >= 200: a1.append(dict(cell=f'{col}={val}', n=len(d), bias_lD=float(d.eD.median())))
    a1p = [dict(cell=f'pred {val}', n=len(d), bias_lD=float(d.eD.median())) for val, d in h.groupby('pcls') if len(d) >= 200]
    a2 = [dict(cell='overall', n=len(h), cov80=float(h.inside.mean()))] + \
         [dict(cell=f'dcls={val}', n=len(d), cov80=float(d.inside.mean())) for val, d in h.groupby('dcls') if len(d) >= 200]
    s_ = ['statecd', 'unitcd', 'countycd', 'plot', 'subp', 'measdate']
    tot = h.groupby(s_).agg(t=('ba', 'sum'), n=('nim_BA', 'sum')); ratio = float((tot.n / tot.t).median())
    ok1 = all(abs(c['bias_lD']) <= 0.10 for c in a1); ok2 = all(0.75 <= c['cov80'] <= 0.85 for c in a2); ok3 = 0.9 <= ratio <= 1.1
    ok4 = all(v_ == 0 for v_ in viol)
    return dict(n_hidden=len(h), A1=a1, A1_pass=ok1, A1_by_prediction_diagnostic=a1p, A2=a2, A2_pass=ok2, A3_median_BA_ratio=ratio, A3_pass=ok3,
                P1_violations=list(viol), P1_pass=ok4, mae_lD=float(h.eD.abs().mean()), benchmark_pass=bool(ok1 and ok2 and ok3 and ok4))

def future_sizes(x):
    a = x[(x.truth_E == 1) & x.dia.notna() & x.nim_lD.notna()].copy(); ld = np.log(a.dia)
    a['eD'] = a.nim_lD - ld; a['inside'] = (ld >= a.nim_q10) & (ld <= a.nim_q90); a['ba'] = M.BA_K * a.dia ** 2
    a1 = [dict(cell='overall', n=len(a), bias_lD=float(a.eD.median()))] + \
         [dict(cell=f'major={g}', n=len(d), bias_lD=float(d.eD.median())) for g, d in a.groupby('major_spgrpcd') if len(d) >= 200]
    s_ = ['statecd', 'unitcd', 'countycd', 'plot', 'subp', 'measdate']
    tot = a.groupby(s_).agg(t=('ba', 'sum'), n=('nim_BA', 'sum')); ratio = float((tot.n / tot.t).median())
    ok = all(abs(c['bias_lD']) <= 0.10 for c in a1) and 0.75 <= a.inside.mean() <= 0.85 and 0.9 <= ratio <= 1.1
    return dict(A1=a1, A2_cov80=float(a.inside.mean()), A3_BA_ratio=ratio, mae_lD=float(a.eD.abs().mean()), pass_=bool(ok))

def score(model, seq_eval, w, tag, log):
    out = {}; P = []
    torch.manual_seed(11); bb = M.bench_sizes(seq_eval, None, True); sq, viol = M.predictions(model, bb, K=100, w_int=w)
    h = sq[sq.hidden].copy(); c = size_criteria(h, viol, float(h.thr_subp.max())); out['sizes'] = c; P.append(h.assign(bench='sizes'))
    log(f"  [{tag}] sizes 5in/interval: n {c['n_hidden']} MAE lD {c['mae_lD']:.3f} | A1 {c['A1_pass']} "
        + ' '.join(f"{x['cell'].split('=')[-1]}:{x['bias_lD']:+.3f}" for x in c['A1']) + f" | by pred " + ' '.join(f"{x['cell']}:{x['bias_lD']:+.3f}" for x in c['A1_by_prediction_diagnostic'])
        + f" | A2 {c['A2_pass']} " + ' '.join(f"{x['cell'].split('=')[-1]}:{x['cov80']:.2f}" for x in c['A2']) + f" | A3 BA {c['A3_median_BA_ratio']:.3f} P1 {c['P1_pass']}")
    torch.manual_seed(11); bb = M.bench_sizes(seq_eval, None, False); sq, _ = M.predictions(model, bb, K=100, w_int=w)
    x = sq[sq.truth_E.notna()]; e1 = reliability(x.nim_pE, x.truth_E); out['E1'] = e1; P.append(x.assign(bench='establishment'))
    log(f"  [{tag}] E1 establishment: n {e1['n']} base {e1['base_rate']:.3f} Brier {e1['brier']:.3f} pass {e1['pass_']} | "
        + ' '.join(f"{c_['decile']}:{c_['mean_pred']:.2f}/{c_['observed']:.2f}" for c_ in e1['cells']))
    torch.manual_seed(11); bb = M.bench_end(seq_eval); sq, _ = M.predictions(model, bb, K=100, w_int=w)
    x = sq[sq.truth_E.notna()].copy(); P.append(x.assign(bench='end'))
    e2 = reliability(x.nim_pE, x.truth_E); g = x[x.truth_E == 0]
    e3 = reliability(g.nim_pCut / (g.nim_pCut + g.nim_pDie).clip(lower=1e-9), g.truth_cut); e4 = future_sizes(x)
    out.update(E2=e2, E3=e3, E4=e4)
    log(f"  [{tag}] E2 end: n {e2['n']} base {e2['base_rate']:.3f} Brier {e2['brier']:.3f} pass {e2['pass_']} | "
        + ' '.join(f"{c_['decile']}:{c_['mean_pred']:.2f}/{c_['observed']:.2f}" for c_ in e2['cells']))
    log(f"  [{tag}] E3 cause: n {e3['n']} base {e3['base_rate']:.3f} Brier {e3['brier']:.3f} pass {e3['pass_']} | E4 future sizes: "
        f"MAE lD {e4['mae_lD']:.3f} A1 " + ' '.join(f"{c_['cell']}:{c_['bias_lD']:+.3f}" for c_ in e4['A1']) + f" cov {e4['A2_cov80']:.2f} A3 BA {e4['A3_BA_ratio']:.3f} pass {e4['pass_']}")
    return out, pd.concat(P, ignore_index=True)

def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--data', required=True); ap.add_argument('--out', required=True)
    ap.add_argument('--dev', action='store_true'); ap.add_argument('--folds', default='0,1,2,3,4')
    ap.add_argument('--epochs', type=int, default=4); ap.add_argument('--max-trees', type=int, default=400000)
    ap.add_argument('--k', type=int, default=32); ap.add_argument('--bs', type=int, default=512); ap.add_argument('--threads', type=int, default=7)
    ap.add_argument('--impl', default='nimiwae54'); ap.add_argument('--censor', default='known'); ap.add_argument('--growth', default='flex')
    a = ap.parse_args(); os.makedirs(a.out, exist_ok=True)
    global M; M = importlib.import_module(a.impl)
    tkw = dict(censor=a.censor, growth=a.growth) if a.impl == 'nimiwae55' else {}
    log = lambda x: (print(x, flush=True), open(f'{a.out}/run.log', 'a').write(x + '\n'))
    seq = pd.read_parquet(f'{a.data}/seq.parquet')
    if a.dev:
        model, hist = M.train(seq[seq.split == 'train'], a.epochs, a.k, a.bs, a.max_trees, threads=a.threads, log=log, **tkw)
        vv = seq[seq.split == 'val']; w = M.fit_width(model, M.bench_sizes(vv, None, True), M.bench_end(vv)); log(f'VAL width factors {w}')
        res, P = score(model, seq[seq.split == 'val'], w, 'dev-VAL', log)
        torch.save({'state': model.state_dict(), 'vocab': model.vocab, 'w_int': w, 'hist': hist, 'impl': a.impl, 'cfg': tkw}, f'{a.out}/dev_model.pt')
        json.dump(res, open(f'{a.out}/dev_report.json', 'w'), indent=1, default=float); return
    keys = seq.plot_key.drop_duplicates()
    fold = {k: zlib.crc32((k + '|ncv').encode()) % NF for k in keys}; inner = {k: zlib.crc32((k + '|ncv-in').encode()) % 10 == 0 for k in keys}
    seq['_fold'] = seq.plot_key.map(fold); seq['_inner'] = seq.plot_key.map(inner)
    for f in map(int, a.folds.split(',')):
        if os.path.exists(f'{a.out}/fold{f}_meta.json'): log(f'fold {f}: done, skipped'); continue
        t0 = time.time()
        tr = seq[(seq._fold != f) & ~seq._inner]; inn = seq[(seq._fold != f) & seq._inner]; outer = seq[(seq._fold == f) & (seq.split != 'val')]
        log(f'fold {f}: train slots {len(tr):,}; inner {len(inn):,}; outer {len(outer):,}')
        model, hist = M.train(tr, a.epochs, a.k, a.bs, a.max_trees, threads=a.threads, log=lambda x: log(f'  [f{f}] {x}'), **tkw)
        w = M.fit_width(model, M.bench_sizes(inn, None, True), M.bench_end(inn)); log(f'  [f{f}] inner-val width factors {w}')
        res, P = score(model, outer, w, f'f{f}', log); P['fold'] = f
        torch.save({'state': model.state_dict(), 'vocab': model.vocab, 'w_int': w, 'hist': hist}, f'{a.out}/fold{f}_model.pt')
        P.to_parquet(f'{a.out}/fold{f}_preds.parquet'); json.dump(dict(res=res, width=w, hist=hist), open(f'{a.out}/fold{f}_meta.json', 'w'), indent=1, default=float)
        log(f'fold {f}: {time.time() - t0:.0f}s')
    if not all(os.path.exists(f'{a.out}/fold{f}_meta.json') for f in range(NF)): return
    P = pd.concat([pd.read_parquet(f'{a.out}/fold{f}_preds.parquet') for f in range(NF)], ignore_index=True)
    Mt = [json.load(open(f'{a.out}/fold{f}_meta.json')) for f in range(NF)]
    viol = (int(sum(m['res']['sizes']['P1_violations'][0] for m in Mt)),)
    h = P[P.bench == 'sizes']; x1 = P[P.bench == 'establishment']; x = P[P.bench == 'end']; g = x[x.truth_E == 0]
    rep = dict(sizes=size_criteria(h, viol, float(h.thr_subp.max())), E1=reliability(x1.nim_pE, x1.truth_E), E2=reliability(x.nim_pE, x.truth_E),
               E3=reliability(g.nim_pCut / (g.nim_pCut + g.nim_pDie).clip(lower=1e-9), g.truth_cut), E4=future_sizes(x),
               widths=[m['width'] for m in Mt])
    json.dump(rep, open(f'{a.out}/ncv_report.json', 'w'), indent=1, default=float)
    log(f"POOLED: sizes pass={rep['sizes']['benchmark_pass']} (MAE lD {rep['sizes']['mae_lD']:.3f}) | E1 {rep['E1']['pass_']} | E2 {rep['E2']['pass_']} | "
        f"E3 {rep['E3']['pass_']} | E4 {rep['E4']['pass_']}")

if __name__ == '__main__':
    main()
