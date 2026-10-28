"""D49 stage 3 — score the pre-registered benchmark ONCE on TEST with the frozen model (D49 amendments 2–3). Writes <data>/bench_report_<split>.json.
A1 unbiased: |median error| of log AGB and log DIA <= 0.10 overall, per hidden DIA class (1-2, 2-3 in) and per major species group (n >= 200).
A2 calibrated: coverage of the central 80 % predictive interval of log AGB in [0.75, 0.85], overall and per DIA class.
A3 totals: median of (sum predicted / sum true) hidden AGB per subplot-visit in [0.9, 1.1] (linear-scale posterior means).
P1 (benchmark part) monotone: 0 imputed DIA / HT / AGB trajectories decreasing up to the debut. The rule's numbers are context only.
Usage: python -I nimiwae_bench.py --data DIR [--split test]"""
import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import argparse, json
import numpy as np, pandas as pd, torch
import nimiwae_train as N

def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--data', required=True); ap.add_argument('--split', default='test')
    ap.add_argument('--k', type=int, default=100); a = ap.parse_args()
    torch.manual_seed(11)
    ck = torch.load(f'{a.data}/nimiwae.pt', weights_only=False); v = ck['vocab']; w = ck.get('w_int', 1.0)
    model = N.NIMIWAE(len(v['spgrpcd']), len(v['epa_l3']), len(v['major_spgrpcd'])); model.load_state_dict(ck['state']); model.vocab = v; model.eval()
    atab = pd.read_parquet(f'{a.data}/age_proxy_table.parquet')
    r, B, R, seq = N.bench_eval(model, a.data, a.split, atab, K=a.k, w_int=w)
    keep = seq['_pos'].to_numpy() < N.T_MAX; seq = seq[keep].copy()
    ti, po = seq['_tid'].to_numpy(), seq['_pos'].to_numpy()
    for kk in ('lA', 'lD', 'A', 'q10', 'q90'): seq[f'nim_{kk}'] = R[kk][ti, po].numpy()
    h = seq[seq.hidden.fillna(False).astype(bool)].copy()
    h['eA'] = h.nim_lA - np.log(h.drybio_g); h['eD'] = h.nim_lD - np.log(h.dia)
    h['inside'] = (np.log(h.drybio_g) >= h.nim_q10) & (np.log(h.drybio_g) <= h.nim_q90)
    h['dcls'] = np.where(h.dia < 2, '1-2 in', '2-3 in'); h['e_rule'] = np.log(h.rule_agb) - np.log(h.drybio_g)
    a1 = [dict(cell='overall', n=len(h), bias_lA=float(h.eA.median()), bias_lD=float(h.eD.median()))]
    for col in ('dcls', 'major_spgrpcd'):
        for val, d in h.groupby(col):
            if len(d) >= 200: a1.append(dict(cell=f'{col}={val}', n=len(d), bias_lA=float(d.eA.median()), bias_lD=float(d.eD.median())))
    ok1 = all(abs(c['bias_lA']) <= 0.10 and abs(c['bias_lD']) <= 0.10 for c in a1)
    a2 = [dict(cell='overall', n=len(h), cov80=float(h.inside.mean()))] + \
         [dict(cell=f'dcls={val}', n=len(d), cov80=float(d.inside.mean())) for val, d in h.groupby('dcls')]
    ok2 = all(0.75 <= c['cov80'] <= 0.85 for c in a2)
    s = ['statecd', 'unitcd', 'countycd', 'plot', 'subp', 'measdate']
    tot = h.groupby(s).agg(t=('drybio_g', 'sum'), n=('nim_A', 'sum'), r=('rule_agb', 'sum'))
    ratio = float((tot.n / tot.t).median()); ok3 = 0.9 <= ratio <= 1.1
    ok4 = tuple(r['viol']) == (0, 0, 0)
    rep = dict(split=a.split, model_epoch=ck['epoch'], width_factor=w, n_hidden=len(h), A1=a1, A1_pass=ok1, A2=a2, A2_pass=ok2,
               A3_median_total_ratio=ratio, n_subplot_visits=len(tot), A3_pass=ok3, P1_benchmark_violations=list(r['viol']), P1_pass=ok4,
               accuracy=dict(mae_lA=float(h.eA.abs().mean()), mae_lD=float(h.eD.abs().mean())),
               context_rule=dict(mae_lA=float(h.e_rule.abs().mean()), bias_lA=float(h.e_rule.median()), median_total_ratio=float((tot.r / tot.t).median())),
               benchmark_pass=bool(ok1 and ok2 and ok3 and ok4))
    json.dump(rep, open(f'{a.data}/bench_report_{a.split}.json', 'w'), indent=1)
    print(json.dumps(rep, indent=1))

if __name__ == '__main__':
    main()
