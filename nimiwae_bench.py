"""D49 stage 3 — score the pre-registered benchmark (TEST, once) for NIMIWAE vs the current rule; writes <data>/bench_report.json.
Acceptance (all four): (1) MAE(log AGB) <= 0.9 x rule; (2) |median bias| <= rule's in every hidden DIA class (1-2, 2-3 in) and major
species group with n >= 200; (3) median |relative error| of the subplot-visit hidden-AGB total <= rule's; (4) 0 imputed trajectories that
decrease between consecutive slots up to the debut (DIA and AGB; D49 amendment).
Usage: python -I nimiwae_bench.py --data DIR [--split test]"""
import argparse, json
import numpy as np, pandas as pd, torch
import nimiwae_train as N

def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--data', required=True); ap.add_argument('--split', default='test')
    ap.add_argument('--k', type=int, default=100); a = ap.parse_args()
    torch.manual_seed(1)
    ck = torch.load(f'{a.data}/nimiwae.pt', weights_only=False); v = ck['vocab']
    model = N.NIMIWAE(len(v['spgrpcd']), len(v['epa_l3']), len(v['major_spgrpcd'])); model.load_state_dict(ck['state']); model.vocab = v
    model.eval()
    bb = pd.read_parquet(f'{a.data}/bench.parquet'); bb = bb[bb.split == a.split].copy()
    bb['obs'] = ~bb['hidden']; bb['thr_in'] = 3.0
    k = ['statecd', 'unitcd', 'countycd', 'plot', 'subp', 'tree']
    bb = bb.sort_values(k + ['measdate']).reset_index(drop=True)
    B, _, keys = N.build(bb, v)
    lA, lD = N.impute(model, B, a.k)
    viol = (N.monotone_violations(lD, B), N.monotone_violations(lA, B))
    g = bb.groupby(k, sort=False); tid = g.ngroup().to_numpy(); pos = g.cumcount().to_numpy()
    ok = pos < N.T_MAX
    bb['nim_lA'] = np.nan; bb.loc[ok, 'nim_lA'] = lA[tid[ok], pos[ok]].numpy()
    h = bb[bb.hidden & bb.nim_lA.notna()].copy()
    h['true_lA'] = np.log(h.drybio_g); h['rule_lA'] = np.log(h.rule_agb)
    h['e_nim'] = h.nim_lA - h.true_lA; h['e_rule'] = h.rule_lA - h.true_lA
    mae_n, mae_r = h.e_nim.abs().mean(), h.e_rule.abs().mean()
    h['dcls'] = np.where(h.dia < 2, '1-2 in', '2-3 in')
    cells = []
    for col in ('dcls', 'major_spgrpcd'):
        for val, d in h.groupby(col):
            if len(d) < 200: continue
            bn, br = float(d.e_nim.median()), float(d.e_rule.median())
            cells.append(dict(by=col, value=str(val), n=len(d), bias_nim=bn, bias_rule=br, mae_nim=float(d.e_nim.abs().mean()),
                              mae_rule=float(d.e_rule.abs().mean()), ok=abs(bn) <= abs(br)))
    s = ['statecd', 'unitcd', 'countycd', 'plot', 'subp', 'measdate']
    tot = h.groupby(s).agg(t=('drybio_g', 'sum'), n=('nim_lA', lambda x: np.exp(x).sum()), r=('rule_agb', 'sum'))
    rel_n, rel_r = float((tot.n / tot.t - 1).abs().median()), float((tot.r / tot.t - 1).abs().median())
    c1, c2, c3 = mae_n <= 0.9 * mae_r, all(c['ok'] for c in cells), rel_n <= rel_r
    c4 = viol == (0, 0)
    rep = dict(split=a.split, n_hidden=len(h), n_trees=int(h.groupby(k).ngroups), mae_nim=float(mae_n), mae_rule=float(mae_r),
               median_bias_nim=float(h.e_nim.median()), median_bias_rule=float(h.e_rule.median()), cells=cells,
               subplot_total_rel_err_nim=rel_n, subplot_total_rel_err_rule=rel_r, n_subplot_visits=len(tot),
               criterion1_mae=bool(c1), criterion2_bias_cells=bool(c2), criterion3_totals=bool(c3), monotone_violations_dia_agb=list(viol), criterion4_monotone=bool(c4),
               accepted=bool(c1 and c2 and c3 and c4),
               model_epoch=ck['epoch'])
    json.dump(rep, open(f'{a.data}/bench_report_{a.split}.json', 'w'), indent=1)
    print(json.dumps({x: rep[x] for x in rep if x != 'cells'}, indent=1))
    for c in cells: print(c)

if __name__ == '__main__':
    main()
