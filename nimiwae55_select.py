"""D55 amendment 1 — apply the pre-registered selection rule to the 2 × 2 dev comparison (VAL only).
Rule: (1) the most bars passed among A1, A2, A3, P1, E1, E2, E3, E4; (2) ties -> the lowest sum of ratios to the best variant for
E1 Brier, E2 Brier, E3 Brier, size MAE (log DIA) and E4 MAE (log DIA).
Usage: python -I nimiwae55_select.py --root DIR [--variants V1,V2,V3,V4]   (reads DIR/d55_<V>/dev_report.json and run.log)"""
import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import argparse, json

NAMES = {'V1': 'known + flex', 'V2': 'mar + flex', 'V3': 'known + struct', 'V4': 'mar + struct'}
BARS = ['A1', 'A2', 'A3', 'P1', 'E1', 'E2', 'E3', 'E4']
TIE = ['E1 Brier', 'E2 Brier', 'E3 Brier', 'size MAE lD', 'E4 MAE lD']

def row(r):
    s = r['sizes']
    bars = dict(A1=s['A1_pass'], A2=s['A2_pass'], A3=s['A3_pass'], P1=s['P1_pass'], E1=r['E1']['pass_'], E2=r['E2']['pass_'],
                E3=r['E3']['pass_'], E4=r['E4']['pass_'])
    tie = {'E1 Brier': r['E1']['brier'], 'E2 Brier': r['E2']['brier'], 'E3 Brier': r['E3']['brier'], 'size MAE lD': s['mae_lD'],
           'E4 MAE lD': r['E4']['mae_lD']}
    return bars, tie

def last_hist(path):
    """Last per-epoch training record in run.log (loss, ESS of the last batch, sd_disc)."""
    rec = None
    for ln in open(path):
        if ln.startswith('{"epoch"'): rec = json.JSONDecoder().raw_decode(ln)[0]
    return rec

def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--root', required=True); ap.add_argument('--variants', default='V1,V2,V3,V4'); ap.add_argument('--report', default='dev_report.json'); ap.add_argument('--prefix', default='d55_')
    a = ap.parse_args(); V = a.variants.split(',')
    R = {v: json.load(open(f'{a.root}/{a.prefix}{v}/{a.report}')) for v in V}
    B, T = {}, {}
    for v in V: B[v], T[v] = row(R[v])
    best = {k: min(T[v][k] for v in V) for k in TIE}
    npass = {v: sum(bool(B[v][b]) for b in BARS) for v in V}
    score = {v: sum(T[v][k] / best[k] if best[k] > 0 else 1.0 for k in TIE) for v in V}
    order = sorted(V, key=lambda v: (-npass[v], score[v]))
    hdr = '| Variant | ' + ' | '.join(BARS) + ' | bars | ' + ' | '.join(TIE) + ' | Σ ratio | ESS (last batch) |'
    print(hdr); print('|' + '---|' * (len(BARS) + len(TIE) + 4))
    for v in order:
        h = last_hist(f'{a.root}/{a.prefix}{v}/run.log') or {}
        print(f"| {v} {NAMES.get(v, '')} | " + ' | '.join('✓' if B[v][b] else '✗' for b in BARS) + f' | {npass[v]} | '
              + ' | '.join(f'{T[v][k]:.4f}' for k in TIE) + f' | {score[v]:.3f} | {h.get("ess_last_batch", float("nan")):.1f} |')
    top = [v for v in order if npass[v] == npass[order[0]]]
    print(f"\nSELECTED: {order[0]} ({NAMES.get(order[0], '')}) — {npass[order[0]]} bars"
          + (f"; tie among {', '.join(top)} broken by Σ ratio {score[order[0]]:.3f}" if len(top) > 1 else ''))

if __name__ == '__main__':
    main()
