"""nimiwae_sapling_coupling.py — does the observed growth rate carry age information for saplings beyond the direct prior? (DECISIONS.md D78)

  python -I nimiwae_sapling_coupling.py --data DIR --fiadb FIADB --out report.json
  python -I nimiwae_sapling_coupling.py --selftest

Western TOTAGE saplings (age_rec source 'tot': 1.0-3.0 in, whorl counts), with the direct prior's out-of-fold log age at the record (DIR/age_oof.parquet,
written by nimiwae_age_direct.py --export). The growth rate comes from consecutive annual DIA of the same tree (natural ids: the latest earlier
annual visit of its plot where it is live with DIA at breast height), never from PREVDIA (often equal to DIA at a first annual visit). Rate per year,
at least COUPLE_MIN_YEARS apart, floored at RATE_FLOOR (D74). y = log TOTAGE - prior log age, x = log rate.
The mechanism holds (pre-registered, D78) if delta < 0 with a 95 % plot-bootstrap interval excluding 0, AND out of fold (5 folds by plot) the shift
delta (x - m) lowers the MAE in years in >= 4 of 5 folds, AND the pooled improvement is >= 2 %.

  python -I nimiwae_sapling_coupling.py --judge --data FRAME --out judge.json
The D78 judge of the extension (nimiwae70.SAP_DELTA / SAP_RATE) on R4: TRAIN planted saplings (field age, DIA < 5 in at the record), the prior read alone
(the posterior equals it there, D77) with and without the sapling shift; m_s over a fixed 20 % of TRAIN plots, both origins, as training computes it.
Per DIA class 1-3 / 3-5 in: |bias| may grow by at most JUDGE_BIAS yr and 80 % coverage fall by at most JUDGE_COVER."""
import sys, os, json, argparse, zlib
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd

RATE_FLOOR, COUPLE_MIN_YEARS = 0.02, 1.0           # as nimiwae70 (D74)
NF, NBOOT, MIN_FOLDS, MIN_GAIN = 5, 1000, 4, 0.02
JUDGE_BIAS, JUDGE_COVER, M_FRAC = 0.25, 0.02, 0.20
K6 = ['statecd', 'unitcd', 'countycd', 'plot', 'subp', 'tree']; K7 = K6 + ['invyr']
MD = "MAKE_DATE(P.MEASYEAR::INT, COALESCE(NULLIF(P.MEASMON::INT, 0), 6), COALESCE(NULLIF(P.MEASDAY::INT, 0), 15))"


def earlier_dia(c, recs):
    """for each record (K7, measdate): DIA and date of the same tree at the latest earlier annual visit of its plot (live, DIA at breast height)"""
    c.register('recs', recs[K7 + ['measdate']])
    return c.execute(f"""WITH prev AS (SELECT T.STATECD statecd, T.UNITCD unitcd, T.COUNTYCD countycd, T.PLOT plot, T.SUBP subp, T.TREE tree, T.INVYR invyr,
                                          T.DIA::DOUBLE dia_prev, {MD} md_prev
              FROM f.main.TREE T JOIN f.main.PLOT P ON P.STATECD = T.STATECD AND P.UNITCD = T.UNITCD AND P.COUNTYCD = T.COUNTYCD AND P.PLOT = T.PLOT AND P.INVYR = T.INVYR
              JOIN (SELECT DISTINCT STATECD, INVYR FROM f.main.SURVEY WHERE ANN_INVENTORY = 'Y' AND P3_OZONE_IND = 'N') V ON V.STATECD = T.STATECD AND V.INVYR = T.INVYR
              JOIN (SELECT DISTINCT statecd, unitcd, countycd, plot FROM recs) R ON R.statecd = T.STATECD AND R.unitcd = T.UNITCD AND R.countycd = T.COUNTYCD AND R.plot = T.PLOT
              WHERE T.STATUSCD = 1 AND T.DIA > 0 AND T.DIAHTCD = 1)
        SELECT r.statecd, r.unitcd, r.countycd, r.plot, r.subp, r.tree, r.invyr, arg_max(p.dia_prev, p.md_prev) dia_prev, max(p.md_prev) md_prev
        FROM recs r JOIN prev p USING (statecd, unitcd, countycd, plot, subp, tree) WHERE p.md_prev < r.measdate::DATE AND p.invyr < r.invyr
        GROUP BY ALL""").df()


def fold_of(keys):
    return np.array([zlib.crc32(f'{k}|sap'.encode()) % NF for k in keys])


def analyse(d, seed=0):
    """d: one row per record with y, x, plot_key, a_obs, mu -> the pre-registered test"""
    X = np.column_stack([np.ones(len(d)), d.x]); b = np.linalg.lstsq(X, d.y.to_numpy(), rcond=None)[0]
    g = np.random.default_rng(seed); pk = d.plot_key.to_numpy(); up = np.unique(pk); idx = {p: np.flatnonzero(pk == p) for p in up}; bs = []
    for _ in range(NBOOT):
        ii = np.concatenate([idx[p] for p in g.choice(up, len(up))]); Xi = X[ii]; bs.append(np.linalg.lstsq(Xi, d.y.to_numpy()[ii], rcond=None)[0][1])
    lo, hi = np.quantile(bs, [0.025, 0.975])
    fo = fold_of(d.plot_key); folds = []; e0s, e1s = [], []
    for k in range(NF):
        tr, te = d[fo != k], d[fo == k]
        if len(te) == 0 or len(tr) < 50: continue
        bk = np.linalg.lstsq(np.column_stack([np.ones(len(tr)), tr.x]), tr.y.to_numpy(), rcond=None)[0]; m = tr.x.mean()
        e0 = np.abs(np.exp(te.mu) - te.a_obs); e1 = np.abs(np.exp(te.mu + bk[1] * (te.x - m)) - te.a_obs)
        folds.append(dict(fold=k, n=len(te), delta=float(bk[1]), mae_prior=float(e0.mean()), mae_shift=float(e1.mean()))); e0s.append(e0); e1s.append(e1)
    e0, e1 = np.concatenate(e0s), np.concatenate(e1s); gain = float(1 - e1.mean() / e0.mean())
    better = sum(f['mae_shift'] < f['mae_prior'] for f in folds)
    ok = dict(delta_negative=bool(hi < 0), folds_better=bool(better >= MIN_FOLDS), pooled_gain=bool(gain >= MIN_GAIN))
    return dict(n=int(len(d)), plots=int(len(up)), delta=float(b[1]), delta_ci95=[float(lo), float(hi)], folds=folds, folds_better=int(better),
                mae_prior=float(e0.mean()), mae_shift=float(e1.mean()), pooled_gain=gain, x_range_1_99=[float(np.quantile(d.x, 0.01)), float(np.quantile(d.x, 0.99))],
                rate_1_99=[float(np.exp(np.quantile(d.x, 0.01))), float(np.exp(np.quantile(d.x, 0.99)))], checks=ok, holds=bool(all(ok.values())))


def main(a):
    import duckdb
    oof = pd.read_parquet(f'{a.data}/age_oof.parquet'); t = oof[oof.source == 'tot'].copy()
    rec = pd.read_parquet(f'{a.data}/age_rec.parquet', columns=K7 + ['source', 'measdate']); rec = rec[rec.source == 'tot'].drop(columns='source').drop_duplicates(K7)
    t = t.merge(rec, on=K7); c = duckdb.connect(); c.execute("SET threads = 8"); c.execute(f"ATTACH '{a.fiadb}' AS f (READ_ONLY)")
    pv = earlier_dia(c, t); t = t.merge(pv, on=K7)
    t['yrs'] = (pd.to_datetime(t.measdate) - pd.to_datetime(t.md_prev)).dt.days / 365.25; t = t[t.yrs >= COUPLE_MIN_YEARS]
    t['rate'] = ((t.dia - t.dia_prev) / t.yrs).clip(lower=RATE_FLOOR); t['x'] = np.log(t.rate); t['mu'] = t.mu_oof; t['y'] = np.log(t.a_obs) - t.mu
    R = dict(n_tot=int((oof.source == 'tot').sum()), n_with_earlier_annual=int(len(t)), share_at_floor=float((t.rate <= RATE_FLOOR + 1e-12).mean()),
             spearman_age_rate=float(pd.Series(np.log(t.a_obs.to_numpy())).corr(pd.Series(t.x.to_numpy()), method='spearman')), **analyse(t))
    json.dump(R, open(a.out, 'w'), indent=1); print(json.dumps(R, indent=1))
    print('mechanism holds (D78 rule):', R['holds'])


def judge(a):
    import torch, nimiwae56 as M, nimiwae70 as A
    seq = pd.read_parquet(f'{a.data}/seq.parquet', filters=[('split', '==', 'train')]); pl = pd.read_parquet(f'{a.data}/planted_refs.parquet')
    keys = pd.Series(sorted(seq.plot_key.unique()))
    Bm, _, _ = A.build(seq[seq.plot_key.isin(set(keys.sample(frac=M_FRAC, random_state=0)))])
    cs = dict(sapling=dict(delta=A.SAP_DELTA, lo=float(np.log(A.SAP_RATE[0])), hi=float(np.log(A.SAP_RATE[1])), m=0.0)); es = A.sapling_eligible(Bm)
    cs['sapling']['m'] = float(A.sapling_x(Bm, cs)[es].double().mean())
    zero = dict(delta={str(c): 0.0 for c in range(1, 6)}, w={str(c): 1.0 for c in range(1, 6)}, m={str(c): 0.0 for c in range(1, 6)})
    p = pl.merge(seq[seq.slot_kind == 'real'][K7 + ['dia', 'plot_key']], on=K7); p = p[(p.dia > 0) & (p.dia < 5)]
    B, vocab, sq = A.build(seq[seq.plot_key.isin(set(p.plot_key))]); m = A.make_model(vocab, age=True)
    pos = sq[K7 + ['_tid', '_pos']].drop_duplicates(K7); p = p.merge(pos, on=K7); ti, po = p._tid.to_numpy(), p._pos.to_numpy()
    out = {}
    for nm, cp in (('without', zero), ('with', zero | cs)):
        A.apply_coupling(B, cp); z = torch.zeros(3, B['v'].shape[0], M.DZ); z[0, :, 0], z[2, :, 0] = -1.2816, 1.2816
        with torch.no_grad(): q = m.ages(z, B).numpy()[:, ti, po]
        d = p.assign(q10=q[0], q50=q[1], q90=q[2], shifted=(A.sapling_eligible(B).numpy()[ti]), cls=pd.cut(p.dia, [0, 3, 5], right=False).astype(str))
        out[nm] = {c: dict(n=int(len(g)), shifted=float(g.shifted.mean()), bias=float((g.q50 - g.fld).median()), cover80=float(((g.fld >= g.q10) & (g.fld <= g.q90)).mean()))
                   for c, g in d.groupby('cls')}
    ok = {c: bool(abs(out['with'][c]['bias']) - abs(out['without'][c]['bias']) <= JUDGE_BIAS and out['with'][c]['cover80'] >= out['without'][c]['cover80'] - JUDGE_COVER) for c in out['with']}
    R = dict(m_s=cs['sapling']['m'], m_s_saplings=int(es.sum()), classes=out, ok=ok, passes=bool(all(ok.values())))
    json.dump(R, open(a.out, 'w'), indent=1); print(json.dumps(R, indent=1)); print('D78 sapling extension judge:', 'PASS' if R['passes'] else 'FAIL')


def selftest():
    g = np.random.default_rng(1); n = 4000
    d = pd.DataFrame(dict(plot_key=[f'p{i // 4}' for i in range(n)], x=g.normal(-3.4, 0.6, n), mu=np.log(30) + g.normal(0, 0.1, n)))
    d['y'] = -0.3 * (d.x - d.x.mean()) + g.normal(0, 0.2, n); d['a_obs'] = np.exp(d.mu + d.y)
    r = analyse(d); assert abs(r['delta'] + 0.3) < 0.03 and r['holds'], r
    d0 = d.assign(y=g.normal(0, 0.25, n)); d0['a_obs'] = np.exp(d0.mu + d0.y); r0 = analyse(d0)
    assert not r0['holds'] and r0['delta_ci95'][0] < 0 < r0['delta_ci95'][1], r0
    assert len(set(fold_of(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']))) > 1 and (fold_of(['a']) == fold_of(['a'])).all()
    print('selftest: 3 checks PASS')


if __name__ == '__main__':
    ap = argparse.ArgumentParser(); ap.add_argument('--data'); ap.add_argument('--fiadb'); ap.add_argument('--out'); ap.add_argument('--selftest', action='store_true'); ap.add_argument('--judge', action='store_true'); a = ap.parse_args()
    selftest() if a.selftest else judge(a) if a.judge else main(a)
