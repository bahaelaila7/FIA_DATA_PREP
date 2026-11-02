"""D70 scoring: bars G1-G8 (DECISIONS.md D70 am. 4) on VAL for one or both dev arms. TEST is not read.
  python -I nimiwae70_eval.py --data DIR --age1 DIR1 --age0 DIR0 --fiadb FIADB --out report.json [--age1b DIR]
Sizes, E1, E2, E3 use the same benchmarks and definitions as the D55 comparison (nimiwae54_ncv.score), run through the joint build."""
import sys, os, json, argparse
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd, torch, duckdb
import nimiwae56 as M, nimiwae70 as A
from nimiwae54_ncv import reliability

def load(d):
    ck = torch.load(f'{d}/model.pt', weights_only=False); m = A.make_model(ck['vocab'], age=ck['cfg']['age']); m.load_state_dict(ck['state']); m.eval(); return m, ck

def completion(m, val, seed=0):
    """size error on the hidden visits, E1 on the same lineages under '< limit only', E2 and E3 on censored ends"""
    torch.manual_seed(seed); out = {}
    h, _ = M.predictions(m, M.bench_sizes(val)); hh = h[h.hidden & h.dia.notna() & h.nim_lD.notna()]; out['mae_lD'] = float((hh.nim_lD - np.log(hh.dia)).abs().mean())
    x1, _ = M.predictions(m, M.bench_sizes(val, interval=False)); x1 = x1[x1.truth_E.notna()]; out['E1'] = reliability(x1.nim_pE, x1.truth_E)['brier']
    b = M.bench_end(val); e, _ = M.predictions(m, b); x = e[e.truth_E.notna()]; out['E2'] = reliability(x.nim_pE, x.truth_E)['brier']
    g = x[x.truth_E == 0]; g = g[g.truth_cut.notna()]; out['E3'] = reliability(g.nim_pCut / (g.nim_pCut + g.nim_pDie).clip(lower=1e-9), g.truth_cut)['brier']
    return out

def linked(c, seq_val):
    """R4 site trees tied to a tally record by exact DIA and exact height, one-to-one; D70 am. 4: scoring only"""
    st = c.execute("""WITH s AS (SELECT S.STATECD statecd, S.UNITCD unitcd, S.COUNTYCD countycd, S.PLOT plot, S.INVYR invyr, S.CONDID condid, S.SPCD spcd, S.DIA dia, S.HT ht, any_value(TRY_CAST(S.AGEDIA AS DOUBLE)) a_obs
                               FROM f.main.SITETREE S WHERE S.STATECD IN (1, 12, 13, 21, 28, 37, 45, 47) AND TRY_CAST(S.AGEDIA AS DOUBLE) > 0 GROUP BY ALL),
        t AS (SELECT T.STATECD statecd, T.UNITCD unitcd, T.COUNTYCD countycd, T.PLOT plot, T.INVYR invyr, T.CONDID condid, T.SPCD spcd, T.DIA dia, T.HT ht, T.SUBP subp, T.TREE tree
              FROM f.main.TREE T WHERE T.STATECD IN (1, 12, 13, 21, 28, 37, 45, 47) AND T.STATUSCD = 1 AND T.SUBP <= 4),
        m AS (SELECT s.*, t.subp, t.tree FROM s JOIN t USING (statecd, unitcd, countycd, plot, invyr, condid, spcd, dia, ht)
              QUALIFY count(*) OVER (PARTITION BY s.statecd, s.unitcd, s.countycd, s.plot, s.invyr, s.condid, s.spcd, s.dia, s.ht) = 1)
        SELECT * FROM m""").df()
    k = ['statecd', 'unitcd', 'countycd', 'plot', 'subp', 'tree', 'invyr']
    return seq_val[k + ['slot_kind', 'sftwd_hrdwd']].merge(st[k + ['a_obs']], on=k).query("slot_kind == 'real'")

@torch.no_grad()
def lineage_age(m, val, keys, K=100):
    """importance-weighted age at the linked visits: median, 10 and 90 %"""
    trees = keys[M.K_].drop_duplicates(); b = val.merge(trees, on=M.K_); B, _, sq = A.build(b, m.vocab)
    out = []
    for i in range(0, B['v'].shape[0], 512):
        s = M.sub(B, slice(i, i + 512)); o = m.joint(s, K); a = m.ages(o['z'], s); w = torch.softmax(o['lw'], 0)
        ix = torch.multinomial(w.T + 1e-30, 400, replacement=True); nn_ = torch.arange(a.shape[1])[:, None]
        q = torch.quantile(a.permute(1, 0, 2)[nn_, ix], torch.tensor([0.1, 0.5, 0.9]), dim=1)   # [3, n, T]
        out.append(q)
    q = torch.cat(out, 1).numpy(); ti, po = sq._tid.to_numpy(), sq._pos.to_numpy()
    return sq.assign(q10=q[0, ti, po], q50=q[1, ti, po], q90=q[2, ti, po])[M.K_ + ['invyr', 'q10', 'q50', 'q90']]

def med(x): return float(np.median(x)) if len(x) else None

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    for x in ('--data', '--age1', '--age0', '--fiadb', '--out', '--db'): ap.add_argument(x)
    ap.add_argument('--age1b'); a = ap.parse_args()
    seq = pd.read_parquet(f'{a.data}/seq.parquet'); val = seq[seq.split == 'val'].copy()
    rec = pd.read_parquet(f'{a.data}/age_rec.parquet'); rv = rec[(rec.split == 'val') & rec.first_of_tree & rec.fit_ok].copy()
    m1, ck1 = load(a.age1); m0, ck0 = load(a.age0); Rp = {}
    R = A.build_rec(rv, m1.vocab['_curve']); rv['pred'] = A.predict_age(m1, R).numpy(); rv['err'] = rv.pred - rv.a_obs
    Rp['G2'] = {s: dict(n=len(g), bias=med(g.err), mae=float(g.err.abs().mean())) for s, g in rv.groupby('source')}
    tally = rv[rv.source != 'site']; Rp['G2_crown'] = {int(c): dict(n=len(g), bias=med(g.err)) for c, g in tally.groupby('cc')}
    sm = tally[(tally.dia >= 1) & (tally.dia < 5)]; Rp['G3'] = dict(n=len(sm), bias=med(sm.err), mae=float(sm.err.abs().mean()), by_source={s: dict(n=len(g), bias=med(g.err)) for s, g in sm.groupby('source')})
    c = duckdb.connect(); c.execute("SET threads = 4"); c.execute(f"ATTACH '{a.fiadb}' AS f (READ_ONLY)")
    ln = linked(c, val); L = lineage_age(m1, val, ln); ln = ln.merge(L, on=M.K_ + ['invyr'])
    lag = np.where(ln.sftwd_hrdwd == 'S', A.L0_SOFT, A.L0_HARD); ln['truth'] = ln.a_obs + lag; ln['e'] = ln.q50 - ln.truth
    t = duckdb.connect(a.db, read_only=True); cur = t.execute("SELECT statecd, unitcd, countycd, plot, subp, tree, invyr, estimated_age FROM (SELECT STATECD statecd, UNITCD unitcd, COUNTYCD countycd, PLOT plot, SUBP subp, TREE tree, INVYR invyr, estimated_age FROM curated_trees)").df()
    j = ln.merge(cur, on=M.K_ + ['invyr'], how='inner'); j = j[j.estimated_age.notna()]
    Rp['G1'] = dict(n=len(j), mae_joint=float((j.q50 - j.truth).abs().mean()), mae_current=float((j.estimated_age - j.truth).abs().mean()), bias_joint=med(j.q50 - j.truth), bias_current=med(j.estimated_age - j.truth))
    Rp['G5'] = dict(cover80=float(((ln.truth >= ln.q10) & (ln.truth <= ln.q90)).mean()))
    Rp['G7'] = dict(sd_S=float(torch.exp(m1.lsS)), tau=float(torch.exp(m1.lstau)), hist=[round(h['sd_S'], 3) for h in ck1['hist']])
    Rp['years_bh'] = {str(k): round(float(v), 2) for k, v in zip(['0'] + list(m1.vocab['_curve']['grp']), m1.curve.years_bh(torch.arange(len(m1.vocab['_curve']['grp']) + 1)))}
    trees = val[M.K_].drop_duplicates().sample(min(30000, val[M.K_].drop_duplicates().shape[0]), random_state=1); vs = val.merge(trees, on=M.K_)
    pl = vs.plot_key.drop_duplicates().sample(frac=1, random_state=2).head(4000); vs = vs[vs.plot_key.isin(pl)]
    c1 = completion(m1, vs); c0 = completion(m0, vs); Rp['completion'] = dict(age1=c1, age0=c0)
    Rp['G6'] = dict(size_ratio=c1['mae_lD'] / c0['mae_lD'], dE2=c1['E2'] - c0['E2'], dE3=c1['E3'] - c0['E3']); Rp['G8'] = dict(dE1=c1['E1'] - c0['E1'])
    json.dump(Rp, open(a.out, 'w'), indent=1, default=float); print(json.dumps(Rp, indent=0, default=float)[:3000])
