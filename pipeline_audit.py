"""pipeline_audit.py — end-to-end checks from the truth DB to the joint model's inputs (DECISIONS.md D70).

  python -I pipeline_audit.py --db FIA/truth/fia_truth_r4.duckdb --data FIA/nimiwae_r8/d70 --out docs/PIPELINE_AUDIT.json

Read-only. Each check states an invariant that must hold whatever the numbers are. Exits 1 on any failure."""
import sys, os, json, argparse
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import duckdb, numpy as np, pandas as pd, torch

RES = []
def check(name, ok, detail=''):
    RES.append(dict(name=name, ok=bool(ok), detail=str(detail))); print(f"{'PASS' if ok else 'FAIL'}  {name}  {detail}", flush=True)
TK = "STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE"
PK = "STATECD, UNITCD, COUNTYCD, PLOT"
one = lambda c, q: c.execute(q).fetchone()


def truth(c):
    r = one(c, f"SELECT count(*) - count(DISTINCT ({TK}, INVYR)) FROM curated_trees"); check('T1 tree x INVYR unique in curated_trees', r[0] == 0, r[0])
    r = one(c, f"SELECT count(*) - count(DISTINCT (statecd, unitcd, countycd, plot, subp, tree, invyr)) FROM tree_trajectories"); check('T2 tree x invyr unique in tree_trajectories', r[0] == 0, r[0])
    r = one(c, f"""SELECT count(*), count(*) FILTER (WHERE e.closure_kind = 'dead_cut') FROM curated_trees t LEFT JOIN tree_status0_events e USING ({TK}, INVYR) WHERE t.closure_dead""")
    check('T3 every closure_dead record is a dead_cut closure event', r[0] == r[1] and r[0] > 0, r)
    r = one(c, f"SELECT count(*) FROM curated_trees t JOIN tree_censor_events e USING ({TK}, INVYR) WHERE t.closure_dead"); check('T4 a closure_dead record is never also a censoring event', r[0] == 0, r[0])
    r = one(c, f"SELECT count(*) FROM tree_status0_events e ANTI JOIN curated_trees t USING ({TK}, INVYR) WHERE e.closure_kind = 'dead_cut'"); check('T5 every dead_cut closure reached curated_trees', r[0] == 0, r[0])
    r = one(c, f"""SELECT count(*) FROM curated_trees d JOIN curated_trees l USING ({TK}) WHERE d.closure_dead AND l.STATUSCD = 1 AND l.INVYR > d.INVYR""")
    check('T6 no live record after a closure death on the same tree id', r[0] == 0, r[0])
    r = one(c, f"""SELECT count(*), count(*) FILTER (WHERE c.reason = 'procedural') FROM tree_status0_events e LEFT JOIN tree_censor_events c USING ({TK}, INVYR) WHERE e.closure_kind IN ('sized', 'unknown')""")
    check('T7 sized and unknown closures stay censored', r[0] == r[1] and r[0] > 0, r)
    r = one(c, "SELECT count(*), count(*) FILTER (WHERE DIA IS NULL) FROM tree_status0_events WHERE closure_kind = 'sized'"); check('T8 sized closures carry a size', r[0] > 0 and r[1] == 0, r)
    r = one(c, f"SELECT count(*) FROM tree_status0_events e JOIN curated_trees t USING ({TK}, INVYR) WHERE e.closure_kind = 'sized' AND t.STATUSCD = 1"); check('T9 a sized closure is never a live row', r[0] == 0, r[0])
    r = one(c, """WITH v AS (SELECT statecd, unitcd, countycd, plot, subp, invyr, min(sim_year) y0, max(sim_year) y1 FROM tree_trajectories GROUP BY ALL),
                  o AS (SELECT *, lag(y1) OVER (PARTITION BY statecd, unitcd, countycd, plot, subp ORDER BY invyr) p FROM v)
                  SELECT count(*) FILTER (WHERE y0 <> y1), count(*) FILTER (WHERE p >= y0) FROM o""")
    check('T10 sim_year is one value per visit and increases with INVYR', r[0] == 0 and r[1] == 0, r)
    r = one(c, "SELECT count(*) FILTER (WHERE PREV_INVYR >= INVYR), count(*) FILTER (WHERE PREV_MEASDATE >= MEASDATE), count(*) FROM cond_link"); check('T11 condition links run forward in time', r[0] == 0 and r[1] == 0 and r[2] > 0, r)
    r = one(c, f"""SELECT count(*) FROM (SELECT {PK}, INVYR FROM procedural_breaks WHERE is_microplot_move) m JOIN raw.PLOT p ON p.STATECD = m.STATECD AND p.INVYR = m.INVYR
                   AND p.PLOT_STATUS_CD >= 3 AND (p.UNITCD, p.COUNTYCD, p.PLOT) = (m.UNITCD, m.COUNTYCD, m.PLOT)""")
    check('T12 no move is flagged on a nonsampled visit', r[0] == 0, r[0])
    r = one(c, "SELECT count(*) FROM (SELECT STATECD, UNITCD, COUNTYCD, PLOT, INVYR FROM procedural_breaks GROUP BY ALL HAVING count(*) > 1)"); check('T13 one break row per visit', r[0] == 0, r[0])
    r = one(c, "SELECT count(*), count(*) FILTER (WHERE accepted) FROM plot_renumber"); check('T14 every stored renumber link is accepted (no ties)', r[0] == r[1] and r[0] > 0, r)
    r = one(c, "SELECT count(*) FROM curated_cohorts_landis WHERE invyr IS NULL"); check('T15 cohort rows all carry invyr', r[0] == 0, r[0])
    r = one(c, f"""SELECT count(*) FROM curated_cohorts_landis c ANTI JOIN (SELECT DISTINCT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, INVYR FROM clean_subplot_years) s
                   ON c.statecd = s.STATECD AND c.unitcd = s.UNITCD AND c.countycd = s.COUNTYCD AND c.plot = s.PLOT AND c.subp = s.SUBP AND c.invyr = s.INVYR""")
    check('T16 every cohort row sits on a scaffold visit', r[0] == 0, r[0])


def frame(c, seq):
    k = ['statecd', 'unitcd', 'countycd', 'plot', 'subp', 'tree']
    check('F1 tree x invyr unique', not seq.duplicated(k + ['invyr']).any()); check('F2 tree x date unique', not seq.duplicated(k + ['measdate']).any())
    s = seq.sort_values(k + ['measdate']); rev = (s.groupby(k, sort=False).invyr.diff() <= 0).sum(); check('F3 slots in date order are in INVYR order', rev == 0, rev)
    n = seq.groupby(k, sort=False).size(); check('F4 no lineage exceeds 8 slots', n.max() <= 8, n.max())
    tr = one(c, "SELECT count(*) FROM tree_trajectories WHERE intro_type = 'real'")[0]; real = int((seq.slot_kind == 'real').sum()); check('F5 every real trajectory row is a real slot', tr == real, (tr, real))
    sp = seq.groupby('plot_key').split.nunique().max(); check('F6 one split per plot', sp == 1, sp)
    cd = c.execute(f"SELECT DISTINCT {TK} FROM curated_trees WHERE closure_dead").df(); cd.columns = k
    x = seq.merge(cd, on=k); x = x[x.slot_kind == 'real'].groupby(k, as_index=False).agg(dm=('dead_invyr', 'first'), fate=('terminal_fate', 'first'))
    check('F7 closure-dead trees in the frame have a dead date', len(x) > 0 and x.dm.notna().all(), (len(x), int(x.dm.isna().sum())))
    bad = x[x.fate.astype(str).str.startswith('censored')]; check('F8 closure-dead trees are not labelled censored', len(bad) == 0, (len(bad), bad.fate.value_counts().to_dict()))
    rr = seq[seq.s0_kind == 'dead_cut']; check('F9 dead closures are not censoring events in the frame', len(rr) == 0, len(rr))
    z = seq[seq.s0_kind == 'sized'].drop_duplicates(k); check('F10 sized closures give a last size', len(z) > 0 and z.s0_dia.notna().all(), (len(z), int(z.s0_dia.isna().sum())))
    t0 = seq.drop_duplicates('plot_key'); soil = float(t0.soil_clay.notna().mean()); check('F11 real soil values on at least 95 % of plots', soil >= 0.95, f'{soil:.5f}')
    check('F11b soil_ok agrees with the values', bool((t0.soil_ok == t0.soil_clay.notna()).all()))
    check('F11c soil varies', t0.soil_clay.std() > 1 and t0.soil_ph.std() > 0.1, (round(t0.soil_clay.std(), 2), round(t0.soil_ph.std(), 3)))
    check('F12 climate normals on every slot', seq.tm_n.notna().mean() >= 0.999, f'{seq.tm_n.notna().mean():.5f}')
    mv = c.execute("SELECT DISTINCT STATECD statecd, UNITCD unitcd, COUNTYCD countycd, PLOT plot FROM procedural_breaks WHERE is_microplot_move").df()
    a = seq[seq.move_invyr.notna()].drop_duplicates('plot_key'); b = a.merge(mv, on=['statecd', 'unitcd', 'countycd', 'plot']); check('F13 move_invyr only on plots with a flagged move', len(a) == len(b) and len(a) > 0, (len(a), len(b)))
    banned = [x_ for x_ in seq.columns if x_.lower() in ('stdage', 'fldage', 'estimated_age', 'age_from_stdage')]; check('F14 no STDAGE or curated age column in the frame', not banned, banned)


def ages(rec, seq):
    kk = ['source', 'statecd', 'unitcd', 'countycd', 'plot', 'invyr', 'subp', 'tree', 'spcd', 'dia', 'ht', 'a_obs']
    check('A1 no record repeated', not rec.duplicated(kk).any(), int(rec.duplicated(kk).sum()))
    check('A2 ages and sizes are positive', (rec.a_obs > 0).all() and (rec.dia > 0).all())
    st = rec[rec.source == 'site']; check('A3 site trees are R4 and at least 5 in', st.statecd.isin([1, 12, 13, 21, 28, 37, 45, 47]).all() and st.dia.min() >= 5.0, st.dia.min())
    w = rec[rec.source != 'site']; check('A4 tally ages are outside R4', not w.statecd.isin([1, 12, 13, 21, 28, 37, 45, 47]).any(), sorted(w.statecd.unique().tolist()))
    pm = seq.drop_duplicates('plot_key').set_index('plot_key').split; sp = st.drop_duplicates('plot_key').set_index('plot_key').split
    j = pd.concat([pm.rename('a'), sp.rename('b')], axis=1, join='inner'); bad = int((j.a != j.b).sum())
    check('A5 a plot has the same split in the frame and the age records', bad == 0 and len(j) > 0, (len(j), bad))
    only = st[~st.plot_key.isin(pm.index)].plot_key.nunique(); print(f'INFO  site-tree plots not in the frame: {only}')
    check('A6 climate on 99 % and real soil on 95 % of records', rec.tm_n.notna().mean() >= 0.99 and rec.soil_clay.notna().mean() >= 0.95, (round(rec.tm_n.notna().mean(), 5), round(rec.soil_clay.notna().mean(), 5)))
    check('A7 fitted ages stay within the curve range', rec[rec.fit_ok].a_obs.max() <= 400, (rec.a_obs.max(), st.a_obs.max(), int((~rec.fit_ok).sum())))
    check('A8 every split has age records from each source', rec.groupby('source').split.nunique().min() >= 3, rec.groupby('source').split.nunique().to_dict())
    print(rec.groupby(['source', 'split']).agg(n=('a_obs', 'size'), first=('first_of_tree', 'sum'), lt5=('dia', lambda d: int((d < 5).sum())), med_age=('a_obs', 'median')).to_string())


def model(seq, rec):
    import nimiwae70 as A, nimiwae56 as M
    torch.manual_seed(0)
    k = M.K_; tr = seq[seq.split == 'train']; rt = rec[(rec.split == 'train') & rec.first_of_tree]
    pl_ = tr.plot_key.drop_duplicates().sample(400, random_state=1); t = tr[tr.plot_key.isin(pl_)]
    cv = A.curve_vocab(t, rt); B, vocab, sq = A.build(t, cv=cv); R = A.build_rec(rt, cv)
    bad = [x for x, v in B.items() if torch.is_floating_point(v) and not torch.isfinite(v).all()]; check('M1 lineage tensors are finite', not bad, bad)
    bad = [x for x, v in R.items() if torch.is_floating_point(v) and not torch.isfinite(v).all()]; check('M2 record tensors are finite', not bad, bad)
    m = A.make_model(vocab); ix = torch.arange(min(256, B['v'].shape[0])); b = M.sub(B, ix); o = m.joint(b, 8, dreg=True)
    check('M3 log weights are finite on real data', torch.isfinite(o['lw']).all(), o['lw'].mean().item())
    loss = -(torch.logsumexp(o['lw'], 0) - np.log(8)).mean() - m.rec_ll(M.sub(R, torch.arange(256))).mean(); loss.backward()
    nog = [n for n, p in m.named_parameters() if p.grad is None and not n.startswith(('st_q', 'qin', 'qtr', 'qout', 'ercd', 'ecc'))]
    bad = [n for n, p in m.named_parameters() if p.grad is not None and not torch.isfinite(p.grad).all()]
    check('M4 gradients are finite', not bad, bad[:5]); check('M5 curve, birth head and scales all receive gradient', all(p.grad is not None for n, p in m.named_parameters() if n.startswith(('curve.out', 'bhead', 'lsS'))), nog[:6])
    a = m.ages(o['z'].detach(), b).detach(); t_ = b['cov'][..., A.ITYR] * 20; v = b['v'][None] > 0
    e = ((a - a.gather(-1, torch.zeros_like(a[..., :1]).long())) - (t_ - t_[:, :1])[None])[v.expand_as(a)]
    check('M6 age step = elapsed years on real dates', float(e.abs().max()) < 1e-3, f'{float(e.abs().max()):.1e}')
    fp = (B['ev'] == M.EV['false_soft']).sum().item(); mk = A.e1_mask(t).sum()
    b2, _, _ = M.build(t, vocab); fb = (b2['ev'] == M.EV['false_soft']).sum().item()
    check('M7 the E1 rule removes FALSE labels', fb >= fp, f'base {fb}, joint {fp}, masked slots {int(mk)}')
    s_ = A.e1_mask(seq); share = seq.assign(m=s_).groupby('split').m.mean().to_dict(); print('INFO  pre-move slots with FALSE dropped, share by split', share, 'total', int(s_.sum()))


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    for x in ('--db', '--data', '--out'): ap.add_argument(x, required=True)
    a = ap.parse_args()
    c = duckdb.connect(a.db, read_only=True); c.execute('SET threads = 6')
    truth(c)
    seq = pd.read_parquet(f'{a.data}/seq.parquet'); rec = pd.read_parquet(f'{a.data}/age_rec.parquet')
    frame(c, seq); ages(rec, seq); model(seq, rec)
    json.dump(RES, open(a.out, 'w'), indent=1)
    n = sum(r['ok'] for r in RES); print(f'audit: {n} of {len(RES)} pass'); sys.exit(0 if n == len(RES) else 1)
