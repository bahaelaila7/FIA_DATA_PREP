"""Score references and D74 side tables for the cross-validation, so the GPU machine needs no database.
  python -I nimiwae70_refs.py --data DIR --fiadb FIADB --db TRUTH
Writes, all keyed by the frame's natural ids (statecd, unitcd, countycd, plot, subp, tree, invyr; a split tree number is TREE + 100000 k in the frame):
  site_refs.parquet      linked site trees on TRAIN plots, the one link rule of site_links.py (D70 am. 17): measured age (AGEDIA), total-age truth,
                         current age. The G1 truth set on held plots and, from D74, the anchors on fit plots.
  site_refs_d70.parquet  the earlier G1 set (exact DIA and HT, unique on the site side, no SUBP rule), for continuity with cv72.
  planted_refs.parquet   field age in planted stands: a diagnostic only, never a target or input.
  ht_refs.parquet        HTCD of every real frame slot (D74 height model), from FIADB TREE at the same visit.
TRAIN plots only for the age references: VAL carried design choices and TEST stays untouched. ht_refs covers every split (it is an input, not a score)."""
import sys, os, argparse
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd, duckdb
import nimiwae56 as M, nimiwae70_eval as E, site_links as SL, nimiwae_l0 as L0T

SPLIT_BASE = 100000                      # cohorts / curation: a reused tree number becomes TREE + 100000 k


def to_frame(raw, frame, k):
    """map records keyed by raw tally numbers onto frame slots: the frame row with the same visit, SUBP and raw number"""
    f = frame[k + ['slot_kind']].drop_duplicates(k).assign(raw_tree=lambda d: d.tree % SPLIT_BASE)
    f = f[f.slot_kind == 'real']
    out = raw.rename(columns={'tree': 'raw_tree'}).merge(f.drop(columns='slot_kind'), on=[x if x != 'tree' else 'raw_tree' for x in k])
    assert not out.duplicated(k).any(), 'one frame slot per record'
    return out.drop(columns='raw_tree')


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    for x in ('--data', '--fiadb', '--db'): ap.add_argument(x, required=True)
    a = ap.parse_args(); k = M.K_ + ['invyr']
    import pyarrow.parquet as pq
    has_l0 = 'l0' in pq.read_schema(f'{a.data}/seq.parquet').names                                   # D78 frames: the per-tree l0 is the truth's l0
    fr = pd.read_parquet(f'{a.data}/seq.parquet', columns=k + ['slot_kind', 'sftwd_hrdwd', 'split', 'spcd', 'stdorgcd_own'] + (['l0'] if has_l0 else []))
    tr = fr[fr.split == 'train']
    c = duckdb.connect(); c.execute("SET threads = 4"); SL.load(c, a.fiadb)                         # attaches FIADB as f (read-only)
    t = duckdb.connect(a.db, read_only=True)
    cur = t.execute("SELECT STATECD statecd, UNITCD unitcd, COUNTYCD countycd, PLOT plot, SUBP subp, TREE tree, INVYR invyr, estimated_age FROM curated_trees").df()
    sh = tr[k + ['sftwd_hrdwd', 'spcd', 'stdorgcd_own'] + (['l0'] if has_l0 else [])].drop_duplicates(k)
    # G1 truth and anchors: the one link rule
    an = SL.anchors(c)
    s = to_frame(an, tr, k).merge(sh, on=k).rename(columns={'agedia': 'a_obs'})
    s['truth'] = s.a_obs + L0T.r4_truth_l0(a.data, s)                                     # D78: l0 table, or 2 / 5 without one
    s = s.merge(cur, on=k, how='left')[k + ['a_obs', 'truth', 'estimated_age', 'cc', 'ssubp_null']]
    # the earlier G1 set, unchanged, for continuity
    ln = E.linked(c, tr); ln['truth'] = ln.a_obs + L0T.r4_truth_l0(a.data, ln)
    s0 = ln.merge(cur, on=k, how='left')[k + ['a_obs', 'truth', 'estimated_age']]
    fa = t.execute("SELECT STATECD statecd, UNITCD unitcd, COUNTYCD countycd, PLOT plot, SUBP subp, TREE tree, INVYR invyr, age_from_stdage fld FROM curated_trees WHERE age_from_stdage IS NOT NULL AND cond_stdorgcd = 1").df()
    p = fa.merge(tr[tr.slot_kind == 'real'][k].drop_duplicates(), on=k)
    # HTCD for every real frame slot (every split)
    st = ",".join(map(str, SL.R4))
    h = c.execute(f"""SELECT STATECD statecd, UNITCD unitcd, COUNTYCD countycd, PLOT plot, SUBP subp, TREE tree, INVYR invyr, TRY_CAST(HTCD AS INT) htcd
                      FROM f.main.TREE WHERE STATECD IN ({st}) AND STATUSCD = 1 AND SUBP <= 4""").df()
    h = h.drop_duplicates(k + ['htcd'])
    assert not h.duplicated(k).any(), 'one live TREE record per natural id and visit'
    h = to_frame(h, fr, k)[k + ['htcd']]
    for d in (s, s0, p): assert not d.duplicated(k).any()
    s.to_parquet(f'{a.data}/site_refs.parquet'); s0.to_parquet(f'{a.data}/site_refs_d70.parquet'); p.to_parquet(f'{a.data}/planted_refs.parquet'); h.to_parquet(f'{a.data}/ht_refs.parquet')
    nreal = int((fr.slot_kind == 'real').sum())
    print(f'site (am. 17) {len(s):,} of {len(an):,} anchors on TRAIN frame slots, with current age {int(s.estimated_age.notna().sum()):,}; '
          f'site (D70 set) {len(s0):,}; planted {len(p):,}; ht_refs {len(h):,} of {nreal:,} real slots, HTCD {h.htcd.value_counts(dropna=False).sort_index().to_dict()}')
