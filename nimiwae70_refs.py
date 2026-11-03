"""Score references for the cross-validation, so the GPU machine needs no database. TRAIN plots only: VAL carried design choices and TEST stays untouched.
  python -I nimiwae70_refs.py --data DIR --fiadb FIADB --db TRUTH
Writes DIR/site_refs.parquet (linked site trees: measured age, total-age truth, current age) and DIR/planted_refs.parquet (field age, a diagnostic only, never a target or input)."""
import sys, os, argparse
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd, duckdb
import nimiwae56 as M, nimiwae70_eval as E

ap = argparse.ArgumentParser()
for x in ('--data', '--fiadb', '--db'): ap.add_argument(x, required=True)
a = ap.parse_args(); k = M.K_ + ['invyr']
tr = pd.read_parquet(f'{a.data}/seq.parquet', columns=k + ['slot_kind', 'sftwd_hrdwd', 'split']); tr = tr[tr.split == 'train']
c = duckdb.connect(); c.execute("SET threads = 4"); c.execute(f"ATTACH '{a.fiadb}' AS f (READ_ONLY)"); ln = E.linked(c, tr)
ln['truth'] = ln.a_obs + np.where(ln.sftwd_hrdwd == 'S', 2.0, 5.0)
t = duckdb.connect(a.db, read_only=True)
cur = t.execute("SELECT STATECD statecd, UNITCD unitcd, COUNTYCD countycd, PLOT plot, SUBP subp, TREE tree, INVYR invyr, estimated_age FROM curated_trees").df()
fa = t.execute("SELECT STATECD statecd, UNITCD unitcd, COUNTYCD countycd, PLOT plot, SUBP subp, TREE tree, INVYR invyr, age_from_stdage fld FROM curated_trees WHERE age_from_stdage IS NOT NULL AND cond_stdorgcd = 1").df()
s = ln.merge(cur, on=k, how='left')[k + ['a_obs', 'truth', 'estimated_age']]; p = fa.merge(tr[tr.slot_kind == 'real'][k].drop_duplicates(), on=k)
assert not s.duplicated(k).any() and not p.duplicated(k).any()
s.to_parquet(f'{a.data}/site_refs.parquet'); p.to_parquet(f'{a.data}/planted_refs.parquet'); print('site', len(s), 'with current age', int(s.estimated_age.notna().sum()), '; planted', len(p))
