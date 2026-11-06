"""nimiwae_l0.py — years from germination to breast height (l0) per species, one definition for every age consumer (DECISIONS.md D78).

  python -I nimiwae_l0.py --fiadb FIADB --data DIR        writes DIR/l0_table.parquet and the per-tree column `l0` in DIR/seq.parquet
  python -I nimiwae_l0.py --selftest

Total age = breast-height age + l0. Rows of the table (scope, spcd, planted, l0, source, n, spread):
  scope 'r4', planted 1   R4 planted conditions, a species with >= MIN_PLANTED linked site trees (D70 am. 17 rule, TRAIN plots) carrying a field age
                          at the same visit: l0 = median(field age - AGEDIA). The field age (FIADB guide 2.5.51: the average total age of the trees in
                          the field size class) is close to a measurement in plantations, and planted trees share it.
  scope 'west'            western BHAGE records (age_rec source 'bha'): FIA's own constant, the modal TOTAGE - BHAGE of the species on western
                          records carrying both (only CA, OR, WA do), if >= MIN_WEST records and the mode holds >= MIN_SHARE of them.
Anything else: 2 yr softwood, 5 yr hardwood (DEFAULT). FVS-SN's site curves are not used: they fail the R4 check (D78)."""
import sys, os, argparse
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd

DEFAULT = dict(S=2.0, H=5.0)
MIN_PLANTED = 200
MIN_WEST, MIN_SHARE = 100, 0.5
WEST_STATES = (4, 8, 16, 30, 32, 35, 49, 56, 6, 41, 53)          # as nimiwae_age_prep.WEST
K = ['statecd', 'unitcd', 'countycd', 'plot', 'subp', 'tree', 'invyr']


def lookup(tab, scope, spcd, planted, sftwd):
    """l0 [n] for records of `scope` ('r4' or 'west'): the table's (spcd, planted) row for 'r4' (planted rows only exist), the species row for
    'west', else DEFAULT by softwood / hardwood (unknown wood type: hardwood)"""
    spcd = np.asarray(spcd, float); planted = np.asarray(planted, float); sw = np.asarray(sftwd, dtype=object)
    out = np.where(sw == 'S', DEFAULT['S'], DEFAULT['H']).astype(float)
    t = tab[tab.scope == scope]
    if scope == 'r4':
        for _, r in t.iterrows(): out[(spcd == r.spcd) & (planted == r.planted)] = r.l0
    else:
        for _, r in t.iterrows(): out[spcd == r.spcd] = r.l0
    return out


def table(data):
    """DIR/l0_table.parquet, or an empty table (every lookup gives DEFAULT) for frames built before D78"""
    p = f'{data}/l0_table.parquet'
    return pd.read_parquet(p) if os.path.exists(p) else pd.DataFrame(columns=['scope', 'spcd', 'planted', 'l0', 'source', 'n', 'spread'])

def r4_truth_l0(data, d):
    """l0 of R4 frame records d (spcd, stdorgcd_own, sftwd_hrdwd: the frame slot's own species and condition): the G1 truth is AGEDIA + this"""
    return lookup(table(data), 'r4', d.spcd, (d.stdorgcd_own == 1).astype(int), d.sftwd_hrdwd)


def planted_rows(site, planted, frame):
    """site: linked site trees (K, a_obs); planted: field ages (K, fld); frame: real TRAIN slots (K, spcd, stdorgcd_own).
    -> rows (scope 'r4', planted 1) for species with >= MIN_PLANTED planted site trees with a field age at the same visit"""
    j = site[K + ['a_obs']].merge(planted[K + ['fld']], on=K).merge(frame[K + ['spcd', 'stdorgcd_own']], on=K)
    j = j[j.stdorgcd_own == 1]; j['d'] = j.fld - j.a_obs; rows = []
    for sp, g in j.groupby('spcd'):
        if len(g) >= MIN_PLANTED:
            rows.append(dict(scope='r4', spcd=int(sp), planted=1, l0=float(g.d.median()), source='r4_planted', n=int(len(g)),
                             spread=f'IQR {g.d.quantile(0.25):.2f} to {g.d.quantile(0.75):.2f}'))
    return rows


def west_rows(pairs):
    """pairs: western records with both ages (spcd, diff = TOTAGE - BHAGE) -> rows (scope 'west') for species meeting MIN_WEST / MIN_SHARE"""
    rows = []
    for sp, g in pairs.groupby('spcd'):
        vc = g['diff'].value_counts()
        if len(g) >= MIN_WEST and vc.iloc[0] / len(g) >= MIN_SHARE:
            mode = float(vc.index[vc == vc.iloc[0]].min())                        # ties: the smallest value (explicit rule, no row order)
            rows.append(dict(scope='west', spcd=int(sp), planted=-1, l0=mode, source='fia_constant', n=int(len(g)), spread=f'mode share {vc.iloc[0] / len(g):.3f}'))
    return rows


def tree_l0(seq, tab):
    """the per-tree l0 of the frame: the tree's species and stand origin at its first real record (scope 'r4')"""
    tk = K[:-1]; r = seq[seq.slot_kind == 'real'].sort_values(tk + ['measdate', 'invyr']).groupby(tk, sort=False).first().reset_index()
    r['l0'] = lookup(tab, 'r4', r.spcd, (r.stdorgcd_own == 1).astype(int), r.sftwd_hrdwd)
    return seq.drop(columns='l0', errors='ignore').merge(r[tk + ['l0']], on=tk, how='left')


def main(a):
    import duckdb
    seq = pd.read_parquet(f'{a.data}/seq.parquet')
    tr = seq[(seq.split == 'train') & (seq.slot_kind == 'real')][K + ['spcd', 'stdorgcd_own']].drop_duplicates(K)
    rows = planted_rows(pd.read_parquet(f'{a.data}/site_refs.parquet'), pd.read_parquet(f'{a.data}/planted_refs.parquet'), tr)
    c = duckdb.connect(); c.execute("SET threads = 4"); c.execute(f"ATTACH '{a.fiadb}' AS f (READ_ONLY)")
    pairs = c.execute(f"""SELECT SPCD spcd, TRY_CAST(TOTAGE AS DOUBLE) - TRY_CAST(BHAGE AS DOUBLE) diff FROM f.main.TREE
                          WHERE STATECD IN ({','.join(map(str, WEST_STATES))}) AND TRY_CAST(TOTAGE AS DOUBLE) > 0 AND TRY_CAST(BHAGE AS DOUBLE) > 0""").df()
    rows += west_rows(pairs)
    tab = pd.DataFrame(rows, columns=['scope', 'spcd', 'planted', 'l0', 'source', 'n', 'spread']); tab.to_parquet(f'{a.data}/l0_table.parquet')
    out = tree_l0(seq, tab); assert len(out) == len(seq) and out.l0.notna().all(), 'every frame row gets an l0'
    out.to_parquet(f'{a.data}/seq.parquet')
    print(tab.to_string(index=False)); print('frame trees by l0:', out.drop_duplicates(K[:-1]).l0.round(2).value_counts().sort_index().to_dict())


def selftest():
    tab = pd.DataFrame([dict(scope='r4', spcd=131, planted=1, l0=2.8, source='r4_planted', n=300, spread=''),
                        dict(scope='west', spcd=202, planted=-1, l0=8.0, source='fia_constant', n=500, spread='')])
    assert lookup(tab, 'r4', [131, 131, 802, 999], [1, 0, 1, 0], ['S', 'S', 'H', None]).tolist() == [2.8, 2.0, 5.0, 5.0]
    assert lookup(tab, 'west', [202, 131], [0, 1], ['S', 'S']).tolist() == [8.0, 2.0]
    pr = pd.DataFrame(dict(spcd=[1] * 60 + [1] * 40 + [2] * 99 + [3] * 50 + [3] * 50, diff=[8] * 60 + [11] * 40 + [5] * 99 + [7] * 50 + [9] * 50))
    w = {r['spcd']: r['l0'] for r in west_rows(pr)}
    assert w == {1: 8.0, 3: 7.0}, w                                        # 2: too few records; 3: a tie, the smaller value
    g = np.random.default_rng(0); n = 250
    base = pd.DataFrame({k: np.arange(n) if k == 'tree' else 1 for k in K})
    site = base.assign(a_obs=g.integers(5, 30, n).astype(float)); planted = site[K].assign(fld=site.a_obs + 3.0)
    frame = base.assign(spcd=131, stdorgcd_own=np.r_[np.ones(n - 10), np.zeros(10)])
    r = planted_rows(site, planted, frame); assert len(r) == 1 and r[0]['l0'] == 3.0 and r[0]['n'] == 240, r
    frame2 = frame.assign(stdorgcd_own=np.r_[np.ones(150), np.zeros(n - 150)]); assert planted_rows(site, planted, frame2) == []   # under MIN_PLANTED
    sq = pd.DataFrame(dict(statecd=1, unitcd=1, countycd=1, plot=1, subp=1, tree=[1, 1, 2], invyr=[2000, 2005, 2000], slot_kind='real',
                           measdate=pd.to_datetime(['2000-06-01', '2005-06-01', '2000-06-01']), spcd=[131, 131, 802], stdorgcd_own=[1, 0, 1], sftwd_hrdwd=['S', 'S', 'H']))
    assert tree_l0(sq, tab).l0.tolist() == [2.8, 2.8, 5.0]                 # stand origin at the first record decides, for every row of the tree
    print('selftest: 5 checks PASS')


if __name__ == '__main__':
    ap = argparse.ArgumentParser(); ap.add_argument('--fiadb'); ap.add_argument('--data'); ap.add_argument('--selftest', action='store_true'); a = ap.parse_args()
    selftest() if a.selftest else main(a)
