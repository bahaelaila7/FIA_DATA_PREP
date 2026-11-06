"""nimiwae_stand.py — stand-structure features for the direct age prior (DECISIONS.md D80). No ages, measurements only.

  python -I nimiwae_stand.py --selftest

Per plot-visit (natural ids: state, unit, county, plot, INVYR), from FIADB TREE for every source alike (R4 site, field-age and frame records; western
age records): live trees (STATUSCD 1) on the four subplots (SUBP <= 4) with DIA >= 5 in, the subplot tally, one sampling design in every region:
  st_n5, st_n15     trees >= 5 in, >= 15 in            st_dmax, st_qmd, st_dsd   largest, quadratic mean and standard deviation of DIA
  st_hmax           tallest HT
and the record against its stand (its own DIA and HT): st_rel_d = DIA / st_dmax, st_rel_q = DIA / st_qmd, st_rel_h = HT / st_hmax.
A visit with no tree >= 5 in, or a record whose plot key has no raw FIADB match (renumbered plots), gets missing values (the booster handles them)."""
import sys, os, argparse
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd

PV = ['statecd', 'unitcd', 'countycd', 'plot', 'invyr']
STAND = ['st_n5', 'st_n15', 'st_dmax', 'st_qmd', 'st_dsd', 'st_hmax', 'st_rel_d', 'st_rel_q', 'st_rel_h']


def stand_table(c, visits, tree='f.main.TREE'):
    """visits: DataFrame with the PV columns (any duplicates) -> one row per plot-visit found in `tree` with at least one live tree >= 5 in"""
    c.register('_stand_visits', visits[PV].drop_duplicates().astype('int64'))
    return c.execute(f"""WITH t AS (SELECT T.STATECD::BIGINT statecd, T.UNITCD::BIGINT unitcd, T.COUNTYCD::BIGINT countycd, T.PLOT::BIGINT plot, T.INVYR::BIGINT invyr,
                                           T.DIA::DOUBLE dia, T.HT::DOUBLE ht
                                    FROM {tree} T JOIN _stand_visits k ON k.statecd = T.STATECD AND k.unitcd = T.UNITCD AND k.countycd = T.COUNTYCD
                                                                      AND k.plot = T.PLOT AND k.invyr = T.INVYR
                                    WHERE T.STATUSCD = 1 AND T.SUBP <= 4 AND T.DIA >= 5)
        SELECT statecd, unitcd, countycd, plot, invyr, count(*)::DOUBLE st_n5, count(*) FILTER (WHERE dia >= 15)::DOUBLE st_n15, max(dia) st_dmax,
               sqrt(avg(dia * dia)) st_qmd, stddev_samp(dia) st_dsd, max(ht) st_hmax
        FROM t GROUP BY ALL""").df()


def attach(d, st):
    """d: records with the PV columns, dia and ht -> d with the STAND columns (missing where the visit has no stand)"""
    key = d[PV].astype('int64')
    out = d.drop(columns=[x for x in STAND if x in d]).assign(**{x: key[x].to_numpy() for x in PV}).merge(st, on=PV, how='left')
    assert len(out) == len(d), 'one stand row per plot-visit'
    out['st_rel_d'] = out.dia / out.st_dmax; out['st_rel_q'] = out.dia / out.st_qmd; out['st_rel_h'] = out.ht / out.st_hmax
    out.index = d.index
    return out


def selftest():
    import duckdb
    c = duckdb.connect()
    T = pd.DataFrame(dict(STATECD=1, UNITCD=1, COUNTYCD=1, PLOT=[1] * 7 + [2], INVYR=[2000] * 6 + [2005, 2000], SUBP=[1, 2, 3, 4, 5, 1, 1, 1], STATUSCD=[1, 1, 1, 2, 1, 1, 1, 1],
                          DIA=[6.0, 10.0, 20.0, 30.0, 40.0, 3.0, 8.0, 4.0], HT=[40.0, 60.0, 90.0, 99.0, 120.0, 20.0, 50.0, 25.0]))
    c.register('fake_tree', T)
    v = pd.DataFrame(dict(statecd=[1, 1, 1], unitcd=1, countycd=1, plot=[1, 1, 2], invyr=[2000, 2000, 2000]))
    st = stand_table(c, v, tree='fake_tree')
    r = st[st['plot'] == 1].iloc[0]                                                          # live, SUBP <= 4, DIA >= 5: 6, 10, 20 (dead 30, SUBP 5 and 3 in left out)
    assert len(st) == 1 and r.st_n5 == 3 and r.st_n15 == 1 and r.st_dmax == 20 and abs(r.st_qmd - np.sqrt((36 + 100 + 400) / 3)) < 1e-9 and r.st_hmax == 90
    assert abs(r.st_dsd - np.std([6, 10, 20], ddof=1)) < 1e-9                               # plot 2 has no tree >= 5 in; 2005 is another visit
    d = pd.DataFrame(dict(statecd=[1, 1, 1], unitcd=1, countycd=1, plot=[1, 2, 1], invyr=[2000, 2000, 2005], dia=[10.0, 4.0, 8.0], ht=[60.0, 25.0, 50.0]), index=[7, 8, 9])
    a = attach(d, st)
    assert list(a.index) == [7, 8, 9] and a.st_rel_d.iloc[0] == 0.5 and abs(a.st_rel_h.iloc[0] - 60 / 90) < 1e-12 and a[['st_n5', 'st_rel_d']].iloc[1:].isna().all().all()
    print('selftest: 3 checks PASS')


if __name__ == '__main__':
    ap = argparse.ArgumentParser(); ap.add_argument('--selftest', action='store_true'); a = ap.parse_args()
    selftest() if a.selftest else ap.print_help()
