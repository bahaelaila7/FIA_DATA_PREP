"""nimiwae_age_prep.py — measured age records for the joint age model (DECISIONS.md D70).

  python -I nimiwae_age_prep.py --fiadb FIADB --prism PRISM --solus SOLUS.parquet --out DIR

Writes DIR/age_rec.parquet: one row per measured age. Read-only on FIADB. Natural ids only.
  site  SITETREE.AGEDIA, R4, annual; rows repeating an earlier row's age, DIA and height are carried forward and left out.
  tot   TREE.TOTAGE, RSCD 22 and 26, annual, live.
  bha   TREE.BHAGE,  RSCD 22 and 26, annual, live.
Diameter must be at breast height (DIAHTCD 1): a root-collar diameter is not on the same scale. HI and the territories are left out: no climate.
first_of_tree marks a tree's first age record per source; later ones are repeats, used to test the step with elapsed time. STDAGE, FLDAGE and SICOND are not
read: SICOND is computed from the site trees themselves."""
import os, sys, argparse
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import duckdb, polars as pl
from nimiwae_prep import split_of, resplit_r3, SOIL

R4 = (1, 12, 13, 21, 28, 37, 45, 47)
WEST = (4, 8, 16, 30, 32, 35, 49, 56, 6, 41, 53)     # RSCD 22 and 26, conterminous states
MD = "MAKE_DATE(P.MEASYEAR::INT, COALESCE(NULLIF(P.MEASMON::INT, 0), 6), COALESCE(NULLIF(P.MEASDAY::INT, 0), 15))"
VISIT = """JOIN f.main.PLOT P ON P.STATECD = R.STATECD AND P.UNITCD = R.UNITCD AND P.COUNTYCD = R.COUNTYCD AND P.PLOT = R.PLOT AND P.INVYR = R.INVYR
           JOIN (SELECT DISTINCT STATECD, INVYR, RSCD FROM f.main.SURVEY WHERE ANN_INVENTORY = 'Y' AND P3_OZONE_IND = 'N') V ON V.STATECD = R.STATECD AND V.INVYR = R.INVYR"""


def records(c):
    st = lambda x: ",".join(map(str, x))
    common = f"R.STATECD statecd, R.UNITCD unitcd, R.COUNTYCD countycd, R.PLOT plot, R.INVYR invyr, {MD} measdate, R.SPCD spcd, R.SPGRPCD spgrpcd, R.DIA::DOUBLE dia, R.HT::DOUBLE ht"
    # carried forward = the same plot, number, species, age, DIA and height at an earlier visit: keep the earliest visit of each such group
    site_r = f"""(SELECT * FROM f.main.SITETREE WHERE STATECD IN ({st(R4)})
                  QUALIFY INVYR = min(INVYR) OVER (PARTITION BY STATECD, UNITCD, COUNTYCD, PLOT, TREE, SPCD, AGEDIA, DIA, HT))"""
    site = f"""SELECT 'site' AS source, {common}, NULL::INT cc, R.TREE::BIGINT tree, R.SUBP::INT subp, TRY_CAST(R.AGEDIA AS DOUBLE) a_obs
        FROM {site_r} R {VISIT} WHERE TRY_CAST(R.AGEDIA AS DOUBLE) > 0 AND R.DIA > 0"""
    tally = lambda src, col: f"""SELECT '{src}' AS source, {common}, R.CCLCD::INT cc, R.TREE::BIGINT tree, R.SUBP::INT subp, TRY_CAST(R.{col} AS DOUBLE) a_obs
        FROM f.main.TREE R {VISIT} WHERE R.STATECD IN ({st(WEST)}) AND V.RSCD IN (22, 26) AND R.STATUSCD = 1 AND TRY_CAST(R.{col} AS DOUBLE) > 0 AND R.DIA > 0 AND R.DIAHTCD = 1"""
    return pl.concat([c.execute(q).pl() for q in (site, tally('tot', 'TOTAGE'), tally('bha', 'BHAGE'))], how='vertical_relaxed')


def attach(df, c, prism, solus):
    ref = c.execute("SELECT SPCD spcd, MAJOR_SPGRPCD major_spgrpcd, SFTWD_HRDWD sftwd_hrdwd FROM f.main.REF_SPECIES").pl()
    eco = c.execute("SELECT statecd, unitcd, countycd, plot, epa_l3, epa_l4 FROM f.main.data_plot_eco").pl()
    k = ['statecd', 'unitcd', 'countycd', 'plot']
    cast = lambda d, cols: d.with_columns([pl.col(x).cast(df.schema[x]) for x in cols])
    df = df.join(cast(ref, ['spcd']), on='spcd', how='left').join(cast(eco, k), on=k, how='left')
    q = duckdb.connect(); q.execute(f"ATTACH '{prism}' AS p (READ_ONLY)")
    nrm = pl.from_arrow(q.execute("""SELECT EPA_L4_ID epa_l4, avg(CAST(CAST(tmean_mean AS VARCHAR) AS DOUBLE)) tm_n, avg(CAST(CAST(ppt_mean AS VARCHAR) AS DOUBLE)) pp_n,
                                     avg(CAST(CAST(vpdmax_mean AS VARCHAR) AS DOUBLE)) vp_n FROM p.EPA_L4_yearly WHERE YEAR BETWEEN 1991 AND 2020 GROUP BY 1""").arrow())
    df = df.join(nrm.with_columns(pl.col('epa_l4').cast(df.schema['epa_l4'])), on='epa_l4', how='left')
    so = pl.read_parquet(solus).select(k + SOIL).rename({x: f'soil_{x}' for x in SOIL})
    return df.join(cast(so, k), on=k, how='left')


def finish(df):
    key = pl.concat_str([pl.col(x).cast(pl.Utf8) for x in ('statecd', 'unitcd', 'countycd', 'plot')], separator='_')
    df = df.with_columns(key.alias('plot_key'))
    keys = df.select('plot_key').unique()
    keys = keys.with_columns(pl.col('plot_key').map_elements(lambda s: resplit_r3(split_of(s), s), return_dtype=pl.Utf8).alias('split'))
    df = df.join(keys, on='plot_key', how='left')
    tid = ['source', 'statecd', 'unitcd', 'countycd', 'plot', 'subp', 'tree', 'spcd']
    return (df.with_columns((pl.col('invyr') == pl.col('invyr').min().over(tid)).alias('first_of_tree'))
              .with_columns((pl.col('invyr').rank('dense').over(tid) - 1).cast(pl.Int32).alias('rec_no')))


def selftest():
    df = pl.DataFrame({'source': ['tot', 'tot', 'site'], 'statecd': [4, 4, 1], 'unitcd': [1, 1, 1], 'countycd': [1, 1, 1], 'plot': [9, 9, 9], 'subp': [1, 1, 1], 'tree': [1, 1, 1],
                       'spcd': [122, 122, 131], 'invyr': [2005, 2015, 2010], 'a_obs': [30.0, 40.0, 25.0]})
    o = finish(df).sort(['source', 'invyr'])
    assert o['first_of_tree'].to_list() == [True, True, False] and o['rec_no'].to_list() == [0, 0, 1]
    assert o.filter(pl.col('plot_key') == '4_1_1_9')['split'].n_unique() == 1                       # one split per plot
    print('selftest: 2 checks PASS')


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    for x in ('--fiadb', '--prism', '--solus', '--out'): ap.add_argument(x)
    ap.add_argument('--selftest', action='store_true'); a = ap.parse_args()
    if a.selftest:
        selftest(); raise SystemExit
    os.makedirs(a.out, exist_ok=True)
    c = duckdb.connect(); c.execute("SET threads = 4"); c.execute(f"ATTACH '{a.fiadb}' AS f (READ_ONLY)")
    df = finish(attach(records(c), c, a.prism, a.solus))
    df.write_parquet(f'{a.out}/age_rec.parquet')
    pl.Config.set_tbl_rows(20); pl.Config.set_tbl_cols(10)
    print(df.group_by('source', 'split').agg(pl.len(), pl.col('tm_n').is_null().sum().alias('no_clim'), pl.col('soil_clay').is_null().sum().alias('no_soil'),
                                             pl.col('first_of_tree').sum().alias('first'), (pl.col('dia') < 5).sum().alias('lt5')).sort('source', 'split'))
