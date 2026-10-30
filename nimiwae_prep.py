"""D49 stage 1 — NIMIWAE phantom backfill: training sequences, the pre-registered pseudo-censoring benchmark and the current rule's
predictions on it (DECISIONS.md D49).

Inputs : curated DBs (tree_trajectories, READ_ONLY), one per state run (e.g. /workspace/FIA/curated_r8/<ST>/curated.duckdb).
Outputs: <out>/seq.parquet    one row per (tree, slot): real visits (observed) and the tree's phantom slots (missing), with covariates
         <out>/bench.parquet  VAL + TEST benchmark trees' real visits (split column): hidden flag (pre-pseudo-debut, DIA < 3 in), truth, rule prediction
Splits : by crc32 of the plot key: < 70 train, < 85 val, else test.
The rule baseline is the production rule (cohorts_landis.estimate_sapling_growth_rates, per-tree basis, floor MIN_BACKFILL_DRYBIO_G) with
rates estimated on the TRAIN trees only and the first VISIBLE (>= 3 in) record as the debut.
Usage: python -I nimiwae_prep.py --out DIR DB [DB ...]"""
import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import argparse, zlib
import duckdb, numpy as np, polars as pl
from cohorts_landis import estimate_sapling_growth_rates, MIN_BACKFILL_DRYBIO_G, TREE_ID

PSEUDO_IN = 3.0          # D49 pseudo tally limit (in)
MICRO_TPA = 50.0         # debut TPA above this = microplot sapling (74.965), else subplot tree (6.018)

def split_of(key: str) -> str:
    h = zlib.crc32(key.encode()) % 100
    return 'train' if h < 70 else 'val' if h < 85 else 'test'

def load(db: str) -> pl.DataFrame:
    c = duckdb.connect(db, read_only=True)
    q = """
    WITH ba AS (SELECT statecd, unitcd, countycd, plot, subp, measdate, sum(ba) sub_ba FROM tree_trajectories
                WHERE intro_type = 'real' GROUP BY ALL)
    SELECT t.statecd, t.unitcd, t.countycd, t.plot, t.subp, t.tree, t.measdate, t.age_calc, t.dia, t.ht, t.drybio_g, t.tpa,
           t.intro_type, t.species_symbol, t.spgrpcd, t.major_spgrpcd, t.sftwd_hrdwd, t.epa_l3, t.land_use, coalesce(ba.sub_ba, 0) sub_ba
    FROM tree_trajectories t LEFT JOIN ba USING (statecd, unitcd, countycd, plot, subp, measdate)
    WHERE t.intro_type IN ('real', 'phantom_backfill')
    ORDER BY t.statecd, t.unitcd, t.countycd, t.plot, t.subp, t.tree, t.measdate"""
    df = pl.from_arrow(c.execute(q).arrow()); c.close()
    return df

def resplit_r2(split: str, key: str) -> str:
    """D50 fresh split: the old TEST is retired (-> train); the new TEST = old-train plots with crc32(key|r2) % 100 < 22; VAL unchanged."""
    if split == 'test': return 'train'
    if split == 'train' and zlib.crc32((key + '|r2').encode()) % 100 < 22: return 'test'
    return split

def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--out', required=True); ap.add_argument('dbs', nargs='*')
    ap.add_argument('--from-seq', help='re-split an existing seq.parquet instead of reading the DBs')
    ap.add_argument('--scheme', default='d49', choices=['d49', 'r2']); a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    k = ['statecd', 'unitcd', 'countycd', 'plot', 'subp', 'tree']
    if a.from_seq:
        df = pl.read_parquet(a.from_seq)
    else:
        df = pl.concat([load(d) for d in a.dbs], how='vertical_relaxed')
        df = df.with_columns(
            pl.concat_str([pl.col(c).cast(pl.Utf8) for c in k[:4]], separator='_').alias('plot_key'),
            (pl.col('intro_type') == 'real').alias('obs'),
            (pl.col('land_use') == 'artificial').alias('planted'))
        keys = df.select('plot_key').unique()
        keys = keys.with_columns(pl.col('plot_key').map_elements(split_of, return_dtype=pl.Utf8).alias('split'))
        df = df.join(keys, on='plot_key', how='left')
        # debut (first real record) per tree: tally (microplot vs subplot) -> the true tally threshold
        deb = (df.filter(pl.col('obs')).group_by(k).agg(pl.col('tpa').sort_by('measdate').first().alias('tpa0'),
                                                         pl.col('dia').sort_by('measdate').first().alias('dia0'),
                                                         pl.len().alias('n_real')))
        df = df.join(deb, on=k, how='inner').with_columns(
            pl.when(pl.col('tpa0') > MICRO_TPA).then(1.0).otherwise(5.0).alias('thr_in'))
    if a.scheme == 'r2':
        keys = df.select('plot_key', 'split').unique()
        keys = keys.with_columns(pl.struct('split', 'plot_key').map_elements(lambda r: resplit_r2(r['split'], r['plot_key']),
                                                                            return_dtype=pl.Utf8).alias('split2'))
        df = df.join(keys.select('plot_key', 'split2'), on='plot_key', how='left').drop('split').rename({'split2': 'split'})
    df.write_parquet(f'{a.out}/seq.parquet')
    print(f'seq: {df.height:,} slots, {df.select(k).unique().height:,} trees; by split',
          df.group_by('split').agg(pl.len()).sort('split').rows())

    # rule rates from TRAIN real records only (production estimator, per-tree basis)
    up = {c: c.upper() for c in k}
    tr = (df.filter(pl.col('obs') & (pl.col('split') == 'train'))
            .rename(up).rename({'measdate': 'MEASDATE'})
            .with_columns(pl.col('drybio_g').alias('agb'), pl.lit(None, dtype=pl.Utf8).alias('intro_type')))
    sp, grp, glb = estimate_sapling_growth_rates(tr, basis='per_tree', tag=None)

    # benchmark trees (VAL for model choices, TEST once): microplot debut below 3 in, >= 3 real visits, a visit < 3 in and a later one >= 3 in
    r = df.filter(pl.col('obs') & pl.col('split').is_in(['val', 'test'])).sort(k + ['measdate'])
    r = r.with_columns(pl.col('dia').ge(PSEUDO_IN).cum_sum().over(k).alias('_nvis'))
    elig = (r.group_by(k).agg(pl.col('tpa0').first(), pl.col('dia0').first(), pl.col('n_real').first(),
                              (pl.col('_nvis') == 0).sum().alias('n_hid'), (pl.col('_nvis') > 0).sum().alias('n_vis'))
              .filter((pl.col('tpa0') > MICRO_TPA) & (pl.col('dia0') < PSEUDO_IN) & (pl.col('n_real') >= 3)
                      & (pl.col('n_hid') >= 1) & (pl.col('n_vis') >= 1)).select(k))
    b = r.join(elig, on=k, how='inner').with_columns((pl.col('_nvis') == 0).alias('hidden'))
    vis0 = (b.filter(~pl.col('hidden')).group_by(k).agg(pl.col('age_calc').sort_by('measdate').first().alias('age_deb'),
                                                        pl.col('drybio_g').sort_by('measdate').first().alias('agb_deb')))
    b = (b.join(vis0, on=k, how='left')
          .join(sp.rename({'rate': '_r_sp'}), on=['species_symbol', 'epa_l3'], how='left')
          .join(grp.rename({'rate': '_r_grp'}), on=['spgrpcd', 'epa_l3'], how='left')
          .join(glb.rename({'rate': '_r_glb'}), on='spgrpcd', how='left')
          .with_columns(pl.coalesce(['_r_sp', '_r_grp', '_r_glb']).alias('rate'))
          .with_columns(pl.when(pl.col('rate').is_not_null())
                          .then((pl.col('agb_deb') * (1 - pl.col('rate') * (pl.col('age_deb') - pl.col('age_calc')).cast(pl.Float64)))
                                .clip(MIN_BACKFILL_DRYBIO_G, None))
                          .otherwise(pl.lit(MIN_BACKFILL_DRYBIO_G))
                          .clip(upper_bound=pl.col('agb_deb')).alias('rule_agb'))
          .drop(['_r_sp', '_r_grp', '_r_glb', '_nvis']))
    b.write_parquet(f'{a.out}/bench.parquet')
    h = b.filter(pl.col('hidden'))
    for sp_ in ('val', 'test'):
        hs = h.filter(pl.col('split') == sp_)
        print(f'bench {sp_}: {b.filter(pl.col("split") == sp_).select(k).unique().height:,} trees, {hs.height:,} hidden visits '
              f'(DIA 1-2 in {hs.filter(pl.col("dia") < 2).height:,}, 2-3 in {hs.filter(pl.col("dia") >= 2).height:,})')

if __name__ == '__main__':
    main()
