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
    """D53 slot frame: EVERY clean visit of a tree's subplot (curation's clean_subplot_years) up to the tree's last real record, so which
    visits are missing no longer depends on the rule's phantom rows (dropped). slot_kind: 'real' (tallied), 'pre' (before the first real
    record: existence unknown), 'gap' (a visit inside the tree's record without a real row: exists, size missing). Per slot: whether the
    tree's plot type was sampled at the visit (forested subplot / microplot condition), the subplot competition from the real rows, the
    condition (COND via the PLOT CN at that date; the tree's condid where it has a row, else its first condid, else 1); per tree: the first
    record's RECONCILECD (1 ingrowth, 3 missed live, ...) and statics from the first real record."""
    c = duckdb.connect(db, read_only=True)
    q = """
    WITH real AS (SELECT * FROM tree_trajectories WHERE intro_type = 'real'),
    sub AS (SELECT statecd, unitcd, countycd, plot, subp, measdate, sum(ba) sub_ba, sum(tpa) sub_tpa FROM real GROUP BY ALL),
    ct AS (SELECT STATECD statecd, UNITCD unitcd, COUNTYCD countycd, PLOT plot, SUBP subp, TREE tree, MEASDATE measdate, CCLCD cclcd, RECONCILECD rcd
           FROM curated_trees WHERE STATUSCD = 1),
    tr AS (SELECT r.statecd, r.unitcd, r.countycd, r.plot, r.subp, r.tree, min(r.measdate) d0, max(r.measdate) d1,
                  arg_min(r.condid, r.measdate) condid0, arg_min(r.species_symbol, r.measdate) species_symbol, arg_min(r.spgrpcd, r.measdate) spgrpcd,
                  arg_min(r.major_spgrpcd, r.measdate) major_spgrpcd, arg_min(r.sftwd_hrdwd, r.measdate) sftwd_hrdwd,
                  arg_min(r.epa_l3, r.measdate) epa_l3, arg_min(r.epa_l4, r.measdate) epa_l4, arg_min(r.land_use, r.measdate) land_use
           FROM real r GROUP BY ALL),
    rc AS (SELECT ct.statecd, ct.unitcd, ct.countycd, ct.plot, ct.subp, ct.tree, ct.rcd rcd0 FROM ct JOIN tr USING (statecd, unitcd, countycd, plot, subp, tree)
           WHERE ct.measdate = tr.d0),
    vis AS (SELECT STATECD statecd, UNITCD unitcd, COUNTYCD countycd, PLOT plot, SUBP subp, MEASDATE measdate,
                   forest_subpcond_prop, forest_micrcond_prop FROM clean_subplot_years),
    fr AS (SELECT tr.*, vis.measdate, vis.forest_subpcond_prop, vis.forest_micrcond_prop FROM tr
           JOIN vis USING (statecd, unitcd, countycd, plot, subp) WHERE vis.measdate <= tr.d1
           UNION ALL                                              -- real rows on visits missing from the clean list (kept)
           SELECT tr.*, r.measdate, NULL, NULL FROM tr JOIN real r USING (statecd, unitcd, countycd, plot, subp, tree)
           WHERE NOT EXISTS (SELECT 1 FROM vis v WHERE v.statecd = r.statecd AND v.unitcd = r.unitcd AND v.countycd = r.countycd
                             AND v.plot = r.plot AND v.subp = r.subp AND v.measdate = r.measdate)),
    pc AS (SELECT STATECD statecd, UNITCD unitcd, COUNTYCD countycd, PLOT plot, make_date(MEASYEAR::INT, MEASMON::INT, MEASDAY::INT) md,
                  max(CN) cn FROM PLOT GROUP BY ALL)   -- tree_trajectories.plt_cn is the plot key, not the FIA CN
    SELECT f.statecd, f.unitcd, f.countycd, f.plot, f.subp, f.tree, f.measdate,
           CASE WHEN t.tree IS NOT NULL THEN 'real' WHEN f.measdate < f.d0 THEN 'pre' ELSE 'gap' END slot_kind,
           CASE WHEN t.tree IS NOT NULL THEN 'real' ELSE 'missing' END intro_type,
           t.age_calc, t.dia, t.ht, t.cr, ct.cclcd, t.drybio_g, t.tpa, coalesce(t.ba, 0) own_ba,
           f.species_symbol, f.spgrpcd, f.major_spgrpcd, f.sftwd_hrdwd, f.epa_l3, f.epa_l4, f.land_use, rc.rcd0,
           f.forest_subpcond_prop, f.forest_micrcond_prop,
           coalesce(sub.sub_ba, 0) sub_ba, coalesce(sub.sub_tpa, 0) sub_tpa,
           coalesce(c1.SICOND, c0.SICOND) sicond, coalesce(c1.SITECLCD, c0.SITECLCD) siteclcd, coalesce(c1.PHYSCLCD, c0.PHYSCLCD) physclcd,
           coalesce(c1.SLOPE, c0.SLOPE) slope, coalesce(c1.ASPECT, c0.ASPECT) aspect,
           coalesce(c1.TRTCD1, c0.TRTCD1) trtcd1, coalesce(c1.DSTRBCD1, c0.DSTRBCD1) dstrbcd1,
           coalesce(c1.STDAGE, c0.STDAGE) stdage, coalesce(c1.FLDAGE, c0.FLDAGE) fldage, coalesce(c1.BALIVE, c0.BALIVE) balive,
           coalesce(c1.FORTYPCD, c0.FORTYPCD) fortypcd, coalesce(c1.STDSZCD, c0.STDSZCD) stdszcd, coalesce(c1.ALSTK, c0.ALSTK) alstk,
           coalesce(c1.LIVE_CANOPY_CVR_PCT, c0.LIVE_CANOPY_CVR_PCT) canopy_pct, coalesce(c1.STDORGCD, c0.STDORGCD) stdorgcd
    FROM fr f
    LEFT JOIN real t USING (statecd, unitcd, countycd, plot, subp, tree, measdate)
    LEFT JOIN ct USING (statecd, unitcd, countycd, plot, subp, tree, measdate)
    LEFT JOIN rc USING (statecd, unitcd, countycd, plot, subp, tree)
    LEFT JOIN sub USING (statecd, unitcd, countycd, plot, subp, measdate)
    LEFT JOIN pc ON pc.statecd = f.statecd AND pc.unitcd = f.unitcd AND pc.countycd = f.countycd AND pc.plot = f.plot AND pc.md = f.measdate
    LEFT JOIN COND c1 ON c1.PLT_CN = pc.cn AND c1.CONDID = coalesce(t.condid, f.condid0)
    LEFT JOIN COND c0 ON c0.PLT_CN = pc.cn AND c0.CONDID = 1
    ORDER BY f.statecd, f.unitcd, f.countycd, f.plot, f.subp, f.tree, f.measdate"""
    df = pl.from_arrow(c.execute(q).arrow()); c.close()
    return df

NB_MIN_IN = 5.0          # neighbours = trees >= 5 in at both ends of the interval: visible in production AND in the 3- / 5-in benchmarks

def add_neighbours(df: pl.DataFrame) -> pl.DataFrame:
    """Per slot: leave-one-out mean annual log-DIA increment of the OTHER trees on the same subplot / plot over the interval ending at the
    visit (both records >= NB_MIN_IN, real), and their counts. The local growth environment, observable whenever the visit happened."""
    k = ['statecd', 'unitcd', 'countycd', 'plot', 'subp', 'tree']; sk, pk = k[:5], k[:4]
    r = (df.filter(pl.col('obs')).sort(k + ['measdate'])
           .with_columns(pl.col('dia').shift(1).over(k).alias('_d0'), pl.col('measdate').shift(1).over(k).alias('_t0'))
           .with_columns(((pl.col('measdate') - pl.col('_t0')).dt.total_days() / 365.25).alias('_dt'))
           .filter((pl.col('dia') >= NB_MIN_IN) & (pl.col('_d0') >= NB_MIN_IN) & (pl.col('_dt') > 0.5))
           .with_columns(((pl.col('dia').log() - pl.col('_d0').log()) / pl.col('_dt')).clip(-0.05, 0.5).alias('_inc'))
           .select(k + ['measdate', '_inc']))
    out = df.join(r, on=k + ['measdate'], how='left')
    for g, nm in ((sk, 'sub'), (pk, 'plt')):
        a = r.group_by(g + ['measdate']).agg(pl.col('_inc').sum().alias('_s'), pl.len().alias('_n'))
        out = out.join(a, on=g + ['measdate'], how='left').with_columns(
            pl.col('_s').fill_null(0.0), pl.col('_n').fill_null(0)).with_columns(
            (pl.col('_n') - pl.col('_inc').is_not_null().cast(pl.Int64)).alias(f'nb_n_{nm}'),
            (pl.col('_s') - pl.col('_inc').fill_null(0.0)).alias('_s2')).with_columns(
            pl.when(pl.col(f'nb_n_{nm}') > 0).then(pl.col('_s2') / pl.col(f'nb_n_{nm}')).otherwise(None).alias(f'nb_inc_{nm}')).drop('_s', '_n', '_s2')
    return out.drop('_inc')

def add_climate(df: pl.DataFrame, prism: str) -> pl.DataFrame:
    """Per slot: mean yearly tmean / ppt / vpdmax of its L4 over the interval ending at the slot (calendar years (prev slot, slot], first slot:
    the 5 years before it; clamped to the PRISM range), as anomalies against the L4 1991-2020 normal; plus the normals themselves (D51)."""
    k = ['statecd', 'unitcd', 'countycd', 'plot', 'subp', 'tree']
    d = df.with_row_index('_r').sort(k + ['measdate'])
    d = d.with_columns(pl.col('measdate').dt.year().alias('_ye'), pl.col('measdate').shift(1).over(k).dt.year().alias('_yp'))
    d = d.with_columns(pl.coalesce([pl.col('_yp') + 1, pl.col('_ye') - 4]).alias('_ys'))
    c = duckdb.connect(); c.execute(f"ATTACH '{prism}' AS p (READ_ONLY)")
    c.execute("""CREATE TEMP TABLE y AS SELECT EPA_L4_ID l4, YEAR yr, CAST(CAST(tmean_mean AS VARCHAR) AS DOUBLE) tm, CAST(CAST(ppt_mean AS VARCHAR) AS DOUBLE) pp,
                 CAST(CAST(vpdmax_mean AS VARCHAR) AS DOUBLE) vp FROM p.EPA_L4_yearly""")
    c.execute("CREATE TEMP TABLE nrm AS SELECT l4, avg(tm) tm_n, avg(pp) pp_n, avg(vp) vp_n FROM y WHERE yr BETWEEN 1991 AND 2020 GROUP BY l4")
    ymax = c.execute("SELECT max(yr) FROM y").fetchone()[0]
    sl = d.select('_r', pl.col('epa_l4').alias('l4'), pl.col('_ys').clip(upper_bound=ymax).alias('ys'), pl.col('_ye').clip(upper_bound=ymax).alias('ye'))
    c.register('sl', sl.to_arrow())
    cl = pl.from_arrow(c.execute("""SELECT sl._r, avg(y.tm) - n.tm_n tm_anom, avg(y.pp) / n.pp_n - 1 pp_anom, avg(y.vp) - n.vp_n vp_anom,
                                   n.tm_n, n.pp_n, n.vp_n
                                   FROM sl JOIN y ON y.l4 = sl.l4 AND y.yr BETWEEN least(sl.ys, sl.ye) AND sl.ye JOIN nrm n ON n.l4 = sl.l4
                                   GROUP BY sl._r, n.tm_n, n.pp_n, n.vp_n""").arrow())
    return df.with_row_index('_r').join(cl, on='_r', how='left').drop('_r')

def resplit_r2(split: str, key: str) -> str:
    """D50 fresh split: the old TEST is retired (-> train); the new TEST = old-train plots with crc32(key|r2) % 100 < 22; VAL unchanged."""
    if split == 'test': return 'train'
    if split == 'train' and zlib.crc32((key + '|r2').encode()) % 100 < 22: return 'test'
    return split

def _never_scored(key: str) -> bool:
    """Not in the D49 test (split_of) and not in the D50 test (r2 rule)."""
    return split_of(key) != 'test' and not (split_of(key) == 'train' and zlib.crc32((key + '|r2').encode()) % 100 < 22)

def resplit_r3(split: str, key: str) -> str:
    """D51 fresh split on the D50 labels: both earlier tests -> train (the D49 test was moved to train by r2, the D50 test is 'test'); the new
    TEST = train plots never scored before (not in either earlier test) with crc32(key|r3) % 100 < 18; VAL unchanged."""
    if split == 'test': return 'train'
    if split == 'train' and _never_scored(key) and zlib.crc32((key + '|r3').encode()) % 100 < 18: return 'test'
    return split

def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--out', required=True); ap.add_argument('dbs', nargs='*')
    ap.add_argument('--from-seq', help='re-split an existing seq.parquet instead of reading the DBs')
    ap.add_argument('--scheme', default='d49', choices=['d49', 'r2', 'r3'])
    ap.add_argument('--prism', help='PRISM L4 yearly DB: add the D51 climate drivers'); a = ap.parse_args()
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
        df = add_neighbours(df)
    if a.prism: df = add_climate(df, a.prism)
    if a.scheme in ('r2', 'r3'):
        fn = resplit_r2 if a.scheme == 'r2' else resplit_r3
        keys = df.select('plot_key', 'split').unique()
        keys = keys.with_columns(pl.struct('split', 'plot_key').map_elements(lambda r: fn(r['split'], r['plot_key']),
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
