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
# Plot-design tally levels are DERIVED FROM THE DATA (D54 am. 9; no hard-coded TPA): per (DESIGNCD, TPA_UNADJ class) the minimum DIA
# tallied = that level's threshold; levels ordered by TPA (largest TPA = smallest plot: microplot, then subplot, then macroplot).

def split_of(key: str) -> str:
    h = zlib.crc32(key.encode()) % 100
    return 'train' if h < 70 else 'val' if h < 85 else 'test'

def load54(db: str) -> pl.DataFrame:
    """D54 lineage frame. Counted plot visits = at least one subplot with a forested condition; a subplot's span = its first to last
    such visit; a tree's lineage = every counted visit in its subplot's span (before its debut, during and after its record).
    slot_kind real / pre / gap / post. Per slot (D55 am. 7): the condition fields are SUBPLOT-level at the visit, area-weighted by
    SUBPCOND_PROP over the subplot's accessible-forest conditions (all its conditions when it has none): weighted means (SICOND, SLOPE,
    canopy; ASPECT circular), area-dominant classes (FORTYPCD, SITECLCD, PHYSCLCD, STDORGCD), the area share carrying each treatment /
    disturbance code group (trt_s*, dst_s*). No CONDID is carried between visits (CONDIDs are per visit in FIADB); the subplot / microplot
    forested shares give the sampling probability. Per tree: first-record RECONCILECD, curated terminal_fate, first dead record (date, AGENTCD), first status-0 event
    (date, RC). Stand fields that include the tree itself (BALIVE, ALSTK, STDSZCD) are not loaded. D54 am. 5: resolved species code and a
    "species revised" flag per tree; per real record TREECLCD, ACTUALHT and, from FIADB TREE by CN, DAMAGE_AGENT_CD1-3 (recorded 2013+)."""
    c = duckdb.connect(db, read_only=True)
    K = "statecd, unitcd, countycd, plot, subp, tree"
    q = f"""
    WITH real AS (SELECT * FROM tree_trajectories WHERE intro_type = 'real'),
    csy AS (SELECT STATECD statecd, UNITCD unitcd, COUNTYCD countycd, PLOT plot, SUBP subp, INVYR invyr, MEASDATE measdate,
                   forest_subpcond_prop fsp, forest_micrcond_prop fmp FROM clean_subplot_years),
    pv AS (SELECT statecd, unitcd, countycd, plot, invyr, measdate FROM csy GROUP BY ALL HAVING max(fsp) > 0),
    span AS (SELECT statecd, unitcd, countycd, plot, subp, min(measdate) s0, max(measdate) s1 FROM csy WHERE fsp > 0 GROUP BY ALL),
    sv AS (SELECT sp.statecd, sp.unitcd, sp.countycd, sp.plot, sp.subp, pv.invyr, pv.measdate, csy.fsp, csy.fmp FROM span sp
           JOIN pv USING (statecd, unitcd, countycd, plot)
           LEFT JOIN csy USING (statecd, unitcd, countycd, plot, subp, invyr) WHERE pv.measdate BETWEEN sp.s0 AND sp.s1),
    ct AS (SELECT STATECD statecd, UNITCD unitcd, COUNTYCD countycd, PLOT plot, SUBP subp, TREE tree, MEASDATE measdate, STATUSCD statuscd,
                  CCLCD cclcd, RECONCILECD rcd, AGENTCD agentcd, INVYR invyr, TREECLCD treeclcd, ACTUALHT actualht,
                  unitcd_raw, countycd_raw, plot_raw FROM curated_trees),
    dm AS (SELECT ct.statecd, ct.unitcd, ct.countycd, ct.plot, ct.subp, ct.tree, ct.invyr,
                  ft.DAMAGE_AGENT_CD1 dmg1, ft.DAMAGE_AGENT_CD2 dmg2, ft.DAMAGE_AGENT_CD3 dmg3 FROM ct
           JOIN raw.TREE ft ON ft.STATECD = ct.statecd AND ft.UNITCD = ct.unitcd_raw AND ft.COUNTYCD = ct.countycd_raw AND ft.PLOT = ct.plot_raw
                AND ft.INVYR = ct.invyr AND ft.SUBP = ct.subp AND ft.TREE = ct.tree % 100000 WHERE ct.statuscd = 1),   -- as-recorded key; curated TREE = raw number + 100000 k after a reuse split
    tr AS (SELECT {K}, min(measdate) d0, max(measdate) d1, min(invyr) y0, arg_min(condid, measdate) condid0,
                  arg_min(species_symbol, measdate) species_symbol, arg_min(spgrpcd, measdate) spgrpcd, arg_min(major_spgrpcd, measdate) major_spgrpcd,
                  arg_min(sftwd_hrdwd, measdate) sftwd_hrdwd, arg_min(epa_l3, measdate) epa_l3, arg_min(epa_l4, measdate) epa_l4,
                  arg_min(land_use, measdate) land_use, arg_max(terminal_fate, measdate) terminal_fate,
                  arg_max(spcd, measdate) spcd, bool_or(coalesce(spcd_changed, false)) sp_revised
           FROM real GROUP BY ALL),
    rc AS (SELECT ct.statecd, ct.unitcd, ct.countycd, ct.plot, ct.subp, ct.tree, ct.rcd rcd0 FROM ct JOIN tr USING ({K})
           WHERE ct.invyr = tr.y0 AND ct.statuscd = 1),
    dr AS (SELECT ct.statecd, ct.unitcd, ct.countycd, ct.plot, ct.subp, ct.tree, min(ct.invyr) dead_invyr, min(ct.measdate) dead_md, arg_min(ct.agentcd, ct.measdate) dead_agent
           FROM ct JOIN tr USING ({K}) WHERE ct.statuscd = 2 AND ct.measdate > tr.d1 GROUP BY ALL),
    s0 AS (SELECT e.STATECD statecd, e.UNITCD unitcd, e.COUNTYCD countycd, e.PLOT plot, e.SUBP subp, e.TREE tree,
                  min(e.MEASDATE) s0_md, arg_min(e.RC, e.MEASDATE) s0_rc
           FROM tree_status0_events e JOIN tr ON tr.statecd = e.STATECD AND tr.unitcd = e.UNITCD AND tr.countycd = e.COUNTYCD
                AND tr.plot = e.PLOT AND tr.subp = e.SUBP AND tr.tree = e.TREE AND e.MEASDATE > tr.d1 GROUP BY ALL),
    fr AS (SELECT tr.*, sv.invyr, sv.measdate, sv.fsp, sv.fmp FROM tr JOIN sv USING (statecd, unitcd, countycd, plot, subp)
           UNION ALL
           SELECT tr.*, r.invyr, r.measdate, NULL, NULL FROM tr JOIN real r USING ({K})
           WHERE NOT EXISTS (SELECT 1 FROM sv WHERE sv.statecd = r.statecd AND sv.unitcd = r.unitcd AND sv.countycd = r.countycd
                             AND sv.plot = r.plot AND sv.subp = r.subp AND sv.invyr = r.invyr)),
    sub AS (SELECT statecd, unitcd, countycd, plot, subp, invyr, sum(ba) sub_ba, sum(tpa) sub_tpa FROM real GROUP BY ALL),
    bc AS (SELECT b.STATECD statecd, b.UNITCD unitcd, b.COUNTYCD countycd, b.PLOT plot, b.SUBP subp, b.INVYR invyr,
                  b.COND_STATUS_CD cs, coalesce(b.SUBPCOND_PROP, 0) sp, b.SICOND, b.SLOPE, b.ASPECT, b.FORTYPCD, b.SITECLCD, b.STDORGCD,
                  cd.PHYSCLCD, cd.LIVE_CANOPY_CVR_PCT canopy, [b.TRTCD1, b.TRTCD2, b.TRTCD3] trt, [b.DSTRBCD1, b.DSTRBCD2, b.DSTRBCD3] dst
           FROM base_subplot_cond b LEFT JOIN COND cd ON cd.STATECD = b.STATECD AND cd.UNITCD = b.UNITCD AND cd.COUNTYCD = b.COUNTYCD
                AND cd.PLOT = b.PLOT AND cd.INVYR = b.INVYR AND cd.CONDID = b.CONDID),
    bw AS (SELECT *, CASE WHEN sum(CASE WHEN cs = 1 THEN sp ELSE 0 END) OVER w > 0 THEN (CASE WHEN cs = 1 THEN sp ELSE 0 END) ELSE sp END wt
           FROM bc WINDOW w AS (PARTITION BY statecd, unitcd, countycd, plot, subp, invyr)),
    sa AS (SELECT statecd, unitcd, countycd, plot, subp, invyr,
                  sum(wt * SICOND) FILTER (WHERE SICOND IS NOT NULL) / nullif(sum(wt) FILTER (WHERE SICOND IS NOT NULL), 0) sicond,
                  sum(wt * SLOPE) FILTER (WHERE SLOPE IS NOT NULL) / nullif(sum(wt) FILTER (WHERE SLOPE IS NOT NULL), 0) slope,
                  CASE WHEN sum(wt) FILTER (WHERE ASPECT IS NOT NULL) > 0 THEN
                       (degrees(atan2(sum(wt * sin(radians(ASPECT))) FILTER (WHERE ASPECT IS NOT NULL),
                                      sum(wt * cos(radians(ASPECT))) FILTER (WHERE ASPECT IS NOT NULL))) + 360) % 360 END aspect,
                  sum(wt * canopy) FILTER (WHERE canopy IS NOT NULL) / nullif(sum(wt) FILTER (WHERE canopy IS NOT NULL), 0) canopy_pct,
                  arg_max(FORTYPCD, struct_pack(w := wt, v := FORTYPCD)) FILTER (WHERE FORTYPCD IS NOT NULL) fortypcd, arg_max(SITECLCD, struct_pack(w := wt, v := SITECLCD)) FILTER (WHERE SITECLCD IS NOT NULL) siteclcd,
                  arg_max(PHYSCLCD, struct_pack(w := wt, v := PHYSCLCD)) FILTER (WHERE PHYSCLCD IS NOT NULL) physclcd, arg_max(STDORGCD, struct_pack(w := wt, v := STDORGCD)) FILTER (WHERE STDORGCD IS NOT NULL) stdorgcd,
                  {", ".join(f"sum(wt * (len(list_filter(trt, x -> x >= {c} AND x < {c + 10})) > 0)::INT) / nullif(sum(wt), 0) trt_s{c}" for c in (10, 20, 30, 40, 50))},
                  {", ".join(f"sum(wt * (len(list_filter(dst, x -> x >= {c} AND x < {c + 10})) > 0)::INT) / nullif(sum(wt), 0) dst_s{c}" for c in (10, 20, 30, 40, 50, 60, 70, 80, 90))}
           FROM bw GROUP BY ALL),
    pc AS (SELECT STATECD statecd, UNITCD unitcd, COUNTYCD countycd, PLOT plot, INVYR invyr, DESIGNCD designcd FROM PLOT)
    SELECT f.statecd, f.unitcd, f.countycd, f.plot, f.subp, f.tree, f.invyr, f.measdate, pc.designcd,
           CASE WHEN t.tree IS NOT NULL THEN 'real' WHEN f.measdate < f.d0 THEN 'pre' WHEN f.measdate <= f.d1 THEN 'gap' ELSE 'post' END slot_kind,
           CASE WHEN t.tree IS NOT NULL THEN 'real' ELSE 'missing' END intro_type,
           t.age_calc, t.dia, t.ht, t.cr, ct.cclcd, t.drybio_g, t.tpa, coalesce(t.ba, 0) own_ba,
           f.species_symbol, f.spcd, f.sp_revised, f.spgrpcd, f.major_spgrpcd, f.sftwd_hrdwd, f.epa_l3, f.epa_l4, f.land_use,
           CASE WHEN t.tree IS NOT NULL THEN ct.treeclcd END treeclcd, CASE WHEN t.tree IS NOT NULL THEN ct.actualht END actualht,
           dm.dmg1, dm.dmg2, dm.dmg3,
           rc.rcd0, f.terminal_fate, dr.dead_invyr, dr.dead_md, dr.dead_agent, s0.s0_md, s0.s0_rc,
           f.fsp forest_subpcond_prop, f.fmp forest_micrcond_prop, NULL::INT cond_status,
           coalesce(sub.sub_ba, 0) sub_ba, coalesce(sub.sub_tpa, 0) sub_tpa,
           sa.sicond, sa.siteclcd, sa.physclcd, sa.slope, sa.aspect, sa.fortypcd, sa.canopy_pct, sa.stdorgcd,
           NULL::INT trtcd1, NULL::INT trtcd2, NULL::INT trtcd3, NULL::INT dstrbcd1, NULL::INT dstrbcd2, NULL::INT dstrbcd3,
           sa.trt_s10, sa.trt_s20, sa.trt_s30, sa.trt_s40, sa.trt_s50,
           sa.dst_s10, sa.dst_s20, sa.dst_s30, sa.dst_s40, sa.dst_s50, sa.dst_s60, sa.dst_s70, sa.dst_s80, sa.dst_s90
    FROM fr f
    LEFT JOIN real t USING ({K}, invyr)
    LEFT JOIN ct USING ({K}, invyr)
    LEFT JOIN dm ON dm.statecd = f.statecd AND dm.unitcd = f.unitcd AND dm.countycd = f.countycd AND dm.plot = f.plot AND dm.subp = f.subp
         AND dm.tree = f.tree AND dm.invyr = f.invyr AND t.tree IS NOT NULL
    LEFT JOIN rc USING ({K}) LEFT JOIN dr USING ({K}) LEFT JOIN s0 USING ({K})
    LEFT JOIN sub USING (statecd, unitcd, countycd, plot, subp, invyr)
    LEFT JOIN pc ON pc.statecd = f.statecd AND pc.unitcd = f.unitcd AND pc.countycd = f.countycd AND pc.plot = f.plot AND pc.invyr = f.invyr
    LEFT JOIN sa USING (statecd, unitcd, countycd, plot, subp, invyr)
    ORDER BY f.statecd, f.unitcd, f.countycd, f.plot, f.subp, f.tree, f.invyr"""
    df = pl.from_arrow(c.execute(q).arrow()); c.close()
    return df

# neighbours = trees at or above the design's SUBPLOT-level threshold at both ends of the interval (column nb_thr; 5 in for DESIGNCD 1):
# visible in production AND in the subplot-limit benchmark

def add_design_levels(df: pl.DataFrame, k) -> pl.DataFrame:
    """Per tree, from the data: thr_in = tally threshold of its debut record's level (min DIA tallied at that DESIGNCD x TPA class),
    thr_micro / thr_subp = the design's smallest / second level thresholds (thr_subp = thr_micro when the design has one level),
    nb_thr = thr_subp. No hard-coded TPA or limits."""
    if 'designcd' not in df.columns: df = df.with_columns(pl.lit(1).alias('designcd'))
    r = df.filter(pl.col('obs') & pl.col('tpa').is_not_null()).with_columns(pl.col('tpa').round(3).alias('_tp'))
    lv = (r.group_by('designcd', '_tp').agg(pl.col('dia').min().alias('_thr'), pl.len().alias('_n')).filter(pl.col('_n') >= 100)
            .with_columns((pl.col('_thr') * 10).floor() / 10).sort(['designcd', '_tp'], descending=[False, True]))
    lv = lv.with_columns(pl.col('_tp').rank('ordinal', descending=True).over('designcd').alias('_lvl'))
    print('design tally levels (DESIGNCD, TPA_UNADJ, threshold in, n, level):', lv.select('designcd', '_tp', '_thr', '_n', '_lvl').rows(), flush=True)
    dz = lv.group_by('designcd').agg(pl.col('_thr').filter(pl.col('_lvl') == 1).max().alias('thr_micro'),
                                     pl.col('_thr').filter(pl.col('_lvl') == 2).max().alias('_t2'))
    dz = dz.with_columns(pl.coalesce(['_t2', 'thr_micro']).alias('thr_subp')).drop('_t2')
    deb = (df.filter(pl.col('obs')).group_by(k).agg(pl.col('designcd').sort_by('measdate').first().alias('_dd'),
                                                                          pl.col('tpa').sort_by('measdate').first().round(3).alias('_tp')))
    deb = deb.join(lv.select(pl.col('designcd').alias('_dd'), '_tp', '_thr'), on=['_dd', '_tp'], how='left')
    deb = deb.join(dz.rename({'designcd': '_dd'}), on='_dd', how='left').with_columns(pl.col('_thr').fill_null(pl.col('thr_subp')).alias('thr_in'))
    return df.drop([c for c in ('thr_in', 'thr_micro', 'thr_subp', 'nb_thr') if c in df.columns]).join(
        deb.select(k + ['thr_in', 'thr_micro', 'thr_subp']), on=k, how='left').with_columns(pl.col('thr_subp').alias('nb_thr'))

def add_neighbours(df: pl.DataFrame) -> pl.DataFrame:
    """Per slot: leave-one-out mean annual log-DIA increment of the OTHER trees on the same subplot / plot over the interval ending at the
    visit (both records >= the design's subplot threshold nb_thr, real), and their counts. The local growth environment, observable whenever the visit happened."""
    k = ['statecd', 'unitcd', 'countycd', 'plot', 'subp', 'tree']; sk, pk = k[:5], k[:4]
    r = (df.filter(pl.col('obs')).sort(k + ['measdate'])
           .with_columns(pl.col('dia').shift(1).over(k, order_by='measdate').alias('_d0'), pl.col('measdate').shift(1).over(k, order_by='measdate').alias('_t0'))
           .with_columns(((pl.col('measdate') - pl.col('_t0')).dt.total_days() / 365.25).alias('_dt'))
           .filter((pl.col('dia') >= pl.col('nb_thr')) & (pl.col('_d0') >= pl.col('nb_thr')) & (pl.col('_dt') > 0.5))
           .with_columns(((pl.col('dia').log() - pl.col('_d0').log()) / pl.col('_dt')).clip(-0.05, 0.5).alias('_inc'))
           .select(k + ['invyr', '_inc']))
    out = df.join(r, on=k + ['invyr'], how='left')
    for g, nm in ((sk, 'sub'), (pk, 'plt')):
        a = r.group_by(g + ['invyr']).agg(pl.col('_inc').sum().alias('_s'), pl.len().alias('_n'))
        out = out.join(a, on=g + ['invyr'], how='left').with_columns(
            pl.col('_s').fill_null(0.0), pl.col('_n').fill_null(0)).with_columns(
            (pl.col('_n') - pl.col('_inc').is_not_null().cast(pl.Int64)).alias(f'nb_n_{nm}'),
            (pl.col('_s') - pl.col('_inc').fill_null(0.0)).alias('_s2')).with_columns(
            pl.when(pl.col(f'nb_n_{nm}') > 0).then(pl.col('_s2') / pl.col(f'nb_n_{nm}')).otherwise(None).alias(f'nb_inc_{nm}')).drop('_s', '_n', '_s2')
    return out.drop('_inc')

def add_climate(df: pl.DataFrame, prism: str) -> pl.DataFrame:
    """Per slot: mean yearly tmean / ppt / vpdmax of its L4 over the interval ending at the slot (calendar years (prev slot, slot], first slot:
    the 5 years before it; clamped to the PRISM range), as anomalies against the L4 1991-2020 normal; plus the normals themselves (D51)."""
    k = ['statecd', 'unitcd', 'countycd', 'plot', 'subp', 'tree']
    d = df.with_columns(pl.col('measdate').dt.year().alias('_ye'), pl.col('measdate').shift(1).over(k, order_by='measdate').dt.year().alias('_yp'))
    d = d.with_columns(pl.coalesce([pl.col('_yp') + 1, pl.col('_ye') - 4]).alias('_ys'))
    c = duckdb.connect(); c.execute(f"ATTACH '{prism}' AS p (READ_ONLY)")
    c.execute("""CREATE TEMP TABLE y AS SELECT EPA_L4_ID l4, YEAR yr, CAST(CAST(tmean_mean AS VARCHAR) AS DOUBLE) tm, CAST(CAST(ppt_mean AS VARCHAR) AS DOUBLE) pp,
                 CAST(CAST(vpdmax_mean AS VARCHAR) AS DOUBLE) vp FROM p.EPA_L4_yearly""")
    c.execute("CREATE TEMP TABLE nrm AS SELECT l4, avg(tm) tm_n, avg(pp) pp_n, avg(vp) vp_n FROM y WHERE yr BETWEEN 1991 AND 2020 GROUP BY l4")
    ymax = c.execute("SELECT max(yr) FROM y").fetchone()[0]
    sl = d.select(*k, 'invyr', pl.col('epa_l4').alias('l4'), pl.col('_ys').clip(upper_bound=ymax).alias('ys'), pl.col('_ye').clip(upper_bound=ymax).alias('ye'))
    c.register('sl', sl.to_arrow())
    cl = pl.from_arrow(c.execute(f"""WITH a AS (SELECT {', '.join('sl.' + x for x in k)}, sl.invyr, sl.l4, avg(y.tm) tm, avg(y.pp) pp, avg(y.vp) vp
                                     FROM sl JOIN y ON y.l4 = sl.l4 AND y.yr BETWEEN least(sl.ys, sl.ye) AND sl.ye GROUP BY ALL)
                                   SELECT {', '.join('a.' + x for x in k)}, a.invyr, a.tm - n.tm_n tm_anom, a.pp / n.pp_n - 1 pp_anom, a.vp - n.vp_n vp_anom,
                                          n.tm_n, n.pp_n, n.vp_n
                                   FROM a JOIN nrm n ON n.l4 = a.l4""").arrow())
    return df.join(cl, on=k + ['invyr'], how='left')

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
    ap.add_argument('--prism', help='PRISM L4 yearly DB: add the D51 climate drivers')
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    k = ['statecd', 'unitcd', 'countycd', 'plot', 'subp', 'tree']
    if a.from_seq:
        df = pl.read_parquet(a.from_seq)
    else:
        df = pl.concat([load54(d) for d in a.dbs], how='vertical_relaxed')
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
        df = df.join(deb, on=k, how='inner')
        df = add_design_levels(df, k)
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
    r = r.with_columns(pl.col('dia').ge(PSEUDO_IN).cum_sum().over(k, order_by='measdate').alias('_nvis'))
    elig = (r.group_by(k).agg(pl.col('tpa0').first(), pl.col('thr_in').first(), pl.col('thr_micro').first(), pl.col('dia0').first(), pl.col('n_real').first(),
                              (pl.col('_nvis') == 0).sum().alias('n_hid'), (pl.col('_nvis') > 0).sum().alias('n_vis'))
              .filter((pl.col('thr_in') == pl.col('thr_micro')) & (pl.col('dia0') < PSEUDO_IN) & (pl.col('n_real') >= 3)
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
