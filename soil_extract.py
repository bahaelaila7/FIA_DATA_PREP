"""soil_extract.py — SOLUS100 soil at FIA plots, for the joint age model (DECISIONS.md D70).

  python -I soil_extract.py --fiadb FIADB --solus SOLUS100_DIR --plots a.parquet b.parquet --out solus_plot.parquet [--workers 6]
  python -I soil_extract.py --selftest

One row per plot in the --plots files (columns statecd, unitcd, countycd, plot). Coordinates from data_plot_eco; FIA fuzzes them, so a value is approximate at plot level.
Properties are the depth-weighted mean over 0-30 cm (trapezoid over the 0, 5, 15, 30 cm nodes); a plot missing any node is NaN, never a partial mean.
Rasters are stored scaled: soc x 1000, pH x 100, bulk density x 100, CEC x 10 (SOLUS100/urls.py). Depths are in cm."""
import os, sys, argparse, tempfile
from multiprocessing import Pool
import numpy as np, polars as pl

NODES = (0, 5, 15, 30)
W = np.array([2.5, 7.5, 12.5, 7.5]) / 30.0            # trapezoid weights over 0-30 cm
PROPS = {'clay': ('claytotal', 1), 'sand': ('sandtotal', 1), 'silt': ('silttotal', 1), 'soc': ('soc', 1000), 'bd': ('dbovendry', 100), 'ph': ('ph1to1h2o', 100), 'cec': ('cec7', 10), 'rock_frag': ('fragvol', 1)}
DEPTHS = {'depth_restriction': 'resdept_all_cm_p.tif', 'depth_bedrock': 'anylithicdpt_cm_p.tif'}


def sample(path, lon, lat):
    """values at lon / lat, NaN at nodata or outside the raster; points are read in block order, so a raster is touched once per block"""
    import rasterio
    from rasterio.warp import transform
    with rasterio.open(path) as d:
        xs, ys = transform('EPSG:4326', d.crs, lon.tolist(), lat.tolist())
        rows, cols = rasterio.transform.rowcol(d.transform, xs, ys)
        rows, cols = np.asarray(rows), np.asarray(cols)
        bh, bw = d.block_shapes[0]
        ok = (rows >= 0) & (rows < d.height) & (cols >= 0) & (cols < d.width)
        out = np.full(len(lon), np.nan); idx = np.nonzero(ok)[0]
        key = (rows[idx] // bh) * d.width + cols[idx] // bw
        order = idx[np.argsort(key, kind='stable')]; key = np.sort(key, kind='stable')
        cuts = np.nonzero(np.diff(key))[0] + 1
        for grp in np.split(order, cuts):
            r0, c0 = rows[grp[0]] // bh * bh, cols[grp[0]] // bw * bw
            win = rasterio.windows.Window(c0, r0, min(bw, d.width - c0), min(bh, d.height - r0))
            a = d.read(1, window=win).astype(float)
            v = a[rows[grp] - r0, cols[grp] - c0]
            if d.nodata is not None: v[v == d.nodata] = np.nan
            out[grp] = v
    return out


def _job(a):
    return a[0], sample(a[1], a[2], a[3])


def extract(solus, lon, lat, workers):
    jobs = [(f'{k}_{d}', f'{solus}/{v}_{d}_cm_p.tif', lon, lat) for k, (v, _) in PROPS.items() for d in NODES] + [(k, f'{solus}/{f}', lon, lat) for k, f in DEPTHS.items()]
    with Pool(workers) as p:
        res = {}
        for i, (k, v) in enumerate(p.imap_unordered(_job, jobs), 1):
            res[k] = v; print(f'  {i}/{len(jobs)} {k}: {np.isfinite(v).mean():.4f} valid', flush=True)
    out = {k: sum(res[f'{k}_{d}'] * w for d, w in zip(NODES, W)) / s for k, (_, s) in PROPS.items()}
    out.update({k: res[k] for k in DEPTHS})
    return out


def selftest():
    import rasterio
    from rasterio.transform import from_origin
    with tempfile.TemporaryDirectory() as t:
        tf = from_origin(-100.0, 40.0, 0.01, 0.01)
        for k, (v, _) in PROPS.items():
            for j, d in enumerate(NODES):
                with rasterio.open(f'{t}/{v}_{d}_cm_p.tif', 'w', driver='GTiff', height=8, width=8, count=1, dtype='uint16', crs='EPSG:4326', transform=tf, nodata=65535) as w:
                    a = np.full((8, 8), 100 * (j + 1), np.uint16); a[0, 0] = 65535; a[7, 7] = 1000 * (j + 1); w.write(a, 1)
        for f in DEPTHS.values():
            with rasterio.open(f'{t}/{f}', 'w', driver='GTiff', height=8, width=8, count=1, dtype='uint8', crs='EPSG:4326', transform=tf, nodata=255) as w: w.write(np.full((8, 8), 201, np.uint8), 1)
        lon = np.array([-99.975, -99.995, -99.925, -50.0]); lat = np.array([39.975, 39.995, 39.925, 10.0])
        o = extract(t, lon, lat, 2)
        exp = float(sum(100 * (j + 1) * W[j] for j in range(4)))
        assert abs(o['clay'][0] - exp) < 1e-9, o['clay'][0]                 # trapezoid mean
        assert abs(o['soc'][0] - exp / 1000) < 1e-9                         # scale
        assert np.isnan(o['clay'][1]), 'nodata stays NaN'
        assert abs(o['clay'][2] - 10 * exp) < 1e-9                          # right cell, not a neighbour
        assert np.isnan(o['clay'][3]), 'outside the raster stays NaN'
        assert o['depth_bedrock'][0] == 201
    print('selftest: 5 checks PASS')


if __name__ == '__main__':
    ap = argparse.ArgumentParser(); ap.add_argument('--fiadb'); ap.add_argument('--solus'); ap.add_argument('--plots', nargs='*'); ap.add_argument('--out'); ap.add_argument('--workers', type=int, default=6); ap.add_argument('--selftest', action='store_true')
    a = ap.parse_args()
    if a.selftest:
        selftest(); raise SystemExit
    import duckdb
    k = ['statecd', 'unitcd', 'countycd', 'plot']
    p = pl.concat([pl.read_parquet(f, columns=k).unique().with_columns([pl.col(x).cast(pl.Int64) for x in k]) for f in a.plots]).unique()
    c = duckdb.connect(); c.execute(f"ATTACH '{a.fiadb}' AS f (READ_ONLY)")
    eco = c.execute("SELECT statecd, unitcd, countycd, plot, TRY_CAST(lon AS DOUBLE) lon, TRY_CAST(lat AS DOUBLE) lat FROM f.main.data_plot_eco WHERE TRY_CAST(lon AS DOUBLE) IS NOT NULL AND TRY_CAST(lat AS DOUBLE) IS NOT NULL").pl().with_columns([pl.col(x).cast(pl.Int64) for x in k])
    d = p.join(eco, on=k, how='inner').sort(k); print(f'{p.height:,} plots, {d.height:,} with coordinates', flush=True)
    r = extract(a.solus, d['lon'].to_numpy(), d['lat'].to_numpy(), a.workers)
    o = d.select(k).with_columns([pl.Series(n, v).fill_nan(None) for n, v in r.items()]); o.write_parquet(a.out)
    print(o.select([pl.col(n).is_not_null().mean().round(4).alias(n) for n in r]).to_dicts()[0]); print(o.select([pl.col(n).median().round(3).alias(n) for n in r]).to_dicts()[0])
