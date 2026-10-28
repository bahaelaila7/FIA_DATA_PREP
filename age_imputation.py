"""
age_imputation.py — step 2 of the curation chain (run_curation_v2.sh).

1. TreeAge -> field-age mapping (calibrate_estage, D2 / D5 / D40): isotonic in log1p per species
   group, fitted and selected on PLANTED calibration rows only (it is applied to planted rows
   only; natural rows keep raw TreeAge). Each curve is applied only where it beats raw TreeAge
   out of fold on the planted rows reaching it (D46). With fewer than CAL_MIN_N planted calibration rows
   nothing is mapped and planted estage ages carry age_domain = 'unmapped_estage'.
2. Age model (build_training_set / fit_age_model): a BAG of BAG_SEEDS HistGradientBoosting
   regressors (mean of the members' log1p predictions) on
       log1p(age) ~ DIA, HT, CR, CCLCD, cond_alstkcd, cond_fldszcd, cond_stdszcd,
                    age-stratum indicator (planted = 1, from age_stratum, D40 / M-11)
                    | categorical: major_spgrpcd, epa_l3
   Targets: planted = field age (TOTAGE / eligible STDAGE), else mapped TreeAge; natural =
   TOTAGE, else raw TreeAge (>= 5 in, Tree_Age > 1). Robust outlier rule per source (p99.5 or
   3x the DIA-class median), computed on each fit's own training rows. Training rows are sorted
   by CN so the fit does not depend on the upstream row order.
3. Recalibration: isotonic map out-of-fold prediction -> target per stratum x DIA-class cell,
   applied only in cells where it lowers the nested out-of-fold MAE (age_recal_cells).
4. Apply (apply_age_model): one prediction per live record with DIA (training rows use their
   cross-fitted prediction, D17), recalibrated, anchored per tree with _select_best_anchor
   (median anchor, age floored to 1 yr at the first measurement) and propagated. Final
   priority: TOTAGE > estage > model > STDAGE. age_domain flags the model ages.

TOTAGE / STDAGE are kept in age_from_totage / age_from_stdage, but estimated_age / age_source
are REWRITTEN with the priority above (a model age outranks STDAGE). Rebuilds
curated_snapshots. Writes estage_calibration, age_model_cv and age_recal_cells.

Usage (resumable; see __main__):
    python -I age_imputation.py --out-db CURATED.duckdb --fiadb FIADB.duckdb
"""

import os as _os, sys as _sys  # curation-fix: sibling modules resolve under `python -I`
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
import os
import sys

import duckdb
import numpy as np
import polars as pl
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.preprocessing import OrdinalEncoder
from threadpoolctl import threadpool_limits

# curation-fix 2026-10-26: was `from fia_curation_v2 import ...` — v2's anchor selection is
# the old youngest-candidate rule; v3 carries the median-anchor fix.
from fia_curation_v3 import (
    ESTAGE_MIN_AGE,
    ESTAGE_MIN_DIA,
    FIX_COLUMNS,
    SUBPLOT_ID,
    anchor_estage,
    flag_age_discrepancies,
    _compute_tree_bounds,
    _select_best_anchor,
    _propagate_from_anchor,
    build_snapshots,
    build_subplot_year_scaffold,
)

TREE_ID = ["STATECD", "UNITCD", "COUNTYCD", "PLOT", "SUBP", "TREE"]

# r3: condition-level stand structure added (field-recorded / computed stand-size class and
# stand origin) so the model can separate understory saplings of uneven-aged natural stands
# from the saplings of young even-aged (sapling-class) stands.
# D40 (M-11): the stand-origin feature follows the tree's age_stratum (the scale of its target),
# not the record's cond_stdorgcd: _STRATUM_FEAT = 1.0 planted / 0.0 natural (appended in _build_X).
_NUM_COLS = ["DIA", "HT", "CR", "CCLCD", "cond_alstkcd", "cond_fldszcd", "cond_stdszcd"]
_STRATUM_FEAT = "age_stratum_planted"
_CAT_COLS = ["major_spgrpcd", "epa_l3"]
_CAT_IDX = list(range(len(_NUM_COLS) + 1, len(_NUM_COLS) + 1 + len(_CAT_COLS)))   # D40 (M-8c)
# OpenMP threads of the HGBT fits (threadpoolctl); the result does not depend on it (verified
# EXCEPT-identical at 1 vs 3 threads, D40)
N_JOBS = int(os.environ.get("FIA_N_JOBS", os.environ.get("OMP_NUM_THREADS", "3")))


# =============================================================================
# Step 1: attach modeling context
# =============================================================================

def attach_modeling_context(con, df: pl.DataFrame) -> pl.DataFrame:
    """
    Add epa_l3 and major_spgrpcd to df.

    epa_l3        joined from data_plot_eco on (STATECD, UNITCD, COUNTYCD, PLOT)
    major_spgrpcd joined from REF_SPECIES on SPCD
    """
    eco = con.execute("""
        SELECT statecd AS STATECD, unitcd AS UNITCD,
               countycd AS COUNTYCD, plot AS PLOT, epa_l3
        FROM data_plot_eco
    """).pl()

    # r3 M5: the major species group of the RESOLVED species (constant per tree), not of the
    # raw per-visit SPCD
    spcol = "spcd_resolved" if "spcd_resolved" in df.columns else "SPCD"
    spgrp = con.execute("""
        SELECT SPCD, MAJOR_SPGRPCD AS major_spgrpcd
        FROM REF_SPECIES
    """).pl().with_columns(
        pl.col("SPCD").cast(df.schema[spcol])
    ).rename({"SPCD": spcol})

    plot_key = ["STATECD", "UNITCD", "COUNTYCD", "PLOT"]
    df = (
        df.join(eco,   on=plot_key, how="left")
          .join(spgrp, on=spcol,    how="left")
    )

    n = df.height
    n_eco  = df["epa_l3"].is_not_null().sum()
    n_grp  = df["major_spgrpcd"].is_not_null().sum()
    print(f"[step1] context attached — "
          f"epa_l3: {n_eco:,}/{n:,}  |  major_spgrpcd: {n_grp:,}/{n:,}")
    return df


# =============================================================================
# Step 1b (I2): map the external TreeAge (estage) onto the FIELD-age scale
# =============================================================================
# The truth is the FIA field age: TOTAGE, or STDAGE where the tree qualifies (planted /
# post-reset stands, FLDSZCD gate) — i.e. age_from_totage, else age_from_stdage. The external
# Tree_Age (Zenodo 14775738) runs older in planted stands, so every PLANTED estage anchor
# candidate uses field_age = f_g(Tree_Age), monotone, fitted in log1p space per species group g;
# NATURAL candidates keep the raw Tree_Age (D5).
#   * calibration (D40): LIVE records of PLANTED trees (age_stratum) with DIA >= 5 in and
#     Tree_Age > 1 that have a field age at the same measurement. The mapping is applied to
#     planted rows only, so it is fitted and selected on planted rows only; natural rows never
#     enter it (before D40 natural post-reset rows dominated the pooled curves applied to
#     planted rows, KY planted out-of-fold bias +5.5 yr);
#   * groups: spgrpcd_resolved -> major_spgrpcd -> global (all planted rows), falling back
#     whenever a group has fewer than CAL_MIN_N planted calibration records;
#   * f = isotonic regression on log1p (sklearn IsotonicRegression) — monotone; inside the
#     fitted range it interpolates its knots, outside it is clamped to the end knot and
#     extended LINEARLY in log1p with the group's OLS log-log slope (clipped to
#     [CAL_SLOPE_MIN, CAL_SLOPE_MAX]) — no wild extrapolation; a per-curve Duan smearing
#     factor on (1 + age) removes the log back-transform bias (still monotone);
#   * fitted once over the whole DB; plot-grouped 5-fold CV reports out-of-fold MAE / bias of
#     mapped vs raw Tree_Age against the field age (planted rows);
#   * D46 acceptance: a curve is applied only if its out-of-fold MAE on the planted
#     calibration rows that reach it is strictly below raw TreeAge's; otherwise those rows fall
#     through to the next level (same test), and rows no accepted curve covers keep the raw
#     TreeAge (identity). Decisions per curve in estage_calibration (applied, n, MAE, bias);
#   * fewer than CAL_MIN_N planted calibration rows in the DB: nothing is mapped (identity),
#     and planted estage ages are flagged age_domain = 'unmapped_estage' (D40, M-6).
CAL_MIN_N = 200
N_FOLDS = 5
# r5 M-5: the age-scale stratum is decided per TREE (fia_curation_v3.add_age_stratum: STDORGCD at
# its first live record), not per record; cond_stdorgcd stays a model FEATURE.
PLANTED = (pl.col("age_stratum") == "planted").fill_null(False)
CV_REPORT = None
RECAL_REPORT = None
UNMAPPED_ESTAGE = "unmapped_estage"


def plot_fold(plot_keys) -> np.ndarray:
    """Deterministic plot -> fold assignment (crc32 of 'STATECD_UNITCD_COUNTYCD_PLOT' mod
    N_FOLDS), shared by the estage-mapping CV and the age-model CV so a plot is out-of-fold
    in the same fold for both (no leakage through the mapping)."""
    import zlib
    return np.array([zlib.crc32(str(k).encode()) % N_FOLDS for k in plot_keys], dtype=np.int64)
CAL_SLOPE_MIN, CAL_SLOPE_MAX = 0.25, 1.5
CAL_COLS = ["CN", "STATECD", "UNITCD", "COUNTYCD", "PLOT", "SPCD", "spcd_resolved", "spgrpcd_resolved",
            "cond_stdorgcd", "STATUSCD", "DIA", "estage_raw", "age_from_totage", "age_from_stdage", "age_stratum"]


def _calibration_set(con) -> pl.DataFrame:
    """Planted calibration rows (D40), sorted by CN (row order never depends on the DB)."""
    cols = ", ".join(CAL_COLS)
    df = con.execute(f"""
        SELECT {cols} FROM curated_trees
        WHERE STATUSCD = 1 AND DIA >= {ESTAGE_MIN_DIA} AND estage_raw > {ESTAGE_MIN_AGE}
          AND COALESCE(age_from_totage, age_from_stdage) IS NOT NULL
          AND age_stratum = 'planted'""").pl()
    spgrp = con.execute("SELECT SPCD AS spcd_resolved, MAJOR_SPGRPCD AS major_spgrpcd FROM REF_SPECIES").pl()
    df = (df.join(spgrp.with_columns(pl.col("spcd_resolved").cast(df.schema["spcd_resolved"])),
                  on="spcd_resolved", how="left")
            .with_columns([
                pl.coalesce(["age_from_totage", "age_from_stdage"]).alias("field_age"),
                pl.concat_str([pl.col(c).cast(pl.Utf8) for c in ("STATECD", "UNITCD", "COUNTYCD", "PLOT")],
                              separator="_").alias("_plot"),
            ])
            .filter(pl.col("field_age") > 0)
            .sort(pl.col("CN").cast(pl.Utf8)))
    return df.with_columns(pl.Series("_fold", plot_fold(df["_plot"].to_list())))


def _fit_curve(x_raw: np.ndarray, y_field: np.ndarray) -> dict:
    from sklearn.isotonic import IsotonicRegression
    lx, ly = np.log1p(x_raw), np.log1p(y_field)
    iso = IsotonicRegression(increasing=True, out_of_bounds="clip").fit(lx, ly)
    kx, ky = np.asarray(iso.X_thresholds_), np.asarray(iso.y_thresholds_)
    slope = float(np.polyfit(lx, ly, 1)[0]) if np.ptp(lx) > 0 else 1.0
    c = {"kx": kx, "ky": ky, "slope": float(np.clip(slope, CAL_SLOPE_MIN, CAL_SLOPE_MAX)),
         "n": int(lx.size), "smear": 1.0}
    # Duan smearing: back-transforming a log1p fit is biased low by Jensen (-2.9 yr mean bias
    # in AL CV); a constant multiplicative factor on (1 + age) removes it and keeps f monotone
    c["smear"] = float(np.mean(np.exp(ly - _log_curve(c, lx))))
    return c


def _log_curve(c: dict, lx: np.ndarray) -> np.ndarray:
    ly = np.interp(lx, c["kx"], c["ky"])                       # clamped to the end knots
    lo, hi = c["kx"][0], c["kx"][-1]
    ly = np.where(lx < lo, c["ky"][0] + c["slope"] * (lx - lo), ly)
    return np.where(lx > hi, c["ky"][-1] + c["slope"] * (lx - hi), ly)


def _apply_curve(c: dict, x_raw: np.ndarray) -> np.ndarray:
    return (np.exp(_log_curve(c, np.log1p(x_raw))) * c["smear"] - 1.0).clip(1.0, None)


def _group_keys(spgrp, major):
    """Candidate curve keys in fallback order for one record (all curves are planted curves)."""
    return [("spgrpcd", spgrp), ("major_spgrpcd", major), ("global", None)]


def _fit_curves(cal: pl.DataFrame) -> dict:
    """{(level, value): curve} for every group of `cal` with >= CAL_MIN_N records, plus the
    global curve (no acceptance test)."""
    models = {}
    x_all, y_all = cal["estage_raw"].to_numpy().astype(float), cal["field_age"].to_numpy().astype(float)
    models[("global", None)] = _fit_curve(x_all, y_all)
    for level, col in (("spgrpcd", "spgrpcd_resolved"), ("major_spgrpcd", "major_spgrpcd")):
        for (val,), g in cal.group_by([col], maintain_order=True):
            if val is None or g.height < CAL_MIN_N:
                continue
            models[(level, val)] = _fit_curve(g["estage_raw"].to_numpy().astype(float),
                                              g["field_age"].to_numpy().astype(float))
    return models


_LEVEL_COL = {"spgrpcd": "spgrpcd_resolved", "major_spgrpcd": "major_spgrpcd"}


def _fit_mapping_tested(cal: pl.DataFrame) -> tuple[dict, list, dict]:
    """D46: fit the curves on `cal`, then accept each one only if its out-of-fold MAE on the
    planted calibration rows that REACH it is strictly below raw TreeAge's on the same rows.
    Levels are decided in order spgrpcd -> major_spgrpcd -> global: a row reaches a curve when
    no curve of an earlier level was accepted for it; rows of a rejected curve fall through to
    the next level, which takes the same test; rows no accepted curve covers keep the identity.
    The out-of-fold predictions come from the same curve (same level and group) refitted
    without each of the plot folds present in `cal` (the shared crc32 folds), so the decision
    uses only `cal` — inside the CV it is therefore nested. Nothing is tuned: a curve is either
    used as fitted or not at all. Returns (accepted curves, per-curve test records)."""
    curves = _fit_curves(cal)
    y = cal["field_age"].to_numpy().astype(float)
    raw = cal["estage_raw"].to_numpy().astype(float)
    folds = cal["_fold"].to_numpy()
    member = {}
    for k in curves:
        member[k] = (np.ones(cal.height, dtype=bool) if k[0] == "global"
                     else (cal[_LEVEL_COL[k[0]]] == k[1]).fill_null(False).to_numpy())
    oof = {k: np.full(cal.height, np.nan) for k in curves}
    for f in sorted(set(folds.tolist())):
        te = folds == f
        if (~te).sum() < CAL_MIN_N:
            continue
        cf = _fit_curves(cal.filter(pl.Series(~te)))
        for k in curves:
            m = te & member[k]
            if k in cf and m.any():
                oof[k][m] = _apply_curve(cf[k], raw[m])
    accepted, tests = {}, []
    reach = np.ones(cal.height, dtype=bool)
    for level in ("spgrpcd", "major_spgrpcd", "global"):
        covered = np.zeros(cal.height, dtype=bool)
        for k in [k for k in curves if k[0] == level]:
            rows = reach & member[k]
            t = rows & np.isfinite(oof[k])
            mae_m = float(np.mean(np.abs(oof[k][t] - y[t]))) if t.any() else None
            mae_r = float(np.mean(np.abs(raw[t] - y[t]))) if t.any() else None
            ok = mae_m is not None and mae_m < mae_r
            tests.append({"key": k, "n_reach": int(rows.sum()), "n_test": int(t.sum()),
                          "oof_mae_mapped": mae_m, "mae_raw": mae_r,
                          "oof_bias_mapped": float(np.mean(oof[k][t] - y[t])) if t.any() else None,
                          "bias_raw": float(np.mean(raw[t] - y[t])) if t.any() else None, "applied": ok})
            if ok:
                accepted[k] = curves[k]
                covered |= rows
        reach &= ~covered
    return accepted, tests, curves


def fit_estage_mapping(cal: pl.DataFrame) -> dict | None:
    """{(level, value): curve} of the ACCEPTED curves (D46) fitted on the (planted) calibration
    rows `cal`; None when the whole set has fewer than CAL_MIN_N records. Records whose groups
    have no accepted curve keep the raw TreeAge (map_estage's identity)."""
    if cal.height < CAL_MIN_N:
        return None
    return _fit_mapping_tested(cal)[0]


def map_estage(models: dict, raw, spgrp, major) -> tuple[np.ndarray, list]:
    """Mapped field-scale age + the curve key used, record by record (vectorised per key).
    `models` holds the accepted curves only: a record takes the first of its spgrpcd ->
    major_spgrpcd -> global curves that exists, else the IDENTITY (raw TreeAge, key None)."""
    raw = np.asarray(raw, dtype=float)
    out = raw.copy()
    used = [None] * raw.size
    keys = {}
    for i, (sg, mj) in enumerate(zip(spgrp, major)):
        if not np.isfinite(raw[i]):
            continue
        for k in _group_keys(sg, mj):
            if k in models:
                keys.setdefault(k, []).append(i)
                used[i] = k
                break
    for k, idx in keys.items():
        idx = np.asarray(idx)
        out[idx] = _apply_curve(models[k], raw[idx])
    return out, used


def _map_frame(models: dict, sub: pl.DataFrame) -> tuple[np.ndarray, list]:
    return map_estage(models, sub["estage_raw"].to_numpy(), sub["spgrpcd_resolved"].to_list(),
                      sub["major_spgrpcd"].to_list())


def mapping_without(cal: pl.DataFrame, folds_out) -> dict | None:
    """The mapping refitted without the calibration plots of the folds in `folds_out` (nested
    CV, D40 M-8a: an inner fold's mapping excludes the inner fold's calibration plots too)."""
    return fit_estage_mapping(cal.filter(~pl.col("_fold").is_in(list(folds_out))))


def cv_estage_mapping(cal: pl.DataFrame) -> dict:
    """Plot-grouped K-fold CV on the planted calibration rows: out-of-fold MAE / bias (yr) of
    mapped and raw vs field age."""
    y = cal["field_age"].to_numpy().astype(float)
    raw = cal["estage_raw"].to_numpy().astype(float)
    pred = np.full(y.shape, np.nan)
    folds = cal["_fold"].to_numpy()
    for k in range(N_FOLDS):
        te = np.where(folds == k)[0]
        m = mapping_without(cal, [k])
        if m is None or te.size == 0:
            continue
        pred[te], _ = _map_frame(m, cal[te])
    return {"n": int(y.size), "mae_mapped": float(np.nanmean(np.abs(pred - y))),
            "bias_mapped": float(np.nanmean(pred - y)), "mae_raw": float(np.mean(np.abs(raw - y))),
            "bias_raw": float(np.mean(raw - y)), "pred": pred}


def calibrate_estage(con):
    """Fit the TreeAge -> field-age mapping on the PLANTED calibration rows of the whole DB,
    report CV, write the estage_calibration table; returns {"models", "cal"}, or None when there
    are fewer than CAL_MIN_N planted calibration rows (then nothing is mapped and planted estage
    ages are flagged UNMAPPED_ESTAGE)."""
    cal = _calibration_set(con)
    print(f"[estage-cal] calibration set (PLANTED trees only, D40): {cal.height:,} live >= {ESTAGE_MIN_DIA:g} in "
          f"records with Tree_Age > {ESTAGE_MIN_AGE} and a field age (TOTAGE "
          f"{cal['age_from_totage'].is_not_null().sum():,} / STDAGE {cal.filter(pl.col('age_from_totage').is_null()).height:,})")
    rule = pl.DataFrame({"level": ["stratum_rule"], "group": [None], "stratum": ["natural"],
                         "applied_to": ["natural rows: identity (estage_mapped = raw Tree_Age)"]})
    if cal.height < CAL_MIN_N:
        print(f"[estage-cal] fewer than {CAL_MIN_N} planted calibration rows — NOTHING MAPPED: planted estage "
              f"stays on the raw TreeAge scale and is flagged age_domain = '{UNMAPPED_ESTAGE}'")
        tab = pl.concat([pl.DataFrame({"level": ["unmapped"], "group": [None], "stratum": ["planted"],
                                       "n_fit": [cal.height],
                                       "applied_to": [f"planted rows: identity, flagged {UNMAPPED_ESTAGE} "
                                                      f"(< {CAL_MIN_N} planted calibration rows)"]}),
                         rule], how="diagonal_relaxed")
        con.register("_cal", tab.to_arrow())
        con.execute("CREATE OR REPLACE TABLE estage_calibration AS SELECT * FROM _cal")
        con.unregister("_cal")
        con.commit()
        return None
    cv = cv_estage_mapping(cal)
    print(f"[estage-cal] plot-grouped {N_FOLDS}-fold CV vs field age, planted rows (n={cv['n']:,}):")
    print(f"             raw Tree_Age  MAE {cv['mae_raw']:6.2f} yr  bias {cv['bias_raw']:+6.2f} yr")
    print(f"             mapped        MAE {cv['mae_mapped']:6.2f} yr  bias {cv['bias_mapped']:+6.2f} yr  "
          f"(nested: each fold's mapping makes its own D46 acceptance on the other folds)")
    _dia = cal["DIA"].to_numpy().astype(float)
    _y, _raw = cal["field_age"].to_numpy().astype(float), cal["estage_raw"].to_numpy().astype(float)
    for lo, hi, lab in ((5, 7, "5-7"), (7, 9, "7-9"), (9, 1e9, "9+")):
        m = (_dia >= lo) & (_dia < hi) & np.isfinite(cv["pred"])
        if m.any():
            print(f"             DIA {lab:4s} n {m.sum():>7,}  mapped MAE {np.mean(np.abs(cv['pred'][m] - _y[m])):6.2f} "
                  f"bias {np.mean(cv['pred'][m] - _y[m]):+6.2f} | raw MAE {np.mean(np.abs(_raw[m] - _y[m])):6.2f} "
                  f"bias {np.mean(_raw[m] - _y[m]):+6.2f}")
    models, tests, curves = _fit_mapping_tested(cal)
    print("[estage-cal] D46 per-curve acceptance (out-of-fold MAE on the planted rows reaching the curve "
          "must be strictly below raw TreeAge's):")
    rows = []
    for t in tests:
        k, c = t["key"], curves[t["key"]]
        print(f"             {k[0]:13s} {'' if k[1] is None else int(k[1]):>4}  n {t['n_test']:>7,}  mapped "
              f"{t['oof_mae_mapped'] if t['oof_mae_mapped'] is not None else float('nan'):6.2f}  raw "
              f"{t['mae_raw'] if t['mae_raw'] is not None else float('nan'):6.2f}  -> "
              f"{'APPLIED' if t['applied'] else 'identity'}")
        rows.append({
            "level": k[0], "group": None if k[1] is None else str(int(k[1])), "stratum": "planted",
            "n_fit": c["n"], "n_reach_cal": t["n_reach"], "n_test": t["n_test"],
            "n_applied_cal": t["n_reach"] if t["applied"] else 0,
            "oof_mae_mapped": t["oof_mae_mapped"], "oof_bias_mapped": t["oof_bias_mapped"],
            "mae_raw": t["mae_raw"], "bias_raw": t["bias_raw"], "applied": t["applied"],
            "ext_slope": c["slope"], "smearing_factor": c["smear"],
            "knots_tree_age": [round(float(v), 3) for v in np.expm1(c["kx"])],
            "knots_field_age": [round(float(v), 3) for v in np.exp(c["ky"]) * c["smear"] - 1.0],
        })
    tab = pl.DataFrame(rows, schema_overrides={"knots_tree_age": pl.List(pl.Float64),
                                               "knots_field_age": pl.List(pl.Float64)})
    tab = tab.with_columns([
        pl.lit(cv["mae_mapped"]).alias("cv_mae_mapped_overall"),
        pl.lit(cv["bias_mapped"]).alias("cv_bias_mapped_overall"),
        pl.lit(cv["mae_raw"]).alias("mae_raw_overall"),
        pl.lit(cv["bias_raw"]).alias("bias_raw_overall"),
        pl.lit("planted rows only (fitted on planted rows only, D40); applied only where accepted (D46)")
        .alias("applied_to"),
        pl.lit("D46: applied iff out-of-fold MAE on the planted calibration rows reaching the curve "
               "(spgrpcd -> major_spgrpcd -> global) < raw TreeAge MAE; else identity / next level").alias("acceptance_rule"),
    ])
    # r4: the stratum rule — natural rows use the identity (raw Tree_Age)
    tab = pl.concat([tab, rule], how="diagonal_relaxed")
    con.register("_cal", tab.to_arrow())
    con.execute("CREATE OR REPLACE TABLE estage_calibration AS SELECT * FROM _cal")
    con.unregister("_cal")
    con.commit()
    print(f"[estage-cal] {len(curves)} curves ({len(models)} applied) → estage_calibration")
    return {"models": models, "cal": cal}


def remap_estage(df: pl.DataFrame, est, tree_bounds: pl.DataFrame) -> pl.DataFrame:
    """Re-anchor estage: estage_mapped = f_g(Tree_Age) (field scale; raw where the curve
    reaching the record was not accepted, D46) on valid PLANTED candidates and = raw Tree_Age on valid NATURAL candidates (DIA >= 5, Tree_Age > 1), then
    the usual median anchor + propagation, and the age-discrepancy flags. With no mapping
    (est None, D40) planted candidates keep the raw value too; it is still recomputed here so a
    resumed step 2 never keeps a previous run's mapped values (M-12). Column order is
    preserved."""
    order = df.columns
    valid = (pl.col("estage_raw").is_not_null() & (pl.col("DIA") >= ESTAGE_MIN_DIA).fill_null(False)
             & (pl.col("estage_raw") > ESTAGE_MIN_AGE))
    sub = df.select(pl.int_range(pl.len()).alias("_i"), valid.alias("_v"), "estage_raw",
                    "spgrpcd_resolved", "major_spgrpcd",
                    PLANTED.alias("_pl")).filter(pl.col("_v"))
    raw = sub["estage_raw"].to_numpy().astype(float)
    if est is not None:
        mapped, _ = _map_frame(est["models"], sub)
        # r4 (user decision: SPLIT BY STRATUM): the field-scale mapping applies to PLANTED rows
        # only; natural rows (age_stratum natural) keep the RAW Tree_Age (estage_mapped = raw)
        mapped = np.where(sub["_pl"].to_numpy(), mapped, raw)
    else:
        mapped = raw
    full = np.full(df.height, np.nan)
    full[sub["_i"].to_numpy()] = mapped
    df = df.with_columns(pl.Series("estage_mapped", full).fill_nan(None))
    cand = (df.filter(pl.col("estage_mapped").is_not_null())
              .select(TREE_ID + ["MEASDATE", "INVYR", pl.col("estage_mapped").alias("src_age")]))
    df = flag_age_discrepancies(anchor_estage(df, cand, tree_bounds))
    return df.select(order)


# =============================================================================
# Feature matrix
# =============================================================================

def _build_X(df: pl.DataFrame, enc: OrdinalEncoder,
             fit_enc: bool = False) -> np.ndarray:
    """
    Float32 feature matrix of shape (N, len(_NUM_COLS) + 1 + len(_CAT_COLS)):
      * _NUM_COLS (polars null -> NaN, which HistGBT handles natively);
      * the age-stratum indicator (1.0 planted / 0.0 natural, from age_stratum; D40 M-11);
      * _CAT_COLS cast to Utf8 and OrdinalEncoded (unknowns and nulls -> NaN); these columns
        (_CAT_IDX) are declared categorical to HistGBT (D40 M-8c), so the codes are labels,
        not an ordering.
    """
    num = (df.select([pl.col(c).cast(pl.Float64) for c in _NUM_COLS]
                     + [PLANTED.cast(pl.Float64).alias(_STRATUM_FEAT)])
             .to_numpy().astype(np.float32))

    cat_pd = df.select(
        [pl.col(c).cast(pl.Utf8) for c in _CAT_COLS]
    ).to_pandas()

    if fit_enc:
        cat_enc = enc.fit_transform(cat_pd)
    else:
        cat_enc = enc.transform(cat_pd)

    return np.hstack([num, cat_enc.astype(np.float32)])


# =============================================================================
# Step 2: fit model
# =============================================================================

OUTLIER_Q = 0.995       # r3 I3a: per-source outlier quantile
OUTLIER_MEDIAN_X = 3.0  # ... or this many times the source's DIA-class median
SUPPORT_MIN = 50       # age_domain: training rows needed in a DIA-class x stratum x size-class cell


def _support_cell(df: pl.DataFrame) -> pl.Expr:
    """DIA 2-in class x stratum (planted/natural) x stand-size class (FLDSZCD 1 vs other)."""
    return pl.concat_str([
        (pl.col("DIA") / 2).floor().clip(0, 12).cast(pl.Int32).cast(pl.Utf8),
        pl.when(PLANTED).then(pl.lit("P")).otherwise(pl.lit("N")),
        pl.when(pl.col("cond_fldszcd") == 1).then(pl.lit("s1")).otherwise(pl.lit("s2")),
    ], separator="|")


def _kept_mask(y: np.ndarray, src: np.ndarray, dcls: np.ndarray, rows: np.ndarray) -> np.ndarray:
    """Robust outlier rule (r3 I3a) computed on the training rows `rows` only (D40 M-8b): a row
    is kept when its target is <= the p99.5 of its source and <= OUTLIER_MEDIAN_X x the median
    of its source x DIA class, both over `rows`. Rows outside `rows`, or with a non-finite
    target, are not kept."""
    keep = np.zeros(y.shape, dtype=bool)
    idx = np.where(rows & np.isfinite(y))[0]
    if idx.size == 0:
        return keep
    f = pl.DataFrame({"y": y[idx], "s": src[idx], "d": dcls[idx]})
    k = f.select((pl.col("y") <= pl.col("y").quantile(OUTLIER_Q).over("s")) &
                 (pl.col("y") <= OUTLIER_MEDIAN_X * pl.col("y").median().over(["s", "d"]))).to_series()
    keep[idx] = k.fill_null(False).to_numpy()
    return keep


def build_training_set(con, est) -> pl.DataFrame:
    """r4 (user decision: SPLIT BY STRATUM) training targets, by the tree's age_stratum:
      planted: the FIELD age (TOTAGE / eligible STDAGE) where present ('field'), else the
               field-scale MAPPED Tree_Age of live >= 5 in records with Tree_Age > 1 ('mapped';
               none when there is no mapping);
      natural: TOTAGE where present ('field'), else the RAW Tree_Age of live >= 5 in records
               with Tree_Age > 1 ('treeage'). Natural STDAGE is not a target.
    SITETREE bored ages are never used for fitting (they are the independent check).
    Rows are sorted by CN (D40), so neither the fit nor the folds depend on the DB row order.
    `_kept` is the robust outlier rule on ALL rows (the production fit); the CV recomputes it
    on each fold's training rows. Outliers are scored but not trained on."""
    cols = ", ".join(TRAIN_COLS + ["estage_raw", "spgrpcd_resolved", "cond_fldszcd", "cond_stdszcd",
                                   "cond_stdorgcd", "age_from_totage", "age_stratum",
                                   "SUBP", "TREE", "MEASDATE", "CN"])
    df = attach_modeling_context(con, con.execute(f"SELECT {cols} FROM curated_trees").pl())
    df = df.filter((pl.col("STATUSCD") == 1) & pl.col("DIA").is_not_null() & pl.col("DIA").is_not_nan())
    df = df.with_columns(
        pl.concat_str([pl.col(c).cast(pl.Utf8) for c in ("STATECD", "UNITCD", "COUNTYCD", "PLOT")],
                      separator="_").alias("_plot"),
        PLANTED.alias("_planted"))
    has_tot = pl.col("age_from_totage").is_not_null()
    valid_te = (pl.col("DIA") >= ESTAGE_MIN_DIA) & (pl.col("estage_raw") > ESTAGE_MIN_AGE)
    field_ok = (pl.col("age_source").is_not_null() & pl.col("estimated_age").is_not_null()
                & pl.col("estimated_age").is_not_nan() & (pl.col("_planted") | has_tot))
    field = df.filter(field_ok).with_columns(pl.col("estimated_age").alias("target"), pl.lit("field").alias("source"))
    parts = [field]
    if est is not None:
        cand = df.filter(~field_ok & pl.col("_planted") & valid_te)
        m, _ = _map_frame(est["models"], cand)
        parts.append(cand.with_columns(pl.Series("target", m), pl.lit("mapped").alias("source")))
    nat = df.filter(~field_ok & ~pl.col("_planted") & valid_te)
    parts.append(nat.with_columns(pl.col("estage_raw").cast(pl.Float64).alias("target"),
                                  pl.lit("treeage").alias("source")))
    train = pl.concat(parts, how="diagonal_relaxed").sort(pl.col("CN").cast(pl.Utf8))
    train = train.with_columns((pl.col("DIA") / 2).floor().clip(0, 15).cast(pl.Int32).alias("_dcls"))
    train = train.with_columns(pl.Series("_kept", _kept_mask(
        train["target"].to_numpy().astype(float), train["source"].to_numpy(), train["_dcls"].to_numpy(),
        np.ones(train.height, dtype=bool))))
    for src in ("field", "mapped", "treeage"):
        sub = train.filter(pl.col("source") == src)
        if sub.height:
            print(f"[step2] training source {src:7s}: {sub.height:,} rows "
                  f"(planted {sub.filter(pl.col('_planted')).height:,}), "
                  f"{sub.filter(~pl.col('_kept')).height:,} outliers (> p{OUTLIER_Q*100:g} or "
                  f"> {OUTLIER_MEDIAN_X:g}x DIA-class median) not trained on")
    return train.with_columns(pl.Series("_fold", plot_fold(train["_plot"].to_list())))


# --- D40: the age model is a bag of BAG_SEEDS HistGBT members (mean of their log1p predictions).
# Each member s gets
#   * a Poisson(1) bootstrap weight per row and
#   * the early-stopping validation split (VAL_FRACTION of the rows; only when the fit has more
#     than ES_MIN_N rows, sklearn's own 'auto' threshold),
# both drawn from crc32(CN | salt | s): a property of the ROW, not of its position, so dropping,
# adding or reordering a training row changes no other row's weight or split (the positional
# sklearn split made a 1-row change move ages by up to 9.9 yr). random_state = s is explicit.
BAG_SEEDS = (42, 43, 44, 45, 46)
VAL_FRACTION = 0.1
ES_MIN_N = 10_000
HGB_PARAMS = dict(max_iter=300, max_depth=6, min_samples_leaf=30)
_POISSON1_CDF = np.cumsum([np.exp(-1.0) / np.prod(np.arange(1, k + 1)) for k in range(20)])


def _row_uniforms(cns, salt: str, seed: int) -> np.ndarray:
    """Uniform [0, 1) per row from crc32('CN|salt|seed') — reproducible, order-free."""
    import zlib
    return np.fromiter((zlib.crc32(f"{c}|{salt}|{seed}".encode()) for c in cns),
                       dtype=np.float64, count=len(cns)) / 2.0 ** 32


def row_draws(cns) -> dict:
    """{seed: (u_val, bootstrap weight)} for every row of the training set (aligned with X)."""
    return {s: (_row_uniforms(cns, "val", s),
                np.searchsorted(_POISSON1_CDF, _row_uniforms(cns, "boot", s), side="right").astype(np.float64))
            for s in BAG_SEEDS}


class BaggedHGB:
    """Mean of BAG_SEEDS HistGradientBoostingRegressor members in the fitted (log1p) space."""

    def __init__(self):
        self.members = []

    def fit(self, X: np.ndarray, y: np.ndarray, rows: np.ndarray, draws: dict) -> "BaggedHGB":
        idx = np.where(rows)[0]
        es = idx.size > ES_MIN_N
        self.members = []
        for s in BAG_SEEDS:
            u_val, w = draws[s]
            m = HistGradientBoostingRegressor(**HGB_PARAMS, categorical_features=_CAT_IDX,
                                              early_stopping=es, random_state=s)
            val = idx[u_val[idx] < VAL_FRACTION] if es else idx[:0]
            fit_i = np.setdiff1d(idx, val, assume_unique=True)
            fit_i = fit_i[w[fit_i] > 0]
            if es:
                m.fit(X[fit_i], y[fit_i], sample_weight=w[fit_i], X_val=X[val], y_val=y[val])
            else:
                m.fit(X[fit_i], y[fit_i], sample_weight=w[fit_i])
            self.members.append(m)
        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        return np.mean([m.predict(X) for m in self.members], axis=0)

    @property
    def n_iter_(self):
        return [m.n_iter_ for m in self.members]


def _remap_targets(train: pl.DataFrame, models) -> np.ndarray:
    """Targets with the 'mapped' rows re-mapped by `models` (a fold-specific mapping); NaN
    (= not trained on, not scored) when that fold has no mapping."""
    y = train["target"].to_numpy().astype(float).copy()
    mrows = np.where(train["source"].to_numpy() == "mapped")[0]
    if mrows.size:
        y[mrows] = _map_frame(models, train[mrows])[0] if models is not None else np.nan
    return y


RECAL_MIN_N = 50


def recal_cell(planted, dia) -> np.ndarray:
    """Recalibration cell: stratum (P/N) x DIA class (<5, 5-10, 10-15, 15+)."""
    d = np.asarray(dia, dtype=float)
    k = np.where(d < 5, "a", np.where(d < 10, "b", np.where(d < 15, "c", "d")))
    return np.char.add(np.where(np.asarray(planted, dtype=bool), "P", "N"), k)


_CELL_DIA = {"a": "<5", "b": "5-10", "c": "10-15", "d": "15+"}


def _recal_fit(pred, y, cells) -> dict:
    """r4: monotone (isotonic) map OUT-OF-FOLD prediction -> target per cell; it undoes the
    model's regression to the mean (old trees predicted young, young trees old)."""
    from sklearn.isotonic import IsotonicRegression
    maps = {}
    ok = np.isfinite(pred) & np.isfinite(y)
    for c in np.unique(cells[ok]):
        m = ok & (cells == c)
        if m.sum() >= RECAL_MIN_N:
            maps[c] = IsotonicRegression(increasing=True, out_of_bounds="clip").fit(pred[m], y[m])
    return maps


def _recal_apply(maps: dict, pred, cells) -> np.ndarray:
    out = np.asarray(pred, dtype=float).copy()
    for c, iso in maps.items():
        m = (cells == c) & np.isfinite(out)
        if m.any():
            out[m] = iso.predict(out[m])
    return out


def fit_age_model(train: pl.DataFrame, est=None) -> tuple:
    """
    Train the BaggedHGB age model on log1p(target) (build_training_set: field ages, planted
    mapped TreeAge, natural raw TreeAge), then a post-hoc monotone recalibration.

    CV: plot-grouped, the SAME crc32 fold assignment as the estage-mapping CV. For outer fold
    k: the mapping is refitted without fold-k calibration plots (mapped targets recomputed with
    it), the outlier rule is recomputed on the training folds, and the model is fitted on the
    other folds. The RECALIBRATION map for fold k is fitted on INNER out-of-fold predictions
    of the other folds only (a nested CV: for each inner fold j != k, a model fitted on folds
    not in {k, j} — with the mapping refitted without the calibration plots of k and j, and the
    outlier rule recomputed on those folds (D40 M-8a/b) — predicts fold j), then applied to
    fold k's predictions. So the scored numbers are out-of-fold for both the model and the
    recalibration.

    Acceptance (D40 M-7, pre-registered): the recalibration is used in a stratum x DIA-class
    cell only if its nested out-of-fold MAE in that cell is LOWER than the raw model's;
    otherwise the cell keeps the identity. Production: the model on all kept rows; the
    recalibration fitted on the outer OOF predictions, accepted cells only.
    Returns (model, enc, support, (recal_maps, oof_tab)).
    """
    enc = OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=np.nan,
                         encoded_missing_value=np.nan)
    X = _build_X(train, enc, fit_enc=True)
    draws = row_draws(train["CN"].cast(pl.Utf8).to_list())
    n = train.height
    kept = train["_kept"].to_numpy()
    folds = train["_fold"].to_numpy()
    src = train["source"].to_numpy()
    dcls = train["_dcls"].to_numpy()
    cells = recal_cell(train["_planted"].to_numpy(), train["DIA"].to_numpy())
    y_full = train["target"].to_numpy().astype(float)
    cal = est["cal"] if est is not None else None

    def targets(folds_out):
        return _remap_targets(train, mapping_without(cal, folds_out)) if cal is not None else y_full

    def fit_on(rows, y):
        return BaggedHGB().fit(X, np.log1p(y), rows, draws)

    raw_oof = np.full(n, np.nan)
    oof = np.full(n, np.nan)
    y_oof_target = np.full(n, np.nan)
    present = [k for k in range(N_FOLDS) if (folds == k).any()]
    for k in present:
        tr, te = folds != k, folds == k
        if not tr.any():
            continue
        y_k = targets([k])
        kept_k = _kept_mask(y_k, src, dcls, tr)
        m = fit_on(tr & kept_k, y_k)
        raw_oof[te] = np.expm1(m.predict(X[te]))          # scored on ALL held-out rows
        y_oof_target[te] = y_k[te]
        # nested: inner OOF predictions on the training folds -> recalibration map for fold k
        inner, inner_y = np.full(n, np.nan), np.full(n, np.nan)
        for j in present:
            if j == k:
                continue
            itr, ite = (folds != k) & (folds != j), folds == j
            if not itr.any():
                continue
            y_kj = targets([k, j])
            mj = fit_on(itr & _kept_mask(y_kj, src, dcls, itr), y_kj)
            inner[ite] = np.expm1(mj.predict(X[ite]))
            inner_y[ite] = y_kj[ite]
        maps_k = _recal_fit(inner[tr & kept_k], inner_y[tr & kept_k], cells[tr & kept_k])
        oof[te] = _recal_apply(maps_k, raw_oof[te], cells[te])
        print(f"         outer fold {k}: n_iter_ {m.n_iter_}", flush=True)

    # D40 (M-7): per-cell acceptance on the nested out-of-fold MAE
    recal_all = _recal_fit(raw_oof[kept], y_oof_target[kept], cells[kept])
    rows, accepted = [], set()
    for c in sorted(set(cells.tolist())):
        msk = (cells == c) & np.isfinite(raw_oof) & np.isfinite(oof) & np.isfinite(y_oof_target)
        e0, e1 = raw_oof[msk] - y_oof_target[msk], oof[msk] - y_oof_target[msk]
        mae0 = float(np.mean(np.abs(e0))) if msk.any() else None
        mae1 = float(np.mean(np.abs(e1))) if msk.any() else None
        ok = c in recal_all and mae0 is not None and mae1 < mae0
        if ok:
            accepted.add(c)
        rows.append({"cell": c, "stratum_pn": "planted" if c[0] == "P" else "natural", "dia_cls": _CELL_DIA[c[1]],
                     "n_scored": int(msk.sum()), "n_fit_production": int((kept & (cells == c)
                                                                         & np.isfinite(y_oof_target)).sum()),
                     "mae_raw": mae0, "mae_recal": mae1,
                     "bias_raw": float(np.mean(e0)) if msk.any() else None,
                     "bias_recal": float(np.mean(e1)) if msk.any() else None,
                     "applied": ok})
    recal = {c: v for c, v in recal_all.items() if c in accepted}
    global RECAL_REPORT
    RECAL_REPORT = pl.DataFrame(rows).with_columns(
        pl.lit("applied only if the nested out-of-fold MAE decreases (D40); >= "
               f"{RECAL_MIN_N} rows to fit").alias("rule"))
    final = np.where(np.isin(cells, list(accepted)), oof, raw_oof)

    print(f"[step2] plot-grouped {N_FOLDS}-fold CV (same folds as the estage mapping), out-of-fold, "
          f"raw model vs recalibrated (nested) vs final (recalibration only in accepted cells):")
    for s_ in ("field", "mapped", "treeage", "all"):
        msk = (src == s_) if s_ != "all" else np.ones_like(src, dtype=bool)
        if msk.any():
            e0, e1, e2 = (p[msk] - y_oof_target[msk] for p in (raw_oof, oof, final))
            print(f"         {s_:7s} n={msk.sum():>9,}  raw MAE {np.nanmean(np.abs(e0)):6.2f} bias {np.nanmean(e0):+6.2f}"
                  f"  | recal MAE {np.nanmean(np.abs(e1)):6.2f} bias {np.nanmean(e1):+6.2f}"
                  f"  | final MAE {np.nanmean(np.abs(e2)):6.2f} bias {np.nanmean(e2):+6.2f} yr")
    with pl.Config(tbl_rows=50, tbl_cols=20, tbl_width_chars=200):
        print(RECAL_REPORT.drop("rule"))
    dia = train["DIA"].to_numpy()
    planted = np.char.startswith(cells, "P")
    reps = []
    # NOTE: age_decile = deciles of the target pooled over all sources (one set of cut points)
    for lab, pred in (("raw", raw_oof), ("recalibrated", oof), ("final", final)):
        rep = pl.DataFrame({"source": src, "y": y_oof_target, "e": pred - y_oof_target, "dia": dia,
                            "stratum_pn": np.where(planted, "planted", "natural")}).filter(pl.col("e").is_not_nan())
        rep = rep.with_columns(
            pl.col("y").qcut(10, labels=[f"d{i:02d}" for i in range(1, 11)], allow_duplicates=True).cast(pl.Utf8).alias("age_decile"),
            pl.when(pl.col("dia") < 5).then(pl.lit("<5")).when(pl.col("dia") < 10).then(pl.lit("5-10"))
            .when(pl.col("dia") < 15).then(pl.lit("10-15")).otherwise(pl.lit("15+")).alias("dia_cls"))
        for by, col in (("age_decile", "age_decile"), ("dia_class", "dia_cls")):
            reps.append(rep.group_by(["source", "stratum_pn", col])
                        .agg(pl.len().alias("n"), pl.col("y").median().alias("median_target"),
                             pl.col("e").abs().mean().alias("mae"), pl.col("e").mean().alias("bias"))
                        .rename({col: "stratum"}).with_columns(pl.lit(by).alias("by"), pl.lit(lab).alias("model")))
    global CV_REPORT
    CV_REPORT = (pl.concat(reps).with_columns(pl.col("stratum").cast(pl.Utf8))
                 .sort(["model", "by", "source", "stratum_pn", "stratum"]))
    with pl.Config(tbl_rows=200):
        print(CV_REPORT.filter((pl.col("by") == "dia_class")))
    model = fit_on(kept, y_full)
    print(f"         production model: n_iter_ {model.n_iter_}")
    support = (train.filter(pl.col("_kept"))
               .with_columns(_support_cell(train.filter(pl.col("_kept"))).alias("_cell"))
               .group_by("_cell").len().rename({"len": "_n_train"}).sort("_cell"))
    # r5 M-6: rows the production model was TRAINED on get their cross-fitted (outer out-of-fold)
    # raw prediction instead of the in-sample one, so the recalibration map (fitted on OOF
    # predictions) is applied to predictions of the same kind
    ok = kept & np.isfinite(raw_oof)
    oof_tab = (train.filter(pl.Series(ok)).select(TREE_ID + ["MEASDATE"])
               .with_columns(pl.Series("_raw_oof", raw_oof[ok])))
    return model, enc, support, (recal, oof_tab)


# =============================================================================
# Step 3: apply + merge
# =============================================================================

# ACCEPT + FLAG (D6 / D8 / D9; r5 D14 + D-log): known-bias cells are flagged in age_domain (which
# then overrides the model-age values above). The bias depends on the FIT SCOPE (the age model
# and the TreeAge mapping are fitted once over all states in the DB), so each rule names the fit
# scopes it was measured on = the sorted, comma-joined STATECDs of the DB (FIT_SCOPE).
# Rule: (age_domain value, STATECD, planted?, age sources, minimum DIA, fit scopes, maximum DIA
# (exclusive; None = no upper bound)). planted = the tree's age_stratum (r5 M-5).
#   (known_bias_ky_natural_treeage: retired by D13 — natural (k) is INFO by wood group)
#   (known_bias_fl_planted_model_10plus: retired by D47 — under the D40 / D46 age model the pooled
#    FL 10-15 in cell passes (-2.9 yr); only 15+ in stays excepted)
# r8 D47.5, re-measured 2026-10-28 on the D40 / D46 model ((j) model minus mapped TreeAge; only
# trees without a TreeAge record take the model age, so the flags mark MODEL ages):
#   known_bias_fl_planted_model_15plus   FL planted, DIA >= 15 in: pooled 12,21,28 -4.3 yr,
#                                        single-state 12 -3.7 yr (regression to the mean)
#   known_bias_ky_planted_model_10to15   KY planted, 10 <= DIA < 15 in: single-state 21 -4.9 yr
#                                        (D46 raw-TreeAge residual), pooled +4.2 yr (sign flips
#                                        with the fit scope: not stable to the training set)
#   known_bias_tn_planted_model_15plus   TN planted, DIA >= 15 in: single-state 47 -3.1 yr
KNOWN_BIAS_FLAGS = [
    ("known_bias_fl_planted_model_15plus", 12, True, ("model",), 15.0, ("12", "12,21,28"), None),
    ("known_bias_ky_planted_model_10to15", 21, True, ("model",), 10.0, ("21", "12,21,28"), 15.0),
    ("known_bias_tn_planted_model_15plus", 47, True, ("model",), 15.0, ("47",), None),
]
FIT_SCOPE = None   # set in __main__ from the DB's states


def flag_known_bias(df: pl.DataFrame) -> pl.DataFrame:
    if "age_domain" not in df.columns:
        df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias("age_domain"))
    planted = PLANTED
    for val, st, pl_, srcs, min_dia, scopes, max_dia in KNOWN_BIAS_FLAGS:
        if FIT_SCOPE not in scopes:
            continue
        m = (pl.col("STATECD") == st) & (planted if pl_ else ~planted) & pl.col("age_source").is_in(list(srcs))
        if min_dia is not None:
            m = m & (pl.col("DIA") >= min_dia)
        if max_dia is not None:
            m = m & (pl.col("DIA") < max_dia)
        df = df.with_columns(pl.when(m.fill_null(False)).then(pl.lit(val)).otherwise(pl.col("age_domain"))
                             .alias("age_domain"))
    return df


def apply_age_model(df: pl.DataFrame,
                    model: "BaggedHGB",
                    enc: OrdinalEncoder,
                    tree_bounds: pl.DataFrame,
                    support: pl.DataFrame | None = None,
                    recal: tuple | dict | None = None,
                    estage_unmapped: bool = False) -> pl.DataFrame:
    """
    Predict a raw age for every alive row with DIA (training rows: their cross-fitted
    out-of-fold prediction, D17), recalibrate it in the accepted cells, then
    _select_best_anchor picks one anchor per tree (median of the candidates' implied ages at
    the first measurement, floored to 1 yr there) and the age is propagated linearly to all
    measurements.

    Adds age_from_model (Float64, null where no anchor). Rewrites estimated_age / age_source
    with the priority TOTAGE > estage > model > STDAGE, and sets age_domain (see below).
    estage_unmapped: no TreeAge mapping exists (D40): planted estage ages get
    'unmapped_estage'.
    """
    # Raw per-row prediction (alive + DIA only; others get NaN → excluded)
    alive_w_dia = df.filter(
        (pl.col("STATUSCD") == 1) & pl.col("DIA").is_not_null()
    )
    X_cand = _build_X(alive_w_dia, enc, fit_enc=False)
    raw    = np.expm1(model.predict(X_cand))
    oof_tab = None
    if isinstance(recal, tuple):
        recal, oof_tab = recal
    if oof_tab is not None and oof_tab.height:   # r5 M-6: cross-fitted predictions for training rows
        o = (alive_w_dia.select(TREE_ID + ["MEASDATE"]).with_row_index("_i")
             .join(oof_tab, on=TREE_ID + ["MEASDATE"], how="inner"))
        if o.height:
            raw[o["_i"].to_numpy()] = o["_raw_oof"].to_numpy()
        print(f"         model ages: {o.height:,} of {alive_w_dia.height:,} live rows use their cross-fitted "
              f"(out-of-fold) prediction (training rows), the rest the full-data model")
    if recal:   # r4: monotone post-hoc recalibration per stratum x DIA class
        raw = _recal_apply(recal, raw, recal_cell(alive_w_dia.select(PLANTED).to_series().to_numpy(),
                                                  alive_w_dia["DIA"].to_numpy()))
    raw    = np.asarray(raw).clip(1.0, 999.0)

    # Build candidates: TREE_ID + MEASDATE + src_age (raw prediction)
    candidates = alive_w_dia.select(TREE_ID + ["MEASDATE"]).with_columns(
        pl.Series("src_age", raw, dtype=pl.Float64)
    )

    # r2 M4: floor the model age at the tree's first measurement to 1 yr (as for TOTAGE),
    # so no negative / < 1 yr live ages remain from model anchors
    anchors = _select_best_anchor(candidates, tree_bounds, apply_min_shift=True)
    df      = _propagate_from_anchor(df, anchors, "age_from_model")

    n_maxviol = anchors.filter(pl.col("max_viol") > 0).height
    print(f"[step3] age_from_model: {anchors.height:,} trees anchored")
    print(f"         residual max-age violations: {n_maxviol:,}")

    # Final age_calc priority: TOTAGE > estage > model > STDAGE.
    # STDAGE is a stand-level even-aged age (only weakly correlated with individual
    # biomass — see corr/R² diagnostics), so it is the fallback of last resort,
    # below the per-tree sources. Built directly from the raw source columns (not
    # the v3-merged estimated_age, which pre-coalesces TOTAGE>STDAGE) so the order
    # is explicit. All sources are float — no quantization before this point.
    has_estage = "age_from_estage" in df.columns
    estage_col = pl.col("age_from_estage") if has_estage else pl.lit(None, dtype=pl.Float64)

    df = df.with_columns([
        pl.coalesce([
            pl.col("age_from_totage"),
            estage_col,
            pl.col("age_from_model"),
            pl.col("age_from_stdage"),
        ]).alias("estimated_age"),
        pl.when(pl.col("age_from_totage").is_not_null()).then(pl.lit("TOTAGE"))
        .when(estage_col.is_not_null()).then(pl.lit("estage"))
        .when(pl.col("age_from_model").is_not_null()).then(pl.lit("model"))
        .when(pl.col("age_from_stdage").is_not_null())
            .then(pl.lit("STDAGE_") + pl.col("stdage_context").fill_null("unknown"))
        .otherwise(pl.lit(None))
        .alias("age_source"),
    ])

    # age_domain (null for other sources, except the overrides below):
    #   model ages —
    #   out_of_support        natural tree (age_stratum) with DIA < 5 in (D40 M-9: natural
    #                         targets are TreeAge >= 5 in only, so natural saplings have no
    #                         same-stratum support), or planted with fewer than SUPPORT_MIN
    #                         training rows in its DIA-class x stratum x stand-size cell
    #   natural_treeage_scale other natural model ages: natural-stand ages are on the RAW
    #                         TreeAge scale (r4 user decision: split by stratum)
    #   in_support            otherwise
    #   estage ages —
    #   unmapped_estage       planted tree whose age is estage while no TreeAge mapping exists
    #                         (< CAL_MIN_N planted calibration rows, D40 M-6)
    #   then the KNOWN_BIAS_FLAGS overrides.
    if support is not None:
        df = (df.with_columns(_support_cell(df).alias("_cell"))
                .join(support, on="_cell", how="left")
                .with_columns(
                    pl.when(pl.col("age_source") == "model")
                    .then(pl.when(~PLANTED & (pl.col("DIA") < ESTAGE_MIN_DIA).fill_null(False))
                          .then(pl.lit("out_of_support"))
                          .when(~PLANTED)   # natural tree (age_stratum, r5 M-5)
                          .then(pl.lit("natural_treeage_scale"))
                          .when(pl.col("_n_train").fill_null(0) >= SUPPORT_MIN)
                          .then(pl.lit("in_support")).otherwise(pl.lit("out_of_support")))
                    .when(pl.lit(estage_unmapped) & (pl.col("age_source") == "estage") & PLANTED)
                    .then(pl.lit(UNMAPPED_ESTAGE))
                    .otherwise(pl.lit(None, dtype=pl.Utf8)).alias("age_domain"))
                .drop(["_cell", "_n_train"]))
    df = flag_known_bias(df)
    n_estage_filled = df.filter(pl.col("age_source") == "estage").height if has_estage else 0
    n_model_filled  = df.filter(pl.col("age_source") == "model").height
    n_still_na = df.filter(
        (pl.col("STATUSCD") == 1) & pl.col("estimated_age").is_null()
    ).height
    print(f"         estage filled:  {n_estage_filled:,} measurements")
    print(f"         model filled:   {n_model_filled:,} measurements  |  "
          f"alive still without age (no DIA): {n_still_na:,}")
    return df


# =============================================================================
# Standalone entry point
# =============================================================================

TRAIN_COLS = ["STATECD", "UNITCD", "COUNTYCD", "PLOT", "SPCD", "spcd_resolved", "STATUSCD", "age_source",
              "estimated_age", "DIA", "HT", "CR", "CCLCD", "cond_alstkcd"]


def _order_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Old curated_trees column order first, curation-fix columns last."""
    fix = [c for c in FIX_COLUMNS if c in df.columns]
    return df.select([c for c in df.columns if c not in fix] + fix)


if __name__ == "__main__":
    # curation-fix 2026-10-26: explicit --out-db/--fiadb (dbconn); the model is fitted ONCE
    # on the training columns of all states in the DB, then applied state by state (bounded
    # memory) — every age rule is tree-local, so this equals a single pass.
    import argparse
    import dbconn
    a = dbconn.add_db_args(argparse.ArgumentParser(description="age imputation")).parse_args()
    dbconn.check_args(a)
    con = dbconn.connect(a.out_db, a.fiadb)
    dbconn.require_tables(con, "curated_trees", "clean_subplot_years",
                          "base_subplot_cond", "longitudinal_subplots", "excluded_tree_ids")
    print("=" * 70)
    print("Age Imputation Pipeline")
    print(f"DB: {a.out_db}")
    print("=" * 70)

    # r2 M3: resumable. If step 2 already ran (age_from_model present), restore the v3 state
    # BEFORE fitting: drop the step-2 columns and rebuild v3's merged training target
    # (estimated_age = TOTAGE > STDAGE, age_source TOTAGE / STDAGE_<context>), which step 2
    # overwrote. Re-running step 2 then gives the same result as the first run: estage_mapped /
    # age_from_estage are always recomputed by remap_estage (also without a mapping, M-12), and
    # estage_calibration / age_model_cv / age_recal_cells are always rewritten.
    if "age_from_model" in dbconn.column_names(con, "curated_trees"):
        print("[resume] curated_trees already imputed — restoring the v3 age columns first")
        con.execute("""
            CREATE OR REPLACE TABLE curated_trees AS
            SELECT * EXCLUDE (epa_l3, major_spgrpcd, age_from_model)
                     REPLACE (COALESCE(age_from_totage, age_from_stdage) AS estimated_age,
                              CASE WHEN age_from_totage IS NOT NULL THEN 'TOTAGE'
                                   WHEN age_from_stdage IS NOT NULL
                                        THEN 'STDAGE_' || COALESCE(stdage_context, 'unknown')
                              END AS age_source)
            FROM curated_trees""")
        con.commit()

    # D40: the HGBT fits use N_JOBS OpenMP threads (the result does not depend on the count)
    _thread_limit = threadpool_limits(limits=N_JOBS)
    # I2 / r3: mapping first (TreeAge -> field scale), then the age model trained on field ages
    # + mapped ages, both once over the whole DB
    estage_models = calibrate_estage(con)
    train_df = build_training_set(con, estage_models)
    model, enc, support, recal = fit_age_model(train_df, estage_models)
    del train_df
    for _name, _tab in (("age_model_cv", CV_REPORT), ("age_recal_cells", RECAL_REPORT)):
        if _tab is not None:
            con.register("_cvr", _tab.to_arrow())
            con.execute(f"CREATE OR REPLACE TABLE {_name} AS SELECT * FROM _cvr")
            con.unregister("_cvr")
    scaffold_all = build_subplot_year_scaffold(con)
    states = [r[0] for r in con.execute(
        "SELECT DISTINCT STATECD FROM curated_trees ORDER BY 1").fetchall()]
    FIT_SCOPE = ",".join(str(int(x)) for x in states)
    # r6 S2: a county-subset run has its own scope (never inherits whole-state KNOWN entries / flags)
    if dbconn.table_exists(con, "run_scope"):
        _cty = con.execute("SELECT counties FROM run_scope").fetchone()[0]
        if _cty:
            FIT_SCOPE += f"|counties={_cty}"
    print(f"[step2] fit scope (states fitted together): {FIT_SCOPE}")
    parts_t, parts_s = [], []
    for st in states:
        print(f"\n----- STATECD {st} -----")
        df = con.execute(f"SELECT * FROM curated_trees WHERE STATECD = {int(st)}").pl()
        all_subplot_years = con.execute(f"""
            SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, MEASDATE
            FROM clean_subplot_years WHERE STATECD = {int(st)}
        """).pl()
        df = attach_modeling_context(con, df)
        tree_bounds = _compute_tree_bounds(df, all_subplot_years)
        df = remap_estage(df, estage_models, tree_bounds)
        df = _order_columns(apply_age_model(df, model, enc, tree_bounds, support, recal,
                                            estage_unmapped=estage_models is None))

        scaffold = scaffold_all.filter(pl.col("STATECD") == st)
        ever_live = (df.filter((pl.col("STATUSCD") == 1) & pl.col("DRYBIO_AG").is_not_null())
                     .select(SUBPLOT_ID).unique())
        snapshots = build_snapshots(df, scaffold.join(ever_live, on=SUBPLOT_ID, how="semi"))
        for name, frame, parts in (("t", df, parts_t), ("s", snapshots, parts_s)):
            tab = f"_ai_{name}_{st}"
            con.register("_part", frame)
            con.execute(f"CREATE OR REPLACE TABLE {tab} AS SELECT * FROM _part")
            con.unregister("_part")
            parts.append(tab)
        con.commit()
        del df, snapshots

    for target, parts in (("curated_trees", parts_t), ("curated_snapshots", parts_s)):
        union = " UNION ALL BY NAME ".join(f"SELECT * FROM {p}" for p in parts)
        con.execute(f"CREATE OR REPLACE TABLE {target} AS {union}")
        for p in parts:
            con.execute(f"DROP TABLE {p}")
        con.commit()
        print(f"[save] {target} -> {con.execute(f'SELECT COUNT(*) FROM {target}').fetchone()[0]:,} rows")
    con.close()
    print("[done]")
