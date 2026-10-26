"""
age_imputation.py
Model-based tree age imputation for trees lacking an age estimate.

Fits a HistGradientBoostingRegressor:
    log1p(age) ~ DIA + HT + CR + CCLCD + cond_alstkcd | major_spgrpcd, epa_l3

major_spgrpcd : REF_SPECIES.LARGE_SPGRPCD  (broad species group)
epa_l3        : data_plot_eco.epa_l3        (EPA Level 3 ecoregion)

Trains on alive (STATUSCD=1) rows that already have estimated_age + DIA.
Fills only rows where estimated_age is currently null; existing TOTAGE/STDAGE
ages are never overwritten.  Rebuilds curated_snapshots so groupings by
estimated_age stay consistent.

Standalone usage:
    python age_imputation.py [path/to/fiadb.duckdb]
    # default db path: ../data_eco_cohorts.duckdb

Library usage (after run_pipeline has populated curated_trees):
    from age_imputation import run_age_imputation
    df = run_age_imputation(con, df)
"""

import os as _os, sys as _sys  # curation-fix: sibling modules resolve under `python -I`
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
import os
import sys

import duckdb
import numpy as np
import polars as pl
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import r2_score
from sklearn.model_selection import cross_val_score
from sklearn.preprocessing import OrdinalEncoder

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

_NUM_COLS = ["DIA", "HT", "CR", "CCLCD", "cond_alstkcd"]
_CAT_COLS = ["major_spgrpcd", "epa_l3"]
_ALL_COLS = _NUM_COLS + _CAT_COLS
N_JOBS = int(os.environ.get("FIA_N_JOBS", "3"))


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

    spgrp = con.execute("""
        SELECT SPCD, MAJOR_SPGRPCD AS major_spgrpcd
        FROM REF_SPECIES
    """).pl().with_columns(
        pl.col("SPCD").cast(df.schema["SPCD"])
    )

    plot_key = ["STATECD", "UNITCD", "COUNTYCD", "PLOT"]
    df = (
        df.join(eco,   on=plot_key, how="left")
          .join(spgrp, on="SPCD",   how="left")
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
# Tree_Age (Zenodo 14775738) runs older, so every estage anchor candidate uses
#   field_age = f_g(Tree_Age), monotone, fitted in log1p space per species group g.
#   * calibration: LIVE records with DIA >= 5 in and Tree_Age > 1 that have a field age at the
#     same measurement;
#   * groups: spgrpcd_resolved -> major_spgrpcd -> global, falling back whenever a group has
#     fewer than CAL_MIN_N overlap records; within a group, planted (STDORGCD=1) vs natural
#     are separate curves only when BOTH strata have >= CAL_MIN_N records;
#   * f = isotonic regression on log1p (sklearn IsotonicRegression) — monotone; inside the
#     fitted range it interpolates its knots, outside it is clamped to the end knot and
#     extended LINEARLY in log1p with the group's OLS log-log slope (clipped to
#     [CAL_SLOPE_MIN, CAL_SLOPE_MAX]) — no wild extrapolation; a per-curve Duan smearing
#     factor on (1 + age) removes the log back-transform bias (still monotone);
#   * fitted once over the whole DB; plot-grouped 5-fold CV reports out-of-fold MAE / bias of
#     mapped vs raw Tree_Age against the field age.
CAL_MIN_N = 200
CAL_SLOPE_MIN, CAL_SLOPE_MAX = 0.25, 1.5
CAL_COLS = ["STATECD", "UNITCD", "COUNTYCD", "PLOT", "SPCD", "spgrpcd_resolved", "cond_stdorgcd",
            "STATUSCD", "DIA", "estage_raw", "age_from_totage", "age_from_stdage"]


def _calibration_set(con) -> pl.DataFrame:
    cols = ", ".join(CAL_COLS)
    df = con.execute(f"""
        SELECT {cols} FROM curated_trees
        WHERE STATUSCD = 1 AND DIA >= {ESTAGE_MIN_DIA} AND estage_raw > {ESTAGE_MIN_AGE}
          AND COALESCE(age_from_totage, age_from_stdage) IS NOT NULL""").pl()
    spgrp = con.execute("SELECT SPCD, MAJOR_SPGRPCD AS major_spgrpcd FROM REF_SPECIES").pl()
    df = (df.join(spgrp.with_columns(pl.col("SPCD").cast(df.schema["SPCD"])), on="SPCD", how="left")
            .with_columns([
                pl.coalesce(["age_from_totage", "age_from_stdage"]).alias("field_age"),
                (pl.col("cond_stdorgcd") == 1).fill_null(False).alias("planted"),
                pl.concat_str([pl.col(c).cast(pl.Utf8) for c in ("STATECD", "UNITCD", "COUNTYCD", "PLOT")],
                              separator="_").alias("_plot"),
            ])
            .filter(pl.col("field_age") > 0))
    return df


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


def _group_keys(spgrp, major, planted):
    """Candidate model keys in fallback order for one record."""
    return [("spgrpcd", spgrp, planted), ("spgrpcd", spgrp, None),
            ("major_spgrpcd", major, planted), ("major_spgrpcd", major, None),
            ("global", None, None)]


def fit_estage_mapping(cal: pl.DataFrame, stratify: bool = True) -> dict:
    """{(level, value, planted|None): curve}. A group gets a curve when it has >= CAL_MIN_N
    records; it is split planted/natural only when both strata reach CAL_MIN_N."""
    models = {}
    x_all, y_all = cal["estage_raw"].to_numpy().astype(float), cal["field_age"].to_numpy().astype(float)
    models[("global", None, None)] = _fit_curve(x_all, y_all)
    for level, col in (("spgrpcd", "spgrpcd_resolved"), ("major_spgrpcd", "major_spgrpcd")):
        for (val,), g in cal.group_by([col], maintain_order=True):
            if val is None or g.height < CAL_MIN_N:
                continue
            gp, gn = g.filter(pl.col("planted")), g.filter(~pl.col("planted"))
            if stratify and gp.height >= CAL_MIN_N and gn.height >= CAL_MIN_N:
                for flag, sub in ((True, gp), (False, gn)):
                    models[(level, val, flag)] = _fit_curve(sub["estage_raw"].to_numpy().astype(float),
                                                            sub["field_age"].to_numpy().astype(float))
            else:
                models[(level, val, None)] = _fit_curve(g["estage_raw"].to_numpy().astype(float),
                                                        g["field_age"].to_numpy().astype(float))
    return models


def map_estage(models: dict, raw, spgrp, major, planted) -> tuple[np.ndarray, list]:
    """Mapped field-scale age + the model key used, record by record (vectorised per key)."""
    raw = np.asarray(raw, dtype=float)
    out = np.full(raw.shape, np.nan)
    used = [None] * raw.size
    keys = {}
    for i, (sg, mj, pt) in enumerate(zip(spgrp, major, planted)):
        if not np.isfinite(raw[i]):
            continue
        for k in _group_keys(sg, mj, bool(pt)):
            if k in models:
                keys.setdefault(k, []).append(i)
                used[i] = k
                break
    for k, idx in keys.items():
        idx = np.asarray(idx)
        out[idx] = _apply_curve(models[k], raw[idx])
    return out, used


def cv_estage_mapping(cal: pl.DataFrame, n_folds: int = 5, stratify: bool = True) -> dict:
    """Plot-grouped K-fold CV: out-of-fold MAE / bias (yr) of mapped and raw vs field age."""
    from sklearn.model_selection import GroupKFold
    y = cal["field_age"].to_numpy().astype(float)
    raw = cal["estage_raw"].to_numpy().astype(float)
    pred = np.full(y.shape, np.nan)
    for tr, te in GroupKFold(n_splits=n_folds).split(raw, y, groups=cal["_plot"].to_numpy()):
        m = fit_estage_mapping(cal[tr], stratify)
        sub = cal[te]
        pred[te], _ = map_estage(m, sub["estage_raw"].to_numpy(), sub["spgrpcd_resolved"].to_list(),
                                 sub["major_spgrpcd"].to_list(), sub["planted"].to_list())
    return {"n": int(y.size), "mae_mapped": float(np.nanmean(np.abs(pred - y))),
            "bias_mapped": float(np.nanmean(pred - y)), "mae_raw": float(np.mean(np.abs(raw - y))),
            "bias_raw": float(np.mean(raw - y)), "pred": pred}


def calibrate_estage(con):
    """Fit the TreeAge -> field-age mapping over the whole DB, report CV, write the
    estage_calibration table; returns the models (None when there is no overlap)."""
    cal = _calibration_set(con)
    print(f"[estage-cal] calibration set: {cal.height:,} live >= {ESTAGE_MIN_DIA:g} in records with "
          f"Tree_Age > {ESTAGE_MIN_AGE} and a field age (TOTAGE {cal['age_from_totage'].is_not_null().sum():,}"
          f" / STDAGE {cal.filter(pl.col('age_from_totage').is_null()).height:,})")
    if cal.height < CAL_MIN_N:
        print("[estage-cal] too little overlap — estage left on the raw scale")
        return None
    cv_s = cv_estage_mapping(cal, stratify=True)
    cv_p = cv_estage_mapping(cal, stratify=False)
    print(f"[estage-cal] plot-grouped 5-fold CV vs field age (n={cv_s['n']:,}):")
    print(f"             raw Tree_Age        MAE {cv_s['mae_raw']:6.2f} yr  bias {cv_s['bias_raw']:+6.2f} yr")
    print(f"             mapped (pooled)     MAE {cv_p['mae_mapped']:6.2f} yr  bias {cv_p['bias_mapped']:+6.2f} yr")
    print(f"             mapped (+planted)   MAE {cv_s['mae_mapped']:6.2f} yr  bias {cv_s['bias_mapped']:+6.2f} yr")
    stratify = cv_s["mae_mapped"] <= cv_p["mae_mapped"]
    print(f"[estage-cal] planted/natural split {'KEPT' if stratify else 'DROPPED'} "
          f"(CV MAE {cv_s['mae_mapped']:.2f} vs pooled {cv_p['mae_mapped']:.2f})")
    best = cv_s if stratify else cv_p
    models = fit_estage_mapping(cal, stratify)
    # per-group out-of-fold diagnostics + knots table
    _, used = map_estage(models, cal["estage_raw"].to_numpy(), cal["spgrpcd_resolved"].to_list(),
                         cal["major_spgrpcd"].to_list(), cal["planted"].to_list())
    y, raw = cal["field_age"].to_numpy().astype(float), cal["estage_raw"].to_numpy().astype(float)
    rows = []
    used_arr = np.array([str(u) for u in used])
    for k, c in models.items():
        m = used_arr == str(k)
        rows.append({
            "level": k[0], "group": None if k[1] is None else str(int(k[1])),
            "stratum": "all" if k[2] is None else ("planted" if k[2] else "natural"),
            "n_fit": c["n"], "n_applied_cal": int(m.sum()),
            "oof_mae_mapped": float(np.nanmean(np.abs(best["pred"][m] - y[m]))) if m.any() else None,
            "oof_bias_mapped": float(np.nanmean(best["pred"][m] - y[m])) if m.any() else None,
            "mae_raw": float(np.mean(np.abs(raw[m] - y[m]))) if m.any() else None,
            "bias_raw": float(np.mean(raw[m] - y[m])) if m.any() else None,
            "ext_slope": c["slope"], "smearing_factor": c["smear"],
            "knots_tree_age": [round(float(v), 3) for v in np.expm1(c["kx"])],
            "knots_field_age": [round(float(v), 3) for v in np.exp(c["ky"]) * c["smear"] - 1.0],
        })
    tab = pl.DataFrame(rows, schema_overrides={"knots_tree_age": pl.List(pl.Float64),
                                               "knots_field_age": pl.List(pl.Float64)})
    tab = tab.with_columns([
        pl.lit(best["mae_mapped"]).alias("cv_mae_mapped_overall"),
        pl.lit(best["bias_mapped"]).alias("cv_bias_mapped_overall"),
        pl.lit(best["mae_raw"]).alias("mae_raw_overall"),
        pl.lit(best["bias_raw"]).alias("bias_raw_overall"),
        pl.lit(stratify).alias("planted_split_used"),
    ])
    con.register("_cal", tab.to_arrow())
    con.execute("CREATE OR REPLACE TABLE estage_calibration AS SELECT * FROM _cal")
    con.unregister("_cal")
    con.commit()
    print(f"[estage-cal] {len(models)} curves → estage_calibration")
    return models


def remap_estage(df: pl.DataFrame, models, tree_bounds: pl.DataFrame) -> pl.DataFrame:
    """Re-anchor estage on the field scale: estage_mapped = f_g(Tree_Age) on valid
    candidates (DIA >= 5, Tree_Age > 1), then the usual median anchor + propagation, and the
    age-discrepancy flags. Column order is preserved."""
    if models is None:
        return df
    order = df.columns
    valid = (pl.col("estage_raw").is_not_null() & (pl.col("DIA") >= ESTAGE_MIN_DIA).fill_null(False)
             & (pl.col("estage_raw") > ESTAGE_MIN_AGE))
    sub = df.select(pl.int_range(pl.len()).alias("_i"), valid.alias("_v"), "estage_raw",
                    "spgrpcd_resolved", "major_spgrpcd",
                    (pl.col("cond_stdorgcd") == 1).fill_null(False).alias("_pl")).filter(pl.col("_v"))
    mapped, _ = map_estage(models, sub["estage_raw"].to_numpy(), sub["spgrpcd_resolved"].to_list(),
                           sub["major_spgrpcd"].to_list(), sub["_pl"].to_list())
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
    Build (N, 7) float32 feature matrix.
    Numeric columns: polars null → NaN (HistGBT handles natively).
    Categorical columns: cast to Utf8 (null → None), then OrdinalEncoded;
    unknowns and nulls map to NaN.
    """
    num = df.select(_NUM_COLS).to_pandas().values.astype(np.float32)

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

def fit_age_model(df: pl.DataFrame) -> tuple:
    """
    Train HistGradientBoostingRegressor on log1p(estimated_age).

    Training set: alive (STATUSCD=1) rows with estimated_age and DIA non-null.
    Uses only the TOTAGE/STDAGE merged estimated_age as training target (not
    estage) so the model learns from field-observed ages only.

    Returns (model, enc).
    """
    train = df.filter(
        (pl.col("STATUSCD") == 1) &
        pl.col("age_source").is_not_null() &
        pl.col("estimated_age").is_not_null() &
        pl.col("estimated_age").is_not_nan() &
        pl.col("DIA").is_not_null() &
        pl.col("DIA").is_not_nan()
    )

    # Remove upper age outliers (above p90) to avoid tail distortion.
    # Lower tail is kept — young saplings are valid and important training signal.
    _p90 = train["estimated_age"].quantile(0.90)
    train_trimmed = train.filter(pl.col("estimated_age") <= _p90)

    n_alive = df.filter(pl.col("STATUSCD") == 1).height
    print(f"[step2] training set: {train_trimmed.height:,} / {n_alive:,} alive rows "
          f"({100 * train_trimmed.height / max(n_alive, 1):.1f}%) after age_source+DIA filter "
          f"and p90 upper trim (age ≤ {_p90:.0f} yr, dropped {train.height - train_trimmed.height:,})")
    train = train_trimmed

    enc = OrdinalEncoder(
        handle_unknown="use_encoded_value",
        unknown_value=np.nan,
        encoded_missing_value=np.nan,
    )
    X = _build_X(train, enc, fit_enc=True)

    ages_raw = train["estimated_age"].to_numpy().astype(np.float64)
    y = np.log1p(ages_raw)
    if np.isnan(y).any():
        bad = ages_raw[np.isnan(y)]
        raise ValueError(
            f"y contains {np.isnan(y).sum()} NaN after log1p — "
            f"estimated_age has non-positive or non-finite values: {np.unique(bad)}"
        )

    model = HistGradientBoostingRegressor(
        max_iter=300,
        max_depth=6,
        min_samples_leaf=30,
        random_state=42,
    )
    model.fit(X, y)

    train_rmse = np.sqrt(
        np.mean((np.expm1(model.predict(X)) - np.expm1(y)) ** 2)
    )
    cv_r2 = cross_val_score(model, X, y, cv=5, scoring="r2", n_jobs=N_JOBS)  # shared box: bounded
    print(f"         train RMSE : {train_rmse:.1f} yr")
    print(f"         5-fold CV R²: {cv_r2.mean():.3f} ± {cv_r2.std():.3f}")

    ages = train["estimated_age"].drop_nulls()
    q = ages.quantile
    print(f"         training age distribution (yr): "
          f"p10={q(0.10):.0f}  p25={q(0.25):.0f}  p50={q(0.50):.0f}  "
          f"p75={q(0.75):.0f}  p90={q(0.90):.0f}  max={ages.max():.0f}")

    # R² breakdown by major_spgrpcd (year scale)
    y_pred_train  = model.predict(X)
    y_years       = np.expm1(y)
    ypred_years   = np.expm1(y_pred_train)
    grp_series    = train["major_spgrpcd"]
    print(f"         R² by major_spgrpcd (in-sample, year scale):")
    for grp_val in sorted(grp_series.drop_nulls().unique().to_list()):
        mask = (grp_series == grp_val).to_numpy()
        n = int(mask.sum())
        if n < 20:
            continue
        r2   = r2_score(y_years[mask], ypred_years[mask])
        rmse = np.sqrt(np.mean((y_years[mask] - ypred_years[mask]) ** 2))
        print(f"           spgrp {str(grp_val):>4s}: n={n:>8,}  R²={r2:.3f}  RMSE={rmse:.1f} yr")

    return model, enc


# =============================================================================
# Step 3: apply + merge
# =============================================================================

def apply_age_model(df: pl.DataFrame,
                    model: HistGradientBoostingRegressor,
                    enc: OrdinalEncoder,
                    tree_bounds: pl.DataFrame) -> pl.DataFrame:
    """
    Predict a raw age for every alive row with DIA, then use _select_best_anchor
    to choose one representative measurement per tree (minimising max-age
    violation, with mandatory min-age shift so age >= 1 at first measurement),
    and propagate linearly to all measurements.

    Adds column age_modeled (Int32, null where no anchor selected).
    Extends estimated_age / age_source with model as lowest-priority source.
    """
    # Raw per-row prediction (alive + DIA only; others get NaN → excluded)
    alive_w_dia = df.filter(
        (pl.col("STATUSCD") == 1) & pl.col("DIA").is_not_null()
    )
    X_cand = _build_X(alive_w_dia, enc, fit_enc=False)
    raw    = np.expm1(model.predict(X_cand)).clip(1.0, 999.0)

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
# Convenience wrapper
# =============================================================================

def run_age_imputation(con, df: pl.DataFrame,
                       all_subplot_years: pl.DataFrame) -> pl.DataFrame:
    """Attach context → compute tree bounds → fit model → apply."""
    df            = attach_modeling_context(con, df)
    tree_bounds   = _compute_tree_bounds(df, all_subplot_years)
    model, enc    = fit_age_model(df)
    df            = apply_age_model(df, model, enc, tree_bounds)
    return df


# =============================================================================
# Standalone entry point
# =============================================================================

TRAIN_COLS = ["STATECD", "UNITCD", "COUNTYCD", "PLOT", "SPCD", "STATUSCD", "age_source",
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
    # overwrote. Re-running step 2 then gives the same result as the first run.
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

    cols = ", ".join(TRAIN_COLS)
    train_df = attach_modeling_context(con, con.execute(f"SELECT {cols} FROM curated_trees").pl())
    model, enc = fit_age_model(train_df)
    del train_df
    estage_models = calibrate_estage(con)        # I2: TreeAge -> field scale, once over the DB
    scaffold_all = build_subplot_year_scaffold(con)
    states = [r[0] for r in con.execute(
        "SELECT DISTINCT STATECD FROM curated_trees ORDER BY 1").fetchall()]
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
        df = _order_columns(apply_age_model(df, model, enc, tree_bounds))

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
