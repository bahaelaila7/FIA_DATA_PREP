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

import sys

import duckdb
import numpy as np
import polars as pl
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import r2_score
from sklearn.model_selection import cross_val_score
from sklearn.preprocessing import OrdinalEncoder

from fia_curation_v2 import (
    SUBPLOT_ID,
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
    cv_r2 = cross_val_score(model, X, y, cv=5, scoring="r2", n_jobs=-1)
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

    anchors = _select_best_anchor(candidates, tree_bounds, apply_min_shift=False)
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

def _save(con, df: pl.DataFrame, snapshots: pl.DataFrame):
    con.register("_df_view", df)
    con.execute("CREATE OR REPLACE TABLE curated_trees AS SELECT * FROM _df_view")
    con.unregister("_df_view")

    con.register("_snap_view", snapshots)
    con.execute("CREATE OR REPLACE TABLE curated_snapshots AS SELECT * FROM _snap_view")
    con.unregister("_snap_view")

    con.commit()
    print(f"[save] curated_trees    → {df.height:,} rows")
    print(f"[save] curated_snapshots → {snapshots.height:,} rows")


if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else "../data_eco_cohorts.duckdb"

    print("=" * 70)
    print("Age Imputation Pipeline")
    print(f"DB: {db_path}")
    print("=" * 70)

    con = duckdb.connect(db_path)

    print("[load] reading curated_trees ...")
    df = con.execute("SELECT * FROM curated_trees").pl()
    print(f"       {df.height:,} rows, {len(df.columns)} columns")

    all_subplot_years = con.execute("""
        SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, MEASDATE
        FROM clean_subplot_years
    """).pl()

    df = run_age_imputation(con, df, all_subplot_years)

    # Rebuild snapshots so estimated_age groupings reflect the imputed ages.
    # build_subplot_year_scaffold uses the SQL tables created by run_pipeline
    # (base_subplot_cond, clean_subplot_years, longitudinal_subplots,
    #  excluded_tree_ids) which remain in the database across runs.
    scaffold = build_subplot_year_scaffold(con)
    ever_live = (
        df.filter(
            (pl.col("STATUSCD") == 1) & pl.col("DRYBIO_AG").is_not_null()
        )
        .select(SUBPLOT_ID).unique()
    )
    n_before  = scaffold.height
    scaffold  = scaffold.join(ever_live, on=SUBPLOT_ID, how="semi")
    print(f"[scaffold] {scaffold.height:,} rows "
          f"(dropped {n_before - scaffold.height:,} non-forested subplot-years)")

    snapshots = build_snapshots(df, scaffold)

    _save(con, df, snapshots)
    con.close()
    print("[done]")
