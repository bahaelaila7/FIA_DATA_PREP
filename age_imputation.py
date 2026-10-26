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
    FIX_COLUMNS,
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
