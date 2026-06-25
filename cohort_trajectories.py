import marimo

__generated_with = "0.23.6"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import duckdb
    import polars as pl
    import numpy as np
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    return duckdb, mo, np, pl, plt


@app.cell
def _(mo):
    db_input = mo.ui.text(
        value="../FIASQLITE2PGSQL/FIADB.duckdb",
        label="Database path",
        full_width=True,
    )
    return (db_input,)


@app.cell
def _(db_input):
    db_input
    return


@app.cell
def _(db_input, duckdb, mo):
    try:
        _con = duckdb.connect(db_input.value, read_only=True)

        _cols = {r[0].lower() for r in _con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'curated_cohorts_landis'"
        ).fetchall()}

        species_options = sorted(
            _con.execute(
                "SELECT DISTINCT species_symbol FROM curated_cohorts_landis "
                "WHERE species_symbol IS NOT NULL ORDER BY 1"
            ).fetchdf()["species_symbol"].tolist()
        )

        eco_l3_options = sorted(
            _con.execute(
                "SELECT DISTINCT epa_l3 FROM curated_cohorts_landis "
                "WHERE epa_l3 IS NOT NULL ORDER BY 1"
            ).fetchdf()["epa_l3"].tolist()
        )

        eco_l4_options = sorted(
            _con.execute(
                "SELECT DISTINCT epa_l4 FROM curated_cohorts_landis "
                "WHERE epa_l4 IS NOT NULL ORDER BY 1"
            ).fetchdf()["epa_l4"].tolist()
        ) if "epa_l4" in _cols else []

        _raw_intro = _con.execute(
            "SELECT DISTINCT intro_type FROM curated_cohorts_landis ORDER BY 1"
        ).fetchdf()["intro_type"].tolist()
        intro_type_options = [v for v in _raw_intro if v is not None] + ["(null)"]

        has_l4 = "epa_l4" in _cols
        _con.close()

    except Exception as _e:
        mo.stop(True, mo.callout(mo.md(f"**Error connecting to database:** {_e}"), kind="danger"))
    return (
        eco_l3_options,
        eco_l4_options,
        has_l4,
        intro_type_options,
        species_options,
    )


@app.cell
def _(has_l4, intro_type_options, mo, species_options):
    eco_field_dd = mo.ui.dropdown(
        options={"EPA L3": "epa_l3", "EPA L4": "epa_l4"} if has_l4 else {"EPA L3": "epa_l3"},
        value="EPA L3",
        label="Ecoregion field",
    )
    species_dd = mo.ui.multiselect(options=species_options, label="Species")
    intro_dd = mo.ui.multiselect(
        options=intro_type_options,
        value=intro_type_options,
        label="Intro type",
    )
    return eco_field_dd, intro_dd, species_dd


@app.cell
def _(eco_field_dd, eco_l3_options, eco_l4_options, intro_dd, mo, species_dd):
    _opts = eco_l3_options if eco_field_dd.value == "epa_l3" else eco_l4_options
    eco_dd = mo.ui.dropdown(options=_opts, label=f"Ecoregion ({eco_field_dd.value})")
    mo.hstack([eco_field_dd, eco_dd, species_dd, intro_dd], justify="start", gap=4)
    return (eco_dd,)


@app.function
def build_intro_clause(intro_values):
    """Return a SQL WHERE fragment for intro_type, or None if unrestricted."""
    if not intro_values:
        return None
    vals     = [v for v in intro_values if v != "(null)"]
    has_null = "(null)" in intro_values
    parts = []
    if vals:
        quoted = ", ".join(f"'{v}'" for v in vals)
        parts.append(f"intro_type IN ({quoted})")
    if has_null:
        parts.append("intro_type IS NULL")
    return f"({' OR '.join(parts)})" if parts else None


@app.cell
def _(db_input, duckdb, eco_dd, eco_field_dd, intro_dd, mo, species_dd):
    mo.stop(not eco_dd.value, mo.md("_Select an ecoregion to load matching plots._"))

    _wheres = [f"{eco_field_dd.value} = '{eco_dd.value}'"]
    if species_dd.value:
        _quoted = ", ".join(f"'{s}'" for s in species_dd.value)
        _wheres.append(f"species_symbol IN ({_quoted})")
    _ic = build_intro_clause(intro_dd.value)
    if _ic:
        _wheres.append(_ic)

    _con = duckdb.connect(db_input.value, read_only=True)
    _rows = _con.execute(f"""
        SELECT DISTINCT statecd, unitcd, countycd, plot
        FROM curated_cohorts_landis
        WHERE {" AND ".join(_wheres)}
        AND subp_has_dstrb = false
        AND subp_meas_num > 1
        ORDER BY 1, 2, 3, 4
    """).fetchall()
    _con.close()

    plot_options = {"All plots": None} | {
        f"{s}-{u}-{c}-{p}": (s, u, c, p) for s, u, c, p in _rows
    }
    return (plot_options,)


@app.cell
def _(mo, plot_options):
    plot_dd = mo.ui.dropdown(
        options=plot_options,
        label=f"Plot  ({len(plot_options):,} available — statecd-unitcd-countycd-plot)",
    )
    plot_dd
    return (plot_dd,)


@app.cell
def _(db_input, duckdb, plot_dd):
    if not plot_dd.value:
        subplot_options = {"All subplots": None}
    else:
        _s, _u, _c, _p = plot_dd.value
        _con = duckdb.connect(db_input.value, read_only=True)
        _subps = _con.execute(
            f"SELECT DISTINCT subp FROM curated_cohorts_landis "
            f"WHERE statecd={_s} AND unitcd={_u} AND countycd={_c} AND plot={_p} "
            f"AND subp_has_dstrb = false AND subp_meas_num > 1"
            f"ORDER BY subp"
        ).fetchdf()["subp"].tolist()
        _con.close()
        subplot_options = {"All subplots": None} | {str(sp): sp for sp in _subps}
    return (subplot_options,)


@app.cell
def _(mo, subplot_options):
    subplot_dd = mo.ui.dropdown(
        options=subplot_options,
        value="All subplots",
        label="Subplot",
    )
    subplot_dd
    return (subplot_dd,)


@app.cell
def _(mo):
    derivative_cb = mo.ui.checkbox(
        label="Show growth rate (ΔAGB / Δage, g/m²/yr)", value=False
    )
    derivative_cb
    return (derivative_cb,)


@app.cell
def _(
    db_input,
    derivative_cb,
    duckdb,
    eco_dd,
    eco_field_dd,
    intro_dd,
    mo,
    np,
    pl,
    plot_dd,
    plt,
    species_dd,
    subplot_dd,
):
    _wheres = [f"{eco_field_dd.value} = '{eco_dd.value}'"]

    if plot_dd.value:
        _s, _u, _c, _p = plot_dd.value
        _wheres.append(
            f"statecd={_s} AND unitcd={_u} AND countycd={_c} AND plot={_p}"
        )
    else:
        _s, _u, _c, _p = None, None, None, None

    if subplot_dd.value is not None:
        _wheres.append(f"subp = {subplot_dd.value}")
    if species_dd.value:
        _quoted = ", ".join(f"'{s}'" for s in species_dd.value)
        _wheres.append(f"species_symbol IN ({_quoted})")
    _ic = build_intro_clause(intro_dd.value)
    if _ic:
        _wheres.append(_ic)

    _con = duckdb.connect(db_input.value, read_only=True)
    _df = _con.execute(f"""
        SELECT
            statecd, unitcd, countycd, plot, subp,
            species_symbol,
            intro_type,
            sim_year,
            age_calc,
            agb,
            (sim_year - age_calc) AS birth_sim_year
        FROM curated_cohorts_landis
        WHERE {" AND ".join(_wheres)}
        ORDER BY statecd, unitcd, countycd, plot, subp,
                 species_symbol, birth_sim_year, sim_year
    """).pl()
    _con.close()

    mo.stop(
        _df.is_empty(),
        mo.callout(mo.md("No data found for this selection."), kind="warn"),
    )

    # Aggregate: sum agb_sum per cohort × sim_year (guards against duplicate rows)
    _cohort_id = ["statecd", "unitcd", "countycd", "plot", "subp",
                  "species_symbol", "birth_sim_year"]
    _df = (
        _df.group_by(_cohort_id + ["sim_year", "age_calc", "intro_type"])
        .agg(pl.col("agb").sum())
        .sort(_cohort_id + ["sim_year"])
    )

    _sp_list  = sorted(_df["species_symbol"].drop_nulls().unique().to_list())
    _cmap     = plt.get_cmap("tab10", max(len(_sp_list), 1))
    _sp_color = {sp: _cmap(i) for i, sp in enumerate(_sp_list)}

    fig, _ax = plt.subplots(figsize=(12, 7))
    _done = set()

    for _sp in _sp_list:
        _sp_pd = _df.filter(pl.col("species_symbol") == _sp).to_pandas()
        for _, _grp in _sp_pd.groupby(_cohort_id, sort=False):
            if len(_grp) < 2:
                continue
            _grp = _grp.sort_values("sim_year")
            if derivative_cb.value:
                _da = np.diff(_grp["age_calc"].values).astype(float)
                _valid = _da > 0
                if not _valid.any():
                    continue
                _x = _grp["age_calc"].values[1:][_valid]
                _y = (np.diff(_grp["agb"].values) / _da)[_valid]
            else:
                _x = _grp["age_calc"].values
                _y = _grp["agb"].values
            _lbl = _sp if _sp not in _done else "_nolegend_"
            _done.add(_sp)
            _ax.scatter(_x, _y, color=_sp_color[_sp], linewidth=1.0, alpha=0.7, label=_lbl)

    _plot_str = f"{_s}-{_u}-{_c}-{_p}" if plot_dd.value else "all plots"
    _subp_str = (
        f"  subp={subplot_dd.value}"
        if (plot_dd.value and subplot_dd.value is not None)
        else ""
    )
    _ylabel = "Growth rate (g/m² / yr)" if derivative_cb.value else "AGB (g/m²)"
    _ax.set_xlabel("Age (yr)")
    _ax.set_ylabel(_ylabel)
    _ax.set_title(
        f"Cohort trajectories  —  {_plot_str}{_subp_str}"
        f"\n{eco_field_dd.value}: {eco_dd.value}"
    )
    if _done:
        _ax.legend(title="Species", loc="upper left", framealpha=0.8)
    plt.tight_layout()
    fig
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
