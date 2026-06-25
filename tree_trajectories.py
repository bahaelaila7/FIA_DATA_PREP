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
    db_input
    return (db_input,)


@app.cell
def _():
    # FIA ownership-group labels (OWNGRPCD).
    OWN_LABELS = {10: "USFS", 20: "Other federal", 30: "State/local", 40: "Private"}
    # Ecoregion field options (table columns).
    ECO_FIELDS = {"EPA L3": "epa_l3", "EPA L4": "epa_l4", "Ecosubcd": "ecosubcd"}
    # Selectable plot axes (X and Y share this set): label → column.
    AXES = {
        "Biomass (g)":    "drybio_g",
        "Age — Selected": "age_calc",
        "Age — TOTAGE":   "age_totage",
        "Age — STDAGE":   "age_stdage",
        "Age — estage":   "age_estage",
        "Age — ML model": "age_model",
        "Diameter (in)":  "dia",
        "Height (ft)":    "ht",
    }
    # column → (axis label, unit) for axis titles and derivative units.
    AXIS_META = {
        "drybio_g":   ("AG dry biomass (g / tree)", "g"),
        "age_calc":   ("Age — selected (yr)", "yr"),
        "age_totage": ("Age — TOTAGE (yr)", "yr"),
        "age_stdage": ("Age — STDAGE (yr)", "yr"),
        "age_estage": ("Age — estage (yr)", "yr"),
        "age_model":  ("Age — ML model (yr)", "yr"),
        "dia":        ("Diameter (in)", "in"),
        "ht":         ("Height (ft)", "ft"),
    }
    return AXES, AXIS_META, ECO_FIELDS, OWN_LABELS


@app.cell
def _(ECO_FIELDS, OWN_LABELS, db_input, duckdb, mo):
    # Load distinct option lists for every filterable column, once.
    def _distinct(con, col):
        return [
            r[0] for r in con.execute(
                f"SELECT DISTINCT {col} FROM tree_trajectories "
                f"WHERE {col} IS NOT NULL ORDER BY 1"
            ).fetchall()
        ]

    try:
        _con = duckdb.connect(db_input.value, read_only=True)
        _has = {r[0] for r in _con.execute("SHOW TABLES").fetchall()}
        if "tree_trajectories" not in _has:
            mo.stop(True, mo.callout(
                mo.md("`tree_trajectories` table not found — run "
                      "`build_tree_trajectories.py` first."), kind="danger"))

        opts = {
            f: _distinct(_con, f) for f in ECO_FIELDS.values()
        }
        opts["species_symbol"]  = _distinct(_con, "species_symbol")
        opts["land_use"]        = _distinct(_con, "land_use")
        opts["cut_event_type"]  = _distinct(_con, "cut_event_type")
        opts["terminal_fate"]   = _distinct(_con, "terminal_fate")
        opts["sftwd_hrdwd"]     = _distinct(_con, "sftwd_hrdwd")
        opts["cycle"]           = _distinct(_con, "cycle")
        opts["owngrpcd"]        = _distinct(_con, "owngrpcd")
        _con.close()
    except Exception as _e:
        mo.stop(True, mo.callout(mo.md(f"**DB error:** {_e}"), kind="danger"))

    own_options = {OWN_LABELS.get(c, str(c)): c for c in opts["owngrpcd"]}
    return opts, own_options


@app.cell
def _(ECO_FIELDS, mo):
    eco_field_dd = mo.ui.dropdown(options=ECO_FIELDS, value="EPA L3", label="Ecoregion level")
    eco_field_dd
    return (eco_field_dd,)


@app.cell
def _(AXES, eco_field_dd, mo, opts, own_options):
    # --- stratification controls ---
    eco_val_dd     = mo.ui.dropdown(options=opts[eco_field_dd.value], label="Ecoregion")
    species_ms     = mo.ui.multiselect(options=opts["species_symbol"], label="Species")
    land_use_ms    = mo.ui.multiselect(options=opts["land_use"], label="Land use")
    cut_event_ms   = mo.ui.multiselect(options=opts["cut_event_type"], label="Cut event (ever)")
    fate_ms        = mo.ui.multiselect(options=opts["terminal_fate"], label="Terminal fate")
    own_ms         = mo.ui.multiselect(options=own_options, label="Ownership")
    swhw_dd        = mo.ui.dropdown(
        options=["(all)"] + opts["sftwd_hrdwd"], value="(all)", label="Soft/hardwood")
    cycle_dd       = mo.ui.dropdown(
        options=["(all)"] + [str(c) for c in opts["cycle"]], value="(all)", label="Cycle")
    color_by_dd    = mo.ui.dropdown(
        options=["species_symbol", "land_use", "terminal_fate", "cut_event_type",
                 "owngrpcd", "sftwd_hrdwd", "cycle", "age_source", "spcd_changed"],
        value="species_symbol", label="Colour by")
    x_axis_dd      = mo.ui.dropdown(options=AXES, value="Age — Selected", label="X axis")
    y_axis_dd      = mo.ui.dropdown(options=AXES, value="Biomass (g)", label="Y axis")
    max_traj       = mo.ui.number(value=300, start=20, stop=5000, step=20,
                                  label="Max trajectories (sample)")
    show_imputed   = mo.ui.checkbox(label="Mark imputed points", value=True)
    derivative_cb  = mo.ui.checkbox(label="Growth rate (Δy/Δx)", value=False)
    repr_dd        = mo.ui.dropdown(
        options={"All points (trajectories)": "all", "First per tree": "first",
                 "Last per tree": "last"},
        value="All points (trajectories)", label="Points")
    logx_cb        = mo.ui.checkbox(label="log X", value=False)
    logy_cb        = mo.ui.checkbox(label="log Y", value=False)
    fit_cb         = mo.ui.checkbox(label="Linear fit + R²", value=False)

    mo.vstack([
        mo.hstack([eco_val_dd, cycle_dd, swhw_dd, own_ms], justify="start", gap=2),
        mo.hstack([land_use_ms, cut_event_ms, fate_ms, species_ms], justify="start", gap=2),
        mo.hstack([x_axis_dd, y_axis_dd, color_by_dd, max_traj, show_imputed,
                   derivative_cb], justify="start", gap=2),
        mo.hstack([repr_dd, logx_cb, logy_cb, fit_cb], justify="start", gap=2),
    ])
    return (color_by_dd, cut_event_ms, cycle_dd, derivative_cb, eco_val_dd,
            fate_ms, fit_cb, land_use_ms, logx_cb, logy_cb, max_traj, own_ms,
            repr_dd, show_imputed, species_ms, swhw_dd, x_axis_dd, y_axis_dd)


@app.cell(hide_code=True)
def _(
    AXIS_META, color_by_dd, cut_event_ms, cycle_dd, db_input, derivative_cb,
    duckdb, eco_field_dd, eco_val_dd, fate_ms, fit_cb, land_use_ms, logx_cb,
    logy_cb, max_traj, mo, np, own_ms, pl, plt, repr_dd, show_imputed,
    species_ms, swhw_dd, x_axis_dd, y_axis_dd,
):
    mo.stop(not eco_val_dd.value, mo.md("_Select an ecoregion to load trajectories._"))

    FATE_MARK = {
        "alive_censored":               ("o", "none"),
        "dead_natural":                 ("v", "full"),
        "dead_disturbance":             ("X", "full"),
        "harvest_explicit":             ("s", "full"),
        "harvest_inferred":             ("s", "none"),
        "disappeared_inferred_removal": ("D", "none"),
    }

    def _inlist(col, vals, quote=True):
        if quote:
            return f"{col} IN ('" + "', '".join(str(v) for v in vals) + "')"
        return f"{col} IN (" + ", ".join(str(v) for v in vals) + ")"

    # Tree-selection filters (a tree is kept if ANY of its rows match — so the
    # FULL trajectory is plotted, not just the matching visit).
    _f = [f"{eco_field_dd.value} = '{eco_val_dd.value}'"]
    if species_ms.value:  _f.append(_inlist("species_symbol", species_ms.value))
    if land_use_ms.value: _f.append(_inlist("land_use", land_use_ms.value))
    if cut_event_ms.value:_f.append(_inlist("cut_event_type", cut_event_ms.value))
    if fate_ms.value:     _f.append(_inlist("terminal_fate", fate_ms.value))
    if own_ms.value:      _f.append(_inlist("owngrpcd", own_ms.value, quote=False))
    if swhw_dd.value and swhw_dd.value != "(all)": _f.append(f"sftwd_hrdwd = '{swhw_dd.value}'")
    if cycle_dd.value and cycle_dd.value != "(all)": _f.append(f"cycle = {cycle_dd.value}")
    _where = " AND ".join(_f)

    _con = duckdb.connect(db_input.value, read_only=True)
    _n_match = _con.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT statecd,unitcd,countycd,plot,subp,tree
            FROM tree_trajectories WHERE {_where})
    """).fetchone()[0]

    # Sample up to max_traj distinct trees, then fetch their FULL trajectories.
    _df = _con.execute(f"""
        WITH ids AS (
            SELECT statecd,unitcd,countycd,plot,subp,tree
            FROM (SELECT DISTINCT statecd,unitcd,countycd,plot,subp,tree
                  FROM tree_trajectories WHERE {_where})
            USING SAMPLE reservoir({int(max_traj.value)} ROWS) REPEATABLE(42)
        )
        SELECT t.statecd,t.unitcd,t.countycd,t.plot,t.subp,t.tree,
               t.age_calc, t.age_totage, t.age_stdage, t.age_estage, t.age_model,
               t.dia, t.ht,
               t.age_source, t.drybio_g, t.species_symbol, t.species_symbol_obs,
               t.spcd_changed, t.intro_type, t.is_terminal,
               t.terminal_fate, t.land_use, t.cut_event_type, t.owngrpcd,
               t.sftwd_hrdwd, t.cycle
        FROM tree_trajectories t
        JOIN ids USING (statecd,unitcd,countycd,plot,subp,tree)
        ORDER BY t.statecd,t.unitcd,t.countycd,t.plot,t.subp,t.tree, t.age_calc
    """).pl()
    _con.close()

    mo.stop(_df.is_empty(), mo.callout(mo.md("No trajectories match the filters."), kind="warn"))

    _cby = color_by_dd.value
    _levels = sorted(_df[_cby].drop_nulls().unique().to_list(), key=str)
    _cmap = plt.get_cmap("tab20", max(len(_levels), 1))
    _color = {lv: _cmap(i) for i, lv in enumerate(_levels)}

    fig, _ax = plt.subplots(figsize=(13, 7.5))
    _legend_done = set()
    _key = ["statecd", "unitcd", "countycd", "plot", "subp", "tree"]
    _n_plotted = 0

    _xcol, _ycol = x_axis_dd.value, y_axis_dd.value
    _repr = repr_dd.value                       # 'all' | 'first' | 'last'
    _logx, _logy = logx_cb.value, logy_cb.value
    _fitx, _fity = [], []                       # plotted points accumulated for the fit
    _base = _df.filter(pl.col(_xcol).is_not_null() & pl.col(_ycol).is_not_null())

    if _repr == "all":
        # full contiguous trajectory per tree (lines + imputed/terminal markers)
        for _tid, _g in _base.group_by(_key, maintain_order=True):
            _g = _g.sort("age_calc")            # age_calc ↑ == measdate order
            if _g.height < 2:
                continue
            _lvl = _g[_cby].drop_nulls().to_list()
            _lv = _lvl[0] if _lvl else None
            _c = _color.get(_lv, (0.5, 0.5, 0.5, 1.0))
            _xv = _g[_xcol].to_numpy().astype(float)
            _yv = _g[_ycol].to_numpy().astype(float)
            if derivative_cb.value:
                _da = np.diff(_xv)
                _ok = _da > 0
                if not _ok.any():
                    continue
                _px, _py = _xv[1:][_ok], (np.diff(_yv) / _da)[_ok]
            else:
                _px, _py = _xv, _yv
            _lbl = str(_lv) if (_lv is not None and _lv not in _legend_done) else "_nolegend_"
            if _lbl != "_nolegend_":
                _legend_done.add(_lv)
            _ax.plot(_px, _py, "-", color=_c, lw=0.9, alpha=0.55, label=_lbl)
            _fitx.extend(_px.tolist()); _fity.extend(_py.tolist())
            if not derivative_cb.value and show_imputed.value:
                _imp = [i for i, t in enumerate(_g["intro_type"].to_list())
                        if t in ("phantom_backfill", "gap_interpolated")]
                if _imp:
                    _ax.scatter(_xv[_imp], _yv[_imp], s=16, facecolors="none",
                                edgecolors=_c, linewidths=0.8, alpha=0.7)
            if not derivative_cb.value:
                _ti = [i for i, t in enumerate(_g["is_terminal"].to_list()) if t]
                if _ti:
                    _i = _ti[0]
                    _mk, _fl = FATE_MARK.get(_g["terminal_fate"].to_list()[_i], ("o", "none"))
                    _ax.scatter(_xv[_i], _yv[_i], marker=_mk, s=42,
                                facecolors=(_c if _fl == "full" else "none"),
                                edgecolors=_c, linewidths=1.0, zorder=3)
            _n_plotted += 1
    else:
        # ONE representative real measurement per tree (first/last by time = age_calc)
        _pts = (
            _base.filter(pl.col("intro_type") == "real").sort("age_calc")
            .unique(subset=_key, keep=("first" if _repr == "first" else "last"))
        )
        for _lv in _levels:
            _sub = _pts.filter(pl.col(_cby) == _lv)
            if _sub.height:
                _ax.scatter(_sub[_xcol].to_numpy(), _sub[_ycol].to_numpy(), s=14,
                            color=_color.get(_lv, (0.5, 0.5, 0.5, 1.0)),
                            alpha=0.6, label=str(_lv))
                _legend_done.add(_lv)
        _fitx = _pts[_xcol].to_list(); _fity = _pts[_ycol].to_list()
        _n_plotted = _pts.height

    if _logx:
        _ax.set_xscale("log")
    if _logy:
        _ax.set_yscale("log")

    # optional linear fit + R² in the CURRENT axis space (log10 where the axis is log)
    if fit_cb.value and len(_fitx) >= 2:
        _fx = np.asarray(_fitx, float); _fy = np.asarray(_fity, float)
        _tx = np.log10(_fx) if _logx else _fx
        _ty = np.log10(_fy) if _logy else _fy
        _m = np.isfinite(_tx) & np.isfinite(_ty)
        _tx, _ty = _tx[_m], _ty[_m]
        if _tx.size >= 2 and np.ptp(_tx) > 0:
            _b, _a = np.polyfit(_tx, _ty, 1)
            _pred = _b * _tx + _a
            _ssr = float(np.sum((_ty - _pred) ** 2))
            _sst = float(np.sum((_ty - _ty.mean()) ** 2))
            _r2 = 1 - _ssr / _sst if _sst > 0 else float("nan")
            _xs = np.linspace(_tx.min(), _tx.max(), 100)
            _ys = _b * _xs + _a
            _ax.plot(10 ** _xs if _logx else _xs, 10 ** _ys if _logy else _ys,
                     "k--", lw=2.2, zorder=6)
            _lx = "log₁₀(x)" if _logx else "x"
            _ly = "log₁₀(y)" if _logy else "y"
            _ax.text(0.98, 0.02,
                     f"{_ly} = {_b:.3g}·{_lx} + {_a:.3g}\nR² = {_r2:.3f}   n = {_tx.size:,}",
                     transform=_ax.transAxes, va="bottom", ha="right", fontsize=9,
                     zorder=7, bbox=dict(boxstyle="round", fc="white", alpha=0.9))

    _xlabel, _xunit = AXIS_META[_xcol]
    _ylabel, _yunit = AXIS_META[_ycol]
    _ax.set_xlabel(_xlabel + ("  (log)" if _logx else ""))
    _ax.set_ylabel((f"Δ({_ylabel}) / Δ({_xunit})" if derivative_cb.value else _ylabel)
                   + ("  (log)" if _logy else ""))
    _ax.set_title(
        f"Tree trajectories — {eco_field_dd.value} {eco_val_dd.value}"
        f"  ·  y={_ylabel} vs x={_xlabel}  ·  pts={_repr}  ·  colour={_cby}"
        f"  ·  {_n_plotted:,} of {_n_match:,} trees")
    if _legend_done:
        _ax.legend(title=_cby, loc="upper left", framealpha=0.85, fontsize=8, ncol=2)
    plt.tight_layout()

    _sampled = _n_match > max_traj.value
    if _repr == "all":
        _legend_txt = (
            "**Lines** = contiguous trajectories · **open points** = phantom/interpolated "
            "(imputed) · **terminal marker**: ○ alive · ▽ dead-natural · ✕ dead-disturbance · "
            "▣ harvest (filled=explicit) · ◇ disappeared.")
    else:
        _legend_txt = (f"**Dots** = each tree's **{_repr}** real measurement "
                       "(one point per tree).")
    if fit_cb.value:
        _legend_txt += " **Dashed line** = OLS fit (computed in the displayed axis space)."
    _caption = mo.md(
        (f"⚠️ **{_n_match:,} trees match** — showing a random sample of "
         f"{_n_plotted:,}. Raise *Max trajectories* to see more.\n\n" if _sampled else "")
        + _legend_txt
    )
    mo.vstack([fig, _caption])
    return


if __name__ == "__main__":
    app.run()
