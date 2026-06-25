"""
trees_fvs.py
Curates FIA data into a TREE-LEVEL table for the Pan FVS simulation path.

Where cohorts_landis.py aggregates live trees into cohorts (dropping the
individual-tree attributes FVS needs), this builder keeps one row per
live tree x measurement visit and carries the fields required to write FVS
TREEDATA records plus the stand/site descriptors for the STDINFO/DESIGN
keywords. It reuses the same eco / species / measurement-count machinery as
cohorts_landis.py so Julia's existing eco / extent / plot filters work
unchanged on the output.

Source tables  : curated_trees, curated_cohorts (FIA curation pipeline)
Reference tables: data_plot_eco, REF_SPECIES, PLOT (FIA raw)
Output table   : curated_trees_fvs

FVS mapping
-----------
  FVS stand  = FIA plot  (statecd, unitcd, countycd, plot)
  FVS point  = FIA subplot (subp)            -> tree-record "plot" field
  PROB       = TPA_UNADJ                      -> per-acre expansion
  species    = SPCD (FVS reads FIA codes directly; no alpha map needed)

Output schema (one row per live tree x visit)
  plt_cn, statecd, unitcd, countycd, plot, subp, tree
  epa_l3, epa_l4, ecosubcd
  spcd, species_symbol, spgrpcd, major_spgrpcd, sftwd_hrdwd
  measdate, sim_year, plot_meas_num, subp_meas_num, subp_has_dstrb
  -- FVS tree record --
  dia, ht, actualht, cr, cclcd, tpa_unadj, statuscd, estimated_age
  -- biomass ground truth --
  drybio_ag, carbon_ag, agb           (agb = DRYBIO_AG*TPA_UNADJ in g/m^2)
  -- stand / site descriptors (constant within a plot visit) --
  site_slope, site_aspect, site_elev,
  site_sicond, site_sibase, site_sisp, site_fortypcd, site_siteclcd,
  lat, lon

Standalone usage:
    python trees_fvs.py [path/to/fiadb.duckdb]
    # default: ../FIASQLITE2PGSQL/FIADB.duckdb
"""

import sys

import duckdb
import polars as pl

PLOT_KEY   = ["STATECD", "UNITCD", "COUNTYCD", "PLOT"]
SUBPLOT_ID = ["STATECD", "UNITCD", "COUNTYCD", "PLOT", "SUBP"]
TREE_ID    = ["STATECD", "UNITCD", "COUNTYCD", "PLOT", "SUBP", "TREE"]

# 1 lb/acre -> g/m^2  (453.592 g/lb / 4046.86 m^2/acre); matches cohorts_landis.py
LBS_ACRE_TO_G_M2 = 453.592 / 4046.86   # ~= 0.11208


def _has_column(con, table: str, col: str) -> bool:
    rows = con.execute(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_name = '{table}'"
    ).fetchall()
    return col.lower() in {r[0].lower() for r in rows}


def _has_table(con, table: str) -> bool:
    rows = con.execute(
        "SELECT table_name FROM information_schema.tables "
        f"WHERE table_name = '{table}'"
    ).fetchall()
    return len(rows) > 0


# =============================================================================
# Step 1: live tree records + eco + species + site context
# =============================================================================

def load_base_data(con) -> pl.DataFrame:
    """One row per live tree x measurement visit with everything FVS needs."""
    has_l4      = _has_column(con, "data_plot_eco", "epa_l4")
    epa_l4_expr = "e.epa_l4" if has_l4 else "NULL::VARCHAR AS epa_l4"

    has_plot = _has_table(con, "PLOT")
    has_ecosubcd = has_plot and _has_column(con, "PLOT", "ECOSUBCD")
    has_latlon   = has_plot and _has_column(con, "PLOT", "LAT") \
                             and _has_column(con, "PLOT", "LON")

    # Join PLOT per measurement (incl. INVYR) so we can carry PLOT.CN — the real
    # plot-measurement control number. A plot CN == (STATECD,UNITCD,COUNTYCD,
    # PLOT,INVYR) == one MEASDATE, and equals TreeMap's PLT_CN, so curated_trees_fvs
    # is joinable to a TreeMap raster for the spatial/raster use case.
    if has_plot:
        ecosubcd_col = "pl.ECOSUBCD::VARCHAR AS ecosubcd" if has_ecosubcd \
                       else "NULL::VARCHAR AS ecosubcd"
        lat_col = "pl.LAT::DOUBLE AS lat" if has_latlon else "NULL::DOUBLE AS lat"
        lon_col = "pl.LON::DOUBLE AS lon" if has_latlon else "NULL::DOUBLE AS lon"
        plt_join = """
        LEFT JOIN PLOT pl
            ON  pl.STATECD  = t.STATECD AND pl.UNITCD   = t.UNITCD
            AND pl.COUNTYCD = t.COUNTYCD AND pl.PLOT     = t.PLOT
            AND pl.INVYR    = t.INVYR
        """
        plt_cn_col = "pl.CN::BIGINT AS plt_cn"
    else:
        plt_join = ""
        ecosubcd_col = "NULL::VARCHAR AS ecosubcd"
        lat_col = "NULL::DOUBLE AS lat"
        lon_col = "NULL::DOUBLE AS lon"
        plt_cn_col = "NULL::BIGINT AS plt_cn"

    df = con.execute(f"""
        SELECT
            {plt_cn_col},
            t.STATECD, t.UNITCD, t.COUNTYCD, t.PLOT, t.INVYR, t.SUBP, t.TREE,
            t.SPCD,
            t.MEASDATE,
            -- FVS tree-record attributes
            t.DIA::DOUBLE        AS dia,
            t.HT::DOUBLE         AS ht,
            t.ACTUALHT::DOUBLE   AS actualht,
            t.CR::DOUBLE         AS cr,
            t.CCLCD::INTEGER     AS cclcd,
            t.TPA_UNADJ::DOUBLE  AS tpa_unadj,
            t.STATUSCD::INTEGER  AS statuscd,
            ROUND(t.estimated_age)::INTEGER AS estimated_age,
            -- biomass ground truth
            t.DRYBIO_AG::DOUBLE  AS drybio_ag,
            t.CARBON_AG::DOUBLE  AS carbon_ag,
            (t.DRYBIO_AG * t.TPA_UNADJ * {LBS_ACRE_TO_G_M2})::DOUBLE AS agb,
            -- stand / site descriptors (constant within a plot visit)
            t.cond_slope::DOUBLE    AS site_slope,
            t.cond_aspect::DOUBLE   AS site_aspect,
            t.cond_elev::DOUBLE     AS site_elev,
            t.cond_sicond::DOUBLE   AS site_sicond,
            t.cond_sibase::DOUBLE   AS site_sibase,
            t.cond_sisp::INTEGER    AS site_sisp,
            t.cond_fortypcd::INTEGER AS site_fortypcd,
            t.cond_siteclcd::INTEGER AS site_siteclcd,
            -- eco + species metadata
            e.epa_l3,
            {epa_l4_expr},
            {ecosubcd_col},
            {lat_col},
            {lon_col},
            r.SPECIES_SYMBOL::VARCHAR AS species_symbol,
            t.SPGRPCD::INTEGER        AS spgrpcd,
            r.MAJOR_SPGRPCD::INTEGER  AS major_spgrpcd,
            r.SFTWD_HRDWD::VARCHAR    AS sftwd_hrdwd
        FROM curated_trees t
        LEFT JOIN data_plot_eco e
            ON  e.statecd  = t.STATECD AND e.unitcd   = t.UNITCD
            AND e.countycd = t.COUNTYCD AND e.plot     = t.PLOT
        {plt_join}
        LEFT JOIN REF_SPECIES r
            ON  r.SPCD = t.SPCD
        WHERE
            t.STATUSCD = 1
            AND t.DIA       IS NOT NULL
            AND t.TPA_UNADJ IS NOT NULL
            AND t.TPA_UNADJ > 0
    """).pl()

    n = df.height
    print(f"[step1] {n:,} live tree-measurements")
    print(f"         epa_l3:   {df['epa_l3'].is_not_null().sum():,}/{n:,}")
    print(f"         epa_l4:   {df['epa_l4'].is_not_null().sum():,}/{n:,}")
    print(f"         species:  {df['species_symbol'].is_not_null().sum():,}/{n:,}")
    print(f"         dia:      {df['dia'].is_not_null().sum():,}/{n:,}")
    print(f"         ht:       {df['ht'].is_not_null().sum():,}/{n:,}")
    print(f"         site_si:  {df['site_sicond'].is_not_null().sum():,}/{n:,}")
    return df


# =============================================================================
# Step 2: subplot disturbance flag (same source as cohorts_landis.py)
# =============================================================================

def build_subp_has_dstrb(con) -> pl.DataFrame:
    """True for any subplot that was ever harvested or had disturbance death."""
    if not _has_table(con, "curated_cohorts"):
        print("[step2] curated_cohorts absent -> subp_has_dstrb = False for all")
        return None

    cohorts = con.execute("""
        SELECT STATECD, UNITCD, COUNTYCD, PLOT, SUBP,
               ever_harvested, ever_disturbed_dead
        FROM curated_cohorts
    """).pl()

    subp_flag = (
        cohorts.group_by(SUBPLOT_ID)
        .agg(
            (pl.col("ever_harvested").cast(pl.Boolean).any() |
             pl.col("ever_disturbed_dead").cast(pl.Boolean).any())
            .alias("subp_has_dstrb")
        )
    )
    n_total = subp_flag.height
    n_dstrb = subp_flag.filter(pl.col("subp_has_dstrb")).height
    print(f"[step2] subp_has_dstrb: {n_dstrb:,}/{n_total:,} subplots disturbed "
          f"({100 * n_dstrb / max(n_total, 1):.1f}%)")
    return subp_flag


# =============================================================================
# Step 3: measurement counts + plot-level sim_year
# =============================================================================

def build_plot_meas_num(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.select(PLOT_KEY + ["MEASDATE"]).unique()
        .group_by(PLOT_KEY)
        .agg(pl.col("MEASDATE").n_unique().alias("plot_meas_num"))
    )


def build_subp_meas_num(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.select(SUBPLOT_ID + ["MEASDATE"]).unique()
        .group_by(SUBPLOT_ID)
        .agg(pl.col("MEASDATE").n_unique().alias("subp_meas_num"))
    )


def add_plot_sim_year(df: pl.DataFrame) -> pl.DataFrame:
    """
    sim_year = integer years since the plot's FIRST measurement visit.

    The FVS stand is the whole plot, so the time axis is anchored at plot level
    (not subplot). Julia picks sim_year == 0 as the FVS initial state and treats
    later sim_years as comparison targets. MEASDATE (full date) drives ordering;
    the calendar year for FVS INVYEAR is derived downstream in Julia.
    """
    first_meas = (
        df.group_by(PLOT_KEY)
        .agg(pl.col("MEASDATE").min().alias("_first_measdate"))
    )
    return (
        df.join(first_meas, on=PLOT_KEY, how="left")
        .with_columns(
            ((pl.col("MEASDATE") - pl.col("_first_measdate"))
             .dt.total_days() / 365.25)
            .round(0).cast(pl.Int32).alias("sim_year")
        )
        .drop("_first_measdate")
    )


# =============================================================================
# Assembly
# =============================================================================

def build_trees_fvs(con) -> pl.DataFrame:
    print("=" * 70)
    print("FVS Tree-Level Curation Pipeline")
    print("=" * 70)

    df = load_base_data(con)

    subp_flag = build_subp_has_dstrb(con)
    if subp_flag is not None:
        df = df.join(subp_flag, on=SUBPLOT_ID, how="left").with_columns(
            pl.col("subp_has_dstrb").fill_null(False)
        )
    else:
        df = df.with_columns(pl.lit(False).alias("subp_has_dstrb"))

    df = df.join(build_plot_meas_num(df), on=PLOT_KEY, how="left")
    df = df.join(build_subp_meas_num(df), on=SUBPLOT_ID, how="left")
    df = add_plot_sim_year(df)

    # plt_cn is the real PLOT.CN (carried from load_base_data); no synthetic key.

    df = df.rename({
        "STATECD": "statecd", "UNITCD": "unitcd", "COUNTYCD": "countycd",
        "PLOT": "plot", "INVYR": "invyr", "SUBP": "subp", "TREE": "tree",
        "SPCD": "spcd", "MEASDATE": "measdate",
    }).select([
        "plt_cn",
        "statecd", "unitcd", "countycd", "plot", "invyr", "subp", "tree",
        "epa_l3", "epa_l4", "ecosubcd",
        "spcd", "species_symbol", "spgrpcd", "major_spgrpcd", "sftwd_hrdwd",
        "measdate", "sim_year", "plot_meas_num", "subp_meas_num", "subp_has_dstrb",
        "dia", "ht", "actualht", "cr", "cclcd", "tpa_unadj", "statuscd",
        "estimated_age",
        "drybio_ag", "carbon_ag", "agb",
        "site_slope", "site_aspect", "site_elev",
        "site_sicond", "site_sibase", "site_sisp",
        "site_fortypcd", "site_siteclcd",
        "lat", "lon",
    ])

    # Diagnostics
    n_rows  = df.height
    n_plots = df.select(PLOT_KEY_L := ["statecd", "unitcd", "countycd", "plot"]).unique().height
    n_long  = df.filter(pl.col("plot_meas_num") > 1).select(PLOT_KEY_L).unique().height
    print(f"\n[summary] {n_rows:,} tree-measurements · {n_plots:,} plots")
    print(f"          longitudinal plots (plot_meas_num>1): {n_long:,}")
    print(f"          species: {df['species_symbol'].n_unique():,} · "
          f"epa_l3: {df['epa_l3'].n_unique():,} · epa_l4: {df['epa_l4'].n_unique():,}")
    return df


def _sanity(out: pl.DataFrame, eco: str, eco_field: str = "epa_l4") -> None:
    """Print a quick sanity summary for one ecoregion (validation aid)."""
    print("\n" + "=" * 70)
    print(f"[sanity] {eco_field} == {eco!r}")
    sub = out.filter(pl.col(eco_field) == eco)
    if sub.height == 0:
        print("  no rows — check the eco code / eco_field")
        return
    pk = ["statecd", "unitcd", "countycd", "plot"]
    long_plots = sub.filter(pl.col("plot_meas_num") > 1).select(pk).unique()
    print(f"  rows={sub.height:,}  plots={sub.select(pk).unique().height:,}  "
          f"longitudinal plots={long_plots.height:,}  "
          f"species={sub['species_symbol'].n_unique():,}")
    if long_plots.height:
        p0 = long_plots.row(0, named=True)
        st = sub.filter(
            (pl.col("statecd") == p0["statecd"]) & (pl.col("unitcd") == p0["unitcd"]) &
            (pl.col("countycd") == p0["countycd"]) & (pl.col("plot") == p0["plot"])
        ).sort(["sim_year", "subp", "tree"])
        yrs = st.select(["sim_year", "measdate"]).unique().sort("sim_year")
        print(f"  sample plot {p0}:")
        print(f"    visits (sim_year, measdate): {yrs.rows()}")
        print("    first-visit tree records (subp, tree, spcd, dia, ht, cr, tpa, agb):")
        first = st.filter(pl.col("sim_year") == st["sim_year"].min())
        for r in first.head(12).iter_rows(named=True):
            print(f"      {r['subp']}/{r['tree']}  spcd={r['spcd']} dia={r['dia']} "
                  f"ht={r['ht']} cr={r['cr']} tpa={r['tpa_unadj']:.3f} agb={r['agb']:.1f}")


if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else "../FIASQLITE2PGSQL/FIADB.duckdb"
    eco_arg = sys.argv[2] if len(sys.argv) > 2 else None
    print(f"DB: {db_path}")
    con = duckdb.connect(db_path)
    out = build_trees_fvs(con)
    con.register("_trees_fvs_view", out)
    con.execute(
        "CREATE OR REPLACE TABLE curated_trees_fvs AS SELECT * FROM _trees_fvs_view"
    )
    con.unregister("_trees_fvs_view")
    con.commit()
    print(f"\n[save] curated_trees_fvs written ({out.height:,} rows)")
    if eco_arg:
        _sanity(out, eco_arg)
    con.close()
