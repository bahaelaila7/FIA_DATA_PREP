"""
cohorts_landis.py
Curates FIA data into the format expected by the Pan/LANDIS-II Biomass
Succession parametrization engine (Julia project).

Source tables  : curated_trees, curated_cohorts  (from the FIA curation pipeline)
Reference tables: data_plot_eco, REF_SPECIES, PLOT (FIA raw)
Output table   : curated_cohorts_landis

Schema:
  plt_cn                      VARCHAR  — composite plot identifier (for subplot counting)
  statecd/unitcd/countycd/plot/subp  INTEGER
  epa_l3, epa_l4, ecosubcd    VARCHAR  — switchable eco field; epa_l4/ecosubcd null if absent
  species_symbol              VARCHAR  — FIA species symbol (e.g. "ABBA")
  spgrpcd                     INTEGER  — FIA species group code (tree-level, region-resolved)
  major_spgrpcd               INTEGER  — REF_SPECIES.MAJOR_SPGRPCD (broad taxonomic group)
  sftwd_hrdwd                 VARCHAR  — REF_SPECIES.SFTWD_HRDWD: "S" softwood / "H" hardwood
  measdate                    DATE
  age_calc                    INTEGER  — estimated_age rounded to nearest year
  agb                         FLOAT    — DRYBIO_AG * TPA_UNADJ, converted lbs/acre → g/m²
  subp_has_dstrb              BOOL     — True if subplot ever had harvest or disturbance death
  plot_meas_num               INTEGER  — distinct measurement years for this plot

Biomass conversion: 1 lb/acre = 453.592/4046.86 ≈ 0.11208 g/m²

Species grouping (12/4/2 tiered bucketing) lives entirely in Julia's
assign_tiered_species!(). This table exposes species_symbol, spgrpcd,
major_spgrpcd, and sftwd_hrdwd so Julia can compute effective_species on
whatever subset it loads after its own SQL filters.

Standalone usage:
    python cohorts_landis.py [path/to/fiadb.duckdb]
    # default: ../FIASQLITE2PGSQL/FIADB.duckdb
"""

import os as _os, sys as _sys  # curation-fix: sibling modules resolve under `python -I`
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
import sys

import duckdb
import polars as pl
from fia_ids import pick, maybe_shuffle

PLOT_KEY   = ["STATECD", "UNITCD", "COUNTYCD", "PLOT"]
SUBPLOT_ID = ["STATECD", "UNITCD", "COUNTYCD", "PLOT", "SUBP"]
TREE_ID    = ["STATECD", "UNITCD", "COUNTYCD", "PLOT", "SUBP", "TREE"]

# 1 lb/acre → g/m²  (453.592 g/lb ÷ 4046.86 m²/acre)
LBS_ACRE_TO_G_M2 = 453.592 / 4046.86   # ≈ 0.11208

# Phantom backfill thresholds
MIN_PHANTOM_AGE   = 5    # skip trees whose expected age at first meas < this (noise)
MIN_BACKFILL_AGB  = 2.0  # g/m² floor — don't generate rows below this biomass
MAX_SAPLING_AGE   = 30   # age at first FIA appearance to qualify as training sapling
MIN_RATE_TREES    = 10   # min trees for species×epa_l3 rate; else fall back to spgrpcd


# =============================================================================
# Helpers
# =============================================================================

def _has_column(con, table: str, col: str) -> bool:
    import dbconn
    return col.lower() in dbconn.column_names(con, table)


def _has_table(con, table: str) -> bool:
    import dbconn
    return dbconn.table_exists(con, table)


# =============================================================================
# Step 1: live tree records + eco + species metadata
# =============================================================================

AGE_META = ["n_trees_natural_treeage_scale", "share_natural_treeage_scale", "age_source_major",
            "n_trees_known_bias", "share_known_bias"]
MANAGED = ["land_use", "cycle", *AGE_META, "procedural_break", "land_use_4way", "all_live_withdrawn_rc7"]


def attach_land_use_and_cycle(con) -> None:
    """Rebuild curated_cohorts_landis with (SQL join, no polars rebuild needed):
      - `land_use` (r7 D44 / C F10): the 2-way STDORGCD label, the same rule as _stdorg / Sim B /
        tree_trajectories (cohorts_landis.plot_land_use_sql; r6 carried the 4-way label here);
      - `cycle`: the FIA inventory cycle, per (plot, invyr) from base_subplot_cond;
      - `land_use_4way` (r7): the DERIVED management land use (reserved / plantation / managed /
        natural; build_tree_trajectories.derive_land_use, per subplot there), as the per-PLOT
        dominant (most frequent over trajectory rows, ties -> alphabetical) value, so it is a
        stable plot attribute;
      - the visit metadata procedural_break and all_live_withdrawn_rc7 (cohorts_landis.add_visit_flags).
    Requires tree_trajectories (land_use_4way) + base_subplot_cond (CYCLE) + COND."""
    import dbconn
    from cohorts_landis import add_visit_flags, plot_land_use_sql
    have = [r[0] for r in con.execute(
        "SELECT column_name FROM duckdb_columns() WHERE database_name = current_database() "
        "AND table_name = 'curated_cohorts_landis' ORDER BY column_index").fetchall()]
    miss = [m for m in AGE_META if m not in have]
    if miss:
        raise SystemExit(f"curated_cohorts_landis lacks {miss}: rerun step 4 (cohorts_landis.py)")
    base = ", ".join(f"c.{x}" for x in have if x.lower() not in {m.lower() for m in MANAGED})
    con.execute(f"""
    CREATE OR REPLACE TABLE curated_cohorts_landis AS
    WITH plot_lu AS ({plot_land_use_sql(con)}),
    plot_lu4 AS (
        SELECT statecd, unitcd, countycd, plot, land_use_4way FROM (
            SELECT statecd, unitcd, countycd, plot, land_use_4way,
                   ROW_NUMBER() OVER (PARTITION BY statecd, unitcd, countycd, plot
                                      ORDER BY COUNT(*) DESC, land_use_4way) AS rn
            FROM tree_trajectories
            WHERE land_use_4way IS NOT NULL
            GROUP BY statecd, unitcd, countycd, plot, land_use_4way
        ) WHERE rn = 1
    ),
    plot_cycle AS (
        SELECT STATECD AS statecd, UNITCD AS unitcd, COUNTYCD AS countycd, PLOT AS plot,
               INVYR AS invyr, MAX(CYCLE) AS cycle
        FROM base_subplot_cond
        GROUP BY STATECD, UNITCD, COUNTYCD, PLOT, INVYR
    )
    SELECT {base},
           lu.land_use, cy.cycle,
           -- r3 I4: appended AFTER the old columns (old schema stays an exact prefix)
           {", ".join(f"c.{m}" for m in AGE_META)},
           {"c.procedural_break" if "procedural_break" in have else "NULL::BOOLEAN AS procedural_break"},
           lu4.land_use_4way,
           {"c.all_live_withdrawn_rc7" if "all_live_withdrawn_rc7" in have else "NULL::BOOLEAN AS all_live_withdrawn_rc7"}
    FROM curated_cohorts_landis c
    LEFT JOIN plot_lu lu
        ON c.statecd=lu.statecd AND c.unitcd=lu.unitcd AND c.countycd=lu.countycd AND c.plot=lu.plot
    LEFT JOIN plot_lu4 lu4
        ON c.statecd=lu4.statecd AND c.unitcd=lu4.unitcd AND c.countycd=lu4.countycd AND c.plot=lu4.plot
    LEFT JOIN plot_cycle cy
        ON c.statecd=cy.statecd AND c.unitcd=cy.unitcd AND c.countycd=cy.countycd
        AND c.plot=cy.plot AND c.invyr=cy.invyr
    """)
    add_visit_flags(con, "curated_cohorts_landis")   # fills the two visit-flag columns in place
    # D69: invyr stays the last column, so the old schema remains an exact prefix
    con.execute("CREATE OR REPLACE TABLE curated_cohorts_landis AS SELECT * EXCLUDE (invyr), invyr FROM curated_cohorts_landis")
    con.commit()
    tot, n_lu, n_lu4, n_cy = con.execute(
        "SELECT COUNT(*), COUNT(land_use), COUNT(land_use_4way), COUNT(cycle) FROM curated_cohorts_landis").fetchone()
    print(f"[land_use+cycle] land_use (2-way) non-null {n_lu:,}/{tot:,}  ·  land_use_4way non-null "
          f"{n_lu4:,}/{tot:,}  ·  cycle non-null {n_cy:,}/{tot:,}")


# =============================================================================
# Standalone entry point
# =============================================================================

if __name__ == "__main__":
    # curation-fix 2026-10-26: explicit --out-db/--fiadb (dbconn). The production table is
    # built by cohorts_landis.py (v1: sim_year + subp_meas_num); this script is used with
    # --attach-only to add land_use + cycle.
    # r7 D44 (C F9): the v2 full build was stale and refused to run; D69 removed its code (see git
    # history), so this script only attaches.
    import argparse
    import dbconn
    ap = dbconn.add_db_args(argparse.ArgumentParser(description="cohorts_landis2"))
    ap.add_argument("--attach-only", action="store_true",
                    help="skip the rebuild; only add land_use + cycle (the only supported mode)")
    a = ap.parse_args()
    dbconn.check_args(a)
    if not a.attach_only:
        sys.exit("cohorts_landis2.py: the full rebuild was removed; build curated_cohorts_landis with "
                 "cohorts_landis.py, then use --attach-only")
    con = dbconn.connect(a.out_db, a.fiadb)
    print(f"DB: {a.out_db}")

    dbconn.require_tables(con, "curated_cohorts_landis", "tree_trajectories", "base_subplot_cond", "COND", "product_visits")
    attach_land_use_and_cycle(con)

    con.close()
    print("[done]")
