"""
cohort_disturbance.py
=====================
Compute a per-cohort effective biomass-drop percentage from PARTIAL disturbance,
for the Pan override-sync (so growth params fit the undisturbed trajectory and the
disturbance is applied as a known shock instead of biasing growth/decay).

Idea (per user spec):
  A cohort = (subplot, species, birth_sim_year = sim_year - age_calc) — Pan's cohort identity.
  At each measurement T (subplot visit), some member trees are removed (cut / disturbance death)
  (r6 D25: the former alive-but-damaged DAMSEV>40 branch is removed). For each DISTURBED tree we estimate the
  biomass it WOULD have had at T by scaling its last-alive biomass by the growth factor of the
  cohort's SURVIVING trees between the previous visit and T (fallback: carry-forward). The cohort
  drop% = dropped / would-have-been.

Only trees ALREADY FIA-marked as disturbed count (terminal_fate in {dead_disturbance, harvest_*,
disappeared_inferred_removal}). NATURAL deaths are excluded (the
growth/longevity model should handle those). Two severity gates (both 40%, testable):
  - per-tree gate: an alive-but-damaged tree counts only if its shortfall vs would-have-been > SEVERITY_FRAC
  - cohort gate (optional): record the cohort drop only if it exceeds COHORT_SEVERITY_FRAC

Biomass is area-weighted per tree as drybio_g*tpa (the lbs/acre->g/m2 constant cancels in the ratio).

Output: table `cohort_disturbance` keyed (statecd,unitcd,countycd,plot,subp,species_symbol,measdate,age_calc)
to LEFT JOIN onto curated_cohorts_landis. `--attach` adds disturbance_drop_pct (fill 0) into it.

Usage:
  python cohort_disturbance.py <db> [STATECD ...]            # compute + diagnostics (no table change)
  python cohort_disturbance.py <db> [STATECD ...] --write    # also write the cohort_disturbance table
  python cohort_disturbance.py <db> [STATECD ...] --attach   # + attach disturbance_drop_pct into curated_cohorts_landis
"""
import os as _os, sys as _sys  # curation-fix: sibling modules resolve under `python -I`
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
import sys
import duckdb
import polars as pl

SUBPLOT_ID = ["statecd", "unitcd", "countycd", "plot", "subp"]
COHORT_ID  = SUBPLOT_ID + ["species_symbol", "birth_sim_year"]

SEVERITY_FRAC        = 0.40   # per-tree: alive-but-damaged tree counts only if shortfall/would-be > this
COHORT_SEVERITY_FRAC = 0.40   # cohort-level gate
APPLY_COHORT_GATE    = False  # toggle the cohort-level gate (test both)
GF_FLOOR, GF_CEIL    = 0.5, 3.0  # clamp survivor growth factor to sane bounds

DISTURB_FATES = ["dead_disturbance", "harvest_explicit", "harvest_inferred", "disappeared_inferred_removal"]
NATURAL_FATES = ["dead_natural"]


def load(con, states):
    ws = f"AND tt.statecd IN ({','.join(map(str, states))})" if states else ""
    df = con.execute(f"""
        SELECT tt.statecd, tt.unitcd, tt.countycd, tt.plot, tt.subp, tt.tree,
               tt.measdate, tt.sim_year, tt.age_calc, tt.species_symbol,
               tt.drybio_g, tt.tpa, tt.intro_type, tt.is_terminal, tt.dia,
               tt.terminal_fate, tt.death_cause,
               ct.STATUSCD AS statuscd, ct.DAMSEV1 AS damsev1, ct.DAMSEV2 AS damsev2
        FROM tree_trajectories tt
        LEFT JOIN curated_trees ct
          ON ct.STATECD=tt.statecd AND ct.UNITCD=tt.unitcd AND ct.COUNTYCD=tt.countycd
          AND ct.PLOT=tt.plot AND ct.SUBP=tt.subp AND ct.TREE=tt.tree AND ct.MEASDATE=tt.measdate
          AND NOT COALESCE(ct.misrecorded_death, FALSE)   -- r6 D26: never a status source
        WHERE tt.drybio_g IS NOT NULL AND tt.tpa IS NOT NULL {ws}
    """).pl()
    print(f"[load] {df.height:,} trajectory rows  ({df.select(SUBPLOT_ID).unique().height:,} subplots)")
    return df


def compute(df, timber_only: bool = False):
    """timber_only (r5 I-1, Sim B): Sim B holds only REAL trees with DIA >= 5 in, so its drops
    are computed from those members only: a pair (tree, previous visit -> visit) counts only if
    the tree is a real (not phantom / gap-interpolated) record with DIA >= 5 at the previous
    visit, and, when alive at the visit, a real record with DIA >= 5 there too. Saplings that
    die or are cut, and trees that grow past 5 in between the visits, no longer put drops on
    surviving timber cohorts."""
    df = df.with_columns([
        (pl.col("drybio_g") * pl.col("tpa")).alias("w"),
        (pl.col("sim_year") - pl.col("age_calc")).alias("birth_sim_year"),
        (   # r6: phantom / gap-interpolated slots are alive by construction
            (pl.col("statuscd") == 1)
            | pl.col("intro_type").is_in(["phantom_backfill", "gap_interpolated"])
        ).fill_null(False).alias("alive"),
        pl.lit(False).alias("severe_dmg"),   # r6 D25: no damage branch (DAMSEV is a 0-9 class code)
    ])

    # dense per-subplot visit rank (the measurement time axis)
    visits = (
        df.select(SUBPLOT_ID + ["measdate", "sim_year"]).unique()
        .sort(SUBPLOT_ID + ["measdate"])
        .with_columns(pl.col("measdate").rank("dense").over(SUBPLOT_ID).cast(pl.Int32).alias("vrank"))
    )
    df = df.join(visits.select(SUBPLOT_ID + ["measdate", "vrank"]), on=SUBPLOT_ID + ["measdate"], how="left")

    # one row per alive tree-visit
    alive = df.filter(pl.col("alive")).with_columns(
        ((pl.col("intro_type") == "real") & (pl.col("dia") >= 5)).fill_null(False).alias("_timber"))
    alive_all = alive
    if timber_only:
        alive = alive.filter(pl.col("_timber"))
    alive = alive.select(COHORT_ID + ["tree", "vrank", "w", "severe_dmg", "terminal_fate"])

    # PAIR consecutive visits: a tree alive at vrank_prev, its state at vrank (= vrank_prev+1)
    prev = (
        alive.select(COHORT_ID + ["tree", "vrank", "w", "terminal_fate"])
        .rename({"vrank": "vrank_prev", "w": "w_prev"})
        .with_columns((pl.col("vrank_prev") + 1).alias("vrank"))
    )
    cur = (
        alive.select(COHORT_ID + ["tree", "vrank", "w", "severe_dmg"])
        .rename({"w": "w_cur", "severe_dmg": "dmg_cur"})
    )
    pair = prev.join(cur, on=COHORT_ID + ["tree", "vrank"], how="left")
    if timber_only:
        # a tree still ALIVE at the visit but not a real >= 5 in record there (shrank below 5 in,
        # or only a gap-interpolated slot) is neither a survivor nor a removal: drop the pair
        nont = (alive_all.filter(~pl.col("_timber")).select(COHORT_ID + ["tree", "vrank"])
                .unique().with_columns(pl.lit(True).alias("_nont")))
        pair = (pair.join(nont, on=COHORT_ID + ["tree", "vrank"], how="left")
                    .filter(pl.col("_nont").is_null()).drop("_nont"))

    # classify each tree at the target visit vrank
    disturbed_removal = pl.col("w_cur").is_null() & pl.col("terminal_fate").is_in(DISTURB_FATES)
    natural_removal   = pl.col("w_cur").is_null() & pl.col("terminal_fate").is_in(NATURAL_FATES)
    other_gone        = pl.col("w_cur").is_null() & ~(disturbed_removal | natural_removal)  # alive_censored etc.
    survivor          = pl.col("w_cur").is_not_null()          # r6 D25: every live tree survives
    damaged_alive     = pl.lit(False)
    pair = pair.with_columns([
        disturbed_removal.alias("is_disturbed_removal"),
        natural_removal.alias("is_natural_removal"),
        other_gone.alias("is_other_gone"),
        survivor.alias("is_survivor"),
        damaged_alive.alias("is_damaged_alive"),
    ])

    # survivor growth factor per (cohort, vrank)
    surv = (
        pair.filter(pl.col("is_survivor"))
        .group_by(COHORT_ID + ["vrank"])
        .agg([pl.col("w_prev").sum().alias("surv_w_prev"), pl.col("w_cur").sum().alias("surv_w_cur")])
        .with_columns(
            pl.when(pl.col("surv_w_prev") > 0)
            .then((pl.col("surv_w_cur") / pl.col("surv_w_prev")).clip(GF_FLOOR, GF_CEIL))
            .otherwise(1.0)            # fallback: carry-forward when no surviving cohort-mates
            .alias("gf")
        )
    )
    pair = pair.join(surv.select(COHORT_ID + ["vrank", "gf", "surv_w_cur"]), on=COHORT_ID + ["vrank"], how="left")
    pair = pair.with_columns(pl.col("gf").fill_null(1.0).alias("gf"))

    # would-have-been biomass + dropped biomass per disturbed tree
    pair = pair.with_columns((pl.col("w_prev") * pl.col("gf")).alias("w_exp"))
    pair = pair.with_columns([
        # r6 R3 (D25): drops come from disturbance REMOVALS only. The old damaged-alive branch
        # (DAMSEV1/2 > 40) could never fire: DAMSEV is a 0-9 class code (10 % classes, recorded
        # only under MANUAL 1.0-1.6; R4 max 9), and FIA does not quantify biomass lost to damage.
        pl.col("is_disturbed_removal").alias("counted"),
    ])
    pair = pair.with_columns([
        pl.when(pl.col("counted")).then(pl.col("w_exp")).otherwise(0.0).alias("counted_would_be"),
        pl.when(pl.col("counted"))
          .then(pl.col("w_exp") - pl.col("w_cur").fill_null(0.0)).otherwise(0.0)
          .clip(0.0, None).alias("counted_dropped"),
        # non-counted live trees contribute their observed w_cur to would-be (survivors + uncounted damaged)
        pl.when(pl.col("w_cur").is_not_null() & ~pl.col("counted")).then(pl.col("w_cur")).otherwise(0.0)
          .alias("noncounted_live_w"),
    ])

    coh = (
        pair.group_by(COHORT_ID + ["vrank"])
        .agg([
            pl.col("counted_would_be").sum().alias("would_be_disturbed"),
            pl.col("counted_dropped").sum().alias("dropped"),
            pl.col("noncounted_live_w").sum().alias("live_w"),
            pl.col("counted").sum().alias("n_disturbed"),
            pl.col("is_survivor").sum().alias("n_survivors"),
        ])
        .with_columns((pl.col("would_be_disturbed") + pl.col("live_w")).alias("would_be"))
        .filter(pl.col("dropped") > 0)
        # r5 M-3: float sums depend on the summation order -> round for bit-identical reruns
        .with_columns(pl.col("would_be_disturbed").round(6), pl.col("dropped").round(6),
                      pl.col("live_w").round(6), pl.col("would_be").round(6))
        .with_columns((pl.col("dropped") / pl.col("would_be")).round(9).alias("disturbance_drop_pct"))
    )
    # Only PARTIAL disturbances are in scope: a surviving cohort remains at T to scale. Full-cohort
    # removals (n_survivors==0) have no live row in curated_cohorts_landis and are already handled by
    # the sync's set-removal of unobserved cohorts.
    n_full = coh.filter(pl.col("n_survivors") == 0).height
    coh = coh.filter(pl.col("n_survivors") > 0)
    print(f"[scope] {n_full:,} full-cohort removals dropped (handled by sync); "
          f"{coh.height:,} partial-disturbance cohort-measurements kept")
    if APPLY_COHORT_GATE:
        coh = coh.filter(pl.col("disturbance_drop_pct") >= COHORT_SEVERITY_FRAC)

    # map (cohort, vrank) -> the measurement key for curated_cohorts_landis
    vr = visits.select(SUBPLOT_ID + ["measdate", "sim_year", "vrank"])
    coh = (
        coh.join(vr, on=SUBPLOT_ID + ["vrank"], how="left")
        .with_columns((pl.col("sim_year") - pl.col("birth_sim_year")).cast(pl.Int32).alias("age_calc"))
        .select(SUBPLOT_ID + ["species_symbol", "measdate", "age_calc",
                              "disturbance_drop_pct", "dropped", "would_be", "n_disturbed", "n_survivors"])
        .sort(SUBPLOT_ID + ["species_symbol", "measdate", "age_calc"])
    )
    return coh


def diagnostics(coh):
    n = coh.height
    print(f"\n[result] {n:,} disturbed cohort-measurements")
    if n == 0:
        return
    p = coh["disturbance_drop_pct"]
    print(f"  drop_pct: min={p.min():.3f}  p25={p.quantile(.25):.3f}  p50={p.quantile(.5):.3f}  "
          f"p75={p.quantile(.75):.3f}  p90={p.quantile(.9):.3f}  max={p.max():.3f}")
    for lo, hi in [(0.0, .1), (.1, .3), (.3, .5), (.5, .8), (.8, 1.01)]:
        c = coh.filter((p >= lo) & (p < hi)).height
        print(f"    {lo:.1f}–{hi:.1f}: {c:,}")
    print(f"  cohorts ≥40% drop: {coh.filter(p >= 0.40).height:,}")
    print(f"  no-survivor (carry-forward) cohorts: {coh.filter(pl.col('n_survivors') == 0).height:,}")


if __name__ == "__main__":
    # curation-fix 2026-10-26: explicit --out-db/--fiadb (dbconn), no default DB path.
    # Censored trees (terminal_fate censored_*) are not in DISTURB_FATES, so a STATUSCD=0
    # procedural/nonsampled exit is never counted as a disturbance drop.
    import argparse
    import dbconn
    ap = dbconn.add_db_args(argparse.ArgumentParser(description="cohort_disturbance"))
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--attach", action="store_true")
    a = ap.parse_args()
    dbconn.check_args(a)
    write, attach = a.write or a.attach, a.attach
    states = dbconn.parse_states(a.states) or []
    print(f"DB: {a.out_db}  states: {states or 'ALL'}  severity={SEVERITY_FRAC}  cohort_gate={APPLY_COHORT_GATE}")
    con = dbconn.connect(a.out_db, a.fiadb)
    dbconn.require_tables(con, "tree_trajectories", "curated_trees")
    coh = compute(load(con, states))
    diagnostics(coh)
    if write:
        con.register("_cd", coh.to_pandas())
        con.execute("CREATE OR REPLACE TABLE cohort_disturbance AS SELECT * FROM _cd")
        con.unregister("_cd"); con.commit()
        print(f"\n[write] cohort_disturbance ← {coh.height:,} rows")
    if attach:
        has_col = "disturbance_drop_pct" in dbconn.column_names(con, "curated_cohorts_landis")  # not information_schema (M6)
        keep = "c.* EXCLUDE (disturbance_drop_pct)" if has_col else "c.*"   # idempotent re-attach
        con.execute(f"""
            CREATE OR REPLACE TABLE curated_cohorts_landis AS
            SELECT {keep}, COALESCE(d.disturbance_drop_pct, 0.0) AS disturbance_drop_pct
            FROM curated_cohorts_landis c
            LEFT JOIN cohort_disturbance d
              ON c.statecd=d.statecd AND c.unitcd=d.unitcd AND c.countycd=d.countycd AND c.plot=d.plot
              AND c.subp=d.subp AND c.species_symbol=d.species_symbol AND c.measdate=d.measdate AND c.age_calc=d.age_calc
        """)
        con.commit()
        tot = con.execute("SELECT COUNT(*) FROM curated_cohorts_landis").fetchone()[0]
        nz = con.execute("SELECT COUNT(*) FROM curated_cohorts_landis WHERE disturbance_drop_pct>0").fetchone()[0]
        print(f"[attach] disturbance_drop_pct: {nz:,}/{tot:,} rows >0")
    con.close()
    print("[done]")
