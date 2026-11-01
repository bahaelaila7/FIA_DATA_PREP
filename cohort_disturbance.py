"""
cohort_disturbance.py
=====================
Compute a per-cohort effective biomass-drop percentage from disturbance REMOVALS, for the Pan
override-sync (so growth params fit the undisturbed trajectory and the disturbance is applied as
a known shock instead of biasing growth/decay).

Idea (per user spec):
  A cohort = (subplot, species, birth_sim_year = sim_year - age_calc) — Pan's cohort identity.
  At each measurement T (subplot visit), some member trees are removed (cut / disturbance death /
  inferred removal). For each REMOVED tree we estimate the biomass it WOULD have had at T by
  scaling its last-alive biomass by the growth factor of the cohort's SURVIVING trees between the
  previous visit and T (fallback: carry-forward). The cohort drop% = dropped / would-have-been.
  (r6 D25: there is no alive-but-damaged branch; DAMSEV is a 0-9 class code.)

r7 D41: a removal counts only AT ITS EVENT VISIT. The events come from
cohorts_landis.disturbance_events — the ONE disturbance definition shared with subp_has_dstrb,
curated_cohorts and the trajectories' terminal_fate: the tree's first dead / cut record that is a
cut (silvicultural_death / harvest_inferred) or a disturbance death (AGENTCD 10-50 or a DSTRBCD other
than 0 / 60), or an inferred removal (disappeared_inferred_removal, at the first scaffold visit
after its last record). NATURAL deaths, censoring, diversions and misrecorded deaths never count.
r7 D38: a removal whose event record is first_record_dead (a first-seen death) is not counted
either.

r7 D42: FULL removals are emitted too: a cohort-visit with counted removals and no member left
alive (no survivor and no member alive in any form) gets a row with full_removal = TRUE and
disturbance_drop_pct = 1.0 (Sim B has no row at that key: the cohort is gone). Partial rows have
full_removal = FALSE. Optional cohort gate (APPLY_COHORT_GATE, off) is unchanged.

Biomass is area-weighted per tree as drybio_g*tpa (the lbs/acre->g/m2 constant cancels in the ratio).

Output: table `cohort_disturbance` keyed (statecd,unitcd,countycd,plot,subp,species_symbol,measdate,age_calc),
written ONLY by attach_disturbance_simB.py (timber-only Sim B drops, D31 semantics) into the Sim B DB.

Usage (diagnostics only; r7 D44 / C F9: the old --write / --attach paths wrote the pre-D19 / pre-D31,
non-timber drop into the curated DB and refuse to run):
  python cohort_disturbance.py --out-db <curated.duckdb> --fiadb <FIADB> [--states ...]
"""
import os as _os, sys as _sys  # curation-fix: sibling modules resolve under `python -I`
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
import sys
import duckdb
import polars as pl

SUBPLOT_ID = ["statecd", "unitcd", "countycd", "plot", "subp"]
COHORT_ID  = SUBPLOT_ID + ["species_symbol", "birth_sim_year"]

COHORT_SEVERITY_FRAC = 0.40   # cohort-level gate
APPLY_COHORT_GATE    = False  # toggle the cohort-level gate (test both)
GF_FLOOR, GF_CEIL    = 0.5, 3.0  # clamp survivor growth factor to sane bounds

DISTURB_FATES = ["dead_disturbance", "silvicultural_death", "harvest_inferred", "disappeared_inferred_removal"]
NATURAL_FATES = ["dead_natural"]   # diagnostics only: removals count from the D41 events (load)


def load(con, states):
    """Trajectory rows + the tree's D41 removal event (cohorts_landis.disturbance_events):
    removal_measdate = the event visit, removal_counted = the event counts for drops (not a
    first_record_dead record, D38)."""
    import cohorts_landis as c1
    ws = f"AND tt.statecd IN ({','.join(map(str, states))})" if states else ""
    df = con.execute(f"""
        SELECT tt.statecd, tt.unitcd, tt.countycd, tt.plot, tt.subp, tt.tree,
               tt.measdate, tt.sim_year, tt.age_calc, tt.species_symbol,
               tt.drybio_g, tt.tpa, tt.intro_type, tt.is_terminal, tt.dia,
               tt.terminal_fate, tt.death_cause,
               ct.STATUSCD AS statuscd
        FROM tree_trajectories tt
        LEFT JOIN curated_trees ct
          ON ct.STATECD=tt.statecd AND ct.UNITCD=tt.unitcd AND ct.COUNTYCD=tt.countycd
          AND ct.PLOT=tt.plot AND ct.SUBP=tt.subp AND ct.TREE=tt.tree AND ct.MEASDATE=tt.measdate
          AND NOT COALESCE(ct.misrecorded_death, FALSE)   -- r6 D26: never a status source
        WHERE tt.drybio_g IS NOT NULL AND tt.tpa IS NOT NULL {ws}
    """).pl()
    ev = (c1.disturbance_events(con, states or None)
          .rename({k: k.lower() for k in c1.TREE_ID})
          .select(["statecd", "unitcd", "countycd", "plot", "subp", "tree",
                   pl.col("event_measdate").alias("removal_measdate"),
                   (~pl.col("first_record_dead")).alias("removal_counted"),
                   pl.col("event_kind").alias("removal_kind")]))
    keys = ["statecd", "unitcd", "countycd", "plot", "subp", "tree"]
    df = (df.join(ev.with_columns([pl.col(k).cast(df.schema[k]) for k in keys]), on=keys, how="left")
            .with_columns(pl.col("removal_counted").fill_null(False)))
    print(f"[load] {df.height:,} trajectory rows  ({df.select(SUBPLOT_ID).unique().height:,} subplots); "
          f"{ev.height:,} D41 removal events ({ev.filter(~pl.col('removal_counted')).height:,} first_record_dead, not counted)")
    return df


def load_scaffold(con, states):
    """r7 D42: the subplot visit axis of the FULL scaffold (clean_subplot_years) with the shared
    strict sim_year (cohorts_landis.sim_year_map, the axis of Sim A / Sim B / the trajectories).
    The trajectory rows alone miss a visit at which no tree has a slot (a clear-cut subplot whose
    cut records carry no TPA): its full removals were never paired. Lower-case keys + measdate,
    sim_year."""
    import cohorts_landis as c1
    import dbconn
    ws = dbconn.state_filter(states or None, col="STATECD", prefix="WHERE")
    v = con.execute(f"SELECT DISTINCT STATECD, UNITCD, COUNTYCD, PLOT, SUBP, MEASDATE FROM clean_subplot_years {ws}").pl()
    return c1.sim_year_map(v).rename({k: k.lower() for k in c1.SUBPLOT_ID + ["MEASDATE"]})


def compute(df, timber_only: bool = False, scaffold=None):
    """timber_only (r5 I-1, Sim B): Sim B holds only REAL trees with DIA >= 5 in, so its drops
    are computed from those members only: a pair (tree, previous visit -> visit) counts only if
    the tree is a real (not phantom / gap-interpolated) record with DIA >= 5 at the previous
    visit, and, when alive at the visit, a real record with DIA >= 5 there too. Saplings that
    die or are cut, and trees that grow past 5 in between the visits, no longer put drops on
    surviving timber cohorts.

    `df` must carry the D41 removal columns of load(): removal_measdate (the tree's event visit)
    and removal_counted (False for a first_record_dead event, D38). A tree gone at the visit is a
    COUNTED removal only when that visit IS its removal event (r7 D41; before, any tree whose
    terminal_fate was a disturbance fate counted at any pair where it was absent, C F7).

    `scaffold` (load_scaffold, r7 D42): the full subplot visit axis; the visits are then ranked over
    it, so a visit with no trajectory row (every tree removed) still follows its predecessor.
    None (the fixtures): the visits of df."""
    miss = {"removal_measdate", "removal_counted"} - set(df.columns)
    if miss:
        raise ValueError(f"cohort_disturbance.compute: df lacks the D41 removal columns {sorted(miss)} "
                         f"(load() joins them from cohorts_landis.disturbance_events)")
    df = df.with_columns([
        (pl.col("drybio_g") * pl.col("tpa")).alias("w"),
        (pl.col("sim_year") - pl.col("age_calc")).alias("birth_sim_year"),
        (   # r6: phantom / gap-interpolated slots are alive by construction
            (pl.col("statuscd") == 1)
            | pl.col("intro_type").is_in(["phantom_backfill", "gap_interpolated"])
        ).fill_null(False).alias("alive"),
    ])

    # dense per-subplot visit rank (the measurement time axis)
    visits = df.select(SUBPLOT_ID + ["measdate", "sim_year"]).unique()
    if scaffold is not None:
        sc = (scaffold.with_columns([pl.col(k).cast(df.schema[k]) for k in SUBPLOT_ID + ["measdate", "sim_year"]])
              .join(visits.select(SUBPLOT_ID).unique(), on=SUBPLOT_ID, how="semi"))
        both = visits.join(sc, on=SUBPLOT_ID + ["measdate"], how="inner", suffix="_sc")
        nbad = both.filter(pl.col("sim_year") != pl.col("sim_year_sc")).height
        if nbad:
            raise ValueError(f"cohort_disturbance.compute: {nbad} trajectory visits disagree with the scaffold sim_year")
        n0 = visits.height
        visits = pl.concat([visits, sc.select(visits.columns).join(visits, on=SUBPLOT_ID + ["measdate"], how="anti")])
        print(f"[visits] {visits.height - n0:,} scaffold visits without a trajectory row added to the visit axis (D42)")
    visits = (
        visits
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
    rem = df.select(COHORT_ID[:5] + ["tree", "removal_measdate", "removal_counted", "terminal_fate"]).unique()
    if rem.select(SUBPLOT_ID + ["tree"]).is_duplicated().any():
        raise ValueError("a tree carries two removal states")

    # PAIR consecutive visits: a tree alive at vrank_prev, its state at vrank (= vrank_prev+1)
    prev = (
        alive.select(COHORT_ID + ["tree", "vrank", "w"])
        .rename({"vrank": "vrank_prev", "w": "w_prev"})
        .with_columns((pl.col("vrank_prev") + 1).alias("vrank"))
        .join(rem, on=SUBPLOT_ID + ["tree"], how="left")
        .join(visits.select(SUBPLOT_ID + ["vrank", pl.col("measdate").alias("_vmd")]),
              on=SUBPLOT_ID + ["vrank"], how="left")
    )
    cur = alive.select(COHORT_ID + ["tree", "vrank", "w"]).rename({"w": "w_cur"})
    pair = prev.join(cur, on=COHORT_ID + ["tree", "vrank"], how="left")
    if timber_only:
        # a tree still ALIVE at the visit but not a real >= 5 in record there (shrank below 5 in,
        # or only a gap-interpolated slot) is neither a survivor nor a removal: drop the pair
        nont = (alive_all.filter(~pl.col("_timber")).select(COHORT_ID + ["tree", "vrank"])
                .unique().with_columns(pl.lit(True).alias("_nont")))
        pair = (pair.join(nont, on=COHORT_ID + ["tree", "vrank"], how="left")
                    .filter(pl.col("_nont").is_null()).drop("_nont"))

    # classify each tree at the target visit vrank
    at_event = (pl.col("removal_measdate") == pl.col("_vmd")).fill_null(False)
    disturbed_removal = pl.col("w_cur").is_null() & at_event & pl.col("removal_counted").fill_null(False)
    natural_removal   = pl.col("w_cur").is_null() & ~disturbed_removal & pl.col("terminal_fate").is_in(NATURAL_FATES)
    other_gone        = pl.col("w_cur").is_null() & ~(disturbed_removal | natural_removal)  # censored, first-seen, ...
    survivor          = pl.col("w_cur").is_not_null()          # r6 D25: every live tree survives
    pair = pair.with_columns([
        disturbed_removal.alias("is_disturbed_removal"),
        natural_removal.alias("is_natural_removal"),
        other_gone.alias("is_other_gone"),
        survivor.alias("is_survivor"),
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
        # r6 R3 (D25): drops come from disturbance REMOVALS only (no damaged-alive branch)
        pl.col("is_disturbed_removal").alias("counted"),
    ])
    pair = pair.with_columns([
        pl.when(pl.col("counted")).then(pl.col("w_exp")).otherwise(0.0).alias("counted_would_be"),
        pl.when(pl.col("counted"))
          .then(pl.col("w_exp") - pl.col("w_cur").fill_null(0.0)).otherwise(0.0)
          .clip(0.0, None).alias("counted_dropped"),
        # survivors contribute their observed w_cur to would-be
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
    # r7 D42: FULL removals (no survivor) are emitted with full_removal = TRUE and drop 1.0, but
    # only when no member of the cohort is alive at the visit in ANY form; a cohort whose only
    # remaining members are alive below 5 in / as gap slots (timber_only) is not a removal of the
    # cohort and is still left out (counted in the log)
    any_alive = (alive_all.select(COHORT_ID + ["vrank"]).unique()
                 .with_columns(pl.lit(True).alias("_any_alive")))
    coh = coh.join(any_alive, on=COHORT_ID + ["vrank"], how="left")
    no_surv = pl.col("n_survivors") == 0
    n_left = coh.filter(no_surv & pl.col("_any_alive").fill_null(False)).height
    coh = (coh.filter(~(no_surv & pl.col("_any_alive").fill_null(False)))
              .with_columns(no_surv.alias("full_removal"))
              .with_columns(pl.when(pl.col("full_removal")).then(pl.lit(1.0))
                            .otherwise(pl.col("disturbance_drop_pct")).alias("disturbance_drop_pct"))
              .drop("_any_alive"))
    n_full = coh.filter(pl.col("full_removal")).height
    print(f"[scope] {coh.height - n_full:,} partial-disturbance cohort-measurements, {n_full:,} FULL removals "
          f"(full_removal, drop 1.0; D42); {n_left:,} no-survivor cohort-measurements left out "
          f"(members still alive below 5 in / as gap slots)")
    if APPLY_COHORT_GATE:
        coh = coh.filter(pl.col("disturbance_drop_pct") >= COHORT_SEVERITY_FRAC)

    # map (cohort, vrank) -> the measurement key for curated_cohorts_landis
    vr = visits.select(SUBPLOT_ID + ["measdate", "sim_year", "vrank"])
    coh = (
        coh.join(vr, on=SUBPLOT_ID + ["vrank"], how="left")
        .with_columns((pl.col("sim_year") - pl.col("birth_sim_year")).cast(pl.Int32).alias("age_calc"))
        .select(SUBPLOT_ID + ["species_symbol", "measdate", "age_calc",
                              "disturbance_drop_pct", "dropped", "would_be", "n_disturbed", "n_survivors",
                              "full_removal"])
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
    print(f"  full removals (D42, drop 1.0): {coh.filter(pl.col('full_removal')).height:,}")


if __name__ == "__main__":
    # curation-fix 2026-10-26: explicit --out-db/--fiadb (dbconn), no default DB path.
    # Censored trees are never removal events, so a STATUSCD=0 procedural/nonsampled exit is never
    # counted as a disturbance drop.
    # r7 D44 (C F9): diagnostics only. --write / --attach wrote a non-timber, pre-D31 drop into the
    # curated DB (and --attach into curated_cohorts_landis); the drop table is produced only by
    # attach_disturbance_simB.py, so both refuse to run.
    import argparse
    import dbconn
    ap = dbconn.add_db_args(argparse.ArgumentParser(description="cohort_disturbance (diagnostics)"))
    ap.add_argument("--write", action="store_true", help="refused (stale path)")
    ap.add_argument("--attach", action="store_true", help="refused (stale path)")
    a = ap.parse_args()
    if a.write or a.attach:
        sys.exit("cohort_disturbance.py --write/--attach is stale (non-timber, pre-D31 drop) and refuses to run; "
                 "the cohort_disturbance table is written by attach_disturbance_simB.py")
    dbconn.check_args(a)
    states = dbconn.parse_states(a.states) or []
    print(f"DB: {a.out_db}  states: {states or 'ALL'}  cohort_gate={APPLY_COHORT_GATE}")
    con = dbconn.connect(a.out_db, a.fiadb)
    dbconn.require_tables(con, "tree_trajectories", "curated_trees")
    coh = compute(load(con, states), scaffold=load_scaffold(con, states))
    diagnostics(coh)
    con.close()
    print("[done]")
