"""
selftest_fixtures.py — synthetic fixtures for code paths the R4 data rarely or never reaches. Run by
verify_curation_v2.py as (n9); returns [(name, ok, detail)]. This module IMPORTS pipeline code
(build_tree_trajectories, cohort_disturbance): it is the documented exception to the verifier's
independence (D27, D45) — the expected values below are written from the decision text, not from
the code.

  interp_basic        a tree with real records at v1 and v3 on an all-forest subplot gets ONE
                      gap_interpolated slot at v2 with linearly interpolated DIA / AGB and the
                      integer age on the subplot axis
  interp_nonforest    the same tree on a subplot whose v2 is entirely nonforest gets NO slot
  interp_censor       a censoring event between v1 and v3 blocks the slot
  interp_dead_at_gap  r7 (E18, zombie rule): a dead / cut record AT the gap visit (the caller passes
                      dead-record dates with the censoring events) blocks the slot too
  timber_branch       cohort_disturbance.compute(timber_only=True), D19: one real >= 5 in tree
                      removed (its D41 event at v3), one survivor, one tree that shrank to 4.9 in
                      (alive), one alive only as a gap slot, one sapling removed. The shrunk and
                      gap-only trees carry a disturbance event AT v3 as well (r7 E18), so dropping
                      the D19 non-timber filter is detected: exactly one counted removal and one
                      survivor; drop = removed x gf / (removed x gf + survivor)
  event_visit         r7 D41: a member alive at v1, ABSENT at v2 (subplot visited), whose removal
                      event (first dead record) is at v3 puts NO drop on v2 (nor on v3: it is not a
                      member alive at v2)
  full_removal        r7 D42: a cohort whose only member is removed at its event visit gets ONE
                      row with full_removal = TRUE, drop 1.0, n_survivors 0
  first_seen_dead     r7 D38: a removal whose event is a first_record_dead record
                      (removal_counted = FALSE) is not counted
  gap_slot_alive      r6 D26(b): a gap-interpolated slot is ALIVE by construction (a misrecorded
                      dead record joined to it is never a status source): with timber_only=False
                      the gap tree is a survivor next to a removed cohort-mate
"""
import datetime as dt
import os as _os, sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))

import polars as pl

D1, D2, D3 = dt.date(2000, 6, 15), dt.date(2005, 6, 15), dt.date(2010, 6, 15)


def _interp_case(nonforest_v2: bool = False, censor: bool = False, dead_at_v2: bool = False):
    import build_tree_trajectories as bt
    key = dict(STATECD=99, UNITCD=1, COUNTYCD=1, PLOT=1, SUBP=1)
    live = pl.DataFrame([
        {**key, "TREE": 1, "INVYR": D1.year, "MEASDATE": D1, "sim_year": 0, "age_calc": 20, "STATUSCD": 1,
         "intro_type": None, "dia": 6.0, "ht": 40.0, "cr": 30.0, "agb": 100.0, "ba": 1.0},
        {**key, "TREE": 1, "INVYR": D3.year, "MEASDATE": D3, "sim_year": 10, "age_calc": 30, "STATUSCD": 1,
         "intro_type": None, "dia": 8.0, "ht": 50.0, "cr": 30.0, "agb": 200.0, "ba": 2.0},
    ], schema_overrides={"intro_type": pl.Utf8, "sim_year": pl.Int32, "age_calc": pl.Int32})
    visits = pl.DataFrame([{**key, "INVYR": d.year, "MEASDATE": d, "forest_subpcond_prop": fp, "forest_micrcond_prop": fp}
                           for d, fp in ((D1, 1.0), (D2, 0.0 if nonforest_v2 else 1.0), (D3, 1.0))])
    ev = []
    if censor:
        ev.append({**key, "TREE": 1, "_cdate": D2 + dt.timedelta(days=30)})
    if dead_at_v2:
        ev.append({**key, "TREE": 1, "_cdate": D2})
    cens = (pl.DataFrame(ev) if ev
            else pl.DataFrame(schema={**{k: pl.Int64 for k in list(key) + ["TREE"]}, "_cdate": pl.Date}))
    out = bt.interpolate_gaps(live, visits, cens)
    return out.filter(pl.col("intro_type") == "gap_interpolated")


_BASE = dict(statecd=99, unitcd=1, countycd=1, plot=1, subp=1, species_symbol="PITA",
             tpa=6.018, death_cause=None)
_SCHEMA = {"statuscd": pl.Int64, "death_cause": pl.Utf8, "removal_invyr": pl.Int64,
           "removal_counted": pl.Boolean, "intro_type": pl.Utf8, "terminal_fate": pl.Utf8}


class _Rows:
    """Trajectory-like rows for cohort_disturbance.compute; every member has birth_sim_year -20."""
    def __init__(self):
        self.rows = []

    def add(self, tree, md, sy, bio, it, st, fate, dia, rem_md=None, counted=False, term=False):
        self.rows.append({**_BASE, "tree": tree, "invyr": md.year, "measdate": md, "sim_year": sy, "age_calc": sy + 20,
                          "drybio_g": bio, "intro_type": it, "statuscd": st, "terminal_fate": fate,
                          "is_terminal": term, "dia": dia, "removal_invyr": rem_md.year if rem_md else None, "removal_counted": counted})

    def df(self):
        return pl.DataFrame(self.rows, schema_overrides=_SCHEMA)


def _compute(rows, timber_only=True):
    import cohort_disturbance as cd
    out = cd.compute(rows.df(), timber_only=timber_only)
    return out.sort(["measdate"])


def _timber_case():
    r = _Rows()
    r.add(1, D1, 0, 1000.0, "real", 1, "alive_censored", 6.0); r.add(1, D3, 10, 1500.0, "real", 1, "alive_censored", 7.0)
    r.add(2, D1, 0, 800.0, "real", 1, "dead_disturbance", 6.0, D3, True)                     # removed at its event (v3)
    r.add(3, D1, 0, 500.0, "real", 1, "dead_disturbance", 5.0, D3, True)
    r.add(3, D3, 10, 480.0, "real", 1, "dead_disturbance", 4.9, D3, True)                    # alive at v3, shrank to 4.9 in
    r.add(4, D1, 0, 600.0, "real", 1, "dead_disturbance", 5.5, D3, True)
    r.add(4, D3, 10, 650.0, "gap_interpolated", None, "dead_disturbance", 5.8, D3, True)     # alive at v3 only as a gap slot
    r.add(5, D1, 0, 100.0, "real", 1, "dead_disturbance", 4.0, D3, True)                     # sapling: not in Sim B
    return _compute(r)


def _event_visit_case():
    r = _Rows()
    r.add(1, D1, 0, 1000.0, "real", 1, "alive_censored", 6.0); r.add(1, D2, 5, 1200.0, "real", 1, "alive_censored", 6.5)
    r.add(1, D3, 10, 1500.0, "real", 1, "alive_censored", 7.0)
    r.add(2, D1, 0, 800.0, "real", 1, "dead_disturbance", 6.0, D3, True)   # absent at v2, first dead record at v3
    return _compute(r)


def _full_removal_case():
    r = _Rows()
    r.add(1, D1, 0, 1000.0, "real", 1, "silvicultural_death", 6.0, D3, True)
    r.add(9, D1, 0, 700.0, "real", 1, "alive_censored", 8.0)      # another cohort (different birth year) keeps v3 visited
    r.rows[-1]["age_calc"] = 50
    r.add(9, D3, 10, 900.0, "real", 1, "alive_censored", 9.0)
    r.rows[-1]["age_calc"] = 60
    return _compute(r)


def _first_seen_case():
    r = _Rows()
    r.add(1, D1, 0, 1000.0, "real", 1, "alive_censored", 6.0); r.add(1, D3, 10, 1500.0, "real", 1, "alive_censored", 7.0)
    r.add(2, D1, 0, 800.0, "real", 1, "dead_disturbance", 6.0, D3, False)   # event is a first_record_dead record
    return _compute(r)


def _gap_alive_case():
    r = _Rows()
    r.add(1, D1, 0, 1000.0, "real", 1, "alive_censored", 6.0)
    r.add(1, D2, 5, 1100.0, "gap_interpolated", 2, "alive_censored", 6.5)   # gap slot joined to a (misrecorded) dead record
    r.add(1, D3, 10, 1500.0, "real", 1, "alive_censored", 7.0)
    r.add(2, D1, 0, 800.0, "real", 1, "dead_disturbance", 6.0, D2, True)    # removed at v2
    r.add(3, D1, 0, 300.0, "real", 1, "alive_censored", 6.0); r.add(3, D2, 5, 330.0, "real", 1, "alive_censored", 6.2)
    return _compute(r, timber_only=False)


def _col(c, name, default=None):
    return c[name][0] if c.height and name in c.columns else default


def run():
    res = []
    g = _interp_case()
    ok = (g.height == 1 and g["MEASDATE"][0] == D2 and abs(g["dia"][0] - 7.0) < 0.02
          and abs(g["agb"][0] - 150.0) < 1.0 and g["age_calc"][0] == 25)
    res.append(("interp_basic: one interpolated slot at v2 (DIA 7.0, AGB 150, age 25)", ok,
                f"({g.height} slot(s){'' if not g.height else f'; dia {g['dia'][0]:.2f} agb {g['agb'][0]:.1f} age {g['age_calc'][0]}'})"))
    g = _interp_case(nonforest_v2=True)
    res.append(("interp_nonforest: no slot at an entirely nonforest visit", g.height == 0, f"({g.height})"))
    g = _interp_case(censor=True)
    res.append(("interp_censor: no slot across a censoring event", g.height == 0, f"({g.height})"))
    g = _interp_case(dead_at_v2=True)
    res.append(("interp_dead_at_gap: no slot at a dead / cut record of the tree (zombie rule)", g.height == 0, f"({g.height})"))

    c = _timber_case()
    gf = 1500.0 / 1000.0
    exp = (800.0 * 6.018 * gf) / (800.0 * 6.018 * gf + 1500.0 * 6.018)
    ok = (c.height == 1 and int(c["n_disturbed"][0]) == 1 and int(c["n_survivors"][0]) == 1
          and abs(float(c["disturbance_drop_pct"][0]) - exp) < 1e-6 and not bool(_col(c, "full_removal", False)))
    res.append(("timber_branch: only the real >= 5 in removal counts; shrunk / gap-only / sapling trees excluded", ok,
                f"({c.height} row(s)" + ("" if not c.height else
                 f"; n_disturbed {c['n_disturbed'][0]}, n_survivors {c['n_survivors'][0]}, drop {float(c['disturbance_drop_pct'][0]):.4f} vs {exp:.4f}") + ")"))
    c = _event_visit_case()
    res.append(("event_visit (D41): no drop at a visit that is not the tree's removal event", c.height == 0,
                f"({c.height} row(s){'' if not c.height else ' at ' + ', '.join(str(x) for x in c['measdate'].to_list())})"))
    c = _full_removal_case()
    ok = (c.height == 1 and bool(_col(c, "full_removal", False)) and abs(float(c["disturbance_drop_pct"][0]) - 1.0) < 1e-12
          and int(c["n_survivors"][0]) == 0 and int(c["n_disturbed"][0]) == 1 and c["measdate"][0] == D3)
    res.append(("full_removal (D42): one row, full_removal TRUE, drop 1.0, no survivor", ok,
                f"({c.height} row(s)" + ("" if not c.height else
                 f"; full_removal {_col(c, 'full_removal')}, drop {float(c['disturbance_drop_pct'][0]):.4f}, "
                 f"n_survivors {c['n_survivors'][0]}") + ")"))
    c = _first_seen_case()
    res.append(("first_seen_dead (D38): a first_record_dead removal is not counted", c.height == 0, f"({c.height} row(s))"))
    c = _gap_alive_case()
    exp = (800.0 * 6.018 * ((1100.0 + 330.0) / (1000.0 + 300.0))) / (
        800.0 * 6.018 * ((1100.0 + 330.0) / (1000.0 + 300.0)) + (1100.0 + 330.0) * 6.018)
    ok = (c.height == 1 and int(c["n_survivors"][0]) == 2 and int(c["n_disturbed"][0]) == 1
          and abs(float(c["disturbance_drop_pct"][0]) - exp) < 1e-6)
    res.append(("gap_slot_alive (D26 b): a gap slot is a survivor even when a dead record is joined to it", ok,
                f"({c.height} row(s)" + ("" if not c.height else
                 f"; n_survivors {c['n_survivors'][0]}, drop {float(c['disturbance_drop_pct'][0]):.4f} vs {exp:.4f}") + ")"))
    return res


if __name__ == "__main__":
    for name, ok, detail in run():
        print(f"  [{'PASS' if ok else 'FAIL'}] {name} {detail}")
