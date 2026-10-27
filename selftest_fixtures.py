"""
selftest_fixtures.py — r6 R5 (D27): synthetic fixtures that exercise code paths the R4 data never
reaches (in R4 every candidate interpolation gap spans a censoring event, so (n3) has no
candidates). Run by verify_curation_v2.py; returns [(name, ok, detail)].

  interp_basic      a tree with real records at v1 and v3 on an all-forest subplot gets ONE
                    gap_interpolated slot at v2 with linearly interpolated DIA / AGB and the
                    integer age on the subplot axis
  interp_nonforest  the same tree on a subplot whose v2 is entirely nonforest gets NO slot
  interp_censor     a censoring event between v1 and v3 blocks the slot
  timber_branch     cohort_disturbance.compute(timber_only=True): one real >= 5 in tree removed
                    by disturbance, one survivor, one tree that shrank to 4.9 in (alive), one
                    alive only as a gap slot, one sapling removed -> exactly one counted
                    removal and one survivor; the drop = removed / (removed + survivor)
"""
import datetime as dt
import os as _os, sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))

import polars as pl

D1, D2, D3 = dt.date(2000, 6, 15), dt.date(2005, 6, 15), dt.date(2010, 6, 15)


def _interp_case(nonforest_v2: bool = False, censor: bool = False):
    import build_tree_trajectories as bt
    key = dict(STATECD=99, UNITCD=1, COUNTYCD=1, PLOT=1, SUBP=1)
    live = pl.DataFrame([
        {**key, "TREE": 1, "MEASDATE": D1, "sim_year": 0, "age_calc": 20, "STATUSCD": 1,
         "intro_type": None, "dia": 6.0, "ht": 40.0, "cr": 30.0, "agb": 100.0, "ba": 1.0},
        {**key, "TREE": 1, "MEASDATE": D3, "sim_year": 10, "age_calc": 30, "STATUSCD": 1,
         "intro_type": None, "dia": 8.0, "ht": 50.0, "cr": 30.0, "agb": 200.0, "ba": 2.0},
    ], schema_overrides={"intro_type": pl.Utf8, "sim_year": pl.Int32, "age_calc": pl.Int32})
    visits = pl.DataFrame([{**key, "MEASDATE": d, "forest_subpcond_prop": fp, "forest_micrcond_prop": fp}
                           for d, fp in ((D1, 1.0), (D2, 0.0 if nonforest_v2 else 1.0), (D3, 1.0))])
    cens = (pl.DataFrame([{**key, "TREE": 1, "_cdate": D2}]) if censor
            else pl.DataFrame(schema={**{k: pl.Int64 for k in list(key) + ["TREE"]}, "_cdate": pl.Date}))
    out = bt.interpolate_gaps(live, visits, cens)
    return out.filter(pl.col("intro_type") == "gap_interpolated")


def _timber_case():
    import cohort_disturbance as cd
    base = dict(statecd=99, unitcd=1, countycd=1, plot=1, subp=1, species_symbol="PITA",
                tpa=6.018, death_cause=None, damsev1=None, damsev2=None)
    rows = []
    def add(tree, md, sy, age, bio, it, st, fate, dia, term=False):
        rows.append({**base, "tree": tree, "measdate": md, "sim_year": sy, "age_calc": age, "drybio_g": bio,
                     "intro_type": it, "statuscd": st, "terminal_fate": fate, "is_terminal": term, "dia": dia})
    # birth_sim_year = -20 for every member (one cohort)
    add(1, D1, 0, 20, 1000.0, "real", 1, "alive_censored", 6.0); add(1, D3, 10, 30, 1500.0, "real", 1, "alive_censored", 7.0)
    add(2, D1, 0, 20, 800.0, "real", 1, "dead_disturbance", 6.0, True)                      # removed by disturbance
    add(3, D1, 0, 20, 500.0, "real", 1, "alive_censored", 5.0); add(3, D3, 10, 30, 480.0, "real", 1, "alive_censored", 4.9)
    add(4, D1, 0, 20, 600.0, "real", 1, "alive_censored", 5.5); add(4, D3, 10, 30, 650.0, "gap_interpolated", None, "alive_censored", 5.8)
    add(5, D1, 0, 20, 100.0, "real", 1, "dead_disturbance", 4.0, True)                      # sapling: not in Sim B
    df = pl.DataFrame(rows, schema_overrides={"statuscd": pl.Int64, "damsev1": pl.Float64, "damsev2": pl.Float64,
                                              "death_cause": pl.Utf8})
    return cd.compute(df, timber_only=True)


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
    c = _timber_case()
    exp = (800.0 * 6.018 * 1.5) / (800.0 * 6.018 * 1.5 + 1500.0 * 6.018)
    ok = (c.height == 1 and int(c["n_disturbed"][0]) == 1 and int(c["n_survivors"][0]) == 1
          and abs(float(c["disturbance_drop_pct"][0]) - exp) < 1e-6)
    res.append(("timber_branch: only the real >= 5 in removal counts; shrunk / gap-only / sapling trees excluded", ok,
                f"({c.height} row(s)" + ("" if not c.height else
                 f"; n_disturbed {c['n_disturbed'][0]}, n_survivors {c['n_survivors'][0]}, drop {float(c['disturbance_drop_pct'][0]):.4f} vs {exp:.4f}") + ")"))
    return res


if __name__ == "__main__":
    for name, ok, detail in run():
        print(f"  [{'PASS' if ok else 'FAIL'}] {name} {detail}")
