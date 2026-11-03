"""fia_truth_d72_regression.py — the D72 truth DB against the live one (DECISIONS.md D72, step 8).

  python -I fia_truth_d72_regression.py --old FIA/truth/fia_truth_r4.duckdb --new FIA/truth/fia_truth_r4_d72.duckdb --out docs/D72_REGRESSION.md

Every table both DBs hold is compared as a row set (EXCEPT ALL both ways). Tables that differ are broken down on natural keys:
curated_trees per column, fates as old -> new transitions, product rows by intro_type, subplot disturbance flags. A difference is
expected only where a D72 rule acts (C1 segments, C3 unobserved / dated removals, C7 cut causes); the notes name them."""
import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import argparse, duckdb

TK = "STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE"


def cols(c, db, t):
    return [r[0] for r in c.execute(f"SELECT column_name FROM duckdb_columns() WHERE database_name = '{db}' AND schema_name = 'main' AND table_name = '{t}' ORDER BY column_index").fetchall()]


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--old", required=True); ap.add_argument("--new", required=True); ap.add_argument("--out", required=True)
    a = ap.parse_args()
    c = duckdb.connect(); c.execute("SET threads = 6"); c.execute(f"ATTACH '{a.old}' AS o (READ_ONLY)"); c.execute(f"ATTACH '{a.new}' AS n (READ_ONLY)")
    tabs = lambda db: {r[0] for r in c.execute(f"SELECT table_name FROM duckdb_tables() WHERE database_name = '{db}' AND schema_name = 'main'").fetchall()}
    to, tn = tabs("o"), tabs("n")
    L = ["# D72 regression: live truth DB vs D72 truth DB", "", f"old `{a.old}`, new `{a.new}`. Row sets compared with EXCEPT ALL on the shared columns.", "",
         f"Tables only new: {', '.join(sorted(tn - to)) or 'none'}. Only old: {', '.join(sorted(to - tn)) or 'none'}.", "",
         "| Table | Rows old | Rows new | Only old | Only new | New columns |", "|---|---|---|---|---|---|"]
    same, diff = [], []
    for t in sorted(to & tn):
        co, cn = cols(c, "o", t), cols(c, "n", t); sh = [x for x in co if x in cn]; q = ", ".join(f'"{x}"' for x in sh)
        r = c.execute(f'''SELECT (SELECT count(*) FROM o.main."{t}"), (SELECT count(*) FROM n.main."{t}"),
                          (SELECT count(*) FROM (SELECT {q} FROM o.main."{t}" EXCEPT ALL SELECT {q} FROM n.main."{t}")),
                          (SELECT count(*) FROM (SELECT {q} FROM n.main."{t}" EXCEPT ALL SELECT {q} FROM o.main."{t}"))''').fetchone()
        extra = [x for x in cn if x not in co]
        (same if r[2] == 0 and r[3] == 0 else diff).append(t)
        if r[2] or r[3] or extra:
            L.append(f"| {t} | {r[0]:,} | {r[1]:,} | {r[2]:,} | {r[3]:,} | {', '.join(extra) or '—'} |")
    L += ["", f"**Identical on shared columns ({len(same)}):** {', '.join(same)}.", ""]
    # curated_trees per column
    co = [x for x in cols(c, "o", "curated_trees") if x in cols(c, "n", "curated_trees")]
    k = ["STATECD", "UNITCD", "COUNTYCD", "PLOT", "SUBP", "TREE", "INVYR"]
    agg = ", ".join(f'count(*) FILTER (WHERE x."{v}" IS DISTINCT FROM y."{v}") AS "{v}"' for v in co if v not in k)
    r = c.execute(f"SELECT count(*) n, count(*) FILTER (WHERE y.STATECD IS NULL) only_old, count(*) FILTER (WHERE x.STATECD IS NULL) only_new, {agg} "
                  f"FROM o.curated_trees x FULL JOIN n.curated_trees y USING ({', '.join(k)})").df().iloc[0]
    L += ["## curated_trees by column", "", f"{int(r.n):,} records on the natural key; only old {int(r.only_old):,}, only new {int(r.only_new):,}.", "",
          "| Column | Records that differ |", "|---|---|"] + [f"| {v} | {int(r[v]):,} |" for v in co if v not in k and r[v]] + [""]
    # fates
    L += ["## final_tree_fate, old -> new (trees)", "", "| old | new | trees |", "|---|---|---|"]
    for row in c.execute(f"""WITH x AS (SELECT {TK}, any_value(final_tree_fate) f FROM o.curated_trees GROUP BY ALL),
                             y AS (SELECT {TK}, any_value(final_tree_fate) f FROM n.curated_trees GROUP BY ALL)
                             SELECT x.f, y.f, count(*) FROM x FULL JOIN y USING ({TK}) WHERE x.f IS DISTINCT FROM y.f GROUP BY ALL ORDER BY 3 DESC""").fetchall():
        L.append(f"| {row[0]} | {row[1]} | {row[2]:,} |")
    if "tree_end" in tn:
        L += ["", "## tree_end (new)", "", "| end_kind | end_reason | cause | trees |", "|---|---|---|---|"] + [
            f"| {r_[0]} | {r_[1]} | {r_[2]} | {r_[3]:,} |" for r_ in c.execute("SELECT end_kind, end_reason, cause, count(*) FROM n.tree_end GROUP BY ALL ORDER BY 4 DESC").fetchall()]
    # products by intro_type
    for t, kk in (("tree_trajectories", "statecd, unitcd, countycd, plot, subp, tree, invyr"),
                  ("curated_cohorts_landis", "statecd, unitcd, countycd, plot, subp, species_symbol, invyr, age_calc, intro_type"),
                  ("curated_cohorts_landis_stdorg", "statecd, unitcd, countycd, plot, subp, species_symbol, invyr, age_calc, intro_type")):
        if t not in (to & tn):
            continue
        L += ["", f"## {t}: rows by intro_type", "", "| intro_type | old | new | only old | only new |", "|---|---|---|---|---|"]
        kk2 = kk.replace(", intro_type", ", COALESCE(intro_type, 'real') AS intro_type")     # a NULL intro_type (real rows) must match itself
        for row in c.execute(f"""WITH x AS (SELECT DISTINCT {kk2}, COALESCE(intro_type, 'real') it FROM o.main.{t}), y AS (SELECT DISTINCT {kk2}, COALESCE(intro_type, 'real') it FROM n.main.{t})
            SELECT COALESCE(x.it, y.it), count(x.it), count(y.it), count(*) FILTER (WHERE y.it IS NULL), count(*) FILTER (WHERE x.it IS NULL)
            FROM x FULL JOIN y USING ({kk}) GROUP BY 1 ORDER BY 1""").fetchall():
            L.append(f"| {row[0]} | {row[1]:,} | {row[2]:,} | {row[3]:,} | {row[4]:,} |")
    if "tree_trajectories" in (to & tn):
        L += ["", "## tree_trajectories terminal_fate, old -> new (trees)", "", "| old | new | trees |", "|---|---|---|"]
        for row in c.execute("""WITH x AS (SELECT statecd, unitcd, countycd, plot, subp, tree, any_value(terminal_fate) f FROM o.tree_trajectories GROUP BY ALL),
                                y AS (SELECT statecd, unitcd, countycd, plot, subp, tree, any_value(terminal_fate) f FROM n.tree_trajectories GROUP BY ALL)
                                SELECT x.f, y.f, count(*) FROM x FULL JOIN y USING (statecd, unitcd, countycd, plot, subp, tree)
                                WHERE x.f IS DISTINCT FROM y.f GROUP BY ALL ORDER BY 3 DESC""").fetchall():
            L.append(f"| {row[0]} | {row[1]} | {row[2]:,} |")
    for t in ("curated_cohorts_landis", "curated_cohorts_landis_simB"):
        if t not in (to & tn):
            continue
        r = c.execute(f"""WITH x AS (SELECT DISTINCT statecd, unitcd, countycd, plot, subp, subp_has_dstrb f FROM o.main.{t}),
                              y AS (SELECT DISTINCT statecd, unitcd, countycd, plot, subp, subp_has_dstrb f FROM n.main.{t})
                         SELECT count(*), count(*) FILTER (WHERE x.f AND NOT y.f), count(*) FILTER (WHERE y.f AND NOT x.f)
                         FROM x JOIN y USING (statecd, unitcd, countycd, plot, subp)""").fetchone()
        L += ["", f"**{t} subp_has_dstrb:** {r[0]:,} subplots; disturbed -> not {r[1]:,}; not -> disturbed {r[2]:,}."]
    if "cohort_disturbance" in (to & tn):
        r = c.execute("""SELECT (SELECT count(*) FROM o.cohort_disturbance), (SELECT count(*) FROM n.cohort_disturbance),
                                (SELECT count(*) FROM o.cohort_disturbance WHERE full_removal), (SELECT count(*) FROM n.cohort_disturbance WHERE full_removal)""").fetchone()
        L += ["", f"**cohort_disturbance:** rows {r[0]:,} -> {r[1]:,}; full removals {r[2]:,} -> {r[3]:,}."]
    open(a.out, "w").write("\n".join(L) + "\n"); print("\n".join(L))


if __name__ == "__main__":
    main()
