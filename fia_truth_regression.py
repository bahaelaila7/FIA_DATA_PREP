"""fia_truth_regression.py — the truth DB against the previous per-state curated products (DECISIONS.md D59).

  python -I fia_truth_regression.py --truth DB --old-dir DIR --out docs/FIA_TRUTH_REGRESSION.md

Per state: records, live records, trees, fates and ages. Row sets are compared on natural keys, so a difference is a
record that entered, left or changed; each is attributed to a rule in the report's notes."""
import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import argparse, glob, duckdb

K = "STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE, MEASDATE"
TABS = [("curated_trees", "curated_trees"), ("curated_cohorts_landis", "curated_cohorts_landis"),
        ("curated_cohorts_landis_stdorg", "curated_cohorts_landis_stdorg"), ("tree_trajectories", "tree_trajectories")]


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--truth", required=True); ap.add_argument("--old-dir", required=True); ap.add_argument("--out", required=True)
    a = ap.parse_args()
    c = duckdb.connect(); c.execute(f"ATTACH '{a.truth}' AS n (READ_ONLY)")
    L = ["# FIA Truth regression", "", f"Truth DB vs `{a.old_dir}`, one previous DB per state.", "",
         "| State | Records old → new | Same key in both | Only old | Only new | Live old → new | Changed fate (trees) | Mean \\|Δ age\\| (live, yr) |", "|---|---|---|---|---|---|---|---|"]
    T = ["", "## Row counts of the product tables, old → new", "", "| State | " + " | ".join(t[0] for t in TABS) + " |", "|---|" + "---|" * len(TABS)]
    for p in sorted(glob.glob(f"{a.old_dir}/*/curated.duckdb")):
        st = c.execute(f"ATTACH '{p}' AS o (READ_ONLY); SELECT DISTINCT STATECD FROM o.curated_trees").fetchall()
        if len(st) != 1:
            c.execute("DETACH o"); continue
        s = st[0][0]
        r = c.execute(f"""
          WITH o AS (SELECT {K}, STATUSCD, estimated_age, final_tree_fate FROM o.curated_trees),
               x AS (SELECT {K}, STATUSCD, estimated_age, final_tree_fate FROM n.curated_trees WHERE STATECD = {s}),
               j AS (SELECT o.STATUSCD os, x.STATUSCD ns, o.estimated_age oa, x.estimated_age na, o.final_tree_fate ofate, x.final_tree_fate nfate,
                            o.TREE ot, x.TREE nt, COALESCE(o.PLOT, x.PLOT) pl, COALESCE(o.SUBP, x.SUBP) sb, COALESCE(o.UNITCD, x.UNITCD) u, COALESCE(o.COUNTYCD, x.COUNTYCD) cc, COALESCE(o.TREE, x.TREE) tr
                     FROM o FULL JOIN x USING ({K}))
          SELECT count(ot), count(nt), count(*) FILTER (WHERE ot IS NOT NULL AND nt IS NOT NULL), count(*) FILTER (WHERE nt IS NULL), count(*) FILTER (WHERE ot IS NULL),
                 count(*) FILTER (WHERE os = 1), count(*) FILTER (WHERE ns = 1),
                 count(DISTINCT (u, cc, pl, sb, tr)) FILTER (WHERE ot IS NOT NULL AND nt IS NOT NULL AND ofate IS DISTINCT FROM nfate),
                 avg(abs(oa - na)) FILTER (WHERE os = 1 AND ns = 1) FROM j""").fetchone()
        L.append(f"| {s} | {r[0]:,} → {r[1]:,} | {r[2]:,} | {r[3]:,} | {r[4]:,} | {r[5]:,} → {r[6]:,} | {r[7]:,} | {r[8]:.2f} |")
        row = [str(s)]
        for t, _ in TABS:
            col = "STATECD" if t == "curated_trees" else "statecd"
            ov = c.execute(f'SELECT count(*) FROM o."{t}"').fetchone()[0]; nv = c.execute(f'SELECT count(*) FROM n."{t}" WHERE {col} = {s}').fetchone()[0]
            row.append(f"{ov:,} → {nv:,}")
        T.append("| " + " | ".join(row) + " |")
        c.execute("DETACH o"); print(L[-1], flush=True)
    open(a.out, "w").write("\n".join(L + T) + "\n"); print("wrote", a.out)


if __name__ == "__main__":
    main()
