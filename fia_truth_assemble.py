"""fia_truth_assemble.py — the single curated-truth DB of a region (DECISIONS.md D59).

One DuckDB file for every downstream module, flat, with the curation's table names:
  * every table of the curated DB and of the Sim B DB;
  * region-scoped annual copies of the raw tables consumers need, read through dbconn's views, so they
    carry the canonical plot key and exclude quarantined rows (natural ids primary; CNs are attributes).
No consumer needs raw FIADB after this. Run by fia_truth.py.
"""
import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import argparse
import dbconn

RAW_VISIT_TABLES = ("SUBPLOT", "SUBP_COND", "TREE", "SITETREE", "SEEDLING")     # per plot visit, keyed by the natural id
RAW_REF_TABLES = ("REF_SPECIES_GROUP", "REF_FOREST_TYPE", "REF_FOREST_TYPE_GROUP")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--curated-db", required=True); ap.add_argument("--simb-db", required=True)
    ap.add_argument("--fiadb", required=True); ap.add_argument("--out", required=True); ap.add_argument("--states", required=True)
    a = ap.parse_args()
    if os.path.exists(a.out):
        sys.exit(f"{a.out} exists; assemble writes a new file")
    tmp = a.out + ".part"
    if os.path.exists(tmp):
        os.remove(tmp)
    states = dbconn.parse_states(a.states)
    con = dbconn.connect(tmp, a.fiadb)
    con.execute(f"ATTACH '{a.curated_db}' AS cur (READ_ONLY)"); con.execute(f"ATTACH '{a.simb_db}' AS sb (READ_ONLY)")
    def tabs(db): return [r[0] for r in con.execute(f"SELECT table_name FROM duckdb_tables() WHERE database_name='{db}'").fetchall()]
    done = set()
    for db in ("cur", "sb"):
        for t in tabs(db):
            if t in done:
                continue
            con.execute(f'CREATE TABLE main."{t}" AS SELECT * FROM {db}.main."{t}"'); done.add(t)
    sf = dbconn.state_filter(states, col="t.STATECD")
    for t in RAW_VISIT_TABLES:
        con.execute(f"""CREATE TABLE main."{t}" AS SELECT t.* FROM temp.main."{t}" t
                        JOIN ANNUAL_INV A ON A.STATECD = t.STATECD AND A.INVYR = t.INVYR WHERE TRUE {sf}""")
        print(f"  {t:<12}{con.execute(f'SELECT count(*) FROM main.\"{t}\"').fetchone()[0]:>12,}", flush=True)
    for t in RAW_REF_TABLES:
        con.execute(f'CREATE TABLE main."{t}" AS SELECT * FROM fia.main."{t}"')
    con.close()
    os.replace(tmp, a.out)
    print(f"wrote {a.out} ({os.path.getsize(a.out) / 2**30:.2f} GiB; {len(done)} curated tables + {len(RAW_VISIT_TABLES) + len(RAW_REF_TABLES)} raw)")


if __name__ == "__main__":
    main()
