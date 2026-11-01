"""fia_truth_assemble.py — the single curated-truth DB of a region (DECISIONS.md D59).

One DuckDB file for every downstream module, flat, with the curation's table names:
  * every table of the curated DB and of the Sim B DB;
  * region-scoped annual copies of the raw tables consumers need, read through dbconn's views, so they
    carry the canonical plot key and exclude quarantined rows (natural ids primary; CNs are attributes);
  * schema raw: the whole inventory record of the region's states, every visit (periodic, nonsampled), as-recorded
    keys, for consumers that need the plot's history (genveg). Its PLT_CN / PREV_PLT_CN / PLOT.CN hold the NATURAL
    visit id (natural_visit_id), so a join on them is a natural-id join by construction; FIA's value is kept in
    FIA_CN / FIA_PLT_CN. Quarantined rows are left out.
No consumer needs raw FIADB after this. Run by fia_truth.py.
"""
import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import argparse
import dbconn

RAW_VISIT_TABLES = ("SUBPLOT", "SUBP_COND", "TREE", "SITETREE", "SEEDLING")     # per plot visit, keyed by the natural id
RAW_REF_TABLES = ("REF_SPECIES_GROUP", "REF_FOREST_TYPE", "REF_FOREST_TYPE_GROUP")
RAW_SCHEMA_TABLES = ("SUBPLOT", "COND", "SUBP_COND", "TREE", "SITETREE", "SEEDLING")


def natural_visit_id(a: str) -> str:
    """VARCHAR like FIADB's CNs: digits [S]S UU CCC PPPPP YYYY (ranges checked at assembly), a plot visit's natural id."""
    return (f"CAST(CAST({a}.STATECD AS BIGINT) * 100000000000000 + CAST({a}.UNITCD AS BIGINT) * 1000000000000 + "
            f"CAST({a}.COUNTYCD AS BIGINT) * 1000000000 + CAST({a}.PLOT AS BIGINT) * 10000 + CAST({a}.INVYR AS BIGINT) AS VARCHAR)")


def write_raw_schema(con, states):
    st = ",".join(map(str, states))
    bad = con.execute(f"SELECT count(*) FROM fia.main.PLOT WHERE STATECD IN ({st}) AND (PLOT >= 100000 OR UNITCD >= 100 OR COUNTYCD >= 1000 OR INVYR >= 10000)").fetchone()[0]
    if bad:
        sys.exit(f"{bad} plots do not fit natural_visit_id")
    con.execute("CREATE SCHEMA raw")
    con.execute(f"""CREATE TABLE raw.PLOT AS
        SELECT t.* REPLACE ({natural_visit_id('t')} AS CN, {natural_visit_id('o')} AS PREV_PLT_CN), t.CN AS FIA_CN
        FROM fia.main.PLOT t LEFT JOIN fia.main.PLOT o ON o.CN = t.PREV_PLT_CN WHERE t.STATECD IN ({st})""")
    for t in RAW_SCHEMA_TABLES:
        con.execute(f"""CREATE TABLE raw."{t}" AS SELECT t.* REPLACE ({natural_visit_id('t')} AS PLT_CN), t.PLT_CN AS FIA_PLT_CN
            FROM fia.main."{t}" t WHERE t.STATECD IN ({st})
              AND EXISTS (SELECT 1 FROM fia.main.PLOT q WHERE {dbconn.nk('q', 't')} AND q.CN = t.PLT_CN)""")
    for t in ("SURVEY", "REF_SPECIES", "data_plot_eco"):
        con.execute(f'CREATE TABLE raw."{t}" AS SELECT * FROM fia.main."{t}"')
    dup = con.execute("SELECT count(*) - count(DISTINCT CN) FROM raw.PLOT").fetchone()[0]
    if dup:
        sys.exit(f"natural_visit_id collides on {dup} visits")
    print(f"  raw.PLOT {con.execute('SELECT count(*) FROM raw.PLOT').fetchone()[0]:,} visits; raw.TREE {con.execute('SELECT count(*) FROM raw.TREE').fetchone()[0]:,}", flush=True)


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
    write_raw_schema(con, states)
    con.close()
    os.replace(tmp, a.out)
    print(f"wrote {a.out} ({os.path.getsize(a.out) / 2**30:.2f} GiB; {len(done)} curated tables + {len(RAW_VISIT_TABLES) + len(RAW_REF_TABLES)} raw)")


if __name__ == "__main__":
    main()
