"""Print distinct epa_l4 / epa_l3 / ecosubcd values to find the right eco code.
Usage: python check_eco.py [DB] [substr]   (default substr '75')"""
import sys
import duckdb

db   = sys.argv[1] if len(sys.argv) > 1 else "../FIASQLITE2PGSQL/FIADB.duckdb"
sub  = sys.argv[2] if len(sys.argv) > 2 else "75"
con  = duckdb.connect(db, read_only=True)

for col in ("epa_l4", "epa_l3", "ecosubcd"):
    print(f"\n==== {col}: values containing {sub!r} ====")
    rows = con.execute(f"""
        SELECT {col} AS v, COUNT(*) AS n
        FROM curated_trees_fvs
        WHERE {col} ILIKE '%{sub}%'
        GROUP BY 1 ORDER BY n DESC LIMIT 30
    """).fetchall()
    for v, n in rows:
        print(f"  {v!r:20s} {n:,}")
    print(f"  (distinct {col} total: "
          f"{con.execute(f'SELECT COUNT(DISTINCT {col}) FROM curated_trees_fvs').fetchone()[0]:,})")

print("\n==== a few raw epa_l4 samples ====")
for (v,) in con.execute(
    "SELECT DISTINCT epa_l4 FROM curated_trees_fvs WHERE epa_l4 IS NOT NULL LIMIT 15"
).fetchall():
    print(f"  {v!r}")
con.close()
