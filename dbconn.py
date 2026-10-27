"""
dbconn.py — one connection helper for every pipeline stage (curation-fix 2026-10-26).

Every stage writes ONLY to an explicit output DuckDB file and reads raw FIADB through a
READ_ONLY attach. Raw FIADB is never opened writable.

    con = dbconn.connect(out_db, fiadb)                       # curated stages
    con = dbconn.connect(out_db, fiadb, curated_db=cur_db)    # side DBs (simB): curated tables
                                                              # come READ_ONLY from cur_db

Name resolution (unqualified names in the stages' SQL):
  * raw FIADB tables (all-UPPERCASE names: PLOT, COND, TREE, SUBPLOT, SUBP_COND, SURVEY,
    REF_SPECIES, SEEDLING, ...) and the two plot/species reference tables
    REFERENCE_TABLES (data_plot_eco, species_longevity_ref) are exposed as TEMP views over
    `fia.main.<name>`;
  * everything else resolves only in the output DB (or, with curated_db, in `cur`).
  * r5 M-2: plot-keyed raw views carry the CANONICAL plot key (build_plot_canon_map); the
    TEMP table plot_canon_map lists the renumbered visits with their raw keys.
FIADB also still holds the OLD curated tables (curated_trees, curated_cohorts_landis*,
clean_subplot_years, tree_trajectories, DATA_SUBPLOTS*, ...). They are NOT exposed: an
unqualified reference to a curated table that is missing from the output DB fails with a
CatalogException instead of silently reading the old version. (This is an explicit
allow-list instead of `SET search_path='main,fia.main'`, which would expose those tables.)
CREATE TABLE (unqualified) always lands in the output DB.

Paths come from arguments or env vars — never hard-coded:
    --out-db  / $FIA_OUT_DB      output DB (created if absent)
    --fiadb   / $FIADB           raw FIADB (read-only)
    --curated-db / $FIA_CURATED_DB   (side-DB stages only) curated DB, read-only
Both --out-db and --fiadb are REQUIRED; there is no default.
"""
import argparse
import os
import sys

import duckdb

# Lower-case FIADB tables that are NOT products of the tree curation and are safe to read:
#   data_plot_eco          plot -> EPA ecoregion spatial join (0_plot_epa.py)
#   species_longevity_ref  per-species longevity reference (independent of tree rules)
REFERENCE_TABLES = ("data_plot_eco", "species_longevity_ref")
# Upper-case FIADB tables that are derived by the OLD SQL pipeline (not raw FIADB).
OLD_DERIVED_PREFIXES = ("DATA_",)

DUCKDB_THREADS = int(os.environ.get("FIA_DUCKDB_THREADS", "3"))
DUCKDB_MEMORY = os.environ.get("FIA_DUCKDB_MEMORY", "5GB")


def add_db_args(ap: argparse.ArgumentParser, curated: bool = False) -> argparse.ArgumentParser:
    ap.add_argument("--out-db", default=os.environ.get("FIA_OUT_DB"),
                    help="output DuckDB (written). env FIA_OUT_DB")
    ap.add_argument("--fiadb", default=os.environ.get("FIADB"),
                    help="raw FIADB DuckDB (attached READ_ONLY). env FIADB")
    if curated:
        ap.add_argument("--curated-db", default=os.environ.get("FIA_CURATED_DB"),
                        help="curated DB (attached READ_ONLY). env FIA_CURATED_DB")
    ap.add_argument("--states", default=os.environ.get("FIA_STATES"),
                    help="comma-separated STATECD subset (env FIA_STATES)")
    return ap


def same_file(p: str, q: str) -> bool:
    """True if p and q are the same file — resolves symlinks / hard links (os.path.samefile
    when both exist), else compares fully resolved paths (audit M6)."""
    if os.path.exists(p) and os.path.exists(q):
        return os.path.samefile(p, q)
    return os.path.realpath(p) == os.path.realpath(q)


def check_args(a, curated: bool = False):
    miss = [n for n in (["out_db", "fiadb"] + (["curated_db"] if curated else []))
            if not getattr(a, n, None)]
    if miss:
        sys.exit(f"missing required path(s): {', '.join('--' + m.replace('_', '-') for m in miss)}")
    if not os.path.exists(a.fiadb):
        sys.exit(f"FIADB not found: {a.fiadb}")
    if same_file(a.out_db, a.fiadb):
        sys.exit("refusing to use FIADB (or a link to it) as the output DB")
    if curated:
        if same_file(a.curated_db, a.fiadb):
            sys.exit("refusing --curated-db = FIADB: its OLD curated tables would be exposed")
        if same_file(a.out_db, a.curated_db):
            sys.exit("refusing to use the curated DB as the side-DB output")


def parse_states(s) -> list[int] | None:
    if not s:
        return None
    return [int(x) for x in str(s).replace(" ", "").split(",") if x]


def build_plot_canon_map(con, max_iter: int = 50) -> int:
    """r5 M-2: a few FIA plots change their (UNITCD, COUNTYCD, PLOT) key between annual visits
    (e.g. GA 13/2/87 146 -> 45 -> 146; most are county corrections). The PREV_PLT_CN chain links
    the visits. Each annual plot visit is mapped to the CANONICAL key = the key at its chain's
    FIRST ANNUAL visit (the chain is followed only through annual-inventory plots). The TEMP
    table plot_canon_map holds ONLY the visits whose key changes:
      CN (PLOT.CN = PLT_CN elsewhere), UNITCD/COUNTYCD/PLOT (canonical), *_RAW (as recorded).
    Every plot-keyed raw view (PLOT by CN, the rest by PLT_CN) then carries the canonical key,
    so all stages see one key per physical plot before anything else. Returns the row count."""
    con.execute("""CREATE OR REPLACE TEMP TABLE _canon_ap AS
        SELECT P.CN, P.PREV_PLT_CN, P.STATECD, P.UNITCD, P.COUNTYCD, P.PLOT
        FROM fia.main.PLOT P JOIN fia.main.SURVEY S ON S.CN = P.SRV_CN AND S.ANN_INVENTORY = 'Y'""")
    con.execute("""CREATE OR REPLACE TEMP TABLE _canon_root AS
        SELECT a.CN, a.CN AS root FROM _canon_ap a""")
    for _ in range(max_iter):
        n = con.execute("""SELECT count(*) FROM _canon_root r JOIN _canon_ap a ON a.CN = r.root
                           JOIN _canon_ap p ON p.CN = a.PREV_PLT_CN""").fetchone()[0]
        if n == 0:
            break
        con.execute("""CREATE OR REPLACE TEMP TABLE _canon_root AS
            SELECT r.CN, COALESCE(p.CN, r.root) AS root FROM _canon_root r JOIN _canon_ap a ON a.CN = r.root
            LEFT JOIN _canon_ap p ON p.CN = a.PREV_PLT_CN""")
    else:
        sys.exit("[dbconn] PREV_PLT_CN chain did not terminate (cycle?)")
    con.execute("""CREATE OR REPLACE TEMP TABLE plot_canon_map AS
        SELECT a.CN, a.STATECD, o.UNITCD, o.COUNTYCD, o.PLOT,
               a.UNITCD AS UNITCD_RAW, a.COUNTYCD AS COUNTYCD_RAW, a.PLOT AS PLOT_RAW
        FROM _canon_root r JOIN _canon_ap a ON a.CN = r.CN JOIN _canon_ap o ON o.CN = r.root
        WHERE (a.UNITCD, a.COUNTYCD, a.PLOT) IS DISTINCT FROM (o.UNITCD, o.COUNTYCD, o.PLOT)""")
    dup = con.execute("""SELECT count(*) FROM (SELECT P.STATECD, COALESCE(m.UNITCD, P.UNITCD), COALESCE(m.COUNTYCD, P.COUNTYCD),
                             COALESCE(m.PLOT, P.PLOT), P.INVYR FROM fia.main.PLOT P
                             JOIN fia.main.SURVEY S ON S.CN = P.SRV_CN AND S.ANN_INVENTORY = 'Y'
                             LEFT JOIN plot_canon_map m ON m.CN = P.CN GROUP BY ALL HAVING count(*) > 1)""").fetchone()[0]
    if dup:
        sys.exit(f"[dbconn] canonical plot keys collide on {dup} plot-INVYRs")
    con.execute("DROP TABLE _canon_ap; DROP TABLE _canon_root")
    return con.execute("SELECT count(*) FROM plot_canon_map").fetchone()[0]


def connect(out_db: str, fiadb: str, curated_db: str | None = None) -> duckdb.DuckDBPyConnection:
    d = os.path.dirname(os.path.abspath(out_db))
    os.makedirs(d, exist_ok=True)
    con = duckdb.connect(out_db)
    con.execute(f"SET threads={DUCKDB_THREADS}")
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY}'")
    con.execute("SET enable_progress_bar=false")
    tmp = os.path.join(d, ".duckdb_tmp")
    con.execute(f"SET temp_directory='{tmp}'")
    con.execute(f"ATTACH '{fiadb}' AS fia (READ_ONLY)")

    local = {r[0].lower() for r in con.execute(
        "SELECT table_name FROM duckdb_tables() WHERE database_name=current_database() "
        "UNION SELECT view_name FROM duckdb_views() WHERE database_name=current_database()"
    ).fetchall()}
    fia_tabs = [r[0] for r in con.execute(
        "SELECT table_name FROM duckdb_tables() WHERE database_name='fia'").fetchall()]
    raw = [t for t in fia_tabs
           if (t == t.upper() and not t.startswith(OLD_DERIVED_PREFIXES)) or t in REFERENCE_TABLES]
    build_plot_canon_map(con)
    ktype = {}
    for tn, cn, dt in con.execute(
            "SELECT table_name, column_name, data_type FROM duckdb_columns() WHERE database_name='fia' "
            "AND column_name IN ('UNITCD','COUNTYCD','PLOT')").fetchall():
        ktype.setdefault(tn, {})[cn] = dt
    keyed = {tn for tn, d in ktype.items() if len(d) == 3}
    pltcn = {r[0] for r in con.execute(
        "SELECT table_name FROM duckdb_columns() WHERE database_name='fia' AND column_name='PLT_CN'").fetchall()}
    for t in raw:
        # M-2 (r5): plot-keyed raw tables carry the CANONICAL plot key (see build_plot_canon_map)
        on = "t.CN" if t == "PLOT" else ("t.PLT_CN" if t in pltcn else None)
        if t in keyed and on:
            rep = ", ".join(f"CASE WHEN m.CN IS NULL THEN t.{k} ELSE CAST(m.{k} AS {ktype[t][k]}) END AS {k}"
                            for k in ("UNITCD", "COUNTYCD", "PLOT"))
            con.execute(f"""CREATE TEMP VIEW "{t}" AS SELECT t.* REPLACE ({rep})
                FROM fia.main."{t}" t LEFT JOIN plot_canon_map m ON m.CN = CAST({on} AS VARCHAR)""")
        else:
            con.execute(f'CREATE TEMP VIEW "{t}" AS SELECT * FROM fia.main."{t}"')

    if curated_db:
        if not os.path.exists(curated_db):
            sys.exit(f"curated DB not found: {curated_db}")
        con.execute(f"ATTACH '{curated_db}' AS cur (READ_ONLY)")
        rawset = {t.lower() for t in raw}
        for (t,) in con.execute(
                "SELECT table_name FROM duckdb_tables() WHERE database_name='cur'").fetchall():
            if t.lower() in local or t.lower() in rawset:
                continue
            con.execute(f'CREATE TEMP VIEW "{t}" AS SELECT * FROM cur.main."{t}"')
    return con


def table_exists(con, name: str) -> bool:
    """True if `name` resolves (output DB, or the temp views) — never looks into fia.* directly."""
    return con.execute(
        "SELECT count(*) FROM (SELECT table_name n, database_name d FROM duckdb_tables() "
        "UNION ALL SELECT view_name, database_name FROM duckdb_views()) "
        "WHERE lower(n)=lower(?) AND d IN (current_database(), 'temp')", [name]).fetchone()[0] > 0


def column_names(con, name: str) -> set[str]:
    return {r[0].lower() for r in con.execute(
        "SELECT column_name FROM duckdb_columns() WHERE lower(table_name)=lower(?) "
        "AND database_name IN (current_database(), 'temp')", [name]).fetchall()}


def require_tables(con, *names):
    miss = [n for n in names if not table_exists(con, n)]
    if miss:
        sys.exit(f"required table(s) missing from the output/curated DB: {miss} "
                 f"(old FIADB copies are deliberately not readable)")


def state_filter(states, col="STATECD", prefix="AND") -> str:
    return f"{prefix} {col} IN ({','.join(map(str, states))})" if states else ""
