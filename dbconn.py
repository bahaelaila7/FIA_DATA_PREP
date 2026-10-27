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


COORD_LINK_MIN_SHARE = 0.30   # D29: share of the earlier chain end's live trees matched at the start


def build_plot_canon_map(con, max_iter: int = 50) -> int:
    """r5 M-2: a few FIA plots change their (UNITCD, COUNTYCD, PLOT) key between annual visits
    (e.g. GA 13/2/87 146 -> 45 -> 146; most are county corrections). The PREV_PLT_CN chain links
    the visits. Each annual plot visit is mapped to the CANONICAL key = the key at its chain's
    FIRST ANNUAL visit (the chain is followed only through annual-inventory plots).

    r6 S1 (D29): a chain START (no annual PREV_PLT_CN) is also linked to an earlier chain END
    when the two visits have IDENTICAL LAT/LON (same state, earlier date, different key) AND >=
    COORD_LINK_MIN_SHARE of the end's live trees reappear at the start with the same SUBP, TREE
    and SPCD and a non-decreasing DIA; each start takes its best end and each end is used once.
    TEMP plot_coord_links lists them.

    r6 R7 (D28): the plot's LOCATION comes from its MOST RECENT annual visit (later records carry
    FIA's coordinate / county corrections; Pan uses one location per plot). TEMP plot_loc_visit
    gives, for every visit of a chain that contains a remapped visit, that latest visit's
    LAT/LON/ELEV and raw key (the PLOT view and data_plot_eco use it). Identity stays the
    first-visit key.

    TEMP plot_canon_map holds ONLY the visits whose key changes:
      CN (PLOT.CN = PLT_CN elsewhere), UNITCD/COUNTYCD/PLOT (canonical), *_RAW (as recorded).
    Returns the plot_canon_map row count."""
    con.execute("""CREATE OR REPLACE TEMP TABLE _canon_ap AS
        SELECT P.CN, P.PREV_PLT_CN, P.STATECD, P.UNITCD, P.COUNTYCD, P.PLOT, P.INVYR, P.LAT, P.LON, P.ELEV,
               MAKE_DATE(CAST(P.MEASYEAR AS BIGINT), CAST(COALESCE(NULLIF(P.MEASMON, 0), 6) AS BIGINT),
                         CAST(COALESCE(NULLIF(P.MEASDAY, 0), 15) AS BIGINT)) AS md
        FROM fia.main.PLOT P JOIN fia.main.SURVEY S ON S.CN = P.SRV_CN AND S.ANN_INVENTORY = 'Y'""")
    # D29: coordinate + tree-continuity links of chain starts to earlier chain ends
    con.execute("""CREATE OR REPLACE TEMP TABLE plot_coord_links AS
        WITH ends AS (SELECT a.* FROM _canon_ap a WHERE NOT EXISTS (SELECT 1 FROM _canon_ap b WHERE b.PREV_PLT_CN = a.CN)),
        starts AS (SELECT a.* FROM _canon_ap a WHERE a.PREV_PLT_CN IS NULL
                   OR NOT EXISTS (SELECT 1 FROM _canon_ap b WHERE b.CN = a.PREV_PLT_CN)),
        pr AS (SELECT s.CN start_cn, e.CN end_cn, s.STATECD, e.md end_md
               FROM starts s JOIN ends e ON e.STATECD = s.STATECD AND e.LAT = s.LAT AND e.LON = s.LON
               WHERE e.md < s.md AND e.CN <> s.CN
                 AND (s.UNITCD, s.COUNTYCD, s.PLOT) IS DISTINCT FROM (e.UNITCD, e.COUNTYCD, e.PLOT)),
        el AS (SELECT pr.start_cn, pr.end_cn, count(*) n_live FROM pr JOIN fia.main.TREE t ON t.PLT_CN = pr.end_cn
               WHERE t.STATUSCD = 1 GROUP BY ALL),
        em AS (SELECT pr.start_cn, pr.end_cn, count(DISTINCT t.CN) n_match FROM pr
               JOIN fia.main.TREE t ON t.PLT_CN = pr.end_cn AND t.STATUSCD = 1
               JOIN fia.main.TREE u ON u.PLT_CN = pr.start_cn AND u.SUBP = t.SUBP AND u.TREE = t.TREE
                                   AND u.SPCD = t.SPCD AND u.DIA >= t.DIA
               GROUP BY ALL),
        sc AS (SELECT pr.*, el.n_live, COALESCE(em.n_match, 0) n_match, COALESCE(em.n_match, 0) / el.n_live AS match_share
               FROM pr JOIN el USING (start_cn, end_cn) LEFT JOIN em USING (start_cn, end_cn)
               WHERE COALESCE(em.n_match, 0) >= """ + str(COORD_LINK_MIN_SHARE) + """ * el.n_live),
        b1 AS (SELECT * FROM sc QUALIFY row_number() OVER (PARTITION BY start_cn ORDER BY match_share DESC, end_md DESC, end_cn) = 1)
        SELECT * FROM b1 QUALIFY row_number() OVER (PARTITION BY end_cn ORDER BY match_share DESC, start_cn) = 1""")
    def _roots(with_links: bool, tab: str):
        lk = "LEFT JOIN plot_coord_links l ON l.start_cn = a.CN" if with_links else \
             "LEFT JOIN (SELECT NULL::VARCHAR AS start_cn, NULL::VARCHAR AS end_cn) l ON FALSE"
        con.execute(f"""CREATE OR REPLACE TEMP TABLE _canon_prev AS
            SELECT a.CN, COALESCE(l.end_cn, CASE WHEN EXISTS (SELECT 1 FROM _canon_ap b WHERE b.CN = a.PREV_PLT_CN)
                                                 THEN a.PREV_PLT_CN END) AS prev
            FROM _canon_ap a {lk}""")
        con.execute(f"CREATE OR REPLACE TEMP TABLE {tab} AS SELECT a.CN, a.CN AS root FROM _canon_ap a")
        for _ in range(max_iter):
            n = con.execute(f"""SELECT count(*) FROM {tab} r JOIN _canon_prev p ON p.CN = r.root
                                WHERE p.prev IS NOT NULL""").fetchone()[0]
            if n == 0:
                return
            con.execute(f"""CREATE OR REPLACE TEMP TABLE {tab} AS
                SELECT r.CN, COALESCE(p.prev, r.root) AS root FROM {tab} r JOIN _canon_prev p ON p.CN = r.root""")
        sys.exit("[dbconn] PREV_PLT_CN chain did not terminate (cycle?)")

    _roots(False, "_canon_root0")          # chains by PREV_PLT_CN only
    n_drop = 0
    for _ in range(10):
        _roots(True, "_canon_root")
        # a coordinate link whose re-keyed chain collides with another visit of the same
        # canonical key and INVYR is dropped (the chains are then different plots)
        bad = con.execute("""
            WITH k AS (SELECT r.CN, r.root, r0.root AS root0, o.UNITCD, o.COUNTYCD, o.PLOT, a.STATECD, a.INVYR
                       FROM _canon_root r JOIN _canon_root0 r0 USING (CN) JOIN _canon_ap a ON a.CN = r.CN
                       JOIN _canon_ap o ON o.CN = r.root),
            d AS (SELECT STATECD, UNITCD, COUNTYCD, PLOT, INVYR FROM k GROUP BY ALL HAVING count(*) > 1)
            SELECT DISTINCT k.root0 FROM k JOIN d USING (STATECD, UNITCD, COUNTYCD, PLOT, INVYR)
            WHERE k.root <> k.root0""").fetchall()
        if not bad:
            break
        n_drop += len(bad)
        con.executemany("DELETE FROM plot_coord_links WHERE start_cn = ?", bad)
    if n_drop:
        print(f"[dbconn] {n_drop} coordinate link(s) dropped: the re-keyed chain would collide on a plot-INVYR")
    con.execute("DROP TABLE _canon_root0")
    con.execute("""CREATE OR REPLACE TEMP TABLE plot_canon_map AS
        SELECT a.CN, a.STATECD, o.UNITCD, o.COUNTYCD, o.PLOT,
               a.UNITCD AS UNITCD_RAW, a.COUNTYCD AS COUNTYCD_RAW, a.PLOT AS PLOT_RAW
        FROM _canon_root r JOIN _canon_ap a ON a.CN = r.CN JOIN _canon_ap o ON o.CN = r.root
        WHERE (a.UNITCD, a.COUNTYCD, a.PLOT) IS DISTINCT FROM (o.UNITCD, o.COUNTYCD, o.PLOT)""")
    # D28: one location per plot = its most recent annual visit (chains with a remapped visit)
    con.execute("""CREATE OR REPLACE TEMP TABLE plot_loc_visit AS
        WITH rc AS (SELECT r.root FROM _canon_root r JOIN plot_canon_map m ON m.CN = r.CN GROUP BY 1),
        last AS (SELECT r.root, arg_max(a.CN, a.md) lcn FROM _canon_root r JOIN rc USING (root)
                 JOIN _canon_ap a ON a.CN = r.CN GROUP BY 1)
        SELECT r.CN, o.STATECD, o.UNITCD, o.COUNTYCD, o.PLOT,
               l.UNITCD AS LOC_UNITCD, l.COUNTYCD AS LOC_COUNTYCD, l.PLOT AS LOC_PLOT, l.LAT, l.LON, l.ELEV
        FROM _canon_root r JOIN last USING (root) JOIN _canon_ap l ON l.CN = last.lcn
        JOIN _canon_ap o ON o.CN = r.root""")
    dup = con.execute("""SELECT count(*) FROM (SELECT P.STATECD, COALESCE(m.UNITCD, P.UNITCD), COALESCE(m.COUNTYCD, P.COUNTYCD),
                             COALESCE(m.PLOT, P.PLOT), P.INVYR FROM fia.main.PLOT P
                             JOIN fia.main.SURVEY S ON S.CN = P.SRV_CN AND S.ANN_INVENTORY = 'Y'
                             LEFT JOIN plot_canon_map m ON m.CN = P.CN GROUP BY ALL HAVING count(*) > 1)""").fetchone()[0]
    if dup:
        sys.exit(f"[dbconn] canonical plot keys collide on {dup} plot-INVYRs")
    con.execute("DROP TABLE _canon_ap; DROP TABLE _canon_root; DROP TABLE _canon_prev")
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
            loc = ""
            if t == "PLOT":   # D28: one location per plot (its most recent visit)
                rep += ", " + ", ".join(f"CASE WHEN lv.CN IS NULL THEN t.{k} ELSE lv.{k} END AS {k}"
                                        for k in ("LAT", "LON", "ELEV"))
                loc = "LEFT JOIN plot_loc_visit lv ON lv.CN = CAST(t.CN AS VARCHAR)"
            con.execute(f"""CREATE TEMP VIEW "{t}" AS SELECT t.* REPLACE ({rep})
                FROM fia.main."{t}" t LEFT JOIN plot_canon_map m ON m.CN = CAST({on} AS VARCHAR) {loc}""")
        elif t == "data_plot_eco":
            # D28: a canonical plot whose chain has a remapped visit takes the eco / location row
            # of its most recent visit's raw key (falls back to its own row if that is missing)
            con.execute("""CREATE TEMP VIEW data_plot_eco AS
                WITH loc AS (SELECT DISTINCT STATECD, UNITCD, COUNTYCD, PLOT, LOC_UNITCD, LOC_COUNTYCD, LOC_PLOT
                             FROM plot_loc_visit),
                rep AS (SELECT e.* REPLACE (loc.UNITCD AS unitcd, loc.COUNTYCD AS countycd, loc.PLOT AS plot)
                        FROM loc JOIN fia.main.data_plot_eco e ON e.statecd = loc.STATECD AND e.unitcd = loc.LOC_UNITCD
                         AND e.countycd = loc.LOC_COUNTYCD AND e.plot = loc.LOC_PLOT
                        QUALIFY row_number() OVER (PARTITION BY loc.STATECD, loc.UNITCD, loc.COUNTYCD, loc.PLOT
                                                   ORDER BY loc.LOC_UNITCD, loc.LOC_COUNTYCD, loc.LOC_PLOT) = 1)
                SELECT * FROM rep
                UNION ALL
                SELECT e.* FROM fia.main.data_plot_eco e
                WHERE NOT EXISTS (SELECT 1 FROM rep WHERE rep.statecd = e.statecd AND rep.unitcd = e.unitcd
                                  AND rep.countycd = e.countycd AND rep.plot = e.plot)""")
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
