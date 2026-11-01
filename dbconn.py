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


# r7 D44 (chain D F8): the --states a stage was given, checked against the DB's run_scope in
# connect(); set by check_args (scope_check=False for step 1, which writes run_scope itself)
_EXPECT_STATES: list[int] | None = None


def check_args(a, curated: bool = False, scope_check: bool = True):
    global _EXPECT_STATES
    _EXPECT_STATES = parse_states(getattr(a, "states", None)) if scope_check else None
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


PLOT_NK = ("STATECD", "UNITCD", "COUNTYCD", "PLOT", "INVYR")     # a plot visit's natural id (guide §2.4.7)
QUARANTINED = ("SUBPLOT", "COND", "SUBP_COND", "TREE", "SITETREE", "SEEDLING")   # tables joined by the stages
RENUM_MIN_PAIRS, RENUM_MIN_SHARE = 2, 0.9     # plot_renumber: trees that must agree across a changed plot key


def nk(a: str, b: str, *extra: str) -> str:
    """Join condition on the natural id (D59): CNs are never join keys. nk("C", "T", "CONDID")."""
    return " AND ".join(f"{a}.{k} = {b}.{k}" for k in PLOT_NK + extra)


def parse_states(s) -> list[int] | None:
    if not s:
        return None
    return [int(x) for x in str(s).replace(" ", "").split(",") if x]


def build_plot_canon_map(con, max_iter: int = 50) -> int:
    """r5 M-2: a few FIA plots change their (UNITCD, COUNTYCD, PLOT) key between annual visits
    (e.g. GA 13/2/87 146 -> 45 -> 146; most are county corrections). Each annual plot visit is
    mapped to the CANONICAL key = the key at its chain's FIRST ANNUAL visit.

    D67 (user: natural ids, no CNs): the chain is built from natural ids and tree records alone.
    A visit's predecessor is the same key's previous annual visit, unless tree evidence says the
    plot arrived from another key (TEMP plot_renumber):
      * the visit is an orphan: its trees' PREVDIA do not match its own key's previous visit, of
        any inventory type (fewer than RENUM_MIN_SHARE agree, of at least RENUM_MIN_PAIRS trees);
      * a candidate is a visit of another key in the state with the same (SUBP, TREE) and PREVDIA =
        its DIA on >= RENUM_MIN_SHARE of the orphan's trees, at least RENUM_MIN_PAIRS of them, earlier
        by date and by INVYR, and after the orphan key's own previous annual visit;
      * a plot is in one key at a time: the candidate's key is not visited again before the
        orphan's key last is (two keys visited side by side are two plots);
      * the link must be one-to-one. A tie is reported and left unlinked.
    FIA's own plot links are not read here; the verifier compares the result with them.

    r8 D48: no coordinate links (coordinates are fuzzed, species is unreliable).

    r6 R7 (D28): the plot's LOCATION comes from its MOST RECENT annual visit (later records carry
    FIA's coordinate / county corrections; Pan uses one location per plot). TEMP plot_loc_visit
    gives, for every visit of a chain that contains a remapped visit, that latest visit's
    LAT/LON/ELEV and raw key (the PLOT view and data_plot_eco use it). Identity stays the
    first-visit key.

    TEMP plot_canon_map holds ONLY the visits whose key changes:
      CN (attribute only), UNITCD/COUNTYCD/PLOT (canonical), *_RAW (as recorded), INVYR,
      reason ('tree_evidence').
    Returns the plot_canon_map row count."""
    con.execute("""CREATE OR REPLACE TEMP VIEW ANNUAL_INV AS
        SELECT DISTINCT STATECD, INVYR FROM fia.main.SURVEY WHERE ANN_INVENTORY = 'Y' AND P3_OZONE_IND = 'N'""")
    VID = "CAST({a}.STATECD AS VARCHAR) || '_' || CAST({a}.UNITCD AS VARCHAR) || '_' || CAST({a}.COUNTYCD AS VARCHAR) || '_' || CAST({a}.PLOT AS VARCHAR) || '_' || CAST({a}.INVYR AS VARCHAR)"
    K = "STATECD, UNITCD, COUNTYCD, PLOT"
    con.execute(f"""CREATE OR REPLACE TEMP TABLE _canon_ap AS
        WITH ap AS (SELECT P.CN, P.STATECD, P.UNITCD, P.COUNTYCD, P.PLOT, P.INVYR, P.LAT, P.LON, P.ELEV,
                           MAKE_DATE(CAST(P.MEASYEAR AS BIGINT), CAST(COALESCE(NULLIF(P.MEASMON, 0), 6) AS BIGINT),
                                     CAST(COALESCE(NULLIF(P.MEASDAY, 0), 15) AS BIGINT)) AS md
                    FROM fia.main.PLOT P JOIN ANNUAL_INV A ON A.STATECD = P.STATECD AND A.INVYR = P.INVYR)
        SELECT a.*, {VID.format(a='a')} AS vid,
               lag({VID.format(a='a')}) OVER w AS prev_vid, lag(a.md) OVER w AS prev_md, lead(a.md) OVER w AS next_md,
               max(a.md) OVER (PARTITION BY a.STATECD, a.UNITCD, a.COUNTYCD, a.PLOT) AS last_md
        FROM ap a WINDOW w AS (PARTITION BY a.STATECD, a.UNITCD, a.COUNTYCD, a.PLOT ORDER BY a.INVYR)""")
    # the previous visit of ANY inventory type: a first annual visit has a periodic predecessor
    con.execute(f"""CREATE OR REPLACE TEMP TABLE _canon_any AS
        SELECT STATECD, UNITCD, COUNTYCD, PLOT, INVYR, lag(INVYR) OVER (PARTITION BY {K} ORDER BY INVYR) AS pinv
        FROM fia.main.PLOT""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE _canon_s AS
        SELECT T.STATECD, T.UNITCD, T.COUNTYCD, T.PLOT, T.INVYR, T.SUBP, T.TREE, T.PREVDIA
        FROM fia.main.TREE T JOIN _canon_ap a USING ({K}, INVYR)
        WHERE T.PREVDIA IS NOT NULL AND T.PREV_STATUS_CD IS NOT NULL""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE _canon_orphan AS
        SELECT s.STATECD, s.UNITCD, s.COUNTYCD, s.PLOT, s.INVYR, count(*) AS nt
        FROM _canon_s s JOIN _canon_any v USING ({K}, INVYR)
        LEFT JOIN fia.main.TREE o ON o.STATECD = s.STATECD AND o.UNITCD = s.UNITCD AND o.COUNTYCD = s.COUNTYCD
             AND o.PLOT = s.PLOT AND o.INVYR = v.pinv AND o.SUBP = s.SUBP AND o.TREE = s.TREE
        GROUP BY ALL
        HAVING count(*) >= {RENUM_MIN_PAIRS}
           AND count(*) FILTER (WHERE abs(s.PREVDIA - o.DIA) <= 0.05) < {RENUM_MIN_SHARE} * count(*)""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE plot_renumber AS
        WITH x AS (SELECT s.*, o.nt, a.CN, a.vid, a.md, a.prev_md, a.last_md
                   FROM _canon_s s JOIN _canon_orphan o USING ({K}, INVYR) JOIN _canon_ap a USING ({K}, INVYR)),
        e AS (SELECT T.STATECD, T.UNITCD, T.COUNTYCD, T.PLOT, T.INVYR, T.SUBP, T.TREE, T.DIA, a.md, a.next_md, a.vid
              FROM fia.main.TREE T JOIN _canon_ap a USING ({K}, INVYR) WHERE T.DIA IS NOT NULL),
        h AS (SELECT x.CN, x.vid, x.STATECD, x.UNITCD, x.COUNTYCD, x.PLOT, x.INVYR, x.md, x.nt,
                     e.UNITCD AS UNITCD_PREV, e.COUNTYCD AS COUNTYCD_PREV, e.PLOT AS PLOT_PREV, e.INVYR AS INVYR_PREV,
                     e.vid AS prev_vid, count(*) AS n_prevdia_ok
              FROM x JOIN e ON e.STATECD = x.STATECD AND e.SUBP = x.SUBP AND e.TREE = x.TREE
                   AND abs(x.PREVDIA - e.DIA) <= 0.05 AND e.INVYR < x.INVYR AND e.md < x.md
                   AND (x.prev_md IS NULL OR x.prev_md < e.md) AND (e.next_md IS NULL OR e.next_md > x.last_md)
              WHERE (e.UNITCD, e.COUNTYCD, e.PLOT) IS DISTINCT FROM (x.UNITCD, x.COUNTYCD, x.PLOT)
              GROUP BY ALL HAVING count(*) >= {RENUM_MIN_PAIRS} AND count(*) >= {RENUM_MIN_SHARE} * x.nt)
        SELECT h.* EXCLUDE (nt), h.nt AS n_pairs,
               count(*) OVER (PARTITION BY h.vid) = 1 AND count(*) OVER (PARTITION BY h.prev_vid) = 1 AS accepted
        FROM h""")
    ties = con.execute("SELECT count(*) FROM plot_renumber WHERE NOT accepted").fetchone()[0]
    if ties:
        print(f"[dbconn] plot_renumber: {ties} tied candidate links left unlinked")
    def _roots(tab: str):
        con.execute("""CREATE OR REPLACE TEMP TABLE _canon_prev AS
            SELECT a.vid, COALESCE(r.prev_vid, a.prev_vid) AS prev
            FROM _canon_ap a LEFT JOIN plot_renumber r ON r.vid = a.vid AND r.accepted""")
        con.execute(f"CREATE OR REPLACE TEMP TABLE {tab} AS SELECT a.vid, a.vid AS root FROM _canon_ap a")
        for _ in range(max_iter):
            n = con.execute(f"""SELECT count(*) FROM {tab} r JOIN _canon_prev p ON p.vid = r.root
                                WHERE p.prev IS NOT NULL""").fetchone()[0]
            if n == 0:
                return
            con.execute(f"""CREATE OR REPLACE TEMP TABLE {tab} AS
                SELECT r.vid, COALESCE(p.prev, r.root) AS root FROM {tab} r JOIN _canon_prev p ON p.vid = r.root""")
        sys.exit("[dbconn] plot chain did not terminate (cycle?)")

    _roots("_canon_root")
    con.execute("""CREATE OR REPLACE TEMP TABLE plot_canon_map AS
        SELECT a.CN, a.vid, a.STATECD, o.UNITCD, o.COUNTYCD, o.PLOT,
               a.UNITCD AS UNITCD_RAW, a.COUNTYCD AS COUNTYCD_RAW, a.PLOT AS PLOT_RAW, a.INVYR,
               'tree_evidence' AS reason
        FROM _canon_root r JOIN _canon_ap a ON a.vid = r.vid JOIN _canon_ap o ON o.vid = r.root
        WHERE (a.UNITCD, a.COUNTYCD, a.PLOT) IS DISTINCT FROM (o.UNITCD, o.COUNTYCD, o.PLOT)""")
    # D28: one location per plot = its most recent annual visit (chains with a remapped visit)
    con.execute("""CREATE OR REPLACE TEMP TABLE plot_loc_visit AS
        WITH rc AS (SELECT r.root FROM _canon_root r JOIN plot_canon_map m ON m.vid = r.vid GROUP BY 1),
        last AS (SELECT r.root, arg_max(a.vid, a.md) lvid FROM _canon_root r JOIN rc USING (root)
                 JOIN _canon_ap a ON a.vid = r.vid GROUP BY 1)
        SELECT a.CN, a.vid, o.STATECD, o.UNITCD, o.COUNTYCD, o.PLOT,
               l.UNITCD AS LOC_UNITCD, l.COUNTYCD AS LOC_COUNTYCD, l.PLOT AS LOC_PLOT, l.LAT, l.LON, l.ELEV,
               a.UNITCD AS UNITCD_RAW, a.COUNTYCD AS COUNTYCD_RAW, a.PLOT AS PLOT_RAW, a.INVYR
        FROM _canon_root r JOIN last USING (root) JOIN _canon_ap l ON l.vid = last.lvid
        JOIN _canon_ap o ON o.vid = r.root JOIN _canon_ap a ON a.vid = r.vid""")
    dup = con.execute("""SELECT count(*) FROM (SELECT P.STATECD, COALESCE(m.UNITCD, P.UNITCD), COALESCE(m.COUNTYCD, P.COUNTYCD),
                             COALESCE(m.PLOT, P.PLOT), P.INVYR FROM fia.main.PLOT P
                             JOIN ANNUAL_INV A ON A.STATECD = P.STATECD AND A.INVYR = P.INVYR
                             LEFT JOIN plot_canon_map m ON m.STATECD = P.STATECD AND m.UNITCD_RAW = P.UNITCD
                                  AND m.COUNTYCD_RAW = P.COUNTYCD AND m.PLOT_RAW = P.PLOT AND m.INVYR = P.INVYR
                             GROUP BY ALL HAVING count(*) > 1)""").fetchone()[0]
    if dup:
        sys.exit(f"[dbconn] canonical plot keys collide on {dup} plot-INVYRs")
    con.execute("DROP TABLE _canon_ap; DROP TABLE _canon_any; DROP TABLE _canon_s; DROP TABLE _canon_orphan; DROP TABLE _canon_root; DROP TABLE _canon_prev")
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
        on = t == "PLOT" or t in pltcn
        if t in keyed and on:
            rep = ", ".join(f"CASE WHEN m.INVYR IS NULL THEN t.{k} ELSE CAST(m.{k} AS {ktype[t][k]}) END AS {k}"
                            for k in ("UNITCD", "COUNTYCD", "PLOT"))
            loc = ""
            if t == "PLOT":   # D28: one location per plot (its most recent visit)
                rep += ", " + ", ".join(f"CASE WHEN lv.INVYR IS NULL THEN t.{k} ELSE lv.{k} END AS {k}"
                                        for k in ("LAT", "LON", "ELEV"))
                loc = ("LEFT JOIN plot_loc_visit lv ON lv.STATECD = t.STATECD AND lv.UNITCD_RAW = t.UNITCD "
                       "AND lv.COUNTYCD_RAW = t.COUNTYCD AND lv.PLOT_RAW = t.PLOT AND lv.INVYR = t.INVYR")
            # D59: a row whose natural key names no plot, or whose PLT_CN disagrees with it, is quarantined
            # (write_id_quarantine); it never reaches a stage (bahaelaila7/FIA_DATA_PREP#1)
            qf = (f"WHERE EXISTS (SELECT 1 FROM fia.main.PLOT q WHERE {nk('q', 't')} AND q.CN = t.PLT_CN)"   # cn-boundary: the load check
                  if t in QUARANTINED else "")
            con.execute(f"""CREATE TEMP VIEW "{t}" AS SELECT t.* REPLACE ({rep})
                FROM fia.main."{t}" t LEFT JOIN plot_canon_map m ON m.STATECD = t.STATECD AND m.UNITCD_RAW = t.UNITCD
                     AND m.COUNTYCD_RAW = t.COUNTYCD AND m.PLOT_RAW = t.PLOT AND m.INVYR = t.INVYR {loc} {qf}""")
        elif on:
            con.execute(f'CREATE TEMP VIEW "{t}" AS SELECT t.* FROM fia.main."{t}" t')
        elif t == "data_plot_eco":
            # D28: a canonical plot whose chain has a remapped visit takes the eco / location row
            # of its most recent visit's raw key (falls back to its own row if that is missing)
            con.execute("""CREATE TEMP VIEW data_plot_eco AS
                WITH loc AS (SELECT DISTINCT STATECD, UNITCD, COUNTYCD, PLOT, LOC_UNITCD, LOC_COUNTYCD, LOC_PLOT
                             FROM plot_loc_visit),
                rep AS (SELECT e.* REPLACE (CAST(loc.UNITCD AS INTEGER) AS unitcd, CAST(loc.COUNTYCD AS INTEGER) AS countycd,
                                      CAST(loc.PLOT AS INTEGER) AS plot)   -- r7 F7-11: keys stay INTEGER
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
            if t.lower() in local or t.lower() in rawset or t.lower() == "plot_renumber":   # (TEMP, rebuilt above)
                continue
            con.execute(f'CREATE TEMP VIEW "{t}" AS SELECT * FROM cur.main."{t}"')
    check_run_scope(con)
    return con


def write_id_quarantine(con, scope: str = "") -> int:
    """D59: table id_quarantine = per table and natural key, the rows the views exclude because
    their natural key names no PLOT row or their PLT_CN is another plot's. scope: AND ... on t.STATECD."""
    parts = [f"""SELECT '{t}' AS tab, t.STATECD, t.UNITCD, t.COUNTYCD, t.PLOT, t.INVYR, CAST(t.PLT_CN AS VARCHAR) AS PLT_CN,
                        count(*) AS n_rows,
                        CASE WHEN bool_or(EXISTS (SELECT 1 FROM fia.main.PLOT q WHERE {nk('q', 't')}))
                             THEN 'plt_cn_is_another_plot' ELSE 'natural_key_names_no_plot' END AS reason
                 FROM fia.main."{t}" t
                 WHERE NOT EXISTS (SELECT 1 FROM fia.main.PLOT q WHERE {nk('q', 't')} AND q.CN = t.PLT_CN) {scope}   -- cn-boundary: the load check
                 GROUP BY ALL""" for t in QUARANTINED]
    con.execute("DROP TABLE IF EXISTS id_quarantine")
    con.execute("CREATE TABLE id_quarantine AS " + " UNION ALL ".join(parts))
    return con.execute("SELECT COALESCE(sum(n_rows), 0) FROM id_quarantine").fetchone()[0]


def check_run_scope(con, states=None):
    """r7 D44 (chain D F8): a stage given --states refuses a DB whose run_scope (written by step
    1) covers different states; without this a resume with another FIA_STATES silently emptied
    the trajectories. Reads the output DB's run_scope, else the curated DB's (side-DB stages)."""
    want = states if states is not None else _EXPECT_STATES
    if want is None:
        return
    for db in (None, "cur"):
        if db == "cur" and not con.execute("SELECT count(*) FROM duckdb_databases() WHERE database_name='cur'").fetchone()[0]:
            continue
        dbn = db or con.execute("SELECT current_database()").fetchone()[0]
        if not con.execute("SELECT count(*) FROM duckdb_tables() WHERE database_name=? AND table_name='run_scope'",
                           [dbn]).fetchone()[0]:
            continue
        have = con.execute(f'SELECT states FROM "{dbn}".main.run_scope').fetchone()[0]
        have_l = sorted(parse_states(have) or [])
        if have_l != sorted(set(want)):
            sys.exit(f"[dbconn] --states {','.join(map(str, sorted(set(want))))} does not match the run_scope "
                     f"states {have!r} of {dbn} (step 1 scope); refusing to mix scopes")
        return


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
