"""fia_ids.py — selection by natural id, never by row position (DECISIONS.md D66).

  pick(df, key, by, which)   the row of each key with the smallest / largest `by`; a tie is an error, not a coin flip
  natural_record_id(...)     a tree record's natural id as text, for hashing (draws, folds)
  maybe_shuffle(df)          test hook: FIA_SHUFFLE_SEED reorders a frame, results must not change
  python -I fia_ids.py shuffle --db DB --seed N   rewrites every table of DB in a random row order (test hook)

Windows use `.over(key, order_by=...)`, so the order is part of the expression. A frame's `.sort` is only for storage and display."""
import os, sys
import polars as pl


def pick(df: pl.DataFrame, key, by, which: str = "first", descending=None, nulls_last: bool = False) -> pl.DataFrame:
    by = [by] if isinstance(by, str) else list(by)
    descending = [False] * len(by) if descending is None else descending
    if which == "last":
        descending = [not d for d in descending]
        nulls_last = not nulls_last
    dup = df.select(list(key) + by).is_duplicated().sum()
    if dup:
        raise ValueError(f"pick: {dup} rows tie on {list(key) + by}; a selection needs a rule that decides")
    if len(by) == 1 and not nulls_last and df[by[0]].null_count() == 0:
        ext = pl.col(by[0]).max() if descending[0] else pl.col(by[0]).min()
        return df.filter(pl.col(by[0]) == ext.over(list(key)))
    rest = [c for c in df.columns if c not in key]
    out = df.group_by(key).agg([pl.col(c).sort_by(by, descending=descending, nulls_last=nulls_last).first() for c in rest])
    return out.select(df.columns)


def natural_record_id(cols=("STATECD", "UNITCD", "COUNTYCD", "PLOT", "SUBP", "TREE", "MEASDATE")) -> pl.Expr:
    return pl.concat_str([pl.col(c).cast(pl.Utf8) for c in cols], separator="_")


def maybe_shuffle(df: pl.DataFrame) -> pl.DataFrame:
    seed = os.environ.get("FIA_SHUFFLE_SEED")
    return df.sample(fraction=1.0, shuffle=True, seed=int(seed)) if seed else df


if __name__ == "__main__":
    import argparse, duckdb
    ap = argparse.ArgumentParser(); ap.add_argument("cmd"); ap.add_argument("--db", required=True); ap.add_argument("--seed", type=int, required=True)
    a = ap.parse_args()
    c = duckdb.connect(a.db); c.execute(f"SELECT setseed({(a.seed % 1000) / 1000})")
    for (t,) in c.execute("SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main'").fetchall():
        c.execute(f'CREATE OR REPLACE TABLE main."{t}" AS SELECT * FROM main."{t}" ORDER BY random()')
    c.close()
