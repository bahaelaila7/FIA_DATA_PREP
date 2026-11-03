"""fia_rules.py — the rules several stages share, defined once (DECISIONS.md D72).

Before D72 each stage kept its own copy (a list of cut fates here, a disturbance test there) and the copies drifted:
land-clearing cuts became deaths in the NIMIWAE frame, and unobserved saplings became removals everywhere.
Stages import these; none restates them.

  DISTURBANCE_AGENTS, NON_DISTURBANCE_DSTRBCD, disturbance_death_expr / _sql   the D41 disturbance-death definition
  CUT_REASONS        tree ends that are cuts (a cut record on forest, an inferred harvest, a cut on land leaving forest)
  TALLY_SPLIT_DIA    a tree last recorded below this DIA is a microplot tree: its spot is the microplot (R4: DESIGNCD 1, 5 in)
  end_kind values    alive, death, cut, inferred_removal, diverted, censored"""
import polars as pl

DISTURBANCE_AGENTS = (10, 20, 30, 40, 50)        # insect, disease, fire, animal, weather
NON_DISTURBANCE_DSTRBCD = (0, 60)                # none, vegetation / competition
HARVEST_REASONS = ("silvicultural_death", "harvest_inferred")
CUT_REASONS = HARVEST_REASONS + ("diverted_landuse",)   # a cut record whose condition left forest is still a cut (C7)
TALLY_SPLIT_DIA = 5.0
END_KINDS = ("alive", "death", "cut", "inferred_removal", "diverted", "censored")


def disturbance_death_sql(alias: str = "") -> str:
    """SQL boolean: the record's death is a disturbance death (D41); `alias` = table alias."""
    a = f"{alias}." if alias else ""
    ag = ", ".join(map(str, DISTURBANCE_AGENTS))
    nd = ", ".join(map(str, NON_DISTURBANCE_DSTRBCD))
    return (f"(COALESCE({a}AGENTCD IN ({ag}), FALSE)"
            + "".join(f" OR COALESCE({a}DSTRBCD{i} NOT IN ({nd}), FALSE)" for i in (1, 2, 3)) + ")")


def disturbance_death_expr() -> pl.Expr:
    """polars twin of disturbance_death_sql (columns AGENTCD, DSTRBCD1-3)."""
    e = pl.col("AGENTCD").is_in(list(DISTURBANCE_AGENTS)).fill_null(False)
    for i in (1, 2, 3):
        c = pl.col(f"DSTRBCD{i}")
        e = e | (c.is_not_null() & ~c.is_in(list(NON_DISTURBANCE_DSTRBCD))).fill_null(False)
    return e


def end_kind_of(reason: pl.Expr) -> pl.Expr:
    """end_kind from the end reason (the final_tree_fate string)"""
    return (pl.when(reason == "alive").then(pl.lit("alive"))
            .when(reason.is_in(list(HARVEST_REASONS))).then(pl.lit("cut"))
            .when(reason == "dead").then(pl.lit("death"))
            .when(reason == "disappeared_inferred_removal").then(pl.lit("inferred_removal"))
            .when(reason == "diverted_landuse").then(pl.lit("diverted"))
            .when(reason.str.starts_with("censored_")).then(pl.lit("censored"))
            .otherwise(pl.lit(None, dtype=pl.Utf8)))
