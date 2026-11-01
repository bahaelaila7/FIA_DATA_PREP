"""fia_truth_lint.py — D59 static rules over the pipeline and its consumers. Exit 1 on any finding.
  L1 no join on a CN column (PLT_CN, TRE_CN, PREV_*_CN, SRV_CN, .CN) in a pipeline stage or consumer;
  L2 no cross-visit keying of a condition: a key list holding CONDID without INVYR / MEASDATE;
  L3 no consumer reads raw FIADB.
Allowed: sql_cond_link (SUBP_COND_CHNG_MTRX has no natural key columns), dbconn's PREV_PLT_CN chain and the CN
validation in its views, and verify_curation_v2 (it uses FIA's CN links on purpose, as an independent check)."""
import re, sys, os, argparse
ROOT = os.path.dirname(os.path.abspath(__file__))
STAGES = ["fia_curation_v3.py", "age_imputation.py", "cohort_tracking.py", "cohorts_landis.py", "build_tree_trajectories.py", "cohorts_landis2.py",
          "build_stdorg.py", "cohorts_landis_simB.py", "attach_disturbance_simB.py", "cohort_disturbance.py", "export_reference_tables.py",
          "fia_truth_assemble.py", "trees_fvs.py", "nimiwae_prep.py"]
JOIN = re.compile(r"\b\w+\.(PLT_CN|CN|TRE_CN|PREV_PLT_CN|PREV_TRE_CN|SRV_CN)\s*=\s*(CAST\()?\w*\.?\w+|=\s*\w+\.(PLT_CN|TRE_CN|SRV_CN)\b", re.I)
ALLOW_BLOCK = {"fia_curation_v3.py": ("def sql_cond_link", "def sql_product_visits")}


def scan(path, name, findings):
    txt = open(path, errors="ignore").read().split("\n"); allow = ALLOW_BLOCK.get(name); inside = False
    for i, ln in enumerate(txt, 1):
        if allow and allow[0] in ln: inside = True
        if allow and allow[1] in ln: inside = False
        s = ln.split("#")[0]
        if not inside and "cn-boundary" not in ln and JOIN.search(s) and ("JOIN" in s.upper() or " ON " in s.upper() or "=" in s) and '"' not in s.split("=")[0][-1:]:
            findings.append(f"L1 {name}:{i}: {ln.strip()[:110]}")
        # D66: no selection or previous-record lookup by row position
        if name not in ("fia_ids.py", "fia_truth_lint.py", "fia_truth_assemble.py") and re.search(r'keep="(first|last)"|\.with_row_index\(|\.with_row_count\(', s) and "order-ok" not in ln:
            findings.append(f"L4 {name}:{i}: {ln.strip()[:110]}")
        if re.search(r"\.(shift|cum_sum|cum_max|cum_min|cum_count)\(", s) and ".over(" in s and "order_by" not in s:
            findings.append(f"L4 {name}:{i}: {ln.strip()[:110]}")
        if re.search(r"\.(first|last)\(\)\.alias", s) and "sort_by" not in s:
            findings.append(f"L4 {name}:{i}: {ln.strip()[:110]}")
        # COND_ID (plot key + CONDID) is a condition only together with its visit
        if re.search(r"(over|group_by|shift|partition_by)\(\s*(COND_ID|cond_id_cols)\s*\)|on\s*=\s*COND_ID\s*[,)]", s):
            findings.append(f"L2 {name}:{i}: {ln.strip()[:110]}")


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--consumers", nargs="*", default=[], help="consumer files: L1 + L3")
    a = ap.parse_args(); out = []
    for f in STAGES:
        p = os.path.join(ROOT, f)
        if os.path.exists(p): scan(p, f, out)
    for p in a.consumers:
        scan(p, p, out)
        if "FIADB.duckdb" in open(p, errors="ignore").read(): out.append(f"L3 {p}: names raw FIADB")
    print("\n".join(out) if out else "lint: clean"); print(f"{len(out)} finding(s)")
    sys.exit(1 if out else 0)


if __name__ == "__main__":
    main()
