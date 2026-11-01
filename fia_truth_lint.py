"""fia_truth_lint.py — D59 static rules over the pipeline and its consumers. Exit 1 on any finding.
  L1 no join on a CN column (PLT_CN, TRE_CN, PREV_*_CN, SRV_CN, .CN) in a pipeline stage or consumer;
  L2 no cross-visit keying of a condition: a key list holding CONDID without INVYR / MEASDATE;
  L3 no consumer reads raw FIADB;
  L4 no row-order logic;
  L5 no time arithmetic on INVYR;
  L6 no date as a key (D69): MEASDATE and its derivatives order records and measure time, they never join or group them.
  L8 a float sum or mean in a keyed aggregate is taken in sorted order, because the result depends on row and thread order;
  L7 no pipeline stage reads FIA's CN links (PREV_PLT_CN, PREV_TRE_CN, SUBP_COND_CHNG_MTRX): D67.
Allowed: the CN validation in dbconn's views, the TreeAge load by CN, fia_truth_assemble's conversion of raw CNs to natural ids
(cn-boundary), and verify_curation_v2, which uses FIA's CN links on purpose, as an independent check."""
import re, sys, os, argparse
ROOT = os.path.dirname(os.path.abspath(__file__))
STAGES = ["fia_curation_v3.py", "age_imputation.py", "cohort_tracking.py", "cohorts_landis.py", "build_tree_trajectories.py", "cohorts_landis2.py",
          "build_stdorg.py", "cohorts_landis_simB.py", "attach_disturbance_simB.py", "cohort_disturbance.py", "export_reference_tables.py",
          "fia_truth_assemble.py", "trees_fvs.py", "nimiwae_prep.py", "dbconn.py"]
JOIN = re.compile(r"\b\w+\.(PLT_CN|CN|TRE_CN|PREV_PLT_CN|PREV_TRE_CN|SRV_CN)\s*=\s*(CAST\()?\w*\.?\w+|=\s*\w+\.(PLT_CN|TRE_CN|SRV_CN)\b", re.I)
ALLOW_BLOCK = {}
FLOAT_AGG = ("cohort_disturbance.py", "cohorts_landis_simB.py", "build_tree_trajectories.py", "cohort_tracking.py", "fia_curation_v3.py", "cohorts_landis.py")


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
        # D68: INVYR is part of the natural id and may order a plot's visits; it never enters a time calculation
        s5 = re.sub(r"\w*_ID(\s\+\s\w+)+", "", s)   # key names in prose: SUBPLOT_ID + INVYR + MEASDATE
        if "invyr-ok" not in ln and re.search(r"\bINVYR\s+[-+]\s+\w|\w\s+[-+]\s+[a-z_.]*INVYR\b|year_diff\([^)]*INVYR|date_diff\([^)]*INVYR", s5):
            findings.append(f"L5 {name}:{i}: {ln.strip()[:110]}")
        # D69: a date is a timestamp, never an identifier (mark a deliberate date-valued grouping with date-ok)
        if "date-ok" not in ln and name not in ("fia_truth_lint.py",) and (
                re.search(r"\bon\s*=\s*[^)]*[\"'](MEASDATE|measdate|prior_measdate)[\"']", s)
                or re.search(r"group_by\([^)]*[\"'](MEASDATE|measdate|prior_measdate)[\"']", s)
                or re.search(r"over\([^)]*[\"'](MEASDATE|measdate)[\"'][^)]*\)", s.replace('order_by="MEASDATE"', "").replace('order_by="measdate"', "").replace("order_by='measdate'", ""))
                or re.search(r"\w+\.(MEASDATE|measdate)\s*=\s*\w+\.(MEASDATE|measdate)\b", s)
                or re.search(r"USING\s*\([^)]*\bmeasdate\b", s, re.I)
                or re.search(r"PARTITION BY [^)]*\bmeasdate\b", s.split(" ORDER BY")[0], re.I)):
            findings.append(f"L6 {name}:{i}: {ln.strip()[:110]}")
        if name != "fia_truth_assemble.py" and "cn-boundary" not in ln and not ln.lstrip().startswith("#") and re.search(r"\b(PREV_PLT_CN|PREV_TRE_CN|SUBP_COND_CHNG_MTRX)\b", ln) and '"""' not in ln:
            findings.append(f"L7 {name}:{i}: {ln.strip()[:110]}")
        if name in FLOAT_AGG and "order-ok" not in ln and re.search(r'pl\.col\("(agb|ba|sp_ba|_precut_ba|_precut_age|_bio|estimated_age|biomass_contrib|DIA|HT|rel_rate|counted_would_be|counted_dropped|noncounted_live_w)"\)\.(sum|mean|std)\(\)', s):
            findings.append(f"L8 {name}:{i}: {ln.strip()[:110]}")
        # COND_ID (plot key + CONDID) is a condition only together with its visit
        if re.search(r"(over|group_by|shift|partition_by)\(\s*(COND_ID|cond_id_cols)\s*\)|on\s*=\s*COND_ID\s*[,)]", s):
            findings.append(f"L2 {name}:{i}: {ln.strip()[:110]}")


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--consumers", nargs="*", default=[], help="consumer files: L1 + L3")
    a = ap.parse_args(); out = []
    for f in STAGES:
        p = os.path.join(ROOT, f)
        if os.path.exists(p): scan(p, f, out)
    keys = []   # the verifier and the current model file: L6 only (the verifier uses CNs on purpose)
    for f in ("verify_curation_v2.py", "nimiwae56.py"):
        scan(os.path.join(ROOT, f), f, keys)
    out += [x for x in keys if x.startswith("L6")]
    for p in a.consumers:
        scan(p, p, out)
        if "FIADB.duckdb" in open(p, errors="ignore").read(): out.append(f"L3 {p}: names raw FIADB")
    print("\n".join(out) if out else "lint: clean"); print(f"{len(out)} finding(s)")
    sys.exit(1 if out else 0)


if __name__ == "__main__":
    main()
