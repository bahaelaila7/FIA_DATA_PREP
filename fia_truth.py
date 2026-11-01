"""fia_truth.py — the one FIA curation driver (DECISIONS.md D59; docs/FIA_TRUTH.md).

  python -I fia_truth.py --config config/fia_truth_r4.yml [--stages chain,verify,assemble,lint]

chain     run_curation_v2.sh steps 1-10 over all the region's states into ONE curated DB (one age-model fit scope)
verify    verify_curation_v2.py on it
assemble  fia_truth_assemble.py: the single regional DB every consumer reads
lint      fia_truth_lint.py
Natural ids are the identity; raw FIADB is opened READ_ONLY here and nowhere downstream."""
import sys, os, subprocess, argparse
import yaml
HERE = os.path.dirname(os.path.abspath(__file__))
WS = os.path.abspath(os.environ.get("WORKSPACE", os.path.join(HERE, "..", "..")))


def run(cmd, log, env=None):
    print("+", " ".join(cmd), ">", log, flush=True)
    with open(log, "a") as f:
        r = subprocess.run(cmd, cwd=HERE, stdout=f, stderr=subprocess.STDOUT, env=env)
    if r.returncode:
        sys.exit(f"failed ({r.returncode}); see {log}")


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--config", required=True); ap.add_argument("--stages", default="chain,verify,assemble,lint")
    a = ap.parse_args(); c = yaml.safe_load(open(a.config)); P = lambda k: os.path.join(WS, c[k])
    work = P("work_dir"); os.makedirs(work, exist_ok=True); states = ",".join(map(str, c["states"]))
    cur, sb = f"{work}/curated.duckdb", f"{work}/simB.duckdb"
    for st in a.stages.split(","):
        if st == "chain":
            run(["bash", "run_curation_v2.sh"], f"{work}/chain.log",
                dict(os.environ, PY=sys.executable, FIADB=P("fiadb"), FIA_CURATED_DB=cur, FIA_SIMB_DB=sb, FIA_ESTAGE_DIR=P("estage_dir"), FIA_STATES=states,
                     TO_STEP="10", FIA_DUCKDB_THREADS=str(c["threads"]), FIA_DUCKDB_MEMORY=str(c["memory"])))
        elif st == "verify":
            run([sys.executable, "-I", "verify_curation_v2.py", "--curated-db", cur, "--simb-db", sb, "--fiadb", P("fiadb"), "--states", states], f"{work}/verify.log")
        elif st == "assemble":
            run([sys.executable, "-I", "fia_truth_assemble.py", "--curated-db", cur, "--simb-db", sb, "--fiadb", P("fiadb"), "--out", P("output"), "--states", states], f"{work}/assemble.log")
        elif st == "lint":
            run([sys.executable, "-I", "fia_truth_lint.py"], f"{work}/lint.log")
        else:
            sys.exit(f"unknown stage {st}")


if __name__ == "__main__":
    main()
