# FIA Truth

One curation pipeline and one curated DuckDB per region, read by every downstream module (DECISIONS.md D59–D61).

```bash
python -I fia_truth.py --config config/fia_truth_r4.yml                  # all stages
python -I fia_truth.py --config config/fia_truth_r4.yml --stages verify  # chain | verify | assemble | lint
```

Use the durable environment `FIA/.venv_fia`. Paths in the config are relative to `$WORKSPACE`, which defaults to the parent of `FIA/`.

| Stage | What it does | Output (R4) |
|---|---|---|
| `chain` | `run_curation_v2.sh` steps 1–10 over all the region's states in one DB, so one age-model fit scope | `FIA/truth/r4/curated.duckdb`, `simB.duckdb`, `chain.log` |
| `verify` | `verify_curation_v2.py`, independent of the pipeline code; exits 1 on any FAIL | `FIA/truth/r4/verify.log` |
| `assemble` | Merges the curated and Sim B tables and adds region-scoped raw copies; refuses to overwrite | `FIA/truth/fia_truth_r4.duckdb` |
| `lint` | Static scan for CN joins and cross-visit CONDID use in stages | `FIA/truth/r4/lint.log` |

Only this pipeline reads FIADB, always READ_ONLY. Add a region by copying the config and changing `region`, `states` and the paths; its first verifier run is fully strict because KNOWN cells are keyed by fit scope.

## Identity rules

Records are identified by their natural keys in the FIADB User Guide v9.4. CNs are never join keys.

| Record | Natural id | Across visits |
|---|---|---|
| Plot visit | STATECD, UNITCD, COUNTYCD, PLOT, INVYR | Same plot key. A key change is accepted only in `plot_renumber`, on tree evidence |
| Subplot visit | visit + SUBP | Same SUBP |
| Condition | visit + CONDID | **Never by CONDID** (it "can change at remeasurement", §2.5.8). Use `cond_link` |
| Tree | plot + SUBP, TREE | Same number under an unchanged DESIGNCD, split where the number is reused |

- **Tree-number reuse:** a record starts a new tree when it has no PREV_STATUS_CD after a dead or cut record, when it has none and a new-tree RECONCILECD, when its PREV_STATUS_CD is 0 (the number was closed), or at a lineage-break visit. Later segments are `TREE + 100000 k`, and `tree % 100000` is FIA's number.
- **Plot renumbering:** a key-changing PREV_PLT_CN counts only with at least 2 same-SUBP, same-TREE pairs, at least 90 % of them with PREVDIA within 0.05 in of the old DIA. R4: 29 of 34 accepted.
- **Quarantine:** a row whose natural key names no parent, or whose CN disagrees with its natural key, goes to `id_quarantine` and is excluded. R4 has none. Nationally the disagreements are OK and AR periodic plots plus one MO and one OR plot.
- **Verifier:** it keeps FIA's CN links on purpose, as an independent cross-check of the natural-key rules. It is the only code allowed to.

## The database

`fia_truth_<region>.duckdb` is one flat file.

- **Curated tables (`main`):** the curation's own names, including `curated_trees`, `clean_subplot_years`, `curated_cohorts_landis`, `curated_cohorts_landis_stdorg`, `curated_cohorts_landis_simB`, `tree_trajectories`, `cohort_disturbance`, `curated_snapshots`, `cond_link`, `plot_renumber`, `id_quarantine`, `run_scope`.
- **Annual raw copies (`main`):** PLOT, COND, SUBPLOT, SUBP_COND, TREE, SITETREE, SEEDLING and the REF tables, with canonical plot keys and quarantined rows removed.
- **Whole inventory (`raw`):** every visit of the region's states (periodic and nonsampled too), with as-recorded keys and quarantined rows removed. `CN`, `PREV_PLT_CN` and `PLT_CN` hold the natural visit id (digits SS UU CCC PPPPP YYYY), so a join on them is a natural-id join. FIA's values are `FIA_CN` and `FIA_PLT_CN`.

To go from a curated tree back to its raw record, use `unitcd_raw`, `countycd_raw`, `plot_raw`, INVYR, SUBP and `TREE % 100000`.

External datasets keyed by CN (TreeMap, TreeAge) are resolved once at the boundary, through PLOT, and carried on natural keys after that.

## Consumers

| Module | Reads | Entry point |
|---|---|---|
| Pan (BS, Sim B, PnET, HBAD) | `main` cohort tables, PLOT, COND, SUBP_COND | `landis_ncv` branch `fia-truth-ncv`; `landis_hbad4` branch `fia-truth-hbad4`; link `landis_region4/data_r4/fia_truth_r4.duckdb` under `PAN_DATA` |
| Genveg | `curated_*`, `raw` via `common.fiadb_con` | `standmap` branch `fia-truth` |
| NIMIWAE | `main` tables and `raw.TREE` | `nimiwae_prep.py` |
| Forest Explorer | `tree_trajectories`, cohort tables | `fe_pan` branch `fia-truth`; the user restarts the server on the host |
| Harvest models | `curated_*` | `harvest_models` branch `fia-truth` |
| TreeAge | stage 1 of `pipeline.py` | `TreeAgeTransformer` |
| PRISM extraction | `raw.PLOT` | `pan_climate/scripts/01_extract.py` |

**Exception (D61):** Forest Explorer's FVS engine reads FIA's own `FVS_STANDINIT_PLOT` and `FVS_TREEINIT_PLOT` from the master SQLite file. See D61 for why and for the key audit.

## Verification state (2026-11-01, R4)

- **Verifier:** 217 passed, 0 failed, 4 KNOWN cells (D60). Steps 4–10 rerun is EXCEPT-identical across all 37 curated and 3 Sim B tables. The steps 1–10 determinism check at two thread counts was not run on this build.
- **Lint:** clean.
- **Regression:** `docs/FIA_TRUTH_REGRESSION.md`. Records and fates are unchanged from `curated_r8`; natural-stand sapling model ages are 1–7 yr younger under the regional age fit.
- **Consumers:**

| Consumer | Check | Result |
|---|---|---|
| Pan BS, HBAD, PnET | tiny-budget nested-CV smoke, fold k1, through test | All 41 steps complete |
| Forest Explorer plots layer | real loader, summary, points and 3 plot trajectories | Works; 36,313 plots |
| Harvest models | `build_dataset.py`, `build_model3.py` | Build |
| NIMIWAE | `nimiwae_prep.py` (`--scheme r3`) from the truth DB, then `nimiwae56_verify.py` | 64 / 64; same 6,985,700 slots, 1,703,185 trees, plot splits and damage coverage as `d54c`; about 3.2 h. Data in `FIA/nimiwae_r8/truth_v1` |
| Genveg | 21 of 22 FIA queries planned against the DB | Resolve; the `table` stage has not been run |
| TreeAge, climate, fold and volume tools | queries planned against the DB | Resolve; not run |

- **Before calibrating HBAD v2, HBAD3P or HBAD4P:** rebuild their interval products. Those smoke runs were not done, and the products in `results/` still come from `slim_r4`.
