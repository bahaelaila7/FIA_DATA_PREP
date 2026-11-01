# FIA Data Prep Pipeline

Python pipeline that curates USFS FIA forest inventory data into the format
consumed by the Pan/LANDIS-II parametrization engine (Julia project at
`../landis_parametrization_julia/`).

Database paths are explicit arguments since curation-fix 2026-10-26 (see that section); the raw
FIADB is `../FIASQLITE2PGSQL/FIADB.duckdb`, opened READ_ONLY.

---

## FIA Truth (D59): start here

The curation now runs as ONE pipeline into ONE DB per region: `python -I fia_truth.py --config config/fia_truth_r4.yml`
(see `docs/FIA_TRUTH.md`). Natural ids are the identity; CNs are never join keys and `fia_truth_lint.py` enforces it.
CONDID is scoped to a visit: link conditions across visits with `cond_link`. Consumers read the truth DB, never FIADB.
Text below on per-state DBs, slim builds and CN-based splits (PREV_TRE_CN, PREV_PLT_CN) is history.

---

## Pipeline sequence

```
fia_curation_v2.py   →   age_imputation.py   →   cohort_tracking.py   →   cohorts_landis.py
```

Each stage reads from and writes back to the DuckDB file.

---

## curation-fix 2026-10-26 (branch `curation-fix`) — supersedes the rules described below

An audit found the old tree curation dropped ~half the live saplings in AL/GA/NC/SC/TN (kept TPA
share AL 0.50, GA 0.53, NC 0.42, SC 0.49, TN 0.46 vs FL 0.93, MS 0.96). Root cause:
`sql_excluded_trees` excluded EVERY record of a tree that ever had RECONCILECD 3/4/6/7/8/9 or
STATUSCD 0, with wrong labels. Per FIADB v9.4 §3.1.82: 3 = missed live, 6 = physical movement,
7 = cruiser error, 8 = procedural change, 9 = nonsampled area. Several states re-tallied their
microplot saplings as new RC3/RC8 trees at the first annual remeasurement.

### Paths: explicit, read-only FIADB (`dbconn.py`)
Every stage takes `--out-db` (written) and `--fiadb` (ATTACHed READ_ONLY); side-DB stages (sim B)
also take `--curated-db` (READ_ONLY). There are no default paths (env fallbacks `FIA_OUT_DB`,
`FIADB`, `FIA_CURATED_DB`, `FIA_STATES`). Unqualified raw tables (all-UPPERCASE FIADB tables
except `DATA_*`, plus `data_plot_eco` and `species_longevity_ref`) resolve to TEMP views over
`fia.main.*`. The OLD curated tables still inside FIADB (curated_trees, clean_subplot_years,
tree_trajectories, curated_cohorts_landis*, ...) are NOT exposed: a reference to a curated
table that is missing from the output DB fails with a CatalogException. This is an explicit
allow-list rather than `search_path`, which would expose them. Helpers: `table_exists`,
`column_names` and `require_tables` look only at the output DB and the temp views, never at
`information_schema` (which lists the attached catalogs). Each script carries a two-line
`sys.path` bootstrap so sibling imports work under `python -I`.

### Corrected rule set (fia_curation_v3.py)
| Old rule | New rule |
|---|---|
| exclude all records of trees with RC 3/4/6/7/8/9 or STATUSCD 0 | drop only the STATUSCD 0 record itself. STATUSCD 0 + RC7 ("erroneously tallied previously") → drop the tree's EARLIER records (`excluded_tree_ids`, now only these). STATUSCD 0 + RC 5/6/8/9 (or null) → **censoring**, NOT death or removal: `censored`, first event `censor_measdate`/`censor_reconcilecd`/`censor_reason` (shrank/moved/procedural/nonsampled/not_in_sample), last event `censor_last_*` (a tree can be censored → live → censored; fates use the last). Every STATUSCD 0 record: `tree_status0_events`; per tree: `tree_reconcile_events`. New trees with RC 3/4/6/7/8/9 are kept as live trees, flagged `reconcile_new` (code) / `not_ingrowth` |
| — | `procedural_breaks`: per (PLOT, MEASDATE) re-tally visit, `break_kind` `rc8` (STATUSCD 0 + RC8 closures; keyed per plot because the AL/GA MANUAL 1.5–2.0 sapling re-tally was plot-wide) or `unrecorded` (silent re-tally). The silent detector (`_sql_procedural_breaks`) works on the previous visit's live saplings (SUBP ≤ 4, own COND_STATUS 1, subplot SUBP_STATUS 1 at the next visit). Plot tier: ≥ 3 and ≥ 50 % have NO record at the next visit. Transition tier: the state × (prev MANUAL, next MANUAL) loses ≥ 50 % of ≥ 20 pooled saplings, and the plot-visit has ≥ 1 unrecorded sapling. Unrecorded saplings at any break are **censored** (`procedural_unrecorded`). `retally_transitions` holds the per-transition shares. In R4, KY 1.0→4.0 (78 % unrecorded) and TN 1.0→4.0 (89 %) are silent re-tallies; the largest residual share in any R4 transition after the rule is 0.27 % |
| `handle_reconcile5` revived RC5 rows as live with DIA 5.0 / TPA 6.018 | not revived (it would fabricate records). RC5 is a censoring reason. `reconcile5_*` columns are kept for schema compatibility (no imputation; `reconcile5_first_measdate` = RC5 date) |
| subplots with ≥2 clean visits only | no visit-count filter; `n_clean_visits` column (downstream filters itself) |
| drop a subplot-visit if ANY condition has COND_STATUS_CD 3 | kept (3 = noncensus water, not nonforest). `clean_subplot_years` gains `has_cond_status3`, `forest_subpcond_prop`, `forest_micrcond_prop` |
| TPA_UNADJ > 0 on every record | **live** records: TPA_UNADJ > 0 on their OWN condition with COND_STATUS_CD = 1. **Dead/cut** records: kept on COND_STATUS 1–4 (nonforest and water too; 193 AL records on 3/4) even with a NULL TPA. FIADB leaves TPA null on many mortality records of previously-live trees, and dropping them turned real deaths into inferred removals. A dead record's TPA is never used in a live total |
| `SUBP_STATUS_CD <> 3` | **SUBP ≤ 4** (the national design; SUBP > 4 rows are auxiliary/regional points with NULL status), `SUBP_STATUS_CD IS DISTINCT FROM 3`, P2A records (`P2A_GRM_FLG='Y'`) excluded explicitly. The annual survey filter and the REF_SPECIES join are unchanged |
| anchor = smallest "max-age violation" (picked the youngest candidate 78 % of the time, −2.7 yr) | anchor age = **median** of the candidates' implied ages at first measurement. Anchor date = the candidate closest to the median. The "crew was there and didn't see it" bound is not used for selection (it is wrong for every new-tree class). `max_viol` is kept as a diagnostic |
| estage (Tree_Age) used at any size | an estage record is a candidate only if **DIA ≥ 5 in and Tree_Age > 1** (`ESTAGE_MIN_DIA`/`ESTAGE_MIN_AGE`). About 42 % of AL live 1–3 in saplings that have a Tree_Age carry Tree_Age ≤ 1 (≈45 % by the auditor's count), with a hard step at 5 in. Saplings get their age from a later ≥5 in record of the same tree (propagated back) or from the age model. `estage_raw` keeps the raw value. **Field scale (I2, user decision)**: every age follows the FIA field scale (TOTAGE, or STDAGE where the tree qualifies), and every estage candidate uses `estage_mapped` = f_g(Tree_Age); see "Field-age scale" below |
| `final_tree_fate` was computed and discarded | returned: FIRST dead/cut fate (r3) · `censored_<last reason>` · `diverted_landuse` · `disappeared_inferred_removal` · `alive`. `diverted_landuse` = the subplot is revisited but the land left forest: a later live record on a nonforest/water condition (`tree_landuse_exits`), or the next visit is entirely nonforest. `dead_at_first_visit` flags a tree that was already dead at the subplot's first visit (no observed mortality) |
| harvest_inferred for any STATUSCD 2 on a TRTCD=10 condition without explicit cut markers | only when **AGENTCD is NULL**. A recorded agent 10–70 is FIA's own mortality call: all 152 AL harvest_inferred vs GRM MORTALITY disagreements had one. GRM CUT→harvest / MORTALITY→dead agreement is now 1.0000 (AL, KY) |
| snapshots by raw SPCD | `curated_snapshots` grouped by `spcd_resolved` / `spgrpcd_resolved` |
| — | `spgrpcd_resolved`; area proportions for consumer-side normalisation (`cond_condprop_unadj`, `cond_subpprop_unadj`, `cond_micrprop_unadj`, `cond_macrprop_unadj`, `subp_subpcond_prop`, `subp_micrcond_prop`, `subp_macrcond_prop`, `subp_forest_*_prop`, `subp_has_cond_status3`, `cond_status_cd`) |

The 35 new curated_trees columns (incl. `estage_mapped`, `age_domain`, `rerecord_dead`, `tree_key_split`, `prev_status_cd`, `prev_tre_cn`; r5: `age_stratum`, `unitcd_raw`, `countycd_raw`, `plot_raw`; r6: `misrecorded_death`) are APPENDED after all old columns (`FIX_COLUMNS`), so the old
schema (names, types, order) is an exact prefix. The polars steps run one state at a time. Every
rule is tree-, subplot- or condition-local, so this matches a single pass. Only the diagnostic
`spcd_confusion_cluster` id is state-local. `age_imputation` fits its model once over all states
in the DB, then applies it state by state. It now imports from **v3**: it used to import v2's
anchor selection. Model anchors use `apply_min_shift=True`: the age at the first measurement is
floored to 1 yr, so AL has 0 negative live ages (was 72). Step 2 is **resumable**: if
`age_from_model` already exists, it first restores the v3 age columns (estimated_age =
TOTAGE > STDAGE, and age_source) and drops the step-2 columns. A `FROM_STEP=2` rerun gives
EXCEPT-identical `curated_trees` / `curated_snapshots` (tested on AL).

### Downstream changes
- **Cohort species** come from `spcd_resolved` / `spgrpcd_resolved`: `cohorts_landis`,
  `cohorts_landis2`, `cohort_tracking`, `build_tree_trajectories`.
- **Phantom backfill blocks** (`cohorts_landis.apply_backfill_blocks`, used by the tree- and
  cohort-level passes and by `build_tree_trajectories`). No phantom at prior visit p for a tree
  debuting at D when:
  - (a) the tree entered as RC 6/9 (it was outside the sampled population);
  - (n) the subplot was entirely nonforest at p, or at any visit between p and D: backfill stops
    at the latest nonforest visit. Candidate prior visits and the nonforest lookup come from the
    FULL `clean_subplot_years` scaffold. sim_year is re-based so phantoms before the subplot's
    first real row keep sim_year ≥ 0 (r5 M-1: no re-base any more; every table's axis is
    anchored at the subplot's first scaffold visit, so sim_year ≥ 0 by construction);
  - (b) the PLOT had a procedural re-tally (`procedural_breaks`) at b with p < b ≤ D:
    - the sapling-debut part uses only breaks with closed-RC8 saplings, `break_kind =
      'unrecorded'`, or unrecorded saplings;
    - the RC8-entry part uses RC8 closure breaks. Unpaired RC8 entries elsewhere (e.g. FL
      MANUAL 9.0 species-definition additions) create no break;
  - (c) cohort tables only, not per-tree trajectories: the tree debuted at ≥ 5 in as true
    ingrowth (RC 1 / null) and p is at or before the latest prior visit with a sampled forest
    microplot. That sapling class is already represented at TPA ~74.965; scaling the TPA-6.018
    subplot debut back double counts it. RC2 through-growth (a seedling before) and RC3/7/8
    entries are exempt.
  - **cohort pass, per member**: a cohort-level slot (ckey, p) is created only if at least one
    MEMBER tree survives its own blocks for that same p. That means (member, p) is an allowed
    tree-level slot, or p is after the member's debut (a missed visit), on forest, with no
    censoring in (p, t]. Before, a slot was blocked only when ALL members carried the same
    flag, so mixed cohorts passed: 1,034 of the 1,039 AL cohort-pass phantoms came from
    individually blocked trees. Source tables: `phantom_tree_sources` (tree pass) and
    `phantom_cohort_sources` (cohort pass, with the surviving member ids).
  `check_cohort_continuity` therefore reports violations; they are expected and are not errors.
- **Determinism**: every `unique(keep=...)` sorts by its full key first (tie-breaks added in
  `cohort_tracking` and the rate table). Float group means and sums (sapling rates, cut-event
  shares, dominant_species_frac) are rounded. Re-running steps 4–7 on the same input gives
  EXCEPT-identical Sim A and trajectory tables (AL, KY).
- **`procedural_break`** (BOOL) is APPENDED as the last column of `curated_cohorts_landis`,
  `_stdorg` and `_simB`. It is TRUE on rows at a re-tally visit, so consumers can tell a
  re-tally from cohort death plus establishment. The slim cohort tables therefore gain one
  appended column; all old columns are unchanged.
- `cohorts_landis2.py`: MEASDATE replaces MEASYEAR; this script is used with `--attach-only`.
  The production `curated_cohorts_landis` comes from `cohorts_landis.py` (v1). The v2 full
  build has no backfill blocks.
- `build_tree_trajectories`:
  - terminal fate `censored_<last reason>` or `diverted_landuse` (it used to be
    `disappeared_inferred_removal`);
  - gap interpolation never spans ANY censoring event (`tree_censor_events`, which includes
    `procedural_unrecorded`);
  - dead records with a null TPA end trajectories;
  - output gains `reconcile_new`, `censor_reason`.
  `cohort_disturbance` never counts censoring or `diverted_landuse` as a disturbance drop (they
  are not in DISTURB_FATES).
- The land-use 2-way change to `build_tree_trajectories.py` in the main checkout is uncommitted
  and is NOT included. It does not affect the slim tables, because `build_stdorg.py` re-derives
  `land_use`.

### Chain (`run_curation_v2.sh`, all paths from env)
1 `fia_curation_v3` → 2 `age_imputation` → 3 `cohort_tracking` → 4 `cohorts_landis` →
5 `build_tree_trajectories` → 6 `cohorts_landis2 --attach-only` → 7 `build_stdorg` (+ `procedural_break`) →
8 `export_reference_tables` (COND/PLOT of the states + REF_SPECIES/species_longevity_ref/data_plot_eco
copied into the curated DB, so the slim builder can use it as its `--fiadb`) → 9 `cohorts_landis_simB` (side DB) →
10 `attach_disturbance_simB` (side DB; + `procedural_break`) → 11 slim builder (optional).
- **Overwrites and resume**: a fresh run refuses to overwrite outputs. `FROM_STEP=N` resumes
  on the partial DBs; every python step rewrites its own tables. Delete a partial `SLIM_OUT`
  before resuming at step 11.
- `TO_STEP=N` stops after step N (used for the determinism rerun of steps 4–7).
- **Slim step**: needs an explicit `JULIA_DEPOT_PATH` (first entry = a writable depot you own).
  `slim_guard.py pre` also requires every `FIX_COLUMNS` column on curated_trees and
  `procedural_break` on both cohort tables.
  It runs the UNCHANGED `landis_region4/tools/build_slim_db_region.jl` inside `slim_guard.py`
  pre/post checks: both sources exist, are not FIADB and hold every table; after the build all
  8 tables exist and are non-empty. This is needed because the builder defaults to the old
  FIADB and simB and silently skips a missing simB.
- **dbconn guards**: `--out-db` must not be FIADB or a link to it (`os.path.samefile`), and
  `--curated-db` must not be FIADB.
- **r5**: `SLIM_REF` (optional) = a reference slim DB, read-only; `slim_guard.py post` then
  requires its schema to be an exact prefix of the new slim's. The determinism rerun covers
  steps 4–10, into a second Sim B DB (`--determinism-simb-db`).
- **Verification**: `verify_curation_v2.py` checks live coverage against an INDEPENDENT raw
  reference (live, SUBP ≤ 4, TPA > 0, own COND_STATUS 1, PLOT_STATUS < 3, SUBP_STATUS ≠ 3,
  annual, not P2A, not before an RC7 erroneous tally). It also checks: SUBP > 4 rows, lost
  dead records, nonforest phantoms, phantoms crossing a re-tally, sapling ages by DIA class,
  record counts, CN uniqueness, the ≥5 in delta and the slim schema. `--counties
  STATECD:COUNTYCD` gives subset runs. Independent PASS/FAIL checks (re-derived from raw
  FIADB and the spec, without importing pipeline code):
  - (a) no cohort-pass phantom whose members are all individually blocked;
  - (b) residual unrecorded saplings per state × MANUAL transition and per plot-visit stay
    below the re-tally rule, and only truly unrecorded saplings are censored;
  - (c1/c2/c3) fates from the FIRST raw dead/cut record, re-records never produce events,
    land-clearing cuts are `diverted_landuse` (r3; replaces the circular GRM check);
  - (d) median age monotone in DIA class (1–3/3–5/5–7/7–9 in) for the top-25 species;
  - (h) pre-registered age step (3–5→5–7 in ≤ 2 × the 5–7→7–9 step + 2 yr), enforced for
    planted strata with ≥ 50 condition-visits; INFO otherwise (see the decision above);
  - (e) determinism with `--determinism-db`;
  - (f) dead records of every raw-reference tree;
  - (g) phantom sources individually blocked, for the Sim A tables and trajectories.
- **Python**: the repo `.venv` points at a removed python3.11, so use any env with duckdb
  1.5.4 / polars 1.41.2 / scikit-learn 1.9.0 (requirements.txt).

### Subset checks after the audit fixes (one county each: AL 89, GA 145, NC 37, FL 29)
Live TPA coverage of the independent reference is **1.000 for saplings and ≥5 in in all four**.
Old coverage, saplings / ≥5 in: AL 0.350 / 0.940 · GA 0.387 / 0.934 · NC 0.341 / 0.942 ·
FL 0.930 / 0.953.

The following hold in all four counties:
- 0 rows on SUBP > 4;
- 0 dead/cut records lost (the null-TPA ones are kept);
- 0 phantoms on nonforest subplot-visits;
- 0 sapling-debut phantoms across a re-tally;
- CN and tree × INVYR unique;
- slim schema: old columns identical, appended columns only (`curated_trees` +23, cohort
  tables +`procedural_break`).

| County | Live ≥5 in records | Live sapling age, 1–3 in (old → new) | Saplings with age ≤ 1.5 yr | Sim A phantom rows | Sim A real rows |
|---|---|---|---|---|---|
| AL 89 | +5.9 % | median 6.2 → 17.9 yr | 26 % → ~0 % | 835 → 206 | 3,736 → 4,389 |
| GA 145 | +6.9 % | median 6.4 → 12.9 yr | 33 % → ~0 % | 2,370 → 384 | 7,557 → 9,281 |
| NC 37 | +5.3 % | median 4.8 → 15.2 yr | 39 % → ~0 % | 1,472 → 402 | 6,081 → 7,829 |
| FL 29 | +4.8 % | median 6.2 → 17.4 yr | 40 % → ~0 % | 1,968 → 315 | 6,823 → 7,183 |

Sapling ages are now about 98 % model-based. Single-county subsets train the age model on only
~1k rows: AL 89 shows 3–5 in saplings (median 36.6 yr) older than 5–7 in trees (31.3 yr).
Check the sapling-age monotonicity on the full run.

### Field-age scale for estage (I2, user decision 2026-10-26)
The truth is the FIA field age: `age_from_totage`, else `age_from_stdage` (eligible STDAGE:
planted / post-reset, FLDSZCD gate). The external TreeAge (Zenodo 14775738) is mapped onto that
scale in `age_imputation.py` (step 2): `calibrate_estage` → `remap_estage`. v3's estage anchors
are interim, on the raw scale.
- **Calibration set**: LIVE records with DIA ≥ 5 in, Tree_Age > 1 and a field age at the same
  measurement. AL: 218,707 records; KY: 55,757. All of them are STDAGE; AL and KY have no TOTAGE.
- **Mapping f_g**:
  - isotonic regression in log1p space, so it is monotone;
  - clamped to the knots, then extended linearly in log1p with the group's OLS slope, clipped
    to [0.25, 1.5];
  - a per-curve Duan smearing factor on (1 + age) removes the log back-transform bias. Without
    it the AL CV bias was −2.9 yr; it stays monotone.
- **Groups**: `spgrpcd_resolved` → `major_spgrpcd` → global, with a fallback whenever a group
  has fewer than 200 records. Within a group, planted vs natural are split only when both strata
  have ≥ 200 records. The split is kept when it lowers CV MAE: AL 9.28 vs 9.90 pooled (it
  matters); KY 13.41 vs 13.42 (it doesn't).
- **Fit**: once over the whole DB, with plot-grouped 5-fold CV. Out-of-fold vs field age:

  | State | Raw Tree_Age MAE / bias | Mapped MAE / bias |
  |---|---|---|
  | AL | 12.85 / +2.81 yr | 9.28 / +0.11 yr |
  | KY | 20.97 / −13.94 yr | 13.41 / +0.26 yr |

  Note that in KY the raw TreeAge runs YOUNGER than the field age.
- **Outputs**: `estage_mapped` (field-scale value on valid candidates) is appended to
  curated_trees. The `estage_calibration` table holds one row per curve: level, group,
  stratum, n, OOF MAE/bias, raw MAE/bias, extension slope, smearing factor, and the knots on
  both scales.
- **Model** (r3): trained on the FIELD age where present (v3 estimated_age = TOTAGE /
  eligible STDAGE), otherwise on `estage_mapped` of live ≥ 5 in records (Tree_Age > 1). Both
  are on the field scale. The mapping is fitted first, the model second, both once over the DB.
  - **Features**: adds condition-level stand structure (`cond_fldszcd`, `cond_stdszcd`,
    `cond_stdorgcd`); `CCLCD` was already in.
  - **Trim**: the p90 upper trim is applied PER SOURCE. The sources span different ranges, so
    a pooled p90 would cut mostly the older mapped ages.
  - **CV**: plot-grouped, with the SAME crc32 plot→fold assignment as the mapping CV
    (`plot_fold`). For fold k the mapping is refitted without fold-k plots, the mapped training
    targets are recomputed with it, and the held-out rows are scored against their field age
    or their out-of-fold mapped age, so there is no leakage through the mapping.
  - **`age_domain`** (appended; null for non-model ages):
    - `natural_treeage_scale` (r4; was `natural_uncalibrated` before r4): the model age is in
      a natural condition (STDORGCD ≠ 1 or unknown). Since r4 it is on the raw TreeAge scale,
      see "Round-4" below;
    - `out_of_support`: planted, and the record falls in a DIA 2-in class × stratum ×
      FLDSZCD (1 vs other) cell with < 50 training rows;
    - `in_support`: otherwise.
    - `known_bias_fl_planted_model_10plus` (r5; overrides the values above, only in the pooled
      FL + KY + MS fit scope): see "Round-5" below. The r4 flags `known_bias_ky_natural_treeage`
      (D13) and `known_bias_fl_planted_model_15plus` (D24) are retired.
    The column flows into the slim DB, because the slim builder copies `curated_trees.*`.

Results (AL, KY whole-state; all verifier checks pass in both, incl. (d) monotone ages: 0/25 and 0/25):
- **Planted loblolly (AL), median age by DIA class 1–3 / 3–5 / 5–7 / 7–9 / 9+ in**:

  | Source | 1–3 | 3–5 | 5–7 | 7–9 | 9+ |
  |---|---|---|---|---|---|
  | Field STDAGE | 6.0 | 8.9 | 13.1 | 16.3 | 23.0 |
  | Mapped estage | 7.3 | 8.8 | 13.8 | 18.1 | 23.8 |
  | Raw Tree_Age (≥5 in only) | – | – | 18.0 | 24.0 | 39.0 |
  | Final age | 6.9 | 9.5 | 13.9 | 17.8 | 23.7 |

  Final minus field is +0.7 to +1.1 yr.
- **Same condition-visit DIA-class steps** (median, yr): 3–5→5–7 in vs 5–7→7–9 in:

  | Stand / species | Before | After |
  |---|---|---|
  | AL planted loblolly | 4.6 vs 2.1 | 1.4 vs 1.1 |
  | AL natural loblolly | 4.4 vs 3.8 | 6.1 vs 2.8 |
  | AL planted, all species | 4.9 vs 3.5 | 2.0 vs 1.2 |
  | AL natural, all species | 6.1 vs 6.6 | **14.8 vs 3.1** |
  | KY natural, all species | – | **19.8 vs 2.8** |

- **r3 (model trained on field + mapped ages)**:
  - **Out-of-fold model MAE / bias**:

    | State | Field rows | Mapped rows |
    |---|---|---|
    | AL | 5.12 / −0.12 yr (n 295,423) | 5.52 / −3.66 yr (n 111,869) |
    | KY | 10.40 / −0.17 yr (n 65,532) | 3.99 / −1.48 yr (n 90,323) |

  - **Pre-registered check (h)**, step(3–5→5–7 in) ≤ 2 × step(5–7→7–9 in) + 2 yr, per state
    × stratum, all species:

    | | Step (yr) | Limit (yr) | Result |
    |---|---|---|---|
    | AL planted | 1.6 | 4.4 | PASS |
    | AL natural | 12.5 | 8.1 | **FAIL** |
    | KY natural | 14.5 | 7.4 | **FAIL** |
    | KY planted | 6.4 | 4.5 | **FAIL** (only 31 condition-visits) |

  - In natural stands the model now matches mapped estage at 5–7 in (AL 30.7 vs 33.9 yr; KY
    51.8 vs 53.3), but 3–5 in saplings stay well below. Even mapped estage alone steps about
    9 yr from 3–5 to 5–7 in there (AL 24.6 → 33.9; KY 44.4 → 53.3).
  - Per the pre-registration, nothing was tuned further. Check (d) passes in both states
    (0/25 non-monotone).
  - Planted loblolly: final age is field + 0.4 to 1.0 yr. Age shares are unchanged
    (≥ 5 in: estage 92–97 %; saplings: model 95–97 %).
  - `age_domain` out_of_support: AL 6, KY 172 model-aged records.
  - The step-2 resume is still EXCEPT-identical.
- **Decision (user, ACCEPT + FLAG): natural-stand ages are kept as of `1d6397b`.** The step
  test assumes saplings and 5–7 in trees form ONE growth sequence. That holds in even-aged
  planted stands but not in uneven-aged natural stands, where understory saplings are not the
  younger stage of the 5–7 in trees. The step exists even among TreeAge-consistent saplings
  there: about 9 yr with mapped estage alone (AL 24.6 → 33.9; KY 44.4 → 53.3).
  - **Numbers**: the remaining natural-stand step (3–5→5–7 vs 5–7→7–9 in) is **AL 12.5 vs
    3.0 yr** and **KY 14.5 vs 2.7 yr**.
  - **Check (h) handling**: it is ENFORCED only for planted strata with ≥ 50 condition-visits
    in both steps (`H_MIN_VISITS`). Natural strata, and planted strata below 50 (KY planted:
    31), are reported as INFO.
  - **Flag**: natural model ages carry `age_domain = 'natural_uncalibrated'`. Natural stands
    have no field-age truth outside post-reset STDAGE stands.
  - **Verifier**: all checks PASS in AL (14 PASS, 1 INFO) and KY (13 PASS, 2 INFO).
  - **`age_domain` counts, live model ages**:

    | | natural_uncalibrated | in_support | out_of_support |
    |---|---|---|---|
    | AL | 112,434 | 49,737 | 5 |
    | KY | 53,535 | 235 | 170 |

    Slim counts are identical.
- **Open issue (natural stands; before r3)**: model-aged saplings sit far below the mapped-estage timber
  in the same stand (AL natural 5–7 in: model 23.6 vs mapped estage 33.9 yr). The field-age
  truth for NATURAL stands comes only from post-reset (young) STDAGE-eligible trees, so both
  the model and the mapping extrapolate into uneven-aged natural stands, and they disagree there.
- **Age-source shares** (live records): AL ≥5 in: estage 92.4 %, model 7.6 %; saplings: model
  95.0 %, estage 5.0 %. KY ≥5 in: estage 96.6 %, model 3.4 %; saplings: model 96.9 %,
  estage 3.1 %. TOTAGE and STDAGE are 0 % as final sources: there is no TOTAGE, and STDAGE is
  the last fallback.
- **Resume**: a step-2 rerun (`FROM_STEP=2`) stays EXCEPT-identical, including
  `estage_calibration`.

### Round-2 audit: whole-state checks (AL = 1, KY = 21; steps 1–11 + verify)
| Check | AL | KY |
|---|---|---|
| Live TPA coverage (independent reference), saplings | 0.434 → 1.000 | 0.767 → 1.000 |
| Live TPA coverage, ≥ 5 in | 0.939 → 1.000 | 0.937 → 1.000 |
| GRM agreement | 1.0000 of 107,661 | 1.0000 of 22,516 |
| Determinism (EXCEPT both ways) | 0 / 0 | 0 / 0 |
| Censoring events (procedural / nonsampled / shrank / procedural_unrecorded / moved) | 23,059 / 2,642 / 100 / 95 / 56 | 2,863 / 4,338 / 81 / 1,203 / 49 |
| Sim A phantoms | 25,735 → 33,232 (tree 33,170 + cohort 62) | 17,717 (tree 17,686 + cohort 31) |

- Failed checks: AL 0. KY 1 before the field-scale mapping (check (d): 11/25 species
  non-monotone); 0 after it.
- The procedural_unrecorded censorings flow to `final_tree_fate` / `terminal_fate`
  `censored_procedural_unrecorded`.
- The AL cohort pass went from 1,039 phantoms to 62. The tree pass grew because candidates now
  come from the full scaffold.

### Round-3 audit (2026-10-26; GA/TN/FL audit, whole-state AL, KY, GA, TN, FL)
- **I1 — re-records.** A dead/cut record with PREV_STATUS_CD = 2, or any dead/cut record
  after the tree's first one, is a re-record of an earlier death. It is kept but flagged
  `rerecord_dead`.
  - Every fate, cause and harvest rule uses the FIRST dead/cut record: `final_tree_fate`,
    `terminal_fate` / `death_cause` / `row_disturbed` (trajectories), and `harvest_inferred`,
    which is never applied to a re-record. Explicit-harvest condition-years count first
    records only.
  - Re-records never create events: cohort_tracking interval events (hence `subp_has_dstrb`),
    cut events, snapshots fate counts.
  - Before → after:

    | | GA | TN |
    |---|---|---|
    | Re-records labelled harvest_inferred | 54 → 0 | 58 → 0 |
    | Trees last-record dead → first-record harvest_explicit | 345 | 211 |
    | Trees last-record dead → first-record harvest_inferred | 730 | 413 |
    | Sim A `subp_has_dstrb` subplots | 12,977 → 11,960 | 4,797 → 4,198 |

    The auditor's measure of subplots flagged only by re-records was 221 in GA. The verifier
    (c2) now finds 0 `subp_has_dstrb` subplots without a first dead/cut record.
- **I2 — STAND-REPLACING resets only.** A reset is either an STDAGE drop with ≥ 50 % loss of
  live trees or live BA since the previous visit, or TRTCD 10 together with TRTCD 30/40 or
  with that loss. TRTCD 10 alone (also at a condition's first visit) and STDAGE drops without
  the loss are not resets. Pre-reset trees never inherit STDAGE (the `pre_reset & ~survived`
  branch is gone).
  - **Resets by reason** (stdage_drop_with_loss / cut_with_regen / cut_with_loss; not resets:
    drop-only / TRTCD-10-only):

    | State | Resets | Not resets |
    |---|---|---|
    | AL | 1,518 / 2,310 / 2,030 | 974 / 1,918 |
    | KY | 204 / 214 / 179 | 837 / 695 |
    | GA | 2,182 / 2,366 / 2,889 | 1,420 / 3,189 |
    | TN | 413 / 478 / 429 | 992 / 804 |
    | FL | 766 / 612 / 912 | 499 / 720 |

  - **Calibration set** (live ≥ 5 in with Tree_Age > 1 and a field age), planted vs natural
    post-reset:

    | State | Planted | Natural post-reset | Old set (planted / reset) |
    |---|---|---|---|
    | AL | 81.9 % | 18.1 % | – |
    | KY | 28.2 % | 71.8 % (n 5,401) | – |
    | GA | 92.4 % | 7.6 % | 147,846 / 101,991 |
    | TN | 63.1 % | 36.9 % | 13,430 / 57,488 |
    | FL | 97.2 % | 2.8 % | 62,722 / 26,596 |

  - **Within-condition-visit SD**, field age vs raw TreeAge vs DIA: the field age is a stand
    age (SD 0.0–0.4 yr), while TreeAge has SD 3.6–14.8 yr and DIA 1.3–3.4 in.
- **I3 — age model.**
  - **Outlier rule**: the per-source p90 trim is replaced by a robust rule (drop only targets
    > p99.5 of the source, or > 3× the source's DIA-class median). The CV is scored on ALL
    rows; `age_model_cv` holds MAE and bias by age decile and DIA class. The young bias of the
    oldest field decile remains (AL d10 −18.4 yr, GA −16.8 yr; mapped d10 −6.2 / −8.0).
  - **TreeAge coverage**: every tree with a valid TreeAge record at ANY measurement carries the
    propagated estage on all its records (check (i), 0 exceptions). The model share of ≥ 5 in
    records in 2017+ (16–22 %) and GA 1997–98 (57 %) is trees that have no TreeAge record at
    all.
  - **Pre-registered (j)**, median model − mapped-estage gap within ±3 yr per state × stratum ×
    DIA class (≥ 200 trees): passes everywhere except **KY planted 5–10 in (−3.2, 512 trees)
    and FL planted 15+ in (−5.1, 357 trees)**. These are reported as FAIL and were NOT tuned.
- **I6 — independent age truth (SITETREE).**
  - **Linkage**: AGEDIA = breast-height age. SITETREE has no TRE_CN, so trees are linked by
    PLT_CN + SUBP + TREE with the same SPCD and |ΔDIA| ≤ 1 in, else by the unique live tally
    tree with the same PLT_CN + CONDID + SPCD + DIA. About 20 % of aged site trees link.
  - **Truth**: AGEDIA + 2 yr (softwood) / 5 yr (hardwood) to breast height. This is an
    audit-specified rule of thumb; FIADB has no years-to-breast-height field.
  - **Selection bias**: site trees are dominant / codominant, free-growing trees.
  - **Results**, median bias of the final age (raw TreeAge in brackets):

    | State | Planted | Natural |
    |---|---|---|
    | AL | −0.9 (raw +12.0), n 1,253 | **−15.5** (raw +2.0), n 3,648 |
    | KY | +1.5, n 17 | −5.8 (raw −5.0) |
    | GA | +1.9 (raw +7.0) | −6.7 (raw +4.0) |
    | TN | −0.3 (raw +7.0) | −7.0 (raw −1.0) |
    | FL | +0.8 (raw +5.0) | −3.3 (raw 0.0); mapped MAE 12.2 vs raw 10.0 |

  - **Correction (r5, D13)**: the D5 evidence summary "natural raw TreeAge −1 to +4 yr" was
    wrong (KY was already −5.0 in this table; MS is +6.0). Raw TreeAge in natural stands splits by
    wood group against bored ages: softwoods run OLD (NC +6.4 / +7.9, SC +5.7 / +6.3, MS +7.0 /
    +4.2 yr), hardwoods YOUNG (NC −2.9, SC −1.4, KY −6.4). Stratum medians pass only where the two
    cancel, and 72–92 % of links are proxies. Natural (k) is therefore INFO by state × wood group
    × link method since r5.
  - **Reading**: planted strata are enforced (|bias| ≤ 5) and pass. In NATURAL stands the
    field-scale ages are biased young against bored ages, while the raw TreeAge is closer: the
    field-age truth available for natural stands (post-reset STDAGE) does not represent
    uneven-aged stands. This is INFO under the ACCEPT + FLAG decision, and these are the
    records marked `natural_uncalibrated`.
- **I4**: `n_trees_natural_treeage_scale`, `share_natural_treeage_scale` (AGB-weighted; named
  `*_natural_uncalibrated` before r4) and
  `age_source_major` (dominant source by AGB), and since r4 ACCEPT + FLAG `n_trees_known_bias` /
  `share_known_bias`, are appended to curated_cohorts_landis, _stdorg and _simB, before
  `procedural_break`. Phantom rows inherit them from their source tree. The
  slim builder carries them; old columns are unchanged.
- **I5**: every MEASDATE is `MAKE_DATE(MEASYEAR, COALESCE(NULLIF(MEASMON,0),6),
  COALESCE(NULLIF(MEASDAY,0),15))`. Periodic plots have MEASDAY = 0, which crashed the GA
  verifier. The verifier's raw TREE ⋈ PLOT reads also carry the annual-survey filter.
- **Check changes**:
  - (b) is threshold-independent: in EVERY transition with ≥ 20 saplings, at most 5 % may be
    unrecorded and uncensored. The maximum in AL is 0.25 %.
  - (c) is replaced by (c1) first raw dead/cut record (100 % agreement), (c2) re-records and
    (c3) diversions.
  - (d) is enforced for planted stands only, INFO for natural (same ACCEPT + FLAG rationale as
    (h); KY natural has 2/25 species non-monotone).
- **M1**: Sim B keeps the Sim A / trajectory time axis: sim_year and age_calc are computed on
  all live rows over the full scaffold BEFORE saplings are dropped. 0 drops misaligned. The
  remaining unmatched drops are sapling-only cohorts that are absent from Sim B by design
  (AL 346 / 6,666).
- **M2**: sim_year is strictly increasing per subplot (`sim_year_map`: s_i = max(raw_i,
  s_{i−1} + 1) on the full scaffold). It is shared by Sim A, Sim B, trajectory phantoms and
  gap slots. Gap slots at visits with no live row used to get a NULL sim_year.
- **M3**: a reused tree number starts a new tree (TREE + 100000·k) when the key's previous
  record was dead/cut and PREV_TRE_CN is null, or PREV_TRE_CN points elsewhere
  (`tree_key_split`). GA: 352 trees on split keys; AL: 10.
- **M4**: DSTRBCD 60 is not a disturbance in cohort_tracking either.
- **M5**: the age model and the calibration use the major group of `spcd_resolved`.
- **M6**: a cut record on a condition that is no longer forest (COND_STATUS 2–4) is
  `diverted_landuse`, decided by condition status rather than GRM. Diversions are excluded
  from cut events and from cohort_disturbance. GA 7,110 cut records (7,108 trees were
  harvest_explicit), TN 2,800. (c3): 100 % of such first records are diverted.
- **Suspected RC3 duplicates (confirmed, fixed)**: RC3 ≥ 5 in debuts that pair with a
  same-subplot, same-species RC7/RC8 closure (|PREVDIA − DIA| ≤ 2 in) at the debut visit are
  the same tree re-tallied. They now get no backfill (rule a). GA a26: 41 → 0.
- **Zombie trees**: trees recorded dead before their live debut get no phantom at or before
  that dead record, and no interpolation across a dead record. This fixed 8 AL slots.
- **Verifier totals**: AL 36 PASS / 7 INFO / 0 FAIL; GA 0 FAIL; TN 0 FAIL; KY and FL 1 FAIL
  each (pre-registered (j)). Determinism (steps 4–7 rerun) is EXCEPT-identical in all five
  states.

### Round-4 (2026-10-26; user decision SPLIT BY STRATUM; whole-state AL, KY, GA, TN, FL)
- **Planted (STDORGCD = 1)**: unchanged. TreeAge is mapped onto the field-age scale
  (`calibrate_estage`), and the model trains on field ages plus planted `estage_mapped`.
- **Natural (STDORGCD ≠ 1 or null)**: estage = RAW TreeAge (Tree_Age > 1, DIA ≥ 5 in), with no
  mapping. `estage_mapped` equals the raw value on these rows. `estage_calibration` has an
  `applied_to` column ("planted rows only") and a `stratum_rule` row ("natural rows: identity").
  - Model targets for natural rows are raw TreeAge (source `treeage`). Natural STDAGE is no
    longer a target; TOTAGE stays a target in both strata.
  - Same plot-grouped CV with the shared crc32 folds. SITETREE is never used for fitting.
- **Rename**: `age_domain` `natural_uncalibrated` → `natural_treeage_scale`; cohort columns
  `n_trees_natural_treeage_scale` / `share_natural_treeage_scale` (I4). Older column rules are
  unchanged. Text above that still says `natural_uncalibrated` describes r2/r3.
- **Recalibration (regression to the mean)**: a post-hoc isotonic map from the model's
  OUT-OF-FOLD prediction to the target, per stratum (P/N) × DIA class (< 5, 5–10, 10–15, 15+ in).
  Cells need ≥ 50 rows (`RECAL_MIN_N`).
  - The CV is nested: the map scored on fold k is fitted on inner OOF predictions of the other
    folds only. Production uses the outer OOF predictions. It is applied in `apply_age_model`
    before the anchor.
  - `age_model_cv` has `model` (raw / recalibrated) and `stratum_pn` columns.
- **Rounding fix (found by r4 M1)**: isotonic plateaus are means of integer targets, so many ages
  are exactly x.5. polars `round(0)` rounds half to even, while DuckDB `ROUND` (Sim A / Sim B)
  rounds half away from zero, so 26 / 6 / 39 / 2 Sim B rows (AL / GA / TN / FL) fell off the
  trajectory axis.
  - The fix: `build_tree_trajectories` (age_calc, alternate age axes) and cohort_tracking's
    birth date now use `mode="half_away_from_zero"`. M1 is now 0 everywhere.
- **CV, out-of-fold, raw → recalibrated, MAE / bias (yr)**:

  | State | field | treeage (natural) | all |
  |---|---|---|---|
  | AL | 3.42/−0.71 → 3.45/−0.22 | 3.61/−0.43 → 3.64/−0.08 | 3.53/−0.55 → 3.56/−0.14 |
  | KY | 7.04/−0.65 → 7.78/+0.16 (n 1,337) | 4.66/−0.56 → 4.66/−0.13 | 4.68/−0.56 → 4.68/−0.13 |
  | GA | 3.20/−0.58 → 3.22/−0.15 | 3.14/−0.36 → 3.11/−0.06 | 3.17/−0.47 → 3.17/−0.11 |
  | TN | 5.67/−1.71 → 5.75/−1.09 | 5.14/−0.60 → 5.15/−0.11 | 5.19/−0.70 → 5.20/−0.20 |
  | FL | 5.11/−1.20 → 5.19/−0.38 | 3.50/−0.39 → 3.53/−0.09 | 4.16/−0.72 → 4.22/−0.21 |

  - Planted `mapped` targets are only 1–21 rows per state, because nearly all planted rows have
    a field age.
  - The low treeage MAE reflects that TreeAge is itself a model of FIA tree attributes.
- **Oldest-decile young bias is NOT removed.** For example, field planted d10 is AL −38.4 →
  −38.0, GA −28.7 → −28.4, FL −27.9 → −27.5, and treeage natural d10 is KY −8.1 → −7.6.
  - Bias conditioned on the TRUE age is regression to the mean. A monotone map from prediction
    to target fixes E[target | prediction] (the per-DIA-class bias), not E[prediction | target].
  - It shrinks the DIA-class biases instead, e.g. FL planted 15+ −4.4 → −2.7, TN 15+ −2.7 → −0.4.
- **Checks**:
  - (j) is unchanged (±3 yr, ≥ 200 trees). KY planted 5–10 in: −3.2 → **−2.2 PASS**. FL planted
    15+ in: −5.1 → **−3.5 FAIL** (357 trees). Reported, NOT tuned.
  - (d) natural re-enforcement FAILED in GA (1/25 species non-monotone) and FL (5/23), so per the
    rule it stays INFO for natural. Planted is enforced and passes everywhere (KY 0/0, AL 0/16,
    GA 0/14, TN 0/4, FL 0/7). Natural: AL / KY / TN 0/25.
  - (k) SITETREE: planted AND natural are now enforced at |median bias| ≤ 5 yr for strata with
    ≥ 100 linked site trees.
- **SITETREE, final age vs AGEDIA + 2/5, MAE / median bias (yr), r3 → r4** (raw TreeAge in
  brackets):

  | State | Planted | Natural |
  |---|---|---|
  | AL | 4.9/−0.9 → 5.0/−0.5 (n 1,253) | 18.6/−15.5 → 9.9/+2.0 (raw 9.2/+2.0; n 3,648) |
  | KY | 3.8/+1.5 → 3.6/+0.7 (n 17, INFO) | 15.0/−5.8 → 10.7/**−5.7 FAIL** (raw 9.5/−5.0; n 2,284) |
  | GA | 4.4/+1.9 → 4.5/+2.1 (n 1,647) | 14.9/−6.7 → 10.3/+2.6 (raw 9.8/+4.0; n 2,374) |
  | TN | 5.8/−0.3 → 6.3/+1.2 (n 284) | 13.6/−7.0 → 10.3/−0.6 (raw 10.1/−1.0; n 5,542) |
  | FL | 5.7/+0.8 → 5.8/+0.8 (n 531) | 12.1/−3.3 → 11.0/−0.5 (raw 10.0/0.0; n 793) |

  The KY natural failure comes from TreeAge itself: raw TreeAge is already −5.0 yr against bored
  ages in KY.
- **Age shares** (live records) are unchanged from r3. ≥ 5 in: estage 82–97 % (GA 82.3 %).
  Saplings: model 95–97 %.
- **`age_domain` live model ages** (natural_treeage_scale / in_support / out_of_support):

  | State | natural_treeage_scale | in_support | out_of_support |
  |---|---|---|---|
  | AL | 112,437 | 49,737 | 5 |
  | KY | 53,535 | 235 | 170 |
  | GA | 200,815 | 93,086 | 9 |
  | TN | 85,731 | 6,674 | 7 |
  | FL | 46,445 | 17,506 | 3 |

- **Verifier totals (r4, after the rounding fix)**:

  | State | PASS | INFO | FAIL |
  |---|---|---|---|
  | AL | 37 | 6 | 0 |
  | GA | 37 | 6 | 0 |
  | TN | 37 | 6 | 0 |
  | KY | 32 | 10 | 1 ((k) natural) |
  | FL | 36 | 6 | 1 ((j) planted 15+) |

  Determinism (steps 4–7 rerun) is EXCEPT-identical in all five states.

### Round-4 ACCEPT + FLAG (2026-10-26, user; DECISIONS.md D6, D8–D12) — partly superseded in r5
r5: the KY (k) entry and `known_bias_ky_natural_treeage` are retired (D13). The FL (j) entry is
now keyed by fit scope, and its flag is `known_bias_fl_planted_model_10plus` (D24). D11 is moot:
`*_natural_treeage_scale` counts `natural_treeage_scale` model ages only. The text below
describes r4.

The two r4 failures are accepted, flagged in the data, and guarded against drift. Every other
cell and state stays strict.
- **Verifier `KNOWN_EXCEPTIONS`** (`verify_curation_v2.py`; exact keys only):

  | Key | Recorded | Limit | Reason | Flag |
  |---|---|---|---|---|
  | (k) KY (21) natural | −5.7 yr | ±5 | TreeAge itself is −5.0 yr vs bored SITETREE ages | `known_bias_ky_natural_treeage` |
  | (j) FL (12) planted 15+ in | −3.5 yr | ±3 | model vs mapped TreeAge regression to the mean; only trees WITHOUT a TreeAge record take the model age | `known_bias_fl_planted_model_15plus` |

  - A failing excepted cell prints `[KNOWN]` (with the recorded value, the reason and the
    flag) while |value| ≤ |recorded| + 1 yr (`KNOWN_DRIFT`). Beyond that it FAILS as "known
    exception DRIFTED".
  - An excepted cell that starts passing gets an INFO line suggesting the exception be retired.
  - The summary lists every KNOWN cell.
  - The values are whole-state; county-subset runs can differ.
- **`age_domain` flags** (`age_imputation.KNOWN_BIAS_FLAGS`, applied by `flag_known_bias()`
  after the model domains, which they override):
  - `known_bias_ky_natural_treeage`: STATECD 21, natural (STDORGCD ≠ 1 or null), age_source
    estage or model;
  - `known_bias_fl_planted_model_15plus`: STATECD 12, STDORGCD = 1, age_source model, DIA ≥ 15 in.
  - The verifier re-derives both rules in SQL and requires 0 missing and 0 extra flags.
- **Cohort columns** (generic, D10): `n_trees_known_bias` and `share_known_bias` (AGB-weighted
  in Sim B) are appended after `age_source_major` on curated_cohorts_landis, _stdorg, _simB
  and the slim tables. `*_natural_treeage_scale` still means natural MODEL ages (D11): it
  includes KY natural model ages that now carry the known-bias flag. Old columns are unchanged.
- **Oldest-decile limitation (not fixable by recalibration).** The age model is biased young
  at the oldest TRUE ages (r4 out-of-fold d10, recalibrated):

  | Stratum / source | AL | GA | TN | FL | KY |
  |---|---|---|---|---|---|
  | field, planted | −38.0 (n 392) | −28.4 (n 670) | −40.1 (n 166) | −27.5 (n 1,972) | – |
  | treeage, natural | −3.4 | −3.2 | −7.0 | −3.2 | −7.6 |

  - The cause is regression to the mean. With imperfect features, the model's prediction
    varies less than the truth, so E[prediction | true age] is pulled toward the mean.
  - The isotonic recalibration is a monotone function g of the prediction. Fitted to the
    out-of-fold data, it makes E[target | g(prediction)] ≈ g(prediction): it removes bias
    conditional on the PREDICTION, which is what the per-DIA-class checks (j) measure.
  - Grouping rows by their TRUE age instead (the deciles) still shows the spread lost to the
    noise. No map of the prediction can restore that spread without making errors larger on
    average, because it is information the features do not carry.
  - Only better features, or a target-aware method, which is impossible at prediction time,
    would help. The oldest planted field ages are a few hundred to 2k rows of old planted
    stands, mostly outside the bulk of the training support.
  - Consumers should treat the oldest model-aged trees as biased young. They are a small
    share: ≥ 5 in trees are 82–97 % estage.
- **Step-2 resume** (`FROM_STEP=2 TO_STEP=2` on a copy of the finished KY DB, EXCEPT ALL both
  ways): curated_trees (280,508 rows), curated_snapshots (235,771),
  estage_calibration and age_model_cv are all EXCEPT-identical (0 / 0).
- **Whole-state reruns, KY and FL (steps 1–11 + verify)**: **0 FAIL in both**. KY: 34 PASS / 10 INFO / 1 KNOWN ((k)
  natural −5.7). FL: 38 PASS / 6 INFO / 1 KNOWN ((j) planted 15+ −3.5). Determinism is
  EXCEPT-identical, and both flag-rule checks show 0 missing / 0 extra.
  - Flagged live records: KY 239,128 (all natural model and estage ages; 53,535 of them are
    model ages, the former natural_treeage_scale count). FL: only 6 live model ages. (j) counts
    trees that have both a model and a TreeAge age, and those carry the TreeAge age as their
    final age, so the FL bias reaches almost no final ages.
  - AGB share known_bias: KY 0.997 (stdorg and simB; KY is almost entirely natural), FL 0.0.
    `share_natural_treeage_scale` is unchanged (KY 0.071 / 0.009).
  - The slim DB carries both new cohort columns and the flags.

### Round-5 (2026-10-27; audit r4 on NC / SC / MS; DECISIONS.md D13–D24)
Whole-state runs for AL, GA, KY, NC, SC, MS, TN and FL, plus a pooled FL + KY + MS DB: steps 1–11,
a determinism rerun of steps 4–10, and the verifier. **0 FAIL in all nine.** Only the pool shows
KNOWN cells (2).

| Run | PASS | INFO | KNOWN | FAIL |
|---|---|---|---|---|
| AL, GA, TN, FL, NC, SC, MS | 59 | 10 | 0 | 0 |
| KY | 56 | 13 | 0 | 0 |
| pool FL + KY + MS | 70 | 17 | 2 | 0 |

- **D13, natural site trees:** natural (k) is INFO, reported per state × wood group × link
  method. Planted (k) stays enforced: AL 0.5, GA 2.1, TN 0.2, FL 0.8, NC 1.3, SC 1.3, MS 0.1
  (KY n 17, INFO).
  - Natural final-age median bias, softwood / hardwood (proxy links): NC +6.0 / −2.9,
    SC +5.3 / −1.4, MS +7.2 / +2.7, AL +4.0 / −1.7, GA +3.7 / −2.1, TN +1.8 / −1.3,
    KY +0.1 / −6.4, FL −0.8 (softwood).
  - The KY KNOWN entry and the `known_bias_ky_natural_treeage` flag are retired, so KY natural
    rows are back to `natural_treeage_scale`.
- **D14 / D24, fit scope:** the KNOWN exceptions and the known-bias flags are keyed by FIT
  SCOPE, the sorted STATECDs of the DB.
  - Single-state FL now passes (j) planted 15+ in at −1.6 yr (r4: −3.5), so that entry and
    the `…_15plus` flag are retired.
  - Under the pooled fit, FL planted is −3.2 at 10–15 in (a newly failing cell) and −3.8 at
    15+ in. Both are recorded for scope `12,21,28` and flagged
    `known_bias_fl_planted_model_10plus` (88 live model ages in the pool).
  - A scope with no entries (e.g. a Region-4 fit) is fully strict, so those cells must be
    re-measured there.
- **I-1, Sim B drops:**
  - Drops now come only from members that are REAL records with DIA ≥ 5 in at the previous
    visit and, if alive, at the visit. A tree that is alive at the visit but has shrunk below
    5 in, or exists only as a gap slot, is neither a survivor nor a removal.
  - A cohort's first Sim B row never carries a drop; that rule removed 0 drops.
  - Drop rows before → after:

    | AL | GA | TN | FL | KY | NC | SC | MS |
    |---|---|---|---|---|---|---|---|
    | 5,888 → 5,290 | 8,373 → 7,524 | 932 → 786 | 2,838 → 2,416 | 514 → 402 | 3,072 → 2,775 | 4,780 → 4,193 | 3,938 → 3,543 |

  - New check (n2): every drop is backed by a real ≥ 5 in member that was removed, died or
    severely damaged (DAMSEV > 40). 0 unbacked everywhere.
- **M-1, one time axis:** every table's sim_year is anchored at the subplot's first scaffold
  visit, and Sim A's re-base is gone.
  - Before: Sim A vs Sim B differed on 407–3,440 shared subplot-visits per state (NC 1,667 of
    52,474), and the trajectories went down to sim_year −19 to −27.
  - After, (n1): 0 differences and sim_year 0 at the first visit in all nine runs.
- **M-2, canonical plot key:** the dbconn views map renumbered plot visits to the key at the
  PREV_PLT_CN chain's first annual visit, and `unitcd_raw` / `countycd_raw` / `plot_raw` keep
  the recorded key.
  - Remapped records (plots): AL 176 (2), FL 233 (3), GA 357 (4), KY 306 (5), MS 0, NC 754 (9),
    SC 88 (2), TN 62 (2).
  - GA `disappeared_inferred_removal`: 101 → 15 trees.
  - (n6): 0 key mismatches against the verifier's own recursive-CTE map.
- **M-3:** Sim B AGB, shares and drops are rounded and sorted. (e) now also covers
  `curated_cohorts_landis_simB` and `cohort_disturbance`: EXCEPT-identical in all nine runs.
- **M-4:** gap interpolation never fills a slot at, or across, an entirely nonforest visit.
  GA: 10 slots no longer filled. (n3): 0.
- **M-5, one age scale per tree:** `age_stratum` is the stratum at the tree's first live
  record, and step 2's scale decisions use it.
  - Trees whose live records span two STDORGCD codes: AL 8,250, GA 8,879, NC 11,059, MS 10,383,
    SC 6,729, FL 5,033, TN 3,130, KY 458.
  - (n5): 0 records differ, and 0 natural trees have mixed-scale candidates.
- **M-6, cross-fitted production predictions:** training rows get their outer out-of-fold
  prediction (AL 389,830 of 576,536 live rows).
  - Planted 5–10 in model age minus the tree's own field age, mean / median: AL +0.05 / +0.68
    → −0.02 / +0.62 yr; NC +0.57 / +1.22 → 0.00 / +0.92 yr.
- **M-7:**
  - The slim schema is ENFORCED: old columns are an exact prefix (verifier section 6, and
    `slim_guard post` with `SLIM_REF`). It passes for all 8 slim tables.
  - (c2) is re-derived from raw records with attribution: 0 flagged subplots lack an
    attributed first dead/cut record.
  - (b) takes the re-tally candidate set from raw FIADB; raw vs pipeline differ in 0 plot-visits
    in every state.
- **M-8, phantom biomass:** the sapling rate uses per-tree biomass from real rows only, and a
  phantom is capped at its debut value. (n4): 0 phantoms exceed their debut value.
  - Phantom AGB share, Sim A / trajectories, before → after:

    | State | Sim A | Trajectories |
    |---|---|---|
    | AL | 0.0117 → 0.0092 | 0.0398 → 0.0360 |
    | GA | 0.0074 → 0.0061 | 0.0268 → 0.0246 |
    | TN | 0.0068 → 0.0058 | 0.0246 → 0.0228 |
    | FL | 0.0116 → 0.0098 | 0.0385 → 0.0358 |
    | KY | 0.0095 → 0.0088 | 0.0247 → 0.0237 |
    | NC | 0.0099 → 0.0082 | 0.0292 → 0.0266 |
    | SC | 0.0103 → 0.0081 | 0.0341 → 0.0314 |
    | MS | 0.0081 → 0.0071 | 0.0290 → 0.0272 |

- **Suspected false tree-number splits: not confirmed.** The only splits linked through a
  STATUSCD 0 record (AL: 5) all show a dead record before, a species change or a large DIA drop.
  No rule change; (n7) monitors it (0 without evidence).
- **Other checks:**
  - (d) planted is 0 non-monotone everywhere. Natural is INFO: TN 1/25 and FL 1/23 (r4 FL
    5/23); the rest 0.
  - (h) planted passes in AL, GA, TN, FL, NC, SC and MS (KY planted has 31 condition-visits:
    INFO).
  - (j) single-state FL passes all six cells.
- **r6 correction:** the r5 trajectory phantom shares above were biased low by the R1 bug (see
  Round-6).

### Round-6 (2026-10-27; audit r5 + genveg T1m; DECISIONS.md D25–D35)
Whole-state runs for AL, GA, KY, NC, SC, MS, TN and FL, plus the pooled FL + KY + MS DB: steps
1–11, a determinism rerun of steps 4–10, and the verifier. **0 FAIL in all nine.**

| Run | PASS | INFO | KNOWN | FAIL |
|---|---|---|---|---|
| AL, GA, TN, FL, NC, SC, MS | 73 | 10 | 0 | 0 |
| KY | 70 | 13 | 0 | 0 |
| pool | 86 | 17 | 2 | 0 |

In the pool, FL planted (j) is 10–15 in −3.2 and 15+ in −4.4 yr (recorded −3.8; the KNOWN drift
bound is 4.8).

- **R1 (D30), sapling growth-rate basis:** `estimate_sapling_growth_rates(df, basis=...)` now
  takes an explicit basis, `tpa_expanded` (Sim A) or `per_tree` (trajectories).
  - In r5 the trajectories were divided by TPA twice, so their phantoms were 5–10 % too low.
  - Trajectory phantom AGB share, r5 → r6:

    | AL | GA | TN | FL | KY | NC | SC | MS |
    |---|---|---|---|---|---|---|---|
    | 0.0360 → 0.0394 | 0.0246 → 0.0266 | 0.0228 → 0.0243 | 0.0358 → 0.0386 | 0.0237 → 0.0248 | 0.0266 → 0.0287 | 0.0314 → 0.0334 | 0.0272 → 0.0292 |

  - The rates are written as `sapling_growth_rates_simA` / `_traj`. (n11): the two are
    identical per species group everywhere.
- **R2, exported COND / PLOT:** `export_reference_tables` copies COND / PLOT / data_plot_eco
  from dbconn's remapped views.
  - Before, slim COND rows for remapped visits of slim plots were 0 everywhere. Now all are
    present: remapped visits AL 3, FL 6, GA 11, KY 9, MS 3, NC 14, SC 12, TN 3, with COND rows
    AL 6, FL 6, GA 21, KY 13, MS 3, NC 21, SC 21, TN 6 (r7 doc fix, F7: the first list was the
    visit counts, not COND rows).
  - (n10) and `slim_guard post`: every Sim A / Sim B / trajectory visit has its PLOT row and
    COND rows, in the curated DB and in the slim DB.
- **R3 (D25):** the damaged-alive drop branch is removed (DAMSEV is a 0–9 class code); drops
  come from removals only.
- **R4 (D26), misrecorded deaths:** a dead / cut record followed by a live record that FIA
  links back (PREV_TRE_CN, PREV_STATUS_CD = 2) gets the new FIX column `misrecorded_death` and
  `tree_fate = 'misrecorded_death'`. It is excluded from every fate, event and dead-record set,
  and the tree continues. The linked record may also be a land-use exit.
  - Records / trees: AL 80 / 72, GA 88 / 72, TN 120 / 94, FL 83 / 80, KY 80 / 76,
    NC 103 / 95, SC 63 / 58, MS 101 / 99.
  - Those visits are now gap-interpolated live slots: AL 66, GA 64, TN 76, FL 38, KY 34, NC 56,
    SC 43, MS 57. (c1) re-derives the flag from raw: 0 missing / 0 extra.
- **R5 (D27):** gap interpolation now has real candidates (the visits above). (n3) checks them
  (0 at / across nonforest). The `selftest_fixtures.py` fixtures (n9) pass 4 / 4.
- **R6 (D31):** the stored Sim B drop is `(dropped / 4046.8564224) / (agb + dropped /
  4046.8564224)`, exactly what Pan's `_weighted_cohort_drop` assumes
  (`would_be = agb / (1 − drop)`).
  - Rows with ingrowth (AGB > the drop model's base × 1.01): AL 211, GA 137, MS 142, SC 87,
    NC 59, FL 44, TN 12, KY 5 (up to 5.4×). These were over-dropped before.
  - (n8): Pan's formula reconstructs the dropped biomass on every row (max relative error
    < 1.5e−8).
- **R7 (D28), plot location:** a plot whose chain has a remapped visit takes its LAT / LON /
  ELEV (PLOT view) and its data_plot_eco row (L3 / L4 / ECOSUBCD) from its most recent visit.
  The identity key stays the first visit's.
  - The latest visit differs in coordinates on 30 of 36 such R4 plots, in county on 32, in L4 on
    13, and in L3 on 1 (TN). Per state in D28.
  - (n12): all curated PLOT rows of those plots carry the latest coordinates.
- **R8 (D32), consumer issue:** Pan recomputes `sim_year` from its own first measurement
  (landis_region4 src/data/utils.jl:371–372) and ignores the curated axis. On the r5 slim, its
  steps disagree on GA 408 / 76,835 Sim A and 947 / 68,093 Sim B steps (NC, AL, KY in D32).
  **Required change for the Pan rebuild: read the curated `sim_year`.**
- **S1 (D29) + D35, coordinate links:** a chain start at an earlier chain end's exact
  coordinates, with ≥ 30 % of the end's live trees continued (same SUBP / TREE / SPCD,
  non-decreasing DIA), is linked into one plot.
  - R4 links: GA 87/45 → 87/146 (0.43), MS 163/84 → 163/101 (0.37), SC 19/74 → 19/279 (0.63),
    SC 19/101 → 19/280 (1.00).
  - The link visit is a procedural re-tally (D35). End trees that are not continued are censored
    `procedural_relink` (GA 29, MS 37, SC 17), and a `coord_link` break blocks backfill across
    it. `disappeared_inferred_removal` therefore stays at its r5 level: GA 15, SC 2, MS 0.
  - (n14) checks every end tree is continued or censored. The verifier's own map (links
    included) equals dbconn's.
- **S2 (D33):** `run_scope` (states, counties) is written by v3, and the fit scope includes the
  county subset.
- **T1m (D34), FIADB live-tree plots missing from the curated product:** the old slim_r4 lacked
  GA 2,507, FL 250 and KY 79 in-scope live-tree plots. Now (n13) absent / plots:

  | AL | GA | TN | FL | KY | NC | SC | MS |
  |---|---|---|---|---|---|---|---|
  | 0 / 5,406 | 4 / 8,384 | 0 / 3,603 | 5 / 3,659 | 10 / 2,645 | 6 / 4,679 | 1 / 3,777 | 5 / 4,187 |

  Every remaining one is a plot whose live trees FIA withdrew as "erroneously tallied" (RC7).
  0 are unexplained.
- **New FIX columns:** `misrecorded_death` (curated_trees). **New tables:** `run_scope`,
  `plot_key_remap`, `plot_key_coord_links`, `plot_location`, `sapling_growth_rates_simA` /
  `_traj`. `tree_landuse_exits` gains `linked_live_after_dead_md`.

### Round-7 (2026-10-27; audit r7, 6 slices; DECISIONS.md D36–D46)
Trees (D36–D39) and ages (D40, D46) are committed separately (dfc92cf, f76c872, 1c7fc9f, b7572de).
The cohort, chain and verifier work:
- **D41, one disturbance definition** (`cohorts_landis.disturbance_events`): a cut, a death with
  AGENTCD 10–50 or a DSTRBCD1–3 other than 0 / 60, or an inferred removal, at the tree's FIRST
  dead / cut record (re-records and misrecorded deaths never). It drives `subp_has_dstrb`
  (Sim A / Sim B / trajectories; trees without a cohort count too), cohort_tracking and the Sim B
  drops, which count a removal only AT its event visit. D38: `first_record_dead` removals are not
  drops.
- **D42, full removals:** `cohort_disturbance` emits `full_removal = TRUE` rows (drop 1.0). The
  visit axis of the drop computation is the full scaffold (`load_scaffold`), so a visit at which
  every tree was removed (cut records without TPA, no trajectory row) is still paired: GA 87,
  449 → 570 full removals.
- **D43:** `export_reference_tables` copies PLOT / COND rows of product visits plus the remap's raw
  visits of the product's plots only (a county subset no longer exports other counties' remapped
  plots); n25 / `slim_guard post` check for foreign PLT_CNs.
- **D44:** the hardening list (synthetic-row stratifiers, microplot test for sapling phantoms,
  per-table phantom floors, ecosubcd from data_plot_eco, 2-way `land_use` + `land_use_4way`,
  stale paths refusing to run, chain checks and the run id / `run_steps` provenance checked by
  slim_guard, column order kept on step-6 reruns).
- **D45 verifier:** independent re-derivation (n14–n26), exits 1 on any FAIL, and the subset
  determinism check (`--determinism-subset`: steps 1–10 twice at two thread counts). The GRM
  read is state-scoped (TREE_GRM_COMPONENT has no COUNTYCD), and n6 compares D37 duplicates in the
  run's scope (plot_key_remap is national).
- **One-county check (GA 13:87, threads 2 vs 1):** `selftest_fixtures` 9 / 9 PASS. Verifier
  149 PASS / 15 INFO / 1 FAIL: (d) planted is EMPTY (no planted species with ≥ 50 records per
  DIA class in one county), an artefact of the subset. Steps 1–10 are EXCEPT-identical at the two
  thread counts (35 curated + 3 Sim B tables). The 8-state validation is still to run.

### Round-8 (2026-10-28; DECISIONS.md D47–D48)
- **D48 (user): the coordinate-link layer is REMOVED.** D29 coordinate links, D35 `procedural_relink`
  censoring and the `coord_link` break, D37 duplicate-visit collapse and D36's link-start split rule are
  gone (D47.1 is moot). Identity = the composite key + PREV_PLT_CN (canonical key, M-2) + PREV_TRE_CN /
  RECONCILECD tree splits. The D29 / D35 / D37 text in Round-6 / Round-7 above is historical; the 18 R4
  coordinate-linked plot pairs are separate plots. `plot_key_remap.reason` is always `prev_plt_cn`, and
  `plot_key_coord_links` is no longer written. The verifier asserts no link artefacts remain.
- **D47:** verifier n21 applies the D34 RC7 exclusion, n18's r_a is NULL-safe, n5 tells a rejected scope
  from an unmapped one; KNOWN cells and `KNOWN_BIAS_FLAGS` re-measured on the D40 / D46 age model.

---

## FIA data model — critical invariants

**Plot identity**: `(STATECD, UNITCD, COUNTYCD, PLOT)` — none of the components
alone is unique (e.g. PLOT=1 exists in many counties).

**Remeasurement identity**: add `INVYR`. A plot is remeasured roughly every 5–7
years; some plots are remeasured more than once within the same calendar year
(different INVYR, typically 1 year apart).

**MEASDATE = `MAKE_DATE(MEASYEAR, MEASMON, MEASDAY)`** — the exact calendar date
of a remeasurement: a timestamp for ordering, intervals and modelling, never a key (D69). A visit
is identified by `(plot, INVYR)` even where two visits share a date.

### Rules enforced throughout this pipeline

| Rule | Reason |
|---|---|
| Use **INVYR** for all SQL joins between FIA tables | correct relational key |
| Use **MEASDATE** (full date) for all temporal ordering and arithmetic | same-year revisits have different MEASDATE but same MEASYEAR |
| **Never extract year from MEASDATE** and never use MEASYEAR for ordering | two visits in the same calendar year would be misordered or deduplicated incorrectly |
| Treat MEASDATE as an opaque timestamp; compute intervals as `.dt.total_days() / 365.25` | no year extraction anywhere |

Subplot key: `SUBPLOT_ID = [STATECD, UNITCD, COUNTYCD, PLOT, SUBP]`
Tree key:    `TREE_ID    = [STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE]`

---

## fia_curation_v2.py

**Input**: raw FIA DuckDB tables (TREE, PLOT, SUBPLOT, COND, SUBP_COND, SURVEY,
REF_SPECIES).
**Output tables**: `base_subplot_cond`, `clean_subplot_years`,
`longitudinal_subplots`, `excluded_tree_ids`, `curated_trees`,
`curated_snapshots`.

### Steps

**SQL (steps 1–3)**
- `sql_base_measurements`: join PLOT×SUBPLOT×SUBP_COND×COND, restrict to annual
  CONUS survey, compute MEASDATE.
- `sql_clean_subplot_years`: drop subplot-invyrs where any COND has
  COND_STATUS_CD=3 (non-forest).
- `sql_longitudinal_subplots`: keep subplots with ≥2 clean measurements.
- `sql_excluded_trees`: flag trees with anomalous RECONCILECD (3,4,6,7,8,9) or
  STATUSCD=0; classified as meaningful vs trivial exclusions.

**Polars (steps 4+)**
- `load_trees`: joins curated tree records with condition context. Sort by
  MEASDATE (not MEASYEAR).
- `analyze_spcd_confusion`: detects SPCD changes across visits (sorted by
  MEASDATE). Resolves to mode SPCD with MEASDATE.max() tie-break.
- `handle_reconcile5`: trees whose DIA dropped below the subplot minimum are
  kept alive with imputed DIA. `reconcile5_first_measdate` (Date) marks first
  RECONCILECD=5 event; recovery flag compares MEASDATE > that date.
- `classify_tree_fate`: alive / harvest_explicit / harvest_inferred / dead.
- `_compute_tree_bounds`: per-tree `first_measdate` and `prev_sub_measdate`
  (latest subplot visit strictly before the tree's debut). Used to bound
  max-plausible age. Filter is `_sub_measdate < first_measdate` (date
  comparison, not year comparison).
- `propagate_totage` / `assign_stdage` / `process_estage`: see Age assignment
  section below.
- `build_subplot_year_scaffold` + `build_snapshots`: aggregate live trees to
  SPCD×age×biomass per subplot-invyr. Sorted by MEASDATE.

### Age assignment

Priority chain: **TOTAGE → estage → ML model → STDAGE → null**
(STDAGE is demoted to last — it is a stand-level even-aged age that correlates only
weakly with individual biomass, so the per-tree sources win. The final coalesce is in
`age_imputation.apply_age_model`; v3 still pre-merges TOTAGE>STDAGE only as the model
*training* target.)

All sources go through `_select_best_anchor(candidates, tree_bounds,
apply_min_shift)`:
1. Compute implied age at tree's `first_measdate` (back-extrapolate).
2. If `apply_min_shift=True` (TOTAGE only): floor the implied age at
   `first_measdate` to a minimum of **1 year** (TOTAGE can be recorded as 0;
   we never allow age 0). The shift added = `max(0, 1 − implied_at_first)`.
   All other sources use `apply_min_shift=False` — no shift is applied, the
   source value is trusted as-is (minimum resulting age is not enforced).
3. Compute `max_viol`: implied age at the most recent subplot visit before
   debut (the crew was there and didn't see the tree — bounds maximum age).
4. Pick candidate with smallest max_viol; tie-break by most recent MEASDATE.
5. Propagate linearly: `age = anchor_age + (MEASDATE − anchor_MEASDATE).days / 365.25`

**STDAGE eligibility** = (one of the two *cohort* conditions below) **AND** the
**FLDSZCD size gate**, evaluated on **live (`STATUSCD=1`) measurements only**.

Cohort conditions:
- `STDORGCD=1` (planted stand): all trees co-established → STDAGE = individual age.
- Post-reset debut: condition had a detected reset (STDAGE drop, TRTCD=10/30/40,
  or ≥80% tree loss) AND tree first appeared at/after that reset date.
- Excluded: pre-reset carry-forwards (tree appeared before reset AND survived
  past it).

**FLDSZCD size gate** (`size_qualifies`): STDAGE is, by FIA definition, the mean
total age of the trees *in the field-recorded stand-size class* (`FLDSZCD`, the
crew's "local procedures" class — not `STDSZCD`, the national stocking algorithm).
So a tree only inherits STDAGE if it qualifies within that class: sapling stands
(`FLDSZCD=1`) keep all trees; in pole/timber and larger (`FLDSZCD≥2`, or
nonstocked/unknown) only timber-sized trees (`DIA≥5″`) qualify. A null DIA does
**not** qualify, and qualification/anchoring use **live rows only** — otherwise a
sub-5″ understory tree could sneak in via a death record that reads ≥5″, or via a
null-DIA dead row, and propagate stand age onto its live sapling measurements.
Without this gate, STDAGE (a stand-level even-aged age) was being stamped on
understory/ingrowth, decorrelating it from individual biomass
(corr(ln biomass, STDAGE-age): 0.14 ungated → 0.29 gated; estage stays 0.52 because
it is a genuine per-tree estimate). The residual is inherent: timber trees of all
sizes in one stand share its STDAGE by construction.

Reset detection uses full MEASDATE throughout:
`stdage_cond_first_reset_measdate`, `stdage_tree_first_measdate`,
`stdage_tree_last_alive_measdate`, `stdage_anchor_measdate` — all Date-typed.

---

## age_imputation.py

**Input**: `curated_trees` (from fia_curation_v2), `clean_subplot_years`.
**Output**: updates `curated_trees` with imputed ages, rebuilds `curated_snapshots`.

Fits a `HistGradientBoostingRegressor` on `log1p(estimated_age)` for trees
that already have TOTAGE/STDAGE ages and DIA.

Features: `DIA, HT, CR, CCLCD, cond_alstkcd | major_spgrpcd, epa_l3`

**Training set**: alive trees with `age_source` + `DIA`, trimmed to **≤ p90
of estimated_age** (upper outliers only; lower tail kept — young saplings are
valid training signal).

Applies `_select_best_anchor(..., apply_min_shift=False)` — model predictions
are not forcibly shifted.

---

## cohort_tracking.py

**Input**: `curated_trees`.
**Output table**: `curated_cohorts` (one row per cohort × MEASDATE).

A **cohort** = `(SUBPLOT_ID, SPCD, birth_date)` where
`birth_date = MEASDATE − duration(round(estimated_age) × 365 days)`.

Since all ages are linearly propagated from a single anchor, birth_date is
stable across remeasurements — no heuristic year-matching needed.

Key output columns:
- `birth_date` (Date), `MEASDATE` (Date), `first_measdate` (Date)
- `n_trees_live`, `biomass_per_acre`, `estimated_age_mean/sd`
- `n_harvested`, `n_disturbance_dead`, `pct_biomass_damaged`
- `ever_harvested`, `ever_disturbed_dead`

`check_tree_continuity`: verifies age propagation (check A) and flags trees
reappearing after a gap (check B). Uses MEASDATE throughout; shift() over
SUBPLOT_ID+MEASDATE for previous-visit lookup.

---

## cohorts_landis.py

**Input**: `curated_trees`, `curated_cohorts`, `data_plot_eco`, `REF_SPECIES`.
**Output table**: `curated_cohorts_landis`.

Converts to the format expected by Pan/LANDIS-II (one row per live tree ×
measurement visit, with integer sim_year and age_calc anchored to subplot time
axis).

### Key transformations

**`compute_sim_year_ages`**: replaces floating-point `age_calc` with integer
offsets on a per-subplot time axis.
```
sim_year  = round((MEASDATE − first subplot MEASDATE).days / 365.25)
age_calc  = debut_age_rounded + (sim_year − debut_sim_year)
```
All trees on the same subplot share the same integer sim_year steps →
cohort ages always have exact integer jumps.

**Phantom backfill** (tree-level first, then cohort-level):
- `backfill_phantom_trees`: for every tree, generate synthetic rows at all
  prior subplot visits where the tree would have been alive (prior_age ≥ 1).
  Biomass scaled back using sapling growth rates trained on trees with
  `first_dia ≤ 5.0 in` (microplot saplings, DIA column).
- `backfill_phantom_cohorts`: second pass at the cohort slot level to catch
  any remaining gaps. Reports "N additional cohort-slots".
- `print_mid_measurement_gaps`: diagnostic only (not fixed) — counts subplot
  visits between a tree's first and last real record where the tree is absent.

**`check_cohort_continuity`**: final validation that every cohort
(subplot×species×measdate×age_calc) has a predecessor at the previous visit
or was born within the inter-visit gap. Expected: 0 violations after backfill.

### Output schema

```
plt_cn, statecd, unitcd, countycd, plot, subp,
epa_l3, epa_l4, ecosubcd,
species_symbol, spgrpcd, major_spgrpcd, sftwd_hrdwd,
measdate, sim_year, age_calc, agb,
subp_has_dstrb, plot_meas_num, subp_meas_num, intro_type
```

`intro_type`: null = real record, `"phantom_backfill"` = synthetic.

Biomass unit: g/m² (converted from lbs/acre via factor ≈ 0.11208).
Sapling DIA threshold: `SAPLING_DIA_MAX = 5.0 in`.

---

## build_tree_trajectories.py

**Input**: `curated_trees` (post-`age_imputation`), `clean_subplot_years`,
`data_plot_eco`, `COND`, `REF_SPECIES`.
**Output table**: `tree_trajectories` — one row per (tree × subplot-visit slot),
**contiguous** from the tree's first introduction to its terminal event.

Reuses `cohorts_landis`: `compute_sim_year_ages`, `estimate_sapling_growth_rates`,
`MIN_BACKFILL_AGB`, and the key lists.

**Biomass = `drybio_g`**: the per-tree above-ground dry biomass `DRYBIO_AG` (lbs) ×453.592 →
grams. NOT TPA-expanded to g/m² — these are individual trees, so per-area is the wrong unit
(that's what `curated_cohorts_landis` uses for stand cohorts). `ba` stays TPA-expanded ft²/acre
because the cut-event class needs stand basal-area fractions.

### Why this stage exists
`cohorts_landis.load_base_data` keeps only `STATUSCD=1`, so death/cut endpoints are
dropped and mid-trajectory gaps were only *diagnosed* (`print_mid_measurement_gaps`),
never filled. This stage produces trustworthy, complete per-tree life histories
(the base for forming species×biomass×age cohorts) and feeds `tree_trajectories.py`.

### Gap filling (two mechanisms)
- **Backward phantom** (`intro_type='phantom_backfill'`): pre-debut slots where the
  tree was alive but too small/suppressed to be tallied. `backfill_phantom_full`
  uses the **full `clean_subplot_years` scaffold** (not just visits with live trees —
  that was the bug that left holes); biomass extrapolated back via sapling growth rate.
- **Mid-trajectory interpolation** (`intro_type='gap_interpolated'`): tree missed at a
  visit then re-tallied later. `interpolate_gaps` linearly interpolates dia/ht/cr/agb/ba
  on MEASDATE between the bracketing real records; `age_calc` from the integer sim_year
  axis. Tree-level flag `was_missed_reintroduced`.
- `intro_type='real'` otherwise. `trajectory_complete` = a slot at every subplot visit
  between first and last (verified 0 incomplete on tested states).

### Terminal fate (per tree) — `classify_terminal_fate`
`terminal_fate` from the last real record + whether the subplot was visited again:
`harvest_explicit` / `harvest_inferred` (carried through) · `dead_disturbance` /
`dead_natural` (split below) · `alive_censored` (alive at last & no later visit) ·
`disappeared_inferred_removal` (alive at last but subplot revisited without it — recovers
the dead-code `final_tree_fate` logic from v3). `death_cause` summarises
natural/disturbance/harvest. `is_terminal` marks the last body slot.

**Natural vs disturbance death = hybrid**: `disturbance` if `AGENTCD ∈ {10,20,30,40,50}`
(insect/disease/fire/animal/weather) OR the condition logged a `DSTRBCD` (≠0/60) at that
visit; else `natural`. (`AGENTCD=80`/`STATUSCD=3` is harvest, classified upstream.)

### Cut-event class (condition × cut visit) — `classify_cut_events`
Per-tree BA = `0.005454·DIA²·TPA` (ft²/acre); pre-cut BA taken at each cut tree's last
alive visit; denominator = live BA in the condition at the visit before the cut.
- `clear_cut` if removed ≥ **85%** of pre-cut live BA (`CLEARCUT_BA_FRAC`)
- else `pre_commercial_thin` if mean cut-tree age < **20 yr** (`PRECOMMERCIAL_AGE`)
- else `commercial_thin`
Attached per-row as `cut_event_type`/`cut_pct_ba_removed`/`cut_mean_age` for every tree
present at that condition-visit.

### Derived `land_use` — **2-way STDORGCD (matches Pan)** + 4-way management (`land_use_4way`)
The emitted **`land_use`** is the **2-way per-plot STDORGCD ground truth** — `artificial` iff any forested
condition has `STDORGCD=1`, else `natural` — the *identical* rule to `build_stdorg.py` /
`curated_cohorts_landis_stdorg`, so the trajectory viewer's strata line up with what the Pan Sim A/B
calibration is fit on. The **old per-condition management/purity classification** (`reserved`→`plantation`
→`managed`→`natural`, from management history + forest-type purity ≥ **0.80** = `PURITY_FRAC`, decision in
`derive_land_use`) is preserved as **`land_use_4way`**; its component markers
(`is_planted/has_clearcut/has_commercial_thin/has_precommercial_thin/is_pure/dominant_species_frac`) are kept.
The 2-way column is attached by a final SQL step in `build_tree_trajectories.py` (`__main__`).

**One-off migration** — to switch an *existing* `tree_trajectories` table to 2-way without a full rebuild
(instant; `curated_trees` unchanged): `.venv/bin/python migrate_tree_traj_land_use.py [DB]` (renames the old
`land_use`→`land_use_4way`, adds the STDORGCD 2-way `land_use`).

### Species — resolved vs observed
FIA confuses similar species across remeasurements (e.g. loblolly `PITA` ↔ slash `PIEL`), so a
tree's raw per-visit `SPCD` flips. The canonical `species_symbol`/`spcd` (and `spgrpcd`,
`major_spgrpcd`, `sftwd_hrdwd`) come from v3's **`spcd_resolved`** (mode species, constant per
tree) — filter/colour on these. The raw per-visit value is kept as `species_symbol_obs`/`spcd_obs`,
and `spcd_changed` flags trees that ever flipped. Using raw species would leak the other species
into a single-species filter.

### Alternate age axes — `add_alt_age_axes`
The selected `estimated_age`/`age_calc` hops between sources (TOTAGE→estage→model→STDAGE),
which adds discontinuities. Every source is also carried as its own integer, sim-year-anchored
axis: `age_totage`, `age_stdage`, `age_estage`, `age_model`
(= `round(<src> at debut) + (sim_year − debut_sim_year)`; null where the tree never had that
source). `age_source` records which source `age_calc` actually used. The viz has an **Age axis**
selector to re-plot trajectories on any of these, to compare/diagnose source noise.

### Run order & notes
`fia_curation_v3 → age_imputation → build_tree_trajectories`. (cohort_tracking not
required — builder computes its own `subp_has_dstrb` from terminal fates.) `CYCLE` comes
from `curated_trees`, so a fresh v3 run is needed for non-null `cycle`. Optional
`states=[...]` arg (CLI: trailing STATECDs) for subset runs.
`.venv/bin/python build_tree_trajectories.py ../FIASQLITE2PGSQL/FIADB.duckdb [STATECD ...]`
Thresholds are module constants near the top.

---

## Visualization notebooks (Marimo)

- **`cohort_trajectories.py`**: plots AGB vs age per cohort from
  `curated_cohorts_landis`. Filters by ecoregion, species, intro_type.
  Cohort identity: `(statecd, unitcd, countycd, plot, subp, species_symbol,
  birth_sim_year)` where `birth_sim_year = sim_year − age_calc`.
- **`tree_trajectories.py`**: plots contiguous per-tree trajectories with **both X and Y axes
  selectable** from the same set — biomass `drybio_g` (g/tree), any age source (`age_calc`/
  `age_totage`/`age_stdage`/`age_estage`/`age_model`), `dia`, or `ht` (so biomass-vs-age,
  biomass-vs-diameter, height-vs-diameter allometry, diameter-vs-age, etc.). Rows are dropped where
  either chosen axis is null, so phantom-backfill rows (null `dia`/`ht`, size unmeasured pre-debut)
  fall out of any size axis while biomass is still modelled for them on the age axes.
  Per-axis **log X / log Y** toggles; an optional **linear fit + R²** (OLS in the displayed axis
  space — so log–log gives a power-law exponent, e.g. biomass∝dia^~2.5, R²≈0.97); and a
  **Points** selector — *All* (trajectory lines) vs *First/Last per tree* (one representative real
  measurement per tree, ordered by `age_calc` = measdate, to avoid pseudo-replication in the fit).
  Contiguous lines; the **leading phantom-backfill portion is drawn dashed** (turns solid once real),
  **single-measurement trees render as a dot** (so one-off / harvest / death records still show), open
  points mark imputed slots, and the terminal point is a `terminal_fate`-shaped marker. Colours are a
  **bold palette** (tab10 ≤10 levels, turbo beyond). Filters/strata: **specific plot(s)** via a `plt_cn`
  text box (`statecd_unitcd_countycd_plot`, comma-sep — ecoregion is *optional* when a plot is given), EPA
  L3/L4 ecoregion, species, **`land_use` = 2-way STDORGCD**, `cycle`; colour-by
  species/land_use/terminal_fate/cut_event_type. (The 4-way management classes live in `land_use_4way` on
  the table if you want to re-expose them.)

Both default to `../FIASQLITE2PGSQL/FIADB.duckdb` in the UI text input.

Also in `cohort_trajectories.py`: reads `curated_cohorts_landis_stdorg` (2-way `land_use` filter), draws
cohorts as **connected lines** (dashed phantom prefix, single-measurement dots) with the same bold palette.

---

## build_bmax_floor.py — B_MAX floor for Pan Sim A

Builds the per-(species-tier, stratum) **B_MAX lower-bound evidence** consumed by the Pan Sim A search
(`bmax_floor_from_data` knob). Writes `../landis_parametrization_julia/runs/bmax_floor.csv`
(`category,l3,lu,n,bmax_floor`).

- **Metric**: `p99` of a tier's **species-own** TPA-expanded biomass (g/m² = `DRYBIO_AG·TPA_UNADJ·0.112092`)
  over plot-measurements where the tier holds **≥80% of stand TPA** (tree-count purity — robust to a single
  giant tree dominating AGB the way an AGB-purity rule was not).
- **Tier space** matches Pan's `SPECIES_LIST`: 10 exact species + `_GRP_41/_GRP_43` (by `spgrpcd`) + `_H/_S`
  (softwood/hardwood catch-all, via `REF_SPECIES.SFTWD_HRDWD`). Study area = the FL5 EPA-L4 codes; stratum =
  `epa_l3 | land_use` (STDORGCD 2-way).
- Only rows with a ≥80%-pure stand are written; Pan applies the floor as **`max(12000, bmax_floor)`** and
  defaults to 12000 where a (tier,stratum) is absent. Live trees only (`STATUSCD=1`).

`.venv/bin/python build_bmax_floor.py [DB] [OUT_CSV]`  (read-only on the DB).
