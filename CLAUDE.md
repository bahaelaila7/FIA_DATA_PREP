# FIA Data Prep Pipeline

Python pipeline that curates USFS FIA forest inventory data into the format
consumed by the Pan/LANDIS-II parametrization engine (Julia project at
`../landis_parametrization_julia/`).

Database paths are explicit arguments since curation-fix 2026-10-26 (see that section); the raw
FIADB is `../FIASQLITE2PGSQL/FIADB.duckdb`, opened READ_ONLY.

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
| estage (Tree_Age) used at any size | an estage record is a candidate only if **DIA ≥ 5 in and Tree_Age > 1** (`ESTAGE_MIN_DIA`/`ESTAGE_MIN_AGE`). About 42 % of AL live 1–3 in saplings that have a Tree_Age carry Tree_Age ≤ 1 (≈45 % by the auditor's count), with a hard step at 5 in. Saplings get their age from a later ≥5 in record of the same tree (propagated back) or from the age model. `estage_raw` keeps the raw value |
| `final_tree_fate` was computed and discarded | returned: last dead/cut fate · `censored_<last reason>` · `diverted_landuse` · `disappeared_inferred_removal` · `alive`. `diverted_landuse` = the subplot is revisited but the land left forest: a later live record on a nonforest/water condition (`tree_landuse_exits`), or the next visit is entirely nonforest. `dead_at_first_visit` flags a tree that was already dead at the subplot's first visit (no observed mortality) |
| harvest_inferred for any STATUSCD 2 on a TRTCD=10 condition without explicit cut markers | only when **AGENTCD is NULL**. A recorded agent 10–70 is FIA's own mortality call: all 152 AL harvest_inferred vs GRM MORTALITY disagreements had one. GRM CUT→harvest / MORTALITY→dead agreement is now 1.0000 (AL, KY) |
| snapshots by raw SPCD | `curated_snapshots` grouped by `spcd_resolved` / `spgrpcd_resolved` |
| — | `spgrpcd_resolved`; area proportions for consumer-side normalisation (`cond_condprop_unadj`, `cond_subpprop_unadj`, `cond_micrprop_unadj`, `cond_macrprop_unadj`, `subp_subpcond_prop`, `subp_micrcond_prop`, `subp_macrcond_prop`, `subp_forest_*_prop`, `subp_has_cond_status3`, `cond_status_cd`) |

The 24 new curated_trees columns are APPENDED after all old columns (`FIX_COLUMNS`), so the old
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
    first real row keep sim_year ≥ 0;
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
  - (c) GRM CUT→harvest / MORTALITY→dead agreement ≥ 99 %;
  - (d) median age monotone in DIA class (1–3/3–5/5–7/7–9 in) for the top-25 species;
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
- CN and tree × MEASDATE unique;
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

### Round-2 audit: whole-state checks (AL = 1, KY = 21; steps 1–11 + verify)
| Check | AL | KY |
|---|---|---|
| Live TPA coverage (independent reference), saplings | 0.434 → 1.000 | 0.767 → 1.000 |
| Live TPA coverage, ≥ 5 in | 0.939 → 1.000 | 0.937 → 1.000 |
| GRM agreement | 1.0000 of 107,661 | 1.0000 of 22,516 |
| Determinism (EXCEPT both ways) | 0 / 0 | 0 / 0 |
| Censoring events (procedural / nonsampled / shrank / procedural_unrecorded / moved) | 23,059 / 2,642 / 100 / 95 / 56 | 2,863 / 4,338 / 81 / 1,203 / 49 |
| Sim A phantoms | 25,735 → 33,232 (tree 33,170 + cohort 62) | 17,717 (tree 17,686 + cohort 31) |

- Failed checks: AL 0. KY 1, check (d): 11/25 species have median age at 3–5 in (88 %
  model-aged) above 5–7 in (92 % estage-aged). That is the two-age-scale issue (I2,
  TreeAge older than field ages), **ON HOLD** pending the user's choice of scale.
- The procedural_unrecorded censorings flow to `final_tree_fate` / `terminal_fate`
  `censored_procedural_unrecorded`.
- The AL cohort pass went from 1,039 phantoms to 62. The tree pass grew because candidates now
  come from the full scaffold.

---

## FIA data model — critical invariants

**Plot identity**: `(STATECD, UNITCD, COUNTYCD, PLOT)` — none of the components
alone is unique (e.g. PLOT=1 exists in many counties).

**Remeasurement identity**: add `INVYR`. A plot is remeasured roughly every 5–7
years; some plots are remeasured more than once within the same calendar year
(different INVYR, typically 1 year apart).

**MEASDATE = `MAKE_DATE(MEASYEAR, MEASMON, MEASDAY)`** — the exact calendar date
of a remeasurement. MEASDATE is unique per `(plot, INVYR)`.

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
