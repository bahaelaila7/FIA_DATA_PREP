# FIA Data Prep Pipeline

Python pipeline that curates USFS FIA forest inventory data into the format
consumed by the Pan/LANDIS-II parametrization engine (Julia project at
`../landis_parametrization_julia/`).

Default database: `../FIASQLITE2PGSQL/FIADB.duckdb`

---

## Pipeline sequence

```
fia_curation_v2.py   →   age_imputation.py   →   cohort_tracking.py   →   cohorts_landis.py
```

Each stage reads from and writes back to the DuckDB file.

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
