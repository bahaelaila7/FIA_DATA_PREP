# Semantic review, 2026-11-03 (code read only, nothing run)

User asked: check semantics and implementation correctness of the model and the curation, by reading the code. Findings are written here as each part is read, so a long session cannot lose them.

Severity: **H** wrong result on real data · **M** wrong in a definable case, or a modelling flaw · **L** latent bug, wording, or an edge with no effect now · **OK** checked and correct.

## Summary, most serious first

| # | Sev. | Where | Finding |
|---|---|---|---|
| M1 | M | model | short-tree term double-counts breast height and is not a likelihood; gate the "not tallied" design term instead |
| C1 | M | frame, cohorts | KINDCD 1 / 3 and DESIGNCD-change visits cut tree numbers but not the subplot history: pre-debut labels, design-rule terms and phantoms on a physically different plot |
| C3 | M | fates, Pan | an unobserved microplot at the next visit makes a sapling an inferred removal: a cut end in the frame, a disturbance flag and a drop in Pan |
| C7 | M | frame | land-clearing cuts (`diverted_landuse`, AGENTCD null) end lineages as deaths |
| C8 | M? | frame | frame real slots come from the trajectory body, which needs a curated age and biomass; a dropped live record becomes an unlabelled or FALSE slot (probably few: every live record with DIA gets an age) |
| C4 | L | STDAGE resets | a sapling re-tally can fake the 50 % loss |
| C2, C5, C6, C9-C12 | L | various | see sections |

Counts for C1, C3, C7 and C8 need one query each on the truth DB; nothing was run for this review.

**Measured after the D72 fixes (DECISIONS D72):** in R4 only C1 changes data (41,910 frame slots, 18,424 phantoms; no tree is cut off because FIA closes trees at re-installation); C3 (21 removals, none unobserved), C7 (all clearing cuts have AGENTCD 80), C8 (0) and C2 have no effect. M1 is the substantive change.

## Order
1. Model: `nimiwae70.py`, `nimiwae56.py` (joint, build, impute, sample_lineages) — done, see M1.
2. Frame: `nimiwae_prep.py`
3. Age data: `nimiwae_age_prep.py`, `nimiwae_age_direct.py`, `soil_extract.py`
4. Identity: `dbconn.py`, `fia_ids.py`, `fia_truth_assemble.py`, `run_curation_v2.sh`
5. Trees: `fia_curation_v3.py`
6. Ages: `age_imputation.py`
7. Trajectories and cohorts: `build_tree_trajectories.py`, `cohorts_landis.py`, `cohort_tracking.py`, `cohorts_landis2.py`, `build_stdorg.py`, Sim B, `cohort_disturbance.py`, `export_reference_tables.py`

## 1. Model

- **M1 (M) short-tree term.** `extra_lw` double-counts log P(at breast height) at every TRUE slot (the gate already sits in `ev_alive`), is an unnormalised constraint whose constant depends on the decoder, and is redundant once the design rule's "not tallied" term is gated by breast height. Fix proposed: delete it; gate `lcut` as P(no record) = 1 - p_s P(BH) sigma(x_t) through a hook that only the age-on model overrides. Not applied yet (user: review first).
- OK: exact age prior, first-record anchor, decoder blind to z0, dates for age steps, gate with l0, grid posterior, at-risk start, lineage `born`, encoder input `af_mu`, age-off arm, E1 mask, fallback anchor (after am. 16).

## 2. Frame (`nimiwae_prep.py`)

- **C1 (M, needs a count) lineage breaks do not reach the frame.** `fia_curation_v3.lineage_break_visits` splits TREE numbers at KINDCD 1 / 3 (re-installation, replacement plot) and DESIGNCD changes, but the frame's slots come from the subplot span over every counted visit of the plot number (`span`, `sv`). A tree that debuts after a replacement therefore gets pre-debut slots from the old, physically different plot: FALSE soft labels where its spot was never sampled (the same error the E1 move rule fixes for microplot moves), and the subplot-level "not tallied" cut-off. Fix: cut the subplot span at the last break visit at or before the tree's debut (and treat the break like a move in the E1 mask).
- **C2 (L) `mv` keeps only the latest microplot move per plot** (`max(INVYR)`). A tree debuting between two moves gets no masking for the earlier one. Fix: mask by the latest move at or before the debut.
- OK: damage join on the as-recorded key with `tree % 100000` (one curated tree per raw key and visit); `dr` first dead / cut record after the last live one (re-records and misrecorded deaths fall out by construction; real slots are live: the export found 0 real slots at or after `dead_md`); `s0` excludes dead-cut closures; subplot context area-weighted over forest conditions; CONDID joined within a visit only; design levels from data; leave-one-out neighbours; climate interval years; plot-key split.

- **C7 (M) land-clearing cuts become deaths in the frame.** `nimiwae56.build`: `cause_cut = terminal_fate in CUT_FATES or AGENTCD == 80`, and `CUT_FATES` lacks `diverted_landuse`. A cut record on land that left forest (STATUSCD 3, fate `diverted_landuse`; GA alone had 7,110 such records in r3) with a null AGENTCD ends the lineage with cause death, which is wrong for E3 and the fate hazards. Fix: a STATUSCD 3 end is a cut whatever its fate (read STATUSCD in `dr`), or add `diverted_landuse` to `CUT_FATES` for records with an end date.
- **C8 (M, needs a count) frame real records depend on curated ages and biomass.** `build_tree_trajectories.build_live_body` keeps a live record only if `estimated_age` and `DRYBIO_AG` are present and age >= 1; the frame takes its real slots from those rows (audit F5 only checks frame = trajectories, not trajectories = curated live records). A live record dropped here becomes a pre / gap slot: a FALSE soft label or an unmeasured slot at a visit where the tree was tallied. It also makes the frame's tree set depend on the curated age imputation, which D70 is to replace. Fix: build the frame's real slots from `curated_trees` live records (STATUSCD 1, TPA > 0), not from the trajectory body; add an audit check that the two sets are equal.

## 3. Age data (`nimiwae_age_prep.py`, `nimiwae_age_direct.py`, `soil_extract.py`)

- **C9 (L) age records and soil use the as-recorded plot key; the frame uses the canonical key.** On the renumbered plots (34 annual links) a site tree's `plot_key`, split and fold can differ from the frame's for the same physical plot, so its record can sit in another split, and `linked()` misses it. Fix: map through `plot_canon_map` (or read the truth DB, which carries it). Same for the soil extract's coordinates (first-visit key instead of the latest location, D28).
- **C10 (L) three scripts read raw FIADB directly** (`nimiwae_age_prep.py`, `soil_extract.py`, `nimiwae70_eval.linked`), against D59's "no consumer reads raw FIADB". They read TREE / SITETREE / data_plot_eco read-only and by natural ids, so the results are right; the rule is not met.
- OK: carried-forward site cores dropped; DIAHTCD 1 for tally ages; identical duplicates collapsed with a disputed origin set unknown; total age = TOTAGE or BH age + l0; first record per tree per source for fitting; plot-hash folds; out-of-fold prior for train plots; trapezoid 0-30 cm soil weights and raster scales; NaN for nodata and missing nodes.

## 4. Identity (`dbconn.py`, `fia_ids.py`, `fia_truth_assemble.py`)

- OK: canonical plot key from tree evidence (orphan test, one-to-one links, chain roots, collision check); views apply it to every PLT_CN-keyed table; CN used only as a quarantine check; `raw` schema keeps as-recorded keys (so the frame's damage join on `*_raw` keys is right) with a natural visit id; `pick` refuses ties.

## 6. Ages (`age_imputation.py`)

- OK, as decided in D5 / D40 / D46: planted TreeAge mapped onto the field scale (monotone, accepted only if it beats raw out of fold), natural rows on raw TreeAge, targets split by stratum, bagged boosters with row-keyed bootstrap and validation draws, cross-fitted predictions for training rows, recalibration per stratum x DIA class, priority TOTAGE > estage > model > STDAGE, every live record with a DIA gets an age (so C8 is probably small: it is then driven by missing DRYBIO_AG).

## 7. Cohorts and drops (`cohorts_landis.py`, `cohort_tracking.py`, `cohorts_landis2.py`, `build_stdorg.py`, Sim B, `cohort_disturbance.py`, `attach_disturbance_simB.py`, `export_reference_tables.py`)

- **C1 also reaches the cohorts:** phantom backfill (`apply_backfill_blocks`) has no block for KINDCD 1 / 3 or DESIGNCD-change visits, so phantoms are made at the old plot's visits, and the subplot time axis spans the replacement.
- **C3 also reaches Pan:** an inferred removal is a disturbance event (`disturbance_events`), so a microplot that was not sampled at the next visit sets `subp_has_dstrb` (Pan's `skip_disturbances` then drops the subplot) and puts a Sim B drop on the cohort.
- **C11 (L) `cohort_tracking` birth dates use 365-day years** (`MEASDATE - round(age) * 365 days`): two trees of the same age first seen at different visits get birth dates days apart, so they are different cohorts. `curated_cohorts` only; Pan reads the sim_year tables.
- **C12 (L) `sim_year_map` ranks visits by MEASDATE alone** (`rank('ordinal')`): two visits of a subplot on the same date (allowed since D69) would be ordered by row position. None in R4 now (audit T10). Fix: rank by (MEASDATE, INVYR).
- OK: disturbance definition (first dead / cut record, re-records and misrecorded deaths out, diversions not cuts, first-record deaths not drops); per-member cohort backfill blocks; nonforest test on the microplot for saplings; sapling rates per tree; Sim B time axis on all live rows before the sapling filter; timber-only drop pairs; full removals on the scaffold axis; land use from product visits only; reference export limited to product visits.

## 5. Trees (`fia_curation_v3.py`), read to line 2000

- **C3 (M) microplot trees whose microplot was not sampled at the next visit become inferred removals.** `final_fate_expr` -> `disappeared_inferred_removal` whenever the subplot is revisited without the tree, unless the next visit is entirely nonforest (subplot AND microplot forest share 0). A sapling (microplot-tallied) whose microplot was nonforest or unsampled while the subplot stayed forest is unobserved, not removed. In the frame this becomes `is_inf_cut`: a definite end with cause cut. Same for trees of the old plot at a KINDCD 3 replacement visit (C1). Fix: test the tree's own tally area (microplot share for trees last tallied below the subplot limit); unobserved -> censored.
- **C4 (L) reset loss can be faked by a sapling re-tally.** `detect_condition_resets` counts live trees, saplings included; a silent / RC8 re-tally visit loses most saplings, which with an STDAGE drop makes `stdage_drop_with_loss`. Fix: count trees >= 5 in, or exclude procedural-break visits from the loss.
- **C5 (L) dominant FORTYPCD / STDORGCD of a subplot-visit counts conditions, not area** (`build_subplot_year_scaffold`, `mcnt`): a 5 % sliver ties with the 95 % condition and the smaller code wins. Feeds `curated_snapshots` only (the NIMIWAE frame area-weights its own). Fix: weight by SUBPCOND_PROP.
- **C6 (L) `sql_cond_link` uses `dbconn.nk` but `dbconn` is imported only under `__main__`**: importing the module and calling `run_pipeline` elsewhere raises NameError. No effect on the chain (run as a script).
- OK (2000-2773): estage anchors (DIA >= 5, Tree_Age > 1, median), discrepancy flags, snapshots (live, re-records and misrecorded deaths out of fate counts), per-state run order (split -> re-key -> censor columns -> fates -> ages), raw plot keys, RC7-withdrawn visits, FIX column order.
- OK: base scaffold filters (annual, PLOT_STATUS < 3, SUBP <= 4, SUBP_STATUS not 3); forest shares sorted before summing; procedural breaks (rc8, unrecorded tiers, MICROPLOT_LOC moves on sampled visits); status-0 events with closure kinds; censor events; land-use exits; RC7 withdrawal; cond_link from PREVCOND; live / dead record rules; tree-number split rules and event re-keying (ASOF on the segment start); misrecorded deaths; re-records; diverted cuts; harvest inference; reset detection on cond_link; STDAGE eligibility (planted species, post-reset debut, size gate, live only); median anchors.
