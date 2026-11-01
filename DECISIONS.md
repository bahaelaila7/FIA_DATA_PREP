# Curation-fix decision log

Every decision point of the curation fix (branch `curation-fix`), with the options considered. Since 2026-10-26 the agent takes the recommended
option without asking (user: "go with your recommendations, but document the decision point and the different options"). Each entry says who
decided and how to reverse it.

## D1 — Sapling gap (2026-10-26, user)
- **Evidence:** about half the live saplings were missing in AL / GA / NC / SC / TN. The cause was the tree-level RECONCILECD / STATUSCD 0 exclusion; in GA, 58.8k of the 59.5k missing saplings belong to trees with an excluded code.
- **Options:**
  - (a) **fix the curation upstream** and re-derive every model's inputs;
  - (b) patch the downstream tables;
  - (c) document and ignore.
- **Chosen:** (a).

## D2 — Age scale (2026-10-26, user; revised for natural stands by D5)
- **Evidence:** TreeAge (Zenodo 14775738), the estage source for trees ≥ 5 in, runs older than FIA field ages in planted loblolly. Saplings are model-aged on field ages, which left about a 2.5-yr step at 5 in.
- **Options:**
  - (a) **field scale**: map TreeAge onto field ages;
  - (b) TreeAge scale;
  - (c) field ages only, dropping TreeAge.
- **Chosen:** (a).

## D3 — Age model targets after D2 (2026-10-26, agent)
- **Evidence:** natural stands still stepped 12–15 yr from 3–5 in to 5–7 in.
- **Options:**
  - (a) **train the model on field ages plus mapped TreeAge** (≥ 5 in);
  - (b) restrict STDAGE truth by stand type;
  - (c) only flag out-of-domain ages.
- **Chosen:** (a), with plot-grouped folds shared by the mapping and the model.

## D4 — Natural-stand sapling / timber step (2026-10-26, user)
- **Evidence:** after D3, the step check (h) still failed in natural stands (AL 12.5, KY 14.5 yr).
- **Options:**
  - (a) **accept, flag `natural_uncalibrated`, and enforce (h) for planted stands only**;
  - (b) enforce continuity by growth back-extrapolation;
  - (c) look for TOTAGE states first.
- **Chosen:** (a).

## D5 — Natural-stand age scale (2026-10-26, user; supersedes D2 for natural stands)
- **Evidence:** compared with bored SITETREE ages (independent truth):
  - planted stands on the field scale: −0.9 to +1.9 yr;
  - natural stands on the field scale: −3.3 to −15.5 yr;
  - natural stands on raw TreeAge: −1 to +4 yr.
  STDAGE is a stand-level age, broadcast onto individual trees.
- **Options:**
  - (a) **split by stratum**: planted keep the field-scale mapping, natural use raw TreeAge (model targets TreeAge), and site trees stay an independent check;
  - (b) calibrate natural TreeAge to site trees on half the plots;
  - (c) keep the field scale everywhere.
- **Chosen:** (a). `age_domain` becomes `natural_treeage_scale`.

## D6 — Two narrow failures after D5 (2026-10-26, user)
- **Evidence:**
  - (k) KY natural: −5.7 yr against bored ages (limit ±5); raw TreeAge alone is already −5.0.
  - (j) FL planted 15+ in: the model runs 3.5 yr younger than mapped TreeAge (limit ±3); this only affects trees with no TreeAge record.
- **Options:**
  - (a) **accept + flag**: named KNOWN exceptions that still FAIL if they get more than 1 yr worse, plus `age_domain` flags;
  - (b) calibrate KY natural to site trees;
  - (c) widen the limits for every state.
- **Chosen:** (a).

## D7 — Decisions taken by the agent while fixing audit findings
- **Silent re-tally detector (r2 B2).**
  - Options: plot tier only (≥ 3 saplings and ≥ 50 %), transition tier only, or **both tiers**.
  - Chosen: both. The plot tier alone missed the KY plots with 1–2 saplings.
- **Stand reset for STDAGE eligibility (r3 I2).**
  - Options: any STDAGE drop, as before; **an STDAGE drop with ≥ 50 % loss, or a cut with regeneration or that loss**; or no STDAGE at all in natural stands.
  - Chosen: the middle option. It is the documented intent, and partial cuts no longer count as resets.
- **Age-model bias at the extremes (r3 I3, r4).**
  - Options: per-source p90 trim, as before; **a robust trim (p99.5 / 3× median) plus isotonic recalibration of out-of-fold predictions**; or quantile regression.
  - Chosen: the middle option. Regression toward the mean within true-age deciles remains and is documented: a monotone map from prediction to target cannot remove bias at a given true age.
- **Rounding.** Polars rounds x.5 to even; DuckDB rounds half away from zero.
  - Chosen: round half away from zero everywhere, so Sim A/B and the trajectories share one time axis.
- **Reused tree numbers (r3 M3).**
  - Chosen: a new tree id (TREE + 100000·k) when the PREV_TRE_CN chain breaks, rather than merging.
- **Land-clearing removals (r3 M6).**
  - Chosen: `diverted_landuse`, decided from condition status rather than GRM, so the check is not circular.

## D8 — Exact form of the KNOWN-exception mechanism (2026-10-26, agent; implements D6)
- **Evidence:** in the r4 whole-state runs exactly two enforced cells fail.
  - (k) KY natural: −5.7 yr, limit ±5 (raw TreeAge −5.0).
  - (j) FL planted 15+ in: −3.5 yr, limit ±3.
  Every other cell in AL, KY, GA, TN and FL passes.
- **Options:**
  - (a) **an explicit `KNOWN_EXCEPTIONS` dict in `verify_curation_v2.py`**. Keys are (check, STATECD, stratum[, DIA class]); each entry holds the observed value, the limit, the reason and the `age_domain` flag. A failing excepted cell prints `[KNOWN]` while |value| ≤ |observed| + 1 yr (`KNOWN_DRIFT`); beyond that it FAILS, with "known exception DRIFTED" in its name.
  - (b) per-cell widened limits (limit = observed + 1). The result would read PASS, so the exception is hidden in the output.
  - (c) INFO for the two cells. This drifts silently, which the user ruled out.
- **Chosen:** (a).
  - Only exact key matches are excepted; every other cell and state stays strict.
  - The drift test compares magnitudes, so a cell flipping sign past the bound also fails.
  - If an excepted cell starts passing, the verifier prints an INFO "now passes (consider retiring it)".
  - The summary lists all KNOWN cells.
  - The recorded values come from whole-state runs; a county-subset run may land elsewhere and then fail.
- **Reverse:** delete the entry from `KNOWN_EXCEPTIONS`; the cell is strict again.

## D9 — `age_domain` known-bias flags: naming, scope, precedence (2026-10-26, agent; implements D6)
- **Evidence:** as in D8. The KY bias comes from TreeAge itself, so it touches estage-sourced as well as model-aged records. The FL bias appears only in model ages, i.e. trees without a TreeAge record.
- **Options:**
  - (a) **override `age_domain`** on the matching rows. The values are `known_bias_ky_natural_treeage` (STATECD 21, natural = STDORGCD ≠ 1 or null, age_source estage or model) and `known_bias_fl_planted_model_15plus` (STATECD 12, STDORGCD = 1, age_source model, DIA ≥ 15 in).
  - (b) a separate `age_flag` column, leaving `age_domain` untouched. This adds a 31st FIX column.
- **Chosen:** (a), as the user asked to extend `age_domain`. The rules live in one table, `age_imputation.KNOWN_BIAS_FLAGS`, applied by `flag_known_bias()` after the model domains.
  - **Consequences:**
    - KY natural estage rows, previously null, now carry the flag.
    - FL planted model ages at 15+ in leave `in_support` / `out_of_support`.
    - The verifier re-derives both rules independently and checks they hold exactly (0 missing, 0 extra).
- **Reverse:** remove the entries from `KNOWN_BIAS_FLAGS` and rerun from step 2.

## D10 — Cohort-level known-bias columns (2026-10-26, agent)
- **Options:**
  - (a) **one generic pair, `n_trees_known_bias` / `share_known_bias`** (AGB-weighted in Sim B), counting members whose `age_domain` starts with `known_bias_`;
  - (b) one pair of columns per flag (four columns now, and more for every future flag).
- **Chosen:** (a).
  - Each flag is tied to one state, so the cohort's state identifies which flag applies.
  - The schema stays fixed when flags are added or retired.
  - The pair is appended after `age_source_major` on curated_cohorts_landis, `_stdorg` and `_simB`, and the slim tables get it too. Old columns are unchanged.
- **Reverse:** drop the two names from `cohorts_landis.AGE_META_COLS` and from the column lists in cohorts_landis2, cohorts_landis_simB, attach_disturbance_simB, slim_guard and the verifier.

## D11 — Keeping `*_natural_treeage_scale` meaning "natural model ages" (2026-10-26, agent)
- **Evidence:** with D9 in place, KY natural model ages no longer carry `natural_treeage_scale`. A plain equality test would drop the KY `share_natural_treeage_scale` to 0.
- **Options:**
  - (a) **count `natural_treeage_scale` plus `known_bias_ky_natural_treeage` with age_source = model** (`cohorts_landis.IS_NATURAL_MODEL`);
  - (b) let the share follow the literal domain value.
- **Chosen:** (a). The old column keeps its r4 meaning, and the user said old columns stay unchanged.
- **Reverse:** set `IS_NATURAL_MODEL` to the plain equality.

## D12 — Check (d) for natural stands after D5 (2026-10-26, user rule applied by the agent)
- **Evidence:** re-enforced in r4, (d) natural failed in GA (1/25 species non-monotone) and FL (5/23). It passed in AL, KY and TN (0/25).
- **Options:**
  - (a) **keep it INFO, as the user's rule says ("keep INFO only if it fails")**;
  - (b) enforce it per state where it passes;
  - (c) add it as a third KNOWN exception.
- **Chosen:** (a). Planted (d) stays enforced and passes in all five states.
- **Reverse:** set the natural tuple's enforce flag to True in the (d) loop of the verifier.
- **D8–D12 outcome (2026-10-26):** whole-state KY and FL reruns give 0 FAIL, with one KNOWN cell each (KY (k) natural −5.7; FL (j) planted 15+ −3.5).
  - Flags: KY 239,128 live records; FL only 6 live model ages. FL's (j) trees carry their TreeAge age as the final age.
  - The KY step-2 resume (`FROM_STEP=2`) is EXCEPT-identical.

## D13 — Natural-stand site-tree check after audit r4 (2026-10-26, agent; per the user's rule; refines D5 / D6 / D8)
- **Evidence (r4 audit, NC / SC / MS whole-state):** raw TreeAge against bored SITETREE ages splits by wood group in natural stands.
  - Softwoods run OLD: NC +6.4 / +7.9, SC +5.7 / +6.3, MS +7.0 / +4.2 yr.
  - Hardwoods run YOUNG: NC −2.9, SC −1.4, KY −6.4 yr.
  - The stratum medians pass only where the two cancel (NC +3.0, SC +4.0). MS fails at +5.9, and KY's −5.7 is the hardwood side.
  - The +2 / +5 yr breast-height allowance alone moves the two groups 3 yr against each other.
  - 72–92 % of site-tree links are proxy matches (a unique live tree on the same condition, species and DIA).
  - D5's stated evidence ("natural raw TreeAge −1 to +4 yr") was wrong: KY was −5.0 even in its own table, and MS is +6.0.
- **Options:**
  - (a) add a KNOWN exception per failing state (MS now, more in a Region-4 run): exception creep, and it hides the wood-group pattern;
  - (b) calibrate natural TreeAge onto site trees per wood group, cross-fitted on half the plots: it uses the only per-tree truth, but that truth is weak here (dominant trees, mostly proxy links, an allowance uncertain by ±3 yr), the check stops being independent, and the user declined this option in D5;
  - (c) **natural (k) becomes INFO, reported per state × wood group (softwood / hardwood) × link method; natural ages stay on the TreeAge scale (D5) and remain flagged `natural_treeage_scale`; the KY-specific KNOWN entry and flag are retired, since all natural ages already carry the natural flag; planted (k) stays enforced; D5's evidence text is corrected**;
  - (d) widen the natural limit, which the user declined in D6 (c).
- **Chosen:** (c). The natural age scale is decided (D5). What the site trees give in natural stands is a documented uncertainty of about ±6 yr by wood group, not a reliable gate.
- **To reverse:** set natural (k) back to enforced, and pick (a) or (b).
- **FL (j):** stays a KNOWN exception (D6). It must be re-measured under the pooled Region-4 fit (D14).

## D14 — Exception values under a pooled fit (2026-10-26, agent)
- **Context:** the age model and the TreeAge mapping are fitted on all states in the DB. The recorded KNOWN values came from single-state fits, so a Region-4 run may drift past them.
- **Options:**
  - (a) re-measure them in the production Region-4 run, which may FAIL it;
  - (b) **measure them on a pooled multi-state test DB now (FL + KY + MS), and record the values as a function of the fit scope**;
  - (c) loosen the drift tolerance.
- **Chosen:** (b).

## D15 — Canonical plot key (audit r4 M-2; 2026-10-26, agent)
- **Evidence:** 75 annual plot visits in FIADB change (UNITCD, COUNTYCD, PLOT) along their PREV_PLT_CN chain, most of them into a different county. In R4: AL 3, FL 4, GA 5, KY 5, MS 0, NC 11, SC 2, TN 4 (audit a04). GA 13/2/87 146 → 45 → 146 is one example. A renumber splits one plot into two, so its trees "disappear" and reappear as new ones: GA `disappeared_inferred_removal` was 101 trees, 86 of them on such plots.
- **Options:**
  - (a) **remap in the dbconn raw views.** Every plot-keyed raw view (PLOT by CN, every other table by PLT_CN) carries the canonical key = the key at the chain's FIRST ANNUAL visit, so every stage sees one key per physical plot before anything else;
  - (b) remap only in v3's output tables. Later stages join raw COND / PLOT / data_plot_eco by plot key and would mismatch;
  - (c) drop the renumbered plots.
- **Chosen:** (a).
  - Guards: a chain that does not terminate, or canonical keys that collide on a plot-INVYR, stops the run (0 collisions in FIADB).
  - `curated_trees` appends `unitcd_raw`, `countycd_raw` and `plot_raw`. All three are kept, not just PLOT, because most renumbers also change the county.
  - The verifier rebuilds the map independently (a recursive CTE over the raw FIADB, attached as `fraw`, and an in-memory `f` catalog of remapped views) and checks (n6).
  - Map cost: about 3 s per stage connection.
- **Reverse:** make `build_plot_canon_map` return an empty map (or create the plain views), and drop the three FIX columns.

## D16 — One age scale per tree (audit r4 M-5; 2026-10-26, agent)
- **Evidence:** trees whose condition STDORGCD changes between visits mixed field-scale-mapped (planted) and raw (natural) TreeAge candidates in one median anchor (AL 5,389 trees, GA 4,262; blended offsets −9.0 / −4.8 yr). Live trees spanning two STDORGCD codes in r5: KY 458, AL 8,250, GA 8,879, TN 3,130, NC 11,059, SC 6,729, MS 10,383.
- **Options:**
  - (a) **the stratum at the tree's FIRST LIVE record** (a tree with no live record takes its first record; null or other codes = natural);
  - (b) the majority over its live records;
  - (c) the latest record;
  - (d) keep it per record (status quo).
- **Chosen:** (a).
  - It is the stand origin when the tree was first seen, and it is stable as later visits are added; the majority rule could flip it.
  - The new FIX column `age_stratum` (v3 `add_age_stratum`) drives every age-scale decision in step 2: the calibration-set planted split, `remap_estage`, training strata, the recalibration cells, support cells, the `natural_treeage_scale` domain and the known-bias flags. `cond_stdorgcd` stays a model FEATURE.
  - Check (j) and the flag rule use `age_stratum`. Checks (d), (h) and (k) keep the record's stand type, since they describe stands rather than age scales.
- **Reverse:** set `PLANTED` in age_imputation back to `(cond_stdorgcd == 1)`.

## D17 — Cross-fitted production predictions (audit r4 M-6; 2026-10-26, agent)
- **Evidence:** the recalibration map was fitted on out-of-fold predictions but applied to in-sample predictions of the training rows. Planted 5–10 in model age minus the tree's own field age, mean / median: AL +0.05 / +0.68 yr, NC +0.57 / +1.22 yr (the audit quoted +0.85 / +1.16).
- **Options:**
  - (a) **training rows (kept, trained-on) use their outer out-of-fold raw prediction, matched by tree × MEASDATE; every other row uses the full-data model; the map is applied to both**;
  - (b) apply the map only to non-training rows;
  - (c) fit the map on in-sample predictions.
- **Chosen:** (a). Outlier rows that were scored but not trained on use the full model.
  - After: AL −0.02 / +0.62 yr, NC 0.00 / +0.92 yr (MAE AL 3.19 → 3.28, NC 4.60 → 4.59).
  - Training rows using OOF predictions: AL 389,830 of 576,536 live rows; NC 317,216 of 506,268.
- **Reverse:** drop `oof_tab` in `apply_age_model`.

## D18 — One sim_year axis (audit r4 M-1; 2026-10-26, agent)
- **Evidence:** each table anchored `sim_year_map` on its own "first real row". Sim A then re-based phantoms to ≥ 0, while Sim B and the trajectories did not.
  - Shared subplot-visits with a different Sim A vs Sim B sim_year: KY 407 / 32,806, FL 1,284, AL 2,188, GA 3,440, TN 779, NC 1,667 / 52,474, SC 1,775, MS 1,727.
  - The trajectories reached sim_year −19 to −27.
- **Options:**
  - (a) **anchor at the subplot's FIRST SCAFFOLD visit (clean_subplot_years) for every table, and drop Sim A's re-base**;
  - (b) re-base every table the way Sim A did;
  - (c) anchor at the first live tree.
- **Chosen:** (a), per the batch.
  - Sim A's sim_year 0 can now be a scaffold visit with no live tree; the gap to the first live row is real elapsed time.
  - Every caller already passes the full scaffold, so the `anchors` argument of `sim_year_map` is ignored.
  - Check (n1): 0 differences and sim_year 0 at the first visit in all states.
- **Reverse:** restore the `anchors` join and the Sim A re-base.

## D19 — Sim B drops from timber members only (audit r4 I-1; 2026-10-26, agent)
- **Options for the backing check:**
  - (a) **removed / died, or alive with DAMSEV > 40** (the drop model's own damaged-alive branch);
  - (b) removed / died only, as in the batch text.
- **Chosen:** (a). Option (b) would flag legitimate damage drops. The check reports separately how many drops are backed only by damaged-alive members: 0 in every state.
- **The drops themselves:**
  - A pair counts only if the tree is a REAL record with DIA ≥ 5 at the previous visit and, if alive at the visit, real with DIA ≥ 5 there too.
  - First Sim B rows are removed from `cohort_disturbance` as well, so the table and the attached column agree.
  - That first-row rule removed 0 drops in every state: with timber-only members, a first row has no previous members.
  - **Found by the n2 check (FL 12/2/73/23 subp 3):** a tree that is still ALIVE at the visit but no longer a real ≥ 5 in record there (it shrank from 5.0 to 4.9 in, or the slot is gap-interpolated) was counted as a removal. Such pairs are now dropped: neither survivor nor removal.
- **Reverse:** `compute(..., timber_only=False)` in attach_disturbance_simB, and drop the `_first_rows` delete.

## D20 — Phantom biomass (audit r4 M-8; 2026-10-26, agent)
- **Options for the growth rate:**
  - (a) **per-tree biomass (Sim A agb / tpa; the trajectories' agb is per tree already), from REAL rows only**;
  - (b) per-tree biomass on all rows, phantoms included (circular: the cohort pass trained on tree-pass phantoms).
- **Chosen:** (a), and a phantom is capped at the debut value it is scaled from (`min(agb, …)`).
  - Training saplings with a negative raw rate (clipped to 0): NC 2,485 of 31,537 (was 5,298 on TPA-expanded AGB). What remains is real shrinkage.
- **Reverse:** remove `_bio`, the real-row filter and the `.clip(upper_bound=agb)` in cohorts_landis / build_tree_trajectories.

## D21 — Suspected false tree-number splits (audit r4; 2026-10-26, agent)
- **Evidence:** across the 8 states, the only splits linked through a dropped STATUSCD 0 record are 5 AL keys (Mobile Co. plot 105, an RC9 nonsampled visit in 2001, then RC3 entries in 2007). Each has evidence of a different tree: a dead record before (2), a species change (3), or a DIA drop of up to 25 in. 0 splits lack such evidence. GA's 176 split segments are other-key links (46) or dead-then-unlinked (130).
- **Options:**
  - (a) **no rule change, plus an INFO monitor (n7) counting status-0-linked splits without evidence**;
  - (b) add the evidence requirement now.
- **Chosen:** (a). The suspicion is not confirmed, and (b) would change 0 rows while adding a raw TREE lookup to step 1.
- **Reverse:** n/a (implement (b) if n7 ever reports > 0).

## D22 — Verifier independence and slim schema (audit r4 M-7; 2026-10-26, agent)
- **(b):** the re-tally candidate set (`ub`) is rebuilt from raw FIADB with the documented plot / transition tiers plus RC8-closure visits that have unrecorded saplings. The mismatch with `procedural_breaks` is reported as INFO; its consequences are enforced through 1g (phantoms across a re-tally).
- **(c2):** a `subp_has_dstrb` subplot needs a raw FIRST dead/cut record (not a re-record) on forest that is a cut, a harvest-inferred death (AGENTCD null, TRTCD 10) or a death under a disturbance code.
  - Enforced: flagged ⇒ attributed.
  - INFO: attributed but not flagged (trees without an age or cohort).
- **Slim:** ENFORCED that old tables keep their columns as an exact prefix (verifier section 6), and in `slim_guard.py post` via `SLIM_REF` (optional env; the r5 runs pass the R4 slim).
- **Reverse:** revert the `ub` block to `new.procedural_breaks`, and drop the checks.

## D23 — Determinism of steps 9–10 (audit r4 M-3; 2026-10-26, agent)
- **Chosen:**
  - Sim B AGB rounded to 6 dp; shares and the drop fraction to 9 dp; `dropped` / `would_be` sums to 6 dp.
  - `cohort_disturbance` sorted on its key, and the attach ordered.
  - (e) also compares `curated_cohorts_landis_simB` and `cohort_disturbance` after rerunning steps 4–10 into a second Sim B DB (`--determinism-simb-db`).
- **Reverse:** remove the rounding.

## D24 — FL (j) under the pooled fit (D14 outcome; 2026-10-26, agent)
- **Evidence:** single-state FL fit (r5): (j) planted 10–15 in and 15+ in both pass (15+ −1.6 yr). Pooled FL + KY + MS fit: 10–15 in **−3.2** (5,243 trees, a NEW failing cell) and 15+ in **−3.8** (431 trees). The 15+ cell moves 2.2 yr between fit scopes (> 1 yr), so per D14 the value is recorded per fit scope.
- **Options:**
  - (a) **key KNOWN_EXCEPTIONS by FIT SCOPE** (the DB's sorted STATECDs).
    - Retire the single-state FL entry (it passes).
    - Record pooled-scope entries for 10–15 (−3.2) and 15+ (−3.8).
    - Flag them `known_bias_fl_planted_model_10plus` (FL planted model ages, DIA ≥ 10 in) only in that fit scope. The old `…_15plus` flag is retired.
  - (b) tune the pooled model (e.g. a state feature). The user's D6 rule is not to tune these cells.
  - (c) apply the pooled values to every scope, which hides the fact that the single-state fit passes.
- **Chosen:** (a).
  - Same nature as D6: regression to the mean, which only reaches trees without a TreeAge record.
    - **r7 correction (audit B M-2): this mechanism does not hold against field truth.** In the pool, on FL planted records that have a model, an estage and a field age, model − field is −0.47 (10–15 in) / −0.33 yr (15+ in) while mapped − field is +1.94 / +0.81 yr: the (j) gap sits largely on the MAPPED side. The 88 flagged records all have a field STDAGE and take the model age; their final − field age is median +2.33 / mean −4.83 yr (single-state fit +3.33 / −2.67). The entries are re-measured under D40.
  - A scope with no recorded entries is fully strict: a Region-4 fit must re-measure these cells and record its own values, which is the point of D14.
- **Reverse:** delete the scope entries from `KNOWN_EXCEPTIONS` and `KNOWN_BIAS_FLAGS`.

## D25–D30 — Round-6 audit (2026-10-27, agent; the implementer records the details and outcomes)
- **D25, DAMSEV damaged-alive drop branch:** removed as dead code. DAMSEV is a 0–9 class code, never > 40 in R4. D19's rationale is corrected.
  - Options rejected: inventing damage biomass from damage codes; keeping the dead code.
- **D26, resurrected trees:** a dead / cut record followed by a PREV_TRE_CN-linked live record is a misrecorded death. It is excluded from fates and events, and the tree continues.
  - Options rejected: end the tree at the death; leave as is.
- **D27, gap interpolation:** kept. Check (n3) is INFO when it has no candidates, and a synthetic fixture test exercises the code.
- **D28, plot location:** the most recent visit's coordinates / ecoregion. The key, COUNTYCD included, stays the first-visit key (r7 doc fix: the latest county is only in `plot_location.LOC_COUNTYCD`).
  - Options rejected: the first-visit location; per-visit locations.
- **D29, renumbers without a PREV_PLT_CN link:** linked only on identical coordinates plus ≥ 30 % tree continuity.
  - Options rejected: ignore them; coordinates alone.
- **D30, consumer requirement (not a curation change; r7 doc fix: recorded in detail as D32, while the detailed D30 is the sapling-rate basis):**
  - Pan (landis_region4 src/data/utils.jl:371-372) recomputes its own sim_year and must use the curated axis.
  - Pan's drop consumer (utils.jl:398) defines the drop semantics the curated table must match (audit R6).
  - Both are recorded for the Pan rebuild phase.

## D31-G — Generalisation beyond Region 4 (2026-10-27, agent; recorded for the expansion phase; renumbered from a second "D31" in r7)
- **Evidence (FIADB annual live trees since 2000, by TPA_UNADJ):**
  - Interior West, North Central, Northeast and the other southern states use only 6.018 (subplot) and 74.965 (microplot), as R4 does.
  - PNW / CA also have **0.999 (macroplot) on 16.6 % of live trees**.
- **Generic, carries over:** the RC / STATUS semantics, censoring, re-records, resurrected trees, renumber links, date guards, silent re-tally detection (learned per state × MANUAL), the time axis, determinism, and the verifier.
- **Must be re-derived per region:**
  - rule (c) and every two-tier (subplot / microplot) assumption, before PNW / CA;
  - the D5 age-scale split and the natural-stand TreeAge bias pattern (measured in SE states);
  - the KNOWN exceptions, which are fit-scope keyed anyway.
- **First step of any expansion:** whole-state chain + verifier on one state per new region (e.g. OR, CO, MN, PA) before modelling.

## D25 — The damaged-alive drop branch (audit r5 R3; 2026-10-27, coordinator option (a))
- **Evidence:** DAMSEV is a 0–9 class code (10 % classes), recorded only under MANUAL 1.0–1.6; the R4 maximum is 9. The `DAMSEV > 40` branch could therefore never fire, and FIA does not quantify the biomass lost to damage.
- **Options:**
  - (a) **remove the branch and correct D19's rationale**;
  - (b) re-define damage from the DAMSEV classes or DAMAGE_AGENT_CD, which would invent a biomass loss;
  - (c) keep the dead code.
- **Chosen:** (a). Drops come from removals only; `compute()` treats every live tree at the visit as a survivor.
  - **D19 correction:** its "damaged-alive backing" option was moot. No drop ever came from that branch (n2 "0 backed only by damaged-alive members" in r5).
- **Reverse:** restore the `counted` / `severe_dmg` expressions in `cohort_disturbance.compute`.

## D26 — Resurrected trees (audit r5 R4; 2026-10-27, coordinator option (a))
- **Evidence:** some trees have a live record after their first dead / cut record, linked back by PREV_TRE_CN with raw PREV_STATUS_CD = 2. These create false deaths and false Sim B drops (GA 3, NC 3, KY 2).
- **Options:**
  - (a) **the dead / cut record is a misrecorded death: excluded from fates and events, and the tree continues**;
  - (b) end the tree at the death and drop the later records;
  - (c) keep it as is.
- **Chosen:** (a). FIA's later live record with an explicit link is the stronger evidence.
- **Implementation:**
  - New FIX column `misrecorded_death`: a dead / cut record dated before the tree's last live record that has PREV_STATUS_CD = 2 and a PREV_TRE_CN. Such a record gets `tree_fate = 'misrecorded_death'`.
  - It is excluded from: the re-record count, `final_tree_fate`, `dead_at_first_visit`, snapshots, cohort_tracking events (by tree_fate), the trajectory record set, the interpolation / zombie dead-record sets, and cohort_disturbance.
  - That visit then becomes an ordinary gap, which can be interpolated live: GA 64 of 88 such visits.
  - The verifier re-derives the rule from raw (c1). Raw records that the curation itself drops (RC7) are out of scope there.
  - **Found by the first r7 run, then fixed:**
    - (a) The later linked live record can sit on a now-NONFOREST condition. The live rule keeps it out of curated_trees, so `tree_landuse_exits` now carries `linked_live_after_dead_md`, and it counts as resurrection evidence too (NC 4 records on 3 trees, TN 1, FL 1, pool 1 trees; r7 doc fix: NC was records, not trees). For the final fate a misrecorded death counts as alive at its visit, so such trees end `diverted_landuse`.
    - (b) cohort_disturbance joined the trajectories' gap slot at a misrecorded visit to that dead record, read STATUSCD 2 and counted a false removal (AL 1, SC 3, TN 2, KY 2 drops). Phantom / gap slots are now alive by construction, and misrecorded records are never a status source.
- **Reverse:** set `res_md` to null in `classify_tree_fate`.

## D27 — Gap interpolation that never fires (audit r5 R5; 2026-10-27, coordinator)
- **Evidence:** in R4 every candidate gap spanned a censoring event, so (n3) read "0 of 0".
- **Chosen:**
  - The code is kept.
  - (n3) prints INFO "no candidates" when there are none.
  - New `selftest_fixtures.py` (run by the verifier as n9) exercises interpolation (basic / nonforest / censoring) and the timber-only drop branch (removal, shrink to 4.9 in, gap-only slot, sapling).
  - With D26 the data now does produce gap slots (GA 64; r7 doc fix: the first r6 run's 67 included 3 slots of trees merged at a coordinate link, F7-1 / D36), and (n3) checks them.
- **Reverse:** n/a.

## D28 — Plot location of a renumbered plot (audit r5 R7; 2026-10-27, coordinator option (b))
- **Evidence:** for remapped visits the coordinates are 10–60 km away, and the L4 ecoregion differs in 20 of 54.
  - For plots whose chain contains a remapped visit, the latest visit differs from the first visit in these attributes (plots: LAT/LON / county / L3 / L4 / ECOSUBCD):

    | State | Plots | LAT/LON | County | L3 | L4 | ECOSUBCD |
    |---|---|---|---|---|---|---|
    | AL | 3 | 3 | 3 | 0 | 2 | 0 |
    | FL | 4 | 2 | 4 | 0 | 1 | 0 |
    | GA | 4 | 3 | 3 | 0 | 1 | 3 |
    | KY | 5 | 5 | 5 | 0 | 1 | 2 |
    | MS | 1 | 0 | 0 | 0 | 0 | 0 |
    | NC | 11 | 11 | 11 | 0 | 6 | 3 |
    | SC | 4 | 2 | 2 | 0 | 1 | 1 |
    | TN | 4 | 4 | 4 | 1 | 1 | 3 |

  - Pan places a plot with `ANY_VALUE(LAT/LON)` over its PLOT rows, so the location it uses was arbitrary.
- **Options:**
  - (a) keep the first-visit location;
  - (b) **use the most recent visit's coordinates / ecoregion for the plot's location attributes, and keep the first-visit key for identity**;
  - (c) per-visit locations.
- **Chosen:** (b).
  - The dbconn PLOT view gives every visit of such a chain the latest visit's LAT / LON / ELEV (TEMP `plot_loc_visit`; the curated table is `plot_location`).
  - The data_plot_eco view gives the canonical key the latest raw key's eco row (L3 / L4 / ECOSUBCD / lat / lon), so the cohort tables' epa_l3 / l4 and the exported copies follow it.
  - COUNTYCD stays part of the identity key and is not changed.
  - Check (n12).
- **Reverse:** drop the LAT/LON/ELEV replacement and the data_plot_eco view branch in `dbconn.connect`.

## D29 — Renumbers without a PREV_PLT_CN link (audit r5 S1; 2026-10-27, coordinator option (b))
- **Evidence:** 42 R4 chain starts sit at an earlier chain end's exact coordinates, some with tree continuity (GA 87/45 → 87/146: 29 of 68 trees match).
- **Options:**
  - (a) ignore them;
  - (b) **link a start to an earlier end when the coordinates are identical AND ≥ 30 % of the end's live trees match on SUBP / TREE / SPCD with non-decreasing DIA**;
  - (c) coordinates alone.
- **Chosen:** (b), plus these agent rules:
  - DIA must be strictly non-decreasing (`u.DIA >= t.DIA`; the auditor's c07 allowed −0.3 in).
  - Each start takes its best end (match share, then latest end), and each end is used once.
  - A link whose re-keyed chain would collide with another visit on a plot-INVYR is dropped. FIADB-wide: 1 dropped, 18 kept.
  - At a linked chain start FIA has no PREV_TRE_CN, so a same-number record whose predecessor is live starts a NEW tree when its SPCD differs or its DIA decreased (the evidence rule in `split_reused_tree_numbers`).
- **R4 links (end → start, trees matched):** GA 13/87/45 → 87/146 (29/68 = 0.43), MS 28/163/84 → 163/101 (30/81 = 0.37), SC 45/19/74 → 19/279 (31/49 = 0.63), SC 45/19/101 → 19/280 (4/4 = 1.00).
  - The auditor's AL 61/71 → 87 (17/37 with −0.3 in tolerance) passes the strict rule too (16/37 = 0.43) and was dropped only by the collision guard (r7 doc fix). Since r7 (D37) it is a duplicate visit: the link is kept and the 71 visit of 2010 is dropped.
- The verifier rebuilds the links independently, and its map equals dbconn's (117 remapped visits, 18 links).
- **Reverse:** set `COORD_LINK_MIN_SHARE` > 1 (no links).

## D30 — Sapling growth-rate basis (audit r5 R1; 2026-10-27, agent)
- **Evidence:** r5 inferred the biomass basis from the presence of a `tpa` column. The trajectories carry `tpa` too, so their per-tree drybio_g was divided by TPA a second time, and trajectory phantoms came out 4.4 % (KY) to 8.6 % (AL; r7 doc fix, was "7.8 % (NC)") too low.
- **Chosen:** `estimate_sapling_growth_rates(df, basis=...)` takes an EXPLICIT basis: `tpa_expanded` (Sim A, agb / tpa) or `per_tree` (trajectories). Anything else raises an error.
  - The rates are written as `sapling_growth_rates_simA` / `_traj`.
  - Check (n11): per species group with ≥ 50 training trees, the two agree within 10 % or 0.002 / yr.
- **D20 correction:** D20's "per-tree biomass (… the trajectories' agb is per tree already)" described the intent, not the r5 code. In r5 only Sim A used per-tree rates. D20's NC numbers (5,298 → 2,485 negative rates) are Sim A's. The r5 trajectory phantom shares in CLAUDE.md were biased low; r6 numbers replace them.
- **Reverse:** n/a (bug fix).

## D31 — Meaning of the stored Sim B drop (audit r5 R6; 2026-10-27, agent)
- **Evidence:** Pan's `_weighted_cohort_drop` (landis_region4 src/data/utils.jl) reconstructs a row's pre-drop biomass as `agb / (1 − drop)`, so it reads drop = dropped / (row agb + dropped). The stored drop was dropped / (the previous visit's timber members grown on). Rows with ingrowth carry more AGB than that base (GA 135 rows, up to 4.8×) and were over-dropped.
- **Options:**
  - (a) **store drop = (dropped / 4046.8564224) / (agb + dropped / 4046.8564224)**, with `dropped` in drybio_g × TPA (g / acre) and agb in g / m²;
  - (b) change Pan (not allowed: read-only).
- **Chosen:** (a), applied in attach_disturbance_simB to `cohort_disturbance` and to the attached column. `dropped` / `would_be` keep the drop model's quantities.
  - Check (n8): Pan's formula reconstructs `dropped` on every drop row (max relative error 1.5e−8 in GA).
- **Reverse:** remove the UPDATE in attach_disturbance_simB.

## D32 — Pan recomputes its own time axis (audit r5 R8; consumer issue, Pan not modified)
- **Evidence:** landis_region4 src/data/utils.jl:371–372 sets `sim_year = round(days(measdate − first measdate of id_cols) / 365.25)` per plot (or subplot), ignoring the curated `sim_year`, which is the strict per-subplot axis from the first scaffold visit (M-1 / D18).
- **Auditor's c08 counts** on the r5 slim: steps where Pan's step ≠ the curated step / visits whose levels differ / steps.

  | State | Sim A (stdorg) | Sim B | Old r4 slim, stdorg |
  |---|---|---|---|
  | GA | 408 / 2,149 / 76,835 | 947 / 5,563 / 68,093 | 1,154 / 6,361 |
  | NC | 223 / 1,074 / 40,865 | 448 / 2,675 / 36,455 | – |
  | AL | 260 / 1,568 / 50,590 | 582 / 3,675 / 44,180 | – |
  | KY | 135 / 960 / 24,988 | 147 / 1,010 / 23,934 | – |

- **Required change for the Pan rebuild phase:** read `sim_year` from the curated tables (per subplot) instead of recomputing it. Otherwise two visits that round to the same year (or a subplot whose first scaffold visit has no live tree) get steps that disagree with the cohort ages' integer offsets.
- **Reverse:** n/a (documentation).

## D33 — Fit scope of a county-subset run (audit r5 S2; 2026-10-27, agent)
- **Chosen:** v3 writes `run_scope(states, counties)`. The fit scope used by `KNOWN_BIAS_FLAGS` and `KNOWN_EXCEPTIONS` is `<states>` or `<states>|counties=<s:c,…>`, so a subset run never inherits pooled entries or flags.
- **County subsets (r7 doc fix, F7-13):** a county subset selects by the CANONICAL (first-visit) COUNTYCD. A renumbered plot recorded in another county later (e.g. KY 21/3/121/18, county 125 since 2015) belongs to its first county's subset.
- **Reverse:** drop the `|counties=` suffix.

## D34 — FIADB live-tree plots absent from the curated product (genveg T1m finding; 2026-10-27, agent)
- **Evidence:** of the FIADB plots with live trees inside the R4 L3 scope, the OLD slim_r4 product lacked GA 2,507 of 8,388, FL 250 of 3,662 and KY 79 of 2,650 (the T1m count of 337 used a narrower scope).
  - The curation-fix chain keeps all but GA 4, FL 5 and KY 10.
  - Each of those residual plots is an annual, forest, sampled plot ALL of whose live trees are recorded at the next visit as STATUSCD 0 + RECONCILECD 7, "erroneously tallied previously" (e.g. GA 13/4/151/7: 7 live trees in 1997, all RC7 in 1999). The documented RC7 rule drops the erroneous earlier records.
  - None is periodic-only, and none is lost to DESIGNCD or a visit-count rule.
  - The slim universe equals the curated plots with Sim A rows. Curated plots without Sim A rows have no live trees.
- **Options:**
  - (a) **intended: keep the RC7 rule; add check (n13)** (every FIADB live-tree plot in the state is in curated_trees, or all its live trees precede an RC7 record; unexplained = 0, absent share reported);
  - (b) keep RC7-erroneous plots, which re-introduces trees FIA withdrew.
- **Chosen:** (a). The old product's gap was the old curation's exclusions, which the curation fix already removed.
- **Reverse:** n/a.

## D35 — A coordinate-linked chain start is a procedural re-tally (2026-10-27, agent; follows D29)
- **Evidence:** with the D29 links alone, the earlier chain end's live trees that were not continued at the linked start (renumbered, or not re-found) became `disappeared_inferred_removal`: GA 15 → 42, SC 2 → 18, MS 0 → 33 trees in the first r6 run. The start's new numbers would also be backfilled as if they were ingrowth.
- **Options:**
  - (a) **treat the link visit like a procedural re-tally**: censor the end's live trees that are not continued (same SUBP / TREE / SPCD, DIA not decreasing — the D29 match) as `procedural_relink` at the start visit, and add a `procedural_breaks` row (`break_kind = 'coord_link'`) that blocks every tree's backfill across it;
  - (b) accept the false removals and false ingrowth;
  - (c) drop the links again (D29 (a)).
- **Chosen:** (a). It is the same treatment FIA's own re-tallies get (RC8 / silent re-tally), and the matched trees keep their continuity.
  - The verifier re-derives both parts independently: link breaks in rule (b), and check (n14) "every end tree is continued or censored". `procedural_relink` also counts as censoring in the (b) sapling census.
- **Reverse:** remove the `_relink` block in `sql_excluded_trees` and `_brk_link` in cohorts_landis.

## D36–D45 — Round-7 audit (2026-10-27, coordinator; 6 slices, reports in scratchpad/curation_audit_r7/rep_{A..F}.md)
The chosen option comes first; rejected options follow "Rejected".

### D36 — Tree identity at reused numbers and coordinate links (F7-1 IMPORTANT, A7-1, F7-2, F14)
- A record starts a new tree when PREV_TRE_CN is null and RECONCILECD is a new-tree code (RC 1 ingrowth / 2 through-growth / 3 missed / 4 shrank-in). This applies whatever the previous same-number record's status was, live or dead (A7-1 extends D7).
- Any key censored `procedural_relink` (D35) is closed: a later record under that number is a new tree.
- Censor and status-0 events are keyed by the post-split tree id. Split trees never inherit the old id's censoring (F7-2, C-F14).
- The verifier re-derives the split independently. n14 also checks that relink-censored keys never reappear and that continued trees are not censored.
- Rejected: keeping the live-predecessor exception (it merges physically different trees: species relabelled, bogus cohorts).

### D37 — Duplicate same-INVYR visits at identical coordinates (A7-3 / F7-7)
- Two plot-visits with the same INVYR, identical LAT/LON and a D29 tree share ≥ 30 % are one physical visit: keep the visit with more live records and drop the other's records, logged in `plot_key_remap` with reason `duplicate_visit`. Only AL 1/3/61/71 vs 87 qualifies in R4.
- The collision guard otherwise stays.
- The D29 share's denominator is live SUBP ≤ 4 trees, as in the D35 census (F7-6 / A7-8). No R4 link flips.

### D38 — Link-start semantics (F7-8)
- At a D29 link start on a KINDCD 3 (replacement) plot, dead records are first-seen deaths (`first_record_dead`). They are not mortality attributed to the link interval and produce no drop.
- `first_record_dead` generalises `dead_at_first_visit` to any tree whose first record is dead (A7-11).

### D39 — Visits whose live trees were all withdrawn (A7-4)
- Flagged per visit (`all_live_withdrawn_rc7`) in curated_trees and in the cohort tables' visit metadata.
- Not dropped: the dead tally may be valid. Consumers decide; genveg's G51.1 treats such a visit as incomplete.
- RC7 dropping of earlier records stays as D34 (A7-5 noted).

### D40 — Ages (B I-1 / I-2 IMPORTANT, M-1 / M-6 / M-7 / M-8 / M-9 / M-10 / M-11)
- **Mapping.** The TreeAge → field mapping is fitted and selected on PLANTED rows only, because it is applied only to planted rows (D5). The pooled fallback is a planted-only pool. Natural rows never enter calibration. The stratum "split" decision is therefore moot and removed.
- **Mapping is enforced.** When calibration returns None (fewer than 200 planted rows), planted estage is flagged `unmapped_estage` and the verifier FAILs unless the cell is listed (M-6).
- **Recalibration acceptance**, pre-registered now: the isotonic recalibration is applied per stratum × DIA-class cell only if the nested out-of-fold MAE decreases. Otherwise that cell is identity (M-7).
- **Determinism and variance.**
  - Training rows are sorted by CN.
  - HGBT gets an explicit random_state and a seeded validation split.
  - The model is a 5-seed bagged ensemble (mean of members), which cuts the chaotic sensitivity.
  - The verifier's determinism check covers step 2 on a one-county subset at threads 1 vs 3.
  - Rejected: a single model, whose 1-row perturbation moved ages up to 9.9 yr.
- **Nesting fixes.** Inner-fold mapping excludes inner calibration plots; outlier thresholds are computed per training fold; categoricals are passed as `categorical_features` (M-8).
- **Support flag.** Natural saplings under 5 in get `out_of_support` (M-9).
- **Stand resets.** Loss is measured across the subplot's CONDIDs, so trees alive in another CONDID are not loss (M-10).
- **Model feature.** It follows `age_stratum` (M-11).
- **KNOWN values are re-measured** after the restart, under the R4 fit scope. This was already planned in D24 and is required anyway because the model changed.
  - The drift guard compares SIGNED values (M-3 / E17).
  - The 1-yr band is unchanged.
- **H_MIN_VISITS = 50** (M-4 / E15) is recorded here as set after seeing KY fail (commit 4b71262). It stays as a reliability floor. A below-floor cell that exceeds its limit is printed `FAIL-SMALL` and counts as a FAIL unless it has an explicit KNOWN entry with a reason. Rejected: a silent INFO.

### D41 — Disturbance deaths, one definition (C F1 IMPORTANT, F17, F7)
- `subp_has_dstrb` and `dead_disturbance` use ONE shared definition: AGENTCD in {10…50} or a condition DSTRBCD other than 0 / 60. It also counts deaths and cuts of trees first seen after the subplot's first visit.
- Drop attribution requires the removal visit to be the tree's first dead / cut record.
- Rejected: two definitions (8–32 % of drop rows sat on "undisturbed" subplots).

### D42 — Full removals and the Pan export (C F2 IMPORTANT)
- cohort_disturbance also emits full-removal rows (`full_removal = TRUE`, drop 1.0) instead of dropping `n_survivors == 0`.
- Consumer item for the Pan rebuild, alongside D30 / D32: Pan must read the curated cohort_disturbance (D31 drops and full removals). It must not use landis_hbad4/tools/export_cohort_disturbance.py, which re-implements pre-D19 / D25 / D26 / D31 rules and imports the main checkout.

### D43 — Reference-table contamination by reused periodic plot keys (D F1 IMPORTANT)
- export_reference_tables copies only PLOT / COND rows whose PLT_CN belongs to a visit in the curated product, plus the canonical remap's raw visits.
- STDORGCD / SITECLCD / land_use use only those rows.
- The verifier checks for absent extra rows: n12 / slim_guard post test that there are no foreign PLT_CNs.

### D44 — Product correctness hardening (cohorts C F3–F16, chain D F3–F15, trees A7-2 / A7-7 / A7-10, F7-3 / F7-4 / F7-10–12)
- **Synthetic rows.** Phantom and gap rows get their own visit's cycle / condid / INVYR / COND stratifiers, or NULL; never the debut's or the previous row's (C F3).
- **Sapling phantoms** test the MICROPLOT forest proportion (C F4).
- **MIN_BACKFILL_AGB** has explicit units per table (C F5).
- **ecosubcd** comes from data_plot_eco (C F6 / D F12).
- **land_use** is 2-way everywhere; the 4-way classification is `land_use_4way` (C F10).
- **Cut events** are keyed by subplot (C F11).
- **Age axes.** Alternate age axes are ≥ 1 or NULL (C F12).
- **Rate scope.** Sapling-rate pooling is recorded in run_scope (C F13).
- **Stale code paths refuse to run:** cohort_disturbance --attach, the cohorts_landis2 full build, and the DAMSEV pct_biomass_damaged column, which is removed (C F9, F7-10, E25).
- **Viz notebooks** take explicit DB paths (C F15).
- **Determinism.** MODE ties are broken deterministically (min) (A7-7 / F7-3); joins before cum_sum keep order explicitly (F7-4).
- **Trees.** Land-use exits use the first exit after the tree's last curated record (A7-2). The status-0 read filters P2A / COND (A7-10).
- **Keys.** Key types are INTEGER (F7-11).
- **Collision loop.** Its root is recomputed (F7-12). The D26 land-use path matches by the PREV_TRE_CN → CN link.
- **Chain** (D F3–F11):
  - correct log counts;
  - FROM_STEP compared numerically;
  - FROM_STEP=11 without SLIM_OUT refused;
  - paths made absolute before the cd;
  - `--states` passed to every step and refused when it mismatches run_scope;
  - the slim carries run_scope / plot_key_remap / plot_location plus a run id that slim_guard checks across curated / simB / slim;
  - partial outputs cleaned on failure;
  - Julia / slim_guard DuckDB capped;
  - Sim A / B land_use joins made consistent (D F15);
  - column order kept on step-6 reruns (D F9).
- **run_scope.counties** is VARCHAR (E29).

### D45 — Verifier: from consistency checker to independent re-derivation (E1–E30, F7-9, C F8, B M-5 / M-14 / M-15)
- **Exit status.** The verifier exits non-zero on any FAIL (E1).
- **Display-only invariants become checks** (E2 / E3):
  - record-level live coverage (1c = 0);
  - lost dead records;
  - SUBP > 4 rows;
  - CN uniqueness;
  - negative ages;
  - nonforest phantoms in the PRODUCT tables.
- **Completeness in both directions** (E4 / E5 / E6): product phantoms are checked against the provenance tables and the D-rules; the drop set and `subp_has_dstrb` (D41) are re-derived independently from raw FIADB and compared in both directions; (a) uses a left join.
- **Values re-derived from raw** (E7 / E8 / E9 / E10):
  - sim_year from MEASDATE by the D18 rule;
  - Sim B drop magnitudes from raw biomass;
  - real-row AGB / TPA / drybio from raw DRYBIO_AG · TPA;
  - censoring reasons and final fates from raw STATUSCD 0 / RECONCILECD.
- **Tightened checks** (E11 / E12 / E13 / E14 / E16):
  - n5 requires that no natural tree is mapped;
  - (c1) requires exact agreement and tests the harvest_inferred rule;
  - (b), n7 and "aged and attributed ⇒ flagged" are enforced;
  - a check on an empty set where data is expected FAILs, and skipped sections print SKIP;
  - tolerances are tightened to observed values plus a margin. Tightening after seeing data is conservative, and recorded here.
- **More coverage** (E20 / E21 / E22 / E23 / E24 / E18 / E19 / E26 / E27 / E28 / E30):
  - D28 is fully checked;
  - n11 asserts the basis;
  - determinism covers steps 1–10 on a subset at two thread counts;
  - product attributes are checked;
  - slim content equals the curated tables (counts and hashes);
  - fixtures are strengthened (D19 shrink branch, zombie interpolation, D26(b), the D31 formula);
  - `arg_min_null` is used;
  - output is ordered;
  - the canonical walk gets a cycle guard;
  - the SITETREE key linkage is investigated and fixed if wrong;
  - circular checks are re-derived where cheap.
- **Other verifier fixes:** (d) 0/0 FAILs (M-5); n11 compares at every level (M-14); the (j) gate and statistic use one unit (M-15); n8 re-derives `dropped` (F7-9).

### Doc items
- The D-numbers are made unique: the "Generalisation" entry becomes D31-G.
- Stale numbers are corrected: D27 GA 64; D26(a) NC records vs trees; D29 AL; D30 AL 8.6 %; R2 COND counts.
- The D28 county wording; county subsets are by canonical county (F7-13).
- D24's mechanism text is corrected (M-2).
- The age_imputation docstrings (M-16).

## D46 — Mapping acceptance per curve (ages implementer f76c872 outcome; coordinator, 2026-10-27)
**Outcome.** With D40's planted-only mapping, KY planted out-of-fold error is MAE 9.11 / bias +0.96. That is better than the old mapping (10.69 / +5.46) but still worse in MAE than raw TreeAge (8.58 / +2.33). It wins at 9+ in (8.86 vs 10.58) and loses at 5–9 in.

**Decision.** Each mapping curve (spgrpcd → major group → global) is applied only if its nested out-of-fold MAE on that curve's planted calibration rows is strictly lower than raw TreeAge's. Otherwise the curve is identity, and the rows fall through to the next level only if that level passes the same test.
- This uses the same acceptance form as D40's recalibration rule, now applied to the mapping.
- It is recorded as decided AFTER seeing the KY result.
- The test can only make the mapping less aggressive; it never tunes a curve.
- The per-curve decisions go in `estage_calibration`.

**Options considered:**
- (a) chosen: per-curve acceptance;
- (b) accept the KY result as is; rejected, because the mapping would add error where it should remove it;
- (c) a per-state on/off switch; rejected as too coarse, since in KY the 9+ in mapping helps;
- (d) DIA-class-specific curves; rejected as a new model, not a guard.

**Runtime.** Step 2 is now about 5× slower (130 HGBT fits). Accepted.

**Also required:**
- **Cohorts:** D11's "natural model ages" share must count natural `out_of_support` rows too (M-9 moved natural saplings to out_of_support).
- **Verifier:** read `age_recal_cells`, the `final` rows of age_model_cv, `unmapped_estage`, the per-curve acceptance, and the M-9 support rule. `planted_split_used` has been dropped.

**D46 outcome (b7572de; coordinator).** In KY, none of the 4 curves beats raw TreeAge out of fold, so KY planted estage is raw TreeAge (0 of 1,317 mapped).
- On the honest metric, the out-of-fold error on planted calibration rows against field STDAGE, raw is best in MAE: 8.58 vs 9.11 (D40) and 9.51 (D46 nested).
- On live records, the final-minus-field comparison favours D40 at 9+ in: MAE 5.87 vs 9.27. That comparison is largely IN-SAMPLE for the D40 mapping, because those records sit on the calibration plots. It is therefore not used as evidence.
- The decision stands as written, with no further iteration. A third post-hoc change would make the rule meaningless.
- **Known residual:** raw TreeAge on KY planted 9+ in carries a bias of +7.5 yr out of fold (n = 506). It is reported in the verifier as INFO under (h) / (k), and is a candidate for DIA-class curves in a later model version (option (d), deferred).
- **Verifier contract:**
  - an unmapped scope (`level = 'unmapped'`, fewer than 200 rows) is distinct from a rejected scope (curve rows with `applied = false`);
  - a planted row must be mapped exactly when the curve reaching it was applied.

## D47 — R8 whole-state validation findings (2026-10-28, coordinator; run at a0c5465, outputs /workspace/FIA/curated_r8)
1. **D35 × D37 (AL, n14; pipeline defect).** A visit dropped as a duplicate by D37 is not a coordinate-link end. Its trees are not censored `procedural_relink`. The kept visit's trees continue normally.
2. **The verifier's raw disturbance re-derivation (n21) applies the D34 RC7 exclusion.** Records of trees FIA withdrew as erroneously tallied (STATUSCD 0 / RECONCILECD 7) are not raw events. This is a verifier defect; the pipeline was right.
3. **The verifier's phantom block rule (n18) is NULL-safe.** A NULL debut RECONCILECD must not blank the r_a term (COALESCE). Verifier defect.
4. **The verifier's n5 distinguishes rejected from unmapped scopes (D46 contract).** Unmapped = a `level='unmapped'` row. Rejected = curve rows with `applied = false`, which are identity and not flagged. Verifier defect.
5. **KNOWN cells after the D40 / D46 age model** (the user's standing choice for age-bias failures is "accept + flag"):
   - Re-baseline the pooled scope "12,21,28" to the values measured on 2026-10-28: FL planted 15+ = −4.3. FL planted 10–15 passes now (−2.9), so its entry is retired.
   - Add new per-scope entries, each with its reason and the same ±1-yr signed drift guard:
     - scope "12", FL planted 15+ = −3.7 (regression to the mean, as the pooled FL cell);
     - scope "21", KY planted 10–15 = −4.9 (D46 raw-TreeAge residual);
     - scope "47", TN planted 15+ = −3.1;
     - pooled-scope KY planted 10–15 = +4.2.
   - Update KNOWN_BIAS_FLAGS to match.
   - **Recorded instability:** KY planted 10–15 in flips sign with the fit scope (−4.9 single-state vs +4.2 pooled). The ages of those 227 trees are not stable to the training set. They stay flagged by age_domain and are a target for the deferred DIA-class model (D46 option d).
   - Rejected: leaving them as FAILs (the verifier would never exit 0, which hides new failures); widening tolerances (post hoc).
6. **Empty or near-tied (d) cells** (KY: no planted top-25 species with ≥ 50 records per class; TN: a < 0.05-yr inversion) are data outcomes. They get KNOWN entries with that reason, keyed by scope and cell, so any NEW (d) failure still FAILs.
- **Implementation note (r8, D47.5).** The new `KNOWN_BIAS_FLAGS` rules change `age_domain` (step 2) and the cohort known-bias columns (steps 4+), and the verifier's (l) re-derives the flags exactly. So the scopes whose flags changed (12, 21, 47 and 12,21,28) are rebuilt from step 2 (FROM_STEP=2, plus the slim and the 4–10 determinism rerun). GA, MS, NC and SC carry no flag in any version and are re-verified on their existing DBs. Options: verify-only everywhere (rejected: the old DBs lack the new flags, so (l) would FAIL); a full rerun from step 1 (rejected: step 1 is unchanged for these scopes).

## D48 — Remove the coordinate-link layer (user decision 2026-10-28; supersedes D29, D35, D37 and D47.1)
**User:** "I don't think coordinate link is a good idea, because we know coordinates are fuzzed, and it's probably a stretch to do that, especially when it comes to relying on species to actually keep the identity."

**Decision.** Plot and tree identity use only FIA's identifiers and FIA's own links:
- the composite key (STATECD, UNITCD, COUNTYCD, PLOT, SUBP, TREE);
- PREV_PLT_CN, for renumbered plots (canonical key, D15 / M-2);
- PREV_TRE_CN and RECONCILECD, for reused tree numbers (M3, D36 rules other than the link rule).

**Removed:**
- **D29:** coordinate + tree-continuity links of chain starts to chain ends.
- **D35:** `procedural_relink` censoring and the `coord_link` break.
- **D37:** the duplicate-visit collapse.
- **D36's link-start split rule:** the species / DIA test at link starts.
- **D47.1:** moot.

**Consequence, accepted.** The 18 R4 coordinate-linked plot pairs (e.g. AL 1/3/61/71 vs 87, a KINDCD 3 replacement plot with the same stand; GA 87/45 → 87/146; MS 163/84 → 163/101; SC 19/74 → 19/279, 19/101 → 19/280) stay separate plots. Their pre-link history is not joined. The AL 2010 stand appears as two plot-visits in that year.

**Options:**
- (a) chosen: FIA-linked corrections only;
- (b) pure composite key with no corrections; rejected, because it reintroduces reused-number "zombie" trees and breaks PREV_PLT_CN-linked renumbered plots;
- (c) keep the links with a species-tolerant test; rejected, because coordinates are fuzzed and species is unreliable for saplings.

## D49 — NIMIWAE phantom backfill: design and pre-registered benchmark (2026-10-28, coordinator; user: NIMIWAE before the Region 4 build)
**Scope.** Only the SIZE of phantom slots, i.e. DIA and per-tree AGB at pre-debut visits. Which slots exist stays with the current rules (prior_age ≥ 1 and the D-rule blocks).

**Why NIMIWAE.** A tree is missing at a visit *because* it was small:
- below 1 in DBH for a microplot tree (debut TPA ≈ 74.965);
- below 5 in for a subplot tree (debut TPA ≈ 6.018).

That is not-missing-at-random. NIMIWAE (Lim et al. 2021) models p(x, m) = ∫ p(x | z) p(m | x) p(z) dz. It is trained with an importance-weighted bound, using K imputations x̃_m ~ p(x_m | z).

**Model.**
- Unit: one tree trajectory over its subplot's visits.
- Covariates: species group, EPA L3, planted stratum, age at visit, years from the tree's first visit, and live BA of the subplot-visit.
- x_t = (log DIA_t, log AGB_t).
- Encoder: deep set over the observed visits plus covariates → q(z), with z ∈ ℝ⁴.
- Decoder: log DIA(t) = f(z, covariates, age_t), Gaussian; log AGB = per-group allometry on log DIA, Gaussian.
- Missingness: p(m_t = observed | x_t) = σ(γ · (log DIA_t − log thr) + b), with thr = 1 in (microplot tree) or 5 in (subplot tree). γ and b are learned, so the known tally limit is soft and field misses are learned.
- Imputation: posterior mean over the importance-weighted samples.

**Data and splits.**
- Real records of tree_trajectories from the curated_r8 builds.
- Plot-level split by crc32(plot key): 70 / 15 / 15 train / val / test.
- Val is used for model choices (epochs, K, dimensions). Test is used once.

**Benchmark (pre-registered; pseudo-censoring with known truth).**
- Test trees: first real record is a microplot sapling below 3 in, there are ≥ 3 real visits, and at least one visit is below 3 in and a later one ≥ 3 in.
- Their records below 3.0 in are hidden, as an MNAR analogue of the tally limits.
- Both methods impute the hidden visits:
  - the current rule (rate back-projection from the first visible record, rates estimated on train exactly as `estimate_sapling_growth_rates` with the per-tree basis, floor 2 g);
  - NIMIWAE, trained on train under the true tally thresholds.

**Acceptance on TEST (all three required):**
1. MAE(log AGB) ≤ 0.9 × the rule's MAE.
2. In every hidden DIA class (1–2, 2–3 in) and major species group with n ≥ 200, the |median bias of log AGB| ≤ the rule's.
3. Median |relative error| of the subplot-visit hidden-AGB total ≤ the rule's.

If accepted, `backfill: nimiwae` becomes the default in cohorts_landis / build_tree_trajectories, with the rule kept as an option. If not, the rule stays and the result is recorded.

**Limits (stated in advance):** the benchmark tests extrapolation below 3 in from trees seen above it. Production also extrapolates below 1 in, and below 5 in for subplot trees, where there is no truth.

**D49 amendment — causality / monotone growth (user, 2026-10-28; recorded BEFORE any test scoring).**
"NIMIWAE must maintain causality, it cannot suggest that diameter and height decreasing then increasing again."
- **Model structure.** Each tree has a latent TRUE size path: log DIA at its last slot, plus non-negative growth increments back in time (softplus rate × elapsed years). The path is non-decreasing in time by construction.
- **Observations** are the path plus measurement noise (σ learned). Imputations are the path itself, so each importance sample and their weighted mean are monotone.
- **AGB** comes from a monotone allometry (log AGB = a_sp + softplus(b_sp) · log DIA). The unconstrained residual term is removed.
- **Bounds.** An imputed value never exceeds the tree's first observed value, the same cap as the rule.
- **Criterion 4 (hard), added:** 0 imputed trajectories with a decrease between consecutive slots or above the debut value, on the benchmark and in production.
- **Evidence.** The discarded unconstrained first model drifted (val MAE 0.63 → 1.26 → 3.45 over 3 epochs, bias −2.3): the MNAR degenerate solution of explaining missingness with ever-smaller missing values. No test data was used.

**D49 amendment 2 — absolute acceptance criteria; the rule is not a yardstick (user, 2026-10-28; recorded before the size-dependent run's results and before any test scoring).**
"Assume the baseline is wrong. You can't judge NIMIWAE this way." Criteria 1–3, which were relative to the rule, are REPLACED. The rule's numbers are reported for context only and decide nothing.

**NIMIWAE is accepted only if it passes all of the following on the TEST benchmark (pseudo-censoring at 3 in, true values known):**
- **A1, unbiased.** |median error of log AGB| ≤ 0.10 (≈ 10 %) overall, and in every hidden DIA class (1–2, 2–3 in) and major species group with n ≥ 200. The same holds for log DIA.
- **A2, calibrated.** The empirical coverage of the model's central 80 % predictive interval of log AGB, over hidden visits, is between 0.75 and 0.85, overall and per DIA class.
- **A3, totals.** The median of the subplot-visit hidden-AGB total, sum predicted / sum true, is within [0.9, 1.1].

**And on every production imputation (all phantom slots of all 8 states):**
- **P1, monotone.** 0 trajectories decrease between consecutive slots up to the debut, or exceed the debut value (the former criterion 4).
- **P2, consistent with the tally.** Imputed DIA below the tree's tally limit at every missing slot: < 1 in for microplot debuts, < 5 in for subplot debuts. Tolerance: ≥ 99 % of slots.
- **P3, physically plausible growth.** Implied annual DIA increments between imputed slots, by species group, lie inside the 1st–99th percentile range of the OBSERVED annual increments of remeasured microplot saplings (train plots, real records), for ≥ 98 % of slots.

**Accuracy is reported, not gated:** MAE of log AGB and log DIA. A1–A3 are checked on VAL for model choices; TEST is scored once.

**D49 amendment 3 — final protocol (2026-10-28; recorded before any test scoring).**
Validation-only development gave:
- unbiased point estimates (log AGB +0.03, log DIA +0.02, log HT +0.03);
- monotone paths;
- intervals too narrow (80 % coverage 0.49), because the growth process noise learned from the observed 5-yr intervals understates 10–20-yr back-extrapolation.

**Model, as frozen:**
- a monotone height path (size-dependent backward growth ODE with log-normal process noise);
- DIA = c_sp e^{ζ(z)} (HT − 4.5 ft)_+^{b_sp} + 0.01 in;
- AGB monotone allometry in DIA and HT;
- a near-step tally missingness (steepness 20 per log DIA) with a learned field-miss rate;
- the leak-free first-observed-record age proxy.

**Interval calibration:** one width factor w on the log-AGB predictive deviations, fitted on the VAL benchmark so the central 80 % interval covers 0.80 there (split-conformal style). It is frozen into the model. The A2 bar (0.75–0.85 on TEST) is unchanged.

**Final run:**
1. Prep on all 8 curated_r8 states (D48 builds).
2. Train on train plots.
3. Select the epoch on VAL by |median bias log AGB| (coverage is handled by w).
4. Fit w on VAL.
5. Score TEST once against A1–A3. Then run P1–P3 on every production imputation.
6. Report everything, pass or fail.

**D49 outcome (TEST scored once, 2026-10-28; /workspace/FIA/nimiwae_r8/final/bench_report_test.json).** NOT ACCEPTED: the rule stays the production backfill.
- **A1 (unbiased) FAIL.** Overall log AGB +0.019 and log DIA +0.011 pass. But at 1–2 in, log AGB is **+0.18** (log DIA +0.07), and softwoods (major group 1) are log AGB **+0.16**. Every other cell is within ±0.07.
- **A2 (calibrated) FAIL.** Overall 80 % coverage is 0.800 and 2–3 in is 0.845, but 1–2 in is **0.70**.
- **A3 (totals) PASS:** median subplot-total ratio 1.06.
- **P1 (monotone) PASS:** 0 / 0 / 0 violations.
- **Accuracy:** MAE log AGB 0.83, log DIA 0.32.
- **Context only:** the current rule on the same test has bias log AGB **+0.47** and a median subplot-total ratio of **1.51**. It would fail A1 and A3 by a wide margin.

**Diagnosis.** The failure is concentrated in the smallest trees and in biomass, not diameter: DIA bias is small, and the AGB bias comes from the allometry, which is log-linear in DIA and HT and too stiff at the smallest sizes. Over training, the validation bias drifts monotonically (+0.26 → −0.24), so the selected epoch is a zero-crossing, not a converged optimum.

**Status.** TEST has now been used. Any revised model needs a FRESH held-out set. Options:
- a new plot split, with this test set retired;
- a different state held out.

## D50 — NIMIWAE revision and fresh held-out test (2026-10-30, user: option 1; recorded before any training)
**Fresh split.**
- The old TEST plots are retired as a test set and move to train.
- VAL is unchanged and used for model choices.
- The new TEST is the old-train plots with crc32(plot key + "|r2") % 100 < 22, about 15 % of all plots. The previous model never evaluated these plots; they were only training data for a model that is now discarded.
- The benchmark definition is unchanged (pseudo-censoring at 3 in).

**Model changes (aimed at the D49 failure, not at the bars):**
1. **Flexible monotone allometry.** log AGB = a_sp + b_sp·s + c·h + Σ_j w_{g,j}·φ_j(s), with s = log DIA and h = log HT. The φ_j are increasing hinge functions, relu(s − t_j) and −relu(t_j − s), at knots of 0.5, 1, 2, 3 and 5 in, with weights w ≥ 0 per major species group. It stays monotone in DIA and HT and can bend at small sizes.
2. **A training schedule that converges.** Cosine learning-rate decay from 2e-3 to 1e-5 over 10 epochs, with weight decay 1e-5. The model is the FINAL epoch; there is no epoch picking, so a bias zero-crossing can no longer be selected. Validation bias per epoch is reported as a convergence diagnostic.
3. **Interval width factors per imputed size class** (imputed DIA < 2 in vs ≥ 2 in), each fitted on VAL to cover 0.80 there. A2 is per class, and the class is known at prediction time from the imputed DIA.

**Unchanged:** the bars A1–A3 and P1–P3; TEST scored once; outcome recorded whatever it is.

**D50 outcome (fresh TEST scored once, 2026-10-30; /workspace/FIA/nimiwae_r8/d50/bench_report_test.json).** NOT ACCEPTED.
- **A1 FAIL.** Overall log AGB +0.025 and log DIA +0.028 pass, and so does 1–2 in (log AGB −0.06). But softwoods (major group 1) are **+0.24** and major group 2 **+0.11**.
- **A2 FAIL.** Overall coverage is 0.80, but 1–2 in is **0.69** and 2–3 in **0.853**.
- **A3 PASS** (1.06). **P1 PASS** (0 violations).
- **Accuracy:** MAE log AGB **1.37**, worse than the rule (1.03, context only). The validation MAE rose over training (0.63 → 1.38) while the bias converged near 0, and the interval width factors needed ≈ 4×.

**Reading.** The NIMIWAE objective, the likelihood of the observed visits under not-missing-at-random missingness, does not identify the pre-debut path: more training fits the observed period better while the extrapolation gets noisier. The fixes for the D49 failure (allometry, schedule) moved the bias but not this.

**Status.** Two test sets are now used. Any further candidate needs a clean held-out set: a whole held-out state is cleanest. Options (user): see the session report. The rule stays in production, with its measured bias (+0.47 log AGB, totals 1.51) on record.

## D51 — NIMIWAE redesign: causal forward growth with growth drivers (user, 2026-10-30; recorded before any code or training)
**User.** "You are mis-modeling with nimiwae… the observed data encoder is doing the heavy lifting… be clever about growth curves and what might affect them… bundle in enough information to guide that, also the causal dependency."

**What was wrong.**
- **Anti-causal generation.** D49/D50 integrated sizes BACKWARDS from the last visit, so the unobserved early states inherited every accumulated error.
- **No drivers.** The growth rate had almost no information about WHY a tree grows fast or slow, so the encoder had nothing to work with.

**Generative model (forward in time, causal).**
- **Per-tree latents:**
  - z (vigour / microsite);
  - the age at the first observed record, A_f. Its prior is log-normal around the leak-free age proxy (from the first observed record only), with sd 0.4 in log space.
- Establishment is at t_f − A_f.
- **Effective age** accumulates forward: a_eff(t) = Σ over the intervals since establishment of Δyears · m_t · ε_t.
  - m_t = exp(f(z, competition_t, release_t, climate anomaly_t)) is the growth modifier of the interval.
  - ε_t is log-normal process noise.
  - So a_eff is non-decreasing, and each interval depends only on its own covariates.
- **Height** is a Chapman–Richards curve, H(t) = Hmax (1 − e^{−k a_eff})^p, with Hmax, k and p depending on z, species and site (site index SICOND with a missing flag, SITECLCD, PHYSCLCD, slope / aspect) and on the L4 climate normals. Height is monotone in time by construction.
- **DIA and AGB.** DIA = c (H − 4.5 ft)_+^b + 0.01 in (the breast-height anchor). AGB comes from the monotone hinge allometry.
- **Missingness** is the near-step tally limit with a learned field-miss rate (as D49).

**Per-interval drivers (causal: the interval ending at each slot).**
- **Competition:** BA of the OTHER trees on the subplot at the slot (the subplot BA minus the tree's own BA, so there is no leak from a hidden tree), and stems per acre.
- **Release:** a cut (TRTCD 10) or disturbance (DSTRBCD other than 0 / 60) recorded on the tree's condition at the slot.
- **Climate:** the mean yearly tmean, ppt and vpdmax of the L4 over the interval, as anomalies against its 1991–2020 normal. Calendar years are used only for this join.
- Crown ratio and crown class are OBSERVATIONS seen by the encoder, masked at hidden or missing visits.

**Encoder.** Observed sizes, crown measures and all per-slot drivers → q(z) and q(log A_f).

**Splits.** VAL is unchanged. The new TEST is plots never scored before: d50-train plots that were in neither earlier test, with crc32(key + "|r3") % 100 < 18. Both earlier tests move to train.

**Training.** Cosine decay; the final epoch is the model; width factors per imputed size class are fitted on VAL.

**Bars unchanged:** A1–A3 and P1–P3. TEST is scored once.

**Dev amendments (2026-10-30; VAL diagnostics only, before any TEST scoring; benchmark construction and bars unchanged).**

Each amendment fixes a specific model error that the VAL failure dumps showed. Dev runs used 200k train trees × 4 epochs.

1. **Prep fixes.**
   - The PRISM values are stored as text: they are now cast via VARCHAR.
   - The COND join was 100 % null: `tree_trajectories.plt_cn` is the plot key, not the FIA CN. The CN is now resolved through PLOT on (state, unit, county, plot, measurement date).
   - Site index is still null on 38 % of slots; it is flagged as missing.
2. **The tally limit was removed from the growth / encoder covariates.**
   - It is a measurement-design property, not a growth driver, and it now lives only in the missingness model.
   - Training sees only 1 in and 5 in, so the bench's 3 in was out of distribution.
3. **Point imputation is the weighted posterior MEDIAN per slot.**
   - The previous summary was the mean of log size. A few pre-breast-height paths (DIA floor 0.01 in) dragged it down.
   - The median is the Bayes point for the absolute-error bars.
   - A pointwise quantile of non-decreasing paths with shared weights is non-decreasing, so P1 holds by construction.
   - Totals keep the linear posterior mean.
4. **The age prior is a learned conditional (empirical-Bayes) prior.**
   - δ ~ N(μ(x_f), σ(x_f)), where x_f is the covariates of the FIRST observed record only (DIA, HT, crown ratio / class, competition, stems) plus site / species.
   - Cause: VAL dumps showed suppressed hardwood advance regeneration (1–2 in for ~20 yr) "established" at the first hidden slot. The curated ages are compressed for these trees: the bench's own curated debut ages (median 23 yr for CCLCD 5) contradict their 1.4 in records 20 yr earlier. The fixed sd 0.4 around the proxy could not correct this.
5. **The AGB likelihood uses the MEASURED DIA / HT.**
   - FIA's DRYBIO is computed from the measured sizes, so p(D, H, A | latent) = p(D, H | latent) · p(A | D_obs, H_obs). Latent HT is used where HT is unmeasured.
   - The latent-size allometry is used only to impute.
   - Before this, fitting AGB on the smooth latent path (errors in variables) left a size-trending residual (−0.50 at 1–2 in → +0.12 above 10 in), even at the true sizes.
6. **The allometry starts from a global OLS on TRAIN measured records.**
   - Per-species intercepts start at the OLS residual medians, and the hinge weights start at ~0.
   - The constant start (a = 3.0 vs the OLS 4.41) had not converged within the dev budget.

Known caveat (not changed): at benchmark slots, the subplot competition sums still include neighbours in the 1–3 in band. Production would not see those trees, because they are below the tally limit. This is a small competition-scale shift, not a leak of the target tree's own size.
7. **The decisive benchmark protocol is interval censoring (declared before TEST).**
   - A hidden bench visit WAS tallied at the real 1-in microplot limit. The truth is therefore in [1, 3) in by construction (the D49 selection), but the original protocol told the model only "< 3 in".
   - A model calibrated for production phantoms, where "not tallied" truly means small or not yet established, is penalised there for information the benchmark withheld. In dev, VAL got WORSE the better the training likelihood fitted.
   - The decisive protocol therefore gives the model the information the data carry: a lower bound of 1 in on hidden visits (a near-step logsigmoid term in the missingness likelihood). Production phantoms have no such bound.
   - The original "< 3 in only" protocol is scored in the SAME single TEST run and reported alongside. The bars are unchanged.
8. **Supervised pseudo-censored term (TRAIN plots only).**
   - The benchmark's exact construction rules are applied to the TRAIN split: 21,539 trees with 33,799 hidden visits.
   - Each step adds the IWAE bound of these trees plus the predictive log p(x_hidden | x_obs) ≈ log Σ_k w_k(x_obs) p(x_hidden | path_k). The posterior is inferred WITHOUT the hidden values, exactly as at test.
   - This is the encoder doing the supervised heavy lifting (user). Neither VAL nor TEST enters training.
   - **Dev result on VAL** (200k trees × 4 epochs, decisive protocol):
     - MAE log AGB 0.57 (the rule: 1.06);
     - overall bias −0.03; A3 1.005; P1 0 violations;
     - **A1 still fails:** +0.49 at 1–2 in, −0.16 at 2–3 in, softwood +0.27, group 2 −0.23;
     - **A2 still fails:** 0.71 at 1–2 in.
   - The per-TRUE-size-class bias is the regression-dilution signature: any imprecise but calibrated predictor binned by its truth shows it. This is noted for the user and is not a reason to change the bar.

**Final protocol run (fixed before training).**
- Full train split (seq r3), 6 epochs, cosine schedule, final epoch.
- K = 16, batch 1024.
- Pseudo batch 64 with λ = 4, which keeps the dev weight 0.25 and limits re-cycling of the 21.5k pseudo trees.
- Widths fitted on VAL (decisive protocol), then TEST scored once.

**D51 outcome (fresh r3 TEST scored once, 2026-10-30; /workspace/FIA/nimiwae_r8/d51/bench_report_test.json).** NOT ACCEPTED under the pre-registered bars, in either protocol.

Model: final epoch 5 of 6 on the full train split (1.28M trees); VAL width factors (1.666, 1.62). The TEST has 4,369 hidden visits on 3,272 subplot-visits.

**Decisive protocol (interval [1, 3) in):**

| criterion | result | verdict |
|---|---|---|
| A1 (bias, log AGB / log DIA) | overall −0.03 / −0.01; 1–2 in **+0.43 / +0.18**; 2–3 in **−0.13** / −0.04; softwood **+0.18**; hardwood groups −0.03 and −0.08 | fail |
| A2 (80 % coverage) | overall 0.80; 1–2 in **0.739**; 2–3 in 0.83 | fail |
| A3 (totals ratio) | 0.996 | pass |
| P1 (monotone) | 0 / 0 / 0 violations | pass |
| accuracy (MAE) | log AGB 0.51, log DIA 0.19 | |

**Original protocol ("< 3 in only"):**
- A1 fails: 1–2 in +0.23; 2–3 in −0.19; hardwoods −0.14 and −0.17; overall −0.13.
- A2 fails: 1–2 in 0.747.
- A3 0.90 passes (on the edge); P1 passes; MAE 0.75.

**Context only (not a yardstick):** the rule has MAE 1.04, bias +0.42 and totals 1.44 on the same TEST, so it fails A1 and A3 by wide margins.

**Reading the result.**
- Every remaining failure is in TRUE-size-class or group cells: small trees are over-predicted and large ones under-predicted. This is the regression-dilution signature of binning by the true value with a predictor that is unbiased overall but imprecise (log-DIA MAE 0.19 over back-casts of 5–20 yr).
- The bars stand as pre-registered.
- Whether a truth-binned bias bar is the right acceptance test for an imputer (versus calibration binned by the PREDICTION, plus totals) is a question for the user. It must not be changed after this result.

**Status.** Three test sets are now used (D49, D50, r3). By the pre-registered rule the production backfill is unchanged (the rule, bias +0.42 and totals 1.44 on record). The adoption decision is the user's: D51 is better than the rule on every measured criterion but does not pass the absolute bars. Any further candidate needs a clean held-out set, preferably a whole held-out state.

## D52 — NIMIWAE: information, model fixes and NESTED CV instead of a holdout (user, 2026-10-30; recorded before any nested-CV scoring)
**User.** "Either you are not pulling enough information for a tree from the plot information which makes you regress to a mean, or the model is not complex enough, or there are bugs. Also you can do nested cross val instead of on holdout."

**Diagnosis (VAL only).**
1. **Information ceiling.** A supervised gradient-boosting model (median loss) predicts log(DIA_hidden / DIA_debut) for the benchmark's hidden visits. It is trained on the TRAIN plots' pseudo-censored trees and scored on VAL by TRUE class:
   - tree-only features: MAE log DIA 0.157; true 1–2 in +0.216, 2–3 in −0.048;
   - + plot / stand / neighbour features: MAE 0.138; +0.158 / −0.038;
   - the same predictions binned by PREDICTION: −0.010 / +0.002 and +0.002 / −0.010;
   - the true log DIA sd within the true 1–2 in class is 0.198.
   - So **the truth-binned A1 cells measure regression dilution**. Even a supervised model with every feature cannot meet them (+0.16 log DIA ≈ +0.38 log AGB vs the ±0.10 bar).
   - Plot information is real signal: −12 % MAE.
2. **Bugs / design errors found and fixed:**
   - **(a) Information only the encoder saw could not move the posterior.** In an IWAE the encoder only proposes, and the weights return the posterior of the generative model. The first record's crown ratio / class and stand context are now conditioning covariates of the generative model (growth curve, modifier, age prior). The per-interval stand state is a modifier driver.
   - **(b) The encoder was blind to each slot's censoring bounds.** The tally limit had been removed from all covariates in D51. The encoder (inference) now sees each missing slot's upper bound and the interval-censoring flag / lower bound; the growth model still does not.
   - **(c) The learned DIA measurement sd (0.11 log) had absorbed curve misfit.** That made a tree's observed growth rate indistinguishable from noise: the error trended +0.21 across post-debut growth quintiles. It is now **FIXED at FIA precision**, sqrt(0.02² + (0.06 in / DIA)²). This removed the depth-dependent bias (3 visits back: −0.15 → −0.05).
3. **Model additions:**
   - prep: per visit, COND stand context (STDAGE, FLDAGE, BALIVE, FORTYPCD, STDSZCD, ALSTK, LIVE_CANOPY_CVR_PCT, STDORGCD) and leave-one-out neighbour log-DIA increments over the interval ending at the visit (subplot and plot; trees ≥ 3 in at both ends, so they are visible both in production and in the 3-in benchmark);
   - a bidirectional-GRU encoder;
   - an amortised per-interval growth-noise proposal q(log ε_t);
   - an AR(1) prior on log ε_t (growth persistence, ρ learned).
   - ε_t > 0, so the paths stay monotone by construction.
4. **Dev result on VAL** (200k trees × 4 epochs):
   - MAE log AGB 0.529 (D51 dev 0.57) and log DIA 0.183;
   - the supervised ceiling is 0.138 log DIA, so a gap remains (evaluation K = 100 / 400 / 1000 gives the same result: the gap is the model, not sampling).

**Nested-CV protocol (fixed now, scored once).**
- **Benchmark trees:** every plot's trees under the D49 benchmark rules (`nimiwae_train.pseudo_bench`; it reproduces the prep benchmark exactly on VAL: 4,334 trees, 6,746 hidden visits).
- **Outer folds:** crc32(plot_key | "ncv") % 5.
- **For each outer fold f:**
  - inner validation = the other folds' plots with crc32(plot_key | "ncv-in") % 10 == 0; width factors are fitted on their benchmark trees, and these plots are not trained on;
  - training = all remaining plots (slots + pseudo-censored trees), 400k sampled trees + all their pseudo-censored trees, 4 epochs, cosine schedule, final epoch, K = 16, batch 1024, pseudo batch 128, λ = 2;
  - scored = fold f's benchmark trees EXCEPT the dev-VAL plots, which shaped the design (38,168 hidden visits pooled vs 4,369 in the last holdout).
- The earlier test plots (D49 / D50 / r3) are included in the outer folds. Their results were seen, but no design choice was tuned on them.
- **Bars unchanged:** A1–A3 + P1 on the POOLED out-of-fold predictions; the per-fold spread is reported.
  - Decisive protocol: interval [1, 3) in (D51 amendment 7); the "< 3 in only" protocol is reported alongside.
  - A1 is truth-binned as pre-registered. The prediction-binned A1 is reported as a DIAGNOSTIC only and is not a pass criterion.

**D52 outcome (nested CV scored once, 2026-10-30; /workspace/FIA/nimiwae_r8/d52_ncv/ncv_report.json).** NOT ACCEPTED under the pre-registered bars.

Pooled out-of-fold, decisive interval protocol: 38,168 hidden visits on 28,388 subplot-visits. Inner-val width factors 1.63–1.76.

| criterion | result | verdict |
|---|---|---|
| A1 (bias, log AGB / log DIA) | overall +0.055 / +0.025; true 1–2 in **+0.56 / +0.22**; 2–3 in −0.06 / −0.01; softwood **+0.26**; groups 2 / 3 / 4 +0.09 / +0.03 / −0.01 | fail |
| A2 (80 % coverage) | overall 0.82; 1–2 in **0.71**; 2–3 in **0.865** | fail |
| A3 (totals ratio) | 1.072 (folds 1.05–1.09) | pass |
| P1 (monotone) | 0 / 0 / 0 violations | pass |
| accuracy (MAE) | log AGB 0.507 (folds 0.50–0.52), log DIA 0.180 | |

**"< 3 in only" protocol:** A1 / A2 fail; A3 0.966; MAE 1.10.

**Diagnostic — A1 binned by PREDICTION:** −0.16 / +0.10 log AGB (predicted 1–2 / 2–3 in). The supervised ceiling model is ~±0.01 on the same split, so NIMIWAE's predictions are noisier than the information justifies. Unlike the truth-binned cells, this part is a model deficit, not regression dilution.

The learned AR(1) persistence ρ ≈ 0.01–0.03, so the persistence prior was inert.

**Status.**
- The production backfill is unchanged by the pre-registered rule.
- On the same features, a direct supervised predictor is more accurate than the generative NIMIWAE (VAL: log DIA MAE 0.138 vs 0.18), and it is calibrated by prediction.
- The truth-binned A1 bar is unattainable for either.
- User decisions:
  1. the A1 bar definition (truth-binned vs prediction-binned calibration + totals);
  2. NIMIWAE vs a supervised (monotone-constrained) imputer vs the rule.

## D53 — NIMIWAE with an explicit existence state, no age, bidirectional transformers, pure (unsupervised) objective (user, 2026-10-30; recorded before any D53 code or training)
**User decisions.**
- "Don't depend on age at all… it's an estimated age."
- A Boolean **established / exists** flag per tree per visit. It flips once (0 up to some visit, 1 afterwards). Every tree that ever exists on the subplot gets it at **every** subplot visit, and the flag gates whether a non-zero reading is possible.
- A **bidirectional** transformer ("not a simulation, data completion").
- **No Chapman–Richards constraint for now**: a CR distillation can come later for robustness.
- **No supervised imputation term**: pure NIMIWAE.
- Validation on microplot lineages is legitimate, because "the dynamics are separate from the plot design".

**Slot frame (prep).**
- Every clean visit of a tree's subplot (curation `clean_subplot_years`) up to the tree's last real record.
- The rule's phantom rows are dropped, so which visits are missing no longer depends on the rule.
- slot_kind:
  - `real` (tallied);
  - `pre` (before the first real record: existence unknown);
  - `gap` (inside the record without a row: exists, size missing).
- AL: 919,239 slots = 576,536 real + 342,637 pre + 66 gap. The rule had 212,936 phantoms.
- Per slot:
  - a **sampled** flag (forested subplot / microplot condition at the visit; 4.4 % of pre slots are unsampled and carry no censoring information);
  - competition, the condition, neighbours (trees ≥ 5 in at both ends, visible under either benchmark limit), climate.
- Per tree: the first record's RECONCILECD (1 ingrowth: below the limit at the previous visit; 3 **missed live**: existed and was at or above the limit, i.e. a crew miss, not censoring).
- No age fields and no STDAGE / FLDAGE (estimated stand ages) are used.

**Model.**
- **Latents:** per-tree z with a prior conditioned on static covariates (species, ecoregion, site, climate normals, forest type, planted), plus an existence **flip index τ** (the first slot at which the tree exists; τ = 0: before the first visit).
  - E_t = 1[t ≥ τ], so exactly one flip.
  - τ is summed out **exactly** over its ≤ 14 positions.
  - τ is constrained to be ≤ the first real slot (one slot earlier for a missed-live tree).
- **Decoder:** a transformer over the slots on (z, always-known covariates, elapsed-time encoding) only. It never sees the tree's own measurements (no identity leak). It outputs:
  - a discrete-time hazard for τ;
  - **non-negative (softplus) increments** of DIA and HT, cumulated from τ, with size 0 before it, so the paths are monotone by construction;
  - crown ratio and crown class, reconstructed, so they constrain z.
  - AGB follows from the allometry on the measured sizes (D51 amendment 5).
- **Encoder:** a bidirectional transformer over all slots. Inputs: the masked measurements, the masks, slot kind, the sampled flag, the tally limit, the lower bound where known, the known existence, RECONCILECD, the covariates and the time encoding. Output: q(z).
- **Observation model:**
  - DIA: fixed FIA measurement sd (D52) + a learned discrepancy sd, which is monitored, as it must not absorb growth signal;
  - HT: learned sd;
  - crown: Gaussian (CR) and categorical (CCLCD).
- **Missingness (MNAR), per slot:**
  - real: E = 1 and P(tallied | D) = (1 − ε) σ(κ(log D − log limit));
  - unrecorded and sampled: E = 0 (no reading), or E = 1 and below the limit or missed;
  - missed-live previous slot: E = 1 and D ≥ limit, missed;
  - unsampled: no missingness information.
- **Objective:** the IWAE bound only (no supervised term).

**Outputs.**
- Per slot: P(E_t); the posterior median of the unconditional DIA / HT / AGB, which is 0 when the tree does not exist (pointwise quantiles of 0-then-increasing paths are monotone); the linear-scale mean AGB (the expected value for totals); an 80 % interval.
- Sampled realisations are available for cohort records.

**Benchmark and validation (nested CV, the D52 folds and rules; dev-VAL plots never scored).**
- Microplot lineages, pseudo-censored.
- **Decisive: pseudo-limit 5 in.** This is the production case: a tree outside the microplot is unseen below 5 in.
  - Benchmark trees: microplot debut < 5 in, ≥ 3 real visits, a visit < 5 in and a later one ≥ 5 in.
  - Hidden: real visits before the first ≥ 5 in, interval [1, 5) in, E = 1 known.
  - The tree's pre slots are treated as unrecorded under the 5-in limit with existence unknown.
- **Alongside:** the 3-in benchmark (continuity with D49–D52) and the "< limit only" protocol (E unknown at hidden visits).
- **Existence diagnostic (not a bar):** P(E) at hidden visits under the "< limit only" protocol, where the truth is E = 1. FIA records no absences, so existence can only be checked one-sidedly.
- **Bars unchanged:** A1–A3 + P1 on sizes. A1 is truth-binned; the prediction-binned version is a diagnostic only.

**D53 dev (VAL only, 200k train trees × 4 epochs, pure IWAE).**
- **3 in / interval:** MAE log AGB 0.547, log DIA 0.196. This matches the semi-supervised D52 (0.529 / 0.183) without any supervised term.
- **5 in / interval (decisive):**
  - MAE 0.529 / 0.184;
  - A1: overall +0.12; true 1–2 in +1.27, 2–3 in +0.54, 3–4 in +0.19, 4–5 in 0.00; softwood +0.21;
  - A2 per class 0.60 / 0.67 / 0.76 / 0.90;
  - A3 1.156; P1 pass.
- **Existence diagnostic** ("< limit only", where the truth is E = 1): mean P(E) at hidden visits 0.93 (5 in) and 0.92 (3 in).
- Learned DIA discrepancy sd 0.027, far below D52's absorbed 0.11; the field-miss rate is 0.009.
- For the 5-in benchmark, A1 / A2 use 1-in true-size classes (1–2 … 4–5 in), as the natural extension of D49's 1-in classes; this is fixed before the nested CV.
- The design is frozen as-is for the nested CV:
  - folds and inner validation as in D52;
  - 400k sampled training trees per fold, 4 epochs, K = 16, batch 512, one-cycle cosine schedule;
  - the final epoch is the model;
  - widths are fitted on the inner validation (decisive config).

**D53 outcome (nested CV scored once, 2026-10-30; /workspace/FIA/nimiwae_r8/d53_ncv/ncv_report.json).** NOT ACCEPTED: A1 / A2 fail only in their small TRUE-size classes.

Decisive configuration: 5 in / interval, 33,160 hidden visits, inner-val widths 1.5–2.0.

| criterion | result | verdict |
|---|---|---|
| A1 (bias, log AGB) | overall +0.04; species groups +0.02 / +0.09 / +0.07 / +0.03 (softwood now passes); true 1–2 in **+1.01**, 2–3 in **+0.31**, 3–4 in +0.08, 4–5 in −0.04 | fail |
| A2 (80 % coverage) | overall 0.81; 1–2 in **0.61**, 2–3 in **0.73**, 3–4 in 0.79, 4–5 in **0.88** | fail |
| A3 (totals ratio) | **1.079** | pass |
| P1 (monotone) | 0 violations | pass |
| accuracy (MAE) | log AGB **0.468** (folds 0.46–0.48; the best so far: D52 0.507), log DIA 0.171 | |

- **Diagnostic, by prediction:** −0.42 / −0.07 / +0.02 / +0.05 for predicted 1–2 / 2–3 / 3–4 / 4–5 in.
- **Existence diagnostic** ("< limit only", truth E = 1): mean P(E) 0.953 (5 in), 0.941 (3 in).
- **Alongside:**
  - 5 in / lt: MAE 0.785, A3 1.025;
  - 3 in / interval: MAE 0.498, A3 1.152;
  - 3 in / lt: MAE 0.929, A3 1.033.

**Next (user, 2026-10-30): D54 existence semantics.**
- Existence means "qualifies, should have a record" (≥ 1 in), with one flip.
- TRUE (posited) from the debut through the last record, gaps included (measurements missing, imputed, no cutoff penalty).
- **Observed FALSE** before the debut for microplot trees with a clean history (RECONCILECD 1, sampled visits); gated, not set to 0.
- Unknown otherwise: subplot trees, RECONCILECD 8 / blank, unsampled visits.
- RECONCILECD 3: TRUE at the previous visit.
- The cutoff is one likelihood term, not the heuristic. Existence becomes scoreable on hidden clean microplot lineages.

## D54 — NIMIWAE lineage completion: existence start AND end, tree fate, future records (user, 2026-10-30; recorded before any D54 code or training)
**User decisions.**
- Complete every tree's lineage over all plot visits that measured something: establishment, size while alive, fate. This is **completion within measured visits, not prediction**; projection is left to process models (Biomass Succession, PnET, HBAD ≥ 3).
- Existence = "qualifies, should have a record" (≥ 1 in), one contiguous interval.
- RECONCILECD 3 / 6 / 7 → missed (TRUE at the previous visit).
- AGENTCD 80 → cut.
- Soft existence evidence, but it must not override existence.
- Per-tree first.
- Sapling totals use the exact split below, not raw expansion.
- Use DReG gradients (Tucker et al. 2018, arXiv 1810.04152).

**Frame.**
- **Counted plot visits:** at least one subplot sampled (forest subplot condition). Plot visits with none are dropped.
- **Subplot span:** its first to last sampled visit. Unsampled visits inside the span are slots (completed); visits outside it are not in the frame.
- **Tree lineage:** every counted visit in its subplot's span, before the debut, during the record and after it.
- **Tree's spot sampled at a visit:** the condition of the nearest real record (CONDID) is forest at that visit; a microplot tree's FALSE evidence needs a fully forested microplot.

**Existence evidence** (soft where marked, with error rate ρ ≤ 0.05 learned and capped):

| slot | evidence |
|---|---|
| debut → last record (gaps included) | TRUE, hard |
| before the debut, microplot tree (code 1 / 2), fully forested microplot | FALSE, soft |
| previous visit of codes 3 / 6 / 7 | TRUE, soft; earlier visits inferred |
| other pre-debut slots | inferred |
| recorded death / cut / move-out (terminal_fate harvest_explicit (incl. AGENTCD 80), harvest_inferred, disappeared_inferred_removal → cut; dead_natural, dead_disturbance → death; censored_moved → end, no cause) | FALSE from that visit on, hard; cause observed |
| censored_shrank | TRUE afterwards, sizes missing (absence uninformative) |
| censored_procedural(_unrecorded) | inferred afterwards, absence uninformative |
| diverted_landuse, censored_nonsampled | the tree's spot is unsampled afterwards (completed only if sandwiched) |
| alive_censored with later sampled visits | inferred; a missing record at a sampled visit is informative (below limit, missed, or gone) |

**Model** (the nimiwae53 structure, extended):
- **Latents:** z (conditional prior on statics: species (curated resolved), ecoregion, site, normals, forest type, planted; no age) and discrete (start s, end e, cause f ∈ {death, cut}), all summed out exactly. E_t = 1[s ≤ t < e].
- **Decoder:** a bidirectional transformer on (z, always-known context) with these heads:
  - an establishment hazard;
  - competing per-interval end hazards (survive / die / cut);
  - softplus DIA / HT increments (DIA = 1 in + cumulative from s; nothing outside [s, e));
  - crown ratio and crown class.
- **Context inputs:** dropped are BALIVE, ALSTK and STDSZCD (they include the tree itself, a benchmark-only existence cue). Competition is leave-one-out from the records. All TRTCD (10 / 20 / 30 / 40 / 50) and DSTRBCD (10–90) groups are multi-hot drivers.
- **Likelihood:** as D53 (fixed FIA DIA error + monitored discrepancy; tally-limit / crew-miss missingness), plus the existence and cause evidence above.
- **Training:** IWAE with **DReG** encoder gradients (decoder / prior: standard), K = 32; pure objective (no supervised term).

**Outputs.**
- Sampled lineages (the cohort product).
- Per slot: P(exists), P(gone by death), P(gone by cut), and size given existence.
- **Sapling totals per visit:** saplings that reach ≥ 5 in later in the lineage are enumerated over the whole subplot (each at the subplot expansion); all other saplings come from the microplot sample only (microplot expansion). No double count.

**Evaluation (nested CV: D52 folds and inner validation; dev-VAL plots never scored).**
- **(i) Sizes, decisive:** clean microplot lineages pseudo-censored at 5 in, interval protocol; A1–A3 + P1 unchanged (1-in true classes; prediction-binned diagnostic).
- **(ii) Establishment:** the same lineages under the "< limit only" protocol, with the pre-debut FALSE labels hidden. Score P(exists) at the hidden slots (truth TRUE at hidden real visits, FALSE before the debut).
  - Bar E1: reliability within ±0.05 in every probability decile with n ≥ 200.
  - Brier reported.
- **(iii) Fate:** recorded ends turned into unknown stops (the death / cut record and its codes hidden).
  - Bar E2: P(exists) reliability within ±0.05 per decile at the hidden end visit and after.
  - Bar E3: P(cut | gone) reliability within ±0.05 per decile.
- **(iv) Future records:** the final live record of continuing trees with ≥ 3 real records hidden.
  - Bar E4: P(exists) reliability within ±0.05 per decile.
  - Sizes: A1 (overall + major group) and A2 overall within the bars, A3 totals 0.9–1.1.
- **Benchmark-leak guards:**
  - converted lineages get the codes a subplot tree would show;
  - hidden ends drop their RECONCILECD / fate fields;
  - stand fields exclude the tree itself.

**D54 amendment 1 (2026-10-30; before any D54 code or training): one mixed censored-end benchmark.**
- **The flaw.** Benchmarks (iii) and (iv) were specified as separate subsets. Calibration of P(exists) on a subset whose truth is all "gone" (or all "alive") cannot be assessed: a per-decile reliability bar there only rewards predicting 0 (or 1).
- **What production looks like.** Production's unknown ends are mainly procedural / non-sampled censoring (absence uninformative).
- **The merged benchmark.** For every lineage on a scored plot with ≥ 2 visible real records before visit j, and a known state at j:
  - j = the recorded death / cut visit (truth gone, cause known), or
  - j = the last real visit of a tree alive there with ≥ 3 real records (truth alive, size known).
- **How it is censored.** Hide everything from j on, procedural-style (absence uninformative, codes and fate hidden), and score slot j:
  - **E2:** P(exists) reliability within ±0.05 per decile (n ≥ 200) on the merged set;
  - **E3:** P(cut | gone) reliability among truly gone;
  - **E4 (sizes):** A1 overall + major group, A2 overall, A3 among truly alive.
- (ii) establishment is already mixed (hidden real visits TRUE, clean pre-debut FALSE) and keeps bar E1.

**D54 amendment 2 (2026-10-30; VAL smoke tests only, before any D54 dev or nested-CV run): every at-risk interval is an instance.**
- **The flaw.** Amendment 1 took one interval per tree: the death interval for gone trees, the last for survivors. That over-represents deaths relative to the per-interval rate (43 % gone on VAL), so a calibrated model would fail E2 by construction.
- **The fix.** Every at-risk interval (j − 1, j] with a known outcome is a separate instance: a real record at j, or a definitive death / cut at j, with ≥ 2 visible real records before j. Each instance is the lineage truncated after j and censored procedural-style at j. The benchmark's gone share then equals the population's per-interval rate. E2 / E3 / E4 are unchanged.
- **Also fixed before any run (a model bug found in smoke testing).** The end hazards now count every interval the tree was at risk, from its start s, during its record too. Before, they applied only after the last record, which conditioned survival away and trained the hazard almost only on trees that ended (smoke VAL: P(exists) 0.20 for both alive and gone).

**D54 amendment 3 (user, 2026-10-30; after the VAL dev check, before any nested-CV scoring): no biomass; implementation bug fixes.**
- **User.** "You can forgo biomass as a tree input and output. I am not interested in having the model be strained by fitted allometry."
  - The learned allometry and every AGB input, output and loss are removed.
  - **Bars re-expressed without biomass:**
    - A1: |median error of log DIA| ≤ 0.10, overall, per true 1-in class and per major group (n ≥ 200);
    - A2: 80 % predictive intervals of log DIA (including the FIA measurement error) cover 0.75–0.85, overall and per class;
    - A3: median ratio of predicted to true **basal area** (0.005454 · DIA², exact) per subplot-visit in 0.9–1.1;
    - P1 unchanged;
    - E4: the same A1 (overall + major group) / A2 overall / A3 on the censored-end benchmark's alive instances.
  - E1–E3 are unchanged.
- **Bug (confirmed on VAL).** The tally step was centred exactly on the limit, giving a tree measured at the limit a 50 % tally probability; the posterior DIA at 1.0–1.1 in microplot debuts was +7.8 % above the measurement. Fixed: P(tallied | DIA) = (1 − ε) σ(κ (log DIA − log(limit − 0.05 in))), κ = 40; the benchmark's 1-in lower bound likewise.
- **Design flaw.** Interval widths were fitted on pre-debut sizes and reused for future records (E4 coverage 0.94). Fixed: width factors per slot type (before the debut / after the last record), each fitted on the inner validation's matching benchmark.
- **Minor.** Moved-out trees (an end without a cause) are excluded from the cause hazards' likelihood (their end is marginalised over survive-to-move); previously either cause was allowed.
- **Consistency checks passed on the dev model:**
  - P(E) = 1 at real records;
  - after the last record, P(E) + P(died) + P(cut) = 1;
  - P(E) = 0 at recorded ends, with the cause respected;
  - clean FALSE slots have mean P(E) 0.012;
  - P(E) is monotone up to the debut.

**D54 amendment 4 (2026-10-30; verification before the nested CV).**
- **The verifier.** `nimiwae54_verify.py` checks the pipeline against independent re-computations:
  - the frame vs the curated DB;
  - the tensor rules per row;
  - the exact (s, e, f) sum and posterior by float64 brute force;
  - DReG gradient routing;
  - the benchmark leak tests;
  - the nested-CV fold logic.
- **Result:** 22 / 22 pass after three fixes.
  1. **Log-space likelihood.** The tally / cutoff / interval terms clamped probabilities at 1e-12 before the log. That truncated the penalty for grossly inconsistent paths (−27.6 instead of −36.8 on one tree) and zeroed their gradient. They are now computed with log-sigmoid / log1p (exact, differentiable).
  2. **End detection.** The end slot is now the first slot ON OR AFTER the dead-record (or move) date. A death recorded at a visit outside the frame (an unsampled subplot visit) previously never ended the lineage: 1 slot in 13,859 in the sample.
  3. **Sampled flag at hidden records.** Hidden benchmark records took sampled = 1 by construction; they now take it from the condition like any other slot. This removes a structural asymmetry between alive and gone instances in the censored-end benchmark.
- Dev run 2 started before these fixes, so a dev run 3 follows.

**D54 amendment 5 (user, 2026-10-31; before any nested-CV run): species, species revisions, damage.**
- **User.**
  1. "Why not species id too?"
  2. "A tree species might change from visit to visit (as the crew better differentiate the tree species)."
  3. "Tree damage might explain why a tree's growth is stunted."
- **Species.** A species embedding on the curated resolved SPCD (GA: 175 codes vs 28 groups), alongside the group embedding. Unseen species fall back to the group.
- **Species revisions.** 3.1 % of GA trees change observed species across visits. The curation resolves each tree to its LATEST identification, constant across its records, which is the right identity for completing earlier states. A per-tree "species revised" flag (curated spcd_changed) is added. The per-visit observed species, the label noise the curation resolved, is not used.
- **Damage.**
  - **Sources:**
    - FIADB TREE.DAMAGE_AGENT_CD1–3 by tree CN, recorded from 2013 (≈ 75–80 % of 2013+ records; ≈ 14–16 % damaged). Grouped by first digit: insect, disease, fire, animal, abiotic, competition, human, multiple, other / structural.
    - Broken top (ACTUALHT < HT, all years, ≈ 2 %).
    - Cull class (TREECLCD 2 / 3 / 4, all years).
  - **How they enter the model:**
    - They exist only where the tree was tallied, so they are **reconstructed observations** at visible real records (masked elsewhere, like DIA), not per-slot decoder inputs. Feeding them to the decoder would make the record pattern a cause of the sizes.
    - Per-tree summaries computed from visible records only (any damage by group, ever broken, ever cull, species revised) enter the static covariates of the prior and the decoder.
  - **Masking and verification:** the damage inputs are masked at every hidden benchmark record; verifier checks B5 and F6 added.
- **Data.** The prep gains `--fiadb`, writing to nimiwae_r8/d54b. Dev run 3 (on d54, without these) is the baseline.

**D54 amendment 6 (2026-10-31; dev, before any nested-CV run): start-interval fraction.**
- **The bias.** DIA at a tree's first existing visit was 1 in + the FULL interval's increment. The tree crossed 1 in somewhere inside that interval, so this overstated its size there. To match an observed debut size, the posterior then shifted the start later, which is the systematic under-prediction of existence in the 1–2 visits before the debut (E1 failing in dev runs 1–3: predicted 0.02 → observed 0.38–0.40).
- **The fix.** A decoder head gives φ_t ∈ (0, 1), the fraction of the interval ending at t that elapsed after crossing 1 in: D[s, t] = 1 in + φ_s δD_s + Σ_{s<u≤t} δD_u (and HT likewise). The brute-force verifier is updated to the same formula.
- **Next.** Dev run 4 (on d54b) combines amendments 5 and 6. Dev run 3 (d54, neither) is the baseline: sizes MAE 0.176, E1 fails, E2 / E3 close, E4 passes.

**D54 amendment 7 (user, 2026-10-31): imputations consistent with the measurements.**
- **User.** Imputed DIA / HT should not exceed later observed measurements ("increments only, unless damage").
- **Before.** The latent paths were monotone, but measurements were matched softly (DIA within the FIA error, HT within the learned error), so an imputed value could sit above a later measurement or below an earlier one. Only the unconditional DIA used for P1 was capped, and only at the first record.
- **Now.** Every imputed slot is bounded by the tree's own measurements: the largest at or before it, the smallest at or after it (running max / min, so monotonicity is preserved). This applies to DIA and HT in all outputs (and to basal area through DIA). Observed slots keep their measured value.
- **Recorded decreases.** Where a tree's own records decrease (measurement error, or damage such as bark loss), the bounds cross and the slot is clamped between them, so the recorded decrease is kept and imputations never invent one.
- **Height.** FIA's HT is total height (a broken top is recorded in ACTUALHT), so it should never decrease.
- **Verifier.** I3 (bounds respected, observed values unchanged) and I4 (no decrease across existing slots).
- **Amendment 7, details after verification.**
  - Per-slot "size given existence" medians can dip as the mix of existing samples changes, so the bounded series also takes a running maximum. This stays within the (non-decreasing) upper bound, and observed values are restored afterwards.
  - Interval endpoints shift with the point.
  - Basal area is rescaled by the same adjustment.
  - Verifier I1–I4: 7 / 7 checks in the gap set pass.
  - Dev run 4 was started before this change, so its scoring does not include the bounds; the nested CV will.

**D54 amendment 8 (2026-10-31; semantic audit before the nested CV and the diagram update).**
1. **Sampled lineages, the cohort product, were promised but not implemented.** Added `sample_lineages`: each draw picks one (z, start, end, cause) from the posterior and takes that path, bounded by the tree's own measurements (monotone paths stay monotone). It is gated by existence, observed values are kept, and it has NaN outside the tree's sampled span.
2. **Completion span per tree.** Leading / trailing slots where the tree's spot was not sampled (e.g. land-use diversion, afforestation) are not completed; only sandwiched unsampled slots are. The `in_span` output flag.
3. **"Damage ever recorded" in the per-tree summaries.** DAMAGE_AGENT_CD exists only from 2013, so a summary of 0 was ambiguous between "no damage" and "not recorded".
4. **The P1 internal size takes only the upper bound.** It is never above a later record; a lower bound would wrongly lift post-death zeros.
5. **Size given existence is undefined (NaN) where no posterior sample has the tree existing.** Before, a placeholder median and a random interval were reported there. The scoring and width fitting skip undefined slots.
6. **Not implemented.** The sapling-total split rule for stand totals is a production aggregation step, outside the model; it remains planned.
- **Verifier:** checks I0 and J1–J3 added; the full verifier passes 24 / 24 core and 13 / 13 gap checks.

**D54 amendment 9 (user, 2026-10-31): plot design from the data, no hard-coded TPA; stand-total split implemented.**
- **User.** "Don't forget about macroplots and other plot design TPA. Be careful not to rely on hard-coded TPA."
- **Data (8 states).** DESIGNCD 1 only; two TPA_UNADJ classes, 74.965 (DIA 1.0–4.9) and 6.018 (≥ 5.0); no MACRO_BREAKPOINT_DIA anywhere. The code is nevertheless design-general.
- **Tally levels from the data.**
  - Per (DESIGNCD, TPA_UNADJ class), the minimum DIA tallied is that level's threshold. Levels are ordered by TPA: largest TPA = smallest plot (microplot, then subplot, then macroplot).
  - Per tree: thr_in = its debut record's level threshold; thr_micro / thr_subp = the design's smallest / second level.
  - Replaced, all derived from these: the prep's "microplot if debut TPA > 50" rule; the model's 1 / 5 in limits (through growth → the smallest level); the neighbour threshold (thr_subp); the size benchmark's pseudo-limit and lower bound (thr_subp / thr_micro per tree); the sapling / tree reporting split (thr_subp).
  - For DESIGNCD 1 these reproduce exactly 1.0 / 5.0 in, so results are unchanged; the data move to nimiwae_r8/d54c.
- **Stand-total split (implemented: `expansion`, `stand_totals`).**
  - A tree's expansion at every visit where it exists = the TPA_UNADJ of its record on the LARGEST nested plot it was ever tallied on, i.e. its smallest observed TPA_UNADJ.
  - Because FIA plots are nested, trees reaching a larger plot's tally level are enumerated over that whole plot (observed or completed), and the rest are represented by the smaller plot they stand on. For DESIGNCD 1:
    - microplot saplings that later reach 5 in → 6.018 at every visit;
    - other microplot saplings → 74.965;
    - subplot trees → 6.018.
  - A macroplot design would give its breakpoint trees the macroplot TPA in the same way.
  - Totals of TPA and basal area per visit are computed over posterior lineage draws (mean, 10 / 90 %), with saplings and trees separate and nothing outside a tree's sampled span.
- **Verifier.** B2 / B6 (levels from the data, consistent with every record), F3 (benchmark limit = the design's subplot level), K1–K3 (expansion rule; single-level trees keep their own TPA; one expansion per tree; totals).

## D55 — Size-defined existence, MAR on measurements (MIWAE), flexible decoder (user, 2026-10-31; recorded before any D55 code)
**Diagnosis (D54 dev runs 3–4).** Establishment calibration E1 fails: predicted 0.02 → observed 0.37–0.40, unchanged with 1,000 importance samples (so it is the model, not inference). The separate start hazard was coupled to size only through "DIA ≥ 1 in from the start". The MNAR mask term rewarded explaining "not tallied" by distorting the unobserved path (a late start plus a burst in one interval), and the flexible decoder made such a burst cheap.

**User.** "MNAR provides freedom for the missing values to be as different as they want from observed … MAR would restrain the model a bit (no mask), so that the same growth model for the observed applies for the missing." Agreed: size-defined existence.

**Model.**
- **Existence from size.** E_t = 1[latent DIA_t ≥ 1 in] and t < e (end). The latent path starts at a latent initial size, which may be below 1 in (unobservable), plus non-negative increments: monotone, so exactly one appearance.
- The start hazard, the start-interval fraction φ and the start enumeration are removed. Only the end e and cause f are enumerated. The tree is at risk from its first existing slot.
- **MAR.** No missingness-mask likelihood: no tally-cutoff, crew-miss or interval term. Missing values follow the same generative growth model as observed ones.
- **Design information as an OBSERVED existence variable**, scored against E_t:
  - clean microplot pre-debut FALSE (soft);
  - missed (codes 3 / 6 / 7) TRUE (soft);
  - debut through the last record TRUE (hard);
  - recorded death / cut / move FALSE from then on (hard) plus the cause;
  - hidden benchmark visits that were tallied TRUE (interval protocol).
  - The existence variable's own missingness depends on location (microplot or not), not on existence: MAR under the design-independence assumption.
- **Size-existence coupling.** E_t uses a soft step σ(κ(log D_t − log(1 in − 0.05 in))), κ = 40, inside the existence evidence.
- **Decoder kept flexible** (per-slot increments from z + context). A structural growth constraint (per-tree vigour + shared responses) is added only if the dev check still shows bursts in unobserved intervals.
- **Unchanged:** species, damage, crown reconstruction; the fixed DIA measurement error; DReG; the outputs (bounds, monotone, undefined where no sample exists, completion span, sampled lineages, stand totals); the benchmarks and bars of D54 with amendments 1–3, 9.
- **Name.** MIWAE on measurements (NIMIWAE with the missingness model removed).

**D55 amendment 1 (user, 2026-10-31; before any D55 training): compare censoring and growth structure, 2 × 2.**
- **Review of the MAR argument.**
  - For NIMIWAE with a LEARNED mask network, MAR restrains the freedom to explain the mask by distorting missing values.
  - Ours is FIA's deterministic design rule (only the crew-miss rate ε is learned), and it carries true information: a subplot tree first seen at 5.3 in was under 5 in at the previous visit. Pure MAR would only know "≤ 5.3".
  - The burst came from the separate start hazard (now removed), not from the mask.
  - The user asked to build both and compare.
- **Factor A, censoring:**
  - **known**: the design rule as an observed inequality on the same path, with no learned missingness parameter (ε removed; missed tallies are already flagged by RECONCILECD 3 / 6 / 7). It applies only where it adds information beyond the existence labels:
    - slots whose tally limit is above the existence threshold (subplot-level trees before the debut: D < limit if the tree exists);
    - missed slots (D ≥ limit);
    - the benchmark interval.
    - Microplot-level slots keep their SOFT FALSE label (not a hard inequality), as the user required.
  - **mar**: existence labels only.
- **Factor B, growth:**
  - **flex**: per-slot increments from the decoder on (z, context);
  - **struct**: δ_t = Δt · exp(v_tree(z) + g(size_{t−1}, species) + c(context_t)). Per-tree vigour plus SHARED size and context responses, so no tree-specific per-interval freedom; the initial size comes from (z, statics).
- **Variants:** V1 known + flex, V2 mar + flex, V3 known + struct, V4 mar + struct. All use size-defined existence and everything else of D54 (amendments 1–9).
- **Dev comparison (VAL only).** Each variant: 150k TRAIN trees, 4 epochs, K = 32, DReG; widths fitted on VAL; scored on the D54 VAL benchmarks.
- **Selection rule (fixed now):**
  1. the most bars passed among A1, A2, A3, P1, E1, E2, E3, E4;
  2. ties → the lowest sum of ratios to the best variant for E1 Brier, E2 Brier, E3 Brier, size MAE (log DIA) and E4 MAE (log DIA).
- The selected variant goes to the nested CV; the comparison table is recorded here first.

**D55 amendment 2 (2026-10-31; semantic audit before any D55 result was read; the first 2 × 2 launch was stopped at ~30 min and discarded).**
- **The audit.** The verifier checked the likelihood against a brute force, but the brute force took the missingness modes from the same `build`, so a wrong mode could not show. Three mode rules did not match the D54 case table (user):
  1. RECONCILECD 8 / 9 entries: the pre-debut visits carry no information, but they were 'cutoff' (an informative "below the limit"). Now 'ignore'.
  2. RECONCILECD 3 / 6 / 7 entries: only the visit before the debut is 'missed' (D ≥ limit); the earlier visits were 'cutoff', but the tree may have been missed there too (inferred back to the first visit). Now 'ignore'.
  3. Known censoring: a sampled, unrecorded visit AFTER the last record is an informative absence at every tally level. The `above_ex` restriction (inequality only where the limit is above the existence threshold) had removed it at microplot-level slots. It now applies only to pre-debut slots; post-record slots always get the cutoff term.
- **Options:** (a) fix the three rules and restart the comparison; (b) let the run finish and fix later. Chosen (a): rules 1–3 change only the known arm, so finishing would compare against a mis-specified arm.
- **New check B7** (`nimiwae55_verify.py`): the mode of every unrecorded slot is recomputed from the written rules, independently of `build` (300 random plots: 0 mismatches). The brute force applies rule 3 itself. 43 / 43 checks pass on d54c, all four variants.
- The comparison design, bars and selection rule (amendment 1) are unchanged.

**D55 note 3 (2026-10-31, user discussion; no change to the running comparison).**
- **Relation to NIMIWAE / not-MIWAE (checked against Lim et al., arXiv 2101.07357, Eq. 8 and Web Appendix).** NIMIWAE: encoder 1 q(z | x_obs) with missing entries set to 0 (it does NOT see r); encoder 2 q(x_m | z, x_obs, r) is the only network that sees the mask; mask model p_φ(r | x_obs, x_m) learned. not-MIWAE: no encoder 2, x_m drawn from the decoder. Ours: missing sizes are the decoder's latent path D(z) (no encoder 2, not-MIWAE-like); the mask model is FIA's fixed design rule (user: deterministic given the measurement, nothing to learn); our z encoder sees r, so it is q(z | x_obs, r, c) — a valid proposal (the true posterior depends on r under MNAR), but not NIMIWAE's encoder 1.
- **Candidate amendment (decide after the 2 × 2; not applied).** The rule acts on the MEASURED DIA. Integrating the fixed FIA measurement error gives P(tallied | D) = Φ((log D − log limit) / σ_meas(D)) exactly; the κ = 40 logistic has sd ≈ 0.045 log units vs σ_meas ≈ 0.023 at 5 in (about 2× too soft). Options: (a) keep the logistic; (b) the exact probit (no sampler needed); (c) full NIMIWAE: per-slot process noise + encoder 2 with a proposal truncated to the rule's side. Recommendation: (b) for the selected variant if the known arm wins; (c) only as an extra variant if the user asks.

**D55 2 × 2 dev comparison (VAL, amendment-2 code; 150k TRAIN trees, 4 epochs, K = 32) — recorded as scored.**

| Variant | A1 | A2 | A3 | P1 | E1 | E2 | E3 | E4 | bars | E1 Brier | E2 Brier | E3 Brier | size MAE lD | E4 MAE lD | Σ ratio | ESS (K=32) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| V1 known + flex | ✗ | ✗ | ✓ | ✓ | ✗ | ✗ | ✗ | ✓ | 3 | 0.1255 | 0.1523 | 0.0780 | 0.1981 | 0.0591 | 5.274 | 2.8 |
| V3 known + struct | ✗ | ✗ | ✗ | ✓ | ✗ | ✗ | ✗ | ✓ | 2 | 0.1440 | 0.1746 | 0.0615 | 0.2470 | 0.0602 | 5.566 | 1.8 |
| V2 mar + flex | ✗ | ✗ | ✗ | ✓ | ✗ | ✗ | ✗ | ✓ | 2 | 0.1461 | 0.1547 | 0.0769 | 0.2554 | 0.0587 | 5.721 | 2.8 |
| V4 mar + struct | ✗ | ✗ | ✗ | ✓ | ✗ | ✗ | ✗ | ✓ | 2 | 0.1577 | 0.1674 | 0.0624 | 0.3576 | 0.0611 | 6.216 | 1.8 |

The rule selects V1. The known arm beats MAR on sizes in both growth forms (MAE lD 0.198 vs 0.255; 0.247 vs 0.358; A3 BA ratio 1.09 vs 1.29 / 1.22 vs 1.53); structured growth is worse than flexible on sizes and E1/E2, better on E3.

**D55 amendment 3 (2026-10-31; from diagnosing the dev results above; before any nested CV): the censored-end benchmark keeps the full frame.**
- **Diagnosis (V1 dev model):**
  - E2 is miscalibrated equally on TRAIN plots (0.15 → 0.28, 0.35 → 0.61), so it is not overfitting.
  - The whole gap is in untreated intervals (VAL: predicted 0.76 vs observed 0.89 existing; treated 0.28 vs 0.30).
  - The prior-predictive hazard at the same training intervals is 0.116 on the FULL lineage vs 0.214 on the benchmark copy truncated after j (empirical end rate 0.115).
  - The D54 construction truncated each instance after j; the bidirectional decoder never sees a frame end there in training, and a censored tree in production keeps its subplot's later visits. The failure was the benchmark's distribution shift, not the hazard model.
- **Options:** (a) keep the truncated construction (it scores a situation that never occurs in production); (b) keep the full frame and hide the tree's own records, end evidence and codes from j on; (c) retrain with randomly truncated frames so the model is robust to both. **Chosen (b):** it is what a censored tree looks like in production. The bars are unchanged. It applies identically to all four variants.
- **Leak checks** (verifier B8 / B9, replacing D54 F2): no tree-own record, evidence or code at or after j; alive vs gone identical in every tree-own input at every slot ≥ j; frames extend equally (0.90 vs 0.90 slots after j). Context of later visits (other trees' BA / TPA, treatments, disturbances) is legitimately known, as in production. 45 / 45 checks pass.
- **Rescoring:** the four trained dev models are rescored on VAL (widths refitted, since the post-record widths come from this benchmark); the selection rule is re-applied to the rescored reports.
- **E1 (open, not a benchmark issue):** the latent early-size posterior is overconfident. At 3 / 4 slots before the first visible record, the share truly existing is 0.51 / 0.23 while mean P(exists) is 0.90 / 0.74. Medians are right (latent D 3.16 vs true 3.20 in at 2 slots back). K = 1000 and a 2–3× wider proposal leave it unchanged, so it is the model, not the importance sampling. Given z, the path is deterministic, so all spread in unobserved growth must come from z. Candidate (D56, after the rescore): per-interval process noise on growth with its own encoder (NIMIWAE's encoder 2), identifiable because the measurement error is fixed.

## D56 — Per-interval process noise on growth, with its own proposal (NIMIWAE encoder 2) (2026-10-31; recorded before any D56 code)
- **Problem (D55 am. 3, E1):** given z the latent size path is deterministic, so the uncertainty of unobserved growth comes only from z; the early-size posterior is overconfident (P(exists) 0.90 vs 0.51 truly existing 3 slots back; pre-debut width factors 2.3–2.6).
- **Change:** each interval's increment gets its own latent u_t ∈ R² (DIA, HT), prior N(0, I):
  - flex: δD_t = softplus(dec_t) · exp(σ_D u_t − σ_D² / 2); HT likewise;
  - struct: rate_t · exp(σ u_t − σ² / 2);
  - σ_D, σ_H learned scalars (init 0.3); the −σ²/2 keeps the mean multiplier at 1.
  - Monotonicity is kept (the multiplier is positive).
- **Proposal:** q(u_t | z, x_obs, r) per slot, from the z encoder's per-slot transformer outputs and z (NIMIWAE's encoder 2). Its parameters are encoder parameters: DReG with both q densities detached.
- **Identifiability:** the DIA measurement error is fixed at FIA precision, so the spread of consecutive measured increments around the z + context prediction identifies σ_D. The learned discrepancy σ_disc stays monitored.
- **Weights:** log w gains Σ_t v_t [log N(u_t; 0, I) − log q(u_t | ·)].
- **Unchanged:** existence from size, (e, f) enumeration, censoring arms, benchmarks (with am. 3), bars, outputs.
- **Dev comparison (pre-registered):** the D55 variant selected on the rescored reports (base) vs base + noise; 150k TRAIN trees, 4 epochs, K = 32; VAL only; the D55 am. 1 selection rule. Diagnostics reported alongside (not selection criteria): pre-debut width factors (closer to 1 is better calibrated), ESS, σ_D, σ_H, σ_disc. The selected model goes to the nested CV.
- **Verification before training:** a nimiwae56 verifier extending the D55 one (brute-force (e, f) sum given the sampled (z, u); u log densities; monotone paths; DReG surrogate with both q terms detached; noise-off limit reproduces D55).

**D55 rescore on the am.-3 benchmark (VAL; same trained models; widths refitted).**

| Variant | A1 | A2 | A3 | P1 | E1 | E2 | E3 | E4 | bars | E1 Brier | E2 Brier | E3 Brier | size MAE lD | E4 MAE lD | Σ ratio |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| V1 known + flex | ✗ | ✗ | ✓ | ✓ | ✗ | ✗ | ✗ | ✓ | 3 | 0.1255 | 0.1508 | 0.0774 | 0.1981 | 0.0587 | 5.258 |
| V3 known + struct | ✗ | ✗ | ✗ | ✓ | ✗ | ✗ | ✓ | ✓ | 3 | 0.1440 | 0.1720 | 0.0615 | 0.2470 | 0.0603 | 5.563 |
| V4 mar + struct | ✗ | ✗ | ✗ | ✓ | ✗ | ✗ | ✓ | ✓ | 3 | 0.1577 | 0.1700 | 0.0625 | 0.3576 | 0.0613 | 6.250 |
| V2 mar + flex | ✗ | ✗ | ✗ | ✓ | ✗ | ✗ | ✗ | ✓ | 2 | 0.1461 | 0.1511 | 0.0765 | 0.2554 | 0.0587 | 5.701 |

The rule still selects **V1** (tie at 3 bars with V3, V4; lowest Σ ratio). E2 barely moved (0.152 → 0.151), so the truncation was not the whole cause.

**D55 amendment 4 (2026-10-31; bug: survivorship leak into the generative side; found diagnosing E2).**
- **Finding:** on the full-frame benchmark the prior-predictive hazard is still 0.211 vs 0.116 on the original lineages. Of all decoder / prior inputs only the per-tree summaries differ (43 % of instances). These are species revised, damage / broken / cull ever recorded, and damage-recorded-ever.
- They are functions of the tree's OWN record sequence, so they encode its survival: "damage ever recorded" ≈ "tallied alive at a 2013+ visit"; a species revision needs repeated visits.
- They entered the prior p(z | statics) and the decoder's tree token, violating the design rule that the generative side never sees the tree's own records. In production a censored tree's summaries miss its unobserved future, so the leak biases real completions too, not only the benchmark.
- **Options:** (a) drop the summaries everywhere; (b) keep them in the encoder only (a proposal may use any information) and remove them from the prior and decoder; (c) make them causal per-slot covariates ("recorded so far"), but those still depend on the tree's own observation pattern. **Chosen (b).**
- **Check B10** (both verifiers): the prior / decoder inputs (statics token, context, padding) are identical for a censored-end instance and its original lineage (Δ = 0 over 11,567 instances; the encoder token differs, up to 0.47, as allowed). D55 46 / 46, D56 54 / 54.
- **Consequence:** the trained D55 models are invalid (leaky); the D56 comparison retrains both arms under am. 4: base = V1 (known + flex, selected in both D55 scorings; the leak is common to all four variants, so the D55 2 × 2 is not rerun) vs V1 + noise.

**D56 dev comparison (VAL; am. 3 + am. 4 code; both arms retrained; 150k TRAIN trees, 4 epochs, K = 32).**

| Arm | A1 | A2 | A3 | P1 | E1 | E2 | E3 | E4 | bars | E1 Brier | E2 Brier | E3 Brier | size MAE lD | E4 MAE lD | Σ ratio | ESS |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| base (V1, no noise) | ✗ | ✗ | ✓ | ✓ | ✗ | ✗ | ✗ | ✓ | 3 | 0.1130 | 0.1136 | 0.0764 | 0.1911 | 0.0582 | 5.144 | 2.3 |
| + noise | ✗ | ✗ | ✓ | ✓ | ✗ | ✗ | ✗ | ✓ | 3 | 0.1308 | 0.1097 | 0.0689 | 0.2031 | 0.0617 | 5.280 | 2.7 |

- **Selected: base** (tie at 3 bars; lower Σ ratio).
- **Am. 4 effect:** E2 Brier 0.151 → 0.114. Deciles are now mildly under-predicting ends (0.15 / 0.04 … 0.65 / 0.54), still outside ±0.05.
- **Noise diagnostics:** σ_D 0.32, σ_H 0.25. Posterior E[u²] on bracketed training intervals is ≈ 1 in every size class (1.19 under 2 in, 0.90–0.94 over 5 in), so the noise is calibrated and size-dependent noise would not help. Pre-debut width factors 2.18 / 2.31 (base) vs 2.24 / 2.11 (noise): the noise did not fix the early-size spread.
- **E1 stays open.** P(exists) is still ≥ 0.9 at almost every establishment slot; noise did not change it. It is not the proposal (am. 3 diagnosis) and not the per-interval noise level. Remaining hypotheses: (i) the timing of reaching 1 in is set by unobserved growth below 1 in, which no measurement constrains, while FALSE labels only bound it; (ii) lognormal increments cannot represent the bimodal sapling histories (suppressed for decades vs released). Not pursued autonomously; for the user.
- **Next (per the D55 am. 1 plan):** 5-fold nested CV of the selected model (`--impl nimiwae56 --noise 0 --censor known --growth flex`), 400k trees, 4 epochs, K = 32 training / 100 evaluation, full benchmarks. Folds, inner validation and dev-VAL exclusion as in D52–D54.

## D57 — Context-conditional prior p(z | statics, context sequence) (2026-10-31; recorded before any D57 code)
- **Evidence (nested-CV fold models of D56-base, while folds 3–4 run):**
  - E1 overconfidence concentrates in planted softwoods (gap pE − truth 0.114 planted vs 0.043 natural; by slots before the first visible record 0.11 / 0.24 / 0.30 at 2 / 3 / 4 back).
  - At the planted-pine FALSE slots the model calls existing (pE > 0.8), the subplot is essentially empty: other trees' TPA median 6 vs 225 at TRUE slots, BA 0.9 ft²/ac, 55 % cutting treatment. These are post-clearcut, pre-establishment visits, and the context says so.
  - The same miscalibration on TRAIN plots converted the same way (fold-0 model: Brier 0.084 train vs 0.079 outer; planted pine pE 0.85 vs truth 0.71 in both). The model has not learned the context → establishment-timing mapping; this is not overfitting.
- **Mechanism (hypothesis):** the prior p(z | statics) sees no per-visit context. In training the encoder (labels, records) always supplies the timing through z, so the decoder need not learn it from context. When the records are hidden, z falls back to a prior blind to the stand history.
- **Options:**
  - (a) a context-conditional prior: a transformer over the tree's known per-visit context (cov) plus the statics token. It never sees the tree's own records, so it stays on the generative side; z then carries only tree-specific residuals.
  - (b) shrink z (a bottleneck forcing the decoder onto context);
  - (c) supervised timing features; out (no supervised terms).
  - **Chosen (a).**
- **Leak guard:** B10 extended: the prior's mean / variance are identical for a censored-end instance and its original lineage.
- **Dev comparison (pre-registered):** base (D56, noise off) vs base + context prior; 150k TRAIN trees, 4 epochs; VAL; the D55 am. 1 selection rule. Diagnostics: E1 by planted / natural, E3 deciles. The selected model goes to nested CV (the running D56-base NCV is completed and recorded either way).
- **E3 evidence for D57 (D56-base NCV folds 0–2, gone instances with predicted cut share 0.5–0.8, n 9,105):** cutting treatment recorded → observed 0.80 vs predicted 0.685; no cutting → 0.55 vs 0.64; disturbance code → 0.57 vs 0.67. The context effects are shrunk toward the middle, the same signature as E1. D57 should move both if the mechanism is right.
- **D57 result (VAL dev, 150k, 4 epochs): no gain.** ctx vs base: 3 vs 3 bars; E1 Brier 0.1117 vs 0.1130, still P ≥ 0.9 everywhere (1.00 / 0.90); E2 0.1141 vs 0.1136; E3 0.0806 vs 0.0764 (worse); size MAE 0.188 vs 0.191. **Selected: base** (Σ ratio 5.039 vs 5.060). The hypothesis (a context-blind prior) is refuted.
- **Follow-up diagnostic (fold-0 NCV model, 400 converted TRAIN trees with a truth-FALSE slot):** the converted data are about equally likely at the encoder's z from the ORIGINAL lineage (which knows the true timing; P(exists) at FALSE slots 0.35) and at the z from the converted lineage (0.53): median Δ log joint −0.23, 48 % favour the original. So it is not inference: with the sapling records hidden, the data hold little timing information, and E1 depends on the learned timing distribution.

## D58 — Training budget (2026-10-31; recorded before the run)
- **Evidence:** in every NCV fold the training loss still falls steeply over the 4 epochs (fold 0: 122.6 → 2.83 → 1.56 → 1.14; ESS 1.7 → 5.3). Dev (150k trees) vs NCV (400k) differ strongly on E1 (Brier 0.113 vs 0.075–0.098), E2 (0.114 vs 0.098–0.100) and widths (2.2 vs 1.3–1.6). The model is under-trained, and capacity / data may dominate the remaining failures.
- **Comparison (pre-registered):** base (D56, noise off, static prior) at 12 epochs vs the existing 4-epoch dev run; 150k TRAIN trees; VAL; the D55 am. 1 rule. If 12 epochs is selected, the next nested CV uses the larger budget (epochs, and trees as compute allows).
- Also: the NCV driver now stores the model config in the fold checkpoints.
- **D58 result (VAL dev, 150k trees): 12 epochs selected.**

  | Budget | bars | E1 Brier | E2 Brier | E3 Brier | size MAE lD | E4 MAE lD | Σ ratio | ESS |
  |---|---|---|---|---|---|---|---|---|
  | 12 epochs | 3 | 0.0765 | 0.1004 | 0.0610 | 0.1786 | 0.0514 | 5.000 | 5.0 |
  | 4 epochs | 3 | 0.1130 | 0.1136 | 0.0764 | 0.1911 | 0.0582 | 6.063 | 2.3 |

  - The same bars pass (A3, P1, E4). Every tie-break metric improves; pre-debut width factors 2.18 / 2.31 → 1.75 / 1.61.
  - The remaining gaps are near the bars:
    - E2 deciles 1–3 and 6–7 are off by 0.04–0.06;
    - E3 deciles 5–7 under-predict cut by 0.05–0.06;
    - E1 decile 8 is still 0.85 vs 0.65 observed.
  - A1 / A2 fail only in the truth-binned 1–2 in cell (+0.48; coverage 0.54). By prediction the 1–2 in bias is −0.18. Conditioning on the truth class makes the extreme classes look biased even for a calibrated posterior, so this cell may not be passable as pre-registered; flagged for the user, not changed.
  - **Monitor:** σ_disc rises over training (0.020 → 0.038 at epoch 11; FIA measurement sd ≈ 0.02–0.03). Watch it in the next runs: a growing discrepancy can absorb growth signal.
  - Loss 1.11 → 1.04 → 1.00 over the last epochs, under the annealing LR, so saturation is not established.
- **Next (decided):** once the running D56-base NCV finishes, two dev runs on VAL, 4 threads each:
  - (a) 150k trees × 24 epochs (more epochs);
  - (b) 400k trees × 12 epochs (more data).
  - Same rule against the 12-epoch run; the winning budget goes to the nested CV.

**D56-base nested CV (5 folds, 400k trees, 4 epochs; outer folds pooled; dev-VAL plots never scored).**
- **Bars:**
  - A1 ✗ (overall +0.013; truth-binned 1–2 in +0.49, 2–3 in +0.16);
  - A2 ✗ (overall 0.81; 1–2 in 0.52, 2–3 in 0.72, 4–5 in 0.91);
  - A3 ✓ (BA ratio 1.023); P1 ✓ (0 violations);
  - E1 ✗ (Brier 0.082); E2 ✗ (Brier 0.100); E3 ✗ (Brier 0.056); E4 ✓ (MAE lD 0.051, coverage 0.80, BA 1.002).
- **Size MAE** lD 0.172 (n 33,160 hidden).
- **E1 deciles (pred / obs):** 0.15 / 0.07, 0.25 / 0.10, 0.35 / 0.19, 0.45 / 0.25, 0.55 / 0.31, 0.65 / 0.38, 0.75 / 0.49, 0.85 / 0.61, 0.99 / 0.95. Existence is over-predicted by ≈ 0.2 across the range, though the ranking is right.
- **E2 deciles:** within 0.05 except 0.65 / 0.60 and 0.76 / 0.70.
- **E3 deciles:** within 0.05 except 0.55 / 0.60, 0.65 / 0.71, 0.75 / 0.80.
- **Superseded by D58:** a 12-epoch budget is better on every tie-break metric. The budget runs (D58b) choose the configuration for the next nested CV; this report is the 4-epoch reference.

**Finding (2026-10-31): the E1 truth labels are least reliable exactly where E1 fails. Open; for the user.**
- **The labels.** E1's truth FALSE is the clean microplot slot before a sapling's debut (RECONCILECD 1 / 2). The label implies the tree grew from < 0.95 in to its debut DIA within one interval.
- **The data** (6,000 plots; clean FALSE slot just before the debut; the tree's own rate over its next interval):

  | Debut DIA | Min. rate implied before (in/yr) | Own rate after | Share needing > 2× faster growth before |
  |---|---|---|---|
  | ≤ 1.2 in | 0.025 | 0.047 | 0.05 |
  | 1.2–1.5 | 0.074 | 0.078 | 0.26 |
  | 1.5–2 | 0.137 | 0.120 | 0.32 |
  | 2–3 | 0.252 | 0.188 | 0.35 |
  | 3–5 | 0.457 | 0.309 | 0.36 |

  About a third of the FALSE labels before debuts ≥ 1.5 in imply implausibly fast pre-debut growth: likely saplings that existed but were coded as new ingrowth, or debuts measured late.
- **The model.** On TRAINING trees with the labels visible, the posterior P(exists) at the FALSE slot just before the debut (D58 12-epoch model; learned ρ = 0.02) rises with debut DIA: 0.105, 0.18, 0.27, 0.37, 0.41. It tracks the implausible share. So the model overrides these labels at about the rate the data say they are suspect, far above its own ρ. The label-noise model is mis-specified: one global ρ cannot express size-dependent reliability.
- **E1 by debut size** (D56-base NCV pooled; worst |pred − obs| over deciles with n ≥ 200):
  - ≤ 1.2 in: 0.045, only the top decile populated;
  - 1.2–1.5: 0.058; 1.5–2: 0.137; 2–5 in (78 % of E1): 0.386.
  - The miscalibration concentrates where the truth is least reliable. A cleaner separation was tried and dropped: a growth-consistency subset keeps only 12 % of instances, almost all in the top decile, so it would pass vacuously.
- **Options (not applied):**
  - (a) keep E1 as pre-registered, accepting that part of its failure is truth noise;
  - (b) redefine E1's truth set before the next nested CV with a pre-specified reliability rule (e.g. debut ≤ 1.5 in), applied to every model;
  - (c) model side, independent of (a) / (b): a label-reliability ρ that depends on the implied debut growth (debut DIA, interval length; learned), extending the user's D54 remark that ρ might "depend on the gap (learned)". Against the current E1 truth it would look worse, because it trusts the suspect labels less. Evaluate it only once (a) / (b) is settled.
- **Recommendation:** (b) + (c). Because (b) changes a bar's truth set after seeing results, it is the user's decision.

**Label investigation (user: "investigate labels more"; 2026-10-31). CORRECTS the finding above: most "implausible" labels are ordinary growth deceleration.**
- **Population:** 153,022 microplot debuts with a clean FALSE slot just before (all splits); 69,886 have a second record. FIADB raw fields joined by the debut record (read-only).
- **Control (no labels involved):** measured consecutive sapling intervals (three records, TPA > 50) with rate_k > 2 × rate_(k+1):
  - by middle size: 0.134 (1–1.5 in), 0.249 (1.5–2), 0.279 (2–3), 0.273 (3–5);
  - planted 0.314, natural 0.250;
  - softwood 0.407, hardwoods 0.205–0.264.
  - Fast-then-slow sapling growth is common, so the label test flags many valid labels.
- **Label-implied flag vs control** (debut ≥ 1.5 in): overall 0.33 vs ≈ 0.27; planted 0.28 vs 0.31 (no excess); softwood 0.23 vs 0.41 (no excess); natural 0.39 vs 0.25 and hardwoods 0.38–0.43 vs 0.21–0.26 (excess ≈ 0.15).
- **Not explained by FIA coding:** RECONCILECD 1 vs 2 (0.33 / 0.36), DIACHECK, DIAHTCD, previous-tree links (none), interval length (0.32–0.40), most MANUAL transitions (0.27–0.41).
- **Two genuine label problems:**
  1. **Debuts at curated procedural re-tally visits:** 0.635 flagged (rc8 0.605, silent 0.650; 1,524 debuts ≥ 1.5 in; 9 % of flagged labels). KY MANUAL 1.0 → 4.0 (the known silent re-tally) 0.608. Under the D54 rules a procedural change carries no information about earlier visits (code 8), but `build` still writes a FALSE label before such debuts. This is a label-construction gap.
  2. **Natural hardwood saplings:** excess ≈ 0.15, consistent with crews missing small saplings in dense understory (coded as new rather than RC3).
- **Consequence for E1:** label noise is a minor part. The model overrides labels 0.27–0.41 (rising with debut size) where perhaps ≈ 0.06 are wrong, so it under-represents fast-then-slow sapling growth. E1 is mainly a model problem, plus problem 1.
- **Proposed next steps:**
  - (i) **label fix:** no FALSE label and mode ignore before a debut at a procedural re-tally visit (prep gains `procedural_breaks`; data d54d). It follows the existing code-8 rule; it changes training labels and E1's truth set alike, by an a-priori rule.
  - (ii) **model:** represent sapling growth variability (size-dependent or heavy-tailed per-interval growth). Check first whether D56's Gaussian noise underfits debut-like intervals; its E[u²] ≈ 1 check covered bracketed intervals only.

**D55 amendment 5 (2026-10-31; bug found answering the user's lineage-pattern question): RC9 (nonsampled) censoring is uninformative after the record.**
- **Pattern census of the lineage frame** (1,703,185 trees; per visit T live record, F sampled / no record, u unsampled, X dead / cut record, r / m / p status-0 shrank / moved / procedural-or-nonsampled; runs collapsed):
  - 99.97 % match `[Fu]* T [Tu]* (X|r|m|p)? [Fu]*`.
  - Deviations (456): internal sampled absence `T F T` / `F T F T` (302) and the same followed by an end `T F T X…` / `T F T r` (104), i.e. missed then re-tallied; `T X p…` (≈ 42), a status-0 closure after the death record (still contiguous).
  - No live-after-death, live-after-censoring or end-before-debut lineages: the curation resolves them.
- **The bug.** 15,128 contiguous lineages end `T F$` (the record stops while later visits look sampled): censored_nonsampled 10,543, diverted_landuse 2,752, censored_procedural_unrecorded 1,654, censored_procedural 125, disappeared_inferred_removal 48, dead 6.
  - `build` made post-record absence uninformative only for procedural / procedural_unrecorded / shrank fates. The ≈ 10.5k RC9 (nonsampled) trees therefore got the informative cutoff and, in the known arm, were forced to END, inflating the end hazards.
  - The D54 rule (user) is that code 9 carries no information. B7 encoded the same incomplete list, so it could not catch this.
- **Fix:** censored_nonsampled added to the ignore-after fates (nimiwae54 / 55 / 56 `build`) and to B7's written rule. D56 verifier 63 / 63.
- **Unchanged, deliberate:** diverted_landuse (2,752) keeps the informative absence. The land left forest, so the tree is gone: the end is real, the cause inferred.
- **Consequence:** every model trained so far (D55–D58, the D56-base NCV) carried this bug; the D58b budget runs still running too. Retrain under am. 5 before the next comparison or NCV.

**D55 amendment 6 (2026-11-01; user: "how do you know the tree's own condition without a record tying it to that condition?"): sampling at a no-record visit is known only at subplot / microplot level.**
- **The flaw.** `build` took the CONDID of the tree's nearest-in-time real record and its COND_STATUS at the no-record visit. FIADB assigns CONDIDs per visit (conditions split / merge / are remapped), and only a tree record ties a tree to a condition (TREE.CONDID). This FIADB build has no TREE DIST / AZIMUTH, so the tree cannot be located against the mapped boundaries.
- **The evidence** (3,256,382 no-record slots):
  - subplot fully forest 95.68 %; the borrowed condition wrongly said unsampled on 0.76 %, discarding real absences;
  - mixed subplot 3.95 % (median forested share 0.69); the borrowed condition guessed sampled on 3.45 %;
  - subplot share 0 on 0.25 % (the borrowed condition said sampled on 0.10 %); share missing 0.13 %.
- **Options:** (a) keep the borrowed condition; (b) subplot share only, and ignore absences on mixed subplots; (c) **P(spot sampled) = the forested share of the tree's tally area** (microplot for microplot-level slots, else subplot; 1 at real records); an absence enters as share · P(below the limit) + (1 − share). **Chosen (c)**, the stated fallback being (b) if the user prefers.
  - The pre-debut FALSE labels already required a fully forested microplot (unchanged).
  - "Sampled" for modes / span / labels is share > 0; the encoder receives the share.
  - (An intermediate patch, condition AND share > 0, was superseded the same day; legacy D54 / D55 keep it.)
- **FIADB v9.4 User Guide checks** (local copy `/workspace/wo-v9-4_Aug2025_UG_FIADB_database_description_NFI.pdf`):
  - **Share column.** SUBP_COND.SUBPCOND_PROP / MICRCOND_PROP = the proportion of the subplot / microplot in the condition. COND_STATUS_CD 1 = accessible forest land (2 nonforest, 3 / 4 water, 5 nonsampled). The curated `forest_subpcond_prop` = Σ SUBPCOND_PROP over COND_STATUS_CD = 1 per subplot-visit (fia_curation_v3 `sql_clean_subplot_years`). Subplot-visits with SUBP_STATUS_CD 3 (nonsampled) are not in the scaffold, so share = 0 → unsampled.
  - **RECONCILECD** (§3.1.82), all consistent with the D54 rules and am. 5:
    - 1 ingrowth, incl. "a new tree on land that was formerly nonforest": safe, because FALSE labels need a fully forested microplot at the previous visit;
    - 2 through growth ("grew from seedling to ≥ 5 in between cycles");
    - 3 missed live (retired from MANUAL 9.0, then 7 / 8);
    - 5 shrank;
    - 6 physical movement;
    - 7 cruiser error, (b) "erroneously not tallied (missed tree)";
    - 8 procedural change, "regardless of d.b.h. at the previous inventory";
    - 9 nonsampled area, "(a) … now in a nonsampled condition, or (b) … previously not sampled". This confirms am. 5.
  - **STATUSCD 0:** "not presently in the sample" (incorrectly tallied before, procedural change, or nonsampled condition).
- **Verification:** D56 verifier 64 / 64, incl. B12 (psamp = the share, recomputed independently of build) and C1 (the brute force uses the mixture). N1 compares with D55 on identical tensors with the share binarised (Δ = 0). Retrain under am. 5 + 6.

**D55 amendment 7 (2026-11-01; user: "how could you take CONDID as a persistent numbering over the visits — a rookie mistake"): no CONDID is carried between visits; condition fields are subplot-level.**
- **The mistake.** Since D54, `load54` gave every slot without a record the CONDID of the tree's nearest-in-time real record (falling back to the first record's, then CONDID 1) and read that CONDID's COND row at the visit: status, SICOND, SITECLCD, PHYSCLCD, SLOPE, ASPECT, TRTCD1–3, DSTRBCD1–3, FORTYPCD, LIVE_CANOPY_CVR_PCT, STDORGCD.
  - FIADB assigns CONDIDs per visit (conditions split / merge / are remapped), and only a tree record ties a tree to a condition.
  - Not checked against the User Guide when written. The verifiers checked the code against my rules, not the rules against FIADB semantics.
- **Exposure** (3,256,382 no-record slots): the subplot has > 1 condition at the visit on 6.63 %, where the tree's condition is unknowable; the plot has > 1 condition on 43.52 %, where the borrowed CONDID may point at a condition on another subplot. Fields drive the treatment / disturbance channels of the end hazards and the context of E1.
- **Fix:** condition fields per subplot-visit from `base_subplot_cond` (+ COND for PHYSCLCD / canopy), area-weighted by SUBPCOND_PROP over the subplot's accessible-forest conditions (all conditions when it has none):
  - weighted means: SICOND, SLOPE, canopy; ASPECT circular;
  - area-dominant classes: FORTYPCD, SITECLCD, PHYSCLCD, STDORGCD;
  - per code group the AREA SHARE carrying a treatment (trt_s10–50) or disturbance (dst_s10–90), replacing the borrowed 0 / 1 flags.
  - Applied at every slot, records included. Using the tree's own condition at record slots only would let the decoder tell record from no-record slots by the covariates' form (a leak).
  - Per-tree statics: `planted` is plot-level (curation land_use), unaffected.
- **Code:** nimiwae_prep.load54 (nearest-record CONDID CTE removed; cond_status deprecated, NULL); nimiwae56.build reads the shares (legacy frames unchanged). The D53 `load` keeps the old logic (legacy, unused).
- **Data rebuilt as d54d.** KY check: same 363,138 slots; null shares SICOND 25 %, canopy 53 % (older inventories), others < 0.3 %.
- **All models so far predate am. 5–7:** retrain on d54d.

## D59 — FIA Truth: one pipeline and DB per region, natural ids primary (user, 2026-11-01; plan approved in auto mode)
- **Request:** consolidate curation into a single source of curated truth for every downstream module; never assume a local id carries over between visits.
- **User decisions:** all consumers move; one DB per region; new schema with Pan's loaders updated; A1 (species-matched STDAGE in planted conditions); A2 (AGENTCD 80 relabelled `silvicultural_death`).
- **Identity (user, after bahaelaila7/FIA_DATA_PREP#1): do not trust CN keys; rely on natural ids.** This replaces the approved plan's CN-keyed design. Measured in CURATION_FIADB_AUDIT.md §Identifier audit.
  - Options: (a) CN primary (the plan as first written); (b) natural ids primary, CN never joined on; (c) both required on every join. **Chosen (b)**, with CN-vs-natural disagreements quarantined.
  - Visit = (STATECD, UNITCD, COUNTYCD, PLOT, INVYR); subplot + SUBP; condition + CONDID (scoped to the visit, never compared across visits); tree + SUBP, TREE.
  - Tree across visits: (plot, SUBP, TREE) under an unchanged DESIGNCD (guide §3.1.10), split where a number is reused after an end.
  - Conditions across visits: SUBP_COND_CHNG_MTRX, resolved to natural keys on both ends (its PREV_PLT_CN is validated against the plot's natural predecessor), or subplot-level evidence.
  - Plot key changes: explicit `plot_renumber` table, evidence-checked. 34 annual links here.
- **Deviations from the approved plan:**
  - Flat DB with the curation's table names, not a new `core` schema. With natural ids primary the existing composite keys are the ids, so a new key layer would add nothing and Pan's loaders would churn. Consumers change their path and CN joins only.
  - Schema `raw` added: genveg needs periodic and nonsampled visits, which the annual copies exclude. Its CN columns hold the natural visit id, so existing joins stay natural by construction; FIA's values are FIA_CN / FIA_PLT_CN.
  - No per-module product tables; consumers read the shared tables.
  - `curated_trees_fvs` is not built: only `tools/legacy` reads it.
  - NIMIWAE's legacy D53 `load` and the `--frame` / `--fiadb` options are removed.

## D60 — R4 regional fit: tree-number closure and the re-measured age cells (2026-11-01)
- **Closed tree numbers (verifier n19).** One AL plot (1/2/25/105) was closed STATUSCD 0 RC9 in 2001 and re-tallied in 2007 as RC3 on the same numbers with different species and sizes. FIA's PREV_STATUS_CD is 0 there, so "PREV_STATUS_CD non-null = link" merged 5 pairs of different trees.
  - **Fix:** PREV_STATUS_CD = 0 starts a new tree. Those are the only 5 such records in R4; D59's equivalence on 2.76 M successors stands, because those links point at the dropped STATUSCD 0 record.
- **Age cells.** One DB per region gives one age fit over 8 states, a scope that had never been measured. Bars are unchanged; the 4 cells that fail them are recorded as KNOWN with the drift guard, per D14 / D24:
  - (j) FL planted 10–15 in −3.6 yr and 15+ in −4.9 yr, flag `known_bias_fl_planted_model_10plus`;
  - (j) KY planted 5–10 in −4.4 yr, flag `known_bias_ky_planted_model_5to10`;
  - (h) KY planted step 5.6 vs limit 4.4 yr, 31 condition-visits, below the reliability floor.
  - **Exposure:** only trees without a TreeAge record take the model age: 86 FL and 123 KY live records.
  - **Option rejected:** fitting per state inside the regional DB. It would keep the single-state cells, but it changes the agreed one-fit-per-region design and gives up pooled support where single-state planted data are thin (KY 31 condition-visits).

## D61 — Forest Explorer's FVS engine stays on FIA's FVS tables (2026-11-01)
- **Situation:** the FVS engine starts from `FVS_STANDINIT_PLOT` / `FVS_TREEINIT_PLOT` in `SQLite_FIADB_ENTIRE.db`, picked by TreeMap's CN, and FVS's DBS reads SQLite. These are FIA's own FVS-ready tables, not curated records.
- **Options:** (a) keep them as a documented exception; (b) copy them into the truth DB, which moves the same uncurated rows and adds a DuckDB-to-SQLite step; (c) derive FVS inputs from curated trees, a new model input needing its own validation and FVS re-baselining. **Chosen (a)**; (c) is open if FVS should start from curated trees.
- **Audit:** R4 has 358,303 stands. 358,254 STAND_CNs resolve to a PLOT; all 358,254 agree on plot number, and state, county and INVYR agree wherever the stand records an inventory year. The 49 others are in the SQLite but not in FIADB.duckdb, so they are not in the truth DB.

## D62 — Region 4 only until the pipeline settles (user, 2026-11-01)
- Forest Explorer's FIA plots layer shrinks from the old table's 48 states to the 8 R4 states, which also removes the pre-fix national rows from the map.
- **No other region is built** until this pipeline is solid and checked further downstream. Many downstream products will move from bare curation to NIMIWAE-completed data once that work succeeds, so the consumer contracts in `docs/FIA_TRUTH.md` should expect a completed-data source later.
