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
  - A scope with no recorded entries is fully strict: a Region-4 fit must re-measure these cells and record its own values, which is the point of D14.
- **Reverse:** delete the scope entries from `KNOWN_EXCEPTIONS` and `KNOWN_BIAS_FLAGS`.
