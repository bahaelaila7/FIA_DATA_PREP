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

## D63 — Work deferred until after NIMIWAE (user, 2026-11-01)
- **After NIMIWAE, because some of it moves to completed data:** the interval-product rebuilds for HBAD v2, HBAD3P and HBAD4P, and the genveg table run.
- **After NIMIWAE:** the Forest Explorer restart; the explorer will move to completed data.
- **FVS stays on direct FIA for now** (D61): it is calibrated on it.

## D64 — Does conditioning on crown class explain sapling growth stalls? (diagnostic, 2026-11-01; nothing changed)
- **Test:** VAL only; checkpoints d58_ep12 (no noise, 12 epochs), d56_base and d56_noise (4 epochs), on d54c, so before am. 5–7.
  - Forecast: the tree's records hidden from its third record on.
  - Back-cast: 4,678 debuts ≥ 1.5 in with a clean pre-debut FALSE label, label hidden, scored as P(exists) at that visit.
- **Forward, already-suppressed trees: sufficient.** d58_ep12 mean rate, in/yr, predicted vs actual: canopy classes 0.224 vs 0.213; intermediate / overtopped 0.062 vs 0.056. The residual is within ±0.012 across the main crown classes.
- **Fast saplings that stall: not captured.** Of 1,970 fast trees, 163 stalled (8.3 %); predicted rate 0.206 vs actual 0.052. Predicted stall share 2.9 %. AUC 0.749. Part of this is unknowable forward, because the crown class at the interval's end is hidden.
- **Back-cast: crown class barely matters.** d58_ep12 P(exists) at the labelled-absent visit, debuts with early rate > 2× later rate: already suppressed 0.901, fell out of the canopy 0.863, stayed in the canopy 0.915; unflagged debuts 0.507. The later growth rate drives it, not the crown history.
- **Process noise did not fix it.** d56_noise: 0.900 / 0.859 / 0.952, unflagged 0.787 (d56_base 0.718); E1 0.131 vs base 0.113. Trained for 4 epochs only.
- **Corrected the same day (user challenge):** the mechanism first written here, "a change of phase is not representable", was wrong, and the proposed per-slot status state is withdrawn. The test hid the FALSE label, which training uses, and it used a 12-epoch model.
  - z does receive the tree's own CCLCD and damage, and the flex decoder is free per slot, so fast-then-stalled paths are representable.
  - Label shown vs hidden, P(exists), d58_ep24, VAL: fell out of canopy 0.386 vs 0.731; already suppressed 0.557 vs 0.851; stayed in canopy 0.524 vs 0.853; unflagged 0.169 vs 0.389.
  - More training helps: fell out of canopy, hidden, 0.858 at 12 epochs, 0.731 at 24; the gap to the other flagged groups grows from about 0.06 to 0.12.
  - z responds to crown class in the direction of the stunted scenario: rewriting records to overtopped vs codominant moves P(exists) by a mean absolute 0.12 per tree (p90 0.37), higher when overtopped.
  - VAL matches the trees seen in training (fell out, hidden: 0.731 vs 0.746), so this is not overfitting. Training used 150,000 trees, about an eighth of the train split.
- **Decision:** no structural change. Finish training on the full train split with am. 5–7, then re-measure these groups. Revisit only if they plateau. Watch the learned ρ: 0.007, 0.020, 0.032 over 1, 12 and 24 epochs against a cap of 0.05, because it bounds the FALSE label's cost at about 3 nats per slot.

## D65 — The "re-tally" at MANUAL 1.0 is a microplot move, not a re-census (diagnostic, 2026-11-01; nothing changed yet)
- **Finding:** PLOT.MICROPLOT_LOC goes CENTER → OFFSET at the end of MANUAL 1.0 (guide §2.4: the offset center is 12 ft due east of subplot center). All 28,295 MANUAL 1.0 visits are CENTER; no later one is. Saplings after the move stand on different ground.
- **Coverage:** of the 13,653 detected breaks with saplings on the old list, 13,334 (97.7 %) are moves; 13,737 such visits are moves in total.
  - 403 moves are not in `procedural_breaks` (1,531 saplings).
  - 319 breaks are not moves (2,385 saplings, 80 % continuing).
- **Census at moves:** 100,378 old live saplings: 8,201 continue (5,063 past 5 in, 3,138 still saplings, 3.1 % of the old list), 89,259 closed RC8, 2,406 unrecorded. New microplot list 97,583. Circles of 6.8 ft radius 12 ft apart overlap on 4.8 % of their area.
- **Relinking rejected.** AZIMUTH and DIST are withheld from FIADB (guide §3.1.12–13, available from FIA SDS), so only species, size and height remain.
  - Same-visit test: 60,585 closed trees were measured at the move. A same-species new tree within 0.2 in and 4 ft: 10 % on the own subplot, 4–5 % on the plot's other subplots. The excess of about 6 % is the size of the overlap.
  - Growth candidates: 30 % vs 17–18 %. Spatial autocorrelation of species also inflates the own subplot, so these are upper bounds.
  - Matcher on normal visits with numbers hidden: a unique candidate is right 82 % of the time, where the truth is available. At a move it mostly is not.
  - KY beech: T8 was on the CENTER microplot in 2000, T10 on the OFFSET one in 2010. "Very likely the same tree" is withdrawn. They are probably two trees.
- **Consequences:** both lists are real trees, so per-tree fills are legitimate; the pre-debut FALSE label is unjustified because the spot was not sampled; RC 3 at a move does not mean missed. Keeping both lists at the 75 TPA expansion gives 1.97 × the old sapling count after a move, against 1.05 with one list.
- **Proposed (not applied; needs the user's approval for the E1 truth change):**
  1. Identify moves from MICROPLOT_LOC, not from the break heuristics.
  2. At microplot level, treat the spot of a post-move debut as not sampled before the move (no FALSE label, no cutoff, no soft TRUE from RC 3), for debuts of every code.
  3. Stand and cohort tables of completed data use one list per visit: the list on the microplot sampled at that visit.
  4. Consider the 60,585 closure measurements as real last sizes; curation drops them.
- **Supersedes** the cut-lineages proposal: lineages stay; only the stand-table membership changes.
- **Closure records split by size (same day).** At moves 60,585 RC8 closures carry a size and 28,674 do not. Recorded deaths are only 116, against about 30 % of saplings per normal interval.
  - **With size:** alive. Growth matches ordinary remeasurement (median 0.049 vs 0.050 in/yr). DIA, HT and TPA only; no CR, crown class or agent. None reach 5 in, because those trees stay on the subplot.
  - **Without size:** behaves like death or cut. Size-less share by the plot's timber-cut share at the visit, vs the sapling dead share at normal visits in the same bin: 0 %: 0.191 / 0.210; up to 25 %: 0.333 / 0.351; 25–50 %: 0.482 / 0.503; 50–75 %: 0.634 / 0.667; over 75 %: 0.645 / 0.884. Indirect evidence; the guide does not define it.
  - 1,895 plots (9,222 trees, including GA MANUAL 1.0 → 4.0) have no sized closure at all, so size-less is uninformative there.
- **Proposed placement (not applied):** the move visit is a seam: the old list's last visit and the new list's first.
  - Sized closure: a real last record at the move, then censored. Not a live row in Sim A or Sim B.
  - Size-less closure on a plot with sized closures: a soft "gone" signal with label noise. Never hard.
  - Both together: adding only the sized ones would select survivors and bias mortality low over that interval.
  - Stand tables at the seam use one list, never the sum.

**D65 amendment 1 (2026-11-01; user: link saplings that graduate to subplot-level records).** The relinking rejected above was old list to new list. This is a different link, and it works as a constraint.
- **Why it can work:** the CENTER circle lies inside the 24-ft subplot, and the subplot tally of trees ≥ 5 in continues through the move. A closed sapling that reaches 5 in must reappear as a new number at subplot level.
- **Measured:** 91,665 closed or unrecorded at moves; 88,415 have a later visit. A same-species subplot debut with growth 0–0.6 in/yr and height not lower by more than 5 ft exists for 29,304 (33 %), and for none of 59,111 (67 %).
- **Candidate rate, own subplot vs placebo (mean of the plot's other subplots):** sized 36 % vs 20 %, size-less 27 % vs 19 %. Excess about 11,200 trees, an upper bound because species cluster.
- **Next visit, sized closures:** expected graduates from normal saplings by size class 4,958; excess 4,468. By size it does not line up: excess is too high under 3 in (spatial autocorrelation) and about half of expected at 4 in and over (1,617 of 3,149). The shortfall is unexplained.
- **One-to-one links are not safe:** 3.25 candidates per tree that has any, 2.35 per debut. Mutual-unique pairs: 4,121 (precision ≈ 0.45); at ≥ 4 in 1,572 (0.59); at ≥ 4.5 in 879 (0.63). With numbers hidden on normal visits, 91.5 % of true graduates pass the band and a unique candidate is right 97.7 % of the time, but 59 % of those passing have a rival debut.
- **Proposed (not applied):**
  - No candidate at a later visit: the slot is a below-5-in cutoff, not `ignore`, so the tree stayed under 5 in or died.
  - Candidates: stays `ignore` at first; later a soft mixture over candidates plus no-graduate.
  - No tree is renumbered.
  - Item 2 of D65 keeps the subplot-level 5-in limit before the move; only the microplot-level evidence goes.

**D65 amendment 2 (2026-11-01; user: think harder). Closure records are three different things, and curation has been wrong about one of them since r1.** Every RC8 closure at a move, with PREV_STATUS_CD 1 (89,460; the guide's AGENTCD text covers trees "dead or removed"):

| Closure | Records | Evidence |
|---|---|---|
| Alive | 60,585 | DIA, HT, ACTUALHT, TPA_UNADJ, TREECLCD, DIACHECK; no AGENTCD |
| Dead or cut | 26,445 | AGENTCD and MORTYR; no size |
| Unknown | 2,430 | neither (GA and KY moving to MANUAL 4.0, MS, FL) |

- **Agents:** 80 silvicultural 11,918; 60 vegetation 9,899; 20 disease 1,571; 70 unknown 982; 30 fire 857; 50 weather 842; 40 animal 210; 10 insect 166. MORTYR is 0–10 yr before the visit, median 2.
- **Curation bug:** `CENSOR_REASON` maps every STATUSCD 0 + RC 8 to censoring, so all 26,445 end as `censored_procedural`: no death, no agent, no cut event, no disturbance. That is 21 % on top of the 125,816 dead sapling records in curated_trees.
- **Withdrawn:** "size-less closures behave like deaths" (they record it), the soft-gone proposal, and the claim that a closure means nothing happened to the tree. The 19,450-tree estimate is replaced by these counts.
- **Why this holds together:** the old list was fully re-visited at the move. Live share there 68.4 % (60,585 + 8,085 continuing) vs 69.0 % at normal visits; recorded dead or cut 26.5 % plus 5.3 % unknown vs 29.7 %. Trees ≥ 5 in stayed on the subplot, which is why no closure is ≥ 5 in.
- **Graduation constraint:** applies to the alive and unknown closures only. Alive with no compatible debut: 37,272 of 58,340 with a later visit (64 %).
- **MORTYR (user):** a crew's estimate, not a measurement. Use it only as a flag that the tree died, together with AGENTCD; the death date stays interval-censored in (previous visit, move visit]. Its exact year is never a model input or a time axis.
- **Proposed fix (not applied):** curation turns a closure with AGENTCD into a dead or cut record with the agent (80 → `silvicultural_death`), dated by the visit like any other dead record, and a sized closure into a real last record, then censored. Disturbance, drops and cut events follow; verifier (c1) re-derives it from raw. Needs a chain rerun and a NIMIWAE rebuild.

**D65 amendment 3 (2026-11-01; user: is graduation in moved plots the same as in normal plots?).**
- **Same graduation rates.** Share of live microplot saplings ≥ 5 in, alive, one interval later, under their own number, by start size (< 2 / 2–3 / 3–4 / 4–4.5 / 4.5+ in):
  - normal OFFSET plots: 0.007 / 0.027 / 0.095 / 0.256 / 0.512;
  - CENTER list, previous visit to the move: 0.007 / 0.027 / 0.092 / 0.244 / 0.508;
  - new OFFSET list, move to next visit: 0.007 / 0.025 / 0.104 / 0.255 / 0.497.
  - Observed vs expected from normal plots, standardised on state × wood × size: 0.96 and 0.97.
- **Correction to am. 1:** "about half of expected at 4 in and over" was wrong. That expected count included dead records ≥ 5 in. With live graduates only, using the OFFSET list in the same plots:

| Size at closure | Closures | Expected graduates | Found (chance-corrected) | P(graduated given no candidate) |
|---|---|---|---|---|
| < 2 in | 22,210 | 0.5 % | 0.1 %+ | under 1 % |
| 2–3 in | 16,063 | 2.0 % | all | under 3 % at worst |
| 3–4 in | 9,836 | 8.7 % | all | 0 to about 12 % |
| 4–4.5 in | 3,571 | 24 % | 0.68–0.93 | 3–12 % |
| 4.5+ in | 3,208 | 49 % | 0.67–0.79 | 21–33 % |

  The lower end of each range uses the estimate as measured; the upper end removes the species-clustering uplift seen in the small classes. Found is above 1 under 4 in, which is that same uplift. Widening the filters gains little (0.79 to 0.82 at 4.5+ in).
- **Consequence:** the no-candidate signal is safe under about 4 in and biased above it. Apply it for closures under 4 in (92 % of no-candidate trees at the next visit) as a soft signal with label noise, and keep `ignore` at 4 in and over. Tested at the next visit only; not tested for later visits, where the tree's size is unknown.

**D65 amendment 4 (2026-11-01; user: dead trees ≥ 5 in count unless there is evidence the tree died before the sapling's last live record; a subplot candidate can serve only one sapling). Replaces the numbers of am. 1 and am. 3.**
- **Method:** a graduate is alive, dead or cut at ≥ 5 in. Dead candidates dropped only on evidence: RC 4 (dead at the previous visit) or crew MORTYR more than 1 yr before the closure (1,115 of 8,956 pairs; the margin because MORTYR is an estimate). Closures with an agent are finished and are not sources. One-to-one assignment per subplot: most pairs, then the most probable sources.
- **Rates by start size (< 2 / 2–3 / 3–4 / 4–4.5 / 4.5+ in):** normal 0.007 / 0.027 / 0.096 / 0.286 / 0.657; OFFSET list after the move 0.007 / 0.025 / 0.105 / 0.284 / 0.670 (standardised 0.985). CENTER list before the move 0.007 / 0.027 / 0.093 / 0.245 / 0.516 (0.85): the gap at 4 in and over is dead trees, which close without a size there; live rates match (0.508 vs 0.512).
- **Sized closures with a later visit, 58,340. One-to-one candidates, species and 0–0.6 in/yr:** next visit 7,697 (placebo 3,982; excess 3,715; expected 4,652 with dead, 3,730 live). All later visits 16,565 (placebo 9,147; excess at most 7,418). About 45–48 % of paired candidates are real, so the two-representations scale is about 3,700 at the next visit and at most 7,400 overall.
- **No candidate after assignment:** next visit 50,643 (87 %); all later visits 41,775 (72 %).
- **Hidden graduates among no-candidate trees at the next visit, as measured and after removing the clustering uplift of about 0.03:**

| Size at closure | Any status | Alive only |
|---|---|---|
| Under 3 in | ≤ 2 % | ≤ 2 % |
| 3–4 in | 0 to 12 % | 0 to 12 % |
| 4–4.5 in | 10–14 % | 7–11 % |
| 4.5+ in | 56–62 % | 24–30 % |

- **Dead graduates rarely leave a candidate:** new dead subplot records (RC 1, 3, 7) are about 2 % of the size of live RC 1 debuts, while about 22 % of tracked 4.5+ in graduates are dead. A sapling that crossed 5 in and died is usually never tallied, which fits "stayed under 5 in or died". The signal is broken by hidden live graduates, so the alive column is the one that matters.
- **Unchanged:** apply the no-candidate signal under 4 in only (91.7 % of no-candidate trees at the next visit), soft, and test later visits first.

**D65 amendment 5 (2026-11-01; user: why 4 in, test later visits, check those). A pseudo-move with truth.** On each plot's first OFFSET visit with a later visit (23,664 plots, 167,707 saplings), every microplot sapling is treated as closed and its number hidden. A tree's first live record ≥ 5 in joins the subplot debut pool; dead graduates do not, because they are rarely tallied in a real move. The same filters and one-to-one assignment run through visits +1, +2, +3.
- **"4 in" was chosen by eye** from the am. 3 and am. 4 tables. Bar now: hidden live graduates ≤ 0.05, the cap on the model's label noise (RHO_MAX).
- **P(live graduate | no candidate):**

| Size at closure | +1 | +2 | +3 |
|---|---|---|---|
| < 2 in | 0.005 | 0.004 | 0.006 |
| 2–3 in | 0.008 | 0.009 | 0.013 |
| 3–3.5 in | 0.010 | 0.015 | 0.025 |
| 3.5–4 in | 0.014 | 0.025 | 0.035 |
| 4–4.5 in | 0.027 | 0.045 | 0.063 |
| 4.5+ in | 0.071 | 0.101 | 0.119 |

- **Rule:** under 4 in passes at all three visits. 4–4.5 in passes through +2. 4.5+ in never passes. Keep 4 in as the single limit. Visit +4 is not tested.
- **Caveat:** the pseudo-move assumes every live graduate is re-tallied, so these are lower bounds. The placebo estimates of am. 4 (24–30 % at 4.5+ in) are higher, but species clustering affects them. Real and pseudo candidate shares agree in pattern, real lower by about a quarter (4.5+ in 0.49 vs 0.57) at every size, including sizes with almost no graduates.
- **Matching is worse than the placebo suggested:** matched pairs are right 13 % overall (35–41 % at 4.5+ in), and 31 % of live graduates get their true debut. This replaces the 45–48 % of am. 4. It confirms no renumbering on species, size and height. Any soft link weight must come from these rates.

**D65 amendment 6 (2026-11-01; user: compare 10–15 yr outcomes of microplot saplings, normal plots vs moved plots including silent moves, to test the bias from relinking).** Saplings at the visit A before a move, followed about 12.4 yr. Comparators: never-CENTER plots (63,308 saplings) and the OFFSET list after the move in the same plots (81,070). Moved set 77,444. Percentages of saplings at A, standardised to the moved plots on state × wood × size. Relinked = hide numbers after the next visit and match one-to-one, as at a real move.

| Outcome | Normal | OFFSET after move | Moved |
|---|---|---|---|
| Dead or cut by the next visit | 29.0 | 28.0 | 27.4 |
| Lost track | 0.8 | 0.8 | 3.4 |
| Graduated, true (by number) | 9.9 | 10.3 | not observable |
| Graduated, relinked | 17.1 | 17.5 | 15.6 |
| Not graduated, true | 60.3 | 60.9 | not observable |
| Not graduated, relinked | 53.2 | 53.7 | 53.6 |

- Of the true not-graduated: still a live sapling 37.9 / 37.5; died later 21.8 / 22.8. Moved plots cannot make that split.
- **Graduated by size at A (< 2 / 2–3 / 3–4 / 4–5 in):** normal true 2.1 / 7.4 / 21.5 / 51.4; normal relinked 7.7 / 20.1 / 32.2 / 51.6; moved relinked 7.3 / 16.1 / 26.0 / 47.2.
- **Finding:** relinking inflates graduation by about 7 points (17 vs 10, 3.7× under 2 in, 1.5× at 3–4 in), because false links outnumber misses. Early mortality matches. Moved plots run through the same relink come out 1.5–1.9 points below normal, so the relink bias looks the same in both and could be calibrated by size from normal plots. Moved vs normal is 4–6 points lower at 2–5 in, unexplained.
- **Silent moves:** 566 KY saplings on 118 plots with a 10–15 yr horizon. Relinked from the previous visit 26.9 vs KY + TN normal plots relinked the same way 21.2, true 8.3. Too few to read, and the link spans an extra interval.
- **AZIMUTH / DIST:** in neither FIADB.duckdb nor SQLite_FIADB_ENTIRE.db; FVS_TREEINIT DISTANCE is empty on 2,000,000 rows checked. Scrambling cannot be tested from local data. The guide's fuzzing and swapping covers plot LAT / LON only.

**D65 amendment 7 (2026-11-01; user: more constraints to suppress the graduation inflation).** Tested on the 10–15 yr setup of am. 6, scored against tree numbers.
- **Hard limits barely help.** Graduated, normal plots, true 9.9: base 17.1; size and wood growth limit (p99 of tracked saplings) 15.8; plus height growth limits 15.7; plus under 5 in at the previous visit 15.7. Precision only 0.16 to 0.18. Most debuts are ordinary ingrowth from outside the microplot and pass any plausibility test, because the circle is about 8 % of the subplot.
- **A calibrated link probability works.** Gradient boosting on pair features (sapling size, debut size and height, growth, interval, wood, rival counts), trained on half of the normal plots (1,149 true pairs of 12,770), tested on the other half and on the after-move group.
  - Calibration, predicted vs true, after-move group: 0.02 / 0.02, 0.15 / 0.15, 0.36 / 0.35, 0.63 / 0.65, 0.84 / 0.90.
  - Soft graduated, each pair weighted by its probability and capped one-to-one: after-move 9.1 vs true 10.3; held-out normal 12.2 vs 13.3. It runs low by about 1 point; dead graduates can't be found.
  - Hard links: p ≥ 0.5 precision 0.75–0.77, recall 0.26–0.30; p ≥ 0.7 precision 0.87, recall 0.18.
  - Moved plots: soft graduated 8.2, against the 9 implied in am. 6.
- **Assumption:** training plots have every live graduate re-tallied. Moved plots may not, and cannot be checked there.

**D65 amendment 8 (2026-11-01): the am. 6 table with the closure fix and link probabilities.** Normal plots are now scored out of fold on all plots, replacing the 12.2 vs 13.3 of am. 7, which covered half of them with a different mix. % of saplings at A, standardised to the moved plots.

| Outcome | Normal | After move | Moved |
|---|---|---|---|
| Dead or cut by next visit, curation today | 29.0 | 28.0 | 0.1 |
| Dead or cut by next visit, closure fix | 29.0 | 28.0 | 27.4 |
| Lost track | 0.8 | 0.8 | 3.4 |
| Graduated, true | 9.9 | 10.3 | unknown |
| Graduated, relinked | 17.1 | 17.5 | 15.6 |
| Graduated, link probability | 9.0 | 9.2 | 8.3 |
| Not graduated, true | 60.3 | 60.9 | unknown |
| Not graduated, link probability | 61.2 | 62.0 | 60.9 |

By size (< 2 / 2–3 / 3–4 / 4–5 in), link probability: normal 1.5 / 6.3 / 20.0 / 47.8; after move 1.7 / 6.0 / 19.7 / 47.7; moved 1.5 / 5.2 / 16.5 / 43.9.

**D65 amendment 9 (2026-11-01; user: include silent re-tally in the table).** Silent moves with a 10–15 yr horizon: 906 saplings on 177 plots (883 KY, 15 GA, 8 TN), 9.4 yr to the next visit against 5.8 in normal plots. None keeps its number even at ≥ 5 in, and 899 have no record at all, so death, cut and survival at the next visit are unknown. The closure-based model does not fit: its first pass gave 1.7, because it had learned that first-interval debuts are never graduates. Refit with all numbers hidden from the start, no visit-gap features, on normal plots out of fold (calibration 0.03 / 0.03, 0.15 / 0.15, 0.35 / 0.35, 0.61 / 0.63, 0.83 / 0.90). Standardised to the silent mix by wood × size, graduated: normal KY + TN true 7.8, relinked 20.2, link probability 6.3; silent relinked 24.8, link probability 6.7.

**D65 amendment 10 (2026-11-01; user: exclude dead graduates from recall). Bug found doing this; corrects am. 6–8.** The hidden-number test added the later records of trees already ≥ 5 in at the next visit to the candidate pool, which a real move would not have (2,880 false pool entries in normal plots, 3,408 after the move). Fixed by restricting the pool to hidden saplings. am. 9 and the am. 5 pseudo-move were built differently and are not affected. Moved-plot relinked values have no such pool and do not change.
- **Corrected table, % of saplings at A:** graduated relinked 16.6 / 17.0 / 15.6 (was 17.1 / 17.5 / 15.6); link probability 9.0 / 9.2 / 8.4; not graduated relinked 53.6 / 54.2 / 53.6; link probability 61.2 / 62.0 / 60.8.
- **By size (< 2 / 2–3 / 3–4 / 4–5 in), link probability:** normal 1.6 / 6.4 / 20.1 / 47.5; after move 1.8 / 6.2 / 19.7 / 47.5; moved 1.5 / 5.4 / 16.8 / 44.0.
- **Hard limits:** 16.6 → 15.3 → 15.2 → 15.2 (normal, true 9.9), precision 0.17 to 0.20.
- **Accuracy, out of fold on all plots, recall on graduates alive at 5 in (2,410 normal, 2,994 after the move):**

| Method | Precision normal / after | Recall normal / after |
|---|---|---|
| Relinked | 0.17 / 0.17 | 0.53 / 0.51 |
| p ≥ 0.3 | 0.62 / 0.64 | 0.48 / 0.48 |
| p ≥ 0.5 | 0.76 / 0.80 | 0.35 / 0.35 |
| p ≥ 0.7 | 0.85 / 0.88 | 0.23 / 0.24 |

  Recall at p ≥ 0.5 by size: under 2 in 0.06–0.08, 2–3 in 0.17–0.21, 3–4 in 0.37–0.38, 4–5 in 0.65–0.69.

**D65 amendment 11 (2026-11-01; user: train on normal and after-move plots together).** There was no need to keep the after-move plots out: five-fold cross-fitting by plot still scores every plot out of fold. Training pairs 56,087 (5,276 true). Calibration out of fold, normal / after: 0.15 → 0.15 / 0.15, 0.35 → 0.35 / 0.35, 0.62 → 0.63 / 0.64, 0.85 → 0.89 / 0.89.
- Graduated, link probability: 9.0 / 9.2 / 8.4 (unchanged). By size, moved 1.5 / 5.4 / 16.9 / 44.2.
- Precision normal / after, recall on live graduates: p ≥ 0.3 0.64 / 0.65, 0.49 / 0.48; p ≥ 0.5 0.77 / 0.79, 0.37 / 0.36; p ≥ 0.7 0.87 / 0.87, 0.25 / 0.26.
- Silent (all numbers hidden; 233,876 pairs, 13,282 true): link probability 7.0 vs after-move plots 6.5 (true 8.2) and normal KY + TN 6.3 (true 7.8).
- Keep this pooled model. Moved plots are the same plots as the after-move list, so it is also the closest training population.

**D65 amendment 12 (2026-11-01; user: stop trusting the numbers, recheck the whole issue). `retally_audit.py` plus `docs/RETALLY_AUDIT.json` are now the only source for these figures; they supersede the figures in am. 1–11.** New SQL implementation from PLOT and TREE, tested on hand-built plots with known answers and on 400 random matching cases against brute force. Tree chains break at a reused number (164 records).
- **New bug found:** equal-cost matches were broken by column order, and hidden debuts carry negative ids, so tie-breaking depended on the truth. Precision and recall of the plain relink moved in either direction depending on the order. Ties now use a seeded jitter and a hashed column order.
- **Corrections:**
  - 36,753 visits are CENTER, all MANUAL 1.0; one MANUAL 1.0 visit is not CENTER. "28,295" was only visits with a following visit.
  - 23,823 move visits in all; 13,737 have saplings on the old list.
  - Closures with neither size nor agent among saplings: 2,229, not 2,430, which counted all sizes.
  - Continuing at moves, alive: 5,016 ≥ 5 in, 3,069 saplings; 116 dead. The earlier 5,063 and 3,138 included dead.
  - Normal graduation one visit later, any status: 0.007 / 0.028 / 0.098 / 0.291 / 0.678; alive 0.007 / 0.026 / 0.094 / 0.252 / 0.508.
- **Table, normal / after / moved:** dead 29.0 / 28.0 / 27.4; lost 0.8 / 0.8 / 3.4; graduated true 9.9 / 10.3 / unknown; relinked 16.4 / 16.9 / 15.6; link probability 8.8 / 9.0 / 8.3; not graduated true 60.3 / 60.9 / unknown; relinked 53.8 / 54.3 / 53.6; link probability 61.4 / 62.2 / 60.9.
- **Accuracy, normal / after, precision then recall on live graduates:** relinked 0.19 / 0.18, 0.57 / 0.55; p ≥ 0.3 0.62 / 0.64, 0.44 / 0.43; p ≥ 0.5 0.76 / 0.79, 0.33 / 0.33; p ≥ 0.7 0.87 / 0.87, 0.22 / 0.23.
- **Silent:** relinked 24.8, link probability 6.8; normal KY + TN true 7.8, relinked 19.8, probability 6.0; after-move true 8.2, relinked 21.1, probability 6.2.
- **Reproduced unchanged:** the closure counts (60,585 / 26,445), agents, the no-candidate table within 0.2 points, move and break coverage, early mortality, all true outcomes.
- **Retired, not re-derived:** the placebo estimates of am. 1, 3, 4 (own subplot vs other subplots). The 24–30 % upper range for hidden graduates at 4.5+ in rests on those and should not be quoted. The curated-fate counts (all but about 10 of 26,445 censored; 125,816 dead sapling records) come from one query and were not rewritten.

**D65 amendment 13 (2026-11-01; user: did the link model condition on NIMIWAE's tree inputs?).** It did not. Added crown class and crown ratio at A and at the debut, damage at the debut, species, subplot basal area and stems, growth of the subplot's ≥ 5 in trees, stand origin, site class, forest type, stand age, BALIVE, slope, disturbance and treatment codes, L3. Not added: climate anomalies, aspect, physiographic class, sapling neighbour growth. Hidden trees contribute nothing after A.
- **Average precision, normal / after:** base 0.607 / 0.600; all inputs 0.626 / 0.615; context only 0.626 / 0.612; tree only 0.613 / 0.606.
- **Links, all inputs vs base:** p ≥ 0.7 precision 0.88 / 0.88 vs 0.87 / 0.87, recall 0.23 / 0.24 vs 0.22 / 0.23. Graduated unchanged (8.8 / 9.1 vs 8.8 / 9.0); moved 8.3.
- **Reading:** a small real gain, mostly from context. Use the full inputs. The remaining uncertainty is identity: most debuts come from outside the microplot and look like graduates.
- **Determinism:** rows are now sorted before matching and fitting; two full runs give byte-identical JSON. Before this, link accuracy moved by up to about 1 point between runs because SQL row order varied. am. 12 figures are replaced by `docs/RETALLY_AUDIT.json`: graduated relinked 16.5 / 16.9 / 15.6; not graduated relinked 53.7 / 54.3 / 53.6; accuracy p ≥ 0.3 0.62 / 0.64, 0.44 / 0.44; p ≥ 0.5 0.76 / 0.78, 0.33 / 0.33; p ≥ 0.7 0.87 / 0.87, 0.22 / 0.23.

## D66 — Natural ids and no row-order logic in the curation (user, 2026-11-01)
- **Request:** use natural unique ids, not CNs and not local ids, and do not rely on sorting.
- **Rules:**
  - One selection helper, `fia_ids.pick(df, key, by, which)`: the row of each key with the smallest or largest `by`. A tie is an error, never a coin flip.
  - Windows state their order: `.over(key, order_by=...)`. A frame's `.sort` is for storage and display only.
  - Ids are never positions: `cohort_id` is the cohort's natural key as text (was a row index offset per state).
  - CN appears once for plot chains, as FIA's claim about the previous visit, then chains run on natural visit ids. SUBP_COND_CHNG_MTRX and the quarantine check stay at the boundary. `curated_trees.CN` stays as an attribute.
  - Age-model order and draws use the natural record id, not CN. That order stays because binning and bootstraps in the fit are order-sensitive; it is an id order, not row luck.
  - NIMIWAE prep joins climate on (tree, MEASDATE), not a row number; area-dominant classes break ties by value.
- **Found and fixed:**
  - a cross-visit shift over CONDID in a diagnostic of `fia_curation_v3` that the lint missed (removed);
  - `cohort_id` as a row index;
  - `unique(keep=...)` with no order in `build_tree_trajectories`, `cohort_disturbance`, `cohorts_landis2`;
  - last-digit float differences in `curated_cohorts` and `curated_snapshots` from summation order, now rounded to 6 decimals.
- **Tests:**
  - `fia_truth_lint.py` gains L4 (no `keep=`, no unordered shift or cumulative window, no bare `.first()` in an aggregate, no row index) and catches `cond_id_cols`.
  - `fia_order_test.py`: steps 1–10 on a county, plain vs loaded trees shuffled and every table rewritten in random order after each step. GA 13:87 and KY 21:175: all 40 tables identical.
- **Consequence:** age-model bootstrap draws change with the id, so model ages move slightly. Full R4 rebuild goes to a separate folder and through the verifier before it replaces the truth DB.

**D66 amendment 1 (2026-11-01): full R4 rebuild with the revision, in `FIA/truth/r4_ids`; the live truth DB is unchanged.**
- **Verifier:** 213 passed, 0 failed, same 4 KNOWN cells within the drift guard ((j) FL 15+ −5.0, KY 5–10 −4.5). Full-region determinism was not rerun.
- **Against the current `FIA/truth/r4`:** curated_trees has the same 4,496,120 records and keys, same columns. Only age columns change, plus 12 last-bit floats in `subp_forest_subpcond_prop` (1e-16), a float sum still to round. Live model ages move 0.48 yr on average (−0.12 signed, 5.6 at most), from the new draws. Sim A rows −4,712 (−0.12 %), trajectories −4,688, Sim B −36, cohort_disturbance −6. Plot renumbering and remaps are identical.
- **`docs/RETALLY_AUDIT.json` final:** shuffled and plain runs byte-identical, with tree ids and no sorting. Relinked 16.4 / 16.9 / 15.6; probability 8.8 / 9.0 / 8.3. Accuracy normal / after: relinked 0.20 / 0.19, recall 0.60 / 0.58; p ≥ 0.3 0.63 / 0.64, 0.44 / 0.44; p ≥ 0.5 0.76 / 0.78, 0.33 / 0.33; p ≥ 0.7 0.87 / 0.86, 0.22 / 0.23. All inputs: average precision 0.629 / 0.620, p ≥ 0.7 0.88 / 0.88, recall 0.24 / 0.25. Silent unchanged. These replace the accuracy figures of D65 am. 12–13.
- **Next:** the closure fix (D65) needs another rebuild, so the live DB is replaced once, with both.

## D67 — CNs out of the curation: measurements (2026-11-01; applied, am. 2)
- **Previous visit:** natural (same plot key, previous annual visit) vs PREV_PLT_CN, 209,100 annual visits: 209,064 agree (155,413 same, 53,651 none on both sides); 33 link only (32 key changes), 2 differ (key changes), 1 natural only. All visits, periodic included: 343 differ, 4,062 link only, 3,611 natural only; plot numbers were reassigned at the periodic-to-annual change (guide §1.2.2), so natural matching across that boundary is not safe. Curation is annual only.
- **Previous status:** natural previous record's STATUSCD vs PREV_STATUS_CD, 3,659,053 records with a previous visit: 2,895,129 comparable, 0 disagree. 31,410 carry PREV_STATUS_CD with no previous record (new-number records at moves), 5 have a previous record and no PREV_STATUS_CD, 732,509 new trees. 47 Georgia records have a PREV_TRE_CN to another record of the same number at an older visit; status still agrees.
- **SUBP_COND_CHNG_MTRX:** used only to find condition resets, which set STDAGE eligibility and propagate to the age fit through the calibration rows. Steps 1–2 on GA 13:87, KY 21:175, TN 47:93 (11,847 live records):
  - no links at all: 30 of 56 resets, 296 records change eligibility; final ages change only in GA (mean 0.42 yr, max 11);
  - TREE.PREVCOND links: 52 of 56 resets, no new ones, 31 records change eligibility; final ages identical in all three.
- **Proposed:** natural previous visit and previous status; key changes found from tree evidence (27 of 29 accepted links, no false match); condition links from TREE.PREVCOND; CNs kept only for TreeMap and for TreeAge at load, and in the verifier as an independent check. Needs a full chain and verifier.

## D68 — INVYR is an id, not a time; annual dates (user, 2026-11-01)
- **Rule:** INVYR identifies a visit; ordering and intervals use MEASYEAR, MEASMON, MEASDAY only. Lint L5 rejects ordering or date comparison on INVYR.
- **Sites fixed:** `lineage_break_visits` and verifier n19 ordered by (MEASYEAR, INVYR); the retally detector, verifier and audit used INVYR as a date tie-break; the verifier's era tables binned on INVYR; plot location used it as a tie-break.
- **Effect:** none on annual ordering in R4: 155,416 consecutive annual pairs, 0 with date and INVYR order reversed, 0 on the same date, 42 in the same calendar year. MEASYEAR differs from INVYR on 80,117 of 209,100 annual visits. County row-order test still identical; the verifier's changed lines have not been rerun.
- **Dates (R4):** annual 209,100 visits: none lacks a year, month or day. Periodic 167,637: 7,507 lack year and month (4.5 %), 165,886 lack the day (99 %). All states: annual 92 of 1,256,461 lack a date. The guide says these fields "may be blank (null) for periodic inventory"; it does not rank their accuracy.
- **D67 caveat:** its all-visit comparison ordered by (MEASYEAR, INVYR) and covers periodic visits without dates, so those counts are rough. Annual counts stand. Its 27-of-29 tree search compared INVYR for "earlier"; to be redone with dates when implemented.

**D68 amendment 1 (user, 2026-11-01):** INVYR has two uses: part of the natural id, and ordering a plot's records chronologically. It is not used for modelling; any time a model sees comes from MEASYEAR, MEASMON, MEASDAY. Lint L5 now rejects time arithmetic on INVYR and allows ordering. The date-based orderings from D68 stay; the two orders agree on all 155,416 consecutive annual pairs in R4. INVYR ordering is the option for periodic records with no date.

**D67 amendment 1 (2026-11-01): rerun in `cn_audit.py` with visits ordered by INVYR (D68 am. 1); results in `docs/CN_AUDIT.json`.** R4 annual counts are unchanged. R4 all visits are replaced: 376,737 visits, 373,411 agree, 3 differ, 1,887 link only (1,751 periodic key changes), 1,436 natural only (1,338 across the periodic-to-annual change). All states, annual (1,221,386): 1,183,299 agree, 28 differ, 3,625 natural only, 34,434 link only, almost all a first annual visit pointing to a periodic one, which annual-only scope excludes. Previous status, all states: 10,262,768 agree, 124 disagree. Renumbered: R4 27 of 34 links, all states 34 of 45, no false match in either. Conditions: R4 124,288 of 212,549, dominant 120,275 agree (96.8 %); all states 410,429 of 985,921, 398,296 agree (97.0 %).

## D69 — Measurement dates are never ids (user, 2026-11-01; applied)
- **Rule:** stay with FIA's semantics. Ids are ids: a visit is (STATECD, UNITCD, COUNTYCD, PLOT, INVYR), a subplot adds SUBP, a condition adds CONDID, a tree adds SUBP and TREE. MEASYEAR, MEASMON, MEASDAY are the timestamp: ordering, intervals and modelling only, never a key.
- **Current state:** the curation breaks this. About 100 sites key on MEASDATE (joins, groups, partitions, unique subsets), heaviest in `build_tree_trajectories`, `fia_curation_v3`, `cohort_tracking`, `cohorts_landis`, `nimiwae_prep`. The count is a text search and undercounts multi-line keys. Ordering uses (`order_by="MEASDATE"`, `pick(..., ["MEASDATE"])`) are correct and stay. This text's "CLAUDE.md" note that MEASDATE is unique per visit describes the same habit.
- **Tables without INVYR:** `curated_cohorts`, `curated_cohorts_landis` (and `_stdorg`), `_simB`, `tree_trajectories`, `cohort_disturbance`, `procedural_breaks`, `cond_link`. Each gains INVYR as an appended column; phantom slots also need the prior visit's INVYR. `retally_audit.py` joins silent moves on a date for the same reason.
- **Expected effect on data:** none. (plot, MEASDATE) is unique across all 205,977 R4 annual visits in the truth DB, so dates and INVYR map one-to-one. Test: tables EXCEPT-identical to the current build apart from the appended columns.
- **Plan:** one pass with D65 and D67, ids first so that step can be checked as a no-change refactor, then the data changes; lint L6 rejects a date in a key.

**D69 amendment 1 (2026-11-01): applied, stage by stage.** Every stage, `nimiwae_prep`, `nimiwae56`, `retally_audit` and the verifier key visits by INVYR; dates stay for ordering, intervals, interpolation and modelling.
- **New columns:** INVYR on `curated_cohorts`, `procedural_breaks`, `tree_censor_events`; INVYR and PREV_INVYR on `cond_link`; `invyr` as the last column of `curated_cohorts_landis`, `_stdorg`, `_simB`, `tree_trajectories`, `cohort_disturbance`; `prior_invyr` on both phantom source tables; `invyr` and `dead_invyr` in the NIMIWAE frame. `disturbance_events` carries `event_invyr`.
- **Result of the no-change test, GA 13:87 and KY 21:175, steps 1-10:** all 40 baseline tables identical on the baseline columns. NIMIWAE `seq.parquet` and `bench.parquet` identical from old and new code on both counties (scheme r3, with climate). Verifier on GA: 151 pass, 1 fail, same as before the conversion except one label; the fail is the planted monotonicity check, which has no rows at county scope.
- **`natural_record_id` moved from date to INVYR, last and on its own.** `row_draws` hashes this id, so the bootstrap draws change and every model-imputed age moves. No record, age source or estage age changes. GA: 3,242 of 7,452 records change, mean difference -0.004 yr, median 0, 99th percentile 2.1, max 4.1. KY: 1,129 of 4,319, mean +0.024, median 0, 99th percentile 1.1, max 5.4. This is a new resampling, not a correction, and the baselines were refrozen after it. (Corrected below: the regional fit moved more, not less.)
- **Also changed:** `cohorts_landis2.py` lost its full build, which refused to run and is in git history, so the script only attaches. `add_climate` in `nimiwae_prep` mixed aggregates with columns under GROUP BY ALL, which fails on duckdb 1.5.4; the rewrite gives the same values on both counties. `nimiwae_prep` reads DESIGNCD by (plot, INVYR) with no aggregate.
- **New guards:** lint L6 rejects a date in a key across the pipeline, `nimiwae56` and the verifier (`date-ok` marks a nearest-in-time join or a date carried beside INVYR). The verifier checks one date per visit, dates following INVYR, and tree x invyr unique in `tree_trajectories`.
- **Left as dates on purpose:** the verifier compares its re-derived event dates with pipeline dates as values. `e.ev_md = m.nmd` is one such equality, and `age_imputation` compares record dates in time windows.
- **Not yet run:** `nimiwae56_verify` needs a `seq.parquet` built from the rebuilt DB, so its edits (`stand_totals` default key, the two merges, `dead_invyr`) are untested until then. `retally_audit.py` on R4 needs `procedural_breaks.INVYR`, so its join change has passed the fixtures only; the full run comes with the rebuild and must reproduce `docs/RETALLY_AUDIT.json`.

**D67 amendment 2 (2026-11-01): applied.** No pipeline stage reads PREV_PLT_CN, PREV_TRE_CN or SUBP_COND_CHNG_MTRX; lint L7 enforces it. `curated_trees.CN` and `product_visits.PLT_CN` stay as lookup attributes for TreeMap and for the verifier, and no logic uses them.
- **Plot renumbering:** `dbconn.build_plot_canon_map` finds key changes from tree records. A visit is an orphan when fewer than 90 % of its trees match its own key's previous visit of any type (at least 2 trees). It links to an earlier visit of another key with the same SUBP and TREE and PREVDIA within 0.05 in of DIA on at least 2 trees and 90 % of the orphan's trees, in the same state, earlier by date and INVYR, and after the orphan key's own previous annual visit. Ties are left unlinked.
- **Rule added during the test:** the first version found all FIA links plus 5 nationally (1 in R4), each on 2 of 2 trees and each a pair of keys still visited in alternation afterwards. A plot is in one key at a time, so a candidate's key may not be visited again before the orphan's key last is. This rule uses no tree count. After it, 36 links nationally, all of them FIA links; the 9 FIA links not found are the 9 that already failed the tree evidence, each with 0 pairs. R4: 29 of 34, as before, now including GA 13/2/87 146 -> 45 -> 146, which the first audit missed. Search time 4 s nationally. `plot_key_remap` is identical on its keys, with reason `tree_evidence`.
- **Conditions:** `cond_link` is built from TREE.PREVCOND with `prop` = remeasured trees on the link, on natural previous visits. A condition with no remeasured tree has no link, so rows fall from 481 to 236 (GA) and 206 to 137 (KY). 235 of 236 and 137 of 137 rows are also in FIA's matrix.
- **Effect:** GA 13:87 and KY 21:175, steps 1-10: no record, `estimated_age` or `age_calc` changes, and Sim B is identical. Reset flags and STDAGE eligibility move: `stdage_eligible` on 28 and 3 records, `age_from_stdage` on 39 and 3, `is_reset` on 3 and 12. `tree_trajectories.age_stdage` changes on 58 and 8 rows. Verifier: same results before and after on KY; GA 151 pass; the remaining fails are county-scope (no planted rows, TreeAge mapping needs a state fit).
- **Changed from the proposal:** PREV_STATUS_CD stays. It is an FIA field, not a CN, it agrees with the natural previous status on all comparable records, and it also carries the 31,410 records with no previous record at moves. `prev_tre_cn` is dropped from `curated_trees`; the verifier reaches it through FIA's TREE.
- **Verifier as the independent check:** n6 compares the natural result with FIA's PREV_PLT_CN chain under its own evidence test, and now reports `cond_link` against the matrix.
- **Not yet applied:** TreeAge still loads by CN (allowed). `fia_truth_assemble` still converts raw PREV_PLT_CN to natural ids for the `raw` schema (allowed, cn-boundary).

**D65 amendment 14 (2026-11-01): closure fix and the move rule applied (am. 2, items 1 and 4 of the first proposal).**
- **Closures with an agent:** an RC8 closure of a tree alive at the previous visit, with AGENTCD, is now a dead or cut record in `curated_trees` (AGENTCD 80 is STATUSCD 3, the rest 2; `closure_dead`). It has no size. It is no longer a censoring event, so fate, death cause, cut events and disturbance follow from the normal paths. The date is the visit, like any dead record; MORTYR is not read.
- **Sized closures:** the tree stays censored, and `tree_status0_events` keeps the size (`closure_kind` = `sized`, DIA, HT, ACTUALHT, TPA_UNADJ, TREECLCD). It is deliberately not a row in `curated_trees`: that keeps it out of every live total, so Sim A and Sim B stay one list per visit. The NIMIWAE frame carries it as `s0_dia`, `s0_ht`, `s0_kind`.
- **Unknown closures:** censored, `closure_kind` = `unknown`.
- **Moves:** `procedural_breaks.is_microplot_move` is read from MICROPLOT_LOC, CENTER at the previous sampled annual visit and OFFSET at this one. (Sampled visits only: 113 plots moved during a nonsampled visit, and the first regional build flagged those on the nonsampled visit, which has no curated row.) A move that the closure and unrecorded detectors missed is a new row of kind `move`. Unrecorded saplings at those visits are censored, and phantoms do not cross them. The two detectors stay for breaks that are not moves.
- **Stand tables:** one list per visit needs no new code, because closures are not live rows and phantoms do not cross a move. Verifier checks hold.
- **Effect, GA 13:87 / KY 21:175, steps 1-10:** no live record, DIA or `estimated_age` changes. Closure dead records 68 (29 cut) / 8 (4 cut). `censored_procedural` trees 164 to 96 / 27 to 19, and dead plus cut rise by the same count. Breaks 23 to 56 / 11 to 18, all new ones moves. Tree phantoms 444 to 417 / 368 to 351. Sim B rows identical; `procedural_break` changes on 81 / 76 rows, and `subp_has_dstrb` on 10 rows in GA. Drops: 645 and 152 rows before and after.
- **Verifier:** re-derives closures and moves from raw (`rm`, CLOSURE_RAW) in n17, n18, n20, n21, n23. GA and KY give the same results as before the change. Only the county-scope checks fail: no planted rows in GA and KY; KY needs a state TreeAge fit.
- **Not applied here, belongs to the NIMIWAE retrain:** the no-candidate signal under 4 in and the link weights of am. 1, 4 and 13. They are frame inputs, and the sampled draws are untested. E1 (no FALSE labels on the spot before a move, item 2) stays the user's decision.

## D70 — NIMIWAE predicts age jointly with measured ages (user, 2026-11-01)
- **Design (user):** NIMIWAE and age modelling are not separate; they constrain each other. Each tally tree has one birth date, so its predicted age advances exactly with elapsed time. A tree predicted 12 yr old at a visit cannot be established more than 12 yr back, which bounds the E1 slots. Soil is an input.
- **Age targets (user):** source measurements only: SITETREE.AGEDIA, TOTAGE, BHAGE. STDAGE and estimates are never targets or inputs.
- **Data (user):** RSCD 22 and 26 tally ages are used for this model. R4 curation and products stay R4-only; those records are loaded for the model and are not curated into a truth DB.
- **Site trees tied to tally records (user):** usable as an extra anchor if the link is reliable. `age_anchor_audit.py` measures it. **Bar, set before the first run:** a tier needs estimated precision >= 0.95 by the DIA-shift placebo, and dominant or codominant share well above placebo. Ages are never used to choose or score a link.

**D70 amendment 1 (2026-11-01): `age_anchor_audit.py`, `docs/AGE_ANCHOR_AUDIT.json`.** R4 annual, 78,366 site-tree rows that are not carried forward.
- **Links:** by estimated precision, DIA only 0.826 (28,014); + SUBP 0.944 (9,876); + height within 5 ft 0.888 (26,639); both 0.959 (9,776). Added after the first run: exact height 0.984 (24,718); SUBP and exact height 0.994 (9,565).
- **The bar is not met as written.** The crown-class clause fails on every tier: dominant or codominant share is 97-98 % on real links and 96-98 % on placebo links, because any tally tree within 0.5 in of a site tree's size is already large. The clause does not discriminate. It is not dropped or replaced without the user; no link is used yet.
- **Exact height, a diagnostic added after the first run:** among DIA-only links 88.2% have the site tree's height exactly, against 8.0% of placebo links.
- **Coverage, RSCD 22 and 26, annual, live:** TOTAGE 127,683 records, of which 36,230 are 1-3 in, 6,979 are 3-5 in, 9,141 overtopped. BHAGE 521,650, of which 20,283 are 1-3 in, 25,084 are 3-5 in, 39,666 overtopped. No age under 1 in.
- **Repeat ages, RSCD 22 only:** TOTAGE 47,386 pairs, 96.1 % step within 1 yr of REMPER; BHAGE 77,153 pairs, 96.4 %. Median step and REMPER 10 yr. Under 1 % go backwards. Some of these may be computed by adding the interval rather than re-cored; the audit cannot tell.

**D70 amendment 12 (2026-11-02): age comes from a direct model; the joint model carries it as a true latent.** Replaces the growth curve, vigour and record likelihood of am. 2-11, kept in `nimiwae70_curve.py`.
- **Why:** d70b (400k trees, 12 epochs) scored G1 MAE 18.8 / bias +7.2 yr, with a tail to 754 yr and slow growth read as age (r -0.72). Inverting a size curve cannot tell slow growth from youth.
- **Gate, set before any joint work:** if a direct model cannot approach the current ages' G1 MAE of 9.4 yr, stop before GPU time. `nimiwae_age_direct.py`, TRAIN records only, scored on VAL. Current ages 9.40 / +0.18 (fitted partly in sample). Chapman-Richards 17.1 / +0.06. `f` 9.30 / -0.63, coverage 0.808. Gate passed. `f+g` 9.13 / -1.17 is not used: growth needs a later visit, so it could read a hidden record, and only western tally records train it.
- **Double check, gate blind spot:** the gate scored R4 only on site trees, all >= 5 in. Only 2.4 % of R4 sapling records are species seen under 5 in in training, and R4 temperature (12.0-21.9 C) sits mostly above the western range (1.0-13.7 C), so saplings are extrapolated. Planted stands' field age is the only outside check: a diagnostic, never a target or input. On VAL `f` ran +7.2 yr at 1-3 in and +7.7 at 3-5 in, coverage 0.15 and 0.20.
- **Adopted `f+p`: `f` plus stand origin**, `STDORGCD = 1` from the record's own condition at that visit, a field observation and not an age estimate. **G1 8.88 / -0.71, coverage 0.824**; G2 site -0.01, BHAGE +0.49, TOTAGE -0.54, coverage 0.825-0.826; crown -0.7 to +1.8; G3 +0.01; largest prediction 112 yr. Planted field-age bias by DIA class, 1-3, 3-5, 5-7, 7-9, 9-15, 15+ in: +3.2, +3.3, +2.5, +1.3, +1.2, +6.8 yr; coverage 0.38, 0.41, 0.53, 0.65, 0.79, 0.76.
- **Known limit, not fixed:** planted R4 saplings stay about 3 yr old against 7-10 yr field ages, with intervals too narrow, and natural R4 saplings have no check at all. Widening sigma from field age would use STDAGE as a target, so it is not done. `transfer_current` in the report is near zero by construction, because the current ages were trained on STDAGE: it is no independent reference. `nimiwae70_eval.py` reports `planted_field` for the joint ages on every run. Pre-registered: this is a reported diagnostic, not a bar, until the user sets one.
- **Model:** `log a_l = af_mu + af_sd z0`, `z0 ~ N(0, 1)` by fixing that prior dimension, at the tree's last visible live record. The unborn term uses the same soft step as the gate, so it reaches z0 (the hard mask gave z0 no gradient). `af_mu`, `af_sd` are per-slot columns of `seq.parquet`, fixed, on the total-age scale. Train plots use the fold that left them out, so a frame tree never sees its own age record. Exact steps, born gate, unborn term, E1 mask and soil statics stay.
- **Options rejected:** a penalty on `log a` from a head on z. It maximises at `a = af_mu`, so the model could collapse age to a point and intervals would be false (found while wiring it; the same reason vigour needed a standardised z0, am. 8). A separate `bhead` and any learned scale.
- **The decoder never reads z0**, so size paths cannot trade growth against age. They meet only through the born gate and the unborn term. The encoder reads `af_mu`, `af_sd` so it knows how close a tree sits to its birth limits. Age off reads neither.
- **Bars:** thresholds of am. 4 unchanged. G1, G4, G5, G6, G8 score lineages. G2 and G3 use western tally records, which have no lineage in the R4 frame, so they stay at record level on `f`: a change of where they are measured, stated here before the run. Pre-registered for this run: G1 MAE <= 9.4 on the joint ages; the joint must not be worse than `f+p` alone on G1.
- **Known limit:** measured ages reach the joint model only through `f`. Site-tree links are not used as anchors; that decision stays open. Ages at the frame's site trees therefore cannot beat what `f+p` and the birth limits give.
- **Verification:** `nimiwae70_verify.py` 34 / 34, three on the real frame (prior slot is the last visible live record, no hidden slot feeds it, age steps equal dates to 0.0000 yr). 3,729,318 of 3,729,318 real slots have a prior; median age 29.0 yr, median sd 0.282. Two identical site-tree rows naming different conditions keep one row, origin unknown (1,821 rows): `age_rec` keeps its 617,782 rows, no row changed.
- **Run:** Adam with OneCycle, as d70b, so the comparison changes one thing. Schedule-Free stays a separate test.

**D70 amendment 13 (2026-11-02): the am. 12 run; the prior moves to the first record.** `d70p`, 400k trees, 12 epochs, Adam, VAL, `d70p_report.json`, `nimiwae70_prior_read.py`.
| Bar | Threshold | d70p | Result |
|---|---|---|---|
| G1 linked trees | MAE <= 9.4; bias <= 2; not worse than `f+p` 8.88 | 11.80; -3.28 | **fail** |
| G4 seeds | <= 0.2 yr | 0.135 | pass |
| G5 coverage | 0.75-0.85 | 0.709 | **fail** |
| G6 size, E2, E3 | size <= 5 % worse; Brier <= 0.005 worse | 7.5 % better; +0.0002; +0.0003 | pass |
| G8 E1 | >= 0.005 better | 0.0126 better (0.0438 vs 0.0564) | pass |
- **Cause of G1, measured with no lineage model:** `f` is not time-consistent. Its age rises 0.52 yr per year across VAL trees (0.64 on the G1 trees). Reading it at the last record and subtracting elapsed time gives 11.35 / -3.24 on its own; the joint gives 11.81 / -3.09 on the same scoring, so the joint added 0.2 yr and did not cause it. This was a wiring error of am. 12, not a failure of the birth limits.
- **Readings, G1 trees:** current 9.40 / +0.18; at the scored record 8.88 / -0.71; last 11.35 / -3.24; mean of implied births 9.21 / -1.25; **first 8.24 / +0.25**.
- **Scored record left out**, so a reading cannot win by being the scored record: from an earlier record 8.53 / +1.23 (bias +0.88 at gaps to 7 yr, +2.02 past 12 yr); from a later record 12.12 / -5.36 (-2.41, then -10.17); mean of the others 9.74 / -1.64.
- **Decision: read the prior at the first visible record.** The bias stays small over long gaps. `mean_birth` is rejected: it mixes in the backward reading and fails the 8.88 clause. A pace correction to `f` is rejected: it adds a fitted step without evidence the first record needs one.
- **Consequence:** no visible record can fall before birth, so the born gate acts only on pre-debut slots, as the user's bound asks.
- **Costs, stated before the run:** chosen on VAL, so TEST stays untouched for the final check. Planted field-age bias worsens at mid sizes: 1-3, 3-5, 5-7, 7-9, 9-15, 15+ in go +2.6, +2.4, +1.3, +0.7, +1.6, +6.5 at the last record to +3.6, +3.9, +2.9, +2.9, +2.8, +4.2 at the first. Prior coverage on G1 trees is 0.777, and d70p lost 0.08 between prior and joint, so G5 may fail again.
- **Bars unchanged.** The next run is `d70r`, not `d70q`; see below.
- **Undertraining (user asked), and the G6 pass was overstated.** All 12-epoch runs end with the loss still falling and ESS near 7 of 32. Two age-off runs with the same settings gave size MAE 0.161 (d70b) and 0.178 (d70p), a 10 % gap, so the 7.5 % G6 gain cannot be read as an effect of age. Age-on seeds agree more closely: 0.164 and 0.161, E1 0.0436 and 0.0405. Evidence on the age-off side is one pair only.
- **Accidental cross-check, `d70p_trained_last_read_first.json`:** the d70p models scored after the change, so trained with the last-record prior and read with the first. Not a valid run, because the encoder never saw the first-record prior; a preview only. It shows G1 near the first-record reading, and G5 0.704 against 0.777 for the prior alone, so the joint still narrows intervals. Low ESS would do that: few effective draws give narrow weighted quantiles. That is a hypothesis, tested by the longer run.
- **Decision: `d70r`, 24 epochs, with a second age-off seed**, so G6 and G8 are read against seed spread on both arms. G1 is not expected to change much, because it is set by the prior. Epochs change together with the read-out, which is acceptable: `nimiwae70_prior_read.py` already isolates the read-out without training. Options rejected: 12 epochs (G5 and G6 stay unreadable), more K before more epochs (costlier and untested here).

**D70 amendment 14 (2026-11-03): `d70q` and the exact age posterior.** `d70q`, 400k trees, 12 epochs, Adam, VAL, first-record prior.
| Bar | Threshold | d70q, importance draws | d70q, exact posterior | Result |
|---|---|---|---|---|
| G1 | MAE <= 9.4; bias <= 2; not worse than `f+p` 8.88 | 8.76; +0.34 | 8.25; +0.25 | pass |
| G4 | <= 0.2 yr | 0.067 | 0.0001 | pass |
| G5 | 0.75-0.85 | 0.706 | 0.772 | pass |
| G6 | size <= 5 % worse; Brier <= 0.005 worse | 3.0 % better; +0.0002; +0.0002 | unchanged | pass |
| G8 | >= 0.005 better | 0.0126 better | unchanged | pass |
- **Cause of the G5 failure, measured:** on the 3,711 G1 trees with no slot before the first record, neither limit can act and the decoder never reads z0, so the exact posterior is the prior. Importance draws moved them by 0.24 sd on average with correlation 0.000 to the true error, coverage 0.806 to 0.748, MAE 8.62 to 8.99. Sampling noise, so not a training or model problem at the age level.
- **Fix: `age_posterior`, a grid on z0 with the rest of the model scored once.** No retraining. Those 3,711 trees now equal the prior exactly. Bars unchanged; only the estimator of the reported age changed, for a measured reason, and it is the exact answer of the same model. The am. 13 hypothesis that ESS drove G5 holds; the remedy differs.
- **What the exact posterior also shows:** the birth limits barely move age. On the 478 G1 trees with earlier slots the median moves 0.03 sd. So joint ages equal the direct model's on these trees, and the gain from D70 is E1 and existence, not age accuracy. G4 passes trivially for the same reason. Those 478 trees have prior coverage of only 0.55, unexplained.
- **Open:** `sample_lineages` and `impute` still use importance draws, so cohort products and E1 carry the noise in age. They need the grid before the ages reach curation. Planted saplings are unchanged: bias +3.6, +4.0; coverage 0.34, 0.33.

**D70 amendment 15 (2026-11-03): cross-validation replaces the 24-epoch run (user).** `nimiwae70_cv.py`, `nimiwae70_score.py`, `nimiwae70_refs.py`. The user pointed out that small deviations are expected after short training and worried about overtraining from a longer run.
- **Design:** 5 folds by plot on TRAIN only, the D54 crc32 rule. VAL carried design choices and is excluded, TEST stays untouched. 10 % of the fitting plots are a monitor, never trained on or scored. Age on and off per fold, 400k trees.
- **Schedule-Free AdamW, 36 epochs, scored at 12, 24, 36.** Averaged weights can be read at any epoch, so epoch 12 of a 36-epoch run is a real model and no decay schedule is tied to length. Replaces separate runs per length. Adam with OneCycle rejected for this: its mid-run weights are not a finished model.
- **Overtraining:** `curve.json` holds the bound on fitted against monitor trees every 2 epochs. It is read directly, not inferred from one end point.
- **Prior on held-out plots:** train plots use the 2-fold out-of-fold `f+p`, so a plot's own age records never set its prior. That model saw half the records, so scores may run slightly worse than the full model's. The bundle reads refs from parquet and needs no database.
- **Bars:** am. 4 thresholds, pooled over folds. The "not worse than `f+p`" clause reads `f+p` at the scored record, as registered. G4 is not rerun: it needs two seeds, and with the exact posterior it passes trivially.

**D70 amendment 16 (2026-11-03): semantics review, four fixes (user asked for a code read, then "fix all").**
- **Breast height (changes the model).** A tally tree's DIA is measured at 4.5 ft, and the age targets define total age as breast-height age + l0 (2 yr softwood, 5 yr hardwood). The gate allowed tally size from germination on. It now needs `a_t - l0 >= 0`: P(exists) = P(size step) * sigma(KAPPA_B (a_t - l0)), and `born` in sampled lineages means past breast height. l0 per tree from `sftwd_hrdwd`, the same values as the targets. Expected effect: the limits bind 2-5 yr earlier, so they may move age more than the 0.03 sd of am. 14.
- **Short-tree term is a mixture (changes the model).** Was P(unborn) * log P(below level), a geometric blend. Now log[P(at BH) + P(not) * P(below level)], the likelihood of the two cases. The two agree outside the half-year step.
- **No-prior fallback** anchored at the last visible record, the reading that failed in d70p. Now the first. No effect on this frame: every tree has a prior.
- **E1 mask** took a tree's debut from hidden records too, so a hidden record could decide which labels drop in a benchmark. Now visible records only. No effect on training frames, which have no hidden records.
- **Runs:** the two model changes act only when age is on. Age off is unchanged, so its am. 15 checkpoints are rescored with the fixed benchmark code, not retrained (`nimiwae70_cv.py --rescore`; tested: a rescore reproduces the training-time scores exactly). Age on is retrained, 5 folds, same settings, into `out/cv16`. Bars unchanged.
- **Checks:** `nimiwae70_verify.py` 47 / 47 (the gate against the written formula, the mixture against a hand formula, no penalty past breast height, hardwoods held longer, fallback anchor on 739 trees without a prior, l0 values, E1 mask on visible records).

## D72 — One definition per concept: shared visit and tree-end tables (user, 2026-11-03)
After the code-read review (`docs/SEMANTIC_REVIEW_2026-11-03.md`) the user asked whether refactoring would make such issues easier to spot, then: "go ahead with the refactor plan". Work log: `docs/REFACTOR_2026-11-03.md`.
- **Why:** four of the five medium findings were one concept implemented in several places that drifted (a tree's end classified four times with hand-kept string lists; lineage breaks known to the tree split only; the frame built from a downstream product).
- **Shared rules:** `fia_rules.py` (disturbance definition, cut reasons, tally split DIA, end kinds); stages import, none restates.
- **plot_segments / visit_status** (step 1): a plot's lineage segment starts at its first annual visit and at KINDCD 1 / 3 or a DESIGNCD change, ordered by date then INVYR. visit_status = the scaffold with forest shares, segment and microplot move. Tree numbers, frame spans, phantoms and the frame's slots never cross a segment start. The integer sim_year axis still spans segments: it is only an axis, and every quantity on it is segment-bounded. Option rejected: new subplot keys per segment (would change every consumer's key).
- **tree_end** (step 1): one row per tree, `end_kind` in alive / death / cut / inferred_removal / diverted / censored, `end_reason` (= final_tree_fate), `cause`, `death_kind`, end visit. Rules as before except:
  - **C3:** a tree alive at its last record is an inferred removal only if a later visit of its own segment had forest in its own tally area (microplot if last DIA < 5 in, else subplot); the removal is dated at the first such visit. With no such visit it is `censored_unobserved`. A partly forested tally area counts as sampled (the spot may be on either part; the frame's sampling probability handles it). Option rejected: requiring a fully forested area (would censor real removals on mixed subplots).
  - **C1:** the next visit starting a new segment gives `censored_lineage_break`.
  - **C7:** a cut record on land leaving forest (`diverted_landuse` with a record) is `end_kind` cut, cause cut. A live tree whose land left forest is `diverted`, no cause.
- **Consumers:** final_tree_fate, trajectories' terminal_fate / death_cause, `disturbance_events` (so `subp_has_dstrb` and the Sim B drops), and the frame read tree_end.
- **Frame (C8, C2):** real slots = every curated live record (STATUSCD 1, TPA > 0), aged or not; move masking by the latest move at or before the tree's debut. An end in tree_end before the tree's last live record (a data error the misrecorded-death rule misses) falls back to the first dead / cut record after it, as before.
- **Model (M1):** no short-tree factor; the design rule's "sampled, no record" term is log(1 - p_s g_t sigma(x_t)), g_t = P(at breast height). The observation model is listed once in the `nimiwae70` docstring.
- **Checks:** verifier n20 re-derives the end rules independently (segments from raw PLOT); n27: no phantom on another segment; audit F15-F18: every live record a real slot, every slot in its tree's segment, inferred removals where the tally area was sampled, cuts end as cuts in the frame.
- **Rebuild:** R4 into a new truth DB (`FIA/truth/fia_truth_r4_d72.duckdb`); the live DB stays until the regression is read.
- **Measured on R4 (rebuild 2026-11-03; `docs/D72_REGRESSION.md`, `fia_truth_d72_regression.py`).** Verifier 217 / 217, lint clean, audit 51 / 51 (F15-F18 pass; 2 trees end on the dead-record fallback, F18b).
  - curated_trees: every record and every column identical; final_tree_fate identical for every tree.
  - C1: all 2,001 segment starts are KINDCD 1 re-installations (no KINDCD 3, no DESIGNCD change). FIA closes every tree alive before one (mostly RECONCILECD 9), so no tree ends `censored_lineage_break`. Effect = slots and phantoms on the earlier segment: 17,315 trajectory phantoms, 1,109 Sim A / stdorg cohort phantoms (1,491 tree-pass sources), 41,910 frame slots (21,213 pre, 20,697 post). Knock-on: trajectories' plot / subplot visit counts on 790 / 2,083 rows, `land_use_4way` on 2 plots (a mode over trajectory rows), cohort_disturbance +8 full removals.
  - C3: 21 inferred removals before and after; none becomes `censored_unobserved`. The frame now dates them at the visit that sampled the spot.
  - C7: no frame cause changes: every land-clearing cut record in R4 carries AGENTCD 80.
  - C8: 0 (curated live records = trajectory real rows = 3,729,318).
  - C2: no E1 mask changes beyond the removed slots.
  - subp_has_dstrb unchanged on every subplot (Sim A and Sim B).
  - The review overstated C3, C7 and C8 for R4; they stay fixed so other regions or later data cannot hit them.
- **Next run:** `FIA/nimiwae_r8/d72`, both arms retrained (the frame changed), `out/cv72`; the age-off checkpoints of `out/cv` are not reused.

**D72 amendment 1 (2026-11-03): four fixes from review round 3** (`docs/SEMANTIC_REVIEW_2026-11-03.md`, round 3 and its double-check; user: "apply the fixes first", before cv72).
- **Tree split on a segment change** between two records of the same number, not only on a record AT a start: 1,100 of the 2,001 R4 starts are on nonsampled visits, where no record sits. 0 R4 trees affected (FIA renumbers at re-installation); the verifier's independent split re-derivation uses the same rule.
- **Frame ends from tree_end only.** The fallback to the first dead record after the last live one read misrecorded deaths and ended 2 diverted trees as deaths; no tree_end end precedes a live record, and `load54` now refuses a DB where one does. Audit F18b: every frame end is the tree_end end.
- **The age prior reads the record's own condition origin** (`stdorgcd_own`), as the age records carry it; the subplot's area-dominant origin differed on 5,223 trees (0.31 %). Audit F19.
- **The end benchmark's cut truth comes from `end_cause`**, not the fate list plus AGENTCD (identical labels in R4; one definition).
- **Withdrawn after the double-check:** R2 (the E1 mask in benchmarks: every benchmark either drops all pre-debut FALSE labels or never hides the debut) and R4 (the unobserved rule is the correct conservative reading; 0 trees).
- **Rebuild:** the first D72 outputs are kept as `FIA/truth/fia_truth_r4_d72am0.duckdb` and `FIA/truth/r4_d72am0` for the am. 0 vs am. 1 regression; the new build replaces `fia_truth_r4_d72.duckdb`.
- **Measured (am. 0 vs am. 1, `docs/D72_AM1_REGRESSION.md`):** truth DB identical on all 48 shared tables except run metadata; 0 records changed tree id (the 4 'visit' splits in GA meet the dead / new-tree rules too, in both builds). Verifier 217 / 217, lint clean. Frame: identical slots; 2 trees lose their (wrong) death end; the age prior changes on 10,994 real slots of 7,348 trees (mean |change| 6.7 yr, max 38 yr; more than the 5,223 first-record estimate because later records differ too); prior G1 8.877 -> 8.867. nimiwae70_verify 48 / 48, nimiwae56_verify 64 / 64, audit 53 / 53 (F18b, F19 new). Bundle rebuilt (seq md5 0b63609e), out/cv72, both arms. The am. 0 frame is kept as `FIA/nimiwae_r8/d72am0`.

**D70 amendment 17 (2026-11-03; user): the site-tree link rule for age anchors.** Closes the link-bar question left open in am. 1.
- **Rule:** a SITETREE record is tied to a tally record when they share the visit (plot, INVYR), CONDID (within the visit), SPCD and DIA to 0.1 in, one-to-one both ways, with **exactly the same HT**, and SITETREE.SUBP is either **equal to the tally SUBP or blank**. A recorded SUBP that differs excludes the link.
- **Measured (R4, annual, AGEDIA > 0, carry-forwards removed; 78,366 site trees, 19,027 with a SUBP):** 9,565 links with SUBP equal + 14,984 with SUBP blank = **24,549 anchors**; 169 conflicting-SUBP links excluded (26 % planted, 53 % loblolly, against 22 % / 44 % for SUBP equal); the within-5-ft tier (1,921) excluded. Exact-height placebo precision 0.984 (SUBP equal 0.994).
- **Why blank counts:** recording SITETREE.SUBP ("core optional" in the User Guide) is a state or crew practice (AL 256 equal vs 3,673 blank; MS 933 vs 211), and only 1.1 % of exact-height links without a match carry a SUBP that disagrees, so a blank is a missing field, not a mismatch.
- **The crown-class clause of am. 1 is not used:** it does not discriminate (dominant / codominant share 97-98 % on real links, 96-98 % on placebo links, because any tally tree within 0.5 in of a site tree's size is already large).
- **Coverage, stated:** about 23 species, mostly commercial timber (top 5 species 68-75 % of links, loblolly 32-44 %), 16-17 % under 9 in DIA. The anchors inform the timber end of the age-growth relation; saplings and minor species inherit it only through features and the latent.
- **Use:** these links are the measured-age observations the joint model was designed around (D70 design: "constrain each other"; am. 12 removed the record likelihood together with the growth curve). How they enter the joint likelihood is D74.

## D74 — Measured-age anchors, a data-calibrated age-vigour coupling, and method-aware height (user, 2026-11-03; recorded before any code)
User: anchors as joint-likelihood observations (the D70 design, "constrain each other"; am. 12 dropped the record likelihood together with the failed growth curve), plus "coupling age and vigour" so species without anchors are constrained too; "think this thoroughly first, identify the weaknesses and looseness, assess things from data"; then "go ahead".

**Evidence (2026-11-03, read-only on FIADB; scripts in the session scratchpad, numbers here):**
- **Growth carries modest age information beyond the direct prior.** 10,091 am. 17 anchors with an increment (PREVDIA, REMPER): out-of-fold by plot, prior recalibrated 9.45 yr MAE; + log DIA growth rate 8.76 (natural 10.63 -> 9.94, planted 4.71 -> 4.02), bias unchanged. Coefficient on log rate given the prior **-0.18** (residual log sd 0.272 -> 0.248); "age = size / rate" would be -1.
- **The coupling fades under suppression** (western BHAGE, 229k trees >= 5 in): coefficient on log rate -0.43 to -0.46 for crown classes 1-3, -0.27 intermediate, -0.20 overtopped; R2 with rate 0.59-0.70, 0.36, 0.22. The anchors are 90 % codominant, 40 % of frame trees are intermediate or overtopped.
- **Coverage:** 57 % of frame trees have >= 2 measured DIA (saplings 43 %, overtopped 48 %); the 9 main anchor species are 54 % of frame trees; anchors are 84 % >= 9 in.
- **PREVDIA pitfall:** at R4's first annual visit (MANUAL 1.0) PREVDIA mostly equals DIA (GA 1997 81 %, NC 2002 79 %; later visits 4-5 %). It is not a measurement there. Nothing here uses PREVDIA; rates come from consecutive annual DIA in the frame.
- **History that constrains the design:** a soft age-size curve (am. 3, 9, 10) failed (G1 18.8); a free vigour absorbed size (am. 7); a learned width collapsed (am. 9); D56 process noise was calibrated but did not move E1; am. 3 (user): no structured growth.

**Options considered for the coupling:** (a) a learned prior p(vigour | age, x): two latents traded off freely, weakly identified; (b) structural "size = integrated growth" (anchored struct ODE / SDE): over-couples about 5x against the data, conflicts with am. 3, large build; (c) **the chosen one: the age prior shifted by the tree's latent growth rate with strength and width fixed from data, outside the joint.**

**Design.**
1. **One link rule.** `site_links()` in one module implements am. 17 (visit, CONDID within the visit, SPCD, DIA to 0.1 in one-to-one both ways, exact HT, SITETREE.SUBP equal or blank). The audit, the G1 truth set and the anchors all call it (the G1 set used its own exact-DIA-and-HT query without the SUBP rule). G1 is reported on the new set and, for continuity with cv72, on the old one.
2. **Anchors as observations**, fit plots only (never held, monitor, VAL or TEST plots): at the linked visit t, log AGEDIA ~ N(log(a_t - l0), S_ANCHOR), S_ANCHOR = 0.10 fixed (ring counts; sensitivity 0.05 / 0.20 reported). AGEDIA is a breast-height age, so it constrains a - l0, the same quantity as the gate; l0 stays 2 / 5 yr until the R4 l0 prior is decided (open, see the l0 discussion; the validated quantity is breast-height age).
3. **Age-vigour coupling in the prior:** log a_l = af_mu + delta_c (rho - m_c) + af_sd' z0, z0 ~ N(0, 1) as now.
   - rho = the tree's latent log DIA growth rate (in / yr), mean over its visible real intervals of the decoder's path; defined only with >= 2 visible measured DIA records, else no shift. Hidden visits stay hidden (benchmarks unchanged).
   - delta_c, m_c and the width factor af_sd' / af_sd are **fixed, not learned in the joint**: calibrated per CV run on that run's fit-plot anchors (log total-age truth, AGEDIA + l0, regressed on af_mu and the log observed rate from consecutive frame DIA: the prior's own space, so the shift and the width factor apply to log a directly; m_c the class's mean log rate, so the mean shift is 0 and the prior's level is unchanged), crown class c of the record at la. Classes 1-3 from the anchors; intermediate and overtopped = the anchor value times the western ratios (0.61, 0.43), because the anchors hold 2.3 % of them.
   - **No shift below 5 in DIA at la** (no R4 sapling has an age; the IW 1.0-2.9 in TOTAGE saplings test whether a sapling coupling exists before one is added).
   - **Two-stage refit, once:** after training, delta_c is refitted on fit-plot anchors with the model's posterior latent rate (denoised, so the raw-rate value is attenuated). The refit and the retrain with it are pre-registered as one step, not a search.
   - The decoder still never reads z0; the exact grid stays exact (the prior term varies along the grid, the decoder is cached; rho comes from the cached path).
4. **Height measurement model.** HTCD joins the frame. Log HT residual: Student-t, nu = 4 fixed, scale learned per class: measured (1) and null; visual (2, 3). HTCD 4 heights are masked (they repeat the previous height: median change 0.0 ft, ratio 1.000). The latent height stays monotone; a broken top is not a decoder input (B10: no tree-own records on the generative side); its effect on growth comes through z, as for the broken-top flag. Height residuals after a break are reported.
5. **Unchanged:** the direct prior (`f+p`, af_mu / af_sd), flexible growth, the gate, the D72 observation model, process noise off, Gaussian q by default (D73 flow optional).

**Checks, set now. D74 replaces cv72's model only if all pass (same folds, frame and seed as cv72):**
- **G1** (held plots, am. 17 set): mean MAE at least 0.2 yr below cv72's on the same trees, no fold worse by more than 0.2; |median bias| <= 2; G1 <= direct prior.
- **G5** coverage 0.75-0.85; **G6, G8** as D72 against age off.
- **Coupling guard (am. 12):** on held anchors with a rate, the slope of posterior log age on latent log rate at fixed prior lies within 0.5x-1.5x of delta_c; on non-anchored held trees, by crown class, not steeper than delta_c in magnitude. Single-visit trees: posterior equals the prior (as now).
- **Anchor fit:** fit-plot anchor residuals (log AGEDIA - log posterior BH age) have median within +-0.05 and spread near S_ANCHOR; held anchors are G1.
- **Height:** learned scales by class reported; measured class below visual class.
- **Reported, not bars:** planted field age by DIA class (bias, coverage); E1; ESS; sensitivity of G1 to delta_c x0.5 / x1.5 and S_ANCHOR 0.05 / 0.20.

**Deferred, with reasons:** structural or SDE growth (over-couples, am. 3); OU noise and break jumps (the t-likelihood first); l0 as a latent and the R4 l0 prior (open user decision); a sapling coupling (after the IW check); recalibrating the prior's level (the coupling is centred so the prior's mean is unchanged).

**Build order (each step read semantically twice before anything runs):** site_links module and G1 refs -> frame (HTCD, anchor slot columns) -> calibration routine in the CV run -> model terms (anchor, coupling, height) -> verifier checks (anchor term against its formula, coupling inert when rho is undefined and below 5 in, grid exactness with the coupling, held-plot anchors never enter training, HTCD 4 masked) -> bundle.

**D74 measured: cv74flow (Q=flow, D74=1, PyTorch; FIA/nimiwae_r8/cv74flow), all 10 runs, epoch 36. Combined flow + D74; attribution open.**
- **G1 on the old link set, the same held trees as cv72 (counts equal): 7.968 yr mean vs cv72 8.278** (f0 8.040 / 8.388, f1 8.007 / 8.458, f2 7.930 / 8.128, f3 7.935 / 8.270, f4 7.929 / 8.146): better in every fold, by 0.20-0.45. New link set: 8.298 (n 3,561-3,751 per fold), bias -0.42 to +0.01, direct prior 9.16, current ages 9.81. The pre-registered G1 bar is on the new set against cv72 rescored there (`G1RESCORE=out/cv72`), still to run.
- **cv74flow's own bars at 36 (age on vs off): G1, G5 (0.768-0.783), G6 (size ratio 0.96-1.03; E2, E3 no worse), G8 (E1 0.054 vs 0.062, every fold) all pass.**
- **Residual-slope guard: passes.** Held G1 trees with a shift (n 15,067): slope of log truth - log posterior median on log rate +0.013, |slope| / |delta| = 0.10 (bar 0.5).
- **Coupling on the full fit sets:** delta -0.125 to -0.133 per log (in / yr), width factor 0.943-0.948, about 10,600 anchor rows per fold (smoke subset: -0.173).
- **Planted field age (diagnostic):** bias +2.1 to +4.4 yr by DIA class, 80 % coverage 0.27-0.33 at 1-5 in, 0.45-0.68 above: the sapling limit stays.
- **Trivial baselines, same held folds (old set, 20,687 trees):** training mean 18.3, median 18.1, species median 12.9, species x DIA class median 9.86, current ages 9.49, direct prior 8.77; joint cv72 8.28, cv74flow 7.97. New set: 18.1 / 13.3 / 10.3 / 9.81 / 9.16 / 8.30. The current curated ages barely beat a species x size lookup.
- **Flow check (fold 0, epoch 36, 2,000 held trees):** far from the identity (median shift 0.63 q-sd, p90 1.6; mean log-det -3.8; 98 % inside the spline range; forward-inverse round trip 6e-6). Flow off under the trained model: bound +0.59 -> -7.89 nats / tree and ESS 7.9 -> 1.2 (K 32; K 100: ESS 22.0 -> 1.5). An upper bound on the flow's value: the encoder co-adapted (a wide Gaussian the flow contracts), so a separately trained Gaussian (cv74) is still the attribution test.

**D74 adopted (user, 2026-11-05: "adopt now, I'll run cv74 gaussian later"). The selected joint age model is cv74flow's configuration: D73 flow q + D74 (anchors, calibrated coupling, Student-t height), on the D72 frame.**
- **Pre-registered G1 bar, new link set, cv72 rescored on the same trees (`--rescore-g1`):** cv72 f0 8.727, f1 8.795, f2 8.469, f3 8.563, f4 8.472 (epoch 36), mean 8.605, against cv74flow 8.377 / 8.327 / 8.287 / 8.232 / 8.267, mean 8.298: gain 0.307 (bar 0.2), every fold better (0.18-0.47; no fold worse, the bar's second clause). The rescore matches cv72's own old-set G1 within Monte Carlo noise (f0 8.388, f1 8.458, f3 8.270, f4 8.146 equal to three decimals; f2 8.125 vs 8.128: the rescore scores both sets in one grid pass, so the draws of the other latents fall differently). [Corrected 2026-11-05: an earlier version used f2's epoch-24 value, 8.482, and said the rescore reproduced cv72 to the last digit.] With bias, G5, G6, G8 and the residual-slope guard (recorded above), every D74 bar passes.
- **Recorded deviation:** D74 was registered as cv74 vs cv72 with the same q. cv74flow also changes the proposal, so the bars passed for flow + D74 together; the flow's separate contribution is unmeasured. The Gaussian D74 arm (`D74=1`, possibly `ARGS="--ckpts 12"`) is to run later and decides only whether the flow's 2x training cost is kept.
- **The official record** is `python -I nimiwae70_cv.py --out out/cv74flow --compare out/cv72_g1` once fold 2's rescore finishes; the numbers above come from the run logs.
- **What adoption does not yet mean:** the ages do not reach curation or consumers. Still open before that: sampled lineages and imputation draw age by importance sampling rather than the exact grid; planted saplings run 2-4 yr old with 80 % coverage near 0.3; the R4 years-to-breast-height prior; a sapling coupling; TEST plots, untouched so far, for the final check.

**D70 amendment 18 (2026-11-05; user: "go ahead with the grid fix in sample_lineages and impute"): the exact age grid in the products.**
- **Correction to the earlier description:** neither function output an age. Age reached them through the breast-height gate on existence, integrated by importance draws, so P(exists) near the debut and each lineage's start carried that noise.
- **Change:** `nimiwae70.AgeGrid` (one shared grid: rest of z drawn once, decoder cached, z0 on 61 nodes in +-3.6, log q(z0 = g) added back) now serves `age_posterior` (unchanged results), `posterior_grid` (impute's weights with z0 integrated on the grid) and `lineage_draws_grid` (each draw picks (grid age, draw of the rest) from their joint posterior, then end and cause given both). `sample_lineages` defaults to the grid for age models and outputs `age` (years at every slot of the draw), coherent with that draw's existence. `nimiwae56` was split into posterior weights (`posterior_draws`, `lineage_draws`) and the summaries built from them; the importance-draw path is unchanged (same draws, same order).
- **Scoring stays on importance draws** (`impute(age='draws')`, the default; `predictions(..., age=)` passes it through), as the CV scored and as the age-off arm needs. Measured on cv74flow fold 0, epoch 36, 13,163 held trees: size 0.1562 vs 0.1568, E1 0.0530 vs 0.0498 (n 381), E2 0.0906 vs 0.0905, E3 equal; the grid takes 9x the time. Switching the scored benchmarks to the grid is left for a separate decision (it would move E1 slightly in the age model's favour).
- **Checks** (nimiwae70_verify section 18, 80 / 80, 86 with the frame): grid weights sum to 1; P(exists) equals a brute-force loop over grid age, draw and end (6e-7); drawn ages follow the scored grid posterior; no drawn lineage exists before breast height; age steps equal elapsed years in every draw; grid impute respects the birth step; the default impute is bit-identical to the importance-draw path. nimiwae56_verify 64 / 64.

**D70 amendment 18, addendum (2026-11-05; semantic re-check of the grid code): the code is correct; the grid range was too narrow for products, and the age gate absorbs unexplained absences.**
- **Re-read:** lwg = log p(x, z0 = g, rest_k) - log q(rest_k) is valid because q factorises (z0's Gaussian is separate, with or without the flow); the z0 prior is inside log p; everything that depends on z0 is re-scored per node (gate, cut-off term, anchors, prior term), only the decoder output is cached; indexing (g K + k), per-draw end, cause, birth gate and ages, and the cache clearing on every exit, as intended.
- **Grid range, measured on cv74flow fold 0 (5,045 held trees, 2 % of plots):** on +-3.6, 0.67 % of trees put more than 1e-3 of their posterior on the edge nodes (worst 40 %), all on the YOUNG edge; 1.7 % of 2,000 anchored fit trees (worst 5.7 %). On +-8 (133 nodes) no tree does (largest outer-node mass 6e-12). Medians move by less than 0.3 yr for all but one bimodal tree (8.1 in, prior 42 yr: 46 % of its mass beyond -3.6; median 33.5 -> 8.6 yr). **Change:** products (posterior_grid, lineage_draws_grid) use PRODUCT_GRID (+-8.04, 135 nodes: the scored grid's 0.12 spacing exactly; about 2.2x the time); scoring keeps SCORE_GRID (+-3.6, 61 nodes) so cv72, cv74flow and the coming cv74 stay comparable.
- **Finding, not changed:** for large trees debuting after a sampled unrecorded visit (no missed code), the model's cheapest explanation of the absence is "not yet at breast height", so the gate makes them implausibly young: posterior median z0 < -2 for 0.32 % of such >= 5 in trees (< -3: 0.24 %), 0.14 % of all held trees, 0 % of trees recorded from the first slot (so G1's site trees are untouched). Likely truth: an uncoded crew miss or a procedural gap. **Candidate fix, to be decided and pre-registered before any run:** a small crew-miss probability in the "sampled, no record" term, P(no record) = 1 - p_s g sigma(x) (1 - eps), eps fixed from FIA QA or bounded like rho, so a present, tally-size tree can be missed without rewriting its age.

## D77 — CV scoring of completion on the age grid, and G1 lookup baselines in every summary (user, 2026-11-05: "go ahead with 8 and 9")
**8, the scoring method.** The products integrate the age latent on the exact grid (D70 am. 18), while every CV completion score (size MAE, E1, E2, E3) used importance draws for every latent. Measured on cv74flow fold 0 (3 % of held plots, the same plots both ways): E1 0.0487 draws vs 0.0466 grid; size MAE 0.1567 vs 0.1577; E2 0.09953 vs 0.09950; E3 0.03786 vs 0.03786; earlier on the same fold, E1 0.0530 vs 0.0498. Grid costs about 23x on CPU (280 s vs 12 s), and draws scoring is about 230 s per checkpoint on the GPU.
- **Options:** (a) switch the bars to grid now; (b) grid at every checkpoint, reported; (c) grid at the last checkpoint only, reported, with a rescore path for finished runs; (d) leave as is.
- **Decision: (c), and grid becomes the scoring of completion bars registered after D76.** D76's bars were registered on importance draws and cv74flow was scored that way, so switching them now would change a pre-registered test after the fact. Every age-on run now also scores `completion_grid` at its last checkpoint (`S.completion(age='grid')`; about +1 h per run at the GPU ratio measured on CPU, reported). `nimiwae70_cv.py --rescore-grid RUN` (`GRIDRESCORE=RUN bash run.sh`) adds `scores_grid.json` to a finished run, on the same score plots and held inputs. `--d76-base` and `--summary` report the grid values (out - base deltas; fold means). G1 / G5 were already on the grid (am. 14) and are unchanged.
- **Smoke:** a 1-epoch CPU run with D74 + D76 writes completion and completion_grid; the summary and the comparison read them. The rescore reproduces the timing test's numbers and skips a done epoch.

**9, the baselines.** `nimiwae70_score.baselines(data, folds, nf)`: per CV fold, lookups fitted on the linked site trees of the other folds' TRAIN plots and scored on the fold's held plots (training mean, median, species median, species x DIA class median with fallbacks), plus the direct prior and the current curated ages. `--summary --data data` prints them next to the joint G1 per epoch and link set, and checks the trees are the ones the run scored (per-fold counts). Reproduces the one-off D74 numbers exactly; cv74flow at 36, am. 17 set: training mean 18.14, median 17.98, species median 13.28, species x DIA median 10.27, current ages 9.81, direct prior 9.16, joint 8.30; D70 set 18.28 / 18.05 / 12.93 / 9.86 / 9.49 / 8.77 / 7.97.

**Found while testing (a reading, not a change):** cv74flow's G1 is flat over training (fold 0: 8.379, 8.377, 8.377 at epochs 12, 24, 36) while size MAE improves (0.163 -> 0.152), and the joint sits 0.30 yr below the prior read alone (`mae_prior`, 8.60; cv72 joint on the same trees 8.605). For site trees the age posterior is essentially the prior with the D74 coupling: the gate only acts near birth, and the trained networks barely move these ages. The learned part of the model can matter for age only where the gate binds (saplings, late debuts), which is where items 3-5 (planted saplings, l0, a sapling coupling) sit.

## D76 — Crew misses of not-yet-listed trees in the "sampled, no record" term (user, 2026-11-05: "yes, write up the crew-miss fix"; recorded before any code)
**Problem (D70 am. 18 addendum):** a tree absent at a sampled visit before its debut can only be (a) below the tally limit, (b) on an unsampled spot, or (c) not yet at breast height. For large ingrowth with no missed code, (c) is the cheapest, so the age gate rewrites the tree's age: posterior median z0 < -2 for 0.32 % of held trees >= 5 in that debut after a sampled unrecorded visit (cv74flow fold 0, 1,260 such trees), 0 % for trees recorded from the first slot.

**Evidence (15 % of TRAIN plots, 191,888 trees, D72 frame; one-off reads, the numbers that set epsilon will come from committed code):**
- **Listed trees are almost never silently missed:** 0.03 % of sampled visits strictly inside a tree's record have no record (42 of 121,533). FIA crews reconcile every previously tallied tree, so absence after a tree is on the list stays informative.
- **New trees are missed often, mostly with a code:** of debuts after a fully sampled earlier visit, coded misses (codes 3 / 6 / 7, already the "missed" mode) are 26 % at the microplot (7,072 of 27,066) and 1.7 % at the subplot (849 of 50,003).
- **Uncoded misses of new trees:** uncoded debuts that would have needed more than 0.75 in / yr of growth above the limit since the earlier visit: 1.3 % (microplot), 1.1 % (subplot); more than 1.0 in / yr: 0.6 %, 0.4 %. Microplot pre-debut slots already carry soft FALSE labels with an error rate rho <= 0.05; subplot-level pre-debut slots have no label and rely on the "sampled, no record" term alone, which has no miss rate.

**Design:**
- **Where:** the "sampled, no record" term at PRE-DEBUT slots (t < f, the first visible record) whose tally limit is above the existence level (subplot-level slots: the term already acts there; microplot slots keep their soft labels). Post-record absences are unchanged (listed trees, 0.03 %), and coded misses keep the missed mode.
- **Form:** P(no record | z) = 1 - p_s g_t sigma(x_t) (1 - eps): a present, at-or-above-limit tree on a sampled spot is missed with probability eps. Age off (g_t = 1): the same eps in the D55 term, so the age-on and age-off arms keep comparable completion (G6, G8).
- **eps fixed from data, never learned in the joint** (a learned miss rate could make every absence uninformative): per CV run, on that run's fit plots, eps = the share of uncoded subplot-level debuts after a fully sampled earlier visit whose implied growth above the limit since that visit exceeds G_MAX, where G_MAX = the 99th percentile of annual DIA growth between consecutive measured records of 5-7 in trees on the same fit plots. Stored in the vocabulary (like the D74 coupling) and logged. Expected from the evidence: about 0.005-0.015.
- **Unchanged:** everything else in D74 + D73 (cv74flow's configuration), frame, folds, seed, epochs, scoring (importance draws; G1 / G5 on the 61-node grid).

**Checks, set now; D76 is adopted only if all pass (cv76flow against cv74flow, same folds, frame, seed, epochs):**
- **Primary:** among held trees >= 5 in at the prior's slot that debut after a sampled unrecorded subplot-level visit, the share with posterior median z0 < -2 (product grid, all five folds' held plots) falls from cv74flow's value to <= 0.10 %.
- **No harm:** G1 (both link sets) mean MAE not worse than cv74flow by more than 0.05 yr; |median bias| <= 2; G5 coverage 0.75-0.85; E1, E2, E3 Brier not worse by more than 0.002; size MAE not worse by more than 1 %; the D74 residual-slope guard still <= 0.5 |delta|.
- **Reported, not bars:** eps per fold; the same prevalence for microplot debuts; sensitivity of the primary to eps x0.5 and x2 (rescored on the trained model, no retraining); planted field age by DIA class.
- **If the primary fails:** reported as is, no tuning; the remaining fix is the breast-height size anchor (deferred in D74), because one miss per visit at eps ~ 0.01 costs about as much as moving z0 three prior standard deviations, so trees with two or more unrecorded visits may still be pulled young.

**Build (each step read twice before anything runs):** the eps calibration routine (frame-level, fit plots), the term in NIMIWAE70.cutoff_term and the age-off base term behind one flag, the same in nimiwae_jax/model.py with the equivalence check extended, verifier checks (the term against its formula; eps = 0 reproduces cv74flow's weights exactly; eps only at pre-debut subplot-level slots; calibration recovers a planted rate), the CV flag (--d76, out/cv76flow), the prevalence script as committed code (it also recomputes cv74flow's baseline value).

**D76 amendment 1 (2026-11-05): built, not yet run.**
- **Form as built:** P(no record) = 1 - p_s (1 - eps) g sigma(x) is the existing term with p_s scaled by miss_keep = 1 - eps at pre-debut subplot-level slots (1 elsewhere), so both the age-gated and the age-off terms take it unchanged; the encoder still reads the observed p_s. `nimiwae70.pre_debut_subplot`, `apply_miss`, `calibrate_miss`; `train(..., d76=)` in both backends (eps in the vocabulary and the resume fingerprint); `--d76` in the CV; run.sh `D74=1 D76=1` -> out/cv76 (Q=flow: out/cv76flow). Scoring builds apply the model's eps too.
- **Primary check as code:** `nimiwae70_gatepull.py` (the same masks as the calibration). Reproduces the baseline: cv74flow fold 0, 2 % of held plots, 0.32 % of subplot-level late large debuts below -2 (the earlier one-off number).
- **eps on real fit plots:** fold 0, 30 % of fit plots: G_MAX 0.675 in / yr (109,561 growth pairs of 5-7 in trees), 1,224 of 69,597 uncoded subplot-level debuts exceed it, eps 0.0176; 6,000 fit plots: 0.0213. Somewhat above the 0.005-0.015 expected in the design (a reported quantity, not a bar).
- **Checks:** nimiwae70_verify 87 / 87 (93 with the frame; 6 new: factor 1 when off and weights bit-identical, 1 - eps exactly at pre-debut subplot-level slots, the term against log(1 - p_s (1 - eps) g sigma(x)), weights move when on, calibration recovers a planted 3 % rate); nimiwae56_verify 64 / 64; JAX equivalence with the factor on 174 slots: pass; both backends calibrate the same eps and carry it in the vocabulary.
- **Run order:** `Q=flow D74=1 D76=1 bash run.sh cuda` (out/cv76flow), then `python -I nimiwae70_gatepull.py --data data --run out/cv76flow --device cuda` and the same for out/cv74flow (the baseline on all held plots), then `python -I nimiwae70_cv.py --out out/cv76flow --compare out/cv72_g1` and `--summary` for the no-harm bars against cv74flow's numbers.

**D76 amendment 2 (2026-11-05): semantic review of the build (user: "semantically double check the code"); still not run.**
- **Read as correct:** `pre_debut_subplot` (t < f, tally limit above existence, inside the window); `apply_miss` after the coupling in `build`, so every build with the run's vocabulary (fit, monitor, held scoring, benchmarks) carries the run's eps, and checkpoints keep it in the vocabulary; the factor enters only the cutoff-mode term (missed and interval modes never read p_s); hidden benchmark records before the first visible one get the factor, as an unlisted subplot tree would. `calibrate_miss`: growth pairs are consecutive measured visible records (hidden records have mD 0), the limit is the slot's own (`exp(llim) + HALF_UNIT`), the needed growth is a lower bound (the tree was below the limit at the earlier visit), coded misses and partly sampled spots leave both the numerator and the denominator. Both backends: eps is calibrated on all fit trees before the subsample and is part of the resume fingerprint; the JAX term is `psamp * miss_keep` (equivalence passes with the factor on).
- **Fixed:** (1) the pre-registered no-harm bars against cv74flow had no code (the am. 1 run order pointed at `--compare out/cv72_g1`, which is D74's comparison). `nimiwae70_cv.py --out out/cv76flow --d76-base out/cv74flow` evaluates all of them per epoch from the two runs' scores (G1 both link sets <= +0.05 yr, |bias| <= 2, G5 0.75-0.85, age-on E1 / E2 / E3 <= +0.002, size MAE ratio <= 1.01, residual slope <= 0.5 |delta|) plus the primary from `gatepull_e{epoch}.json`, which counts only when it covers all held plots of all five folds; it reports eps per fold (new `miss.json` per run) and the age-off deltas. Tested on cv74flow against itself: every delta 0, ratio 1, residual slope 0.0128 as recorded. (2) The reported eps x0.5 / x2 sensitivity had no code: `nimiwae70_gatepull.py --eps-scale S` rescores a D76 run with eps x S into `gatepull_e{epoch}_eps{S}.json` (no bar). (3) Am. 1 said gatepull uses "the same masks as the calibration": it does not, by design. The primary's population (pre-registered) is debuts after a SAMPLED visit at DIA >= 5 in at the prior's slot; the calibration's is debuts after a FULLY sampled visit, any size. (4) Comment of WEST_RATIO had moved onto the MISS_Q line.
- **Verifier:** two new checks: calibration ignores coded, partly sampled, microplot-level and previously recorded debuts (eps unchanged when 400 such fast-growing debuts are added); no factor at a pre-debut microplot-level slot. nimiwae70_verify 89 / 89 (95 with the frame).
- **Flaky check fixed (not caused by D76):** nimiwae56_verify N1 failed about 1 run in 3 with the same code and inputs (posterior difference ~1.6e-3; 0 on one thread, 0 within a process). The float32 passes differ by a couple of ulps from process to process (vectorised kernels), and at |lw| ~ 3e4 that moves posteriors by ~1e-3. N1 now runs in float64 with a 1e-9 bar: exactly 0 in five runs; 64 / 64. The am. 1 "64 / 64" was a passing draw.
- **Run order:** `Q=flow D74=1 D76=1 bash run.sh cuda`; `nimiwae70_gatepull.py --run out/cv76flow` and `--run out/cv74flow` (all held plots); `--eps-scale 0.5` and `2` on cv76flow; `nimiwae70_cv.py --out out/cv76flow --d76-base out/cv74flow`.

**D76 amendment 3 (2026-11-05): second read of the comparison and gatepull code (user: "semantically double check the new compare and gatepull code").**
- **Fixed in `compare_d76`:** (1) an incomplete run could adopt (fold means over whatever folds existed): new bar `complete` (all five age-on folds in both runs, with their G1 rows and coupling files); the residual slope and delta now use exactly the folds the other bars use. (2) The primary ignored the base value and trusted the stored `pass`: it is now recomputed as pre-registered, the out share below the base's AND <= 0.10 %, from files that cover all held plots of all five folds at the same epoch and eps scale 1, in BOTH runs. (3) The gatepull population is defined by the data alone, so the per-fold tree counts must be equal in the two runs (`primary_same_trees`); otherwise the primary fails. (4) No G1 rows no longer crashes (the slope bar fails). Reported in addition: the D70-set bias and coverage, the coupled G1 count.
- **Tested** on synthetic gatepull files over cv74flow scores: falls 0.32 % -> 0.05 % passes; equal shares, a half-coverage file, different tree counts, a missing base file all fail the primary; a missing fold fails `complete` and adoption.
- **Gatepull read as correct:** masks from data only; `--eps-scale` replaces the factor (it never multiplies the run's own eps); the same seed per population; held builds get the CV's inputs (HTCD for the t height model, the run's coupling and eps; no anchors).
- **Interpretation recorded:** the completion bars (E1 / E2 / E3, size) apply to the age-on arm, the product; the age-off arm's deltas are reported. Bias and G5 bars are on the am. 17 link set, as in D74; the D70 set's values are reported.

## D75 — Training in JAX (user, 2026-11-04)
User: "could you make an implementation using JAX ecosystem instead of pytorch?", then "use idiomatic jax design", "comment the code well", "jax 0.7.2 is too old use latest".
- **Stack:** JAX 0.12.0.1, Flax 0.12.12 (NNX), optax 0.2.8 (installed into FIA/.venv_fia; GPU: `pip install -U "jax[cuda12]" flax optax`).
- **Scope (decided):** training only, for the configuration the CV trains (known rule, flexible growth, no process noise, static prior; age on / off; Gaussian or flow q; D74 terms). The data build stays PyTorch / NumPy (preprocessing both backends must share); scoring, the CV hook and the flow check keep using the PyTorch module. Options: (a) port everything (scoring and benchmarks too: large, every benchmark re-verified); (b) training only with weight transfer, chosen: the costly GPU part moves, the scoring code is unchanged and already verified.
- **Design:** package `nimiwae_jax/` (flow.py, model.py, weights.py, train.py, equivalence.py). The model has an `encoder` subtree (statics, transformer, flow) and a `generative` subtree (prior, decoder, scales), so the two gradients route by subtree: one forward pass, one vjp, two cotangents (the bound's -softmax(lw)/n for the generative side, DReG's -sg(w~)^2/n for the encoder), as nimiwae70.train. Optimizer: optax clip_by_global_norm(5) + contrib.schedule_free_adamw, the same algorithm as nimiwae70.ScheduleFreeAdamW except that optax's warmup starts at learning rate 0. The frame sits on the device once; batches are gathered by index inside the jitted step; matmuls at full float32 (no TF32).
- **Weights:** one table (`weights.pairs`) maps every PyTorch parameter to its Flax variable and back (Linear transpose, fused attention in-projection split into query / key / value heads); every PyTorch parameter must be paired except lsH under the t height model, which PyTorch never trains.
- **Equivalence (equivalence.py, real batch of 300 frame trees with 13 anchors, 131 coupled trees, 23 visual heights), all six configurations (age on / off x Gaussian / flow x Gaussian / t height):** log weights and DReG weights within 3e-7 relative, both gradient sets within 1.8e-5, weight round trip exact. The first attempt failed on the flow configurations: a test perturbation of 0.2 on every flow weight made the splines so steep that inverting them was ill-conditioned in BOTH frameworks (float64 round trip 9.6 in each; forward passes agreed to 1e-14), so the port was exact and the test was not; the flow perturbation is now 0.02 (a trained flow's last layers average |w| ~ 0.004). nimiwae70_flowcheck now reports the flow's forward-inverse round trip on a trained checkpoint (smoke 2.5e-6).
- **Smoke (CPU, 6,000 trees, D74, 2 epochs):** trains with both proposals; the hook receives a PyTorch NIMIWAE70 with the averaged weights; epoch after compile 7 s (flow) vs about 11 s in PyTorch.
- **Bundle:** `BACKEND=jax bash run.sh cuda` (output folder with _jax, e.g. out/cv74flow_jax), runs the equivalence check on the GPU machine first, sets XLA_PYTHON_CLIENT_PREALLOCATE=false (several processes per GPU). Runs record the backend in q.json; a folder refuses another backend.
- **Not a result yet:** JAX and PyTorch runs differ in noise draws, so their CV numbers agree only statistically; a backend comparison on the same folds is the check.

**D75 amendment 1 (2026-11-04): semantic review of the JAX code; resumable training (user: "if the training is interrupted, it currently restarts the epochs from zero").**
- **Review findings, fixed:** (1) optax's warmup with 0 steps holds the learning rate at 0 forever (PyTorch guards with max(1, warmup)); JAX now passes max(1, warmup) (only runs under about 20 steps were exposed; the CV warms up about 1,400). (2) to_torch built tensors from read-only JAX arrays (warnings only); now copies. Everything else read as equivalent: gradients (both cotangents), routing, initialisation identical to PyTorch's (same build, calibration, subsample, init order and seed), batch partition, averaged-weight evaluation, weight table completeness and inverses.
- **Equivalence extended:** forced branches the real batch lacks (20 moved trees, 30 benchmark-only interval slots), both proposals: within 6e-7 / 5e-6. Optimizer: optax clip + schedule_free_adamw vs nimiwae70.ScheduleFreeAdamW + clip_grad_norm_ on the same 5-gradient sequence (clipped steps included): y and x within 7e-7.
- **Resume (both backends):** `train(..., state_path=)` saves after every epoch, after its hook, atomically (temporary file then rename): parameters, optimizer state (Schedule-Free z, x, second moments, step and weight sums; optax state via nnx.to_pure_dict), batch-order and noise RNG states (numpy, torch / CUDA, JAX key) and the history, with a fingerprint of the setup (backend, trees, epochs, K, batch size, seed, lr, q, cfg, D74, calibrated coupling); a different setup refuses to resume. The CV driver passes f*_age*/train_state.pt (.pkl for JAX), reloads curve.json and scores.json when resuming, and replaces a curve point of a redone epoch instead of duplicating it.
- **Tested:** 3 epochs straight vs interrupted inside epoch 3's hook and resumed: bit-identical weights and identical losses, PyTorch and JAX (CPU). On a GPU, kernels are not bitwise deterministic, so a resumed run matches statistically.
- **Runs started before this change** have no state file and still restart from epoch 0 once.

**D74 amendment 1 (2026-11-03): build notes and two changes, made before any D74 result.**
- **Observed, not latent, growth rate.** The coupling reads the tree's observed log DIA growth rate (first to last measured visible record, >= 1 yr apart, floor 0.02 in / yr), the quantity the calibration regresses on, so prior and calibration agree by construction and the two-stage refit is dropped. No path from the other latents into age opens; the exact grid is unchanged. Hidden records have mD = 0, so benchmarks never read them.
- **The am. 12 guard becomes a residual check.** With a data-defined shift, the posterior's age-on-rate slope on held trees equals delta by construction, so the planned slope bar tested nothing. Replaced by: on held G1 trees with a shift, regress log truth - log posterior median on the log rate; |slope| <= 0.5 |delta| (too strong a coupling gives a negative slope, too weak a positive one).
- **Age-off arm:** with D74 it gets the same Student-t height model (G6, G8 compare like with like), no anchors and no coupling.
- **G1 sets.** site_refs.parquet now holds the am. 17 set (18,364 TRAIN links; 18,362 shared with the D70 set, same ages); the D70 set is site_refs_d70.parquet, reproduced bit-identically. Every scoring reports both from one grid pass. cv72's checkpoints are rescored on both (`--rescore-g1`), so the D74 bars compare the same trees (fold 0: 3,751 vs 4,204 trees, D70 count equal to cv72's).
- **HTCD** (ht_refs.parquet, from FIADB TREE at the same visit; split tree numbers mapped by TREE mod 100000): 3,727,795 of 3,729,318 real slots; 1: 3.48 M, 2: 47k, 3: 8.6k, 4: 92, null: 189k. Modelled heights barely reach the curated frame.
- **The frame is unchanged** (seq md5 0b63609e...), so cv74 and cv72 differ only in the model.
- **Smoke (CPU, 3,000 fit plots):** calibration on 1,345 anchor rows gives delta -0.173 and width factor 0.915, matching the standalone analysis (-0.18, 0.91); class means of the log rate 1: -1.46, 2: -2.00, 3: -2.04, 4: -2.75, 5: -2.92; 66k of 139k trees shifted, mean shift 0.000; no anchors in held builds; checkpoints reload with the coupling; the age-off D74 arm trains.
- **Checks:** nimiwae70_verify 73 / 73 (79 with the frame; 13 new: rate, la features, anchor slot, HTCD classes and masking, shift formula and eligibility, coupled ages, anchor term, Student-t against torch.distributions, calibration recovers a planted delta, grid exactness with a shift); nimiwae56_verify 64 / 64; audit and CV selftests pass.
- **Run order (bundle run.sh):** `G1RESCORE=out/cv72 bash run.sh cuda`, then `D74=1 bash run.sh cuda` (out/cv74), then `python -I nimiwae70_cv.py --out out/cv74 --compare out/cv72_g1`, and `--summary` for G6 / G8.

## D73 — Optional flow proposal q for the joint age model (user, 2026-11-03)
User: "Could you make the training optionally use a flow-based q ? but first check what the state of the art is". Off by default; `--q flow` (CV) / `Q=flow bash run.sh` (bundle, into `out/cv72flow`).
- **Literature (2026-11-03):** Agrawal, Sheldon & Domke 2020 (arXiv 2006.10343): real-NVP coupling flows with importance weighting and DReG / STL; >= 1 nat over full-rank Gaussians on about half of 30 models; DReG needs a second (inverse) pass. Durkan et al. 2019 (arXiv 1906.04032): rational-quadratic spline couplings, same analytic inverse as affine, far more flexible; the default conditional flow of sbi / zuko. Kingma et al. 2016 (IAF): one-pass sampling, but the density at a given z needs one pass per dimension. TarFlow (Zhai et al., ICML 2025, arXiv 2412.06329): transformer autoregressive flows for images, sequential inverse. Tucker et al. 2018 (arXiv 1810.04152): DReG holds for any reparameterised q.
- **Options:** (a) full-covariance Gaussian; (b) affine (real-NVP) coupling; (c) RQ-spline coupling; (d) IAF / autoregressive; (e) continuous flow / flow matching.
- **Decision: (c), conditional on the encoder's tree state.** DReG and the exact age grid both need log q at a given z, so the inverse must be one pass: coupling, not IAF or ODE flows. Splines over affine for flexibility at the same cost. (a) is subsumed by stacking, and a non-Gaussian, multimodal posterior (e.g. a size path consistent with two growth histories) is the case worth testing.
- **Design (`nimiwae_flow.py`):** e ~ N(mu, sigma) from the encoder as before; z0 (age) = e0 untouched; z_rest = T(e_rest; c), c = the encoder's tree token, T = 6 coupling layers (masks cycle even / odd / low / high half), 8 bins on [-5, 5], identity tails, MLP 64 x 2 per layer, last layer zero so T starts as the identity (the flow q starts exactly as the Gaussian q). log q(z) = log N(e) - log |dT/de|. q(z) = q(z0) q(z_rest), so the exact age grid (D70 am. 14) is unchanged: it draws z_rest from the flow and adds back q(z0 = g). Age off: all 16 dimensions flow.
- **DReG:** the density uses detached Gaussian, context and flow weights (`torch.func.functional_call`), z keeps its path; the flow weights are encoder parameters. Hooks `sample_z` / `log_qz` in `nimiwae56`; the Gaussian path is unchanged (same draws, same formula).
- **Checks** (`nimiwae70_verify.py` section 16, 18 new, 66 / 66 with the frame): spline derivative and inverse against autograd in float64 (1e-14 / 1e-12); flow log-determinant against the autograd Jacobian; flow at initialisation = Gaussian q (same draws, lw within 1e-4); z0 untouched; forward density = inverse density; DReG weights equal lw in value, train the flow and hold every q parameter fixed; no-limit grid posterior is the prior under a perturbed flow; checkpoints rebuild from cfg (pre-D73 checkpoints load as Gaussian). `nimiwae56_verify` 64 / 64.
- **Verifier fix, found while checking (not caused by D73):** `nimiwae56_verify` N1 failed on the D72 frames (also at HEAD): an untrained model gives |lw| ~ 59,000 on some trees, where float32 spacing is 0.004, and the check compared lw to an absolute 1e-5. The lw comparison is now relative (1e-5 of the scale); the posterior comparison stays absolute. Standalone it differs by 0.0; after another check it differed by 2 ulps.
- **Age grid without the inverse (second read):** the grid used to score its fixed draws through the flow inverse. Where a trained spline is flat (slope ~1e-3, allowed by MIN_D), the float32 inverse misses e by ~3e-3, up to ~0.4 nats of log q at sigma 0.018. It now takes the exact log q(z_rest) of the draws from the forward pass (`joint(..., lqz=)`), and asserts age on. DReG keeps the inverse: only its gradient is used.
- **Cost (CPU smoke, 4,000 trees, K 16):** 7 s vs 5 s per epoch, +120k parameters (163k to 283k).
- **Pre-registered comparison (cv72flow vs cv72, same folds, frame, seed, epochs):** adopt the flow only if, at the epoch chosen for the Gaussian run, the mean monitor bound improves by >= 0.05 nats per tree in at least 4 of 5 folds, the monitor gap (fit - monitor) is not larger by more than 0.02, and every D72 bar that passes for the Gaussian run still passes (G1 MAE also not worse by more than 0.1 yr). Otherwise the Gaussian stays and the flow remains an option. ESS is reported, not a bar.

**D69 amendment 2 (2026-11-01): the id change on the regional fit.** Am. 1 guessed a regional fit would move less. It moved more. Step 2 rerun on the same data with only `natural_record_id` changed: natural saplings under 5 in +0.58 yr mean, 0.84 mean absolute, 3.4 at the 99th percentile (596,563 records); natural 5+ in -0.01, 0.18; planted under 5 in -0.08, 0.34; planted 5+ in 0.00, 0.10. No estage age changes. The data changes of steps 2-3 alone move natural saplings -0.09 yr. Out-of-fold errors are unchanged, because those saplings are not scored. Reading: natural sapling ages sit outside the model's training sizes, so they depend on the bootstrap draw by about half a year. They are not stable values; the joint age model (D70) replaces them.

**D66 amendment 2 (2026-11-01): float aggregates in sorted order.** The final regional pair differed on one `cohort_disturbance` row: `would_be` sat on a 6-decimal rounding boundary, so summing in another order rounded it the other way (6949585.934721 vs 6949585.93472). Rounding had only made this rare. Every float sum, mean and std in a keyed aggregate now sorts first (SQL `SUM(... ORDER BY ...)` for the forest proportions). Lint L8 rejects the unsorted form.

## D71 — Live R4 DB replaced: natural ids, no CN logic, closure fix (2026-11-01)
- **Build:** commit acab08f, all 8 states, one fit scope. `FIA/truth/fia_truth_r4.duckdb` replaced once; the previous DB is kept as `fia_truth_r4.pre_d69.duckdb` until the user drops it.
- **Checks passed:** verifier 219 pass, 0 fail, 4 KNOWN cells; plain and shuffled builds at different thread counts identical on all 40 tables; lint clean; `retally_audit.py` reproduces 392 of 395 values, and the other 3 are break coverage, with moves not in breaks going from 403 to 0.
- **Skipped:** the verifier's subset two-thread-count section (e, D40); the full-region pair covers more.
- **Old vs new, R4:** curated_trees 4,496,120 to 4,522,556 (26,436 closure dead records, 11,918 cut); live records and DIA unchanged, 3,729,318 in both. Trajectories 4,845,688 to 4,838,871. Sim A 3,940,757 to 3,934,023; phantom rows 211,439 to 204,705. Sim B 2,293,491 to 2,293,520. Drops 263,183 to 263,200. Breaks 13,734 to 24,212. cond_link 238,106 to 131,311. Disturbed subplots 73,581 to 74,184 of 129,227.
- **Tree fates:** censored_procedural 91,021 to 64,585; dead 269,851 to 284,370; silvicultural_death 350,102 to 360,511; diverted_landuse 36,647 to 38,156; disappeared_inferred_removal 48 to 21.
- **Ages:** mean change +0.04 yr, 0.12 absolute. Model ages +0.15 yr, estage ages +0.0003 yr. Part of this is the D66 revision, which the previous live DB did not have; the rest is the redraw of D69 am. 2.
- **Not run on the new DB:** any consumer, calibration or training; `nimiwae56_verify`. Results on the old DB are not comparable with new ones.

**D70 amendment 2 (2026-11-01; user: use structured growth for the joint model).** Reason: with flexible growth, age is tied to size only at the first slot; structured growth shares the size response, so age and size stay consistent along the whole path. The joint model keeps the D56 per-interval noise (structured form, rate · exp(σ u - σ²/2)), so a tree can depart from the shared response in an interval while its age stays exact. This reverses the earlier growth selection for this model only. Cost on record, D55 VAL, V1 known + flex vs V3 known + struct: size MAE log DIA 0.198 vs 0.247, E1 Brier 0.126 vs 0.144, E2 Brier 0.152 vs 0.175, E3 Brier 0.078 vs 0.062, ESS 2.8 vs 1.8, bars 3 vs 2. Expected risks to score: ages pulled toward the mean, suppressed-then-released trees, and an overconfident b that makes the E1 bound too sharp. Bars are set before scoring.

**D70 amendment 3 (2026-11-01; user, after seeing the bias risks: do not use structured growth). Replaces am. 2.** The joint model keeps flexible growth, the D55 and D56 selection, with no per-interval noise (D56 selected the base arm). The gap am. 2 was meant to close is closed by scoring the shared age-size curve at every slot: log D_t ~ N(log S(t - b; species, site, soil, climate, v(z)), sigma_S). Age stays exact through b. The tie between age and size is soft, so a suppressed or released tree can leave the curve at a cost, and sigma_S is learned and monitored so it cannot swallow the constraint.

**D70 amendment 4 (2026-11-01): build notes and bars, set before any training.** `nimiwae70.py`, `nimiwae70_verify.py`, `nimiwae_age_prep.py`.
- **Birth anchor:** b = t_N - a_N, a_N > 0, N the frame's last slot. The first version anchored at the first slot, which forced birth before every slot and made the E1 bound impossible; the verifier now tests birth inside a lineage. t_N is known context, so the birth date cannot leak the tree's record pattern.
- **Western ages go through the curve, not lineages.** Those regions have no curated lineages (D70), so TOTAGE and BHAGE records train and test the curve as site trees do. R4 lineages reach age only through the curve.
- **Crown class in the curve:** delta(cc), with site trees as their own class. Western CCLCD identifies the crown effect; the site-tree class is its selection offset. Years to breast height come from the curve, TOTAGE none.
- **Frame fix:** cut closures (STATUSCD 3, 11,918 records) had no dead date in the frame; `dr` now reads STATUSCD 2 and 3.
- **Bars (VAL for choices; TEST once). All must pass before these ages replace `estimated_age`.**
  - G1 site-tree age, linked held-out trees: MAE at or below the current `estimated_age` on the same trees; |median bias| <= 2 yr. Linked = exact DIA and exact height tier, scoring only, because the anchor decision is open.
  - G2 curve age on held-out records, by source: |median bias| <= 3 yr; tally trees by crown class: overtopped |median bias| <= 5 yr.
  - G3 small trees, RSCD 22 and 26 held-out, 1-5 in: |median bias| <= 3 yr.
  - G4 stability: two seeds, natural saplings under 5 in: mean age differs by <= 0.2 yr (the current imputation moves 0.58 yr with a redraw).
  - G5 calibration: 80 % interval of predicted age on G1 trees covers 0.75-0.85.
  - G6 no harm to completion, against age off on the same data: size MAE log DIA at most 5 % worse; E2 and E3 Brier at most 0.005 worse.
  - G7 sigma_S stays between 0.05 and 0.5, the range where the curve neither forces the path nor stops constraining it. Reported every epoch.
  - G8 E1: on the hidden-number benchmark, E1 Brier at least 0.005 better than age off. If this fails the age bound stays off E1; the ages may still pass the rest.

**D70 amendment 5 (2026-11-02): end-to-end audit, `pipeline_audit.py`, `docs/PIPELINE_AUDIT.json`.** Truth DB, frame, age records and the joint model on real data.
- **Found by review, fixed:** E1 had no code (pre-move FALSE labels stayed); unborn trees could carry tally size; `predictions` and `stand_totals` used the base build and would have crashed; nothing kept validation records out of fitting. Fitting now asserts train split, first record per tree, ages within range.
- **Found by running, fixed:** the frame's final query took hours at regional scale because `t.tree IS NOT NULL` in a join's ON clause forced a nested-loop join. Moved to the select, the frame builds in 42 s and steps are timed. County tests were too small to show it. The age prep's correlated NOT EXISTS hung; a window replaces it. Cut closures had no dead date in the frame.
- **Soil:** `TreeAgeTransformer/tree_age_output/solus_soil.parquet` is NaN in every field on all 389,614 plots, so its null count of 0 meant nothing. Replaced by `soil_extract.py` and `FIA/nimiwae_r8/solus_plot.parquet`: 0-30 cm depth-weighted, restriction and bedrock depth, real values on 99.1 % of frame plots and 98.7 % of age records. TreeAgeTransformer read the same empty file; what used it is unchecked.
- **Age records:** identical repeated site-tree rows count once. Ages above 400 yr are out of fitting (`fit_ok`), 5,054 records, all but a handful western. TOTAGE keeps 24,202 of 127,047 records because 102,845 are woodland species measured at root collar; all kept are under 5 in. No tree has both TOTAGE and BHAGE at breast height, so years to breast height are identified only across sizes.
- **Check changed after seeing data:** A7 first capped ages at 1000 yr, then required site trees <= 400 yr. Both were guesses, not G bars. Western ages above 1000 yr are long-lived species. The one R4 site tree above 400 yr, a 9.9 in chestnut oak at 458 yr, looks like a recording error and is out of fitting by the cap; two yellow-poplars at 318 and 368 yr stay in and are suspect. A7 now tests only that fitted ages stay in the curve range.
- **Result:** audit all pass; `nimiwae56_verify` 64 of 64 in 4 of 5 runs on this code, N1 failed once (9.8e-03 on log weights near -3000) and has not been reproduced, cause unknown; `nimiwae70_verify` 17 of 17.

**D70 amendment 6 (2026-11-02; user chose the prior).** Years to breast height: one value per species group, log l ~ N(log l0, 0.3), l0 = 2 yr softwood, 5 yr hardwood, the values verifier (k) already uses. One prior term per group enters the objective, scaled like the rest. Report how much E1 moves when l0 shifts by 2 yr. The prior stays valid if the model later trains on all regions: only RSCD 22 and 26 hold TOTAGE or BHAGE, so elsewhere it keeps most of the weight.

**D70 amendment 7 (2026-11-02): first dev run failed; the birth date collapsed.** 150,000 trees, 4 epochs, GPU, seed 0, VAL. G1 fail (MAE 29.4 vs current 9.4, bias -25.6), G2 fail (site +0.4 passes; BHAGE +4.4, TOTAGE -11.2, overtopped -14.4), G3 fail (-9.2), G5 fail (0.01), G6 fail (size 24.5 % worse; E2, E3 pass), G7 pass (0.22), G4 not run, G8 unusable.
- **Cause:** predicted age at the last slot was 30.4 +- 1.0 yr for every tree, 30.3 yr at 1-3 in and 30.3 yr at 15+ in, correlation with log DIA 0.08. Vigour v(z) had sd 0.67 and no cost, so it absorbed all size and b stayed constant. More epochs would not fix it.
- **Change:** v = tau (z_0 - mu_0) / sigma_0, so under the prior v ~ N(0, tau), with tau the between-tree spread the age records identify. No bar changes.
- **E1 benchmark bug:** `bench_sizes` required `cond_status == 1`, a column null since D55 am. 7, so the benchmark held only TRUE labels and E1 scored nothing. Fixed. E1 from frames after am. 7 must be rechecked, `truth_v1` included.
- **Curve shape to watch:** inverted on site trees, bias runs -9 yr at 5-8 in to +18 yr at 16+ in. Inverting size given age spreads ages too wide; lineage ages should improve on this through the prior on b, and G1 will show it.

**D70 amendment 8 (2026-11-02): birth head on a log scale.** With vigour fixed, a 30,000-tree test still gave ages of 26.8 to 27.9 yr across all sizes, correlation with log DIA 0.46. The head output years through softplus from a start near 25, and at a learning rate of 1e-3 its weights could not span tens of years. It now outputs log age.

**D70 amendment 9 (2026-11-02): the curve width collapsed; the curve term moves to measurements.** Run d70b (400,000 trees, 12 epochs): `sd_S` fell from 0.23 to 0.013, below G7's 0.05, tau rose to 0.53, and the loss went to -7.7 against about +0.1 with age off. Cause, a design error: the curve term scored the latent size path, which is deterministic given z, so it is a density on the model's own output with no entropy term. The path can sit on the curve, and each slot earns log(1 / sigma) without limit.
- **Change:** the term reads measured log DIA at visible records, with a fixed width SIG_S = 0.15, so it is a likelihood of data and cannot grow without limit. Records use the same width. Hidden visits and unmeasured slots are no longer pulled toward the curve, which should also ease G6. Unmeasured slots are still limited by the unborn rule and by growth being monotone.
- **G7 no longer applies:** it tested a learned width that no longer exists. This retires a bar rather than loosening it, and the reason is recorded here.
- **SIG_S = 0.15 is a judgement**, between the size measurement error and the between-tree tau. Sensitivity to be reported.

**D70 amendment 10 (2026-11-02): second dev run, scored on VAL. No bar passes except site-tree bias in G2; the ages are not used.** 400,000 trees, 12 epochs, GPU; `FIA/nimiwae_r8/out/d70b_report.json`.
| Bar | Target | Result | |
|---|---|---|---|
| G1 linked trees | MAE <= current 9.4; bias <= 2 | MAE 18.8; bias +7.2 | fail |
| G2 records | bias <= 3 by source | site -0.02; BHAGE +3.3; TOTAGE -5.2 | fail, site passes |
| G2 overtopped | bias <= 5 | -16.9 | fail |
| G3 1-5 in | bias <= 3 | -9.0 | fail |
| G4 two seeds | <= 0.2 yr | 3.0 yr mean, 5.1 per tree | fail |
| G5 coverage | 0.75-0.85 | 0.41 | fail |
| G6 completion | size <= 5 % worse; E2, E3 <= 0.005 worse | size 11 % worse; E2 +0.001; E3 +0.000 | fail on size |
| G8 E1 | >= 0.005 better | 0.016 worse (0.077 vs 0.061, n 3,423, base 0.83) | fail |
- **Better than the first run:** G1 MAE 29.4 to 18.8, bias -25.6 to +7.2, coverage 0.01 to 0.41, size penalty 24.5 % to 11 %. No collapse: loss, tau and sd_disc level off.
- **Implausible ages:** 1.7 % of validation trees above 200 yr, 42 of 38,839 above 400 yr, up to 754 yr.
- **Years to breast height:** reach 34 yr in some groups against l0 of 2 and 5, so one prior term per group is too weak against the records; l is absorbing curve misfit.
- **Crown:** bias runs from +47 yr (CCLCD 2) to -17 yr (CCLCD 5), so a crown offset on the curve does not capture it.
- **Reading:** age is poorly determined by DIA through an inverted curve; tau 0.43 in log size spreads ages widely. The current imputation stays.

**D70 amendment 11 (2026-11-02; user asked): Schedule-Free AdamW available.** `ScheduleFreeAdamW` in `nimiwae70.py`, `--opt sf` (Defazio et al. 2024, arXiv 2405.15682): lr 1e-3, beta 0.9, warmup the same 5 % of steps, no Polyak step because this loss has no zero floor. Evaluation uses the averaged weights. An optimizer comparison needs both arms on the same optimizer. Bars unchanged. Expected effect: stable late training and anytime stopping; it does not address the design limits of am. 10.

