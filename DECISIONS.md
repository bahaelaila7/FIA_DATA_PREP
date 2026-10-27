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

## D25–D30 — Round-6 audit (2026-10-27, agent; the implementer records the details and outcomes)
- **D25, DAMSEV damaged-alive drop branch:** removed as dead code. DAMSEV is a 0–9 class code, never > 40 in R4. D19's rationale is corrected.
  - Options rejected: inventing damage biomass from damage codes; keeping the dead code.
- **D26, resurrected trees:** a dead / cut record followed by a PREV_TRE_CN-linked live record is a misrecorded death. It is excluded from fates and events, and the tree continues.
  - Options rejected: end the tree at the death; leave as is.
- **D27, gap interpolation:** kept. Check (n3) is INFO when it has no candidates, and a synthetic fixture test exercises the code.
- **D28, plot location:** the most recent visit's coordinates / county / ecoregion. The key stays the first-visit key.
  - Options rejected: the first-visit location; per-visit locations.
- **D29, renumbers without a PREV_PLT_CN link:** linked only on identical coordinates plus ≥ 30 % tree continuity.
  - Options rejected: ignore them; coordinates alone.
- **D30, consumer requirement (not a curation change):**
  - Pan (landis_region4 src/data/utils.jl:371-372) recomputes its own sim_year and must use the curated axis.
  - Pan's drop consumer (utils.jl:398) defines the drop semantics the curated table must match (audit R6).
  - Both are recorded for the Pan rebuild phase.

## D31 — Generalisation beyond Region 4 (2026-10-27, agent; recorded for the expansion phase)
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
    - (a) The later linked live record can sit on a now-NONFOREST condition. The live rule keeps it out of curated_trees, so `tree_landuse_exits` now carries `linked_live_after_dead_md`, and it counts as resurrection evidence too (NC 4, TN 1, FL 1, pool 1 trees). For the final fate a misrecorded death counts as alive at its visit, so such trees end `diverted_landuse`.
    - (b) cohort_disturbance joined the trajectories' gap slot at a misrecorded visit to that dead record, read STATUSCD 2 and counted a false removal (AL 1, SC 3, TN 2, KY 2 drops). Phantom / gap slots are now alive by construction, and misrecorded records are never a status source.
- **Reverse:** set `res_md` to null in `classify_tree_fate`.

## D27 — Gap interpolation that never fires (audit r5 R5; 2026-10-27, coordinator)
- **Evidence:** in R4 every candidate gap spanned a censoring event, so (n3) read "0 of 0".
- **Chosen:**
  - The code is kept.
  - (n3) prints INFO "no candidates" when there are none.
  - New `selftest_fixtures.py` (run by the verifier as n9) exercises interpolation (basic / nonforest / censoring) and the timber-only drop branch (removal, shrink to 4.9 in, gap-only slot, sapling).
  - With D26 the data now does produce gap slots (GA 67), and (n3) checks them.
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
  - The auditor's AL 61/71 → 87 (17/37 with −0.3 in tolerance) fails the strict rule or the collision guard.
- The verifier rebuilds the links independently, and its map equals dbconn's (117 remapped visits, 18 links).
- **Reverse:** set `COORD_LINK_MIN_SHARE` > 1 (no links).

## D30 — Sapling growth-rate basis (audit r5 R1; 2026-10-27, agent)
- **Evidence:** r5 inferred the biomass basis from the presence of a `tpa` column. The trajectories carry `tpa` too, so their per-tree drybio_g was divided by TPA a second time, and trajectory phantoms came out 4.4 % (KY) to 7.8 % (NC) too low.
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
