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
