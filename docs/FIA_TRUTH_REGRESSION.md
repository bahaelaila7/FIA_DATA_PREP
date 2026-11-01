# FIA Truth regression

Truth DB vs `/workspace/FIA/curated_r8`, one previous DB per state.

| State | Records old → new | Same key in both | Only old | Only new | Live old → new | Changed fate (trees, A2 label normalised) | Mean \|Δ age\| live / model / other (yr) | Changed age source | \|Δ age\| > 5 yr |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 706,088 → 706,088 | 706,088 | 0 | 0 | 576,536 → 576,536 | 0 | 1.14 / 2.94 / 0.44 | 0 | 35,769 |
| 12 | 368,874 → 368,874 | 368,874 | 0 | 0 | 299,503 → 299,503 | 0 | 0.99 / 3.57 / 0.28 | 0 | 17,375 |
| 13 | 1,062,964 → 1,062,964 | 1,062,964 | 0 | 0 | 880,697 → 880,697 | 0 | 1.22 / 2.94 / 0.36 | 0 | 67,021 |
| 21 | 280,506 → 280,506 | 280,506 | 0 | 0 | 240,600 → 240,600 | 0 | 1.26 / 5.39 / 0.07 | 0 | 27,581 |
| 28 | 494,827 → 494,827 | 494,827 | 0 | 0 | 409,774 → 409,774 | 0 | 0.80 / 2.04 / 0.38 | 0 | 10,891 |
| 37 | 612,996 → 612,996 | 612,996 | 0 | 0 | 506,268 → 506,268 | 0 | 1.40 / 4.34 / 0.28 | 0 | 54,280 |
| 45 | 518,426 → 518,426 | 518,426 | 0 | 0 | 431,850 → 431,850 | 0 | 1.24 / 4.16 / 0.29 | 0 | 38,524 |
| 47 | 451,439 → 451,439 | 451,439 | 0 | 0 | 384,090 → 384,090 | 0 | 1.40 / 5.36 / 0.15 | 0 | 44,613 |

## Row counts of the product tables, old → new

| State | curated_trees | curated_cohorts_landis | curated_cohorts_landis_stdorg | tree_trajectories | Sim A real / phantom |
|---|---|---|---|---|---|
| 1 | 706,088 → 706,088 | 615,827 → 612,520 | 615,827 → 612,520 | 789,538 → 790,417 | 576,536 → 576,536 / 39,291 → 35,984 |
| 12 | 368,874 → 368,874 | 319,992 → 315,786 | 319,992 → 315,786 | 385,710 → 381,123 | 299,503 → 299,503 / 20,489 → 16,283 |
| 13 | 1,062,964 → 1,062,964 | 933,128 → 925,309 | 933,128 → 925,309 | 1,163,062 → 1,157,754 | 880,697 → 880,697 / 52,431 → 44,612 |
| 21 | 280,506 → 280,506 | 259,448 → 254,007 | 259,448 → 254,007 | 296,643 → 291,128 | 240,600 → 240,600 / 18,848 → 13,407 |
| 28 | 494,827 → 494,827 | 432,534 → 429,666 | 432,534 → 429,666 | 520,377 → 519,758 | 409,774 → 409,774 / 22,760 → 19,892 |
| 37 | 612,996 → 612,996 | 543,376 → 537,062 | 543,376 → 537,062 | 659,362 → 652,016 | 506,268 → 506,268 / 37,108 → 30,794 |
| 45 | 518,426 → 518,426 | 466,375 → 461,672 | 466,375 → 461,672 | 581,770 → 576,282 | 431,850 → 431,850 / 34,525 → 29,822 |
| 47 | 451,439 → 451,439 | 411,557 → 404,735 | 411,557 → 404,735 | 484,894 → 477,210 | 384,090 → 384,090 / 27,467 → 20,645 |

## Reading the differences

- **Records:** identical sets of keys in all 8 states, and no live record lost or added. That includes the tree ids from the natural-key splits.
- **Fates:** no tree changed fate once `harvest_explicit` is read as `silvicultural_death` (A2).
- **Age sources:** no live record changed source.
- **Non-model ages:** these move 0.07–0.44 yr on average, consistent with F1 and A1 on eligible STDAGE (KY alone: 0.14 yr).
- **Model ages:** these move 2.0–5.4 yr on average. This is the largest change and is **systematic**. Saplings in natural stands get younger:

| State | Mean new − old, 1–3 in (yr) | 3–5 in | ≥ 5 in | Live natural model-aged records |
|---|---|---|---|---|
| AL | -3.1 | -2.9 | +0.1 | 112,172 |
| FL | -4.9 | -3.7 | -0.4 | 45,697 |
| GA | -4.3 | -3.5 | +1.1 | 199,633 |
| KY | -5.8 | -4.1 | -0.1 | 53,442 |
| MS | -0.9 | -0.9 | +0.6 | 69,996 |
| NC | -4.5 | -4.0 | -2.5 | 111,881 |
| SC | -3.9 | -4.1 | -0.1 | 82,563 |
| TN | -6.8 | -4.3 | -1.2 | 85,571 |

- **Likely cause:** one age fit over 8 states replaces 8 single-state fits. F1 and A1 are too small to explain it (KY 0.14 yr), but this was not isolated by refitting with one change at a time.
- **Which is right:** unknown. Natural-stand saplings have no field-age truth, so the verifier checks ordering, not level. Age monotonicity and the natural (h) step hold in all 8 states; (h) is INFO there, not enforced.
- **Phantom rows:** Sim A phantoms fall 8–29 % per state, the expected result of younger debuts, since a phantom needs a prior age of at least 1. Real Sim A rows are unchanged at each state's live count. Trajectory and cohort tables shrink accordingly.
