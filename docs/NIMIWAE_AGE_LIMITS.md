# NIMIWAE ages: known limitations

What consumers of the NIMIWAE tree ages (cohort birth dates, age-structure products) must know. Decisions: DECISIONS.md D77–D81.
Numbers: cv74flow, epoch 36, all five CV folds' held linked site trees (G1, 18,364 trees, D72 frame). Every age CV re-measures them:
`python -I nimiwae70_cv.py --out RUN --summary --data DATA` writes `age_limits` per epoch into `RUN/cv_summary.json`
(`nimiwae70_score.age_limits`).

## 1. Old trees: point estimates are shrunk toward the middle (accepted, D81)

The data cannot support a tight per-tree age for old trees. Among R4 site trees with a measured age (TRAIN), few are old,
and fewest in the fast-growing species, whose size stops tracking age:

| Species group | Aged trees | ≥ 60 yr | ≥ 80 yr | ≥ 100 yr |
|---|---|---|---|---|
| loblolly | 16,434 | 1,096 | 184 | 23 |
| slash | 3,486 | 338 | 59 | 5 |
| shortleaf + Virginia pine | 3,780 | 1,047 | 300 | 41 |
| longleaf | 1,842 | 533 | 105 | 15 |
| yellow-poplar | 5,768 | 1,281 | 214 | 22 |
| sweetgum | 2,781 | 451 | 60 | 2 |
| white oak | 3,696 | 2,504 | 1,042 | 214 |
| other oaks | 5,567 | 2,804 | 838 | 176 |

**Per-tree error by true age** (posterior median vs truth):

| True age | Trees | MAE (yr) | Median bias (yr) | Truth above the 90 % limit | below the 10 % limit |
|---|---|---|---|---|---|
| < 20 | 1,536 | 3.7 | +3.1 | 0.01 | 0.34 |
| 20–40 | 6,603 | 5.5 | +1.7 | 0.06 | 0.16 |
| 40–60 | 5,302 | 8.3 | −0.6 | 0.11 | 0.06 |
| 60–80 | 3,318 | 10.6 | −6.2 | 0.15 | 0.02 |
| 80+ | 1,605 | 19.6 | −17.7 | 0.39 | 0.00 |

**Old trees (≥ 60 yr) by species group:** loblolly bias −19.4 yr (435 trees), slash −18.0 (95), shortleaf + Virginia pine
−12.8 (490), yellow-poplar −11.8 (672), sweetgum −11.0 (177), longleaf −8.5 (201), other oaks −7.1 (1,451), white oak
−5.4 (1,262).

**What this is and is not:**
- The estimates are calibrated **given the prediction**: grouped by predicted age, the median of truth − prediction stays
  within −0.7 to +1.3 yr in every decile. The bias above appears only when trees are grouped by their **true** age, which
  is what a predictor with too little information does: a tree that looks 50 is predicted 50, even though some such trees
  are 80. Stretching the predictions would lower this bias and raise the error.
- **The posterior envelope is honest at population level.** Trees older than A, observed vs the sum of the posteriors'
  P(age > A) vs counted from the medians: 80 yr 1,482 / 1,496 / 699; 100 yr 285 / 347 / 31.
- Tried and rejected (D80 and its note): pooling the residuals of a plot's site trees (it makes legacy trees in managed
  stands worse), interval widths keyed to the predicted age (circular), species-specific growth-rate coupling (no gain
  beyond D74), stand-structure features in the direct prior (D80 gate failed). The truth noise floor is about 1.7 yr per
  record (re-cored site trees), so most of the error is the model's lack of information.

**Rule for products:** build age distributions, cohort birth dates and anything summed over trees from **posterior draws**
(`nimiwae56.sample_lineages`, which draws the age from its exact grid posterior), never from per-tree medians or
`impute` point summaries. Medians lose half the trees above 80 yr and almost all above 100 yr.

## 2. Planted stands, DIA ≥ 15 in: known exception (D79)

The planted-condition direct model is biased −1.03 yr with 80 % coverage 0.908 on validation trees ≥ 15 in (the bars are
±1.0 and 0.70–0.90). Flagged per tree: `B['age_known_bias']` (nimiwae70.build), returned by `sample_lineages` as
`age_known_bias`. A refit fails if the class drifts by more than 0.25 yr / 0.02 coverage (`nimiwae_age_direct.KNOWN_P1`).

## 3. Saplings outside plantations are unvalidated

R4 has no measured sapling ages outside planted stands, so natural saplings' ages rest on the direct prior (western
whorl-counted saplings, a different flora). The growth-rate coupling is not applied below 5 in: the western test showed
the mechanism, but it overshoots on R4 planted saplings (D78 judge).

## 4. Years to breast height

Total age = breast-height age + l0 (`nimiwae_l0.py`). l0 is measured in R4 only for planted loblolly (2.76 yr) and planted
slash (3.0 yr); western records use FIA's own species constants; every other species uses 2 yr (softwood) / 5 yr
(hardwood). FVS-SN's site curves were rejected: they give 0.84 yr for loblolly against the measured 2.76. l0 shifts total
ages by a constant per species, so it moves cohort birth dates, not the breast-height ages the scores rest on.

## 5. The truth itself

G1 truth is linked site trees: dominant and codominant trees, ring counts at breast height plus l0. Suppressed and
understory trees are not represented in the truth, so their age errors are not measured.
