# Curation pipeline vs FIADB User Guide v9.4 — semantic audit (2026-11-01)

Source of truth: *The Forest Inventory and Analysis Database: Database Description and User Guide*, version 9.4 (Aug 2025),
local copy `/workspace/wo-v9-4_Aug2025_UG_FIADB_database_description_NFI.pdf` (§ numbers below). Code at `267f137` (branch `curation-fix`).

**Scope.** The 13 curation scripts:
- the main chain: dbconn, fia_curation_v3, age_imputation, cohort_tracking, cohorts_landis, build_tree_trajectories, cohorts_landis2, build_stdorg;
- Sim B: cohorts_landis_simB, attach_disturbance_simB;
- cohort_disturbance, export_reference_tables, verify_curation_v2.

**Method.**
- Every identifier was matched against the FIADB schema: 113 FIADB columns are used.
- Each field's User Guide definition and code table was extracted.
- Every rule that interprets a field was checked against that text.
- Data checks are restricted to the 8 states (AL FL GA KY MS NC SC TN), annual inventory.

## Mismatches (the code contradicts the manual)

| # | Where | Rule in code | Manual | Evidence | Impact |
|---|---|---|---|---|---|
| **F1** | `fia_curation_v3.detect_condition_resets`, `assign_stdage` (`COND_ID = STATECD, UNITCD, COUNTYCD, PLOT, CONDID`, no INVYR) | A condition is followed across visits by its CONDID: an "STDAGE drop since the condition's previous visit", live-tree / BA loss, `cond_ever_reset`, `stdage_cond_first_reset_measdate` | §2.5.8 CONDID: "each sampled condition class must have a unique number that **can change at remeasurement** to reflect new conditions on the plot". Cross-visit linkage is SUBP_COND_CHNG_MTRX (§2.9: CONDID ↔ PREVCOND per subplot with SUBPTYP_PROP_CHNG) or TREE.PREVCOND | FIA's change matrix (8 states, annual): **6.4 %** of current conditions have no area that carried the same CONDID at the previous visit; **12.7 %** of condition area maps to a different previous CONDID. D40 patched only the tree count for re-delineation | Reset detection, hence STDAGE eligibility of post-reset trees (the planted / reset age chain). The `STDAGE trajectory anomalies` block (≈ l. 2320) makes the same pairing but is a printed diagnostic only |
| F2 | NIMIWAE prep `load54` (not curation) | A borrowed nearest-record CONDID at visits without a record | §2.5.8 as above | — | **Fixed: D55 am. 7** |
| F3 | NIMIWAE `build` (not curation) | RC9 (nonsampled) absence treated as informative after the record | §3.1.82 RC9 "now in a nonsampled condition" | 10,543 trees | **Fixed: D55 am. 5** |
| ~~F4~~ | `verify_curation_v2` check (k) | **Withdrawn 2026-11-01: not a bug.** r7 E28 already removed the by-number SITETREE link; only the proxy link (same plot + condition + species + DIA) remains. A stale comment above it misled this audit | §3.10.10 | — | Stale comment only |

## Approximations / decisions to confirm (consistent with the manual's letter, contestable in substance)

| # | Rule | Manual | Note |
|---|---|---|---|
| A1 | STDAGE is assigned to ALL trees in STDORGCD = 1 conditions (FLDSZCD gate), regardless of species | §2.5.18 STDAGE = FLDAGE = "the **average** total age … of the trees in the field-recorded stand-size class"; §2.5.26 STDORGSP = the predominant artificially regenerated species | Natural ingrowth of other species inherits the plantation age. `matches_stdorgsp` is computed but only reported; the option is to require an STDORGSP match (or softwood group) |
| A2 | AGENTCD 80 ⇒ "harvest" (`harvest_explicit`) | §3.1.27 code 80: death by "harvesting **or other silvicultural activity, including girdling, chaining** … or landclearing" | Includes killed-in-place trees; "silvicultural death" is the accurate label. STATUSCD 3 (Removed) is a retired code (§3.1.15: from MANUAL 9.3, RSCD 23 / 24 only); it never occurs on these states' annual remeasurements (cuts are STATUSCD 2 + AGENTCD 80, ≈ 454k records), and the rule covers both |
| A3 | DSTRBCD 60 is not a disturbance (`disturbed`, D41) | §2.5.32 code 60 = "Vegetation (suppression, competition, vines)" | A judgment, consistent with the code's meaning |
| A4 | SUBPLOT / SUBP_COND / COND joined to PLOT by (STATECD, UNITCD, COUNTYCD, PLOT, INVYR), not PLT_CN | §2.4.7: PLOT with STATECD, INVYR, UNITCD, COUNTYCD "may be used to uniquely identify a plot" | Correct as written, and the safer join. **Corrected 2026-11-01:** an earlier version of this note called PLT_CN "the robust key"; the opposite holds (bahaelaila7/FIA_DATA_PREP#1 and the identifier audit below). Natural keys are unique nationally; a natural-key join drops child rows whose key names no plot, a PLT_CN join admits them |
| A5 | KINDCD is not used | §2.4.16 KINDCD: 1 = initial installation **or resampling after a nonsampled visit**, 3 = replacement plot; "revisited plots with KINDCD = 1, 3 are not used for remeasurement estimates" | KINDCD 3: 75 visits, none with an earlier visit on the key. KINDCD 1 with an earlier visit: 2,001 (728 forested); 3 of 19,167 live trees share a number with an earlier visit (none with PREV_TRE_CN). Negligible; add an explicit guard (no lineage across KINDCD 1 / 3) |
| A6 | DSTRBYR / TRTYR are carried through | §2.5.38: "If the disturbance occurs continuously … 9999" | Consumers (e.g. the genveg prior) must treat 9999 as "continuous", not as a year |
| A7 | TREE numbers track trees across visits | §3.1.10 TREE: "can be used to track trees **when PLOT.DESIGNCD is the same** between inventories" | All 209,100 annual visits here are DESIGNCD 1; 0 design changes. Add a guard for other regions |

## Confirmed consistent

| Rule / field | Manual |
|---|---|
| STATUSCD 0 record dropped; RC7 on STATUSCD 0 ⇒ the tree's earlier records dropped; RC 5 / 6 / 8 / 9 ⇒ censoring | §3.1.15 (0 = "not presently in the sample … incorrectly tallied … procedural change … nonsampled condition"), §3.1.82 RC 5–9 |
| New trees with RC 3 / 4 / 6 / 7 / 8 / 9 kept, flagged `reconcile_new`; RC 1 / 2 = ingrowth / through growth | §3.1.82 (MANUAL ≥ 9.0: 1–2 exclude procedural / cruiser-error trees; RC3 / 4 retired at 9.0) |
| RC5 not revived | §3.1.82 code 5 "shrank … no longer qualify as tally (STATUSCD 0)" |
| Re-records: PREV_STATUS_CD = 2 dead / cut records are not events | §3.1.90 |
| Tree linkage PREV_TRE_CN; plot linkage PREV_PLT_CN (canonical key, M-2) | §3.1.3, §2.4.4 (annual-to-annual only) |
| Live records: TPA_UNADJ > 0 on the record's OWN condition (TREE.CONDID at that visit) with COND_STATUS_CD = 1 | §3.1.92, §2.5.9 (same-visit join is the documented use of TREE.CONDID) |
| SUBP ≤ 4; SUBP_STATUS_CD 3 excluded; PLOT_STATUS_CD < 3; P2A_GRM_FLG excluded; SURVEY.ANN_INVENTORY = 'Y' | §2.6.10 (3 = nonsampled), §2.4.10, §2.6.19, §2.1.8 |
| Forested proportions = Σ SUBPCOND_PROP / MICRCOND_PROP over COND_STATUS_CD = 1 | §2.7.16–17, §2.5.9 |
| Tally levels from data (TPA 74.965 microplot 1 in, 6.018 subplot 5 in; macroplot 0.999 not used here) | §3.1.92; MACRO_BREAKPOINT_DIA null (§2.4.40, no macroplots) |
| DRYBIO_AG = pounds per tree, wood + bark, excl. foliage, × TPA_UNADJ for per-acre values | §3.1.133 |
| STDAGE anchors require STDAGE > 0 (0 = nonstocked); FLDAGE sentinels 998 / 999 never used as ages; the FLDSZCD gate | §2.5.18, §2.5.51, §2.5.20 |
| TOTAGE / BHAGE absent in the South | §3.1.56–57 (RSCD 22, 26 only) |
| SITETREE.AGEDIA + 2 / 5 yr = total age | §3.10.14 (age at d.b.h.) |
| DAMSEV not used as a percentage (r6 D25) | §3.1.31 ("severity codes vary by damage type and location") |
| TRTCD 10 cut, 30 / 40 regeneration, 50 other; recorded "since the last measurement" | §2.5.43 |
| Cut-event classes keyed by subplot (r7) | (avoids F1) |
| MEASDATE = MAKE_DATE(MEASYEAR, MEASMON or 6, MEASDAY or 15) | §2.4.12–14 (MEASDAY 0 in periodic data) |

## Recommended fixes

1. **F1.** Link condition-visits with SUBP_COND_CHNG_MTRX, or define resets per tree: the tree's condition at this visit vs its previous condition through TREE.PREVCOND, with STDAGE drop + loss measured on the conditions FIA links. Re-run curation steps 1–11 (8 states) and the verifier, then rebuild the downstream products (slim, NIMIWAE prep).
2. ~~F4~~ (withdrawn).
3. **A5 / A7.** Explicit guards: no lineage across KINDCD 1 / 3 visits or DESIGNCD changes. Zero effect here; protects other regions.
4. **A1, A2.** User decision: species-matched STDAGE; the harvest vs silvicultural-death label.

## Identifier audit: CN vs natural id (2026-11-01; user: "verify that CN keys actually uniquely match the natural identifier", case bahaelaila7/FIA_DATA_PREP#1)

Scripted over every FIADB table with PLT_CN (45 tables), nationally and for the 8 states.

| Check | National | 8 states |
|---|---|---|
| Duplicate natural keys: PLOT, SUBPLOT, COND, SUBP_COND, TREE | 0 | 0 |
| Duplicate CN in those tables | 0 | 0 |
| Rows whose natural key differs from their PLT_CN's PLOT row: TREE | 82,557 | 0 |
| — SUBPLOT | 55,149 | 0 |
| — COND | 5,904 | 0 |
| — SITETREE | 21 | 0 |
| — SUBP_COND | 14 | 0 |
| TREE without a SUBPLOT row for (PLT_CN, SUBP) | 2,889,179 | 68,797 (all periodic, DESIGNCD 211 / 217, SUBP > 4) |
| TREE / SUBP_COND without COND; SUBP_COND without SUBPLOT | 0 | 0 |
| PREV_PLT_CN dangling | 34,488 | 105 (104 periodic 1980s; 1 annual AL, KINDCD 1) |
| PREV_PLT_CN: plot key changes | 1,803 | 1,785 (1,751 KY periodic, same location; 34 annual) |
| Forks: 2+ records sharing a PREV_PLT_CN / PREV_TRE_CN | 0 | 0 |
| PREV_TRE_CN dangling | 50,287 | 0 |
| PREV_TRE_CN: SUBP or TREE number differs; previous tree not on PREV_PLT_CN | 0 | 0 |
| PREV_TRE_CN: PREVDIA differs from the previous DIA by > 0.05 in | 847 | 23 |
| SUBP_COND_CHNG_MTRX: PREV_PLT_CN differs from PLOT | 12 | 0 |
| SUBP_COND_CHNG_MTRX: CONDID / PREVCOND not in COND | 0 | 0 |

**Mismatches are localised:** Oklahoma periodic (SUBPLOT 55,140, COND 5,901, TREE 82,279), Arkansas periodic (TREE 259), one Missouri annual plot (the issue's 29/4/41/10213, DESIGNCD 319), one Oregon annual plot. In all of them except a few SITETREE rows, the child's natural key names NO real PLOT row: the row hangs off the plot only through its PLT_CN.

**The 34 annual key-changing PREV_PLT_CN links (8 states):** 29 moved > 1 km in public coordinates, nearly all changed county. The trees agree they are one plot: 1,042 linked trees, PREVDIA matches on 1,042, same species on 1,002, DIA non-decreasing on 732 of 739 live. 5 links have no trees to test.

**Rule (user, 2026-11-01): do not trust CN keys; rely on natural ids.**
- Identity and every join use natural keys. CNs are carried as unverified attributes and are never join keys.
- A row whose CN and natural key disagree is quarantined with a reason, not repaired.
- Where FIA's PREV_* link states something the natural keys do not (a changed plot key), it enters only through an explicit, evidence-checked table (`plot_renumber`), never silently.
