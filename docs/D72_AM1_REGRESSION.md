# D72 regression: live truth DB vs D72 truth DB

old `../truth/fia_truth_r4_d72am0.duckdb`, new `../truth/fia_truth_r4_d72.duckdb`. Row sets compared with EXCEPT ALL on the shared columns.

Tables only new: none. Only old: none.

| Table | Rows old | Rows new | Only old | Only new | New columns |
|---|---|---|---|---|---|
| run_scope | 1 | 1 | 1 | 1 | — |
| run_steps | 8 | 8 | 8 | 8 | — |

**Identical on shared columns (48):** COND, PLOT, REF_FOREST_TYPE, REF_FOREST_TYPE_GROUP, REF_SPECIES, REF_SPECIES_GROUP, SEEDLING, SITETREE, SUBPLOT, SUBP_COND, TREE, age_model_cv, age_recal_cells, base_subplot_cond, clean_subplot_years, cohort_disturbance, cond_link, curated_cohorts, curated_cohorts_landis, curated_cohorts_landis_simB, curated_cohorts_landis_stdorg, curated_snapshots, curated_trees, data_plot_eco, estage_calibration, excluded_tree_ids, id_quarantine, longitudinal_subplots, phantom_cohort_sources, phantom_tree_sources, plot_key_remap, plot_location, plot_renumber, plot_segments, procedural_breaks, product_visits, rc7_withdrawn_visits, retally_transitions, sapling_growth_rates_simA, sapling_growth_rates_traj, species_longevity_ref, tree_censor_events, tree_end, tree_landuse_exits, tree_reconcile_events, tree_status0_events, tree_trajectories, visit_status.

## curated_trees by column

4,522,556 records on the natural key; only old 0, only new 0.

| Column | Records that differ |
|---|---|

## final_tree_fate, old -> new (trees)

| old | new | trees |
|---|---|---|

## tree_end (new)

| end_kind | end_reason | cause | trees |
|---|---|---|---|
| alive | alive | None | 958,509 |
| cut | silvicultural_death | cut | 360,511 |
| death | dead | death | 284,370 |
| censored | censored_procedural | None | 64,585 |
| censored | censored_nonsampled | None | 35,018 |
| cut | diverted_landuse | cut | 29,065 |
| diverted | diverted_landuse | None | 9,091 |
| cut | harvest_inferred | cut | 4,705 |
| censored | censored_procedural_unrecorded | None | 1,711 |
| censored | censored_shrank | None | 1,000 |
| censored | censored_moved | None | 411 |
| inferred_removal | disappeared_inferred_removal | cut | 21 |

## tree_trajectories: rows by intro_type

| intro_type | old | new | only old | only new |
|---|---|---|---|---|
| gap_interpolated | 434 | 434 | 0 | 0 |
| phantom_backfill | 1,091,804 | 1,091,804 | 0 | 0 |
| real | 3,729,318 | 3,729,318 | 0 | 0 |

## curated_cohorts_landis: rows by intro_type

| intro_type | old | new | only old | only new |
|---|---|---|---|---|
| phantom_backfill | 158,576 | 158,576 | 0 | 0 |
| real | 2,913,902 | 2,913,902 | 0 | 0 |

## curated_cohorts_landis_stdorg: rows by intro_type

| intro_type | old | new | only old | only new |
|---|---|---|---|---|
| phantom_backfill | 158,576 | 158,576 | 0 | 0 |
| real | 2,913,902 | 2,913,902 | 0 | 0 |

## tree_trajectories terminal_fate, old -> new (trees)

| old | new | trees |
|---|---|---|

**curated_cohorts_landis subp_has_dstrb:** 129,227 subplots; disturbed -> not 0; not -> disturbed 0.

**curated_cohorts_landis_simB subp_has_dstrb:** 126,256 subplots; disturbed -> not 0; not -> disturbed 0.

**cohort_disturbance:** rows 263,208 -> 263,208; full removals 237,015 -> 237,015.
