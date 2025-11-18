--- GET all trees with age data from never disturbed subplots
DROP TABLE IF EXISTS DATA_SUBPLOTS_TREES_NO_DSRTRBS_AGES_CALC;
CREATE TABLE DATA_SUBPLOTS_TREES_NO_DSRTRBS_AGES_CALC AS
SELECT T.STATECD, T.UNITCD, T.COUNTYCD, T.PLOT, T.SUBP, T.subp_num_meas, T.subp_has_dstrb, T.TREE,
T.TREE_CNS,PREV_TRE_CNS,PLT_CNS,INVYRs,CONDIDS,SPCDs,SPGRPCDs,TPA_UNADJs,dias,  stockings,  DRYBIO_AGs, HTs,measdates,STATUSCDs,STANDING_DEAD_CDs,RECONCILECDs,
T.SPCD, T.SPGRPCD, T.SPECIES_SYMBOL, T.sftwd_hrdwd, T.GENUS,
array_agg(
GREATEST(1, ROUND(T.age_calc + (vv.measdate - T.age_measdate ) /(365.25)))
ORDER BY vv.invyr
) ages_calc

FROM (
--- FIND the last treeage estimate from TOTAGE or AGE_CALC and apply it to other measurements shifting age by measdate
SELECT TT.*, 

(CASE WHEN totage_idx is not null then totages[totage_idx] ELSE ages[age_idx] END) age_calc,
(CASE WHEN totage_idx is not null then measdates[totage_idx] ELSE measdates[age_idx] END) age_measdate,
(CASE WHEN totage_idx is not null then invyrs[totage_idx] ELSE invyrs[age_idx] END) age_invyr,
SP.GENUS, SP.SPECIES_SYMBOL, SP.sftwd_hrdwd
FROM (
SELECT *, 
totage_idx_non_null[array_upper(totage_idx_non_null,1)] totage_idx,
age_idx_non_null[array_upper(age_idx_non_null,1)] age_idx,
SPCDs[array_upper(SPCDs,1)] SPCD,
SPGRPCDs[array_upper(SPGRPCDs,1)] SPGRPCD
FROM (
SELECT DST.*, 
(SELECT array_agg(sub.idx) FROM generate_subscripts(DST.totages,1) sub(idx)
WHERE DST.totages[sub.idx] is not null)  totage_idx_non_null, 

(SELECT array_agg(sub.idx) FROM generate_subscripts(DST.ages,1) sub(idx)
WHERE DST.ages[sub.idx] is not null) age_idx_non_null FROM
DATA_SUBPLOTS_TREES DST
WHERE NOT subp_has_dstrb AND subp_num_meas > 1 
AND NOT EXISTS(
SELECT 1 FROM unnest(drybio_ags, dias, statuscds) v(agb, dia, statuscd)
WHERE (statuscd =1 and agb is null))
AND (
EXISTS(
SELECT 1 FROM unnest(ages, statuscds) v(age, statuscd)
WHERE (statuscd =1 and age is not null)
)
OR
EXISTS(
SELECT 1 FROM unnest(totages, statuscds) v(totage, statuscd)
WHERE (statuscd =1 and totage is not null)
)
)

)
) TT
JOIN REF_SPECIES SP ON TT.SPCD = SP.SPCD
) T
CROSS JOIN LATERAL unnest(T.measdates, T.invyrs) vv(measdate, invyr)
GROUP BY T.STATECD, T.UNITCD, T.COUNTYCD, T.PLOT, T.SUBP, T.subp_num_meas, T.subp_has_dstrb, T.TREE,
T.TREE_CNS,PREV_TRE_CNS,PLT_CNS,INVYRs,CONDIDS,SPCDs,SPGRPCDs,TPA_UNADJs,dias,  stockings,  DRYBIO_AGs, HTs,measdates,STATUSCDs,STANDING_DEAD_CDs,RECONCILECDs,T.SPCD, T.SPGRPCD, T.SPECIES_SYMBOL, T.sftwd_hrdwd, T.GENUS;
