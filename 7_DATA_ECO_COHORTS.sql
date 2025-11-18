-- GROUP into cohorts of subplot, ecoregion, species, age. Calculate biomass
-- FIA reports biomass as lbs/acre, LANDIS wants g/m2
-- drybio_ag * TPA_UNADJ is in lbs/acre , so multiply by 0.112085 to become g/m^2 
-- 1 lbs/acre * (1000/2.20462) g/lbs*   (1/4840) (acre/sq yard) * (1/0.9144)^2 (yard / m)^2  = 0.112085
DROP TABLE IF EXISTS DATA_ECO_COHORTS;
CREATE TABLE DATA_ECO_COHORTS AS

SELECT T.STATECD, T.UNITCD, T.COUNTYCD, T.PLOT,T.SUBP, SP_MAP.ECO, v.measdate, SP_MAP.SPECIES_SYMBOL_MAP, 
(v.measdate - T.measdates[1]) / 365.25 SIM_YEAR, v.age_calc, sum(v.drybio_ag*v.TPA_UNADJ)*0.112085 AGB

FROM DATA_SUBPLOTS_TREES_NO_DSRTRBS_AGES_CALC T
JOIN DATA_PLOT_EPA EPA ON
T.STATECD = EPA.STATECD AND T.UNITCD = EPA.UNITCD AND
T.COUNTYCD = EPA.COUNTYCD AND T.PLOT = EPA.PLOT
JOIN DATA_SPECIES_EPA_L4_MAP SP_MAP ON
T.SPECIES_SYMBOL = SP_MAP.SPECIES_SYMBOL AND SP_MAP.ECO = EPA.EPA_L4
CROSS JOIN LATERAL unnest(T.drybio_ags, T.statuscds, T.ages_calc, T.measdates, T.TPA_UNADJs)
v(drybio_ag,statuscd, age_calc, measdate, TPA_UNADJ)
WHERE v.statuscd = 1
GROUP BY T.STATECD, T.UNITCD, T.COUNTYCD, T.PLOT,T.SUBP, SP_MAP.ECO,v.measdate, SP_MAP.SPECIES_SYMBOL_MAP, v.age_calc,
SIM_YEAR
HAVING COALESCE(sum(v.drybio_ag*v.TPA_UNADJ)*0.112085 ,0)  > 0;
