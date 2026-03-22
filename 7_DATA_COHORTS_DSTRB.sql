-- GROUP into cohorts of subplot, ecoregion, species, age. Calculate biomass
-- FIA reports biomass as lbs/acre, LANDIS wants g/m2
-- drybio_ag * TPA_UNADJ is in lbs/acre , so multiply by 0.112085 to become g/m^2 
-- 1 lbs/acre * (1000/2.20462) g/lbs*   (1/4840) (acre/sq yard) * (1/0.9144)^2 (yard / m)^2  = 0.112085

DROP TABLE IF EXISTS DATA_COHORTS_DSTRB;
CREATE TABLE DATA_COHORTS_DSTRB AS

WITH Grouped_COHORTS AS (
SELECT v.PLT_CN, T.STATECD, T.UNITCD, T.COUNTYCD, T.PLOT,T.SUBP, v.measdate, T.SPECIES_SYMBOL, T.SPGRPCD, T.subp_has_dstrb, T.first_dstrb_measdate,
CAST ( round(v.age_calc) AS INTEGER) age_calc, sum(v.drybio_ag*v.TPA_UNADJ)*0.112085 AGB, sum(v.TPA_UNADJ) tree_count
FROM DATA_SUBPLOTS_TREES_AGES_CALC T
CROSS JOIN LATERAL unnest(T.PLT_CNs, T.drybio_ags, T.statuscds, T.ages_calc, T.measdates, T.TPA_UNADJs)
v(PLT_CN, drybio_ag,statuscd, age_calc, measdate, TPA_UNADJ)
WHERE v.statuscd = 1 and v.age_calc is not null and v.drybio_ag is not null and v.TPA_UNADJ > 0.0
GROUP BY v.PLT_CN, T.STATECD, T.UNITCD, T.COUNTYCD, T.PLOT,T.SUBP, v.measdate, T.SPECIES_SYMBOL, T.SPGRPCD, T.SFTWD_HRDWD, T.subp_has_dstrb, T.first_dstrb_measdate, age_calc),

Measurements AS (
SELECT STATECD, UNITCD, COUNTYCD, PLOT, min(measdate) first_measdate, count(distinct measdate) plot_meas_num
FROM Grouped_COHORTS
GROUP BY STATECD, UNITCD, COUNTYCD, PLOT)

SELECT G.*, m.first_measdate, m.plot_meas_num, (G.measdate - m.first_measdate) / 365.25 SIM_YEAR
FROM GROUPED_COHORTS G
JOIN Measurements m on 
G.STATECD=m.STATECD AND
G.UNITCD=m.UNITCD AND
G.COUNTYCD=m.COUNTYCD AND
G.PLOT = m.PLOT
