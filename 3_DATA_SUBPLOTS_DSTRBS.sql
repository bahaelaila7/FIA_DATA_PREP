--- Exclude SUBPLOTS that had harvest or disturbances after its first measurement including DSTRYR=9999 (continual distrb).
DROP TABLE IF EXISTS DATA_SUBPLOTS_DSTRBS;
CREATE TABLE DATA_SUBPLOTS_DSTRBS AS
SELECT DSD.*, (TRUE = ANY(DSD.DSTRBS)) subp_has_dstrb, DSD.measdates[array_position(DSD.DSTRBS,TRUE)] first_dstrb_measdate
FROM (



SELECT 
DSCOND.STATECD, DSCOND.UNITCD, DSCOND.COUNTYCD, DSCOND.PLOT , DSCOND.SUBP, DSCOND.plt_cns, DSCOND.invyrs, DSCOND.measdates, DSCOND.subp_num_meas,
 array_agg( DSCOND.DSTRBs ORDER BY DSCOND.invyr) DSTRBs
FROM (
SELECT DS.*, v.plt_cn, v.invyr, bool_or(
		(Coalesce(cond.DSTRBCD1,0) != 0 AND coalesce(cond.DSTRBYR1,DATE_PART('year', DS.measdates[1])) >= DATE_PART('year', DS.measdates[1])) OR 
		(Coalesce(cond.DSTRBCD2,0) != 0 AND coalesce(cond.DSTRBYR2,DATE_PART('year', DS.measdates[1])) >= DATE_PART('year', DS.measdates[1])) OR
		(Coalesce(cond.DSTRBCD3,0) != 0 AND coalesce(cond.DSTRBYR3,DATE_PART('year', DS.measdates[1])) >= DATE_PART('year', DS.measdates[1])) OR
		(Coalesce(cond.TRTCD1,0) = 10 AND coalesce(cond.TRTYR1,DATE_PART('year', DS.measdates[1])) >= DATE_PART('year', DS.measdates[1]))OR
		(Coalesce(cond.TRTCD2,0) = 10 AND coalesce(cond.TRTYR2,DATE_PART('year', DS.measdates[1])) >= DATE_PART('year', DS.measdates[1]))OR
		(Coalesce(cond.TRTCD3,0) = 10 AND coalesce(cond.TRTYR3,DATE_PART('year', DS.measdates[1])) >= DATE_PART('year', DS.measdates[1]))
	 ) DSTRBs
FROM DATA_SUBPLOTS DS
CROSS JOIN LATERAL unnest(DS.PLT_CNS, DS.INVYRS) v(plt_cn,invyr) 
JOIN SUBP_COND SUBCOND ON 
	v.plt_cn = SUBCOND.PLT_CN 
	AND DS.SUBP = SUBCOND.SUBP
JOIN COND ON 
	v.plt_cn = COND.PLT_CN 
	AND SUBCOND.CONDID = COND.CONDID

GROUP BY DS.STATECD, DS.UNITCD, DS.COUNTYCD, DS.PLOT , DS.SUBP, DS.plt_cns, DS.invyrs, DS.measdates, DS.subp_num_meas, v.plt_cn, v.invyr) AS DSCOND

GROUP BY DSCOND.STATECD, DSCOND.UNITCD, DSCOND.COUNTYCD, DSCOND.PLOT , DSCOND.SUBP, DSCOND.plt_cns, DSCOND.invyrs, DSCOND.measdates, DSCOND.subp_num_meas) AS DSD;
