import psycopg2
import argparse
from pathlib import Path

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dbname', type=str)
    parser.add_argument('--user', type=str)
    parser.add_argument('--host', type=Path)
    parser.add_argument('--port', type=int)
    
    args = parser.parse_args()
    assert args.host.exists(), f"{args.host} does not exist"
    with psycopg2.connect(dbname = args.dbname, host=args.host, port=args.port, user=args.user) as con:
        for ECO in ['EPA_L3','EPA_L4','ECOSUBCD','ZONE_NUM']:
            print(f"Creating Species mapping by {ECO}")
            sql=f"""
            ----- repeat for EPA_L3 and ECOSUBCD
            DROP TABLE IF EXISTS DATA_SPECIES_{ECO}_MAP;
            CREATE TABLE DATA_SPECIES_{ECO}_MAP AS
            WITH COUNTS AS (
                SELECT E.{ECO} ECO, T.SPECIES_SYMBOL, T.sftwd_hrdwd , count(*) c 
                FROM DATA_SUBPLOTS_TREES_AGES_CALC T
                JOIN DATA_PLOT_ECO E ON
                T.STATECD = E.STATECD AND T.UNITCD = E.UNITCD AND
                T.COUNTYCD = E.COUNTYCD AND T.PLOT = E.PLOT
                --T.UNITCD, T.COUNTYCD, T.PLOT, T.SUBP, T.SPCD,
                --WHERE T.STATECD=12 AND T.COUNTYCD IN (125,121,003,023,047)
                GROUP BY ECO, T.SPECIES_SYMBOL, T.sftwd_hrdwd
            ),
            TOTAL AS (
                SELECT ECO, SUM(c) total FROM COUNTS GROUP BY ECO
            ),
            CUMSUMS AS (
                SELECT  COUNTS.ECO, SPECIES_SYMBOL,sftwd_hrdwd,c, 
                (sum(c) OVER (PARTITION BY COUNTS.ECO ORDER BY c DESC, SPECIES_SYMBOL ASC)*100.0/Total.total) cumsum,
                ROW_NUMBER() OVER (PARTITION BY COUNTS.ECO ORDER BY c DESC, SPECIES_SYMBOL ASC) r
                FROM COUNTS
                JOIN TOTAL ON COUNTS.ECO = TOTAL.ECO
                --ORDER BY cumsum ASC,SPECIES_SYMBOL ASC
            )

            SELECT ECO, SPECIES_SYMBOL, sftwd_hrdwd, c, cumsum , r, 
            (CASE WHEN r > 13 OR CUMSUM > 90.0 OR c < 100  THEN CONCAT(ECO,'_', SFTWD_HRDWD) ELSE SPECIES_SYMBOL END) SPECIES_SYMBOL_MAP
            FROM CUMSUMS
            ORDER BY ECO ASC, CUMSUM ASC, SPECIES_SYMBOL ASC;
            """
            cur=con.cursor()
            cur.execute(sql)
            con.commit()
