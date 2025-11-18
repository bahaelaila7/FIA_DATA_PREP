# pip install geopandas pyogrio shapely sqlalchemy sqlite3
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
import sys
# --- config ---
SQLITE_PATH = "./SQLite_FIADB_ENTIRE.db"
#POINTS_TABLE = ""            # columns: id, lon, lat, crs_flag ('WGS84' or 'NAD83')
POLY_PATH = "../EPA_ECOREGIONS/EPA_ECOREGIONS/us_eco_l4/us_eco_l4_no_st.shp"         # replace with your polygons
#POLY_ID_COL = "id"                 # unique polygon id column
OUT_PREFIX = "./FIA_EPA"
OUT_TABLE = "plot_epa"       # (point_id, polygon_id)

#print(pts_df)
#sys.exit()
# 1) read polygons
polys = gpd.read_file(POLY_PATH)            # uses pyogrio if installed
gdf = polys[['geometry']].copy()
gdf['EPA_L1'] = polys['NA_L1CODE']
gdf['EPA_L2'] = polys['NA_L2CODE']
gdf['EPA_L3'] = polys['NA_L3CODE']
gdf['EPA_L4'] = polys['NA_L3CODE'].str.cat(polys['US_L4CODE'], sep='.')
gdf.crs = gdf.crs or "EPSG:5070"
gdf['poly_area'] = gdf.geometry.area

#assert POLY_ID_COL in polys.columns, f"Missing {POLY_ID_COL}"
#polys.crs = polys.crs or "EPSG:5070"        # set if missing; pick your actual CRS
#polys["poly_area"] = polys.geometry.area    # tie-breaker if overlaps

# 2) read points from SQLite
eng = create_engine(f"sqlite:///{SQLITE_PATH}")
pts_df = pd.read_sql(f"""WITH  plots AS (
    SELECT P.STATECD, P.UNITCD, P.COUNTYCD, P.PLOT, P.LON, P.LAT, P.ECOSUBCD,
    (CASE WHEN SRV.RSCD=26 and P.STATECD IN (60, 64, 66, 68, 69,70) THEN 'WGS84' ELSE 'NAD83' END) DATUM,
    ROW_NUMBER() OVER (PARTITION BY P.STATECD, P.UNITCD, P.COUNTYCD, P.PLOT ORDER BY P.INVYR DESC) r
    FROM PLOTSNAP P
    JOIN SURVEY SRV ON SRV.ANN_INVENTORY = 'Y' AND 
    P.SRV_CN = SRV.CN AND (P.STATECD <= 56 and P.STATECD <> 2 and P.STATECD <> 15) 
    )

SELECT STATECD, UNITCD, COUNTYCD, PLOT, LON, LAT, DATUM, ECOSUBCD
FROM plots WHERE r = 1
""", eng)
# 3) build GeoDataFrames per CRS
def make_gdf(df, epsg):
    g = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df["LON"], df["LAT"]), crs=f"EPSG:{epsg}")
    return g.to_crs(polys.crs)  # reproject to polygon CRS

wgs = make_gdf(pts_df[pts_df["DATUM"].str.upper().isin(["WGS84","EPSG:4326"])], 4326)
nad = make_gdf(pts_df[pts_df["DATUM"].str.upper().isin(["NAD83","EPSG:4269"])], 4269)

pts = pd.concat([wgs, nad], ignore_index=True)
if len(pts) != len(pts_df):
    missing = set(pts_df["DATUM"].str.upper()) - {"WGS84","EPSG:4326","NAD83","EPSG:4269"}
    raise ValueError(f"Unknown DATUM values present: {missing}")

#pts.drop(['LON','LAT','DATUM'], axis=1, inplace = True)
# 4) spatial join
# Use `predicate="intersects"` to count boundary points; use "within" if boundaries should be excluded.
joined = gpd.sjoin(pts, gdf,
                   how="left", predicate="intersects")

# 5) resolve overlaps (pick smallest polygon by area; replace with your priority rule if needed)
joined = joined.sort_values(["STATECD","UNITCD","COUNTYCD","PLOT","poly_area"]).drop_duplicates(["STATECD","UNITCD","COUNTYCD","PLOT"], keep="first")

joined.to_file(f'{OUT_PREFIX}.gpkg')

# 6) persist (point_id, polygon_id) back to SQLite
#out = joined[["id", POLY_ID_COL]].rename(columns={"id":"point_id", POLY_ID_COL:"polygon_id"})

eng_out = create_engine(f"sqlite:///{OUT_PREFIX}.db")
joined[['STATECD','UNITCD','COUNTYCD','PLOT', 'LON','LAT','DATUM','ECOSUBCD', 'EPA_L1','EPA_L2','EPA_L3','EPA_L4']].to_sql(OUT_TABLE, eng_out, if_exists='replace', index=False)

