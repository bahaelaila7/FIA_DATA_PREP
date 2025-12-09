# pip install geopandas pyogrio shapely sqlalchemy sqlite3
import pandas as pd
import pandasql as pdsql
import geopandas as gpd
from sqlalchemy import create_engine
import sys
# --- config ---
PREFIX='/home/bma09868/data/perseus'
SQLITE_PATH = f"{PREFIX}/FIA/SQLite_FIADB_ENTIRE.db"
COUNTIES_PATH=f"{PREFIX}/FIA/tl_2024_us_county/tl_2024_us_county.shp"
#POINTS_TABLE = ""            # columns: id, lon, lat, crs_flag ('WGS84' or 'NAD83')
POLY_PATH = f"{PREFIX}/LANDFIRE/firezones/conus_mz_0k.shp"         # replace with your polygons
#POLY_ID_COL = "id"                 # unique polygon id column
OUT_PREFIX = "./FIA_FZ"
OUT_PREFIX_TMP = f"{OUT_PREFIX}_tmp"
OUT_TABLE = "plot_fz"       # (point_id, polygon_id)

def make_gdf(df, epsg):
    g = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df["LON"], df["LAT"]), crs=f"EPSG:{epsg}")
    return g.to_crs(gdf.crs)  # reproject to polygon CRS

def to_geom(pts_df):
    wgs = make_gdf(pts_df[pts_df["DATUM"].str.upper().isin(["WGS84","EPSG:4326"])], 4326)
    nad = make_gdf(pts_df[pts_df["DATUM"].str.upper().isin(["NAD83","EPSG:4269"])], 4269)
    pts = pd.concat([wgs, nad], ignore_index=True)
    if len(pts) != len(pts_df):
        missing = set(pts_df["DATUM"].str.upper()) - {"WGS84","EPSG:4326","NAD83","EPSG:4269"}
        raise ValueError(f"Unknown DATUM values present: {missing}")
    return pts

#print(pts_df)
#sys.exit()
# 1) read polygons
gdf = gpd.read_file(POLY_PATH)            # uses pyogrio if installed
gdf.crs = gdf.crs or "EPSG:5070"
gdf['poly_area'] = gdf.geometry.area

#assert POLY_ID_COL in polys.columns, f"Missing {POLY_ID_COL}"
#polys.crs = polys.crs or "EPSG:5070"        # set if missing; pick your actual CRS
#polys["poly_area"] = polys.geometry.area    # tie-breaker if overlaps

# 2) read points from SQLite
eng = create_engine(f"sqlite:///{SQLITE_PATH}")
pts_df = pd.read_sql(f"""WITH  plots AS (
    SELECT P.STATECD, P.UNITCD, P.COUNTYCD, P.PLOT, P.LON, P.LAT, PG.EMAP_HEX, PG.ECOSUBCD,
    (CASE WHEN SRV.RSCD=26 and P.STATECD IN (60, 64, 66, 68, 69,70) THEN 'WGS84' ELSE 'NAD83' END) DATUM,
    ROW_NUMBER() OVER (PARTITION BY P.STATECD, P.UNITCD, P.COUNTYCD, P.PLOT ORDER BY P.INVYR DESC) r
    FROM PLOT P
    LEFT OUTER JOIN PLOTGEOM PG ON  
        PG.STATECD = P.STATECD AND
        PG.UNITCD = P.UNITCD AND
        PG.COUNTYCD = P.COUNTYCD AND
        PG.PLOT = P.PLOT AND
        PG.INVYR = P.INVYR
    JOIN SURVEY SRV ON SRV.ANN_INVENTORY = 'Y' AND 
    P.SRV_CN = SRV.CN AND (P.STATECD <= 56 and P.STATECD <> 2 and P.STATECD <> 15) 
    )

SELECT P.STATECD, P.UNITCD, P.COUNTYCD, P.PLOT, P.LON, P.LAT, P.DATUM, P.EMAP_HEX, P.ECOSUBCD
FROM plots P
WHERE r = 1
""", eng)
# 3) build GeoDataFrames per CRS

pts = to_geom(pts_df)

#pts.drop(['LON','LAT','DATUM'], axis=1, inplace = True)
# 4) spatial join
# Use `predicate="intersects"` to count boundary points; use "within" if boundaries should be excluded.
joined = gpd.sjoin(pts, gdf,
                   how="left", predicate="intersects")


# 5) resolve overlaps (pick smallest polygon by area; replace with your priority rule if needed)
joined = joined.sort_values(["STATECD","UNITCD","COUNTYCD","PLOT","poly_area"]).drop_duplicates(["STATECD","UNITCD","COUNTYCD","PLOT"], keep="first")

assert len(joined) == len(pts_df)

joined.to_file(f'{OUT_PREFIX_TMP}.gpkg')

# 6) persist (point_id, polygon_id) back to SQLite
#out = joined[["id", POLY_ID_COL]].rename(columns={"id":"point_id", POLY_ID_COL:"polygon_id"})

eng_out = create_engine(f"sqlite:///{OUT_PREFIX_TMP}.db")
plots = joined[['STATECD','UNITCD','COUNTYCD','PLOT', 'LON','LAT','DATUM','EMAP_HEX', 'ECOSUBCD', 'ZONE_NUM']]
plots.to_sql(OUT_TABLE, eng_out, if_exists='replace', index=False)

plots_match = plots[~plots['ZONE_NUM'].isna()]
plots = to_geom(plots)
plots_mismatch = plots[plots['ZONE_NUM'].isna()].drop(columns=['ZONE_NUM'])

#joined = pd.read_sql('SELECT * FROM PLOT_EPA', con = eng_out)

# read counties from TIGER
counties_gdf_orig = gpd.read_file(COUNTIES_PATH).to_crs(gdf.crs)
counties_gdf_orig[['STATECD','COUNTYCD']] = counties_gdf_orig[['STATEFP','COUNTYFP']].astype('int64')

#
counties_join = gpd.sjoin(plots,counties_gdf_orig, how='inner', predicate='dwithin', distance=0)
counties_join_sql = counties_join[['STATECD_left', 'COUNTYCD_left', 'STATECD_right', 'COUNTYCD_right']].sort_values(['STATECD_left', 'COUNTYCD_left', 'STATECD_right', 'COUNTYCD_right'])

FIA2_TIGER_COUNTIES  = pdsql.sqldf('''
With county_match_counts AS (
    SELECT * , count(*) counts
    FROM counties_join_sql
    GROUP BY STATECD_left, STATECD_right , COUNTYCD_left , COUNTYCD_right
),
county_match_counts_highest AS (
    SELECT *, (STATECD_left = STATECD_right AND COUNTYCD_left = COUNTYCD_right) is_match, 
    RANK() OVER (PARTITION BY STATECD_left, COUNTYCD_left  ORDER BY counts DESC) rank
    FROM county_match_counts
)

SELECT 
STATECD_left STATECD,
COUNTYCD_left COUNTYCD,
STATECD_right STATECD_TIGER,
COUNTYCD_right COUNTYCD_TIGER,
is_match
FROM county_match_counts_highest WHERE rank = 1''')

plots_mismatch_tiger = plots_mismatch.merge(FIA2_TIGER_COUNTIES)
counties_mismatch= plots_mismatch_tiger[['STATECD_TIGER','COUNTYCD_TIGER']].drop_duplicates().sort_values(['STATECD_TIGER','COUNTYCD_TIGER']).rename(columns={'STATECD_TIGER':'STATECD','COUNTYCD_TIGER':'COUNTYCD'})

plots_mismatch_tiger = to_geom(plots_mismatch_tiger)
counties_gdf=counties_gdf_orig.merge(counties_mismatch,how='inner')
intersection = gpd.overlay(counties_gdf,gdf, how='intersection')
# Only one firezone polygon in the county
only_one_polygon = intersection.groupby(['STATECD','COUNTYCD']).filter(lambda g: len(g) == 1)
only_one_polygon = only_one_polygon[['STATECD','COUNTYCD', 'ZONE_NUM']]


plots_mismatch_one = plots_mismatch_tiger.merge(only_one_polygon, how='inner',left_on=['STATECD_TIGER','COUNTYCD_TIGER'],right_on=['STATECD','COUNTYCD'])
plots_mismatch_one = plots_mismatch_one.rename(columns={'STATECD_x':'STATECD','COUNTYCD_x':'COUNTYCD'})
       
plots_mismatch_one = plots_mismatch_one [['STATECD', 'UNITCD', 'COUNTYCD', 'PLOT', 'LON', 'LAT', 'DATUM',
       'EMAP_HEX', 'ECOSUBCD', 'ZONE_NUM']]

more_than_one = intersection.groupby(['STATECD','COUNTYCD']).filter(lambda g: len(g) != 1)
plots_mismatch_more = plots_mismatch_tiger.merge(more_than_one, how='inner',left_on=['STATECD_TIGER','COUNTYCD_TIGER'],right_on=['STATECD','COUNTYCD'])
plots_mismatch_more['dist'] = plots_mismatch_more.apply(lambda row: row['geometry_x'].distance(row['geometry_y']), axis=1)
plots_mismatch_more = plots_mismatch_more.rename(columns={'STATECD_x':'STATECD','COUNTYCD_x':'COUNTYCD'})
plots_mismatch_more = plots_mismatch_more.sort_values(['STATECD', 'UNITCD', 'COUNTYCD', 'PLOT', 'LON', 'LAT', 'DATUM',
       'EMAP_HEX', 'ECOSUBCD', 'dist']).drop_duplicates(['STATECD', 'UNITCD', 'COUNTYCD', 'PLOT', 'LON', 'LAT', 'DATUM',
       'EMAP_HEX', 'ECOSUBCD'], keep='first')[['STATECD', 'UNITCD', 'COUNTYCD', 'PLOT', 'LON', 'LAT', 'DATUM',
       'EMAP_HEX', 'ECOSUBCD','ZONE_NUM']]

full_plots = pd.concat([plots_match, plots_mismatch_one, plots_mismatch_more]).sort_values(['STATECD','UNITCD','COUNTYCD','PLOT'])

eng_out = create_engine(f"sqlite:///{OUT_PREFIX}.db")
full_plots.to_sql(OUT_TABLE, eng_out, if_exists='replace', index=False)
