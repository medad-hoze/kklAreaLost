
import os
import ast 

import pandas    as pd
import geopandas as gpd


from arcgis.gis import GIS
from shapely    import wkt
from utiles     import read_layer_to_gdf,create_compilation


id_current   = '07d981b9fb364280bec5c79778fe6e5e'
id_lastMonth = '9f3bae6056a44a5bbd0275db5a87d7c7'


def load_WKT(x):
    try:
        wkt_ = wkt.loads(x.WKT)
        if not wkt_.is_valid:
            wkt_ = wkt_.buffer(0)
    except Exception as e:
        print("Error with WKT:", e)
        print(x)
        wkt_ = x
    return wkt_



def check_added_deleted_parcels(gis,output_added, output_deleted):


    item_current = gis.content.get(id_current)
    df_current   = item_current.layers[0].query(where='1=1', out_fields='*', return_geometry=True).sdf


    item_lastMonth = gis.content.get(id_lastMonth)
    df_lastMonth   = item_lastMonth.layers[0].query(where='1=1', out_fields='*', return_geometry=True).sdf

    df_current['GUSH'       ] = df_current['GUSH'       ].astype(str).str.replace('.0', '', regex=False)
    df_current['MisparHelka'] = df_current['MisparHelka'].astype(str).str.replace('.0', '', regex=False)

    df_lastMonth['GUSH'       ] = df_lastMonth['GUSH'       ].astype(str).str.replace('.0', '', regex=False)
    df_lastMonth['MisparHelka'] = df_lastMonth['MisparHelka'].astype(str).str.replace('.0', '', regex=False)


    df_current  ['key'] = df_current  ['GUSH'] +'_' + df_current  ['MisparHelka']
    df_lastMonth['key'] = df_lastMonth['GUSH'] +'_' + df_lastMonth['MisparHelka']


    df_merged       = pd.merge(df_current, 
                               df_lastMonth[['key']],
                                 on='key', how='outer', 
                                 indicator=True)
    
    df_newRows      = df_merged[df_merged['_merge'] == 'left_only']
    df_deletedRows  = df_merged[df_merged['_merge'] == 'right_only']
    df_deletedRows  = df_lastMonth[df_lastMonth['key'].isin(df_deletedRows['key'])]
    df_newRows      = df_current  [df_current  ['key'].isin(df_newRows  ['key'])]


    df_newRows    .to_excel(output_added, index=False)
    df_deletedRows.to_excel(output_deleted, index=False)

    return df_newRows, df_deletedRows


def esri_rings_to_wkt(rings_data):


    # try:
    ring = ast.literal_eval(rings_data)
    rings = ring['rings']

    
    if len(rings) == 0:
        return None
    
    exterior_ring = rings[0]
    coord_strings = [f"{x} {y}" for x, y in exterior_ring]
    coords_wkt    = ", ".join(coord_strings)
    wkt_string    = f"POLYGON (({coords_wkt}))"
    
    return wkt_string
    # except:
    #     print ('error in rings_data')
    #     return None




def add_mavat_name_by_overlap(gdf: gpd.GeoDataFrame,
                              gdf_mavat: gpd.GeoDataFrame,
                              mavat_col: str = "mavat_name",
                              work_crs: int = 2039):

    if mavat_col not in gdf_mavat.columns:
        raise ValueError(f"Column '{mavat_col}' not found in gdf_mavat.")

    out      = gdf.copy()
    gdf_w    = gdf.to_crs(work_crs)
    mavat_w  = gdf_mavat.to_crs(work_crs)

    gdf_w   = gdf_w[gdf_w.geometry.notna() & ~gdf_w.geometry.is_empty].copy()
    mavat_w = mavat_w[mavat_w.geometry.notna() & ~mavat_w.geometry.is_empty].copy()

    gdf_w["_rowid_"] = gdf_w.index

    left  = gdf_w[["_rowid_", "geometry"]]
    right = mavat_w[[mavat_col, "geometry"]]
    inter = gpd.overlay(left, right, how="intersection")

    if inter.empty:
        out[mavat_col] = pd.NA
        return out

    inter["ov_area"] = inter.geometry.area

    best = (inter.sort_values(["_rowid_", "ov_area"], ascending=[True, False])
                 .drop_duplicates("_rowid_")
                 .loc[:, ["_rowid_", mavat_col]])

    mapper = best.set_index("_rowid_")[mavat_col]
    out[mavat_col] = out.index.map(mapper)

    return out





username = 'medadhozekkl'
password = 'medadhozekkl123'
org      = 'https://kkl.maps.arcgis.com/home'
gis      = GIS(org, username, password)


#######################################################################################

script_folder  = os.path.dirname(os.path.abspath(__file__))
output_added   = os.path.join(script_folder, 'added_reports.xlsx')
output_deleted = os.path.join(script_folder, 'deleted_reports.xlsx')

data_folder    = os.path.dirname(script_folder) + '\\data'
if not os.path.exists(data_folder):
    os.makedirs(data_folder)

##########################  ייעודי קרקע   #####################################


# id_MAVAT = '4155083fb9944d7a9f528b9495c2c79a'

# item_mavat = gis.content.get(id_MAVAT)
# df_mavat   = item_mavat.layers[0].query(where='1=1').sdf

path_script = os.path.dirname(os.path.abspath(__file__))
path_main   = os.path.dirname(path_script)
path_data   = os.path.join(path_main, 'data')

layer_mavat  = path_data + '\\' + 'yahud_parcel.gpkg' +'\\' +'yahud_parcel'

gpkg         = path_data + '\\' + 'mavat_landuseResult.gpkg'
name_comp    = 'mavat_compilation'


# gdf_mavat_all   = read_layer_to_gdf(layer_mavat)

# if not os.path.exists(gpkg):

#     gdf_mavat = create_compilation(gdf_mavat_all, 'last_update_date')
#     gdf_mavat.to_file(gpkg, driver='GPKG', layer=name_comp)

# else:
#     print ('exists compilation')
#     gdf_mavat = gpd.read_file(gpkg, layer=name_comp)


path_temp_compi = r'C:\Users\medad\meidad\Work\KKL\NetWork_Report\data\data.gdb\mavat_compilation'
gdf_mavat = read_layer_to_gdf(path_temp_compi)



#######################################################################################


if os.path.exists(output_added) and os.path.exists(output_deleted):

    df_newRows     = pd.read_excel(output_added)
    df_deletedRows = pd.read_excel(output_deleted)

else:
    df_newRows, df_deletedRows = check_added_deleted_parcels(gis,output_added, output_deleted)


df_newRows    ['key'] = df_newRows    ['GUSH'].astype(str) +'-' + df_newRows    ['MisparHelka'].astype(str) +'-0'
df_deletedRows['key'] = df_deletedRows['GUSH'].astype(str) +'-' + df_deletedRows['MisparHelka'].astype(str) +'-0'


df = df_deletedRows[['HelkaMerhav','GUSH','MisparHelka','BaalutBefoal','ShetachBaalutKKL','key','SHAPE','SHAPE.STArea()']]

df.rename(columns={'SHAPE.STArea()':'Area'}, inplace=True)

df['Area'            ] = df['Area'            ].astype(int)
df['ShetachBaalutKKL'] = df['ShetachBaalutKKL'].astype(int)
df['precentage_kkl']   = (df['ShetachBaalutKKL'] / df['Area']) * 100
df['precentage_kkl']   = df['precentage_kkl'].round(1)

df.loc[(df['BaalutBefoal'] == 'קק"ל') & (df['precentage_kkl'] <= 105) & (df['precentage_kkl'] >= 95), 'precentage_kkl'] = 100


df['geometry'] = df['SHAPE'].apply(lambda x: wkt.loads(esri_rings_to_wkt(x)))
gdf            = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:2039")

# set gemetry crs 2039

gdf           = add_mavat_name_by_overlap(gdf      ,
                                          gdf_mavat,
                                          mavat_col = "mavat_name",
                                          work_crs  = 2039)


path_currreent = r'C:\Users\medad\meidad\Work\KKL\NetWork_Report\data\LAYERS.gpkg\PARCEL_ALL'
gdf_currreent  = read_layer_to_gdf(path_currreent)
gdf_current3   = gpd.overlay(gdf_currreent, gdf, how='intersection', keep_geom_type=True)



gdf_current3.rename(columns={'BaalutBefoal_1':'BaalutBefoal'}, inplace=True)

columns_to_remove = ['SHAPE','SHAPE_2','SHAPE_1']

for col in columns_to_remove:
    if col in gdf_current3.columns:
        gdf_current3.drop(columns=[col], inplace=True, errors='ignore')
    if col in gdf.columns:
        gdf.drop(columns=[col], inplace=True, errors='ignore')


gdf          .to_file(gpkg, driver='GPKG', layer='parcel_old')
gdf_current3.to_file(gpkg , driver='GPKG', layer='parcel_current')


#################  new parcels kkl  ###########################


df_newRows2 = df_newRows[['HelkaMerhav','TeurShita','GUSH','MisparHelka','BaalutBefoal','MatzavRishum','ShetachBaalutKKL','isKklInBaalutBefoal']]
gdf_newRows2 = gpd.GeoDataFrame(df_newRows2)
gdf_newRows2.to_file(gpkg , driver='GPKG', layer='parcel_added')

# id_current   = '07d981b9fb364280bec5c79778fe6e5e'
# item_current = gis.content.get(id_current)
# df_current   = item_current.layers[0].query(where='1=1').sdf



# df_current['geometry'] = df_current['SHAPE'].apply(lambda x: load_WKT(x))
# df_current             = df_current.drop(columns=['SHAPE'])
# gdf_current            = gpd.GeoDataFrame(df_current, geometry='geometry', crs="EPSG:2039")


