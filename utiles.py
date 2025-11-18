
import geopandas as gpd
import os
from datetime import datetime
from tqdm import tqdm
from shapely.validation import make_valid
import warnings
warnings.filterwarnings('ignore')

def print_message(msg, status=1):
    prefixes = {
        1: '[info]',
        2: '[!warning!]',
        3: '[!FINISH!]',
        0: '[!!!err!!!]'
    }
    prefix = prefixes.get(status, '[info]')
    msg    = f"{prefix} {datetime.now()} {msg}"
    print(msg)


def find_driver(path_layer):

    directory, filename = os.path.split(path_layer)
    if directory.endswith('.gdb'):
        driver = 'FileGDB'
    elif directory.endswith('.gpkg'):
        driver = 'GPKG'
    elif filename.endswith('.shp'):
        driver = 'ESRI Shapefile'
    else:
        driver = 'ESRI Shapefile'
    return driver




def read_layer_to_gdf(path_layer):

    directory, filename = os.path.split(path_layer)
    driver = find_driver(path_layer)

    if (driver == 'FileGDB') or (driver == 'GPKG'):
        gdf = gpd.read_file(directory, driver=driver, layer=filename)
    else:
        gdf = gpd.read_file(path_layer)

    return gdf


def clean_geometries(gdf):

    gdf = gdf[gdf.geometry.notna()].copy()
    gdf = gdf[~gdf.geometry.is_empty].copy()
    
    invalid_count = (~gdf.geometry.is_valid).sum()
    if invalid_count > 0:
        gdf['geometry'] = gdf.geometry.apply(lambda geom: make_valid(geom) if not geom.is_valid else geom)
    
    gdf['geometry'] = gdf.geometry.buffer(0)
    gdf = gdf[gdf.geometry.is_valid].copy()
    gdf = gdf[~gdf.geometry.is_empty].copy()
    
    print(f"Final count: {len(gdf)} valid features")
    
    return gdf


def convert_to_single_polygons(gdf):

    def extract_polygons(geom):
        if geom.geom_type == 'GeometryCollection':
            polys = [g for g in geom.geoms if g.geom_type in ['Polygon', 'MultiPolygon']]
            if len(polys) == 1:
                return polys[0]
            elif len(polys) > 1:
                from shapely.ops import unary_union
                return unary_union(polys)
            else:
                return None
        return geom
    
    gdf['geometry'] = gdf.geometry.apply(extract_polygons)
    gdf = gdf[gdf.geometry.notna()].copy()
    gdf = gdf[~gdf.geometry.is_empty].copy()
    
    if (gdf.geometry.geom_type == 'MultiPolygon').any():
        gdf = gdf.explode(index_parts=False).reset_index(drop=True)
    
    gdf = gdf[gdf.geometry.geom_type == 'Polygon'].copy()
    

    return gdf


def create_compilation(gdf, date_field, by_old_date=False):

    
    if gdf.crs is None:
        print("No CRS found, setting to EPSG:2039")
        gdf = gdf.set_crs('EPSG:2039')
    elif gdf.crs.to_epsg() != 2039:
        print(f"Converting from {gdf.crs} to EPSG:2039")
        gdf = gdf.to_crs('EPSG:2039')
    else:
        print("CRS is already EPSG:2039")
    
    gdf = clean_geometries(gdf)
    gdf = convert_to_single_polygons(gdf)

    gdf = gdf.sort_values(by=date_field, ascending=not by_old_date).reset_index(drop=True)

    gdf['idx'] = gdf.index

    try:
        merged = gpd.sjoin(gdf, gdf, predicate='intersects', how='inner', rsuffix='right_')
    except TypeError:
        merged = gpd.sjoin(gdf, gdf, op='intersects', how='inner', rsuffix='right_')
    
    older = merged[merged[f"{date_field}_left"] < merged[f"{date_field}_right_"]]
    grouped = older.groupby("idx_right_")['idx_left'].apply(list).reset_index()

    for idx, row in tqdm(grouped.iterrows(), total=len(grouped)):
        for older_id in row['idx_left']:
            try:
                if gdf.at[older_id, 'geometry'] is None or gdf.at[row['idx_right_'], 'geometry'] is None:
                    continue
                
                if gdf.at[older_id, 'geometry'].is_empty or gdf.at[row['idx_right_'], 'geometry'].is_empty:
                    continue
                    
                new_geom = gdf.at[older_id, 'geometry'].difference(gdf.at[row['idx_right_'], 'geometry'])
                
                if new_geom is None or new_geom.is_empty:
                    gdf.at[older_id, 'geometry'] = None
                elif not new_geom.is_valid:
                    new_geom = make_valid(new_geom)
                    if not new_geom.is_empty and new_geom.geom_type in ['Polygon', 'MultiPolygon']:
                        gdf.at[older_id, 'geometry'] = new_geom
                    else:
                        gdf.at[older_id, 'geometry'] = None
                else:
                    gdf.at[older_id, 'geometry'] = new_geom
                    
            except Exception as e:
                print_message(f'Error in feature {older_id}: {e}', status=0)
                gdf.at[older_id, 'geometry'] = None

    gdf = gdf[gdf.geometry.notna()].copy()
    gdf = gdf[~gdf.geometry.is_empty].copy()
    gdf = convert_to_single_polygons(gdf)
    
    if (~gdf.geometry.is_valid).any():
        gdf['geometry'] = gdf.geometry.apply(lambda g: make_valid(g) if not g.is_valid else g)
        gdf = convert_to_single_polygons(gdf)
    
    gdf.drop(columns=['shape.STAr','OBJECTID','idx'], inplace=True, errors='ignore')
    return gdf




# if __name__ == "__main__":
    
#     path_script = os.path.dirname(os.path.abspath(__file__))
#     path_main   = os.path.dirname(path_script)
#     path_data   = os.path.join(path_main, 'data')

#     layer  = path_data + '\\' + 'mavat_landuse.gpkg' +'\\' +'mavat_landuse'
#     output = path_data + '\\' + 'mavat_landuse_compilation.shp'

#     gdf        = read_layer_to_gdf(layer)
#     gdf_result = create_compilation(gdf, 'last_update_date')
    



