

import os
import geopandas as gpd

from shapely.validation import make_valid
from utiles             import read_layer_to_gdf,convert_to_single_polygons


def compare_layers_with_scoring(old_layer,
                                new_layer,
                                area_tolerance = 0.25,
                                key_name       = 'key',
                                type_name      = 'type',
                                baalot_name    = 'baalot',
                                geometry_name  = 'geometry',
                                percentage_kkl_name = 'precentage_kkl'):
    

    type_points = { 'מגבלות בניה ופיתוח'           :10, 
                    'מגורים'                        :100, 
                    'יעוד עפ"י תכנית מאושרת אחרת' : 50,
                    'שטח שהתוכנית אינה חלה עליו'  :10,
                    'יעוד עפ"י תכנית מאושרת אחרת' :15};


    if old_layer.crs != new_layer.crs:
        new_layer = new_layer.to_crs(old_layer.crs)
    
    results = []
    
    for idx, old_feature in old_layer.iterrows():
        old_key    = old_feature[key_name]
        old_type   = old_feature[type_name]
        old_baalot = str(old_feature[baalot_name])
        old_geom   = old_feature[geometry_name]
        
        # Get the percentage_kkl value (default to 100 if not present)
        percentage_kkl = old_feature.get(percentage_kkl_name, 100)
        
        # Calculate the actual area based on percentage_kkl
        old_geom_area = old_geom.area
        old_area = old_geom_area * (percentage_kkl / 100.0)
        
        old_points = type_points.get(old_type, 0)
        old_score  = old_area * old_points
        
        intersecting = new_layer[new_layer.intersects(old_geom)]
        
        if len(intersecting) == 0:
            results.append({
                'old_key'       : old_key,
                'old_baalot'    : old_baalot,
                'old_type'      : old_type,
                'percentage_kkl': percentage_kkl,
                'new_keys'      : [],
                'new_baalot'    : [],
                'new_type'      : [],
                'old_area'      : old_area,
                'new_total_area': 0,
                'area_lost'     : old_area,
                'area_lost_pct' : 100.0,
                'old_score'     : old_score,
                'new_score'     : 0,
                'score_lost'    : old_score,
                'geometry'      : old_geom,
                'area_list'     : [],
                'score_list'    : []
            })
            continue
        
        new_keys        = []
        new_total_area  = 0
        new_total_score = 0
        new_baalot_list = []
        new_type_list   = []
        new_area_list   = []
        new_score_list  = []
        
        for _, new_feature in intersecting.iterrows():
            new_key    = new_feature[key_name]
            new_type   = new_feature[type_name]
            new_baalot = str(new_feature['ownershipType'])
            new_geom   = new_feature[geometry_name]
            
            intersection      = old_geom.intersection(new_geom)
            intersection_area_raw = intersection.area
            
            # Apply the percentage_kkl to the intersection area as well
            intersection_area = intersection_area_raw * (percentage_kkl / 100.0)
            
            if intersection_area < area_tolerance:
                continue
            
            new_keys.append(new_key)
            new_baalot_list.append(new_baalot)
            new_type_list  .append(new_type)
            new_area_list  .append(intersection_area)
            
            # Calculate score for this feature
            new_points = type_points.get(new_type, 0)
            feature_score = intersection_area * new_points
            new_score_list.append(feature_score)
            
            # Only add to totals if ownership is קק"ל
            if 'קק"ל' in new_baalot or 'קקל' in new_baalot or 'קק״ל' in new_baalot:
                new_total_area += intersection_area
                new_total_score += feature_score
        
        if len(new_keys) == 0:
            results.append({
                'old_key'       : old_key,
                'old_baalot'    : old_baalot,
                'old_type'      : old_type,
                'percentage_kkl': percentage_kkl,
                'new_keys'      : [],
                'new_baalot'    : [],
                'new_type'      : [],
                'old_area'      : old_area,
                'new_total_area': 0,
                'area_lost'     : old_area,
                'area_lost_pct' : 100.0,
                'old_score'     : old_score,
                'new_score'     : 0,
                'score_lost'    : old_score,
                'geometry'      : old_geom,
                'area_list'     : new_area_list,
                'score_list'    : new_score_list
            })
            continue
        
        area_lost     = old_area - new_total_area
        area_lost_pct = (area_lost / old_area * 100) if old_area > 0 else 0
        score_lost    = old_score - new_total_score
        
        results.append({
            'old_key'       : old_key,
            'old_baalot'    : old_baalot,
            'old_type'      : old_type,
            'percentage_kkl': percentage_kkl,
            'new_keys'      : new_keys,
            'new_baalot'    : new_baalot_list,
            'new_type'      : new_type_list,
            'old_area'      : old_area,
            'new_total_area': new_total_area,
            'area_lost'     : area_lost,
            'area_lost_pct' : area_lost_pct,
            'old_score'     : old_score,
            'new_score'     : new_total_score,
            'score_lost'    : score_lost,
            'geometry'      : old_geom,
            'area_list'     : new_area_list,
            'score_list'    : new_score_list
        })
    
    result_gdf = gpd.GeoDataFrame(results, crs=old_layer.crs)
    
    return result_gdf


if __name__ == "__main__":

    path_script = os.path.dirname(os.path.abspath(__file__))
    path_main   = os.path.dirname(path_script)
    path_data   = os.path.join   (path_main, 'data')

    gpkg        = path_data + '\\' + 'mavat_landuseResult.gpkg'
    folder      = r'C:\Users\medad\meidad\Work\KKL\NetWork_Report\data'

    # newLayer = folder + '\\' + 'newLayer.shp'
    # source   = folder + '\\' + 'source.shp'
    output      = folder + '\\' + 'comparison_results.shp'
    output_json    = r'C:\Users\medad\meidad\Work\KKL\NetWork_Report\script' + '\\' + 'comparison_results.json'
    out_added_json = r'C:\Users\medad\meidad\Work\KKL\NetWork_Report\script' + '\\' + 'add_layers.json'

    source   = gpkg + '\\' + 'parcel_old'
    newLayer = gpkg + '\\' + 'parcel_current'
    add_layer   = gpkg + '\\' + 'parcel_added'

    added_layer = read_layer_to_gdf(add_layer)
    old_layer   = read_layer_to_gdf(source)
    new_layer   = read_layer_to_gdf(newLayer)

    new_layer.set_geometry('geometry', inplace=True)
    old_layer.set_geometry('geometry', inplace=True)

    old_layer['geometry'] = old_layer['geometry'].apply(make_valid)
    new_layer['geometry'] = new_layer['geometry'].apply(make_valid)

    old_layer = convert_to_single_polygons(old_layer)
    new_layer = convert_to_single_polygons(new_layer)

    results = compare_layers_with_scoring(old_layer, new_layer,
                                            area_tolerance = 0.25,
                                            key_name       = 'key',
                                            type_name      = 'mavat_name',
                                            baalot_name    = 'BaalutBefoal',
                                            geometry_name  = 'geometry')

    results.to_file(output, driver='ESRI Shapefile')

    results             = results.to_crs(epsg=4326)
    results['geometry'] = results['geometry'].apply(lambda geom: geom.wkt)

    results    .to_file(output_json, driver='GeoJSON')
    import json
    # make ready for web
    with open(out_added_json, 'w', encoding='utf-8') as f:
        json.dump(added_layer.to_dict(orient='records'), f, ensure_ascii=False, indent=4)   

    new_layer             = new_layer.to_crs(epsg=4326)
    new_layer['geometry'] = new_layer['geometry'].apply(lambda geom: geom.wkt)
    new_layer.to_file(r'C:\Users\medad\meidad\Work\KKL\NetWork_Report\script' + '\\' + 'newLayer_wkt.json', driver='GeoJSON')
