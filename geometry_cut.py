

import geopandas as gpd
from shapely.geometry import mapping

def compare_layers_with_scoring(old_layer, new_layer, area_tolerance=0.25):
    
    
    type_points = {
        1: 10,
        2: 100,
        3: 5
    }
    
    if old_layer.crs != new_layer.crs:
        new_layer = new_layer.to_crs(old_layer.crs)
    
    results = []
    
    for idx, old_feature in old_layer.iterrows():
        old_key = old_feature['key']
        old_type = old_feature['type']
        old_baalot = str(old_feature['baalot'])
        old_geom = old_feature['geometry']
                
        old_area = old_geom.area
        
        old_points = type_points.get(old_type, 0)
        old_score = old_area * old_points
        
        intersecting = new_layer[new_layer.intersects(old_geom)]
        
        if len(intersecting) == 0:
            results.append({
                'old_key': old_key,
                'old_baalot': old_baalot,
                'old_type': old_type,
                'new_keys': [],
                'new_baalot': [],
                'new_type': [],
                'old_area': old_area,
                'new_total_area': 0,
                'area_lost': old_area,
                'area_lost_pct': 100.0,
                'old_score': old_score,
                'new_score': 0,
                'score_lost': old_score,
                'geometry': old_geom
            })
            continue
        
        new_keys = []
        new_total_area = 0
        new_total_score = 0
        new_baalot_list = []
        new_type_list = []
        
        for _, new_feature in intersecting.iterrows():
            new_key = new_feature['key']
            new_type = new_feature['type']
            new_baalot = str(new_feature['baalot'])
            new_geom = new_feature['geometry']
            
            intersection = old_geom.intersection(new_geom)
            intersection_area = intersection.area
            
            if intersection_area < area_tolerance:
                continue
            
            new_keys.append(new_key)
            new_baalot_list.append(new_baalot)
            new_type_list.append(new_type)
            new_total_area += intersection_area
            
            new_points = type_points.get(new_type, 0)
            new_total_score += intersection_area * new_points
        
        if len(new_keys) == 0:
            results.append({
                'old_key': old_key,
                'old_baalot': old_baalot,
                'old_type': old_type,
                'new_keys': [],
                'new_baalot': [],
                'new_type': [],
                'old_area': old_area,
                'new_total_area': 0,
                'area_lost': old_area,
                'area_lost_pct': 100.0,
                'old_score': old_score,
                'new_score': 0,
                'score_lost': old_score,
                'geometry': old_geom
            })
            continue
        
        area_lost = old_area - new_total_area
        area_lost_pct = (area_lost / old_area * 100) if old_area > 0 else 0
        score_lost = old_score - new_total_score
        
        results.append({
            'old_key': old_key,
            'old_baalot': old_baalot,
            'old_type': old_type,
            'new_keys': new_keys,
            'new_baalot': new_baalot_list,
            'new_type': new_type_list,
            'old_area': old_area,
            'new_total_area': new_total_area,
            'area_lost': area_lost,
            'area_lost_pct': area_lost_pct,
            'old_score': old_score,
            'new_score': new_total_score,
            'score_lost': score_lost,
            'geometry': old_geom
        })
    
    result_gdf = gpd.GeoDataFrame(results, crs=old_layer.crs)
    
    return result_gdf


if __name__ == "__main__":
    folder = r'C:\Users\medad\meidad\Work\KKL\NetWork_Report\data'
    newLayer = folder + '\\' + 'newLayer.shp'
    source   = folder + '\\' + 'source.shp'
    output   = folder + '\\' + 'comparison_results.shp'
    output_json = r'C:\Users\medad\meidad\Work\KKL\NetWork_Report\script' + '\\' + 'comparison_results.json'

    old_layer = gpd.read_file(source)
    new_layer = gpd.read_file(newLayer)


    results = compare_layers_with_scoring(old_layer, new_layer)

    results.to_file(output, driver='ESRI Shapefile')

    # convert coordinates to WGS84 for GeoJSON and to WKT
    results = results.to_crs(epsg=4326)

    results['geometry'] = results['geometry'].apply(lambda geom: geom.wkt)

    results.to_file(output_json, driver='GeoJSON')

    # json new geometrys
    new_layer = new_layer.to_crs(epsg=4326)
    new_layer['geometry'] = new_layer['geometry'].apply(lambda geom: geom.wkt)
    new_layer.to_file(r'C:\Users\medad\meidad\Work\KKL\NetWork_Report\script' + '\\' + 'newLayer_wkt.json', driver='GeoJSON')
