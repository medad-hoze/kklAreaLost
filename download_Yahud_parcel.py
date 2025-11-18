



import requests
import geopandas as gpd
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import os

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

base_url = 'https://ags.iplan.gov.il/arcgisiplan/rest/services/PlanningPublic/Xplan/MapServer'
layer_id = 4  

def get_feature_count(base_url, layer_id):

    query_url = f'{base_url}/{layer_id}/query'
    params = {
        'where': '1=1',
        'returnCountOnly': 'true',
        'f': 'json'
    }
    response = requests.get(query_url, params=params, verify=False)
    return response.json().get('count', 0)


def download_batch(base_url, layer_id, offset, batch_size=1000):
    
    query_url = f'{base_url}/{layer_id}/query'
    params = {
        'where': '1=1',
        'outFields': '*',
        'f': 'geojson',
        'returnGeometry': 'true',
        'outSR': '2039',  
        'resultOffset': offset,
        'resultRecordCount': batch_size
    }
    
    try:
        response = requests.get(query_url, params=params, verify=False)
        data = response.json()
        features = data.get('features', [])
        return features
    except Exception as e:
        print(f"\nError at offset {offset}: {e}")
        return []

def download_layer_parallel(base_url, layer_id, crs='EPSG:2039'):

    max_workers = os.cpu_count() 
    
    total_count = get_feature_count(base_url, layer_id)
    batch_size = 1000
    offsets    = list(range(0, total_count, batch_size))
    
    
    all_features = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_offset = {
            executor.submit(download_batch, base_url, layer_id, offset, batch_size): offset 
            for offset in offsets
        }
        
        with tqdm(total=len(offsets), desc="Downloading batches", unit="batch") as pbar:
            for future in as_completed(future_to_offset):
                features = future.result()
                all_features.extend(features)
                pbar.update(1)
                pbar.set_postfix({'features': f"{len(all_features):,}"})
    
    
    gdf = gpd.GeoDataFrame.from_features(all_features, crs=crs)
    return gdf





# if __name__ == "__main__":
#     gdf = download_layer_parallel(base_url, layer_id=layer_id)

    # export to gpkg path_gpkg = C:\Users\medad\meidad\Work\KKL\NetWork_Report\data

    # path_gpkg  = r'C:\Users\medad\meidad\Work\KKL\NetWork_Report\data\yahud_parcel.gpkg'
    # layer_name = 'yahud_parcel'
    # gdf.to_file(path_gpkg, driver='GPKG', layer=layer_name)