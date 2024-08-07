import os

import requests

from _downloader import DownloadTools

def get_creation_date(wfs_url, tiles, data_type):
    
    print("Fetching meta data", end="", flush=True)
    
    if data_type == "DTM":
        layer_name = 'verm:v_dgm_kacheln_als_2_2016_2021'
        kachel_col = 'dgm_kachel'
        creation_date_col = 'fortfuehrungsdatum'
        version = '2.0.0'
    elif data_type in ("iDSM", "DOP"):
        layer_name = 'verm:v_dop_20_bildflugkacheln'
        kachel_col = 'dop_kachel'
        creation_date_col = 'befliegungsdatum'
        version = '1.1.0'

    # Construct the GetFeature request URL
    params = {
        'service': 'WFS',
        'version': version,
        'request': 'GetFeature',
        'typeName': layer_name,
        'outputFormat': 'json'
    }
    response = requests.get(wfs_url, params=params)

    # Check if the request was successful
    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        return

    # Parse the response JSON
    data = response.json()

    # Create a dictionary to map tile names to Befliegungsdatum
    tile_dict = {}
    for feature in data.get('features', []):
        dop_kachel = feature['properties'].get(kachel_col)
        creationdate = feature['properties'].get(creation_date_col)
        if dop_kachel and creationdate:
            tile_dict[dop_kachel] = creationdate

    # Update the JSON data with timestamps
    for tile in tiles:
        # Convert tile name to the format used in WFS
        wfs_tile_name = tile['tile_name'].replace('_', '')
        if wfs_tile_name in tile_dict:
            tile['timestamp'] = tile_dict[wfs_tile_name]

    print("\rUpdated metadata successfully.")

def download_tiles(tiles_data, config_data):
    state = os.path.basename(__file__)[:2].upper()
    init, config = config_data
    landing = init['local_landing_path']

    state_data = tiles_data["tiles"][state]
    tiles = state_data["tile_list"]
    data_type = state_data["data_type"]

    config_info = config[data_type][state]
    meta_path = init['meta_path']

    DT = DownloadTools()

    if not config_info['links']['download_link']:
        print(f"No links provided for {state} ({data_type}) in configuration file.")
        return

    meta_data_url = config_info['links']['meta_data_link']
    get_creation_date(meta_data_url, tiles, data_type)
    tiles = DT.filter_tiles_by_date(tiles, init["date_range"])
    state_data["tile_list"] = tiles
    tiles_data["tiles"][state] = state_data

    total_tiles = len(tiles)

    if not init["download"]: return

    for i, tile in enumerate(tiles, start=1):
        tile_name = tile['tile_name']
        download_url = config_info['links']['download_link'].format(tile_name)

        filename = f"{data_type.lower()}_{tile_name}.{download_url.split('.')[-1]}"
        save_path = f"{landing}/{state.lower()}/{data_type.lower()}_{tile_name}/{filename}"
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        
        # Download the file
        if not tile["location"]:
            try:
                DT.download_file(download_url, save_path, tile)
                print(f" [{i} of {total_tiles}]")
            except Exception as e:
                print(f"Error while downloading to {tile_name}: {e}")
                DT.delete_files_and_dir(save_path)

            file_path_list = DT.find_file(os.path.dirname(save_path))

            # Update the tile format
            tile['format'] = file_path_list[0].split('.')[-1]
            tile['location'] = []

            if init['upload_s3']:
                # Upload the file to S3
                for file_path in file_path_list:
                    file_name = os.path.basename(file_path)
                    sub_folder = f"{data_type.lower()}_{'_'.join(file_name.split('_')[1:4])}"
                    s3_path = f"{config_info['links']['s3_path']}{sub_folder}/{file_name}"
                    try:
                        DT.upload_file(file_path, s3_path)
                        tile['location'].append(s3_path)
                        
                    except Exception as e:
                        print(f"Error while uploading to {s3_path}: {e}")
                
                if init['delete']:
                            DT.delete_files_and_dir(os.path.dirname(save_path))
            else:
                tile['location'] = os.path.dirname(save_path)

        else:
            print(f"Tile {tile['tile_name']} is already downloaded [{i} of {total_tiles}]")
    
    if init['delete']:
        DT.delete_files_and_dir(landing)