import os

import requests

from _downloader import DownloadTools

def get_creation_date(result, tiles):
    print(f"Fetching meta data", end="", flush=True)
    for i, tile in enumerate(tiles, start=1):
        for feature in result['features']:
            if feature['properties']['tile_id'] == tile['tile_name'].replace("_",""):
                tile["timestamp"] = feature['properties']['Aktualitaet']
    print("\rUpdated metadata successfully")

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

    info_link = config_info['links']['download_link']

    try:
        response = requests.get(info_link)
        if response.status_code == 200:
            result = response.json()
            get_creation_date(result, tiles)
            tiles = DT.filter_tiles_by_date(tiles, init["date_range"])
            state_data["tile_list"] = tiles
            tiles_data["tiles"][state] = state_data
    except Exception as e:
        print(f"Error: No reponse from server {e}")
    
    total_tiles = len(tiles)

    if not init["download"]: return

    for i, tile in enumerate(tiles, start=1):
        tile_name = tile['tile_name']
        if not tile["location"]:
            for feature in result['features']:
                if feature['properties']['tile_id'] == tile['tile_name'].replace("_",""):
                    match data_type:
                        case "DOP":
                            download_link = feature['properties']['rgbi']
                        case "iDSM":
                            download_link = feature['properties']['bdom']
                        case "DTM":
                            download_link = feature['properties']['dgm1']
                        case _:
                            print(f"Error with data type {data_type} is not in the configured or set correctly")

            filename = download_link.split('/')[-1]
            save_path = f"{landing}/{state.lower()}/{data_type.lower()}_{tile_name}/{filename}"
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
        
            # Download the file
            try:
                DT.download_file(download_link, save_path, tile)
                print(f" [{i} of {total_tiles}]")
            except Exception as e:
                print(f"Error while downloading to {tile_name}: {e}")
                DT.delete_files_and_dir(save_path)
                
            if init['upload_s3']:
                # Upload the file to S3
                try:
                    s3_path = f"{config_info['links']['s3_path']}{data_type.lower()}_{tile_name}/{filename}"
                    DT.upload_file(save_path, s3_path)
                    tile['location'] = s3_path
                    if init['delete']:
                        DT.delete_files_and_dir(os.path.dirname(save_path))
                except Exception as e:
                    print(f"Error while uploading to {s3_path}: {e}")
            else:
                tile['location'] = os.path.dirname(save_path)
            
            # Update the tile format
            tile['format'] = download_link.split('.')[-1]

        else:
            print(f"Tile {tile_name} is already downloaded [{i} of {total_tiles}]")
    
    if init['delete']:
        DT.delete_files_and_dir(landing)