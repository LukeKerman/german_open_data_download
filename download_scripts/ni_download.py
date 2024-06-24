import os
import json
import requests

from _downloader import DownloadTools

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

    total_tiles = len(tiles)

    info_link = config_info['links']['download_link']

    try:
        response = requests.get(info_link)
        if response.status_code == 200:
            result = response.json()
    except Exception as e:
        print(f"Error: No reponse from server {e}")

    for i, tile in enumerate(tiles, start=1):
        if not tile["location"]:
            tile_name = tile['tile_name'].replace("_","")

            for feature in result['features']:
                if feature['properties']['tile_id'] == tile_name:
                    match data_type:
                        case "DOP":
                            download_link = feature['properties']['rgbi']
                        case "iDSM":
                            download_link = feature['properties']['bdom']
                        case "DTM":
                            download_link = feature['properties']['dgm1']
                        case _:
                            print(f"Error with data type {data_type} is not in the configured or set correctly")

                    tile["timestamp"] = feature['properties']['Aktualitaet']

            # Check if timestamp is within date range
            if DT.within_date_range(tile["timestamp"], init["date_range"]):
                pass
            else:
                print(f"Tile {tile_name} not in date range")
                continue

            filename = download_link.split('/')[-1]
            save_path = f"{landing}/{state.lower()}/{data_type.lower()}_{tile['tile_name']}/{filename}"
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
                    s3_path = f"{config_info['links']['s3_path']}{data_type.lower()}_{tile['tile_name']}/{filename}"
                    DT.upload_file(save_path, s3_path)
                    tile['location'] = s3_path
                    if init['delete']:
                        DT.delete_files_and_dir(os.path.dirname(save_path))
                except Exception as e:
                    print(f"Error while uploading to {s3_path}: {e}")
            else:
                tile['location'] = save_path.split('.')[0]
            
            # Update the tile format
            tile['format'] = download_link.split('.')[-1]

            DT.save_json(meta_path, tiles_data)
        else:
            print(f"Tile {tile['tile_name']} is already downloaded [{i} of {total_tiles}]")
    
    if init['delete']:
        DT.delete_files_and_dir(landing)