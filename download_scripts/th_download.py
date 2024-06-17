import os
import json
import requests
import time

from _downloader import download_file, upload_file, delete_files_and_dir

def get_creation_date(tiles, meta_url):
    start_id = 537000#530448
    end_id = 549479

    tile_ids = []
    
    for idx, tile_id in enumerate(range(start_id, end_id + 1)):
        response = requests.get(meta_url.format(tile_id))
        print(f"\rLoading meta data: {idx/(end_id-start_id):>3.1f}%", end="")
        if response.status_code == 200:
            data = response.json()
            if data["success"] == "true" and "object" in data:
                object_data = data["object"]
                tile_nr = f'{object_data["bildnr"][:2]}_{object_data["bildnr"][2:]}'
                for tile in tiles:
                    if tile_nr == tile["tile_name"]:
                        if tile["timestamp"] != None:
                            print(f"Timestamp already set for tile: {tile['tile_name']}")
                        else:
                            tile["timestamp"] = object_data["datum"][:10]
                            tile_ids.append((tile["tile_name"], tile_id)) 
                        break
            #time.sleep(0.001)  # To avoid overwhelming the server with requests
    return tiles, dict(tile_ids)

def download_tiles(state_data, config):
    tiles = state_data.get("tile_list")
    data_type = state_data.get("data_type")
    config_info = config[data_type]["TH"]
    meta_url = config_info["links"]["meta_data_link"]

    tiles, tile_ids = get_creation_date(tiles, meta_url)

    for tile in tiles:
        data_type = tile['data_type']
        tile_name = tile['tile_name']
        tile_id = tile_ids.get(tile_name)
        
        download_url = config_info['links']['download_link'].format(tile_id)
        #meta_data_link = config_info['links']['meta_data_link']
        save_path = f"./tmp/{data_type.lower()}_{tile_name}.{download_url.split('.')[-1]}"
        os.makedirs("./tmp/", exist_ok=True)
        
        # Download the file
        #download_file(download_url, save_path, tile)
        
        # Upload the file to S3
        s3_path = f"s3://your-bucket/dop_{tile['tile_name']}"
        #upload_file(save_path, s3_path)
        
        # Update the tile information
        #tile['location'] = s3_path
        #tile['format'] = download_url.split('.')[-1]
        #tile['timestamp'] = extract_creation_date(meta_data_link)
    
    delete_files_and_dir("./tmp")