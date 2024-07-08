import os
import pandas as pd
import requests
from zipfile import ZipFile
import re

from _downloader import DownloadTools

def get_creation_date(meta_url, tiles, data_type):

    print(f"Fetching meta data", end="", flush=True)
    meta_extract_path = "tmp"
    meta_save_path = f"{meta_extract_path}/nw_meta"
    os.makedirs(meta_extract_path, exist_ok=True)

    # Send a GET request to the URL
    response = requests.get(meta_url)
    
    if response.status_code == 200:
        with open(meta_save_path, 'wb') as f:
            f.write(response.content)
        with ZipFile(meta_save_path, 'r') as zip_ref:
            filename = zip_ref.namelist()[0]
            zip_ref.extractall(meta_extract_path)
        os.remove(meta_save_path)
    else:
        raise Exception(f"Failed to download file. Status code: {response.status_code}")
    
    # Load CSV file
    meta = pd.read_csv(f"tmp/{filename}", delimiter=';', skiprows=5, low_memory=False)
    
    # Select the columns "tile_nr" and "creationdate"
    creationdates = meta[["Kachelname", "Aktualitaet"]]
    
    # Update the JSON object with the creation dates
    for tile in tiles:
        if data_type == "iDSM":
            tile_nr = tile.get("tile_name").replace('_', '', 1)
        else:
            tile_nr = tile.get("tile_name")
        # Use regex to find the matching Kachelname
        matching_row = creationdates[creationdates["Kachelname"].str.contains(re.escape(tile_nr))]
        if not matching_row.empty:
            if data_type == "DTM":
                tile["timestamp"] = matching_row["Aktualitaet"].values[0] + "-15" # Set timstamp of DTM to middle of the month (original YYYY-MM)
            else:
                tile["timestamp"] = matching_row["Aktualitaet"].values[0]
    
    os.remove(f"{meta_extract_path}/{filename}")
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

    total_tiles = len(tiles)

    meta_data_url = config_info['links']['meta_data_link']
    get_creation_date(meta_data_url, tiles, data_type)

    for i, tile in enumerate(tiles, start=1):
        tile_name = tile['tile_name']
        if data_type == "iDSM":
            download_url = config_info['links']['download_link'].format(tile_name.replace('_', '', 1), tile['timestamp'][:4])
        else:
            download_url = config_info['links']['download_link'].format(tile_name, tile['timestamp'][:4])

        filename = download_url.split('/')[-1]
        save_path = f"{landing}/{state.lower()}/{data_type.lower()}_{tile_name}/{filename}"
        os.makedirs(f"{landing}/{state.lower()}/{data_type.lower()}_{tile_name}", exist_ok=True)

        # Check if timestamp is within date range
        if DT.within_date_range(tile["timestamp"], init["date_range"]):
            pass
        else:
            print(f"Tile {tile_name} not in date range")
            continue

        # Download the file
        if not tile["location"]:
            try:
                DT.download_file(download_url, save_path, tile)
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
                tile['location'] = os.path.dirname(save_path)
            # Update the tile format
            tile['format'] = save_path.split('.')[-1]
            
        else:
            print(f"Tile {tile['tile_name']} is already downloaded [{i} of {total_tiles}]")
    
    if init['delete']:
        DT.delete_files_and_dir(landing)