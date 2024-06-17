import os
import json
from io import StringIO
import pandas as pd
import requests

from _downloader import download_file, upload_file, delete_files_and_dir

def get_creation_date(url, tiles):

    print(f"Fetching meta data", end="", flush=True)
    meta_path = "tmp/bb_meta.csv"

    # Send a GET request to the URL
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code == 200:
        # Open a local file in binary write mode
        with open(meta_path, 'wb') as f:
            # Write the content of the response to the local file
            f.write(response.content)
    else:
        raise Exception(f"Failed to download file. Status code: {response.status_code}")
    
    # Load CSV file
    meta = pd.read_csv(meta_path, delimiter=';')
    
    # Rename the column "sheetnr" to "tile_nr"
    meta.rename(columns={"sheetnr": "tile_nr"}, inplace=True)
    
    # Select the columns "tile_nr" and "creationdate"
    creationdates = meta[["tile_nr", "creationdate"]]
    
    # Update the JSON object with the creation dates
    for tile in tiles:
        tile_nr = tile.get("tile_name").replace('_', '', 1).replace('_', '-')
        if tile_nr in creationdates["tile_nr"].values:
            tile["timestamp"] = creationdates[creationdates["tile_nr"] == tile_nr]["creationdate"].values[0]
    
    os.remove(meta_path)
    print("\rUpdated metadata successfully")

def find_file(save_path):
    # Create the directory path by removing the .zip extension
    dir_path = save_path[:-4]

    # List of file extensions to look for
    extensions = ('.tif', '.xyz', '.laz')

    # Search for files with the specified extensions
    for root, _, files in os.walk(dir_path):
        for file in files:
            if file.endswith(extensions):
                return os.path.join(root, file)

    return None
    

def download_tiles(state_data, config):
    tiles = state_data["tile_list"]
    data_type = state_data["data_type"]
    config_info = config[data_type][os.path.basename(__file__)[:2].upper()]

    total_tiles = len(tiles)

    os.makedirs("tmp", exist_ok=True)

    meta_data_url = config_info['links']['meta_data_link']
    get_creation_date(meta_data_url, tiles)

    for i, tile in enumerate(tiles, start=1):
        tile_name = tile['tile_name'].replace('_', '', 1).replace('_', '-')
        download_url = config_info['links']['download_link'].format(tile_name)

        filename = f"{data_type.lower()}_{tile_name}.{download_url.split('.')[-1]}"
        save_path = os.path.join("tmp", filename)

        # Download the file
        download_file(download_url, save_path, tile)
        print(f" [{i} of {total_tiles}]")

        # Find the relevant files in the extract path
        file_path = find_file(save_path)
        
        # Upload the file to S3
        s3_path = f"{config_info['links']['s3_path']}dop_{tile['tile_name']}"
        #upload_file(file_path, s3_path)
        
        # Update the tile information
        tile['location'] = s3_path
        tile['format'] = file_path.split('.')[-1]

        #delete_files_and_dir(save_path.split('.')[0])
    
    delete_files_and_dir("tmp")