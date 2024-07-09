import os
import pandas as pd
import requests

from _downloader import DownloadTools

def get_creation_date(url, tiles):

    print(f"Fetching meta data", end="", flush=True)
    meta_path = "tmp/bb_meta.csv"
    os.makedirs("tmp", exist_ok=True)

    # Send a GET request to the URL
    response = requests.get(url)
    
    if response.status_code == 200:
        with open(meta_path, 'wb') as f:
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

    meta_data_url = config_info['links']['meta_data_link']
    get_creation_date(meta_data_url, tiles)
    tiles = DT.filter_tiles_by_date(tiles, init["date_range"])
    state_data["tile_list"] = tiles
    tiles_data["tiles"][state] = state_data

    total_tiles = len(tiles)

    if not init["download"]: return

    for i, tile in enumerate(tiles, start=1):
        tile_name = tile['tile_name']
        download_url = config_info['links']['download_link'].format(tile['tile_name'].replace('_', '', 1).replace('_', '-'))

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

            # Find the relevant files in the extract path
            file_path = DT.find_file(os.path.dirname(save_path))

            # Update the tile format
            tile['format'] = file_path.split('.')[-1]

            if init['upload_s3']:
                # Upload the file to S3
                try:
                    s3_path = f"{config_info['links']['s3_path']}{data_type.lower()}_{tile_name}/{os.path.basename(file_path)}"
                    DT.upload_file(file_path, s3_path)
                    tile['location'] = s3_path
                    if init['delete']:
                        DT.delete_files_and_dir(os.path.dirname(file_path))
                except Exception as e:
                    print(f"Error while uploading to {s3_path}: {e}")
            else:
                tile['location'] = os.path.dirname(file_path)

        else:
            print(f"Tile {tile_name} is already downloaded [{i} of {total_tiles}]")
    
    if init['delete']:
        DT.delete_files_and_dir(landing)