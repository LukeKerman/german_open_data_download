import os
import json
import requests

from _downloader import download_file, upload_file, delete_files_and_dir

def extract_creation_date(meta_data_link):
    response = requests.get(meta_data_link)
    if response.status_code == 200:
        # Assume the creation date is in the first line of the metadata
        return response.text.splitlines()[0]
    return None

def download_tiles(state_data, config):
    tiles = state_data["tile_list"]
    data_type = state_data["data_type"]
    config_info = config[data_type]["BW"]

    for tile in tiles:
        tile_name = tile['tile_name']
        download_url = config_info['links']['download_link'].format(tile_name)

        filename = f"{data_type.lower()}_{tile_name}.{download_url.split('.')[-1]}"
        save_path = os.path.join("tmp", filename)
        os.makedirs("tmp", exist_ok=True)
        
        # Download the file
        download_file(download_url, save_path, tile)
        print(f" ()")
        
        # Upload the file to S3
        s3_path = f"s3://your-bucket/dop_{tile['tile_name']}"
        #upload_file(save_path, s3_path)
        
        # Update the tile information
        tile['location'] = s3_path
        tile['format'] = download_url.split('.')[-1]
    
    delete_files_and_dir("./tmp")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python bw_download.py <tiles_json> <config_json>")
        sys.exit(1)

    tiles_json_path = sys.argv[1]
    config_json_path = sys.argv[2]

    with open(tiles_json_path, 'r') as tiles_file:
        tiles = json.load(tiles_file)

    with open(config_json_path, 'r') as config_file:
        config = json.load(config_file)

    download_tiles(tiles, config)
