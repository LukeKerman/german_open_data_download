import os
import json
import requests

from _downloader import download_file, upload_file, delete_files_and_dir, save_tile_metadata

def download_tiles(state_data, config):
    state = "NI"
    tiles = state_data.get("tile_list")
    data_type = state_data.get("data_type")
    config_info = config[data_type][state]

    for tile in tiles:
        tile_name = tile['tile_name'].replace("_","")
        search_url = config_info['links']['download_link']

        # Define the request body
        body = { "kachelname": tile_name}

        # Make the POST request
        response = requests.post(search_url, json=body)

        # Check the response status and print the response
        if response.status_code == 200:
            #print("Request was successful.")
            result = response.json()
            if result["features"]:
                match data_type:
                    case "DOP":
                        download_link = result["features"][0]["assets"]["dop20_rgbi"]["href"]
                    case "iDSM":
                        download_link = result["features"][0]["assets"]["bdom20"]["href"]
                    case "DTM":
                        download_link = result["features"][0]["assets"]["dgm1-tif"]["href"]
                    case _:
                        print(f"Error with data type {data_type} is not in the configured or set correctly")

                tile["timestamp"] = result["features"][0]["properties"]["datetime"][:10]
        else:
            print("Request failed with status code:", response.status_code)
            print(response.text)

        filename = download_link.split('/')[-1]
        save_path = os.path.join("tmp", filename)
        os.makedirs("tmp", exist_ok=True)
        
        # Download the file
        download_file(download_link, save_path, tile)
        
        # Upload the file to S3
        s3_path = f"s3://your-bucket/dop_{tile['tile_name']}"
        #upload_file(save_path, s3_path)
        
        # Update the tile information
        tile['location'] = s3_path
        tile['format'] = download_link.split('.')[-1]
        #tile['timestamp'] = extract_creation_date(meta_data_link)
    
    delete_files_and_dir("tmp")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python ni_download.py <tiles_json> <config_json>")
        sys.exit(1)

    tiles_json_path = sys.argv[1]
    config_json_path = sys.argv[2]

    with open(tiles_json_path, 'r') as tiles_file:
        tiles = json.load(tiles_file)

    with open(config_json_path, 'r') as config_file:
        config = json.load(config_file)

    download_tiles(tiles, config)