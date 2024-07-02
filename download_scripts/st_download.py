import os
import json
import re
from datetime import datetime

import requests

from _downloader import DownloadTools

def get_creation_date(meta_url, tiles, data_type):

    print(f"Fetching meta data", end="", flush=True)

    total = len(tiles)
    
    # Update the JSON object with the creation dates
    if not data_type == "DTM":
        for i, tile in enumerate(tiles, start=1):
            tile_nr = tile.get("tile_name").replace('_', '')
            meta_response = requests.get(meta_url.format(tile_nr))
            # Use regex to find the matching Kachelname
            meta_data = json.loads(meta_response.content)

            bfdatum = meta_data['features'][0]['attributes']['BFDATUM']
            name = meta_data['features'][0]['attributes']['NAME']

            bfdatum_formatted = (datetime.fromtimestamp(bfdatum / 1000)).strftime('%Y-%m-%d')

            print(f"\rLoading meta data:  {i/total*100:.1f}%", end="")

            if tile_nr == name:
                tile["timestamp"] = bfdatum_formatted
        print("\rUpdated metadata successfully")
    else:
        print(f"\rNo Metadata available for {data_type}")

def get_tile_id(url):
    print("Fetching tile IDs", end="", flush=True)

    response = requests.get(url)
    
    match = re.search(r"gc.mod.MapDownloadSelector\([^,]*,\s*'([^']+)'", response.text)
    prepare_link = re.findall(r"https?://\S+prepare\S+", response.text)[0][:-2]

    tile_ids = []

    if match:
        json_string = match.group(1)
        json_data = json.loads(json_string)

        for feature in json_data["features"]:
            id = feature["properties"]["id"]
            label = feature["properties"]["label"]
            tile_nr = f"{label[:2]}_{label[2:5]}_{label[5:]}"
            tile_ids.append((tile_nr, id))
    else:
        print("No data to match")

    print("\rFetched tile IDs successfully", flush=True)

    return prepare_link, dict(tile_ids)

def find_meta_file_and_get_date(dir_path, tile):
    # Search for the .meta file
    meta_file = None
    for root, _, files in os.walk(dir_path):
        for file in files:
            if file.endswith('.meta'):
                meta_file = os.path.join(root, file)
                break
        if meta_file:
            break

    if not meta_file:
        return

    # Read the .meta file and search for the Aktualitaet entry
    with open(meta_file, 'r', encoding='latin-1') as file:
        content = file.read()

    # Find the Aktualitaet entry
    match = re.search(r'Aktualitaet:\s*([0-9]{4}-[0-9]{2}(?:-[0-9]{2})?)', content)
    if match:
        date = match.group(1)
        # Append -01 if the date is in YYYY-MM format
        if re.match(r'^[0-9]{4}-[0-9]{2}$', date):
            date += '-01'
            tile["timestamp"] = date
    else:
        return

def request_download_link(prep_url, tile_name, tile_id):
    print(f"\rRequesting download link for tile: {tile_name}", end="", flush=True)
    full_prepare_link = f"{prep_url}items={tile_id}&format=zip"
    download_link = requests.get(full_prepare_link)
    print(f"\r{46 * ' '}", end="")
    return download_link.text

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

    base_url = config_info['links']['download_link']
    prepare_base_url, tile_ids = get_tile_id(base_url)

    for i, tile in enumerate(tiles, start=1):
        tile_name = tile['tile_name']
        id = tile_ids.get(tile_name)

        # Check if timestamp is within date range
        if DT.within_date_range(tile["timestamp"], init["date_range"]):
            pass
        else:
            print(f"Tile {tile_name} not in date range")
            continue

        # Download the file
        if not tile["location"]:
            try:
                download_url = request_download_link(prepare_base_url, tile_name, id)

                save_path = f"{landing}/{state.lower()}/{data_type.lower()}_{tile_name}/{data_type.lower()}_{tile_name}.{download_url.split('.')[-1]}"
                os.makedirs(f"{landing}/{state.lower()}/{data_type.lower()}_{tile_name}", exist_ok=True)

                DT.download_file(download_url, save_path, tile)
                print(f" [{i} of {total_tiles}]")
            except Exception as e:
                print(f"Error while downloading to {tile_name}: {e}")
                DT.delete_files_and_dir(save_path)

            file_path = DT.find_file(os.path.dirname(save_path))

            if data_type == "DTM":
                find_meta_file_and_get_date(os.path.dirname(save_path), tile)
                

            # Update the tile format
            tile['format'] = file_path.split('.')[-1]

            if init['upload_s3']:
                # Upload the file to S3
                try:
                    s3_path = f"{config_info['links']['s3_path']}{data_type.lower()}_{tile['tile_name']}/{os.path.basename(file_path)}"
                    DT.upload_file(save_path, s3_path)
                    tile['location'] = s3_path
                    if init['delete']:
                        DT.delete_files_and_dir(os.path.dirname(save_path))
                except Exception as e:
                    print(f"Error while uploading to {s3_path}: {e}")
            else:
                tile['location'] = os.path.dirname(file_path)

            DT.save_json(meta_path, tiles_data)
        else:
            print(f"Tile {tile['tile_name']} is already downloaded [{i} of {total_tiles}]")
    
    if init['delete']:
        DT.delete_files_and_dir(landing)