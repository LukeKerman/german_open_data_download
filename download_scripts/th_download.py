import os
import csv

import requests

from _downloader import DownloadTools


def get_id_and_creation_date(meta_url, tiles, data_type):
    csv_path = f'helper/{os.path.basename(__file__)[:2].lower()}_{data_type.lower()}_ids.csv'

    if data_type == "DOP":
        start_id = 530448
        end_id = 549479

    tile_ids = []

    if os.path.exists(csv_path):
        with open(csv_path, mode='r', newline='') as file:
            reader = csv.reader(file, delimiter=';')
            next(reader)
            tile_ids = [(row[0], row[1]) for row in reader]
        for i, tile in enumerate(tiles, start=1):
            progress = i/len(tiles)*100
            print(f"\rLoading meta data: {progress:>3.1f}%", end="")
            tile_id = [tile_id for tile_nr, tile_id in tile_ids if tile_nr == tile['tile_name']][0]
            try:
                response = requests.get(meta_url.format(tile_id))
                if response.status_code == 200:
                    data = response.json()
                    object_data = data["object"]
                    if data["success"] == "true" and object_data["bildnr"] == tile["tile_name"].replace('_', '', 1):
                        if tile["timestamp"] != None:
                            print(f"Timestamp already set for tile: {tile['tile_name']}")
                        else:
                            tile["timestamp"] = object_data["datum"][:10]
                            print()
            except Exception as e:
                print(f"Error with {tile['tile_name']} (id: {tile_id}){e}")
    else:
        for tile_id in range(start_id, end_id + 1):
            try:
                response = requests.get(meta_url.format(tile_id))
                print(f"\Requesting meta data: {tile_id/(end_id-start_id)*100:>3.1f}%", end="")
                if response.status_code == 200:
                    data = response.json()
                    if data["success"] == "true" and "object" in data:
                        object_data = data["object"]
                        tile_nr = f"{object_data['bildnr'][:2]}_{object_data['bildnr'][2:]}"
                        tile_ids.append((tile_nr, tile_id)) 
                        for tile in tiles:
                            if tile_nr == tile["tile_name"]:
                                if tile["timestamp"] != None:
                                    print(f"Timestamp already set for tile: {tile['tile_name']}")
                                else:
                                    tile["timestamp"] = object_data["datum"][:10]
                                break
            except Exception as e:
                print(f"Error with id: {tile_id} {e}")

        with open(csv_path, mode='w', newline='') as file:
            writer = csv.writer(file, delimiter=';')
            writer.writerow(['tile_nr', 'id'])
            for row in tile_ids:
                writer.writerow(row)
    return dict(tile_ids)

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
        print(f"No links provided for {state} in configuration file.")
        return

    meta_data_url = config_info['links']['meta_data_link']
    tile_ids = get_id_and_creation_date(meta_data_url, tiles, data_type)
    tiles = DT.filter_tiles_by_date(tiles, init["date_range"])
    state_data["tile_list"] = tiles
    tiles_data["tiles"][state] = state_data

    total_tiles = len(tiles)

    if not init["download"]: return

    for i, tile in enumerate(tiles, start=1):
        tile_name = tile['tile_name']
        download_url = config_info['links']['download_link'].format(tile_ids.get(tile_name))

        save_path = f"{landing}/{state.lower()}/{data_type.lower()}_{tile_name}.zip"
        
        # Download the file
        if not tile["location"]:
            try:
                DT.download_file(download_url, save_path, tile)
                print(f" [{i} of {total_tiles}]")
            except Exception as e:
                print(f"Error while downloading to {tile_name}: {e}")
                DT.delete_files_and_dir(save_path)

            # Find the relevant files in the extract path
            file_path = DT.find_file(save_path)

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
                tile['location'] = save_path.split('.')[0]
                
            # Update the tile format
            tile['format'] = file_path.split('.')[-1]

        else:
            print(f"Tile {tile['tile_name']} is already downloaded [{i} of {total_tiles}]")
    
    if init['delete']:
        DT.delete_files_and_dir(landing)