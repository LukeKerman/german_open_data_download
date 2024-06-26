import os
import requests
import csv
import time

from _downloader import DownloadTools


def get_id_and_creation_date(meta_url, tiles, data_type):
    csv_path = f'helper/{os.path.basename(__file__)[:2].lower()}_{data_type.lower()}_ids.csv'

    tile_ids = []

    if os.path.exists(csv_path):
        with open(csv_path, mode='r', newline='') as file:
            reader = csv.reader(file, delimiter=';')
            next(reader)
            tile_ids = [(row[0], row[1]) for row in reader]
        for i, tile in enumerate(tiles, start=1):
            progress = i/len(tiles)*100
            print(f"\rLoading meta data: {progress:>3.1f}%", end="", flush=True)
            tile_id = [tile_id for tile_nr, tile_id in tile_ids if tile_nr == tile['tile_name']][0]
            try:
                response = requests.get(meta_url.format(tile_id), verify=False)
                if response.status_code == 200:
                    data = response.json()
                    object_data = data["object"]
                    if data["success"] == "true" and object_data["title"] == tile["tile_name"].replace('_', ''):
                        if tile["timestamp"] != None:
                            print(f"\tTimestamp already set for tile: {tile['tile_name']}", end="")
                        else:
                            tile["timestamp"] = object_data["e_datum"][:10]
            except Exception as e:
                print(f"Error with {tile['tile_name']} (id: {tile_id}){e}")
    else:
        start_id = 1
        end_id = 22000

        for tile_id in range(start_id, end_id + 1):
            try:
                response = requests.get(meta_url.format(tile_id), verify=False)
                print(f"\rRequesting meta data: {tile_id/(end_id-start_id)*100:>3.1f}%", end="", flush=True)
                if response.status_code == 200:
                    data = response.json()
                    if data["success"] == "true" and "object" in data:
                        object_data = data["object"]
                        tile_nr = f"{object_data['title'][:2]}_{object_data['title'][2:5]}_{object_data['title'][5:]}"
                        tile_ids.append((tile_nr, tile_id)) 
                        for tile in tiles:
                            if tile_nr == tile["tile_name"]:
                                if tile["timestamp"] != None:
                                    print(f"Timestamp already set for tile: {tile['tile_name']}")
                                else:
                                    tile["timestamp"] = object_data["e_datum"]
                                break
            except Exception as e:
                print(f"Error with id: {tile_id} {e}")

        with open(csv_path, mode='w', newline='') as file:
            writer = csv.writer(file, delimiter=';')
            writer.writerow(['tile_nr', 'id'])
            for row in tile_ids:
                writer.writerow(row)
    return dict(tile_ids)

def request_download_link(request_url):
    
    response = requests.get(request_url, verify=False)
    
    response_json = response.json()
    job_id = response_json["id"]
    status_base_url = response_json["statusUrl"]
    status_url = f"{status_base_url}?action=status&job={job_id}"

    start_time = time.time()
    request_frequency = 5

    while True:
        interation_start = time.time()
        status_response = requests.get(status_url, verify=False)
        status_json = status_response.json()
        elapsed_time = time.time() - start_time
        print(f"\rStatus: {status_json['status']} - request time: {elapsed_time:.0f}s ", end="")
        
        if status_json.get('status') == 'done':
            return status_json["downloadUrl"]
        elif elapsed_time > 300 or status_json['success'] == False:
            print("Error: The server did not provide a download link.")
            break

        time.sleep(max(0, request_frequency - (time.time() - interation_start)))


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
    tile_ids = get_id_and_creation_date(meta_data_url, tiles, data_type)

    for i, tile in enumerate(tiles, start=1):
        tile_name = tile['tile_name']
        
        if data_type == "DTM":
            request_url = config_info['links']['download_link'].format(tile_name, tile_ids.get(tile_name))
            save_path = f"{landing}/{state.lower()}/{data_type.lower()}_{tile_name}/{data_type.lower()}_{tile_name}"
        else:
            year = tile["timestamp"][:4]
            tile_name_1km = tile_name.replace("_", "", 1)
            tile_name_10km = f"{tile_name_1km[:4]}0_{tile_name_1km[-4:-1]}0"
            request_url = config_info['links']['download_link'].format(year, tile_name_10km, tile_name_1km, tile_ids.get(tile_name))
            save_path = f"{landing}/{state.lower()}/{data_type.lower()}_{tile_name}"

        save_path = f"{landing}/{state.lower()}/{data_type.lower()}_{tile_name}/{data_type.lower()}_{tile_name}"

        # Check if timestamp is within date range
        if DT.within_date_range(tile["timestamp"], init["date_range"]):
            pass
        else:
            print(f"Tile {tile_name} not in date range")
            continue
        
        # Download the file
        if not tile["location"]:
            try:
                download_url = request_download_link(request_url)

                DT.download_file(download_url, save_path, tile)
                print(f" [{i} of {total_tiles}]")

                file_path = DT.find_file(os.path.dirname(save_path))

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
            except Exception as e:
                print(f"Error while downloading to {tile_name}: {e}")

            DT.save_json(meta_path, tiles_data)
        else:
            print(f"Tile {tile['tile_name']} is already downloaded [{i} of {total_tiles}]")
    
    if init['delete']:
        DT.delete_files_and_dir(landing)