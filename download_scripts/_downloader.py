import json
import os
import requests
from zipfile import ZipFile
from cloudpathlib import CloudPath

def upload_file(file_path, target):
    s3_target = CloudPath(target)
    #s3_target.upload_from(file_path)
    #delete_files_and_dir(file_path)

def delete_files_and_dir(dir):
    files_and_dir = os.listdir(dir)
    for item in files_and_dir:
        item_path = os.path.join(dir, item)
        if os.path.isfile(item_path):
            os.remove(item_path)
        elif os.path.isdir(item_path):
            delete_files_and_dir(item_path)
    os.rmdir(dir)

def download_file(download_url, save_path, tile_info):
    response = requests.get(download_url, stream=True)
    total_size = int(response.headers.get('content-length', 0))
    chunk_size = 512*1024
    downloaded_size = 0
    
    with open(save_path, 'wb') as file:
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:
                file.write(chunk)
                downloaded_size += len(chunk)
                progress = (downloaded_size / total_size) * 100
                if progress == 100:
                    print(f"\rDownload progress of tile {tile_info['tile_name']}:\t{progress:>.1f}% completed", end="")
                else:
                    print(f"\rDownload progress of tile {tile_info['tile_name']}:\t{progress:>.1f}% ({total_size/(1024 * 1024):.1f} MB)", end="")
    
    if download_url.endswith('.zip'):
        with ZipFile(save_path, 'r') as zip_ref:
            file_list = zip_ref.namelist()
            if len(file_list) == 1:
                zip_ref.extractall(os.path.dirname(save_path))
            else:
                extract_dir = os.path.join(os.path.dirname(save_path), os.path.splitext(os.path.basename(save_path))[0])
                os.makedirs(extract_dir, exist_ok=True)
                zip_ref.extractall(extract_dir)
        os.remove(save_path)

def save_tile_metadata(state, tiles, data_type):
    state_tiles = {}
    state_tiles[state] = {
            "data_type": data_type,
            "tile_list": tiles
            }
    with open(f'meta/{state.lower()}_tile_data.json', 'w') as f:
        json.dump(tiles, f, indent=4)