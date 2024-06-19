import os
import json
import requests

from _downloader import DownloadTools


def download_tiles(tiles_data, config_data):
    state = os.path.basename(__file__)[:2].upper()
    init, config = config_data
    landing = init['local_landing_path']

    state_data = tiles_data[state]
    tiles = state_data["tile_list"]
    data_type = state_data["data_type"]

    config_info = config[data_type][state]
    meta_path = init['meta_path']

    DT = DownloadTools()

    total_tiles = len(tiles)

    for i, tile in enumerate(tiles, start=1):
        tile_name = tile['tile_name']
        download_url = config_info['links']['download_link'].format(tile_name)

        filename = f"{data_type.lower()}_{tile_name}.{download_url.split('.')[-1]}"
        save_path = f"{landing}/{state.lower()}/{filename}"
        os.makedirs(f"{landing}/{state.lower()}", exist_ok=True)

        '''# Check if timestamp is within date range
        if DT.within_date_range(tile["timestamp"], init["date_range"]):
            pass
        else:
            print(f"Tile {tile_name} not in date range")
            continue'''
        
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
                file_path_list = DT.find_file(save_path)
                for file_path in file_path_list:
                    file_name = os.path.basename(file_path)
                    sub_folder = f"{data_type.lower()}_{'_'.join(file_name.split('_')[1:4])}"
                    s3_path = f"{config_info['links']['s3_path']}{sub_folder}/{file_name}"
                    try:
                        DT.upload_file(save_path, s3_path)
                        tile['location'] = s3_path
                        if init['delete']:
                            DT.delete_files_and_dir(os.path.dirname(save_path))
                    except Exception as e:
                        print(f"Error while uploading to {s3_path}: {e}")
            else:
                tile['location'] = save_path.split('.')[0]
            
            # Update the tile format
            tile['format'] = download_url.split('.')[-1]

            DT.save_json(meta_path, tiles_data)
        else:
            print(f"Tile {tile['tile_name']} is already downloaded [{i} of {total_tiles}]")
    
    if init['delete']:
        DT.delete_files_and_dir(landing)