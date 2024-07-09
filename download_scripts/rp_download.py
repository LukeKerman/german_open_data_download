import os

import requests

from _downloader import DownloadTools

def get_creation_date(meta_url, tiles, data_type):

    for tile in tiles:
        tile_name_parts = tile["tile_name"].split('_')
        if data_type == "DTM":
            tile_nr = f'{tile_name_parts[1]}_{tile_name_parts[2]}'
            start_tag = '<gco:DateTime>'
            end_tag = '</gco:DateTime>'
        else:
            tile_nr = tile_name_parts[1] + tile_name_parts[2]
            start_tag = '<Date>'
            end_tag = '</Date>'

        # Fetch the XML content from the URL
        response = requests.get(meta_url.format(tile_nr), verify=False)
        
        # Raise an exception if the request was unsuccessful
        response.raise_for_status()
        
        # Convert the response content to a string
        xml_content = response.text
        
        # Find the start and end of the <Date> tag
        
        start_index = xml_content.find(start_tag)
        end_index = xml_content.find(end_tag)
        
        if start_index != -1 and end_index != -1:
            # Extract the date string
            date_str = xml_content[start_index + len(start_tag):end_index]
            tile["timestamp"] = date_str[:10]
        else:
            tile["timestamp"] = None
            print("Date element not found in the XML file.")


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
    get_creation_date(meta_data_url, tiles, data_type)
    tiles = DT.filter_tiles_by_date(tiles, init["date_range"])
    state_data["tile_list"] = tiles
    tiles_data["tiles"][state] = state_data

    total_tiles = len(tiles)

    if not init["download"]: return

    for i, tile in enumerate(tiles, start=1):
        tile_name = tile['tile_name']
        download_url = config_info['links']['download_link'].format(tile_name)

        filename = download_url.split('/')[-1]
        save_path = f"{landing}/{state.lower()}/{data_type.lower()}_{tile_name}/{filename}"
        os.makedirs(f"{landing}/{state.lower()}/{data_type.lower()}_{tile_name}", exist_ok=True)
        
        # Download the file
        if not tile["location"]:
            try:
                DT.download_file(download_url, save_path, tile)
                print(f" [{i} of {total_tiles}]")
            except Exception as e:
                print(f"Error while downloading to {tile_name}: {e}")
                DT.delete_files_and_dir(os.path.dirname(save_path))

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