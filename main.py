import json
import os
import sys

from state_tile_creator import load_json, save_json, create_state_tile_file


def call_download_script(state, tiles_data, config):
    script_path = os.path.join('download_scripts', f'{state.lower()}_download.py')
    if not os.path.exists(script_path):
        print(f"Script {script_path} does not exist.")
        return
    
    module_name = f"download_scripts.{state.lower()}_download"
    if module_name in sys.modules:
        module = sys.modules[module_name]
    else:
        module = __import__(module_name, fromlist=['download_tiles'])
    module.download_tiles(tiles_data, config)

def main():
    # Add the download_scripts directory to the Python path
    sys.path.append(os.path.join(os.path.dirname(__file__), 'download_scripts'))

    # Load the JSON files
    init = load_json('./init.json')
    config = load_json('./config.json')

    config_data = (init, config)

    create_state_tile_file(init, config, show=False)

    tiles_data = load_json(init['meta_path'])
    
    # Iterate through each state in the tiles data
    for state, state_data in tiles_data.items():
        if state_data["tile_list"]:
            print(f"INITIALIZING DOWNLOAD PROCESS\n\nCalling {state.lower()}_download.py for state {state} with {len(state_data['tile_list'])} tiles.")
            call_download_script(state, tiles_data, config_data)
            
    save_json(init['meta_path'], tiles_data)
    print("Download process completed")

if __name__ == "__main__":
    main()
