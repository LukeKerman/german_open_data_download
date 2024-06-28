import json
import os
import time
import re

import numpy as np
import pandas as pd
import geopandas as gpd
from pyproj import Transformer, CRS
from shapely.geometry import box, Polygon, MultiPolygon
from shapely.ops import transform
from geojson import Feature, Polygon as GeoPolygon
import folium
import matplotlib.pyplot as plt


def load_json(file_path):
    """
    Loads a JSON file from the specified file path.

    Args:
        file_path (str): The path to the JSON file.

    Returns:
        dict: The loaded JSON data.
    """
    with open(file_path, 'r') as f:
        return json.load(f)

def save_json(file_path, data):
    """
    Saves data to a JSON file at the specified file path.

    Args:
        file_path (str): The path to the JSON file.
        data (dict): The data to save to the JSON file.
    """
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4)

def get_multipolygon_from_geojson(file_path):
    """
    Loads a GeoJSON file and ensures its CRS is EPSG:25832.

    Args:
        file_path (str): The path to the GeoJSON file.

    Returns:
        MultiPolygon: A MultiPolygon object containing all polygons and multipolygons from the GeoJSON file.
    """
    gdf = gpd.read_file(file_path)
    if gdf.crs != 'EPSG:25832':
        gdf = gdf.to_crs('EPSG:25832')
    polygons = [geom for geom in gdf.geometry if geom.geom_type in ['Polygon', 'MultiPolygon']]
    multi_polygon = MultiPolygon([poly for geom in polygons for poly in (geom.geoms if geom.geom_type == 'MultiPolygon' else [geom])])
    return multi_polygon

def create_tiles_within_polygon(polygon, config, data_type, state_name, crs="EPSG:25832"):
    """
    Creates tiles within a given polygon.

    Args:
        polygon (Polygon or MultiPolygon): The input polygon.
        config (dict): The configuration dictionary containing tile size information.
        data_type (str): The type of data.
        state_name (str): The name of the state.
        crs (str, optional): The coordinate reference system. Defaults to "EPSG:25832".

    Returns:
        list: A list of tuples, each containing a tile name and its coordinates.
    """
    tile_info = config[data_type][state_name]['tile_info']
    tile_size = tile_info['tile_size']
    start_x = tile_info['x']
    start_y = tile_info['y']
    min_x, min_y, max_x, max_y = polygon.bounds
    tiles_in_polygon = []

    x_coords = np.arange(np.floor(min_x / tile_size) * tile_size - start_x, np.ceil(max_x / tile_size) * tile_size + start_x, tile_size)
    y_coords = np.arange(np.floor(min_y / tile_size) * tile_size - start_y, np.ceil(max_y / tile_size) * tile_size + start_y, tile_size)

    for x in x_coords:
        for y in y_coords:
            tile_bbox = box(x, y, x + tile_size, y + tile_size)
            if tile_bbox.intersects(polygon):
                tile_name = f"{str(crs)[-2:]}_{int(x // 1000):03}_{int(y // 1000):04}"
                tile_coords = list(tile_bbox.exterior.coords)[:-1]
                tiles_in_polygon.append((tile_name, tile_coords))
    
    return tiles_in_polygon

def print_progress(state_name, current, total):
    """
    Prints the progress of a task.

    Args:
        state_name (str): The name of the state.
        current (int): The current progress value.
        total (int): The total value for completion.
    """
    progress = (current / total) * 100
    if progress == 100:
        print(f"\rProgress of state {state_name}:\t100.0% (Completed)")
    else:
        print(f"\rProgress of state {state_name}:\t{progress:>4.1f}%", end="", flush=True)

def process_state_tiles(state_row, multi_polygon, config, data_type, crs, transform_func=None, show_progress=True):
    """
    Processes tiles for a given state.

    Args:
        state_row (GeoSeries): The GeoSeries containing state geometry.
        multi_polygon (MultiPolygon): The MultiPolygon object for the area of interest.
        config (dict): The configuration dictionary.
        data_type (str): The type of data.
        crs (str): The coordinate reference system.
        transform_func (function, optional): A function to transform coordinates. Defaults to None.
        show_progress (bool, optional): Whether to show progress. Defaults to True.

    Returns:
        list: A list of dictionaries, each containing tile information.
    """
    state_name = state_row['GEN']
    intersecting_polygon = multi_polygon.intersection(state_row['geometry'])
    if intersecting_polygon.is_empty or intersecting_polygon.bounds is None:
        return []

    tiles = create_tiles_within_polygon(intersecting_polygon, config, data_type, state_name, crs)
    total_tiles = len(tiles)

    state_tile_list = []
    for i, (tile_name, tile_coords) in enumerate(tiles):
        tile_poly = Polygon(tile_coords)
        if state_row['geometry'].intersects(tile_poly):
            tile_coords_formatted = [transform_func(x, y) if transform_func else (x, y) for x, y in tile_coords]
            state_tile_list.append({
                "tile_name": tile_name,
                "timestamp": None,
                "location": None,
                "format": None,
                "tile_coords": tile_coords_formatted
            })
        if show_progress:
            print_progress(state_name, i + 1, total_tiles)
            time.sleep(0.001)
    return state_tile_list

def display_results(file_path):
    """
    Displays the results of tile processing from a JSON file.

    Args:
        file_path (str): The path to the JSON file containing the results.
    """
    full_data = load_json(file_path)
    data = full_data["tiles"]
    
    if not data:
        print("\nNo tile data found in the file.\n")
        return

    total_tiles = sum(len(state_data["tile_list"]) for state_data in data.values())
    if total_tiles == 0:
        print("\nNo tiles found for any state.\n")
        return

    print(f"\n\n{'-' * 10} RESULTS {'-' * 10}\n\n Tile counts per state ({list(data.values())[0]['data_type']})")
    print("-" * 29)
    for state, state_data in data.items():
        tile_count = len(state_data["tile_list"])
        if tile_count:
            print(f"{state}:  {tile_count:>5} tiles")
    print("-" * 29)
    print(f"Total number of tiles: {total_tiles}\n")

def create_json_from_csv(aoi_path, config, init):
    """
    Creates a JSON file with tile data from a CSV input.

    Args:
        aoi_path (str): The path to the CSV file containing the area of interest.
        config (dict): The configuration dictionary.
        init (dict): The initialization dictionary.

    Returns:
        dict: A dictionary containing the tile data.
    """
    csv = pd.read_csv(aoi_path)
    if len(init["selected_states"]) != 1:
        raise ValueError(f"'selected_states' must be a single state to be read from a CSV file.")
    
    state_name = init["selected_states"][0]
    data_type = init["data_type"]

    transformer = Transformer.from_crs("EPSG:25833", "EPSG:25832", always_xy=True).transform
    
    def transform_tile_name(tile_name):
        pattern = re.match(r"(\d{2})_(\d+)_(\d+)", tile_name)
        if not pattern:
            raise ValueError(f"Tile name {tile_name} does not match the expected pattern")
        
        crs, x, y = pattern.groups()
        x, y = int(x), int(y)
        tile_size = config[data_type][state_name]['tile_info']['tile_size']
        
        x_coords = [x * 1000, x * 1000, x * 1000 + tile_size, x * 1000 + tile_size]
        y_coords = [y * 1000, y * 1000 + tile_size, y * 1000 + tile_size, y * 1000]
        return [transformer(xx, yy) if crs == '33' else (xx, yy) for xx, yy in zip(x_coords, y_coords)]

    total = len(csv) - 1
    tiles = []
    for index, (_, row) in enumerate(csv.iterrows()):
        tiles.append({"tile_name": row['tile_name'], "timestamp": None, "location": None, "format": None, "tile_coords": transform_tile_name(row['tile_name'])})
        print_progress(state_name, index, total)

    return {
        "aoi_name": os.path.basename(init["aoi_path"]),
        "data_type": data_type,
        "tiles": {state_name: {"data_type": init["data_type"], "tile_list": tiles}}
    }

def convert_and_save_geojson(meta_path, input_json, target_crs="EPSG:25832"):
    """
    Converts and saves GeoJSON with CRS conversion support.

    Args:
        meta_path (str): The path to the metadata file.
        input_json (dict): The input JSON data.
        target_crs (str, optional): The target coordinate reference system. Defaults to "EPSG:25832".
    """
    crs_src = CRS.from_epsg(25832)
    crs_dst = CRS.from_epsg(target_crs.split(":")[1])
    transformer = Transformer.from_crs(crs_src, crs_dst, always_xy=True)

    features = []

    for region, region_data in input_json["tiles"].items():
        for tile in region_data["tile_list"]:
            coords = [tuple(transformer.transform(x, y)) for x, y in tile["tile_coords"]]
            coords.append(coords[0])  # Close the polygon

            polygon = GeoPolygon([coords])
            feature = Feature(
                geometry=polygon,
                properties={
                    "tile_name": tile["tile_name"],
                    "state": region,
                    "timestamp": tile["timestamp"],
                    "format": tile["format"],
                    "location": tile["location"]
                }
            )
            features.append(feature)
    
    geojson = {
        "type": "FeatureCollection",
        "name": input_json["aoi_name"],
        "crs": {
            "type": "name",
            "properties": {
                "name": f"urn:ogc:def:crs:EPSG::{target_crs.split(':')[1]}"
            }
        },
        "features": features
    }

    geojson_path = os.path.splitext(meta_path)[0] + ".geojson"
    save_json(geojson_path, geojson)

def create_folium_map(meta_path, aoi_path):
    """
    Creates a Folium map with the tile data.

    Args:
        meta_path (str): The path to the metadata file.
        aoi_path (str): The path to the area of interest file.
    """
    with open(meta_path.replace("json", "geojson")) as f:
        data = json.load(f)
    
    with open(aoi_path) as f:
        aoi_data = json.load(f)

    # Extract unique states from the data
    states = list(set(feature['properties']['state'] for feature in data['features']))

    # Generate colors for each state using a colormap
    colors = [plt.cm.gist_rainbow(i / len(states)) for i in range(len(states))]
    color_map = {state: f'#{int(color[0]*255):02x}{int(color[1]*255):02x}{int(color[2]*255):02x}' for state, color in zip(states, colors)}

    transformer = Transformer.from_crs("EPSG:25832", "EPSG:4326", always_xy=True)

    def transform_coordinates(coordinates):
        return [transformer.transform(x, y) for x, y in coordinates]

    def transform_polygon(polygon):
        return [transform_coordinates(ring) for ring in polygon]

    def style_function(feature):
        state = feature['properties']['state']
        return {
            'fillColor': color_map.get(state, 'black'),
            'color': color_map.get(state, 'black'),
            'weight': 1.5,
            'fillOpacity': 0.4,
        }

    def aoi_style_function(feature):
        return {
            'fillColor': 'none',
            'color': 'black',
            'weight': 1,
            'fillOpacity': 0.6,
        }

    for feature in data['features']:
        if feature['geometry']['type'] == 'Polygon':
            feature['geometry']['coordinates'] = transform_polygon(feature['geometry']['coordinates'])
        elif feature['geometry']['type'] == 'MultiPolygon':
            feature['geometry']['coordinates'] = [transform_polygon(polygon) for polygon in feature['geometry']['coordinates']]

    for feature in aoi_data['features']:
        if feature['geometry']['type'] == 'Polygon':
            feature['geometry']['coordinates'] = transform_polygon(feature['geometry']['coordinates'])
        elif feature['geometry']['type'] == 'MultiPolygon':
            feature['geometry']['coordinates'] = [transform_polygon(polygon) for polygon in feature['geometry']['coordinates']]

    def calculate_bounding_box(aoi_data):
        min_lon, min_lat = float('inf'), float('inf')
        max_lon, max_lat = float('-inf'), float('-inf')
        
        for feature in aoi_data['features']:
            if feature['geometry']['type'] == 'Polygon':
                for ring in feature['geometry']['coordinates']:
                    for lon, lat in ring:
                        min_lon, min_lat = min(min_lon, lon), min(min_lat, lat)
                        max_lon, max_lat = max(max_lon, lon), max(max_lat, lat)
            elif feature['geometry']['type'] == 'MultiPolygon':
                for polygon in feature['geometry']['coordinates']:
                    for ring in polygon:
                        for lon, lat in ring:
                            min_lon, min_lat = min(min_lon, lon), min(min_lat, lat)
                            max_lon, max_lat = max(max_lon, lon), max(max_lat, lat)
        
        return [[min_lat, min_lon], [max_lat, max_lon]]

    aoi_bounds = calculate_bounding_box(aoi_data)

    m = folium.Map(tiles='CartoDB Positron No Labels')
    m.fit_bounds(aoi_bounds)

    def create_popup(properties):
        state = properties.get('state', 'N/A')
        tile_name = properties.get('tile_name', 'N/A')
        timestamp = properties.get('timestamp', 'N/A')
        fmt = properties.get('format', 'N/A')
        popup_content = f"""
        <table style="width:160px">
            <tr>
                <td style="text-align:left"><b>State:</b></td>
                <td style="text-align:right">{state}</td>
            </tr>
            <tr>
                <td style="text-align:left"><b>Name:</b></td>
                <td style="text-align:right">{tile_name}</td>
            </tr>
            <tr>
                <td style="text-align:left"><b>Timestamp:</b></td>
                <td style="text-align:right">{timestamp}</td>
            </tr>
            <tr>
                <td style="text-align:left"><b>Format:</b></td>
                <td style="text-align:right">{fmt}</td>
            </tr>
        </table>
        """
        return folium.Popup(popup_content, max_width=250)

    geojson_fg = folium.FeatureGroup(name='geojson')

    for feature in data['features']:
        coords = feature['geometry']['coordinates']
        if feature['geometry']['type'] == 'Polygon':
            folium.GeoJson(
                feature,
                style_function=style_function,
                name='geojson',
                smooth_factor=0,
                zoom_on_click=False,
                highlight_function=lambda x: {'weight': 5, 'color': 'yellow'},
                popup=create_popup(feature['properties'])
            ).add_to(geojson_fg)
        elif feature['geometry']['type'] == 'MultiPolygon':
            for polygon in coords:
                folium.GeoJson(
                    {'type': 'Feature', 'geometry': {'type': 'Polygon', 'coordinates': polygon}, 'properties': feature['properties']},
                    style_function=style_function,
                    name='geojson',
                    smooth_factor=0,
                    zoom_on_click=False,
                    highlight_function=lambda x: {'weight': 5, 'color': 'yellow'},
                    popup=create_popup(feature['properties'])
                ).add_to(geojson_fg)

    geojson_fg.add_to(m)

    folium.GeoJson(
        aoi_data,
        style_function=aoi_style_function,
        name='aoi',
        smooth_factor=-1,
    ).add_to(m)

    folium.LayerControl().add_to(m)

    legend_html = '''
         <div style="
         position: fixed; 
         bottom: 50px; left: 50px; width: 75px; height: auto; 
         background-color: white; z-index:9999; font-size:14px;
         padding: 10px;
         border: 2px solid grey;
         border-radius: 5px;
         ">
         <b>States</b><br>
    '''
    for state, color in color_map.items():
        legend_html += f'''
        <i style="background: {color}; width: 10px; height: 10px; display: inline-block; opacity: 0.5;"></i>
        {state}<br>
        '''
    legend_html += '</div>'

    # Step 3: Add the Legend to the Map
    legend = folium.Element(legend_html)
    m.get_root().html.add_child(legend)

    html_path = os.path.splitext(meta_path)[0] + '_map.html'
    m.save(html_path)

def create_state_tile_file(init, config):
    """
    Creates state tile files from the provided initialization and configuration data.

    Args:
        init (dict): The initialization dictionary containing paths and other settings.
        config (dict): The configuration dictionary containing tile and state information.
    """
    aoi_path = init['aoi_path']
    data_type = init['data_type']
    meta_path = init['meta_path']
    selected_states = init['selected_states']
    os.makedirs(os.path.dirname(meta_path), exist_ok=True)

    print(f"\n===== Generating {data_type} Tiles by State =====\n\nAOI file: {os.path.basename(aoi_path)}\n{'-' * 42}")

    if os.path.exists(meta_path):
        full_data = load_json(meta_path)
        if full_data["aoi_name"] == os.path.basename(aoi_path) and full_data["data_type"] == data_type:
            print(f"Tile data file for '{full_data['aoi_name']}' ({data_type}) already exists.\nProceeding with the existing file.")
            display_results(meta_path)
            return
    else:
        os.makedirs(os.path.dirname(meta_path), exist_ok=True)

    selected_states = selected_states or []
    state_files_dir = 'bdl'
    state_geo_25832 = gpd.read_file(os.path.join(state_files_dir, 'DE_bdl_utm32.geojson'))
    state_geo_25833 = gpd.read_file(os.path.join(state_files_dir, 'DE_bdl_utm33.geojson'))
    
    state_tiles = {"aoi_name": os.path.basename(aoi_path), "data_type": data_type, "tiles": {}}
    transform_25832_to_25833 = Transformer.from_crs(state_geo_25832.crs, state_geo_25833.crs, always_xy=True).transform
    transform_25833_to_25832 = Transformer.from_crs(state_geo_25833.crs, state_geo_25832.crs, always_xy=True).transform

    if aoi_path.endswith(".csv"):
        state_tiles = create_json_from_csv(aoi_path, config, init)
    else:
        aoi_multi_polygon = get_multipolygon_from_geojson(aoi_path)
        for _, state_row in state_geo_25832.iterrows():
            state_name = state_row['GEN']
            if not selected_states or state_name in selected_states:
                state_tiles["tiles"][state_name] = {"data_type": data_type, "tile_list": process_state_tiles(state_row, aoi_multi_polygon, config, data_type, state_geo_25832.crs)}

        aoi_multi_polygon_25833 = transform(transform_25832_to_25833, aoi_multi_polygon)
        for _, state_row in state_geo_25833.iterrows():
            state_name = state_row['GEN']
            if not selected_states or state_name in selected_states:
                state_tiles["tiles"][state_name] = {"data_type": data_type, "tile_list": process_state_tiles(state_row, aoi_multi_polygon_25833, config, data_type, state_geo_25833.crs, transform_func=transform_25833_to_25832)}

    save_json(meta_path, state_tiles)
    convert_and_save_geojson(meta_path, state_tiles)
    create_folium_map(meta_path, aoi_path)
    display_results(meta_path)

# Main entry point
if __name__ == "__main__":
    config = load_json('config.json')
    init = load_json('init.json')

    create_state_tile_file(init, config)

    tiles_data = load_json(init['meta_path'])
    convert_and_save_geojson(init['meta_path'], tiles_data)

    create_folium_map(init['meta_path'], init['aoi_path'])
