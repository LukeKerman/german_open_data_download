"""This script processes GeoJSON or CSV data to create tiles within specified polygons and plots the results."""

import json
import os
import time
import re
import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.collections import PatchCollection
from matplotlib.patches import Polygon as MplPolygon
import geopandas as gpd
from pyproj import Transformer, CRS
from shapely.geometry import box, Polygon, MultiPolygon
from shapely.ops import transform
from geojson import Feature, Polygon as GeoPolygon, FeatureCollection

def load_json(file_path):
    """
    Load a JSON file from the specified file path.

    Parameters:
    - file_path (str): Path to the JSON file.

    Returns:
    - dict: The loaded JSON data.
    """
    with open(file_path, 'r') as f:
        return json.load(f)
    
def save_json(file_path, data):
    """
    Save data to a JSON file at the specified file path.

    Parameters:
    - file_path (str): Path to the JSON file.
    - data (dict): Data to be saved.
    """
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4)

def get_multipolygon_from_geojson(file_path):
    """
    Extract a MultiPolygon from a GeoJSON file.

    Parameters:
    - file_path (str): Path to the GeoJSON file.

    Returns:
    - MultiPolygon: A MultiPolygon containing all polygons and multipolygons from the GeoJSON file.
    """
    print("Loading GeoJSON files", end="")
    gdf = gpd.read_file(file_path)

    if gdf.crs != 'EPSG:25832':
        gdf = gdf.to_crs('EPSG:25832')
    
    polygons = [geom for geom in gdf.geometry if geom.geom_type in ['Polygon', 'MultiPolygon']]
    multi_polygon = MultiPolygon([poly for geom in polygons for poly in (geom.geoms if geom.geom_type == 'MultiPolygon' else [geom])])
    
    file_name = os.path.basename(file_path)
    print(f"\rGeoJSON file: {file_name}\n{'-' * 42}")
    
    return multi_polygon

def create_tiles_within_polygon(polygon, config, data_type, state_name, crs="UTM32"):
    """
    Create a grid of tiles within the given polygon.

    Parameters:
    - polygon (Polygon or MultiPolygon): The polygon geometry.
    - config (dict): Configuration dictionary containing tile information.
    - data_type (str): Data type for the tiles.
    - state_name (str): Name of the state for which tiles are being created.
    - crs (str): Coordinate reference system to be used (default is "UTM32").

    Returns:
    - list: A list of tile names and their geometries that are partially or fully within the polygon.
    """

    tile_info = config[data_type][state_name]['tile_info']
    tile_size = tile_info['tile_size']
    start_x = tile_info['x']
    start_y = tile_info['y']

    utm = 32 if crs == "UTM32" else 33 if crs == "UTM33" else ValueError("Error in CRS definition. UTM needs to be UTM32 or UTM33")

    min_x, min_y, max_x, max_y = polygon.bounds

    tiles_in_polygon = []

    x_coords = np.arange(np.floor(min_x / tile_size) * tile_size - start_x, np.ceil(max_x / tile_size) * tile_size + start_x, tile_size)
    y_coords = np.arange(np.floor(min_y / tile_size) * tile_size - start_y, np.ceil(max_y / tile_size) * tile_size + start_y, tile_size)

    for x in x_coords:
        for y in y_coords:
            tile_bbox = box(x, y, x + tile_size, y + tile_size)
            if tile_bbox.intersects(polygon):
                tile_name = f"{utm}_{int(x // 1000):03}_{int(y // 1000):04}"
                tile_coords = list(tile_bbox.exterior.coords)[:-1]
                tiles_in_polygon.append((tile_name, tile_coords))
    
    return tiles_in_polygon

def print_progress(state_name, current, total):
    """
    Print the progress of tile creation for a state.

    Parameters:
    - state_name (str): Name of the state.
    - current (int): Current progress value.
    - total (int): Total number of tiles.
    """
    progress = (current / total) * 100
    if progress == 100:
        print(f"\rProgress of state {state_name}:\t100.0% (Completed)")
    else:
        print(f"\rProgress of state {state_name}:\t{progress:>4.1f}%", end="", flush=True)

def process_state_tiles(state_row, multi_polygon, config, data_type, crs, transform_func=None, show_progress=True):
    """
    Process tiles for a specific state.

    Parameters:
    - state_row (GeoSeries): Row of the state's GeoDataFrame.
    - multi_polygon (MultiPolygon): The multipolygon representing the area of interest.
    - config (dict): Configuration dictionary containing tile information.
    - data_type (str): Data type for the tiles.
    - crs (str): Coordinate reference system.
    - transform_func (callable, optional): Function to transform coordinates.
    - show_progress (bool): Whether to show progress.

    Returns:
    - list: A list of dictionaries containing tile information.
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

def create_state_tile_file(init, config, show=False):
    """
    Main function to create state tile files and plot the results.

    Parameters:
    - init (dict): Initialization dictionary containing paths and settings.
    - config (dict): Configuration dictionary containing tile information.
    - show (bool): Boolean to indicate whether to display the plot.
    """
    aoi_path = init['aoi_path']
    data_type = init['data_type']
    meta_path = init['meta_path']
    selected_states = init['selected_states']

    print(f"\n### TILE BY STATE CREATOR for {data_type} ###\n")

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
    state_geo_utm32 = gpd.read_file(os.path.join(state_files_dir, 'DE_bdl_utm32.geojson'))
    state_geo_utm33 = gpd.read_file(os.path.join(state_files_dir, 'DE_bdl_utm33.geojson'))
    
    state_tiles = {"aoi_name": os.path.basename(aoi_path), "data_type": data_type, "tiles": {}}
    transform_to_utm33 = Transformer.from_crs("EPSG:25832", "EPSG:25833", always_xy=True).transform
    transform_to_utm32 = Transformer.from_crs("EPSG:25833", "EPSG:25832", always_xy=True).transform

    if aoi_path.endswith(".csv"):
        state_tiles = create_json_from_csv(aoi_path, config, init)
        plot_polygons_and_tiles(None, state_tiles["tiles"], state_geo_utm32, state_geo_utm33, meta_path, show=show)
    else:
        multi_polygon = get_multipolygon_from_geojson(aoi_path)

        for _, state_row in state_geo_utm32.iterrows():
            state_name = state_row['GEN']
            if not selected_states or state_name in selected_states:
                state_tiles["tiles"][state_name] = {"data_type": data_type, "tile_list": process_state_tiles(state_row, multi_polygon, config, data_type, 'UTM32')}

        multi_polygon_utm33 = transform(transform_to_utm33, multi_polygon)
        for _, state_row in state_geo_utm33.iterrows():
            state_name = state_row['GEN']
            if not selected_states or state_name in selected_states:
                state_tiles["tiles"][state_name] = {"data_type": data_type, "tile_list": process_state_tiles(state_row, multi_polygon_utm33, config, data_type, 'UTM33', transform_func=transform_to_utm32)}

        plot_polygons_and_tiles(multi_polygon, state_tiles["tiles"], state_geo_utm32, state_geo_utm33, meta_path, show=show)
    save_json(meta_path, state_tiles)
    display_results(meta_path)

def plot_polygons_and_tiles(multi_polygon, state_tiles, state_geo, state_geo_of_utm33, meta_path, show):
    """
    Plot the polygon, tiles, and state boundaries.

    Parameters:
    - multi_polygon (MultiPolygon or None): The multipolygon representing the area of interest.
    - state_tiles (dict): Dictionary of state tiles.
    - state_geo (GeoDataFrame): GeoDataFrame containing the state boundaries in UTM32 projection.
    - state_geo_of_utm33 (GeoDataFrame): GeoDataFrame containing the state boundaries in UTM33 projection.
    - meta_path (str): Path to save the plot.
    - show (bool): Whether to display the plot.
    """
    print(f"{'-' * 42}\ngenerating plot...", end="", flush=True)

    if multi_polygon and not multi_polygon.is_empty:
        minx, miny, maxx, maxy = multi_polygon.bounds
    else:
        all_coords = [coord for tiles in state_tiles.values() for tile in tiles["tile_list"] for coord in tile["tile_coords"]]
        minx, miny = np.min(all_coords, axis=0)
        maxx, maxy = np.max(all_coords, axis=0)

    x_ext, y_ext = maxx - minx, maxy - miny

    dpi, lw_st, lw_p, lw_t = calculate_dpi_and_lw(x_ext, y_ext)
    fig, ax = plt.subplots(figsize=(20, 20))
    colors = [mpl.colormaps['jet'](i / len(state_tiles)) for i in range(len(state_tiles))]

    tile_patches = [
        MplPolygon(tile["tile_coords"], closed=True, edgecolor=tuple(list(color[:3]) + [1]), facecolor=tuple(list(color[:3]) + [0.2]), linestyle='-', linewidth=lw_t)
        for color, tiles in zip(colors, state_tiles.values())
        for tile in tiles["tile_list"]
    ]
    ax.add_collection(PatchCollection(tile_patches, match_original=True))

    if multi_polygon and not multi_polygon.is_empty:
        polygon_patches = [
            MplPolygon(list(polygon.exterior.coords), closed=True, edgecolor='black', facecolor='none', linewidth=lw_p)
            for polygon in multi_polygon.geoms
        ]
        polygon_patches.extend([
            MplPolygon(list(interior.coords), closed=True, edgecolor='black', facecolor='none', linewidth=lw_p)
            for polygon in multi_polygon.geoms for interior in polygon.interiors
        ])
        ax.add_collection(PatchCollection(polygon_patches, match_original=True))

    transform_to_utm32 = Transformer.from_crs("EPSG:25833", "EPSG:25832", always_xy=True).transform
    state_patches = []
    for state_row in state_geo.itertuples():
        state_polygon = state_row.geometry
        if state_polygon.geom_type == 'Polygon':
            state_patches.append(MplPolygon(list(state_polygon.exterior.coords), closed=True, edgecolor='k', facecolor='none', linestyle='-', linewidth=lw_st, alpha=1))
            state_patches.extend([MplPolygon(list(interior.coords), closed=True, edgecolor='k', facecolor='none', linestyle='-', linewidth=lw_st, alpha=1) for interior in state_polygon.interiors])
        elif state_polygon.geom_type == 'MultiPolygon':
            for sub_polygon in state_polygon.geoms:
                state_patches.append(MplPolygon(list(sub_polygon.exterior.coords), closed=True, edgecolor='k', facecolor='none', linestyle='-', linewidth=lw_st, alpha=1))
                state_patches.extend([MplPolygon(list(interior.coords), closed=True, edgecolor='k', facecolor='none', linestyle='-', linewidth=lw_st, alpha=1) for interior in sub_polygon.interiors])
    for state_row in state_geo_of_utm33.itertuples():
        state_polygon = transform(transform_to_utm32, state_row.geometry)
        if state_polygon.geom_type == 'Polygon':
            state_patches.append(MplPolygon(list(state_polygon.exterior.coords), closed=True, edgecolor='k', facecolor='none', linestyle='-', linewidth=lw_st, alpha=1))
            state_patches.extend([MplPolygon(list(interior.coords), closed=True, edgecolor='k', facecolor='none', linestyle='-', linewidth=lw_st, alpha=1) for interior in state_polygon.interiors])
        elif state_polygon.geom_type == 'MultiPolygon':
            for sub_polygon in state_polygon.geoms:
                state_patches.append(MplPolygon(list(sub_polygon.exterior.coords), closed=True, edgecolor='k', facecolor='none', linestyle='-', linewidth=lw_st, alpha=1))
                state_patches.extend([MplPolygon(list(interior.coords), closed=True, edgecolor='k', facecolor='none', linestyle='-', linewidth=lw_st, alpha=1) for interior in sub_polygon.interiors])
    ax.add_collection(PatchCollection(state_patches, match_original=True))

    edge_space_x, edge_space_y = max(x_ext * 0.2, 2500), max(y_ext * 0.2, 2500)
    ax.set_xlim(minx - edge_space_x, maxx + edge_space_x)
    ax.set_ylim(miny - edge_space_y, maxy + edge_space_y)
    ax.set_aspect('equal')
    ax.set_xticks([])
    ax.set_yticks([])

    plot_path = os.path.join(os.path.dirname(meta_path), 'tile_overview.png')
    plt.savefig(plot_path, dpi=dpi, bbox_inches='tight')
    if show:
        plt.show()
    print("\rPlot generation complete.", flush=True)
    time.sleep(1)


def display_results(file_path):
    """
    Display the results of the tile creation process.

    Parameters:
    - file_path (str): Path to the JSON file containing the results.
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

    print(f"\n\n{10 * '-'} RESULTS {10 * '-'}\n\n Tile counts per state ({list(data.values())[0]['data_type']})")
    print("-" * 29)
    for state, state_data in data.items():
        tile_count = len(state_data["tile_list"])
        if tile_count:
            print(f"{state}:  {tile_count:>5} tiles")
    print("-" * 29)
    print(f"Total number of tiles: {total_tiles}\n")

def calculate_dpi_and_lw(x_ext, y_ext):
    """
    Calculate DPI and line widths based on the extent of the area.

    Parameters:
    - x_ext (float): Extent in the x-direction.
    - y_ext (float): Extent in the y-direction.

    Returns:
    - tuple: Rounded DPI value, line width for borders, line width for polygons, line width for tiles.
    """
    avg_ext = (x_ext + y_ext) / 2.0
    min_dpi, max_dpi = 500, 2000
    max_extent = 250000
    normalized_ext = min(avg_ext / max_extent, 1)
    dpi = min_dpi + (max_dpi - min_dpi) * normalized_ext
    dpi_rounded = round(dpi / 100) * 100

    reference_extent = 40000
    base_linewidth = 0.08
    linewidth_border = base_linewidth * (reference_extent / avg_ext)
    linewidth_polygon = linewidth_border * 2
    linewidth_tile = linewidth_border * 2.5

    linewidth_border = max(round(linewidth_border / 0.005) * 0.005, 0.015)
    linewidth_polygon = max(round(linewidth_polygon / 0.005) * 0.005, 0.01)
    linewidth_tile = max(round(linewidth_tile / 0.005) * 0.005, 0.01)
    
    return dpi_rounded, linewidth_border, linewidth_polygon, linewidth_tile

def create_json_from_csv(aoi_path, config, init):
    """
    Create tile data JSON from CSV input.

    Parameters:
    - aoi_path (str): Path to the CSV file.
    - config (dict): Configuration dictionary containing tile information.
    - init (dict): Initialization dictionary containing paths and settings.

    Returns:
    - dict: JSON data created from the CSV input.
    """
    print("Loading CSV file", end="")
    csv = pd.read_csv(aoi_path)
    csv_name = os.path.basename(aoi_path)

    if len(init["selected_states"]) != 1:
        raise ValueError(f"'selected_states' must be a single state to be read from a CSV file.")
    
    state_name = init["selected_states"][0]
    data_type = init["data_type"]
    print(f"\rCSV file: {csv_name}\n{'-' * 42}")

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

def convert_and_save_geojson(meta_path, input_json):
    # Define the source and destination CRS
    crs_src = CRS.from_epsg(25832)
    crs_dst = CRS.from_epsg(25832)
    transformer = Transformer.from_crs(crs_src, crs_dst, always_xy=True)

    features = []

    for region, region_data in input_json["tiles"].items():
        for tile in region_data["tile_list"]:
            # Transform coordinates if needed (in this case it's the same CRS)
            coords = [tuple(transformer.transform(x, y)) for x, y in tile["tile_coords"]]
            coords.append(coords[0])  # Close the polygon

            polygon = GeoPolygon([coords])
            feature = Feature(
                geometry=polygon,
                properties={
                    "tile_name": tile["tile_name"],
                    "state": region,
                    "timestamp": tile["timestamp"],
                    "location": tile["location"],
                    "format": tile["format"]
                }
            )
            features.append(feature)
    
    # Create the final GeoJSON
    geojson = {
        "type": "FeatureCollection",
        "name": input_json["aoi_name"],
        "crs": {
            "type": "name",
            "properties": {
                "name": "urn:ogc:def:crs:EPSG::25832"
            }
        },
        "features": features
    }

    # Modify the meta_path to have .geojson extension
    geojson_path = os.path.splitext(meta_path)[0] + ".geojson"

    # Save the GeoJSON to the specified path
    save_json(geojson_path, geojson)


if __name__ == "__main__":
    config = load_json('config.json')
    init = load_json('init.json')
    create_state_tile_file(init, config, show=False)
