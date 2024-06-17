"""This script processes GeoJSON data to create tiles within specified polygons and plots the results."""

import json
import os
import time

import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.collections import PatchCollection
from matplotlib.patches import Polygon as MplPolygon
import geopandas as gpd
from pyproj import Transformer
from shapely.geometry import box, Polygon, MultiPolygon, shape
from shapely.ops import transform

def load_json(file_path):
    """
    Load a JSON file from the specified file path.

    Parameters:
    - file_path: Path to the JSON file.

    Returns:
    - The loaded JSON data.
    """
    with open(file_path, 'r') as f:
        return json.load(f)
    
def save_json(file_path, file):
    """
    Save data to a JSON file at the specified file path.

    Parameters:
    - file_path: Path to the JSON file.
    - file: Data to be saved.
    """
    with open(file_path, 'w') as f:
        json.dump(file, f, indent=4)

def get_multipolygon_from_geojson(file_path):
    """
    Extract a MultiPolygon from a GeoJSON file.

    Parameters:
    - file_path: Path to the GeoJSON file.

    Returns:
    - A MultiPolygon containing all polygons and multipolygons from the GeoJSON file.
    """
    print("Loading GeoJSON files", end="")
    gdf = gpd.read_file(file_path)

    if gdf.crs != 'EPSG:25832':
        gdf = gdf.to_crs('EPSG:25832')
    
    # Extract all the polygons and multipolygons
    polygons = []
    for geom in gdf.geometry:
        if geom.geom_type == 'Polygon':
            polygons.append(geom)
        elif geom.geom_type == 'MultiPolygon':
            polygons.extend(geom.geoms)
    
    # Create a MultiPolygon from the list of polygons
    multi_polygon = MultiPolygon(polygons)
    print(f"\rLoading GeoJSON files complete.\n{'-' * 28}")
    
    return multi_polygon

def create_tiles_within_polygon(polygon, config, data_type, state_name, crs="UTM32"):
    """
    Create a grid of tiles and return those that are partially or fully within the given polygon.

    Parameters:
    - polygon: The polygon geometry.
    - config: Configuration dictionary containing tile information.
    - data_type: Data type for the tiles.
    - state_name: Name of the state for which tiles are being created.
    - crs: Coordinate reference system to be used (default is "UTM32").

    Returns:
    - A list of tile names and their geometries that are partially or fully within the polygon.
    """
    if config[data_type][state_name]:
        tile_size = config[data_type][state_name]['tile_info']['tile_size']
        start_x = config[data_type][state_name]['tile_info']['x']
        start_y = config[data_type][state_name]['tile_info']['y']
    else:
        return []


    if crs == "UTM32": utm = 32
    elif crs == "UTM33": utm = 33
    else: 
        raise ValueError("Error in CRS definition. UTM needs to be UTM32 or UTM33")

    min_x, min_y, max_x, max_y = polygon.bounds
    
    # Create a list to store tile names and geometries
    tiles_in_polygon = []

    # Generate the tile grid
    x_coords = np.arange(np.floor(min_x / tile_size) * tile_size - start_x, np.ceil(max_x / tile_size) * tile_size + start_x, tile_size)
    y_coords = np.arange(np.floor(min_y / tile_size) * tile_size - start_y, np.ceil(max_y / tile_size) * tile_size + start_y, tile_size)
    
    for x in x_coords:
        for y in y_coords:
            tile_bbox = box(x, y, x + tile_size, y + tile_size)
            if tile_bbox.intersects(polygon):
                tile_name = f"{utm}_{int(x // 1000):03}_{int(y // 1000):04}"
                tile_coords = list(tile_bbox.exterior.coords)[:-1]  # Get the corners
                tiles_in_polygon.append((tile_name, tile_coords))
    
    return tiles_in_polygon

def print_progress(state_name, current, total):
    """
    Print the progress of tile creation for a state.

    Parameters:
    - state_name: Name of the state.
    - current: Current progress value.
    - total: Total number of tiles.
    """
    progress = (current / total) * 100
    if progress == 100:
        print(f"\rProgress of state {state_name}:\t100.0% (Completed)")
    else:
        print(f"\rProgress of state {state_name}:\t{progress:>4.1f}%", end="", flush=True)

def create_state_tile_file(show=False):
    """
    Main function to create state tile files and plot the results.

    Parameters:
    - show: Boolean to indicate whether to display the plot.
    """
    init = load_json('init.json')
    
    geojson_path = init['geojson_path']
    data_type = init['data_type']
    meta_path = init['meta_path']
    selected_states = init.get('selected_states', None)

    print(f"TILE BY STATE CREATOR for {data_type}")

    if selected_states is None:
        selected_states = []

    # Define paths to state files
    state_files_dir = 'bdl'
    state_geojson_path_utm32 = os.path.join(state_files_dir, 'DE_bdl_utm32.geojson')
    state_geojson_path_utm33 = os.path.join(state_files_dir, 'DE_bdl_utm33.geojson')

    # Load the configuration file
    config = load_json('config.json')

    # Load the polygon from geojson file
    multi_polygon = get_multipolygon_from_geojson(geojson_path)
    
    state_geo_utm32 = gpd.read_file(state_geojson_path_utm32)
    state_geo_utm33 = gpd.read_file(state_geojson_path_utm33)
    
    # Prepare a dictionary to hold the tiles for each state
    state_tiles = {}

    # Define transformers
    transform_to_utm33 = Transformer.from_crs("EPSG:25832", "EPSG:25833", always_xy=True).transform
    transform_to_utm32 = Transformer.from_crs("EPSG:25833", "EPSG:25832", always_xy=True).transform

    time_delay = 0.001

    # Process EPSG:25832 states
    for _, state_row in state_geo_utm32.iterrows():
        state_name = state_row['GEN']
        if selected_states and state_name not in selected_states:
            continue
        state_tiles[state_name] = {
            "data_type": data_type,
            "tile_list": []
            }
        
        # Get the part of the multi_polygon that intersects with the current state
        intersecting_polygon = multi_polygon.intersection(state_row['geometry'])

        if not intersecting_polygon.is_empty:
            # Load the tiles covering the polygon area
            tiles = create_tiles_within_polygon(intersecting_polygon, config, data_type, state_name, 'UTM32')
            total_tiles = len(tiles)
            
            if tiles:
                for i, (tile_name, tile_coords) in enumerate(tiles):
                    tile_poly = Polygon(tile_coords) 
                    if state_row['geometry'].intersects(tile_poly):
                        tile_coords_formatted = [(x, y) for x, y in tile_coords]
                        state_tiles[state_name]["tile_list"].append({
                            "tile_name": tile_name,
                            "timestamp": None,
                            "location": None,
                            "format": None,
                            "tile_coords": tile_coords_formatted
                        })
                    print_progress(state_name, i + 1, total_tiles)
                    time.sleep(time_delay)
            else:
                print(f"Download script for {state_name} not found")

    # Process EPSG:25833 states
    for _, state_row in state_geo_utm33.iterrows():
        state_name = state_row['GEN']
        if selected_states and state_name not in selected_states:
            continue
        state_tiles[state_name] = {
            "data_type": data_type,
            "tile_list": []
            }
        
        # Transform the AOI polygon to EPSG:25833
        multi_polygon_utm33 = transform(transform_to_utm33, multi_polygon)

        # Get the part of the multi_polygon that intersects with the current state
        intersecting_polygon = multi_polygon_utm33.intersection(state_row['geometry'])

        if not intersecting_polygon.is_empty:
            # Load the tiles covering the polygon area
            tiles = create_tiles_within_polygon(intersecting_polygon, config, data_type, state_name, 'UTM33')
            total_tiles = len(tiles)

            if tiles:
                for i, (tile_name, tile_coords) in enumerate(tiles):
                    tile_poly = Polygon(tile_coords)
                    if state_row['geometry'].intersects(tile_poly):
                        # Transform tile bounding box back to EPSG:25832
                        tile_coords_utm32 = [transform_to_utm32(x, y) for x, y in tile_coords]
                        tile_coords_formatted = [(x, y) for x, y in tile_coords_utm32]
                        state_tiles[state_name]["tile_list"].append({
                            "tile_name": tile_name,
                            "timestamp": None,
                            "location": None,
                            "format": None,
                            "tile_coords": tile_coords_formatted
                        })
                    print_progress(state_name, i + 1, total_tiles)
                    time.sleep(time_delay)
            else:
                print(f"Download script for {state_name} not found")

    # Save the state-tiles mapping to a json file
    save_json(meta_path, state_tiles)

    plot_polygons_and_tiles(multi_polygon, state_tiles, state_geo_utm32, state_geo_utm33, show=show)

    display_results(meta_path)


def plot_polygons_and_tiles(multi_polygon, state_tiles, state_geo, state_geo_of_utm33, show):
    """
    Plot the polygon, tiles, and state boundaries.

    Parameters:
    - multi_polygon: The multipolygon representing the area of interest.
    - state_tiles: A dictionary where each key is a state name and the value is a dictionary
                   containing 'data_type' and 'tile_list'.
    - state_geo: A GeoDataFrame containing the state boundaries.
    - state_geo_of_utm33: A GeoDataFrame containing the state boundaries in UTM33 projection.
    """

    print(f"{'-' * 28}\ngenerating overview plot...", end="", flush=True)

    minx, miny, maxx, maxy = multi_polygon.bounds
    x_ext = maxx - minx
    y_ext = maxy - miny

    dpi, lw_st, lw_p, lw_t = calculate_dpi_and_lw(x_ext, y_ext)

    # Create a Matplotlib figure and axis with a larger size
    fig, ax = plt.subplots(figsize=(20, 20))

    # Generate a color map with a distinct color for each state
    colors = [mpl.colormaps['gist_rainbow'](i / len(state_tiles)) for i in range(len(state_tiles))]

    # Create a list to hold the tile patches
    tile_patches = []

    # Add the tile patches first, assigning a unique color to each state
    for idx, (state_name, state_data) in enumerate(state_tiles.items()):
        color = colors[idx]
        for tile in state_data["tile_list"]:
            tile_coords = tile["tile_coords"]
            tile_patch = MplPolygon(tile_coords, closed=True, edgecolor=tuple(list(color[:3]) + [1]), facecolor=tuple(list(color[:3]) + [0.2]), linestyle='-', linewidth=lw_t)
            tile_patches.append(tile_patch)
    
    # Create a PatchCollection from the tile patches
    tile_patch_collection = PatchCollection(tile_patches, match_original=True)

    # Add the tile PatchCollection to the axis first
    ax.add_collection(tile_patch_collection)

    # Create a list to hold the polygon patches
    polygon_patches = []

    # Iterate through each polygon in the multipolygon
    for polygon in multi_polygon.geoms:
        # Get the exterior coordinates of the polygon
        exterior_coords = list(polygon.exterior.coords)
        # Create a Polygon patch for the exterior
        poly_patch = MplPolygon(exterior_coords, closed=True, edgecolor='black', facecolor='none', linewidth=lw_p)
        # Add the patch to the list
        polygon_patches.append(poly_patch)

        # Iterate through each interior (hole) in the polygon
        for interior in polygon.interiors:
            # Get the interior coordinates of the polygon
            interior_coords = list(interior.coords)
            # Create a Polygon patch for the interior
            interior_poly_patch = MplPolygon(interior_coords, closed=True, edgecolor='black', facecolor='none', linewidth=lw_p)
            # Add the patch to the list
            polygon_patches.append(interior_poly_patch)

    # Create a PatchCollection from the polygon patches
    polygon_patch_collection = PatchCollection(polygon_patches, match_original=True)

    # Add the polygon PatchCollection to the axis
    ax.add_collection(polygon_patch_collection)

    transform_to_utm32 = Transformer.from_crs("EPSG:25833", "EPSG:25832", always_xy=True).transform

    ec = 'k'
    fc = 'none'
    ls = '-'
    al = 1

    # Plot the state boundaries
    state_patches = []
    for _, state_row in state_geo.iterrows():
        state_polygon = state_row['geometry']
        if state_polygon.geom_type == 'Polygon':
            state_patches.append(MplPolygon(list(state_polygon.exterior.coords), closed=True, edgecolor=ec, facecolor=fc, linestyle=ls, linewidth=lw_st, alpha=al))
            for interior in state_polygon.interiors:
                state_patches.append(MplPolygon(list(interior.coords), closed=True, edgecolor=ec, facecolor=fc, linestyle=ls, linewidth=lw_st, alpha=al))
        elif state_polygon.geom_type == 'MultiPolygon':
            for sub_polygon in state_polygon.geoms:
                state_patches.append(MplPolygon(list(sub_polygon.exterior.coords), closed=True, edgecolor=ec, facecolor=fc, linestyle=ls, linewidth=lw_st, alpha=al))
                for interior in sub_polygon.interiors:
                    state_patches.append(MplPolygon(list(interior.coords), closed=True, edgecolor=ec, facecolor=fc, linestyle=ls, linewidth=lw_st, alpha=al))

    for _, state_row in state_geo_of_utm33.iterrows():
        state_polygon = state_row['geometry']
        state_polygon = transform(transform_to_utm32, state_polygon)
        if state_polygon.geom_type == 'Polygon':
            state_patches.append(MplPolygon(list(state_polygon.exterior.coords), closed=True, edgecolor=ec, facecolor=fc, linestyle=ls, linewidth=lw_st, alpha=al))
            for interior in state_polygon.interiors:
                state_patches.append(MplPolygon(list(interior.coords), closed=True, edgecolor=ec, facecolor=fc, linestyle=ls, linewidth=lw_st, alpha=al))
        elif state_polygon.geom_type == 'MultiPolygon':
            for sub_polygon in state_polygon.geoms:
                state_patches.append(MplPolygon(list(sub_polygon.exterior.coords), closed=True, edgecolor=ec, facecolor=fc, linestyle=ls, linewidth=lw_st, alpha=al))
                for interior in sub_polygon.interiors:
                    state_patches.append(MplPolygon(list(interior.coords), closed=True, edgecolor=ec, facecolor=fc, linestyle=ls, linewidth=lw_st, alpha=al))

    state_patch_collection = PatchCollection(state_patches, match_original=True)
    ax.add_collection(state_patch_collection)

    # Set the limits of the plot based on the multipolygon bounds
    edge_space_x = max(x_ext * 0.2, 2500)
    edge_space_y = max(y_ext * 0.2, 2500)
    ax.set_xlim(minx - edge_space_x, maxx + edge_space_x)
    ax.set_ylim(miny - edge_space_y, maxy + edge_space_y)

    # Set aspect ratio to equal to ensure the polygons are not distorted
    ax.set_aspect('equal')

    # Remove the axis labels
    ax.set_xticks([])
    ax.set_yticks([])

    plt.savefig('tile_overview.png', dpi=dpi, bbox_inches='tight')

    if show:
        plt.show()

    print("\rOverview plot generation complete.", flush=True)
    time.sleep(1)

def display_results(file_path):
    """
    Display the results of the tile creation process.

    Parameters:
    - file_path: Path to the JSON file containing the results.
    """
    data = load_json(file_path)
    
    if not data:
        print("\nNo tile data found in the file.\n")
        return

    total_tiles = 0
    state_tile_counts = []

    for state, state_data in data.items():
        tile_list = state_data["tile_list"]
        tile_count = len(tile_list)
        if tile_count:
            state_tile_counts.append((state, tile_count))
            total_tiles += tile_count

    if state_tile_counts:
        print(f"\n\nTile counts per state ({state_data['data_type']})")
        print("-" * 28)
        for state, count in state_tile_counts:
            print(f"{state}:  {count:>5} tiles")
        print("-" * 28)
        print(f"Total number of tiles: {total_tiles}\n")
    else:
        print("\nNo tiles found for any state.\n")

def calculate_dpi_and_lw(x_ext, y_ext):
    """
    Calculate DPI and line widths based on the extent of the area.

    Parameters:
    - x_ext: Extent in the x-direction.
    - y_ext: Extent in the y-direction.

    Returns:
    - dpi_rounded: Rounded DPI value.
    - linewidth_border: Line width for borders.
    - linewidth_polygon: Line width for polygons.
    - linewidth_tile: Line width for tiles.
    """
    # Calculate the average extent
    avg_ext = (x_ext + y_ext) / 2.0
    
    # Define the DPI calculation range
    min_dpi = 500
    max_dpi = 2000
    
    # Define a maximum extent for normalization (based on typical use cases)
    max_extent = 250000  # This can be adjusted based on the application context
    
    # Normalize the extent
    normalized_ext = min(avg_ext / max_extent, 1)
    
    # Use a linear relationship for DPI scaling
    dpi = min_dpi + (max_dpi - min_dpi) * normalized_ext
    
    # Round the DPI to the nearest 100
    dpi_rounded = round(dpi / 100) * 100
    
    # Calculate the line widths based on the normalized extent
    reference_extent = 40000
    base_linewidth = 0.08
    
    linewidth_border = base_linewidth * (reference_extent / avg_ext)
    linewidth_polygon = linewidth_border * (0.2 / 0.1) 
    linewidth_tile = linewidth_border * (0.25 / 0.1) 
    
    # Round the line widths to the nearest 0.01
    linewidth_border = max(round(linewidth_border / 0.005) * 0.005, 0.015)
    linewidth_polygon = max(round(linewidth_polygon / 0.005) * 0.005, 0.01)
    linewidth_tile = max(round(linewidth_tile / 0.005) * 0.005, 0.01)
    
    return dpi_rounded, linewidth_border, linewidth_polygon, linewidth_tile


if __name__ == "__main__":
    create_state_tile_file(show=False)