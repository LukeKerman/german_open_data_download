import sys
import warnings

import numpy as np
import geojson
import utm
from pyproj import Proj, Transformer
from scipy.spatial import cKDTree
from shapely.geometry import LineString, MultiPolygon, Polygon, mapping, shape
from shapely.ops import nearest_points, transform, unary_union

warnings.filterwarnings("ignore", category=RuntimeWarning, message="invalid value encountered in shortest_line")

def load_geojson(file_path):
    """
    Loads GeoJSON data from a file.

    Args:
        file_path (str): The path to the GeoJSON file.

    Returns:
        dict: The loaded GeoJSON data.
    """
    with open(file_path) as f:
        data = geojson.load(f)
    return data

def identify_crs(geojson_data):
    """
    Identifies the coordinate reference system (CRS) of the GeoJSON data.

    Args:
        geojson_data (dict): The GeoJSON data.

    Returns:
        str: The CRS of the GeoJSON data.
    """
    if 'crs' in geojson_data:
        crs = geojson_data['crs']['properties']['name']
    else:
        crs = "EPSG:4326"  # Default to WGS84
    return crs

def transform_to_utm(polygon, crs):
    """
    Transforms a polygon to UTM coordinates.

    Args:
        polygon (Polygon): The polygon to transform.
        crs (str): The CRS of the polygon.

    Returns:
        tuple: The transformed polygon and its UTM CRS.
    """
    centroid_lon, centroid_lat = polygon.centroid.x, polygon.centroid.y
    utm_zone_number, utm_zone_letter = utm.from_latlon(centroid_lat, centroid_lon)[2:4]
    utm_crs = Proj(proj="utm", zone=utm_zone_number, ellps="WGS84", south=utm_zone_letter < 'N')
    transformer = Transformer.from_proj(Proj(crs), utm_crs, always_xy=True)
    return transform(transformer.transform, polygon), utm_crs

def transform_to_original_crs(polygon, original_crs, utm_crs):
    """
    Transforms a polygon back to its original CRS.

    Args:
        polygon (Polygon): The polygon to transform.
        original_crs (str): The original CRS.
        utm_crs (Proj): The UTM CRS.

    Returns:
        Polygon: The transformed polygon.
    """
    transformer = Transformer.from_proj(utm_crs, Proj(original_crs), always_xy=True)
    return transform(transformer.transform, polygon)

def extract_polygons(geojson_data, crs):
    """
    Extracts polygons from GeoJSON data and transforms them to UTM coordinates.

    Args:
        geojson_data (dict): The GeoJSON data.
        crs (str): The CRS of the data.

    Returns:
        tuple: A list of transformed polygons and their UTM CRS.
    """
    polygons = []
    utm_crs = None
    for feature in geojson_data['features']:
        geom = shape(feature['geometry'])
        if isinstance(geom, Polygon):
            transformed_geom, utm_crs = transform_to_utm(geom, crs)
            polygons.append(transformed_geom)
        elif isinstance(geom, MultiPolygon):
            for poly in geom.geoms:
                transformed_geom, utm_crs = transform_to_utm(poly, crs)
                polygons.append(transformed_geom)
    return polygons, utm_crs

def calculate_areas(polygons):
    """
    Calculates the areas of polygons.

    Args:
        polygons (list): A list of polygons.

    Returns:
        list: A list of areas corresponding to the polygons.
    """
    areas = [polygon.area for polygon in polygons]
    return areas

def split_polygons_by_area(polygons, min_area):
    """
    Splits polygons into two lists based on a minimum area threshold.

    Args:
        polygons (list): A list of polygons.
        min_area (float): The minimum area threshold.

    Returns:
        tuple: Two lists of polygons - larger and smaller than the threshold.
    """
    larger_polygons = []
    smaller_polygons = []
    for polygon in polygons:
        if polygon.area >= min_area:
            larger_polygons.append(polygon)
        else:
            smaller_polygons.append(polygon)
    return larger_polygons, smaller_polygons

def find_nearest_neighbor(polygon, polygons, centroids, k=3):
    """
    Finds the nearest neighbor of a polygon from a list of polygons.

    Args:
        polygon (Polygon): The polygon to find the nearest neighbor for.
        polygons (list): A list of polygons to search.
        centroids (ndarray): The centroids of the polygons.
        k (int): The number of nearest neighbors to consider.

    Returns:
        tuple: The nearest polygon and the line connecting them.
    """
    polygon_centroid = np.array(polygon.centroid.coords[0])
    kdtree = cKDTree(centroids)
    _, idxs = kdtree.query(polygon_centroid, k=k)
    
    min_dist = float('inf')
    nearest_polygon = None
    nearest_line = None
    
    for idx in idxs:
        candidate_polygon = polygons[idx]
        
        point1, point2 = nearest_points(polygon.exterior, candidate_polygon.exterior)
        distance = point1.distance(point2)
        if distance < min_dist:
            min_dist = distance
            nearest_polygon = candidate_polygon
            nearest_line = LineString([point1, point2])
    
    return nearest_polygon, nearest_line

def merge_and_buffer(polygons, min_area, buffer_size):
    """
    Merges and buffers polygons based on a minimum area threshold.

    Args:
        polygons (list): A list of polygons.
        min_area (float): The minimum area threshold.
        buffer_size (float): The buffer size for merging.

    Returns:
        list: The merged and buffered polygons.
    """
    _, smaller_polygons = split_polygons_by_area(polygons, min_area)
    print(f"     Polygons Below Threshold: {len(smaller_polygons)}")

    total_polygons = len(smaller_polygons)
    total_buffer_area = 0

    for i, small_poly in enumerate(smaller_polygons):

        filtered_polygons = [polygon for polygon in polygons if polygon != small_poly]
        centroids = np.array([np.array(poly.centroid.coords[0]) for poly in filtered_polygons])

        nearest_poly, nearest_line = find_nearest_neighbor(small_poly, filtered_polygons, centroids)
        if nearest_poly is None:
            polygons.append(small_poly)
            continue

        buffered_line = nearest_line.buffer(buffer_size)
        merged_polygon = unary_union([small_poly, buffered_line, nearest_poly])
        
        if nearest_line.length > 0.1: # issues with area calc if line is very short
            buffer_area = merged_polygon.area - small_poly.area - nearest_poly.area
        else:
            buffer_area = buffered_line.area
        total_buffer_area += buffer_area

        polygons[polygons.index(nearest_poly)] = merged_polygon
        if small_poly in polygons:
            polygons.remove(small_poly)

        # Progress update
        progress = (i + 1) / total_polygons * 100
        print(f"\r     Progress: {progress:.1f}%", end="")

    print()  # New line after progress completion
    return polygons, total_buffer_area

def process_geojson(file_path, min_area, buffer_size):
    """
    Processes a GeoJSON file to merge and buffer polygons based on a minimum area threshold.

    Args:
        file_path (str): The path to the GeoJSON file.
        min_area (float): The minimum area threshold.
        buffer_size (float): The buffer size for merging.

    Returns:
        list: The processed polygons.
    """
    geojson_data = load_geojson(file_path)
    original_crs = identify_crs(geojson_data)
    polygons, utm_crs = extract_polygons(geojson_data, original_crs)
    areas = calculate_areas(polygons)
    total_area = sum(areas)
    
    print(f"Initial Number of Polygons: {len(polygons)}\n  Iteration Summary:")

    buffer = 0
    iteration = 0
    while True:
        
        if all(area >= min_area for area in areas):
            break
        print(f"   Iteration: {iteration}")
        polygons, buffer_i = merge_and_buffer(polygons, min_area, buffer_size)

        buffer += buffer_i
        iteration += 1

        areas = calculate_areas(polygons)

    print(f"Final Number of Polygons: {len(polygons)}")
    print(f"Buffer Ratio: {buffer/total_area*100:.2f}%")
    print(f"Buffer Area: {buffer:.1f} sqm")

    # Transform polygons back to original CRS
    polygons = [transform_to_original_crs(poly, original_crs, utm_crs) for poly in polygons]

    return polygons

def save_geojson(polygons, output_file, crs):
    """
    Saves polygons to a GeoJSON file.

    Args:
        polygons (list): A list of polygons.
        output_file (str): The path to the output file.
        crs (str): The CRS of the data.

    Returns:
        None
    """
    features = [geojson.Feature(geometry=mapping(poly)) for poly in polygons]
    feature_collection = geojson.FeatureCollection(features, crs={"type": "name", "properties": {"name": crs}})
    with open(output_file, 'w') as f:
        geojson.dump(feature_collection, f)


def main():
    '''Organizing input parameter'''
    # Default internal parameters
    params = {
        "file_path": 'bdl/test/train_aoi_test.geojson',
        "output_file_path": 'bdl/test/train_aoi_test_connected.geojson',
        "min_area": 250000,
        "buffer_size": 1
    }

    # Update parameters based on command line arguments
    arg_names = ["file_path", "output_file_path", "min_area", "buffer_size"]
    for i, arg in enumerate(sys.argv[1:5], start=1):
        params[arg_names[i-1]] = int(arg) if i > 2 else arg

    if len(sys.argv) > 5:
        print("Usage: python script_name.py <file_path> <output_file_path> [min_area] [buffer_size]")
        return

    polygons = process_geojson(params["file_path"], params["min_area"], params["buffer_size"])
    save_geojson(polygons, params["output_file_path"], identify_crs(load_geojson(params["file_path"])))

if __name__ == "__main__":
    main()


