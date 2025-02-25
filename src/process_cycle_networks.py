import os
import argparse
import json
import h3
from tqdm import tqdm
from osgeo import osr
import geopandas as gpd
from shapely.geometry import Point, LineString, MultiLineString
from shapely import wkt
import pandas as pd
import math
from pathlib import Path
from datetime import datetime

from utils.geotiles import get_os_grid_reference

# NOTE: this is a good generalisable function that could be used in other projects.
def import_json_data(file_path):
    """
    Import GeoJSON data from a file path.
    
    Args:
        file_path: Path to the GeoJSON file
        
    Returns:
        GeoDataFrame with the imported data, or None if import fails
    """
    try:
        print(f"Importing file: {file_path}")
        
        # Read GeoJSON file
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        # Check if it's a FeatureCollection or a single Feature
        if 'type' in data and data['type'] == 'FeatureCollection':
            features = data['features']
        elif 'type' in data and data['type'] == 'Feature':
            features = [data]
        else:
            features = []
            if isinstance(data, list):
                features = data  # Assume it's a list of features
        
        if not features:
            print(f"No features found in {file_path}")
            return None
        
        # Convert to GeoDataFrame (this preserves the geometry)
        gdf = gpd.GeoDataFrame.from_features(features, crs="EPSG:27700")
        return gdf
        
    except Exception as e:
        print(f"Error importing {file_path}: {e}")
        return None
    

def process_route_features(gdf, file_path):
    """
    Process route features from a GeoDataFrame, calculating basic properties.
    
    Args:
        gdf: GeoDataFrame with route features
        file_path: Original file path (for metadata)
        
    Returns:
        GeoDataFrame with processed features, or None if processing fails
    """
    try:
        # Create coordinate system transformations for WGS84 conversion
        src_crs = osr.SpatialReference()
        src_crs.ImportFromEPSG(27700)  # British National Grid
        
        wgs84_crs = osr.SpatialReference()
        wgs84_crs.ImportFromEPSG(4326)  # WGS84
        
        transform = osr.CoordinateTransformation(src_crs, wgs84_crs)
        
        # Process each feature
        results = []
        
        for idx, row in tqdm(gdf.iterrows(), total=len(gdf), desc="Processing routes"):
            # Extract properties
            properties = {k: v for k, v in row.items() if k != 'geometry'}
            
            try:
                # Calculate route length in meters (in British National Grid projection)
                route_length_m = row.geometry.length
                
                # Create a result dictionary with original properties and calculated data
                result = {
                    **properties,  # Include all original properties
                    'geometry': row.geometry,
                    'route_length_m': route_length_m,
                    'source_file': os.path.basename(file_path)
                }
                
                results.append(result)
                
            except Exception as e:
                print(f"Error processing feature {idx}: {e}")
                continue
        
        if not results:
            print(f"No valid results extracted from data")
            return None
        
        # Convert results to GeoDataFrame
        result_gdf = gpd.GeoDataFrame(results, crs="EPSG:27700")
        
        # Convert to WGS84 for standardization
        result_gdf = result_gdf.to_crs("EPSG:4326")
        return result_gdf
        
    except Exception as e:
        print(f"Error processing features: {e}")
        return None


def process_route_json_file(file_path, output_dir, output_format="geojson"):
    """
    Process a GeoJSON file containing cycling routes and save the output.
    This is a wrapper function that uses import_json_data and process_route_features.

    Args:
        file_path: Path to the GeoJSON file
        output_dir: Directory to save processed data
        output_format: Format to save data ('geojson', 'csv', or 'postgres')

    Returns:
        Path to output file or GeoDataFrame if postgres format
    """
    try:
        # Import the data
        gdf = import_json_data(file_path)
        if gdf is None:
            return None
            
        # Process the features
        result_gdf = process_route_features(gdf, file_path)
        if result_gdf is None:
            return None
            
        # Create output filename
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save in the specified format
        if output_format == "geojson":
            output_file = os.path.join(output_dir, f"{base_name}_{timestamp}.geojson")
            result_gdf.to_file(output_file, driver="GeoJSON")
            print(f"Saved processed data to {output_file}")
            return output_file
            
        elif output_format == "csv":
            output_file = os.path.join(output_dir, f"{base_name}_{timestamp}.csv")
            # Add WKT for geometry
            result_gdf["geometry_wkt"] = result_gdf["geometry"].apply(lambda geom: geom.wkt)
            # Save to CSV
            result_gdf.drop(columns=["geometry"]).to_csv(output_file, index=False)
            print(f"Saved processed data to {output_file}")
            return output_file
            
        elif output_format == "postgres":
            # Return the GeoDataFrame for later processing in bulk
            return result_gdf
            
    except Exception as e:
        print(f"Error in processing pipeline: {e}")
        return None
    

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Process cycling route GeoJSON files")
    parser.add_argument(
        "--input-file",
        type=str,
        default=os.path.join(
            os.path.expanduser("~"), "data/active_travel/Cycling_Network_Scotland_Full.json"
        ),
        help="JSON file containing cycling routes",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "../generated_data"
        ),
        help="Directory to save processed data",
    )
    parser.add_argument(
        "--format",
        type=str,
        choices=["geojson", "csv"],
        default="geojson",
        help="Output format",
    )

    args = parser.parse_args()

    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Process GeoJSON file
    result = process_route_json_file(args.input_file, args.output_dir, args.format)
    
    if result is not None:
        print("Processing complete!")
    else:
        print("Processing failed.")

if __name__ == "__main__":
    main()