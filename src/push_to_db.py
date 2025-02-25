#!/usr/bin/env python
"""
Load cycling route data into PostgreSQL/PostGIS using SQLAlchemy.
This script takes GeoJSON files and efficiently inserts them into a PostgreSQL database
using SQLAlchemy models for better type safety and code organization.
"""

import os
import argparse
import glob
from dotenv import load_dotenv
from utils.db_models import Base, CyclingRoute
from utils.db_helpers import load_processed_geojson_to_db

load_dotenv()

def main():
    """Main function to handle command line arguments and load data."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Load processed cycling route data into PostgreSQL with SQLAlchemy')
    parser.add_argument('--input-file', type=str, help='Processed GeoJSON file to load')
    parser.add_argument('--input-dir', type=str, 
                       default=os.path.join(os.path.dirname(os.path.abspath(__file__)), '../generated_data/'),
                       help='Directory containing processed GeoJSON files')
    parser.add_argument('--pattern', type=str, default='*.geojson', 
                      help='File pattern to match when using --input-dir (default: *.geojson)')
    parser.add_argument('--batch-size', type=int, default=64000,
                      help='Batch size for inserting records')
    parser.add_argument('--drop-existing', action='store_true',
                      help='Drop existing tables before creating new ones')
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.input_file and not args.input_dir:
        print("Error: Either --input-file or --input-dir is required")
        return
    
    if args.input_file and args.input_dir:
        print("Warning: Both --input-file and --input-dir provided. Using --input-file only.")
    
    # Initialize counters
    total_files = 0
    total_records = 0
    
    # Process a single file
    if args.input_file:
        if not os.path.exists(args.input_file):
            print(f"Error: Input file not found: {args.input_file}")
            return
        
        print(f"Processing file: {args.input_file}")
        
        # Load the data to the database
        try:
            records_inserted = load_processed_geojson_to_db(
                geojson_file_path=args.input_file,
                model_class=CyclingRoute,
                batch_size=args.batch_size,
                drop_existing=args.drop_existing
            )
            
            print(f"Successfully inserted {records_inserted} records into the database")
            total_files += 1
            total_records += records_inserted
            
        except Exception as e:
            print(f"Error: {e}")
    
    # Process multiple files in a directory
    elif args.input_dir:
        if not os.path.isdir(args.input_dir):
            print(f"Error: Input directory not found: {args.input_dir}")
            return
        
        # Find matching files
        file_pattern = os.path.join(args.input_dir, args.pattern)
        files = glob.glob(file_pattern)
        
        if not files:
            print(f"No files matching pattern '{args.pattern}' found in {args.input_dir}")
            return
        
        print(f"Found {len(files)} files to process")
        
        # Process each file
        for i, file_path in enumerate(files):
            print(f"Processing file {i+1}/{len(files)}: {os.path.basename(file_path)}")
            
            try:
                # Only drop tables for the first file
                drop_this_file = args.drop_existing and i == 0
                
                records_inserted = load_processed_geojson_to_db(
                    geojson_file_path=file_path,
                    model_class=CyclingRoute,
                    batch_size=args.batch_size,
                    drop_existing=drop_this_file
                )
                
                print(f"Successfully inserted {records_inserted} records from {os.path.basename(file_path)}")
                total_files += 1
                total_records += records_inserted
                
            except Exception as e:
                print(f"Error processing {file_path}: {e}")
    
    # Print summary
    print("\nSummary:")
    print(f"Processed {total_files} files")
    print(f"Inserted {total_records} total records")

if __name__ == "__main__":
    main()