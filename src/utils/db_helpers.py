"""
Database utility functions for working with geospatial data.
"""

import os
from sqlalchemy import create_engine, MetaData, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import CreateSchema
from sqlalchemy_utils import database_exists, create_database
from geoalchemy2 import Geometry
import geopandas as gpd
from shapely import wkt
from tqdm import tqdm
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_SCHEMA = os.getenv("DB_SCHEMA")

def get_db_url():
    """Build the database URL from environment variables."""
    # Print out environment variables for debugging (hiding password)
    print(f"Database connection parameters:")
    print(f"  Host: {DB_HOST}")
    print(f"  Port: {DB_PORT}")
    print(f"  Database: {DB_NAME}")
    print(f"  User: {DB_USER}")
    print(f"  Password: {'*' * (len(DB_PASSWORD) if DB_PASSWORD else 0)}")
    print(f"  Schema: {DB_SCHEMA}")
    
    if not DB_HOST:
        print("Warning: DB_HOST is not set or empty")
    if not DB_NAME:
        print("Warning: DB_NAME is not set or empty")
    if not DB_USER:
        print("Warning: DB_USER is not set or empty")
    
    # NOTE: this is for when the default database is used, because there is no password
    if DB_PASSWORD:
        return f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    else:
        # For empty password, don't include the colon
        return f"postgresql://{DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def get_engine(echo=False):
    """Create a SQLAlchemy engine."""
    db_url = get_db_url()
    return create_engine(db_url, echo=echo)

def get_session():
    """Create a SQLAlchemy session."""
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()

def ensure_database_exists():
    """Ensure the database exists, create it if it doesn't."""
    db_url = get_db_url()
    if not database_exists(db_url):
        create_database(db_url)
        print(f"Created database: {DB_NAME}")

def ensure_schema_exists(engine=None):
    """Ensure the schema exists, create it if it doesn't."""
    if engine is None:
        engine = get_engine()
    
    # Check if schema exists
    with engine.connect() as conn:
        schema_exists = conn.execute(
            f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{DB_SCHEMA}'"
        ).fetchone() is not None
        
        if not schema_exists and DB_SCHEMA != 'public':
            conn.execute(CreateSchema(DB_SCHEMA))
            print(f"Created schema: {DB_SCHEMA}")

def ensure_postgis_extension(engine=None):
    """Ensure the PostGIS extension is installed."""
    if engine is None:
        engine = get_engine()
    
    with engine.connect() as conn:
        conn.execute("CREATE EXTENSION IF NOT EXISTS postgis")
        print("Ensured PostGIS extension is installed")

def create_tables(metadata_obj, engine=None, drop_first=False):
    """
    Create all tables defined in the given metadata.
    
    Args:
        metadata_obj: SQLAlchemy MetaData object with table definitions
        engine: Optional SQLAlchemy engine (created if not provided)
        drop_first: If True, drop existing tables before creating
    """
    if engine is None:
        engine = get_engine()
    
    # Drop tables if requested
    if drop_first:
        metadata_obj.drop_all(engine)
        print("Dropped existing tables")

    # TODO: fix these for robust validation
    # ensure_database_exists()
    # ensure_schema_exists(engine)
    # ensure_postgis_extension(engine)
    
    # Create tables
    metadata_obj.create_all(engine)
    print("Created tables")
    
    # Print created tables
    inspector = inspect(engine)
    tables = inspector.get_table_names(schema=DB_SCHEMA)
    print(f"Tables in schema {DB_SCHEMA}: {', '.join(tables)}")

def gdf_to_sql_model(gdf, model_class, session=None, batch_size=64000):
    """
    Convert a GeoDataFrame to SQLAlchemy model instances and save to database.
    
    Args:
        gdf (GeoDataFrame): The GeoDataFrame to convert
        model_class: The SQLAlchemy model class to use
        session: Optional SQLAlchemy session (created if not provided)
        batch_size: Number of records to insert in each batch
        
    Returns:
        int: Number of records inserted
    """
    if session is None:
        session = get_session()
    
    try:
        # Process data in batches
        total_records = len(gdf)
        records_inserted = 0
        
        with tqdm(total=total_records, desc="Loading data") as pbar:
            # Process in batches to avoid memory issues
            for i in range(0, total_records, batch_size):
                batch_gdf = gdf.iloc[i:i+batch_size]
                batch_models = []
                
                for _, row in batch_gdf.iterrows():
                    # Create model instance
                    model = model_class()
                    
                    # Set attributes from GeoDataFrame
                    for key, value in row.items():
                        # Skip geometry, we'll handle that separately
                        if key == 'geometry':
                            continue
                            
                        # Match attribute names from source to model
                        if key == 'type' and hasattr(model, 'route_type'):
                            setattr(model, 'route_type', value)
                            continue
                            
                        # Skip unknown attributes
                        if hasattr(model, key):
                            setattr(model, key, value)
                    
                    # Handle geometry separately to ensure correct format
                    if hasattr(model, 'geometry') and row.geometry is not None:
                        # Convert geometry to WKT
                        wkt_geom = wkt.dumps(row.geometry)
                        # Use SQLAlchemy-compatible format
                        model.geometry = f'SRID=4326;{wkt_geom}'
                    
                    batch_models.append(model)
                
                # Add batch to session
                session.add_all(batch_models)
                session.commit()
                
                # Update progress
                records_inserted += len(batch_models)
                pbar.update(len(batch_models))
        
        return records_inserted
    
    except Exception as e:
        session.rollback()
        raise e

def load_processed_geojson_to_db(
    geojson_file_path, 
    model_class, 
    batch_size=64000,
    drop_existing=False
):
    """
    Load processed GeoJSON file directly to the database.
    
    Args:
        geojson_file_path: Path to the processed GeoJSON file
        model_class: SQLAlchemy model class to use
        batch_size: Number of records to insert in each batch
        drop_existing: If True, drop existing tables before creating
        
    Returns:
        int: Number of records inserted
    """
    try:
        # Load the processed GeoJSON file
        gdf = gpd.read_file(geojson_file_path)
        if gdf.empty:
            print(f"No data found in {geojson_file_path}")
            return 0
        
        # Ensure the CRS is correct (should be WGS84 from the processing script)
        if gdf.crs != "EPSG:4326":
            gdf = gdf.to_crs("EPSG:4326")
            print("Converted GeoDataFrame to WGS84 (EPSG:4326)")

        engine = get_engine()
        
        # Create tables if needed
        create_tables(model_class.__table__.metadata, engine, drop_first=drop_existing)    

        # Create a session
        Session = sessionmaker(bind=engine)
        session = Session()
        
        try:
            # Insert data
            return gdf_to_sql_model(gdf, model_class, session, batch_size)
        
        except Exception as e:
            session.rollback()
            print(f"Error loading data: {e}")
            return 0
        
        finally:
            session.close()
            
    except Exception as e:
        print(f"Error reading GeoJSON file: {e}")
        return 0