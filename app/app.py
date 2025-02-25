import streamlit as st
import pandas as pd
import geopandas as gpd
import folium
from folium.plugins import MarkerCluster
from streamlit_folium import folium_static
import sqlalchemy
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from shapely.geometry import MultiPoint, MultiLineString, MultiPolygon

# Page configuration
st.set_page_config(
    page_title="DroneBoyz - Cycling Routes Viewer",
    page_icon="🚲",
    layout="wide"
)

# Load environment variables for database connection
load_dotenv()

# Database connection parameters
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_SCHEMA = os.getenv("DB_SCHEMA")
ROUTES_TABLE_NAME = os.getenv("ROUTES_TABLE_NAME", "cycling_routes")

# Function to create database connection
@st.cache_resource
def get_db_connection():
    """Create and return a SQLAlchemy engine for database connection."""
    try:
        connection_string = f"postgresql://{DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        st.error(f"Database connection error: {e}")
        return None

# Function to load local authorities
# @st.cache_data(ttl=3600)
def get_local_authorities(engine):
    """Fetch unique local authorities from the database."""
    if engine is None:
        return []
    query = f"SELECT DISTINCT local_authority FROM {DB_SCHEMA}.{ROUTES_TABLE_NAME} WHERE local_authority IS NOT NULL ORDER BY local_authority"
    try:
        df = pd.read_sql(query, engine)
        return df['local_authority'].dropna().tolist()
    except Exception as e:
        st.error(f"Error fetching local authorities: {e}")
        return []

# Function to load cycling routes
# @st.cache_data(ttl=600)
def load_cycling_routes(engine, local_authority=None):
    """Load cycling routes from PostgreSQL into a GeoDataFrame."""
    if engine is None:
        return gpd.GeoDataFrame()
    
    query = f"""
    SELECT id, route_id, street, locality, route_type, notes, surface,
           local_authority, route_length_m, ST_AsText(geometry) as geometry_wkt
    FROM {DB_SCHEMA}.{ROUTES_TABLE_NAME}
    """
    
    if local_authority and local_authority != "All":
        query += f" WHERE local_authority = '{local_authority}'"
    
    query += " ORDER BY id LIMIT 1000"  # Adjust limit for performance

    try:
        df = pd.read_sql(query, engine)
        if df.empty:
            return gpd.GeoDataFrame(columns=df.columns, geometry='geometry', crs="EPSG:4326")
        
        df['geometry'] = gpd.GeoSeries.from_wkt(df['geometry_wkt'])
        df.drop(columns=['geometry_wkt'], inplace=True)
        return gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")
    
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return gpd.GeoDataFrame()

def create_map(gdf):
    """Create and return a Folium map with cycling routes."""
    default_location = [56.4907, -4.2026]  # Default to Scotland
    
    # Center map based on available data
    if not gdf.empty:
        bounds = gdf.total_bounds
        center_lat = (bounds[1] + bounds[3]) / 2
        center_lon = (bounds[0] + bounds[2]) / 2
    else:
        center_lat, center_lon = default_location

    # Initialize the folium map
    m = folium.Map(location=[center_lat, center_lon], zoom_start=6, tiles="OpenStreetMap")

    # Tile layers
    # folium.TileLayer('CartoDB positron', name='Light Map').add_to(m)
    # folium.TileLayer('CartoDB dark_matter', name='Dark Map').add_to(m)
    folium.LayerControl().add_to(m)

    # Define route colors
    route_colors = {
        'Cycle Lane': '#00aa00',      # Green
        'Cycle Path': '#0066ff',      # Blue
        'Mixed Use Path': '#aa00aa',  # Purple
        'Shared Use Path': '#ff9900', # Orange
        'default': '#3388ff'          # Default blue
    }

    # Add cycling routes to the map
    if not gdf.empty:
        for _, row in gdf.iterrows():
            route_type = row.get('route_type', None)
            color = route_colors.get(route_type, route_colors['default'])
            
            popup_html = f"""
            <b>Route ID:</b> {row.get('route_id', 'N/A')}<br>
            <b>Street:</b> {row.get('street', 'N/A')}<br>
            <b>Locality:</b> {row.get('locality', 'N/A')}<br>
            <b>Type:</b> {route_type}<br>
            <b>Surface:</b> {row.get('surface', 'N/A')}<br>
            <b>Length:</b> {round(row.get('route_length_m', 0), 2)} meters<br>
            """

            geometry = row.geometry
            locations = []  # To store coordinates

            # Handle multi-part geometries
            if isinstance(geometry, (MultiPoint, MultiLineString, MultiPolygon)):
                for geom in geometry.geoms:
                    locations.extend([(point[1], point[0]) for point in geom.coords])
            else:
                locations = [(point[1], point[0]) for point in geometry.coords]

            folium.PolyLine(
                locations=locations,
                color=color,
                weight=4,
                popup=folium.Popup(popup_html, max_width=300)
            ).add_to(m)

    return m

# ---- STREAMLIT UI ----

# Sidebar components
st.sidebar.header("Filter Options")
engine = get_db_connection()

# Dropdown for local authority selection
local_authorities = get_local_authorities(engine)
local_authority = st.sidebar.selectbox("Select Local Authority", ["All"] + local_authorities)

# Load data based on selection
gdf = load_cycling_routes(engine, local_authority)

# Tabs for viewing map and data
tab1, tab2 = st.tabs(["🗺 Map View", "📊 Data View"])

with tab1:
    st.subheader("Cycling Routes Map")
    if gdf.empty:
        st.warning("No data available for the selected filters.")
    else:
        folium_static(create_map(gdf))

with tab2:
    st.subheader("Cycling Routes Data")
    if gdf.empty:
        st.warning("No data available.")
    else:
        st.dataframe(gdf.drop(columns=['geometry']))
