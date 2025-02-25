"""
SQLAlchemy models for geospatial data storage.
"""

from sqlalchemy import Column, String, Float, Integer, Boolean, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from geoalchemy2 import Geometry
from sqlalchemy.dialects.postgresql import JSONB
import os

# Get schema from environment variable or use a default
DB_SCHEMA = os.getenv("DB_SCHEMA", "public")
ROUTES_TABLE_NAME = os.getenv("ROUTES_TABLE_NAME", "cycling_routes")

Base = declarative_base()

class CyclingRoute(Base):
    """SQLAlchemy model for cycling route data."""
    __tablename__ = ROUTES_TABLE_NAME
    __table_args__ = {"schema": DB_SCHEMA}

    # Primary key
    id = Column(Integer, primary_key=True)
    
    # Route identification
    route_id = Column(String, index=True)
    
    # Route metadata from source
    street = Column(String)
    locality = Column(String, index=True)
    route_type = Column(String)
    notes = Column(String)
    surface = Column(String)
    ncn_route = Column(String)
    traffic = Column(String)
    
    # Administrative information
    local_authority = Column(String, index=True)
    la_s_code = Column(String)
    
    # Source information
    sh_date_uploaded = Column(String)
    sh_src = Column(String)
    sh_src_id = Column(Float)
    
    # Calculated properties
    route_length_m = Column(Float)
    
    # File tracking
    source_file = Column(String)
    
    # Timestamps for database tracking
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Geometry column - using GeometryType to support both LineString and MultiLineString
    geometry = Column(Geometry('GEOMETRY', srid=4326))
    
    def __repr__(self):
        return f"<CyclingRoute(id={self.id}, route_id='{self.route_id}')>"