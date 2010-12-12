#!/usr/bin/python
# -*- coding: utf-8 -*-
# :noTabs=true:indentSize=4:

"""OSM Database API
"""

from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import *

QUERY_STREETS = """
SELECT road.osm_id, road.name, road."kladr:user", 'line' as type, 'name' as tag, false as is_area
FROM osm_polygon city, osm_line road 
WHERE city.osm_id = :osm_id
    AND road.name <> '' 
    AND road.highway in ('trunk', 'primary', 'secondary', 'tertiary', 'residential', 'service', 'living_street', 'unclassified', 'pedestrian') 
    AND ST_Within(road.way, city.way)
UNION
SELECT area.osm_id, area.name, area."kladr:user", 'polygon' as type, 'name' as tag, true as is_area
FROM osm_polygon city, osm_polygon area
WHERE city.osm_id = :osm_id
    AND area.name <> '' 
    AND (area.landuse <> '' OR area.highway in ('trunk', 'primary', 'secondary', 'tertiary', 'residential', 'service', 'living_street', 'unclassified', 'pedestrian') )
    AND ST_Within(area.way, city.way)
UNION
SELECT area.osm_id, area."addr:street" as name, null, 'polygon' as type, 'addr:street' as tag, false as is_area
FROM osm_polygon city, osm_polygon area
WHERE city.osm_id = :osm_id
    AND area.building <> '' 
    AND (area."addr:street" <> '')
    AND ST_Within(area.way, city.way)
"""

QUERY_OKTMO_OKATO_SETTLEMENTS = """
SELECT s.polygon_osm_id AS osm_id, s.kladr
FROM settlement s 
WHERE polygon_osm_id IS NOT NULL AND s.kladr IS NOT NULL;
"""

Base = declarative_base()
class OsmRecord(Base):
    __tablename__ = "osm"
    osm_id = Column(Integer, primary_key=True)
    name = Column(String)
    kladr_user = Column('kladr:user', String)
    type = Column(String)
    tag = Column(String)
    is_area = Column(Boolean)
    
class Settlement(Base):
    __tablename__ = "settlement"

    id = Column(Integer, primary_key=True)
    polygon_osm_id = Column(Integer)
    kladr = Column(BigInteger)
    
class OsmDB:
    """OSM database API
    """
    def __init__(self, engine):
        self.Session = sessionmaker(bind=engine)        

    def load_data(self, prepare_key, osm_id):
        """Load streets in specified polygon
        """
        session = self.Session()
        
        osm_data = []
        for street in session.query(OsmRecord).from_statement(QUERY_STREETS).params(osm_id=osm_id).all():
            street.key = prepare_key(street.name)
            osm_data.append(street)
        
        session.close()
        return osm_data

    def query_settlements(self):
        session = self.Session()
        
        settlements = []
        for settlement in session.query(Settlement) \
            .filter(Settlement.polygon_osm_id != None) \
            .filter(Settlement.kladr != None) \
            .all():
            settlements.append(settlement)

        session.close()
        return settlements