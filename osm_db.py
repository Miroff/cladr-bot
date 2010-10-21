#!/usr/bin/python
# -*- coding: utf-8 -*-

"""OSM Database API
"""

import pgdb

QUERY_STREETS = """
SELECT road.name, road."kladr:user", road.osm_id 
FROM osm_polygon city, osm_line road 
WHERE city.osm_id = %s 
    AND road.name <> '' 
    AND road.highway in ('trunk', 'primary', 'secondary', 'tertiary', 'residential', 'service', 'living_street', 'unclassified', 'pedestrian') 
    AND city.way_valid 
    AND ST_Within(road.way, city.way)
"""

QUERY_AREAS = """
SELECT area.name, area."kladr:user", area.osm_id 
FROM osm_polygon city, osm_polygon area
WHERE city.osm_id = %s 
    AND area.name <> '' 
    AND (area.landuse <> '' OR area.highway in ('trunk', 'primary', 'secondary', 'tertiary', 'residential', 'service', 'living_street', 'unclassified', 'pedestrian') )
    AND city.way_valid 
    AND ST_Within(area.way, city.way)
"""

QUERY_ALL_CITIES = """
SELECT city.osm_id AS osm_id, city."cladr:code" AS "cladr:code" FROM osm_polygon city WHERE city.place IN ('city', 'town', 'village', 'hamlet') AND city."cladr:code" <> ''
UNION
SELECT city.osm_id AS osm_id, point."cladr:code" AS "cladr:code" FROM osm_polygon city, osm_point point WHERE point."cladr:code" <> '' AND ST_Within(point.way, city.way) AND point.place IN ('city', 'town', 'village', 'hamlet') AND city.place IN ('city', 'town', 'village', 'hamlet') AND (point.name = city.place_name OR point.name = city.name)
"""

QUERY_OKTMO_OKATO_SETTLEMENTS = """
SELECT s.polygon_osm_id AS osm_id, s.kladr
FROM settlement s 
WHERE polygon_osm_id IS NOT NULL AND s.kladr IS NOT NULL;
"""

SQL_SAVE_CLADR_BOT_RESULT = """
INSERT INTO cladr_bot_result (osm_id, code) VALUES (%s, %s);
"""

SQL_DROP_RESULT_TABLE = """
DROP TABLE IF EXISTS cladr_bot_result;
"""

SQL_CREATE_RESULT_TABLE = """
CREATE TABLE cladr_bot_result (
  osm_id integer, 
  code varchar(17)
);"""

class OSMDB:
    """OSM database API
    """
    def __init__(self, host, port, name, user, password, quiet):
        self.data = []
        self.connection = pgdb.connect(host="%s:%s" % (host, port), \
                database=name, user=user, password=password)
        self.count = 0
        self.quiet = quiet
        cursor = self.connection.cursor()
        cursor.execute(SQL_DROP_RESULT_TABLE)
        cursor.execute(SQL_CREATE_RESULT_TABLE)
        cursor.close()

    def load_data(self, prepare_name, key):
        """Load streets in specified polygon
        """

        return self.load_street(QUERY_STREETS, prepare_name, key) + self.load_street(QUERY_AREAS, prepare_name, key, True)
        
    def load_street(self, sql, prepare_name, key, omit_in_logs=False):
        cursor = self.connection.cursor()
        
        cursor.execute(sql % pgdb.escape_string(str(key)))
        
        osm_data = []
        
        for street in cursor.fetchall():
            data = {
                'key': prepare_name(street[0]), 
                'name': street[0], 
                'kladr:user': street[1], 
                'osm_id': street[2],
                'omit_in_logs': omit_in_logs,
            } 

            osm_data.append(data)

        return osm_data

    def query_all_cities(self):
        """Fetch list of all settlements with place tags
        """
        cursor = self.connection.cursor()
        cursor.execute(QUERY_ALL_CITIES)

        cities = []

        for city in cursor.fetchall():
            cities.append((city[0], city[1]))
        
        return cities

    def query_oktmo_okato_settlements(self):
        """Fetch list of all settlements with OKTMO/OKATO tags
        """
        cursor = self.connection.cursor()
        cursor.execute(QUERY_OKTMO_OKATO_SETTLEMENTS)

        cities = []

        for city in cursor.fetchall():
            cities.append((city[0], city[1]))
        
        return cities
        
    def save_cladr_bot_result(self, data):
        cursor = self.connection.cursor()
        for row in data:
            sql = SQL_SAVE_CLADR_BOT_RESULT % (pgdb.escape_string(str(row['osm_id'])), pgdb.escape_string(str(row['cladr:code'])))
            cursor.execute(sql)
        cursor.close()
        self.connection.commit()

    def close(self):
        """Close database connection
        """
        self.connection.close()    
