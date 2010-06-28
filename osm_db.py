#!/usr/bin/python
# -*- coding: utf-8 -*-

"""OSM Database API
"""

import pgdb

QUERY = """
SELECT road.name, road."cladr:code", road."cladr:note", road.osm_id, road."cladr:name", road."cladr:suffix", road."addr:postcode" FROM osm_polygon city, osm_line road WHERE city.osm_id = %s AND road.name <> '' AND road.highway in ('trunk','primary', 'secondary', 'tertiary', 'residential', 'service', 'living_street', 'unclassified') AND city.way_valid AND ST_Within(road.way, city.way)
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

class OSMDB:
    """OSM database API
    """
    def __init__(self, host, port, name, user, password, quiet):
        self.data = []
        self.connection = pgdb.connect(host="%s:%s" % (host, port), \
                database=name, user=user, password=password)
        self.count = 0
        self.quiet = quiet

    def load_data(self, prepare_name, key):
        """Load streets in specified polygon
        """
        cursor = self.connection.cursor()
        
        cursor.execute(QUERY % pgdb.escape_string(str(key)))
        
        osm_data = []
        osm_by_code = {}
        
        for street in cursor.fetchall():
            if street[2] != None:
                cladr_code = street[2]
            else:
                cladr_code = street[1]
                
            osm_by_code.setdefault(cladr_code, [])

            data = {
                'key': prepare_name(street[0]), 
                'name': street[0], 
                'cladr:code': cladr_code, 
                'osm_id': street[3],
                'cladr:name': street[4],
                'cladr:suffix': street[5],
                'addr:postcode': street[6],
            } 

            osm_by_code[cladr_code].append(data)
            osm_data.append(data)

        return (osm_data, osm_by_code)

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


    def close(self):
        """Close database connection
        """
        self.connection.close()    
