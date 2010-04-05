#!/usr/bin/python
# -*- coding: utf-8 -*-

import pgdb

query = """
SELECT road.name, road."cladr:code", road."cladr:note", road.osm_id FROM planet_osm_polygon city, planet_osm_line road WHERE city.osm_id = %s AND road.name <> '' AND road.highway in ('trunk','primary', 'secondary', 'tertiary', 'residential', 'service', 'living_street', 'unclassified') AND ST_Within(road.way, city.way)
"""

query_all_cities = """
SELECT city.osm_id AS osm_id, city."cladr:code" AS "cladr:code" FROM planet_osm_polygon city WHERE city.place<>'' AND city."cladr:code" <> ''
UNION
SELECT city.osm_id AS osm_id, point."cladr:code" AS "cladr:code" FROM planet_osm_polygon city, planet_osm_point point WHERE point."cladr:code" <> '' AND ST_Within(point.way, city.way) AND point.place<>'' AND city.place <> ''
"""

class OSMDB:
  def __init__(self, host, port, database, user, password, quiet):
    self.data = []
    self.connection = pgdb.connect(host="%s:%s" % (host, port), database=database, user=user, password=password)
    self.count = 0
    self.quiet = quiet

  def compact(self, key, name, cladr_code, osm_id): 
    return {
        'key': key, 
        'name': name, 
        'cladr:code': cladr_code, 
        'osm_id': osm_id,
     } 

  def load_data(self, prepare_name, key):
    cursor = self.connection.cursor()
    
    cursor.execute(query % pgdb.escape_string(str(key)))
    
    osm_data = []
    osm_by_code = {}
    
    for street in cursor.fetchall():
      if street[2] != None:
        code = street[2]
      else:
        code = street[1]

      if code not in osm_by_code:
        osm_by_code[code] = []

      data = self.compact(prepare_name(street[0]), street[0], code, street[3])
      osm_by_code[code].append(data)
      osm_data.append(data)
    return (osm_data, osm_by_code)
    

  def query_all_cities(self):
    cursor = self.connection.cursor()
    cursor.execute(query_all_cities)

    cities = []

    for city in cursor.fetchall():
      cities.append((city[0], city[1]))
    
    return cities

  def close(self):
    self.connection.close()  
