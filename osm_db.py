#!/usr/bin/python
# -*- coding: utf-8 -*-

import pgdb

query = """
SELECT road.name, road."cladr:code", road."cladr:note", road.osm_id, road."cladr:name", road."cladr:suffix", road."addr:postcode" FROM osm_polygon city, osm_line road WHERE city.osm_id = %s AND road.name <> '' AND road.highway in ('trunk','primary', 'secondary', 'tertiary', 'residential', 'service', 'living_street', 'unclassified') AND city.way_valid AND ST_Within(road.way, city.way)
"""

query_all_cities = """
SELECT city.osm_id AS osm_id, city."cladr:code" AS "cladr:code" FROM osm_polygon city WHERE city.place IN ('city', 'town', 'village', 'hamlet') AND city."cladr:code" <> ''
UNION
SELECT city.osm_id AS osm_id, point."cladr:code" AS "cladr:code" FROM osm_polygon city, osm_point point WHERE point."cladr:code" <> '' AND ST_Within(point.way, city.way) AND point.place IN ('city', 'town', 'village', 'hamlet') AND city.place IN ('city', 'town', 'village', 'hamlet') AND (point.name = city.place_name OR point.name = city.name)
"""

query_oktmo_okato_settlements = """
SELECT p.osm_id, c.code
FROM osm_polygon p 
  LEFT JOIN cladr c ON okatd = okato_code AND status <> '99' AND c.name = p.name
WHERE okato_code IS NOT NULL AND c.code IS NOT NULL;
"""


class OSMDB:
  def __init__(self, host, port, database, user, password, quiet):
    self.data = []
    self.connection = pgdb.connect(host="%s:%s" % (host, port), database=database, user=user, password=password)
    self.count = 0
    self.quiet = quiet

  def compact(self, key, name, cladr_code, osm_id, cladr_name, cladr_suffix, postcode): 
    return {
        'key': key, 
        'name': name, 
        'cladr:code': cladr_code, 
        'osm_id': osm_id,
        'cladr:name': cladr_name,
        'cladr:suffix': cladr_suffix,
        'addr:postcode': postcode,
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

      data = self.compact(prepare_name(street[0]), street[0], code, street[3], street[4], street[5], street[6])
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

  def query_oktmo_okato_settlements(self):
    cursor = self.connection.cursor()
    cursor.execute(query_oktmo_okato_settlements)

    cities = []

    for city in cursor.fetchall():
      cities.append((city[0], city[1]))
    
    return cities


  def close(self):
    self.connection.close()  
