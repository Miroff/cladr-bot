#!/usr/bin/python
# -*- coding: utf-8 -*-

import pgdb

CHUNK_SIZE = 1000

drop_table_sql = """
DROP TABLE IF EXISTS cladr;
"""


create_table_sql = """
CREATE TABLE cladr (
  code varchar(17),
  status char(2),
  name varchar(256),
  type varchar(256),
  code_prefix char(11),
  code_suffix char(4),
  actuality char(2),
  postcode varchar(6),
  okatd varchar(11)
);

CREATE INDEX cladr_code_prefix_idx ON cladr (code_prefix, actuality);
CREATE UNIQUE INDEX cladr_code_idx ON cladr (code);
CREATE INDEX cladr_okatd_idx ON cladr (okatd);
CREATE INDEX cladr_actuality_idx ON cladr (actuality);
"""

insert_sql = """
INSERT INTO cladr VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

get_cladr_streets_query = """
SELECT code, name, type, postcode, okatd FROM cladr WHERE code_prefix = '%s' AND actuality = '00' AND status = '99'
"""

class CladrDB:
  def __init__(self, host, port, database, user, password, quiet):
    self.data = []
    self.connection = pgdb.connect(host="%s:%s" % (host, port), database=database, user=user, password=password)
    self.count = 0
    self.quiet = quiet
    
  def recreate(self):
    if not self.quiet:
      print "Recreate database"
    cursor = self.connection.cursor()
    cursor.execute(drop_table_sql)
    cursor.execute(create_table_sql)
    cursor.close()
  
  def insert(self, code, status, name, typename, code_prefix, code_suffix, actuality, postcode, okatd):
    self.data.append((code, status, name, typename, code_prefix, code_suffix, actuality, postcode, okatd))
    self.count += 1
    if self.count % CHUNK_SIZE == 0:
      self.dump();
      if not self.quiet:
        print "Chunk %s\r" % (self.count / CHUNK_SIZE)
        
  def compact(self, key, name, cladr_code, cladr_name, cladr_suffix, postcode): 
    return {
        'key': key, 
        'name': name, 
        'cladr:code': cladr_code, 
        'cladr:name': cladr_name,
        'cladr:suffix': cladr_suffix,
        'addr:postcode': postcode,
       }

  def load_data(self, prepare_name, city_cladr_code):
    cursor = self.connection.cursor()
    cursor.execute(get_cladr_streets_query % pgdb.escape_string(city_cladr_code[0:11]))
    
    cladr_by_name = {}
    cladr_by_code = {}
    short_street_names = {}
    
    skip_streets = []
    for street in cursor.fetchall():
      key = prepare_name(street[1] + " " + street[2])

      #Count streets with the same name count
      key2 = prepare_name(street[1])
      if key2 not in short_street_names: 
        short_street_names[key2] = 0
      short_street_names[key2] += 1
      
      name = street[1] + " " + street[2]
      data = self.compact(key, name, street[0], street[1], street[2], street[3])
      
      if key not in cladr_by_name:
        cladr_by_name[key] = data
      else:
        #only one street with the same name is allowed
        skip_streets.append(key)

      cladr_by_code[street[0]] = data
      
    #Add streets without status part if the name is only one
    for data in cladr_by_name.values():
      key = prepare_name(data['cladr:name'])
      if short_street_names[key] == 1:
        cladr_by_name[key] = data

    for street in skip_streets:
      del cladr_by_name[street]

    return (cladr_by_name, cladr_by_code)
    
  def dump(self):
    cursor = self.connection.cursor()
    cursor.executemany(insert_sql, self.data)
    self.data = []
    cursor.close()

  def complete(self):
    self.dump()
    if not self.quiet:
      print "%s rows inserted" % self.count
    self.connection.commit()
    self.close()

  def close(self):
    self.connection.close()  
   
