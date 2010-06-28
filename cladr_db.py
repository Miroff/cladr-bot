#!/usr/bin/python
# -*- coding: utf-8 -*-
# :noTabs=true:indentSize=4:

"""CLADR database API
"""

import pgdb
import re

CHUNK_SIZE = 10000

DROP_TABLE_SQL = """
DROP TABLE IF EXISTS cladr;
"""

CREATE_TABLE_SQL = """
CREATE TABLE cladr (
  code varchar(17),
  status char(2),
  name varchar(256),
  type varchar(256),
  code_prefix char(11),
  code_suffix char(4),
  actuality char(2),
  postcode varchar(6),
  okatd varchar(11),
  obj_class char(1),
  is_actual boolean
);

CREATE INDEX cladr_code_prefix_idx ON cladr (code_prefix, actuality);
CREATE UNIQUE INDEX cladr_code_idx ON cladr (code);
CREATE INDEX cladr_okatd_idx ON cladr (okatd);
CREATE INDEX cladr_actuality_idx ON cladr (actuality);
CREATE INDEX cladr_obj_class  ON cladr (obj_class);
CREATE INDEX cladr_is_actual  ON cladr (is_actual);
"""

INSERT_SQL = """
INSERT INTO cladr VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

GET_CLADR_STREETS_QUERY = """
SELECT code, name, type, postcode, okatd FROM cladr WHERE code_prefix = '%s' AND actuality = '00' AND status = '99'
"""

GET_AREA_INFO_QUERY = """
SELECT code, name, type, postcode, okatd FROM cladr WHERE code = '%s'
"""

GET_AREA_QUERY = """
SELECT code, name, type, postcode, okatd FROM cladr WHERE actuality='00' AND status != '99' ORDER BY code ASC
"""

class CladrDB:
    """CLADR database API
    """
    def __init__(self, host, port, name, user, password, quiet):
        self.data = []
        self.connection = pgdb.connect(host="%s:%s" % (host, port), \
            database=name, user=user, password=password)
        self.count = 0
        self.quiet = quiet
        
    def query_hierarchy(self, cladr_code):
        """Get object hierarchy like Region -> City -> District -> Street
        """
        match = re.search('^(\d{2})(\d{3})(\d{3})(\d{3})\d{2}$', cladr_code)

        if not match:
            return []
            
        region = match.group(1)
        district = match.group(2)
        city = match.group(3)
        area = match.group(4)
        
        result = []
        if region != '00':
            result += self.get_area_info("%s00000000000" % region)

        if district != '000':
            result += self.get_area_info("%s%s00000000" % (region, district))

        if city != '000':
            result += self.get_area_info("%s%s%s00000" % \
                (region, district, city))

        if area != '000':
            result += self.get_area_info("%s%s%s%s00" % \
                (region, district, city, area))
        
        return result
        
    def get_area_info(self, cladr_code):
        """Get area name + code
        """
        cursor = self.connection.cursor()
        cursor.execute(GET_AREA_INFO_QUERY % pgdb.escape_string(cladr_code))
        
        result = []

        for area in cursor.fetchall():
            result.append(area[1] + " " + area[2])

        cursor.close()
        return result

    def get_info(self):
        """Get CLADR area info
        """
        cursor = self.connection.cursor()
        cursor.execute(GET_AREA_QUERY)
        
        result = []

        for area in cursor.fetchall():         
            data = {}
            data['name'] = area[1] + " " + area[2]
            match = re.search('^(\d{2})(\d{3})(\d{3})(\d{3})\d{2}$', area[0])
            data['region'] = match.group(1)
            data['district'] = match.group(2)
            data['city'] = match.group(3)
            data['area'] = match.group(4)
            data['code'] = area[0]

            result.append(data)

        cursor.close()
        return result

    def recreate(self):
        """DROP and CREATE database
        """
        if not self.quiet:
            print "Recreate database"
        cursor = self.connection.cursor()
        cursor.execute(DROP_TABLE_SQL)
        cursor.execute(CREATE_TABLE_SQL)
        cursor.close()
    
    def insert(self, code, status, name, typename, code_prefix, code_suffix, \
            actuality, postcode, okatd, obj_class):
        """Inser CLADR record
        """
        if len(okatd) == 11 and okatd[8:11] == '000':
            okatd = okatd[0:8]
        self.data.append((code, status, name, typename, code_prefix, \
            code_suffix, actuality, postcode, okatd, obj_class, \
            actuality == u'00'))
        self.count += 1
        if self.count % CHUNK_SIZE == 0:
            self.dump()
            if not self.quiet:
                print "Chunk %s\r" % (self.count / CHUNK_SIZE)
                
    def load_data(self, prepare_name, city_cladr_code):
        """Load CLADR records
        """
        cursor = self.connection.cursor()
        cursor.execute(GET_CLADR_STREETS_QUERY % \
            pgdb.escape_string(city_cladr_code[0:11]))
        
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
            data = {
                'key': key, 
                'name': name, 
                'cladr:code': street[0], 
                'cladr:name': street[1],
                'cladr:suffix': street[2],
                'addr:postcode': street[3],
             }

            
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
            if (street in cladr_by_name):
                del cladr_by_name[street]

        return (cladr_by_name, cladr_by_code)
        
    def dump(self):
        """Save stored records to database
        """
        cursor = self.connection.cursor()
        cursor.executemany(INSERT_SQL, self.data)
        self.data = []
        cursor.close()

    def complete(self):
        """Save stored records to database and close connection
        """
        self.dump()
        if not self.quiet:
            print "%s rows inserted" % self.count
        self.connection.commit()
        self.close()
        
    def close(self):
        """Close CLADR database connection
        """
        self.connection.close()    
     
