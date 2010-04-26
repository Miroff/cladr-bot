#!/usr/bin/python
# -*- coding: utf-8 -*-

#Tool for creating logs index and calculate total coverage on cities level

#1. List files in logs directory
#2. Load CLADR hierarchy
#3. For each CLADR record check file and if it exist add link

# Layout:
# logs/index.html CLADR level 1
# logs/index/5400000000000.html Level 2-4

import os
import shutil
import re
from datetime import datetime
from optparse import OptionParser
from cladr_db import CladrDB

__version__ = "2.0.5"

HEADER = """
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html>
  <head>
    <link media='all' href='main.css' type='text/css' rel='stylesheet' />
    <title>CLADR processing log for #%s</title>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
  </head>
  <body>
  <h1>%s</h1>
  <h3>Generated at: %s</h3>
"""

TABLE_BEGIN = """
  <h2>%s</h2>
  <table>
"""

TABLE_END = """
  </table>
"""

FOOTER = """
  </hr>
  </body>
</html>
"""


parser = OptionParser(usage="%prog [options]", version="%prog " + __version__)
parser.add_option("-D", "--database", dest="db_name", help="PostgreSQL databse name", default="osm")
parser.add_option("-U", "--user", dest="db_user", help="PostgreSQL databse user", default="osm")
parser.add_option("-P", "--password", dest="db_password", help="PostgreSQL databse password", default="osm")
parser.add_option("-H", "--host", dest="db_host", help="PostgreSQL databse host", default="localhost")
parser.add_option("-p", "--port", dest="db_port", help="PostgreSQL databse port", type="int", default="-1")
parser.add_option("-q", "--quiet", action="store_true", dest="quiet", default=False, help="don't print status messages to stdout")
parser.add_option("-l", "--logs-path", dest="logs_path", help="Path where log files will be stored", default="./logs")

(options, args) = parser.parse_args()

def parse_code(code):
  match = re.search('^(\d{2})(\d{3})(\d{3})(\d{3})\d{2}$', code)
  code = {}
  code['region'] = match.group(1)
  code['district'] = match.group(2)
  code['city'] = match.group(3)
  code['area'] = match.group(4)
  
  return code

def read_files(path):
  files = []
  for file in os.listdir(path):
    match = re.search('^\d{13}\.html$', file)
    if match != None:
      code = file.split('.')[0]

      files.append(code)
  return files
  
def is_region(code):
  return code['district'] == '000' and code['city'] == '000' and code['area'] == '000'

def is_district(code):
  return code['city'] == '000' and code['area'] == '000'

def is_region_city(code):
  return code['district'] == '000' and code['area'] == '000'

def is_district_city(code): 
  return code['district'] != '000' and code['area'] == '000'

def is_district_city_area(code):
  return code['district'] != '000' and code['city'] != '000' and code['area'] != '000'

def is_region_city_area(code):
  return code['district'] == '000' and code['city'] != '000' and code['area'] != '000'

def is_district_area(code):
  return code['district'] != '000' and code['city'] == '000' and code['area'] != '000'
def is_region_area(code):
  return code['district'] == '000' and code['city'] == '000' and code['area'] != '000'

def load_cladr_items(db_host, db_port, db_name, db_user, db_password, quiet):
   cladrDB = CladrDB(db_host, db_port, db_name, db_user, db_password, quiet)
   
   regions = {}

   items = cladrDB.get_info()
   for item in items:
     code = parse_code(item['code'])
     if is_region(code):
       item['districts'] = {}
       item['cities'] = {}
       item['areas'] = {}
       regions[code['region']] = item
     elif is_district(code):
       item['cities'] = {}
       item['areas'] = {}
       regions[code['region']]['districts'][code['district']] = item
     elif is_district_city(code):
       item['areas'] = {}
       regions[code['region']]['districts'][code['district']]['cities'][code['city']] = item
     elif is_region_city(code):
       item['areas'] = {}
       regions[code['region']]['cities'][code['city']] = item
     elif is_district_city_area(code):
       regions[code['region']]['districts'][code['district']]['cities'][code['city']]['areas'][code['area']] = item
     elif is_region_city_area(code) and code['city'] in regions[code['region']]['cities']:       
       regions[code['region']]['cities'][code['city']]['areas'][code['area']] = item
     elif is_district_area(code):
       regions[code['region']]['districts'][code['district']]['areas'][code['area']] = item
     elif is_region_area(code):
       regions[code['region']]['areas'][code['area']] = item
     else:       
       print "Invalid entry: %s" % item['code']
     
   cladrDB.close()
   return regions

def save_items(title, items, files, fh):
  if len(items) > 0:
    fh.write(TABLE_BEGIN % title)   
    for item in sorted(items.values(), lambda a, b: cmp(a['code'], b['code'])):
      if item['code'] in files:
        fh.write("""<tr><td>%s</td><td><a href="../%s.html">%s</a></td></tr>\n""" % (item['code'], item['code'], item['name']))
      else:
        fh.write("<tr><td>%s</td><td>%s</td></tr>\n" % (item['code'], item['name']))
    fh.write(TABLE_END)
  


files = read_files(options.logs_path)
regions = load_cladr_items(options.db_host, options.db_port, options.db_name, options.db_user, options.db_password, options.quiet)

shutil.rmtree("%s/index" % options.logs_path, True)
os.mkdir("%s/index" % options.logs_path)

index = open("%s/index.html" % (options.logs_path), 'w')
index.write(HEADER % (' Russia', 'Russia', datetime.now()))
index.write(TABLE_BEGIN % "Regions")
for region in sorted(regions.values(), lambda a, b: cmp(a['code'], b['code'])):
  index.write("<tr><td>%s</td><td><a href=\"index/%s.html\">%s</a></td>" % (region['code'], region['code'], region['name']))
index.write(TABLE_END)
index.write(FOOTER)
index.close()

for region in regions.values():
  fh = open("%s/index/%s.html" % (options.logs_path, region['code']), 'w')

  name = region['name']
  if region['code'] in files:
    name = """<a href "../%s.html">%s</a>""" % (region['code'], region['name'])

  fh.write(HEADER % (region['code'], name, datetime.now()))

  save_items('Города', region['cities'], files, fh)
  save_items('Поселения', region['areas'], files, fh)

  fh.write(TABLE_BEGIN % "Районы")
  for district in sorted(region['districts'].values(), lambda a, b: cmp(a['code'], b['code'])):
    fh.write("""<tr><td>%s</td><td><a href="%s.html">%s</a></td></tr>\n""" % (district['code'], district['code'], district['name']))
    dh = open("%s/index/%s.html" % (options.logs_path, district['code']), 'w')
    dh.write(HEADER % (district['code'], district['name'], datetime.now()))

    save_items('Города', district['cities'], files, dh)    
    save_items('Поселения', district['areas'], files, dh)

    dh.write(FOOTER)
    dh.close()

  fh.write(TABLE_END)
  fh.write(FOOTER)
  fh.close()
