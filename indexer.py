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
import re
from optparse import OptionParser
from cladr_db import CladrDB

__version__ = "2.0.5"
parser = OptionParser(usage="%prog [options]", version="%prog " + __version__)
parser.add_option("-D", "--database", dest="db_name", help="PostgreSQL databse name", default="osm")
parser.add_option("-U", "--user", dest="db_user", help="PostgreSQL databse user", default="osm")
parser.add_option("-P", "--password", dest="db_password", help="PostgreSQL databse password", default="osm")
parser.add_option("-H", "--host", dest="db_host", help="PostgreSQL databse host", default="localhost")
parser.add_option("-p", "--port", dest="db_port", help="PostgreSQL databse port", type="int", default="-1")
parser.add_option("-q", "--quiet", action="store_true", dest="quiet", default=False, help="don't print status messages to stdout")
parser.add_option("-l", "--logs-path", dest="logs_path", help="Path where log files will be stored", default="./logs")

(options, args) = parser.parse_args()

def read_files(path):
  files = []
  for file in os.listdir(path):
    match = re.search('^(\d{2})(\d{3})(\d{3})(\d{3})(\d{2})\.html$', file)
    if match != None:
      code = {}
      code['region'] = match.group(1)
      code['district'] = match.group(2)
      code['city'] = match.group(3)
      code['area'] = match.group(4)
      code['file'] = file

      files.append(code)
  return files

def load_cladr_items(db_host, db_port, db_name, db_user, db_password, quiet):
   cladrDB = CladrDB(db_host, db_port, db_name, db_user, db_password, quiet)

   regions = cladrDB.get_area_info("%00000000000")
   for region in regions:
     districts = cladrDB.get_area_info('%s%%00000000' % region['region'])
     print region

   cladrDB.close()


load_cladr_items(options.db_host, options.db_port, options.db_name, options.db_user, options.db_password, options.quiet)




