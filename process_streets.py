#!/usr/bin/python
# -*- coding: utf-8 -*-

from dbfpy import dbf
from optparse import OptionParser
import os.path
import pgdb
import re
import OsmApi
import traceback
from cladr_db import CladrDB
from osm_db import OSMDB
from updater_api import APIUpdater
from updater_osc import OSCUpdater
from dummy_updater import DummyUpdater
from logger import Logger

__version__ = u"2.0.4"
NUMBER_IN_NAMES_REGEX = re.compile(ur'(\d+)\-(Й|ГО|Я|АЯ|ИЙ|ЫЙ|ОЙ)\s+', re.UNICODE)

parser = OptionParser(usage="%prog [options]", version="%prog " + __version__)
parser.add_option("-D", "--database", dest="db_name", help="PostgreSQL databse name", default="osm")
parser.add_option("-U", "--user", dest="db_user", help="PostgreSQL databse user", default="osm")
parser.add_option("-P", "--password", dest="db_password", help="PostgreSQL databse password", default="osm")
parser.add_option("-H", "--host", dest="db_host", help="PostgreSQL databse host", default="localhost")
parser.add_option("-p", "--port", dest="db_port", help="PostgreSQL databse port", type="int", default="-1")
parser.add_option("-c", "--polygon", dest="city_osm_id", help="OSM ID of city polygon")
parser.add_option("-o", "--cladr", dest="city_cladr_code", help="CLADR code of city")
parser.add_option("-k", "--api-user", dest="api_user", help="OSM API User")
parser.add_option("-j", "--api-password", dest="api_password", help=",OSM API password")
parser.add_option("-q", "--quiet", action="store_true", dest="quiet", default=False, help="don't print status messages to stdout")
parser.add_option("-d", "--do-changes", action="store_true", dest="do_changes", default=False, help="Upload changes to OSM")
parser.add_option("-l", "--logs-path", dest="logs_path", help="Path where log files will be stored", default="./logs")
parser.add_option("-s", '--source-set', dest="source_set", default="streets")

(options, args) = parser.parse_args()

if options.api_user != None:
  updater = APIUpdater(options.api_user.decode('utf-8'), options.api_password.decode('utf-8'), options.do_changes, options.quiet, __version__)
else:
  updater = DummyUpdater(options.quiet)


if not options.quiet:
  print "Read abbreviations"
  
#Read short names expansion table
abbrevs = {}
for line in open('abbrev.txt','r').readlines():
  line = line.decode("utf-8").upper()

  abbrevs[line.split('=')[0]] = line.split('=')[1].rstrip()
  
def prepare_name(name):
  name = name.decode("utf-8").upper()
  for k, v in abbrevs.iteritems():
    name = name.replace(k, v)
    
  words = name.split(" ")
  words.sort()
  
  name = " ".join(words)  
  
  name = NUMBER_IN_NAMES_REGEX.sub(lambda i: i.group(1) + " ", name)
  name = re.sub(u"Ё", u"Е", name)
  
  return name.strip()
  
def changed(osm, cladr):
  if osm['cladr:code'] != cladr['cladr:code'] and osm['cladr:code'] == None:
    return True
  if osm['cladr:name'] != cladr['cladr:name'] and osm['cladr:name'] == None:
    return True
  if osm['cladr:suffix'] != cladr['cladr:suffix'] and osm['cladr:suffix'] == None:
    return True
  if osm['addr:postcode'] != cladr['addr:postcode'] and cladr['addr:postcode'] != '' and osm['addr:postcode'] == None:
    return True
    
  return False

def process(city_polygon_id, city_cladr_code, db_host, db_port, db_name, db_user, db_password, do_changes, quiet):
  #Fetch data
  if not quiet: print "Fetching data"

  cladrDB = CladrDB(db_host, db_port, db_name, db_user, db_password, quiet)
  osmDB = OSMDB(db_host, db_port, db_name, db_user, db_password, quiet)

  (cladr_by_name, cladr_by_code) = cladrDB.load_data(prepare_name, city_cladr_code)
  (osm_data, osm_by_code) = osmDB.load_data(prepare_name, city_polygon_id)

  log = Logger(cladrDB, options.logs_path, city_cladr_code)
  if not options.quiet:
    print "Comparing"
  for osm in osm_data:
    if osm['cladr:code'] != None:
      if osm['cladr:code'] in cladr_by_code: 
        if changed(osm, cladr_by_code[osm['cladr:code']]):
          updater.update(osm['osm_id'], cladr_by_code[osm['cladr:code']])
      if osm['cladr:code'] in osm_by_code:
        osm_by_code[osm['cladr:code']].append(osm)
      else:
        osm_by_code[osm['cladr:code']] = [osm]
    elif osm['key'] in cladr_by_name:
      if changed(osm, cladr_by_name[osm['key']]):
        updater.update(osm['osm_id'], cladr_by_name[osm['key']])
      if cladr_by_name[osm['key']]['cladr:code'] in osm_by_code:
        osm_by_code[cladr_by_name[osm['key']]['cladr:code']].append(osm)
      else:
        osm_by_code[cladr_by_name[osm['key']]['cladr:code']] = [osm]
    else:
      log.missing_in_cladr(osm)

  for code in cladr_by_code:
    if code in osm_by_code:
      log.found_in_osm(cladr_by_code[code], osm_by_code[code])
    else:
      log.missing_in_osm(cladr_by_code[code])
    
  updater.complete()
  log.close()
  cladrDB.close()
  osmDB.close()


if options.source_set == 'streets' :
  if not options.quiet:
    print "Processing all cities in region"
  osmDB = OSMDB(options.db_host, options.db_port, options.db_name, options.db_user, options.db_password, options.quiet)
  for (osm_id, cladr) in osmDB.query_all_cities():
    if not options.quiet:
      print "Processing city #%s (%s)" % (cladr, osm_id)
    
    try:
      process(str(osm_id), cladr, options.db_host, options.db_port, options.db_name, options.db_user, options.db_password, options.do_changes, options.quiet)
    except Exception as ex:
      print "Died in city #%s (%s)" % (cladr, osm_id)
      traceback.print_exc()
      
elif options.source_set == 'oktmo-okato' :
  if not options.quiet:
    print "Processing oktmo-okato cities"
  osmDB = OSMDB(options.db_host, options.db_port, options.db_name, options.db_user, options.db_password, options.quiet)
  for (osm_id, cladr) in osmDB.query_oktmo_okato_settlements():
    if not options.quiet:
      print "Processing city #%s (%s)" % (cladr, osm_id)
    
    try:
      process(str(osm_id), cladr, options.db_host, options.db_port, options.db_name, options.db_user, options.db_password, options.do_changes, options.quiet)
    except Exception as ex:
      print "Died in city #%s (%s)" % (cladr, osm_id)
      traceback.print_exc()
  
elif options.city_osm_id != None and options.city_cladr_code != None:
  process(options.city_osm_id, options.city_cladr_code, options.db_host, options.db_port, options.db_name, options.db_user, options.db_password, options.do_changes, options.quiet)
