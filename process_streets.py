#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Checks all streets in specified OSM database and check it against 
CLADR database. Optionally this tool can upload Cladr codes to 
OpenStreetMap. 

Usage: python process_streets.py --help
"""

#TODO: group streets by status part in the log
#TODO: Check addr:street on the buildings 
#TODO: Add JOSM link to the log

from optparse import OptionParser
import re
import traceback
from cladr_db import CladrDB
from osm_db import OSMDB
from dummy_updater import DummyUpdater
from logger import Logger
from logger_db import LoggerDB

__version__ = u"2.0.5"
REGEX = ur'(\d+)\-(Й|ГО|Я|АЯ|ИЙ|ЫЙ|ОЙ)\s+'
NUMBER_IN_NAMES_REGEX = re.compile(REGEX, re.UNICODE)
ABBREVS = {}

def read_options():
    """Main function
    """

    parser = OptionParser( \
        usage="%prog [options]", \
        version="%prog " + __version__)
    parser.add_option("-D", "--database", \
        dest="db_name", \
        help="PostgreSQL databse name", \
        default="osm")
    parser.add_option("-U", "--user", \
        dest="db_user", \
        help="PostgreSQL databse user", \
        default="osm")
    parser.add_option("-P", "--password", \
        dest="db_password", \
        help="PostgreSQL databse password", \
        default="osm")
    parser.add_option("-H", "--host", \
        dest="db_host", \
        help="PostgreSQL databse host", \
        default="localhost")
    parser.add_option("-p", "--port", \
        dest="db_port", \
        help="PostgreSQL databse port", \
        type="int", \
        default="-1")
    parser.add_option("-c", "--polygon", \
        dest="city_osm_id", \
        help="OSM ID of city polygon")
    parser.add_option("-o", "--cladr", \
        dest="city_cladr_code", \
        help="CLADR code of city")
    parser.add_option("-q", "--quiet", \
        action="store_true", \
        dest="quiet", \
        default=False, \
        help="don't print status messages to stdout")
    parser.add_option("-l", "--logs-path", \
        dest="logs_path", \
        help="Path where log files will be stored", \
        default="./logs")
    parser.add_option("-s", '--source-set', \
        dest="source_set", \
        help="Whitch populated areas will be processed: streets|oktmo-okato",
        default="streets")

    (options, args) = parser.parse_args()
    
    cladr_db = CladrDB(
        host=options.db_host, \
        port=options.db_port, \
        name=options.db_name, \
        user=options.db_user, \
        password=options.db_password, \
        quiet=options.quiet)

    osm_db = OSMDB(
        host=options.db_host, \
        port=options.db_port, \
        name=options.db_name, \
        user=options.db_user, \
        password=options.db_password, \
        quiet=options.quiet)

    logger = [Logger(cladr_db, options.logs_path), LoggerDB(osm_db)]
    ABBREVS.update(read_abbrevs())
    
    return (osm_db, cladr_db, logger, options.source_set, options.quiet)

    
def read_abbrevs():
    """Return readed short names expansion table
    """    
    abbrevs = {}
    with open('abbrev.txt','r') as fhx:
        for line in fhx:
            line = line.decode("utf-8").upper()
            abbrevs[line.split('=')[0]] = line.split('=')[1].rstrip()
    
    return abbrevs
    
def expand_abbrevs(name):
    """Replace abbreviations in specified name to full words.
    """
    name = name.decode("utf-8").upper()
    for abbrev, word in ABBREVS.iteritems():
        name = name.replace(abbrev, word)
        
    words = name.split(" ")
    words.sort()
    
    name = " ".join(words)    
    
    name = NUMBER_IN_NAMES_REGEX.sub(lambda i: i.group(1) + " ", name)
    name = re.sub(u"Ё", u"Е", name)
    
    return name.strip()
    
def process(city_polygon_id, city_cladr_code, osm_db, cladr_db, logger):
    """Match CLADR and OSM streets by name and kladr:user tags
    """

    (cladr_by_name, cladr_by_code) = cladr_db.load_data( \
        expand_abbrevs, city_cladr_code)
    osm_data = osm_db.load_data(expand_abbrevs, city_polygon_id)
    
    osm_by_code = {}

    map(lambda log: log.new_file(city_cladr_code), logger)

    for osm in osm_data:
        osm_kladr_code = osm['kladr:user']
        osm_key = osm['key']

        if osm_kladr_code != None:
            osm_by_code.setdefault(osm_kladr_code, []).append(osm)
        elif osm_key in cladr_by_name:
            cladr = cladr_by_name[osm_key]['cladr:code']
            osm_by_code.setdefault(cladr, []).append(osm)
        else:
            map(lambda log: log.missing_in_cladr(osm), logger)

    for code in cladr_by_code:
        if code in osm_by_code:
            map(lambda log: log.found_in_osm(cladr_by_code[code], osm_by_code[code]), logger)
        else:
            map(lambda log: log.missing_in_osm(cladr_by_code[code]), logger)

    map(lambda log: log.close(), logger)

def main():
    """Application entery point
    """
    (osm_db, cladr_db, logger, source_set, quiet) = read_options()
    
    if source_set == 'streets' :
        if not quiet:
            print "Processing all cities in region"
        
        for (osm_id, cladr) in osm_db.query_all_cities():
            if not quiet:
                print "Processing city #%s (%s)" % (cladr, osm_id)
        
            try:    
                process(str(osm_id), cladr, osm_db, cladr_db, logger)
            except Exception:
                print "Died in city #%s (%s)" % (cladr, osm_id)
                traceback.print_exc()
            
    elif source_set == 'oktmo-okato' :
        if not quiet:
            print "Processing oktmo-okato cities"
    
        for (osm_id, cladr) in osm_db.query_oktmo_okato_settlements():
            if not quiet:
                print "Processing city #%s (%s)" % (cladr, osm_id)
        
            try:
                process(str(osm_id), cladr, osm_db, cladr_db, logger)
            except Exception:
                print "Died in city #%s (%s)" % (cladr, osm_id)
                traceback.print_exc()
        
    cladr_db.close()
    osm_db.close()

if __name__ == "__main__":
    main()