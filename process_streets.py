#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Checks all streets in specified OSM database and check it against 
CLADR database. Optionally this tool can upload Cladr codes to 
OpenStreetMap. 

Usage: python process_streets.py --help
"""


from optparse import OptionParser
import re
import traceback
from cladr_db import CladrDB
from osm_db import OSMDB
from updater_api import APIUpdater
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
    parser.add_option("-k", "--api-user", \
        dest="api_user", \
        help="OSM API User")
    parser.add_option("-j", "--api-password", \
        dest="api_password", \
        help=",OSM API password")
    parser.add_option("-q", "--quiet", \
        action="store_true", \
        dest="quiet", \
        default=False, \
        help="don't print status messages to stdout")
    parser.add_option("-d", "--do-changes", \
        action="store_true", \
        dest="do_changes", \
        default=False, \
        help="Upload changes to OSM")
    parser.add_option("-l", "--logs-path", \
        dest="logs_path", \
        help="Path where log files will be stored", \
        default="./logs")
    parser.add_option("-s", '--source-set', \
        dest="source_set", \
        help="Whitch populated areas will be processed: streets|oktmo-okato",
        default="streets")

    (options, args) = parser.parse_args()
    
    if options.api_user != None:
        updater = APIUpdater( \
            user=options.api_user.decode('utf-8'), \
            password=options.api_password.decode('utf-8'), \
            do_changes=options.do_changes, \
            quiet=options.quiet, \
            version=__version__)
    else:
        updater = DummyUpdater(options.quiet)
    
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
    
    return (osm_db, cladr_db, updater, logger, options.source_set, \
        options.quiet)

    
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
    
def changed(osm, cladr):
    """Return true if there is significant difference between 
    OSM and CLADR records
    """
    if osm['cladr:code'] != cladr['cladr:code'] and osm['cladr:code'] == None:
        return True
    if osm['cladr:name'] != cladr['cladr:name'] and osm['cladr:name'] == None:
        return True
    if osm['cladr:suffix'] != cladr['cladr:suffix'] and \
        osm['cladr:suffix'] == None:
        return True
    if osm['addr:postcode'] != cladr['addr:postcode'] and \
        cladr['addr:postcode'] != '' and osm['addr:postcode'] == None:
        return True
        
    return False

def process(city_polygon_id, city_cladr_code, osm_db, cladr_db, updater, \
    logger):
    """Main function of object procesing
    """

    (cladr_by_name, cladr_by_code) = cladr_db.load_data( \
        expand_abbrevs, city_cladr_code)
    osm_data = osm_db.load_data(expand_abbrevs, city_polygon_id)
    
    osm_by_code = {}

    map(lambda log: log.new_file(city_cladr_code), logger)

    for osm in osm_data:
        osm_code = osm['cladr:code']
        osm_key = osm['key']
        
        if osm_code != None:
            if osm_code in cladr_by_code and \
                changed(osm, cladr_by_code[osm_code]):
                updater.update(osm['osm_id'], cladr_by_code[osm_code])
            
            osm_by_code.setdefault(osm_code, []).append(osm)
        elif osm_key in cladr_by_name:
            cladr = cladr_by_name[osm_key]['cladr:code']

            if changed(osm, cladr_by_name[osm_key]):
                updater.update(osm['osm_id'], cladr_by_name[osm_key])
                
            osm_by_code.setdefault(osm_code, []).append(osm)
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
    (osm_db, cladr_db, updater, logger, source_set, quiet) = read_options()
    
    if source_set == 'streets' :
        if not quiet:
            print "Processing all cities in region"
        
        for (osm_id, cladr) in osm_db.query_all_cities():
            if not quiet:
                print "Processing city #%s (%s)" % (cladr, osm_id)
        
            try:    
                process(str(osm_id), cladr, osm_db, cladr_db, updater, logger)
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
                process(str(osm_id), cladr, osm_db, cladr_db, updater, logger)
            except Exception:
                print "Died in city #%s (%s)" % (cladr, osm_id)
                traceback.print_exc()
        
    updater.complete()
    cladr_db.close()
    osm_db.close()

if __name__ == "__main__":
    main()