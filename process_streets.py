#!/usr/bin/python
# -*- coding: utf-8 -*-
# :noTabs=true:indentSize=4:

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
from osm_db import OsmDB
import logging
from logger_db import DatabaseLogger

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

__version__ = u"3.0.0"
REGEX = ur'(\d+)\-(Й|ГО|Я|АЯ|ИЙ|ЫЙ|ОЙ)\s+'
NUMBER_IN_NAMES_REGEX = re.compile(REGEX, re.UNICODE)
ABBREVS = {}

#TODO: Log exceptions
#TODO: Logging config
#TODO: Extract CladrBot to class

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
        type="int", \
        help="OSM ID of city polygon, optional. If not set settlements table will be used")
    parser.add_option("-o", "--cladr", \
        dest="city_cladr_code", \
        type="str", \
        help="CLADR code of city, optional. If not set settlements table will be used")

    (options, args) = parser.parse_args()

    engine = create_engine('postgresql://%s:%s@%s:%d/%s' % (options.db_user, options.db_password, options.db_host, options.db_port, options.db_name))

    cladr_db = CladrDB(engine)
    osm_db = OsmDB(engine)

    result_listeners = [DatabaseLogger(engine)]
    
    map(lambda l: l.clear(), result_listeners)
    
    ABBREVS.update(read_abbrevs())
    
    return (osm_db, cladr_db, result_listeners, options.city_osm_id, options.city_cladr_code)
    
def read_abbrevs():
    """Return readed short names expansion table
    """    
    abbrevs = {}
    with open('abbrev.txt','r') as fhx:
        for line in fhx:
            line = line.decode("utf-8").upper()
            abbrevs[line.split('=')[0]] = line.split('=')[1].rstrip() + " "
    
    return abbrevs
    
def expand_abbrevs(name):
    """Replace abbreviations in specified name to full words.
    """
    key = name.upper()
    for abbrev, word in ABBREVS.iteritems():
        key = re.sub(abbrev, word, key)
    
    #Remove (.*) from the street name
    key = re.sub(r'\(.*?(:?\)|$)', '', key)
    words = key.split(" ")
    words.sort()
    
    key = " ".join(words)    
    
    key = NUMBER_IN_NAMES_REGEX.sub(lambda i: i.group(1) + " ", key)
    key = re.sub(u"Ё", u"Е", key)
    key = re.sub(u"[\"'«»№]", u" ", key)
    key = re.sub(u"\s+", u" ", key).strip()

    logging.debug("Street name %s was converted to %s" % (name, key))
    
    return key
    
def process(settlement_id, osm_id, cladr, osm_db, cladr_db, result_listeners):
    """Match CLADR and OSM streets by name and kladr:user tags
    """

    (cladr_by_name, cladr_by_code) = cladr_db.load_data(expand_abbrevs, cladr)
    osm_data = osm_db.load_data(expand_abbrevs, osm_id)
    
    matched_streets = {}
    missed_streets = {}
    
    for osm in osm_data:
        if osm.kladr_user in cladr_by_code:
            #Found by kladr:user
            logging.debug("Found '%s' (#%s) by kladr:user" % (osm.name, osm.kladr_user))
            matched_streets.setdefault(osm.kladr_user, []).append(osm)
        elif osm.key in cladr_by_name:
            #Found by name
            for cladr in cladr_by_name[osm.key]:
                logging.debug("Found '%s' (#%s) by name" % (osm.name, cladr.code))
                matched_streets.setdefault(cladr.code, []).append(osm)
        else:
            #Not found
            logging.debug("Missed '%s'" % (osm.name))
            missed_streets.setdefault(osm.key, []).append(osm)
    
    map(lambda l: l.save_found_streets(settlement_id, matched_streets), result_listeners)
    map(lambda l: l.save_missed_streets(settlement_id, missed_streets), result_listeners)

    logging.info("Processing complete: %d/%d (found/missed)" % (len(matched_streets), len(missed_streets)))

def main():
    """Application entery point
    """
    
    (osm_db, cladr_db, result_listeners, osm_id, cladr) = read_options()

    if osm_id and cladr:
        logging.info("Processing city #%s (%d)" % (cladr, osm_id))
        
        process(0, str(osm_id), cladr, osm_db, cladr_db, result_listeners)
        
    else:
        logging.info("Processing settlements table")
    
        for settlement in osm_db.query_settlements():
            logging.info("Processing city #%s (%s)" % (str(settlement.polygon_osm_id), str(settlement.kladr)))
        
            try:
                process(settlement.id, str(settlement.polygon_osm_id), str(settlement.kladr), osm_db, cladr_db, result_listeners)
            except Exception:
                logging.error("Died in city #%s (%s)" % (str(settlement.polygon_osm_id), str(settlement.kladr)))
                traceback.print_exc()
        
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()