#!/usr/bin/python
# -*- coding: utf-8 -*-
# :noTabs=true:indentSize=4:

from dbfpy import dbf
from optparse import OptionParser
import os.path
from cladr_db import CladrDB
from cladr_db import CladrRecord
from sqlalchemy import create_engine

import logging

logging.basicConfig(level=logging.DEBUG)

parser = OptionParser()
parser.add_option("-D", "--database", dest="db_name", help="PostgreSQL databse name", default="osm")
parser.add_option("-U", "--user", dest="db_user", help="PostgreSQL databse user", default="osm")
parser.add_option("-P", "--password", dest="db_password", help="PostgreSQL databse password", default="osm")
parser.add_option("-H", "--host", dest="db_host", help="PostgreSQL databse host", default="localhost")
parser.add_option("-p", "--port", dest="db_port", help="PostgreSQL databse port", type="int", default="-1")
parser.add_option("-c", "--cladr-dir", dest="cladr_path", help="Path to CLADR files", default=".")

(options, args) = parser.parse_args()

streets_dbf = os.path.join(options.cladr_path, "STREET.DBF")
cladr_dbf = os.path.join(options.cladr_path, "KLADR.DBF")
shortname_dbf = os.path.join(options.cladr_path, "SOCRBASE.DBF")

if not os.path.exists(streets_dbf):
    print "File doesn't exists: " + streets_dbf
    exit(0)

if not os.path.exists(shortname_dbf):
    print "File doesn't exists: " + shortname_dbf
    exit(0)

engine = create_engine('postgresql://%s:%s@%s:%d/%s' % (options.db_user, options.db_password, options.db_host, options.db_port, options.db_name))

db = CladrDB(engine)
db.recreate()

names = {}

for rec in dbf.Dbf(shortname_dbf):
    names[rec['SCNAME']] = rec['SOCRNAME'].decode("CP866")

logging.info("Inserting sreets")

for rec in dbf.Dbf(streets_dbf):
    code = rec['CODE'].decode("CP866")
    cladr = CladrRecord(
        code=code, 
        code_prefix=code[0:11], 
        code_suffix=code[11:15], 
        actuality=code[15:17], 
        name=rec['NAME'].decode("CP866"), 
        typename=names[rec['SOCR']], 
        postcode=rec['INDEX'].decode("CP866"), 
        okatd=rec['OCATD'].decode("CP866"), 
        status=u'99',
        obj_class=u'S')
     
    db.insert(cladr)

logging.info("Inserting territories")
        
for rec in dbf.Dbf(cladr_dbf):
    code = rec['CODE'].decode("CP866")
    cladr = CladrRecord(
        code=code, 
        code_prefix=code[0:8], 
        code_suffix=code[8:11], 
        actuality=code[11:13], 
        name=rec['NAME'].decode("CP866"), 
        typename=names[rec['SOCR']], 
        postcode=rec['INDEX'].decode("CP866"), 
        okatd=rec['OCATD'].decode("CP866"), 
        status=u'99',
        obj_class=u'B')
    db.insert(cladr)

db.flush()
