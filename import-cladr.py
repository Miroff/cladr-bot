#!/usr/bin/python

from dbfpy import dbf
from optparse import OptionParser
import os.path
from cladr_db import CladrDB

parser = OptionParser()
parser.add_option("-D", "--database", dest="db_name", help="PostgreSQL databse name", default="osm")
parser.add_option("-U", "--user", dest="db_user", help="PostgreSQL databse user", default="osm")
parser.add_option("-P", "--password", dest="db_password", help="PostgreSQL databse password", default="osm")
parser.add_option("-H", "--host", dest="db_host", help="PostgreSQL databse host", default="localhost")
parser.add_option("-p", "--port", dest="db_port", help="PostgreSQL databse port", type="int", default="-1")
parser.add_option("-c", "--cladr-dir", dest="cladr_path", help="Path to CLADR files", default=".")
parser.add_option("-u", "--quiet", action="store_false", dest="quiet", default=False, help="don't print status messages to stdout")

(options, args) = parser.parse_args()

streets_dbf = options.cladr_path + "/STREET.DBF"

cladr_dbf = options.cladr_path + "/KLADR.DBF"

shortname_dbf = options.cladr_path + "/SOCRBASE.DBF"

if not os.path.exists(streets_dbf):
  print "File doesn't exists: " + streets_dbf
  exit(0)

if not os.path.exists(shortname_dbf):
  print "File doesn't exists: " + shortname_dbf
  exit(0)

sqlDB = CladrDB(options.db_host, options.db_port, options.db_name, options.db_user, options.db_password, options.quiet)
sqlDB.recreate()

names = {}

for rec in dbf.Dbf(shortname_dbf):
  names[rec['SCNAME']] = rec['SOCRNAME'].decode("CP866")

if not options.quiet:
  print "Inserting sreets"

for rec in dbf.Dbf(streets_dbf):
  sqlDB.insert(
        code=rec['CODE'].decode("CP866"), 
        code_prefix=rec['CODE'][0:11].decode("CP866"), 
        code_suffix=rec['CODE'][11:15].decode("CP866"), 
        actuality=rec['CODE'][15:17].decode("CP866"), 
        name=rec['NAME'].decode("CP866"), 
        typename=names[rec['SOCR']], 
        postcode=rec['INDEX'].decode("CP866"), 
        okatd=rec['OCATD'].decode("CP866"), 
        status=u'99')

if not options.quiet:
  print "Inserting territories"
        
for rec in dbf.Dbf(cladr_dbf):
  sqlDB.insert(
        code=rec['CODE'].decode("CP866"), 
        code_prefix=rec['CODE'][0:8].decode("CP866"), 
        code_suffix=rec['CODE'][8:11].decode("CP866"), 
        actuality=rec['CODE'][11:13].decode("CP866"), 
        name=rec['NAME'].decode("CP866"), 
        typename=names[rec['SOCR']], 
        postcode=rec['INDEX'].decode("CP866"), 
        okatd=rec['OCATD'].decode("CP866"), 
        status=rec['STATUS'].decode("CP866"))
        
sqlDB.complete()

