#!/usr/bin/python
# -*- coding: utf-8 -*-

"""This class save processing results to HTML
"""

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
    
    <h3>Progress: %d%%</h3>
    <h3>Generated at: %s</h3>
"""

MISSING_IN_CLADR_BEGIN = """
    <h2>Missing in CLADR</h2>
    <table>
"""

MISSING_IN_CLADR_END = """
    </table>
"""

FOUND_IN_CLADR_BEGIN = """
    <h2>Found in CLADR</h2>
    <table>
"""

FOUND_IN_CLADR_END = """
    </table>
"""

ROW = """
      <tr><td id="state" class="%s">%s</td><td id="code">%s</td><td id="name">%s</td></tr>
"""

OSM_LINK = """<a href="http://www.openstreetmap.org/browse/way/%s">%s</a>"""

FOOTER = """
    </table>
    </hr>
  </body>
</html>
"""

from datetime import datetime

class Logger:
    """Class for saving OSM - CLADR check results
    """
    def __init__(self, cladr_db, path):
        self.cladr_db = cladr_db
        self.path = path
    
    def new_file(self, cladr_code):
        """Start saving of new city file
        """
        self.file = open("%s/%s.html" % (self.path, cladr_code), 'w')
        
        self.hierarchy = self.cladr_db.query_hierarchy(cladr_code)
        self.cladr_code = cladr_code
        self.osm = []
        self.missing_cladr = []
        self.found = 0
        self.missing = 0

    def missing_in_cladr(self, osm):
        """Item was found in OSM but missed in CLADR
        """
        if not osm['omit_in_logs']:
            self.missing_cladr.append(osm)
        
    def missing_in_osm(self, cladr):
        """Item was found in CLADR but missed in OSM
        """
        cladr['class'] = 'missing'
        self.osm.append(cladr)
        self.missing += 1
    
    def found_in_osm(self, cladr, osm_rows):
        """Item was found both in OSM and in CLADR
        """
        cladr['osm'] = osm_rows
        cladr['class'] = 'found'
        self.osm.append(cladr)
        self.found += 1
    
    def close(self):
        """Close current city file
        """
        if self.missing + self.found > 0:
            percent = round(100.0 * self.found / (self.missing + self.found))
        else:
            percent = 100


        self.file.write(HEADER % (self.cladr_code, ", ".join(self.hierarchy), \
            percent, datetime.now()))
        
        if len(self.missing_cladr) > 0: 
            self.file.write(MISSING_IN_CLADR_BEGIN)
            for osm in self.missing_cladr:
                self.file.write(ROW % ("missing", "Missing in CLADR", \
                    '', OSM_LINK % (osm['osm_id'], \
                    osm['name'])))
            
            self.file.write(MISSING_IN_CLADR_END)
        
        self.osm.sort(lambda a, b: cmp(a['name'].lower(), b['name'].lower()))
        
        if len(self.osm) > 0:
            self.file.write(FOUND_IN_CLADR_BEGIN)
            for cladr in self.osm:
                if cladr['class'] == 'missing':
                    self.file.write(ROW % ("missing", "Missing in OSM", \
                        cladr['cladr:code'], cladr['name']))
                else:
                    links = []
                    for osm in cladr['osm']:
                        links.append(OSM_LINK % (osm['osm_id'], cladr['name']))
            
                    self.file.write(ROW % ("found", "Found in OSM", \
                        cladr['cladr:code'], "<br />".join(links)))
            self.file.write(FOUND_IN_CLADR_END)
                
            
        self.file.write(FOOTER)
        self.file.close()