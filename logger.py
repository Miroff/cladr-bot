#!/usr/bin/python
# -*- coding: utf-8 -*-

#This class save processing results to HTML

HEADER = """
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html>
  <head>
    <link media='all' href='main.css' type='text/css' rel='stylesheet' />
    <title>CLADR processing log for #%s</title>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />  
  </head>
  <body>
    <h1>Missing in CLADR</h1>
    <table>
"""

SEPARATOR = """
    </table>
    <h1>Found in CLADR</h1>
    <table>
"""

ROW = """
      <tr><td id="state" class="%s">%s</td><td id="code">%s</td><td id="name">%s</td></tr>
"""

FOOTER = """
    </table>
    
    <h2>Progress: %d%%</h2>
  </body>
</html>
"""
class Logger:
  def __init__(self, path, cladr_code):
    self.file = open("%s/%s.html" % (path, cladr_code), 'w')
    self.file.write(HEADER % cladr_code)
    self.tables_switched = False
    self.found = 0
    self.missing = 0

  def missing_in_cladr(self, osm):
    self.file.write(ROW % ("missing", "Missing in CLADR", osm['cladr:code'], """<a href="http://www.openstreetmap.org/browse/way/%s">%s</a>""" % (osm['osm_id'], osm['name'])))
    
  def missing_in_osm(self, cladr):
    if not self.tables_switched: 
      self.file.write(SEPARATOR)
      self.tables_switched = True
      
    self.file.write(ROW % ("missing", "Missing in OSM", cladr['cladr:code'], cladr['name']))
    self.missing += 1

  def found_in_osm(self, cladr, osm_rows):
    links = []
  
    for osm in osm_rows:
      links.append("""<a href="http://www.openstreetmap.org/browse/way/%s">%s</a>""" % (osm['osm_id'], cladr['name']))
      
    self.file.write(ROW % ("found", "Found in OSM", cladr['cladr:code'], "<br />".join(links)))
    self.found += 1
  
  def close(self):
    if self.missing + self.found > 0:
      self.file.write(FOOTER % round(100.0 * self.found / (self.missing + self.found)))
    self.file.close()
  
 
  
  
