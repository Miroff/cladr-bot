#!/usr/bin/python
# -*- coding: utf-8 -*-

#This file is used to save CLADR changes directly through API <http://wiki.openstreetmap.org/wiki/PythonOsmApi>

import OsmApi

COMMENT_STRING = u"КЛАДР-бот v2.0.3"
XAPI_URL = "api.openstreetmap.org"
CHUNK_SIZE = 25

class APIUpdater:
  def __init__(self, user, password, dry_run, quiet):
    self.api = OsmApi.OsmApi(username = user.decode("utf-8"), password = password.decode("utf-8"))
    self.password = password
    self.user = user
    self.data = []       
    self.ways = {}
    self.dry_run = dry_run
    self.quiet = quiet

  def complete(self):
    self.dump()

    if not self.dry_run:
      if not self.quiet:
        print "Saving data"

      api = OsmApi.OsmApi(username = self.user.decode("utf-8"), password = self.password.decode("utf-8"))
      api.ChangesetCreate({u"comment": COMMENT_STRING})
    
      for way in self.data:
        api.WayUpdate(way)
      
      api.ChangesetClose()
      api.flush()
    

  def update(self, osm_id, cladr_data):
    if not self.quiet:
      print "Processing %s" % osm_id
    self.ways[osm_id] = cladr_data

    if len(self.ways) % CHUNK_SIZE == 0:
      self.dump()
    
  def dump(self):
    if len(self.ways) == 0: return
    
    ways = self.api.WaysGet(self.ways.keys())
    
    for way_id in ways:
      way = ways[way_id]

      #Node was already removed
      if not way['visible']: 
        continue

      cladr_data = self.ways[way_id]     
      
      osm_data = way['tag']

      changed = False
      if 'cladr:code' not in osm_data:
        osm_data['cladr:code'] = cladr_data['cladr:code'].decode('utf-8')
        if not self.quiet:
          print "cladr:code=%s" % cladr_data['cladr:code'] 
        changed = True
      if 'cladr:name' not in osm_data:
        osm_data['cladr:name'] = cladr_data['cladr:name'].decode('utf-8')
        if not self.quiet:
          print "cladr:name=%s" % cladr_data['cladr:name'] 
        changed = True
      if 'cladr:suffix' not in osm_data:
        osm_data['cladr:suffix'] = cladr_data['cladr:suffix'].decode('utf-8')
        if not self.quiet:
          print "cladr:suffix=%s" % cladr_data['cladr:suffix'] 
        changed = True
      if 'addr:postcode' not in osm_data and cladr_data['addr:postcode'] != '':
        osm_data['addr:postcode'] = cladr_data['addr:postcode'].decode('utf-8')
        if not self.quiet:
          print "addr:postcode=%s" % cladr_data['addr:postcode'] 
        changed = True

      if changed:      
        if not self.quiet:
          print "Way was modified #%s" % way_id
        self.data.append(way)
    self.ways = {}
  
