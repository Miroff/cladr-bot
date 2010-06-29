#!/usr/bin/python
# -*- coding: utf-8 -*-

"""Saver of new OSM CLADR data via API 
<http://wiki.openstreetmap.org/wiki/PythonOsmApi>
"""

import OsmApi

COMMENT_STRING = u"КЛАДР-бот v%s"
CHUNK_SIZE = 25

class APIUpdater:
    """Save CLADR data to OSM via API
    """
    def __init__(self, user, password, do_changes, quiet, version):
        self.api = OsmApi.OsmApi(username = user.decode("utf-8"), \
            password = password.decode("utf-8"))
        self.password = password
        self.user = user
        self.data = []             
        self.ways = {}
        self.do_changes = do_changes
        self.quiet = quiet
        
        self.comment = COMMENT_STRING % version

    def complete(self):
        """Send stored data to server in one chunk
        """
        self.dump()

        if len(self.data) <= 0:
            if not self.quiet:
                print "No data to be saved"
            self.data = []             
            self.ways = {}
            return

        if self.do_changes:
            if not self.quiet:
                print "Saving data"

            api = OsmApi.OsmApi(username = self.user.decode("utf-8"), \
                password = self.password.decode("utf-8"))
            api.ChangesetCreate({u"comment": self.comment})
        
            for way in self.data:
                api.WayUpdate(way)
            
            api.ChangesetClose()
            api.flush()
        
        self.data = []             
        self.ways = {}
        

    def update(self, osm_id, cladr_data):
        """Store data for one OSM object
        """
        self.ways[osm_id] = cladr_data

        if len(self.ways) % CHUNK_SIZE == 0:
            self.dump()
        
    def dump(self):
        """Get object from OSM and propagate it with CLADR data
        """
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
                osm_data['cladr:code'] = \
                    cladr_data['cladr:code'].decode('utf-8')
                if not self.quiet:
                    print "cladr:code=%s" % cladr_data['cladr:code'] 
                changed = True
            if 'cladr:name' not in osm_data:
                osm_data['cladr:name'] = \
                    cladr_data['cladr:name'].decode('utf-8')
                if not self.quiet:
                    print "cladr:name=%s" % cladr_data['cladr:name'] 
                changed = True
            if 'cladr:suffix' not in osm_data:
                osm_data['cladr:suffix'] = \
                    cladr_data['cladr:suffix'].decode('utf-8')
                if not self.quiet:
                    print "cladr:suffix=%s" % cladr_data['cladr:suffix'] 
                changed = True
            if 'addr:postcode' not in osm_data and \
                cladr_data['addr:postcode'] != '':
                osm_data['addr:postcode'] = \
                    cladr_data['addr:postcode'].decode('utf-8')
                if not self.quiet:
                    print "addr:postcode=%s" % cladr_data['addr:postcode'] 
                changed = True

            if changed:            
                if not self.quiet:
                    print "Way was modified #%s" % way_id
                self.data.append(way)
        self.ways = {}
    
