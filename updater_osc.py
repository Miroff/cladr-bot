#!/usr/bin/python

# -*- coding: utf-8 -*-

#This file is used to save CLADR changes into OSC <http://wiki.openstreetmap.org/wiki/Osc> file

import OsmApi

class OSCUpdater:
  def init(self, user, password):
    self.api = OsmApi.OsmApi(api='xapi.openstreetmap.org', username = user, password = password)

  def close(self):
    self.api.flush()

  def update(self, osm_id, cladr_object):
    print "TODO: Implement %s" % osm_id
  
