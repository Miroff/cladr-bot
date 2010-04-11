#!/usr/bin/python
# -*- coding: utf-8 -*-

#Do-nothing updater

class DummyUpdater:
  def __init__(self, quiet):
    self.quiet = quiet
    
  def complete(self):
    if not self.quiet:
      print "Complete updating"
    

  def update(self, osm_id, cladr_data):
    if not self.quiet:
      print "Processing %s" % osm_id
    
