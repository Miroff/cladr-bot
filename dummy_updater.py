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
      print "Found modified street %s (%s) %s" % (osm_id, cladr_data['cladr:code'], cladr_data['name'])
    
