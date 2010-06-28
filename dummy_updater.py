#!/usr/bin/python
# -*- coding: utf-8 -*-

"""Do-nothing updater
"""

class DummyUpdater:
    """Simple implementation of Updater interface
    """
    
    def __init__(self, quiet):
        self.quiet = quiet
    
    def complete(self):
        """Updating is complete
        """
        if not self.quiet:
            print "Complete"
        

    def update(self, osm_id, cladr_data):
        """Item can be updated
        """
        if not self.quiet:
            print "Found modified street #%s (%s) %s" % \
                (cladr_data['cladr:code'], osm_id, cladr_data['name'])
        
