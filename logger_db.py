#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Logger class used for saving CLADR-bot processing results to the DB.
"""

class LoggerDB:
    """
    Class for saving OSM - CLADR check results to the DD
    """
    def __init__(self, osm_db):
        self.osm_db = osm_db
    
    def new_file(self, cladr_code):
        """Start saving of new city file
        """
        self.matches = []

    def missing_in_cladr(self, osm):
        """Item was found in OSM but missed in CLADR
        """
        pass
        
    def missing_in_osm(self, cladr):
        """Item was found in CLADR but missed in OSM
        """
        pass
    
    def found_in_osm(self, cladr, osm_rows):
        """Item was found both in OSM and in CLADR
        """
        for row in osm_rows:
            self.matches.append({'osm_id': row['osm_id'], 'cladr:code':  cladr['cladr:code']})
    
    def close(self):
        """Close current city file
        """
        
        self.osm_db.save_cladr_bot_result(self.matches)
        self.matches = []
