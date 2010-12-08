#!/usr/bin/python
# -*- coding: utf-8 -*-
# :noTabs=true:indentSize=4:

"""
Logger class used for saving CLADR-bot processing results to the DB.
"""

from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import *
from sqlalchemy.orm import relationship

Base = declarative_base()
class Street(Base):
    __tablename__ = "street"

    id = Column(Integer, Sequence('street_id_seq'), primary_key=True)
    settlement_id = Column(BigInteger)
    name = Column(String)
    objects = relationship("StreetObj", backref="street")
    match = relationship("StreetMatch", uselist=False, backref="street")

    def __init__(self, settlement_id, name):
        self.settlement_id = settlement_id
        self.name= name

class StreetObj(Base):
    __tablename__ = "street_obj"

    street_id = Column(BigInteger, ForeignKey('street.id'), primary_key=True)
    cls = Column(String(10), primary_key=True)
    osm_id = Column(Integer, primary_key=True)
    tag = Column(String, primary_key=True)
    value = Column(String)

    def __init__(self, cls, osm_id, tag, value):
        self.cls = cls
        self.osm_id = osm_id
        self.tag = tag
        self.value = value

class StreetMatch(Base):
    __tablename__ = "street_kladr_match"
    
    def __init__(self, kladr_id):
        self.kladr_id = kladr_id

    street_id = Column(BigInteger, ForeignKey('street.id'), primary_key=True)
    kladr_id = Column(BigInteger, primary_key=True)
    
class DatabaseLogger:
    """
    Class for saving OSM - CLADR check results to the DB
    """
    def __init__(self, engine):
        self.Session = sessionmaker(bind=engine)  
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
    
    def clear(self):
        """Start saving of new city file
        """
        session = self.Session()
        session.execute("TRUNCATE TABLE street, street_obj, street_kladr_match")
        

    def save_found_streets(self, settlement_id, found):
        self.save_streets(settlement_id, found, True)
        
    def save_missed_streets(self, settlement_id, missed):
        self.save_streets(settlement_id, missed, False)

    def save_streets(self, settlement_id, data, is_matched):
        session = self.Session()
        
        results = []
        for cladr, streets in data.items():
            names = ";".join(set(map(lambda osm: osm.name, streets)))
            
            street = Street(settlement_id, names)
            
            for osm in streets:
                obj = StreetObj(osm.type, osm.osm_id, osm.tag, osm.name)
                street.objects.append(obj)
                
                if is_matched:
                    street.match = StreetMatch(cladr)
            
            results.append(street)
            
        session.add_all(results)
        session.commit()
