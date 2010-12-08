#!/usr/bin/python
# -*- coding: utf-8 -*-
# :noTabs=true:indentSize=4:

"""CLADR database API
"""
import logging
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import *

CHUNK_SIZE = 10000

Base = declarative_base()
class CladrRecord(Base):
    __tablename__ = 'cladr'
    
    def __init__(self, code, code_prefix, code_suffix, actuality, name, typename, postcode, okatd, status, obj_class):
        self.code = code
        self.code_prefix = code_prefix
        self.code_suffix = code_suffix
        self.actuality = actuality
        self.name = name
        self.type = typename
        self.postcode = postcode
        self.okatd = okatd
        self.status = status
        self.obj_class = obj_class
        
        if len(self.okatd) == 11 and self.okatd[8:11] == '000':
            self.okatd = self.okatd[0:8]
    
    code = Column(BigInteger, primary_key=True)
    status = Column(String(2))
    code_prefix = Column(String(11))
    code_suffix = Column(String(4))
    actuality = Column(String(2))
    postcode = Column(String(6))
    okatd = Column(String(11))
    obj_class = Column(String(1))
    name = Column(String(256))
    type = Column(String(256))
    is_actual = Column(Boolean)

class CladrDB:
    """CLADR database API
    """
    def __init__(self, engine):
        self.engine = engine
        self.Session = sessionmaker(bind=engine)      
        self.data = []

    def insert(self, cladr):
        self.data.append(cladr)
        
        if len(self.data) >= CHUNK_SIZE:
            self.flush()
        
    def flush(self):
        session = self.Session()
        session.add_all(self.data)
        session.commit()
        self.data = []
        
    def recreate(self):
        """DROP and CREATE database
        """
        
        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)
        
        Index('code_prefix_idx', CladrRecord.code_prefix, CladrRecord.actuality, CladrRecord.status).create(self.engine)

    def load_data(self, prepare_key, cladr):
        """Load CLADR records
        """

        session = self.Session()

        by_key = {}
        by_code = {}

        for cladr_rec in session.query(CladrRecord) \
            .filter(CladrRecord.code_prefix == str(cladr)[0:11]) \
            .filter(CladrRecord.actuality == '00') \
            .filter(CladrRecord.status == '99'): 

            keys = [prepare_key(" ".join([cladr_rec.name, cladr_rec.type]))]
            keys.append(prepare_key(cladr_rec.name))
            
            for key in keys:
                by_key.setdefault(key, []).append(cladr_rec)
                
            by_code.setdefault(cladr_rec.code, []).append(cladr_rec)

        session.close()
        return (by_key, by_code)