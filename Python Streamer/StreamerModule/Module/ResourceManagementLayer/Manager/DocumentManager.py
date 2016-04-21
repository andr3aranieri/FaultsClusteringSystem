# -*- coding: utf-8 -*-
"""
Created on Sat Jul 11 08:49:06 2015

@author: andrearanieri
"""

from Module.ResourceManagementLayer.DB import VolatileDocumentDB
from Module.ResourceManagementLayer.DB import PersistentDocumentDB

class DocumentManager:
    volatileDocumentDB = VolatileDocumentDB.VolatileDocumentDB()
    persistentDocumentDB = PersistentDocumentDB.PersistentDocumentDB()
    
    def insertVolatileDocument(self, document):
        self.volatileDocumentDB.insertDocument(document)
                
        
    def insertPersistentDocument(self, document):
        self.persistentDocumentDB.insertDocument(document)
                    