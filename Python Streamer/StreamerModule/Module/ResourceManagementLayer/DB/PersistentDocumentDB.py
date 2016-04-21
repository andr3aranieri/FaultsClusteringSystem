# -*- coding: utf-8 -*-
"""
Created on Sat Jul 11 08:36:42 2015

@author: andrearanieri
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Jul  8 23:26:38 2015

@author: andrearanieri
"""

import DBManager
import DBConfig

class PersistentDocumentDB:
 
    def __init__(self):
        self.config = DBConfig.DBConfig() 
        self.dbManager = DBManager.DBManager(self.config.ip, self.config.user, self.config.password, self.config.schema)
         
    def insertDocument(self, document):
        self.dbManager.createConnection()
        idDocument = self.dbManager.executeInsertQueryParam("INSERT INTO PersistentDocument(idUser, rawData, vector, insertDate) VALUES(%s, %s, %s, current_timestamp())", (str(document.idUser), document.rawData, document.vector))
        #idDocument = self.dbManager.executeInsertQuery("INSERT INTO PersistentDocument(idUser, rawData, vector, insertDate) VALUES(" + str(document.idUser) + ", '" + document.rawData + "', '" + document.vector + "', current_timestamp())")
        self.dbManager.closeConnection()
        return idDocument                
