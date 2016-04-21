# -*- coding: utf-8 -*-
"""
Created on Wed Jul  8 23:26:38 2015

@author: andrearanieri
"""

import DBManager
import DBConfig

class StreamDB:
 
    def __init__(self):
        self.config = DBConfig.DBConfig() 
        self.dbManager = DBManager.DBManager(self.config.ip, self.config.user, self.config.password, self.config.schema)
         
    def insertStream(self, stream):
        self.dbManager.createConnection()
        idStream = self.dbManager.executeInsertQuery("INSERT INTO Stream(creationDate, type, parameters, active, idUser) VALUES(current_timestamp(), '" + stream.streamType + "', '" + stream.parameters + "', 1, " + str(stream.idUser) + ")")
        self.dbManager.closeConnection()
        return idStream                

    def deactivateStream(self, idStream):
        self.dbManager.createConnection()
        self.dbManager.executeUpdateQuery("UPDATE Stream SET active=0 WHERE idStream=" + str(idStream))
        self.dbManager.closeConnection()             
         
    def getStreams(self, idUser):
        self.dbManager.createConnection()
        streams = self.dbManager.executeQuery("SELECT * FROM Stream WHERE idUser = " + idUser)
        self.dbManager.closeConnection()
        return streams
        
    def getStream(self, idStream):
        self.dbManager.createConnection()
        stream = self.dbManager.executeQuerySR("SELECT * FROM Stream WHERE IdStream=" + str(idStream))
        self.dbManager.closeConnection()
        return stream
        
    def isActive(self, idStream):
        stream = self.getStream(idStream)
        return stream[4] == True        
