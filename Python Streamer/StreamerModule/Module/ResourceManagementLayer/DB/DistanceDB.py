# -*- coding: utf-8 -*-
"""
Created on Sat Jul 11 08:36:42 2015

@author: andrearanieri
"""

import DBManager
import DBConfig

class DistanceDB:
 
    def __init__(self):
        self.config = DBConfig.DBConfig() 
        self.dbManager = DBManager.DBManager(self.config.ip, self.config.user, self.config.password, self.config.schema)
         
    def insertDistance(self, distance):
        self.dbManager.createConnection()
        idDistance = self.dbManager.executeInsertQueryParam("INSERT INTO Distance(idUser, points, distance, read) VALUES(%s, %s, %s, 0)", (str(distance.idUser), distance.points, str(distance.distance)))
        self.dbManager.closeConnection()
        return idDistance                
