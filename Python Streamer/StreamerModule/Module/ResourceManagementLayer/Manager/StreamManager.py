# -*- coding: utf-8 -*-
"""
Created on Fri Jul 10 19:18:12 2015

@author: andrearanieri
"""
from Module.ResourceManagementLayer.DB import StreamDB

class StreamManager:
    #streamDB = ResourceManagementLayer.StreamDB.StreamDB()
    streamDB = StreamDB.StreamDB()
    
    def insertStream(self, stream):
        return self.streamDB.insertStream(stream)                

    def isActive(self, idStream):
        return self.streamDB.isActive(idStream)
    
    def closeStream(self, idStream):
        self.streamDB.deactivateStream(idStream)