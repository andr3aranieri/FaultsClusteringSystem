# -*- coding: utf-8 -*-
"""
Created on Sat Jul 11 09:33:33 2015

@author: andrearanieri
"""
from Module.ModuleInterface import StreamerServer

import sys
import signal

def deleteSocket(self):
     print "Delete socket..."
     self.serversocket.close()
     del self.serversocket
     print "Bye!"
     sys.exit(0)

signal.signal(signal.SIGINT, deleteSocket)            
server = StreamerServer.StreamerServer() 
server.createServerSocket()
server.mainThread()    
    