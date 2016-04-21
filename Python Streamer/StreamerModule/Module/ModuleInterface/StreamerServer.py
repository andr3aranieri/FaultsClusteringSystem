# -*- coding: utf-8 -*-
"""
Created on Sat Jul  4 14:14:49 2015

@author: andrearanieri
"""

#!/usr/bin/python

from Module.BusinessLogic import ThreadDispatcher

import threading    
import socket  
import sys
import traceback
  
class StreamerServer:

    portNumber = 8899

    def __init__(self):
        self.stop = False
        self.serversocket = None
        self.threadID = 1
        self.lock = threading.Lock()
        self.activeStreams = dict()
    
    def createServerSocket(self):
        self.serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:        
            self.serversocket.bind(("127.0.0.1", self.portNumber))
        except:
            print "Address already in use... Kill the preoviuosly launched StreamerServer Instance."
            sys.exit(0)            
            
        self.serversocket.listen(5)
    
    def mainThread(self):
        while not self.stop:
            try:   
                print "Server Socket listening on host: " + socket.gethostname() + ", port: " + str(self.portNumber)  + "> waiting for client..."           
                (socketClient, address) = self.serversocket.accept()
                print "Server Socket listening on host: " + socket.gethostname() + ", port: " + str(self.portNumber)  + "> received connection from " + str(address)           
                threadDispatcher = ThreadDispatcher.ThreadDispatcher(socketClient, self.threadID, self.lock)
                threadDispatcher.start()
                #threadDispatcher.run()
                self.threadID += 1    
            except:
                print "\n**********************************"
                print "******* UNEXPECTED ERROR *********\n"                
                print traceback.format_exc()
                print "**********************************"
                print "**********************************\n"
                print "Delete socket..."
                self.serversocket.close()
                del self.serversocket
                print "Bye!"
                sys.exit(0)
                raise
            
    def addStream(self, stream):
        self.activeStreams.add(stream)