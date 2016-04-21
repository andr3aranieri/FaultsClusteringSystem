# -*- coding: utf-8 -*-
"""
Created on Wed Jul  8 13:36:55 2015

@author: andrearanieri
"""
from Module.BusinessLogic import TwitterReader
from Module.BusinessLogic import MailReader
from Module.ResourceManagementLayer.Manager import StreamManager

import threading

class ThreadDispatcher (threading.Thread):

    MSGLEN = 2048
    outputMsg = "OK##STOP##"
    streamManager = StreamManager.StreamManager()

    def __init__(self, sock, threadID, lock):
        threading.Thread.__init__(self)
        self.socketClient = sock
        self.lock = lock
        self.threadID = threadID

    def run(self):
        msgRequest = self.myReceive()
        self.processRequest(msgRequest)
        self.mySend(self.outputMsg)
        self.closeConnection()
        
    #request: type:param1=value1|param2=value2|param3=value3...|paramN=valueN    
    def processRequest(self, request):
        rValues = request.split(':')
        operation = rValues[0]
        streamParams = rValues[1]    
        
        streamParams = streamParams.replace("##STOP##", "")
        streamParams = streamParams.strip('\t\n\r')        
        
        if operation == 'twtr_pub':
            thread = TwitterReader.TwitterReader(self.threadID, streamParams, self.lock, 'public')
            thread.start()
        elif operation == 'twtr_pri':
            thread = TwitterReader.TwitterReader(self.threadID, streamParams, self.lock, 'private')
            thread.start()           
        elif operation == 'mail_ima':
            thread = MailReader.MailReader(self.threadID, streamParams, self.lock)
            thread.start()
            #thread.run()
        elif operation == 'clos_str':
            print "closing stream... params: " + streamParams            
            self.streamManager.closeStream(int(streamParams))
            print "close streamer"
            
    def myReceive(self):
        msg = ''
        while True:
            chunk = self.socketClient.recv(self.MSGLEN)
            print "chunk received: " + chunk      
            if chunk == '':
                raise RuntimeError, "Socket connection interrupted"
            msg = msg + chunk
            if "##STOP##" in msg:
                break
        return msg

    def mySend(self, msg):
        totalsent = 0
        while True:
            sent = self.socketClient.send(msg[totalsent:])
            if sent == 0:
                raise RuntimeError, "Socket connection interrupted"
            totalsent = totalsent + sent
            if "##STOP##" in msg[:totalsent]:
                break
            
    def closeConnection(self):
        self.socketClient.close()
        del self.socketClient