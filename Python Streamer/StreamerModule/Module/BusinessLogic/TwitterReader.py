# -*- coding: utf-8 -*-
"""
Created on Mon Jul  6 16:59:02 2015

@author: andrearanieri
"""


from Module.ResourceManagementLayer.Manager import StreamManager
from Module.ResourceManagementLayer.Manager import DocumentManager
from Module.ResourceManagementLayer.Entity import Stream
from Module.ResourceManagementLayer.Entity import VolatileDocument
from Module.BusinessLogic import TextPreprocessManager

import time
import threading
from twython import TwythonStreamer
import sys
import os
import HTMLParser
import traceback

#Twitter Stream Implementation;
class TweetStreamer (TwythonStreamer):
    file_rows = []
    start_time = time.time()
    estimatedDistinctUsers = ''
    estimatedSecondMoment = '' 
    end = False
    threadName = ''
    streamManager = StreamManager.StreamManager()
    idStream = 0
    idUser = 0
    textPreprocessManager = TextPreprocessManager.TextPreprocessManager()
    documentManager = DocumentManager.DocumentManager()
    tweet_count = 0
    parser = HTMLParser.HTMLParser()
    
    def on_success(self, data):
        self.checkActive()        
        if self.end:
            print self.threadName + "> has been closed..."
            #write last tweets in memory to DB;            
            self.writeToDB()            
            sys.exit(0)

        creation_date = ''
        if 'created_at' in data:
            creation_date = data['created_at'].encode('utf-8')
        
        user_id = ''            
        if 'user' in data and 'id_str' in data['user']:
            user_id = data['user']['id_str'].encode('utf-8')

        place = '' 
        if 'place' in data and data['place'] is not None and 'full_name' in data['place']:
            place = data['place']['full_name'].encode('utf-8')

        text = ''
        if 'text' in data:
            text = data['text'].encode('utf-8').replace('\n', ' ')
                   
        str_tweet = self.parser.unescape((user_id + ' ' + text +' ' + place).replace('\t', ' ').replace('"', '').replace('\n', ' ').replace('|', ' '))

        print self.threadName + "> " + str_tweet   
     
        if self.tweet_count == 3:
            self.writeToDB()
            self.tweet_count = 0
            self.file_rows = []
        else:
            self.file_rows.append(str_tweet)
            self.tweet_count += 1
       
    def writeToDB(self):
        print self.threadName + "> write " + str(len(self.file_rows)) + " to DB"
        for sTweet in self.file_rows:
            try:            
                processedTweet = self.textPreprocessManager.preprocess(sTweet)
                
                volatileDocument = VolatileDocument.VolatileDocument()
                volatileDocument.idUser = self.idUser
                volatileDocument.rawData = processedTweet
                volatileDocument.vector = ''        
                self.documentManager.insertVolatileDocument(volatileDocument)
                
                """
                persistentDocument = PersistentDocument.PersistentDocument()
                persistentDocument.idUser = self.idUser
                persistentDocument.rawData = processedTweet
                persistentDocument.vector = ''        
                self.documentManager.insertPersistentDocument(persistentDocument)
                """
            except:
                print self.threadName + "> \n**********************************"
                print self.threadName + "> ******* UNEXPECTED ERROR *********\n"                
                print self.threadName + "> " + traceback.format_exc()
                print self.threadName + "> **********************************"
                print self.threadName + "> **********************************\n"
                continue
       
    def scriviSuFile(self):
        out_file = open(self.threadName + "_tweets.tsv","a")
        for row in self.file_rows:
            out_file.write(row)
        out_file.close()
        
    def setThreadName(self, tn):
        self.threadName = tn

    def setIdStream(self, ids):
        self.idStream = ids
        
    def setIdUser(self, idu):
        self.idUser = idu

    def checkActive(self):
        if not self.streamManager.isActive(self.idStream):
            self.endStream()
            
    def endStream(self):
        print self.threadName + "> ending stream...Bye"
        self.end = True

#Thread invoked by Request Dispatcher
class TwitterReader (threading.Thread):
    consumer_key = 'kONfnsrP16egMgBr39f9o3He3'
    consumer_secret = 'Xe1zdwUzT1MOfcqPaZuzZ9jJEj4hFx6M1nwtwc62ESs43J4Rfc'
    access_token = '255160588-ZDtazGlaU95uIA0oJCNVNxiY1H3t9YlpEMFIaMvp'
    access_token_secret = 'NEUk61nw7VeIG0GPPd5gASe2qDXGbeSEPi0TeSkTSK3Bi'  
    
    def __init__(self, threadID, params, lock, pp):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.lock = lock
        self.params = params
        self.end = False
        self.idStream = 0 #set when we insert a row in Stream DB Table
        self.streamManager = StreamManager.StreamManager()
        self.twitterStreamer = None
        self.privateOrPublic = pp
        self.idUser = 0
        self.locations = ''
        self.track = ''
        self.consKeyUser = ''
        self.consSecretUser = ''
        self.accTokenUser = ''
        self.accTokenSecretUser = ''
        
    def run(self):        
        self.parseParams()        
        self.createStream()
        if self.privateOrPublic == 'public':
            self.createPublicTwitterStream()
        elif self.privateOrPublic == 'private':
            self.createPrivateTwitterStream()
            
    def createStream(self):
        #stream = ResourceManagementLayer.Entity.Stream.Stream()
        stream = Stream.Stream()
        stream.streamType = "TWTR"
        stream.parameters = self.params
        stream.active = 1
        stream.idUser = self.idUser        
        self.idStream = self.streamManager.insertStream(stream) 

    def deleteOldFile(self, fileName):
        #rimuoviamo il file dei tweet creato in esecuzioni precedenti;
        try:
            os.remove(fileName)
        except OSError:
            pass       
        
    def createPublicTwitterStream(self):
        print "Create Streamer..."
        self.twitterStreamer = TweetStreamer(self.consumer_key, self.consumer_secret, self.access_token, self.access_token_secret)
        self.twitterStreamer.setThreadName('Pub_twitter_stream_' + str(self.threadID))
        self.twitterStreamer.setIdStream(self.idStream)
        self.twitterStreamer.setIdUser(self.idUser)
        self.deleteOldFile(self.twitterStreamer.threadName + "_tweets.tsv")
                
        #set filters;        
        if not self.locations == '':        
            self.twitterStreamer.statuses.filter(locations = self.locations)
            
        if not self.track == '':
            self.twitterStreamer.statuses.filter(track = self.track)
        
        print "Start Stream..."
        
    def createPrivateTwitterStream(self):
        print "Create Streamer..."
        ##create stream connected to user account (identified by parameters passed in input)
        self.twitterStreamer = TweetStreamer(self.consKeyUser, self.consSecretUser, self.accTokenUser, self.accTokenSecretUser)
        self.twitterStreamer.setThreadName('Pri_twitter_stream_' + str(self.threadID))
        self.twitterStreamer.setIdStream(self.idStream)
        self.deleteOldFile(self.twitterStreamer.threadName + "_tweets.tsv")

        #set filters;        
        self.twitterStreamer.user()
        print "Start Stream..."
        
    def parseParams(self):
        aParams = self.params.split('|')
        aValues = None        
        for param in aParams:
            aValues = param.split('=')
            if aValues[0] == 'loc':
                self.locations = aValues[1]
            elif aValues[0] == 'kw':
                self.track = aValues[1]
            elif aValues[0] == 'cku':
                self.consKeyUser = aValues[1]
            elif aValues[0] == 'csu':
                self.consSecretUser = aValues[1]
            elif aValues[0] == 'atu':
                self.accTokenUser = aValues[1]
            elif aValues[0] == 'atus':
                self.accTokenSecretUser = aValues[1]
            elif aValues[0] == 'idUser':
                try:                
                    self.idUser = int(aValues[1])
                except:
                    print self.twitterStream.threadName + "> can't read idUser parameter... exit"
                    self._Thread__stop()
