# -*- coding: utf-8 -*-
"""
Created on Mon Jul  6 16:59:02 2015

@author: andrearanieri
"""
from Module.ResourceManagementLayer.Manager import StreamManager
from Module.ResourceManagementLayer.Entity import Stream
from Module.BusinessLogic import TextPreprocessManager
from Module.ResourceManagementLayer.Entity import VolatileDocument
from Module.ResourceManagementLayer.Manager import DocumentManager

import time
import threading
import imaplib
import email
import email.header
import traceback
import HTMLParser

class MailReader (threading.Thread):
    server = ''
    userName = ''
    password = ''
    folder = ''
    idStream = 0
    idUser = 0
    M = None    
    streamManager = StreamManager.StreamManager()
    textPreprocessManager = TextPreprocessManager.TextPreprocessManager()
    file_rows = list()
    mail_count = 0
    parser = HTMLParser.HTMLParser()
    documentManager = DocumentManager.DocumentManager()
   
    def __init__(self, threadID, params, lock):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.lock = lock
        self.end = False
        self.params = params
        self.threadName = 'Mail_IMAP_stream_' + str(self.threadID)
    
    def run(self):
        self.readParams()
        self.createStream()
        while not self.end:
            self.readMail()            
            time.sleep(5)
            self.checkActive()            
        self.closeConnection()    
            
    def readParams(self):
        aParams = self.params.split('|')
        aValues = None
        for p in aParams:
            aValues = p.split('=')            
            if aValues[0] == 'server':
                self.server = aValues[1]
            elif aValues[0] == 'userName':
                self.userName = aValues[1]
            elif aValues[0] == 'password':
                self.password = aValues[1]
            elif aValues[0] == 'folder':
                self.folder = aValues[1]
            elif aValues[0] == 'idUser':
                try:                
                    self.idUser = int(aValues[1])
                except:
                    print self.twitterStream.threadName + "> can't read idUser parameter... exit"
                    self._Thread__stop()
                
                
        if self.folder == '':
            self.folder = 'INBOX'
        
    def createStream(self):
        #stream = ResourceManagementLayer.Entity.Stream.Stream()
        stream = Stream.Stream()
        stream.streamType = "MAIL"
        stream.parameters = self.params
        stream.active = 1
        stream.idUser = self.idUser
        self.idStream = self.streamManager.insertStream(stream) 
    
    def startConnection(self):
        self.M = imaplib.IMAP4_SSL(self.server)
        self.M.login(self.userName, self.password)
        self.M.list()

        rv, data = self.M.select(self.folder)
        if not rv == 'OK':
            print self.threadName + "> error, ending stream"
            self.endStream()

        #self.pop_conn = poplib.POP3_SSL('pop.googlemail.com')
        #self.pop_conn.user('and.ranieri.datamining2015@gmail.com') 
        #self.pop_conn.pass_('datamining2015')

    def readMail(self):
        print self.threadName + "> Start process mailbox"
        self.process_mailbox()
        print self.threadName + "> End process mailbox"        
            
    def process_mailbox(self):
        self.startConnection()
        
        rv, data = self.M.search(None, 'UNSEEN')
        if rv != 'OK':
            print "No messages found!"
            return
        
        for num in data[0].split():
            rv, data = self.M.fetch(num, '(RFC822)')
            if rv != 'OK':
                print "ERROR getting message", num
                return
                
            msg = email.message_from_string(data[0][1])
            decode = email.header.decode_header(msg['Subject'])[0]
            subject = unicode(decode[0])
            print 'Message %s: %s' % (num, subject)
            print 'Raw Date:', msg['Date']

            mailText = ''
            # NON RIESCO A LEGGERE IL CORPO DELLA MAIL
            if msg.is_multipart():
                for part in msg.get_payload():
                    mailText += part.get_payload() + ' '
            else:
                mailText += msg.get_payload() + ' '
            
            str_mail = self.parser.unescape((subject + ' ' + mailText).replace('\t', ' ').replace('"', '').replace('\n', ' ').replace('|', ' '))
            if self.mail_count == 1:
                self.writeToDB()
                self.mail_count = 0
                self.file_rows = []
            else:
                self.file_rows.append(str_mail)
                self.mail_count += 1
            print "Processed mail num " + str(self.mail_count) + " : " + subject 
            
            #set read mail SEEN so we don't downlaod it in next request;            
            typ, data = self.M.store(num,'+FLAGS','(\SEEN)')
        
        #logout from mailbox        
        self.M.logout()
      
    def writeToDB(self):
        print self.threadName + "> write " + str(len(self.file_rows)) + " to DB"
        for sMail in self.file_rows:
            try:            
                processedMail = self.textPreprocessManager.preprocess(sMail.encode("utf-8"))
                
                volatileDocument = VolatileDocument.VolatileDocument()
                volatileDocument.idUser = self.idUser
                volatileDocument.rawData = processedMail
                volatileDocument.vector = ''        
                self.documentManager.insertVolatileDocument(volatileDocument)
                
            except:
                print self.threadName + "> \n**********************************"
                print self.threadName + "> ******* UNEXPECTED ERROR *********\n"                
                print self.threadName + "> " + traceback.format_exc()
                print self.threadName + "> **********************************"
                print self.threadName + "> **********************************\n"
                continue
      
    def closeConnection(self):
        print self.threadName + "> closing imap connestion... Bye" 
        self.M.close()
    
    def endStream(self):
        self.end = True
        
    def checkActive(self):
        if not self.streamManager.isActive(self.idStream):
            self.endStream()
        

