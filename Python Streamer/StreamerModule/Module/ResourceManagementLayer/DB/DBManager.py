# -*- coding: utf-8 -*-
"""
Created on Wed Jul  8 17:57:07 2015

@author: andrearanieri
"""

import MySQLdb as mdb

class DBManager:
    
    def __init__(self, ip, user, password, schema):
        self.ip = ip
        self.user = user
        self.password = password
        self.schema = schema
        self.conn = None
    
    def createConnection(self):
        self.conn = mdb.connect(self.ip, self.user, self.password, self.schema)
        
    def closeConnection(self):
        self.conn.close()

    def executeInsertQuery(self, insertQuery):
        cur = self.conn.cursor()
        cur.execute(insertQuery)
        self.conn.commit()        
        return cur.lastrowid

    def executeInsertQueryParam(self, insertQuery, params):
        cur = self.conn.cursor()
        try:        
            cur.execute(insertQuery, params)
            self.conn.commit()
        except:
            print "DBManager: problem with text codec"
            
        return cur.lastrowid
        
    def executeUpdateQuery(self, updateQuery):
        cur = self.conn.cursor()
        cur.execute(updateQuery)
        self.conn.commit()        
        
    def executeQuery(self, selectQuery):
        cur = self.conn.cursor()
        cur.execute(selectQuery)
        rows = cur.fecthall()
        return rows
    
    def executeQuerySR(self, selectQuery):
        cur = self.conn.cursor()
        cur.execute(selectQuery)
        row = cur.fetchone()
        return row
                