#!/usr/bin/env python3

import pymysql
import csv
import os
import sys

'''
Created on 06.12.2016
@author: Christoph Meyer

Set the following variables to desired values

'''

path = "/root"

host = "192.168.1.63"

user = "user"

passwd = "pwambi"

database = "sakila"

filename = "db.csv"

SQL_statement = """

SELECT * FROM actor

"""




class DB():
    
    def __init__(self, host, user, passwd, database):
        self.liste=[]
        self.conn = ""
        self.cur = ""
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = database
    
    def connect(self):
        try:
            self.conn = pymysql.connect(host=self.host, port=3306, user=self.user, passwd=self.passwd, db=self.db)
            self.cur = self.conn.cursor()
        except:
            print("Error: Connection failed")
            sys.exit(1)
    
    def fetch(self, statement):
        try:
            self.cur.execute(statement)
        except:
            print("SQL statement: Syntax error")
            sys.exit(1)
    def close(self):
        self.cur.close()
        self.conn.close()
 
    def get_data(self):
        for row in self.cur:
            self.liste.append(list(row))
            
        return self.liste    

    def write_csv(self, fname):
        try:
            with open(fname, "w", newline='') as csvfile:
                writer = csv.writer(csvfile)
                for row in self.get_data():
                    writer.writerow(row)
                    
        except:
            print("Error: File was not created")
            sys.exit(1)
                    
        finally:
            csvfile.close()      


def main():
    try:
        os.chdir(path)
    except:
        print("Error: Path not found")
        sys.exit(1)
        
    db = DB(host, user, passwd, database)
    db.connect()
    db.fetch(SQL_statement)
    db.close()
    db.write_csv(filename)


if __name__ == "__main__":
    main()