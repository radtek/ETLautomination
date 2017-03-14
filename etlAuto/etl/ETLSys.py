# -*- coding: utf-8 -*-
import os
import sys
import logging
import time
import datetime
import cx_Oracle as oracle
from ETL import ETL
from HouseKeeping import HouseKeeping

logging.basicConfig(level=logging.INFO)

class ETLSys:
    sys = ''
    DataKeepPeriod = 30
    LogKeepPeriod = 30
    RecordKeepPeriod = 30
    etl = ETL()
    etl.Initialize()
    houKeep = HouseKeeping()

    def dbGetSys(sys,con):
        sysList = []
        sqlText = "select ETL_System, DataKeepPeriod , LogKeepPeriod, RecordKeepPeriod from ETL_Sys"
        try:
            cursor = con.cursor()
            cursor.execute(sqlText)
            result = cursor.fetchall()
            for resTuple in result:
                e = ETLSys()
                e.sys = resTuple[0]
                e.DataKeepPeriod = 30 if resTuple[1] is None else resTuple[1]
                e.LogKeepPeriod = 30 if  resTuple[2] is None else resTuple[2]
                e.RecordKeepPeriod = 30 if  resTuple[3] is None else resTuple[3]
                sysList.append(e)
        except oracle.DatabaseError as e:
            logging.error("Database Error Message: "+str(e.message))
            self.houKeep.dberr = True    
        return sysList

    def  cleaup(self,con):
        expiredDate = (datetime.date.today()-datetime.timedelta(days=self.DataKeepPeriod)).strftime('%Y%m%d')
        self.houKeep.clearupPath(etl.Auto_home + "/DATA/complete/" + self.sys,expiredDate)
        self.houKeep.clearupPath(etl.Auto_home + "/DATA/fail/corrupt/" + self.sys,expiredDate)   
        self.houKeep.clearupPath(etl.Auto_home + "/DATA/fail/bypass/" + self.sys,expiredDate)
        self.houKeep.clearupPath(etl.Auto_home + "/DATA/fail/duplicate/" + self.sys,expiredDate)
        self.houKeep.clearupPath(etl.Auto_home + "/DATA/fail/error/" + self.sys,expiredDate)

        expiredDate = (datetime.date.today()-datetime.timedelta(days=self.LogKeepPeriod)).strftime('%Y%m%d')
        self.houKeep.clearupPath(etl.Auto_home + "/LOG/" + self.sys, expiredDate)

        expiredDate = (datetime.date.today()-datetime.timedelta(days=self.RecordKeepPeriod)).strftime('%Y%m%d')
        self.houKeep.clearupPath(etl.Auto_home + "/LOG/" + self.sys, expiredDate)

        if etl.isPrimaryServer:
            self.houKeep.log.write("Clean repository log for system '" + self.sys + "'")
            sqlText = "DELETE FROM ETL_Received_File WHERE ETL_System = '" + self.sys + "' AND ReceivedTime <= '" + expiredDate + "'"
            cursor = con.cursor()
            try:
                cursor.execute(sqlText)
                con.commit()
            except oracle.DatabaseError as e:
                logging.error("Oracle-Error-Message: "+str(e.message))
                cursor.close()
                con.close()

            sqlText = "DELETE FROM ETL_Record_Log WHERE ETL_System = '" + self.sys + "' AND RecordTime <= '" + expiredDate + "'"
            cursor = con.cursor()
            try:
                cursor.execute(sqlText)
                con.commit()
            except oracle.DatabaseError as e:
                logging.error("Oracle-Error-Message: "+str(e.message))
                cursor.close()
                con.close()

            sqlText = "DELETE FROM ETL_Job_Log WHERE ETL_System = '" + self.sys + "' AND StartTime <= '" + expiredDate + "'"
            cursor = con.cursor()
            try:
                cursor.execute(sqlText)
                con.commit()
            except oracle.DatabaseError as e:
                logging.error("Oracle-Error-Message: "+str(e.message))
                cursor.close()
                con.close()
                
if __name__ == '__main__':
    #etl = ETL.ETL()
    #con = etl.Connect()
    #etlsys = ETLSys()
    #testList = etlsys.dbGetSys(con)
    #for test in testList:
    #    print test.sys
    #pass
    #print ETLSys.DataKeepPeriod    
    #print ETL.PrintVersionInfo('ETL AUTOMINATION')
    print ETLSys.etl.Auto_home
