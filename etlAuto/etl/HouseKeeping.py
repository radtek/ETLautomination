# -*- coding: utf-8 -*-
import os
import sys
import time
import datetime
import logging
import glob
import cx_Oracle as oracle
import shutil
from ETL import ETL
from ETLSys import ETLSys
from ControlFilter import ControlFilter

## logging配置可能采取复杂的配置文件配置
logging.basicConfig(level=logging.INFO)

class HouseKeeping:
    dberr = False
    log = None
    lastHour = 15
    etl = ETL()
    etl.Initialize()
    config = etl.getConfig()
    etlSys = ETLSys()

    def removeSubDirectory(self,dirF):
        if os.path.exists(dirF) and os.path.isdir(dirF):
            shutil.rmtree(dirF, ignore_errors = False, onerror = None)           
        else:
            return

    def doCleanup(self,con):
        if(self.config.has_option('ETL','AUTO_KEEP_PERIOD')):
            keepDays = self.config.get('ETL','AUTO_KEEP_PERIOD')
        else:
            keepDays = 30
        expiredDate = (datetime.date.today()-datetime.timedelta(days=keepDays)).strftime('%Y%m%d')
        clearupPath(etl.Auto_home+"/DATA/fail/unknown",expiredDate)
        logDir = etl.Auto_home + "/LOG"
        if os.path.isdir(logDir):
            ctrlFilter = ControlFilter(4)
            globPattern = ctrlFilter.getPattern()
            fileList = glob.glob(logDir+'/'+globPattern)
            for file in fileList:
                if os.path.isfile(file):
                    fileName = os.path.basename(file)
                    logDate = fileName[len(fileName)-12,len(fileName)-4]
                    if(cmp(logDate,expiredDate) < 0):
                        logging.info("delete file:"+fileName)
                        os.unlink(file)        
        for sysL in self.etlSys.dbGetSys(con):
            for s in sysL:
                s.cleaup(con)

        return self.dberr        

    def CleanupAll(self,con):
        currHour = time.localtime().tm_hour
        #currDay = (datetime.date.today()).strftime('%Y%m%d')
        #if(((self.lastHour+1)%24 == currHour) and (self.etl.cleanHour == currHour)):
        #    self.dberr = False
        #    fileName = self.etl.Auto_home + "/LOG/etlclean_" + currDay + ".log"
            ## 采用logging模块重定向方式
        logging.info("beging clean up ...")
        doCleanup(con)
        self.lastHour = currHour
        return self.dberr

    def clearupPath(self,basePath,expiredDate):
        if not os.path.exists(basePath):
            return
        dirList = os.listdir(basePath)
        if not dirList:
            return
        for file in dirList:
            if os.path.isdir(file):
                if len(os.path.basename(file))>8:
                    if(cmp(os.path.basename(file),self.expiredDate)<0):
                        logging.info("clean up directory: "+os.path.dirname(file))
                        removeSubDirectory(file)

if __name__ == '__main__':
     con = None
     hok = HouseKeeping()
     print hok.doCleanup(con)                               



