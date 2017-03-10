import os
import sys
import time
import datetime
import logging
import cx_Oracle as oracle
import shutil
import ETL
import ETLSys

logging.baseConfig(level = logging.DEBUG)

class HouseKeeping:
    dberr = False
    log = None
    lastHour = 15

    def removeSubDirectory(self,dirF):
        if os.path.exists(dirF) and os.path.isdir(dirF):
            shutil.rmtree(dirF, ignore_errors = False, onerror = None)           
        else:
            return

    def doCleanup(self,con):
    ## 暂时没想明白配置文件的属性如何获取
        keepDays = 30
        expiredDate = (datetime.date.today()-datetime.timedelta(days=self.keepDays)).strftime('%Y%m%d')
        clearupPath(ETL.ETL().Auto_home+"/DATA/fail/unknown",expiredDate)
        logDir = ETL.ETL().Auto_home + "/LOG"
        if os.path.isdir(logDir):
            pass
        pass


    def CleanupAll(self,con):
        pass

    def clearupPath(self,basePath,expiredDate);
        pass

if __name__ == '__main__':
    pass                                



