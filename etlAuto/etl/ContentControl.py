# -*- coding: utf-8 -*-
import os
import re
import sys
import time
import datetime
import logging
import cx_Oracle as oracle
from ETL import ETL


logging.basicConfig(level=logging.INFO)

class ContentControl:
    nameControl = None
    dataCount = None
    dataFile = None
    fileSize = None
    expect = None
    totalExpectedRecord = None

    def __init__(self,**kwargs):            
        if kwargs.has_key('dir') and kwargs.has_key('name'):
            logging.info("dir is "+kwargs['dir']+" and name is: "+kwargs['name'] )
            self.initialize(kwargs['dir']+'/'+kwargs['name'])

        if kwargs.has_key('pathControl'):
            self.initialize(kwargs['pathControl'])

    def initialize(self,pathControl):
        self.dataCount = 0
        self.nameControl = pathControl
        # 文件不存在
        if not os.path.exists(self.nameControl):
            return
        # 文件为空    
        if not os.path.getsize(self.nameControl):
            return
        with open(self.nameControl,'rb') as CONTROLFILE:
            lineLists = CONTROLFILE.readlines()
            self.totalExpectedRecord = 0
            self.dataCount = 0
            for i,line in enumerate(lineLists):
                fileds = re.split('\s+',line)
                if fileds.count() >= 2:
                    self.dataFile[i] = fileds[0]
                    self.fileSize[i] = 0
                    self.expect[i] = 0
                    if((fileds.count() > 1) and (re.match("[0-9]+",fileds[1]))):
                        self.fileSize[i] = int(fields[1])
                    if((fileds.count() > 2) and (re.match("[0-9]+",fileds[2]))):
                        self.expect[i] = int(fields[2])
                    self.totalExpectedRecord = self.totalExpectedRecord + self.expect[i]
                    self.dataCount = self.dataCount + 1

    def MoveTo(self,toDir):
        if not os.path.exists(toDir):
            os.makedirs(toDir)
        if not os.path.exists(self.nameControl):
            return
        for i in range(self.dataCount):
            fileName1 = os.path.dirname(self.nameControl) + '/' + self.dataFile[i]
            fileName2 = toDir + '/' + self.dataFile[i]
            if os.path.exists(fileName2):
               os.unlink(fileName2)
            os.rename(fileName1,fileName2)
        controlNewFile = toDir + '/' + os.path.basename(self.nameControl)
        if(os.path.exists(controlNewFile)):
            os.unlink(controlNewFile)
        os.rename(self.nameControl,controlNewFile)
                            
    def CheckDataFileSize(self):
        for i in range(self.dataCount):
            fileName = os.path.dirname(self.nameControl)+'/'+self.dataFile[i]
            if(os.path.getsize(fileName) != self.fileSize[i]):
                return False
        return True    

    def MoveToday(self,toDir):
        MoveTo(toDir+'/'+ETL.today)

    def updateFileLocation(self,con,loc,sys,job,sess=None):
        for i in range(self.dataCount):
            if not sess:
                sqlText = "UPDATE ETL_Received_File SET Location = '" + loc + \
                "' WHERE ETL_System = '" + sys + "' AND ETL_Job = '" + job + \
                "'   AND ReceivedFile = '" + self.dataFile[i] + "'"
            else：
                sqlText =  "UPDATE ETL_Received_File SET JobSessionID = " + sess + "+1, Location = '" + loc + \ 
                "' WHERE ETL_System = '" + sys + "' AND ETL_Job = '" + job + \
                "' AND ReceivedFile = '" + self.dataFile[i] + "'"
            try:
                cursor = con.cursor()
                cursor.execute(sqlText)
                con.commit()
            except oracle.DatabaseError as e:
                logging.error("Oracle-Error-Message: "+str(e.message))
                cursor.close()
                con.close()
            finally:
                cursor.close()
                con.close()
          
    def CheckDataFileDuplicate(self,con,sysName,jobName):
        ret = True
        if(self.dataCount == 0):
            return ret
        for i in range(self.dataCount):
            sqlText = "SELECT ReceivedFile FROM ETL_Received_File WHERE ETL_System = '" + sysName + \
            "' AND ETL_Job = '" + jobName + "' AND ReceivedFile = '" + self.dataFile[i] + "'"
            cursor = con.cursor()
            try:
                cursor.execute(sqlText)
                result = cursor.fetchall()
                if result:
                    logging.info("The source file '%s' is already received!" % self.dataFile[i])
                    ret = False
                    eventDesc = "[" + sysName + "], [" + jobName + "] has received duplicate file " + self.dataFile[i]
                    etl.InsertEventLog(con, "RCV", "M", eventDesc)    
            except oracle.DatabaseError as e:
                logging.error("Oracle-Error-Message: "+str(e.message))
                cursor.close()
                con.close()
            finally:
                cursor.close()
                con.close()
        return ret                                                  

if __name__ == '__main__':
    ## 对象构造时必须指定参数名
    cc1 = ContentControl(dir='/home/FSICBC',name='liuhaoran.ctrl1')
    cc2 = ContentControl(pathControl='/home/FSICBC/liuhaoran.ctrl2')         
