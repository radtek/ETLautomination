import os
import sys
import re
import datetime
import time
import ConfigParser
import logging
import cx_Oracle as oracle
import itertools

logging.basicConfig(level=logging.INFO)

class ETL:
    Version = "2.7.1_01"
    Auto_home = "/ETL"
    Auto_server = "ETL1"
    Auto_url = "etl/etl@172.17.0.120/orcl"
    Auto_dsn = ""
    UserName = "etl"
    UserPass = "etl"
    Auto_db = "etl"
    Auto_ip = "xxx.xxx.xxx.xxx"
    today = ""
    event_count = 0
    jobEvent = {}
    AutoMaxJobCount = 0
    AutoSleep = 0
    isPrimaryServer = False
    cleanHour = 4
    startDateTime = ""
    msgCount = itertools.count(0)
    cfgFile = "/home/FSICBC/liuhr/ETLAuto/etl.cfg"
    #Properties cfgVar
    #Connection lockCon
    firstCall = True
    serviceHold = 0

    def getConfig(self,cfg=cfgFile):
        config = ConfigParser.SafeConfigParser()
        try:
            config.read(cfg)
        except ConfigParser.Error as e:
            logging.error("!!! The initial confgiure file can not be opened. !!!")
            sys.exit(1)
        return config      

    def isOKDate(self,txDate):
        if(len(txDate) < 8):
            return False
        try:
            year = int(txDate[0:4])
            month = int(txDate[4:6])
            day = int(txDate[6:8])
            if((year < 1) or (month < 1) or (day < 1) or (month > 12) or (day > 31)):
                return False
            if(((day < 29) and (month == 2))): 
                return True
            if((day < 31) and (month != 2)):
                return True
            if((day == 31) and (((month == 1) or (month == 3) or (month == 5) or (month == 7) or (month == 8) or (month == 10) or (month == 12)))):
                return True                    
            return (month == 2) and (day == 29) and (((year % 400 == 0) or ((year % 4 == 0) or (year % 100 != 0))))
        except Exception as e:
            raise e
        return False

    def InsertEventLog(self,con,prg,severity,desc):
        self.event_count = 0 if self.event_count>999 else self.event_count+1
        #String eventId = String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS%2$s%3$03d", new Object[] { c, prg, Integer.valueOf(event_count) });
        eventId = time.strftime('%Y%m%d%H%M%S',time.localtime(time.time()))+prg+"%03d"%(self.event_count)
        strCurrentTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        rows = [(eventId,severity,desc,strCurrentTime)]
        sqlText = "insert into ETL_Event (EventID, EventStatus, Severity, Description, LogTime, CloseTime) values (:1,'0',:2,:3,:4,null)"
        try:
            cursor = con.cursor()
            cursor.executemany(sqlText,rows)
            con.commit()
        except oracle.DatabaseError as error:
            logging.error("Oracle-Error-Message: "+str(error.message)) 
            cursor.close()
            con.close()
            return False
        finally:
            cursor.close()
            con.close()
        return True
    
    def Sleep(self,second):
        pass

    def FromTxDate(self,txdate):
        year = int(txDate[0:4])
        month = int(txDate[4:6])
        date = int(txDate[6:8])
        return datetime.datetime(year,month-1,date)

    def GetToday(self):
        return time.strftime('%Y%m%d',time.localtime(time.time()))

    def GetDateTime(self):
        return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))

    def putVarsToCommand(self,props,cmds):
        pos1 = 0
        pos=cmds.index('$',pos1)
        while(pos >= 0):
            pos2 = pos
            encloser = False
            pos += 1
            if(cmds[pos] == '$'):
                encloser = True
                pos += 1
            varNameLen = 0
            while((pos + varNameLen) < len(cmds)):
                varNameLen += 1
                #if (!cmds.substring(pos + varNameLen, pos + varNameLen + 1).matches("[a-zA-Z0-9_]")) break;
                if(re.match('[a-zA-Z0-9_]',cmds[pos + varNameLen,pos + varNameLen+1])):
                    break
            varName =  cmds[pos,pos + varNameLen]
            val = props.get('Properties',varName)
            pos += (1+varNameLen if encloser else varNameLen)
            cmds = cmds[0:pos2] + val +cmds[pos:]
            pos1 = pos2 + len(val)
            pos=cmds.index('$',pos1)
        return cmds
            
    def Initialize(self,cfg=cfgFile):
        startDateTime = time.strftime('%Y%m%d%H%M%S',time.localtime(time.time()))
        #cfgInit = cfg
        config = self.getConfig(cfg)

        if(not config.has_section('ETL')):
            logging.info("!!! The initial confgiure file has not setion \[ETL\]. !!!")
        else:
            if(config.has_option('ETL','AUTO_HOME')):
                self.Auto_home = config.get('ETL','AUTO_HOME')
            else:
                self.Auto_home = '/ETL'
            if(config.has_option('ETL','AUTO_SERVER')):
                self.Auto_server = config.get('ETL','AUTO_SERVER')
            else:
                self.Auto_server = 'ETL1'
            if(config.has_option('ETL','AUTO_URL')):
                self.Auto_url = config.get('ETL','AUTO_URL')
            else:
                self.Auto_url = 'jdbc:teradata://127.0.0.1/CLIENT_CHARSET=cp936'
            if(config.has_option('ETL','AUTO_DB')):
                self.Auto_db = config.get('ETL','AUTO_DB')
            else:
                self.Auto_db = 'etl'
            if(config.has_option('ETL','AUTO_DSN')):
                self.Auto_dsn = config.get('ETL','AUTO_DSN')
            else:
                self.Auto_dsn = ''
            if(config.has_option('ETL','AUTO_JOB_COUNT')):
                self.AutoMaxJobCount = config.getint('ETL','AUTO_JOB_COUNT')
            else:
                self.AutoMaxJobCount = 10
            if(config.has_option('ETL','AUTO_SLEEP')):
                self.AutoSleep = config.getint('ETL','AUTO_SLEEP')
            else:
                self.AutoSleep = 30
            if(config.has_option('ETL','AUTO_CLEAN_HOUR')):
                self.cleanHour = config.getint('ETL','AUTO_CLEAN_HOUR')
            else:
                self.cleanHour = 4
            if(config.has_option('ETL','UserName')):
                self.UserName = config.get('ETL','UserName')
            else:
                self.UserName = 'etl'
            if(config.has_option('ETL','UserPass')):
                self.UserPass = config.get('ETL','UserPass')
            else:
                self.UserPass = 'etl'
           

    def HearBeat(self):
        pass

    def ShowTime(self):
        logging.info('['+time.strftime('%H:%M:%S',time.localtime(time.time()))+']')

    def IsJobAlreadyHasEvent(self,job,eventDesc):
        if(self.jobEvent.has_key(job) and str(self.jobEvent.get(job))== eventDesc):
            return True
        self.jobEvent[job] = eventDesc
        return False

    def RemoveJobEventRecord(self,job):
        self.jobEvent.pop(job)

    def ShowPrefixSpace(self):
        logging.info("             ")

    def PrintVersionInfo(self,servAuto):
        logging.info("*******************************************************************") 
        logging.info("* ETL Automation " + servAuto + " Program " + "2.7.1_01" + " ,slim 2017 Copyright. *")
        logging.info("*******************************************************************")

    def Connect(self):
        if not self.Auto_url:
            self.Auto_url = self.UserName + '/' +  self.UserPass + '@' + self.Auto_ip + self.Auto_dsn
        try:
            logging.info("Auto_dsn is: "+self.Auto_url)
            con = oracle.connect(self.Auto_url)
        except oracle.Error as e:
            logging.error("Connect Error:"+str(e.message))
            return None            
        return con

    def ping(self,con):
        if not con:
            return False
        sqlText = "select 1 from dual"
        try:
            cursor = con.cursor()
            cursor.execute(sqlText)
            result = cursor.fetchone()
            if not result[0]:
                return False
            return True    
        except oracle.DatabaseError as e:
            logging.error("Database Error Message: "+str(e.message))
            return False
    
    def WriteMessageNotification(self,sys,job,txdate,type,subject,content,logName=None):
        msgId = self.msgCount.next()
        msgFileName = self.Auto_home + "/DATA/message/" + self.startDateTime + "_" + msgId + ".msg"
        with open(msgFileName,'a') as msg:
            msg.write("Automation Message Notification")
            msg.write("SYSTEM: %s\nJOB: %s\nTXDATE: %s\nTYPE: %s\n") % (sys,job,txdate,type)
            if not logName:
                msg.write("ATT:\nSUBJECT: %s\nCONTENT: %s\n") % (subject,content)
            else:    
                msg.write("ATT: %s\nSUBJECT: %s\nCONTENT: %s\n") % (logName,subject,content)


    def WriteMessageNotification(self,sys,job,txdate,type,subject,content):
        pass

    def getAgentPort(self,con,serverName):
        sqlText = "SELECT  AgentPort, isPrimary FROM ETL_Server WHERE ETL_Server = '" + serverName +"'"
        try:
            cursor = con.cursor()
            cursor.execute(sqlText)
            result = cursor.fetchone()
            if not result[0]:
                serverPort = 0
            serverPort = result[0]
            yn = result[1]
            if(yn == 'Y'):
                self.isPrimaryServer = True   
        except oracle.DatabaseError as e:
            logging.error("Database Error Message: "+str(e.message))
            serverPort = 0
        finally:    
            return serverPort
    
    def getLogContentText(self,con,logName):
        textContent = ""
        with open(logName,'r') as log:
            while(True):
                line = log.readline()
                if not line:
                    break
                textContent = textContent + "\n" + line
        return textContent            

if __name__== '__main__':
    logging.info(ETL.Version)
    etl = ETL()
    etl.Initialize()
    print etl.Auto_home
    #print ETL.isOKDate('20170309') 
    #etl = ETL()
    #etl.Initialize()
    #logging.info(etl.Auto_server)
    #logging.info(etl.isOKDate('20170344'))
    #cfgFile = 'D:\GitHub\ETL_automination\etlAuto\etl\etl.cfg'
    #etl.Initialize(cfgFile)
    #print etl.AutoMaxJobCount
    #print etl.AutoSleep
    #print etl.Auto_db
    #print etl.Auto_dsn
    #print etl.Auto_home
    #logging.info(etl.ShowTime())
    #logging.info(etl.PrintVersionInfo('ETL1'))
    #con = etl.Connect()
    #logging.info(etl.ping(con))

