import os
import sys
import datetime
import time
#import cx_Oracle

class ETL:
    Version = "2.7.1_01"
    Auto_home = "/ETL"
    Auto_server = "ETL1"
    Auto_url = "jdbc:teradata://153.65.143.251/CLIENT_CHARSET=cp936"
    Auto_dsn = ""
    UserName = "etl"
    UserPass = "etl"
    Auto_db = "etl"
    today = ""
    event_count = 0
    jobEvent = {}
    AutoMaxJobCount = 0
    AutoSleep = 0
    isPrimaryServer = False
    cleanHour = 4
    startDateTime = ""
    msgCount = 0
    #Properties cfgVar
    #Connection lockCon
    firstCall = True
    serviceHold = 0

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
        event_count = 0 if event_count>999 else event_count+1
        #String eventId = String.format("%1$tY%1$tm%1$td%1$tH%1$tM%1$tS%2$s%3$03d", new Object[] { c, prg, Integer.valueOf(event_count) });
        eventId = time.strftime('%Y%m%d%H%M%S',time.localtime(time.time()))+prg+"%03d"%(event_count)
        strCurrentTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        rows = [(eventId,severity,desc,strCurrentTime)]
        sqlText = "insert into ETL_Event (EventID, EventStatus, Severity, Description, LogTime, CloseTime) values (:1,'0',:2,:3,:4,null)"
        try:
            cursor = con.cursor()
            cursor.executemany(sqlText,rows)
            con.commit
        except DatabaseError as error:
            print "Oracle-Error-Code:", error.code
            print "Oracle-Error-Message:", error.message
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

        


            

        





        





if __name__== '__main__':
    print ETL.Version
    etl = ETL()
    print etl.isOKDate('20170344')
