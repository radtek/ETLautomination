import os
import sys
import re
import time
import datetime
import cx_Oracle as oracle
import logging
from ETL import ETL

logging.basicConfig(level=logging.INFO)

class JobStepInfo(object):
    logName = None
    sys = None
    job = None
    txDate = None
    sessionId = None
    Step_No = None
    Step_type = None
    OSProgram = None
    WorkDir = None
    ScriptName = None
    ScriptPath = None
    AdditionParameters = None
    exeCommand = None
    logContentID = None
    sTime = None

    etl = ETL.ETL()

    def StepCommand(self,props):
        cmds = None
        if len(self.ScriptName) > 0:
            cmds = self.OSProgram + " " + self.ScriptPath + "/" + self.ScriptName + " $CTLFILE " + self.AdditionParameters
        else:
            cmds = self.OSProgram + " " + self.AdditionParameters

        return etl.putVarsToCommand(props,cmds)

    def updateRecordLog(self,con,retLog):
        if not self.Step_type == 'L':
            return
        num_ins = 0
        num_dup = 0
        num_upd = 0
        num_del = 0
        num_out = 0
        num_er1 = 0
        num_er2 = 0
        num_et = 0
        num_uv = 0
        loadTypeFind = True
        loadTool = ""

        pos = 0
        breakFlag = False
        while(pos < len(retLog)):
            endLine = retLog.find("\n",pos)
            if endLine == -1:
                break
            line = retLog[pos,endLine]
            pos = endLine + 1
            if loadTypeFind:
                if line.find("UTILITY") > 0 or line.find("Utility") > 0:
                    w = line.split(" +")
                    if len(w) <= 4 or not w[3] == "UTILITY":
                        continue
                    loadTool = w[2]
                    if not loadTool == "FASTLOAD" and not loadTool == "MultiLoad":
                        continue
                    loadTypeFind = False
                    continue
                if line.startwith("     =              LOAD OPERATOR"):
                    loadTool = "TPT-LOADOP"
                    loadTypeFind = false

            if loadTool == "FASTLOAD":
                if line.find("Total Inserts Applied") > 0:
                    num_ins = int(line.split(" +")[5])
                elif line.find("Total Duplicate Rows") > 0:
                    num_dup = int(line.split(" +")[5])
                elif line.find("Total Error Table 1") > 0:
                    num_er1 = int(line.split(" +")[6])
                elif line.find("Total Error Table 2") > 0:
                    num_er2 = int(line.split(" +")[6])        
            elif loadTool == "MultiLoad":
                if line[0:16] == "        Inserts:":
                    num_ins = int(line.split(" +")[2]) 
                elif line[0:16] == "        Updates:":
                    num_upd = int(line.split(" +")[2])
                elif line[0:16] == "        Deletes:":
                    num_del = int(line.split(" +")[2])
                elif line[0:37] == "     Number of Rows  Error Table Name":
                    LineError = retLog[pos:].split("\n")
                    num_et = int(LineError[1].split(" +")[1])
                    num_uv = int(LineError[2].split(" +")[1])
            elif loadTool == "TPT-LOADOP":
                if line.startwith("              Total Rows Applied:")：
                    num_ins = int(line.split(" +")[4])
                elif line.startsWith("              Total Possible Duplicate Rows:"):
                    num_dup = int(line.split(" +")[5])
                elif line == "     Number of rows  Error Table Name":
                    endLine = retLog.find("\n",pos)
                    if endLine == -1:
                        breakFlag = True
                        break
                    pos = endLine + 1
                    endLine = retLog.find("\n", pos)                   
                    if endLine == -1:
                        breakFlag = True
                        break
                    line = retLog[pos:endLine]
                    pos = endLine + 1
                    num_er1 = int(line.split(" +")[1])
                    endLine = retLog.find("\n", pos)
                    if endLine == -1:
                        breakFlag = True
                        break
                    line = retLog[pos:endLine]
                    pos = endLine + 1
                    num_er2 = int(line.split(" +")[1])

        if ((num_dup > 0) or (num_et > 0) or (num_uv > 0) or (num_er1 > 0) or (num_er2 > 0)) or breakFlag == True:
            content = "Duplicate Record = " + num_dup + "\n" + \
            "Multiload ET record = " + num_et + "\n" + \
            "Multiload UV record = " + num_uv + "\n" + \
            "Fastload ER1 record = " + num_er1 + "\n" + \
            "Fastload ER2 record = " + num_er2 + "\n"
            desc = "Job [" + self.sys + "," + self.job + "] has error record"
            etl.WriteMessageNotification(self.sys, self.job, self.txDate, "RecordError", desc, content)

        sqlText = "DELETE FROM ETL_Record_Log WHERE ETL_System ='" + self.sys + "' AND ETL_Job = '" + self.job + \
        "' AND JobSessionID = " + self.sessionId 
        sqlText2 =  " INSERT INTO ETL_Record_Log (ETL_System, ETL_Job, JobSessionID, RecordTime, " + \
        " InsertedRecord, UpdatedRecord, DeletedRecord,DuplicateRecord, OutputRecord, ETRecord, UVRecord," + \
        " ER1Record, ER2Record)" + \
        " VALUES ('" + self.sys + "', '" + self.job + "', " + self.sessionId + ", '" + etl.GetDateTime() + "', " + \
        num_ins + ", " + num_upd + ", " + num_del + ", " + num_dup + ", " + num_out + ", " + \
        num_et + ", " + num_uv + ", " + num_er1 + ", " + num_er2 + ")"
        try:
            cursor = con.cursor()
            cursor.execute(sqlText)
            cursor.execute(sqlText2)
            con.commit()
        except oracle.DatabaseError as error:
            logging.error("Oracle-Error-Message: "+str(error.message)) 
            cursor.close()
            con.close()
        finally:
            cursor.close()
            con.close()
    
    def updateStepRunning(self,con,s):
        setStartT = None
        sqlText1 = None
        if s == 0:
            setStartT = " ,Last_StartTime='" + etl.GetDateTime() + "', Last_EndTime=null, " + \
            "Last_JobStatus='Running', Last_TXDate = CAST('" + self.txDate + "' AS DATE FORMAT 'YYYYMMDD')"
            sqlText1 = "UPDATE ETL_Job_GroupChild SET CheckFlag = 'N' WHERE ETL_System = '" + self.sys + "' AND ETL_Job = '" + self.job
        sqlText2 = "Update ETL_job SET RunningScript='" + self.ScriptName + "'" + setStartT + " WHERE ETL_System='" + self.sys + \
        "' AND ETL_Job='" + self.job
        self.sTime = etl.GetDateTime()
        sqlText3 = "Insert INTO ETL_Job_Log(ETL_System, ETL_Job, JobSessionID, ScriptFile, TxDate, StartTime, Step_No) "+\
        " select ETL_System, ETL_Job, JobSessionID, RunningScript, Last_TXDate, '" + self.sTime + "', " + self.Step_No + \
        " FROM ETL_Job WHERE ETL_System='" + self.sys + "' AND ETL_Job='" + self.job + "'"
        try:
            cursor = con.cursor()
            if sqlText:
                cursor.execute(sqlText)
            cursor.execute(sqlText2)
            cursor.execute(sqlText3)
            con.commit()
        except oracle.DatabaseError as error:
            logging.error("Oracle-Error-Message: "+str(error.message)) 
            cursor.close()
            con.close()
        finally:
            cursor.close()
            con.close()

    def updateStepFinal(self,con,retCode,eTime,logContent):
        sqlText = "Update ETL_Job_Log SET EndTime='" + eTime + "', ReturnCode=" + retCode + \
        ", LogContent = :blobData WHERE ETL_System='" + self.sys + \
        "' AND ETL_Job='" + self.job + "' AND JobSessionID=" + self.sessionId + " AND StartTime='" + self.sTime + \
        "' AND Step_No=" + self.Step_No
        ## blob 类型的写入操作
        try:
            cursor = con.cursor()
            cursor.setinputsizes(blobData=cx_Oracle.BLOB)
            cursor.execute(sqlText, {'blobData':logContent})
            con.commit()
        except oracle.DatabaseError as error:
            logging.error("Oracle-Error-Message: "+str(error.message)) 
            cursor.close()
            con.close()
        finally:
            cursor.close()
            con.close()

    def getStep(self,con,sys,job,prop):
        stepList = []
        sqlText = "select JobType, JobSessionID FROM ETL_Job WHERE ETL_System = '" + sys + "' AND ETL_Job = '" + job + "'"
        try:
            cursor = con.cursor()
            cursor.execute(sqlText)
            result = cursor.fetchone() 
        except oracle.DatabaseError as e:
            logging.error("Oracle-Error-Message: "+str(error.message)) 
            cursor.close()
            con.close()
        finally:
            cursor.close()
            con.close()

        jobType = ""
        sessionId = 0
        txDate = etl.getConfig(prop).get("ETL","TXDATE")  
        if result:
            jobType = result[0]
            sessionId = result[1]
        if jobType == "V":
            s = JobStepInfo()
            s.sys = sys
            s.job = job
            s.txDate = txDate    
            s.sessionId = sessionId
            s.OSProgram = "VirtualPGM"
            s.ScriptName = "Virtual-Script"
            s.exeCommand = "V"
            s.Step_type = "V"
            stepList.extend(s)
            return stepList

        sqlText = "SELECT Step_No, Step_type, OSProgram, WorkDir, ScriptName, ScriptPath, AdditionParameters FROM ETL_Job_Step " + \
        "WHERE Enable = '1' AND ETL_System = '" + sys + "' AND ETL_Job = '" + job + "' ORDER BY Step_NO"
        try:
            cursor = con.cursor()
            cursor.execute(sqlText)
            result = cursor.fetchall() 
        except oracle.DatabaseError as e:
            logging.error("Oracle-Error-Message: "+str(error.message)) 
            cursor.close()
            con.close()
        finally:
            cursor.close()
            con.close()

        if result:
            for listInfo in result:
                s = JobStepInfo()
                s.sys = sys
                s.job = job
                s.txDate = txDate
                s.sessionId = sessionId
                s.Step_No = listInfo[0]
                s.Step_type = listInfo[1]
                s.OSProgram = listInfo[2]
                if listInfo[3]:
                    s.WorkDir = listInfo[3]
                else:
                    s.WorkDir = ""
                if len(s.WorkDir) == 0:
                    s.WorkDir = "$AUTO_HOME/DATA/process"
                if listInfo[4]:
                    s.ScriptName = listInfo[4]
                else:
                    s.ScriptName = ""
                if listInfo[5]:
                    s.ScriptPath = listInfo[5]
                else:
                    s.ScriptPath = ""
                if len(s.ScriptPath):
                    s.ScriptPath = "$AUTO_HOME/APP/" + sys + "/" + job + "/bin"
                if listInfo[6]:
                    s.AdditionParameters = listInfo[6]
                else:
                    s.AdditionParameters = ""
                s.exeCommand = s.StepCommand(prop)
                s.WorkDir = etl.putVarsToCommand(prop, s.WorkDir)
                stepList.extend(s)
        return stepList

if __name__ == '__main__':
    pass                       




               




