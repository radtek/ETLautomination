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

            


                            
               




