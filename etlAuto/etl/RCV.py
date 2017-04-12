# -*- coding: utf-8 -*-
import os
import sys
import re
import datetime
import time
import shutil
import logging
from glob import glob
import cx_Oracle as oracle
from ETL import ETL
from ControlFilter import ControlFilter
from ContentControl import ContentControl
from JobInfo import JobInfo


logging.basicConfig(level=logging.INFO)

class RCV(object):
	conn = None
	rcvDir = ''
	dberr = False
	StableCheckSize = None

	def RefreshSizeStable(self):
		for file in self.StableCheckSize:
			if os.path.exists(file):
				continue
			self.StableCheckSize.pop(file,'')
	
	def IsSizeStable(self,fileName):
		fileLen = os.path.getsize(fileName)
		oldLen = self.StableCheckSize.get(fileName)
		if oldLen is None :
			self.StableCheckSize[fileName] = fileLen
			return False
		if oldLen < fileLen:
			self.StableCheckSize[fileName] = fileLen
			return False
		return True
	
	def GetSrc(self,controlFile):
		return controlFile[4:len(controlFile)-8]

	def GetTXDate(self,controlFile):
		return controlFile[len(controlFile)-8:]

	def CheckReceiveDir(self,conn):
		rcvDir = ETL.Auto_home + "/DATA/receive"
		dberr = False
		self.conn = conn

		logging.info(ETL.ShowTime())
		logging.info("Checking receiving directory '" + rcvDir + "...")

		fns = glob(rcvDir+ControlFilter(1).getPattern())
		if fns is None:
			logging.info("The Path is not exist.")
			return
		RefreshSizeStable()
		controlList = [None] * len(fns)
		lstLen = 0
		j = 0
		for i in range(0,len(lstLen)):
			if not IsSizeStable(fns[i]):
				continue
			source = GetSrc(os.path.basename(fns[i]))
			for j in range(j,lstLen):
				if self.source == GetSrc(controlList[j]):
					break
			if j == lstLen:				
				controlList[lstLen] = os.path.basename(fns[i])
				lstLen = lstLen + 1
			else:
				tx1 = GetSrc(controlList[j])
				if cmp(GetTXDate(os.path.basename(fns[i])),tx1) < 0:
					controlList[j] = os.path.basename(fns[i])

		if lstLen == 0:
			return

		## 这里需要实现对 fns 列表的排序操作，暂时先不实现
		for i in range(0,lstLen):
			if ProcessControlFile(controlList[i]):
				return

	def CheckRelatedJobStatus(self,job,txDate):
		ret = True
		logging.info(ETL.ShowPrefixSpace())
		logging.info("Check related job status...")
		sqlText = """
		select  B.ETL_System, B.ETL_Job, A.CheckMode, Last_JobStatus, Last_TXDate, JobType, CheckCalendar
		,to_date('%s','YYYYMMDD')-Last_TXDate as dayDiff
		from ETL_RelatedJob A, ETL_JOB B
		WHERE A.etl_system = '%s' AND A.etl_job='%s'
		AND A.RelatedSystem = B.ETL_System  AND A.RelatedJob = B.ETL_Job
		""" %(txDate,job.sysName,job.jobName)

		try:
			relCalCheckJobs = ''
            cursor = self.conn.cursor()
            cursor.execute(sqlText)
            result = cursor.fetchall()
            for resTuple in result:
                sys = resTuple[0]
                reljob = resTuple[1]
                chkMode = resTuple[2]
                status = resTuple[3]
                lastTxdate = resTuple[4].replace("-","")
                jobType = resTuple[5]
                checkCal = resTuple[6]
                dayDiff = resTuple[7] if not resTuple[7] else 0
                if status.lower() != "done":
                	ret = False
                	logging.info(ETL.ShowPrefixSpace())
                	logging.info("Related job " + sys + "," + reljob + " is not Done. Current is " + status)
                elif checkCal.lower() == "y":
                	relCalCheckJobs = relCalCheckJobs + "\t" + sys + "," + reljob + "," + chkMode + "," + lastTxdate
                elif jobType.lower() == 'd':
                	logging.info(ETL.ShowPrefixSpace())
                	logging.info("Related job [" + sys + "," + reljob + "] is daily job.")	
                	if dayDiff <= 1:
                		logging.info("Related daily job date offset is in range. txDate=" + lastTxdate)
                	else:
                		ret = False
                		logging.info("Related daily job date offset is over range. txDate=" + lastTxdate)
                elif jobType.lower() == 'w':
                	logging.info(ETL.ShowPrefixSpace())
                	logging.info("Related job [" + sys + "," + reljob + "] is weekly job.")	
                	if dayDiff <= 7:
                		logging.info("Related daily job date offset is in range. txDate=" + lastTxdate)
                	else:
                		ret = False
                		logging.info("Related daily job date offset is over range. txDate=" + lastTxdate)
                elif jobType.lower() == 'm':
                	logging.info(ETL.ShowPrefixSpace())
                	logging.info("Related job [" + sys + "," + reljob + "] is Monthly job.")
                	last = int(lastTxdate[0:6])
                	txd = int(txDate[0:6])
                	diffMon = int(txd / 100 * 12 + txd % 100 - last / 100 * 12 - last % 100)
                	if diffMon <= 1:
                		logging.info("Related Monthly job date offset is in range.")
                	else:
                		ret = False
                		logging.info("Related Monthly job date offset is over range.") 

            if ret and len(relCalCheckJobs) > 0:
            	logging.info(ETL.ShowPrefixSpace())
            	logging.info("Check related job by data calendar")
            	relJobs = relCalCheckJobs.split("\t")

            	for k in range(1,len(relJobs)-1):
            		rel = relJobs[k].split(",")
            		cmp = "<" if rel[2]=="1" else "<="
            		logging.info(ETL.ShowPrefixSpace())
            		logging.info("Get prior calendar date, %s, %s, %s, %s" %(rel[0],rel[1],rel[2],txDate))
            		sqlText = """
            		select CalendarYear*10000 + CalendarMonth*100 + CalendarDay dt 
            		FROM  DataCalendar WHERE ETL_system = '%s' AND ETL_job='%s'   AND dt %s %s ORDER BY dt DESC
            		""" % (rel[0],rel[1],cmp,txDate)
            		cursor.execute(sqlText)            		
            		result = cursor.fetchone()
            		priorDate = "0000-00-00"
            		if not result:
            			priorDate = result[0]
            		logging.info(ETL.ShowPrefixSpace())
            		logging.info("Related job date should be " + priorDate)
            		if priorDate == rel[3] or (priorDate == "0000-00-00"):
            			continue
            		logging.info(ETL.ShowPrefixSpace())
            		logging.info("    But date is " + rel[3])
            		ret = False
        except oracle.DatabaseError as e:
            logging.error("Database Error Message: "+str(e.message))
            self.dberr = True

		logging.info(ETL.ShowPrefixSpace())
		if ret:
			logging.info("Check related job status ok.")
		else:
			logging.info("Check related job status fail.")

		return ret

	def ProcessControlFile(self,controlFile):
		logging.info(ETL.ShowTime())
		logging.info("Processing control file '" + controlFile + "'...")
		jobSource = GetSrc(controlFile)
		txDate = GetTXDate(controlFile)
		job = JobInfo(self.conn,jobSource)
		logging.info(ETL.ShowPrefixSpace())
		logging.info("System='" + job.sysName + "', Job='" + job.jobName + "', ConvHead='" + job.convHead + "' TXDATE=" + txDate)
		if self.dberr:
			return True

		cont = ContentControl(dir=rcvDir,name=controlFile)
		if job.sysName is None 	or (not ETL.isOKDate(txDate)):
			logging.info(ETL.ShowTime())
			logging.info("Unknown control file '" + controlFile + "'")
			desc = "Unknown control file " + controlFile
			ETL.InsertEventLog(self.conn, "RCV", "L", desc)	
			cont.MoveToday(ETL.Auto_home + "/DATA/fail/unknown")
			return False
		
		if UpdateSourceLastCount(jobSource):
			return True

		if not cont.CheckDataFileSize():
			cont.MoveToday(ETL.Auto_home + "/DATA/fail/corrupt/" + job.sysName)
			eventDesc = "[" + job.sysName + "," + job.jobName + " has corrupt data in control file " + controlFile
			ETL.InsertEventLog(self.conn, "RCV", "H", eventDesc)
			logging.info(ETL.ShowTime())
			logging.info("Generate data file corrupt message...")
			ETL.WriteMessageNotification(job.sysName, job.jobName, txDate,"Receiving","Job [" + job.sysName + "," + job.jobName + "] has corrupt data in control file " + controlFile, "")
			return False

		if not job.enable == "1":
			logging.info(ETL.ShowPrefixSpace())
			logging.info("WARNING - The job is not enabled, we will process this file next time.")
			return False

		if ((not job.checkLastStatus.lower() == 'n') or (not job.jobStatus.lower() == 'falied')) and (not job.jobStatus.lower() == 'done') and (not job.jobStatus.lower() == 'ready') :
			logging.info(ETL.ShowPrefixSpace())
			logging.info("WARNING - The job is in " + job.jobStatus + ", we will process this file next time.")
			if ETL.IsJobAlreadyHasEvent(job.sysName + "_" + job.jobName, "Status Mismatch"):
				return False
			eventDesc = "[" + job.sysName + ", [" + job.jobName + "] still in <" + job.jobStatus + " but has received another file " + controlFile	
			ETL.InsertEventLog(self.conn, "RCV", "M", eventDesc)
			return False

		logging.info(ETL.ShowPrefixSpace())
		logging.info("Check job frequency " + job.sysName + ", " + job.jobName)

		if not CheckJobFrequency(job.frequency, txDate):
			cont.MoveToday(ETL.Auto_home + "/DATA/fail/bypass/" + job.sysName)
			eventDesc = "[" + job.sysName + "," + job.jobName + "] has received a control file " + controlFile + " did not match the frequency"
			ETL.InsertEventLog(con, "RCV", "L", eventDesc)
			ETL.RemoveJobEventRecord(job.sysName + "_" + job.jobName)
			logging.info(ETL.ShowTime())
			logging.info("Generate data file frequency did not match message...")
			ETL.WriteMessageNotification(job.sysName,job,jobName,txDate,"Receiving",\
				"Job ["+job.sysName+","+job.jobName+"] has received a control file "+controlFile+" did not match the frequency"\
				,"")
			return False

		if job.checkCalendar.lower() == 'y':
			logging.info(ETL.ShowPrefixSpace())
			logging.info("Check data calendar " + job.sysName + ", " + job.jobName + " TXDATE=" + txDate)
			if not CheckDataCalendar(job,txDate):
				logging.info(ETL.ShowPrefixSpace())
				logging.info("Data-calendar mismatch.")
				cont.MoveToday(ETL.Auto_home + "/DATA/fail/bypass/" + job.sysName)
				ETL.RemoveJobEventRecord(job.sysName + "_" + job.jobName)
				eventDesc = "[" + job.sysName + "," + job.jobName + "] has received a control file " + controlFile + " did not match the data-calendar"
				ETL.InsertEventLog(self.conn, "RCV", "L", eventDesc)

				logging.info(ETL.ShowTime())
				logging.info("Generate data file did not match data-calendar message...")
				ETL.WriteMessageNotification(job.sysName, job.jobName, txDate,"Receiving",\
				"Job ["+job.sysName+","+job.jobName+"] has received a control file "+controlFile+" did not match the data-calendar","")
				return False
			logging.info(ETL.ShowPrefixSpace())
			logging.info("Check data calendar OK.")

		if not CheckRelatedJobStatus(job, txDate):
			logging.info(ETL.ShowTime())
			logging.info("Check job related job failed!")
			if ETL.IsJobAlreadyHasEvent(job.sysName + "_" + job.jobName, "Related Job"):
				return False
			eventDesc = "[" + job.sysName + "," + job.jobName + "] has related job did not finish yet, wait to next time"
			ETL.InsertEventLog(self.conn, "RCV", "L", eventDesc)
			return False

		if cmp(txDate,job.txDate) < 0:
			cont.MoveToday(ETL.Auto_home + "/DATA/fail/bypass/" + job.sysName)
			logging.info(ETL.ShowPrefixSpace())
			logging.info("ERROR - The TxDate '" + txDate + "' is less then job current TxDate.")
			eventDesc = "[" + job.sysName + "," + job.jobName + "] has received a control file, whose txdate is less then the current txdate"
			ETL.InsertEventLog(self.conn, "RCV", "L", eventDesc)
			ETL.RemoveJobEventRecord(job.sysName + "_" + job.jobName)
			logging.info(ETL.ShowTime())
			logging.info("Generate data file has less than current TxDate message...")
			ETL.WriteMessageNotification(job.sysName, job.jobName, txDate,\
				"Receiving","Job "+job.sysName+","+job.jobName+" has received a control file, whose txdate is less then the current txdate","")
			return False

		if not cont.CheckDataFileDuplicate(self.con, job.sysName, job.jobName):
			cont.MoveToday(ETL.Auto_home + "/DATA/fail/duplicate/" + job.sysName)
			ETL.RemoveJobEventRecord(job.sysName + "_" + job.jobName)
			logging.info(ETL.ShowTime())
			logging.info("Generate data file duplicate message...")
			ETL.WriteMessageNotification(job.sysName, job.jobName, txDate,"Receiving"\
				,"Job [%s,%s] has received a control file %s with duplicate file"%(job.sysName, job.jobName, controlFile),"")
			return False

		if self.dberr:
			return True

		markDataDate(job,txDate)
		logging.info(ETL.ShowPrefixSpace())
		logging.info("All Checks are done")
		ConvertControlFile(job, cont, txDate)
		return self.dberr

	def markDataDate(self,job,txDate):
		if job.checkCalendar.lower() == 'y':
			sqlText = "update DataCalendar set CheckFlag = 'Y'    WHERE ETL_System = '%s' AND ETL_Job = '%s' AND CalendarYear*10000+ CalendarMonth*100 + CalendarDay = %s"%(job.sysName, job.jobName, txDate)
			try:
            	cursor = self.conn.cursor()
            	cursor.execute(sqlText)
            	self.conn.commit()
            except oracle.DatabaseError as e:
            	logging.error("Database Error Message: "+str(e.message))
            	self.dberr = True	

    def ConvertControlFile(self,job,cont,txDate):
    	rcvTime = time.strftime('%Y%m%d %H%M%S',time.localtime(time.time()))
    	baseDir = os.path.dirname(cont.nameControl)
    	insertCmd = "insert into ETL_Received_File (ETL_System, ETL_Job, JobSessionID, ReceivedFile, FileSize, ExpectedRecord, ArrivalTime, ReceivedTime, Location, Status) \
    				values ('%s', '%s', %s, '%s', %d, %d, '%s', '%s', '%s', '1')"
    	ctlFileName = ETL.Auto_home + "/tmp/%s_%s_%s.dir"%(job.sysName,job.convHead,txDate)
    	with open(ctlFileName,'w') as CTLF:
    		for i in range(0,len(cont.dataCount)):
    			file = baseDir + "/" + cont.dataFile[i]
    			ariTime = datetime.datetime.fromtimestamp(os.path.getatime(file)).strftime('%Y%m%d %H%M%S')
    			sqlText = insertCmd %(job.sysName, job.jobName, job.jobSessionID, cont.dataFile[i],cont.fileSize[i],cont.expect[i],ariTime,rcvTime,ETL.Auto_home + "/DATA/queue")
    			try:
    				cursor = self.conn.cursor()
            		cursor.execute(sqlText)
            		self.conn.commit()
    			except oracle.DatabaseError as e:
    				logging.error("Database Error Message: "+str(e.message))
    				self.dberr = True
    				logging.info(ETL.ShowPrefixSpace())
    				logging.info("Error - insert source files into ETL_Received_file failed!")
    			CTLF.write(("%s\t%d")%(cont.dataFile[i],cont.fileSize[i]))
    	
    	for i in range(0,len(cont.dataCount)):
    		dataF = baseDir + "/" + cont.dataFile[i]
    		dataQ = ETL.Auto_home + "/DATA/queue/" + cont.dataFile[i]
    		shutil.move(dataF,dataQ)

    	shutil.move(ctlFileName,ETL.Auto_home+("/DATA/queue/%s_%s_%s.dir")%(job.sysName,job.convHead,txDate))
    	logging.info(ETL.ShowPrefixSpace())
    	completeDir = ETL.Auto_home + "/DATA/complete/" + job.sysName + "/" + ETL.today
    	if not os.path.exists(completeDir):
    		os.makedirs(completeDir)
    	compF = os.path.dirname(completeDir)+"/"+os.path.basename(cont.nameControl)
    	shutil.move(cont.nameControl,compF)

    	logging.info(("Update job status to 'Pending' for %s, %s, %s")%(job.sysName,job.jobName,txDate))
    	sqlText = "update ETL_Job set Last_StartTime = '%s', Last_EndTime = null, Last_JobStatus = 'Pending',\
    			  Last_TXDate = %s-19000000, Last_FileCnt= %d, Last_CubeStatus = null, ExpectedRecord =%d \
    			  WHERE ETL_System = '%s' AND ETL_Job = '%s'" %(rcvTime,txDate,cont.dataCount,cont.totalExpectedRecord,job.sysName,job.jobName)	
    	try:
    		cursor = self.conn.cursor()
            cursor.execute(sqlText)
            self.conn.commit()
    	except oracle.DatabaseError as e:
    		logging.error("Database Error Message: "+str(e.message))
    		self.dberr = True
    		logging.info(ETL.ShowPrefixSpace())
    		logging.info("Error--Update job status failed!")		     					

    def CheckDataCalendar(self,job,txDate):
    	sqlText = "select  CalendarYear*10000 + CalendarMonth*100 + CalendarDay dt, CheckFlag \
    			   FROM DataCalendar WHERE etl_system='" + job.sysName + "' AND etl_job='" + job.jobName + "'AND dt  <=" + txDate + " ORDER BY dt DESC"
    	try:
    		cursor = self.conn.cursor()
    		cursor.execute(sqlText)
    		result = cursor.fetchall()
    		cDate = ''
    		checkFlag = 'X'
    		lFlag = ' '
    		if result[0] is not None:
    			cDate = result[0][0]
    			checkFlag = result[0][1]
    		if result[1] is not None:
    			lFlag = result[1][1]    				
    	except oracle.DatabaseError as e:
            logging.error("Database Error Message: "+str(e.message))

         if len(cDate) == 0:
         	if (job.jobStatus.lower() == 'done') and (job.jobType.lower() == 'd'):
         		c = ETL.FromTxDate(txDate)
         		c = c+datetime.timedelta(days = 1)
         		tx1 = c.strftime('%Y%m%d')
         		return tx1.endswith(txDate)
         	return True
         
         if (not cDate == txDate) or (not checkFlag == 'N'):
         	return False

         if not lFlag == 'N':
         	return True

         return True				  	   

    def CheckJobFrequency(self,frequency,txDate):
    	c = ETL.FromTxDate(txDate)
    	fileds = frequency.split("[,]")
    	check = False
    	i = 0
    	while( i < len(fileds)):
    		try:
    			t = int(fileds[i])
    		except TypeError:
    			t = 0
    		except ValueError:
    			t = 0
    		except Exception:
    			t = 0
    		if t == 0 :
    			check = False
    		elif t == -1
    			c = c + datetime.timedelta(days = 1)
    			if int(c.strftime('%d')) == 1:
    				check = True
    		elif t > =1 and t <= 31:
    			if int(c.strftime('%d')) == t:
    				check = True
    		else :
    			if (t<41) or (t>47) or (not int(c.strftime('%w'))+1 == (t-40+1)):
    				i = i+1
    				continue
    			check = True									
    		i = i+1
    	return check
    		        				

    def UpdateSourceLastCount(self,source):
    	sqlText = "update ETL_Job_Source set LastCount = extract(day FROM sysdate) WHERE Source = '" +source + "'"
    	try:
            cursor = self.conn.cursor()
            cursor.execute(sqlText)
            self.conn.commit()
        except oracle.DatabaseError as e:
            logging.error("Database Error Message: "+str(e.message))
            self.dberr = True
            return False
        
        return True
            



if __name__ == '__main__':
	pass	

