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
from RCV import RCV
from ControlFilter import ControlFilter
from ContentControl import ContentControl
from JobInfo import JobInfo
from JobRunner import JobRunner


logging.basicConfig(level=logging.INFO)

class Master(object):
	conn = None
	dberr = False
	lastRunTime = '-1'

	def getPendingJob(self):
		ret = []
		selPendingJob = "select B.etl_system, B.etl_job, (B.Last_TXDate(INTEGER))+19000000 , B.JobType FROM \
						ETL_Job_TimeWindow A, ETL_Job B \
						where A.etl_system = B.etl_system AND A.etl_job = B.etl_job AND Last_JobStatus = 'Pending' AND \
						B.Enable = '1' AND B.ETL_Server = '"+ETL.Auto_server+"'\ AND ( CASE WHEN %d>=beginhour AND %d<=endhour THEN allow \
						WHEN (beginhour >= endhour ) AND ( %d>=beginhour OR %d <= endhour) THEN allow \
						ELSE CASE allow WHEN 'Y' THEN 'N' ELSE 'Y' END END )  = 'Y'\
						ORDER BY B.Job_Priority DESC, B.Last_TXDate, B.Last_StartTime "
		hour = int(datetime.datetime.now().strftime('%H'))
		sqlText = selPendingJob %(hour,hour,hour,hour)

		try:
			cursor = self.conn.cursor()
            cursor.execute(sqlText)
            result = cursor.fetchall()
            for resTuple in result:
            	rowStr = (resTuple[0],resTuple[1],resTuple[2],resTuple[3])
            	ret.append(rowStr)
		except oracle.DatabaseError as e:
    		logging.error("Database Error Message: "+str(e.message))
    		self.dberr = True
    	return ret	


	def getControlFile(self,conn,pendingJob,path):
		w = pendingJob.split("\t")
		sqlText = "select Conv_File_Head FROM ETL_Job_Source WHERE ETL_System ='"+w[0]+"' AND ETL_Job = '"+w[1]+"'"
		cf = None
		try:
			cursor = self.conn.cursor()
            cursor.execute(sqlText)
            result = cursor.fetchall()
            for resTuple in result:
            	conSrc = resTuple[0]
            	ctrlFile = w[0] + "_" + convSrc + "_" + w[2] + ".dir"
            	cf = path + "/" + ctrlFile
            	if os.path.exists(cf):
            		break	
		except oracle.DatabaseError as e:
    		logging.error("Database Error Message: "+str(e.message))
    		self.dberr = True
    	return cf


	def checkJobDependency(self,sys,job,txdate):
		ret = True
		dsys = None
		djob = None
		sqlText = "select Dependency_System, Dependency_Job, B.Last_JobStatus, B.Last_TXDate FROM "
		+ "ETL_Job_Dependency A,  ETL_JOB B WHERE A.ETL_System = '" + sys + "' AND A.ETL_Job = '" + job
		+ "' AND A.Enable = '1' AND A.Dependency_System=B.ETL_System AND A.Dependency_Job = B.ETL_Job " 
		+ "AND (B.Last_JobStatus <>'Done' OR B.Last_TXDate < CAST('" + txdate + "' AS DATE FORMAT 'YYYYMMDD')) "
		try:
			cursor = self.conn.cursor()
            cursor.execute(sqlText)
            result = cursor.fetchone()
            if result is not None:
            	dsys = result[0]
            	djob = result[1]
            	ret = False

            if not ret:
            	logging.info(ETL.ShowPrefixSpace())
            	logging.info("Dependent job [" + dsys + "," + djob + "] does not finish yet, wait for next time!")
            	if ETL.IsJobAlreadyHasEvent(sys + "_" + job, "Dependent Job"):
            		return ret
            	eventDesc = "[" + sys + "," + job + "] has dependent job[" + dsys + "," + djob + "] did not finish yet!"
            	ETL.InsertEventLog(con, "MAS", "L", eventDesc)			

		except oracle.DatabaseError as e:
    		logging.error("Database Error Message: "+str(e.message))
    		self.dberr = True

    	return ret	

	def doSchedule(self,conn):
		tDate = datetime.datetime.now().strftime('%Y%m%d')
		tTime = datetime.datetime.now().strftime('%H%M')
		if tTime == self.lastRunTime:
			return 	False
		if not ETL.isPrimaryServer:
			return False
		self.lastRunTime = tTime
		## sql比较复杂，暂时先不改写
		sqlText = """
		"""%(tDate,ETL.Auto_server,tTime)
		count = 0 
		jobList = []
		try:
			cursor = conn.cursor()
            cursor.execute(sqlText)
            result = cursor.fetchall()
            for resTuple in result:
            	count = count + 1
            	jobList.append((resTuple[0],resTuple[1],resTuple[2],resTuple[3]))
		except oracle.DatabaseError as e:
    		logging.error("Database Error Message: "+str(e.message))
    		self.dberr = True

    	if count >0:
    		logging.info(ETL.ShowTime())
    		logging.info("Timely Triggered Job")

    	sqlText = """
    	insert into ETL_Job_Queue(ETL_Server, SeqID, ETL_System, ETL_Job, TXDate, RequestTime)
    	values
    	(%s,0,%s,%s,%s,'%s')
    	"""

    	try:
			cursor = conn.cursor()
			for i in range(0,count):
    			jobW = jobList[i]
            	cursor.execute(sqlText%(jobW[0],jobW[1],jobW[2],jobW[3],ETL.GetDateTime()))
            	self.conn.commit()
            	logging.info(ETL.ShowPrefixSpace())
            	logging.info("Generate job [" + jobW[1] + "," + jobW[2] + "] into job queue for '" + jobW[3] + "'")
		except oracle.DatabaseError as e:
    		logging.error("Database Error Message: "+str(e.message))
    		self.dberr = True
    	
    	if count>0:
    		sqlText = """
    		update ETL_Server SET lastRunDate=CAST('%s' AS DATE FORMAT 'YYYYMMDD'), lastRunTime=%s
    		""" % (tDate,tTime)
    		try:
				cursor = conn.cursor()
				cursor.execute(sqlText)
				self.conn.commit()
				logging.info(ETL.ShowPrefixSpace())
				logging.info("Update Automation lastRunDate and time to '" + tDate + " " + tTime + "'.")
			except oracle.DatabaseError as e:
				logging.error("Database Error Message: "+str(e.message))
				self.dberr = True

	def CheckJobQueue(self,conn):
		self.dberr = False
		sqlText1 = """
		select A.ETL_System, A.ETL_Job, (TXDate (INT))+19000000 , B.SOURCE 
		from etl_job_queue A, ETL_Job_Source B WHERE ETL_Server = '%s' 
		AND A.ETL_System = B.ETL_System 
		AND A.ETL_Job = B.ETL_Job
		order by (TXDate (INT))+19000000
		""" % (ETL.Auto_server)

		sqlText2 = """
		delete from ETL_Job_Queue WHERE ETL_Server='%s' 
		and ETL_System = '%s' AND ETL_Job = '%s' AND TXDate = '%s'
		"""

		count = 0
		try:
			cursor = conn.cursor()
			cursor.execute(sqlText1)
			result = cursor.fetchall()
			for resTuple in result:
				if count = 0:
					logging.info(ETL.ShowPrefixSpace())
					logging.info("Generate control file from ETL_Job_queue")
				count = count+1
				sys = resTuple[0]
				job = resTuple[1]
				txDate = resTuple[2]
				source = resTuple[3]
				logging.info(ETL.ShowPrefixSpace())
				logging.info("System:[" + sys + "], Job:[" + job + "], TxDate:[" + txDate + "]")
				logging.info(ETL.ShowPrefixSpace())
				logging.info("Generate control file dir." + source + txDate)
				txDate = txDate[0:4]+"-"+txDate[4:6]+"-"+txDate[6,8]
				fileName = ETL.Auto_home + "/DATA/receive/dir." + source + txDate
				RCV.IsSizeStable(fileName)
				try:
					cursor = conn.cursor()
					cursor.execute(sqlText2%(ETL.Auto_server,sys,job,txDate,))
					conn.commit()
				except oracle.DatabaseError as e:
					logging.error("Database Error Message: "+str(e.message))
					self.dberr = True
		except oracle.DatabaseError as e:
			logging.error("Database Error Message: "+str(e.message))
			self.dberr = True

		return self.dberr			

	def CheckJobQueue1(self,conn):
		self.dberr = False
		sqlText1 = """
		select A.ETL_System, A.ETL_Job, (TXDate (INT))+19000000 , B.SOURCE 
		from etl_job_queue A, ETL_Job_Source B WHERE ETL_Server = '%s' 
		AND A.ETL_System = B.ETL_System 
		AND A.ETL_Job = B.ETL_Job
		order by (TXDate (INT))+19000000
		""" % (ETL.Auto_server)

		triggeredJobs = []
		count = 0
		try:
			cursor = conn.cursor()
			cursor.execute(sqlText1)
			result = cursor.fetchall()
			for resTuple in result:
				if count = 0:
					logging.info(ETL.ShowPrefixSpace())
					logging.info("Generate control file from ETL_Job_queue")
				count = count+1
				sys = resTuple[0]
				job = resTuple[1]
				txDate = resTuple[2]
				source = resTuple[3]
				logging.info(ETL.ShowPrefixSpace())
				logging.info("System:[" + sys + "], Job:[" + job + "], TxDate:[" + txDate + "]")
				logging.info(ETL.ShowPrefixSpace())
				logging.info("Generate control file dir." + source + txDate)
				txDate = txDate[0:4]+"-"+txDate[4:6]+"-"+txDate[6,8]
				triggeredJobs.append((sys,job,txDate))
				fileName = ETL.Auto_home + "/DATA/receive/dir." + source + txDate
				RCV.IsSizeStable(fileName)

			if count >0:
				for triggeredJob in triggeredJobs:
					sqlText = """
					delete from ETL_Job_Queue WHERE ETL_Server='%s' and
					ETL_System = '%s' AND ETL_Job = '%s' AND TXDate = '%s'
					""" % (ETL.Auto_server,triggeredJob[0],triggeredJob[1],triggeredJob[2])
					try:
						cursor = conn.cursor()
						cursor.execute(sqlText)
						conn.commit()
					except oracle.DatabaseError as e:
						logging.error("Database Error Message: "+str(e.message))
						self.dberr = True
		except oracle.DatabaseError as e:
			logging.error("Database Error Message: "+str(e.message))
			self.dberr = True			

		return self.dberr

	def CheckPendingJob(self,conn):
		jobs = getPendingJob()
		for i in range(0,len(jobs)):
			if self.dberr:
				return False
			sys = jobs[i][0]
			job = jobs[i][1]
			txdate = jobs[i][2]
			jobType = jobs[i][3]
			ctlF = getControlFile(conn,jobs[i],ETL.Auto_home+"/DATA/queue")
			if ctrlF is None:
				logging.info(ETL.ShowTime())
				logging.info("The Pending Job [" + sys + "," + job + "] has not JOB-Source")
				if ETL.IsJobAlreadyHasEvent(sys + "_" + job, "has not JOB-Source"):
					break
				eventDesc = "[" + sys + "," + job + "] need define JOB-Source !"
				ETL.InsertEventLog(con, "MAS", "M", eventDesc)
			else :
				logging.info(ETL.ShowTime())
				logging.info("Processing control file '" + os.path.basename(ctrlF) + "'")
				logging.info(ETL.ShowPrefixSpace())
				logging.info("System:[" + sys + "], Job:[" + job + "], TxDate:[" + txdate + "]")
				if not checkJobDependency(sys,job,txdate):
					continue
				if (jobType == 'V') and (JobRunner.RunningCount.get() >= ETL.AutoMaxJobCount):
					logging.info(ETL.ShowTime())
					logging.info("Current running jobs reached the limitation, wait for next time!")
					if ETL.IsJobAlreadyHasEvent(sys + "_" + job, "Max Job Count Job"):
						break
					eventDesc = "[" + sys + "," + job + "] Current running jobs has reached the limitation, wait for next time!"
					ETL.InsertEventLog(con, "MAS", "M", eventDesc)
					break
				if (jobType == 'V') and (getStepNumber(sys, job) < 1):
					logging.info(ETL.ShowPrefixSpace())
					logging.info("No steps for the job, wait for next time!")
					if ETL.IsJobAlreadyHasEvent(sys + "_" + job, "No Job Step"):
						continue
					eventDesc = "[" + sys + "," + job + "] No steps for the job, wait for next time!"
					ETL.InsertEventLog(con, "MAS", "H", eventDesc)
				else:
					logging.info(ETL.ShowPrefixSpace())
					logging.info("Check Dependant Job Running")
					if isDependentJobRunning(conn,sys,job):
						logging.info(ETL.ShowPrefixSpace())
						logging.info("There is dependant job running, wait for next time!")
						if ETL.IsJobAlreadyHasEvent(sys + "_" + job, "Dependant Job Running"):
							continue
						eventDesc = "[" + sys + "," + job + "]has dependant job running, wait for next time!"
						ETL.InsertEventLog(con, "MAS", "L", eventDesc)
					else :
						logging.info(ETL.ShowPrefixSpace())
						logging.info("Check Group Head Job Running")
						if isHeadJobRunning(conn,sys,job):
							logging.info(ETL.ShowPrefixSpace())
							logging.info("There is group head job running, wait for next time!")
							if ETL.IsJobAlreadyHasEvent(sys + "_" + job, "Head Job Running"):
								continue
							eventDesc = "[" + sys + "," + job + "] has group head job running, wait for next time!"
							ETL.InsertEventLog(con, "MAS", "L", eventDesc)
						else:
							logging.info(ETL.ShowPrefixSpace())
							logging.info("Check Group Child Job Running")	
							if isChildJobRunning(conn,sys,job):
								logging.info(ETL.ShowPrefixSpace())
								logging.info("There is group child job running, wait for next time!")
								if ETL.IsJobAlreadyHasEvent(sys + "_" + job, "Child Job Running"):
									continue
								eventDesc = "[" + sys + "," + job + "] has group child job running, wait for next time!"
								ETL.InsertEventLog(con, "MAS", "L", eventDesc)
							else:
								logging.info(ETL.ShowPrefixSpace())
								logging.info("Submit the job[" + sys + "," + job + "] control file=" + ctlF.getName())
								ct = ContentControl(os.path.dirname(ctlF))
								ct.MoveTo(ETL.Auto_home + "/DATA/process")
								ct.updateFileLocation(conn,ETL.Auto_home + "/DATA/process", sys, job)
								JobRunner.submitFisrtStep(con, sys, job, os.path.basename(ctlF))
		return self.dberr

	def isChildJobRunning(self,conn,sys,job):
		sqlText = """
		select C.Last_JobStatus
		from ETL_Job_GroupChild A, ETL_Job_Group B,  ETL_JOB C
		WHERE B.ETL_System = '%s' AND B.ETL_Job = '%s' AND A.Enable = '1' 
		AND A.GroupName=B.GroupName AND A.ETL_System  = C.ETL_System AND A.ETL_Job = C.ETL_Job
		and C.Last_JobStatus ='Running'
		""" % (sys,job)
		ret = False
		try:
			cursor = conn.cursor()
			cursor.execute(sqlText1)
			result = cursor.fetchall()
			if result:
				ret = True
		except oracle.DatabaseError as e:
			logging.error("Database Error Message: "+str(e.message))
			self.dberr = True
		return ret
				
	def isHeadJobRunning(self,conn,sys,job):
		sqlText = """
		select C.Last_JobStatus
		from ETL_Job_GroupChild A, ETL_Job_Group B,  ETL_JOB C
		WHERE A.ETL_System = '%s' AND A.ETL_Job = '%s' AND A.Enable = '1' 
		AND A.GroupName=B.GroupName AND B.ETL_System  = C.ETL_System AND B.ETL_Job = C.ETL_Job
		and C.Last_JobStatus ='Running'
		""" % (sys,job)
		ret = False
		try:
			cursor = conn.cursor()
			cursor.execute(sqlText1)
			result = cursor.fetchall()
			if result:
				ret = True
		except oracle.DatabaseError as e:
			logging.error("Database Error Message: "+str(e.message))
			self.dberr = True
		return ret

	def isDependentJobRunning(self,conn,sys,job):
		sqlText = """
		select Dependency_System, Dependency_Job
		from ETL_Job_Dependency A,  ETL_JOB B
		WHERE A.ETL_System = '%s' AND A.ETL_Job = '%s' AND A.Enable = '1' 
		AND A.Dependency_System=B.ETL_System AND A.Dependency_Job = B.ETL_Job 
		AND B.Last_JobStatus ='Running' 
		union all
		select A.ETL_System , A.ETL_Job
		FROM ETL_Job_Dependency A,  ETL_JOB B 
		WHERE A.Dependency_System = '%s' AND A.Dependency_Job = '%s'
		AND A.Enable = '1' AND A.ETL_System=B.ETL_System AND A.ETL_Job = B.ETL_Job
		AND B.Last_JobStatus ='Running'
		""" % (sys,job,sys,job)
		ret = False
		try:
			cursor = conn.cursor()
			cursor.execute(sqlText1)
			result = cursor.fetchall()
			if result:
				ret = True
		except oracle.DatabaseError as e:
			logging.error("Database Error Message: "+str(e.message))
			self.dberr = True
		return ret

	def getStepNumber(self,sys,job):
		sqlText = """
		select count(1) from etl_job_step
		WHERE ETL_System = '%s' AND ETL_Job = '%s' and Enable='1'
		""" % (sys,job)
		count = 0
		try:
			cursor = conn.cursor()
			cursor.execute(sqlText1)
			result = cursor.fetchall()
			count = result[0]
		except oracle.DatabaseError as e:
			logging.error("Database Error Message: "+str(e.message))
			self.dberr = True
		return count	

	def ResetRunningJob(self,conn):
		sqlText = """
		update ETL_Job SET Last_JobStatus='Failed', JobSessionID = JobSessionId+1
		WHERE Last_JobStatus = 'Running' AND ETL_Server='%s'
		""" % (ETL.Auto_server)
		ret = False
		try:
			cursor = conn.cursor()
			cursor.execute(sqlText1)
			rc = cursor.rowcount()
			if rc >0:
				logging.info(ETL.ShowPrefixSpace())
				logging.info("Warning Total " + rc +" Jobs is abend because of DWAuto Restart.")
			ret = True	
		except oracle.DatabaseError as e:
			logging.error("Database Error Message: "+str(e.message))
			self.dberr = True
		return ret											

if __name__ == '__main__':
	pass
