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
            	rowStr = resTuple[0] + "\t" + resTuple[1] + "\t" + resTuple[2] + "\t" + resTuple[3]
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
		pass

	def CheckJobQueue(self,conn):
		pass

	def CheckJobQueue1(self,conn):
		pass

	def CheckPendingJob(self,conn):
		pass

	def isChildJobRunning(self,conn,sys,job):
		pass

	def isHeadJobRunning(self,conn,sys,job):
		pass

	def isDependentJobRunning(self,conn,sys,job):
		pass

	def getStepNumber(self,sys,job):
		pass

	def ResetRunningJob(self,conn):
		pass											

if __name__ == '__main__':
	pass
